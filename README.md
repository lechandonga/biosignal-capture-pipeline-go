# Wearable 生物信号采集与处理管线（本地可运行）

一条本地可运行的多设备 / 多传感器生物信号采集与处理管线。用**模拟设备**与**文件存储**驱动会话，
保证采样、时钟校正、标定、事件检测与数据落盘在**丢包、时钟漂移、时钟回退、断连重连与并发访问**
下结论一致、原因可区分、全程可追溯。

无外部依赖，仅用 Go 标准库。

```bash
go run ./cmd/demo            # 端到端演示（默认 3 秒，存储在 /tmp/biosignal-demo）
go test ./...                # 全部单元 / 集成测试（日志打印输入样本与判定依据）
go test ./pkg/pipeline -v    # 重点：丢包 / 时钟回退 / 标定变更 / 并发读写
```

---

## 1. 目录与职责

| 包 | 职责 |
|---|---|
| `pkg/model` | 核心领域类型：原始样本、对齐样本、标定样本、事件、拒绝记录、批次 |
| `pkg/clock` | 每传感器时钟漂移校正；重复 / 序号回退 / 时钟回退 / 统一时间回退的明确拒绝；重连 epoch |
| `pkg/calibration` | 带版本与生效时间窗的标定与基线；非法 / 过期参数拒绝；按样本时间选版本，不回溯改写历史 |
| `pkg/detect` | 可配置、可复现的阈值规则；质量不足只输出 `UNCERTAIN`；结论携带完整依据 |
| `pkg/storage` | 崩溃一致的批次落盘：长度前缀 + CRC 帧、原子 seal、gzip 归档、重启区分完整 / 不完整 |
| `pkg/device` | 确定性模拟设备：多速率、丢包、漂移、断连 / 重连、时钟回退 |
| `pkg/pipeline` | 有界队列 + 背压的会话编排：时钟→标定→检测→落盘单 worker，优雅排空关闭 |
| `cmd/demo` | 本地端到端演示 |

---

## 2. 数据模型

```
RawSample        设备发出的原始样本
  Device/Sensor/Seq(设备单调序号)/DevTime(设备钟)/RecvTime(主机接收钟)/Values/Quality/Lost
        │  clock.Correct（统一时间轴）
        ▼
AlignedSample    归一到统一时间轴
  UniTime(校正后)/Kind/Gap(丢包标记)
        │  calibration.Store.At(UniTime) → Params.Apply
        ▼
CalibratedSample 物理单位 + 基线 + 钉住的标定版本
  Values(physical)/Baseline/CalVersion/Uncertain
        │  detect.Engine.Push
        ▼
Event            NORMAL / EVENT / ANOMALY / UNCERTAIN，含 RuleID/Threshold/Evidence/CalVersion
```

* **批次 `Batch`** 是持久化与恢复单位，状态机：
  `INCOMPLETE(.partial) ──CompleteBatch(原子rename)──▶ COMPLETE(.batch) ──ArchiveBatch(gzip)──▶ ARCHIVED(.batch.gz)`
* **`RejectedRecord`** 记录每一个未被正常处理的样本：
  * `Action`：`REJECTED`（硬拒绝）或 `HANDLED`（被显式处理，如丢包缺口）；
  * `Reason`：可区分的原因枚举（见下），`Detail` 带细节。**任何样本都不会被静默丢弃。**

拒绝 / 处理原因（`model.RejectReason`）：

| Reason | 含义 |
|---|---|
| `duplicate_sample` | 序号与当前最后序号相同（重发） |
| `sequence_regression` | 序号比当前最后序号更小（重放 / 逻辑时钟回退） |
| `clock_rollback` | 设备钟读数相对上一样本回退超出抖动容忍 |
| `unified_time_rollback` | 校正后的统一时间仍会比上一统一时间早 |
| `packet_loss` | 丢包缺口标记（HANDLED，保留依据，不臆造数据） |
| `no_valid_calibration` | 样本时间早于任何标定生效时间 |
| `expired_calibration` | 曾有标定生效，但在样本时间所有窗口都已过期 |
| `bad_calibration` | 标定参数非法或与样本通道宽度不符 |
| `buffer_full` | 有界队列满，触发背压拒绝 / 延迟 |
| `unknown_sensor` | 设备未注册或传感器未绑定 |
| `session_closed` | Stop 后仍尝试写入 |

---

## 3. 时间对齐规则

1. **序号是单调真相**。每传感器维护 `lastSeq`：
   * `seq == lastSeq` → `duplicate_sample`；`seq < lastSeq` → `sequence_regression`。二者在落盘前即被拒绝，重连后重发旧序号也不会重复计入。
2. **设备钟不得回退**：`DevTime` 比上一接受样本早超过 5ms 抖动容忍 → `clock_rollback`。时钟回退是**致命事件**：该 epoch 不再接受样本，设备必须 `Reconnect` 开新校正 epoch 后才能恢复（序号仍连续，故不重不漏）。
3. **漂移估计**：用锚点 `(设备已流逝时间, 瞬时偏移 host−dev)` 在当前 epoch 内做最小二乘线性拟合，斜率钳制在 ±5% 物理合理范围；基于“相对已流逝时间”而非绝对 Unix 秒，避免大时间戳淹没截距。`unified = devTime + offset`。
4. **统一轴严格单调**：校正结果若不严格晚于上一统一时间 → `unified_time_rollback`，绝不污染时间轴。
5. **多速率归一**：不同传感器按各自速率推进设备时间（`pkg/device`），统一以 `UniTime` 落在同一时间轴。
6. **丢包不臆造**：丢包位置产生 `Lost=true` 缺口标记，只做序号连续性检查（`CheckSequence`）、**不参与时钟拟合**（否则缺口的合成时间会污染漂移估计）。检测器对缺口只能给 `UNCERTAIN`。

---

## 4. 标定与基线规则

* 每个 `Params` 含 `Version / Sensor / [Effective, Expires) 时间窗 / Gain / Offset / Baseline`。
* **注册即校验**：版本非空、窗合法（Expires>Effective）、三个向量等长、增益非零且全部有限。非法参数返回 `ErrInvalidParams`，**拒绝而非退化为默认恒等标定**。
* **按样本统一时间选版本**：`Store.At(sensor, t)` 返回在 `t` 时刻窗口覆盖的、生效时间最新的版本。
  * `t` 早于首个窗口 → `ErrNoValidCalibration`（`no_valid_calibration`）；
  * 之前有窗口但在 `t` 全部结束、无继任 → `ErrCalibrationExpired`（`expired_calibration`）。
* **不回溯**：标定存储只追加。历史样本用其时间点有效的版本计算，并把 `CalVersion` 钉在产出的每条记录与每个事件上；新增 v2 不会改变已定标的 v1 历史结论。
* **换算**：`physical[i] = Gain[i]*raw[i] + Offset[i]`，基线单独保留，便于基线相对判定且两者都可追溯。通道宽度不匹配 / 非有限值 → `bad_calibration`。

---

## 5. 事件 / 异常检测逻辑

`detect.Rule` 是纯配置，相同输入流恒产生相同结论（测试 `TestPush_EventAnomalyAndReproducibility` 双跑比对）：

```
ID/Sensor/Kind/Channel     作用域
Window + AggregateWindow   窗口与归约（MEAN/MAX/MIN）
Op(GT/LT) + High + Low     判定方向与两级阈值（Low=0 表示关闭 ANOMALY 级）
MinQuality                 最低信号质量
Cooldown                   两次确定性事件的最小样本间隔（首个事件必触发）
UseBaseline                是否对 (value - baseline) 判定
```

判定流程：

1. 取窗口归约值 `agg`；
2. 若 `Quality < MinQuality`、样本 `Uncertain` 或为丢包 `Gap` → 只输出 **UNCERTAIN**，依据字符串保留 `agg/high/low/q/minQ/cal`，**绝不在信号不足时给确定结论**；
3. 否则按 `Op` 与阈值比较：越过 `High` → `EVENT`，进一步越过 `Low` → `ANOMALY`，否则不出结论；
4. 每个 `Event` 都带 `Value / Threshold / RuleID / CalVersion / Evidence`（含窗口、归约、质量、所用标定），判定依据完全可复核。

---

## 6. 落盘一致性

* **帧格式**：`uint32 长度 | payload | uint32 CRC32(payload)`。半写 / 撕裂的尾帧在读取时被识别（`io.ErrUnexpectedEOF`），不会与后续记录错位拼接。
* **原子 seal**：写入 `.partial`，每条记录 append 后 `fsync`；`CompleteBatch` 执行 fsync + `rename` 为 `.batch`。崩溃只会留下 `.partial`。
* **重启恢复**：`Open` 后 `LoadBatch` 按 `.partial / .batch / .gz` 判定状态；带撕裂尾帧的批次恢复为 **INCOMPLETE**，只保留完整前缀，绝不与新数据静默拼接。
* **并发**：append 逐帧加锁 + fsync；归档先写 `.tmp`、fsync、再原子 rename 进 `archive/` 并删除原文件，读者任一时刻只看到“完整批次”或“完整归档”之一（测试 `TestConcurrentAppendReadArchive`）。

---

## 7. 背压、上限与资源

* 队列容量 `QueueDepth`、批次大小 `BatchSize`、刷新间隔 `FlushInterval` 均可配置，内存有界。
* `PolicyReject`：队列满立即返回 `ErrBackpressure`（原因 `buffer_full`），入队前不解析任何数据，故无残留解析状态。
* `PolicyDelay`：至多等待 `EnqueueWait` 腾位，超时同样按 `buffer_full` 拒绝。
* 关闭模型（无竞态、不泄漏）：`Run` 启动后发就绪信号；`Stop` 取写锁等待所有在途 `Ingest` 结束 → 标记关闭 → `close(queue)` → 单 worker `range` 排空全部已接受样本 → seal 最后批次。
* 会计恒等式（测试强校验）：
  `Submitted = Processed + Gaps + Rejected`，
  且并发下 `Processed + 背压数 = 提交总数`。

---

## 8. 验证方法与测试矩阵

所有测试均用 `-v` 打印 **INPUT 样本**与 **JUDGEMENT 依据**：

| 测试 | 关注点 |
|---|---|
| `pkg/clock` | 漂移估计、单调性、重复、序号回退、时钟回退、重连后旧序号拒绝 / 新序号接受 |
| `pkg/calibration` | 非法 / 零增益 / 反向窗拒绝；历史样本钉 v1、新样本用 v2（不回溯）；过期与从无标定可区分 |
| `pkg/detect` | EVENT/ANOMALY、双跑可复现、低质量与缺口只给 UNCERTAIN、Cooldown |
| `pkg/storage` | 批次往返、撕裂尾帧恢复为 INCOMPLETE、gzip 归档、并发 append/read/archive 无错位 |
| `pkg/pipeline` `TestSession_*` | 丢包 HANDLED、时钟回退、重复、序号回退、标定过期、重连恢复、新旧标定并存、事件依据、重启读回 |
| `pkg/pipeline` `TestBackpressure_*` | 深 2 队列下 20 投 18 拒，原因可区分，Stop 排空无残留 |
| `pkg/pipeline` `TestConcurrentIngest_*` | 8 传感器并发，800/800 处理，会计闭合，落盘统一时间逐传感器单调 |
| `pkg/pipeline` `TestRestart_*` | 未 seal 的批次重启后明确 INCOMPLETE，不静默拼接 |
| `pkg/device` | 多速率、确定性丢包、断连 / 重连序号不重用、时钟回退建模 |

一次完整自检：

```bash
go build ./... && go vet ./... && go test ./...
```

演示程序会在终端打印每个 `INPUT`、故障注入（断连 / 回退 / 重连 / 缺口）、会话会计、
带原因的拒绝日志，以及各批次的状态与事件依据。
