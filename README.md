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
| `pkg/storage` | 崩溃一致的分段落盘：长度前缀 + CRC 帧、原子 seal（按时间键重排+尾部索引）、gzip 归档、重启区分完整 / 不完整、按时间范围跨段流式导出、配额回收与审计 |
| `pkg/device` | 确定性模拟设备：多速率、丢包、漂移、断连 / 重连、时钟回退 |
| `pkg/pipeline` | 有界队列 + 背压的会话编排：时钟→标定→检测→落盘单 worker，优雅排空关闭；长会话按时间跨度/大小自动分段、重启接续、自动归档与配额回收 |
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

* **批次 `Batch`（段 segment）** 是持久化与恢复单位，状态机：
  `INCOMPLETE(.partial) ──CompleteBatch(原子rename)──▶ COMPLETE(.batch) ──ArchiveBatch(gzip)──▶ ARCHIVED(.batch.gz)`
  配额回收后追加一个仅保留元数据的墓碑状态 **RECLAIMED**（`meta/<id>.meta.json`）：数据已删除、占用为零，但时间段与审计仍可追溯。
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

## 7. 长会话自动分段（multi-day session）

* **切段条件**（`pipeline.Config`，满足任一即封存旧段、顺序开新段）：
  * `BatchSize`：段内已处理样本数；
  * `SegmentSpan`：段内统一时间跨度（按最新样本 `UniTime − 首样本时间`，与挂钟快慢无关）；
  * `SegmentBytes`：段文件累计字节数（按落盘大小计量）；
  * `FlushInterval`：挂钟空闲/年龄上限，沿用原有行为。
* **切段不丢不串**：样本由唯一的处理 worker 顺序落盘；封存时把段内帧按确定的时间键
  `(UniTime, device, sensor, seq, recordType)` **稳定重排**后原子重写为 `.batch`，并在尾部追加
  该段的时间索引帧（`recIndex`）与固定 `BSPF` footer。跨段按同一时间键做 k 路归并，
  因此切段边界不会丢样本、串样本或改变时间线（测试 `TestSegmentRolling_BoundaryNoLoss`）。
* **重启接续**：新进程扫描同一存储目录（`Store.Segments`），按 `会话ID-batch-序号` 恢复编号水位；
  新数据只写入**序号更大的新段**。上次崩溃留下的 `.partial` 恢复为 `INCOMPLETE`，保持原样、
  **永不与新数据拼接**（测试 `TestLongSession_RestartContinues`，用 `Pipeline.Abort()` 模拟硬崩溃）。

### 7.1 段的盘上布局（含升级前老数据）

```
batches/<id>.partial     正在写（帧按到达顺序追加）
batches/<id>.batch       已封存：header + 按时间键排序的数据帧 + 索引帧 + BSPF footer
archive/<id>.batch.gz    已封存段的 gzip（内容与 .batch 字节一致，直接压缩）
meta/<id>.meta.json      段元数据（时间范围、计数、字节、设备/传感器集合、缺口标记、状态）
reclaim/<id>.json        回收两阶段日志（仅在回收进行中瞬时存在）
audit/reclaim.log        追加式回收审计：哪些段、原来状态、释放字节数、时间
```

**对升级前老数据零迁移**：老的 `.batch` 没有索引帧和 footer。读取器在帧边界识别不到 footer 就按
普通帧流读到 EOF；首次需要其元数据时扫描一次、推导 `meta` sidecar 并缓存，之后与新段行为一致。
跨段查询不需要任何重导（测试 `TestLegacySegments_QueryWithoutMigration`）。

## 8. 按时间范围跨段查询 / 导出

入口：`Store.ExportRange(RangeFilter{From, To, Devices, Sensors}, emit)`，逐条回调；
`QueryRange` 是它的缓冲版。输出合并**样本、事件、拒绝/处理记录**为一条时间线。

* **只扫覆盖段**：先用每段 sidecar 的 `[FirstTime, LastTime]` 与设备/传感器集合做剪枝，
  只打开与区间相交的段；库里段再多也不会全库扫描（测试窄区间只命中 1 段）。
* **顺序确定、可复现**：每段（封存段含归档段）内部按时间键有序，跨段做 k 路堆归并；
  同一输入两次导出顺序逐点一致（测试双跑比对）。
* **归档等价**：`.batch.gz` 与未归档 `.batch` 内容一致，归并结果不因是否压缩而变化。
* **流式、内存有界**：封存段/归档段用流式 `FrameReader` + gzip reader，内存占用与并发打开的
  **段数**相关，不随范围内数据量增长；未写完段（只有一个）把完整前缀缓冲在内存里参与归并。
* **读保护**：导出开始即对覆盖段加引用（`Pin`），归档与回收跳过被引用段；文件定位/打开与
  回收删除由读写锁串行化，并发下不会读到半截文件。

### 8.1 结果里的“完整性”定义

返回的 `RangeSummary` 把 `[From, To]` 划分为三类互斥信息，**“没有数据”和“数据不完整”严格区分**：

| 类别 | 含义 |
|---|---|
| `CompleteIntervals` | 被已封存段（COMPLETE/ARCHIVED）覆盖、且不含任何缺口标记的时间段 |
| `Incomplete[]`（带 `Reason` 与小结计数 `CountsByReason`） | `packet_loss`（丢包缺口）、硬拒绝原因（如 `duplicate_sample`、`clock_rollback` 等）、`segment_incomplete`（段没封存/尾帧撕裂）、`segment_reclaimed`（该段已被配额回收） |
| `NoData[]` | 落在区间内、但**没有任何段声称覆盖**的时间段——系统只是没采到/没数据，并不代表数据坏了 |

设备/传感器过滤同样作用于完整性判断（例如只查 ECG 时，PPG 段不会影响 ECG 的覆盖区间）。

## 9. 容量上限与回收

* `QuotaBytes`（`pipeline.Config`，0 关闭）：全部**存活数据文件**的总字节上限。
  worker 的维护节拍（`MaintInterval`）在归档后调用 `Store.EnforceQuota`。
* **回收对象**：仅 COMPLETE / ARCHIVED 段，按 `(FirstTime, 段序号)` **最老优先**，直到用量 ≤ 上限。
* **绝不动**：正在写的段、INCOMPLETE 段、当前被导出 Pin 住的段（配额也不会因此删除最新数据）。
* **崩溃一致的两阶段回收**（`reclaim/<id>.json` 日志 + `audit/reclaim.log`）：
  1. 先 fsync 回收意图日志；2. 删除数据文件；3. 写 RECLAIMED 墓碑 sidecar 与追加审计；4. 删除意图日志。
  重开时 `recoverReclaim`：数据还在 → 删除日志回滚（没有任何丢失）；数据已删 → 补完墓碑与审计。
  不会出现“查得到元信息却读不到内容”或审计与实际不符（测试 `TestReclaim_CrashMidway`）。
* 查询命中已回收段时不报错、不造数据：该段贡献一个 `segment_reclaimed` 的不完整区间，
  其它段照常合并。

### 9.1 维护与并发

自动归档由 `ArchiveAfter` 控制（封存后超过该年龄即 gzip；归档同样跳过 Pin 段）。
归档/回收与采集、导出并发运行时：文件生命周期转换（rename/remove）与读者“定位→打开”由
`lifeMu` 读写锁串行化；测试 `TestConcurrent_WriteQueryArchiveReclaim` 持续混跑写入、跨段查询、
归档与回收，要求全程无撕裂、无错位、无单调序违例。

## 10. 背压、上限与资源

* 队列容量 `QueueDepth`、批次大小 `BatchSize`、刷新间隔 `FlushInterval` 均可配置，内存有界。
* `PolicyReject`：队列满立即返回 `ErrBackpressure`（原因 `buffer_full`），入队前不解析任何数据，故无残留解析状态。
* `PolicyDelay`：至多等待 `EnqueueWait` 腾位，超时同样按 `buffer_full` 拒绝。
* 关闭模型（无竞态、不泄漏）：`Run` 启动后发就绪信号；`Stop` 取写锁等待所有在途 `Ingest` 结束 → 标记关闭 → `close(queue)` → 单 worker `range` 排空全部已接受样本 → seal 最后批次。
* 会计恒等式（测试强校验）：
  `Submitted = Processed + Gaps + Rejected`，
  且并发下 `Processed + 背压数 = 提交总数`。

---

## 11. 验证方法与测试矩阵

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

长会话新增（均可用 `-race` 稳定多跑）：

| 测试 | 关注点 |
|---|---|
| `pkg/storage` `TestSegmentRolling_BoundaryNoLoss` | 乱序到达的两传感器跨两段封存后，全局时间线与不切段逐点一致；边界不丢不串 |
| `pkg/storage` `TestRangeQuery_CrossSegmentsArchiveAndCompleteness` | 跨 COMPLETE+ARCHIVED 段合并、设备过滤、归档等价、双跑可复现、只打开覆盖段；完整/不完整（丢包+拒绝+未封存）/无数据三分 |
| `pkg/storage` `TestQuota_ReclaimsOldestOnly` | 最老优先、Pin 段与在写段不动、释放字节审计、回收段在查询中标记不完整 |
| `pkg/storage` `TestReclaim_CrashMidway` | 回收两阶段中被杀：日志在而数据在→回滚无丢失；数据已删→补全墓碑与审计，存储自洽 |
| `pkg/storage` `TestConcurrent_WriteQueryArchiveReclaim` | 写入+跨段查询+归档+配额回收并发，无半截读取、无错位、无乱序 |
| `pkg/storage` `TestLegacySegments_QueryWithoutMigration` | 无 footer/索引的升级前老段被跨段查询直接读到，零迁移 |
| `pkg/pipeline` `TestLongSession_SpanRollingAndTimeline` | 按统一时间跨度切段，500 样本跨多段合并后序号 1..500 严格连续 |
| `pkg/pipeline` `TestLongSession_RestartContinues` | 模拟硬崩溃后重启：旧封存段不动、未写完段保持 INCOMPLETE、新数据写新序号段 |
| `pkg/pipeline` `TestLongSession_QuotaAndArchive` | 自动 gzip + 超配额回收与采集并发进行，最新数据总在、回收跨度可识别、审计可查 |

一次完整自检：

```bash
go build ./... && go vet ./... && go test ./...
CGO_ENABLED=1 go test -race -count=3 ./...   # 并发用例竞态检测、多跑
```

演示程序会在终端打印每个 `INPUT`、故障注入（断连 / 回退 / 重连 / 缺口）、会话会计、
带原因的拒绝日志，以及各批次的状态与事件依据。
