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

---

# 长会话连续佩戴：自动分段、跨段查询、容量回收

面向连续佩戴数十小时的会话，管线在原有批次能力之上增加三块：**长会话自动分段**、
**按时间范围跨段查询 / 导出**、**存储容量上限与回收**。所有新能力只追加、不破坏旧布局，
升级前已经落盘的数据无需迁移即可被查询读到。

## 9. 段（Segment）与自动分段

长会话不再滚成一个越写越大的批次，而是按三个条件中**最先到达者**顺序切成多段并封存：

| 配置 | 含义 |
|---|---|
| `BatchSize` | 段内标定样本数上限（默认 256） |
| `SegmentMaxAge` | 段自打开起的最大墙钟跨度（原 `FlushInterval` 仍是其别名） |
| `SegmentMaxBytes` | 段在盘上的累计字节数上限（帧总长，每帧 append 后统计） |

切段只发生在单 worker 的“一样本处理完成”之后：被接受的样本要么完整落在旧段、要么完整
落在新段，**不会跨段拆帧、丢样本或串样本**。跨段按时间读出的结果与整段不切时一致
（测试 `TestSegmentation_ByCountAndTime` 用 250 样本 / 5 段强校验逐点时间线相同）。

封存时在帧尾追加一条**持久化 footer**（`recFooter`，含 `ClosedAt`）并 fsync + rename。
因此“干净封存”与“只有完整帧但没写完”在重启后可明确区分：

```
INCOMPLETE(.partial，无 footer/尾帧撕裂) ──rename + footer──▶ COMPLETE(.batch) ──gzip──▶ ARCHIVED(.batch.gz)
```

### 重启后续接

`Pipeline.Resume()` 从盘上已有的 `<session>-batch-NNNNNN` 编号推出下一个序号：

* 旧的完整段保持不动；
* 上次没写完的段保持 INCOMPLETE、内容与编号都保留，**新来的数据绝不接进该段**；
* 新数据写进编号更大的**新段**，不会覆盖任何旧文件（测试 `TestRestart_ContinuesAppends`）。

## 10. 段索引

`Open` 时维护并对账 `index/segments.json`：记录每段的会话、状态、相对路径、盘上大小、
`StartedAt/ClosedAt`、首末记录时间、样本/事件/拒绝计数、是否有撕裂尾帧。

* 封存 / 归档 / 回收后原子重写索引；开放段的时间范围与计数在内存索引中随 append 实时推进
  （不落盘刷索引，避免每条记录都重写索引文件）。
* 重启后只对“新增 / 变化 / 未封存”的段做一次**常量内存流式扫描**，未变化的已封存段直接
  复用索引（仅 stat 校验大小）。
* 回收在“删数据文件”和“改索引”之间崩溃也会在下次 `Open` 自动收敛：索引中存在但文件已无
  的条目被丢弃。

## 11. 按时间范围跨段查询 / 导出（`pkg/query`）

`query.Export(store, Filter, yield)` 与 `query.Completeness(store, Filter)` 支持：

* `From / To`（**闭区间**）、`Device`、`Sensor`、`SessionID` 过滤；
* 样本、事件、拒绝/处理记录三类都在结果中；
* 自动包含 **COMPLETE / INCOMPLETE / ARCHIVED** 段，归档段解 gzip 后结果与未归档逐点一致
  （测试 `TestExport_CrossSegmentOrderAndEquivalence`）；
* 对升级前无 footer、无索引的老 `.batch` 同样可读，不迁移、不重导
  （测试 `TestSegment_LegacyDataNoMigration`）。

### 只处理覆盖到的段

选段完全基于索引的首末时间做区间相交判断，**不会因为库里段多而扫整库**；窄区间只打开
命中的段。段在“列索引→拿到租约”之间若刚好被回收，导出对该段做幂等跳过。

### 顺序确定、可复现

段内记录本就是多传感器交错、并非全局时间有序，因此每段先流式过滤并在内存中按以下**全序键**
排序，再做多路堆合并：

```
时间 → 记录类别(sample<event<rejected) → 设备 → 传感器 → 段内帧位置 → 段 ID
```

同一输入与同一磁盘状态恒产生同一输出。

### 边读边输出、内存有界

记录通过 `yield` 回调逐条产出；每段独立扫描、过滤、排序，合并堆每段只持有一个有序切片。
内存占用为 **O(最大单段大小)**，不随查询范围或段总数线性增长
（测试 `TestExport_StreamingBounded` 校验 8 段 ×200 样本全量有序产出）。

## 12. 完整性（Completeness）的定义

查询结果把 `[From,To]` 切成分段不交、首尾相接的时间片，每片三选一：

| 判定 | 定义 |
|---|---|
| `COMPLETE` | 被已封存完整段（COMPLETE/ARCHIVED，无撕裂）覆盖，且该时刻没有缺陷证据 |
| `INCOMPLETE` | 落在没写完 / 尾帧撕裂的段上，或该时刻存在丢包缺口 / 拒绝-处理记录 |
| `NO_DATA` | 没有任何段覆盖该时间片 |

**“没有数据”和“数据不完整”严格区分**：断采区间是 `NO_DATA`；有记录但段没写完、发生过
丢包或拒绝，则只把对应时间片标 `INCOMPLETE`。`Report` 同时给出：

* `Ranges`：带起止、判定、原因的完整分区；
* `ReasonCounts`：按原因（`packet_loss`、`clock_rollback`、`session_closed` 等）区分的小结；
* `Defects`：每条丢包 / 拒绝证据；
* `Segments`：本次命中的段元数据。

一个丢包点只把该时刻标为 INCOMPLETE，段内其余时刻仍 COMPLETE（见
`TestCompleteness_Verdicts`）。

## 13. 容量上限与回收

`Store.SetQuota(limitBytes)`（pipeline 中对应 `Config.StorageLimit`）限制数据文件总量。
超配额时 `EnforceQuota` 按**最老优先**（首记录时间，其次段 ID）回收，且只回收：

* 已封存的 COMPLETE / ARCHIVED 段；

**绝不动**：正在写的段、没写完（INCOMPLETE/撕裂）的段、当前正被读 / 导出（持有租约）的段。
若剩余超量只能由受保护段解释，返回非零 `residual` 而不是误删最新数据。

### 租约与并发

`OpenSegmentReader` 成功即对该段加租约，`Close` 释放；归档与回收都在“定位→判定→执行”
全程持 `ioMu` 与租约检查，读者要么拿到一个完整可读且不会消失的段，要么收到 `ErrNotFound`
（已被回收，导出跳过）/ `ErrSegmentBusy`（归档遇到在读者，稍后重试）。因此回收与采集、
查询、导出并发时不会出现读半截、时间线错位或误删（测试
`TestSegment_ConcurrentAppendReadReclaim`、`TestConcurrent_IngestQueryReclaim`，竞态检测
多跑稳定通过）。

### 崩溃一致与审计

每次回收走崩溃一致流程：

1. 在 `reclaim/pending/<op>.json` 写 `PENDING` 记录（段 ID、状态、将释放字节）；
2. 删除数据文件并 fsync 目录；
3. 向 `reclaim/journal.jsonl` 追加 `COMMITTED` 审计（段、释放字节、原因、时间、op id）；
4. 删除 pending、更新索引。

`Open` 时对遗留 pending 逐条收敛：数据文件已删除→补审计并清索引（`crash-recovery`）；
数据文件还在→中止回收、段保持完整可读。因此回收中途被杀后，不会出现“索引查得到但读不到
内容”或反之。审计可用 `Store.ReclaimLog()` 读出，记录回收了哪些段、释放了多少字节
（测试 `TestQuota_ReclaimCrashRecovery`、`TestQuota_OldestFirstReclaim`）。

## 14. 对已有落盘数据的兼容范围

* 旧 `.partial / .batch / .batch.gz` 文件位置与帧格式不变；
* 旧的已封存段**没有 footer**：读取时按文件后缀仍判为 COMPLETE（不依赖 footer），
  首末时间等索引信息在首次 `Open` 扫描时补建——**不改动数据文件、不需要迁移或重导**；
* footer 是新增的第 5 类帧，旧版读取器遇到未知类型会报错，故混用版本时应以升级版读取为准。

## 15. 本地验证

```bash
# 全部单元/集成/并发测试（竞态检测，多跑）
CGO_ENABLED=1 go test ./... -race -count=3

# 只看新能力
go test ./pkg/storage/ -run 'Segment|Quota|Archive|Legacy|Concurrent' -v
go test ./pkg/query/   -run 'Export|Completeness' -v
go test ./pkg/pipeline/ -run 'Segmentation|Restart|Concurrent' -v

# 静态检查
go build ./... && go vet ./...

# 端到端：小段自动切分、跨段查询、5s 后归档、1KiB 配额触发最老回收+审计
go run ./cmd/demo --segsize 30 --archive-after 5s --quota 1024
```
