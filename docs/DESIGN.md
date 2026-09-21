# 可穿戴生物信号采集与处理管线 — 设计与验证说明

本工程是一条**本地可运行**的生物信号管线：用模拟设备产生 ECG/PPG/EDA/体温/加速度等
多速率信号，经统一时间轴对齐、版本化标定、可复现事件判定后原子落盘，并在丢包、
时钟漂移、断连重连与并发访问下保证结论一致、原因可区分、失败不污染已有数据。

## 1. 包结构

| 包 | 职责 |
|---|---|
| `timeseries` | 样本模型、逻辑时钟保护（去重/乱序/缺口/漂移/回退）、统一时间轴栅格 |
| `calibration` | 带版本与生效时间的标定/基线注册表，参数合法性与过期校验 |
| `detect` | 阈值可配置、依据可复现的事件/异常判定，质量门限与不确定结论 |
| `storage` | 分帧 CRC、批次原子提交、崩溃恢复隔离、gzip 归档压缩 |
| `backpressure` | 泛型有界队列，阻塞/拒绝/丢旧三种背压策略 |
| `device` | 本地模拟设备：多速率、丢包、漂移、抖动、周期性断连重连 |
| `session` | 多设备多传感器会话，设备事件泵 + 时钟处理 |
| `pipeline` | 全链路组装：背压入队 → 对齐 → 标定 → 检测 → 批次落盘/归档 |
| `cmd/biosignal-demo` | 可运行演示与重启读回 |

## 2. 数据模型

### 2.1 原始样本 `timeseries.Sample`
```
DeviceID / SensorID / Signal   设备、传感器、信号类型
Seq        uint64              设备内严格单调序列号（丢包、断连也消耗序列号）
DeviceTime time.Time           设备时钟时间戳（含漂移与抖动）
ReceivedAt time.Time           主机接收时刻
Value      float64             原始物理量读数
Quality    float64             信号质量 0..1
```

### 2.2 对齐样本 `timeseries.AlignedSample`
在 `Sample` 基础上增加：
```
UnifiedTime time.Time      校正后的统一时间轴时间
ClockOffset time.Duration  实际使用的锚点偏移
SkewPPM     float64        实际使用的漂移修正率（可追溯）
```

### 2.3 处理结论 `timeseries.IngestResult`
每个样本都有一个显式结论，**不静默丢弃任何异常**：
```
Accepted bool          是否进入时间轴
Reason   RejectReason  可区分原因码
Gaps     []Gap         检测到的序列号缺口
Aligned  *AlignedSample
Detail   string        人类可读依据（回退量、窗口、连续覆盖点等）
```

原因码（互斥、可区分）：

| 原因码 | 含义 | 处置 |
|---|---|---|
| `""` | 完全正常 | 接收 |
| `missing_gap` | 序列号出现缺口（当前样本仍接收） | 接收 + 缺口单列落盘 |
| `late_within_window` | 重排窗口内的迟到乱序 | 接收，统一时间不回退 |
| `clock_corrected` | 回退超窗口但 ≤ 回退上界 | 接收，时间显式修正为单调值并留痕 |
| `duplicate_sequence` | 序列号曾接收（含重连后旧包重放） | 拒绝 |
| `out_of_order` | 迟到超出重排窗口 | 拒绝 |
| `clock_regression` | 时钟回退超上界 | 拒绝，时间轴不被污染 |

`Gap` 记录缺失序列号列表、缺口估算起止统一时间与检测时刻。

## 3. 时间对齐规则

### 3.1 漂移模型与换算
每台设备以「主机接收时刻 ↔ 设备时间」为锚点。设备时钟漂移率 `skewPPM`
（正值表示设备走慢）：

```
unified = anchorHost + (deviceTime - anchorDevice) * (1 + skewPPM/1e6)
```

模拟设备按同一模型反向产生带漂移的 `DeviceTime`，因此校正后间隔回到真实间隔
（见 `TestDriftCorrectionApplied`：1s 设备间隔 @1000ppm → 统一轴 1.001s）。

### 3.2 去重、乱序与缺口
每个传感器维护：
- `maxSeq`：已接收最大序列号；
- `contiguous`：已连续覆盖到的序列号（此前全部到齐）；
- `filled`：超出 `contiguous` 的迟到补缺集合（有界，随连续推进被回收）。

判定顺序：
1. **重复优先**：序列号已接收即拒为 `duplicate_sequence`。这使断连重连后
   设备重放的旧包也被识别，绝不重复计入。
2. **缺口**：`seq > maxSeq+1` 时为 `maxSeq+1 .. seq-1` 生成 `Gap`，当前样本照常接收。
3. **时间单调性**：统一时间早于上一接收样本时按回退量三档处理（窗口内接纳 /
   阈值内修正 / 超阈值拒绝），保证统一时间轴单调、不被静默污染。

### 3.3 断连重连
- 设备断连期间模拟设备**继续消耗序列号但不发送样本**，重连后首个样本即用
  主机时刻重新锚定（`Reanchor`）；序列号历史保留。
- 因此重连不会重复计入（旧序列号判重）也不会漏计（缺口被显式列出）。

### 3.4 统一时间轴栅格 `timeseries.Grid`
固定周期 `period`（向下取整量化）。`Register` 声明预期传感器集合后：
- 多速率样本落入同一槽；
- 槽内缺失的已注册传感器进入 `Missing` 列表；
- 起止跨度内完全无样本的空槽也被显式合成，时间轴不出现静默空洞。

## 4. 标定与基线规则

### 4.1 版本与生效时间
`calibration.Calibration` 携带 `Version`、半开生效区间 `[EffectiveFrom, EffectiveTo)`、
`Offset/Scale/Baseline` 与原值合法区间 `[MinRaw,MaxRaw]`。

- 注册表按 `(传感器, 信号)` 保存并按生效起点排序；区间**不允许重叠**，版本号不允许重复。
- 查询 `At(sensor, signal, t)` 按**数据样本的统一时间**返回当时生效版本。
- **更新不回溯**：v2 在某时刻生效后，对更早数据的查询仍返回 v1，历史结论不被改写
  （`TestAddAndVersionedLookupNoRetroactiveChange`）。
- 允许为历史区间补录版本（只要不重叠），但同样不改变既存判定。

### 4.2 显式失败，绝不退化默认值
以下全部返回明确错误，管线据此输出 `uncertain` 而非套用默认阈值：
- 空 ID/空版本、`Scale<=0`、`MinRaw>MaxRaw`、区间反转 → `ErrInvalidCalibration`；
- 区间重叠 → `ErrOverlappingCalibration`；
- 查询时刻早于首个版本 → `ErrNoCalibration`；晚于已关闭版本 → `ErrExpiredCalibration`；
- 原值越界 → `ErrRawOutOfRange`。

物理量换算：`physical = (raw - Offset) * Scale`。

## 5. 检测逻辑

`detect.Rule` 完全可配置：`Low/High` 双阈值、`MinQuality` 质量门限、
`MinConsecutive` 连续越限数。判定状态三态：`normal / event / uncertain`。

处理顺序（每“传感器+事件”独立维护连续计数）：
1. 标定不可用（无标定/过期/原值越界）→ `uncertain`，原因记录，**连续计数冻结**；
2. `Quality < MinQuality` → `uncertain(signal_quality_below_threshold)`，计数冻结；
3. 缺口来源输入 → `uncertain(missing_or_gapped_input)`，计数冻结；
4. 否则越限则计数 +1，回界则清零并记录“streak reset”；
5. 连续越限 `>= MinConsecutive` 才输出确定 `event`。

每次判定都落 `Basis`：原值、标定值、基线、质量、上下限、当前/所需连续数、
标定版本号与文字依据，保证结论**可复现、可审计**。质量不足永远不会被当成确定事件
（见 `TestLowQualityIsUncertainAndFreezesCounter`）。

## 6. 落盘一致性与崩溃恢复

### 6.1 文件布局
```
<workdir>/batches/<batchID>.meta   JSON 批次头（temp+rename 原子落盘）
<workdir>/batches/<batchID>.data   分帧记录 + 提交尾标
<workdir>/batches/<batchID>.data.gz 归档（gzip）
```
数据帧：
```
"BIOS"(4) | type(1) | payloadLen(4 BE) | payload | crc32-IEEE(payload)(4)
```
提交尾标：
```
"EOT!"(4) | recordCount(8 BE) | frameBytes(8 BE) | crc32(全部帧字节)(4)
```

### 6.2 原子提交
`Seal` 先 `Sync` 帧数据，再写尾标并 `fsync`。只有尾标存在、计数/字节数/滚动哈希
全部匹配、且尾标后无尾随字节的批次才算**完整**。单帧整段写入，并发 `Append`
由互斥锁串行，读路径逐帧校验 CRC——不会出现半写或错位记录。

### 6.3 崩溃恢复（重启不静默拼接）
`Open` 扫描全部批次：
- 尾标完整 → 完整批次，可读；
- 尾标缺失（写入中崩溃，帧被截断）→ 重命名为 `.incomplete` **物理隔离**，
  读取返回 `ErrIncompleteBatch`，绝不与新批次拼接；
- 帧 CRC/魔数/尾标不一致 → 重命名为 `.corrupt` 隔离，不影响其他批次打开。

### 6.4 归档压缩
`Archive` 把已封板批次压成 gzip：先写 `*.tmp`，回读解压并比对 **sha256**，
校验通过才 rename 为正式 `.gz`，**之后**才删除原始 `.data`。rename 先于 unlink，
任一时刻「原文件或归档」至少其一存在，并发读者无空窗（见
`TestConcurrentReadWhileWritingAndArchiving`）。读取对两种形态透明。

### 6.5 批次滚动与优雅停止
按「最大记录数」或「定时 flush」滚动；批次在处理 goroutine 内串行创建/封板，
状态不被并发撕裂。关闭时先排空有界队列（已接收数据不丢），再封板/归档当前非空批次；
空批次直接清理，不产生 0 记录批次。封板/归档使用独立宽限上下文，设备 ctx
到期不影响在途批次的完整性。

## 7. 背压与资源边界

`backpressure.Queue` 为固定容量泛型队列，满时三选一：
- `PolicyBlock`：阻塞等待空位（尊重 ctx 取消/超时）；
- `PolicyReject`：立即返回 `ErrRejected`，管线计入 `RejectedBP`，调用方有明确反馈；
- `PolicyDropOldest`：丢最旧一条腾位并累计 `DroppedCount`。

容量上限固定，**绝不无界占用内存**。管线用单一 bridge goroutine 把队列项转发到
处理 select，避免“控制消息抢先”造成的孤儿读取与静默丢数据。失败路径（标定错误、
序列化错误、写盘错误）都不会推进检测计数或写出半条记录，不残留部分解析状态。

## 8. 验证方法

运行全部测试（建议开启竞态检测，需 CGO）：
```bash
CGO_ENABLED=1 go test ./... -race -v
```

关键测试与对应的需求点：

| 测试 | 覆盖需求 |
|---|---|
| `timeseries.TestNormalAndGapAndDuplicate` | 丢包缺口、重复拒绝，原因可区分 |
| `timeseries.TestClockRegressionLargeRejected` | 超大时钟回退被拒，后续时间轴仍可用 |
| `timeseries.TestClockRegressionSmallCorrected` | 阈值内回退显式修正、不回退 |
| `timeseries.TestReconnectNoDuplicateNoLoss` | 重连不重计、不漏计 |
| `timeseries.TestDriftCorrectionApplied` | 漂移校正 |
| `timeseries.TestGridAlignmentAndMissing` | 多速率归一、缺位/空槽不静默 |
| `calibration.Test*` | 参数合法性、区间重叠、非回溯版本、过期拒绝、越界、并发 |
| `detect.Test*` | 连续越限、质量不足冻结计数、无标定不确定、规则校验 |
| `storage.TestCrashRecoveryDistinguishesIncomplete` | 重启区分完整/不完整、物理隔离 |
| `storage.TestConcurrentAppend/ReadWhileWritingAndArchiving` | 并发读写归档无半写/错位（-race） |
| `storage.TestCorruptFrameRejected` | 损坏帧隔离 |
| `backpressure.Test*` | 三种背压、容量上限、ctx 取消、关闭排空 |
| `device.TestDeviceDisconnectReconnectEvents` 等 | 丢包/漂移/断连注入 |
| `session.TestSessionDeviceEndToEnd` | 真实设备会话结论流 |
| `pipeline.TestPipelineCalibrationChangeAndConsistency` | 标定变更期结论一致且只依据 v1 |
| `pipeline.TestPipelineBackpressureRejectsBounded` | 输入守恒：入队+拒绝=总数 |
| `pipeline.TestPipelineSimulatedDevicesAndRecovery` | 端到端 + 重启读回 |

所有测试均通过 `t.Logf` 打印**输入样本与判定依据**（原值、物理值、质量、
阈值、连续数、标定版本、缺口序列号、拒绝原因）。

## 9. 运行演示

```bash
go run ./cmd/biosignal-demo --duration 3s --dir ./.demo-data -v
```
输出包含：接收/拒绝/缺口/正常/事件/不确定计数、封板与归档批次数，以及模拟进程
重启后对完整批次的读回结果（归档批次自动透明解压）。
