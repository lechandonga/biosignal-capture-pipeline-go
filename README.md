# biosignal-capture-pipeline-go

本地可运行的可穿戴生物信号采集与处理管线（Go）。多设备多传感器、不同采样率归一到
统一时间轴；在丢包、时钟漂移、断连重连、标定变更与并发读写下保证结论一致、原因可
区分、失败不污染已有数据。

- 设计、数据模型、时间对齐/标定规则、检测逻辑与验证方法见 [docs/DESIGN.md](docs/DESIGN.md)
- 运行测试：`CGO_ENABLED=1 go test ./... -race -v`
- 运行演示：`go run ./cmd/biosignal-demo --duration 3s -v`
