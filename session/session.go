// Package session 管理多设备多设备多传感器的接入会话与采样流。
package session

import (
	"context"
	"sync"
	"time"

	"github.com/lechandonga/biosignal-capture-pipeline-go/device"
	"github.com/lechandonga/biosignal-capture-pipeline-go/timeseries"
)

// Config 是会话配置。
type Config struct {
	ReorderWindow time.Duration // 允许的乱序到达重排窗口
	MaxRegression time.Duration // 可修正的时钟回退上界，超过即拒绝
	Buffer        int           // 输出事件缓冲
}

// attachedDevice 记录已挂载设备及其传感器漂移参数。
type attachedDevice struct {
	dev  *device.Device
	sigs map[string]float64 // sensorID -> drift ppm（在 Run 开始时设置）
}

// Session 是一次多设备采集会话。
type Session struct {
	id    string
	cfg   Config
	guard *timeseries.ClockGuard

	mu      sync.Mutex
	devices []*attachedDevice
	out     chan timeseries.IngestResult
	closed  bool

	closeOnce sync.Once
}

// New 创建会话。
func New(id string, cfg Config) *Session {
	buf := cfg.Buffer
	if buf <= 0 {
		buf = 256
	}
	return &Session{
		id:    id,
		cfg:   cfg,
		guard: timeseries.NewClockGuard(cfg.ReorderWindow, cfg.MaxRegression),
		out:   make(chan timeseries.IngestResult, buf),
	}
}

// ID 返回会话 ID。
func (s *Session) ID() string { return s.id }

// Attach 挂载一台模拟设备；漂移参数在 Run 启动时写入时钟保护器。
func (s *Session) Attach(d *device.Device, driftBySensor map[string]float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.devices = append(s.devices, &attachedDevice{dev: d, sigs: driftBySensor})
}

// Ingest 直接注入一个样本（也供测试与管线使用），返回处理结论。
// 处理结论尽力投递到事件流；事件流已关闭或 ctx 取消时不 panic（时钟处理仍已完成）。
func (s *Session) Ingest(ctx context.Context, sample timeseries.Sample) timeseries.IngestResult {
	res := s.guard.Ingest(sample)
	s.mu.Lock()
	closed := s.closed
	out := s.out
	s.mu.Unlock()
	if closed {
		return res
	}
	select {
	case out <- res:
	case <-ctx.Done():
	}
	return res
}

// NotifyReconnect 显式通知设备重连：对该设备全部传感器重锚时钟。
// 序列号历史在 ClockGuard 中保留，因此不会重复计入或漏计。
func (s *Session) NotifyReconnect(sensorIDs []string, at time.Time) {
	for _, id := range sensorIDs {
		s.guard.Reanchor(id, at)
	}
}

// closeOut 幂等关闭事件流。
func (s *Session) closeOut() {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		close(s.out)
		s.mu.Unlock()
	})
}

// Run 驱动所有已挂载设备，把样本与连接事件泵入时钟处理并输出结论。
// 设备断连期间的缺口由序列号检测在重连后首样本处统一暴露。
func (s *Session) Run(ctx context.Context) error {
	raw := make(chan device.Event, 64)

	s.mu.Lock()
	devs := make([]*attachedDevice, len(s.devices))
	copy(devs, s.devices)
	s.mu.Unlock()

	var wg sync.WaitGroup
	for _, ad := range devs {
		for sensorID, ppm := range ad.sigs {
			s.guard.SetSkewPPM(sensorID, ppm)
		}
		wg.Add(1)
		go func(ad *attachedDevice) {
			defer wg.Done()
			_ = ad.dev.Run(ctx, raw)
		}(ad)
	}

	sensorsByDevice := map[string][]string{}
	for _, ad := range devs {
		list := make([]string, 0, len(ad.sigs))
		for k := range ad.sigs {
			list = append(list, k)
		}
		sensorsByDevice[ad.dev.DeviceID()] = list
	}

	pumpDone := make(chan struct{})
	go func() {
		defer close(pumpDone)
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-raw:
				if !ok {
					return
				}
				switch ev.Kind {
				case device.EventDisconnected:
					// 不产出时间轴数据；缺口在重连后由序列号暴露。
				case device.EventReconnected:
					s.NotifyReconnect(sensorsByDevice[ev.DeviceID], ev.At)
				case device.EventSample:
					s.Ingest(ctx, ev.Sample)
				}
			}
		}
	}()

	wg.Wait()
	close(raw)
	<-pumpDone
	s.closeOut()
	return nil
}

// Events 返回处理结论事件流（含接收、拒绝与缺口）。
func (s *Session) Events() <-chan timeseries.IngestResult { return s.out }

// GuardIngest 直接走逻辑时钟处理（不向事件流发送），供确定性注入使用。
func (s *Session) GuardIngest(sample timeseries.Sample) timeseries.IngestResult {
	return s.guard.Ingest(sample)
}
