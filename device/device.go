// Package device 提供本地可运行的可穿戴模拟设备。
package device

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/lechandonga/biosignal-capture-pipeline-go/timeseries"
)

// SensorConfig 描述一个模拟传感器及其网络/时钟故障注入参数。
type SensorConfig struct {
	SensorID string
	Signal   timeseries.SignalType
	RateHz   int
	// DropRate 每样本独立丢包概率 [0,1)。
	DropRate float64
	// DriftPPM 设备时钟相对真实时间的漂移率（ppm，正值=走慢）。
	DriftPPM float64
	// Jitter 采样时间戳抖动上界。
	Jitter time.Duration
	// Baseline/Amplitude 模拟正弦波形参数。
	Baseline  float64
	Amplitude float64
	// Phase 波形初相（弧度）。
	Phase float64
	// QualityLow 允许模拟质量下降；质量在 [QualityLow,1] 间均匀取值。
	QualityLow float64
}

// Config 描述一台模拟设备及其挂载的多个传感器。
type Config struct {
	DeviceID string
	Sensors  []SensorConfig
	// DisconnectAfter/ReconnectAfter 控制周期性断连与重连；为零则不断连。
	DisconnectAfter time.Duration
	ReconnectAfter  time.Duration
}

// EventKind 设备事件类型。
type EventKind int

const (
	EventSample       EventKind = iota // 样本
	EventDisconnected                  // 设备断连
	EventReconnected                   // 设备重连
)

// Event 是设备产出的事件：样本或连接状态变化。
type Event struct {
	Kind     EventKind
	At       time.Time // 主机时刻
	Sample   timeseries.Sample
	DeviceID string
}

// sensorRuntime 是传感器在设备内的运行时状态。
type sensorRuntime struct {
	cfg         SensorConfig
	seq         uint64
	nextTick    time.Time
	deviceRef   time.Time // 该传感器首个样本时的设备时钟读数
	hostRef     time.Time // 对应的主机时刻
	initialized bool
}

// Device 是一台本地模拟设备。
type Device struct {
	cfg Config
	rng *rand.Rand
}

// New 创建模拟设备。
func New(cfg Config, seed int64) *Device {
	if cfg.DeviceID == "" {
		cfg.DeviceID = "device"
	}
	return &Device{cfg: cfg, rng: rand.New(rand.NewSource(seed))}
}

// DeviceID 返回设备 ID。
func (d *Device) DeviceID() string { return d.cfg.DeviceID }

// validate 检查配置合法性。
func (d *Device) validate() error {
	if len(d.cfg.Sensors) == 0 {
		return fmt.Errorf("device %s has no sensors", d.cfg.DeviceID)
	}
	for _, sc := range d.cfg.Sensors {
		if sc.RateHz <= 0 {
			return fmt.Errorf("sensor %s has non-positive rate %d", sc.SensorID, sc.RateHz)
		}
		if sc.DropRate < 0 || sc.DropRate >= 1 {
			return fmt.Errorf("sensor %s drop-rate %v out of [0,1)", sc.SensorID, sc.DropRate)
		}
	}
	if (d.cfg.DisconnectAfter == 0) != (d.cfg.ReconnectAfter == 0) {
		return fmt.Errorf("device %s: disconnect-after and reconnect-after must both be set or both zero", d.cfg.DeviceID)
	}
	return nil
}

// Run 按配置产出样本与断连/重连事件，直到 ctx 取消。
// 序列号在传感器生命周期内严格单调，丢包与断连期间都消耗序列号，
// 因此下游会以 missing_gap 形式看到可追溯的缺口。
func (d *Device) Run(ctx context.Context, out chan<- Event) error {
	if err := d.validate(); err != nil {
		return err
	}

	start := time.Now()
	rts := make([]*sensorRuntime, 0, len(d.cfg.Sensors))
	for _, sc := range d.cfg.Sensors {
		rts = append(rts, &sensorRuntime{cfg: sc, nextTick: start})
	}

	connected := true
	var disconnectAt, reconnectAt time.Time
	if d.cfg.DisconnectAfter > 0 {
		disconnectAt = start.Add(d.cfg.DisconnectAfter)
		reconnectAt = disconnectAt.Add(d.cfg.ReconnectAfter)
	}

	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case now := <-ticker.C:
			// 连接状态切换。
			if connected && !disconnectAt.IsZero() && !now.Before(disconnectAt) {
				connected = false
				emit(ctx, out, Event{Kind: EventDisconnected, At: now, DeviceID: d.cfg.DeviceID})
			} else if !connected && !now.Before(reconnectAt) {
				connected = true
				disconnectAt = now.Add(d.cfg.DisconnectAfter)
				reconnectAt = disconnectAt.Add(d.cfg.ReconnectAfter)
				emit(ctx, out, Event{Kind: EventReconnected, At: now, DeviceID: d.cfg.DeviceID})
			}

			if !connected {
				// 断连期间仍推进每个传感器的序列号（丢包），但不发送。
				for _, rt := range rts {
					rt.advanceTicks(now)
				}
				continue
			}

			for _, rt := range rts {
				for !now.Before(rt.nextTick) {
					hostTime := rt.nextTick
					rt.seq++

					// 设备时钟：参考点 + 漂移 + 抖动。
					if !rt.initialized {
						rt.hostRef = hostTime
						rt.deviceRef = hostTime
						rt.initialized = true
					}
					elapsedReal := hostTime.Sub(rt.hostRef)
					elapsedDevice := time.Duration(float64(elapsedReal) / (1 + rt.cfg.DriftPPM/1_000_000))
					jitter := time.Duration(0)
					if rt.cfg.Jitter > 0 {
						jitter = time.Duration(d.rng.Int63n(int64(2*rt.cfg.Jitter)+1) - int64(rt.cfg.Jitter))
					}
					deviceTime := rt.deviceRef.Add(elapsedDevice + jitter)

					period := time.Second / time.Duration(rt.cfg.RateHz)
					rt.nextTick = rt.nextTick.Add(period)

					// 独立丢包：序列号已消耗，样本直接不发送 => 下游缺口。
					if d.rng.Float64() < rt.cfg.DropRate {
						continue
					}

					// 正弦生理波形 + 偶发尖峰由测试/演示上层通过 Baseline/Amplitude 决定。
					tSeconds := elapsedReal.Seconds()
					value := rt.cfg.Baseline + rt.cfg.Amplitude*sinWave(2*mathPi*float64(rt.cfg.RateHz)/60*tSeconds+rt.cfg.Phase)
					quality := 1.0
					if rt.cfg.QualityLow < 1 {
						quality = rt.cfg.QualityLow + (1-rt.cfg.QualityLow)*d.rng.Float64()
					}

					ev := Event{
						Kind:     EventSample,
						At:       time.Now(),
						DeviceID: d.cfg.DeviceID,
						Sample: timeseries.Sample{
							DeviceID:   d.cfg.DeviceID,
							SensorID:   rt.cfg.SensorID,
							Signal:     rt.cfg.Signal,
							Seq:        rt.seq,
							DeviceTime: deviceTime,
							ReceivedAt: time.Now(),
							Value:      value,
							Quality:    quality,
						},
					}
					if !emit(ctx, out, ev) {
						return nil
					}
				}
			}
		}
	}
}

// advanceTicks 在断连期间空转序列号到当前时刻，使下游观测到连续缺口。
func (rt *sensorRuntime) advanceTicks(now time.Time) {
	period := time.Second / time.Duration(rt.cfg.RateHz)
	for !now.Before(rt.nextTick) {
		rt.seq++
		rt.nextTick = rt.nextTick.Add(period)
	}
}

func emit(ctx context.Context, out chan<- Event, ev Event) bool {
	select {
	case out <- ev:
		return true
	case <-ctx.Done():
		return false
	}
}

const mathPi = math.Pi

func sinWave(x float64) float64 { return math.Sin(x) }
