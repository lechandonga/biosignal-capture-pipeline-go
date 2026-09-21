// Package calibration 管理带版本与生效时间的传感器标定参数与基线。
package calibration

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/lechandonga/biosignal-capture-pipeline-go/timeseries"
)

// 标定相关错误，均为显式失败，绝不退化为默认取值。
var (
	ErrInvalidCalibration     = errors.New("invalid calibration parameters")
	ErrOverlappingCalibration = errors.New("calibration effective interval overlaps an existing version")
	ErrNoCalibration          = errors.New("no calibration effective at the requested time")
	ErrExpiredCalibration     = errors.New("all calibration versions expired at the requested time")
	ErrRawOutOfRange          = errors.New("raw value outside calibrated valid range")
)

// Calibration 是某传感器某信号的一版标定参数。
// 时间区间为半开区间 [EffectiveFrom, EffectiveTo)；EffectiveTo 为零值表示长期有效。
type Calibration struct {
	SensorID      string
	Signal        timeseries.SignalType
	Version       string
	EffectiveFrom time.Time // 含
	EffectiveTo   time.Time // 不含；零值表示长期有效
	Offset        float64   // 原值零点偏移
	Scale         float64   // 缩放系数，必须为正
	Baseline      float64   // 生理基线
	MinRaw        float64   // 合法原值下界
	MaxRaw        float64   // 合法原值上界
	CreatedAt     time.Time
}

// Validate 校验标定参数合法性；任何非法项都显式拒绝。
func (c Calibration) Validate() error {
	switch {
	case c.SensorID == "":
		return fmt.Errorf("%w: empty sensor id", ErrInvalidCalibration)
	case c.Signal == "":
		return fmt.Errorf("%w: empty signal type", ErrInvalidCalibration)
	case c.Version == "":
		return fmt.Errorf("%w: empty version", ErrInvalidCalibration)
	case c.EffectiveFrom.IsZero():
		return fmt.Errorf("%w: zero effective-from time", ErrInvalidCalibration)
	case !c.EffectiveTo.IsZero() && !c.EffectiveTo.After(c.EffectiveFrom):
		return fmt.Errorf("%w: effective-to (%s) must be after effective-from (%s)",
			ErrInvalidCalibration, c.EffectiveTo, c.EffectiveFrom)
	case c.Scale <= 0:
		return fmt.Errorf("%w: scale must be positive, got %v", ErrInvalidCalibration, c.Scale)
	case c.MinRaw > c.MaxRaw:
		return fmt.Errorf("%w: min-raw %v greater than max-raw %v", ErrInvalidCalibration, c.MinRaw, c.MaxRaw)
	case c.CreatedAt.IsZero():
		return fmt.Errorf("%w: zero created-at time", ErrInvalidCalibration)
	}
	return nil
}

// contains 判断时刻 t 是否落在本版本的半开生效区间内。
func (c Calibration) contains(t time.Time) bool {
	if t.Before(c.EffectiveFrom) {
		return false
	}
	if !c.EffectiveTo.IsZero() && !t.Before(c.EffectiveTo) {
		return false
	}
	return true
}

// Apply 按本版标定把原始值换算为物理量：physical = (raw - Offset) * Scale。
// 原值超出合法区间时显式报错，由上层标记不确定，而不是静默给出结论。
func (c Calibration) Apply(raw float64) (float64, error) {
	if raw < c.MinRaw || raw > c.MaxRaw {
		return 0, fmt.Errorf("%w: sensor %s value %v not in [%v,%v] (cal %s)",
			ErrRawOutOfRange, c.SensorID, raw, c.MinRaw, c.MaxRaw, c.Version)
	}
	return (raw - c.Offset) * c.Scale, nil
}

// calKey 标定注册键。
type calKey struct {
	sensor string
	signal timeseries.SignalType
}

// Registry 保存各传感器标定版本，按数据时刻查询；并发安全。
type Registry struct {
	mu    sync.RWMutex
	store map[calKey][]Calibration // 始终按 EffectiveFrom 排序
}

// NewRegistry 创建空标定注册表。
func NewRegistry() *Registry {
	return &Registry{store: make(map[calKey][]Calibration)}
}

// Add 校验并登记一版标定；非法参数、重复版本号或时间区间重叠均被拒绝。
// 允许为历史时刻补录版本（区间不与既有版本重叠即可），
// 但历史数据在查询时命中的是其当时生效的版本，新标定不会回溯改写结论。
func (r *Registry) Add(c Calibration) error {
	if err := c.Validate(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	key := calKey{c.SensorID, c.Signal}
	versions := r.store[key]
	for _, v := range versions {
		if v.Version == c.Version {
			return fmt.Errorf("%w: duplicate version %q for %s/%s",
				ErrInvalidCalibration, c.Version, c.SensorID, c.Signal)
		}
		if intervalsOverlap(v, c) {
			return fmt.Errorf("%w: %s/%s version %q [%s,%s) overlaps %q [%s,%s)",
				ErrOverlappingCalibration, c.SensorID, c.Signal, c.Version,
				c.EffectiveFrom.Format(time.RFC3339Nano), formatTo(c.EffectiveTo),
				v.Version, v.EffectiveFrom.Format(time.RFC3339Nano), formatTo(v.EffectiveTo))
		}
	}
	versions = append(versions, c)
	sort.Slice(versions, func(i, j int) bool {
		return versions[i].EffectiveFrom.Before(versions[j].EffectiveFrom)
	})
	r.store[key] = versions
	return nil
}

// At 返回传感器在数据时刻 t 生效的标定版本。
// 若所有版本均已过期返回 ErrExpiredCalibration；从未配置返回 ErrNoCalibration。
func (r *Registry) At(sensorID string, sig timeseries.SignalType, t time.Time) (Calibration, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	versions := r.store[calKey{sensorID, sig}]
	if len(versions) == 0 {
		return Calibration{}, fmt.Errorf("%w: sensor %s signal %s", ErrNoCalibration, sensorID, sig)
	}
	last := versions[len(versions)-1]
	for _, v := range versions {
		if v.contains(t) {
			return v, nil
		}
	}
	if !t.Before(last.EffectiveFrom) {
		// t 晚于最后一个版本起点却未命中 => 处于已关闭版本的区间之后：过期。
		return Calibration{}, fmt.Errorf("%w: sensor %s signal %s at %s (latest version %q)",
			ErrExpiredCalibration, sensorID, sig, t.Format(time.RFC3339Nano), last.Version)
	}
	// t 早于任何版本起点：尚未生效。
	return Calibration{}, fmt.Errorf("%w: sensor %s signal %s at %s (first version starts %s)",
		ErrNoCalibration, sensorID, sig, t.Format(time.RFC3339Nano),
		versions[0].EffectiveFrom.Format(time.RFC3339Nano))
}

// intervalsOverlap 判断两个半开时间区间是否重叠。零值 to 表示 +inf。
func intervalsOverlap(a, b Calibration) bool {
	start := a.EffectiveFrom
	if b.EffectiveFrom.After(start) {
		start = b.EffectiveFrom
	}
	aEnd := a.EffectiveTo
	bEnd := b.EffectiveTo
	var end time.Time
	switch {
	case aEnd.IsZero() && bEnd.IsZero():
		return true
	case aEnd.IsZero():
		end = bEnd
	case bEnd.IsZero():
		end = aEnd
	default:
		end = aEnd
		if bEnd.Before(end) {
			end = bEnd
		}
	}
	return start.Before(end)
}

func formatTo(t time.Time) string {
	if t.IsZero() {
		return "+inf"
	}
	return t.Format(time.RFC3339Nano)
}
