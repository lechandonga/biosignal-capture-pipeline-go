package timeseries

import (
	"testing"
	"time"
)

func mkSample(sensor string, seq uint64, dt time.Time, val float64) Sample {
	return Sample{
		DeviceID:   "dev1",
		SensorID:   sensor,
		Signal:     SignalECG,
		Seq:        seq,
		DeviceTime: dt,
		ReceivedAt: dt.Add(10 * time.Millisecond),
		Value:      val,
		Quality:    0.9,
	}
}

func TestNormalAndGapAndDuplicate(t *testing.T) {
	base := time.Date(2026, 9, 22, 10, 0, 0, 0, time.UTC)
	g := NewClockGuard(50*time.Millisecond, 20*time.Millisecond)

	// 序列号 1,2 正常；序列号 5 到达 => 缺口 [3,4]，样本接收；再发 seq=3 => 重复? 不，是迟到旧包
	r1 := g.Ingest(mkSample("s1", 1, base, 1.0))
	r2 := g.Ingest(mkSample("s1", 2, base.Add(10*time.Millisecond), 2.0))
	r5 := g.Ingest(mkSample("s1", 5, base.Add(40*time.Millisecond), 5.0))

	for _, r := range []IngestResult{r1, r2} {
		if !r.Accepted {
			t.Fatalf("early samples should be accepted, got %s: %s", r.Reason, r.Detail)
		}
	}
	if !r5.Accepted || r5.Reason != ReasonMissingGap || len(r5.Gaps) != 1 || len(r5.Gaps[0].MissingSeq) != 2 {
		t.Fatalf("seq=5 accepted with one gap of 2, got accepted=%v reason=%s gaps=%d", r5.Accepted, r5.Reason, len(r5.Gaps))
	}
	t.Logf("INPUT seq=5 value=5.0 REASON=%s GAP_MISSING=%v BASIS=%s", r5.Reason, r5.Gaps[0].MissingSeq, r5.Detail)

	// 重复 seq=5 必须被拒绝，原因可区分。
	dup := g.Ingest(mkSample("s1", 5, base.Add(40*time.Millisecond), 5.0))
	if dup.Accepted || dup.Reason != ReasonDuplicate {
		t.Fatalf("duplicate must be rejected as %s, got accepted=%v reason=%s", ReasonDuplicate, dup.Accepted, dup.Reason)
	}
	t.Logf("INPUT duplicate seq=5 REJECTED=%s BASIS=%s", dup.Reason, dup.Detail)
}

func TestClockRegressionLargeRejected(t *testing.T) {
	base := time.Date(2026, 9, 22, 10, 0, 0, 0, time.UTC)
	g := NewClockGuard(50*time.Millisecond, 20*time.Millisecond)

	g.Ingest(mkSample("s2", 1, base, 1))
	g.Ingest(mkSample("s2", 2, base.Add(10*time.Millisecond), 2))

	// seq=3 但设备时间倒退 100ms（超过 maxRegression 20ms）：拒绝。
	r := g.Ingest(mkSample("s2", 3, base.Add(10*time.Millisecond).Add(-100*time.Millisecond), 3))
	if r.Accepted || r.Reason != ReasonClockRegression {
		t.Fatalf("large regression must be rejected, got accepted=%v reason=%s", r.Accepted, r.Reason)
	}
	t.Logf("INPUT seq=3 deviceTime=-100ms REJECTED=%s BASIS=%s", r.Reason, r.Detail)

	// 后续正常样本仍能处理，时间轴未被污染。
	ok := g.Ingest(mkSample("s2", 4, base.Add(30*time.Millisecond), 4))
	if !ok.Accepted {
		t.Fatalf("timeline must remain usable after rejection, got reason=%s detail=%s", ok.Reason, ok.Detail)
	}
}

func TestClockRegressionSmallCorrected(t *testing.T) {
	base := time.Date(2026, 9, 22, 10, 0, 0, 0, time.UTC)
	// 重排窗口 5ms：10ms 回退超出窗口但低于 50ms 回退上界 => 显式修正。
	g := NewClockGuard(5*time.Millisecond, 50*time.Millisecond)

	g.Ingest(mkSample("s3", 1, base, 1))
	// seq=2 时间倒退 10ms（阈值内）：接收且统一时间被显式修正为单调值。
	r := g.Ingest(mkSample("s3", 2, base.Add(-10*time.Millisecond), 2))
	if !r.Accepted || r.Reason != ReasonClockCorrected {
		t.Fatalf("small regression must be corrected, got accepted=%v reason=%s", r.Accepted, r.Reason)
	}
	if !r.Aligned.UnifiedTime.Equal(r.Aligned.UnifiedTime) || r.Aligned.UnifiedTime.Before(base) {
		t.Fatalf("corrected unified time must not move backwards")
	}
	t.Logf("INPUT seq=2 -10ms ACCEPTED=%s UNIFIED=%s BASIS=%s", r.Reason, r.Aligned.UnifiedTime.Format(time.RFC3339Nano), r.Detail)
}

func TestReconnectNoDuplicateNoLoss(t *testing.T) {
	base := time.Date(2026, 9, 22, 10, 0, 0, 0, time.UTC)
	g := NewClockGuard(50*time.Millisecond, 50*time.Millisecond)

	g.Ingest(mkSample("s4", 10, base, 1))
	g.Ingest(mkSample("s4", 11, base.Add(10*time.Millisecond), 2))

	// 断连后重连：重锚主机时钟，但序列号历史保留。
	reconnectAt := base.Add(time.Second)
	g.Reanchor("s4", reconnectAt)

	// 重连后设备时钟跳变也无妨（按新锚点换算）；seq=12 应正常接收。
	r12 := g.Ingest(mkSample("s4", 12, base.Add(100*time.Millisecond), 3))
	if !r12.Accepted {
		t.Fatalf("post-reconnect seq=12 must be accepted, got reason=%s detail=%s", r12.Reason, r12.Detail)
	}
	// 重复旧包 seq=11 仍被拒绝，不重复计入。
	dup := g.Ingest(mkSample("s4", 11, base.Add(10*time.Millisecond), 2))
	if dup.Accepted || dup.Reason != ReasonDuplicate {
		t.Fatalf("old seq replayed after reconnect must be duplicate-rejected")
	}
	// seq=15 到达 => 缺口 [13,14]，不漏记。
	r15 := g.Ingest(mkSample("s4", 15, base.Add(130*time.Millisecond), 5))
	if !r15.Accepted || len(r15.Gaps[0].MissingSeq) != 2 {
		t.Fatalf("gap after reconnect must be detected, got accepted=%v gaps=%d", r15.Accepted, len(r15.Gaps))
	}
	t.Logf("RECONNECT seq=12 accepted; replay seq=11 -> %s; seq=15 gap=%v", dup.Reason, r15.Gaps[0].MissingSeq)
}

func TestDriftCorrectionApplied(t *testing.T) {
	base := time.Date(2026, 9, 22, 10, 0, 0, 0, time.UTC)
	g := NewClockGuard(time.Second, time.Second)
	g.SetSkewPPM("s5", 1000) // 设备慢 1000ppm => 每秒少 1ms，校正后统一时间加快 0.1%

	r1 := g.Ingest(mkSample("s5", 1, base, 1))
	r2 := g.Ingest(mkSample("s5", 2, base.Add(time.Second), 2))
	want := time.Second + time.Millisecond // 设备慢1000ppm：1s设备间隔≈真实1.001s
	got := r2.Aligned.UnifiedTime.Sub(r1.Aligned.UnifiedTime)
	if delta := got - want; delta > 100*time.Nanosecond || delta < -100*time.Nanosecond {
		t.Fatalf("drift correction: want interval ~%s, got %s", want, got)
	}
	t.Logf("INPUT 1s device interval at 1000ppm drift -> unified interval=%s (skew=%fppm)", got, r2.Aligned.SkewPPM)
}

func TestGridAlignmentAndMissing(t *testing.T) {
	base := time.Date(2026, 9, 22, 10, 0, 0, 0, time.UTC)
	grid := NewGrid(100 * time.Millisecond)
	grid.Register("fast")
	grid.Register("slow")

	align := func(sensor string, at time.Time) AlignedSample {
		s := mkSample(sensor, 1, at, 1)
		return AlignedSample{Sample: s, UnifiedTime: at}
	}
	grid.Add(align("fast", base))
	grid.Add(align("slow", base))
	grid.Add(align("fast", base.Add(50*time.Millisecond)))  // 同槽
	grid.Add(align("fast", base.Add(250*time.Millisecond))) // 跳过槽1

	slots := grid.Snapshot()
	if len(slots) != 3 {
		t.Fatalf("want 3 slots, got %d", len(slots))
	}
	if len(slots[0].Values) != 2 {
		t.Fatalf("slot0 should hold both sensors, got %d", len(slots[0].Values))
	}
	found := false
	for _, m := range slots[1].Missing {
		if m == "slow" {
			found = true
		}
	}
	if !found {
		t.Fatalf("slot1 must mark slow sensor missing, got %v", slots[1].Missing)
	}
	t.Logf("GRID slots=%d slot0=%d sensors slot1.missing=%v", len(slots), len(slots[0].Values), slots[1].Missing)
}
