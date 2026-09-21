package backpressure

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRejectPolicyBounded(t *testing.T) {
	q := New[int](2, PolicyReject)
	ctx := context.Background()
	if err := q.Put(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if err := q.Put(ctx, 2); err != nil {
		t.Fatal(err)
	}
	err := q.Put(ctx, 3)
	if !errors.Is(err, ErrRejected) {
		t.Fatalf("third put must be rejected, got %v", err)
	}
	t.Logf("INPUT capacity=2 puts=1,2,3 => rejected 3 with %v; len stays %d", err, q.Len())
}

func TestDropOldestPolicy(t *testing.T) {
	q := New[int](2, PolicyDropOldest)
	ctx := context.Background()
	for _, v := range []int{1, 2, 3, 4} {
		if err := q.Put(ctx, v); err != nil {
			t.Fatal(err)
		}
	}
	if q.Len() != 2 || q.DroppedCount() != 2 {
		t.Fatalf("want len=2 dropped=2, got len=%d dropped=%d", q.Len(), q.DroppedCount())
	}
	v, ok := q.Get(ctx)
	if !ok || v != 3 {
		t.Fatalf("oldest surviving must be 3, got %d ok=%v", v, ok)
	}
	t.Logf("INPUT 1,2,3,4 with cap=2 drop-oldest => queue=[3,4], dropped=%d", q.DroppedCount())
}

func TestBlockPolicyAndClose(t *testing.T) {
	q := New[int](1, PolicyBlock)
	ctx := context.Background()
	if err := q.Put(ctx, 1); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- q.Put(ctx, 2) }()
	select {
	case e := <-done:
		t.Fatalf("put should block while full, instead returned %v", e)
	case <-time.After(20 * time.Millisecond):
	}
	if v, ok := q.Get(ctx); !ok || v != 1 {
		t.Fatalf("expected to get 1")
	}
	select {
	case e := <-done:
		if e != nil {
			t.Fatalf("blocked put should succeed after space freed, got %v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked put never unblocked")
	}

	// 关闭后排空 => Get 返回 false。
	v, ok := q.Get(ctx)
	if !ok || v != 2 {
		t.Fatalf("expected buffered 2, got %d ok=%v", v, ok)
	}
	q.Close()
	if _, ok := q.Get(ctx); ok {
		t.Fatal("Get on drained closed queue must return false")
	}
	if err := q.Put(ctx, 3); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("put after close must fail, got %v", err)
	}
	t.Log("BLOCK policy blocks while full, unblocks on space, close drains cleanly")
}

func TestBlockRespectsContext(t *testing.T) {
	q := New[int](1, PolicyBlock)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_ = q.Put(context.Background(), 1)
	if err := q.Put(ctx, 2); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked put must honor context, got %v", err)
	}
}
