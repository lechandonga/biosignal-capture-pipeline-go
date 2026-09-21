// Package backpressure 提供有界队列与可配置背压策略。
package backpressure

import (
	"context"
	"errors"
	"sync"
)

// Policy 队列积压时的背压策略。
type Policy int

// 阻塞等待、拒绝新数据、丢弃最旧数据三种策略。
const (
	PolicyBlock Policy = iota
	PolicyReject
	PolicyDropOldest
)

// ErrRejected 表示按背压策略数据被显式拒绝。
var ErrRejected = errors.New("backpressure: data rejected by policy")

// ErrQueueClosed 表示队列已关闭。
var ErrQueueClosed = errors.New("backpressure: queue closed")

// Queue 是泛型有界队列。容量固定，绝不无界增长。
type Queue[T any] struct {
	mu       sync.Mutex
	cond     *sync.Cond
	items    []T
	capacity int
	policy   Policy
	closed   bool
	dropped  int
}

// New 创建容量为 capacity 的有界队列。
func New[T any](capacity int, policy Policy) *Queue[T] {
	if capacity <= 0 {
		panic("backpressure: capacity must be positive")
	}
	q := &Queue[T]{
		items:    make([]T, 0, capacity),
		capacity: capacity,
		policy:   policy,
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// Put 按背压策略放入数据。
//   - PolicyBlock：队列满时阻塞直到有空位、ctx 取消或队列关闭。
//   - PolicyReject：满时立即返回 ErrRejected。
//   - PolicyDropOldest：满时丢弃最旧一条腾出空位，并标记是否发生丢弃。
func (q *Queue[T]) Put(ctx context.Context, v T) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	// 上下文取消优先：即使暂时不能入队也及时退出。
	if err := ctx.Err(); err != nil {
		return err
	}

	for len(q.items) >= q.capacity && !q.closed {
		switch q.policy {
		case PolicyReject:
			return ErrRejected
		case PolicyDropOldest:
			q.items = q.items[1:] // 腾出最旧位置，新值随后入队
			q.dropped++
			q.cond.Broadcast() // 消费侧可能在等待非空
		case PolicyBlock:
			// 在 cond 上等待空位；ctx 取消通过单独 goroutine 唤醒。
			waited := make(chan struct{})
			stop := context.AfterFunc(ctx, func() {
				q.mu.Lock()
				q.cond.Broadcast()
				q.mu.Unlock()
			})
			q.cond.Wait()
			stop()
			close(waited)
			if err := ctx.Err(); err != nil {
				return err
			}
		}
	}
	if q.closed {
		return ErrQueueClosed
	}
	q.items = append(q.items, v)
	q.cond.Broadcast() // 通知等待非空的消费者
	return nil
}

// DroppedCount 返回累计因背压被丢弃的条数（仅 PolicyDropOldest 下增长）。
func (q *Queue[T]) DroppedCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.dropped
}

// Get 阻塞获取数据；队列关闭且排空后返回 false。
func (q *Queue[T]) Get(ctx context.Context) (T, bool) {
	var zero T
	q.mu.Lock()
	defer q.mu.Unlock()

	for len(q.items) == 0 && !q.closed {
		if err := ctx.Err(); err != nil {
			return zero, false
		}
		stop := context.AfterFunc(ctx, func() {
			q.mu.Lock()
			q.cond.Broadcast()
			q.mu.Unlock()
		})
		q.cond.Wait()
		stop()
	}
	if len(q.items) == 0 {
		return zero, false // 关闭且排空
	}
	v := q.items[0]
	q.items = q.items[1:]
	q.cond.Broadcast() // 通知等待空位的生产者
	return v, true
}

// Close 关闭队列：不再接受新数据，已缓冲数据仍可被排空。
func (q *Queue[T]) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.cond.Broadcast()
}

// Len 返回当前队列长度。
func (q *Queue[T]) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}
