package rxgo

import (
	"context"
	"sync"
)

type channelIterable struct {
	next                   <-chan Item  // 实际数据源
	opts                   []Option     // 控制行为
	subscribers            []chan Item  // 可连接对象的所有订阅者
	mutex                  sync.RWMutex // 防并发锁
	producerAlreadyCreated bool         // 是否已开始生产标志，确保出发可连接操作时，数据只生产一遍
}

func newChannelIterable(next <-chan Item, opts ...Option) Iterable {
	return &channelIterable{
		next:        next,
		subscribers: make([]chan Item, 0),
		opts:        opts,
	}
}

func (i *channelIterable) Observe(opts ...Option) <-chan Item {
	mergedOptions := append(i.opts, opts...)
	option := parseOptions(mergedOptions...)

	// 是否可连接
	if !option.isConnectable() {
		return i.next
	}

	// 是否触发可连接操作
	if option.isConnectOperation() {
		i.connect(option.buildContext())
		return nil
	}

	// 可连接，但是暂不触发连接，只是增加一个订阅者
	ch := option.buildChannel()
	i.mutex.Lock()
	i.subscribers = append(i.subscribers, ch)
	i.mutex.Unlock()
	return ch
}

// connect 触发生产
func (i *channelIterable) connect(ctx context.Context) {
	i.mutex.Lock()
	if !i.producerAlreadyCreated {
		go i.produce(ctx)
		i.producerAlreadyCreated = true
	}
	i.mutex.Unlock()
}

// produce 实际生产，将channel中的数据向所有订阅者推送一份
func (i *channelIterable) produce(ctx context.Context) {
	defer func() {
		i.mutex.RLock()
		for _, subscriber := range i.subscribers {
			close(subscriber)
		}
		i.mutex.RUnlock()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case item, ok := <-i.next:
			if !ok {
				return
			}
			i.mutex.RLock()
			for _, subscriber := range i.subscribers {
				subscriber <- item
			}
			i.mutex.RUnlock()
		}
	}
}
