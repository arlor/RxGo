// Package rxgo is the main RxGo package.
package rxgo

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/emirpasic/gods/trees/binaryheap"
)

// Observable is the standard interface for Observables.
type Observable interface {
	Iterable
	All(predicate Predicate, opts ...Option) Single
	AverageFloat32(opts ...Option) Single
	AverageFloat64(opts ...Option) Single
	AverageInt(opts ...Option) Single
	AverageInt8(opts ...Option) Single
	AverageInt16(opts ...Option) Single
	AverageInt32(opts ...Option) Single
	AverageInt64(opts ...Option) Single
	BackOffRetry(backOffCfg backoff.BackOff, opts ...Option) Observable
	BufferWithCount(count int, opts ...Option) Observable
	BufferWithTime(timespan Duration, opts ...Option) Observable
	BufferWithTimeOrCount(timespan Duration, count int, opts ...Option) Observable
	Connect(ctx context.Context) (context.Context, Disposable)
	Contains(equal Predicate, opts ...Option) Single
	Count(opts ...Option) Single
	Debounce(timespan Duration, opts ...Option) Observable
	DefaultIfEmpty(defaultValue interface{}, opts ...Option) Observable
	Distinct(apply Func, opts ...Option) Observable
	DistinctUntilChanged(apply Func, opts ...Option) Observable
	DoOnCompleted(completedFunc CompletedFunc, opts ...Option) Disposed
	DoOnError(errFunc ErrFunc, opts ...Option) Disposed
	DoOnNext(nextFunc NextFunc, opts ...Option) Disposed
	ElementAt(index uint, opts ...Option) Single
	Error(opts ...Option) error
	Errors(opts ...Option) []error
	Filter(apply Predicate, opts ...Option) Observable
	Find(find Predicate, opts ...Option) OptionalSingle
	First(opts ...Option) OptionalSingle
	FirstOrDefault(defaultValue interface{}, opts ...Option) Single
	FlatMap(apply ItemToObservable, opts ...Option) Observable
	ForEach(nextFunc NextFunc, errFunc ErrFunc, completedFunc CompletedFunc, opts ...Option) Disposed
	GroupBy(length int, distribution func(Item) int, opts ...Option) Observable
	GroupByDynamic(distribution func(Item) string, opts ...Option) Observable
	IgnoreElements(opts ...Option) Observable
	Join(joiner Func2, right Observable, timeExtractor func(interface{}) time.Time, window Duration, opts ...Option) Observable
	Last(opts ...Option) OptionalSingle
	LastOrDefault(defaultValue interface{}, opts ...Option) Single
	Map(apply Func, opts ...Option) Observable
	Marshal(marshaller Marshaller, opts ...Option) Observable
	Max(comparator Comparator, opts ...Option) OptionalSingle
	Min(comparator Comparator, opts ...Option) OptionalSingle
	OnErrorResumeNext(resumeSequence ErrorToObservable, opts ...Option) Observable
	OnErrorReturn(resumeFunc ErrorFunc, opts ...Option) Observable
	OnErrorReturnItem(resume interface{}, opts ...Option) Observable
	Reduce(apply Func2, opts ...Option) OptionalSingle
	Repeat(count int64, frequency Duration, opts ...Option) Observable
	Retry(count int, shouldRetry func(error) bool, opts ...Option) Observable
	Run(opts ...Option) Disposed
	Sample(iterable Iterable, opts ...Option) Observable
	Scan(apply Func2, opts ...Option) Observable
	SequenceEqual(iterable Iterable, opts ...Option) Single
	Send(output chan<- Item, opts ...Option)
	Serialize(from int, identifier func(interface{}) int, opts ...Option) Observable
	Skip(nth uint, opts ...Option) Observable
	SkipLast(nth uint, opts ...Option) Observable
	SkipWhile(apply Predicate, opts ...Option) Observable
	StartWith(iterable Iterable, opts ...Option) Observable
	SumFloat32(opts ...Option) OptionalSingle
	SumFloat64(opts ...Option) OptionalSingle
	SumInt64(opts ...Option) OptionalSingle
	Take(nth uint, opts ...Option) Observable
	TakeLast(nth uint, opts ...Option) Observable
	TakeUntil(apply Predicate, opts ...Option) Observable
	TakeWhile(apply Predicate, opts ...Option) Observable
	TimeInterval(opts ...Option) Observable
	Timestamp(opts ...Option) Observable
	ToMap(keySelector Func, opts ...Option) Single
	ToMapWithValueSelector(keySelector, valueSelector Func, opts ...Option) Single
	ToSlice(initialCapacity int, opts ...Option) ([]interface{}, error)
	Unmarshal(unmarshaller Unmarshaller, factory func() interface{}, opts ...Option) Observable
	WindowWithCount(count int, opts ...Option) Observable
	WindowWithTime(timespan Duration, opts ...Option) Observable
	WindowWithTimeOrCount(timespan Duration, count int, opts ...Option) Observable
	ZipFromIterable(iterable Iterable, zipper Func2, opts ...Option) Observable
}

// ObservableImpl implements Observable.
type ObservableImpl struct {
	iterable Iterable
}

// defaultErrorFuncOperator 默认错误处理方式，发出错误，停止迭代
func defaultErrorFuncOperator(ctx context.Context, item Item, dst chan<- Item, operatorOptions operatorOptions) {
	item.SendContext(ctx, dst)
	operatorOptions.stop()
}

// customObservableOperator 自定义流操作，分为立即执行和延迟执行
func customObservableOperator(f func(ctx context.Context, next chan Item, option Option, opts ...Option), opts ...Option) Observable {
	option := parseOptions(opts...)

	if option.isEagerObservation() {
		next := option.buildChannel()
		ctx := option.buildContext()
		go f(ctx, next, option, opts...)
		return &ObservableImpl{iterable: newChannelIterable(next)}
	}

	return &ObservableImpl{
		iterable: newFactoryIterable(func(propagatedOptions ...Option) <-chan Item {
			mergedOptions := append(opts, propagatedOptions...)
			option := parseOptions(mergedOptions...)
			next := option.buildChannel()
			ctx := option.buildContext()
			go f(ctx, next, option, mergedOptions...)
			return next
		}),
	}
}

type operator interface {
	next(ctx context.Context, item Item, dst chan<- Item, operatorOptions operatorOptions)
	err(ctx context.Context, item Item, dst chan<- Item, operatorOptions operatorOptions)
	end(ctx context.Context, dst chan<- Item)
	gatherNext(ctx context.Context, item Item, dst chan<- Item, operatorOptions operatorOptions)
}

func observable(iterable Iterable, operatorFactory func() operator, forceSeq, bypassGather bool, opts ...Option) Observable {
	option := parseOptions(opts...)
	parallel, _ := option.getPool()

	// 立即生产
	if option.isEagerObservation() {
		next := option.buildChannel()
		ctx := option.buildContext()
		// 强制串行或者非并发，那就串行运行
		if forceSeq || !parallel {
			runSequential(ctx, next, iterable, operatorFactory, option, opts...)
		} else {
			// 否则并发运行
			runParallel(ctx, next, iterable.Observe(opts...), operatorFactory, bypassGather, option, opts...)
		}
		return &ObservableImpl{iterable: newChannelIterable(next)}
	}

	// 延迟串行运行
	if forceSeq || !parallel {
		return &ObservableImpl{
			iterable: newFactoryIterable(func(propagatedOptions ...Option) <-chan Item {
				mergedOptions := append(opts, propagatedOptions...)
				option := parseOptions(mergedOptions...)

				next := option.buildChannel()
				ctx := option.buildContext()
				runSequential(ctx, next, iterable, operatorFactory, option, mergedOptions...)
				return next
			}),
		}
	}

	// 将并行结果有序化
	if serialized, f := option.isSerialized(); serialized {
		firstItemIDCh := make(chan Item, 1)
		fromCh := make(chan Item, 1)
		obs := &ObservableImpl{
			iterable: newFactoryIterable(func(propagatedOptions ...Option) <-chan Item {
				mergedOptions := append(opts, propagatedOptions...)
				option := parseOptions(mergedOptions...)

				next := option.buildChannel()
				ctx := option.buildContext()
				observe := iterable.Observe(opts...)
				go func() {
					select {
					case <-ctx.Done():
						return
					case firstItemID := <-firstItemIDCh: // 上游发出第一个元素的index，用此index触发serialize和runParallel
						// 如果错误，发出item，如果正常，发出id
						if firstItemID.Error() {
							firstItemID.SendContext(ctx, fromCh)
							return
						}
						Of(firstItemID.V.(int)).SendContext(ctx, fromCh)
						// 并发执行
						runParallel(ctx, next, observe, operatorFactory, bypassGather, option, mergedOptions...)
					}
				}()
				runFirstItem(ctx, f, firstItemIDCh, observe, next, operatorFactory, option, mergedOptions...)
				return next
			}),
		}
		return obs.serialize(fromCh, f)
	}

	// 延迟并行运行
	return &ObservableImpl{
		iterable: newFactoryIterable(func(propagatedOptions ...Option) <-chan Item {
			mergedOptions := append(opts, propagatedOptions...)
			option := parseOptions(mergedOptions...)

			next := option.buildChannel()
			ctx := option.buildContext()
			runParallel(ctx, next, iterable.Observe(mergedOptions...), operatorFactory, bypassGather, option, mergedOptions...)
			return next
		}),
	}
}

func single(iterable Iterable, operatorFactory func() operator, forceSeq, bypassGather bool, opts ...Option) Single {
	option := parseOptions(opts...)
	parallel, _ := option.getPool()

	if option.isEagerObservation() {
		next := option.buildChannel()
		ctx := option.buildContext()
		if forceSeq || !parallel {
			runSequential(ctx, next, iterable, operatorFactory, option, opts...)
		} else {
			runParallel(ctx, next, iterable.Observe(opts...), operatorFactory, bypassGather, option, opts...)
		}
		return &SingleImpl{iterable: newChannelIterable(next)}
	}

	return &SingleImpl{
		iterable: newFactoryIterable(func(propagatedOptions ...Option) <-chan Item {
			mergedOptions := append(opts, propagatedOptions...)
			option = parseOptions(mergedOptions...)

			next := option.buildChannel()
			ctx := option.buildContext()
			if forceSeq || !parallel {
				runSequential(ctx, next, iterable, operatorFactory, option, mergedOptions...)
			} else {
				runParallel(ctx, next, iterable.Observe(mergedOptions...), operatorFactory, bypassGather, option, mergedOptions...)
			}
			return next
		}),
	}
}

func optionalSingle(iterable Iterable, operatorFactory func() operator, forceSeq, bypassGather bool, opts ...Option) OptionalSingle {
	option := parseOptions(opts...)
	parallel, _ := option.getPool()

	if option.isEagerObservation() {
		next := option.buildChannel()
		ctx := option.buildContext()
		if forceSeq || !parallel {
			runSequential(ctx, next, iterable, operatorFactory, option, opts...)
		} else {
			runParallel(ctx, next, iterable.Observe(opts...), operatorFactory, bypassGather, option, opts...)
		}
		return &OptionalSingleImpl{iterable: newChannelIterable(next)}
	}

	return &OptionalSingleImpl{
		iterable: newFactoryIterable(func(propagatedOptions ...Option) <-chan Item {
			mergedOptions := append(opts, propagatedOptions...)
			option = parseOptions(mergedOptions...)

			next := option.buildChannel()
			ctx := option.buildContext()
			if forceSeq || !parallel {
				runSequential(ctx, next, iterable, operatorFactory, option, mergedOptions...)
			} else {
				runParallel(ctx, next, iterable.Observe(mergedOptions...), operatorFactory, bypassGather, option, mergedOptions...)
			}
			return next
		}),
	}
}

// runSequential 对流里每个元素顺序执行传入的operator操作，并将执行结果作为新流的元素发出
func runSequential(ctx context.Context, next chan Item, iterable Iterable, operatorFactory func() operator, option Option, opts ...Option) {
	observe := iterable.Observe(opts...)
	go func() {
		op := operatorFactory()
		stopped := false
		operator := operatorOptions{
			stop: func() {
				if option.getErrorStrategy() == StopOnError {
					stopped = true
				}
			},
			resetIterable: func(newIterable Iterable) {
				observe = newIterable.Observe(opts...)
			},
		}

	loop:
		for !stopped {
			select {
			case <-ctx.Done():
				break loop
			case i, ok := <-observe:
				if !ok {
					break loop
				}
				if i.Error() {
					op.err(ctx, i, next, operator)
				} else {
					op.next(ctx, i, next, operator)
				}
			}
		}
		op.end(ctx, next)
		close(next)
	}()
}

// runParallel 对流里每个元素并行执行传入的operator操作，并将执行结果作为新流的元素发出
/*
思考：
	为什么runParallel相比runSequential多了一个忽略收集（bypassGather）的参数
原因：
	因为并发运行的时候，可能涉及到修改operator对象的内部参数，这时需要考虑竞态条件，
通过gather（chan Item），可以将并发执行得到的结果进行串行处理，避免了加锁操作。
而runSequential本身就是串行的，因此不需要多此一举。

*/
func runParallel(ctx context.Context, next chan Item, observe <-chan Item, operatorFactory func() operator, bypassGather bool, option Option, opts ...Option) {
	wg := sync.WaitGroup{}
	_, pool := option.getPool()
	wg.Add(pool)

	// 如果忽略收集（bypassGather），则所有流发出所有数据直接进入next
	// 否则，所有数据要先经过gather，然后才经过next
	var gather chan Item
	if bypassGather {
		gather = next
	} else {
		gather = make(chan Item, 1)

		// Gather
		// 收集并发处理的流本身发出的数据
		go func() {
			op := operatorFactory()
			stopped := false
			operator := operatorOptions{
				stop: func() {
					if option.getErrorStrategy() == StopOnError {
						stopped = true
					}
				},
				resetIterable: func(newIterable Iterable) {
					observe = newIterable.Observe(opts...)
				},
			}
			for item := range gather {
				if stopped {
					break
				}
				if item.Error() {
					op.err(ctx, item, next, operator)
				} else {
					op.gatherNext(ctx, item, next, operator)
				}
			}
			op.end(ctx, next)
			close(next)
		}()
	}

	// Scatter
	// 分散流里的元素，并发执行，然后将结果作为新流里的元素发出
	for i := 0; i < pool; i++ {
		go func() {
			// 这里使用工厂，所以每次返回operator都是不同的对象
			// 对operator进行赋值操作时则不存在并发风险
			op := operatorFactory()
			stopped := false
			operator := operatorOptions{
				stop: func() {
					if option.getErrorStrategy() == StopOnError {
						stopped = true
					}
				},
				resetIterable: func(newIterable Iterable) {
					observe = newIterable.Observe(opts...)
				},
			}
			defer wg.Done()
			for !stopped {
				select {
				case <-ctx.Done():
					return
				case item, ok := <-observe:
					// 当前流结束，并且不省略收集，则将operator本身作为元素发出
					if !ok {
						if !bypassGather {
							Of(op).SendContext(ctx, gather)
						}
						return
					}
					if item.Error() {
						op.err(ctx, item, gather, operator)
					} else {
						op.next(ctx, item, gather, operator)
					}
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(gather)
	}()
}

// runFirstItem 后台异步找到流发出元素的index发出到notif，并将元素向下游流发出
func runFirstItem(ctx context.Context, f func(interface{}) int, notif chan Item, observe <-chan Item, next chan Item, operatorFactory func() operator, option Option, opts ...Option) {
	go func() {
		op := operatorFactory()
		stopped := false
		operator := operatorOptions{
			stop: func() {
				// 发生错误，立即停止
				if option.getErrorStrategy() == StopOnError {
					stopped = true
				}
			},
			resetIterable: func(newIterable Iterable) {
				observe = newIterable.Observe(opts...)
			},
		}

	loop:
		for !stopped {
			select {
			case <-ctx.Done():
				break loop
			case i, ok := <-observe:
				if !ok { // 上游流完结，跳出
					break loop
				}
				if i.Error() {
					op.err(ctx, i, next, operator) // 向中游发出错误
					i.SendContext(ctx, notif)      // 向firstItemIDCh发出上游错误
				} else {
					op.next(ctx, i, next, operator)    // 向中游发出元素
					Of(f(i.V)).SendContext(ctx, notif) // 向firstItemIDCh发出上游元素的index
				}
			}
		}
		op.end(ctx, next)
	}()
}

// serialize rxgo.Serialize选项，对本流发出的结果进行有序化处理
func (o *ObservableImpl) serialize(fromCh chan Item, identifier func(interface{}) int, opts ...Option) Observable {
	option := parseOptions(opts...)
	next := option.buildChannel()

	ctx := option.buildContext()
	minHeap := binaryheap.NewWith(func(a, b interface{}) int {
		return a.(int) - b.(int)
	})
	items := make(map[int]interface{})

	var from int
	var counter int64
	src := o.Observe(opts...)
	go func() {
		select {
		case <-ctx.Done():
			close(next)
			return
		case item := <-fromCh: // runFirstItem触发firstItemIDCh，然后才触发fromCh
			if item.Error() {
				item.SendContext(ctx, next)
				close(next)
				return
			}
			from = item.V.(int)
			counter = int64(from) // 首个发出元素的index

			go func() {
				defer close(next)

				for {
					select {
					case <-ctx.Done():
						return
					case item, ok := <-src: // 从中游获取元素
						if !ok {
							return
						}
						if item.Error() {
							next <- item
							return
						}

						id := identifier(item.V) // 获取元素的index
						minHeap.Push(id)         // 将index放入小根堆
						items[id] = item.V       // 获取元素与id的映射

						for !minHeap.Empty() {
							v, _ := minHeap.Peek()
							id := v.(int) // 最小index
							if atomic.LoadInt64(&counter) == int64(id) {
								if itemValue, contains := items[id]; contains {
									minHeap.Pop()
									delete(items, id)
									Of(itemValue).SendContext(ctx, next) // 发出index对应的元素
									counter++                            // 索引值增加
									continue
								}
							}
							break
						}
					}
				}
			}()
		}
	}()

	return &ObservableImpl{
		iterable: newChannelIterable(next),
	}
}
