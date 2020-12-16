package rxgo

// factoryIterable 迭代器工厂
type factoryIterable struct {
	factory func(opts ...Option) <-chan Item
}

// newFactoryIterable 创建迭代器工厂
func newFactoryIterable(factory func(opts ...Option) <-chan Item) Iterable {
	return &factoryIterable{factory: factory}
}

// Observe 迭代器工厂，将实际生产过程延迟到Observe调用时执行
func (i *factoryIterable) Observe(opts ...Option) <-chan Item {
	return i.factory(opts...)
}
