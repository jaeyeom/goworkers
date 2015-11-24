// Package base has the basic types of the workers.
package base

import "golang.org/x/net/context"

// MapData is a pipeline data that can be keyed by Key().
type MapData struct {
	Key   []byte
	Value interface{}
	Error interface{}
}

// Producer function emits output with key and Value.
type Producer func(ctx context.Context, outputs chan<- *MapData)

// Consumer function receives inputs.
type Consumer func(inputs <-chan *MapData)

// Mapper function takes a single input with key and value, and emits
// outputs with key and value.
type Mapper func(input *MapData, outputs chan<- *MapData)

// MapWorker function takes inputs and emits outputs. If done is
// closed, the worker stops.
type MapWorker func(ctx context.Context, inputs <-chan *MapData, outputs chan<- *MapData)

// IdentityMapper just propagates input to output as is without
// copying the content of key and value. It does not even copy the
// data struct content.
func IdentityMapper(input *MapData, outputs chan<- *MapData) {
	defer close(outputs)
	outputs <- input
}

// NopConsumer consumes all inputs without doing anything.
func NopConsumer(inputs <-chan *MapData) {
	for _ = range inputs {
	}
}

// SequentialMapWorker runs mapper function for each input in inputs
// and emits to outputs sequentially until inputs channel is closed.
func SequentialMapWorker(mapper Mapper) MapWorker {
	return func(ctx context.Context, inputs <-chan *MapData, outputs chan<- *MapData) {
		defer close(outputs)
		for input := range inputs {
			c := make(chan *MapData)
			go mapper(input, c)
			for output := range c {
				select {
				case outputs <- output:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

// ChainMapWorkers chains multiple map workers sequentially.
func ChainMapWorkers(mapWorkers []MapWorker) MapWorker {
	return func(ctx context.Context, inputs <-chan *MapData, outputs chan<- *MapData) {
		if len(mapWorkers) == 0 {
			go SequentialMapWorker(IdentityMapper)(ctx, inputs, outputs)
			return
		}
		if len(mapWorkers) == 1 {
			go mapWorkers[0](ctx, inputs, outputs)
			return
		}
		channels := make([]chan *MapData, len(mapWorkers)-1)
		channels[0] = make(chan *MapData)
		go mapWorkers[0](ctx, inputs, channels[0])
		for i := 1; i < len(mapWorkers)-1; i++ {
			channels[i] = make(chan *MapData)
			go mapWorkers[i](ctx, channels[i-1], channels[i])
		}
		go mapWorkers[len(mapWorkers)-1](
			ctx,
			channels[len(mapWorkers)-2],
			outputs,
		)
	}
}

// Produce is a helper function to run the producer. It returns the
// output channel.
func Produce(ctx context.Context, producer Producer) <-chan *MapData {
	c := make(chan *MapData)
	go producer(ctx, c)
	return c
}

// Work is a helper function to run the map worker. It returns the
// output channel.
func Work(ctx context.Context, mapWorker MapWorker, inputs <-chan *MapData) <-chan *MapData {
	c := make(chan *MapData)
	go mapWorker(ctx, inputs, c)
	return c
}
