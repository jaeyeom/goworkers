// Package base has the basic types of the workers.
package base

import "golang.org/x/net/context"

// Keyer is an interface for keyed data.
type Keyer interface {
	Key() []byte
}

// Producer function emits output with key and Value.
type Producer func(ctx context.Context, outputs chan<- Keyer)

// Consumer function receives inputs.
type Consumer func(inputs <-chan Keyer)

// Mapper function takes a single input with key and value, and emits
// outputs with key and value.
type Mapper func(input Keyer, outputs chan<- Keyer)

// MapWorker function takes inputs and emits outputs. If done is
// closed, the worker stops.
type MapWorker func(ctx context.Context, inputs <-chan Keyer, outputs chan<- Keyer)

// IdentityMapper just propagates input to output as is without
// copying the content of key and value. It does not even copy the
// data struct content.
func IdentityMapper(input Keyer, outputs chan<- Keyer) {
	defer close(outputs)
	outputs <- input
}

// NopConsumer consumes all inputs without doing anything.
func NopConsumer(inputs <-chan Keyer) {
	for _ = range inputs {
	}
}

// SequentialMapWorker runs mapper function for each input in inputs
// and emits to outputs sequentially until inputs channel is closed.
func SequentialMapWorker(mapper Mapper) MapWorker {
	return func(ctx context.Context, inputs <-chan Keyer, outputs chan<- Keyer) {
		defer close(outputs)
		for input := range inputs {
			c := make(chan Keyer)
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
	return func(ctx context.Context, inputs <-chan Keyer, outputs chan<- Keyer) {
		if len(mapWorkers) == 0 {
			go SequentialMapWorker(IdentityMapper)(ctx, inputs, outputs)
			return
		}
		if len(mapWorkers) == 1 {
			go mapWorkers[0](ctx, inputs, outputs)
			return
		}
		channels := make([]chan Keyer, len(mapWorkers)-1)
		channels[0] = make(chan Keyer)
		go mapWorkers[0](ctx, inputs, channels[0])
		for i := 1; i < len(mapWorkers)-1; i++ {
			channels[i] = make(chan Keyer)
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
func Produce(ctx context.Context, producer Producer) <-chan Keyer {
	c := make(chan Keyer)
	go producer(ctx, c)
	return c
}

// Work is a helper function to run the map worker. It returns the
// output channel.
func Work(ctx context.Context, mapWorker MapWorker, inputs <-chan Keyer) <-chan Keyer {
	c := make(chan Keyer)
	go mapWorker(ctx, inputs, c)
	return c
}
