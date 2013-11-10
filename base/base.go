// Package base has the basic types of the workers.
package base

// MapData has key and value.
type MapData struct {
	Key   []byte
	Value interface{}
}

// ReduceData has key and multiple values.
type ReduceData struct {
	Key    []byte
	Values []interface{}
}

// Mapper function takes a single input with key and value, and emits
// outputs with key and value.
type Mapper func(input *MapData, outputs chan *MapData)

// MapWorker function takes inputs and emits outputs.
type MapWorker func(inputs chan *MapData, outputs chan *MapData)

// IdentityMapper just propagates input to output as is without
// copying the content of key and value. It does not even copy the
// data struct content.
func IdentityMapper(input *MapData, outputs chan *MapData) {
	defer close(outputs)
	outputs <- input
}

// SequentialMapWorker runs mapper function for each input in inputs
// and emits to outputs sequentially until inputs channel is closed.
func SequentialMapWorker(mapper Mapper) MapWorker {
	return func(inputs chan *MapData, outputs chan *MapData) {
		defer close(outputs)
		for input := range inputs {
			c := make(chan *MapData)
			go mapper(input, c)
			for output := range c {
				outputs <- output
			}
		}
	}
}

// ChainMapWorkers chains multiple map workers sequentially.
func ChainMapWorkers(mapWorkers []MapWorker) MapWorker {
	return func(inputs chan *MapData, outputs chan *MapData) {
		in := inputs
		out := inputs
		for _, mapWorker := range mapWorkers {
			in, out = out, make(chan *MapData)
			go mapWorker(in, out)
		}
		SequentialMapWorker(IdentityMapper)(out, outputs)
	}
}
