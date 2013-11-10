package base

import (
	"fmt"
)

func ExampleSequentialMapWorker() {
	p := func(outputs chan<- *MapData) {
		defer close(outputs)
		outputs <- &MapData{Key: []byte{}, Value: []byte("tes")}
		outputs <- &MapData{Key: []byte{}, Value: []byte("las")}
	}
	w := SequentialMapWorker(func(input *MapData, outputs chan<- *MapData) {
		defer close(outputs)
		input.Value = append(input.Value.([]byte), 't')
		outputs <- input
	})
	consume := func(inputs <-chan *MapData) {
		for input := range inputs {
			fmt.Println(string(input.Value.([]byte)))
		}
	}
	consume(Work(w, Produce(p)))
	// Output:
	// test
	// last
}

func ExampleChainMapWorkers() {
	p := func(outputs chan<- *MapData) {
		defer close(outputs)
		outputs <- &MapData{Key: []byte{}, Value: []byte{}}
		outputs <- &MapData{Key: []byte{}, Value: []byte("2")}
	}
	w := ChainMapWorkers([]MapWorker{
		SequentialMapWorker(func(input *MapData, outputs chan<- *MapData) {
			defer close(outputs)
			input.Value = append(input.Value.([]byte), 't')
			outputs <- input
		}),
		SequentialMapWorker(func(input *MapData, outputs chan<- *MapData) {
			defer close(outputs)
			input.Value = append(input.Value.([]byte), 'e')
			outputs <- input
		}),
		SequentialMapWorker(func(input *MapData, outputs chan<- *MapData) {
			defer close(outputs)
			input.Value = append(input.Value.([]byte), 's')
			outputs <- input
		}),
		SequentialMapWorker(func(input *MapData, outputs chan<- *MapData) {
			defer close(outputs)
			input.Value = append(input.Value.([]byte), 't')
			outputs <- input
		}),
	})
	consume := func(inputs <-chan *MapData) {
		for input := range inputs {
			fmt.Println(string(input.Value.([]byte)))
		}
	}
	consume(Work(w, Produce(p)))
	// Output:
	// test
	// 2test
}
