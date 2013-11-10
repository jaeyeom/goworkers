package base

import (
	"fmt"
)

func ExampleSequentialMapWorker() {
	w := SequentialMapWorker(func(input *MapData, outputs chan *MapData) {
		defer close(outputs)
		input.Value = append(input.Value.([]byte), 't')
		outputs <- input
	})
	ins, outs := make(chan *MapData), make(chan *MapData)
	go w(ins, outs)
	go func() {
		defer close(ins)
		ins <- &MapData{Key: []byte{}, Value: []byte("tes")}
		ins <- &MapData{Key: []byte{}, Value: []byte("las")}
	}()
	for out := range outs {
		fmt.Println(string(out.Value.([]byte)))
	}
	// Output:
	// test
	// last
}

func ExampleChainMapWorkers() {
	w := ChainMapWorkers([]MapWorker{
		SequentialMapWorker(func(input *MapData, outputs chan *MapData) {
			defer close(outputs)
			input.Value = append(input.Value.([]byte), 't')
			outputs <- input
		}),
		SequentialMapWorker(func(input *MapData, outputs chan *MapData) {
			defer close(outputs)
			input.Value = append(input.Value.([]byte), 'e')
			outputs <- input
		}),
		SequentialMapWorker(func(input *MapData, outputs chan *MapData) {
			defer close(outputs)
			input.Value = append(input.Value.([]byte), 's')
			outputs <- input
		}),
		SequentialMapWorker(func(input *MapData, outputs chan *MapData) {
			defer close(outputs)
			input.Value = append(input.Value.([]byte), 't')
			outputs <- input
		}),
	})
	ins, outs := make(chan *MapData), make(chan *MapData)
	go w(ins, outs)
	go func() {
		defer close(ins)
		ins <- &MapData{Key: []byte{}, Value: []byte{}}
		ins <- &MapData{Key: []byte{}, Value: []byte("2")}
	}()
	for out := range outs {
		fmt.Println(string(out.Value.([]byte)))
	}
	// Output:
	// test
	// 2test
}
