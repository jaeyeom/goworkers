package base

import (
	"fmt"
)

func ExampleSequentialMapWorker() {
	done := make(chan struct{})
	defer close(done)
	p := func(done <-chan struct{}, outputs chan<- *MapData) {
		defer close(outputs)
		data := []*MapData{
			&MapData{Key: []byte{}, Value: []byte("tes")},
			&MapData{Key: []byte{}, Value: []byte("las")},
		}
		for _, datum := range data {
			select {
			case outputs <- datum:
			case <-done:
				return
			}
		}
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
	consume(Work(w, done, Produce(p, done)))
	// Output:
	// test
	// last
}

func ExampleChainMapWorkers_normal() {
	done := make(chan struct{})
	defer close(done)
	p := func(done <-chan struct{}, outputs chan<- *MapData) {
		defer close(outputs)
		data := []*MapData{
			&MapData{Key: []byte{}, Value: []byte{}},
			&MapData{Key: []byte{}, Value: []byte("2")},
		}
		for _, datum := range data {
			select {
			case outputs <- datum:
			case <-done:
				return
			}
		}
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
	consume(Work(w, done, Produce(p, done)))
	// Output:
	// test
	// 2test
}

func ExampleChainMapWorkers_one() {
	done := make(chan struct{})
	defer close(done)
	p := func(done <-chan struct{}, outputs chan<- *MapData) {
		defer close(outputs)
		data := []*MapData{
			&MapData{Key: []byte{}, Value: []byte{}},
			&MapData{Key: []byte{}, Value: []byte("2")},
		}
		for _, datum := range data {
			select {
			case outputs <- datum:
			case <-done:
				return
			}
		}
	}
	w := ChainMapWorkers([]MapWorker{
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
	consume(Work(w, done, Produce(p, done)))
	// Output:
	// t
	// 2t
}

func ExampleChainMapWorkers_two() {
	done := make(chan struct{})
	defer close(done)
	p := func(done <-chan struct{}, outputs chan<- *MapData) {
		defer close(outputs)
		data := []*MapData{
			&MapData{Key: []byte{}, Value: []byte{}},
			&MapData{Key: []byte{}, Value: []byte("2")},
		}
		for _, datum := range data {
			select {
			case outputs <- datum:
			case <-done:
				return
			}
		}
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
	})
	consume := func(inputs <-chan *MapData) {
		for input := range inputs {
			fmt.Println(string(input.Value.([]byte)))
		}
	}
	consume(Work(w, done, Produce(p, done)))
	// Output:
	// te
	// 2te
}

func ExampleChainMapWorkers_done() {
	done := make(chan struct{})
	p := func(done <-chan struct{}, outputs chan<- *MapData) {
		defer close(outputs)
		data := []*MapData{
			&MapData{Key: []byte{}, Value: []byte{}},
			&MapData{Key: []byte{}, Value: []byte("2")},
		}
		for _, datum := range data {
			select {
			case outputs <- datum:
			case <-done:
				return
			}
		}
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
	})
	consume := func(inputs <-chan *MapData) {
		for input := range inputs {
			fmt.Println(string(input.Value.([]byte)))
			close(done)
			break
		}
	}
	consume(Work(w, done, Produce(p, done)))
	// Output:
	// te
}
