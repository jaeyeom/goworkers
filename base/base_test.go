package base

import (
	"fmt"

	"golang.org/x/net/context"
)

func ExampleSequentialMapWorker() {
	ctx := context.Background()
	p := func(ctx context.Context, outputs chan<- *MapData) {
		defer close(outputs)
		data := []*MapData{
			&MapData{Key: []byte{}, Value: []byte("tes")},
			&MapData{Key: []byte{}, Value: []byte("las")},
		}
		for _, datum := range data {
			select {
			case outputs <- datum:
			case <-ctx.Done():
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
	consume(Work(ctx, w, Produce(ctx, p)))
	// Output:
	// test
	// last
}

func ExampleChainMapWorkers_normal() {
	ctx := context.Background()
	p := func(ctx context.Context, outputs chan<- *MapData) {
		defer close(outputs)
		data := []*MapData{
			&MapData{Key: []byte{}, Value: []byte{}},
			&MapData{Key: []byte{}, Value: []byte("2")},
		}
		for _, datum := range data {
			select {
			case outputs <- datum:
			case <-ctx.Done():
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
	consume(Work(ctx, w, Produce(ctx, p)))
	// Output:
	// test
	// 2test
}

func ExampleChainMapWorkers_one() {
	ctx := context.Background()
	p := func(ctx context.Context, outputs chan<- *MapData) {
		defer close(outputs)
		data := []*MapData{
			&MapData{Key: []byte{}, Value: []byte{}},
			&MapData{Key: []byte{}, Value: []byte("2")},
		}
		for _, datum := range data {
			select {
			case outputs <- datum:
			case <-ctx.Done():
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
	consume(Work(ctx, w, Produce(ctx, p)))
	// Output:
	// t
	// 2t
}

func ExampleChainMapWorkers_two() {
	ctx := context.Background()
	p := func(ctx context.Context, outputs chan<- *MapData) {
		defer close(outputs)
		data := []*MapData{
			&MapData{Key: []byte{}, Value: []byte{}},
			&MapData{Key: []byte{}, Value: []byte("2")},
		}
		for _, datum := range data {
			select {
			case outputs <- datum:
			case <-ctx.Done():
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
	consume(Work(ctx, w, Produce(ctx, p)))
	// Output:
	// te
	// 2te
}

func ExampleChainMapWorkers_done() {
	ctx, cancel := context.WithCancel(context.Background())
	p := func(ctx context.Context, outputs chan<- *MapData) {
		defer close(outputs)
		data := []*MapData{
			&MapData{Key: []byte{}, Value: []byte{}},
			&MapData{Key: []byte{}, Value: []byte("2")},
		}
		for _, datum := range data {
			select {
			case outputs <- datum:
			case <-ctx.Done():
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
			cancel()
			break
		}
	}
	consume(Work(ctx, w, Produce(ctx, p)))
	// Output:
	// te
}
