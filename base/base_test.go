package base

import (
	"fmt"

	"golang.org/x/net/context"
)

type mapData struct {
	KeyBytes []byte
	Value    []byte
}

func (m mapData) Key() []byte {
	return m.KeyBytes
}

func ExampleSequentialMapWorker() {
	ctx := context.Background()
	p := func(ctx context.Context, outputs chan<- Keyer) {
		defer close(outputs)
		data := []Keyer{
			&mapData{KeyBytes: []byte{}, Value: []byte("tes")},
			&mapData{KeyBytes: []byte{}, Value: []byte("las")},
		}
		for _, datum := range data {
			select {
			case outputs <- datum:
			case <-ctx.Done():
				return
			}
		}
	}
	w := SequentialMapWorker(func(input Keyer, outputs chan<- Keyer) {
		defer close(outputs)
		input.(*mapData).Value = append(input.(*mapData).Value, 't')
		outputs <- input
	})
	consume := func(inputs <-chan Keyer) {
		for input := range inputs {
			fmt.Println(string(input.(*mapData).Value))
		}
	}
	consume(Work(ctx, w, Produce(ctx, p)))
	// Output:
	// test
	// last
}

func ExampleChainMapWorkers_normal() {
	ctx := context.Background()
	p := func(ctx context.Context, outputs chan<- Keyer) {
		defer close(outputs)
		data := []Keyer{
			&mapData{KeyBytes: []byte{}, Value: []byte{}},
			&mapData{KeyBytes: []byte{}, Value: []byte("2")},
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
		SequentialMapWorker(func(input Keyer, outputs chan<- Keyer) {
			defer close(outputs)
			input.(*mapData).Value = append(input.(*mapData).Value, 't')
			outputs <- input
		}),
		SequentialMapWorker(func(input Keyer, outputs chan<- Keyer) {
			defer close(outputs)
			input.(*mapData).Value = append(input.(*mapData).Value, 'e')
			outputs <- input
		}),
		SequentialMapWorker(func(input Keyer, outputs chan<- Keyer) {
			defer close(outputs)
			input.(*mapData).Value = append(input.(*mapData).Value, 's')
			outputs <- input
		}),
		SequentialMapWorker(func(input Keyer, outputs chan<- Keyer) {
			defer close(outputs)
			input.(*mapData).Value = append(input.(*mapData).Value, 't')
			outputs <- input
		}),
	})
	consume := func(inputs <-chan Keyer) {
		for input := range inputs {
			fmt.Println(string(input.(*mapData).Value))
		}
	}
	consume(Work(ctx, w, Produce(ctx, p)))
	// Output:
	// test
	// 2test
}

func ExampleChainMapWorkers_one() {
	ctx := context.Background()
	p := func(ctx context.Context, outputs chan<- Keyer) {
		defer close(outputs)
		data := []Keyer{
			&mapData{KeyBytes: []byte{}, Value: []byte{}},
			&mapData{KeyBytes: []byte{}, Value: []byte("2")},
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
		SequentialMapWorker(func(input Keyer, outputs chan<- Keyer) {
			defer close(outputs)
			input.(*mapData).Value = append(input.(*mapData).Value, 't')
			outputs <- input
		}),
	})
	consume := func(inputs <-chan Keyer) {
		for input := range inputs {
			fmt.Println(string(input.(*mapData).Value))
		}
	}
	consume(Work(ctx, w, Produce(ctx, p)))
	// Output:
	// t
	// 2t
}

func ExampleChainMapWorkers_two() {
	ctx := context.Background()
	p := func(ctx context.Context, outputs chan<- Keyer) {
		defer close(outputs)
		data := []Keyer{
			&mapData{KeyBytes: []byte{}, Value: []byte{}},
			&mapData{KeyBytes: []byte{}, Value: []byte("2")},
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
		SequentialMapWorker(func(input Keyer, outputs chan<- Keyer) {
			defer close(outputs)
			input.(*mapData).Value = append(input.(*mapData).Value, 't')
			outputs <- input
		}),
		SequentialMapWorker(func(input Keyer, outputs chan<- Keyer) {
			defer close(outputs)
			input.(*mapData).Value = append(input.(*mapData).Value, 'e')
			outputs <- input
		}),
	})
	consume := func(inputs <-chan Keyer) {
		for input := range inputs {
			fmt.Println(string(input.(*mapData).Value))
		}
	}
	consume(Work(ctx, w, Produce(ctx, p)))
	// Output:
	// te
	// 2te
}

func ExampleChainMapWorkers_done() {
	ctx, cancel := context.WithCancel(context.Background())
	p := func(ctx context.Context, outputs chan<- Keyer) {
		defer close(outputs)
		data := []Keyer{
			&mapData{KeyBytes: []byte{}, Value: []byte{}},
			&mapData{KeyBytes: []byte{}, Value: []byte("2")},
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
		SequentialMapWorker(func(input Keyer, outputs chan<- Keyer) {
			defer close(outputs)
			input.(*mapData).Value = append(input.(*mapData).Value, 't')
			outputs <- input
		}),
		SequentialMapWorker(func(input Keyer, outputs chan<- Keyer) {
			defer close(outputs)
			input.(*mapData).Value = append(input.(*mapData).Value, 'e')
			outputs <- input
		}),
	})
	consume := func(inputs <-chan Keyer) {
		for input := range inputs {
			fmt.Println(string(input.(*mapData).Value))
			cancel()
			break
		}
	}
	consume(Work(ctx, w, Produce(ctx, p)))
	// Output:
	// te
}
