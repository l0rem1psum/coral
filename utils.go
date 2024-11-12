package processor

import (
	"sync"
)

func roundRobinFanIn[T any](channelBufferCap int, upstreams ...<-chan T) <-chan fannedInResult[T] {
	out := make(chan fannedInResult[T], channelBufferCap)

	go func() {
		defer close(out)

		n := len(upstreams)
		closed := make([]bool, len(upstreams))

		for n > 0 {
			for i, upstream := range upstreams {
				if closed[i] {
					continue
				}

				if value, ok := <-upstream; ok {
					out <- fannedInResult[T]{
						t:     value,
						index: i,
					}
				} else {
					closed[i] = true
					n--
				}
			}
		}
	}()

	return out
}

func parallelFanIn[T any](channelBufferCap int, upstreams ...<-chan T) <-chan fannedInResult[T] {
	out := make(chan fannedInResult[T], channelBufferCap)
	var wg sync.WaitGroup

	wg.Add(len(upstreams))
	for i := range upstreams {
		go func(index int) {
			for n := range upstreams[index] {
				out <- fannedInResult[T]{t: n, index: index}
			}
			wg.Done()
		}(i)
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

type fannedInResult[T any] struct {
	t     T
	index int
}

func closeMultipleChans[T any](chs ...chan T) {
	for _, ch := range chs {
		close(ch)
	}
}
