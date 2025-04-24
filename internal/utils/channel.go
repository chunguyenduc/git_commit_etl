package utils

import (
	"sync"
)

func FanIn[T any](channels ...chan T) chan T {
	var wg sync.WaitGroup

	multiplexedStream := make(chan T)

	multiplex := func(c <-chan T) {
		defer wg.Done()

		for i := range c {
			multiplexedStream <- i
		}
	}

	wg.Add(len(channels))

	for _, c := range channels {
		go multiplex(c)
	}

	go func() {
		wg.Wait()
		close(multiplexedStream)
	}()

	return multiplexedStream
}
