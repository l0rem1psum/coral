package processorutils_test

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	processor "github.com/l0rem1psum/coral"
	processorutils "github.com/l0rem1psum/coral/utils"
	"github.com/stretchr/testify/require"
)

type (
	foo struct {
		i int
	}
	fooio struct{}
)

var _ processor.Generic1In1OutAsyncProcessorIO[*foo, *foo, *foo, *foo] = &fooio{}

func (f *fooio) AsInput(t *foo) *foo    { return t }
func (f *fooio) FromOutput(t *foo) *foo { return t }
func (f *fooio) ReleaseInput(t *foo)    {}
func (f *fooio) ReleaseOutput(t *foo)   {}

func TestRateLimiting1In1OutAsyncProcessor_FastInput(t *testing.T) {
	var (
		ratePerSecond           float64 = 1 + rand.Float64()*(200-1) // Random float64 between 1 and 200
		generatorRateMultiplier float64 = func() float64 {
			if rand.Float64() < 0.5 { // Fast
				return 1 + rand.Float64()*(20-1) // Random float64 between 1 and 20
			} else { // Slow
				return 1 / (1 + rand.Float64()*(20-1)) // Random float64 between 1/20 and 1
			}
		}()
		expectedRate float64 = min(ratePerSecond*generatorRateMultiplier, ratePerSecond)
		tolerance    float64 = 0.01
	)

	inputCh := func() chan *foo {
		ch := make(chan *foo)
		go func() {
			defer close(ch)
			i := 0
			for {
				ch <- &foo{i}
				i++
				time.Sleep(time.Duration(1e9 / (ratePerSecond * generatorRateMultiplier)))
			}
		}()
		return ch
	}()

	ratelimiter := processorutils.NewRateLimiting1In1OutAsyncProcessor[*fooio](ratePerSecond)
	controller, outputCh, err := processor.InitializeGeneric1In1OutAsyncProcessor[*fooio](ratelimiter)(inputCh)
	require.NoError(t, err)

	err = controller.Start()
	require.NoError(t, err)

	finish := time.After(5 * time.Second)

	n := -1
	lastReceivedTime := time.Now()
	totalWaitTime := time.Duration(0)

LOOP:
	for {
		select {
		case <-finish:
			err = controller.Stop()
			require.NoError(t, err)
			break LOOP
		case <-outputCh:
			now := time.Now()
			if n >= 0 {
				totalWaitTime += now.Sub(lastReceivedTime)
			}
			lastReceivedTime = now
			n++
		}
	}

	avgWaitTimeInNs := float64(totalWaitTime.Nanoseconds()) / float64(n)
	observedRate := 1e9 / avgWaitTimeInNs

	fmt.Println("observedRate", observedRate)
	fmt.Println("expectedRate", expectedRate)

	require.InDelta(t, expectedRate, observedRate, expectedRate*tolerance)
}
