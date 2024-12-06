package pipeline

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"reflect"

	"github.com/dominikbraun/graph"
	"github.com/dominikbraun/graph/draw"
	processor "github.com/l0rem1psum/coral"
)

type EdgeLabels []string

type ProcessorVertex struct {
	Label   string
	Inputs  []EdgeLabels
	Outputs []EdgeLabels
}

type processorVertex struct {
	label   string
	inputs  []EdgeLabels
	outputs []EdgeLabels

	procInitializer any
	controller      *processor.Controller
}

func processorVertexHash(n *processorVertex) string {
	return n.label
}

type Pipeline struct {
	graph graph.Graph[string, *processorVertex]

	logger *slog.Logger
}

func NewPipeline(vertices []ProcessorVertex, logger *slog.Logger) (*Pipeline, error) {
	g := graph.New(processorVertexHash, graph.Directed(), graph.Acyclic())

	outputEdges := make(map[string]string)
	// Add vertices
	for _, vertex := range vertices {
		if err := g.AddVertex(&processorVertex{
			label:   vertex.Label,
			inputs:  vertex.Inputs,
			outputs: vertex.Outputs,
		},
			graph.VertexAttribute("shape", "box"),
			graph.VertexAttribute("style", "rounded"),
		); err != nil {
			return nil, err
		}
		for _, outputLabels := range vertex.Outputs {
			for _, output := range outputLabels {
				if _, present := outputEdges[output]; present {
					return nil, fmt.Errorf("output %s already present", output)
				}
				outputEdges[output] = vertex.Label
			}
		}
	}

	// Add edges
	for _, vertex := range vertices {
		for _, labels := range vertex.Inputs {
			for _, input := range labels {
				inputVertex, outputPresent := outputEdges[input]
				if !outputPresent {
					return nil, fmt.Errorf("output %s not found", input)
				}
				if err := g.AddEdge(inputVertex, vertex.Label, graph.EdgeAttribute("label", input)); err != nil {
					return nil, err
				}
			}
		}
	}

	return &Pipeline{
		graph:  g,
		logger: logger,
	}, nil
}

func (ppl *Pipeline) addProcessor(label string, procInitializer any) error {
	procInitializerType := reflect.TypeOf(procInitializer)

	if procInitializerType.Kind() != reflect.Func {
		return fmt.Errorf("processor cannot be added to vertex %s: not a function", label)
	}

	pv, err := ppl.graph.Vertex(label)
	if err != nil {
		if errors.Is(err, graph.ErrVertexNotFound) {
			return fmt.Errorf("processor cannot be added: vertex %s not found", label)
		}
		return fmt.Errorf("processor cannot be added to vertex %s: %w", label, err)
	}

	if pv.procInitializer != nil {
		return fmt.Errorf("vertex %s already has a processor", label)
	}

	if numIn := procInitializerType.NumIn(); len(pv.inputs) != numIn {
		return fmt.Errorf("vertex %s requires %d inputs, but processor takes in %d", label, len(pv.inputs), numIn)
	}

	// The first output is always the controller.
	if !isType[*processor.Controller](procInitializerType.Out(0)) {
		return fmt.Errorf("processor must return a controller as the first output")
	}

	// The last output is an error or a slice of errors.
	if lastOut := procInitializerType.Out(procInitializerType.NumOut() - 1); !isType[error](lastOut) && !isType[[]error](lastOut) {
		return fmt.Errorf("processor must return an error or []error as the last output, got %s", lastOut.String())
	}

	// Initializer may output more than what a vertex requires. In that case, the extra outputs are ignored.
	if numOut := procInitializerType.NumOut() - 2; len(pv.outputs) > numOut { // -2 for controller and error
		return fmt.Errorf("vertex %s requires %d outputs, but processor has %d", label, len(pv.outputs), numOut)
	}

	pv.procInitializer = procInitializer
	return nil
}

func AddGeneric0In1OutSyncProcessor[
	IO processor.Generic0In1OutSyncProcessorIO[O, Out],
	O, Out any,
](ppl *Pipeline,
	proc processor.Generic0In1OutSyncProcessor[Out],
	label string,
	opts ...processor.Option,
) error {
	defaultOpts := []processor.Option{
		processor.WithLogger(ppl.logger),
		processor.WithLabel(label),
	}
	initializer := processor.InitializeGeneric0In1OutSyncProcessor[IO](proc, append(defaultOpts, opts...)...)
	return ppl.addProcessor(label, initializer)
}

func AddGeneric0In1OutAsyncProcessor[
	IO processor.Generic0In1OutAsyncProcessorIO[O, Out],
	O, Out any,
](ppl *Pipeline,
	proc processor.Generic0In1OutAsyncProcessor[Out],
	label string,
	opts ...processor.Option,
) error {
	defaultOpts := []processor.Option{
		processor.WithLogger(ppl.logger),
		processor.WithLabel(label),
	}
	initializer := processor.InitializeGeneric0In1OutAsyncProcessor[IO](proc, append(defaultOpts, opts...)...)
	return ppl.addProcessor(label, initializer)
}

func AddGeneric1In1OutSyncProcessor[
	IO processor.Generic1In1OutSyncProcessorIO[I, O, In, Out],
	I, O, In, Out any,
](ppl *Pipeline,
	proc processor.Generic1In1OutSyncProcessor[In, Out],
	label string,
	opts ...processor.Option,
) error {
	defaultOpts := []processor.Option{
		processor.WithLogger(ppl.logger),
		processor.WithLabel(label),
	}
	initializer := processor.InitializeGeneric1In1OutSyncProcessor[IO](proc, append(defaultOpts, opts...)...)
	return ppl.addProcessor(label, initializer)
}

func AddGeneric1In1OutAsyncProcessor[
	IO processor.Generic1In1OutAsyncProcessorIO[I, O, In, Out],
	I, O, In, Out any,
](ppl *Pipeline,
	proc processor.Generic1In1OutAsyncProcessor[In, Out],
	label string,
	opts ...processor.Option,
) error {
	defaultOpts := []processor.Option{
		processor.WithLogger(ppl.logger),
		processor.WithLabel(label),
	}
	initializer := processor.InitializeGeneric1In1OutAsyncProcessor[IO](proc, append(defaultOpts, opts...)...)
	return ppl.addProcessor(label, initializer)
}

func AddGenericMInNOutSyncProcessor[
	IO processor.GenericMInNOutSyncProcessorIO[I, O, In, Out],
	I, O, In, Out any,
](ppl *Pipeline,
	proc processor.GenericMInNOutSyncProcessor[In, Out],
	label string,
	opts ...processor.Option,
) error {
	defaultOpts := []processor.Option{
		processor.WithLogger(ppl.logger),
		processor.WithLabel(label),
	}
	initializer := processor.InitializeGenericMInNOutSyncProcessor[IO](proc, append(defaultOpts, opts...)...)
	return ppl.addProcessor(label, initializer)
}

func AddGenericNIn0OutAsyncProcessor[
	IO processor.GenericNIn0OutAsyncProcessorIO[I, In],
	I, In any,
](ppl *Pipeline,
	proc processor.GenericNIn0OutAsyncProcessor[In],
	label string,
	opts ...processor.Option,
) error {
	defaultOpts := []processor.Option{
		processor.WithLogger(ppl.logger),
		processor.WithLabel(label),
	}
	initializer := processor.InitializeGenericNIn0OutAsyncProcessor[IO](proc, append(defaultOpts, opts...)...)
	return ppl.addProcessor(label, initializer)
}

func AddGeneric1In1OutSyncMultiProcessor[
	IO processor.Generic1In1OutSyncProcessorIO[I, O, In, Out],
	I, O, In, Out any,
	P processor.Generic1In1OutSyncProcessor[In, Out],
](
	ppl *Pipeline,
	processors []P,
	label string,
	opts ...processor.Option,
) error {
	defaultOpts := []processor.Option{
		processor.WithLogger(ppl.logger),
		processor.WithLabel(label),
	}
	initializer := processor.InitializeGeneric1In1OutSyncMultiProcessor[IO](processors, append(defaultOpts, opts...)...)
	return ppl.addProcessor(label, initializer)
}

func AddGeneric1In0OutSyncMultiProcessor[
	IO processor.Generic1In0OutSyncProcessorIO[I, In],
	I, In any,
	P processor.Generic1In0OutSyncProcessor[In],
](
	ppl *Pipeline,
	processors []P,
	label string,
	opts ...processor.Option,
) error {
	defaultOpts := []processor.Option{
		processor.WithLogger(ppl.logger),
		processor.WithLabel(label),
	}
	initializer := processor.InitializeGeneric1In0OutSyncMultiProcessor[IO](processors, append(defaultOpts, opts...)...)
	return ppl.addProcessor(label, initializer)
}

func AddGeneric1InNOutSyncMultiProcessor[
	IO processor.Generic1InNOutSyncProcessorIO[I, O, In, Out],
	I, O, In, Out any,
	P processor.Generic1InNOutSyncProcessor[In, Out],
](
	ppl *Pipeline,
	processors []P,
	label string,
	opts ...processor.Option,
) error {
	defaultOpts := []processor.Option{
		processor.WithLogger(ppl.logger),
		processor.WithLabel(label),
	}
	initializer := processor.InitializeGeneric1InNOutSyncMultiProcessor[IO](processors, append(defaultOpts, opts...)...)
	return ppl.addProcessor(label, initializer)
}

func AddGenericNIn1OutAsyncProcessor[
	IO processor.GenericNIn1OutAsyncProcessorIO[I, O, In, Out],
	I, O, In, Out any,
](
	ppl *Pipeline,
	proc processor.GenericNIn1OutAsyncProcessor[In, Out],
	label string,
	opts ...processor.Option,
) error {
	defaultOpts := []processor.Option{
		processor.WithLogger(ppl.logger),
		processor.WithLabel(label),
	}
	initializer := processor.InitializeGenericNIn1OutAsyncProcessor[IO](proc, append(defaultOpts, opts...)...)
	return ppl.addProcessor(label, initializer)
}

func (ppl *Pipeline) Initialize() error {
	topSortedVertices, err := graph.TopologicalSort(ppl.graph)
	if err != nil {
		return fmt.Errorf("failed to initialize graph: %w", err)
	}

	// Check all vertices have an associated processor initializer
	for _, vertex := range topSortedVertices {
		pv, err := ppl.graph.Vertex(vertex)
		if err != nil {
			if errors.Is(err, graph.ErrVertexNotFound) {
				panic(fmt.Errorf("failed to initialize pipeline: vertex %s not found", vertex))
			} else {
				return fmt.Errorf("failed to initialize pipeline: %w", err)
			}
		}
		if pv.procInitializer == nil {
			return fmt.Errorf("failed to initialize pipeline: vertex %s has not been added with a processor", vertex)
		}
	}

	// TODO: Check whether processors' types can form a valid graph before initializing them

	outputChs := make(map[string]reflect.Value)

	// Initialize from top to bottom
	for _, vertex := range topSortedVertices {
		pv, err := ppl.graph.Vertex(vertex)
		if err != nil {
			return err
		}

		initializerResults, err := callProcessorInitializer(pv, outputChs)
		if err != nil {
			return fmt.Errorf("failed to initialize processor for vortex %s: %w", pv.label, err)
		}

		// Check if the initializer returned an error
		initializerErr := initializerResults[len(initializerResults)-1]
		if isType[error](initializerErr.Type()) {
			if err := errOrNil(initializerErr); err != nil {
				ppl.stopAllControllers()
				return initializerErr.Interface().(error)
			}
		}
		if isType[[]error](initializerErr.Type()) {
			errs := asType[[]error](initializerErr)
			for _, err := range errs {
				if err != nil {
					ppl.stopAllControllers()
					return err
				}
			}
		}

		// Processor has been initialized, store its controller
		if controller := initializerResults[0].Interface().(*processor.Controller); controller != nil {
			pv.controller = controller
		} else {
			panic(fmt.Errorf("controller is nil"))
		}

		// Store the output channels
		if len(initializerResults)-2 != len(pv.outputs) { // -2 for controller and error
			return fmt.Errorf("expected %d outputs, got %d", len(pv.outputs), len(initializerResults)-2)
		}

		for i, outputLabels := range pv.outputs {
			if initializerResults[i+1].Kind() == reflect.Slice {
				if initializerResults[i+1].Len() != len(outputLabels) {
					return fmt.Errorf("expected %d outputs, got %d", len(outputLabels), initializerResults[i+1].Len())
				}
				for j, output := range outputLabels {
					outputChs[output] = initializerResults[i+1].Index(j)
				}
			} else {
				if len(outputLabels) != 1 {
					return fmt.Errorf("expected 1 output, got %d", len(outputLabels))
				}
				outputChs[outputLabels[0]] = initializerResults[i+1]
			}
		}
	}

	return nil
}

func (ppl *Pipeline) stopAllControllers() {
	topSortedVertices, err := graph.TopologicalSort(ppl.graph)
	if err != nil {
		panic(fmt.Errorf("failed to stop all controllers: %w", err))
	}

	for _, vertex := range topSortedVertices {
		pv, err := ppl.graph.Vertex(vertex)
		if err != nil {
			panic(fmt.Errorf("failed to stop all controllers: %w", err))
		}

		if pv.controller == nil {
			continue
		}

		if err := pv.controller.Stop(); err != nil {
			ppl.logger.With("error", err).With("vertex", vertex).Error("failed to stop controller, continuing...")
		}
	}
}

func (ppl *Pipeline) Start() error {
	topSortedVertices, err := graph.TopologicalSort(ppl.graph)
	if err != nil {
		return fmt.Errorf("failed to start pipeline: %w", err)
	}

	for _, vertex := range topSortedVertices {
		pv, err := ppl.graph.Vertex(vertex)
		if err != nil {
			return fmt.Errorf("failed to start pipeline: %w", err)
		}

		if pv.controller == nil {
			return fmt.Errorf("failed to start pipeline: vertex %s has not been initialized", vertex)
		}
	}

	for _, vertex := range topSortedVertices {
		pv, err := ppl.graph.Vertex(vertex)
		if err != nil {
			return fmt.Errorf("failed to start pipeline: %w", err)
		}

		if err := pv.controller.Start(); err != nil {
			ppl.stopAllControllers()
			return err
		}
	}

	return nil
}

func (ppl *Pipeline) Stop() error {
	ppl.stopAllControllers()
	return nil
}

func callProcessorInitializer(
	processorVertex *processorVertex,
	inputChs map[string]reflect.Value,
) ([]reflect.Value, error) {
	procInitializerType := reflect.TypeOf(processorVertex.procInitializer)

	paramsIn := make([]reflect.Value, procInitializerType.NumIn())
	for i, labels := range processorVertex.inputs {
		if inType := procInitializerType.In(i); inType.Kind() == reflect.Slice {
			paramsIn[i] = reflect.MakeSlice(inType, 0, 0)
			for _, input := range labels {
				inputVal, ok := inputChs[input]
				if !ok {
					return nil, fmt.Errorf("input %s not found", input)
				}
				delete(inputChs, input)
				paramsIn[i] = reflect.Append(paramsIn[i], inputVal)
			}
		} else {
			if len(labels) != 1 {
				return nil, fmt.Errorf("expected 1 input, got %d", len(labels))
			}
			inputVal, ok := inputChs[labels[0]]
			if !ok {
				return nil, fmt.Errorf("input %s not found", labels[0])
			}
			delete(inputChs, labels[0])
			paramsIn[i] = inputVal
		}
	}

	return reflect.ValueOf(processorVertex.procInitializer).Call(paramsIn), nil
}

func (ppl *Pipeline) DumpDot() {
	file, _ := os.Create("./pipeline.gv")
	_ = draw.DOT(ppl.graph, file)
}

func (ppl *Pipeline) GetControllerByVertex(vertex string) (*processor.Controller, error) {
	pv, err := ppl.graph.Vertex(vertex)
	if err != nil {
		return nil, err
	}
	return pv.controller, nil
}
