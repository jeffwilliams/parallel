// Parallel processes slices in parallel. The main entry point is the function Parallel.
package parallel

import "sort"

// Processor is a function to process one item in the input array.
type Processor func(index int, input interface{}) (output interface{}, err error)

type Result struct {
	Input  interface{}
	Output interface{}
	Error  error
	index  int
}

// Parallel processes the input in parallel. It runs `processor` once for each item in `items` in parallel. The results are
// returned in `results`. For each result, the fields Output and Error are set to the return values of `processor`.
func Parallel(items []interface{}, processor Processor, opts ...Opt) (results []Result, err error) {
	options, err := buildOpts(opts)
	if err != nil {
		return
	}

	ex := newExecutor(len(items), processor, options.notif)

	for i, x := range items {
		go ex.execute(i, x)
	}

	results = ex.results()

	if options.preserveOrder {
		sortResults(results)
	}

	return
}

func sortResults(results []Result) {
	sort.Slice(results, func(i, j int) bool {
		return results[i].index < results[j].index
	})
}

type Opt func(opts *Opts) error

type Opts struct {
	preserveOrder bool
	notif         FinishNotif
}

// PreserveOrder is an option that when set preserves the ordering of the results (sorts the results).
func PreserveOrder() Opt {
	return func(opts *Opts) error {
		opts.preserveOrder = true
		return nil
	}
}

// OnFinish adds a handler that gets called as each task gets completed.
// Useful for displaying progress.
func OnFinish(f FinishNotif) Opt {
	return func(opts *Opts) error {
		opts.notif = f
		return nil
	}
}

type FinishNotif func(index int)

func buildOpts(opts []Opt) (options *Opts, err error) {
	options = &Opts{}

	for _, o := range opts {
		err = o(options)
		if err != nil {
			break
		}
	}
	return
}

type executor struct {
	ch        chan Result
	processor Processor
	notif     FinishNotif
	size      int
}

func newExecutor(numResults int, processor Processor, notif FinishNotif) executor {
	return executor{
		ch:        make(chan Result, numResults),
		processor: processor,
		notif:     notif,
		size:      numResults,
	}
}

func (e executor) execute(index int, item interface{}) {
	output, err := e.processor(index, item)
	e.ch <- Result{item, output, err, index}
}

func (e executor) results() []Result {
	results := make([]Result, e.size)
	i := 0
	for r := range e.ch {
		results[i] = r
		e.notify(i)
		i++
		if i >= e.size {
			break
		}
	}
	return results
}

func (e executor) notify(i int) {
	if e.notif != nil {
		e.notif(i)
	}
}
