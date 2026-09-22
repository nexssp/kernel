package action

import "iter"

// StreamOp is a generic, typed stream operator.
//
// Takes a stream of elements In, returns a stream of elements Out.
// Contract:
//   - lazy: does not consume input until output is iterated
//   - backpressure: yield(false) upstream stops the chain
//   - an item error propagates without breaking the stream (unless the operator decides otherwise)
//
// StreamOp is not AnyAction nor AnyStreamAction — it is the "middle" in the pipeline.
// Concrete implementations (Filter, Collect, Map) live in stream_ops.go.
type StreamOp[In, Out any] func(iter.Seq2[In, error]) iter.Seq2[Out, error]
