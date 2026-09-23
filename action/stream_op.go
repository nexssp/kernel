package action

import "github.com/nexssp/kernel/stream"

// StreamOp is a generic type alias for stream.StreamOp.
// It is exposed here so consumers defining action operators don't need
// to import the stream package manually, and to maintain backward compatibility
// with existing flow nodes.
type StreamOp[In, Out any] = stream.StreamOp[In, Out]
