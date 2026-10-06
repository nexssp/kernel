package dag

import "github.com/nexssp/kernel/xerr"

// WithMaxSteps bounds the number of dynamic-execution steps when the
// DAG contains conditional edges. Without a bound, a router that
// returns a cycle would loop forever. The default (32) is applied at
// execute time when the value is <= 0.
//
// Ignored for DAGs without conditional edges — layer execution is
// always bounded by the layer count.
func (b *Builder) WithMaxSteps(n int) *Builder {
	if b.compileErr != nil {
		return b
	}
	if n < 0 {
		b.compileErr = xerr.BadRequest("graph: max steps cannot be negative")
		return b
	}
	b.maxSteps = n
	return b
}

// WithEntry designates the node a dynamic DAG starts from. Only
// meaningful when the DAG has at least one conditional edge; ignored
// otherwise.
//
// The default entry is the first node added via AddNode. Set this
// explicitly when the natural start is not the first AddNode call —
// e.g. when a common setup node is registered before the actual
// branch point.
func (b *Builder) WithEntry(nodeID string) *Builder {
	if b.compileErr != nil {
		return b
	}
	if nodeID == "" {
		b.compileErr = xerr.BadRequest("graph: entry node ID cannot be empty")
		return b
	}
	b.entryID = nodeID
	return b
}
