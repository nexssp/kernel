// Package dag provides a small action-oriented directed acyclic graph.
//
// Nodes are Nexss actions. Compile validates dependencies and produces
// deterministic execution layers. Execute runs each layer with cancellation
// and merges results only at layer boundaries.
package dag
