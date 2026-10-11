// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.
package action

import (
	"reflect"
	"slices"
	"strconv"
)

const (
	IntrospectionSchemaVersion   = 1
	DefaultMaxIntrospectionNodes = 10000
	DefaultMaxIntrospectionEdges = 20000
)

// IntrospectionNode is the legacy tree snapshot. It deliberately excludes
// request, response, and example values. Use InspectActionGraph for exports.
type IntrospectionNode struct {
	Name      string              `json:"name,omitempty"`
	Tags      []string            `json:"tags,omitempty"`
	Children  []IntrospectionNode `json:"children,omitempty"`
	Truncated bool                `json:"truncated,omitempty"`
}

// IntrospectionChild describes one outgoing composition relationship. Node
// may be an AnyAction or another Describable value, such as a stream stage.
type IntrospectionChild struct {
	Node     Describable
	Relation string
	Role     string
	Label    string
}

// IntrospectionComposition describes only stable structure. Notes must be
// concise capability/opacity statements and must not contain request values.
type IntrospectionComposition struct {
	Kind     string
	Opaque   bool
	Notes    []string
	Children []IntrospectionChild
}

// IntrospectionProvider exposes opt-in composition metadata without changing
// AnyAction or requiring action closures to be inspected.
type IntrospectionProvider interface {
	Introspection() IntrospectionComposition
}

// WithComposition explicitly marks an action with its known composition.
// The returned action delegates execution, metadata, hooks, and bindings to
// act; structure is stored only because the caller requested introspection.
func WithComposition(act AnyAction, composition IntrospectionComposition) AnyAction {
	if isNilDescribable(act) {
		return nil
	}
	return &introspectionAction{AnyAction: act, composition: cloneComposition(composition)}
}

// WithChildren is the compatibility helper for generic composites. Prefer
// WithComposition when the kind and child relationships are known.
func WithChildren(act AnyAction, children ...AnyAction) AnyAction {
	composition := IntrospectionComposition{Kind: "composite", Opaque: true}
	for _, child := range children {
		if !isNilDescribable(child) {
			composition.Children = append(composition.Children, IntrospectionChild{
				Node: child, Relation: "contains", Role: "child",
			})
		}
	}
	return WithComposition(act, composition)
}

type introspectionAction struct {
	AnyAction
	composition IntrospectionComposition
}

var (
	_ Composite             = (*introspectionAction)(nil)
	_ IntrospectionProvider = (*introspectionAction)(nil)
)

func (a *introspectionAction) Children() []AnyAction {
	children := make([]AnyAction, 0, len(a.composition.Children))
	for _, child := range a.composition.Children {
		if act, ok := child.Node.(AnyAction); ok && !isNilDescribable(act) {
			children = append(children, act)
		}
	}
	return children
}

func (a *introspectionAction) Introspection() IntrospectionComposition {
	return cloneComposition(a.composition)
}

func (a *introspectionAction) CloneWithHooks(hooks ...AnyHook) AnyAction {
	return WithComposition(a.AnyAction.CloneWithHooks(hooks...), a.composition)
}

// IntrospectionGraph is a versioned, value-safe representation of one built
// program. IDs are stable within the snapshot; repeated action references share
// IDs, and cycles are represented by edges rather than recursively expanded.
type IntrospectionGraph struct {
	SchemaVersion int                      `json:"schema_version"`
	Root          string                   `json:"root,omitempty"`
	Nodes         []IntrospectionGraphNode `json:"nodes,omitempty"`
	Edges         []IntrospectionEdge      `json:"edges,omitempty"`
	Truncated     bool                     `json:"truncated,omitempty"`
}

// IntrospectionGraphNode describes one action or opaque stage. It contains
// stable descriptive metadata and structure only, never payloads, argument
// values, or action examples.
type IntrospectionGraphNode struct {
	ID              string   `json:"id"`
	Kind            string   `json:"kind"`
	Name            string   `json:"name,omitempty"`
	Description     string   `json:"description,omitempty"`
	Tags            []string `json:"tags,omitempty"`
	Opaque          bool     `json:"opaque,omitempty"`
	Notes           []string `json:"notes,omitempty"`
	Truncated       bool     `json:"truncated,omitempty"`
	OmittedChildren int      `json:"omitted_children,omitempty"`
}

// IntrospectionEdge is a directed relationship from a composing node to a
// child. Order records declared order; it is not a runtime scheduling promise.
type IntrospectionEdge struct {
	From     string `json:"from"`
	To       string `json:"to"`
	Relation string `json:"relation"`
	Role     string `json:"role,omitempty"`
	Label    string `json:"label,omitempty"`
	Order    int    `json:"order"`
}

// IntrospectionOptions bounds traversal work and serialized output.
type IntrospectionOptions struct {
	MaxDepth int
	MaxNodes int
	MaxEdges int
}

// InspectActionGraph returns a bounded, value-safe graph snapshot of root.
func InspectActionGraph(root AnyAction) IntrospectionGraph {
	return InspectActionGraphWithOptions(root, IntrospectionOptions{})
}

// InspectActionGraphWithOptions returns a graph snapshot, deduplicating shared
// action references and marking omitted structure when a safety bound is hit.
func InspectActionGraphWithOptions(root AnyAction, opts IntrospectionOptions) IntrospectionGraph {
	if opts.MaxDepth <= 0 {
		opts.MaxDepth = DefaultMaxTreeDepth
	}
	if opts.MaxNodes <= 0 {
		opts.MaxNodes = DefaultMaxIntrospectionNodes
	}
	if opts.MaxEdges <= 0 {
		opts.MaxEdges = DefaultMaxIntrospectionEdges
	}
	builder := graphBuilder{
		graph: IntrospectionGraph{SchemaVersion: IntrospectionSchemaVersion},
		opts:  opts,
		seen:  make(map[uintptr]string),
	}
	builder.graph.Root = builder.add(root, 0)
	return builder.graph
}

type graphBuilder struct {
	graph IntrospectionGraph
	opts  IntrospectionOptions
	seen  map[uintptr]string
}

func (b *graphBuilder) add(target Describable, depth int) string {
	if isNilDescribable(target) {
		return ""
	}
	identity := actionPointer(target)
	if identity != 0 {
		if id := b.seen[identity]; id != "" {
			return id
		}
	}
	if len(b.graph.Nodes) >= b.opts.MaxNodes {
		b.graph.Truncated = true
		return ""
	}

	composition, children := describeComposition(target)
	meta := target.Describe()
	node := IntrospectionGraphNode{
		ID:     "n" + strconv.Itoa(len(b.graph.Nodes)+1),
		Kind:   composition.Kind,
		Opaque: composition.Opaque,
		Notes:  slices.Clone(composition.Notes),
	}
	if node.Kind == "" {
		node.Kind = "action"
	}
	if meta != nil {
		node.Name = meta.Name
		node.Description = meta.Description
		node.Tags = slices.Clone(meta.Tags)
	}
	b.graph.Nodes = append(b.graph.Nodes, node)
	if identity != 0 {
		b.seen[identity] = node.ID
	}
	if depth >= b.opts.MaxDepth {
		b.graph.Nodes[len(b.graph.Nodes)-1].Truncated = true
		b.graph.Truncated = true
		return node.ID
	}

	for i, child := range children {
		if isNilDescribable(child.Node) {
			continue
		}
		if len(b.graph.Edges) >= b.opts.MaxEdges {
			b.markOmitted(node.ID, len(children)-i)
			break
		}
		childID := b.add(child.Node, depth+1)
		if childID == "" {
			b.markOmitted(node.ID, len(children)-i)
			break
		}
		relation := child.Relation
		if relation == "" {
			relation = "contains"
		}
		role := child.Role
		if role == "" {
			role = "child"
		}
		b.graph.Edges = append(b.graph.Edges, IntrospectionEdge{
			From: node.ID, To: childID, Relation: relation,
			Role: role, Label: child.Label, Order: i,
		})
	}
	return node.ID
}

func (b *graphBuilder) markOmitted(parentID string, count int) {
	for i := range b.graph.Nodes {
		if b.graph.Nodes[i].ID == parentID {
			b.graph.Nodes[i].Truncated = true
			b.graph.Nodes[i].OmittedChildren += count
			break
		}
	}
	b.graph.Truncated = true
}

func describeComposition(target Describable) (IntrospectionComposition, []IntrospectionChild) {
	if provider, ok := target.(IntrospectionProvider); ok {
		composition := cloneComposition(provider.Introspection())
		return composition, composition.Children
	}
	if composite, ok := target.(Composite); ok {
		children := composite.Children()
		composition := IntrospectionComposition{Kind: "composite", Opaque: true}
		refs := make([]IntrospectionChild, 0, len(children))
		for _, child := range children {
			if !isNilDescribable(child) {
				refs = append(refs, IntrospectionChild{Node: child, Relation: "contains", Role: "child"})
			}
		}
		composition.Children = refs
		return composition, refs
	}
	return IntrospectionComposition{Kind: "action", Opaque: true}, nil
}

func cloneComposition(composition IntrospectionComposition) IntrospectionComposition {
	composition.Notes = slices.Clone(composition.Notes)
	composition.Children = slices.Clone(composition.Children)
	return composition
}

// InspectActionTree returns the legacy nested view of optional AnyAction
// Composite children. New exports should prefer InspectActionGraph.
func InspectActionTree(root AnyAction) IntrospectionNode {
	return inspectActionTree(root, 0, make(map[uintptr]bool))
}

func inspectActionTree(act AnyAction, depth int, ancestors map[uintptr]bool) IntrospectionNode {
	if isNilDescribable(act) {
		return IntrospectionNode{}
	}
	node := introspectionNode(act)
	if depth >= DefaultMaxTreeDepth {
		node.Truncated = true
		return node
	}
	identity := actionPointer(act)
	if identity != 0 {
		if ancestors[identity] {
			node.Truncated = true
			return node
		}
		ancestors[identity] = true
		defer delete(ancestors, identity)
	}
	composite, ok := act.(Composite)
	if !ok {
		return node
	}
	for _, child := range composite.Children() {
		if !isNilDescribable(child) {
			node.Children = append(node.Children, inspectActionTree(child, depth+1, ancestors))
		}
	}
	return node
}

func introspectionNode(act AnyAction) IntrospectionNode {
	meta := act.Describe()
	if meta == nil {
		return IntrospectionNode{}
	}
	return IntrospectionNode{Name: meta.Name, Tags: slices.Clone(meta.Tags)}
}

func actionPointer(act any) uintptr {
	value := reflect.ValueOf(act)
	if value.IsValid() && value.Kind() == reflect.Pointer && !value.IsNil() {
		return value.Pointer()
	}
	return 0
}

func isNilDescribable(node Describable) bool {
	if node == nil {
		return true
	}

	value := reflect.ValueOf(node)
	kind := value.Kind()

	if kind == reflect.Chan ||
		kind == reflect.Func ||
		kind == reflect.Interface ||
		kind == reflect.Map ||
		kind == reflect.Pointer ||
		kind == reflect.Slice {
		return value.IsNil()
	}

	return false
}
