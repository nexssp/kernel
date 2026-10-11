// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.
package action_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/nexssp/kernel/action"
)

func TestInspectActionTree_OptInSnapshotAndClone(t *testing.T) {
	leaf := testAction("demo.leaf").Tag("leaf").Example(map[string]any{"secret": "not copied"}).Build()
	root := testAction("pipe").Tag("pipe", "dynamic").Build()
	composed := action.WithChildren(root, leaf)

	tree := action.InspectActionTree(composed)
	if tree.Name != "pipe" || len(tree.Tags) != 2 || len(tree.Children) != 1 {
		t.Fatalf("unexpected tree: %#v", tree)
	}
	if tree.Children[0].Name != "demo.leaf" || len(tree.Children[0].Children) != 0 {
		t.Fatalf("unexpected child: %#v", tree.Children[0])
	}

	composite, ok := composed.(action.Composite)
	if !ok {
		t.Fatalf("composed action does not implement action.Composite: %T", composed)
	}
	children := composite.Children()
	children[0] = root
	if got := action.InspectActionTree(composed).Children[0].Name; got != "demo.leaf" {
		t.Fatalf("mutating returned children changed action tree: %q", got)
	}

	cloned := composed.CloneWithHooks(action.AnyHook{})
	if got := action.InspectActionTree(cloned).Children[0].Name; got != "demo.leaf" {
		t.Fatalf("clone lost child structure: %q", got)
	}
	encoded, err := json.Marshal(tree)
	if err != nil {
		t.Fatalf("marshal tree: %v", err)
	}
	if strings.Contains(string(encoded), "secret") || strings.Contains(string(encoded), "not copied") {
		t.Fatalf("tree leaked an example value: %s", encoded)
	}
}

func TestInspectActionGraph_RecordsTypedEdgesAndOmitsValues(t *testing.T) {
	left := testAction("demo.left").Description("static description").Example(map[string]any{"token": "do-not-export"}).Build()
	right := testAction("demo.right").Build()
	root := action.WithComposition(testAction("pipe").Tag("pipe").Build(), action.IntrospectionComposition{
		Kind:  "sequence",
		Notes: []string{"declared order; execution remains delegated to Kernel"},
		Children: []action.IntrospectionChild{
			{Node: left, Relation: "sequence", Role: "left"},
			{Node: right, Relation: "sequence", Role: "right"},
		},
	})

	graph := action.InspectActionGraph(root)
	if graph.SchemaVersion != action.IntrospectionSchemaVersion || graph.Root != "n1" {
		t.Fatalf("unexpected graph header: %#v", graph)
	}
	if len(graph.Nodes) != 3 || len(graph.Edges) != 2 {
		t.Fatalf("unexpected graph size: %#v", graph)
	}
	if graph.Nodes[0].Kind != "sequence" || graph.Nodes[0].Name != "pipe" {
		t.Fatalf("unexpected root node: %#v", graph.Nodes[0])
	}
	if !graph.Nodes[1].Opaque || graph.Nodes[1].Kind != "action" {
		t.Fatalf("leaf implementation should be explicitly opaque: %#v", graph.Nodes[1])
	}
	if graph.Nodes[1].Description != "static description" {
		t.Fatalf("static action description was not preserved: %#v", graph.Nodes[1])
	}
	if graph.Edges[0].Relation != "sequence" || graph.Edges[0].Role != "left" || graph.Edges[0].Order != 0 {
		t.Fatalf("unexpected first edge: %#v", graph.Edges[0])
	}
	if graph.Edges[1].Role != "right" || graph.Edges[1].Order != 1 {
		t.Fatalf("unexpected second edge: %#v", graph.Edges[1])
	}

	encoded, err := json.Marshal(graph)
	if err != nil {
		t.Fatalf("marshal graph: %v", err)
	}
	if strings.Contains(string(encoded), "token") || strings.Contains(string(encoded), "do-not-export") {
		t.Fatalf("graph leaked action example data: %s", encoded)
	}
}

func TestInspectActionGraph_DeduplicatesSharedActionsAndRepresentsCycles(t *testing.T) {
	shared := testAction("shared").Build()
	root := action.WithComposition(testAction("parallel").Build(), action.IntrospectionComposition{
		Kind: "parallel",
		Children: []action.IntrospectionChild{
			{Node: shared, Relation: "branch", Role: "branch", Label: "first"},
			{Node: shared, Relation: "branch", Role: "branch", Label: "second"},
		},
	})
	graph := action.InspectActionGraph(root)
	if len(graph.Nodes) != 2 || len(graph.Edges) != 2 || graph.Edges[0].To != graph.Edges[1].To {
		t.Fatalf("shared action was not represented once: %#v", graph)
	}

	cycle := &cycleAction{AnyAction: testAction("cycle").Build()}
	cycle.next = cycle
	graph = action.InspectActionGraph(cycle)
	if len(graph.Nodes) != 1 || len(graph.Edges) != 1 || graph.Edges[0].From != graph.Edges[0].To || graph.Truncated {
		t.Fatalf("cycle should be an explicit edge, not recursive truncation: %#v", graph)
	}
}

func TestInspectActionGraph_BoundsTraversal(t *testing.T) {
	root := action.WithComposition(testAction("root").Build(), action.IntrospectionComposition{
		Kind: "parallel",
		Children: []action.IntrospectionChild{
			{Node: testAction("one").Build(), Relation: "branch"},
			{Node: testAction("two").Build(), Relation: "branch"},
		},
	})
	graph := action.InspectActionGraphWithOptions(root, action.IntrospectionOptions{MaxNodes: 2})
	if !graph.Truncated || len(graph.Nodes) != 2 || len(graph.Edges) != 1 {
		t.Fatalf("node limit was not applied: %#v", graph)
	}
	if !graph.Nodes[0].Truncated || graph.Nodes[0].OmittedChildren != 1 {
		t.Fatalf("omitted child was not reported: %#v", graph.Nodes[0])
	}
}

func TestInspectActionTree_LeafCanBeMarked(t *testing.T) {
	leaf := testAction("demo.leaf").Build()
	tree := action.InspectActionTree(action.WithChildren(leaf))
	if tree.Name != "demo.leaf" || len(tree.Children) != 0 || tree.Truncated {
		t.Fatalf("unexpected introspected leaf: %#v", tree)
	}
}

func testAction(name string) *action.Builder[any, any] {
	return action.New(name, func(_ context.Context, in any) (any, error) { return in, nil })
}

type cycleAction struct {
	action.AnyAction
	next action.Describable
}

func (a *cycleAction) Introspection() action.IntrospectionComposition {
	return action.IntrospectionComposition{Kind: "cycle", Children: []action.IntrospectionChild{{
		Node: a.next, Relation: "recursive", Role: "self",
	}}}
}
