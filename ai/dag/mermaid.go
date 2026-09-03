package dag

import (
	"cmp"
	"slices"
	"strings"
)

// ToMermaid exports the compiled DAG topology to Mermaid flowchart syntax.
// It is deterministic, safe for concurrent calls, and allocation-optimized.
func (d *DAG) ToMermaid() string {
	if d == nil || len(d.nodes) == 0 {
		return ""
	}

	// Pre-size builder to prevent intermediate reallocations
	estimatedBytes := 32 + (len(d.nodes) * 48) + (len(d.edges) * 40)
	var sb strings.Builder
	sb.Grow(estimatedBytes)

	sb.WriteString("flowchart LR\n")

	// Sort node IDs for deterministic output
	nodeIDs := make([]string, 0, len(d.nodes))
	for id := range d.nodes {
		nodeIDs = append(nodeIDs, id)
	}
	slices.Sort(nodeIDs)

	for _, id := range nodeIDs {
		sb.WriteString("    ")
		sb.WriteString(sanitizeMermaidID(id))
		sb.WriteString("[\"")
		sb.WriteString(escapeMermaidLabel(id))
		sb.WriteString("\"]\n")
	}

	// Sort edges for deterministic output
	if len(d.edges) > 0 {
		edges := slices.Clone(d.edges)
		slices.SortFunc(edges, func(a, b Edge) int {
			if c := cmp.Compare(a.From, b.From); c != 0 {
				return c
			}
			return cmp.Compare(a.To, b.To)
		})

		for _, e := range edges {
			sb.WriteString("    ")
			sb.WriteString(sanitizeMermaidID(e.From))
			sb.WriteString(" --> ")
			sb.WriteString(sanitizeMermaidID(e.To))
			sb.WriteString("\n")
		}
	}

	return sb.String()
}

func escapeMermaidLabel(s string) string {
	if !strings.Contains(s, "\"") {
		return s
	}
	return strings.ReplaceAll(s, "\"", "#quot;")
}
