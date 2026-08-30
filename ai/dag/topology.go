package dag

import (
	"fmt"
	"sort"
	"strings"
)

type EdgeMeta struct {
	From     string
	To       string
	Channel  string
	Protocol string
}

type TopologyRegistry struct {
	edges []EdgeMeta
}

var GlobalTopology = &TopologyRegistry{}

func (t *TopologyRegistry) Register(from, to, channel, protocol string) {
	t.edges = append(t.edges, EdgeMeta{
		From:     from,
		To:       to,
		Channel:  channel,
		Protocol: protocol,
	})
}

// ToMermaid generates Mermaid.js edge connections without the graph header.
func (t *TopologyRegistry) ToMermaid() string {
	var sb strings.Builder

	sort.Slice(t.edges, func(i, j int) bool {
		return t.edges[i].From < t.edges[j].From
	})

	for _, e := range t.edges {
		// Sanitize node IDs to match the subgraphs generated in architecture.go
		fromID := sanitizeMermaidID(e.From)
		toID := sanitizeMermaidID(e.To)

		fmt.Fprintf(&sb, "    %s -- \"%s (%s)\" --> %s\n", fromID, e.Channel, e.Protocol, toID)
	}
	return sb.String()
}

// sanitizeMermaidID ensures Mermaid doesn't crash on invalid characters like hyphens or dots.
func sanitizeMermaidID(s string) string {
	s = strings.ReplaceAll(s, ".", "_")
	s = strings.ReplaceAll(s, "-", "_")
	s = strings.ReplaceAll(s, " ", "_")
	s = strings.ReplaceAll(s, "(", "")
	s = strings.ReplaceAll(s, ")", "")
	return "id_" + s
}
