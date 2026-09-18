package dag

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

type EdgeMeta struct {
	From     string
	To       string
	Channel  string
	Protocol string
}

type TopologyRegistry struct {
	mu    sync.Mutex
	edges map[EdgeMeta]struct{}
}

var GlobalTopology = &TopologyRegistry{}

func (t *TopologyRegistry) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.edges = nil
}

func (t *TopologyRegistry) Register(from, to, channel, protocol string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.edges == nil {
		t.edges = make(map[EdgeMeta]struct{})
	}
	t.edges[EdgeMeta{
		From:     from,
		To:       to,
		Channel:  channel,
		Protocol: protocol,
	}] = struct{}{}
}

func (t *TopologyRegistry) ToMermaid() string {
	t.mu.Lock()
	defer t.mu.Unlock()

	var sb strings.Builder
	edges := make([]EdgeMeta, 0, len(t.edges))
	for e := range t.edges {
		edges = append(edges, e)
	}
	sort.SliceStable(edges, func(i, j int) bool {
		if edges[i].From != edges[j].From {
			return edges[i].From < edges[j].From
		}
		if edges[i].To != edges[j].To {
			return edges[i].To < edges[j].To
		}
		if edges[i].Channel != edges[j].Channel {
			return edges[i].Channel < edges[j].Channel
		}
		return edges[i].Protocol < edges[j].Protocol
	})

	for _, e := range edges {
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
