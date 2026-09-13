package action

type NodeFilterPolicy struct {
	ActiveNode              string
	DefaultNode             string
	AllowUntaggedEverywhere bool
}

func (m *Meta) MatchesNode(policy NodeFilterPolicy) bool {
	if m == nil {
		return false
	}

	// Monolith mode: every action is local.
	if policy.ActiveNode == "" {
		return true
	}

	// System actions must run everywhere: /health, /metrics, etc.
	if m.IsSystem() {
		return true
	}

	// Exact node placement.
	if m.Node == policy.ActiveNode {
		return true
	}

	// Untagged actions.
	if m.Node == "" {
		if policy.AllowUntaggedEverywhere {
			return true
		}
		if policy.DefaultNode != "" && policy.ActiveNode == policy.DefaultNode {
			return true
		}
	}

	return false
}

func FilterByNode(actions []AnyAction, policy NodeFilterPolicy) []AnyAction {
	if policy.ActiveNode == "" {
		return actions
	}
	filtered := make([]AnyAction, 0, len(actions))
	for _, act := range actions {
		if act != nil && act.Describe() != nil && act.Describe().MatchesNode(policy) {
			filtered = append(filtered, act)
		}
	}
	return filtered
}
