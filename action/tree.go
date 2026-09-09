package action

import "fmt"

const DefaultMaxTreeDepth = 64

type TreeHookOptions struct {
	MaxDepth int
}

type Composite interface {
	Children() []AnyAction
}

func ApplyTreeHook(act AnyAction, hooks ...AnyHook) error {
	return ApplyTreeHookOpts(act, TreeHookOptions{MaxDepth: DefaultMaxTreeDepth}, hooks...)
}

func ApplyTreeHookOpts(act AnyAction, opts TreeHookOptions, hooks ...AnyHook) error {
	if act == nil || len(hooks) == 0 {
		return nil
	}
	if opts.MaxDepth <= 0 {
		opts.MaxDepth = DefaultMaxTreeDepth
	}

	var stack [32]AnyAction
	_, err := applyTreeHookInternal(act, stack[:0], 0, opts.MaxDepth, hooks)
	return err
}

func applyTreeHookInternal(act AnyAction, visited []AnyAction, depth, maxDepth int, hooks []AnyHook) ([]AnyAction, error) {
	if act == nil {
		return visited, nil
	}

	if depth > maxDepth {
		return visited, fmt.Errorf("action: tree hook max depth exceeded (%d)", maxDepth)
	}

	for i := 0; i < len(visited); i++ {
		if visited[i] == act {
			return visited, nil
		}
	}

	visited = append(visited, act)

	if proxy, ok := act.(*Proxy); ok {
		if active := proxy.Current(); active != nil {
			return applyTreeHookInternal(active, visited, depth+1, maxDepth, hooks)
		}
		return visited, nil
	}

	act.AddAnyHook(hooks...)

	if comp, ok := act.(Composite); ok {
		children := comp.Children()
		for i := 0; i < len(children); i++ {
			var err error
			visited, err = applyTreeHookInternal(children[i], visited, depth+1, maxDepth, hooks)
			if err != nil {
				return visited, err
			}
		}
	}
	return visited, nil
}
