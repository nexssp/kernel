// file: kernel/action/resources.go
package action

import "sync"

// actionResources guarantees that lifecycle cleanups run exactly once,
// even when an action is cloned multiple times for different registries.
type actionResources struct {
	cleanups  []func()
	closeOnce sync.Once
}

func newActionResources(cleanups []func()) *actionResources {
	if len(cleanups) == 0 {
		return nil
	}
	return &actionResources{
		cleanups: append([]func(){}, cleanups...),
	}
}

func (resources *actionResources) Close() {
	if resources == nil {
		return
	}
	resources.closeOnce.Do(func() {
		for _, cleanup := range resources.cleanups {
			if cleanup != nil {
				cleanup()
			}
		}
	})
}
