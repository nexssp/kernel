package dagstate_test

import (
	"fmt"
	"maps"
	"runtime"
	"testing"

	"github.com/arnonrgo/immutable"
)

var (
	stateLengthSink    int
	snapshotLengthSink int
)

func BenchmarkStatePublication(b *testing.B) {
	for _, implementation := range []string{"map", "cow", "hamt"} {
		for _, initialSize := range []int{0, 128, 1024} {
			for _, outputCount := range []int{16, 64, 256} {
				b.Run(fmt.Sprintf("%s/initial=%d/outputs=%d", implementation, initialSize, outputCount), func(b *testing.B) {
					initialMap := make(map[string]any, 16)
					initialTree := immutable.NewMap[string, any](nil)
					for index := range initialSize {
						key := fmt.Sprintf("seed-%04d", index)
						initialMap[key] = index
						initialTree = initialTree.Set(key, index)
					}
					outputKeys := make([]string, outputCount)
					for index := range outputCount {
						outputKeys[index] = fmt.Sprintf("tasks.node-%03d.output", index)
					}
					mapViews := make([]map[string]any, outputCount)
					treeViews := make([]*immutable.Map[string, any], outputCount)

					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						switch implementation {
						case "map":
							state := cloneStateMap(initialMap)
							for index, key := range outputKeys {
								mapViews[index] = state
								state[key] = index
							}
							stateLengthSink = len(state)
							snapshotLengthSink = len(mapViews[0])
							runtime.KeepAlive(mapViews)
						case "cow":
							state := cloneStateMap(initialMap)
							for index, key := range outputKeys {
								mapViews[index] = state
								state = cloneStateMap(state)
								state[key] = index
							}
							stateLengthSink = len(state)
							snapshotLengthSink = len(mapViews[0])
							runtime.KeepAlive(mapViews)
						case "hamt":
							state := initialTree
							for index, key := range outputKeys {
								treeViews[index] = state
								state = state.Set(key, index)
							}
							stateLengthSink = state.Len()
							snapshotLengthSink = treeViews[0].Len()
							runtime.KeepAlive(treeViews)
						default:
							b.Fatalf("unknown implementation %q", implementation)
						}
					}
				})
			}
		}
	}
}

func cloneStateMap(source map[string]any) map[string]any {
	destination := make(map[string]any, 16)
	maps.Copy(destination, source)
	return destination
}

func TestPublicationSnapshotBehavior(t *testing.T) {
	mapState := cloneStateMap(map[string]any{"seed": "input"})
	mapView := mapState
	mapState["output"] = "done"
	if _, ok := mapView["output"]; !ok {
		t.Fatal("baseline map view did not observe the in-place mutation")
	}

	cowState := cloneStateMap(map[string]any{"seed": "input"})
	cowView := cowState
	cowState = cloneStateMap(cowState)
	cowState["output"] = "done"
	if _, ok := cowView["output"]; ok {
		t.Fatal("COW view observed a later output")
	}

	hamtState := immutable.NewMap[string, any](nil).Set("seed", "input")
	hamtView := hamtState
	hamtState = hamtState.Set("output", "done")
	if value, ok := hamtState.Get("output"); !ok || value != "done" {
		t.Fatal("updated HAMT root did not retain the new output")
	}
	if _, ok := hamtView.Get("output"); ok {
		t.Fatal("HAMT view observed a later output")
	}
}
