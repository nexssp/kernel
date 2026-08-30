package dag_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/ai/dag"
)

func TestDAG_ContextCancellation_WaitgroupSafety(t *testing.T) {
	t.Parallel()

	// Węzeł 1: Opóźniony. Uruchamia się, ale celowo śpi, by sprowokować wyścig.
	slowNode := action.New("slow_node", func(ctx context.Context, nCtx *dag.NodeContext) (string, error) {
		// Śpimy ignorując na chwilę kontekst, aby sprawdzić odporność struktury
		time.Sleep(100 * time.Millisecond)

		// Próbujemy odczytać ze stanu wejściowego.
		// BŁĄD ARCHITEKTONICZNY: Jeśli nadrzędny 'errgroup' już zwrócił błąd/anulowanie
		// i główna pętla wykonała 'Release()' na tym obiekcie 'State', czytamy zwolnioną pamięć.
		_, _ = nCtx.Input.Get("jakikolwiek_klucz")

		return "done", nil
	}).Build()

	// Węzeł 2: Natychmiastowo rzuca błąd
	fastFailNode := action.New("fail_node", func(ctx context.Context, nCtx *dag.NodeContext) (string, error) {
		return "", errors.New("natychmiastowy błąd")
	}).Build()

	// Kompilujemy DAG, oba węzły są w tej samej warstwie (brak zależności).
	cdag, err := dag.New("race_dag").
		AddNode("slow", "out_slow", slowNode).
		AddNode("fast", "out_fast", fastFailNode).
		Compile()
	if err != nil {
		t.Fatalf("failed to compile DAG: %v", err)
	}

	initialState := dag.AcquireState()
	initialState.Set("jakikolwiek_klucz", "test")
	defer initialState.Release()

	// Uruchomienie. Węzeł 'fast' natychmiast zwróci błąd, co przerwie 'errgroup.Wait()'
	// i funkcja DAG.Execute się zakończy zwalniając initialState!
	_, err = cdag.Execute(context.Background(), initialState)
	if err == nil {
		t.Fatal("Oczekiwano błędu od fail_node")
	}

	// Czekamy chwilę, aby opóźniona gorutyna 'slow_node' zdążyła spróbować
	// odczytać z nCtx.Input (który już trafił do sync.Pool).
	time.Sleep(200 * time.Millisecond)

	// Jeśli odpalisz test z `-race`, kompilator Go zgłosi "DATA RACE" w tym miejscu,
	// udowadniając lukę Use-After-Free.
}
