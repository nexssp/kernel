package main

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
	"github.com/nexssp/kernel/xtest/ktest"
)

func TestModHost_CrashIsolationAndCompensation(t *testing.T) {
	t.Parallel()

	player := &PlayerState{ID: "test-user", Gold: 500, Inventory: []string{"Dagger"}}

	crashingMod := action.New("mod.crash", func(_ context.Context, _ EnchantRequest) (EnchantResult, error) {
		panic("fatal plugin memory corruption")
	}).Build()

	proxy := action.NewProxy(crashingMod)

	saga := action.NewSaga[EnchantRequest, EnchantResult]("test.enchant").
		AddStep(
			"gold_deduct",
			func(_ context.Context, req EnchantRequest) (EnchantResult, error) {
				req.Player.Gold -= req.Cost
				return EnchantResult{Success: true}, nil
			},
			func(_ context.Context, req EnchantRequest) error {
				req.Player.Gold += req.Cost
				return nil
			},
		).
		AddStep(
			"plugin_execution",
			func(ctx context.Context, req EnchantRequest) (EnchantResult, error) {
				resAny, err := proxy.DoAny(ctx, req)
				if err != nil {
					return EnchantResult{}, err
				}
				res, ok := resAny.(EnchantResult)
				if !ok {
					return EnchantResult{}, xerr.Internal("type mismatch")
				}
				return res, nil
			},
			nil,
		).
		Build()

	req := EnchantRequest{Player: player, Cost: 200, Item: "Dagger", Enchant: "Ice"}

	// 1. Verify crash is isolated as an error and does not panic the test runner
	result, err := saga.Do(context.Background(), req)

	ktest.RequireCondition(t, err != nil, "expected error from recovered panic")
	ktest.RequireCondition(t, result.RolledBack, "expected saga to roll back")
	ktest.RequireEqual(t, player.Gold, 500) // Gold must be refunded

	// 2. Hot-swap plugin to a healthy implementation
	healthyMod := action.New("mod.healthy", func(_ context.Context, req EnchantRequest) (EnchantResult, error) {
		req.Player.Inventory[0] = "Dagger + Ice"
		return EnchantResult{Success: true, Buff: "Chilled"}, nil
	}).Build()

	proxy.Swap(healthyMod)

	// 3. Execute again without restarting
	result2, err2 := saga.Do(context.Background(), req)
	ktest.RequireNoError(t, err2)
	ktest.RequireEqual(t, result2.Success, true)
	ktest.RequireEqual(t, player.Gold, 300)
	ktest.RequireEqual(t, player.Inventory[0], "Dagger + Ice")
}
