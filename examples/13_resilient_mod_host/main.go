package main

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/observe"
	"github.com/nexssp/kernel/xctx"
	"github.com/nexssp/kernel/xerr"
)

// PlayerState represents the in-memory core entity protected by the host.
type PlayerState struct {
	ID        string
	Gold      int
	Inventory []string
}

// EnchantRequest defines the typed transaction boundary passed to plugins.
type EnchantRequest struct {
	Player  *PlayerState
	Cost    int
	Item    string
	Enchant string
}

// EnchantResult represents the outcome delivered back to the game loop.
type EnchantResult struct {
	Success bool
	Buff    string
}

func main() {
	ctx := xctx.WithExecutionID(context.Background(), "game-tick-9941")

	// 1. Host observability sink to capture mod crashes and rollbacks
	sink := observe.NewMemorySink(50)
	lifecycleHook := observe.Hook(sink)

	player := &PlayerState{
		ID:        "player-hero-1",
		Gold:      1000,
		Inventory: []string{"Excalibur"},
	}

	fmt.Println("🎮 [Host] Game Server Initialized. Starting Player State:")
	fmt.Printf("   Player: %s | Gold: %d | Inventory: %v\n", player.ID, player.Gold, player.Inventory)
	fmt.Println(strings.Repeat("─", 75))

	// 2. Mod v1.0 (Buggy): Simulates an untrusted community plugin with a fatal crash
	modV1Action := action.New("mod.weapon_enchant.v1", func(_ context.Context, req EnchantRequest) (EnchantResult, error) {
		fmt.Printf("📦 [Plugin v1.0] Processing enchantment %q on item %q...\n", req.Enchant, req.Item)
		// Simulate severe bug: unhandled panic inside plugin code
		panic("nil pointer dereference in community_mod_v1.0.so: missing shader definition")
	}).AnyHook(lifecycleHook).Build()

	// 3. Mount plugin inside an atomic hot-swappable proxy
	modProxy := action.NewProxy(modV1Action)

	// 4. Host Transaction Engine: Coordinates state mutations using Saga compensation
	enchantWorkflow := action.NewSaga[EnchantRequest, EnchantResult]("enchantment_pipeline").
		AddStep(
			"reserve_gold",
			func(_ context.Context, req EnchantRequest) (EnchantResult, error) {
				if req.Player.Gold < req.Cost {
					return EnchantResult{}, xerr.Forbidden("insufficient gold")
				}
				req.Player.Gold -= req.Cost
				fmt.Printf("💰 [Host] Deducted %d gold. Remaining: %d\n", req.Cost, req.Player.Gold)
				return EnchantResult{Success: true}, nil
			},
			func(_ context.Context, req EnchantRequest) error {
				// LIFO Rollback: Refund player gold if any subsequent step fails
				req.Player.Gold += req.Cost
				fmt.Printf("🔄 [Host/Undo] Rollback triggered! Refunded %d gold. Current balance: %d\n", req.Cost, req.Player.Gold)
				return nil
			},
		).
		AddStep(
			"apply_mod_effects",
			func(stepCtx context.Context, req EnchantRequest) (EnchantResult, error) {
				// Invoke active mod through the proxy boundary
				resAny, err := modProxy.DoAny(stepCtx, req)
				if err != nil {
					return EnchantResult{}, err
				}
				result, ok := resAny.(EnchantResult)
				if !ok {
					return EnchantResult{}, errors.New("invalid plugin response type")
				}
				return result, nil
			},
			nil, // No undo needed if this step fails; previous steps will compensate
		).
		Build()

	// --- PHASE 1: Execution with faulty Mod v1.0 ---
	fmt.Println("\n⚡ [Action] Player attempting to cast 'Dragonfire' using Plugin v1.0...")
	castRequest := EnchantRequest{
		Player:  player,
		Cost:    350,
		Item:    "Excalibur",
		Enchant: "Dragonfire",
	}

	result, err := enchantWorkflow.Do(ctx, castRequest)
	if err != nil {
		fmt.Printf("🛡️  [Host Panic Guard] Mod crashed safely caught: %v\n", err)
		fmt.Printf("   Saga RolledBack: %t | Player Gold: %d (Zero Loss Guaranteed)\n",
			result.RolledBack, player.Gold)
	}

	// --- PHASE 2: Zero-Downtime Hot-Swap ---
	fmt.Println("\n🔧 [Host] Administrator deployed patch: Hot-swapping to Plugin v2.0 (Live, 0ms)...")

	modV2Action := action.New("mod.weapon_enchant.v2", func(_ context.Context, req EnchantRequest) (EnchantResult, error) {
		fmt.Printf("✨ [Plugin v2.0] Successfully bound %q to %q!\n", req.Enchant, req.Item)
		req.Player.Inventory[0] = fmt.Sprintf("%s + %s", req.Item, req.Enchant)
		return EnchantResult{
			Success: true,
			Buff:    "+50 Fire Damage",
		}, nil
	}).AnyHook(lifecycleHook).Build()

	// Atomic lock-free swap without taking server down
	modProxy.Swap(modV2Action)

	// --- PHASE 3: Re-execution on Live Host ---
	fmt.Println("\n⚡ [Action] Player re-attempting enchantment on patched server...")
	resultV2, errV2 := enchantWorkflow.Do(ctx, castRequest)
	if errV2 != nil {
		panic(fmt.Sprintf("unexpected error on v2: %v", errV2))
	}

	fmt.Printf("✅ [Success] Enchantment complete!\n")
	fmt.Printf("   Buff Granted: %s\n", resultV2.Output.Buff)
	fmt.Printf("   Final Player State -> Gold: %d | Inventory: %v\n", player.Gold, player.Inventory)

	// --- PHASE 4: Telemetry verification ---
	fmt.Println("\n📊 [Telemetry Events Captured by Kernel Ring]:")
	events := sink.Events()
	for i := range events {
		ev := &events[i]
		fmt.Printf("   - [%-8s] Action: %-22s Error: %v\n", ev.Kind, ev.Action, ev.Error)
	}
}
