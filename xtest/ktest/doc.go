// Package ktest provides high-level test helpers for the kernel action runtime.
//
// Mirrors xtest:
//
//	Require*  Fatal. Calls tb.Fatalf and stops the current test (or subtest).
//	Assert*   Soft. Calls tb.Errorf and continues. Only AssertContracts uses
//	          this form today (reports every structural problem in one run).
//	Run(...)  Fluent chain. Every terminal method is fatal.
//
// Parameters of type testing.TB are named tb (not t).
package ktest
