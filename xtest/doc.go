// Package xtest provides fatal-by-default test helpers.
//
// Naming convention:
//
//	Require*  Fatal. Calls tb.Fatalf and stops the current test (or subtest).
//	          The default. Use when continuing past a failure would test
//	          nothing real (failed setup, nil response, blown budget).
//
//	Assert*   Soft. Calls tb.Errorf and continues, so one run reports every
//	          violation. Combine with t.Run subtests. Use only when the
//	          checks are genuinely independent.
//
//	Eventually / Never / WaitFor*   Polling helpers; fail on timeout.
//	Golden*                         Golden-file helpers.
//
// Parameters of type testing.TB are named tb (not t).
package xtest
