// Package action provides typed, composable application actions.
//
// Build an action with New, configure it with fluent policies and middleware,
// then reuse the resulting BuiltAction concurrently. The typed Do method is
// the preferred execution path; decoded execution is intended for adapters.
package action
