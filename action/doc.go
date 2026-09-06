// Copyright 2018-2026 Marcin Polak. All rights reserved.
// Use of this source code is governed by an Apache-2.0 license
// that can be found in the LICENSE file.
//
// Package action provides typed, composable application actions.
//
// Build an action with New, configure it with fluent policies and middleware,
// then reuse the resulting BuiltAction concurrently. The typed Do method is
// the preferred execution path; decoded execution is intended for adapters.
package action
