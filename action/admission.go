package action

import "context"

// Admission controls whether an execution may enter a protected section.
// Implementations may be local or distributed. Acquire must return an error
// without retaining the request when admission is denied.
type Admission interface {
	Acquire(context.Context) error
	Release()
}

// AdmissionMiddleware applies an external admission policy around execution.
// It keeps the policy outside Builder while allowing typed middleware
// composition and automatic request/response inference.
func AdmissionMiddleware[Req, Res any](admission Admission) DispatcherMiddleware[Req, Res] {
	return func(next Fn[Req, Res], _ HookDispatcher[Req, Res]) Fn[Req, Res] {
		return func(ctx context.Context, req Req) (res Res, err error) {
			if err := admission.Acquire(ctx); err != nil {
				return res, err
			}
			defer admission.Release()
			return next(ctx, req)
		}
	}
}
