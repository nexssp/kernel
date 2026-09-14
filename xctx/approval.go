package xctx

import "context"

// ApprovalTokenKey is the single, framework-wide context key for the
// approval token. Every consumer — flow, agent/permission, agent/policy
// reads and writes through this one key so a token
// minted by one package is visible to all others.
var ApprovalTokenKey = NewKey[string]("nexss.approval.token")

// WithApprovalToken binds an approval token to ctx.
func WithApprovalToken(ctx context.Context, token string) context.Context {
	return ApprovalTokenKey.With(ctx, token)
}

// ApprovalTokenFrom extracts the approval token, or "".
func ApprovalTokenFrom(ctx context.Context) string {
	t, _ := ApprovalTokenKey.From(ctx)
	return t
}
