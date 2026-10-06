package xctx

import "context"

type Progress struct {
	Current  int64          `json:"current"`
	Total    int64          `json:"total,omitempty"`
	Unit     string         `json:"unit,omitempty"`
	Message  string         `json:"message,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

type ProgressReporter func(Progress)

var progressKey = NewKey[ProgressReporter]("kernel.progress")

func WithProgressReporter(ctx context.Context, reporter ProgressReporter) context.Context {
	if reporter == nil {
		return ctx
	}
	return progressKey.With(ctx, reporter)
}

func ReportProgress(ctx context.Context, progress Progress) {
	if reporter, ok := progressKey.From(ctx); ok && reporter != nil {
		reporter(progress)
	}
}
