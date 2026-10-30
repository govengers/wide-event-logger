package wideevent

import "context"

type contextKey struct{}

// NewContext returns a copy of ctx that carries we.
func NewContext(ctx context.Context, we *Event) context.Context {
	return context.WithValue(ctx, contextKey{}, we)
}

// FromContext returns the event stored in ctx by NewContext, or nil outside a unit of work. The methods
// of a nil *Event do nothing, so the result can be used without a nil check.
func FromContext(ctx context.Context) *Event {
	we, _ := ctx.Value(contextKey{}).(*Event)
	return we
}
