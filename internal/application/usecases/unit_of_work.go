package usecases

import (
	"context"

	"github.com/claudioed/order-management/internal/application/ports"
)

// atomically runs fn inside uow when one is wired, or directly otherwise.
// Keeping this in one place means every use case treats a nil UnitOfWork
// identically instead of each re-deciding the fallback (mirrors
// process-path-management ADR 0003's identical helper).
func atomically(ctx context.Context, uow ports.UnitOfWork, fn func(ctx context.Context) error) error {
	if uow == nil {
		return fn(ctx)
	}
	return uow.Execute(ctx, fn)
}
