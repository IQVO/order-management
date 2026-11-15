package main_test

// Step definitions of features/mutation_closeout.feature: the few steps the
// documentation-derived vocabulary lacked to pin the survivors of the first
// mutation pass (a WorkUnitCreated without an id, a sweep with a small batch).

import (
	"context"
	"errors"
	"fmt"

	"github.com/cucumber/godog"

	"github.com/claudioed/order-management/internal/application/usecases"
)

func (w *world) workUnitCreatedWithoutID(lineNo int) error {
	_, err := w.rules.confirm.Execute(context.Background(), usecases.ConfirmReleaseRequest{
		EventID: "", Reference: w.lastOrderID, LineNo: lineNo,
	})
	w.rules.lastClassErr = err
	return nil
}

func (w *world) confirmationIsRefusedAsInvalid() error {
	if !errors.Is(w.rules.lastClassErr, usecases.ErrInvalidReleaseConfirmation) {
		return fmt.Errorf("expected ErrInvalidReleaseConfirmation, got %v", w.rules.lastClassErr)
	}
	return nil
}

func (w *world) sweepRunsInBatches(minutes, batch int) error {
	w.rules.flag.BatchSize = batch
	return w.sweepRuns(minutes)
}

func (w *world) heldOrderWithShipBy(skus, shipBy string) error {
	return w.postAndCapture(map[string]any{"lines": simpleLines(skus), "requiredShipBy": shipBy, "releaseOnAllocation": false})
}

// registerCloseoutSteps registers the closeout vocabulary on sc.
func registerCloseoutSteps(sc *godog.ScenarioContext, w *world) {
	sc.Step(`^wes-work-planning publishes a WorkUnitCreated without a CloudEvents id for line (\d+) of the order$`, w.workUnitCreatedWithoutID)
	sc.Step(`^an order is placed on hold for SKUs "([^"]*)" with required ship-by "([^"]*)"$`, w.heldOrderWithShipBy)
	sc.Step(`^the confirmation is refused as invalid$`, w.confirmationIsRefusedAsInvalid)
	sc.Step(`^the housekeeping sweep runs with a confirmation timeout of (\d+) minutes in batches of (\d+) lines$`, w.sweepRunsInBatches)
}
