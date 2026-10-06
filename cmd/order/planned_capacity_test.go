package main

import (
	"testing"
	"time"

	inboundhttp "github.com/claudioed/order-management/internal/adapters/inbound/http"
	"github.com/claudioed/order-management/internal/adapters/outbound/memory"
)

// With the consumer-group env unset the whole integration is absent: this is
// the "behaves exactly as before" switch of ADR 0031.
func TestBuildPlannedCapacity_DisabledWithoutAConsumerGroup(t *testing.T) {
	t.Setenv(plannedCapacityGroupEnv, "")
	if pc := buildPlannedCapacity(nil, quietLogger()); pc != nil {
		t.Fatalf("buildPlannedCapacity = %+v, want nil when %s is unset", pc, plannedCapacityGroupEnv)
	}
}

func TestBuildPlannedCapacity_ConfigurationFromTheEnvironment(t *testing.T) {
	tests := []struct {
		name         string
		defaultSite  string
		plannedSite  string
		wantSite     string
		wantGroupEnv string
	}{
		{"falls back to the built-in default site", "", "", DefaultSiteId, "om-planned-capacity-test"},
		{"falls back to DEFAULT_SITE_ID, the site the promise is computed for", "SIM7", "", "SIM7", "om-planned-capacity-test"},
		{"PLANNED_CAPACITY_SITE_ID wins", "SIM7", "SIM1", "SIM1", "om-planned-capacity-test"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(plannedCapacityGroupEnv, tt.wantGroupEnv)
			t.Setenv("DEFAULT_SITE_ID", tt.defaultSite)
			t.Setenv(plannedCapacitySiteEnv, tt.plannedSite)

			pc := buildPlannedCapacity(nil, quietLogger())
			if pc == nil {
				t.Fatal("buildPlannedCapacity = nil, want a wired read model")
			}
			if pc.siteID != tt.wantSite {
				t.Fatalf("siteID = %q, want %q", pc.siteID, tt.wantSite)
			}
			if pc.groupID != tt.wantGroupEnv {
				t.Fatalf("groupID = %q, want the env value verbatim", pc.groupID)
			}
		})
	}
}

// No DATABASE_URL: in-memory adapters and, importantly, a NIL interface UoW
// (not an interface holding a nil *postgres.UnitOfWork).
func TestBuildPlannedCapacity_NoDatabaseUsesMemoryAndNoUnitOfWork(t *testing.T) {
	t.Setenv(plannedCapacityGroupEnv, "om-planned-capacity-test")
	pc := buildPlannedCapacity(nil, quietLogger())
	if _, ok := pc.windows.(*memory.PlannedCapacityRepo); !ok {
		t.Fatalf("windows = %T, want the in-memory repo", pc.windows)
	}
	if _, ok := pc.processed.(*memory.PlannedCapacityProcessedEventsRepo); !ok {
		t.Fatalf("processed = %T, want the in-memory repo", pc.processed)
	}
	if pc.uow != nil {
		t.Fatalf("uow = %v, want a true nil interface", pc.uow)
	}
}

func TestPlannedCapacityAttach_WiresTheServer(t *testing.T) {
	t.Setenv(plannedCapacityGroupEnv, "om-planned-capacity-test")
	t.Setenv(plannedCapacitySiteEnv, "SIM1")
	pc := buildPlannedCapacity(nil, quietLogger())

	server := &inboundhttp.Server{}
	pc.attach(server, memory.NewFixedClock(time.Date(2026, 10, 4, 21, 15, 30, 0, time.UTC)))
	if server.CapacityConstraints == nil || server.CapacityConstraints.SiteID != "SIM1" {
		t.Fatalf("CapacityConstraints = %+v, want site SIM1", server.CapacityConstraints)
	}
	if server.PlannedCapacity == nil {
		t.Fatal("PlannedCapacity must be wired")
	}
}

func TestPlannedCapacityStartConsumer_NoBrokersIsAHarmlessNoOp(t *testing.T) {
	t.Setenv(plannedCapacityGroupEnv, "om-planned-capacity-test")
	t.Setenv("KAFKA_BROKERS", "")
	stop := buildPlannedCapacity(nil, quietLogger()).startConsumer(quietLogger())
	if stop == nil {
		t.Fatal("startConsumer must always return a stop function")
	}
	stop() // must not panic or block
}
