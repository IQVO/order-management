package main

import (
	"testing"

	"github.com/claudioed/order-management/internal/application/usecases"
)

// Unset, the projection is the zero value: every use case carrying it
// behaves exactly as before the projection existed (the additive-off
// switch, mirroring ADR 0031's consumer-group gate).
func TestBuildDemandProjection_DisabledWithoutASite(t *testing.T) {
	t.Setenv(demandProjectionSiteEnv, "")
	p := buildDemandProjection(quietLogger())
	if p.SiteID != "" || p.AssignmentVersion != "" {
		t.Fatalf("buildDemandProjection = %+v, want the disabled zero value", p)
	}
}

func TestBuildDemandProjection_ConfigurationFromTheEnvironment(t *testing.T) {
	t.Setenv(demandProjectionSiteEnv, "SIM1")
	p := buildDemandProjection(quietLogger())
	if p.SiteID != "SIM1" {
		t.Fatalf("SiteID = %q, want SIM1", p.SiteID)
	}
	if p.AssignmentVersion != usecases.StaticDemandAssignmentVersion {
		t.Fatalf("AssignmentVersion = %q, want %q", p.AssignmentVersion, usecases.StaticDemandAssignmentVersion)
	}
}
