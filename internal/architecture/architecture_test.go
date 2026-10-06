// Package architecture contains executable architecture fitness tests (the
// Go equivalent of ArchUnit, via github.com/arch-go/arch-go) that enforce
// this service's hexagonal dependency rule: dependencies point inward only,
// domain is at the center, and only cmd/ wires every layer together. It
// additionally enforces this repo's own ADR-0006 rule that the analytics
// read side depends on nothing internal.
package architecture

import (
	"testing"

	archgo "github.com/arch-go/arch-go/api"
	"github.com/arch-go/arch-go/api/configuration"
)

// modulePath is this repo's Go module path, from go.mod.
const modulePath = "github.com/claudioed/order-management"

// Package glob patterns for arch-go, matching this repo's real layout:
// internal/domain/*, internal/application/{ports,usecases},
// internal/adapters/{inbound,outbound}/*, internal/analytics/*.
const (
	domainPackages       = "**.internal.domain.**"
	applicationPackages  = "**.internal.application.**"
	inboundPackages      = "**.internal.adapters.inbound.**"
	outboundPackages     = "**.internal.adapters.outbound.**"
	analyticsPackages    = "**.internal.analytics.**"
	allInternalPackages  = "**.internal.**"
	cmdPackages          = "**.cmd.**"
	applicationPortsPkgs = "**.internal.application.ports.**"
)

// runDependenciesRule executes a single arch-go dependencies rule and fails
// the test with the offending package(s)/detail(s) if it doesn't pass.
func runDependenciesRule(t *testing.T, rule *configuration.DependenciesRule) {
	t.Helper()

	moduleInfo := configuration.Load(modulePath)
	cfg := configuration.Config{
		DependenciesRules: []*configuration.DependenciesRule{rule},
	}

	result := archgo.CheckArchitecture(moduleInfo, cfg)

	if result.DependenciesRuleResult != nil && result.DependenciesRuleResult.Passes {
		return
	}

	for _, ruleResult := range result.DependenciesRuleResult.Results {
		for _, v := range ruleResult.Verifications {
			if v.Passes {
				continue
			}

			for _, d := range v.Details {
				t.Errorf("%s", archViolation("dependency", "package "+v.Package, d))
			}
		}
	}

	t.FailNow()
}

// TestHexagonalDependencyRule encodes the strict dependency rule from
// CLAUDE.md: domain depends on nothing, application depends only on domain,
// inbound and outbound adapters never depend on each other, and only cmd/
// is allowed to wire together every layer.
func TestHexagonalDependencyRule(t *testing.T) {
	t.Run("domain has no internal dependencies except domain", func(t *testing.T) {
		runDependenciesRule(t, &configuration.DependenciesRule{
			Package: domainPackages,
			ShouldOnlyDependsOn: &configuration.Dependencies{
				Internal: []string{domainPackages},
			},
		})
	})

	t.Run("application depends only on domain", func(t *testing.T) {
		runDependenciesRule(t, &configuration.DependenciesRule{
			Package: applicationPackages,
			ShouldOnlyDependsOn: &configuration.Dependencies{
				Internal: []string{domainPackages, applicationPackages},
			},
		})
	})

	t.Run("inbound adapters do not depend on outbound adapters", func(t *testing.T) {
		runDependenciesRule(t, &configuration.DependenciesRule{
			Package: outboundPackages,
			ShouldNotDependsOn: &configuration.Dependencies{
				Internal: []string{inboundPackages},
			},
		})
	})

	t.Run("outbound adapters do not depend on inbound adapters", func(t *testing.T) {
		runDependenciesRule(t, &configuration.DependenciesRule{
			Package: inboundPackages,
			ShouldNotDependsOn: &configuration.Dependencies{
				Internal: []string{outboundPackages},
			},
		})
	})

	t.Run("only cmd wires every layer", func(t *testing.T) {
		runDependenciesRule(t, &configuration.DependenciesRule{
			Package: allInternalPackages,
			ShouldNotDependsOn: &configuration.Dependencies{
				Internal: []string{cmdPackages},
			},
		})
	})
}

// TestAnalyticsIsolation encodes ADR-0006: internal/analytics is an
// additive read side that must never depend on this service's OLTP layers
// (domain, application, or adapters) — the analytics data product stays a
// separate bounded surface fed by events, not a package reaching into the
// operational core.
func TestAnalyticsIsolation(t *testing.T) {
	t.Run("analytics depends on nothing internal except itself", func(t *testing.T) {
		runDependenciesRule(t, &configuration.DependenciesRule{
			Package: analyticsPackages,
			ShouldOnlyDependsOn: &configuration.Dependencies{
				Internal: []string{analyticsPackages},
			},
		})
	})
}

// TestPortsAreCustomerOwned encodes ADR-0002's direction rule at the port
// level: internal/application/ports expresses cross-context calls in THIS
// context's own types, so the ports package must never import an adapter
// package (which would mean a Supplier's implementation detail leaked into
// the Customer's contract) nor cmd/ (which would invert the wiring
// direction).
//
// NOTE: unlike wes-work-planning's interfaces-only contents rule, this repo
// deliberately keeps request/result structs (ReservationRequest,
// ReservationResult) in the ports package — the Customer/Supplier decision
// in ADR-0002 calls for own-type DTOs, so "interfaces only" would be false
// against the actual design. The dependencies rule below is the honest
// equivalent.
func TestPortsAreCustomerOwned(t *testing.T) {
	t.Run("ports never depend on adapters or cmd", func(t *testing.T) {
		runDependenciesRule(t, &configuration.DependenciesRule{
			Package: applicationPortsPkgs,
			ShouldNotDependsOn: &configuration.Dependencies{
				Internal: []string{inboundPackages, outboundPackages, cmdPackages},
			},
		})
	})
}
