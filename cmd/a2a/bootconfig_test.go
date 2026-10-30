package main

import (
	"net"
	"strings"
	"testing"
	"time"
)

const testAgentURL = "http://order-management-a2a.warehouse-systems.svc.cluster.local:8093/"

func setBootEnv(t *testing.T) {
	t.Helper()
	t.Setenv("DATABASE_URL", "postgres://u:p@127.0.0.1:1/db?sslmode=disable")
	t.Setenv("ANALYTICS_DATABASE_URL", "postgres://r:p@127.0.0.1:1/analytics?sslmode=disable")
	t.Setenv("A2A_AGENT_URL", testAgentURL)
}

func TestLoadBootConfigAcceptsACompleteEnvironment(t *testing.T) {
	setBootEnv(t)
	cfg, err := loadBootConfig()
	if err != nil || cfg.databaseURL == "" || cfg.analyticsURL == "" || cfg.agentURL != testAgentURL {
		t.Fatalf("config = %+v, %v", cfg, err)
	}
	// The agent URL is trimmed, not rejected for stray whitespace.
	t.Setenv("A2A_AGENT_URL", "  "+testAgentURL+" ")
	if cfg, err = loadBootConfig(); err != nil || cfg.agentURL != testAgentURL {
		t.Fatalf("a padded A2A_AGENT_URL must be trimmed, got %+v, %v", cfg, err)
	}
}

// Each missing or rejected value is a boot error that names its variable.
func TestLoadBootConfigRefusesEachMissingOrRejectedValue(t *testing.T) {
	cases := []struct {
		name, variable, value, want string
	}{
		{"database unset", "DATABASE_URL", "", "DATABASE_URL is required: this agent reads orders from real Postgres"},
		{"analytics unset", "ANALYTICS_DATABASE_URL", "", "ANALYTICS_DATABASE_URL is required: promise_health reads the analytics data product"},
		{"agent url unset", "A2A_AGENT_URL", "", "A2A_AGENT_URL is required: the Agent Card must advertise"},
		{"agent url blank", "A2A_AGENT_URL", "   ", "A2A_AGENT_URL is required"},
		{"agent url relative", "A2A_AGENT_URL", "/a2a", `A2A_AGENT_URL="/a2a" is not an absolute http(s) URL`},
		{"agent url schemeless", "A2A_AGENT_URL", "order-management-a2a:8093", "is not an absolute http(s) URL"},
		{"agent url wrong scheme", "A2A_AGENT_URL", "ftp://host/", "is not an absolute http(s) URL"},
		{"agent url no host", "A2A_AGENT_URL", "http:///a2a", "is not an absolute http(s) URL"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setBootEnv(t)
			t.Setenv(tc.variable, tc.value)
			_, err := loadBootConfig()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("%s=%q: %v, want a boot error containing %q", tc.variable, tc.value, err, tc.want)
			}
			if !strings.Contains(err.Error(), tc.variable) {
				t.Fatalf("the error does not name %s: %v", tc.variable, err)
			}
		})
	}
}

// Every problem is reported at once, so an operator fixes the deployment in one
// pass instead of one variable per restart.
func TestLoadBootConfigReportsEveryProblemAtOnce(t *testing.T) {
	for _, v := range []string{"DATABASE_URL", "ANALYTICS_DATABASE_URL", "A2A_AGENT_URL"} {
		t.Setenv(v, "")
	}
	_, err := loadBootConfig()
	if err == nil {
		t.Fatal("an empty environment must be refused")
	}
	for _, v := range []string{"DATABASE_URL is required", "ANALYTICS_DATABASE_URL is required", "A2A_AGENT_URL is required"} {
		if !strings.Contains(err.Error(), v) {
			t.Errorf("the error does not report %q: %v", v, err)
		}
	}
}

// run fails fast on a bad configuration: before telemetry, any connection and
// before the listener binds.
func TestRunFailsFastBeforeBindingTheListener(t *testing.T) {
	setBootEnv(t)
	t.Setenv("ANALYTICS_DATABASE_URL", "")
	l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	t.Setenv("A2A_ADDR", addr)

	start := time.Now()
	if err := run(); err == nil || !strings.Contains(err.Error(), "ANALYTICS_DATABASE_URL is required") {
		t.Fatalf("run = %v, want a boot error naming ANALYTICS_DATABASE_URL", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("a configuration error must not wait on any dependency")
	}
	if c, derr := (&net.Dialer{Timeout: 200 * time.Millisecond}).DialContext(t.Context(), "tcp", addr); derr == nil {
		_ = c.Close()
		t.Fatal("the listener was bound although the configuration is invalid")
	}
}
