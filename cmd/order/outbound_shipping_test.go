package main

import "testing"

func osEnv(adjust func(*outboundShippingEnv)) outboundShippingEnv {
	e := outboundShippingEnv{
		mode:        "kafka",
		groupID:     "order-management-outbound-shipping",
		brokers:     "localhost:9092",
		databaseURL: "postgres://om:om@localhost/om",
	}
	if adjust != nil {
		adjust(&e)
	}
	return e
}

func TestParseOutboundShippingConfigModes(t *testing.T) {
	tests := []struct {
		name    string
		mode    string
		wantOn  bool
		wantErr bool
	}{
		{name: "empty is off", mode: "", wantOn: false},
		{name: "off is off", mode: "off", wantOn: false},
		{name: "kafka is on", mode: "kafka", wantOn: true},
		{name: "Kafka trimmed is on", mode: " Kafka ", wantOn: true},
		{name: "unknown is rejected", mode: "pulsar", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := parseOutboundShippingConfig(osEnv(func(e *outboundShippingEnv) { e.mode = tt.mode }))
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if cfg.enabled() != tt.wantOn {
				t.Fatalf("enabled = %v, want %v", cfg.enabled(), tt.wantOn)
			}
		})
	}
}

func TestParseOutboundShippingConfigKafkaPrerequisites(t *testing.T) {
	tests := []struct {
		name   string
		adjust func(*outboundShippingEnv)
	}{
		{name: "no consumer group", adjust: func(e *outboundShippingEnv) { e.groupID = "" }},
		{name: "no brokers", adjust: func(e *outboundShippingEnv) { e.brokers = "" }},
		{name: "no database", adjust: func(e *outboundShippingEnv) { e.databaseURL = "" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := parseOutboundShippingConfig(osEnv(tt.adjust)); err == nil {
				t.Fatal("kafka mode without its prerequisites must fail boot, got nil")
			}
		})
	}
}

func TestParseOutboundShippingConfigOffNeedsNothing(t *testing.T) {
	cfg, err := parseOutboundShippingConfig(outboundShippingEnv{})
	if err != nil {
		t.Fatalf("off mode must accept an empty environment: %v", err)
	}
	if cfg.enabled() {
		t.Fatal("an empty environment must leave the consumer off")
	}
}
