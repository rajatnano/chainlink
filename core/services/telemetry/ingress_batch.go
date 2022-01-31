package telemetry

import (
	"context"

	"github.com/smartcontractkit/chainlink/core/services/synchronization"
	ocrtypes "github.com/smartcontractkit/libocr/commontypes"
)

var _ MonitoringEndpointGenerator = &IngressAgentBatchWrapper{}

type IngressAgentBatchWrapper struct {
	telemetryIngressBatchClient synchronization.TelemetryIngressBatchClient
}

func NewIngressAgentBatchWrapper(telemetryIngressBatchClient synchronization.TelemetryIngressBatchClient) *IngressAgentBatchWrapper {
	return &IngressAgentBatchWrapper{telemetryIngressBatchClient}
}

func (t *IngressAgentBatchWrapper) GenMonitoringEndpoint(contractID string) ocrtypes.MonitoringEndpoint {
	return NewIngressAgentBatch(t.telemetryIngressBatchClient, contractID)
}

type IngressAgentBatch struct {
	telemetryIngressBatchClient synchronization.TelemetryIngressBatchClient
	contractID                  string
}

func NewIngressAgentBatch(telemetryIngressBatchClient synchronization.TelemetryIngressBatchClient, contractID string) *IngressAgentBatch {
	return &IngressAgentBatch{
		telemetryIngressBatchClient,
		contractID,
	}
}

// SendLog sends a telemetry log to the ingress server
func (t *IngressAgentBatch) SendLog(telemetry []byte) {
	payload := synchronization.TelemPayload{
		Ctx:        context.Background(),
		Telemetry:  telemetry,
		ContractID: t.contractID,
	}
	t.telemetryIngressBatchClient.Send(payload)
}
