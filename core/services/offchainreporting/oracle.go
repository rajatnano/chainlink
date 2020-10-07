package offchainreporting

import (
	"context"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jinzhu/gorm"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/pkg/errors"

	"github.com/smartcontractkit/chainlink/core/logger"
	"github.com/smartcontractkit/chainlink/core/networking"
	"github.com/smartcontractkit/chainlink/core/services/eth"
	"github.com/smartcontractkit/chainlink/core/services/job"
	"github.com/smartcontractkit/chainlink/core/services/pipeline"
	"github.com/smartcontractkit/chainlink/core/store/models"
	"github.com/smartcontractkit/chainlink/core/store/orm"
	"github.com/smartcontractkit/chainlink/core/utils"
	ocrnetworking "github.com/smartcontractkit/libocr/networking"
	ocr "github.com/smartcontractkit/libocr/offchainreporting"
	ocrtypes "github.com/smartcontractkit/libocr/offchainreporting/types"
)

const JobType job.Type = "offchainreporting"

func RegisterJobType(
	db *gorm.DB,
	config *orm.Config,
	keyStore *KeyStore,
	jobSpawner job.Spawner,
	pipelineRunner pipeline.Runner,
	ethClient eth.Client,
	logBroadcaster eth.LogBroadcaster,
) {
	jobSpawner.RegisterDelegate(
		NewJobSpawnerDelegate(db, config, keyStore, pipelineRunner, ethClient, logBroadcaster),
	)
}

type jobSpawnerDelegate struct {
	db             *gorm.DB
	config         *orm.Config
	keyStore       *KeyStore
	pipelineRunner pipeline.Runner
	ethClient      eth.Client
	logBroadcaster eth.LogBroadcaster
}

func NewJobSpawnerDelegate(
	db *gorm.DB,
	config *orm.Config,
	keyStore *KeyStore,
	pipelineRunner pipeline.Runner,
	ethClient eth.Client,
	logBroadcaster eth.LogBroadcaster,
) *jobSpawnerDelegate {
	return &jobSpawnerDelegate{db, config, keyStore, pipelineRunner, ethClient, logBroadcaster}
}

func (d jobSpawnerDelegate) JobType() job.Type {
	return JobType
}

func (d jobSpawnerDelegate) ToDBRow(spec job.Spec) models.JobSpecV2 {
	concreteSpec, ok := spec.(OracleSpec)
	if !ok {
		panic(fmt.Sprintf("expected an offchainreporting.OracleSpec, got %T", spec))
	}
	return models.JobSpecV2{OffchainreportingOracleSpec: &concreteSpec.OffchainReportingOracleSpec}
}

func (d jobSpawnerDelegate) FromDBRow(spec models.JobSpecV2) job.Spec {
	if spec.OffchainreportingOracleSpec == nil {
		return nil
	}
	return &OracleSpec{
		OffchainReportingOracleSpec: *spec.OffchainreportingOracleSpec,
		jobID:                       spec.ID,
	}
}

func (d jobSpawnerDelegate) ServicesForSpec(spec job.Spec) ([]job.Service, error) {
	concreteSpec := spec.(*OracleSpec)

	// FIXME: Use proper values
	fromAddress := common.Address{}
	gasLimit := uint64(0)

	transmitter := NewTransmitter(d.db.DB(), fromAddress, gasLimit)

	ocrContract, err := NewOCRContract(
		concreteSpec.ContractAddress.Address(),
		d.ethClient,
		d.logBroadcaster,
		concreteSpec.JobID(),
		transmitter,
		*logger.Default,
	)
	if err != nil {
		return nil, err
	}

	p2pkey, exists := d.keyStore.DecryptedP2PKey(peer.ID(concreteSpec.P2PPeerID))
	if !exists {
		return nil, errors.Errorf("P2P key '%v' does not exist", concreteSpec.P2PPeerID)
	}

	ocrkey, exists := d.keyStore.DecryptedOCRKey(concreteSpec.EncryptedOCRKeyBundleID)
	if !exists {
		return nil, errors.Errorf("OCR key '%v' does not exist", concreteSpec.EncryptedOCRKeyBundleID)
	}

	peerstore, err := networking.NewPeerstore(context.Background(), d.db.DB())
	if err != nil {
		return nil, errors.Wrap(err, "could not make new peerstore")
	}

	ocrLogger := NewLogger(logger.Default)

	peer, err := ocrnetworking.NewPeer(ocrnetworking.PeerConfig{
		PrivKey:    p2pkey.PrivKey,
		ListenPort: d.config.OCRListenPort(),
		ListenIP:   d.config.OCRListenIP(),
		Logger:     ocrLogger,
		Peerstore:  peerstore,
		EndpointConfig: ocrnetworking.EndpointConfig{
			IncomingMessageBufferSize: d.config.OCRIncomingMessageBufferSize(),
			OutgoingMessageBufferSize: d.config.OCROutgoingMessageBufferSize(),
			NewStreamTimeout:          d.config.OCRNewStreamTimeout(),
			DHTLookupInterval:         d.config.OCRDHTLookupInterval(),
		},
	})
	if err != nil {
		return nil, err
	}

	oracle, err := ocr.NewOracle(ocr.OracleArgs{
		LocalConfig: ocrtypes.LocalConfig{
			DataSourceTimeout:                      time.Duration(concreteSpec.ObservationTimeout),
			BlockchainTimeout:                      time.Duration(concreteSpec.BlockchainTimeout),
			ContractConfigTrackerSubscribeInterval: time.Duration(concreteSpec.ContractConfigTrackerSubscribeInterval),
			ContractConfigTrackerPollInterval:      time.Duration(concreteSpec.ContractConfigTrackerPollInterval),
			ContractConfigConfirmations:            concreteSpec.ContractConfigConfirmations,
		},
		Database:                     NewDB(d.db.DB(), concreteSpec.ID),
		Datasource:                   dataSource{jobID: concreteSpec.JobID(), pipelineRunner: d.pipelineRunner},
		ContractTransmitter:          ocrContract,
		ContractConfigTracker:        ocrContract,
		PrivateKeys:                  &ocrkey,
		BinaryNetworkEndpointFactory: peer,
		MonitoringEndpoint:           ocrtypes.MonitoringEndpoint(nil),
		Logger:                       ocrLogger,
		Bootstrappers:                concreteSpec.P2PBootstrapPeers,
	})
	if err != nil {
		return nil, err
	}

	service := oracleService{oracle}

	return []job.Service{service}, nil
}

type oracleService struct{ *ocr.Oracle }

func (o oracleService) Stop() error { return o.Oracle.Close() }

// dataSource is an abstraction over the process of initiating a pipeline run
// and capturing the result.  Additionally, it converts the result to an
// ocrtypes.Observation (*big.Int), as expected by the offchain reporting library.
type dataSource struct {
	pipelineRunner pipeline.Runner
	jobID          int32
}

var _ ocrtypes.DataSource = (*dataSource)(nil)

func (ds dataSource) Observe(ctx context.Context) (ocrtypes.Observation, error) {
	runID, err := ds.pipelineRunner.CreateRun(ds.jobID, nil)
	if err != nil {
		return nil, err
	}

	err = ds.pipelineRunner.AwaitRun(ctx, runID)
	if err != nil {
		return nil, err
	}

	results, err := ds.pipelineRunner.ResultsForRun(runID)
	if err != nil {
		return nil, err
	} else if len(results) != 1 {
		return nil, errors.Errorf("offchain reporting pipeline should have a single output (job spec ID: %v, pipeline run ID: %v)", ds.jobID, runID)
	}
	result := results[0]
	if result.Error != nil {
		return nil, errors.Wrapf(result.Error, "pipeline error")
	}

	asDecimal, err := utils.ToDecimal(result.Value)
	if err != nil {
		return nil, err
	}
	return ocrtypes.Observation(asDecimal.BigInt()), nil
}