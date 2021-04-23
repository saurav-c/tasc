package common

const (
	// Load Balancer ports
	LoadBalancerPort = 8000

	// Key Node ports
	KeyReadPullPort     = 8000
	KeyValidatePullPort = 8050
	KeyEndTxnPullPort   = 8100

	// Transaction Manager ports
	TxnManagerServerPort = 9000
	TxnReadPullPort      = 9050
	TxnValidatePullPort  = 9100
	TxnEndTxnPullPort    = 9150
	TxnRoutingPullPort   = 9200
	TxnAckPullPort       = 9250

	// Monitor ports
	MonitorPushPort = 10000

	// Router ports
	RouterPullPort = 11000

	// Worker ports
	WorkerPullPort    = 12000
	WorkerRtrPullPort = TxnRoutingPullPort
	WorkerEndTxnPullPort = TxnEndTxnPullPort

	// Trasaction ID Template
	TidTemplate = "%s-%d-%s"

	// Address Templates
	PullTemplate = "tcp://*:%d"
	PushTemplate = "tcp://%s:%d"

	// Delimeters for Key encoding
	KeyDelimeter         = ":"
	VersionDelimeter     = "-"
	StorageKeyTemplate   = "%s" + KeyDelimeter + "%s" + VersionDelimeter + "%s" // Key:CommitTs-Tid
	StorageIndexTemplate = "%s:index"

	// Log File Templates
	TxnLogTemplate   = "txnManager-%s-%d"
	KeyLogTemplate   = "keyNode-%s-%d"
	RouterLogTempate = "router-%s"
)
