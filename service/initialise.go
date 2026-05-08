package service

import (
	"context"
	"net/http"

	"github.com/ONSdigital/dp-api-clients-go/v2/health"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	"github.com/ONSdigital/dp-image-api/api"
	"github.com/ONSdigital/dp-image-api/config"
	"github.com/ONSdigital/dp-image-api/mongo"
	kafka "github.com/ONSdigital/dp-kafka/v5"
	dphttp "github.com/ONSdigital/dp-net/v3/http"
)

// KafkaProducerType to differentiate the kafka producers
type KafkaProducerType int

// All possible Kafka producers
const (
	KafkaProducerUploaded KafkaProducerType = iota
	KafkaProducerPublished
)

// ExternalServiceList holds the initialiser and initialisation state of external services.
type ExternalServiceList struct {
	MongoDB                bool
	HealthCheck            bool
	KafkaProducerUploaded  bool
	KafkaProducerPublished bool
	Init                   Initialiser
}

// NewServiceList creates a new service list with the provided initialiser
func NewServiceList(initialiser Initialiser) *ExternalServiceList {
	return &ExternalServiceList{
		MongoDB:                false,
		HealthCheck:            false,
		KafkaProducerUploaded:  false,
		KafkaProducerPublished: false,
		Init:                   initialiser,
	}
}

// Init implements the Initialiser interface to initialise dependencies
type Init struct{}

// GetHTTPServer creates an http server and sets the Server flag to true
func (e *ExternalServiceList) GetHTTPServer(bindAddr string, router http.Handler) HTTPServer {
	s := e.Init.DoGetHTTPServer(bindAddr, router)
	return s
}

// GetMongoDB creates a mongoDB client and sets the Mongo flag to true
func (e *ExternalServiceList) GetMongoDB(ctx context.Context, cfg config.MongoConfig) (api.MongoServer, error) {
	mongoDB, err := e.Init.DoGetMongoDB(ctx, cfg)
	if err != nil {
		return nil, err
	}
	e.MongoDB = true
	return mongoDB, nil
}

// GetKafkaProducer returns a kafka producer
func (e *ExternalServiceList) GetKafkaProducer(ctx context.Context, cfg *config.Config, producerType KafkaProducerType) (kafkaProducer kafka.IProducer, err error) {
	switch producerType {
	case KafkaProducerUploaded:
		kafkaProducer, err = e.Init.DoGetKafkaProducer(ctx, cfg, cfg.ImageUploadedTopic)
		if err != nil {
			return nil, err
		}
		e.KafkaProducerUploaded = true
	case KafkaProducerPublished:
		kafkaProducer, err = e.Init.DoGetKafkaProducer(ctx, cfg, cfg.StaticFilePublishedTopic)
		if err != nil {
			return nil, err
		}
		e.KafkaProducerPublished = true
	}
	return kafkaProducer, nil
}

// GetHealthClient returns a healthclient for the provided URL
func (e *ExternalServiceList) GetHealthClient(name, url string) *health.Client {
	return e.Init.DoGetHealthClient(name, url)
}

// GetHealthCheck creates a healthcheck with versionInfo and sets teh HealthCheck flag to true
func (e *ExternalServiceList) GetHealthCheck(cfg *config.Config, buildTime, gitCommit, version string) (HealthChecker, error) {
	hc, err := e.Init.DoGetHealthCheck(cfg, buildTime, gitCommit, version)
	if err != nil {
		return nil, err
	}
	e.HealthCheck = true
	return hc, nil
}

// DoGetHTTPServer creates an HTTP Server with the provided bind address and router
func (e *Init) DoGetHTTPServer(bindAddr string, router http.Handler) HTTPServer {
	s := dphttp.NewServer(bindAddr, router)
	s.HandleOSSignals = false
	return s
}

// DoGetMongoDB returns a MongoDB
func (e *Init) DoGetMongoDB(ctx context.Context, cfg config.MongoConfig) (api.MongoServer, error) {
	mongodb, err := mongo.NewMongoStore(ctx, cfg)
	if err != nil {
		return nil, err
	}

	return mongodb, nil
}

// DoGetKafkaProducer creates a kafka producer for the provided broker addresses, topic and envMax values in config
func (e *Init) DoGetKafkaProducer(ctx context.Context, cfg *config.Config, topic string) (kafka.IProducer, error) {
	pConfig := &kafka.ProducerConfig{
		KafkaVersion:    &cfg.KafkaVersion,
		MaxMessageBytes: &cfg.KafkaMaxBytes,
		Topic:           topic,
		BrokerAddrs:     cfg.Brokers,
	}
	if cfg.KafkaSecProtocol == "TLS" {
		pConfig.SecurityConfig = kafka.GetSecurityConfig(
			cfg.KafkaSecCACerts,
			cfg.KafkaSecClientCert,
			cfg.KafkaSecClientKey,
			cfg.KafkaSecSkipVerify,
		)
	}
	return kafka.NewProducer(ctx, pConfig)
}

// DoGetHealthClient creates a new Health Client for the provided name and url
func (e *Init) DoGetHealthClient(name, url string) *health.Client {
	return health.NewClient(name, url)
}

// DoGetHealthCheck creates a healthcheck with versionInfo
func (e *Init) DoGetHealthCheck(cfg *config.Config, buildTime, gitCommit, version string) (HealthChecker, error) {
	versionInfo, err := healthcheck.NewVersionInfo(buildTime, gitCommit, version)
	if err != nil {
		return nil, err
	}
	hc := healthcheck.New(versionInfo, cfg.HealthCheckCriticalTimeout, cfg.HealthCheckInterval)
	return &hc, nil
}
