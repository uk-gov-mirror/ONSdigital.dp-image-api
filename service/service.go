package service

import (
	"context"

	"github.com/ONSdigital/dp-image-api/url"

	"github.com/ONSdigital/dp-api-clients-go/v2/health"
	dpauth "github.com/ONSdigital/dp-authorisation/auth"
	"github.com/ONSdigital/dp-image-api/api"
	"github.com/ONSdigital/dp-image-api/config"
	kafka "github.com/ONSdigital/dp-kafka/v5"
	"github.com/ONSdigital/dp-net/v3/handlers"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/gorilla/mux"
	"github.com/justinas/alice"
	"github.com/pkg/errors"
)

const (
	labelTopic = "topic"
)

// Service contains all the configs, server and clients to run the Image API
type Service struct {
	config                 *config.Config
	server                 HTTPServer
	router                 *mux.Router
	api                    *api.API
	serviceList            *ExternalServiceList
	healthCheck            HealthChecker
	mongoDB                api.MongoServer
	uploadedKafkaProducer  kafka.IProducer
	publishedKafkaProducer kafka.IProducer
}

// Run the service
func Run(ctx context.Context, cfg *config.Config, serviceList *ExternalServiceList, buildTime, gitCommit, version string, svcErrors chan error) (*Service, error) {
	log.Info(ctx, "running service")

	// Get HTTP Server with collectionID checkHeader middleware
	r := mux.NewRouter()
	middleware := alice.New(handlers.CheckHeader(handlers.CollectionID))
	s := serviceList.GetHTTPServer(cfg.BindAddr, middleware.Then(r))

	// Get MongoDB client
	mongoDB, err := serviceList.GetMongoDB(ctx, cfg.MongoConfig)
	if err != nil {
		log.Fatal(ctx, "failed to initialise mongo DB", err)
		return nil, err
	}

	var a *api.API

	urlBuilder := url.NewBuilder(cfg.APIURL)
	// The following dependencies will only be initialised if we are in publishing mode
	var zc *health.Client
	var auth api.AuthHandler
	var uploadedKafkaProducer kafka.IProducer
	var publishedKafkaProducer kafka.IProducer
	if cfg.IsPublishing {
		// Get Health client for Zebedee and permissions
		zc = serviceList.GetHealthClient("Zebedee", cfg.ZebedeeURL)
		auth = getAuthorisationHandlers(zc)

		// Get Uploaded Kafka producer
		uploadedKafkaProducer, err = serviceList.GetKafkaProducer(ctx, cfg, KafkaProducerUploaded)
		if err != nil {
			log.Fatal(ctx, "failed to create image-uploaded kafka producer", err)
			return nil, err
		}

		// Get Published Kafka producer
		publishedKafkaProducer, err = serviceList.GetKafkaProducer(ctx, cfg, KafkaProducerPublished)
		if err != nil {
			log.Fatal(ctx, "failed to create image-published kafka producer", err)
			return nil, err
		}

		// Setup the API in publishing
		a = api.Setup(ctx, cfg, r, auth, mongoDB, uploadedKafkaProducer, publishedKafkaProducer, urlBuilder)
	} else {
		// Setup the API in web mode
		a = api.Setup(ctx, cfg, r, auth, mongoDB, nil, nil, urlBuilder)
	}

	// Get HealthCheck
	hc, err := serviceList.GetHealthCheck(cfg, buildTime, gitCommit, version)
	if err != nil {
		log.Fatal(ctx, "could not instantiate healthcheck", err)
		return nil, err
	}
	if err := registerCheckers(ctx, cfg, hc, mongoDB, uploadedKafkaProducer, publishedKafkaProducer, zc); err != nil {
		return nil, errors.Wrap(err, "unable to register checkers")
	}

	r.StrictSlash(true).Path("/health").HandlerFunc(hc.Handler)
	hc.Start(ctx)

	if cfg.IsPublishing {
		// kafka error channel logging go-routines
		uploadedKafkaProducer.LogErrors(ctx)
		publishedKafkaProducer.LogErrors(ctx)
	}

	// Run the http server in a new go-routine
	go func() {
		if err := s.ListenAndServe(); err != nil {
			svcErrors <- errors.Wrap(err, "failure in http listen and serve")
		}
	}()

	return &Service{
		config:                 cfg,
		server:                 s,
		router:                 r,
		api:                    a,
		serviceList:            serviceList,
		healthCheck:            hc,
		mongoDB:                mongoDB,
		uploadedKafkaProducer:  uploadedKafkaProducer,
		publishedKafkaProducer: publishedKafkaProducer,
	}, nil
}

// Close gracefully shuts the service down in the required order, with timeout
func (svc *Service) Close(ctx context.Context) error {
	timeout := svc.config.GracefulShutdownTimeout
	log.Info(ctx, "commencing graceful shutdown", log.Data{"graceful_shutdown_timeout": timeout})
	ctx, cancel := context.WithTimeout(ctx, timeout)

	// track shutown gracefully closes up
	var gracefulShutdown bool

	go func() {
		defer cancel()
		var hasShutdownError bool

		// stop healthcheck, as it depends on everything else
		if svc.serviceList.HealthCheck {
			svc.healthCheck.Stop()
		}

		// stop any incoming requests before closing any outbound connections
		if err := svc.server.Shutdown(ctx); err != nil {
			log.Error(ctx, "failed to shutdown http server", err)
			hasShutdownError = true
		}

		// close API
		if err := svc.api.Close(ctx); err != nil {
			log.Error(ctx, "error closing API", err)
			hasShutdownError = true
		}

		// close mongoDB
		if svc.serviceList.MongoDB {
			if err := svc.mongoDB.Close(ctx); err != nil {
				log.Error(ctx, "error closing mongoDB", err)
				hasShutdownError = true
			}
		}

		// close kafka uploaded producer
		if svc.serviceList.KafkaProducerUploaded {
			if err := svc.uploadedKafkaProducer.Close(ctx); err != nil {
				log.Error(ctx, "error closing Uploaded Kafka Producer", err)
				hasShutdownError = true
			}
		}

		// close kafka published producer
		if svc.serviceList.KafkaProducerUploaded {
			if err := svc.publishedKafkaProducer.Close(ctx); err != nil {
				log.Error(ctx, "error closing Published Kafka Producer", err)
				hasShutdownError = true
			}
		}

		if !hasShutdownError {
			gracefulShutdown = true
		}
	}()

	// wait for shutdown success (via cancel) or failure (timeout)
	<-ctx.Done()

	if !gracefulShutdown {
		err := errors.New("failed to shutdown gracefully")
		log.Error(ctx, "failed to shutdown gracefully ", err)
		return err
	}

	log.Info(ctx, "graceful shutdown was successful")
	return nil
}

func registerCheckers(ctx context.Context,
	cfg *config.Config,
	hc HealthChecker,
	mongoDB api.MongoServer,
	uploadedKafkaProducer, publishedKafkaProducer kafka.IProducer,
	zebedeeClient *health.Client) (err error) {
	hasErrors := false

	if err = hc.AddCheck("Mongo DB", mongoDB.Checker); err != nil {
		hasErrors = true
		log.Error(ctx, "error adding check for mongo db", err)
	}

	if cfg.IsPublishing {
		if err = hc.AddCheck("Uploaded Kafka Producer", uploadedKafkaProducer.Checker); err != nil {
			hasErrors = true
			log.Error(ctx, "error adding check for uploaded kafka producer", err, log.Data{labelTopic: cfg.ImageUploadedTopic})
		}

		if err = hc.AddCheck("Published Kafka Producer", publishedKafkaProducer.Checker); err != nil {
			hasErrors = true
			log.Error(ctx, "error adding check for published kafka producer", err, log.Data{labelTopic: cfg.StaticFilePublishedTopic})
		}

		if err = hc.AddCheck("Zebedee", zebedeeClient.Checker); err != nil {
			hasErrors = true
			log.Error(ctx, "error adding check for zebedee", err)
		}
	}

	if hasErrors {
		return errors.New("Error(s) registering checkers for healthcheck")
	}
	return nil
}

// generate permissions from dp-auth-api, using the provided health client, reusing its http Client
func getAuthorisationHandlers(zc *health.Client) api.AuthHandler {
	log.Info(context.Background(), "getting Authorisation Handlers", log.Data{"zc_url": zc.URL})

	authClient := dpauth.NewPermissionsClient(zc.Client)
	authVerifier := dpauth.DefaultPermissionsVerifier()

	// for checking caller permissions when we only have a user/service token
	permissions := dpauth.NewHandler(
		dpauth.NewPermissionsRequestBuilder(zc.URL),
		authClient,
		authVerifier,
	)

	return permissions
}
