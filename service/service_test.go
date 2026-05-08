package service_test

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"

	"github.com/ONSdigital/dp-api-clients-go/v2/health"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	"github.com/ONSdigital/dp-image-api/api"
	apiMock "github.com/ONSdigital/dp-image-api/api/mock"
	"github.com/ONSdigital/dp-image-api/config"
	"github.com/ONSdigital/dp-image-api/service"
	serviceMock "github.com/ONSdigital/dp-image-api/service/mock"
	kafka "github.com/ONSdigital/dp-kafka/v5"
	"github.com/ONSdigital/dp-kafka/v5/kafkatest"
	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	ctx           = context.Background()
	testBuildTime = "BuildTime"
	testGitCommit = "GitCommit"
	testVersion   = "Version"
	errServer     = errors.New("HTTP Server error")
)

var (
	errMongoDB       = errors.New("mongoDB error")
	errKafkaProducer = errors.New("KafkaProducer error")
	errHealthcheck   = errors.New("healthCheck error")
)

var funcDoGetMongoDBErr = func(ctx context.Context, cfg config.MongoConfig) (api.MongoServer, error) {
	return nil, errMongoDB
}

var funcDoGetHealthcheckErr = func(cfg *config.Config, buildTime string, gitCommit string, version string) (service.HealthChecker, error) {
	return nil, errHealthcheck
}

var funcDoGetHTTPServerNil = func(bindAddr string, router http.Handler) service.HTTPServer {
	return nil
}

func TestRunPublishing(t *testing.T) {
	Convey("Having a set of mocked dependencies", t, func() {
		cfg, err := config.Get()
		So(err, ShouldBeNil)

		mongoDBMock := &apiMock.MongoServerMock{
			CheckerFunc: func(ctx context.Context, state *healthcheck.CheckState) error { return nil },
		}

		kafkaProducerMock := &kafkatest.IProducerMock{
			ChannelsFunc: func() *kafka.ProducerChannels {
				return &kafka.ProducerChannels{}
			},
			LogErrorsFunc: func(ctx context.Context) {
				// Do nothing
			},
		}

		hcMock := &serviceMock.HealthCheckerMock{
			AddCheckFunc: func(name string, checker healthcheck.Checker) error { return nil },
			StartFunc:    func(ctx context.Context) {},
		}

		serverWg := &sync.WaitGroup{}
		serverMock := &serviceMock.HTTPServerMock{
			ListenAndServeFunc: func() error {
				serverWg.Done()
				return nil
			},
		}

		failingServerMock := &serviceMock.HTTPServerMock{
			ListenAndServeFunc: func() error {
				serverWg.Done()
				return errServer
			},
		}

		funcDoGetMongoDBOk := func(ctx context.Context, cfg config.MongoConfig) (api.MongoServer, error) {
			return mongoDBMock, nil
		}

		funcDoGetHealthcheckOk := func(cfg *config.Config, buildTime string, gitCommit string, version string) (service.HealthChecker, error) {
			return hcMock, nil
		}

		funcDoGetHTTPServer := func(bindAddr string, router http.Handler) service.HTTPServer {
			return serverMock
		}

		funcDoGetFailingHTTPSerer := func(bindAddr string, router http.Handler) service.HTTPServer {
			return failingServerMock
		}

		funcDoGetKafkaProducerOk := func(ctx context.Context, cfg *config.Config, topic string) (kafka.IProducer, error) {
			return kafkaProducerMock, nil
		}

		doGetKafkaProducerErrOnTopic := func(errTopic string) func(ctx context.Context, cfg *config.Config, topic string) (kafka.IProducer, error) {
			return func(ctx context.Context, cfg *config.Config, topic string) (kafka.IProducer, error) {
				if topic == errTopic {
					return nil, errKafkaProducer
				}
				return kafkaProducerMock, nil
			}
		}

		funcDoGetHealthClientOk := func(name string, url string) *health.Client {
			return &health.Client{
				URL:  url,
				Name: name,
			}
		}

		Convey("Given that initialising mongoDB returns an error", func() {
			initMock := &serviceMock.InitialiserMock{
				DoGetHTTPServerFunc:    funcDoGetHTTPServerNil,
				DoGetMongoDBFunc:       funcDoGetMongoDBErr,
				DoGetKafkaProducerFunc: funcDoGetKafkaProducerOk,
				DoGetHealthClientFunc:  funcDoGetHealthClientOk,
			}
			svcErrors := make(chan error, 1)
			svcList := service.NewServiceList(initMock)
			_, err := service.Run(ctx, cfg, svcList, testBuildTime, testGitCommit, testVersion, svcErrors)

			Convey("Then service Run fails with the same error and the flag is not set. No further initialisations are attempted", func() {
				So(err, ShouldResemble, errMongoDB)
				So(svcList.MongoDB, ShouldBeFalse)
				So(svcList.KafkaProducerUploaded, ShouldBeFalse)
				So(svcList.KafkaProducerPublished, ShouldBeFalse)
				So(svcList.HealthCheck, ShouldBeFalse)
			})
		})

		Convey("Given that initialising kafka image-uploaded producer returns an error", func() {
			initMock := &serviceMock.InitialiserMock{
				DoGetHTTPServerFunc:    funcDoGetHTTPServerNil,
				DoGetMongoDBFunc:       funcDoGetMongoDBOk,
				DoGetKafkaProducerFunc: doGetKafkaProducerErrOnTopic(cfg.ImageUploadedTopic),
				DoGetHealthClientFunc:  funcDoGetHealthClientOk,
			}
			svcErrors := make(chan error, 1)
			svcList := service.NewServiceList(initMock)
			_, err := service.Run(ctx, cfg, svcList, testBuildTime, testGitCommit, testVersion, svcErrors)

			Convey("Then service Run fails with the same error and the flag is not set. No further initialisations are attempted", func() {
				So(err, ShouldResemble, errKafkaProducer)
				So(svcList.MongoDB, ShouldBeTrue)
				So(svcList.KafkaProducerUploaded, ShouldBeFalse)
				So(svcList.KafkaProducerPublished, ShouldBeFalse)
				So(svcList.HealthCheck, ShouldBeFalse)
			})
		})

		Convey("Given that initialising kafka static-file-published producer returns an error", func() {
			initMock := &serviceMock.InitialiserMock{
				DoGetHTTPServerFunc:    funcDoGetHTTPServerNil,
				DoGetMongoDBFunc:       funcDoGetMongoDBOk,
				DoGetKafkaProducerFunc: doGetKafkaProducerErrOnTopic(cfg.StaticFilePublishedTopic),
				DoGetHealthClientFunc:  funcDoGetHealthClientOk,
			}
			svcErrors := make(chan error, 1)
			svcList := service.NewServiceList(initMock)
			_, err := service.Run(ctx, cfg, svcList, testBuildTime, testGitCommit, testVersion, svcErrors)

			Convey("Then service Run fails with the same error and the flag is not set. No further initialisations are attempted", func() {
				So(err, ShouldResemble, errKafkaProducer)
				So(svcList.MongoDB, ShouldBeTrue)
				So(svcList.KafkaProducerUploaded, ShouldBeTrue)
				So(svcList.KafkaProducerPublished, ShouldBeFalse)
				So(svcList.HealthCheck, ShouldBeFalse)
			})
		})

		Convey("Given that initialising healthcheck returns an error", func() {
			initMock := &serviceMock.InitialiserMock{
				DoGetHTTPServerFunc:    funcDoGetHTTPServerNil,
				DoGetMongoDBFunc:       funcDoGetMongoDBOk,
				DoGetHealthCheckFunc:   funcDoGetHealthcheckErr,
				DoGetKafkaProducerFunc: funcDoGetKafkaProducerOk,
				DoGetHealthClientFunc:  funcDoGetHealthClientOk,
			}
			svcErrors := make(chan error, 1)
			svcList := service.NewServiceList(initMock)
			_, err := service.Run(ctx, cfg, svcList, testBuildTime, testGitCommit, testVersion, svcErrors)

			Convey("Then service Run fails with the same error and the flag is not set", func() {
				So(err, ShouldResemble, errHealthcheck)
				So(svcList.MongoDB, ShouldBeTrue)
				So(svcList.KafkaProducerUploaded, ShouldBeTrue)
				So(svcList.KafkaProducerPublished, ShouldBeTrue)
				So(svcList.HealthCheck, ShouldBeFalse)
			})
		})

		Convey("Given that Checkers cannot be registered", func() {
			errAddheckFail := errors.New("Error(s) registering checkers for healthcheck")
			hcMockAddFail := &serviceMock.HealthCheckerMock{
				AddCheckFunc: func(name string, checker healthcheck.Checker) error { return errAddheckFail },
				StartFunc:    func(ctx context.Context) {},
			}

			initMock := &serviceMock.InitialiserMock{
				DoGetHTTPServerFunc:    funcDoGetHTTPServerNil,
				DoGetMongoDBFunc:       funcDoGetMongoDBOk,
				DoGetKafkaProducerFunc: funcDoGetKafkaProducerOk,
				DoGetHealthCheckFunc: func(cfg *config.Config, buildTime string, gitCommit string, version string) (service.HealthChecker, error) {
					return hcMockAddFail, nil
				},
				DoGetHealthClientFunc: funcDoGetHealthClientOk,
			}
			svcErrors := make(chan error, 1)
			svcList := service.NewServiceList(initMock)
			_, err := service.Run(ctx, cfg, svcList, testBuildTime, testGitCommit, testVersion, svcErrors)

			Convey("Then service Run fails, but all checks try to register", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldResemble, fmt.Sprintf("unable to register checkers: %s", errAddheckFail.Error()))
				So(svcList.MongoDB, ShouldBeTrue)
				So(svcList.HealthCheck, ShouldBeTrue)
				So(hcMockAddFail.AddCheckCalls(), ShouldHaveLength, 4)
				So(hcMockAddFail.AddCheckCalls()[0].Name, ShouldResemble, "Mongo DB")
				So(hcMockAddFail.AddCheckCalls()[1].Name, ShouldResemble, "Uploaded Kafka Producer")
				So(hcMockAddFail.AddCheckCalls()[2].Name, ShouldResemble, "Published Kafka Producer")
				So(hcMockAddFail.AddCheckCalls()[3].Name, ShouldResemble, "Zebedee")
			})
		})

		Convey("Given that all dependencies are successfully initialised", func() {
			initMock := &serviceMock.InitialiserMock{
				DoGetHTTPServerFunc:    funcDoGetHTTPServer,
				DoGetMongoDBFunc:       funcDoGetMongoDBOk,
				DoGetKafkaProducerFunc: funcDoGetKafkaProducerOk,
				DoGetHealthCheckFunc:   funcDoGetHealthcheckOk,
				DoGetHealthClientFunc:  funcDoGetHealthClientOk,
			}
			svcErrors := make(chan error, 1)
			svcList := service.NewServiceList(initMock)
			serverWg.Add(1)
			_, err := service.Run(ctx, cfg, svcList, testBuildTime, testGitCommit, testVersion, svcErrors)

			Convey("Then service Run succeeds and all the flags are set", func() {
				So(err, ShouldBeNil)
				So(svcList.MongoDB, ShouldBeTrue)
				So(svcList.KafkaProducerUploaded, ShouldBeTrue)
				So(svcList.KafkaProducerPublished, ShouldBeTrue)
				So(svcList.HealthCheck, ShouldBeTrue)
			})

			Convey("The checkers are registered and the healthcheck and http server started", func() {
				So(hcMock.AddCheckCalls(), ShouldHaveLength, 4)
				So(hcMock.AddCheckCalls()[0].Name, ShouldResemble, "Mongo DB")
				So(hcMock.AddCheckCalls()[1].Name, ShouldResemble, "Uploaded Kafka Producer")
				So(hcMock.AddCheckCalls()[2].Name, ShouldResemble, "Published Kafka Producer")
				So(hcMock.AddCheckCalls()[3].Name, ShouldEqual, "Zebedee")
				So(initMock.DoGetHTTPServerCalls(), ShouldHaveLength, 1)
				So(initMock.DoGetHTTPServerCalls()[0].BindAddr, ShouldEqual, "localhost:24700")
				So(hcMock.StartCalls(), ShouldHaveLength, 1)
				serverWg.Wait() // Wait for HTTP server go-routine to finish
				So(serverMock.ListenAndServeCalls(), ShouldHaveLength, 1)
			})
		})

		Convey("Given that all dependencies are successfully initialised but the http server fails", func() {
			initMock := &serviceMock.InitialiserMock{
				DoGetHTTPServerFunc:    funcDoGetFailingHTTPSerer,
				DoGetMongoDBFunc:       funcDoGetMongoDBOk,
				DoGetKafkaProducerFunc: funcDoGetKafkaProducerOk,
				DoGetHealthCheckFunc:   funcDoGetHealthcheckOk,
				DoGetHealthClientFunc:  funcDoGetHealthClientOk,
			}
			svcErrors := make(chan error, 1)
			svcList := service.NewServiceList(initMock)
			serverWg.Add(1)
			_, err := service.Run(ctx, cfg, svcList, testBuildTime, testGitCommit, testVersion, svcErrors)
			So(err, ShouldBeNil)

			Convey("Then the error is returned in the error channel", func() {
				sErr := <-svcErrors
				So(sErr.Error(), ShouldResemble, fmt.Sprintf("failure in http listen and serve: %s", errServer.Error()))
				So(failingServerMock.ListenAndServeCalls(), ShouldHaveLength, 1)
			})
		})

		Convey("Given that all required dependencies are successfully initialised in web mode", func() {
			cfg.IsPublishing = false
			initMock := &serviceMock.InitialiserMock{
				DoGetHTTPServerFunc:  funcDoGetHTTPServer,
				DoGetMongoDBFunc:     funcDoGetMongoDBOk,
				DoGetHealthCheckFunc: funcDoGetHealthcheckOk,
			}
			svcErrors := make(chan error, 1)
			svcList := service.NewServiceList(initMock)
			serverWg.Add(1)
			_, err := service.Run(ctx, cfg, svcList, testBuildTime, testGitCommit, testVersion, svcErrors)

			Convey("Then service Run succeeds but only the required flags are set", func() {
				So(err, ShouldBeNil)
				So(svcList.MongoDB, ShouldBeTrue)
				So(svcList.KafkaProducerUploaded, ShouldBeFalse)
				So(svcList.KafkaProducerPublished, ShouldBeFalse)
				So(svcList.HealthCheck, ShouldBeTrue)
			})
		})
	})
}

func TestClose(t *testing.T) {
	Convey("Having a correctly initialised service", t, func() {
		cfg, err := config.Get()
		So(err, ShouldBeNil)

		hcStopped := false
		serverStopped := false
		mongoStopped := false

		// healthcheck Stop does not depend on any other service being closed/stopped
		hcMock := &serviceMock.HealthCheckerMock{
			AddCheckFunc: func(name string, checker healthcheck.Checker) error { return nil },
			StartFunc:    func(ctx context.Context) {},
			StopFunc:     func() { hcStopped = true },
		}

		// server Shutdown will fail if healthcheck is not stopped
		serverMock := &serviceMock.HTTPServerMock{
			ListenAndServeFunc: func() error { return nil },
			ShutdownFunc: func(ctx context.Context) error {
				if !hcStopped {
					return errors.New("Server stopped before healthcheck")
				}
				serverStopped = true
				return nil
			},
		}

		// mongoDB Close will fail if healthcheck and http server are not already closed
		mongoDBMock := &apiMock.MongoServerMock{
			CheckerFunc: func(ctx context.Context, state *healthcheck.CheckState) error { return nil },
			CloseFunc: func(ctx context.Context) error {
				if !hcStopped || !serverStopped {
					return errors.New("MongoDB closed before stopping healthcheck or HTTP server")
				}
				mongoStopped = true
				return nil
			},
		}

		// kafkaProducerMock (for any kafka producer) will fail if healthcheck, http server and mongo are not already closed
		createKafkaProducerMock := func() *kafkatest.IProducerMock {
			return &kafkatest.IProducerMock{
				CheckerFunc: func(ctx context.Context, state *healthcheck.CheckState) error { return nil },
				CloseFunc: func(ctx context.Context) error {
					if !hcStopped || !serverStopped || !mongoStopped {
						return errors.New("KafkaProducer closed before stopping healthcheck, MongoDB or HTTP server")
					}
					return nil
				},
				LogErrorsFunc: func(ctx context.Context) {
					// Do nothing
				},
				ChannelsFunc: kafka.CreateProducerChannels,
			}
		}
		kafkaUploadedProducerMock := createKafkaProducerMock()
		kafkaPublishedProducerMock := createKafkaProducerMock()
		doGetKafkaProducerFunc := func(ctx context.Context, cfg *config.Config, topic string) (kafka.IProducer, error) {
			switch topic {
			case cfg.ImageUploadedTopic:
				return kafkaUploadedProducerMock, nil
			case cfg.StaticFilePublishedTopic:
				return kafkaPublishedProducerMock, nil
			default:
				return nil, errors.New("wrong topic")
			}
		}

		Convey("Closing the service results in all the dependencies being closed in the expected order", func() {
			initMock := &serviceMock.InitialiserMock{
				DoGetHTTPServerFunc:    func(bindAddr string, router http.Handler) service.HTTPServer { return serverMock },
				DoGetMongoDBFunc:       func(ctx context.Context, cfg config.MongoConfig) (api.MongoServer, error) { return mongoDBMock, nil },
				DoGetKafkaProducerFunc: doGetKafkaProducerFunc,
				DoGetHealthCheckFunc: func(cfg *config.Config, buildTime string, gitCommit string, version string) (service.HealthChecker, error) {
					return hcMock, nil
				},
				DoGetHealthClientFunc: func(name, url string) *health.Client { return &health.Client{} },
			}

			svcErrors := make(chan error, 1)
			svcList := service.NewServiceList(initMock)
			svc, err := service.Run(ctx, cfg, svcList, testBuildTime, testGitCommit, testVersion, svcErrors)
			So(err, ShouldBeNil)

			err = svc.Close(context.Background())
			So(err, ShouldBeNil)
			So(hcMock.StopCalls(), ShouldHaveLength, 1)
			So(serverMock.ShutdownCalls(), ShouldHaveLength, 1)
			So(mongoDBMock.CloseCalls(), ShouldHaveLength, 1)
			So(kafkaUploadedProducerMock.CloseCalls(), ShouldHaveLength, 1)
			So(kafkaPublishedProducerMock.CloseCalls(), ShouldHaveLength, 1)
		})

		Convey("If services fail to stop, the Close operation tries to close all dependencies and returns an error", func() {
			failingserverMock := &serviceMock.HTTPServerMock{
				ListenAndServeFunc: func() error { return nil },
				ShutdownFunc: func(ctx context.Context) error {
					return errors.New("Failed to stop http server")
				},
			}

			initMock := &serviceMock.InitialiserMock{
				DoGetHTTPServerFunc:    func(bindAddr string, router http.Handler) service.HTTPServer { return failingserverMock },
				DoGetMongoDBFunc:       func(ctx context.Context, cfg config.MongoConfig) (api.MongoServer, error) { return mongoDBMock, nil },
				DoGetKafkaProducerFunc: doGetKafkaProducerFunc,
				DoGetHealthCheckFunc: func(cfg *config.Config, buildTime string, gitCommit string, version string) (service.HealthChecker, error) {
					return hcMock, nil
				},
				DoGetHealthClientFunc: func(name, url string) *health.Client { return &health.Client{} },
			}

			svcErrors := make(chan error, 1)
			svcList := service.NewServiceList(initMock)
			svc, err := service.Run(ctx, cfg, svcList, testBuildTime, testGitCommit, testVersion, svcErrors)
			So(err, ShouldBeNil)

			err = svc.Close(context.Background())
			So(err, ShouldNotBeNil)
			So(hcMock.StopCalls(), ShouldHaveLength, 1)
			So(failingserverMock.ShutdownCalls(), ShouldHaveLength, 1)
			So(mongoDBMock.CloseCalls(), ShouldHaveLength, 1)
			So(kafkaUploadedProducerMock.CloseCalls(), ShouldHaveLength, 1)
			So(kafkaPublishedProducerMock.CloseCalls(), ShouldHaveLength, 1)
		})
	})
}
