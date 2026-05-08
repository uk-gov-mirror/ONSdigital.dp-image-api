package config_test

import (
	"testing"
	"time"

	"github.com/ONSdigital/dp-image-api/config"
	. "github.com/smartystreets/goconvey/convey"
)

func TestConfig(t *testing.T) {
	Convey("Given an environment with no environment variables set", t, func() {
		cfg, err := config.Get()

		Convey("When the config values are retrieved", func() {
			Convey("Then there should be no error returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("Then the values should be set to the expected defaults", func() {
				So(cfg.BindAddr, ShouldEqual, "localhost:24700")
				So(cfg.APIURL, ShouldResemble, "http://localhost:24700")
				So(cfg.Brokers, ShouldResemble, []string{"localhost:9092", "localhost:9093", "localhost:9094"})
				So(cfg.KafkaVersion, ShouldEqual, "1.0.2")
				So(cfg.KafkaSecProtocol, ShouldEqual, "")
				So(cfg.KafkaMaxBytes, ShouldEqual, 2000000)
				So(cfg.ImageUploadedTopic, ShouldEqual, "image-uploaded")
				So(cfg.StaticFilePublishedTopic, ShouldEqual, "static-file-published")
				So(cfg.GracefulShutdownTimeout, ShouldEqual, 5*time.Second)
				So(cfg.HealthCheckInterval, ShouldEqual, 30*time.Second)
				So(cfg.HealthCheckCriticalTimeout, ShouldEqual, 90*time.Second)
				So(cfg.ClusterEndpoint, ShouldEqual, "localhost:27017")
				So(cfg.Database, ShouldEqual, "images")
				So(cfg.Collections, ShouldResemble, map[string]string{config.ImagesCollection: "images", config.ImagesLockCollection: "images_locks"})
				So(cfg.Username, ShouldEqual, "")
				So(cfg.Password, ShouldEqual, "")
				So(cfg.ReplicaSet, ShouldEqual, "")
				So(cfg.IsStrongReadConcernEnabled, ShouldEqual, false)
				So(cfg.IsWriteConcernMajorityEnabled, ShouldEqual, true)
				So(cfg.QueryTimeout, ShouldEqual, 15*time.Second)
				So(cfg.ConnectTimeout, ShouldEqual, 5*time.Second)
				So(cfg.IsSSL, ShouldEqual, false)
				So(cfg.VerifyCert, ShouldEqual, false)
				So(cfg.IsPublishing, ShouldBeTrue)
				So(cfg.ZebedeeURL, ShouldEqual, "http://localhost:8082")
				So(cfg.DownloadServiceURL, ShouldEqual, "http://localhost:23600")
				So(cfg.EnableURLRewriting, ShouldEqual, false)
			})
			Convey("Then a second call to config should return the same config", func() {
				newCfg, newErr := config.Get()
				So(newErr, ShouldBeNil)
				So(newCfg, ShouldResemble, cfg)
			})
		})
	})
}
