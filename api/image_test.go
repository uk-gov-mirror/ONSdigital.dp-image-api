package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	dpauth "github.com/ONSdigital/dp-authorisation/auth"
	"github.com/ONSdigital/dp-image-api/api"
	"github.com/ONSdigital/dp-image-api/api/mock"
	"github.com/ONSdigital/dp-image-api/apierrors"
	"github.com/ONSdigital/dp-image-api/config"
	"github.com/ONSdigital/dp-image-api/event"
	"github.com/ONSdigital/dp-image-api/models"
	"github.com/ONSdigital/dp-kafka/v5/avro"
	"github.com/ONSdigital/dp-kafka/v5/kafkatest"
	"github.com/ONSdigital/dp-net/v3/handlers"
	dpreq "github.com/ONSdigital/dp-net/v3/request"

	. "github.com/smartystreets/goconvey/convey"
)

// Constants for testing
const (
	testUserAuthToken      = "UserToken"
	testImageID1           = "imageImageID1"
	testImageID2           = "imageImageID2"
	testImageCreatedID     = "imageCreatedID"
	testImageUploadedID    = "imageUploadedID"
	testImageImportingID   = "imageImportingID"
	testImagePublishedID   = "imagePublishedID"
	testCollectionID1      = "1234"
	testVariantOriginal    = "original"
	testVariantAlternative = "bw1024"
	testVariantPng         = "png_w500"
	testUploadFilename     = "newimage.png"
	testUploadPath         = "s3://images/" + testUploadFilename
	testLockID             = "image-myID-123456789"
	testDownloadType       = "originally uploaded file"
	testPrivateHref        = "http://download.ons.gov.uk/images/imageImageID2/original/some-image-name"
	testFilename           = "some-image-name"
	testLicence            = "Open Government Licence v3.0"
	testLicenceHref        = "https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/"
	testType               = "chart"
	contentTypeKey         = "Content-Type"
	contentTypeJSON        = "application/json; charset=utf-8"
	downloadServiceURL     = "http://download-web.ons.example"
)

var (
	longName             = strings.Repeat("Llanfairpwllgwyngyllgogerychwyrndrobwllllantysiliogogogoch", 10)
	testSize             = 1024
	testWidth            = 123
	testHeight           = 321
	testImportStarted    = time.Date(2020, time.April, 26, 8, 5, 52, 0, time.UTC)
	testImportCompleted  = time.Date(2020, time.April, 26, 8, 7, 32, 0, time.UTC)
	testPublishStarted   = time.Date(2020, time.April, 26, 9, 51, 3, 0, time.UTC)
	testPublishCompleted = time.Date(2020, time.April, 26, 10, 1, 28, 0, time.UTC)
	expectedProto        = "https"
	expectedHost         = "api.somehost"
	expectedPathPrefix   = "v1"
)

var errMongoDB = errors.New("MongoDB generic error")

// Empty JSON Payload
var emptyJSONPayload = `{}`

// New Image Payload without any extra field.
var newImagePayloadFmt = `{
	"collection_id": "%s",
	"filename": "%s",
	"license": {
		"title": "Open Government Licence v3.0",
		"href": "https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/"
	},
    "state": "created",
	"type": "chart"
}`

// New Image Payload with extra state field.
var newImageWithStatePayloadFmt = `{
	"collection_id": "%s",
	"filename": "some-image-name",
	"state": "%s",
	"license": {
		"title": "Open Government Licence v3.0",
		"href": "https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/"
	},
	"type": "chart"
}`

// Image Upload Payload without any extra field.
var imageUploadPayloadFmt = `{
	"collection_id": "%s",
	"filename": "some-image-name",
	"state": "uploaded",
	"license": {
		"title": "Open Government Licence v3.0",
		"href": "https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/"
	},
	"type": "chart",
	"upload": {
		"path": "%s"
	}
}`

// New Image Download Payload without any extra field.
var newImageDownloadPayloadFmt = `{
	"id": "%s",
	"type": "%s",
	"state": "%s",
    "import_started" : "2020-04-26T08:05:52Z"
}`

// Update Image Download Payload without any extra field.
var updateImageDownloadImportedPayloadFmt = `{
	"id": "%s",
	"type": "%s",
	"state": "%s",
    "import_started" : "2020-04-26T08:05:52Z",
    "import_completed" : "2020-04-26T08:05:52Z"
}`

// Update Image Download Payload without any extra field.
var updateImageDownloadCompletedPayloadFmt = `{
	"id": "%s",
	"type": "%s",
	"state": "%s",
    "import_started" : "2020-04-26T08:05:52Z",
    "import_completed" : "2020-04-26T08:07:32Z",
    "publish_started" : "2020-04-26T09:51:03Z",
    "publish_completed" : "2020-04-26T10:01:28Z"
}`

// Image Download Payload with all possible fields.
var (
	imageDownloadPayloadFmt = `{
		"size": 1024,
		"palette": "bw",
		"type": "%s",
		"width": 123,
		"height": 321,
		"public": false,
		"href": "http://someURL.com",
		"private": "my-private-bucket",
		"state": "published",
		"error": "someError",
		"import_started": "2020-04-26T08:05:52Z",
		"import_completed": "2020-04-26T08:07:32+00:00",
		"publish_started": "2020-04-26T09:51:03-00:00",
		"publish_completed": "2020-04-26T10:01:28Z"
	}`
	imageDownloadPayload = fmt.Sprintf(imageDownloadPayloadFmt, testDownloadType)
)

// Full Image payload, containing all possible fields
var (
	fullImagePayloadFmt = `{
		"id": "%s",
		"collection_id": "%s",
		"filename": "some-image-name",
		"license": {
			"title": "Open Government Licence v3.0",
			"href": "https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/"
		},
		"type": "chart",
		"state": "%s",
		"upload": {
			"path": "images/025a789c-533f-4ecf-a83b-65412b96b2b7/image-name.png"
		},
		"downloads": {
			"original": {
				"size": 1024,
				"palette": "bw",
				"type": "originally uploaded file",
				"width": 123,
				"height": 321,
				"public": true,
				"href": "http://download.ons.gov.uk/images/042e216a-7822-4fa0-a3d6-e3f5248ffc35/image-name.png",
				"private": "my-private-bucket",
				"state": "published",
				"error": "",
				"import_started": "2020-04-26T08:05:52Z",
				"import_completed": "2020-04-26T08:07:32+00:00",
				"publish_started": "2020-04-26T09:51:03-00:00",
				"publish_completed": "2020-04-26T10:01:28Z"
			}
		}
	}`
	fullImagePayload = fmt.Sprintf(fullImagePayloadFmt, testImageID2, testCollectionID1, models.StatePublished.String())
)

// DB model corresponding to an image in the provided state, without any download variant
func dbImage(state models.State) *models.Image {
	return dbImageWithID(state, testImageID1)
}

func dbImageWithID(state models.State, id string) *models.Image {
	return &models.Image{
		ID:           id,
		CollectionID: testCollectionID1,
		Filename:     "some-image-name",
		License: &models.License{
			Title: testLicence,
			Href:  testLicenceHref,
		},
		Links: &models.ImageLinks{
			Self:      fmt.Sprintf("http://example.com/images/%s", id),
			Downloads: fmt.Sprintf("http://example.com/images/%s/downloads", id),
		},
		Type:  testType,
		State: state.String(),
	}
}

func dbDownload(variantState models.DownloadState) models.Download {
	return dbDownloadWithID("", testVariantOriginal, variantState)
}

func dbDownloadWithID(id, variant string, state models.DownloadState) models.Download {
	download := models.Download{
		ID:    variant,
		Type:  "originally uploaded file",
		State: state.String(),
		Links: &models.DownloadLinks{
			Self:  fmt.Sprintf("http://example.com/images/%s/downloads/%s", id, variant),
			Image: fmt.Sprintf("http://example.com/images/%s", id),
		},
	}
	if state != models.StateDownloadImporting {
		download.Size = &testSize
		download.Width = &testWidth
		download.Height = &testHeight
		download.Href = dbDownloadHRef(state)
		download.Private = "my-private-bucket"
	}
	if state == models.StateDownloadImporting {
		download.ImportStarted = &testImportStarted
	}
	if state == models.StateDownloadImported {
		download.ImportStarted = &testImportStarted
		download.ImportCompleted = &testImportCompleted
	}
	if state == models.StateDownloadPublished {
		download.ImportStarted = &testImportStarted
		download.ImportCompleted = &testImportCompleted
		download.PublishStarted = &testPublishStarted
	}
	if state == models.StateDownloadPublished {
		download.ImportStarted = &testImportStarted
		download.ImportCompleted = &testImportCompleted
		download.PublishStarted = &testPublishStarted
		download.PublishCompleted = &testPublishCompleted
	}
	return download
}

func dbDownloadHRef(variantState models.DownloadState) string {
	switch variantState {
	case models.StateDownloadPublished:
		return testPrivateHref
	default:
		return ""
	}
}

// DB model corresponding to an image in the provided state, with a download variant in the provided state.
func dbFullImage(state models.State) *models.Image {
	return &models.Image{
		ID:           testImageID2,
		CollectionID: testCollectionID1,
		Filename:     testFilename,
		License: &models.License{
			Title: testLicence,
			Href:  testLicenceHref,
		},
		Links: &models.ImageLinks{
			Self:      fmt.Sprintf("http://example.com/images/%s", testImageID2),
			Downloads: fmt.Sprintf("http://example.com/images/%s/downloads", testImageID2),
		},
		Type:  testType,
		State: state.String(),
		Upload: &models.Upload{
			Path: testUploadPath,
		},
	}
}

func dbFullImageWithDownloads(state models.State, downloads ...models.Download) *models.Image {
	image := dbFullImage(state)
	image.Downloads = map[string]models.Download{}
	for i := range downloads {
		image.Downloads[downloads[i].ID] = downloads[i]
	}
	return image
}

// API model corresponding to an image in the provided state
func apiFullImage(state models.State) *models.Image {
	return &models.Image{
		ID:           testImageID2,
		CollectionID: testCollectionID1,
		Filename:     testFilename,
		License: &models.License{
			Title: testLicence,
			Href:  testLicenceHref,
		},
		Links: &models.ImageLinks{
			Self:      fmt.Sprintf("http://example.com/images/%s", testImageID2),
			Downloads: fmt.Sprintf("http://example.com/images/%s/downloads", testImageID2),
		},
		Type:  testType,
		State: state.String(),
		Upload: &models.Upload{
			Path: testUploadPath,
		},
	}
}

// DB model corresponding to an image in uploaded state
func dbUploadedImage() *models.Image {
	return dbImage(models.StateUploaded)
}

// API model corresponding to dbCreatedImage
func createdImage() *models.Image {
	return dbImage(models.StateCreated)
}

// API model corresponding to dbImage with StateImported
func importedImage() *models.Image {
	return dbImage(models.StateImported)
}

// DB model corresponding to an image in created state without collectionID
func dbCreatedImageNoCollectionID() *models.Image {
	return &models.Image{
		ID:       testImageID1,
		Filename: testFilename,
		License: &models.License{
			Title: testLicence,
			Href:  testLicenceHref,
		},
		Type:  testType,
		State: models.StateCreated.String(),
	}
}

// API model corresponding to dbCreatedImageNoCollectionID
func createdImageNoCollectionID() *models.Image {
	return dbCreatedImageNoCollectionID()
}

var imagesWithCollectionID1 = models.Images{
	Items:      []models.Image{*createdImage(), *importedImage(), *apiFullImage(models.StatePublished)},
	Count:      3,
	Limit:      3,
	TotalCount: 3,
	Offset:     0,
}

var allImages = models.Images{
	Items:      []models.Image{*createdImage(), *createdImageNoCollectionID(), *importedImage(), *apiFullImage(models.StatePublished)},
	Count:      4,
	Limit:      4,
	TotalCount: 4,
	Offset:     0,
}

var emptyImages = models.Images{
	Items:      []models.Image{},
	Count:      0,
	Limit:      0,
	TotalCount: 0,
	Offset:     0,
}

// API response for GET …/downloads
func apiGetDownloadsResponse(downloads ...models.Download) *models.Downloads {
	lenDownloads := len(downloads)
	downloadsList := make([]models.Download, 0, lenDownloads)
	downloadsList = append(downloadsList, downloads...)
	return &models.Downloads{
		Count:      lenDownloads,
		Offset:     0,
		Limit:      lenDownloads,
		Items:      downloadsList,
		TotalCount: lenDownloads,
	}
}

// Sorting functions for sorting a list of Downloads by their ID
type downloadsByID []models.Download

func (s downloadsByID) Len() int           { return len(s) }
func (s downloadsByID) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s downloadsByID) Less(i, j int) bool { return s[i].ID < s[j].ID }

// kafkaProducer mock which exposes Channels function returning empty channels
// to be used on tests that are not supposed to send any kafka message
var kafkaStubProducer = &kafkatest.IProducerMock{
	SendFunc: func(ctx context.Context, schema *avro.Schema, event interface{}) error {
		return nil
	},
}

func TestGetImagesHandler(t *testing.T) {
	Convey("Given an image API in publishing mode", t, func() {
		cfg, err := config.Get()
		cfg.IsPublishing = true
		So(err, ShouldBeNil)
		doTestGetImagesHandler()
	})

	Convey("Given an image API in web mode", t, func() {
		cfg, err := config.Get()
		cfg.IsPublishing = false
		So(err, ShouldBeNil)
		doTestGetImagesHandler()
	})
}

func doTestGetImagesHandler() {
	cfg, err := config.Get()
	So(err, ShouldBeNil)

	Convey("And an image API with mongoDB returning the images as expected according to the collectionID filter", func() {
		mongoDBMock := &mock.MongoServerMock{
			GetImagesFunc: func(ctx context.Context, collectionID string) ([]models.Image, error) {
				switch collectionID {
				case testCollectionID1:
					return []models.Image{*dbImage(models.StateCreated), *dbImage(models.StateImported), *dbFullImageWithDownloads(models.StatePublished, dbDownload(models.StateDownloadPublished))}, nil
				case "":
					return []models.Image{*dbImage(models.StateCreated), *dbCreatedImageNoCollectionID(), *dbImage(models.StateImported), *dbFullImageWithDownloads(models.StatePublished, dbDownload(models.StateDownloadPublished))}, nil
				default:
					return []models.Image{}, nil
				}
			},
		}
		authHandlerMock := &mock.AuthHandlerMock{
			RequireFunc: func(required dpauth.Permissions, handler http.HandlerFunc) http.HandlerFunc {
				return handler
			},
		}
		imageAPI := GetAPIWithMocks(cfg, mongoDBMock, authHandlerMock, kafkaStubProducer, kafkaStubProducer)

		Convey("When existing images are requested with a valid Collection-Id context and query parameter value", func() {
			r := httptest.NewRequest(http.MethodGet, fmt.Sprintf("http://localhost:24700/images?collection_id=%s", testCollectionID1), http.NoBody)
			r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
			w := httptest.NewRecorder()
			imageAPI.Router.ServeHTTP(w, r)

			Convey("Then the expected images are returned with status code 200", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
				So(w.Header().Get(contentTypeKey), ShouldEqual, contentTypeJSON)
				payload, err := io.ReadAll(w.Body)
				So(err, ShouldBeNil)
				retImages := models.Images{}
				err = json.Unmarshal(payload, &retImages)
				So(err, ShouldBeNil)
				So(retImages, ShouldResemble, imagesWithCollectionID1)
			})
		})

		Convey("When nonexistent images are requested with a valid Collection-Id context and query parameter value", func() {
			r := httptest.NewRequest(http.MethodGet, fmt.Sprintf("http://localhost:24700/images?collection_id=%s", "otherCollectionId"), http.NoBody)
			r = r.WithContext(context.WithValue(r.Context(), handlers.CollectionID.Context(), "otherCollectionId"))
			r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
			w := httptest.NewRecorder()
			imageAPI.Router.ServeHTTP(w, r)

			Convey("Then an empty list of images is returned with status code 200", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
				So(w.Header().Get(contentTypeKey), ShouldEqual, contentTypeJSON)
				payload, err := io.ReadAll(w.Body)
				So(err, ShouldBeNil)
				retImages := models.Images{}
				err = json.Unmarshal(payload, &retImages)
				So(err, ShouldBeNil)
				So(retImages, ShouldResemble, emptyImages)
			})
		})

		Convey("When no collection_id query parameter value is provided", func() {
			r := httptest.NewRequest(http.MethodGet, "http://localhost:24700/images", http.NoBody)
			r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
			w := httptest.NewRecorder()
			imageAPI.Router.ServeHTTP(w, r)

			Convey("Then the full list of images is returned with status code 200", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
				So(w.Header().Get(contentTypeKey), ShouldEqual, contentTypeJSON)
				payload, err := io.ReadAll(w.Body)
				So(err, ShouldBeNil)
				retImages := models.Images{}
				err = json.Unmarshal(payload, &retImages)
				So(err, ShouldBeNil)
				So(retImages, ShouldResemble, allImages)
			})
		})

		Convey("Providing different values for Collection-Id header and query parameter is allowed", func() {
			r := httptest.NewRequest(http.MethodGet, fmt.Sprintf("http://localhost:24700/images?collection_id=%s", testCollectionID1), http.NoBody)
			r = r.WithContext(context.WithValue(r.Context(), handlers.CollectionID.Context(), "otherCollectionId"))
			r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
			w := httptest.NewRecorder()
			imageAPI.Router.ServeHTTP(w, r)
			So(w.Code, ShouldEqual, http.StatusOK)
			So(w.Header().Get(contentTypeKey), ShouldEqual, contentTypeJSON)
		})

		Convey("When the request headers for host, prefix and protocol are set", func() {
			r := httptest.NewRequest(http.MethodGet, fmt.Sprintf("http://localhost:24700/images?collection_id=%s", testCollectionID1), http.NoBody)
			r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
			r.Header.Add("X-Forwarded-Proto", expectedProto)
			r.Header.Add("X-Forwarded-Host", expectedHost)
			r.Header.Add("X-Forwarded-Path-Prefix", expectedPathPrefix)

			w := httptest.NewRecorder()

			Convey("And URL rewriting is enabled", func() {
				cfg.EnableURLRewriting = true

				imageAPI := GetAPIWithMocks(cfg, mongoDBMock, authHandlerMock, kafkaStubProducer, kafkaStubProducer)

				imageAPI.Router.ServeHTTP(w, r)

				Convey("Then the response body should contain the rewritten links", func() {
					var images models.Images
					err := json.Unmarshal(w.Body.Bytes(), &images)
					So(err, ShouldBeNil)

					So(images.Items[0].Links.Self, ShouldEqual, fmt.Sprintf("%s://%s/%s/images/%s", expectedProto, expectedHost, expectedPathPrefix, testImageID1))
					So(images.Items[0].Links.Downloads, ShouldEqual, fmt.Sprintf("%s://%s/%s/images/%s/downloads", expectedProto, expectedHost, expectedPathPrefix, testImageID1))
				})
			})

			Convey("And URL rewriting is disabled", func() {
				cfg.EnableURLRewriting = false

				imageAPI := GetAPIWithMocks(cfg, mongoDBMock, authHandlerMock, kafkaStubProducer, kafkaStubProducer)

				imageAPI.Router.ServeHTTP(w, r)

				Convey("Then the response body should not contain the rewritten links", func() {
					var images models.Images
					err := json.Unmarshal(w.Body.Bytes(), &images)
					So(err, ShouldBeNil)

					So(images.Items[0].Links.Self, ShouldEqual, fmt.Sprintf("http://example.com/images/%s", testImageID1))
					So(images.Items[0].Links.Downloads, ShouldEqual, fmt.Sprintf("http://example.com/images/%s/downloads", testImageID1))
				})
			})
		})

		Convey("When some images have empty or missing links", func() {
			r := httptest.NewRequest(http.MethodGet, fmt.Sprintf("http://localhost:24700/images?collection_id=%s", testCollectionID1), http.NoBody)
			r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
			r.Header.Add("X-Forwarded-Proto", expectedProto)
			r.Header.Add("X-Forwarded-Host", expectedHost)
			r.Header.Add("X-Forwarded-Path-Prefix", expectedPathPrefix)

			w := httptest.NewRecorder()

			cfg.EnableURLRewriting = true

			image := dbFullImage(models.StateUploaded)

			mongoDBMock := &mock.MongoServerMock{
				GetImagesFunc: func(ctx context.Context, collectionID string) ([]models.Image, error) {
					return []models.Image{*image, {
						ID:       testImageID1,
						Filename: "image-without-links",
						Links: &models.ImageLinks{
							Self:      "",
							Downloads: "",
						},
					}}, nil
				},
			}

			imageAPI := GetAPIWithMocks(cfg, mongoDBMock, authHandlerMock, kafkaStubProducer, kafkaStubProducer)

			imageAPI.Router.ServeHTTP(w, r)

			Convey("Then the image with missing links should be skipped without error", func() {
				var images models.Images
				err := json.Unmarshal(w.Body.Bytes(), &images)
				So(err, ShouldBeNil)

				So(images.Items[0].Links.Self, ShouldEqual, fmt.Sprintf("%s://%s/%s/images/%s", expectedProto, expectedHost, expectedPathPrefix, testImageID2))
				So(images.Items[0].Links.Downloads, ShouldEqual, fmt.Sprintf("%s://%s/%s/images/%s/downloads", expectedProto, expectedHost, expectedPathPrefix, testImageID2))

				So(images.Items[1].Links.Self, ShouldBeBlank)
				So(images.Items[1].Links.Downloads, ShouldBeBlank)
				So(w.Code, ShouldEqual, http.StatusOK)
			})
		})
	})

	Convey("And an image API with mongoDB returning an error", func() {
		mongoDBMock := &mock.MongoServerMock{
			GetImagesFunc: func(ctx context.Context, collectionID string) ([]models.Image, error) {
				return []models.Image{}, errMongoDB
			},
		}
		authHandlerMock := &mock.AuthHandlerMock{
			RequireFunc: func(required dpauth.Permissions, handler http.HandlerFunc) http.HandlerFunc {
				return handler
			},
		}
		imageAPI := GetAPIWithMocks(cfg, mongoDBMock, authHandlerMock, kafkaStubProducer, kafkaStubProducer)

		Convey("Then when images are requested, a 500 error is returned", func() {
			r := httptest.NewRequest(http.MethodGet, "http://localhost:24700/images", http.NoBody)
			r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
			w := httptest.NewRecorder()
			imageAPI.Router.ServeHTTP(w, r)
			So(w.Code, ShouldEqual, http.StatusInternalServerError)
		})
	})
}

func TestCreateImageHandler(t *testing.T) {
	api.NewID = func() string { return testImageID1 }

	Convey("Given an image API in publishing mode that can successfully store valid images in mongoDB", t, func() {
		cfg, err := config.Get()
		So(err, ShouldBeNil)
		cfg.IsPublishing = true

		mongoDBMock := &mock.MongoServerMock{
			UpsertImageFunc: func(ctx context.Context, id string, image *models.Image) error { return nil },
		}

		authHandlerMock := &mock.AuthHandlerMock{
			RequireFunc: func(required dpauth.Permissions, handler http.HandlerFunc) http.HandlerFunc {
				return handler
			},
		}

		imageAPI := GetAPIWithMocks(cfg, mongoDBMock, authHandlerMock, kafkaStubProducer, kafkaStubProducer)

		Convey("When a valid new image is posted", func() {
			r := httptest.NewRequest(http.MethodPost, "http://localhost:24700/images", bytes.NewBufferString(
				fmt.Sprintf(newImagePayloadFmt, testCollectionID1, testFilename)))
			r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
			w := httptest.NewRecorder()
			imageAPI.Router.ServeHTTP(w, r)

			Convey("Then a newly created image with the new id and provided details is returned with status code 201", func() {
				So(w.Code, ShouldEqual, http.StatusCreated)
				So(w.Header().Get(contentTypeKey), ShouldEqual, contentTypeJSON)
				payload, err := io.ReadAll(w.Body)
				So(err, ShouldBeNil)
				retImage := models.Image{}
				err = json.Unmarshal(payload, &retImage)
				So(err, ShouldBeNil)
				So(retImage, ShouldResemble, *createdImage())
			})
		})

		Convey("When a valid new image with extra fields is posted", func() {
			r := httptest.NewRequest(http.MethodPost, "http://localhost:24700/images",
				bytes.NewBufferString(fmt.Sprintf(fullImagePayloadFmt, testImageID2, testCollectionID1, models.StateCreated.String())))
			r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
			w := httptest.NewRecorder()
			imageAPI.Router.ServeHTTP(w, r)
			Convey("Then a newly created image with the new id and provided details is returned with status code 201, ignoring any field that is not supposed to be provided at creation time", func() {
				So(w.Code, ShouldEqual, http.StatusCreated)
				So(w.Header().Get(contentTypeKey), ShouldEqual, contentTypeJSON)
				payload, err := io.ReadAll(w.Body)
				So(err, ShouldBeNil)
				retImage := models.Image{}
				err = json.Unmarshal(payload, &retImage)
				So(err, ShouldBeNil)
				So(retImage, ShouldResemble, *createdImage())
			})
		})

		Convey("When a new image with an invalid state field is posted", func() {
			r := httptest.NewRequest(http.MethodPost, "http://localhost:24700/images", bytes.NewBufferString(
				fmt.Sprintf(newImageWithStatePayloadFmt, testCollectionID1, "invalidState")))
			r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
			w := httptest.NewRecorder()
			imageAPI.Router.ServeHTTP(w, r)
			Convey("Then an BadRequest response is returned", func() {
				So(w.Code, ShouldEqual, http.StatusBadRequest)
			})
		})

		Convey("Posting an image with a filename longer than the maximum allowed results in BadRequest response", func() {
			r := httptest.NewRequest(http.MethodPost, "http://localhost:24700/images", bytes.NewBufferString(
				fmt.Sprintf(newImagePayloadFmt, testCollectionID1, longName)))
			r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
			w := httptest.NewRecorder()
			imageAPI.Router.ServeHTTP(w, r)
			So(w.Code, ShouldEqual, http.StatusBadRequest)
		})

		Convey("Posting an empty image (without collection id) results in BadRequest response", func() {
			r := httptest.NewRequest(http.MethodPost, "http://localhost:24700/images", bytes.NewBufferString(emptyJSONPayload))
			r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
			w := httptest.NewRecorder()
			imageAPI.Router.ServeHTTP(w, r)
			So(w.Code, ShouldEqual, http.StatusBadRequest)
		})

		Convey("An empty request body results in a BadRequest response", func() {
			r := httptest.NewRequest(http.MethodPost, "http://localhost:24700/images", http.NoBody)
			r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
			w := httptest.NewRecorder()
			imageAPI.Router.ServeHTTP(w, r)
			So(w.Code, ShouldEqual, http.StatusBadRequest)
		})

		Convey("An invalid request body results in a BadRequest response", func() {
			r := httptest.NewRequest(http.MethodPost, "http://localhost:24700/images", bytes.NewBufferString("invalidJson"))
			r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
			w := httptest.NewRecorder()
			imageAPI.Router.ServeHTTP(w, r)
			So(w.Code, ShouldEqual, http.StatusBadRequest)
		})
	})

	Convey("Given an image API with mongoDB that fails to insert images", t, func() {
		cfg, err := config.Get()
		So(err, ShouldBeNil)
		cfg.IsPublishing = true

		mongoDBMock := &mock.MongoServerMock{
			UpsertImageFunc: func(ctx context.Context, id string, image *models.Image) error { return errMongoDB },
		}

		authHandlerMock := &mock.AuthHandlerMock{
			RequireFunc: func(required dpauth.Permissions, handler http.HandlerFunc) http.HandlerFunc {
				return handler
			},
		}
		imageAPI := GetAPIWithMocks(cfg, mongoDBMock, authHandlerMock, kafkaStubProducer, kafkaStubProducer)

		Convey("When a new image is posted a 500 InternalServerError status code is returned", func() {
			r := httptest.NewRequest(http.MethodPost, "http://localhost:24700/images", bytes.NewBufferString(
				fmt.Sprintf(newImagePayloadFmt, testCollectionID1, testFilename)))
			r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
			w := httptest.NewRecorder()
			imageAPI.Router.ServeHTTP(w, r)
			So(w.Code, ShouldEqual, http.StatusInternalServerError)
		})
	})
}

func TestGetImageHandler(t *testing.T) {
	Convey("Given an image API in publishing mode", t, func() {
		cfg, err := config.Get()
		cfg.IsPublishing = true
		So(err, ShouldBeNil)
		doTestGetImageHandler(cfg)
	})

	Convey("Given an image API in web mode", t, func() {
		cfg, err := config.Get()
		cfg.IsPublishing = false
		So(err, ShouldBeNil)
		doTestGetImageHandler(cfg)
	})
}

func doTestGetImageHandler(cfg *config.Config) {
	Convey("And an image API with mongoDB returning 'created' and 'published' images", func() {
		mongoDBMock := &mock.MongoServerMock{
			GetImageFunc: func(ctx context.Context, id string) (*models.Image, error) {
				switch id {
				case testImageID1:
					return dbImage(models.StateCreated), nil
				case testImageID2:
					return dbFullImageWithDownloads(models.StatePublished, dbDownload(models.StateDownloadPublished)), nil
				default:
					return nil, apierrors.ErrImageNotFound
				}
			},
		}
		authHandlerMock := &mock.AuthHandlerMock{
			RequireFunc: func(required dpauth.Permissions, handler http.HandlerFunc) http.HandlerFunc {
				return handler
			},
		}
		imageAPI := GetAPIWithMocks(cfg, mongoDBMock, authHandlerMock, kafkaStubProducer, kafkaStubProducer)

		Convey("When an existing 'created' image is requested with the valid Collection-Id context value", func() {
			r := httptest.NewRequest(http.MethodGet, fmt.Sprintf("http://localhost:24700/images/%s", testImageID1), http.NoBody)
			r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
			w := httptest.NewRecorder()
			imageAPI.Router.ServeHTTP(w, r)
			Convey("Then the expected image is returned with status code 200", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
				So(w.Header().Get(contentTypeKey), ShouldEqual, contentTypeJSON)
				payload, err := io.ReadAll(w.Body)
				So(err, ShouldBeNil)
				retImage := models.Image{}
				err = json.Unmarshal(payload, &retImage)
				So(err, ShouldBeNil)
				So(retImage, ShouldResemble, *createdImage())
			})
		})

		Convey("When an existing 'published' image is requested without a Collection-Id context value", func() {
			r := httptest.NewRequest(http.MethodGet, fmt.Sprintf("http://localhost:24700/images/%s", testImageID2), http.NoBody)
			r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
			w := httptest.NewRecorder()
			imageAPI.Router.ServeHTTP(w, r)
			Convey("Then the published image is returned with status code 200", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
				So(w.Header().Get(contentTypeKey), ShouldEqual, contentTypeJSON)
				payload, err := io.ReadAll(w.Body)
				So(err, ShouldBeNil)
				retImage := models.Image{}
				err = json.Unmarshal(payload, &retImage)
				So(err, ShouldBeNil)
				So(retImage, ShouldResemble, *apiFullImage(models.StatePublished))
			})
		})

		Convey("Requesting an nonexistent image ID results in a NotFound response", func() {
			r := httptest.NewRequest(http.MethodGet, "http://localhost:24700/images/inexistent", http.NoBody)
			r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
			w := httptest.NewRecorder()
			imageAPI.Router.ServeHTTP(w, r)
			So(w.Code, ShouldEqual, http.StatusNotFound)
		})

		Convey("When the request headers for host, prefix and protocol are set", func() {
			r := httptest.NewRequest(http.MethodGet, fmt.Sprintf("http://localhost:24700/images/%s", testImageID2), http.NoBody)
			r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
			r.Header.Add("X-Forwarded-Proto", expectedProto)
			r.Header.Add("X-Forwarded-Host", expectedHost)
			r.Header.Add("X-Forwarded-Path-Prefix", expectedPathPrefix)

			w := httptest.NewRecorder()

			Convey("And URL rewriting is enabled", func() {
				cfg.EnableURLRewriting = true

				imageAPI := GetAPIWithMocks(cfg, mongoDBMock, authHandlerMock, kafkaStubProducer, kafkaStubProducer)

				imageAPI.Router.ServeHTTP(w, r)

				Convey("Then the response body should contain the rewritten links", func() {
					var image models.Image
					err := json.Unmarshal(w.Body.Bytes(), &image)
					So(err, ShouldBeNil)

					So(image.Links.Self, ShouldEqual, fmt.Sprintf("%s://%s/%s/images/%s", expectedProto, expectedHost, expectedPathPrefix, testImageID2))
					So(image.Links.Downloads, ShouldEqual, fmt.Sprintf("%s://%s/%s/images/%s/downloads", expectedProto, expectedHost, expectedPathPrefix, testImageID2))
				})
			})

			Convey("And URL rewriting is disabled", func() {
				cfg.EnableURLRewriting = false

				imageAPI := GetAPIWithMocks(cfg, mongoDBMock, authHandlerMock, kafkaStubProducer, kafkaStubProducer)

				imageAPI.Router.ServeHTTP(w, r)

				Convey("Then the response body should not contain the rewritten links", func() {
					var image models.Image
					err := json.Unmarshal(w.Body.Bytes(), &image)
					So(err, ShouldBeNil)

					So(image.Links.Self, ShouldEqual, fmt.Sprintf("http://example.com/images/%s", testImageID2))
					So(image.Links.Downloads, ShouldEqual, fmt.Sprintf("http://example.com/images/%s/downloads", testImageID2))
				})
			})
		})
	})
}

func TestUpdateImageHandler(t *testing.T) {
	Convey("Given a valid config, auth handler, kafka producer", t, func() {
		cfg, err := config.Get()
		So(err, ShouldBeNil)
		authHandlerMock := &mock.AuthHandlerMock{
			RequireFunc: func(required dpauth.Permissions, handler http.HandlerFunc) http.HandlerFunc {
				return handler
			},
		}

		Convey("And an empty MongoDB mock", func() {
			mongoMock := &mock.MongoServerMock{}
			imageAPI := GetAPIWithMocks(cfg, mongoMock, authHandlerMock, kafkaStubProducer, kafkaStubProducer)

			Convey("Calling update image with an invalid body results in 400 response", func() {
				r := httptest.NewRequest(http.MethodPut, fmt.Sprintf("http://localhost:24700/images/%s", testImageID1), bytes.NewBufferString("wrong"))
				r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
				w := httptest.NewRecorder()
				imageAPI.Router.ServeHTTP(w, r)
				So(w.Code, ShouldEqual, http.StatusBadRequest)
			})

			Convey("Calling update image with an image that has a different id results in 400 response", func() {
				r := httptest.NewRequest(http.MethodPut, fmt.Sprintf("http://localhost:24700/images/%s", testImageID1), bytes.NewBufferString(fullImagePayload))
				r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
				w := httptest.NewRecorder()
				imageAPI.Router.ServeHTTP(w, r)
				So(w.Code, ShouldEqual, http.StatusBadRequest)
			})

			Convey("Calling update image with an image that has a filename that is too long results in 400 response", func() {
				r := httptest.NewRequest(http.MethodPut, fmt.Sprintf("http://localhost:24700/images/%s", testImageID1), bytes.NewBufferString(
					fmt.Sprintf(`{"filename": "%q"}`, longName)))
				r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
				w := httptest.NewRecorder()
				imageAPI.Router.ServeHTTP(w, r)
				So(w.Code, ShouldEqual, http.StatusBadRequest)
			})
		})

		Convey("And an image in created state in MongoDB", func() {
			mongoDBMock := &mock.MongoServerMock{
				GetImageFunc: func(ctx context.Context, id string) (*models.Image, error) {
					return dbImage(models.StateCreated), nil
				},
				UpdateImageFunc: func(ctx context.Context, id string, image *models.Image) (bool, error) {
					return true, nil
				},
				AcquireImageLockFunc: func(ctx context.Context, id string) (string, error) { return testLockID, nil },
				UnlockImageFunc:      func(ctx context.Context, id string) {},
			}
			imageAPI := GetAPIWithMocks(cfg, mongoDBMock, authHandlerMock, kafkaStubProducer, kafkaStubProducer)

			Convey("Calling update image with same state results in 403 Forbidden response and it is not updated", func() {
				r := httptest.NewRequest(http.MethodPut, fmt.Sprintf("http://localhost:24700/images/%s", testImageID2), bytes.NewBufferString(
					fmt.Sprintf(newImageWithStatePayloadFmt, testCollectionID1, models.StateCreated.String())))
				r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
				w := httptest.NewRecorder()
				imageAPI.Router.ServeHTTP(w, r)
				So(w.Code, ShouldEqual, http.StatusForbidden)
				So(mongoDBMock.GetImageCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.GetImageCalls()[0].ID, ShouldEqual, testImageID2)
				So(mongoDBMock.AcquireImageLockCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.UnlockImageCalls(), ShouldHaveLength, 1)
			})
		})

		Convey("And MongoDB returning imageNotFound error", func() {
			mongoDBMock := &mock.MongoServerMock{
				GetImageFunc: func(ctx context.Context, id string) (*models.Image, error) {
					return nil, apierrors.ErrImageNotFound
				},
				AcquireImageLockFunc: func(ctx context.Context, id string) (string, error) { return testLockID, nil },
				UnlockImageFunc:      func(ctx context.Context, id string) {},
			}
			imageAPI := GetAPIWithMocks(cfg, mongoDBMock, authHandlerMock, kafkaStubProducer, kafkaStubProducer)

			Convey("Calling update image with an nonexistent image id results in 404 response", func() {
				r := httptest.NewRequest(http.MethodPut, fmt.Sprintf("http://localhost:24700/images/%s", "nonexistent"), bytes.NewBufferString(
					fmt.Sprintf(newImageWithStatePayloadFmt, testCollectionID1, models.StateCreated.String())))
				r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
				w := httptest.NewRecorder()
				imageAPI.Router.ServeHTTP(w, r)
				So(w.Code, ShouldEqual, http.StatusNotFound)
				So(mongoDBMock.GetImageCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.GetImageCalls()[0].ID, ShouldEqual, "nonexistent")
				So(mongoDBMock.AcquireImageLockCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.UnlockImageCalls(), ShouldHaveLength, 1)
			})
		})

		Convey("And MongoDB failing to get an image", func() {
			mongoDBMock := &mock.MongoServerMock{
				GetImageFunc: func(ctx context.Context, id string) (*models.Image, error) {
					return nil, errors.New("internal mongoDB error")
				},
				AcquireImageLockFunc: func(ctx context.Context, id string) (string, error) { return testLockID, nil },
				UnlockImageFunc:      func(ctx context.Context, id string) {},
			}
			imageAPI := GetAPIWithMocks(cfg, mongoDBMock, authHandlerMock, kafkaStubProducer, kafkaStubProducer)

			Convey("Calling update image results in 500 response", func() {
				r := httptest.NewRequest(http.MethodPut, fmt.Sprintf("http://localhost:24700/images/%s", testImageID1), bytes.NewBufferString(
					fmt.Sprintf(newImageWithStatePayloadFmt, testCollectionID1, models.StateCreated.String())))
				r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
				w := httptest.NewRecorder()
				imageAPI.Router.ServeHTTP(w, r)
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(mongoDBMock.GetImageCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.GetImageCalls()[0].ID, ShouldEqual, testImageID1)
				So(mongoDBMock.AcquireImageLockCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.UnlockImageCalls(), ShouldHaveLength, 1)
			})
		})

		Convey("And an API with a mongoDB containing an image that fails to update", func() {
			mongoDBMock := &mock.MongoServerMock{
				GetImageFunc: func(ctx context.Context, id string) (*models.Image, error) {
					return dbImage(models.StateImporting), nil
				},
				UpsertImageFunc: func(ctx context.Context, id string, image *models.Image) error {
					return errors.New("internal mongoDB error")
				},
				AcquireImageLockFunc: func(ctx context.Context, id string) (string, error) { return testLockID, nil },
				UnlockImageFunc:      func(ctx context.Context, id string) {},
			}
			imageAPI := GetAPIWithMocks(cfg, mongoDBMock, authHandlerMock, kafkaStubProducer, kafkaStubProducer)

			Convey("Calling update image results in 500 result", func() {
				r := httptest.NewRequest(http.MethodPut, fmt.Sprintf("http://localhost:24700/images/%s", testImageID1), bytes.NewBufferString(
					fmt.Sprintf(newImageWithStatePayloadFmt, testCollectionID1, models.StateFailedImport.String())))
				r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
				w := httptest.NewRecorder()
				imageAPI.Router.ServeHTTP(w, r)
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(mongoDBMock.GetImageCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.GetImageCalls()[0].ID, ShouldEqual, testImageID1)
				So(mongoDBMock.UpsertImageCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.UpsertImageCalls()[0].ID, ShouldEqual, testImageID1)
				So(mongoDBMock.AcquireImageLockCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.UnlockImageCalls(), ShouldHaveLength, 1)
			})
		})

		Convey("And an image in published state in MongoDB", func() {
			mongoDBMock := &mock.MongoServerMock{
				GetImageFunc: func(ctx context.Context, id string) (*models.Image, error) {
					return dbImage(models.StatePublished), nil
				},
				AcquireImageLockFunc: func(ctx context.Context, id string) (string, error) { return testLockID, nil },
				UnlockImageFunc:      func(ctx context.Context, id string) {},
			}
			imageAPI := GetAPIWithMocks(cfg, mongoDBMock, authHandlerMock, kafkaStubProducer, kafkaStubProducer)

			Convey("Calling update image results in 403 Forbidden response and it is not updated", func() {
				r := httptest.NewRequest(http.MethodPut, fmt.Sprintf("http://localhost:24700/images/%s", testImageID2), bytes.NewBufferString(
					fmt.Sprintf(newImageWithStatePayloadFmt, testImageID2, models.StateCreated.String())))
				r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
				w := httptest.NewRecorder()
				imageAPI.Router.ServeHTTP(w, r)
				So(w.Code, ShouldEqual, http.StatusForbidden)
				So(mongoDBMock.GetImageCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.GetImageCalls()[0].ID, ShouldEqual, testImageID2)
				So(mongoDBMock.AcquireImageLockCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.UnlockImageCalls(), ShouldHaveLength, 1)
			})
		})

		Convey("And an image in created state in MongoDB plus a kafka uploadedProducer", func() {
			uploadedProducer := &kafkatest.IProducerMock{
				SendFunc: func(ctx context.Context, schema *avro.Schema, event interface{}) error {
					return nil
				},
			}

			mongoDBMock := &mock.MongoServerMock{
				GetImageFunc: func(ctx context.Context, id string) (*models.Image, error) {
					return dbImageWithID(models.StateCreated, testImageID2), nil
				},
				UpsertImageFunc:      func(ctx context.Context, id string, image *models.Image) error { return nil },
				AcquireImageLockFunc: func(ctx context.Context, id string) (string, error) { return testLockID, nil },
				UnlockImageFunc:      func(ctx context.Context, id string) {},
			}
			imageAPI := GetAPIWithMocks(cfg, mongoDBMock, authHandlerMock, uploadedProducer, kafkaStubProducer)

			Convey("Calling image upload with a valid image upload results in 200 OK response with the expected image provided to mongoDB and the message sent to kafka producer", func() {
				r := httptest.NewRequest(http.MethodPut, fmt.Sprintf("http://localhost:24700/images/%s", testImageID2), bytes.NewBufferString(
					fmt.Sprintf(imageUploadPayloadFmt, testCollectionID1, testUploadPath)))
				r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
				w := httptest.NewRecorder()
				imageAPI.Router.ServeHTTP(w, r)
				So(w.Code, ShouldEqual, http.StatusOK)
				So(w.Header().Get(contentTypeKey), ShouldEqual, contentTypeJSON)
				So(mongoDBMock.GetImageCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.GetImageCalls()[0].ID, ShouldEqual, testImageID2)
				So(mongoDBMock.UpsertImageCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.UpsertImageCalls()[0].ID, ShouldEqual, testImageID2)
				So(*mongoDBMock.UpsertImageCalls()[0].Image, ShouldResemble, *dbFullImage(models.StateUploaded))
				So(mongoDBMock.AcquireImageLockCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.UnlockImageCalls(), ShouldHaveLength, 1)

				fmt.Println("got line 996")

				Convey("And the expected avro event is sent to the corresponding kafka output channel", func() {
					So(uploadedProducer.SendCalls(), ShouldHaveLength, 1)
					expectedEvent := &event.ImageUploaded{
						ImageID:  testImageID2,
						Path:     testUploadFilename,
						Filename: testFilename,
					}
					So(uploadedProducer.SendCalls()[0].Event, ShouldResemble, expectedEvent)
				})
			})

			Convey("Calling image upload results in a 500 InternalError response when an invalid image uploaded event is generated, and the image is not updated in mongoDB", func() {
				uploadedProducerErr := &kafkatest.IProducerMock{
					SendFunc: func(ctx context.Context, schema *avro.Schema, event interface{}) error {
						return fmt.Errorf("blah")
					},
				}
				imageAPI := GetAPIWithMocks(cfg, mongoDBMock, authHandlerMock, uploadedProducerErr, kafkaStubProducer)
				r := httptest.NewRequest(http.MethodPut, fmt.Sprintf("http://localhost:24700/images/%s", testImageID2), bytes.NewBufferString(
					fmt.Sprintf(imageUploadPayloadFmt, testCollectionID1, testUploadPath)))
				r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
				r = r.WithContext(context.WithValue(r.Context(), handlers.CollectionID.Context(), testCollectionID1))
				w := httptest.NewRecorder()
				imageAPI.Router.ServeHTTP(w, r)
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(mongoDBMock.GetImageCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.GetImageCalls()[0].ID, ShouldEqual, testImageID2)
				So(mongoDBMock.UpdateImageCalls(), ShouldHaveLength, 0)
			})

			Convey("Calling image upload on an image that is already uploaded returns a 403 response.", func() {
				mongoDBMock := &mock.MongoServerMock{
					GetImageFunc: func(ctx context.Context, id string) (*models.Image, error) {
						return dbUploadedImage(), nil
					},
					AcquireImageLockFunc: func(ctx context.Context, id string) (string, error) { return testLockID, nil },
					UnlockImageFunc:      func(ctx context.Context, id string) {},
				}
				imageAPI := GetAPIWithMocks(cfg, mongoDBMock, authHandlerMock, kafkaStubProducer, kafkaStubProducer)

				Convey("Calling 'upload image' results in 403 Forbidden response", func() {
					r := httptest.NewRequest(http.MethodPut, fmt.Sprintf("http://localhost:24700/images/%s", testImageID2), bytes.NewBufferString(
						fmt.Sprintf(imageUploadPayloadFmt, testUploadPath)))
					r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
					r = r.WithContext(context.WithValue(r.Context(), handlers.CollectionID.Context(), testCollectionID1))
					w := httptest.NewRecorder()
					imageAPI.Router.ServeHTTP(w, r)
					So(w.Code, ShouldEqual, http.StatusForbidden)
					So(mongoDBMock.GetImageCalls(), ShouldHaveLength, 1)
					So(mongoDBMock.GetImageCalls()[0].ID, ShouldEqual, testImageID2)
					So(mongoDBMock.AcquireImageLockCalls(), ShouldHaveLength, 1)
					So(mongoDBMock.UnlockImageCalls(), ShouldHaveLength, 1)
				})
			})
		})
	})
}

func TestGetDownloadsHandler(t *testing.T) {
	Convey("Given an image API in publishing mode", t, func() {
		cfg, err := config.Get()
		cfg.IsPublishing = true
		So(err, ShouldBeNil)
		doTestGetDownloadsHandler()
	})

	Convey("Given an image API in web mode", t, func() {
		cfg, err := config.Get()
		cfg.IsPublishing = false
		So(err, ShouldBeNil)
		doTestGetDownloadsHandler()
	})
}

func doTestGetDownloadsHandler() {
	cfg, err := config.Get()
	So(err, ShouldBeNil)

	Convey("And an image API with existing valid images stored in a mongoDB mock", func() {
		cfg.IsPublishing = true

		firstDownload := dbDownloadWithID(testImagePublishedID, testVariantAlternative, models.StateDownloadPublished)
		secondDownload := dbDownloadWithID(testImagePublishedID, testVariantOriginal, models.StateDownloadPublished)
		mongoDBMock := &mock.MongoServerMock{
			GetImageFunc: func(ctx context.Context, id string) (*models.Image, error) {
				switch id {
				case testImageUploadedID:
					return dbFullImage(models.StateUploaded), nil
				case testImageImportingID:
					return dbFullImageWithDownloads(models.StateImporting, dbDownload(models.StateDownloadImported)), nil
				case testImagePublishedID:
					return dbFullImageWithDownloads(models.StatePublished, firstDownload, secondDownload), nil
				default:
					return nil, apierrors.ErrImageNotFound
				}
			},
		}

		authHandlerMock := &mock.AuthHandlerMock{
			RequireFunc: func(required dpauth.Permissions, handler http.HandlerFunc) http.HandlerFunc {
				return handler
			},
		}

		imageAPI := GetAPIWithMocks(cfg, mongoDBMock, authHandlerMock, kafkaStubProducer, kafkaStubProducer)

		Convey("When downloads are requested from an image with no downloads", func() {
			r := httptest.NewRequest(http.MethodGet, fmt.Sprintf("http://localhost:24700/images/%s/downloads", testImageUploadedID), http.NoBody)
			r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
			w := httptest.NewRecorder()
			imageAPI.Router.ServeHTTP(w, r)

			Convey("Then a valid Downloads response with 0 items is returned with status code 200", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
				So(w.Header().Get(contentTypeKey), ShouldEqual, contentTypeJSON)
				payload, err := io.ReadAll(w.Body)
				So(err, ShouldBeNil)
				retDownloads := models.Downloads{}
				err = json.Unmarshal(payload, &retDownloads)
				So(err, ShouldBeNil)
				So(retDownloads, ShouldResemble, *apiGetDownloadsResponse())
			})
		})

		Convey("When downloads are requested from an image with one download", func() {
			r := httptest.NewRequest(http.MethodGet, fmt.Sprintf("http://localhost:24700/images/%s/downloads", testImageImportingID), http.NoBody)
			r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
			w := httptest.NewRecorder()
			imageAPI.Router.ServeHTTP(w, r)

			Convey("Then a valid Downloads response with 1 item is returned with status code 200", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
				So(w.Header().Get(contentTypeKey), ShouldEqual, contentTypeJSON)
				payload, err := io.ReadAll(w.Body)
				So(err, ShouldBeNil)
				retDownloads := models.Downloads{}
				err = json.Unmarshal(payload, &retDownloads)
				So(err, ShouldBeNil)
				So(retDownloads, ShouldResemble, *apiGetDownloadsResponse(dbDownload(models.StateDownloadImported)))
			})
		})

		Convey("When downloads are requested from an image with two downloads", func() {
			r := httptest.NewRequest(http.MethodGet, fmt.Sprintf("http://localhost:24700/images/%s/downloads", testImagePublishedID), http.NoBody)
			r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
			w := httptest.NewRecorder()
			imageAPI.Router.ServeHTTP(w, r)

			Convey("Then a valid Downloads response with 2 items is returned with status code 200", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
				So(w.Header().Get(contentTypeKey), ShouldEqual, contentTypeJSON)
				payload, err := io.ReadAll(w.Body)
				So(err, ShouldBeNil)
				retDownloads := models.Downloads{}
				err = json.Unmarshal(payload, &retDownloads)
				So(err, ShouldBeNil)
				So(retDownloads.Count, ShouldEqual, 2)
				So(retDownloads.Offset, ShouldEqual, 0)
				So(retDownloads.Limit, ShouldEqual, 2)
				So(retDownloads.TotalCount, ShouldEqual, 2)

				items := retDownloads.Items
				So(items, ShouldHaveLength, 2)
				// sort the items because they are stored in a map so could come out in any order
				sort.Sort(downloadsByID(items))
				So(items[0], ShouldResemble, firstDownload)
				So(items[1], ShouldResemble, secondDownload)
			})
		})

		Convey("When downloads are requested from an image not in mongo", func() {
			r := httptest.NewRequest(http.MethodGet, fmt.Sprintf("http://localhost:24700/images/%s/downloads", testImageID1), http.NoBody)
			r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
			w := httptest.NewRecorder()
			imageAPI.Router.ServeHTTP(w, r)

			Convey("Then a status code 404 (NotFound) is returned", func() {
				So(w.Code, ShouldEqual, http.StatusNotFound)
			})
		})

		Convey("When the request headers for host, prefix and protocol are set", func() {
			r := httptest.NewRequest(http.MethodGet, fmt.Sprintf("http://localhost:24700/images/%s/downloads", testImagePublishedID), http.NoBody)
			r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
			r.Header.Add("X-Forwarded-Proto", expectedProto)
			r.Header.Add("X-Forwarded-Host", expectedHost)
			r.Header.Add("X-Forwarded-Path-Prefix", expectedPathPrefix)

			w := httptest.NewRecorder()

			image := dbDownloadWithID(testImagePublishedID, testVariantAlternative, models.StateDownloadPublished)
			mongoDBMock := &mock.MongoServerMock{
				GetImageFunc: func(ctx context.Context, id string) (*models.Image, error) {
					return dbFullImageWithDownloads(models.StatePublished, image), nil
				},
			}

			Convey("And URL rewriting is enabled", func() {
				cfg.EnableURLRewriting = true

				imageAPI := GetAPIWithMocks(cfg, mongoDBMock, authHandlerMock, kafkaStubProducer, kafkaStubProducer)

				imageAPI.Router.ServeHTTP(w, r)

				Convey("Then the response body should contain the rewritten links", func() {
					var downloads models.Downloads
					err := json.Unmarshal(w.Body.Bytes(), &downloads)
					So(err, ShouldBeNil)

					So(downloads.Items[0].Links.Self, ShouldEqual, fmt.Sprintf("%s://%s/%s/images/%s/downloads/%s", expectedProto, expectedHost, expectedPathPrefix, testImagePublishedID, testVariantAlternative))
					So(downloads.Items[0].Links.Image, ShouldEqual, fmt.Sprintf("%s://%s/%s/images/%s", expectedProto, expectedHost, expectedPathPrefix, testImagePublishedID))
				})
			})

			Convey("And URL rewriting is disabled", func() {
				cfg.EnableURLRewriting = false

				imageAPI := GetAPIWithMocks(cfg, mongoDBMock, authHandlerMock, kafkaStubProducer, kafkaStubProducer)

				imageAPI.Router.ServeHTTP(w, r)

				Convey("Then the response body should not contain the rewritten links", func() {
					var downloads models.Downloads
					err := json.Unmarshal(w.Body.Bytes(), &downloads)
					So(err, ShouldBeNil)

					So(downloads.Items[0].Links.Self, ShouldEqual, fmt.Sprintf("http://example.com/images/%s/downloads/%s", testImagePublishedID, testVariantAlternative))
					So(downloads.Items[0].Links.Image, ShouldEqual, fmt.Sprintf("http://example.com/images/%s", testImagePublishedID))
				})
			})
		})
	})

	Convey("And an image API with mongoDB returning an error", func() {
		mongoDBMock := &mock.MongoServerMock{
			GetImageFunc: func(ctx context.Context, id string) (*models.Image, error) {
				return nil, errMongoDB
			},
		}
		authHandlerMock := &mock.AuthHandlerMock{
			RequireFunc: func(required dpauth.Permissions, handler http.HandlerFunc) http.HandlerFunc {
				return handler
			},
		}
		imageAPI := GetAPIWithMocks(cfg, mongoDBMock, authHandlerMock, kafkaStubProducer, kafkaStubProducer)

		Convey("Then when images are requested, a 500 error is returned", func() {
			r := httptest.NewRequest(http.MethodGet, fmt.Sprintf("http://localhost:24700/images/%s/downloads", testImageUploadedID), http.NoBody)
			r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
			w := httptest.NewRecorder()
			imageAPI.Router.ServeHTTP(w, r)
			So(w.Code, ShouldEqual, http.StatusInternalServerError)
		})
	})
}

func TestCreateDownloadHandler(t *testing.T) {
	Convey("Given an image API in publishing mode with existing valid images stored in a mongoDB mock", t, func() {
		cfg, err := config.Get()
		So(err, ShouldBeNil)
		cfg.IsPublishing = true

		mongoDBMock := &mock.MongoServerMock{
			GetImageFunc: func(ctx context.Context, id string) (*models.Image, error) {
				switch id {
				case testImageCreatedID:
					return dbImage(models.StateCreated), nil
				case testImageUploadedID:
					return dbFullImage(models.StateUploaded), nil
				case testImageImportingID:
					return dbFullImageWithDownloads(models.StateImporting, dbDownload(models.StateDownloadImported)), nil
				case testImagePublishedID:
					return dbFullImageWithDownloads(models.StatePublished, dbDownload(models.StateDownloadPublished)), nil
				default:
					return nil, apierrors.ErrImageNotFound
				}
			},
			UpsertImageFunc:      func(ctx context.Context, id string, image *models.Image) error { return nil },
			AcquireImageLockFunc: func(ctx context.Context, id string) (string, error) { return testLockID, nil },
			UnlockImageFunc:      func(ctx context.Context, id string) {},
		}

		authHandlerMock := &mock.AuthHandlerMock{
			RequireFunc: func(required dpauth.Permissions, handler http.HandlerFunc) http.HandlerFunc {
				return handler
			},
		}

		imageAPI := GetAPIWithMocks(cfg, mongoDBMock, authHandlerMock, kafkaStubProducer, kafkaStubProducer)

		Convey("When a valid new download is posted to an image in an 'uploaded' state", func() {
			r := httptest.NewRequest(http.MethodPost, fmt.Sprintf("http://localhost:24700/images/%s/downloads", testImageUploadedID), bytes.NewBufferString(
				fmt.Sprintf(newImageDownloadPayloadFmt, testVariantOriginal, testDownloadType, models.StateDownloadImporting.String())))
			r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
			w := httptest.NewRecorder()
			imageAPI.Router.ServeHTTP(w, r)

			Convey("Then a newly created download is returned with status code 201", func() {
				So(w.Code, ShouldEqual, http.StatusCreated)
				So(w.Header().Get(contentTypeKey), ShouldEqual, contentTypeJSON)
				payload, err := io.ReadAll(w.Body)
				So(err, ShouldBeNil)
				retDownload := models.Download{}
				err = json.Unmarshal(payload, &retDownload)
				So(err, ShouldBeNil)
				So(retDownload, ShouldResemble, dbDownloadWithID(testImageUploadedID, testVariantOriginal, models.StateDownloadImporting))
			})
		})

		Convey("When a new download with an imported state is posted to an image in an 'uploaded' state", func() {
			r := httptest.NewRequest(http.MethodPost, fmt.Sprintf("http://localhost:24700/images/%s/downloads", testImageUploadedID), bytes.NewBufferString(
				fmt.Sprintf(newImageDownloadPayloadFmt, testVariantOriginal, testDownloadType, models.StateDownloadImported.String())))
			r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
			w := httptest.NewRecorder()
			imageAPI.Router.ServeHTTP(w, r)

			Convey("Then a status code 403 (Forbidden) is returned", func() {
				So(w.Code, ShouldEqual, http.StatusForbidden)
			})
		})

		Convey("When a new download with an invalid state is posted to an image in an 'uploaded' state", func() {
			r := httptest.NewRequest(http.MethodPost, fmt.Sprintf("http://localhost:24700/images/%s/downloads", testImageUploadedID), bytes.NewBufferString(
				fmt.Sprintf(newImageDownloadPayloadFmt, testVariantOriginal, testDownloadType, "stateWibbling")))
			r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
			w := httptest.NewRecorder()
			imageAPI.Router.ServeHTTP(w, r)

			Convey("Then a status code 400 (BadRequest) is returned", func() {
				So(w.Code, ShouldEqual, http.StatusBadRequest)
			})
		})

		Convey("When a valid new download is posted to an image in an 'importing' state", func() {
			r := httptest.NewRequest(http.MethodPost, fmt.Sprintf("http://localhost:24700/images/%s/downloads", testImageImportingID), bytes.NewBufferString(
				fmt.Sprintf(newImageDownloadPayloadFmt, testVariantAlternative, testDownloadType, models.StateDownloadImporting.String())))
			r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
			w := httptest.NewRecorder()
			imageAPI.Router.ServeHTTP(w, r)

			Convey("Then a newly created download is returned with status code 201", func() {
				So(w.Code, ShouldEqual, http.StatusCreated)
				So(w.Header().Get(contentTypeKey), ShouldEqual, contentTypeJSON)
				payload, err := io.ReadAll(w.Body)
				So(err, ShouldBeNil)
				retDownload := models.Download{}
				err = json.Unmarshal(payload, &retDownload)
				So(err, ShouldBeNil)
				So(retDownload, ShouldResemble, dbDownloadWithID(testImageImportingID, testVariantAlternative, models.StateDownloadImporting))
			})
		})

		Convey("When a download with an already existing ID is posted to an image in an 'importing' state", func() {
			r := httptest.NewRequest(http.MethodPost, fmt.Sprintf("http://localhost:24700/images/%s/downloads", testImageImportingID), bytes.NewBufferString(
				fmt.Sprintf(newImageDownloadPayloadFmt, testVariantOriginal, testDownloadType, models.StateDownloadImporting.String())))
			r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
			w := httptest.NewRecorder()
			imageAPI.Router.ServeHTTP(w, r)

			Convey("Then a status code 403 (Forbidden) is returned", func() {
				So(w.Code, ShouldEqual, http.StatusForbidden)
			})
		})

		Convey("When a valid new download is posted to an image in a 'published' state", func() {
			r := httptest.NewRequest(http.MethodPost, fmt.Sprintf("http://localhost:24700/images/%s/downloads", testImagePublishedID), bytes.NewBufferString(
				fmt.Sprintf(newImageDownloadPayloadFmt, testVariantAlternative, testDownloadType, models.StateDownloadImporting.String())))
			r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
			w := httptest.NewRecorder()
			imageAPI.Router.ServeHTTP(w, r)

			Convey("Then a status code 403 (Forbidden) is returned", func() {
				So(w.Code, ShouldEqual, http.StatusForbidden)
			})
		})

		Convey("When a valid new download is posted to an image with an ID not found in mongodb", func() {
			r := httptest.NewRequest(http.MethodPost, fmt.Sprintf("http://localhost:24700/images/%s/downloads", "nonExistantID"), bytes.NewBufferString(
				fmt.Sprintf(newImageDownloadPayloadFmt, testVariantAlternative, testDownloadType, models.StateDownloadImporting.String())))
			r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
			w := httptest.NewRecorder()
			imageAPI.Router.ServeHTTP(w, r)

			Convey("Then a status code 404 (NotFound) is returned", func() {
				So(w.Code, ShouldEqual, http.StatusNotFound)
			})
		})
	})

	Convey("Given an image API in publishing mode with a mongoDB that fails to update", t, func() {
		cfg, err := config.Get()
		So(err, ShouldBeNil)
		cfg.IsPublishing = true

		mongoDBMock := &mock.MongoServerMock{
			GetImageFunc:         func(ctx context.Context, id string) (*models.Image, error) { return dbImage(models.StateUploaded), nil },
			UpsertImageFunc:      func(ctx context.Context, id string, image *models.Image) error { return errMongoDB },
			AcquireImageLockFunc: func(ctx context.Context, id string) (string, error) { return testLockID, nil },
			UnlockImageFunc:      func(ctx context.Context, id string) {},
		}

		authHandlerMock := &mock.AuthHandlerMock{
			RequireFunc: func(required dpauth.Permissions, handler http.HandlerFunc) http.HandlerFunc {
				return handler
			},
		}

		imageAPI := GetAPIWithMocks(cfg, mongoDBMock, authHandlerMock, kafkaStubProducer, kafkaStubProducer)

		Convey("When a valid new download is posted to an image in a 'uploaded' state", func() {
			r := httptest.NewRequest(http.MethodPost, fmt.Sprintf("http://localhost:24700/images/%s/downloads", testImageUploadedID), bytes.NewBufferString(
				fmt.Sprintf(newImageDownloadPayloadFmt, testVariantOriginal, testDownloadType, models.StateDownloadImporting.String())))
			r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
			w := httptest.NewRecorder()
			imageAPI.Router.ServeHTTP(w, r)

			Convey("Then a status code 500 (InternalServerError) is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
			})
		})
	})

	Convey("Given an image API in publishing mode with a mongoDB that fails to lock", t, func() {
		cfg, err := config.Get()
		So(err, ShouldBeNil)
		cfg.IsPublishing = true

		mongoDBMock := &mock.MongoServerMock{
			GetImageFunc:         func(ctx context.Context, id string) (*models.Image, error) { return dbImage(models.StateUploaded), nil },
			UpsertImageFunc:      func(ctx context.Context, id string, image *models.Image) error { return nil },
			AcquireImageLockFunc: func(ctx context.Context, id string) (string, error) { return "", errors.New("fail to lock") },
			UnlockImageFunc:      func(ctx context.Context, id string) {},
		}

		authHandlerMock := &mock.AuthHandlerMock{
			RequireFunc: func(required dpauth.Permissions, handler http.HandlerFunc) http.HandlerFunc {
				return handler
			},
		}

		imageAPI := GetAPIWithMocks(cfg, mongoDBMock, authHandlerMock, kafkaStubProducer, kafkaStubProducer)

		Convey("When a valid new download is posted to an image in a 'uploaded' state", func() {
			r := httptest.NewRequest(http.MethodPost, fmt.Sprintf("http://localhost:24700/images/%s/downloads", testImageUploadedID), bytes.NewBufferString(
				fmt.Sprintf(newImageDownloadPayloadFmt, testVariantOriginal, testDownloadType, models.StateDownloadImporting.String())))
			r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
			w := httptest.NewRecorder()
			imageAPI.Router.ServeHTTP(w, r)

			Convey("Then a status code 500 (InternalServerError) is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
			})
		})
	})
}
func TestGetDownloadHandler(t *testing.T) {
	Convey("Given an image API in publishing mode", t, func() {
		cfg, err := config.Get()
		cfg.IsPublishing = true
		So(err, ShouldBeNil)
		doTestGetDownloadHandler()
	})

	Convey("Given an image API in web mode", t, func() {
		cfg, err := config.Get()
		cfg.IsPublishing = false
		So(err, ShouldBeNil)
		doTestGetDownloadHandler()
	})
}

func doTestGetDownloadHandler() {
	cfg, err := config.Get()
	So(err, ShouldBeNil)

	Convey("And an image API with existing valid images stored in a mongoDB mock", func() {
		cfg.IsPublishing = true

		firstDownload := dbDownloadWithID(testImagePublishedID, testVariantAlternative, models.StateDownloadPublished)
		secondDownload := dbDownloadWithID(testImagePublishedID, testVariantOriginal, models.StateDownloadPublished)
		mongoDBMock := &mock.MongoServerMock{
			GetImageFunc: func(ctx context.Context, id string) (*models.Image, error) {
				switch id {
				case testImageUploadedID:
					return dbFullImage(models.StateUploaded), nil
				case testImageImportingID:
					return dbFullImageWithDownloads(models.StateImporting, dbDownload(models.StateDownloadImported)), nil
				case testImagePublishedID:
					return dbFullImageWithDownloads(models.StatePublished, firstDownload, secondDownload), nil
				default:
					return nil, apierrors.ErrImageNotFound
				}
			},
		}

		authHandlerMock := &mock.AuthHandlerMock{
			RequireFunc: func(required dpauth.Permissions, handler http.HandlerFunc) http.HandlerFunc {
				return handler
			},
		}

		imageAPI := GetAPIWithMocks(cfg, mongoDBMock, authHandlerMock, kafkaStubProducer, kafkaStubProducer)

		Convey("When an existing download is requested from an image with one download", func() {
			r := httptest.NewRequest(http.MethodGet, fmt.Sprintf("http://localhost:24700/images/%s/downloads/%s", testImageImportingID, testVariantOriginal), http.NoBody)
			r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
			w := httptest.NewRecorder()
			imageAPI.Router.ServeHTTP(w, r)

			Convey("Then a valid Download response is returned with status code 200", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
				So(w.Header().Get(contentTypeKey), ShouldEqual, contentTypeJSON)
				payload, err := io.ReadAll(w.Body)
				So(err, ShouldBeNil)
				retDownloads := models.Download{}
				err = json.Unmarshal(payload, &retDownloads)
				So(err, ShouldBeNil)
				So(retDownloads, ShouldResemble, dbDownload(models.StateDownloadImported))
			})
		})

		Convey("When an existing download is requested from an image with two downloads", func() {
			r := httptest.NewRequest(http.MethodGet, fmt.Sprintf("http://localhost:24700/images/%s/downloads/%s", testImagePublishedID, testVariantAlternative), http.NoBody)
			r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
			w := httptest.NewRecorder()
			imageAPI.Router.ServeHTTP(w, r)

			Convey("Then a valid Download response is returned with status code 200", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
				So(w.Header().Get(contentTypeKey), ShouldEqual, contentTypeJSON)
				payload, err := io.ReadAll(w.Body)
				So(err, ShouldBeNil)
				retDownloads := models.Download{}
				err = json.Unmarshal(payload, &retDownloads)
				So(err, ShouldBeNil)
				So(retDownloads, ShouldResemble, dbDownloadWithID(testImagePublishedID, testVariantAlternative, models.StateDownloadPublished))
			})
		})

		Convey("When a download is requested from an image not in mongo", func() {
			r := httptest.NewRequest(http.MethodGet, fmt.Sprintf("http://localhost:24700/images/%s/downloads/%s", "nonExistantID", testVariantOriginal), http.NoBody)

			r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
			w := httptest.NewRecorder()
			imageAPI.Router.ServeHTTP(w, r)

			Convey("Then a status code 404 (NotFound) is returned", func() {
				So(w.Code, ShouldEqual, http.StatusNotFound)
			})
		})

		Convey("When a nonexistent download is requested from an image with one download", func() {
			r := httptest.NewRequest(http.MethodGet, fmt.Sprintf("http://localhost:24700/images/%s/downloads/%s", testImageImportingID, "nonExistantVariant"), http.NoBody)

			r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
			w := httptest.NewRecorder()
			imageAPI.Router.ServeHTTP(w, r)

			Convey("Then a status code 404 (NotFound) is returned", func() {
				So(w.Code, ShouldEqual, http.StatusNotFound)
			})
		})

		Convey("When the request headers for host, prefix and protocol are set", func() {
			r := httptest.NewRequest(http.MethodGet, fmt.Sprintf("http://localhost:24700/images/%s/downloads/%s", testImagePublishedID, testVariantOriginal), http.NoBody)
			r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
			r.Header.Add("X-Forwarded-Proto", expectedProto)
			r.Header.Add("X-Forwarded-Host", expectedHost)
			r.Header.Add("X-Forwarded-Path-Prefix", expectedPathPrefix)

			w := httptest.NewRecorder()

			Convey("And URL rewriting is enabled", func() {
				cfg.EnableURLRewriting = true

				imageAPI := GetAPIWithMocks(cfg, mongoDBMock, authHandlerMock, kafkaStubProducer, kafkaStubProducer)

				imageAPI.Router.ServeHTTP(w, r)

				Convey("Then the response body should contain the rewritten links", func() {
					var download models.Download
					err := json.Unmarshal(w.Body.Bytes(), &download)
					So(err, ShouldBeNil)

					So(download.Links.Self, ShouldEqual, fmt.Sprintf("%s://%s/%s/images/%s/downloads/%s", expectedProto, expectedHost, expectedPathPrefix, testImagePublishedID, testVariantOriginal))
					So(download.Links.Image, ShouldEqual, fmt.Sprintf("%s://%s/%s/images/%s", expectedProto, expectedHost, expectedPathPrefix, testImagePublishedID))
				})
			})

			Convey("And URL rewriting is disabled", func() {
				cfg.EnableURLRewriting = false

				imageAPI := GetAPIWithMocks(cfg, mongoDBMock, authHandlerMock, kafkaStubProducer, kafkaStubProducer)

				imageAPI.Router.ServeHTTP(w, r)

				Convey("Then the response body should not contain the rewritten links", func() {
					var download models.Download
					err := json.Unmarshal(w.Body.Bytes(), &download)
					So(err, ShouldBeNil)

					So(download.Links.Self, ShouldEqual, fmt.Sprintf("http://example.com/images/%s/downloads/%s", testImagePublishedID, testVariantOriginal))
					So(download.Links.Image, ShouldEqual, fmt.Sprintf("http://example.com/images/%s", testImagePublishedID))
				})
			})
		})
	})

	Convey("And an image API with mongoDB returning an error", func() {
		mongoDBMock := &mock.MongoServerMock{
			GetImageFunc: func(ctx context.Context, id string) (*models.Image, error) {
				return nil, errMongoDB
			},
		}
		authHandlerMock := &mock.AuthHandlerMock{
			RequireFunc: func(required dpauth.Permissions, handler http.HandlerFunc) http.HandlerFunc {
				return handler
			},
		}
		imageAPI := GetAPIWithMocks(cfg, mongoDBMock, authHandlerMock, kafkaStubProducer, kafkaStubProducer)

		Convey("Then when a specific download requested, a 500 error is returned", func() {
			r := httptest.NewRequest(http.MethodGet, fmt.Sprintf("http://localhost:24700/images/%s/downloads/%s", testImageUploadedID, testVariantOriginal), http.NoBody)
			r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
			w := httptest.NewRecorder()
			imageAPI.Router.ServeHTTP(w, r)
			So(w.Code, ShouldEqual, http.StatusInternalServerError)
		})
	})
}

func TestUpdateDownloadHandler(t *testing.T) {
	Convey("Given a valid config, auth handler", t, func() {
		cfg, err := config.Get()
		So(err, ShouldBeNil)
		authHandlerMock := &mock.AuthHandlerMock{
			RequireFunc: func(required dpauth.Permissions, handler http.HandlerFunc) http.HandlerFunc {
				return handler
			},
		}

		Convey("And an image in a state in 'created' state", func() {
			mongoDBMock := &mock.MongoServerMock{
				GetImageFunc: func(ctx context.Context, id string) (*models.Image, error) {
					return dbFullImageWithDownloads(models.StateCreated, dbDownloadWithID(id, testVariantOriginal, models.StateDownloadImporting)), nil
				},
				UpsertImageFunc:      func(ctx context.Context, id string, image *models.Image) error { return nil },
				AcquireImageLockFunc: func(ctx context.Context, id string) (string, error) { return testLockID, nil },
				UnlockImageFunc:      func(ctx context.Context, id string) {},
			}
			imageAPI := GetAPIWithMocks(cfg, mongoDBMock, authHandlerMock, kafkaStubProducer, kafkaStubProducer)

			Convey("Calling 'update variant' for the image results in 403 Forbidden response and nothing is updated", func() {
				r := httptest.NewRequest(http.MethodPut, fmt.Sprintf("http://localhost:24700/images/%s/downloads/%s", testImageID2, testVariantOriginal), bytes.NewBufferString(
					fmt.Sprintf(updateImageDownloadImportedPayloadFmt, testVariantOriginal, testDownloadType, models.StateDownloadImported.String())))
				r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
				r = r.WithContext(context.WithValue(r.Context(), handlers.CollectionID.Context(), testCollectionID1))
				w := httptest.NewRecorder()
				imageAPI.Router.ServeHTTP(w, r)
				So(w.Code, ShouldEqual, http.StatusForbidden)
				So(mongoDBMock.GetImageCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.GetImageCalls()[0].ID, ShouldEqual, testImageID2)
				So(mongoDBMock.AcquireImageLockCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.UnlockImageCalls(), ShouldHaveLength, 1)
			})
		})

		Convey("And an image in 'importing' state in MongoDB, with a download variant in 'importing' state", func() {
			mongoDBMock := &mock.MongoServerMock{
				GetImageFunc: func(ctx context.Context, id string) (*models.Image, error) {
					return dbFullImageWithDownloads(models.StateImporting, dbDownloadWithID(id, testVariantOriginal, models.StateDownloadImporting)), nil
				},
				UpsertImageFunc:      func(ctx context.Context, id string, image *models.Image) error { return nil },
				AcquireImageLockFunc: func(ctx context.Context, id string) (string, error) { return testLockID, nil },
				UnlockImageFunc:      func(ctx context.Context, id string) {},
			}
			imageAPI := GetAPIWithMocks(cfg, mongoDBMock, authHandlerMock, kafkaStubProducer, kafkaStubProducer)

			Convey("Calling 'update variant' for the image results in 200 OK response and the image is updated as expected", func() {
				r := httptest.NewRequest(http.MethodPut, fmt.Sprintf("http://localhost:24700/images/%s/downloads/%s", testImageID2, testVariantOriginal), bytes.NewBufferString(
					fmt.Sprintf(updateImageDownloadImportedPayloadFmt, testVariantOriginal, testDownloadType, models.StateDownloadImported.String())))
				r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
				r = r.WithContext(context.WithValue(r.Context(), handlers.CollectionID.Context(), testCollectionID1))
				w := httptest.NewRecorder()
				imageAPI.Router.ServeHTTP(w, r)
				So(w.Code, ShouldEqual, http.StatusOK)
				So(w.Header().Get(contentTypeKey), ShouldEqual, contentTypeJSON)
				So(mongoDBMock.GetImageCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.GetImageCalls()[0].ID, ShouldEqual, testImageID2)
				So(mongoDBMock.UpsertImageCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.UpsertImageCalls()[0].ID, ShouldResemble, testImageID2)
				update := mongoDBMock.UpsertImageCalls()[0].Image
				So(update.State, ShouldResemble, models.StateImported.String())
				So(update.Downloads[testVariantOriginal].State, ShouldResemble, models.StateDownloadImported.String())
				So(*update.Downloads[testVariantOriginal].ImportStarted, ShouldResemble, testImportStarted)
				So(mongoDBMock.AcquireImageLockCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.UnlockImageCalls(), ShouldHaveLength, 1)
			})

			Convey("Calling update download with a download that has a different id results in 400 response", func() {
				r := httptest.NewRequest(http.MethodPut, fmt.Sprintf("http://localhost:24700/images/%s/downloads/%s", testImageID2, testVariantOriginal), bytes.NewBufferString(
					fmt.Sprintf(updateImageDownloadImportedPayloadFmt, testVariantAlternative, testDownloadType, models.StateDownloadImported.String())))
				r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
				r = r.WithContext(context.WithValue(r.Context(), handlers.CollectionID.Context(), testCollectionID1))
				w := httptest.NewRecorder()
				imageAPI.Router.ServeHTTP(w, r)
				So(w.Code, ShouldEqual, http.StatusBadRequest)
			})
		})

		Convey("And an image in 'uploaded' state in MongoDB, without any download variants", func() {
			mongoDBMock := &mock.MongoServerMock{
				GetImageFunc: func(ctx context.Context, id string) (*models.Image, error) {
					return dbImage(models.StateUploaded), nil
				},
				AcquireImageLockFunc: func(ctx context.Context, id string) (string, error) { return testLockID, nil },
				UnlockImageFunc:      func(ctx context.Context, id string) {},
			}
			imageAPI := GetAPIWithMocks(cfg, mongoDBMock, authHandlerMock, kafkaStubProducer, kafkaStubProducer)

			Convey("Calling 'update variant' for the existing image without variants results in 404 Not found response", func() {
				r := httptest.NewRequest(http.MethodPut, fmt.Sprintf("http://localhost:24700/images/%s/downloads/%s", testImageID1, testVariantOriginal), bytes.NewBufferString(
					fmt.Sprintf(updateImageDownloadImportedPayloadFmt, testVariantOriginal, testDownloadType, models.StateDownloadImported.String())))
				r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
				r = r.WithContext(context.WithValue(r.Context(), handlers.CollectionID.Context(), testCollectionID1))
				w := httptest.NewRecorder()
				imageAPI.Router.ServeHTTP(w, r)
				So(w.Code, ShouldEqual, http.StatusNotFound)
				So(mongoDBMock.GetImageCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.GetImageCalls()[0].ID, ShouldEqual, testImageID1)
				So(mongoDBMock.AcquireImageLockCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.UnlockImageCalls(), ShouldHaveLength, 1)
			})
		})

		Convey("And an image in 'published' state in MongoDB, with a download variant in 'published' state", func() {
			mongoDBMock := &mock.MongoServerMock{
				GetImageFunc: func(ctx context.Context, id string) (*models.Image, error) {
					return dbFullImageWithDownloads(models.StatePublished, dbDownloadWithID(id, testVariantOriginal, models.StateDownloadPublished)), nil
				},
				UpsertImageFunc:      func(ctx context.Context, id string, image *models.Image) error { return nil },
				AcquireImageLockFunc: func(ctx context.Context, id string) (string, error) { return testLockID, nil },
				UnlockImageFunc:      func(ctx context.Context, id string) {},
			}
			imageAPI := GetAPIWithMocks(cfg, mongoDBMock, authHandlerMock, kafkaStubProducer, kafkaStubProducer)

			Convey("Calling 'complete variant' for the image results in 200 OK response and the image is completed", func() {
				r := httptest.NewRequest(http.MethodPut, fmt.Sprintf("http://localhost:24700/images/%s/downloads/%s", testImageID2, testVariantOriginal), bytes.NewBufferString(
					fmt.Sprintf(updateImageDownloadCompletedPayloadFmt, testVariantOriginal, testDownloadType, models.StateDownloadCompleted.String())))
				r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
				r = r.WithContext(context.WithValue(r.Context(), handlers.CollectionID.Context(), testCollectionID1))
				w := httptest.NewRecorder()
				imageAPI.Router.ServeHTTP(w, r)
				So(w.Code, ShouldEqual, http.StatusOK)
				So(w.Header().Get(contentTypeKey), ShouldEqual, contentTypeJSON)
				So(mongoDBMock.GetImageCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.GetImageCalls()[0].ID, ShouldEqual, testImageID2)
				So(mongoDBMock.UpsertImageCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.UpsertImageCalls()[0].ID, ShouldResemble, testImageID2)
				update := mongoDBMock.UpsertImageCalls()[0].Image
				So(update.State, ShouldResemble, models.StateCompleted.String())
				So(update.Downloads[testVariantOriginal].State, ShouldResemble, models.StateDownloadCompleted.String())
				So(*update.Downloads[testVariantOriginal].PublishCompleted, ShouldResemble, testPublishCompleted)
				So(mongoDBMock.AcquireImageLockCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.UnlockImageCalls(), ShouldHaveLength, 1)
			})
		})

		Convey("And an image in 'published' state in MongoDB, with 2 download variants in 'published' state", func() {
			mongoDBMock := &mock.MongoServerMock{
				GetImageFunc: func(ctx context.Context, id string) (*models.Image, error) {
					return dbFullImageWithDownloads(
						models.StatePublished,
						dbDownloadWithID(id, testVariantOriginal, models.StateDownloadPublished),
						dbDownloadWithID(id, testVariantAlternative, models.StateDownloadPublished)), nil
				},
				UpsertImageFunc:      func(ctx context.Context, id string, image *models.Image) error { return nil },
				AcquireImageLockFunc: func(ctx context.Context, id string) (string, error) { return testLockID, nil },
				UnlockImageFunc:      func(ctx context.Context, id string) {},
			}
			imageAPI := GetAPIWithMocks(cfg, mongoDBMock, authHandlerMock, kafkaStubProducer, kafkaStubProducer)

			Convey("Calling 'update variant' for the image results in 200 OK response, the variant is completed, but the image is not", func() {
				r := httptest.NewRequest(http.MethodPut, fmt.Sprintf("http://localhost:24700/images/%s/downloads/%s", testImageID2, testVariantOriginal), bytes.NewBufferString(
					fmt.Sprintf(updateImageDownloadCompletedPayloadFmt, testVariantOriginal, testDownloadType, models.StateDownloadCompleted.String())))
				r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
				r = r.WithContext(context.WithValue(r.Context(), handlers.CollectionID.Context(), testCollectionID1))
				w := httptest.NewRecorder()
				imageAPI.Router.ServeHTTP(w, r)
				So(w.Code, ShouldEqual, http.StatusOK)
				So(w.Header().Get(contentTypeKey), ShouldEqual, contentTypeJSON)
				So(mongoDBMock.GetImageCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.GetImageCalls()[0].ID, ShouldEqual, testImageID2)
				So(mongoDBMock.UpsertImageCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.UpsertImageCalls()[0].ID, ShouldResemble, testImageID2)
				update := mongoDBMock.UpsertImageCalls()[0].Image
				So(update.State, ShouldResemble, models.StatePublished.String())
				So(update.Downloads[testVariantOriginal].State, ShouldResemble, models.StateDownloadCompleted.String())
				So(*update.Downloads[testVariantOriginal].PublishCompleted, ShouldResemble, testPublishCompleted)
				So(mongoDBMock.AcquireImageLockCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.UnlockImageCalls(), ShouldHaveLength, 1)
			})
		})

		Convey("And a MongoDB mock that fails to lock", func() {
			mongoDBMock := &mock.MongoServerMock{
				AcquireImageLockFunc: func(ctx context.Context, id string) (string, error) { return "", errMongoDB },
			}
			imageAPI := GetAPIWithMocks(cfg, mongoDBMock, authHandlerMock, kafkaStubProducer, kafkaStubProducer)

			Convey("Calling 'update variant' for the variant results in 500 StatusInternalServerError response", func() {
				r := httptest.NewRequest(http.MethodPut, fmt.Sprintf("http://localhost:24700/images/%s/downloads/%s", testImageID1, testVariantOriginal),
					bytes.NewBufferString(imageDownloadPayload))
				r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
				w := httptest.NewRecorder()
				imageAPI.Router.ServeHTTP(w, r)
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(mongoDBMock.AcquireImageLockCalls(), ShouldHaveLength, 1)
			})
		})

		Convey("And a MongoDB returning error on GetImage", func() {
			mongoDBMock := &mock.MongoServerMock{
				GetImageFunc: func(ctx context.Context, id string) (*models.Image, error) {
					return nil, errMongoDB
				},
				AcquireImageLockFunc: func(ctx context.Context, id string) (string, error) { return testLockID, nil },
				UnlockImageFunc:      func(ctx context.Context, id string) {},
			}
			imageAPI := GetAPIWithMocks(cfg, mongoDBMock, authHandlerMock, kafkaStubProducer, kafkaStubProducer)

			Convey("Calling 'update variant' for an nonexistent image results in 500 InternalServerError response", func() {
				r := httptest.NewRequest(http.MethodPut, fmt.Sprintf("http://localhost:24700/images/%s/downloads/%s", testImageID1, testVariantOriginal), bytes.NewBufferString(
					fmt.Sprintf(updateImageDownloadImportedPayloadFmt, testVariantOriginal, testDownloadType, models.StateDownloadImported.String())))
				r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
				r = r.WithContext(context.WithValue(r.Context(), handlers.CollectionID.Context(), testCollectionID1))
				w := httptest.NewRecorder()
				imageAPI.Router.ServeHTTP(w, r)
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(mongoDBMock.GetImageCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.GetImageCalls()[0].ID, ShouldEqual, testImageID1)
				So(mongoDBMock.AcquireImageLockCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.UnlockImageCalls(), ShouldHaveLength, 1)
			})
		})

		Convey("And a MongoDB returning error on UploadImage", func() {
			mongoDBMock := &mock.MongoServerMock{
				GetImageFunc: func(ctx context.Context, id string) (*models.Image, error) {
					return dbFullImageWithDownloads(models.StateImporting, dbDownloadWithID(id, testVariantOriginal, models.StateDownloadImporting)), nil
				},
				UpsertImageFunc:      func(ctx context.Context, id string, image *models.Image) error { return errMongoDB },
				AcquireImageLockFunc: func(ctx context.Context, id string) (string, error) { return testLockID, nil },
				UnlockImageFunc:      func(ctx context.Context, id string) {},
			}
			imageAPI := GetAPIWithMocks(cfg, mongoDBMock, authHandlerMock, kafkaStubProducer, kafkaStubProducer)

			Convey("Calling 'import variant' results in 500 InternalServerError response", func() {
				r := httptest.NewRequest(http.MethodPut, fmt.Sprintf("http://localhost:24700/images/%s/downloads/%s", testImageID1, testVariantOriginal), bytes.NewBufferString(
					fmt.Sprintf(updateImageDownloadImportedPayloadFmt, testVariantOriginal, testDownloadType, models.StateDownloadImported.String())))
				r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
				r = r.WithContext(context.WithValue(r.Context(), handlers.CollectionID.Context(), testCollectionID1))
				w := httptest.NewRecorder()
				imageAPI.Router.ServeHTTP(w, r)
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(mongoDBMock.GetImageCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.GetImageCalls()[0].ID, ShouldEqual, testImageID1)
				So(mongoDBMock.UpsertImageCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.UpsertImageCalls()[0].ID, ShouldEqual, testImageID1)
				So(mongoDBMock.AcquireImageLockCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.UnlockImageCalls(), ShouldHaveLength, 1)
			})
		})
	})
}

func TestPublishImageHandler(t *testing.T) {
	Convey("Given a valid config, auth handler, kafka producer", t, func() {
		cfg, err := config.Get()
		So(err, ShouldBeNil)
		cfg.DownloadServiceURL = downloadServiceURL
		authHandlerMock := &mock.AuthHandlerMock{
			RequireFunc: func(required dpauth.Permissions, handler http.HandlerFunc) http.HandlerFunc {
				return handler
			},
		}

		Convey("And an image in 'imported' state in MongoDB", func() {
			expectedSrcPathOriginal := fmt.Sprintf("images/%s/original", testImageID1)
			expectedSrcPathPngW500 := fmt.Sprintf("images/%s/png_w500", testImageID1)
			expectedDstPathOriginal := fmt.Sprintf("%s/some-image-name", expectedSrcPathOriginal)
			expectedDstPathPngW500 := fmt.Sprintf("%s/some-image-name", expectedSrcPathPngW500)
			mongoDBMock := &mock.MongoServerMock{
				GetImageFunc: func(ctx context.Context, id string) (*models.Image, error) {
					image := dbImage(models.StateImported)
					image.Filename = testFilename
					image.Downloads = map[string]models.Download{testVariantOriginal: {ID: testVariantOriginal, Href: expectedSrcPathOriginal}, testVariantPng: {ID: testVariantPng, Href: expectedSrcPathPngW500}}
					return image, nil
				},
				UpdateImageFunc: func(ctx context.Context, id string, image *models.Image) (bool, error) {
					return true, nil
				},
				AcquireImageLockFunc: func(ctx context.Context, id string) (string, error) { return testLockID, nil },
				UnlockImageFunc:      func(ctx context.Context, id string) {},
			}

			Convey("Calling 'publish image' results in 204 NoContent response with the expected image state update to mongoDB and the message sent to kafka producer", func() {
				sentEvents := make([]interface{}, 0, 2)
				publishedProducer := &kafkatest.IProducerMock{
					SendFunc: func(ctx context.Context, schema *avro.Schema, event interface{}) error {
						sentEvents = append(sentEvents, event)
						return nil
					},
				}
				imageAPI := GetAPIWithMocks(cfg, mongoDBMock, authHandlerMock, kafkaStubProducer, publishedProducer)
				r := httptest.NewRequest(http.MethodPost, fmt.Sprintf("http://localhost:24700/images/%s/publish", testImageID1), http.NoBody)
				r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
				r = r.WithContext(context.WithValue(r.Context(), handlers.CollectionID.Context(), testCollectionID1))
				w := httptest.NewRecorder()
				imageAPI.Router.ServeHTTP(w, r)
				So(w.Code, ShouldEqual, http.StatusNoContent)
				So(mongoDBMock.GetImageCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.GetImageCalls()[0].ID, ShouldEqual, testImageID1)
				So(mongoDBMock.UpdateImageCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.UpdateImageCalls()[0].ID, ShouldEqual, testImageID1)
				So(mongoDBMock.UpdateImageCalls()[0].Image.State, ShouldEqual, models.StatePublished.String())
				So(mongoDBMock.UpdateImageCalls()[0].Image.Downloads, ShouldHaveLength, 2)
				So(mongoDBMock.UpdateImageCalls()[0].Image.Downloads[testVariantOriginal].State, ShouldEqual, models.StateDownloadPublished.String())
				So(mongoDBMock.UpdateImageCalls()[0].Image.Downloads[testVariantOriginal].Href, ShouldEqual, downloadServiceURL+"/images/"+testImageID1+"/original/some-image-name")
				So(mongoDBMock.UpdateImageCalls()[0].Image.Downloads[testVariantPng].State, ShouldEqual, models.StateDownloadPublished.String())
				So(mongoDBMock.UpdateImageCalls()[0].Image.Downloads[testVariantPng].Href, ShouldEqual, downloadServiceURL+"/images/"+testImageID1+"/png_w500/some-image-name")
				So(mongoDBMock.AcquireImageLockCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.UnlockImageCalls(), ShouldHaveLength, 1)

				Convey("And the expected avro event is sent to the corresponding kafka output channel, with the expected source and dest paths", func() {
					So(publishedProducer.SendCalls(), ShouldHaveLength, 2)
					// Note: the paths correspond to the path part of DownloadHrefFmt format "http://<host>/images/<imageID>/<variantName>/<fileName>"
					expectedEventOriginal := &event.ImagePublished{
						SrcPath:      expectedSrcPathOriginal,
						DstPath:      expectedDstPathOriginal,
						ImageID:      testImageID1,
						ImageVariant: testVariantOriginal,
					}
					expectedEventPngW500 := &event.ImagePublished{
						SrcPath:      expectedSrcPathPngW500,
						DstPath:      expectedDstPathPngW500,
						ImageID:      testImageID1,
						ImageVariant: testVariantPng,
					}
					So(sentEvents, ShouldContain, interface{}(expectedEventOriginal))
					So(sentEvents, ShouldContain, interface{}(expectedEventPngW500))
				})
			})

			Convey("Calling 'publish image' with a 500 InternalError response when an invalid image published event is generated", func() {
				publishedProducer := &kafkatest.IProducerMock{
					SendFunc: func(ctx context.Context, schema *avro.Schema, event interface{}) error {
						return fmt.Errorf("blah")
					},
				}
				imageAPI := GetAPIWithMocks(cfg, mongoDBMock, authHandlerMock, kafkaStubProducer, publishedProducer)
				r := httptest.NewRequest(http.MethodPost, fmt.Sprintf("http://localhost:24700/images/%s/publish", testImageID1), http.NoBody)
				r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
				r = r.WithContext(context.WithValue(r.Context(), handlers.CollectionID.Context(), testCollectionID1))
				w := httptest.NewRecorder()
				imageAPI.Router.ServeHTTP(w, r)
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(mongoDBMock.GetImageCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.GetImageCalls()[0].ID, ShouldEqual, testImageID1)
				So(mongoDBMock.UpdateImageCalls(), ShouldHaveLength, 1)
			})
		})

		Convey("And an image in 'created' state in MongoDB (non-publishable)", func() {
			mongoDBMock := &mock.MongoServerMock{
				GetImageFunc: func(ctx context.Context, id string) (*models.Image, error) {
					return dbImage(models.StateCreated), nil
				},
				AcquireImageLockFunc: func(ctx context.Context, id string) (string, error) { return testLockID, nil },
				UnlockImageFunc:      func(ctx context.Context, id string) {},
			}
			imageAPI := GetAPIWithMocks(cfg, mongoDBMock, authHandlerMock, kafkaStubProducer, kafkaStubProducer)

			Convey("Calling 'publish image' results in 403 Forbidden response", func() {
				r := httptest.NewRequest(http.MethodPost, fmt.Sprintf("http://localhost:24700/images/%s/publish", testImageID1), http.NoBody)
				r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
				r = r.WithContext(context.WithValue(r.Context(), handlers.CollectionID.Context(), testCollectionID1))
				w := httptest.NewRecorder()
				imageAPI.Router.ServeHTTP(w, r)
				So(w.Code, ShouldEqual, http.StatusForbidden)
				So(mongoDBMock.GetImageCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.GetImageCalls()[0].ID, ShouldEqual, testImageID1)
				So(mongoDBMock.AcquireImageLockCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.UnlockImageCalls(), ShouldHaveLength, 1)
			})
		})

		Convey("And MongoDB failing to lock an image", func() {
			mongoDBMock := &mock.MongoServerMock{
				AcquireImageLockFunc: func(ctx context.Context, id string) (string, error) {
					return "", errors.New("mongoDB lock error")
				},
			}
			imageAPI := GetAPIWithMocks(cfg, mongoDBMock, authHandlerMock, kafkaStubProducer, kafkaStubProducer)

			Convey("Calling 'publish image' results in 500 response", func() {
				r := httptest.NewRequest(http.MethodPost, fmt.Sprintf("http://localhost:24700/images/%s/publish", testImageID1), http.NoBody)
				r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
				w := httptest.NewRecorder()
				imageAPI.Router.ServeHTTP(w, r)
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(mongoDBMock.AcquireImageLockCalls(), ShouldHaveLength, 1)
			})
		})

		Convey("And MongoDB failing to get an image", func() {
			mongoDBMock := &mock.MongoServerMock{
				GetImageFunc: func(ctx context.Context, id string) (*models.Image, error) {
					return nil, errors.New("internal mongoDB error")
				},
				AcquireImageLockFunc: func(ctx context.Context, id string) (string, error) { return testLockID, nil },
				UnlockImageFunc:      func(ctx context.Context, id string) {},
			}
			imageAPI := GetAPIWithMocks(cfg, mongoDBMock, authHandlerMock, kafkaStubProducer, kafkaStubProducer)

			Convey("Calling 'publish image' results in 500 response", func() {
				r := httptest.NewRequest(http.MethodPost, fmt.Sprintf("http://localhost:24700/images/%s/publish", testImageID1), http.NoBody)
				r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
				w := httptest.NewRecorder()
				imageAPI.Router.ServeHTTP(w, r)
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(mongoDBMock.GetImageCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.GetImageCalls()[0].ID, ShouldEqual, testImageID1)
				So(mongoDBMock.AcquireImageLockCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.UnlockImageCalls(), ShouldHaveLength, 1)
			})
		})

		Convey("And an API with a mongoDB containing an imported image that fails to update", func() {
			mongoDBMock := &mock.MongoServerMock{
				GetImageFunc: func(ctx context.Context, id string) (*models.Image, error) {
					return dbImage(models.StateImported), nil
				},
				UpdateImageFunc: func(ctx context.Context, id string, image *models.Image) (bool, error) {
					return false, errors.New("internal mongoDB error")
				},
				AcquireImageLockFunc: func(ctx context.Context, id string) (string, error) { return testLockID, nil },
				UnlockImageFunc:      func(ctx context.Context, id string) {},
			}
			imageAPI := GetAPIWithMocks(cfg, mongoDBMock, authHandlerMock, kafkaStubProducer, kafkaStubProducer)

			Convey("Calling 'publish image' results in 500 response", func() {
				r := httptest.NewRequest(http.MethodPost, fmt.Sprintf("http://localhost:24700/images/%s/publish", testImageID1), http.NoBody)
				r = r.WithContext(context.WithValue(r.Context(), dpreq.FlorenceIdentityKey, testUserAuthToken))
				r = r.WithContext(context.WithValue(r.Context(), handlers.CollectionID.Context(), testCollectionID1))
				w := httptest.NewRecorder()
				imageAPI.Router.ServeHTTP(w, r)
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(mongoDBMock.GetImageCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.GetImageCalls()[0].ID, ShouldEqual, testImageID1)
				So(mongoDBMock.UpdateImageCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.UpdateImageCalls()[0].ID, ShouldEqual, testImageID1)
				So(mongoDBMock.UpdateImageCalls()[0].Image.State, ShouldEqual, models.StatePublished.String())
				So(mongoDBMock.AcquireImageLockCalls(), ShouldHaveLength, 1)
				So(mongoDBMock.UnlockImageCalls(), ShouldHaveLength, 1)
			})
		})
	})
}
