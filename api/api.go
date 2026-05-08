package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"

	dpurl "github.com/ONSdigital/dp-image-api/url"

	dpauth "github.com/ONSdigital/dp-authorisation/auth"
	"github.com/ONSdigital/dp-image-api/apierrors"
	"github.com/ONSdigital/dp-image-api/config"
	kafka "github.com/ONSdigital/dp-kafka/v5"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/gorilla/mux"
)

// API provides a struct to wrap the api around
type API struct {
	Router             *mux.Router
	mongoDB            MongoServer
	auth               AuthHandler
	uploadProducer     kafka.IProducer
	publishedProducer  kafka.IProducer
	urlBuilder         *dpurl.Builder
	downloadServiceURL string
	apiUrl             *url.URL
	enableURLRewriting bool
}

// Setup creates the API struct and its endpoints with corresponding handlers
func Setup(ctx context.Context, cfg *config.Config, r *mux.Router, auth AuthHandler, mongoDB MongoServer, uploadedKafkaProducer, publishedKafkaProducer kafka.IProducer, builder *dpurl.Builder) *API {
	apiURL, err := url.Parse(cfg.APIURL)
	if err != nil {
		log.Error(ctx, "could not parse image api url", err, log.Data{"url": cfg.APIURL})
		return nil
	}

	api := &API{
		Router:             r,
		auth:               auth,
		mongoDB:            mongoDB,
		urlBuilder:         builder,
		downloadServiceURL: cfg.DownloadServiceURL,
		enableURLRewriting: cfg.EnableURLRewriting,
		apiUrl:             apiURL,
	}

	if cfg.IsPublishing {
		api.uploadProducer = uploadedKafkaProducer
		api.publishedProducer = publishedKafkaProducer
		r.HandleFunc("/images", auth.Require(dpauth.Permissions{Read: true}, api.GetImagesHandler)).Methods(http.MethodGet)
		r.HandleFunc("/images", auth.Require(dpauth.Permissions{Create: true}, api.CreateImageHandler)).Methods(http.MethodPost)
		r.HandleFunc("/images/{id}", auth.Require(dpauth.Permissions{Read: true}, api.GetImageHandler)).Methods(http.MethodGet)
		r.HandleFunc("/images/{id}", auth.Require(dpauth.Permissions{Update: true}, api.UpdateImageHandler)).Methods(http.MethodPut)
		r.HandleFunc("/images/{id}/downloads", auth.Require(dpauth.Permissions{Read: true}, api.GetDownloadsHandler)).Methods(http.MethodGet)
		r.HandleFunc("/images/{id}/downloads", auth.Require(dpauth.Permissions{Update: true}, api.CreateDownloadHandler)).Methods(http.MethodPost)
		r.HandleFunc("/images/{id}/downloads/{variant}", auth.Require(dpauth.Permissions{Read: true}, api.GetDownloadHandler)).Methods(http.MethodGet)
		r.HandleFunc("/images/{id}/downloads/{variant}", auth.Require(dpauth.Permissions{Update: true}, api.UpdateDownloadHandler)).Methods(http.MethodPut)
		r.HandleFunc("/images/{id}/publish", auth.Require(dpauth.Permissions{Update: true}, api.PublishImageHandler)).Methods(http.MethodPost)
	} else {
		r.HandleFunc("/images", api.GetImagesHandler).Methods(http.MethodGet)
		r.HandleFunc("/images/{id}", api.GetImageHandler).Methods(http.MethodGet)
		r.HandleFunc("/images/{id}/downloads", api.GetDownloadsHandler).Methods(http.MethodGet)
		r.HandleFunc("/images/{id}/downloads/{variant}", api.GetDownloadHandler).Methods(http.MethodGet)
	}
	return api
}

// Close is called during graceful shutdown to give the API an opportunity to perform any required disposal task
func (*API) Close(ctx context.Context) error {
	log.Info(ctx, "graceful shutdown of api complete")
	return nil
}

// WriteJSONBody marshals the provided interface into json, and writes it to the response body.
func WriteJSONBody(v interface{}, w http.ResponseWriter, httpStatus int) error {
	// Set headers
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(httpStatus)

	// Marshal provided model
	payload, err := json.Marshal(v)
	if err != nil {
		return err
	}

	// Write payload to body
	if _, err := w.Write(payload); err != nil {
		return err
	}
	return nil
}

// ReadJSONBody reads the bytes from the provided body, and marshals it to the provided model interface.
func ReadJSONBody(_ context.Context, body io.ReadCloser, v interface{}) error {
	defer body.Close()

	// Get Body bytes
	payload, err := io.ReadAll(body)
	if err != nil {
		return apierrors.ErrUnableToReadMessage
	}

	// Unmarshal body bytes to model
	if err := json.Unmarshal(payload, v); err != nil {
		return apierrors.ErrUnableToParseJSON
	}

	return nil
}

// handleError is a utility function that maps api errors to an http status code and sets the provided responseWriter accordingly
func handleError(ctx context.Context, w http.ResponseWriter, err error, data log.Data) {
	var status int
	if err != nil {
		switch err {
		case apierrors.ErrImageNotFound,
			apierrors.ErrVariantNotFound:
			status = http.StatusNotFound
		case apierrors.ErrUnableToReadMessage,
			apierrors.ErrUnableToParseJSON,
			apierrors.ErrImageFilenameTooLong,
			apierrors.ErrImageNoCollectionID,
			apierrors.ErrImageInvalidState,
			apierrors.ErrImageDownloadTypeMismatch,
			apierrors.ErrImageDownloadInvalidState,
			apierrors.ErrImageIDMismatch,
			apierrors.ErrVariantIDMismatch:
			status = http.StatusBadRequest
		case apierrors.ErrImageAlreadyPublished,
			apierrors.ErrImageAlreadyCompleted,
			apierrors.ErrImageStateTransitionNotAllowed,
			apierrors.ErrImageBadInitialState,
			apierrors.ErrImageNotImporting,
			apierrors.ErrImageNotPublished,
			apierrors.ErrVariantAlreadyExists,
			apierrors.ErrVariantStateTransitionNotAllowed,
			apierrors.ErrImageDownloadBadInitialState:
			status = http.StatusForbidden
		default:
			status = http.StatusInternalServerError
		}
	}

	if data == nil {
		data = log.Data{}
	}

	data["response_status"] = status
	log.Error(ctx, "request unsuccessful", err, data)
	http.Error(w, err.Error(), status)
}
