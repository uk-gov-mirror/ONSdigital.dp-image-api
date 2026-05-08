package api

import (
	"context"
	"fmt"
	"net/http"
	"path"
	"time"

	"github.com/ONSdigital/dp-image-api/apierrors"
	"github.com/ONSdigital/dp-image-api/event"
	"github.com/ONSdigital/dp-image-api/models"
	"github.com/ONSdigital/dp-image-api/schema"
	"github.com/ONSdigital/dp-net/v2/links"
	"github.com/ONSdigital/dp-net/v3/handlers"
	dpreq "github.com/ONSdigital/dp-net/v3/request"
	"github.com/ONSdigital/log.go/v2/log"
	uuid "github.com/google/uuid"
	"github.com/gorilla/mux"
)

// NewID returns a new UUID
var NewID = func() string {
	return uuid.New().String()
}

// GetImagesHandler is a handler that gets all images in a collection from MongoDB
func (api *API) GetImagesHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	hColID := ctx.Value(handlers.CollectionID.Context())
	logdata := log.Data{
		handlers.CollectionID.Header(): hColID,
		"request-id":                   ctx.Value(dpreq.RequestIdKey),
	}

	// get collection_id query parameter (optional)
	colID := req.URL.Query().Get("collection_id")
	logdata["collection_id"] = colID

	// get images from MongoDB with the requested collection_id filter, if provided.
	items, err := api.mongoDB.GetImages(ctx, colID)
	if err != nil {
		handleError(ctx, w, err, logdata)
		return
	}

	images := models.Images{
		Items:      items,
		Count:      len(items),
		TotalCount: len(items),
		Limit:      len(items),
	}

	imageLinkBuilder := links.FromHeadersOrDefault(&req.Header, api.apiUrl)

	if api.enableURLRewriting {
		for image := range images.Items {
			err := rewriteImageLinks(ctx, *imageLinkBuilder, &images.Items[image])
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
	}

	if err := WriteJSONBody(images, w, http.StatusOK); err != nil {
		handleError(ctx, w, err, logdata)
		return
	}
	log.Info(ctx, "Successfully retrieved images", logdata)
}

// CreateImageHandler is a handler that inserts an image into mongoDB with a newly generated ID
func (api *API) CreateImageHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	hColID := ctx.Value(handlers.CollectionID.Context())
	logdata := log.Data{
		handlers.CollectionID.Header(): hColID,
		"request-id":                   ctx.Value(dpreq.RequestIdKey),
	}

	newImageRequest := &models.Image{}
	if err := ReadJSONBody(ctx, req.Body, newImageRequest); err != nil {
		handleError(ctx, w, err, logdata)
		return
	}

	id := NewID()

	// generate new image from request, mapping only allowed fields at creation time (model newImage in swagger spec)
	// the image is always created in 'created' state, and it is assigned a newly generated ID
	newImage := models.Image{
		ID:           id,
		CollectionID: newImageRequest.CollectionID,
		State:        newImageRequest.State,
		Filename:     newImageRequest.Filename,
		License:      newImageRequest.License,
		Links:        api.createLinksForImage(id),
		Type:         newImageRequest.Type,
	}

	// generic image validation
	if err := newImage.Validate(); err != nil {
		handleError(ctx, w, err, logdata)
		return
	}

	// Check provided image state supplied is correct
	if newImage.State != models.StateCreated.String() {
		handleError(ctx, w, apierrors.ErrImageBadInitialState, logdata)
		return
	}

	// check that collectionID is provided in the request body
	if newImage.CollectionID == "" {
		handleError(ctx, w, apierrors.ErrImageNoCollectionID, logdata)
		return
	}

	log.Info(ctx, "storing new image", log.Data{"image": newImage})

	// Upsert image in MongoDB
	if err := api.mongoDB.UpsertImage(req.Context(), newImage.ID, &newImage); err != nil {
		handleError(ctx, w, err, logdata)
		return
	}

	if err := WriteJSONBody(newImage, w, http.StatusCreated); err != nil {
		handleError(ctx, w, err, logdata)
		return
	}
	log.Info(ctx, "successfully created image", logdata)
}

// GetImageHandler is a handler that gets an image by its id from MongoDB
func (api *API) GetImageHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	vars := mux.Vars(req)
	id := vars["id"]
	hColID := ctx.Value(handlers.CollectionID.Context())
	logdata := log.Data{
		handlers.CollectionID.Header(): hColID,
		"request-id":                   ctx.Value(dpreq.RequestIdKey),
		"image-id":                     id,
	}

	// get image from mongoDB by id
	image, err := api.mongoDB.GetImage(req.Context(), id)
	if err != nil {
		handleError(ctx, w, err, logdata)
		return
	}

	imageLinkBuilder := links.FromHeadersOrDefault(&req.Header, api.apiUrl)

	if api.enableURLRewriting {
		err := rewriteImageLinks(ctx, *imageLinkBuilder, image)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	if err := WriteJSONBody(image, w, http.StatusOK); err != nil {
		handleError(ctx, w, err, logdata)
		return
	}
	log.Info(ctx, "Successfully retrieved image", logdata)
}

// UpdateImageHandler is a handler that updates an existing image in MongoDB
func (api *API) UpdateImageHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	vars := mux.Vars(req)
	id := vars["id"]
	hColID := ctx.Value(handlers.CollectionID.Context())
	logdata := log.Data{
		handlers.CollectionID.Header(): hColID,
		"request-id":                   ctx.Value(dpreq.RequestIdKey),
		"image-id":                     id,
	}

	// Unmarshal image from body and validate it
	image := &models.Image{}
	if err := ReadJSONBody(ctx, req.Body, image); err != nil {
		handleError(ctx, w, err, logdata)
		return
	}

	// apply the update
	if updatedImage := api.doUpdateImage(w, req, id, image, logdata); updatedImage != nil {
		if err := WriteJSONBody(updatedImage, w, http.StatusOK); err != nil {
			handleError(ctx, w, err, logdata)
			return
		}
		log.Info(ctx, "successfully updated image", logdata)
	}
}

// doUpdateImage is a function to apply the provided image update after doing all the necessary validations, it returns the updated image, or nil if it was not updated
func (api *API) doUpdateImage(w http.ResponseWriter, req *http.Request, id string, image *models.Image, logdata log.Data) (updatedImage *models.Image) {
	ctx := req.Context()

	// Validate new model regardless of existing state
	if err := image.Validate(); err != nil {
		handleError(ctx, w, err, logdata)
		return nil
	}
	log.Info(ctx, "updating image", log.Data{"image": image})

	// Validate a possible mismatch of image id, if provided in image body
	if image.ID != "" && image.ID != id {
		handleError(ctx, w, apierrors.ErrImageIDMismatch, logdata)
		return nil
	}
	image.ID = id

	// Acquire lock for image ID, and defer unlocking
	lockID, err := api.mongoDB.AcquireImageLock(ctx, id)
	if err != nil {
		handleError(ctx, w, err, logdata)
		return nil
	}
	defer api.unlockImage(ctx, lockID)

	// get existing image from mongoDB by id
	existingImage, err := api.mongoDB.GetImage(req.Context(), id)
	if err != nil {
		handleError(ctx, w, err, logdata)
		return nil
	}

	// Check that transition from the existing image state is valid
	if transitionErr := image.ValidateTransitionFrom(existingImage); transitionErr != nil {
		logdata["current_image_state"] = existingImage.State
		logdata["target_image_state"] = image.State
		handleError(ctx, w, transitionErr, logdata)
		return nil
	}

	// Copy existing Links to newly updated image
	image.Links = existingImage.Links

	// If the new state is 'uploaded', generate and send the kafka event to trigger import
	if image.State == models.StateUploaded.String() {
		uploadS3Path := path.Base(image.Upload.Path)
		log.Info(ctx, "sending image uploaded message", logdata)
		uploadedEvent := &event.ImageUploaded{
			ImageID:  id,
			Path:     uploadS3Path,
			Filename: image.Filename,
		}
		if uploadErr := api.uploadProducer.Send(ctx, schema.ImageUploadedEvent, uploadedEvent); uploadErr != nil {
			handleError(ctx, w, uploadErr, logdata)
			return nil
		}
	}

	// Update image in mongo DB
	err = api.mongoDB.UpsertImage(ctx, id, image)
	if err != nil {
		handleError(ctx, w, err, logdata)
		return nil
	}
	return image
}

// GetDownloadsHandler is a handler that returns all the download variant for an image
func (api *API) GetDownloadsHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	vars := mux.Vars(req)
	id := vars["id"]
	hColID := ctx.Value(handlers.CollectionID.Context())
	logdata := log.Data{
		handlers.CollectionID.Header(): hColID,
		"request-id":                   ctx.Value(dpreq.RequestIdKey),
		"image-id":                     id,
	}

	// get image from mongoDB by id
	image, err := api.mongoDB.GetImage(req.Context(), id)
	if err != nil {
		handleError(ctx, w, err, logdata)
		return
	}

	imageLinkBuilder := links.FromHeadersOrDefault(&req.Header, api.apiUrl)

	if api.enableURLRewriting {
		for download := range image.Downloads {
			err := rewriteDownloadLinks(ctx, *imageLinkBuilder, image.Downloads[download])
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
	}

	downloadsList := make([]models.Download, 0)
	for i := range image.Downloads {
		downloadsList = append(downloadsList, image.Downloads[i])
	}
	downloads := models.Downloads{
		Items:      downloadsList,
		Count:      len(downloadsList),
		TotalCount: len(downloadsList),
		Limit:      len(downloadsList),
	}

	if err := WriteJSONBody(downloads, w, http.StatusOK); err != nil {
		handleError(ctx, w, err, logdata)
		return
	}
	log.Info(ctx, "Successfully retrieved downloads", logdata)
}

// CreateDownloadHandler is a handler that adds a new download to an existing image
func (api *API) CreateDownloadHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	vars := mux.Vars(req)
	id := vars["id"]
	hColID := ctx.Value(handlers.CollectionID.Context())
	logdata := log.Data{
		handlers.CollectionID.Header(): hColID,
		"request-id":                   ctx.Value(dpreq.RequestIdKey),
		"image-id":                     id,
	}

	// Unmarshal image from body and validate it
	newDownload := &models.Download{}
	if err := ReadJSONBody(ctx, req.Body, newDownload); err != nil {
		handleError(ctx, w, err, logdata)
		return
	}

	// generic download validation
	if err := newDownload.Validate(); err != nil {
		handleError(ctx, w, err, logdata)
		return
	}

	variant := newDownload.ID

	// Create HATEOS links for download variant
	newDownload.Links = api.createLinksForDownload(id, variant)

	// Check provided variant state supplied is correct
	if newDownload.State != models.StateDownloadImporting.String() {
		handleError(ctx, w, apierrors.ErrImageDownloadBadInitialState, logdata)
		return
	}

	// Acquire lock for image ID, and defer unlocking
	lockID, err := api.mongoDB.AcquireImageLock(ctx, id)
	if err != nil {
		handleError(ctx, w, err, logdata)
		return
	}
	defer api.unlockImage(ctx, lockID)

	// get image from mongoDB by id
	image, err := api.mongoDB.GetImage(req.Context(), id)
	if err != nil {
		handleError(ctx, w, err, logdata)
		return
	}

	// Validate new download against parent image
	if validationErr := newDownload.ValidateForImage(image); validationErr != nil {
		handleError(ctx, w, validationErr, logdata)
		return
	}

	// check for existing variant
	_, found := image.Downloads[variant]
	if found {
		handleError(ctx, w, apierrors.ErrVariantAlreadyExists, logdata)
		return
	}

	// Add new download to existing image and update image state
	if image.Downloads == nil {
		image.Downloads = map[string]models.Download{}
	}
	image.Downloads[variant] = *newDownload
	image.State = models.StateImporting.String()

	// Update image in mongo DB
	err = api.mongoDB.UpsertImage(ctx, id, image)
	if err != nil {
		handleError(ctx, w, err, logdata)
		return
	}

	if err := WriteJSONBody(newDownload, w, http.StatusCreated); err != nil {
		handleError(ctx, w, err, logdata)
		return
	}
	log.Info(ctx, "successfully created download variant", logdata)
}

func (api *API) createLinksForImage(id string) *models.ImageLinks {
	self := api.urlBuilder.BuildImageURL(id)
	downloads := api.urlBuilder.BuildImageDownloadsURL(id)
	return &models.ImageLinks{
		Self:      self,
		Downloads: downloads,
	}
}

func (api *API) createLinksForDownload(id, variant string) *models.DownloadLinks {
	self := api.urlBuilder.BuildImageDownloadURL(id, variant)
	image := api.urlBuilder.BuildImageURL(id)
	return &models.DownloadLinks{
		Self:  self,
		Image: image,
	}
}

// GetDownloadHandler is a handler that returns an individual download variant
func (api *API) GetDownloadHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	vars := mux.Vars(req)
	id := vars["id"]
	variant := vars["variant"]
	hColID := ctx.Value(handlers.CollectionID.Context())
	logdata := log.Data{
		handlers.CollectionID.Header(): hColID,
		"request-id":                   ctx.Value(dpreq.RequestIdKey),
		"image-id":                     id,
		"download-variant":             variant,
	}

	// get image from mongoDB by id
	image, err := api.mongoDB.GetImage(req.Context(), id)
	if err != nil {
		handleError(ctx, w, err, logdata)
		return
	}

	imageLinkBuilder := links.FromHeadersOrDefault(&req.Header, api.apiUrl)

	if api.enableURLRewriting {
		err := rewriteDownloadLinks(ctx, *imageLinkBuilder, image.Downloads[variant])
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	download, found := image.Downloads[variant]
	if !found {
		handleError(ctx, w, apierrors.ErrVariantNotFound, logdata)
		return
	}

	if err := WriteJSONBody(download, w, http.StatusOK); err != nil {
		handleError(ctx, w, err, logdata)
		return
	}
	log.Info(ctx, "Successfully retrieved download", logdata)
}

// UpdateDownloadHandler is a handler to update an image download variant
func (api *API) UpdateDownloadHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	vars := mux.Vars(req)
	id := vars["id"]
	variant := vars["variant"]
	hColID := ctx.Value(handlers.CollectionID.Context())
	logdata := log.Data{
		handlers.CollectionID.Header(): hColID,
		"request-id":                   ctx.Value(dpreq.RequestIdKey),
		"image-id":                     id,
		"download-variant":             variant,
	}

	// Unmarshal download variant from body and validate it
	download := &models.Download{}
	if err := ReadJSONBody(ctx, req.Body, download); err != nil {
		handleError(ctx, w, err, logdata)
		return
	}

	// Validate a possible mismatch of variant id
	if download.ID != "" && download.ID != variant {
		handleError(ctx, w, apierrors.ErrVariantIDMismatch, logdata)
		return
	}
	download.ID = variant

	// generic download validation
	if err := download.Validate(); err != nil {
		handleError(ctx, w, err, logdata)
		return
	}

	// Acquire lock for image ID, and defer unlocking
	lockID, err := api.mongoDB.AcquireImageLock(ctx, id)
	if err != nil {
		handleError(ctx, w, err, logdata)
		return
	}
	defer api.unlockImage(ctx, lockID)

	// get image from mongoDB by id
	image, err := api.mongoDB.GetImage(req.Context(), id)
	if err != nil {
		handleError(ctx, w, err, logdata)
		return
	}

	// check for existing variant
	existing, found := image.Downloads[variant]
	if !found {
		handleError(ctx, w, apierrors.ErrVariantNotFound, logdata)
		return
	}

	// Validate download variant against parent image state
	if validationErr := download.ValidateForImage(image); validationErr != nil {
		logdata["current_image_state"] = image.State
		logdata["target_download_state"] = download.State
		handleError(ctx, w, validationErr, logdata)
		return
	}

	// Validate download variant against existing variant state
	if transitionErr := download.ValidateTransitionFrom(&existing); transitionErr != nil {
		logdata["current_image_state"] = existing.State
		logdata["target_download_state"] = download.State
		handleError(ctx, w, transitionErr, logdata)
		return
	}

	// Copy Links from existing
	download.Links = existing.Links

	// Update new download to existing image
	image.Downloads[variant] = *download

	// Update image state based on change to download
	image.State = image.UpdatedState()
	if download.State == models.StateDownloadFailed.String() {
		image.Error = fmt.Sprintf("error in variant '%s'", variant)
	}

	// Update image in mongo DB
	err = api.mongoDB.UpsertImage(ctx, id, image)
	if err != nil {
		handleError(ctx, w, err, logdata)
		return
	}

	if err := WriteJSONBody(download, w, http.StatusOK); err != nil {
		handleError(ctx, w, err, logdata)
		return
	}
	log.Info(ctx, "successfully updated download variant", logdata)
}

// PublishImageHandler is a handler that triggers the publishing of an image
func (api *API) PublishImageHandler(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	vars := mux.Vars(req)
	id := vars["id"]
	hColID := ctx.Value(handlers.CollectionID.Context())
	logdata := log.Data{
		handlers.CollectionID.Header(): hColID,
		"request-id":                   ctx.Value(dpreq.RequestIdKey),
		"image-id":                     id,
	}

	imageUpdate := &models.Image{State: models.StatePublished.String()}

	// Acquire lock for image ID, and defer unlocking
	lockID, err := api.mongoDB.AcquireImageLock(ctx, id)
	if err != nil {
		handleError(ctx, w, err, logdata)
		return
	}
	defer api.unlockImage(ctx, lockID)

	// get image from mongoDB by id
	existingImage, err := api.mongoDB.GetImage(req.Context(), id)
	if err != nil {
		handleError(ctx, w, err, logdata)
		return
	}

	// validate that the publish transition state is allowed
	if !existingImage.StateTransitionAllowed(imageUpdate.State) {
		logdata["current_image_state"] = existingImage.State
		logdata["target_image_state"] = imageUpdate.State
		handleError(ctx, w, apierrors.ErrImageStateTransitionNotAllowed, logdata)
		return
	}

	startTime := time.Now().UTC()

	// update image variants
	imageUpdate.Downloads = map[string]models.Download{}
	for variant := range existingImage.Downloads {
		imageUpdate.Downloads[variant] = models.Download{
			ID:             variant,
			State:          models.StateDownloadPublished.String(),
			Href:           fmt.Sprintf("%s/images/%s/%s/%s", api.downloadServiceURL, id, variant, existingImage.Filename),
			PublishStarted: &startTime,
		}
	}

	// Update image in mongo DB
	_, err = api.mongoDB.UpdateImage(ctx, id, imageUpdate)
	if err != nil {
		handleError(ctx, w, err, logdata)
		return
	}

	// Generate 'image published' events for all download variants
	events := generateImagePublishEvents(existingImage)

	// Send 'image published' kafka messages corresponding to all the download variants
	log.Info(ctx, "sending image published messages", logdata)
	for _, e := range events {
		if err := api.publishedProducer.Send(ctx, schema.ImagePublishedEvent, e); err != nil {
			handleError(ctx, w, err, logdata)
			return
		}
	}

	// Publish handler does not return any content on success
	w.WriteHeader(http.StatusNoContent)
}

// generateImagePublishEvents creates a kafka 'image-published' event for each download variant for the provided image.
func generateImagePublishEvents(image *models.Image) (events []*event.ImagePublished) {
	for i := range image.Downloads {
		variant := image.Downloads[i]
		srcPath := path.Join("images", image.ID, variant.ID)
		events = append(events, &event.ImagePublished{
			SrcPath:      srcPath,
			DstPath:      path.Join(srcPath, image.Filename),
			ImageID:      image.ID,
			ImageVariant: variant.ID,
		})
	}
	return events
}

// unlockImage unlocks the provided image lockID
func (api *API) unlockImage(ctx context.Context, lockID string) {
	api.mongoDB.UnlockImage(ctx, lockID)
}

// rewriteImageLinks rewrites the self and download links of a given image
func rewriteImageLinks(ctx context.Context, builder links.Builder, image *models.Image) error {
	if image.Links == nil || image.Links.Self == "" || image.Links.Downloads == "" {
		log.Warn(ctx, "image link missing, skipping", log.Data{"image_id": image.ID})
		return nil
	}

	var err error

	image.Links.Self, err = builder.BuildLink(image.Links.Self)
	if err != nil {
		log.Error(ctx, "could not build self link", err, log.Data{"link": image.Links.Self})
		return err
	}

	image.Links.Downloads, err = builder.BuildLink(image.Links.Downloads)
	if err != nil {
		log.Error(ctx, "could not build download link", err, log.Data{"link": image.Links.Downloads})
		return err
	}

	return nil
}

// rewriteDownloadLinks rewrites the self and image links of a given image download
func rewriteDownloadLinks(ctx context.Context, builder links.Builder, download models.Download) error {
	var err error

	download.Links.Self, err = builder.BuildLink(download.Links.Self)
	if err != nil {
		log.Error(ctx, "could not build self link", err, log.Data{"link": download.Links.Self})
		return err
	}

	download.Links.Image, err = builder.BuildLink(download.Links.Image)
	if err != nil {
		log.Error(ctx, "could not build image link", err, log.Data{"link": download.Links.Image})
		return err
	}

	return nil
}
