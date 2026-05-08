package mongo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	errs "github.com/ONSdigital/dp-image-api/apierrors"
	"github.com/ONSdigital/dp-image-api/config"
	"github.com/ONSdigital/dp-image-api/models"
	"github.com/ONSdigital/log.go/v2/log"

	mongolock "github.com/ONSdigital/dp-mongodb/v3/dplock"
	mongohealth "github.com/ONSdigital/dp-mongodb/v3/health"
	mongodriver "github.com/ONSdigital/dp-mongodb/v3/mongodb"
	"go.mongodb.org/mongo-driver/bson"
)

const (
	labelId = "id"
)

type Mongo struct {
	mongodriver.MongoDriverConfig

	connection   *mongodriver.MongoConnection
	healthClient *mongohealth.CheckMongoClient
	lockClient   *mongolock.Lock
}

// NewMongoStore creates a new Mongo object encapsulating a connection to the mongo server/cluster with the given configuration,
// and a health client to check the health of the mongo server/cluster
func NewMongoStore(ctx context.Context, cfg config.MongoConfig) (m *Mongo, err error) {
	m = &Mongo{MongoDriverConfig: cfg}
	m.connection, err = mongodriver.Open(&m.MongoDriverConfig)
	if err != nil {
		return nil, err
	}

	databaseCollectionBuilder := map[mongohealth.Database][]mongohealth.Collection{
		mongohealth.Database(m.Database): {
			mongohealth.Collection(m.ActualCollectionName(config.ImagesCollection)),
			mongohealth.Collection(m.ActualCollectionName(config.ImagesLockCollection)),
		},
	}
	m.healthClient = mongohealth.NewClientWithCollections(m.connection, databaseCollectionBuilder)
	m.lockClient = mongolock.New(ctx, m.connection, m.ActualCollectionName(config.ImagesCollection))

	return m, nil
}

// AcquireImageLock tries to lock the provided imageID.
// If the image is already locked, this function will block until it's released,
// at which point we acquire the lock and return.
func (m *Mongo) AcquireImageLock(ctx context.Context, imageID string) (lockID string, err error) {
	return m.lockClient.Acquire(ctx, imageID)
}

// UnlockImage releases an exclusive mongoDB lock for the provided lockId (if it exists)
func (m *Mongo) UnlockImage(ctx context.Context, lockID string) {
	m.lockClient.Unlock(ctx, lockID)
}

// Close closes the mongo session and returns any error
func (m *Mongo) Close(ctx context.Context) error {
	m.lockClient.Close(ctx)
	return m.connection.Close(ctx)
}

// Checker is called by the healthcheck library to check the health state of this mongoDB instance
func (m *Mongo) Checker(ctx context.Context, state *healthcheck.CheckState) error {
	return m.healthClient.Checker(ctx, state)
}

// GetImages retrieves all images documents corresponding to the provided collectionID
func (m *Mongo) GetImages(ctx context.Context, collectionID string) ([]models.Image, error) {
	log.Info(ctx, "getting images for collectionID", log.Data{"collectionID": collectionID})

	// Filter by collectionID, if provided
	colIDFilter := make(bson.M)
	if collectionID != "" {
		colIDFilter["collection_id"] = collectionID
	}

	var results []models.Image
	_, err := m.connection.Collection(m.ActualCollectionName(config.ImagesCollection)).Find(ctx, colIDFilter, &results)
	if err != nil {
		return nil, err
	}

	return results, nil
}

// GetImage retrieves an image document by its ID
func (m *Mongo) GetImage(ctx context.Context, id string) (*models.Image, error) {
	log.Info(ctx, "getting image by ID", log.Data{labelId: id})

	var image models.Image
	err := m.connection.Collection(m.ActualCollectionName(config.ImagesCollection)).FindOne(ctx, bson.M{"_id": id}, &image)
	if err != nil {
		if errors.Is(err, mongodriver.ErrNoDocumentFound) {
			return nil, errs.ErrImageNotFound
		}
		return nil, err
	}

	return &image, nil
}

// UpdateImage updates an existing image document
func (m *Mongo) UpdateImage(ctx context.Context, id string, image *models.Image) (bool, error) {
	log.Info(ctx, "updating image", log.Data{labelId: id})

	updates := createImageUpdateQuery(ctx, id, image)
	if len(updates) == 0 {
		log.Info(ctx, "nothing to update")
		return false, nil
	}

	//nolint:goconst // mongo keyword, clearer without const
	update := bson.M{"$set": updates, "$currentDate": bson.M{"last_updated": true}}
	if _, err := m.connection.Collection(m.ActualCollectionName(config.ImagesCollection)).Must().UpdateById(ctx, id, update); err != nil {
		if errors.Is(err, mongodriver.ErrNoDocumentFound) {
			return false, errs.ErrImageNotFound
		}
		return false, err
	}

	return true, nil
}

// createImageUpdateQuery generates the bson model to update an image with the provided image update.
// Fields present in mongoDB will not be deleted if they are not present in the image update object.
//
//nolint:gocognit,gocyclo // cognitive and cyclomatic complexity too high but not in scope for this sprint
func createImageUpdateQuery(ctx context.Context, id string, image *models.Image) bson.M {
	updates := make(bson.M)

	log.Info(ctx, "building update query for image resource", log.INFO, log.Data{"image_id": id, "image": image, "updates": updates})

	if image.CollectionID != "" {
		updates["collection_id"] = image.CollectionID
	}
	if image.State != "" {
		updates["state"] = image.State
	}
	if image.Error != "" {
		updates["error"] = image.Error
	}
	if image.Filename != "" {
		updates["filename"] = image.Filename
	}
	if image.Type != "" {
		updates["type"] = image.Type
	}

	if image.License != nil {
		if image.License.Title != "" {
			updates["license.title"] = image.License.Title
		}
		if image.License.Href != "" {
			updates["license.href"] = image.License.Href
		}
	}

	if image.Upload != nil {
		if image.Upload.Path != "" {
			updates["upload"] = image.Upload
		}
	}

	if image.Downloads != nil {
		for i := range image.Downloads {
			variant := i
			download := image.Downloads[i]
			if download.ID != "" {
				updates[fmt.Sprintf("downloads.%s.id", variant)] = download.ID
			}
			if download.Size != nil {
				updates[fmt.Sprintf("downloads.%s.size", variant)] = download.Size
			}
			if download.Type != "" {
				updates[fmt.Sprintf("downloads.%s.type", variant)] = download.Type
			}
			if download.Width != nil {
				updates[fmt.Sprintf("downloads.%s.width", variant)] = download.Width
			}
			if download.Height != nil {
				updates[fmt.Sprintf("downloads.%s.height", variant)] = download.Height
			}
			if download.Links != nil {
				updates[fmt.Sprintf("downloads.%s.links", variant)] = download.Links
			}
			if download.Private != "" {
				updates[fmt.Sprintf("downloads.%s.private_bucket", variant)] = download.Private
			}
			if download.Href != "" {
				updates[fmt.Sprintf("downloads.%s.href", variant)] = download.Href
			}
			if download.State != "" {
				updates[fmt.Sprintf("downloads.%s.state", variant)] = download.State
			}
			if download.Error != "" {
				updates[fmt.Sprintf("downloads.%s.error", variant)] = download.Error
			}
			if download.ImportStarted != nil {
				updates[fmt.Sprintf("downloads.%s.import_started", variant)] = download.ImportStarted
			}
			if download.ImportCompleted != nil {
				updates[fmt.Sprintf("downloads.%s.import_completed", variant)] = download.ImportCompleted
			}
			if download.PublishStarted != nil {
				updates[fmt.Sprintf("downloads.%s.publish_started", variant)] = download.PublishStarted
			}
			if download.PublishCompleted != nil {
				updates[fmt.Sprintf("downloads.%s.publish_completed", variant)] = download.PublishCompleted
			}
		}
	}
	return updates
}

// UpsertImage adds or overides an existing image document
func (m *Mongo) UpsertImage(ctx context.Context, id string, image *models.Image) (err error) {
	log.Info(ctx, "upserting image", log.Data{labelId: id})

	update := bson.M{
		"$set": image, //nolint:goconst // mongo keyword, clearer without const
		"$setOnInsert": bson.M{
			"last_updated": time.Now(),
		},
	}

	_, err = m.connection.Collection(m.ActualCollectionName(config.ImagesCollection)).UpsertById(ctx, id, update)
	return
}
