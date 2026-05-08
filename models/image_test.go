package models_test

import (
	"strings"
	"testing"
	"time"

	"github.com/ONSdigital/dp-image-api/apierrors"
	"github.com/ONSdigital/dp-image-api/models"
	. "github.com/smartystreets/goconvey/convey"
)

// Constants for testing
const (
	testVariantOriginal = "original"
	testVariant1        = "var1"
	testVariant2        = "var2"
	testDownloadType    = "originally uploaded file"
	testStateWrong      = "wrong"
	testStateUploaded   = "uploaded"
)

var (
	testImportStarted   = time.Date(2020, time.April, 26, 8, 5, 52, 0, time.UTC)
	testImportCompleted = time.Date(2020, time.April, 26, 8, 7, 32, 0, time.UTC)
)

func TestImageAllDownloadsOfState(t *testing.T) {
	Convey("Given an image with all download variants in completed state", t, func() {
		image := models.Image{
			Downloads: map[string]models.Download{
				testVariantOriginal: {State: models.StateDownloadCompleted.String()},
				testVariant1:        {State: models.StateDownloadCompleted.String()},
				testVariant2:        {State: models.StateDownloadCompleted.String()},
			},
		}
		Convey("Then AllDownloadsOfStateShould return expected values", func() {
			So(image.AllDownloadsOfState(models.StateDownloadCompleted), ShouldBeTrue)
			So(image.AllDownloadsOfState(models.StateDownloadImporting), ShouldBeFalse)
		})
	})

	Convey("Given an image with all download variants in completed state except the original", t, func() {
		image := models.Image{
			Downloads: map[string]models.Download{
				testVariantOriginal: {State: models.StateDownloadPublished.String()},
				testVariant1:        {State: models.StateDownloadCompleted.String()},
				testVariant2:        {State: models.StateDownloadCompleted.String()},
			},
		}
		Convey("Then AllDownloadsOfStateShould return expected values", func() {
			So(image.AllDownloadsOfState(models.StateDownloadPublished), ShouldBeFalse)
			So(image.AllDownloadsOfState(models.StateDownloadCompleted), ShouldBeFalse)
			So(image.AllDownloadsOfState(models.StateDownloadImporting), ShouldBeFalse)
		})
	})

	Convey("Given an image with no download variants", t, func() {
		image := models.Image{}
		Convey("Then AllDownloadsOfStateShould return expected values", func() {
			So(image.AllDownloadsOfState(models.StateDownloadPublished), ShouldBeFalse)
		})
	})
}

func TestImageAnyDownloadsOfState(t *testing.T) {
	Convey("Given an image with all download variants in completed state", t, func() {
		image := models.Image{
			Downloads: map[string]models.Download{
				testVariantOriginal: {State: models.StateDownloadCompleted.String()},
				testVariant1:        {State: models.StateDownloadCompleted.String()},
				testVariant2:        {State: models.StateDownloadCompleted.String()},
			},
		}
		Convey("Then AnyDownloadsOfStateShould return expected values", func() {
			So(image.AnyDownloadsOfState(models.StateDownloadCompleted), ShouldBeTrue)
			So(image.AnyDownloadsOfState(models.StateDownloadImporting), ShouldBeFalse)
		})
	})

	Convey("Given an image with all download variants in completed state except the original", t, func() {
		image := models.Image{
			Downloads: map[string]models.Download{
				testVariantOriginal: {State: models.StateDownloadPublished.String()},
				testVariant1:        {State: models.StateDownloadCompleted.String()},
				testVariant2:        {State: models.StateDownloadCompleted.String()},
			},
		}
		Convey("Then AnyDownloadsOfStateShould return expected values", func() {
			So(image.AnyDownloadsOfState(models.StateDownloadPublished), ShouldBeTrue)
			So(image.AnyDownloadsOfState(models.StateDownloadCompleted), ShouldBeTrue)
			So(image.AnyDownloadsOfState(models.StateDownloadImporting), ShouldBeFalse)
		})
	})

	Convey("Given an image with no download variants", t, func() {
		image := models.Image{}
		Convey("Then AnyDownloadsOfStateShould return expected values", func() {
			So(image.AnyDownloadsOfState(models.StateDownloadPublished), ShouldBeFalse)
		})
	})
}

func TestImageValidation(t *testing.T) {
	Convey("Given an empty image, it is successfully validated", t, func() {
		image := models.Image{
			State: models.StateCreated.String(),
		}
		err := image.Validate()
		So(err, ShouldBeNil)
	})

	Convey("Given an image with no state supplied, it fails to validate  with the expected error", t, func() {
		image := models.Image{}
		err := image.Validate()
		So(err, ShouldResemble, apierrors.ErrImageInvalidState)
	})

	Convey("Given an image with a filename that is longer than the maximum allowed, it fails to validate with the expected error", t, func() {
		image := models.Image{
			Filename: strings.Repeat("Llanfairpwllgwyngyllgogerychwyrndrobwllllantysiliogogogoch", 10),
		}
		err := image.Validate()
		So(err, ShouldResemble, apierrors.ErrImageFilenameTooLong)
	})

	Convey("Given an image with a state that does not correspond to any expected state, it fails to validate with the expected error", t, func() {
		image := models.Image{
			State: testStateWrong,
		}
		err := image.Validate()
		So(err, ShouldResemble, apierrors.ErrImageInvalidState)
	})

	Convey("Given an image with a state of uploaded has no upload section it fails to validate with the expected error", t, func() {
		image := models.Image{
			State: testStateUploaded,
		}
		err := image.Validate()
		So(err, ShouldResemble, apierrors.ErrImageUploadEmpty)
	})

	Convey("Given an image with a state of uploaded has no path in its upload section it fails to validate with the expected error", t, func() {
		image := models.Image{
			State:  testStateUploaded,
			Upload: &models.Upload{},
		}
		err := image.Validate()
		So(err, ShouldResemble, apierrors.ErrImageUploadPathEmpty)
	})

	Convey("Given a fully populated valid image with a valid download variant, it is successfully validated", t, func() {
		image := models.Image{
			ID:           "123",
			CollectionID: "456",
			State:        models.StatePublished.String(),
			Filename:     "image-file-name",
			License: &models.License{
				Title: "testLicense",
				Href:  "http://testLicense.co.uk",
			},
			Upload: &models.Upload{
				Path: "image-upload-path",
			},
			Type: "icon",
		}
		err := image.Validate()
		So(err, ShouldBeNil)
	})
}

func TestImageValidateTransitionFrom(t *testing.T) {
	Convey("Given an existing image in an uploaded state", t, func() {
		existing := &models.Image{
			State: models.StateUploaded.String(),
		}

		Convey("When we try to transition to an Importing state", func() {
			image := &models.Image{
				State: models.StateImporting.String(),
			}
			err := image.ValidateTransitionFrom(existing)
			So(err, ShouldBeNil)
		})

		Convey("When we try to transition to a Created state", func() {
			image := &models.Image{
				State: models.StateCreated.String(),
			}
			err := image.ValidateTransitionFrom(existing)
			So(err, ShouldResemble, apierrors.ErrImageStateTransitionNotAllowed)
		})
	})

	Convey("Given an existing image in a Completed state", t, func() {
		existing := &models.Image{
			State: models.StateCompleted.String(),
		}

		Convey("When we try to transition to an Importing state", func() {
			image := &models.Image{
				State: models.StateImporting.String(),
			}
			err := image.ValidateTransitionFrom(existing)
			So(err, ShouldResemble, apierrors.ErrImageStateTransitionNotAllowed)
		})

		Convey("When we try to transition to a Deleted state", func() {
			image := &models.Image{
				State: models.StateDeleted.String(),
			}
			err := image.ValidateTransitionFrom(existing)
			So(err, ShouldResemble, apierrors.ErrImageAlreadyCompleted)
		})
	})
}

func TestImageStateTransitionAllowed(t *testing.T) {
	Convey("Given an image in created state", t, func() {
		image := models.Image{
			State: models.StateCreated.String(),
		}
		validateTransitionsToCreated(image)
	})

	Convey("Given an image with a wrong state value, then no transition is allowed", t, func() {
		image := models.Image{State: testStateWrong}
		validateTransitionsToCreated(image)
	})

	Convey("Given an image without state, then created state is assumed when checking for transitions", t, func() {
		image := models.Image{}
		validateTransitionsToCreated(image)
	})
}

func TestImageUpdatedState(t *testing.T) {
	Convey("Given an image in FailedImport State", t, func() {
		image := models.Image{
			State: models.StateFailedImport.String(),
		}
		Convey("Then UpdatedState should be the same", func() {
			So(image.UpdatedState(), ShouldResemble, models.StateFailedImport.String())
		})
	})

	Convey("Given an image in FailedPublish State", t, func() {
		image := models.Image{
			State: models.StateFailedPublish.String(),
		}
		Convey("Then UpdatedState should be the same", func() {
			So(image.UpdatedState(), ShouldResemble, models.StateFailedPublish.String())
		})
	})

	Convey("Given an image in Importing State with any downloads in Failed state", t, func() {
		image := models.Image{
			State: models.StateImporting.String(),
			Downloads: map[string]models.Download{
				testVariantOriginal: {State: models.StateDownloadImporting.String()},
				testVariant1:        {State: models.StateDownloadImported.String()},
				testVariant2:        {State: models.StateDownloadFailed.String()},
			},
		}
		Convey("Then UpdatedState should be FailedImport", func() {
			So(image.UpdatedState(), ShouldResemble, models.StateFailedImport.String())
		})
	})

	Convey("Given an image in Importing State with all downloads in Imported state", t, func() {
		image := models.Image{
			State: models.StateImporting.String(),
			Downloads: map[string]models.Download{
				testVariantOriginal: {State: models.StateDownloadImported.String()},
				testVariant1:        {State: models.StateDownloadImported.String()},
			},
		}
		Convey("Then UpdatedState should be Impoprted", func() {
			So(image.UpdatedState(), ShouldResemble, models.StateImported.String())
		})
	})

	Convey("Given an image in Importing State with downloads in Imported and Importing state", t, func() {
		image := models.Image{
			State: models.StateImporting.String(),
			Downloads: map[string]models.Download{
				testVariantOriginal: {State: models.StateDownloadImporting.String()},
				testVariant1:        {State: models.StateDownloadImported.String()},
			},
		}
		Convey("Then UpdatedState should remain Importing", func() {
			So(image.UpdatedState(), ShouldResemble, models.StateImporting.String())
		})
	})

	Convey("Given an image in Published State with any downloads in Failed state", t, func() {
		image := models.Image{
			State: models.StatePublished.String(),
			Downloads: map[string]models.Download{
				testVariantOriginal: {State: models.StateDownloadCompleted.String()},
				testVariant1:        {State: models.StateDownloadImported.String()},
				testVariant2:        {State: models.StateDownloadFailed.String()},
			},
		}
		Convey("Then UpdatedState should be FailedPublish", func() {
			So(image.UpdatedState(), ShouldResemble, models.StateFailedPublish.String())
		})
	})

	Convey("Given an image in Published State with all downloads in Completed state", t, func() {
		image := models.Image{
			State: models.StatePublished.String(),
			Downloads: map[string]models.Download{
				testVariantOriginal: {State: models.StateDownloadCompleted.String()},
				testVariant1:        {State: models.StateDownloadCompleted.String()},
			},
		}
		Convey("Then UpdatedState should be Completed", func() {
			So(image.UpdatedState(), ShouldResemble, models.StateCompleted.String())
		})
	})

	Convey("Given an image in Published State with downloads in Published and Completed state", t, func() {
		image := models.Image{
			State: models.StatePublished.String(),
			Downloads: map[string]models.Download{
				testVariantOriginal: {State: models.StateCompleted.String()},
				testVariant1:        {State: models.StatePublished.String()},
			},
		}
		Convey("Then UpdatedState should remain Published", func() {
			So(image.UpdatedState(), ShouldResemble, models.StatePublished.String())
		})
	})
}

// validateTransitionsToCreated validates that the provided image can transition to created state,
// and not to any forbidden of invalid state
func validateTransitionsToCreated(image models.Image) {
	Convey("Then an allowed transition is successfully checked", func() {
		So(image.StateTransitionAllowed(models.StateUploaded.String()), ShouldBeTrue)
	})
	Convey("Then a forbidden transition to a valid state is not allowed", func() {
		So(image.StateTransitionAllowed(models.StatePublished.String()), ShouldBeFalse)
	})
	Convey("Then a transition to an invalid state is not allowed", func() {
		So(image.StateTransitionAllowed(testStateWrong), ShouldBeFalse)
	})
}

func TestDownloadValidation(t *testing.T) {
	Convey("Given an empty download variant, it is successfully validated", t, func() {
		download := models.Download{}
		err := download.Validate()
		So(err, ShouldBeNil)
	})

	Convey("Given a download variant with an invalid state name, it fails to validate with the expected error", t, func() {
		download := models.Download{
			State: testStateWrong,
		}
		err := download.Validate()
		So(err, ShouldResemble, apierrors.ErrImageDownloadInvalidState)
	})

	Convey("Given a download variant with a valid state name, it is successfully validated", t, func() {
		download := models.Download{
			State: models.StateDownloadImported.String(),
		}
		err := download.Validate()
		So(err, ShouldBeNil)
	})
}

func TestDownloadValidateTransitionFrom(t *testing.T) {
	Convey("And an existing download variant with valid importing state", t, func() {
		existing := &models.Download{
			ID:            testVariantOriginal,
			Type:          testDownloadType,
			State:         models.StateDownloadImporting.String(),
			ImportStarted: &testImportStarted,
		}

		Convey("Then with a new download variant with valid state, the transition successfully validates", func() {
			download := models.Download{
				ID:              testVariantOriginal,
				Type:            testDownloadType,
				State:           models.StateDownloadImported.String(),
				ImportStarted:   &testImportStarted,
				ImportCompleted: &testImportCompleted,
			}
			err := download.ValidateTransitionFrom(existing)
			So(err, ShouldBeNil)
		})

		Convey("Then with a new download variant with a changed type, the transition fails validation", func() {
			download := models.Download{
				ID:              testVariantOriginal,
				Type:            "some other type",
				State:           models.StateDownloadImported.String(),
				ImportStarted:   &testImportStarted,
				ImportCompleted: &testImportCompleted,
			}
			err := download.ValidateTransitionFrom(existing)
			So(err, ShouldNotBeNil)
			So(err, ShouldResemble, apierrors.ErrImageDownloadTypeMismatch)
		})
	})

	Convey("And an existing download variant with valid imported state", t, func() {
		existing := &models.Download{
			ID:              testVariantOriginal,
			Type:            testDownloadType,
			State:           models.StateDownloadImported.String(),
			ImportStarted:   &testImportStarted,
			ImportCompleted: &testImportCompleted,
		}

		Convey("Then with a new download variant with importing state, the transition fails validation", func() {
			download := models.Download{
				ID:            testVariantOriginal,
				Type:          testDownloadType,
				State:         models.StateDownloadImporting.String(),
				ImportStarted: &testImportStarted,
			}
			err := download.ValidateTransitionFrom(existing)
			So(err, ShouldNotBeNil)
			So(err, ShouldResemble, apierrors.ErrVariantStateTransitionNotAllowed)
		})
	})
}

func TestDownloadValidateForImage(t *testing.T) {
	Convey("Given an existing image in importing state", t, func() {
		image := &models.Image{
			State: models.StateImporting.String(),
		}
		Convey("Then with a new download variant of state Importing is valid", func() {
			download := models.Download{
				ID:            testVariantOriginal,
				Type:          testDownloadType,
				State:         models.StateDownloadImporting.String(),
				ImportStarted: &testImportStarted,
			}
			err := download.ValidateForImage(image)
			So(err, ShouldBeNil)
		})
	})

	Convey("Given an existing image in imported state", t, func() {
		image := &models.Image{
			State: models.StateImported.String(),
		}
		Convey("Then a  download variant of state Importing is invalid", func() {
			download := models.Download{
				State: models.StateDownloadImporting.String(),
			}
			err := download.ValidateForImage(image)
			So(err, ShouldNotBeNil)
			So(err, ShouldResemble, apierrors.ErrImageNotImporting)
		})
		Convey("Then a download variant of state Completed is invalid", func() {
			download := models.Download{
				State: models.StateDownloadCompleted.String(),
			}
			err := download.ValidateForImage(image)
			So(err, ShouldNotBeNil)
			So(err, ShouldResemble, apierrors.ErrImageNotPublished)
		})
	})

	Convey("Given an existing image in published state", t, func() {
		image := &models.Image{
			State: models.StatePublished.String(),
		}
		Convey("Then a download variant of state Completed is valid", func() {
			download := models.Download{
				State: models.StateDownloadCompleted.String(),
			}
			err := download.ValidateForImage(image)
			So(err, ShouldBeNil)
		})
		Convey("Then with a new download variant of state Imported is invalid", func() {
			download := models.Download{
				State: models.StateDownloadImported.String(),
			}
			err := download.ValidateForImage(image)
			So(err, ShouldNotBeNil)
			So(err, ShouldResemble, apierrors.ErrImageNotImporting)
		})
	})

	Convey("Given an existing image in completed state", t, func() {
		image := &models.Image{
			State: models.StateCompleted.String(),
		}
		Convey("Then a download variant of state Completed is invalid", func() {
			download := models.Download{
				State: models.StateDownloadCompleted.String(),
			}
			err := download.ValidateForImage(image)
			So(err, ShouldNotBeNil)
			So(err, ShouldResemble, apierrors.ErrImageAlreadyCompleted)
		})
		Convey("Then a download variant of state Importing is invalid", func() {
			download := models.Download{
				State: models.StateDownloadImporting.String(),
			}
			err := download.ValidateForImage(image)
			So(err, ShouldNotBeNil)
			So(err, ShouldResemble, apierrors.ErrImageNotImporting)
		})
	})
}

func TestDownloadStateTransitionAllowed(t *testing.T) {
	Convey("Given an image download variant in pending state", t, func() {
		download := models.Download{
			State: models.StateDownloadPending.String(),
		}
		validateDownloadTransitionsToPending(download)
	})

	Convey("Given an image download variant with a wrong state value, then no transition is allowed", t, func() {
		download := models.Download{State: testStateWrong}
		validateDownloadTransitionsToPending(download)
	})

	Convey("Given an image download variant without state, then pending state is assumed when checking for transitions", t, func() {
		download := models.Download{}
		validateDownloadTransitionsToPending(download)
	})
}

func validateDownloadTransitionsToPending(download models.Download) {
	Convey("Then an allowed transition is successfully checked", func() {
		So(download.StateTransitionAllowed(models.StateImporting.String()), ShouldBeTrue)
	})
	Convey("Then a forbidden transition to a valid state is not allowed", func() {
		So(download.StateTransitionAllowed(models.StateDownloadPublished.String()), ShouldBeFalse)
	})
	Convey("Then a transition to an invalid state is not allowed", func() {
		So(download.StateTransitionAllowed(testStateWrong), ShouldBeFalse)
	})
}
