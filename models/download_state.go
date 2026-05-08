package models

import "github.com/ONSdigital/dp-image-api/apierrors"

// DownloadState - iota enum of possible image states
type DownloadState int

// Possible values for a State of a download variant. It can only be one of the following:
const (
	StateDownloadPending DownloadState = iota
	StateDownloadImporting
	StateDownloadImported
	StateDownloadPublished
	StateDownloadCompleted
	StateDownloadFailed
)

// nolint:goconst // not really a reused constant
var downloadStateValues = []string{"pending", "importing", "imported", "published", "completed", "failed"}

// String returns the string representation of a downlad state
func (ds DownloadState) String() string {
	return downloadStateValues[ds]
}

// ParseDownloadState returns a download state from its string representation
func ParseDownloadState(downloadStateStr string) (DownloadState, error) {
	for s, valid := range downloadStateValues {
		if downloadStateStr == valid {
			return DownloadState(s), nil
		}
	}
	return -1, apierrors.ErrImageDownloadInvalidState
}

// TransitionAllowed returns true only if the transition from the current state and the provided target DownloadState is allowed
func (ds DownloadState) TransitionAllowed(target DownloadState) bool {
	switch ds {
	case StateDownloadPending:
		switch target {
		case StateDownloadImporting:
			return true
		default:
			return false
		}
	case StateDownloadImporting:
		switch target {
		case StateDownloadImported, StateDownloadFailed:
			return true
		default:
			return false
		}
	case StateDownloadImported:
		switch target {
		case StateDownloadPublished:
			return true
		default:
			return false
		}
	case StateDownloadPublished:
		switch target {
		case StateDownloadCompleted:
			return true
		default:
			return false
		}
	case StateDownloadCompleted:
		return false
	case StateDownloadFailed:
		return false
	default:
		return false
	}
}
