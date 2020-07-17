package azrblob

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

// A container name must be a valid DNS name, conforming to the following naming rules:
// Container names must start or end with a letter or number, and can contain only letters, numbers, and the dash (-) character.
// Every dash (-) character must be immediately preceded and followed by a letter or number; consecutive dashes are not permitted in container names.
// All letters in a container name must be lowercase.
// Container names must be from 3 through 63 characters long.

// A blob name must conforming to the following naming rules:
// A blob name can contain any combination of characters.
// A blob name must be at least one character long and cannot be more than 1,024 characters long, for blobs in Azure Storage.
// The Azure Storage emulator supports blob names up to 256 characters long. For more information, see Use the Azure storage emulator for development and testing.
// Blob names are case-sensitive.
// Reserved URL characters must be properly escaped.
// The number of path segments comprising the blob name cannot exceed 254. A path segment is the string between consecutive delimiter characters (e.g., the forward slash '/') that corresponds to the name of a virtual directory.

func (fs *Fs) getContainers() ([]string, error) {
	var containers []string
	for marker := (azblob.Marker{}); marker.NotDone(); {
		listCont, err := fs.serviceURL.ListContainersSegment(*fs.ctx, marker, azblob.ListContainersSegmentOptions{})
		if err != nil {
			LogError(err)
			return containers, err
		}
		marker = listCont.NextMarker
		for _, item := range listCont.ContainerItems {
			containers = append(containers, item.Name)
		}
	}
	return containers, nil
}

func (fs *Fs) createContainer(name string) error {
	if strings.ToLower(name) == "cdrs" {
		return fmt.Errorf("cannot create [%s] container", name)
	}
	containerURL := fs.serviceURL.NewContainerURL(strings.ToLower(name))
	_, err := containerURL.Create(*fs.ctx, azblob.Metadata{}, azblob.PublicAccessNone)
	if err != nil {
		LogError(err)
	}

	return err
}

func (fs *Fs) getBlobsInContainer() (blobs []string, err error) {
	containerURL := fs.serviceURL.NewContainerURL(fs.container)
	for marker := (azblob.Marker{}); marker.NotDone(); { // The parens around Marker{} are required to avoid compiler error.
		// Get a result segment starting with the blob indicated by the current Marker.
		listBlob, err := containerURL.ListBlobsFlatSegment(*fs.ctx, marker, azblob.ListBlobsSegmentOptions{})
		if err != nil {
			LogError(err)
			return blobs, err
		}

		// IMPORTANT: ListBlobs returns the start of the next segment; you MUST use this to get
		// the next segment (after processing the current result segment).
		marker = listBlob.NextMarker

		// Process the blobs returned in this result segment
		for _, blobInfo := range listBlob.Segment.BlobItems {
			blobs = append(blobs, blobInfo.Name)
		}
	}
	return blobs, nil
}
func (f *File) getBlobsInContainerFileInfoMarker(maxResults int32, prefix, filter string) (blobs []os.FileInfo, err error) {
	// https://godoc.org/github.com/Azure/azure-storage-blob-go/azblob#ListBlobsSegmentOptions
	// type ListBlobsSegmentOptions struct {
	// 	Details BlobListingDetails // No IncludeType header is produced if ""
	// 	Prefix  string             // No Prefix header is produced if ""
	// 	SetMaxResults sets the maximum desired results you want the service to return.
	// 	Note, the service may return fewer results than requested.
	// 	MaxResults=0 means no 'MaxResults' header specified.
	// 	MaxResults int32
	// }
	var options azblob.ListBlobsSegmentOptions
	if maxResults > 0 {
		options.MaxResults = maxResults
	}
	if prefix != "" {
		options.Prefix = prefix
	}

	var rexp *regexp.Regexp
	if filter != "" {
		rexp, err = getFilterRegExp(filter)
		if err != nil {
			return nil, err
		}
	}

	containerURL := f.fs.serviceURL.NewContainerURL(f.fs.container)
	if f.azureMarker.NotDone() {
		listBlob, err := containerURL.ListBlobsFlatSegment(*f.fs.ctx, f.azureMarker, options)
		if err != nil {
			LogError(err)
			return blobs, err
		}

		f.azureMarker = listBlob.NextMarker

		// https://godoc.org/github.com/Azure/azure-storage-blob-go/azblob#BlobProperties
		// type BlobProperties struct {
		// 	// XMLName is used for marshalling and is subject to removal in a future release.
		// 	XMLName      xml.Name   `xml:"Properties"`
		// 	CreationTime *time.Time `xml:"Creation-Time"`
		// 	LastModified time.Time  `xml:"Last-Modified"`
		// 	Etag         ETag       `xml:"Etag"`
		// 	// ContentLength - Size in bytes
		// 	ContentLength      *int64  `xml:"Content-Length"`
		// 	ContentType        *string `xml:"Content-Type"`
		// 	ContentEncoding    *string `xml:"Content-Encoding"`
		// 	ContentLanguage    *string `xml:"Content-Language"`
		// 	ContentMD5         []byte  `xml:"Content-MD5"`
		// 	ContentDisposition *string `xml:"Content-Disposition"`
		// 	CacheControl       *string `xml:"Cache-Control"`
		// 	BlobSequenceNumber *int64  `xml:"x-ms-blob-sequence-number"`
		// 	// BlobType - Possible values include: 'BlobBlockBlob', 'BlobPageBlob', 'BlobAppendBlob', 'BlobNone'
		// 	BlobType BlobType `xml:"BlobType"`
		// 	// LeaseStatus - Possible values include: 'LeaseStatusLocked', 'LeaseStatusUnlocked', 'LeaseStatusNone'
		// 	LeaseStatus LeaseStatusType `xml:"LeaseStatus"`
		// 	// LeaseState - Possible values include: 'LeaseStateAvailable', 'LeaseStateLeased', 'LeaseStateExpired', 'LeaseStateBreaking', 'LeaseStateBroken', 'LeaseStateNone'
		// 	LeaseState LeaseStateType `xml:"LeaseState"`
		// 	// LeaseDuration - Possible values include: 'LeaseDurationInfinite', 'LeaseDurationFixed', 'LeaseDurationNone'
		// 	LeaseDuration LeaseDurationType `xml:"LeaseDuration"`
		// 	CopyID        *string           `xml:"CopyId"`
		// 	// CopyStatus - Possible values include: 'CopyStatusPending', 'CopyStatusSuccess', 'CopyStatusAborted', 'CopyStatusFailed', 'CopyStatusNone'
		// 	CopyStatus             CopyStatusType `xml:"CopyStatus"`
		// 	CopySource             *string        `xml:"CopySource"`
		// 	CopyProgress           *string        `xml:"CopyProgress"`
		// 	CopyCompletionTime     *time.Time     `xml:"CopyCompletionTime"`
		// 	CopyStatusDescription  *string        `xml:"CopyStatusDescription"`
		// 	ServerEncrypted        *bool          `xml:"ServerEncrypted"`
		// 	IncrementalCopy        *bool          `xml:"IncrementalCopy"`
		// 	DestinationSnapshot    *string        `xml:"DestinationSnapshot"`
		// 	DeletedTime            *time.Time     `xml:"DeletedTime"`
		// 	RemainingRetentionDays *int32         `xml:"RemainingRetentionDays"`
		// 	// AccessTier - Possible values include: 'AccessTierP4', 'AccessTierP6', 'AccessTierP10', 'AccessTierP15', 'AccessTierP20', 'AccessTierP30', 'AccessTierP40', 'AccessTierP50', 'AccessTierP60', 'AccessTierP70', 'AccessTierP80', 'AccessTierHot', 'AccessTierCool', 'AccessTierArchive', 'AccessTierNone'
		// 	AccessTier         AccessTierType `xml:"AccessTier"`
		// 	AccessTierInferred *bool          `xml:"AccessTierInferred"`
		// 	// ArchiveStatus - Possible values include: 'ArchiveStatusRehydratePendingToHot', 'ArchiveStatusRehydratePendingToCool', 'ArchiveStatusNone'
		// 	ArchiveStatus             ArchiveStatusType `xml:"ArchiveStatus"`
		// 	CustomerProvidedKeySha256 *string           `xml:"CustomerProvidedKeySha256"`
		// 	AccessTierChangeTime      *time.Time        `xml:"AccessTierChangeTime"`
		// }
		for _, blobInfo := range listBlob.Segment.BlobItems {
			fi := FileInfo{
				directory:   false,
				name:        blobInfo.Name,
				sizeInBytes: *blobInfo.Properties.ContentLength,
				modTime:     blobInfo.Properties.LastModified,
			}
			if rexp != nil && !rexp.Match([]byte(blobInfo.Name)) {
				continue
			}
			blobs = append(blobs, fi)
		}
	}

	return blobs, nil
}

func (fs *Fs) getBlobURL(blob string) azblob.BlockBlobURL {
	containerURL := fs.serviceURL.NewContainerURL(fs.container)
	return containerURL.NewBlockBlobURL(blob)
}

func (fs *Fs) blobRead(blob string, offset, count int64) (*[]byte, error) {
	blobURL := fs.getBlobURL(blob)
	resp, err := blobURL.Download(*fs.ctx, offset, count, azblob.BlobAccessConditions{}, false)
	if err != nil {
		LogError(err)
		return nil, err
	}

	result, err := ioutil.ReadAll(resp.Body(azblob.RetryReaderOptions{}))
	if err != nil {
		LogError(err)
		return nil, err
	}

	if len(result) == 0 {
		LogError(io.EOF)
		return nil, io.EOF
	}

	return &result, nil
}

func (fs *Fs) blobStageBlock(blob, base64BlockID string, p *[]byte) (*azblob.BlockBlobStageBlockResponse, error) {
	blobURL := fs.getBlobURL(blob)
	return blobURL.StageBlock(*fs.ctx, base64BlockID, bytes.NewReader(*p), azblob.LeaseAccessConditions{}, nil)
}

func (fs *Fs) blobCommitBlockList(blob string, base64BlockIDs *[]string) (*azblob.BlockBlobCommitBlockListResponse, error) {
	blobURL := fs.getBlobURL(blob)
	return blobURL.CommitBlockList(*fs.ctx, *base64BlockIDs, azblob.BlobHTTPHeaders{}, nil, azblob.BlobAccessConditions{})
}

func (fs *Fs) getContainerFileInfo() (*FileInfo, error) {
	var result FileInfo
	containerURL := fs.serviceURL.NewContainerURL(fs.container)
	contProps, err := containerURL.GetProperties(*fs.ctx, azblob.LeaseAccessConditions{})
	if err != nil {
		LogError(err)
		return &result, err
	}

	result.directory = true
	result.name = fs.container
	result.modTime = contProps.LastModified()

	return &result, nil
}
func (fs *Fs) getBlobFileInfo(blob string) (*FileInfo, error) {
	var result FileInfo

	if strings.ContainsAny(blob, "*?") {
		// result.directory = false
		// does this trigger read dir all?
		result.directory = true
		// result.name = "/" + container + "/" + blob
		result.name = blob
		result.sizeInBytes = -1
		result.modTime = time.Now()

		return &result, nil
	}

	blobURL := fs.getBlobURL(blob)
	blobProps, err := blobURL.GetProperties(*fs.ctx, azblob.BlobAccessConditions{})
	if err != nil {
		LogError(err)
		return &result, err
	}

	result.directory = false
	// result.name = "/" + container + "/" + blob
	result.name = blob
	result.sizeInBytes = blobProps.ContentLength()
	result.modTime = blobProps.LastModified()

	return &result, nil
}

func (fs *Fs) deleteBlob(blob string) error {
	blobURL := fs.getBlobURL(blob)
	_, err := blobURL.Delete(*fs.ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
	if err != nil {
		LogError(err)
	}

	return err
}

func (fs *Fs) copyBlob(srcBlob, dstBlob string) error {
	srcBlobURL := fs.getBlobURL(srcBlob)
	dstBlobURL := fs.getBlobURL(dstBlob)
	startCopy, err := dstBlobURL.StartCopyFromURL(*fs.ctx, srcBlobURL.URL(), nil, azblob.ModifiedAccessConditions{}, azblob.BlobAccessConditions{})
	if err != nil {
		LogError(err)
		return err
	}

	copyStatus := startCopy.CopyStatus()
	for copyStatus == azblob.CopyStatusPending {
		time.Sleep(time.Second * 2)
		getMetadata, err := dstBlobURL.GetProperties(*fs.ctx, azblob.BlobAccessConditions{})
		if err != nil {
			LogError(err)
			return err
		}
		copyStatus = getMetadata.CopyStatus()
	}

	return nil
}

func (fs *Fs) renameBlob(oldName, newName string) error {
	err := fs.copyBlob(oldName, newName)
	if err != nil {
		LogError(err)
		return err
	}

	err = fs.deleteBlob(oldName)
	if err != nil {
		LogError(err)
	}

	return err
}
