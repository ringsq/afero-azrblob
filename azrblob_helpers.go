package azrblob

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	log "github.com/inconshreveable/log15"
)

const helperFileName = "azrblob_helpers"

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
	fcall := fmt.Sprintf(helperFileName+".(%s) listContainers()", fs.container)
	log.Debug(fcall)
	var containers []string
	for marker := (azblob.Marker{}); marker.NotDone(); {
		listCont, err := fs.serviceURL.ListContainersSegment(*fs.ctx, marker, azblob.ListContainersSegmentOptions{})
		if err != nil {
			log.Error(fcall + " (" + err.Error() + ")")
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
	fcall := fmt.Sprintf(helperFileName+".(%s) createContainer(%s)", fs.container, name)
	log.Debug(fcall)
	if strings.ToLower(name) == "cdrs" {
		return fmt.Errorf("cannot create [%s] container", name)
	}
	containerURL := fs.serviceURL.NewContainerURL(strings.ToLower(name))
	_, err := containerURL.Create(*fs.ctx, azblob.Metadata{}, azblob.PublicAccessNone)
	if err != nil {
		log.Error(fcall + " (" + err.Error() + ")")
	}

	return err
}
func (fs *Fs) deleteContainer(name string) error {
	fcall := fmt.Sprintf(helperFileName+".(%s) deleteContainer(%s)", fs.container, name)
	log.Debug(fcall)
	if strings.ToLower(name) == "cdrs" {
		return fmt.Errorf("cannot delete [%s] container", name)
	}
	containerURL := fs.serviceURL.NewContainerURL(strings.ToLower(name))
	_, err := containerURL.Delete(*fs.ctx, azblob.ContainerAccessConditions{})
	return err
}
func (fs *Fs) getBlobsInContainer() (blobs []string, err error) {
	fcall := fmt.Sprintf(helperFileName+".(%s) getBlobsInContainer()", fs.container)
	log.Debug(fcall)
	containerURL := fs.serviceURL.NewContainerURL(fs.container)
	for marker := (azblob.Marker{}); marker.NotDone(); { // The parens around Marker{} are required to avoid compiler error.
		// Get a result segment starting with the blob indicated by the current Marker.
		listBlob, err := containerURL.ListBlobsFlatSegment(*fs.ctx, marker, azblob.ListBlobsSegmentOptions{})
		if err != nil {
			log.Error(fcall + " (" + err.Error() + ")")
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

func (fs *Fs) getBlobURL(blob string) azblob.BlockBlobURL {
	fcall := fmt.Sprintf(helperFileName+".(%s) getBlobURL(%s)", fs.container, blob)
	log.Debug(fcall)
	containerURL := fs.serviceURL.NewContainerURL(fs.container)
	return containerURL.NewBlockBlobURL(blob)
}

func (fs *Fs) blobRead(blob string, offset, count int64) (*[]byte, error) {
	fcall := fmt.Sprintf(helperFileName+".(%s) blockBlobRead(%s,%d,%d)", fs.container, blob, offset, count)
	log.Debug(fcall)
	blobURL := fs.getBlobURL(blob)
	resp, err := blobURL.Download(*fs.ctx, offset, count, azblob.BlobAccessConditions{}, false)
	if err != nil {
		log.Error(fcall + " (" + err.Error() + ")")
		return nil, err
	}

	result, err := ioutil.ReadAll(resp.Body(azblob.RetryReaderOptions{}))
	if err != nil {
		log.Error(fcall + " (" + err.Error() + ")")
		return nil, err
	}

	if len(result) == 0 {
		log.Error(fcall + " (" + io.EOF.Error() + ")")
		return nil, io.EOF
	}

	return &result, nil
}

func (fs *Fs) blobStageBlock(blob, base64BlockID string, p *[]byte) (*azblob.BlockBlobStageBlockResponse, error) {
	fcall := fmt.Sprintf(helperFileName+".(%s) blobStageBlock(%s, %s, p *[%d]byte)", fs.container, blob, base64BlockID, len(*p))
	log.Debug(fcall)
	blobURL := fs.getBlobURL(blob)
	return blobURL.StageBlock(*fs.ctx, base64BlockID, bytes.NewReader(*p), azblob.LeaseAccessConditions{}, nil)
}

func (fs *Fs) blobCommitBlockList(blob string, base64BlockIDs *[]string) (*azblob.BlockBlobCommitBlockListResponse, error) {
	fcall := fmt.Sprintf(helperFileName+".(%s) blobCommitBlockList(%s, base64BlockIDs *[%d]string)", fs.container, blob, len(*base64BlockIDs))
	log.Debug(fcall)
	blobURL := fs.getBlobURL(blob)
	return blobURL.CommitBlockList(*fs.ctx, *base64BlockIDs, azblob.BlobHTTPHeaders{}, nil, azblob.BlobAccessConditions{})
}

func (fs *Fs) getContainerFileInfo() (*FileInfo, error) {
	fcall := fmt.Sprintf(helperFileName+".(%s) getContainerFileInfo()", fs.container)
	log.Debug(fcall)
	var result FileInfo
	containerURL := fs.serviceURL.NewContainerURL(fs.container)
	contProps, err := containerURL.GetProperties(*fs.ctx, azblob.LeaseAccessConditions{})
	if err != nil {
		log.Error(fcall + " (" + err.Error() + ")")
		return &result, err
	}

	result.directory = true
	result.name = fs.container
	result.modTime = contProps.LastModified()

	return &result, nil
}

func (fs *Fs) getBlobFileInfo(blob string) (*FileInfo, error) {
	fcall := fmt.Sprintf(helperFileName+".(%s) getBlobFileInfo(%s)", fs.container, blob)
	log.Debug(fcall)
	var result FileInfo
	blobURL := fs.getBlobURL(blob)
	blobProps, err := blobURL.GetProperties(*fs.ctx, azblob.BlobAccessConditions{})
	if err != nil {
		log.Error(fcall + " (" + err.Error() + ")")
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
	fcall := fmt.Sprintf(helperFileName+".(%s) deleteBlob(%s)", fs.container, blob)
	log.Debug(fcall)
	blobURL := fs.getBlobURL(blob)
	_, err := blobURL.Delete(*fs.ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
	if err != nil {
		log.Error(fcall + " (" + err.Error() + ")")
	}

	return err
}

func (fs *Fs) copyBlob(srcBlob, dstBlob string) error {
	fcall := fmt.Sprintf(helperFileName+".(%s) copyBlob(%s, %s)", fs.container, srcBlob, dstBlob)
	log.Debug(fcall)
	srcBlobURL := fs.getBlobURL(srcBlob)
	dstBlobURL := fs.getBlobURL(dstBlob)
	startCopy, err := dstBlobURL.StartCopyFromURL(*fs.ctx, srcBlobURL.URL(), nil, azblob.ModifiedAccessConditions{}, azblob.BlobAccessConditions{})
	if err != nil {
		log.Error(fcall + " (" + err.Error() + ")")
		return err
	}

	copyStatus := startCopy.CopyStatus()
	for copyStatus == azblob.CopyStatusPending {
		time.Sleep(time.Second * 2)
		getMetadata, err := dstBlobURL.GetProperties(*fs.ctx, azblob.BlobAccessConditions{})
		if err != nil {
			log.Error(fcall + " (" + err.Error() + ")")
			return err
		}
		copyStatus = getMetadata.CopyStatus()
	}

	return nil
}

func (fs *Fs) renameBlob(oldName, newName string) error {
	fcall := fmt.Sprintf(helperFileName+".(%s) renameBlob(%s, %s)", fs.container, oldName, newName)
	log.Debug(fcall)
	err := fs.copyBlob(oldName, newName)
	if err != nil {
		log.Error(fcall + " (" + err.Error() + ")")
		return err
	}

	err = fs.deleteBlob(oldName)
	if err != nil {
		log.Error(fcall + " (" + err.Error() + ")")
	}

	return err
}
