package azrblob

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

const (
	cacheDateFormat       = "2006-01-02T15:04:05Z"
	cacheFileSuffixFormat = "20060102150405"
	secCycleCheckSleep    = 60
	maxFileOpRetries      = 10
	secFileOpRetrySleep   = 5
)

// CreateCache - fields needed to initialize a cached container
type CreateCache struct {
	Name        string
	Cycle       float64
	Path        string
	AccountName string
	AccountKey  string
}

// ContainerCache - a struct that represents all the necessary info to manage the caching of a container's blob list
type ContainerCache struct {
	container  string
	cycle      float64
	path       string
	stop       bool
	updating   bool
	lastUpdate time.Time
	ctx        *context.Context
	serviceURL *azblob.ServiceURL
	marker     azblob.Marker
}

// CachedContainers - collection of cached containers
var CachedContainers []ContainerCache
var errNotCacheConfig = errors.New("config not for cached container")

// GetContainerCache - gets the specified container cache specifically for reading
func GetContainerCache(container string) (ContainerCache, error) {
	var cache ContainerCache
	for _, c := range CachedContainers {
		if c.container == container {
			cache = c
		}
	}
	return cache, nil
}

// createContainerCache - takes the provided parameters and initializes the caching of a container blob list
func createContainerCache(container CreateCache) (ContainerCache, error) {
	var cache ContainerCache
	if !(container.Cycle > 0.0) {
		return cache, fmt.Errorf("invalid value for cache cycle %f on container %s", container.Cycle, container.Name)
	}

	if container.Name == "" {
		err := errors.New("container name missing from cached container config")
		return cache, err
	}

	if container.Path == "" {
		container.Path = "/tmp"
	}

	if container.AccountName == "" {
		err := fmt.Errorf("accountName not specified for cached container %s", container.Name)
		return cache, err
	}
	if container.AccountKey == "" {
		err := fmt.Errorf("accountKey not specified for cached container %s", container.Name)
		return cache, err

	}

	cache.cycle = container.Cycle
	cache.container = container.Name
	cache.path = container.Path

	err := cache.initCredentials(container.AccountName, container.AccountKey)
	if err != nil {
		return cache, err
	}

	err = cache.update()
	if err != nil {
		return cache, err
	}

	err = cache.renameNew()
	if err != nil {
		return cache, err
	}

	err = cache.deleteOld()
	if err != nil {
		return cache, err
	}

	CachedContainers = append(CachedContainers, cache)

	return cache, nil
}

// InitCachedContainers - identify and initialize any containers marked for caching
func InitCachedContainers(containers []CreateCache) error {
	for _, container := range containers {
		newCache, err := createContainerCache(container)
		if err != nil {
			newCache.logError(err)
			return err
		}
		go newCache.startCycling()
	}
	return nil
}

func (cc *ContainerCache) logError(err error) {
	fmt.Printf("CACHE-ERRO[%s] [%s] %s\n", time.Now().Format("01-02|15:04:05"), cc.container, err.Error())
	return
}
func (cc *ContainerCache) logInfo(msg string) {
	fmt.Printf("CACHE-INFO[%s] [%s] %s\n", time.Now().Format("01-02|15:04:05"), cc.container, msg)
	return
}
func (cc *ContainerCache) logDebug(msg string) {
	fmt.Printf("CACHE-DBUG[%s] [%s] %s\n", time.Now().Format("01-02|15:04:05"), cc.container, msg)
	return
}
func (cc *ContainerCache) getCacheFilePath() string {
	return cc.path + "/" + "cache-" + cc.container + ".csv"
}
func (cc *ContainerCache) getCacheNewFilePath(ts time.Time) string {
	return cc.path + "/" + "cache-" + cc.container + "-" + ts.Format(cacheFileSuffixFormat) + ".csv"
}
func (cc *ContainerCache) getCacheOldFilePath() string {
	return cc.path + "/" + "cache-" + cc.container + "-old.csv"
}

// initCredentials - initialize the context and service for the provided credentials
func (cc *ContainerCache) initCredentials(accountName, accountKey string) error {
	if accountName == "" || accountKey == "" {
		err := fmt.Errorf("accountName and accountKey are  both requird for azure container %s", cc.container)
		return err
	}

	// get the credentials
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return err
	}

	// build the context for the Azure Blob Storage
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})
	u, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", accountName))
	su := azblob.NewServiceURL(*u, p)
	c := context.Background()
	cc.serviceURL = &su
	cc.ctx = &c

	return nil
}

// startCycling - starts the periodic updating of the container cache based on the cycle
func (cc *ContainerCache) startCycling() {
	for cc.stop == false {
		if !cc.updating {
			if time.Since(cc.lastUpdate).Minutes() >= cc.cycle {
				err := make(chan error)
				go cc.cycleUpdate(err)
				cerr := <-err
				if cerr != nil {
					cc.logError(cerr)
				}
			}
		}
		time.Sleep(time.Second * secCycleCheckSleep)
	}
	return
}

// cycleUpdate - the thread that updates the cache data and manages the cache files
func (cc *ContainerCache) cycleUpdate(err chan error) {
	cerr := cc.update()
	if cerr != nil {
		err <- cerr
		return
	}

	rerr := cc.renameNew()
	if rerr != nil {
		err <- rerr
		return
	}

	derr := cc.deleteOld()
	err <- derr

	return
}

// createRetry - attempts to create the cache file with a retry mechanism up to a maximum number of retries
func (cc *ContainerCache) createRetry(filePath string, maxAttempts int) (*os.File, error) {
	var (
		file     *os.File
		err      error
		attempts int
	)
	attempts = 0
	for file, err = os.Create(filePath); err != nil && attempts < maxAttempts; attempts++ {
		cc.logInfo(fmt.Sprintf("unable to create cache file %s on attempt %d due to %s", filePath, attempts+1, err.Error()))
		time.Sleep(time.Second * secFileOpRetrySleep)
		file, err = os.Create(filePath)
	}
	if err != nil {
		return file, err
	}
	return file, nil
}

// update - gets the latest blob listing from the container and writes [Name,Size,LastModified] for each blob to a CSV file
func (cc *ContainerCache) update() error {
	cc.updating = true
	defer func() { cc.updating = false }()
	cc.logInfo("updating")

	updatedOn := time.Now()
	filePath := cc.getCacheNewFilePath(updatedOn)

	file, err := cc.createRetry(filePath, maxFileOpRetries)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	containerURL := cc.serviceURL.NewContainerURL(cc.container)
	for cc.marker = (azblob.Marker{}); cc.marker.NotDone(); {
		listBlob, err := containerURL.ListBlobsFlatSegment(*cc.ctx, cc.marker, azblob.ListBlobsSegmentOptions{})
		if err != nil {
			return err
		}

		// IMPORTANT: ListBlobs returns the start of the next segment; you MUST use this to get
		// the next segment (after processing the current result segment).
		cc.marker = listBlob.NextMarker

		// Process the blobs returned in this result segment
		for _, blobInfo := range listBlob.Segment.BlobItems {
			record := []string{blobInfo.Name, fmt.Sprintf("%d", *blobInfo.Properties.ContentLength), blobInfo.Properties.LastModified.Format(cacheDateFormat)}
			err = writer.Write(record)
			if err != nil {
				return err
			}
		}
	}
	cc.lastUpdate = updatedOn
	cc.logInfo("updated")
	return nil
}

// renameRetry - attempts to rename the old cache file and new cache file with a retry mechanism up to a maximum number of retries
func (cc *ContainerCache) renameRetry(oldFilePath, newFilePath string, maxAttempts int) error {
	var (
		err      error
		attempts int
	)
	attempts = 0
	for err = os.Rename(oldFilePath, newFilePath); err != nil && attempts < maxAttempts; attempts++ {
		cc.logInfo(fmt.Sprintf("unable to rename cache file %s on attempt %d due to %s", oldFilePath, attempts+1, err.Error()))
		time.Sleep(time.Second * secFileOpRetrySleep)
		err = os.Rename(oldFilePath, newFilePath)
	}
	if err != nil {
		return err
	}
	return nil
}

// renameNew - renames the newly created cache file
func (cc *ContainerCache) renameNew() error {
	var err error

	cacheFilePath := cc.getCacheFilePath()
	cacheNewFilePath := cc.getCacheNewFilePath(cc.lastUpdate)
	cacheOldFilePath := cc.getCacheOldFilePath()

	// check to make sure the new file exists
	if _, err = os.Stat(cacheNewFilePath); err != nil {
		return err
	}

	// rename the current cache file to old
	if _, err = os.Stat(cacheFilePath); err == nil {
		err = cc.renameRetry(cacheFilePath, cacheOldFilePath, maxFileOpRetries)
		if err != nil {
			return err
		}
	}

	// rename the new file to be the current cache file
	err = cc.renameRetry(cacheNewFilePath, cacheFilePath, maxFileOpRetries)
	if err != nil {
		rerr := cc.rollbackOld()
		if rerr != nil {
			cc.logError(err)
			return rerr
		}
		cc.logError(fmt.Errorf("rolled back to previous cache file for container %s due to %s", cc.container, err.Error()))
	}

	return nil
}

// deleteRetry - attempts to delete the old cache file with a retry mechanism up to a maximum number of retries
func (cc *ContainerCache) deleteRetry(filePath string, maxAttempts int) error {
	var (
		err      error
		attempts int
	)
	attempts = 0
	for err = os.Remove(filePath); err != nil && attempts < maxAttempts; attempts++ {
		cc.logInfo(fmt.Sprintf("unable to remove cache file %s on attempt %d due to %s", filePath, attempts+1, err.Error()))
		time.Sleep(time.Second * secFileOpRetrySleep)
		err = os.Remove(filePath)
	}
	if err != nil {
		return err
	}
	return nil
}

// deleteOld - deletes the old cache file
func (cc *ContainerCache) deleteOld() error {
	var err error

	cacheOldFilePath := cc.getCacheOldFilePath()
	if _, err = os.Stat(cacheOldFilePath); err == nil {
		err = cc.deleteRetry(cacheOldFilePath, maxFileOpRetries)
		if err != nil {
			return err
		}
	}
	return nil
}

// rollbackOld - if something goes wrong with renaming the new cache file go back to using the previous file
func (cc *ContainerCache) rollbackOld() error {
	var err error

	cacheOldFilePath := cc.getCacheOldFilePath()
	cacheFilePath := cc.getCacheFilePath()
	if _, err = os.Stat(cacheOldFilePath); err == nil {
		if _, err = os.Stat(cacheFilePath); err != nil {
			err = cc.renameRetry(cacheOldFilePath, cacheFilePath, maxFileOpRetries)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// openFileRetry - attempts to open the cache file for reading with a retry mechanism up to a maximum number of retries
func (cc *ContainerCache) openFileRetry(filePath string, maxAttempts int) (*os.File, error) {
	var (
		file     *os.File
		err      error
		attempts int
	)
	attempts = 0
	for file, err = os.Open(filePath); err != nil && attempts < maxAttempts; attempts++ {
		cc.logInfo(fmt.Sprintf("unable to open cache file %s on attempt %d due to %s", filePath, attempts+1, err.Error()))
		time.Sleep(time.Second * secFileOpRetrySleep)
		file, err = os.Open(filePath)
	}
	if err != nil {
		return file, err
	}
	return file, nil
}

// ReadCache - reads in the cached container CSV file and returns an array of FileInfo
func (cc *ContainerCache) ReadCache(prefix, lastListing string, n int) ([]os.FileInfo, error) {
	var result []os.FileInfo

	cacheFilePath := cc.getCacheFilePath()

	// check to make sure the cache file exists
	if _, err := os.Stat(cacheFilePath); err != nil {
		cc.logError(err)
		return result, err
	}

	file, err := cc.openFileRetry(cacheFilePath, maxFileOpRetries)
	if err != nil {
		cc.logError(err)
		return result, err
	}
	defer file.Close()
	count := 0
	reader := csv.NewReader(file)
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			cc.logError(err)
			return result, err
		}
		if n > 0 && count > n {
			break
		}
		name := record[0]
		if prefix != "" && strings.HasPrefix(name, prefix) == false {
			continue
		}
		if lastListing != "" && name <= lastListing {
			continue
		}
		size, err := strconv.ParseInt(record[1], 10, 64)
		if err != nil {
			cc.logError(err)
			return result, err
		}
		modified, err := time.Parse(cacheDateFormat, record[2])
		if err != nil {
			cc.logError(err)
			return result, err
		}
		fi := NewFileInfo(name, false, size, modified)

		result = append(result, fi)
		count++
	}
	return result, nil
}
