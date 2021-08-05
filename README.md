# Azure Blob Storage Backend for Afero
## About
It provides an [Azure Blob](https://azure.microsoft.com/en-us/services/storage/blobs/#overview) backend for [afero](https://github.com/spf13/afero/).

## Key points
- Uses azblob in [azure-storage-blob-go](https://github.com/Azure/azure-storage-blob-go/) for Azure Blob REST APIs
- Download & upload files
- 80% coverage (all APIs are tested, but not all errors are reproduced)
- Containers that have a large enough number of blobs that cause the listing to timeout can be cached
- Listing with the ? and * wildcards is supported for cached and non-cached containers
- Archived blobs are excluded from listing in cached and non-cached containers

## Known limitations
- File appending is not supported because Azure Blob Storage doesn't support it for Block Blobs.
- Chmod / Chtimes are not supported because Azure Blob Storage doesn't support it.
- Seeking for write is not supported, seeking for read is functional though.
- Creating Directories is not supported.  Azure Blob Storage doesn't support Containers within Containers.

## How to use
Note: More Errors handling needs to be added right now it's just being logged.
```golang

import(
	"github.com/Azure/azure-storage-blob-go/azblob"
 	"github.com/spf13/afero"

	azrblob "github.com/magna5/afero-azrblob"
)

func main() {
  accountName := "accountName"
  accountKey := "accountKey"
  container := "mycontainer"

  // cache the container with a cycle of 7.0 minutes and a path of /tmp for the cache files
  cachedContainers := []azrblob.CreateCache{{Name: container, Cycle: 7.0, Path: "/tmp", AccountName: accountName, AccountKey: accountKey}}
  err := azrblob.InitCachedContainers(cachedContainers)
  if err != nil {
    log(err)
    return
  }

  // get the credentials
  credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
  if err != nil {
    log(err)
    return
  }

  // build the context for the Azure Blob Storage
  p := azblob.NewPipeline(credential, azblob.PipelineOptions{})
  u, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", accountName))
  serviceURL := azblob.NewServiceURL(*u, p)
  ctx := context.Background()

  // Initialize the file system - the last parameter indicates wether or not the container is cached
  azrblobFs := azrblob.NewFs(&ctx, &serviceURL, container, true)

  // And do your thing
  file, _ := azrblobFs.OpenFile(name, os.O_WRONLY, 0777)
  file.WriteString("Hello world!")
  file.Close()
}
```

## Thanks

The code comes from:
- [cflairamb afero-s3](https://github.com/fclairamb/afero-s3)
- Which came from [wreulicke's fork](https://github.com/wreulicke/afero-s3)
- Itself forked from [aviau's fork](https://github.com/aviau/).
- Initially proposed as [an afero PR](https://github.com/spf13/afero/pull/90) by [rgarcia](https://github.com/rgarcia) and updated by [aviau](https://github.com/aviau).
