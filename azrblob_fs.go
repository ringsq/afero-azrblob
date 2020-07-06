package azrblob

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	log "github.com/inconshreveable/log15"
	"github.com/spf13/afero"
)

// File System Methods Available:
// Chmod(name string, mode os.FileMode) : error
// Chtimes(name string, atime time.Time, mtime time.Time) : error
// Create(name string) : File, error
// Mkdir(name string, perm os.FileMode) : error
// MkdirAll(path string, perm os.FileMode) : error
// Name() : string
// Open(name string) : File, error
// OpenFile(name string, flag int, perm os.FileMode) : File, error
// Remove(name string) : error
// RemoveAll(path string) : error
// Rename(oldname, newname string) : error
// Stat(name string) : os.FileInfo, error

// Fs is an FS object backed by Azure.
type Fs struct {
	container  string
	cached     bool
	ctx        *context.Context
	serviceURL *azblob.ServiceURL
	marker     azblob.Marker
}

// LogError logs any errors encountered
func LogError(err error) {
	msg := ""
	msgFmt := "[ERROR] in %s within %s at line %d due to [%s]"
	pc, file, line, ok := runtime.Caller(1)
	if ok {
		fnc := runtime.FuncForPC(pc)
		name := "undetermined"
		if fnc != nil {
			name = fnc.Name()
		}
		msg = fmt.Sprintf(msgFmt, file, name, line, err.Error())
	}
	log.Error(msg)
	return
}

// LogDebug logs any debug messages
func LogDebug(entry string) {
	msg := ""
	msgFmt := "[DEBUG] from %s within %s at line %d [%s]"
	pc, file, line, ok := runtime.Caller(1)
	if ok {
		fnc := runtime.FuncForPC(pc)
		name := "undetermined"
		if fnc != nil {
			name = fnc.Name()
		}
		msg = fmt.Sprintf(msgFmt, file, name, line, entry)
	}
	log.Debug(msg)
	return
}

// NewFs creates a new Fs object writing files to a given Azure container.
func NewFs(ctx *context.Context, serviceURL *azblob.ServiceURL, container string, cached bool) *Fs {
	return &Fs{
		container:  container,
		ctx:        ctx,
		serviceURL: serviceURL,
		cached:     cached,
	}
}

// ErrNotImplemented is returned when this operation is not (yet) implemented
var ErrNotImplemented = errors.New("not implemented")

// ErrNotSupported is returned when this operations is not supported by Azure
var ErrNotSupported = errors.New("azure blob doesn't support this operation")

// ErrAlreadyOpened is returned when the file is already opened
var ErrAlreadyOpened = errors.New("already opened")

// ErrInvalidSeek is returned when the seek operation is not doable
var ErrInvalidSeek = errors.New("invalid seek offset")

// Name returns the type of FS object this is: Fs.
func (Fs) Name() string { return "azrblob" }

// Create a file
func (fs Fs) Create(name string) (afero.File, error) {
	file, err := fs.OpenFile(name, os.O_WRONLY, 0750)
	if err != nil {
		LogError(err)
		return file, err
	}

	return file, nil
}

// Mkdir makes a container in Azure Blob Storage.
func (fs *Fs) Mkdir(name string, perm os.FileMode) error {
	// file, err := fs.OpenFile(fmt.Sprintf("%s/", filepath.Clean(name)), os.O_CREATE, perm)
	file, err := fs.OpenFile(fmt.Sprintf("%s/", trimLeadingSlash(name)), os.O_CREATE, perm)
	if err == nil {
		err = file.Close()
	} else {
		LogError(err)
	}

	return err
}

// MkdirAll creates a directory and all parent directories if necessary.
func (fs *Fs) MkdirAll(path string, perm os.FileMode) error {
	return fs.Mkdir(path, perm)
}

// Open a file for reading.
func (fs *Fs) Open(name string) (afero.File, error) {
	/*
		if _, err := fs.Stat(name); err != nil {
			return nil, err
		}
	*/
	file, err := fs.OpenFile(name, os.O_RDONLY, 0777)
	if err != nil {
		LogError(err)
	}

	return file, err
}

// OpenFile opens a file.
func (fs *Fs) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	// // Exactly one of O_RDONLY, O_WRONLY, or O_RDWR must be specified.
	// O_RDONLY int = syscall.O_RDONLY // open the file read-only.
	// O_WRONLY int = syscall.O_WRONLY // open the file write-only.
	// O_RDWR   int = syscall.O_RDWR   // open the file read-write.
	// // The remaining values may be or'ed in to control behavior.
	// O_APPEND int = syscall.O_APPEND // append data to the file when writing.
	// O_CREATE int = syscall.O_CREAT  // create a new file if none exists.
	// O_EXCL   int = syscall.O_EXCL   // used with O_CREATE, file must not exist.
	// O_SYNC   int = syscall.O_SYNC   // open for synchronous I/O.
	// O_TRUNC  int = syscall.O_TRUNC  // truncate regular writable file when opened.
	file := NewFile(fs, name)

	// Reading and writing doesn't make sense for Azure Block Blobs
	if flag&os.O_RDWR != 0 {
		LogError(ErrNotSupported)
		return nil, ErrNotSupported
	}

	// Appending is not supported by Azure Block Blobs
	if flag&os.O_APPEND != 0 {
		LogError(ErrNotSupported)
		return nil, ErrNotSupported
	}

	// Creating is basically a write
	if flag&os.O_CREATE != 0 {
		flag |= os.O_WRONLY
	}

	// Write a file
	if flag&os.O_WRONLY != 0 {
		file.streamWrite = true
		return file, nil
	}

	info, err := file.Stat()

	if err != nil {
		LogError(err)
		return nil, err
	}

	if info.IsDir() {
		return file, nil
	}

	file.streamRead = true
	return file, nil
}

// Remove a file
func (fs *Fs) Remove(name string) error {
	_, err := fs.Stat(name)
	if err != nil {
		LogError(err)
		return err
	}

	return fs.deleteBlob(trimLeadingSlash(name))
}

// RemoveAll removes all blobs in the container
func (fs *Fs) RemoveAll(path string) error {
	blobs, err := fs.getBlobsInContainer()
	if err != nil {
		LogError(err)
		return err
	}

	pathPrefix := trimLeadingSlash(path)
	for _, blob := range blobs {
		if pathPrefix == "/" || strings.HasPrefix(blob, pathPrefix) {
			err = fs.deleteBlob(blob)
			if err != nil {
				LogError(err)
				return err
			}
		}
	}

	return nil
}

// Rename a file
// There is no method to directly rename an Azure Blob, so Rename
// will copy the file to a new blob with the new name and then delete
// the original.
func (fs Fs) Rename(oldname, newname string) error {
	if oldname == newname {
		return nil
	}

	err := fs.renameBlob(trimLeadingSlash(oldname), trimLeadingSlash(newname))
	if err != nil {
		LogError(err)
	}

	return err
}

func hasTrailingSlash(s string) bool {
	return len(s) > 0 && s[len(s)-1] == '/'
}

func trimLeadingSlash(s string) string {
	if len(s) > 1 && s[0] == '/' {
		return s[1:]
	}
	return s
}

// Stat returns a FileInfo describing the named file.
func (fs Fs) Stat(name string) (os.FileInfo, error) {
	nameClean := trimLeadingSlash(name)
	// nameClean = filepath.Clean(name)
	if nameClean == "/" {
		fi, err := fs.getContainerFileInfo()
		if err != nil {
			LogError(err)
			return nil, err
		}
		return fi, nil
	}

	fi, err := fs.getBlobFileInfo(nameClean)
	if err != nil {
		// if strings.Contains(err.Error(), "Status: 404 The specified blob does not exist") {
		// 	log.Debug("Is this a directory?")
		// }
		LogError(err)
		return nil, err
	}

	return fi, nil
}

// Chmod doesn't exists in Azure Blob Storage
func (fs Fs) Chmod(name string, mode os.FileMode) error {
	LogError(ErrNotSupported)
	return ErrNotSupported
}

// Chtimes doesn't exists in Azure Blob Storage
func (fs Fs) Chtimes(name string, old time.Time, new time.Time) error {
	LogError(ErrNotSupported)
	return ErrNotSupported
}
