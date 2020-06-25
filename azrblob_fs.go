package azrblob

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	log "github.com/inconshreveable/log15"
	"github.com/spf13/afero"
)

const fsFileName = "azrblob_fs"

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
	ctx        *context.Context
	serviceURL *azblob.ServiceURL
}

// NewFs creates a new Fs object writing files to a given Azure container.
func NewFs(ctx *context.Context, serviceURL *azblob.ServiceURL, container string) *Fs {
	fcall := fmt.Sprintf(fsFileName+".NewFs(ctx *context.Context, serviceURL *azblob.ServiceURL,%s)", container)
	log.Debug(fcall)
	return &Fs{
		container:  container,
		ctx:        ctx,
		serviceURL: serviceURL,
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
	fcall := fmt.Sprintf(fsFileName+".(%s) Create(%s)", fs.container, name)
	log.Debug(fcall)
	file, err := fs.OpenFile(name, os.O_WRONLY, 0750)
	if err != nil {
		log.Error(fcall + " (" + err.Error() + ")")
		return file, err
	}

	return file, nil
}

// Mkdir makes a container in Azure Blob Storage.
func (fs *Fs) Mkdir(name string, perm os.FileMode) error {
	fcall := fmt.Sprintf(fsFileName+".(%s) Mkdir(%s, %d)", fs.container, name, perm)
	log.Debug(fcall)
	// file, err := fs.OpenFile(fmt.Sprintf("%s/", filepath.Clean(name)), os.O_CREATE, perm)
	file, err := fs.OpenFile(fmt.Sprintf("%s/", trimLeadingSlash(name)), os.O_CREATE, perm)
	if err == nil {
		err = file.Close()
	} else {
		log.Error(fcall + " (" + err.Error() + ")")
	}

	return err
}

// MkdirAll creates a directory and all parent directories if necessary.
func (fs *Fs) MkdirAll(path string, perm os.FileMode) error {
	fcall := fmt.Sprintf(fsFileName+".(%s) MkdirAll(%s, %d)", fs.container, path, perm)
	log.Debug(fcall)
	return fs.Mkdir(path, perm)
}

// Open a file for reading.
func (fs *Fs) Open(name string) (afero.File, error) {
	fcall := fmt.Sprintf(fsFileName+".(%s) Open(%s)", fs.container, name)
	log.Debug(fcall)
	/*
		if _, err := fs.Stat(name); err != nil {
			return nil, err
		}
	*/
	file, err := fs.OpenFile(name, os.O_RDONLY, 0777)
	if err != nil {
		log.Error(fcall + " (" + err.Error() + ")")
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
	fcall := fmt.Sprintf(fsFileName+".(%s) OpenFile(%s,%d,%d)", fs.container, name, flag, perm)
	log.Debug(fcall)
	file := NewFile(fs, name)

	// Reading and writing doesn't make sense for Azure Block Blobs
	if flag&os.O_RDWR != 0 {
		log.Error(fcall + " (O_RDWR " + ErrNotSupported.Error() + ")")
		return nil, ErrNotSupported
	}

	// Appending is not supported by Azure Block Blobs
	if flag&os.O_APPEND != 0 {
		log.Error(fcall + " (O_APPEND " + ErrNotSupported.Error() + ")")
		return nil, ErrNotSupported
	}

	// Creating is basically a write
	if flag&os.O_CREATE != 0 {
		log.Debug(fcall + " (O_CREATE)")
		flag |= os.O_WRONLY
	}

	// Write a file
	if flag&os.O_WRONLY != 0 {
		log.Debug(fcall + " (O_WRONLY)")
		file.streamWrite = true
		return file, nil
	}

	info, err := file.Stat()

	if err != nil {
		log.Error(fcall + " (" + err.Error() + ")")
		return nil, err
	}

	if info.IsDir() {
		log.Debug(fcall + " (info.IsDir())")
		return file, nil
	}

	file.streamRead = true
	return file, nil
}

// Remove a file
func (fs *Fs) Remove(name string) error {
	fcall := fmt.Sprintf(fsFileName+".(%s) Remove(%s)", fs.container, name)
	log.Debug(fmt.Sprintf("_fs.Remove(%s)", name))
	_, err := fs.Stat(name)
	if err != nil {
		log.Error(fcall + " (" + err.Error() + ")")
		return err
	}

	return fs.deleteBlob(name)
}

// RemoveAll removes all blobs in the container
func (fs *Fs) RemoveAll(path string) error {
	fcall := fmt.Sprintf(fsFileName+".(%s) RemoveAll(%s)", fs.container, path)
	log.Debug(fcall)
	blobs, err := fs.getBlobsInContainer()
	if err != nil {
		log.Error(fcall + " (" + err.Error() + ")")
		return err
	}

	pathPrefix := trimLeadingSlash(path)
	for _, blob := range blobs {
		if pathPrefix == "/" || strings.HasPrefix(blob, pathPrefix) {
			err = fs.deleteBlob(blob)
			if err != nil {
				log.Error(fcall + " (" + err.Error() + ")")
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
	fcall := fmt.Sprintf(fsFileName+".(%s) Rename(%s, %s)", fs.container, oldname, newname)
	log.Debug(fcall)
	if oldname == newname {
		log.Error(fcall + " (oldname == newname)")
		return nil
	}

	err := fs.renameBlob(trimLeadingSlash(oldname), trimLeadingSlash(newname))
	if err != nil {
		log.Error(fcall + " (" + err.Error() + ")")
	}

	return err
}

func hasTrailingSlash(s string) bool {
	fcall := fmt.Sprintf(fsFileName+".hasTrailingSlash(%s)", s)
	log.Debug(fcall)
	return len(s) > 0 && s[len(s)-1] == '/'
}

func trimLeadingSlash(s string) string {
	fcall := fmt.Sprintf(fsFileName+".trimLeadingSlash(%s)", s)
	log.Debug(fcall)
	if len(s) > 1 && s[0] == '/' {
		return s[1:]
	}
	return s
}

// Stat returns a FileInfo describing the named file.
func (fs Fs) Stat(name string) (os.FileInfo, error) {
	fcall := fmt.Sprintf(fsFileName+".(%s) Stat(%s)", fs.container, name)
	log.Debug(fcall)
	nameClean := trimLeadingSlash(name)
	// nameClean = filepath.Clean(name)
	if nameClean == "/" {
		fi, err := fs.getContainerFileInfo()
		if err != nil {
			log.Error(fcall + " (" + err.Error() + ")")
			return nil, err
		}
		return fi, nil
	}

	fi, err := fs.getBlobFileInfo(nameClean)
	if err != nil {
		// if strings.Contains(err.Error(), "Status: 404 The specified blob does not exist") {
		// 	log.Debug("Is this a directory?")
		// }
		log.Error(fcall + " (" + err.Error() + ")")
		return nil, err
	}

	return fi, nil
}

// Chmod doesn't exists in Azure Blob Storage
func (fs Fs) Chmod(name string, mode os.FileMode) error {
	fcall := fmt.Sprintf(fsFileName+".(%s) Chmod(%s, %d)", fs.container, name, mode)
	log.Debug(fcall)
	return ErrNotSupported
}

// Chtimes doesn't exists in Azure Blob Storage
func (fs Fs) Chtimes(name string, old time.Time, new time.Time) error {
	fcall := fmt.Sprintf(fsFileName+".(%s) Chtimes(%s,%v,%v)", fs.container, name, old, new)
	log.Debug(fcall)
	return ErrNotSupported
}
