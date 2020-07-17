package azrblob

import (
	"encoding/base64"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/google/uuid"

	"github.com/spf13/afero"
)

// File Interfaces and Methods Available:
// io.Closer
// io.Reader
// io.ReaderAt
// io.Seeker
// io.Writer
// io.WriterAt
// Name() : string
// Readdir(count int) : []os.FileInfo, error
// Readdirnames(n int) : []string, error
// Stat() : os.FileInfo, error
// Sync() : error
// Truncate(size int64) : error
// WriteString(s string) : ret int, err error

// using UUIDs for BlockIDs
func newBase64BlockID() string {
	blockUUID := uuid.New()
	blockID := blockUUID.String()
	base64BlockID := base64.StdEncoding.EncodeToString([]byte(blockID))
	return base64BlockID
}

// File represents a file in Azure Blob storage.
type File struct {
	fs         *Fs         // Parent file system
	name       string      // Name of the file
	cachedInfo os.FileInfo // File info cached for later used

	// State of the stream if we are reading the file
	streamRead       bool
	streamReadOffset int64

	// State of the stream if we are writing the file
	streamWrite    bool
	base64BlockIDs []string

	azureMarker azblob.Marker
	cacheMarker string
}

// NewFile initializes an File object.
func NewFile(fs *Fs, name string) *File {
	name = trimLeadingSlash(name)
	return &File{
		fs:   fs,
		name: name,
	}
}

// Name returns the filename, i.e. Azure blob path without the container name.
func (f *File) Name() string {
	return f.name
}

func (f *File) path() string {
	path := filepath.Dir(f.name)
	// check for no path or Windows root path
	if path == "" || path == "\\" {
		path = "/"
	}

	return path
}

func getFilterRegExp(filter string) (rexp *regexp.Regexp, err error) {
	if filter != "" {
		pattern := strings.ReplaceAll(filter, ".", "\\.")
		pattern = strings.ReplaceAll(pattern, "?", ".")
		pattern = strings.ReplaceAll(pattern, "*", ".*")
		pattern = "^" + pattern + "$"

		rexp, err = regexp.Compile(pattern)
		if err != nil {
			return nil, err
		}
	}
	return rexp, nil
}

func (f *File) setPrefixFilter() (prefix, filter string) {
	if strings.ContainsAny(f.name, "?*") {
		filter = f.name
	} else {
		path := f.path()
		prefix := trimLeadingSlash(path)
		if prefix == "/" {
			prefix = ""
		}
	}
	return
}
func (f *File) readDirCache(n int) (fileInfos []os.FileInfo, err error) {
	if !f.fs.cached {
		return
	}

	prefix, filter := f.setPrefixFilter()

	cache, err := GetContainerCache(f.fs.container)
	if err != nil {
		LogError(err)
		return nil, err
	}

	fileInfos, err = cache.ReadCache(prefix, filter, "", n)
	if err != nil {
		LogError(err)
		return nil, err
	}

	if n > 0 {
		if len(fileInfos) == n {
			f.cacheMarker = fileInfos[len(fileInfos)-1].Name()
		} else {
			f.cacheMarker = ""
		}
	}

	return
}
func (f *File) readDirNoCache(n int) (fileInfos []os.FileInfo, err error) {
	prefix, filter := f.setPrefixFilter()

	fileInfos, err = f.getBlobsInContainerFileInfoMarker(int32(n), prefix, filter)
	if err != nil {
		LogError(err)
		return nil, err
	}
	return
}

// Readdir reads the contents of the directory associated with file and
// returns a slice of up to n FileInfo values in directory order.
// Subsequent calls on the same file will yield further FileInfos.
//
// If n > 0, Readdir returns at most n FileInfo structures. In this case, if
// Readdir returns an empty slice, it will return a non-nil error
// explaining why. At the end of a directory, the error is io.EOF.
//
// If n <= 0, Readdir returns all the FileInfo from the directory in
// a single slice. In this case, if Readdir succeeds (reads all
// the way to the end of the directory), it returns the slice and a
// nil error. If it encounters an error before the end of the
// directory, Readdir returns the FileInfo read until that point
// and a non-nil error.
func (f *File) Readdir(n int) (fileInfos []os.FileInfo, err error) {
	if n <= 0 {
		return f.ReaddirAll()
	}

	if f.fs.cached {
		fileInfos, err = f.readDirCache(n)
		if err != nil {
			f.cacheMarker = ""
			return
		}

		if f.cacheMarker != "" {
			return fileInfos, nil
		}
	} else {
		fileInfos, err = f.readDirNoCache(n)
		if err != nil {
			f.azureMarker = azblob.Marker{}
			return
		}

		if f.azureMarker.NotDone() {
			return fileInfos, nil
		}

		f.azureMarker = azblob.Marker{}
	}

	err = io.EOF
	return
}

// ReaddirAll provides list of file cachedInfo.
func (f *File) ReaddirAll() (fileInfos []os.FileInfo, err error) {
	if f.fs.cached {
		fileInfos, err = f.readDirCache(-1)
	} else {
		for {
			infos, err := f.Readdir(5000)
			fileInfos = append(fileInfos, infos...)
			if err != nil {
				if err == io.EOF {
					break
				} else {
					LogError(err)
					return nil, err
				}
			}
		}
	}
	return
}

// Readdirnames reads and returns a slice of names from the directory f.
//
// If n > 0, Readdirnames returns at most n names. In this case, if
// Readdirnames returns an empty slice, it will return a non-nil error
// explaining why. At the end of a directory, the error is io.EOF.
//
// If n <= 0, Readdirnames returns all the names from the directory in
// a single slice. In this case, if Readdirnames succeeds (reads all
// the way to the end of the directory), it returns the slice and a
// nil error. If it encounters an error before the end of the
// directory, Readdirnames returns the names read until that point and
// a non-nil error.
func (f *File) Readdirnames(n int) ([]string, error) {
	fi, err := f.Readdir(n)
	if err != nil {
		LogError(err)
		return nil, err
	}
	names := make([]string, len(fi))
	for i, f := range fi {
		_, names[i] = filepath.Split(f.Name())
	}
	return names, nil
}

// Stat returns the FileInfo structure describing file.
// If there is an error, it will be of type *PathError.
func (f *File) Stat() (os.FileInfo, error) {
	info, err := f.fs.Stat(f.Name())
	if err == nil {
		f.cachedInfo = info
	} else {
		LogError(err)
	}

	return info, err
}

// Sync is a noop.
func (f *File) Sync() error {
	return nil
}

// Truncate changes the size of the file.
// It does not change the I/O offset.
// If there is an error, it will be of type *PathError.
func (f *File) Truncate(int64) error {
	LogError(ErrNotImplemented)
	return ErrNotImplemented
}

// WriteString is like Write, but writes the contents of string s rather than
// a slice of bytes.
func (f *File) WriteString(s string) (int, error) {
	return f.Write([]byte(s))
}

// Close closes the File, rendering it unusable for I/O.
// It returns an error, if any.
func (f *File) Close() error {
	// Closing a reading stream
	if f.streamRead {
		defer func() {
			f.streamRead = false
		}()
	}

	// Closing a writing stream
	if f.streamWrite {
		defer func() {
			f.streamWrite = false
		}()
		if len(f.base64BlockIDs) > 0 {
			_, err := f.fs.blobCommitBlockList(f.name, &f.base64BlockIDs)
			if err != nil {
				LogError(err)
			}
			return err
		}
	}

	return nil
}

// Read reads up to len(b) bytes from the File.
// It returns the number of bytes read and an error, if any.
// EOF is signaled by the read offset equaling the file size with err set to io.EOF.
func (f *File) Read(p []byte) (int, error) {
	bufSize := int64(len(p))
	data, err := f.fs.blobRead(f.name, f.streamReadOffset, bufSize)
	if err != nil {
		LogError(err)
	}

	bytesCopied := copy(p, *data)

	if err == nil {
		f.streamReadOffset += int64(bytesCopied)
	}

	// EOF
	if f.streamReadOffset == f.cachedInfo.Size() && err == nil {
		return bytesCopied, io.EOF
	}

	return bytesCopied, err
}

// ReadAt reads len(p) bytes from the file starting at byte offset off.
// It returns the number of bytes read and the error, if any.
// ReadAt always returns a non-nil error when n < len(b).
// At end of file, that error is io.EOF.
func (f *File) ReadAt(p []byte, off int64) (n int, err error) {
	_, err = f.Seek(off, io.SeekStart)
	if err != nil {
		LogError(err)
		return
	}
	n, err = f.Read(p)
	return
}

// Seek sets the offset for the next Read or Write on file to offset, interpreted
// according to whence: 0 means relative to the origin of the file, 1 means
// relative to the current offset, and 2 means relative to the end.
// It returns the new offset and an error, if any.
// The behavior of Seek on a file opened with O_APPEND is not specified.
func (f *File) Seek(offset int64, whence int) (int64, error) {
	// Write seek is not supported
	if f.streamWrite {
		LogError(ErrNotSupported)
		return 0, ErrNotSupported
	}

	// Read seek
	if f.streamRead {
		startByte := int64(0)

		switch whence {
		case io.SeekStart:
			startByte = offset
		case io.SeekCurrent:
			startByte = f.streamReadOffset + offset
		case io.SeekEnd:
			startByte = f.cachedInfo.Size() - offset
		}

		if startByte < 0 {
			LogError(ErrInvalidSeek)
			return startByte, ErrInvalidSeek
		}

		f.streamReadOffset = startByte
		return startByte, nil
	}

	LogError(afero.ErrFileClosed)
	return 0, afero.ErrFileClosed
}

// Write writes len(b) bytes to the File.
// It returns the number of bytes written and an error, if any.
// Write returns a non-nil error when n != len(b).
func (f *File) Write(p []byte) (int, error) {
	base64BlockID := newBase64BlockID()
	f.base64BlockIDs = append(f.base64BlockIDs, base64BlockID)

	_, err := f.fs.blobStageBlock(f.name, base64BlockID, &p)
	if err != nil {
		LogError(err)
	}
	n := len(p)

	return n, err
}

// WriteAt writes len(p) bytes to the file starting at byte offset off.
// It returns the number of bytes written and an error, if any.
// WriteAt returns a non-nil error when n != len(p).
func (f *File) WriteAt(p []byte, off int64) (n int, err error) {
	_, err = f.Seek(off, 0)
	if err != nil {
		LogError(err)
		return
	}
	n, err = f.Write(p)
	return
}
