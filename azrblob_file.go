package azrblob

import (
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	log "github.com/inconshreveable/log15"

	"github.com/spf13/afero"
)

const fileFileName = "azrblob_file"

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
}

// NewFile initializes an File object.
func NewFile(fs *Fs, name string) *File {
	fcall := fmt.Sprintf(fileFileName+".NewFile(fs,%s)", name)
	log.Debug(fcall)
	name = trimLeadingSlash(name)
	return &File{
		fs:   fs,
		name: name,
	}
}

// Name returns the filename, i.e. Azure blob path without the container name.
func (f *File) Name() string {
	fcall := fmt.Sprintf(fileFileName+".Name() (%s)", f.name)
	log.Debug(fcall)
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

// Readdir reads the contents of the directory associated with file and
// returns a slice of up to n FileInfo values, as would be returned
// by ListObjects, in directory order. Subsequent calls on the same file will yield further FileInfos.
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
func (f *File) Readdir(n int) ([]os.FileInfo, error) {
	fcall := fmt.Sprintf(fileFileName+".(%s) Readdir(%d)", f.name, n)
	log.Debug(fcall)
	if n <= 0 {
		return f.ReaddirAll()
	}
	path := f.path()
	log.Debug(fcall + " (" + path + ")")

	blobs, err := f.fs.getBlobsInContainer()
	if err != nil {
		log.Error(fcall + " (" + err.Error() + ")")
		return nil, err
	}

	var fis []os.FileInfo
	for _, blob := range blobs {
		if path == "/" || strings.HasPrefix(blob, path+"/") {
			fi, err := f.fs.getBlobFileInfo(blob)
			if err != nil {
				log.Error(fcall + " (" + err.Error() + ")")
				return nil, err
			}
			fis = append(fis, fi)
		}
	}

	return fis, io.EOF
}

// ReaddirAll provides list of file cachedInfo.
func (f *File) ReaddirAll() ([]os.FileInfo, error) {
	fcall := fmt.Sprintf(fileFileName+".(%s) ReaddirAll()", f.name)
	log.Debug(fcall)
	var fileInfos []os.FileInfo
	for {
		infos, err := f.Readdir(100)
		fileInfos = append(fileInfos, infos...)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				log.Error(fcall + " (" + err.Error() + ")")
				return nil, err
			}
		}
	}
	return fileInfos, nil
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
	fcall := fmt.Sprintf(fileFileName+".(%s) Readdirnames(%d)", f.name, n)
	log.Debug(fcall)
	fi, err := f.Readdir(n)
	if err != nil {
		log.Error(fcall + " (" + err.Error() + ")")
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
	fcall := fmt.Sprintf(fileFileName+".(%s) Stat()", f.name)
	log.Debug(fcall)
	info, err := f.fs.Stat(f.Name())
	if err == nil {
		f.cachedInfo = info
	} else {
		log.Error(fcall + " (" + err.Error() + ")")
	}

	return info, err
}

// Sync is a noop.
func (f *File) Sync() error {
	log.Debug("_file.Sync()")
	return nil
}

// Truncate changes the size of the file.
// It does not change the I/O offset.
// If there is an error, it will be of type *PathError.
func (f *File) Truncate(int64) error {
	log.Debug("_file.Truncate()")
	return ErrNotImplemented
}

// WriteString is like Write, but writes the contents of string s rather than
// a slice of bytes.
func (f *File) WriteString(s string) (int, error) {
	log.Debug(fmt.Sprintf("_file.WriteString(%s)", s))
	return f.Write([]byte(s))
}

// Close closes the File, rendering it unusable for I/O.
// It returns an error, if any.
func (f *File) Close() error {
	fcall := "_file.Close()"
	log.Debug(fcall)
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
				log.Error(fcall + " (" + err.Error() + ")")
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
	fcall := fmt.Sprintf(fileFileName+".(%s) Read(p [%d]byte)", f.name, len(p))
	log.Debug(fcall)
	bufSize := int64(len(p))
	data, err := f.fs.blobRead(f.name, f.streamReadOffset, bufSize)
	if err != nil {
		log.Error(fcall + " (" + err.Error() + ")")
	}

	bytesCopied := copy(p, *data)

	if err == nil {
		f.streamReadOffset += int64(bytesCopied)
		log.Debug(fmt.Sprintf("streamReadOffset: %d FileSize: %d", f.streamReadOffset, f.cachedInfo.Size()))
	}

	// EOF
	if f.streamReadOffset == f.cachedInfo.Size() && err == nil {
		log.Debug(fcall + " (bytesCopied < bufSize && err == nil)")
		return bytesCopied, io.EOF
	}

	log.Debug(fcall + fmt.Sprintf(" (n: %d)", bytesCopied))
	return bytesCopied, err
}

// ReadAt reads len(p) bytes from the file starting at byte offset off.
// It returns the number of bytes read and the error, if any.
// ReadAt always returns a non-nil error when n < len(b).
// At end of file, that error is io.EOF.
func (f *File) ReadAt(p []byte, off int64) (n int, err error) {
	fcall := fmt.Sprintf(fileFileName+".(%s) ReadAt(p [%d]byte,%d)", f.name, len(p), off)
	log.Debug(fcall)
	_, err = f.Seek(off, io.SeekStart)
	if err != nil {
		log.Error(fcall + " (" + err.Error() + ")")
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
	fcall := fmt.Sprintf(fileFileName+".(%s) Seek(%d, %d)", f.name, offset, whence)
	log.Debug(fcall)

	// Write seek is not supported
	if f.streamWrite {
		log.Error(fmt.Sprintf(fcall+" (%d, %s)", 0, ErrNotSupported.Error()))
		return 0, ErrNotSupported
	}

	// Read seek
	if f.streamRead {
		log.Debug(fmt.Sprintf(fcall+" (%d, %s)", 0, "f.seekRead(offset, whence)"))
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
			log.Error(fcall + " (" + ErrInvalidSeek.Error() + ")")
			return startByte, ErrInvalidSeek
		}

		f.streamReadOffset = startByte
		return startByte, nil
	}

	log.Error(fmt.Sprintf(fcall+" (%d,%s)", 0, afero.ErrFileClosed.Error()))
	return 0, afero.ErrFileClosed
}

// Write writes len(b) bytes to the File.
// It returns the number of bytes written and an error, if any.
// Write returns a non-nil error when n != len(b).
func (f *File) Write(p []byte) (int, error) {
	fcall := fmt.Sprintf(fileFileName+".(%s) Write(p [%d]byte)", f.name, len(p))
	log.Debug(fcall)
	base64BlockID := newBase64BlockID()
	f.base64BlockIDs = append(f.base64BlockIDs, base64BlockID)

	_, err := f.fs.blobStageBlock(f.name, base64BlockID, &p)
	if err != nil {
		log.Error(fcall + " (" + err.Error() + ")")
	}
	n := len(p)

	return n, err
}

// WriteAt writes len(p) bytes to the file starting at byte offset off.
// It returns the number of bytes written and an error, if any.
// WriteAt returns a non-nil error when n != len(p).
func (f *File) WriteAt(p []byte, off int64) (n int, err error) {
	fcall := fmt.Sprintf(fileFileName+".(%s) WriteAt(p [%d]byte, %d)", f.name, len(p), off)
	log.Debug(fcall)
	_, err = f.Seek(off, 0)
	if err != nil {
		log.Error(fcall + " (" + err.Error() + ")")
		return
	}
	n, err = f.Write(p)
	return
}
