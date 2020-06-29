// Package azrblob brings azure blob storage handling to afero
package azrblob

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	env "github.com/magna5/godotenv"
	"github.com/spf13/afero"
)

// need to have a .env file of the form:
// AZR_ACCOUNT_NAME=myaccountname
// AZR_ACCOUNT_KEY=myaccountkey
//
func accountInfo() (string, string) {
	err := env.LoadOpt()
	if err != nil {
		return "", ""
	}

	name := os.Getenv("AZR_ACCOUNT_NAME")
	key := os.Getenv("AZR_ACCOUNT_KEY")

	return name, key
}

func emptyTestContainer(fs *Fs) error {
	containers, err := fs.getContainers()
	if err != nil {
		return err
	}

	exists := false
	for _, container := range containers {
		if container == fs.container {
			exists = true
		}
	}

	if exists {
		blobs, err := fs.getBlobsInContainer()
		if err != nil {
			return err
		}
		for _, blob := range blobs {
			fs.deleteBlob(blob)
		}
	} else {
		err = fs.createContainer(fs.container)
		if err != nil {
			return err
		}
	}

	return nil
}

func TestCompatibleAferoAzrBlob(t *testing.T) {
	var _ afero.Fs = (*Fs)(nil)
	var _ afero.File = (*File)(nil)
}

func TestCompatibleOsFileInfo(t *testing.T) {
	var _ os.FileInfo = (*FileInfo)(nil)
}

func GetFs(t *testing.T) afero.Fs {
	accountName, accountKey := accountInfo()
	container := "afero-test"

	if accountName == "" || accountKey == "" {
		t.Fatal("Error loading .env file")
	}
	// get the credentials
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return nil
	}

	// build the context for the Azure Blob Storage
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})
	u, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", accountName))
	serviceURL := azblob.NewServiceURL(*u, p)
	ctx := context.Background()

	// Initialize the file system
	azrblobFs := NewFs(&ctx, &serviceURL, container)

	// err = createTestContainer(azrblobFs, container)
	err = emptyTestContainer(azrblobFs)
	if err != nil {
		t.Fatal("Could not create empty test container", err)
	}

	return azrblobFs
}
func testWriteFileChunks(t *testing.T, file afero.File, fileSize, bufSize int, fillByte byte) {
	for filePos := 0; filePos < fileSize; filePos += bufSize {
		chunkSize := bufSize
		if chunkSize > fileSize-filePos {
			chunkSize = fileSize - filePos
		}
		var p []byte
		p = make([]byte, chunkSize, chunkSize)
		for i := 0; i < chunkSize; i++ {
			p[i] = fillByte
		}
		_, err := file.Write(p)
		if err != nil {
			t.Fatal("Could not write chunk to file:", err)
		}
	}
	return
}
func testReadFileChunks(t *testing.T, file afero.File, startAt, fileSize, bufSize int, fillByte byte) {
	for filePos := startAt; filePos < fileSize; filePos += bufSize {
		chunkSize := bufSize
		if chunkSize > fileSize-filePos {
			chunkSize = fileSize - filePos
		}
		var p []byte
		p = make([]byte, chunkSize, chunkSize)
		_, err := file.Read(p)
		if err != nil && err != io.EOF {
			t.Fatal("Could not read file:", err)
		}
		for i := 0; i < chunkSize; i++ {
			if p[i] != fillByte {
				t.Fatal("read file not equal to written file")
			}
		}
	}
	return
}
func testWriteFile(t *testing.T, fs afero.Fs, name string, size int) {
	t.Logf("Working on %s with %d bytes", name, size)
	fillByte := byte(32)
	bufSize := 32 * 1024

	{ // First we write the file
		t.Log("  Writing file")

		file, errOpen := fs.OpenFile(name, os.O_WRONLY, 0777)
		if errOpen != nil {
			t.Fatal("Could not open file:", errOpen)
		}

		testWriteFileChunks(t, file, size, bufSize, fillByte)

		if errClose := file.Close(); errClose != nil {
			t.Fatal("Couldn't close file", errClose)
		}
	}

	{ // Then we read the file
		t.Log("  Reading file")

		file, errOpen := fs.OpenFile(name, os.O_RDONLY, 0777)
		if errOpen != nil {
			t.Fatal("Could not open file:", errOpen)
		}

		testReadFileChunks(t, file, 0, size, bufSize, fillByte)

		if errClose := file.Close(); errClose != nil {
			t.Fatal("Couldn't close file", errClose)
		}
	}
}

func TestFileWrite(t *testing.T) {
	fs := GetFs(t)
	testWriteFile(t, fs, "/file-1K", 1024)
	testWriteFile(t, fs, "/file-1M", 1*1024*1024)
	// need to increase timeout for these
	testWriteFile(t, fs, "/file-10M", 10*1024*1024)
	// testWriteFile(t, fs, "/file-100M", 100*1024*1024)
}

func TestFsName(t *testing.T) {
	fs := GetFs(t)
	if fs.Name() != "azrblob" {
		t.Fatal("Wrong name")
	}
}

func TestFileSeekBig(t *testing.T) {
	fs := GetFs(t)
	size := 10 * 1024 * 1024 // 10MB
	name := "file-10M"
	fillByte := byte(32)
	bufSize := 32 * 1024

	{ // First we write the file
		t.Log("Writing initial file")

		file, errOpen := fs.OpenFile(name, os.O_WRONLY, 0777)
		if errOpen != nil {
			t.Fatal("Could not open file:", errOpen)
		}
		testWriteFileChunks(t, file, size, bufSize, fillByte)

		if errClose := file.Close(); errClose != nil {
			t.Fatal("Couldn't close file", errClose)
		}
	}

	{
		t.Log("Checking the second half of it")
		file, errOpen := fs.OpenFile(name, os.O_RDONLY, 0777)

		if errOpen != nil {
			t.Fatal("Cannot open", errOpen)
		}

		n, err := file.Seek(5*1024*1024, io.SeekCurrent)
		if err != nil {
			t.Fatal("Cannot seek:", err)
		}

		testReadFileChunks(t, file, int(n), size, bufSize, fillByte)

		if err := file.Close(); err != nil {
			t.Fatal("Cannot close", err)
		}
	}
}

//nolint: gocyclo, funlen
func TestFileSeekBasic(t *testing.T) {
	fs := GetFs(t)

	{ // Writing an initial file
		file, errOpen := fs.OpenFile("file1", os.O_WRONLY, 0777)
		if errOpen != nil {
			t.Fatal("Could not open file:", errOpen)
		}

		if _, err := file.WriteString("Hello world !"); err != nil {
			t.Fatal("Could not write file:", err)
		}

		if errClose := file.Close(); errClose != nil {
			t.Fatal("Couldn't close file", errClose)
		}
	}

	file, errOpen := fs.Open("file1")
	if errOpen != nil {
		t.Fatal("Could not open file:", errOpen)
	}

	defer func() {
		if err := file.Close(); err != nil {
			t.Fatal("Could not close file:", err)
		}
	}()

	buffer := make([]byte, 5)

	{ // Reading the world
		if pos, err := file.Seek(6, io.SeekStart); err != nil || pos != 6 {
			t.Fatal("Could not seek:", err)
		}

		if _, err := file.Read(buffer); err != nil {
			t.Fatal("Could not read buffer:", err)
		}

		if string(buffer) != "world" {
			t.Fatal("Bad fetch:", string(buffer))
		}
	}

	{ // Going 3 bytes backwards
		if pos, err := file.Seek(-3, io.SeekCurrent); err != nil || pos != 8 {
			t.Fatal("Could not seek:", err)
		}

		//smallbuf := buffer[0:2]

		if _, err := file.Read(buffer); err != io.EOF {
			t.Fatal("Could not read buffer:", err)
		}

		if string(buffer) != "rld !" {
			t.Fatal("Bad fetch:", string(buffer))
		}
	}

	{ // And then going back to the beginning
		if pos, err := file.Seek(1, io.SeekStart); err != nil || pos != 1 {
			t.Fatal("Could not seek:", err)
		}

		if _, err := file.Read(buffer); err != nil {
			t.Fatal("Could not read buffer:", err)
		}

		if string(buffer) != "ello " {
			t.Fatal("Bad fetch:", string(buffer))
		}
	}

	{ // And from the end
		if pos, err := file.Seek(5, io.SeekEnd); err != nil || pos != 8 {
			t.Fatal("Could not seek:", err)
		}

		if _, err := file.Read(buffer); err != io.EOF {
			t.Fatal("Could not read buffer:", err)
		}

		if string(buffer) != "rld !" {
			t.Fatal("Bad fetch:", string(buffer))
		}
	}
}

func TestReadAt(t *testing.T) {
	fs := GetFs(t)

	{ // Writing an initial file
		file, errOpen := fs.OpenFile("file1", os.O_WRONLY, 0777)
		if errOpen != nil {
			t.Fatal("Could not open file:", errOpen)
		}

		if _, err := file.WriteString("Hello world !"); err != nil {
			t.Fatal("Could not write file:", err)
		}

		if err := file.Close(); err != nil {
			t.Fatal("Could not close file:", err)
		}
	}

	{ // Reading a file
		file, errOpen := fs.Open("file1")
		if errOpen != nil {
			t.Fatal("Could not open file:", errOpen)
		}

		defer func() {
			if err := file.Close(); err != nil {
				t.Fatal("Could not close file:", err)
			}
		}()

		buffer := make([]byte, 5)
		if _, err := file.ReadAt(buffer, 6); err != nil {
			t.Fatal("Could not perform ReadAt:", err)
		}

		if string(buffer) != "world" {
			t.Fatal("Bad fetch:", string(buffer))
		}
	}
}

func TestWriteAt(t *testing.T) {
	fs := GetFs(t)

	file, errOpen := fs.OpenFile("file1", os.O_WRONLY, 0777)
	if errOpen != nil {
		t.Fatal("Could not open file:", errOpen)
	}

	defer func() {
		if err := file.Close(); err != nil {
			t.Fatal("Could not close file:", err)
		}
	}()

	if _, err := file.WriteAt([]byte("hello !"), 1); err == nil {
		t.Fatal("We have no way to make this work !")
	}
}

func TestFileCreate(t *testing.T) {
	fs := GetFs(t)

	if _, err := fs.Stat("/file1"); err == nil {
		t.Fatal("We shouldn't be able to get a file cachedInfo at this stage")
	}

	if file, err := fs.Create("/file1"); err != nil {
		t.Fatal("Could not create file:", err)
	} else if err := file.Close(); err != nil {
		t.Fatal("Couldn't close file:", err)
	}

	// Empty files are not acually created on Azure
	// so getting the Stat or deleting them isn't possible
	// if stat, err := fs.Stat("/file1"); err != nil {
	// 	t.Fatal("Could not access file:", err)
	// } else if stat.Size() != 0 {
	// 	t.Fatal("File should be empty")
	// }

	// if err := fs.Remove("/file1"); err != nil {
	// 	t.Fatal("Could not delete file:", err)
	// }

	if _, err := fs.Stat("/file1"); err == nil {
		t.Fatal("Should not be able to access file")
	}
}
func TestRemove(t *testing.T) {
	fs := GetFs(t)

	file, err := fs.Create("/file1")
	if err != nil {
		t.Fatal("Could not create /file1:", err)
	}

	if _, err := file.WriteString("Hello world!"); err != nil {
		t.Fatal("Could not write file:", err)
	}

	if err := file.Close(); err != nil {
		t.Fatal("Could not close file1 err:", err)
	}

	err = fs.Remove("/file1")
	if err != nil {
		t.Fatal("Could not remove /file1:", err)
	}
}
func TestRemoveAll(t *testing.T) {
	fs := GetFs(t)

	if err := fs.Mkdir("/dir1", 0750); err != nil {
		t.Fatal("Could not create dir1:", err)
	}

	if err := fs.Mkdir("/dir1/dir2", 0750); err != nil {
		t.Fatal("Could not create dir2:", err)
	}
	file, err := fs.Create("/dir1/file1")
	if err != nil {
		t.Fatal("Could not create /dir1/file1:", err)
	}

	if _, err := file.WriteString("Hello world!"); err != nil {
		t.Fatal("Could not write file:", err)
	}

	if err := file.Close(); err != nil {
		t.Fatal("Could not close /dir1/file1 err:", err)
	}

	if err := fs.RemoveAll("/dir1"); err != nil {
		t.Fatal("Could not delete all files:", err)
	}

	if root, err := fs.Open("/"); err != nil {
		t.Fatal("Could not access root:", root)
	} else {
		if files, err := root.Readdir(-1); err != nil {
			t.Fatal("Could not readdir:", err)
		} else if len(files) != 0 {
			t.Fatal("We should not have any files !")
		}
	}
}

func TestMkdirAll(t *testing.T) {
	fs := GetFs(t)
	if err := fs.MkdirAll("/dir3/dir4", 0755); err != nil {
		t.Fatal("Could not perform MkdirAll:", err)
	}

	// Cannot actually make directories in Azure containers
	// getting the Stat fails
	// if _, err := fs.Stat("/dir3/dir4"); err != nil {
	// 	t.Fatal("Could not read dir4:", err)
	// }
}

func TestDirHandle(t *testing.T) {
	fs := GetFs(t)

	// We create a "dir1" directory
	if err := fs.Mkdir("/dir1", 0750); err != nil {
		t.Fatal("Could not create dir:", err)
	}

	// Then create a "file1" file in it
	if file, err := fs.Create("/dir1/file1"); err != nil {
		t.Fatal("Could not create file:", err)
	} else if err := file.Close(); err != nil {
		t.Fatal("Couldn't close file:", err)
	}

	// Opening "dir1" should work
	// if _, err := fs.Open("/dir1"); err != nil {
	// 	t.Fatal("Could not open dir1:", err)
	// } else {
	// 	// Listing files should be OK too
	// 	if files, errReaddir := dir1.Readdir(-1); errReaddir != nil {
	// 		t.Fatal("Could not read dir")
	// 	} else if len(files) != 1 || files[0].Name() != "file1" {
	// 		t.Fatal("Listed files are incorrect !")
	// 	}
	// }

	// Opening "dir2" should fail
	if _, err := fs.Open("/dir2"); err == nil {
		t.Fatal("Opening /dir2 should have triggered an error !")
	}
}

func TestFileReaddirnames(t *testing.T) {
	fs := GetFs(t)

	// We create some dirs
	for _, dir := range []string{"/dir1", "/dir2", "/dir3"} {
		if err := fs.Mkdir(dir, 0750); err != nil {
			t.Fatal("Could not create dir:", err)
		}
	}

	// root, errOpen := fs.Open("/")
	_, errOpen := fs.Open("/")
	if errOpen != nil {
		t.Fatal(errOpen)
	}

	// cannot read directory name since Azure doesn't allow
	// direcotries to be created in containers
	// {
	// 	dirs, err := root.Readdirnames(2)
	// 	if err != nil {
	// 		t.Fatal(err)
	// 	}
	// 	if len(dirs) != 0 {
	// 		t.Fatal("Wrong dirs")
	// 	}
	// 	if len(dirs) != 2 || dirs[0] != "dir1" || dirs[1] != "dir2" {
	// 		t.Fatal("Wrong dirs")
	// 	}
	// }

	// 	{
	// 		dirs, err := root.Readdirnames(2)
	// 		if err != nil {
	// 			t.Fatal(err)
	// 		}
	// 		if len(dirs) != 1 || dirs[0] != "dir3" {
	// 			t.Fatal("Wrong dirs")
	// 		}
	// 	}
}

func TestFileStat(t *testing.T) {
	fs := GetFs(t)

	// We create a "dir1" directory
	if err := fs.Mkdir("/dir1", 0750); err != nil {
		t.Fatal("Could not create dir:", err)
	}

	// Then create a "file1" file in it
	if file, err := fs.Create("/dir1/file1"); err != nil {
		t.Fatal("Could not create file:", err)
	} else if err := file.Close(); err != nil {
		t.Fatal("Couldn't close file:", err)
	}

	// Cannot actually make directories in Azure containers
	// so getting the Stat fails
	// if dir1, err := fs.Open("/dir1"); err != nil {
	// 	t.Fatal(err)
	// } else {
	// 	if stat, err := dir1.Stat(); err != nil {
	// 		t.Fatal(err)
	// 	} else if stat.Mode() != 0755 {
	// 		t.Fatal("Wrong dir mode")
	// 	}
	// }

	// Nothing was written to the file so it wasn't actually created
	// so getting the Stat fails
	// if file1, err := fs.Open("/dir1/file1"); err != nil {
	// 	t.Fatal(err)
	// } else {
	// 	if stat, err := file1.Stat(); err != nil {
	// 		t.Fatal(err)
	// 	} else if stat.Mode() != 0664 {
	// 		t.Fatal("Wrong file mode")
	// 	}
	// }
}

func testCreateFile(t *testing.T, fs afero.Fs, name string, content string) {
	file, err := fs.OpenFile(name, os.O_WRONLY, 0750)
	if err != nil {
		t.Fatal("Could not open file", name, ":", err)
	}
	if _, err := file.WriteString(content); err != nil {
		t.Fatal("Could not write content to file", err)
	}
	if err := file.Close(); err != nil {
		t.Fatal("Could not close file")
	}
}

func TestRename(t *testing.T) {
	fs := GetFs(t)

	if errMkdirAll := fs.MkdirAll("/dir1/dir2", 0750); errMkdirAll != nil {
	} else if file, errOpenFile := fs.OpenFile("/dir1/dir2/file1", os.O_WRONLY, 0750); errOpenFile != nil {
		t.Fatal("Couldn't open file:", errOpenFile)
	} else {
		if _, errWriteString := file.WriteString("Hello world !"); errWriteString != nil {
			t.Fatal("Couldn't write:", errWriteString)
		} else if errClose := file.Close(); errClose != nil {
			t.Fatal("Couldn't close:", errClose)
		}
	}

	time.Sleep(time.Second * 2)

	if errRename := fs.Rename("/dir1/dir2/file1", "/dir1/dir2/file2"); errRename != nil {
		t.Fatal("Couldn't rename file err:", errRename)
	}

	if _, err := fs.Stat("/dir1/dir2/file1"); err == nil {
		t.Fatal("File shouldn't exist anymore")
	}

	if _, err := fs.Stat("/dir1/dir2/file2"); err != nil {
		t.Fatal("Couldn't fetch file cachedInfo:", err)
	}

	// Renaming of a directory isn't tested because it's not supported by afero in the first place
}

func TestFileTime(t *testing.T) {
	fs := GetFs(t)
	name := "/dir1/file1"
	beforeCreate := time.Now().UTC()
	// Well, we have a 1-second precision
	time.Sleep(time.Second)
	testCreateFile(t, fs, name, "Hello world !")
	time.Sleep(time.Second)
	afterCreate := time.Now().UTC()
	var modTime time.Time
	if info, errStat := fs.Stat(name); errStat != nil {
		t.Fatal("Couldn't stat", name, ":", errStat)
	} else {
		modTime = info.ModTime()
	}
	if modTime.Before(beforeCreate) || modTime.After(afterCreate) {
		t.Fatal("Invalid dates", "modTime =", modTime, "before =", beforeCreate, "after =", afterCreate)
	}
	if err := fs.Chtimes(name, time.Now().UTC(), time.Now().UTC()); err == nil {
		t.Fatal("If Chtimes is supported, we should have a check here")
	}
}

func TestChmod(t *testing.T) {
	fs := GetFs(t)
	name := "/dir1/file1"
	testCreateFile(t, fs, name, "Hello world !")
	if err := fs.Chmod(name, 0750); err == nil {
		t.Fatal("If Chmod is supported, we should have a check here")
	}
}
