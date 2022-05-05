package mr

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

func atomicWriteFile(filename string, r io.Reader) (err error) {
	dir, file := filepath.Split(filename)

	if dir == "" {
		dir = "."
	}

	f, err := ioutil.TempFile(dir, file)
	if err != nil {
		return fmt.Errorf("cannot create temp file: %v", err)
	}

	defer func() {
		if err != nil {
			_ = os.Remove(f.Name())
		}
	}()
	name := f.Name()
	if _, err := io.Copy(f, r); err != nil {
		return fmt.Errorf("cannot write data to tempfile %q: %v", name, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("cannot close temp file %q: %v", name, err)
	}
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {

	} else if err != nil {
		return err
	} else {
		if err := os.Chmod(name, info.Mode()); err != nil {
			return fmt.Errorf("cannot set filemode on tempfile %q: %v", name, err)
		}
	}
	if err := os.Rename(name, filename); err != nil {
		return fmt.Errorf("cannot replace %q with tempfile %q: %v", filename, err)
	}
	return nil
}
