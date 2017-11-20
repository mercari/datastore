package migrator

import (
	"bytes"
	"go/format"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestMigrator_LKGCheck(t *testing.T) {
	fInfos, err := ioutil.ReadDir("./fixture")
	if err != nil {
		t.Fatal(err)
	}

	fileNames := make([]string, 0, len(fInfos))
	for _, f := range fInfos {
		t.Log(f.Name())
		fileNames = append(fileNames, filepath.Join("./fixture", f.Name()))
	}

	fset, fs, err := Main(fileNames)
	if err != nil {
		t.Fatal(err)
	}

	for idx, f := range fs {
		t.Run(fInfos[idx].Name(), func(t *testing.T) {
			buf := bytes.NewBufferString("")
			err = format.Node(buf, fset, f)
			if err != nil {
				t.Fatal(err)
			}
			newSrc := buf.String()

			expectedFile := filepath.Join("./expected", fInfos[idx].Name())
			expected, err := ioutil.ReadFile(expectedFile)
			if os.IsNotExist(err) {
				t.Logf("write new result to %s", expectedFile)
				expected = []byte(newSrc)
				err = ioutil.WriteFile(expectedFile, expected, 0644)
				if err != nil {
					t.Fatal(err)
				}
			}

			if v := newSrc; v != string(expected) {
				t.Error("unexpected")
			}
		})
	}
}
