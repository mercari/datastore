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

	for idx, fInfo := range fInfos {
		t.Run(fInfo.Name(), func(t *testing.T) {
			w := &Walker{
				PackageNameAE:          "appengine",
				PackageNameAEDatastore: "datastore",
				PackageNameGoon:        "goon",
				PackageNameBoom:        "boom",
				PackageNameContext:     "context",
				ClientVarName:          "client",
				ContextVarName:         "ctx",
				QueryVarName:           "q",
				TxVarName:              "tx",
				CommitVarName:          "commit",
				GoonVarName:            "g",
				BoomVarName:            "bm",
				GoonTxName:             "tg",
			}

			fset, f, err := MigrateFile(w, filepath.Join("./fixture", fInfo.Name()))
			if err != nil {
				t.Fatal(err)
			}

			var buf bytes.Buffer
			err = format.Node(&buf, fset, f)
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
