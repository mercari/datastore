package aedatastore

import (
	"fmt"
	"os"
	"testing"

	"github.com/favclip/testerator"
)

func TestMain(m *testing.M) {
	_, _, err := testerator.SpinUp()
	if err != nil {
		fmt.Fprint(os.Stderr, err.Error())
		os.Exit(1)
	}

	status := m.Run()

	err = testerator.SpinDown()
	if err != nil {
		fmt.Fprint(os.Stderr, err.Error())
		os.Exit(1)
	}

	os.Exit(status)
}
