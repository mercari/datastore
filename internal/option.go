package internal

import (
	"net/http"
	"os"

	"golang.org/x/oauth2"
	"google.golang.org/grpc"
)

type ClientSettings struct {
	ProjectID string

	Scopes          []string
	TokenSource     oauth2.TokenSource
	CredentialsFile string // if set, Token Source is ignored.
	HTTPClient      *http.Client
	GRPCDialOpts    []grpc.DialOption
}

func GetProjectID() string {
	return os.Getenv("PROJECT_ID") // NOTE ないよりマシ
}
