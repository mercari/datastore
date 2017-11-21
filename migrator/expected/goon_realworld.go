package fixture

import (
	"context"
	"go.mercari.io/datastore/boom"
	"google.golang.org/appengine/log"
)

type MailStore struct{}
type Event struct{}
type MailInfo struct{}

func (store *MailStore) ApplyCircleTemplate(bm *boom.Boom, event *Event) (*MailInfo, error) {
	log.Infof(bm.Context, "foobar")
	return nil, nil
}
