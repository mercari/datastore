package fixture

import (
	"google.golang.org/appengine/log"
	"github.com/mjibson/goon"
)

type MailStore struct{}
type Event struct{}
type MailInfo struct{}

func (store *MailStore) ApplyCircleTemplate(g *goon.Goon, event *Event) (*MailInfo, error) {
	log.Infof(g.Context, "foobar")
	return nil, nil
}
