package rpcretry

import (
	"fmt"
	"strings"

	"go.mercari.io/datastore"
)

var _ datastore.Middleware = &glitchEmulator{}

type glitchEmulator struct {
	raised   map[string]map[string]int // raised["PutMultiWithoutTx"]["Data/1"] = 1
	errCount int
}

func (gm *glitchEmulator) keysToString(keys []datastore.Key) string {
	keyStrings := make([]string, 0, len(keys))
	for _, key := range keys {
		keyStrings = append(keyStrings, key.String())
	}

	return strings.Join(keyStrings, ", ")
}

func (gm *glitchEmulator) raiseError(opName string, keys []datastore.Key) error {
	if gm.raised == nil {
		gm.raised = make(map[string]map[string]int)
	}
	if _, ok := gm.raised[opName]; !ok {
		gm.raised[opName] = make(map[string]int)
	}
	keysStr := gm.keysToString(keys)
	cnt := gm.raised[opName][keysStr]
	if cnt != gm.errCount {
		gm.raised[opName][keysStr] = cnt + 1
		return fmt.Errorf("error by *glitchEmulator: %s, keys=%s", opName, keysStr)
	}

	return nil
}

func (gm *glitchEmulator) AllocateIDs(info *datastore.MiddlewareInfo, keys []datastore.Key) ([]datastore.Key, error) {

	if err := gm.raiseError("AllocateIDs", keys); err != nil {
		return nil, err
	}

	return info.Next.AllocateIDs(info, keys)
}

func (gm *glitchEmulator) PutMultiWithoutTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) ([]datastore.Key, error) {

	if err := gm.raiseError("PutMultiWithoutTx", keys); err != nil {
		return nil, err
	}

	return info.Next.PutMultiWithoutTx(info, keys, psList)
}

func (gm *glitchEmulator) PutMultiWithTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) ([]datastore.PendingKey, error) {

	if err := gm.raiseError("PutMultiWithTx", keys); err != nil {
		return nil, err
	}

	return info.Next.PutMultiWithTx(info, keys, psList)
}

func (gm *glitchEmulator) GetMultiWithoutTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) error {

	if err := gm.raiseError("GetMultiWithoutTx", keys); err != nil {
		return err
	}

	return info.Next.GetMultiWithoutTx(info, keys, psList)
}

func (gm *glitchEmulator) GetMultiWithTx(info *datastore.MiddlewareInfo, keys []datastore.Key, psList []datastore.PropertyList) error {

	if err := gm.raiseError("GetMultiWithTx", keys); err != nil {
		return err
	}

	return info.Next.GetMultiWithTx(info, keys, psList)
}

func (gm *glitchEmulator) DeleteMultiWithoutTx(info *datastore.MiddlewareInfo, keys []datastore.Key) error {

	if err := gm.raiseError("DeleteMultiWithoutTx", keys); err != nil {
		return err
	}

	return info.Next.DeleteMultiWithoutTx(info, keys)
}

func (gm *glitchEmulator) DeleteMultiWithTx(info *datastore.MiddlewareInfo, keys []datastore.Key) error {

	if err := gm.raiseError("DeleteMultiWithTx", keys); err != nil {
		return err
	}

	return info.Next.DeleteMultiWithTx(info, keys)
}

func (gm *glitchEmulator) PostCommit(info *datastore.MiddlewareInfo, tx datastore.Transaction, commit datastore.Commit) error {
	return info.Next.PostCommit(info, tx, commit)
}

func (gm *glitchEmulator) PostRollback(info *datastore.MiddlewareInfo, tx datastore.Transaction) error {
	return info.Next.PostRollback(info, tx)
}

func (gm *glitchEmulator) Run(info *datastore.MiddlewareInfo, q datastore.Query, qDump *datastore.QueryDump) datastore.Iterator {
	return info.Next.Run(info, q, qDump)
}

func (gm *glitchEmulator) GetAll(info *datastore.MiddlewareInfo, q datastore.Query, qDump *datastore.QueryDump, psList *[]datastore.PropertyList) ([]datastore.Key, error) {

	if err := gm.raiseError("GetAll", nil); err != nil {
		return nil, err
	}

	return info.Next.GetAll(info, q, qDump, psList)
}

func (gm *glitchEmulator) Next(info *datastore.MiddlewareInfo, q datastore.Query, qDump *datastore.QueryDump, iter datastore.Iterator, ps *datastore.PropertyList) (datastore.Key, error) {
	return info.Next.Next(info, q, qDump, iter, ps)
}

func (gm *glitchEmulator) Count(info *datastore.MiddlewareInfo, q datastore.Query, qDump *datastore.QueryDump) (int, error) {

	if err := gm.raiseError("Count", nil); err != nil {
		return 0, err
	}

	return info.Next.Count(info, q, qDump)
}
