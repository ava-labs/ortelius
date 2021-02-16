package stream

import (
	"context"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services"
	"github.com/gocraft/dbr/v2"
)

func fetchPollForTopic(sess *dbr.Session, topicName string) ([]*services.TxPool, error) {
	ctx, cancelFn := context.WithTimeout(context.Background(), cfg.DefaultConsumeProcessWriteTimeout)
	defer cancelFn()

	var rowdata []*services.TxPool
	_, err := sess.Select(
		"id",
		"network_id",
		"chain_id",
		"msg_key",
		"serialization",
		"processed",
		"topic",
		"created_at",
	).From(services.TableTxPool).
		Where("processed=? and topic=?", 0, topicName).
		OrderAsc("processed").OrderAsc("topic").OrderAsc("created_at").
		Limit(pollLimit).
		LoadContext(ctx, &rowdata)
	if err != nil {
		return nil, err
	}
	return rowdata, nil
}