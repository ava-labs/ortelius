package stream

import (
	"context"
	"fmt"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services"
	"github.com/gocraft/dbr/v2"
)

func fetchPollForTopic(sess *dbr.Session, topicName string, part *int, maxPart int) ([]*services.TxPool, error) {
	ctx, cancelFn := context.WithTimeout(context.Background(), cfg.DefaultConsumeProcessWriteTimeout)
	defer cancelFn()

	var rowdata []*services.TxPool
	q := sess.Select(
		"id",
		"network_id",
		"chain_id",
		"msg_key",
		"serialization",
		"processed",
		"topic",
		"created_at",
	).From(services.TableTxPool)

	if part != nil {
		createdAtStr := fmt.Sprintf("floor( mod(unix_timestamp(created_at)*100000,%d))", maxPart)
		if *part < maxPart-1 {
			q = q.
				Where("processed=? and topic=? and "+createdAtStr+"=?", 0, topicName, *part)
		} else {
			q = q.
				Where("processed=? and topic=? and "+createdAtStr+">=?", 0, topicName, *part)
		}
	} else {
		q = q.
			Where("processed=? and topic=?", 0, topicName)
	}

	_, err := q.
		OrderAsc("topic").OrderAsc("processed").OrderAsc("created_at").
		Limit(pollLimit).
		LoadContext(ctx, &rowdata)
	if err != nil {
		return nil, err
	}
	return rowdata, nil
}
