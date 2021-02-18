package stream

import (
	"context"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services"
	"github.com/gocraft/dbr/v2"
)

const (
	totalParts = "4"
)

func fetchPollForTopic(sess *dbr.Session, topicName string, part *int) ([]*services.TxPool, error) {
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
		switch *part {
		case 0:
			q = q.
				Where("processed=? and topic=? and floor( mod(unix_timestamp(created_at)*100000,"+totalParts+"))=0", 0, topicName)
		case 1:
			q = q.
				Where("processed=? and topic=? and floor( mod(unix_timestamp(created_at)*100000,"+totalParts+"))=1", 0, topicName)
		case 2:
			q = q.
				Where("processed=? and topic=? and floor( mod(unix_timestamp(created_at)*100000,"+totalParts+"))=2", 0, topicName)
		default:
			q = q.
				Where("processed=? and topic=? and floor( mod(unix_timestamp(created_at)*100000,"+totalParts+"))>=3", 0, topicName)
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
