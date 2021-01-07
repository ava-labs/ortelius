package stream

import (
	"strings"
	"time"

	"github.com/ava-labs/ortelius/services/metrics"

	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/ortelius/services/db"
)

func RetryDb(retries uint32, persist func() error, log logging.Logger, msgprefix string, collectors metrics.Collector) error {
	var err error
	icnt := uint32(0)
	for ; icnt <= retries; icnt++ {
		err = persist()
		if err == nil {
			return nil
		}
		if !strings.Contains(err.Error(), db.DeadlockDBErrorMessage) {
			log.Warn("%s %v", msgprefix, err)
		} else {
			icnt = 0
		}

		time.Sleep(500 * time.Millisecond)
	}
	if err != nil {
		collectors.Error()
		log.Error("%s %v", msgprefix, err)
		return err
	}
	return err
}
