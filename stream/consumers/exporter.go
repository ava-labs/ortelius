// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package consumers

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"time"

	"github.com/ava-labs/avalanchego/utils/wrappers"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/stream"
	"github.com/segmentio/kafka-go"
)

var (
	ErrExportFileExists = errors.New("export file exists")
)

// ExportToDisk writes the stream data for a given chain out to disk
func ExportToDisk(conf *cfg.Config, exportPath string, chainID string) (int64, error) {
	// Create path if it doesn't exist yet
	if _, err := os.Stat(exportPath); os.IsNotExist(err) {
		if err := os.MkdirAll(exportPath, 0755); err != nil {
			log.Printf("Filed to create export directory %s: %s\n", exportPath, err.Error())
			return 0, err
		}
	}

	// Create exporter
	fileName := path.Join(exportPath, chainID+"-consensus-export.txt")
	log.Println("Exporting to file:", fileName)
	exportNextFn, closeFn, err := newExportReadWriter(fileName, conf.Brokers, conf.NetworkID, chainID)
	if err != nil {
		return 0, err
	}

	// Export until an error is returned
	log.Println("Starting exporter...")
	var i int64
	var exportErr error
	for exportErr = exportNextFn(); err == nil; exportErr = exportNextFn() {
		i++
		if i%1000 == 0 {
			fmt.Printf("Exported %d records\n", i)
		}
	}

	// Close and log any errors
	errs := wrappers.Errs{}
	errs.Add(closeFn())
	if exportErr != context.DeadlineExceeded {
		errs.Add(exportErr)
	}
	return i, errs.Err
}

func newExportReadWriter(fileName string, brokers []string, networkID uint32, chainID string) (func() error, func() error, error) {
	// Open file and kafka reader
	_, err := os.Stat(fileName)
	if !os.IsNotExist(err) {
		if err == nil {
			err = ErrExportFileExists
		}
		return nil, nil, err
	}
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0660)
	if err != nil {
		return nil, nil, err
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		MaxBytes:    stream.ConsumerMaxBytesDefault,
		Brokers:     brokers,
		StartOffset: kafka.FirstOffset,
		GroupID:     fmt.Sprintf("exporter-%d", time.Now().UTC().Unix()),
		Topic:       stream.GetTopicName(networkID, chainID, stream.ConsumerEventTypeDefault),
	})

	// Create functions for exporting and closing
	closeFn := func() error {
		errs := wrappers.Errs{}
		errs.Add(reader.Close(), file.Close())
		return errs.Err
	}

	writeFn := func() error {
		ctx, cancelFn := context.WithTimeout(context.Background(), 30*time.Second)
		cancelFn()

		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			return err
		}

		_, err = fmt.Fprintf(file, "%s\n", hex.EncodeToString(msg.Value))
		return err
	}

	return writeFn, closeFn, nil
}
