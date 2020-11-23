package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils"
	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/indexes/avm"
	"github.com/ava-labs/ortelius/services/indexes/pvm"
	"github.com/ava-labs/ortelius/stream"
	"github.com/ava-labs/ortelius/stream/consumers"
	"github.com/segmentio/kafka-go"

	// sqlite
	_ "github.com/mattn/go-sqlite3"
)

type DBHolder struct {
	DB   *sql.DB
	Lock sync.Mutex
}

type Replay struct {
	Config    *string
	GroupName *string
	DedupDB   *string
	DBHolder  *DBHolder
	errs      *utils.AtomicInterface
	running   *utils.AtomicBool
	c         *cfg.Config
}

func main() {
	cfg.PerformUpdates = true

	config := flag.String("config", "", "config file")
	groupName := flag.String("group", "", "group name")
	dedupDB := flag.String("dedupDb", "dedupDb.db", "dedupDb")

	flag.Parse()

	replay := &Replay{
		Config:    config,
		GroupName: groupName,
		DedupDB:   dedupDB,
	}
	replay.Start()
}

func (replay *Replay) Start() {
	c, err := cfg.NewFromFile(*replay.Config)
	if err != nil {
		log.Fatalln("config file not found", replay.Config, ":", err.Error())
		return
	}

	replay.c = c

	_, err = logging.New(c.Logging)
	if err != nil {
		log.Fatalln("Failed to create log", c.Logging.Directory, ":", err.Error())
		return
	}

	db, err := sql.Open("sqlite3", *replay.DedupDB)
	if err != nil {
		log.Fatalln("config file not found", replay.Config, ":", err.Error())
		return
	}

	replay.DBHolder = &DBHolder{DB: db}

	err = replay.createTable()
	if err != nil {
		log.Fatalln("create dedup table failed", ":", err.Error())
		return
	}

	replay.errs = &utils.AtomicInterface{}
	replay.running = &utils.AtomicBool{}
	replay.running.SetValue(true)

	for _, chainID := range c.Chains {
		err = replay.handleReader(chainID)
		if err != nil {
			log.Fatalln("reader failed", chainID, ":", err.Error())
			return
		}
	}

	for replay.running.GetValue() {
		time.Sleep(time.Second)
	}

	fmt.Fprintf(os.Stderr, "err %v\n", replay.errs.GetValue())
}

func (replay *Replay) createTable() error {
	replay.DBHolder.Lock.Lock()
	defer replay.DBHolder.Lock.Unlock()

	statement, err := replay.DBHolder.DB.Prepare("CREATE TABLE IF NOT EXISTS " +
		"dedup( " +
		"id VARCHAR NOT NULL, " +
		"primary key (id) " +
		")")
	if err != nil {
		return err
	}

	_, err = statement.Exec()
	return err
}

func (replay *Replay) idPresent(id string) (bool, error) {
	replay.DBHolder.Lock.Lock()
	defer replay.DBHolder.Lock.Unlock()

	statement, err := replay.DBHolder.DB.Prepare("select id from dedup")
	if err != nil {
		return false, err
	}

	var rows *sql.Rows
	rows, err = statement.Query(id)
	if err != nil {
		return false, err
	}
	if rows.Err() != nil {
		return false, err
	}
	found := false
	for ok := rows.Next(); ok; ok = rows.Next() {
		var data string
		err := rows.Scan(
			&data,
		)
		if err != nil {
			return false, err
		}
		found = true
	}
	return found, nil
}

func (replay *Replay) idProcessed(id string) error {
	replay.DBHolder.Lock.Lock()
	defer replay.DBHolder.Lock.Unlock()

	statement, err := replay.DBHolder.DB.Prepare("insert into dedup (id) values (?)")
	if err != nil {
		return err
	}

	_, err = statement.Exec(id)
	if err != nil {
		return err
	}
	return nil
}

type MessageR struct {
	id        string
	chainID   string
	body      []byte
	timestamp int64
}

func (m *MessageR) ID() string       { return m.id }
func (m *MessageR) ChainID() string  { return m.chainID }
func (m *MessageR) Body() []byte     { return m.body }
func (m *MessageR) Timestamp() int64 { return m.timestamp }

func (replay *Replay) handleReader(chain cfg.Chain) error {
	conns, err := services.NewConnectionsFromConfig(replay.c.Services, false)
	if err != nil {
		return err
	}

	var writer services.Consumer
	switch chain.VMType {
	case consumers.IndexerAVMName:
		writer, err = avm.NewWriter(conns, replay.c.NetworkID, chain.ID)
		if err != nil {
			return err
		}
	case consumers.IndexerPVMName:
		writer, err = pvm.NewWriter(conns, replay.c.NetworkID, chain.ID)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown vmtype")
	}

	go func() {
		defer replay.running.SetValue(false)

		reader := kafka.NewReader(kafka.ReaderConfig{
			Topic:       stream.GetTopicName(replay.c.NetworkID, chain.ID, stream.ConsumerEventTypeDefault),
			Brokers:     replay.c.Kafka.Brokers,
			GroupID:     *replay.GroupName,
			StartOffset: kafka.FirstOffset,
			MaxBytes:    stream.ConsumerMaxBytesDefault,
		})

		ctx := context.Background()

		for replay.running.GetValue() {
			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				replay.errs.SetValue(err)
				return
			}

			id, err := ids.ToID(msg.Key)
			if err != nil {
				replay.errs.SetValue(err)
				return
			}

			present, err := replay.idPresent(id.String())
			if err != nil {
				replay.errs.SetValue(err)
				return
			}
			if present {
				continue
			}

			msgc := &MessageR{
				chainID:   chain.ID,
				body:      msg.Value,
				id:        id.String(),
				timestamp: msg.Time.UTC().Unix(),
			}

			err = writer.Consume(ctx, msgc)
			if err != nil {
				replay.errs.SetValue(err)
				return
			}

			err = replay.idProcessed(id.String())
			if err != nil {
				replay.errs.SetValue(err)
				return
			}
		}
	}()

	return nil
}
