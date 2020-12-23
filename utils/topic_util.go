package utils

import (
	"context"
	"fmt"

	"net"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"

	avlancheGoUtils "github.com/ava-labs/avalanchego/utils"
)

var (
	ErrStopTopicUtil = fmt.Errorf("stop")
)

type TopicUtil interface {
	Partitions(ctx context.Context) (map[int]kafka.PartitionOffsets, error)
	Consume(brokers []string, offset int64, maxBytes int, readTimeout time.Duration, accept func(kafka.Message) error, partitions []int) error
	Close()
}

type topicUtil struct {
	addr        net.Addr
	timeout     time.Duration
	topicName   string
	kafkaClient kafka.Client
	doneCh      chan bool
	errs        *avlancheGoUtils.AtomicInterface
	running     avlancheGoUtils.AtomicBool
}

func NewTopicUtil(addr net.Addr, timeout time.Duration, topicName string) TopicUtil {
	tu := topicUtil{addr: addr, timeout: timeout, topicName: topicName, doneCh: make(chan bool)}
	tu.init()
	return &tu
}

func (tu *topicUtil) init() {
	tu.running.SetValue(true)
	tu.kafkaClient = kafka.Client{Addr: tu.addr, Timeout: time.Duration(0)}
}

func (tu *topicUtil) Partitions(ctx context.Context) (map[int]kafka.PartitionOffsets, error) {
	partitions := make(map[int]kafka.PartitionOffsets)
	for topicPartition := 0; ; topicPartition++ {
		kafkaOffsetReq := make(map[string][]kafka.OffsetRequest)
		kafkaOffsetReq[tu.topicName] = []kafka.OffsetRequest{kafka.FirstOffsetOf(topicPartition)}

		loresp, err := tu.kafkaClient.ListOffsets(ctx, &kafka.ListOffsetsRequest{Topics: kafkaOffsetReq})
		if err != nil {
			return nil, err
		}

		topv, ok := loresp.Topics[tu.topicName]
		if !ok {
			return nil, fmt.Errorf("not found %s", tu.topicName)
		}

		if len(topv) == 0 {
			return nil, fmt.Errorf("not found %s", tu.topicName)
		}

		if topicPartition == 0 && topv[0].Error == kafka.UnknownTopicOrPartition {
			return nil, topv[0].Error
		}

		if topv[0].Error == kafka.UnknownTopicOrPartition {
			break
		}

		if topv[0].Error != nil {
			return nil, topv[0].Error
		}
		partitions[topicPartition] = topv[0]
	}

	return partitions, nil
}

func (tu *topicUtil) Consume(brokers []string, offset int64, maxBytes int, readTimeout time.Duration, accept func(kafka.Message) error, partitions []int) error {
	if len(partitions) == 0 {
		return nil
	}

	wg := sync.WaitGroup{}
	for ipos := 0; ipos < len(partitions); ipos++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			reader := kafka.NewReader(kafka.ReaderConfig{
				Topic:       tu.topicName,
				Brokers:     brokers,
				Partition:   ipos,
				StartOffset: offset,
				MaxBytes:    maxBytes,
			})

			for {
				switch {
				case !tu.running.GetValue():
					break
				case tu.errs.GetValue() != nil:
					break
				case <-tu.doneCh:
					break
				default:
					msg, err := tu.readMessage(readTimeout, reader)
					if err == nil {
						erraccept := accept(msg)
						if erraccept == ErrStopTopicUtil {
							return
						}
						if erraccept != nil {
							tu.errs.SetValue(err)
							return
						}
					}
					if err != context.DeadlineExceeded {
						tu.errs.SetValue(err)
						return
					}
				}
			}
		}()
	}

	wg.Wait()
	return nil
}

func (tu *topicUtil) readMessage(duration time.Duration, reader *kafka.Reader) (kafka.Message, error) {
	ctx, cancelFn := context.WithTimeout(context.Background(), duration)
	defer cancelFn()
	return reader.ReadMessage(ctx)
}

func (tu *topicUtil) Close() {
	tu.running.SetValue(false)
	close(tu.doneCh)
}