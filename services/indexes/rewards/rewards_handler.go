package rewards

import (
	"context"
	"time"

	"github.com/ava-labs/ortelius/services/idb"
	"github.com/ava-labs/ortelius/services/servicesconn"
	"github.com/ava-labs/ortelius/utils/controlwrap"

	"github.com/ava-labs/ortelius/services/indexes/avax"

	"github.com/ava-labs/avalanchego/api"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/formatting"
	avalancheGoAvax "github.com/ava-labs/avalanchego/vms/components/avax"
	"github.com/ava-labs/avalanchego/vms/platformvm"
	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/indexes/models"
)

type RewardsHandler struct {
	client      *platformvm.Client
	conns       *servicesconn.Connections
	perist      idb.Persist
	avaxAssetID ids.ID
	writer      *avax.Writer
	cid         ids.ID
}

func (r *RewardsHandler) Start(sc controlwrap.ControlWrap) error {
	conns, err := sc.DatabaseOnly()
	if err != nil {
		return err
	}
	go r.runTicker(sc, conns)
	return nil
}

func (r *RewardsHandler) runTicker(sc controlwrap.ControlWrap, conns *servicesconn.Connections) {
	ticker := time.NewTicker(1 * time.Second)

	doneCh := make(chan struct{}, 1)

	r.conns = conns
	r.client = platformvm.NewClient("http://localhost:9650", 1*time.Minute)
	r.perist = idb.NewPersist()

	r.avaxAssetID = sc.Genesis().AvaxAssetID

	r.cid = ids.Empty
	r.writer = avax.NewWriter(r.cid.String(), r.avaxAssetID)

	defer func() {
		close(doneCh)
		ticker.Stop()
		_ = conns.Close()
	}()

	for {
		select {
		case <-ticker.C:
			err := r.processRewards()
			if err != nil {
				sc.LogMe().Error("process rewards %s", err)
			}
		case <-doneCh:
			return
		}
	}
}

func (r *RewardsHandler) processRewards() error {
	job := r.conns.StreamDBDedup().NewJob("rewards-handler")
	sess := r.conns.DB().NewSessionForEventReceiver(job)

	ctx := context.Background()

	var err error

	type RewardTx struct {
		ID        string
		Txid      string
		Type      models.BlockType
		CreatedAt time.Time
	}
	var reardsTxs []RewardTx
	_, err = sess.Select(
		idb.TableRewards+".id",
		idb.TableRewards+".txid",
		idb.TablePvmBlocks+".type",
		idb.TableRewards+".created_at",
	).
		From(idb.TableRewards).
		Join(idb.TablePvmBlocks, idb.TableRewards+".block_id = "+idb.TablePvmBlocks+".parent_id").
		Where(idb.TableRewards+".processed = ? and "+idb.TableRewards+".created_at < ?", 0, time.Now()).
		LoadContext(ctx, &reardsTxs)
	if err != nil {
		return err
	}
	if len(reardsTxs) == 0 {
		return nil
	}

	for _, rewardTx := range reardsTxs {
		if rewardTx.Type == models.BlockTypeAbort {
			err = r.markRewardProcessed(rewardTx.ID)
			if err != nil {
				return err
			}
			continue
		}

		id, err := ids.FromString(rewardTx.Txid)
		if err != nil {
			panic(err)
		}
		var rewardsUtxos [][]byte
		arg := &api.GetTxArgs{TxID: id, Encoding: formatting.Hex}
		rewardsUtxos, err = r.client.GetRewardUTXOs(arg)
		if err != nil {
			return err
		}

		err = r.processRewardUtxos(rewardsUtxos)
		if err != nil {
			return err
		}

		err = r.markRewardProcessed(rewardTx.ID)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *RewardsHandler) processRewardUtxos(rewardsUtxos [][]byte) error {
	job := r.conns.StreamDBDedup().NewJob("rewards-handler-persist")
	sess := r.conns.DB().NewSessionForEventReceiver(job)

	dbTx, err := sess.Begin()
	if err != nil {
		return err
	}
	defer dbTx.RollbackUnlessCommitted()

	ctx := context.Background()
	tnow := time.Now()

	for _, reawrdUtxo := range rewardsUtxos {
		var utxo *avalancheGoAvax.UTXO
		_, err = platformvm.Codec.Unmarshal(reawrdUtxo, &utxo)
		if err != nil {
			return err
		}

		cCtx := services.NewConsumerContext(ctx, job, sess, tnow.Unix(), int64(tnow.Nanosecond()), r.perist)

		_, _, err = r.writer.ProcessStateOut(
			cCtx,
			utxo.Out,
			utxo.TxID,
			utxo.OutputIndex,
			utxo.AssetID(),
			0,
			0,
			r.cid.String(),
			false,
			false,
		)
		if err != nil {
			return err
		}
	}

	return dbTx.Commit()
}

func (r *RewardsHandler) markRewardProcessed(id string) error {
	job := r.conns.StreamDBDedup().NewJob("rewards-handler")
	sess := r.conns.DB().NewSessionForEventReceiver(job)

	ctx := context.Background()

	reward := &idb.Rewards{
		ID:        id,
		Processed: 1,
	}

	return r.perist.UpdateRewardsProcessed(ctx, sess, reward)
}
