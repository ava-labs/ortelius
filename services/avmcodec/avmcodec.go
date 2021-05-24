package avmcodec

import (
	"errors"
	"time"

	"github.com/ava-labs/avalanchego/codec"
	"github.com/ava-labs/avalanchego/database"
	"github.com/ava-labs/avalanchego/genesis"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/snow"
	"github.com/ava-labs/avalanchego/snow/engine/common"
	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/avalanchego/vms/avm"
	"github.com/ava-labs/avalanchego/vms/nftfx"
	"github.com/ava-labs/avalanchego/vms/platformvm"
	"github.com/ava-labs/avalanchego/vms/secp256k1fx"
	"github.com/ava-labs/ortelius/utils"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	ErrIncorrectGenesisChainTxType = errors.New("incorrect genesis chain tx type")
)

const MaxCodecSize = 100_000_000

func NewAVMCodec(networkID uint32, chainID string) (*avm.VM, *snow.Context, codec.Manager, database.Database, error) {
	genesisBytes, _, err := genesis.Genesis(networkID, "")
	if err != nil {
		return nil, nil, nil, nil, err
	}

	g, err := genesis.VMGenesis(genesisBytes, avm.ID)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	createChainTx, ok := g.UnsignedTx.(*platformvm.UnsignedCreateChainTx)
	if !ok {
		return nil, nil, nil, nil, ErrIncorrectGenesisChainTxType
	}

	bcLookup := &ids.Aliaser{}
	bcLookup.Initialize()
	id, err := ids.FromString(chainID)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	if err = bcLookup.Alias(id, "X"); err != nil {
		return nil, nil, nil, nil, err
	}

	var (
		fxIDs = createChainTx.FxIDs
		fxs   = make([]*common.Fx, 0, len(fxIDs))
		ctx   = &snow.Context{
			NetworkID:     networkID,
			ChainID:       id,
			Log:           logging.NoLog{},
			Metrics:       prometheus.NewRegistry(),
			BCLookup:      bcLookup,
			EpochDuration: time.Hour,
		}
	)
	for _, fxID := range fxIDs {
		switch {
		case fxID == secp256k1fx.ID:
			fxs = append(fxs, &common.Fx{
				Fx: &secp256k1fx.Fx{},
				ID: fxID,
			})
		case fxID == nftfx.ID:
			fxs = append(fxs, &common.Fx{
				Fx: &nftfx.Fx{},
				ID: fxID,
			})
		default:
			// return nil, fmt.Errorf("Unknown FxID: %s", fxID)
		}
	}

	db := &utils.NoopDatabase{}
	dbm := utils.NewNoopManager(db)

	// Initialize an producer to use for tx parsing
	// An error is returned about the DB being closed but this is expected because
	// we're not using a real DB here.
	vm := &avm.VM{}
	err = vm.Initialize(ctx, dbm, createChainTx.GenesisData, nil, nil, make(chan common.Message, 1), fxs)
	if err != nil && err != database.ErrClosed {
		return nil, nil, nil, nil, err
	}

	vm.Codec().SetMaxSize(MaxCodecSize)

	return vm, ctx, vm.Codec(), db, nil
}
