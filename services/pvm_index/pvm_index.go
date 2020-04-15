// (c) 2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package pvm_index

import (
	"fmt"

	"github.com/ava-labs/gecko/genesis"
	"github.com/ava-labs/gecko/ids"
	"github.com/ava-labs/gecko/vms/platformvm"

	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/services"
)

const (
	AVANetworkID = 12345
)

type Index struct {
	chainID ids.ID
	// vm      *platformvm.VM
	db *DBIndex
	// cache   *RedisIndex
}

func New(conf cfg.ServiceConfig, chainID ids.ID) (*Index, error) {
	conns, err := services.NewConnectionsFromConfig(conf)
	if err != nil {
		return nil, err
	}
	return newForConnections(conns, chainID)
}

func newForConnections(conns *services.Connections, chainID ids.ID) (*Index, error) {
	// vm, err := newPVM(chainID, AVANetworkID)
	// if err != nil {
	// 	return nil, err
	// }

	return &Index{
		// vm:      vm,
		chainID: chainID,
		db:      NewDBIndex(conns.Stream(), conns.DB(), chainID, platformvm.Codec),
		// cache:   NewRedisIndex(conns.Redis(), chainID),
	}, nil
}

func (i *Index) Add(ingestable services.Ingestable) error {
	// Parse the serialized fields from bytes into a new block
	var block platformvm.Block
	if err := platformvm.Codec.Unmarshal(ingestable.Body(), &block); err != nil {
		return err
	}

	switch blk := block.(type) {
	case *platformvm.ProposalBlock:
		fmt.Println(blk.Tx)
	case *platformvm.Abort:
		fmt.Println(blk.CommonDecisionBlock.CommonBlock.ID())
	case *platformvm.Commit:
		fmt.Println(blk.CommonDecisionBlock.CommonBlock.ID())
	case *platformvm.StandardBlock:
		fmt.Println(blk.Txs)
	case *platformvm.AtomicBlock:
		fmt.Println(blk.Tx.InputUTXOs())
		// i.db.ingestAtomicBlock(blk)
	}

	return nil
}

func (i *Index) Bootstrap() error {
	pvmGenesisBytes, err := genesis.Genesis(AVANetworkID)
	if err != nil {
		panic(err)
		return err
	}

	pvmGenesis := &platformvm.Genesis{}
	if err := platformvm.Codec.Unmarshal(pvmGenesisBytes, pvmGenesis); err != nil {
		panic(err)
		return err
	}

	for _, acct := range pvmGenesis.Accounts {
		fmt.Println("acct addr:", acct.Address)
		fmt.Println("acct balance:", acct.Balance)
		fmt.Println("acct nonce:", acct.Nonce)
		// i.ingestAccount(acct)
	}

	for _, createChainTx := range pvmGenesis.Chains {
		err = i.db.ingestCreateChainTx(createChainTx)
		if err != nil {
			panic(err)
			return err
		}
	}

	for _, addValidatorTx := range pvmGenesis.Validators.Txs {
		dvTx, ok := addValidatorTx.(*platformvm.AddDefaultSubnetValidatorTx)
		if !ok {
			panic("asdf")
		}
		err = i.db.ingestAddDefaultSubnetValidatorTx(dvTx)
		if err != nil {
			panic(err)
			return err
		}
	}

	return nil
}

func (i *Index) GetTxs() (interface{}, error) {
	return nil, nil
}
func (i *Index) GetTx(id ids.ID) (interface{}, error) {
	return nil, nil
}
func (i *Index) GetTxsForAccount(id ids.ShortID) (interface{}, error) {
	return nil, nil
}

func (i *Index) GetChains() (interface{}, error) {
	return nil, nil
}
func (i *Index) GetChain(id ids.ID) (interface{}, error) {
	return nil, nil
}

func (i *Index) GetSubnets() (interface{}, error) {
	return nil, nil
}
func (i *Index) GetSubnet(id ids.ID) (interface{}, error) {
	return nil, nil
}

func (i *Index) GetValidatorsForSubnet(id ids.ID) (interface{}, error) {
	return nil, nil
}
func (i *Index) GetCurrentValidatorsForSubnet(id ids.ID) (interface{}, error) {
	return nil, nil
}
