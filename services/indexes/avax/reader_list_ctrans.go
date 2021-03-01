package avax

import (
	"context"
	"encoding/json"
	"math/big"
	"strings"

	"github.com/ava-labs/coreth/core/types"
	"github.com/ava-labs/ortelius/cfg"
	cblock "github.com/ava-labs/ortelius/models"
	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/indexes/models"
	"github.com/ava-labs/ortelius/services/indexes/params"
	"github.com/gocraft/dbr/v2"
)

func (r *Reader) ListCTransactions(ctx context.Context, p *params.ListCTransactionsParams) (*models.CTransactionList, error) {
	toCTransactionData := func(t *types.Transaction) *models.CTransactionData {
		res := &models.CTransactionData{}
		res.Hash = t.Hash().Hex()
		if !strings.HasPrefix(res.Hash, "0x") {
			res.Hash = "0x" + res.Hash
		}
		res.Nonce = t.Nonce()
		if t.GasPrice() != nil {
			str := t.GasPrice().String()
			res.GasPrice = &str
		}
		res.GasLimit = t.Gas()
		if t.To() != nil {
			str := t.To().Hex()
			if !strings.HasPrefix(str, "0x") {
				str = "0x" + str
			}
			res.Recipient = &str
		}
		if t.Value() != nil {
			str := t.Value().String()
			res.Amount = &str
		}
		res.Payload = t.Data()
		v, s, r := t.RawSignatureValues()
		if v != nil {
			str := v.String()
			res.V = &str
		}
		if s != nil {
			str := s.String()
			res.S = &str
		}
		if r != nil {
			str := r.String()
			res.R = &str
		}
		return res
	}

	dbRunner, err := r.conns.DB().NewSession("list_ctransactions", cfg.RequestTimeout)
	if err != nil {
		return nil, err
	}

	var dataList []*services.CvmTransactionsTxdata

	sq := dbRunner.Select(
		"hash",
		"block",
		"idx",
		"rcpt",
		"nonce",
		"serialization",
		"created_at",
	).From(services.TableCvmTransactionsTxdata)

	if len(p.CAddresses) > 0 {
		subq := dbRunner.Select("hash").From(services.TableCvmTransactionsTxdataTrace).
			Where("to_addr in ? or from_addr in ?", p.CAddresses, p.CAddresses)
		sq.
			Where("rcpt in ? or hash in ?", p.CAddresses, dbRunner.Select("hash").From(subq.As("a")))
	}
	if len(p.Hashes) > 0 {
		sq.
			Where("hash in ?", p.Hashes)
	}

	_, err = p.Apply(sq).
		OrderDesc("created_at").
		LoadContext(ctx, &dataList)
	if err != nil {
		return nil, err
	}

	trItemsByHash := make(map[string]*models.CTransactionData)

	trItems := make([]*models.CTransactionData, 0, len(dataList))
	hashes := make([]string, 0, len(dataList))

	blocksMap := make(map[string]struct{})
	blocks := make([]string, 0, len(dataList))

	for _, txdata := range dataList {
		var tr types.Transaction
		err := tr.UnmarshalJSON(txdata.Serialization)
		if err != nil {
			return nil, err
		}
		ctr := toCTransactionData(&tr)
		ctr.Block = txdata.Block
		ctr.CreatedAt = txdata.CreatedAt
		trItems = append(trItems, ctr)

		trItemsByHash[ctr.Hash] = ctr
		hashes = append(hashes, ctr.Hash)

		if _, ok := blocksMap[txdata.Block]; !ok {
			blocksMap[txdata.Block] = struct{}{}
			blocks = append(blocks, txdata.Block)
		}
	}

	cblocksMap, err := r.fetchAndDecodeCBlocks(ctx, dbRunner, blocks)
	if err != nil {
		return nil, err
	}

	err = r.handleDressTraces(ctx, dbRunner, hashes, trItemsByHash)
	if err != nil {
		return nil, err
	}

	for _, trItem := range trItemsByHash {
		if cblock, ok := cblocksMap[trItem.Block]; ok {
			trItem.BlockGasUsed = cblock.Header.GasUsed
			trItem.BlockGasLimit = cblock.Header.GasLimit
			trItem.BlockNonce = cblock.Header.Nonce.Uint64()
			trItem.BlockHash = cblock.Header.Hash().String()
		}
		if trItem.TracesMax != 0 {
			trItem.Traces = make([]*models.CvmTransactionsTxDataTrace, trItem.TracesMax)
			for k, v := range trItem.TracesMap {
				v.Idx = nil
				trItem.Traces[k] = v
			}
		}
	}

	listParamsOriginal := p.ListParams

	return &models.CTransactionList{
		Transactions: trItems,
		StartTime:    listParamsOriginal.StartTime,
		EndTime:      listParamsOriginal.EndTime,
	}, nil
}

func (r *Reader) handleDressTraces(ctx context.Context, dbRunner *dbr.Session, hashes []string, trItemsByHash map[string]*models.CTransactionData) error {
	if len(hashes) == 0 {
		return nil
	}
	var err error
	var txTransactionTraceServices []*services.CvmTransactionsTxdataTrace
	_, err = dbRunner.Select(
		"hash",
		"idx",
		"to_addr",
		"from_addr",
		"call_type",
		"type",
		"serialization",
		"created_at",
	).From(services.TableCvmTransactionsTxdataTrace).
		Where("hash in ?", hashes).
		LoadContext(ctx, &txTransactionTraceServices)
	if err != nil {
		return err
	}

	for _, txTransactionTraceService := range txTransactionTraceServices {
		txTransactionTraceModel := &models.CvmTransactionsTxDataTrace{}
		err = json.Unmarshal(txTransactionTraceService.Serialization, txTransactionTraceModel)
		if err != nil {
			return err
		}
		if txTransactionTraceService.Idx == 0 {
			trItemsByHash[txTransactionTraceService.Hash].ToAddr = txTransactionTraceModel.ToAddr
			trItemsByHash[txTransactionTraceService.Hash].FromAddr = txTransactionTraceModel.FromAddr
		}

		toDecimal := func(v *string) {
			vh := strings.TrimPrefix(*v, "0x")
			vInt, okVInt := big.NewInt(0).SetString(vh, 16)
			if okVInt && vInt != nil {
				*v = vInt.String()
			}
		}
		toDecimal(&txTransactionTraceModel.Value)
		toDecimal(&txTransactionTraceModel.Gas)
		toDecimal(&txTransactionTraceModel.GasUsed)

		nilEmpty := func(v *string, def string) *string {
			if v != nil && *v == def {
				return nil
			}
			return v
		}
		txTransactionTraceModel.CreatedContractAddressHash = nilEmpty(txTransactionTraceModel.CreatedContractAddressHash, "")
		txTransactionTraceModel.Init = nilEmpty(txTransactionTraceModel.Init, "")
		txTransactionTraceModel.CreatedContractCode = nilEmpty(txTransactionTraceModel.CreatedContractCode, "")
		txTransactionTraceModel.Error = nilEmpty(txTransactionTraceModel.Error, "")
		txTransactionTraceModel.Input = nilEmpty(txTransactionTraceModel.Input, "0x")
		txTransactionTraceModel.Output = nilEmpty(txTransactionTraceModel.Output, "0x")

		if trItemsByHash[txTransactionTraceService.Hash].TracesMap == nil {
			trItemsByHash[txTransactionTraceService.Hash].TracesMap = make(map[uint32]*models.CvmTransactionsTxDataTrace)
		}
		if txTransactionTraceService.Idx+1 > trItemsByHash[txTransactionTraceService.Hash].TracesMax {
			trItemsByHash[txTransactionTraceService.Hash].TracesMax = txTransactionTraceService.Idx + 1
		}
		trItemsByHash[txTransactionTraceService.Hash].TracesMap[txTransactionTraceService.Idx] = txTransactionTraceModel
	}

	return nil
}

func (r *Reader) fetchAndDecodeCBlocks(ctx context.Context, dbRunner *dbr.Session, blocks []string) (map[string]*cblock.Block, error) {
	var err error
	cblocksMap := make(map[string]*cblock.Block)

	if len(blocks) > 0 {
		var cvmTxs []*services.CvmTransactions
		_, err = dbRunner.Select(
			"cast(block as char) as block",
			"serialization",
		).From(services.TableCvmTransactions).
			Where("block in ("+strings.Join(blocks, ",")+")").
			LoadContext(ctx, &cvmTxs)
		if err != nil {
			return nil, err
		}

		for _, cvmTx := range cvmTxs {
			cblock, err := cblock.Unmarshal(cvmTx.Serialization)
			if err != nil {
				return nil, err
			}
			cblocksMap[cvmTx.Block] = cblock
		}
	}

	return cblocksMap, nil
}
