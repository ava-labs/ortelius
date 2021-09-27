package avax

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/ortelius/cfg"
	"github.com/ava-labs/ortelius/db"
	"github.com/ava-labs/ortelius/models"
	"github.com/ava-labs/ortelius/services/indexes/params"
	"github.com/gocraft/dbr/v2"
)

func (r *Reader) listTxsQuery(baseStmt *dbr.SelectStmt, p *params.ListTransactionsParams) *dbr.SelectStmt {
	builder := baseStmt

	if p.ListParams.ID != nil {
		builder.Where("avm_transactions.id = ?", p.ListParams.ID.String())
	}
	if p.ListParams.Query != "" {
		builder.Where(dbr.Like("avm_transactions.id", p.ListParams.Query+"%"))
	}
	if p.ListParams.StartTimeProvided && !p.ListParams.StartTime.IsZero() {
		builder.Where("avm_transactions.created_at >= ?", p.ListParams.StartTime)
	}
	if p.ListParams.EndTimeProvided && !p.ListParams.EndTime.IsZero() {
		builder.Where("avm_transactions.created_at < ?", p.ListParams.EndTime)
	}
	if len(p.ChainIDs) > 0 {
		builder.Where("avm_transactions.chain_id in ?", p.ChainIDs)
	}

	assetCheck := func(stmt *dbr.SelectStmt) {
		stmt.Where("avm_outputs.asset_id = ?", p.AssetID.String())
		if len(p.OutputOutputTypes) != 0 {
			stmt.Where("avm_outputs.output_type in ?", p.OutputOutputTypes)
		}
		if len(p.OutputGroupIDs) != 0 {
			stmt.Where("avm_outputs.group_id in ?", p.OutputGroupIDs)
		}
	}

	if len(p.Addresses) > 0 {
		addrs := make([]string, len(p.Addresses))
		for i, id := range p.Addresses {
			addrs[i] = id.String()
		}

		subquery := dbr.Select("avm_outputs.transaction_id").
			From("avm_outputs").
			LeftJoin("avm_output_addresses", "avm_outputs.id = avm_output_addresses.output_id")
		subqueryRedeem := dbr.Select("avm_outputs_redeeming.redeeming_transaction_id as transaction_id").
			From("avm_outputs_redeeming").
			LeftJoin("avm_output_addresses", "avm_outputs_redeeming.id = avm_output_addresses.output_id").
			Where("avm_outputs_redeeming.redeeming_transaction_id is not null")

		subquery = subquery.Where("avm_output_addresses.address IN ?", addrs)
		subqueryRedeem = subqueryRedeem.Where("avm_output_addresses.address IN ?", addrs)

		if p.AssetID != nil {
			assetCheck(subquery)
		}

		uq := dbr.Union(subquery, subqueryRedeem).As("union_q")
		// builder.Join(uq, "avm_transactions.id = union_q.transaction_id")
		builder.Where("avm_transactions.id in ?",
			dbr.Select("union_q.transaction_id").From(uq),
		)
	} else if p.AssetID != nil {
		builder.Join("avm_outputs", "avm_transactions.id = avm_outputs.transaction_id")
		assetCheck(builder)
	}

	return builder
}

func (r *Reader) listTxFromCache(p *params.ListTransactionsParams) *models.Transaction {
	if !r.sc.IsAggregateCache {
		return nil
	}
	if len(p.ListParams.Values) != 0 {
		return nil
	}
	if p.ListParams.ID == nil {
		return nil
	}

	if tx, ok := r.readerAggregate.txDesc.Get(models.StringID(p.ListParams.ID.String())); ok {
		return tx
	}
	if tx, ok := r.readerAggregate.txAsc.Get(models.StringID(p.ListParams.ID.String())); ok {
		return tx
	}
	return nil
}

func (r *Reader) listTxsFromCache(p *params.ListTransactionsParams) ([]*models.Transaction, bool) {
	if !r.sc.IsAggregateCache || p.ListParams.Limit == 0 || p.ListParams.Offset != 0 {
		return nil, false
	}

	readerAggregateTxList := &r.readerAggregate.txAsc
	if p.Sort == params.TransactionSortTimestampDesc {
		readerAggregateTxList = &r.readerAggregate.txDesc
	}

	if !readerAggregateTxList.IsProcessed() {
		return nil, false
	}

	// only allow certain values for this cache to hit..
	for key := range p.ListParams.Values {
		switch key {
		case params.KeySortBy:
		case params.KeyLimit:
		case params.KeyOffset:
		case params.KeyDisableCount:
		case params.KeyChainID:
		default:
			// unknown key, no cache hit
			return nil, false
		}
	}

	txs := readerAggregateTxList.FindTxs(p.ChainIDs, p.ListParams.Limit)
	if txs != nil {
		return txs, true
	}
	return nil, false
}

func (r *Reader) listTxs(
	ctx context.Context,
	p *params.ListTransactionsParams,
	dbRunner *dbr.Session,
) ([]*models.Transaction, bool, error) {
	applySort := func(sort params.TransactionSort, stmt *dbr.SelectStmt) *dbr.SelectStmt {
		if p.ListParams.Query != "" {
			return stmt
		}

		switch sort {
		case params.TransactionSortTimestampDesc:
			stmt.OrderDesc("avm_transactions.created_at")
			stmt.OrderDesc("avm_transactions.chain_id")
		default:
			// default is ascending...
			stmt.OrderAsc("avm_transactions.created_at")
			stmt.OrderAsc("avm_transactions.chain_id")
		}
		return stmt
	}

	if p.ListParams.ID != nil {
		tx := r.listTxFromCache(p)
		if tx != nil {
			return []*models.Transaction{tx}, true, nil
		}
	}

	if len(p.Addresses) > 0 || (p.AssetID == nil && len(p.Addresses) == 0) {
		txsAggr, dressed := r.listTxsFromCache(p)
		if txsAggr != nil {
			return txsAggr, dressed, nil
		}

		builderBase := applySort(
			p.Sort,
			r.listTxsQuery(dbRunner.Select("avm_transactions.id").From("avm_transactions"), p),
		)
		if p.ListParams.Limit != 0 {
			builderBase.Limit(uint64(p.ListParams.Limit))
		}
		if p.ListParams.Offset != 0 {
			builderBase.Offset(uint64(p.ListParams.Offset))
		}

		builder := applySort(
			p.Sort,
			transactionQuery(dbRunner).
				Join(builderBase.As("avm_transactions_id"), "avm_transactions.id = avm_transactions_id.id"),
		)

		var txs []*models.Transaction
		if _, err := builder.LoadContext(ctx, &txs); err != nil {
			return nil, false, err
		}

		return txs, false, nil
	}

	builder := applySort(
		p.Sort,
		r.listTxsQuery(transactionQuery(dbRunner), p),
	).Limit(100000)

	txs := make([]*models.Transaction, 0, params.PaginationMaxLimit+1)
	txsm := make(map[models.StringID]struct{})
	txsmoffset := make(map[models.StringID]struct{})

	itr, err := builder.IterateContext(ctx)
	if err != nil {
		return nil, false, err
	}

	defer func() {
		_ = itr.Close()
	}()

	for itr.Next() {
		err = itr.Err()
		if err != nil {
			return nil, false, err
		}
		var tx *models.Transaction
		err = itr.Scan(&tx)
		if err != nil {
			return nil, false, err
		}
		if p.ListParams.Offset > 0 && len(txsmoffset) < p.ListParams.Offset {
			txsmoffset[tx.ID] = struct{}{}
			continue
		}
		if _, ok := txsmoffset[tx.ID]; ok {
			continue
		}
		if _, ok := txsm[tx.ID]; !ok {
			txs = append(txs, tx)
			txsm[tx.ID] = struct{}{}
		}
		if p.ListParams.Limit > 0 && len(txsm) >= p.ListParams.Limit {
			break
		}
	}

	return txs, false, nil
}

func (r *Reader) ListTransactions(ctx context.Context, p *params.ListTransactionsParams, avaxAssetID ids.ID) (*models.TransactionList, error) {
	dbRunner, err := r.conns.DB().NewSession("get_transactions", cfg.RequestTimeout)
	if err != nil {
		return nil, err
	}

	txs, dressed, err := r.listTxs(ctx, p, dbRunner)
	if err != nil {
		return nil, err
	}

	if !dressed {
		if err := dressTransactions(ctx, dbRunner, txs, avaxAssetID, p.ListParams.ID, p.DisableGenesis); err != nil {
			return nil, err
		}
	}

	listParamsOriginal := p.ListParams

	var count *uint64
	if !p.ListParams.DisableCounting {
		count = uint64Ptr(uint64(p.ListParams.Offset) + uint64(len(txs)))
		if len(txs) >= p.ListParams.Limit {
			count = uint64Ptr(uint64(p.ListParams.Offset) + uint64(len(txs)) + 1)
		}
	}

	next := r.transactionProcessNext(txs, listParamsOriginal, p)

	return &models.TransactionList{ListMetadata: models.ListMetadata{
		Count: count,
	},
		Transactions: txs,
		StartTime:    listParamsOriginal.StartTime,
		EndTime:      listParamsOriginal.EndTime,
		Next:         next,
	}, nil
}

func (r *Reader) transactionProcessNext(txs []*models.Transaction, listParams params.ListParams, transactionsParams *params.ListTransactionsParams) *string {
	if len(txs) == 0 || len(txs) < listParams.Limit {
		return nil
	}

	lasttxCreated := txs[len(txs)-1].CreatedAt

	next := ""
	switch transactionsParams.Sort {
	case params.TransactionSortTimestampAsc:
		next = fmt.Sprintf("%s=%d", params.KeyStartTime, lasttxCreated.Add(time.Second).Unix())
	case params.TransactionSortTimestampDesc:
		next = fmt.Sprintf("%s=%d", params.KeyEndTime, lasttxCreated.Unix())
	}

	for k, vs := range listParams.Values {
		switch k {
		case params.KeyLimit:
			next = fmt.Sprintf("%s&%s=%d", next, params.KeyLimit, transactionsParams.ListParams.Limit)
		case params.KeyStartTime:
			if transactionsParams.Sort == params.TransactionSortTimestampDesc {
				for _, v := range vs {
					next = fmt.Sprintf("%s&%s=%s", next, params.KeyStartTime, v)
				}
			}
		case params.KeyEndTime:
			if transactionsParams.Sort == params.TransactionSortTimestampAsc {
				for _, v := range vs {
					next = fmt.Sprintf("%s&%s=%s", next, params.KeyEndTime, v)
				}
			}
		case params.KeyOffset:
		case params.KeySortBy:
		default:
			for _, v := range vs {
				next = fmt.Sprintf("%s&%s=%s", next, k, v)
			}
		}
	}
	next = fmt.Sprintf("%s&%s=%s", next, params.KeySortBy, transactionsParams.Sort.String())
	return &next
}

// Load output data for all inputs and outputs into a single list
// We can't treat them separately because some my be both inputs and outputs
// for different transactions
type compositeRecord struct {
	models.Output
	models.OutputAddress
}

type rewardsTypeModel struct {
	Txid      models.StringID  `json:"txid"`
	Type      models.BlockType `json:"type"`
	CreatedAt time.Time        `json:"created_at"`
}

func newPvmProposerModel(pvmProposer db.PvmProposer) *models.PvmProposerModel {
	return &models.PvmProposerModel{
		ID:           models.StringID(pvmProposer.ID),
		ParentID:     models.StringID(pvmProposer.ParentID),
		PChainHeight: pvmProposer.PChainHeight,
		Proposer:     models.StringID(pvmProposer.Proposer),
		TimeStamp:    pvmProposer.TimeStamp,
	}
}

func dressTransactions(
	ctx context.Context,
	dbRunner dbr.SessionRunner,
	txs []*models.Transaction,
	avaxAssetID ids.ID,
	txID *ids.ID,
	disableGenesis bool,
) error {
	if len(txs) == 0 {
		return nil
	}

	// Get the IDs returned so we can get Input/Output data
	txIDs := make([]models.StringID, len(txs))
	blockTxIds := make([]models.StringID, 0, len(txs))
	blockTxIdsFound := make(map[models.StringID]struct{})
	for i, tx := range txs {
		if txs[i].Memo == nil {
			txs[i].Memo = []byte("")
		}
		txIDs[i] = tx.ID
		if len(tx.TxBlockID) != 0 {
			if _, ok := blockTxIdsFound[tx.TxBlockID]; !ok {
				blockTxIds = append(blockTxIds, tx.TxBlockID)
				blockTxIdsFound[tx.TxBlockID] = struct{}{}
			}
		}
	}

	proposersMap, err := resolveProposers(ctx, dbRunner, blockTxIds)
	if err != nil {
		return err
	}
	for _, tx := range txs {
		if proposer, ok := proposersMap[tx.TxBlockID]; ok {
			tx.Proposer = proposer
		}
	}

	rewardsTypesMap, err := resolveRewarded(ctx, dbRunner, txIDs)
	if err != nil {
		return err
	}

	outputs, err := collectInsAndOuts(ctx, dbRunner, txIDs)
	if err != nil {
		return err
	}

	// Create a map of addresses for each output and maps of transaction ids to
	// inputs, outputs, and the total amounts of the inputs and outputs
	var (
		outputAddrs     = make(map[models.StringID]map[models.Address]struct{}, len(txs)*2)
		inputsMap       = make(map[models.StringID]map[models.StringID]*models.Input, len(txs))
		outputsMap      = make(map[models.StringID]map[models.StringID]*models.Output, len(txs))
		inputTotalsMap  = make(map[models.StringID]map[models.StringID]*big.Int, len(txs))
		outputTotalsMap = make(map[models.StringID]map[models.StringID]*big.Int, len(txs))
	)

	// Create a helper to safely add big integers
	addToBigIntMap := func(m map[models.StringID]*big.Int, assetID models.StringID, amt *big.Int) {
		prevAmt := m[assetID]
		if prevAmt == nil {
			prevAmt = big.NewInt(0)
		}
		m[assetID] = prevAmt.Add(amt, prevAmt)
	}

	// Collect outpoints into the maps
	for _, output := range outputs {
		out := &output.Output

		bigAmt := new(big.Int)
		if _, ok := bigAmt.SetString(string(out.Amount), 10); !ok {
			return errors.New("invalid amount")
		}

		if _, ok := inputsMap[out.RedeemingTransactionID]; !ok {
			inputsMap[out.RedeemingTransactionID] = map[models.StringID]*models.Input{}
		}
		if _, ok := inputTotalsMap[out.RedeemingTransactionID]; !ok {
			inputTotalsMap[out.RedeemingTransactionID] = map[models.StringID]*big.Int{}
		}
		if _, ok := outputsMap[out.TransactionID]; !ok {
			outputsMap[out.TransactionID] = map[models.StringID]*models.Output{}
		}
		if _, ok := outputTotalsMap[out.TransactionID]; !ok {
			outputTotalsMap[out.TransactionID] = map[models.StringID]*big.Int{}
		}
		if _, ok := outputAddrs[out.ID]; !ok {
			outputAddrs[out.ID] = map[models.Address]struct{}{}
		}

		outputAddrs[out.ID][output.OutputAddress.Address] = struct{}{}
		outputsMap[out.TransactionID][out.ID] = out
		inputsMap[out.RedeemingTransactionID][out.ID] = &models.Input{Output: out}
		addToBigIntMap(outputTotalsMap[out.TransactionID], out.AssetID, bigAmt)
		addToBigIntMap(inputTotalsMap[out.RedeemingTransactionID], out.AssetID, bigAmt)
	}

	// Collect the addresses into a list on each outpoint
	var input *models.Input
	for _, out := range outputs {
		out.Addresses = make([]models.Address, 0, len(outputAddrs[out.ID]))
		for addr := range outputAddrs[out.ID] {
			// mock in records have a blank address.  Drop them.
			if len(addr) == 0 {
				continue
			}
			out.Addresses = append(out.Addresses, addr)
		}

		// If this Address didn't sign any txs then we're done
		if len(out.Signature) == 0 {
			continue
		}

		// Get the Input and add the credentials for this Address
		for _, input = range inputsMap[out.RedeemingTransactionID] {
			if input.Output.ID.Equals(out.OutputID) {
				input.Creds = append(input.Creds, models.InputCredentials{
					Address:   out.Address,
					PublicKey: out.PublicKey,
					Signature: out.Signature,
				})
				break
			}
		}
	}

	cvmin, cvmout, err := collectCvmTransactions(ctx, dbRunner, txIDs)
	if err != nil {
		return err
	}

	dressTransactionsTx(txs, disableGenesis, txID, avaxAssetID, inputsMap, outputsMap, inputTotalsMap, outputTotalsMap, rewardsTypesMap, cvmin, cvmout)
	return nil
}

func dressTransactionsTx(
	txs []*models.Transaction,
	disableGenesis bool,
	txID *ids.ID,
	avaxAssetID ids.ID,
	inputsMap map[models.StringID]map[models.StringID]*models.Input,
	outputsMap map[models.StringID]map[models.StringID]*models.Output,
	inputTotalsMap map[models.StringID]map[models.StringID]*big.Int,
	outputTotalsMap map[models.StringID]map[models.StringID]*big.Int,
	rewardsTypesMap map[models.StringID]rewardsTypeModel,
	cvmins map[models.StringID][]models.Output,
	cvmouts map[models.StringID][]models.Output,
) {
	// Add the data we've built up for each transaction
	for _, tx := range txs {
		if disableGenesis && (txID == nil && string(tx.ID) == avaxAssetID.String()) {
			continue
		}
		if inputs, ok := inputsMap[tx.ID]; ok {
			for _, input := range inputs {
				tx.Inputs = append(tx.Inputs, input)
			}
		}
		if inputs, ok := cvmins[tx.ID]; ok {
			for _, input := range inputs {
				var i models.Input
				var o = input
				i.Output = &o
				tx.Inputs = append(tx.Inputs, &i)
			}
		}

		if outputs, ok := outputsMap[tx.ID]; ok {
			for _, output := range outputs {
				tx.Outputs = append(tx.Outputs, output)
			}
		}
		if outputs, ok := cvmouts[tx.ID]; ok {
			for _, output := range outputs {
				var o = output
				tx.Outputs = append(tx.Outputs, &o)
			}
		}

		tx.InputTotals = make(models.AssetTokenCounts, len(inputTotalsMap[tx.ID]))
		for k, v := range inputTotalsMap[tx.ID] {
			tx.InputTotals[k] = models.TokenAmount(v.String())
		}

		tx.OutputTotals = make(models.AssetTokenCounts, len(outputTotalsMap[tx.ID]))
		for k, v := range outputTotalsMap[tx.ID] {
			tx.OutputTotals[k] = models.TokenAmount(v.String())
		}

		if rewardsType, ok := rewardsTypesMap[tx.ID]; ok {
			tx.Rewarded = rewardsType.Type == models.BlockTypeCommit
			tx.RewardedTime = &rewardsType.CreatedAt
		}
	}
}

func resolveProposers(ctx context.Context, dbRunner dbr.SessionRunner, properIds []models.StringID) (map[models.StringID]*models.PvmProposerModel, error) {
	pvmProposerModels := make(map[models.StringID]*models.PvmProposerModel)
	if len(properIds) == 0 {
		return pvmProposerModels, nil
	}
	pvmProposers := []db.PvmProposer{}
	_, err := dbRunner.Select("id",
		"parent_id",
		"blk_id",
		"p_chain_height",
		"proposer",
		"time_stamp",
		"created_at",
	).From(db.TablePvmProposer).
		Where("blk_id in ?", properIds).
		LoadContext(ctx, &pvmProposers)
	if err != nil {
		return nil, err
	}

	for _, pvmProposer := range pvmProposers {
		pvmProposerModels[models.StringID(pvmProposer.BlkID)] = newPvmProposerModel(pvmProposer)
	}
	return pvmProposerModels, nil
}

func resolveRewarded(ctx context.Context, dbRunner dbr.SessionRunner, txIDs []models.StringID) (map[models.StringID]rewardsTypeModel, error) {
	rewardsTypes := []rewardsTypeModel{}
	blocktypes := []models.BlockType{models.BlockTypeAbort, models.BlockTypeCommit}
	_, err := dbRunner.Select("rewards.txid",
		"pvm_blocks.type",
		"pvm_blocks.created_at",
	).
		From("rewards").
		LeftJoin("pvm_blocks", "rewards.block_id = pvm_blocks.parent_id").
		Where("rewards.txid IN ? and pvm_blocks.type IN ?", txIDs, blocktypes).
		LoadContext(ctx, &rewardsTypes)
	if err != nil {
		return nil, err
	}

	rewardsTypesMap := make(map[models.StringID]rewardsTypeModel)
	for _, rewardsType := range rewardsTypes {
		rewardsTypesMap[rewardsType.Txid] = rewardsType
	}
	return rewardsTypesMap, nil
}

func collectInsAndOuts(ctx context.Context, dbRunner dbr.SessionRunner, txIDs []models.StringID) ([]*compositeRecord, error) {
	s1_0 := dbRunner.Select("avm_outputs.id").
		From("avm_outputs").
		Where("avm_outputs.transaction_id IN ?", txIDs)

	s1_1 := dbRunner.Select("avm_outputs_redeeming.id").
		From("avm_outputs_redeeming").
		Where("avm_outputs_redeeming.redeeming_transaction_id IN ?", txIDs)

	var outputs []*compositeRecord
	_, err := selectOutputs(dbRunner, false).
		Join(dbr.Union(s1_0, s1_1).As("union_q_x"), "union_q_x.id = avm_outputs.id").LoadContext(ctx, &outputs)
	if err != nil {
		return nil, err
	}

	s2 := dbRunner.Select("avm_outputs.id").
		From("avm_outputs_redeeming").
		Join("avm_outputs", "avm_outputs.id = avm_outputs_redeeming.id").
		Where("avm_outputs_redeeming.redeeming_transaction_id IN ?", txIDs)

	// if we get an input but have not yet seen the output.
	var outputs2 []*compositeRecord
	_, err = selectOutputs(dbRunner, true).
		Where("avm_outputs_redeeming.redeeming_transaction_id IN ? and avm_outputs_redeeming.id not in ?",
			txIDs, dbr.Select("sq_s2.id").From(s2.As("sq_s2"))).LoadContext(ctx, &outputs2)
	if err != nil {
		return nil, err
	}

	return append(outputs, outputs2...), nil
}

func collectCvmTransactions(ctx context.Context, dbRunner dbr.SessionRunner, txIDs []models.StringID) (map[models.StringID][]models.Output, map[models.StringID][]models.Output, error) {
	var cvmAddress []models.CvmOutput
	_, err := dbRunner.Select(
		"cvm_addresses.type",
		"cvm_transactions.type as transaction_type",
		"cvm_addresses.idx",
		"cast(cvm_addresses.amount as char) as amount",
		"cvm_addresses.nonce",
		"cvm_addresses.id",
		"cvm_addresses.transaction_id",
		"cvm_addresses.address",
		"cvm_addresses.asset_id",
		"cvm_addresses.created_at",
		"cvm_transactions.blockchain_id as chain_id",
		"cvm_transactions.block",
	).
		From("cvm_addresses").
		Join("cvm_transactions", "cvm_addresses.transaction_id=cvm_transactions.transaction_id").
		Where("cvm_addresses.transaction_id IN ?", txIDs).
		LoadContext(ctx, &cvmAddress)
	if err != nil {
		return nil, nil, err
	}

	ins := make(map[models.StringID][]models.Output)
	outs := make(map[models.StringID][]models.Output)

	for _, a := range cvmAddress {
		if _, ok := ins[a.TransactionID]; !ok {
			ins[a.TransactionID] = make([]models.Output, 0)
		}
		if _, ok := outs[a.TransactionID]; !ok {
			outs[a.TransactionID] = make([]models.Output, 0)
		}
		switch a.Type {
		case models.CChainIn:
			ins[a.TransactionID] = append(ins[a.TransactionID], mapOutput(a))
		case models.CchainOut:
			outs[a.TransactionID] = append(outs[a.TransactionID], mapOutput(a))
		}
	}

	return ins, outs, nil
}

func mapOutput(a models.CvmOutput) models.Output {
	var o models.Output
	o.TransactionID = a.TransactionID
	o.ID = a.ID
	o.OutputIndex = a.Idx
	o.AssetID = a.AssetID
	o.Amount = a.Amount
	o.ChainID = a.ChainID
	o.CreatedAt = a.CreatedAt
	switch a.TransactionType {
	case models.CChainExport:
		o.OutputType = models.OutputTypesAtomicExportTx
	case models.CChainImport:
		o.OutputType = models.OutputTypesAtomicImportTx
	}
	o.CAddresses = []string{a.Address}
	o.Nonce = a.Nonce
	o.Block = a.Block
	return o
}

func selectOutputs(dbRunner dbr.SessionRunner, redeem bool) *dbr.SelectBuilder {
	tbl := "avm_outputs"
	if redeem {
		tbl = "avm_outputs_redeeming"
	}
	cols := make([]string, 0, 50)
	cols = append(cols, tbl+".id")
	if !redeem {
		cols = append(cols, "avm_outputs.transaction_id")
	} else {
		cols = append(cols, "avm_outputs_redeeming.intx as transaction_id")
	}
	cols = append(cols, tbl+".output_index")
	cols = append(cols, tbl+".asset_id")
	cols = append(cols, "case when avm_outputs.output_type is null then 0 else avm_outputs.output_type end as output_type")
	cols = append(cols, tbl+".amount")
	cols = append(cols, "case when avm_outputs.locktime is null then 0 else avm_outputs.locktime end as locktime")
	cols = append(cols, "case when avm_outputs.stake_locktime is null then 0 else avm_outputs.stake_locktime end as stake_locktime")
	cols = append(cols, "case when avm_outputs.threshold is null then 0 else avm_outputs.threshold end as threshold")
	cols = append(cols, tbl+".created_at")
	cols = append(cols, "case when avm_outputs_redeeming.redeeming_transaction_id IS NULL then '' else avm_outputs_redeeming.redeeming_transaction_id end as redeeming_transaction_id")
	cols = append(cols, "case when avm_outputs.group_id is null then 0 else avm_outputs.group_id end as group_id")
	cols = append(cols, "case when avm_output_addresses.output_id is null then '' else avm_output_addresses.output_id end AS output_id")
	cols = append(cols, "case when avm_output_addresses.address is null then '' else avm_output_addresses.address end AS address")
	cols = append(cols, "avm_output_addresses.redeeming_signature AS signature")
	cols = append(cols, "addresses.public_key AS public_key")
	cols = append(cols, tbl+".chain_id")
	cols = append(cols, "case when avm_outputs.chain_id is null then '' else avm_outputs.chain_id end as out_chain_id")
	cols = append(cols, "case when avm_outputs_redeeming.chain_id is null then '' else avm_outputs_redeeming.chain_id end as in_chain_id")
	cols = append(cols, "case when avm_outputs.payload is null then '' else avm_outputs.payload end as payload")
	cols = append(cols, "case when avm_outputs.stake is null then 0 else avm_outputs.stake end as stake")
	cols = append(cols, "case when avm_outputs.stakeableout is null then 0 else avm_outputs.stakeableout end as stakeableout")
	cols = append(cols, "case when avm_outputs.genesisutxo is null then 0 else avm_outputs.genesisutxo end as genesisutxo")
	cols = append(cols, "case when avm_outputs.frozen is null then 0 else avm_outputs.frozen end as frozen")
	cols = append(cols, "case when transactions_rewards_owners_outputs.id is null then false else true end as reward_utxo")

	sq := dbRunner.Select(cols...).From(tbl)

	if !redeem {
		sq = sq.LeftJoin("avm_outputs_redeeming", "avm_outputs.id = avm_outputs_redeeming.id")
	} else {
		sq = sq.LeftJoin("avm_outputs", "avm_outputs_redeeming.id = avm_outputs.id")
	}

	return sq.
		LeftJoin("avm_output_addresses", tbl+".id = avm_output_addresses.output_id").
		LeftJoin("transactions_rewards_owners_outputs", tbl+".id = transactions_rewards_owners_outputs.id").
		LeftJoin("addresses", "addresses.address = avm_output_addresses.address")
}
