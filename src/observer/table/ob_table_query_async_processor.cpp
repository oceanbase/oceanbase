/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER
#include "ob_table_query_async_processor.h"
#include "src/observer/table/ob_table_cache.h"
#include "ob_htable_filter_operator.h"
#include "observer/table/common/ob_table_query_session_mgr.h"
#include "observer/table/models/ob_model_factory.h"

using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::share;
using namespace oceanbase::sql;
using namespace oceanbase::sql::stmt;

/**
 * -------------------------------------- ObTableQueryAsyncP ----------------------------------------
 */
ObTableQueryAsyncP::ObTableQueryAsyncP(const ObGlobalContext &gctx)
    : ObTableRpcProcessor(gctx),
      result_row_count_(0),
      query_session_id_(0),
      query_session_(nullptr),
      is_full_table_scan_(false)
{
  allocator_.set_attr(ObMemAttr(MTL_ID(), "TbQueryASyncP", ObCtxIds::DEFAULT_CTX_ID));
}

int ObTableQueryAsyncP::deserialize()
{
  arg_.query_.set_deserialize_allocator(&allocator_);
  return ParentType::deserialize();
}

int ObTableQueryAsyncP::check_arg()
{
  int ret = OB_SUCCESS;
  if (arg_.query_type_ == ObQueryOperationType::QUERY_START && !arg_.query_.is_valid() &&
      arg_.query_.get_htable_filter().is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table query request", K(ret), "query", arg_.query_);
  } else if (!(arg_.consistency_level_ == ObTableConsistencyLevel::STRONG ||
                 arg_.consistency_level_ == ObTableConsistencyLevel::EVENTUAL)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "consistency level");
    LOG_WARN("some options not supported yet", K(ret), "consistency_level", arg_.consistency_level_);
  }
  return ret;
}

uint64_t ObTableQueryAsyncP::get_request_checksum()
{
  uint64_t checksum = 0;
  checksum = ob_crc64(checksum, arg_.table_name_.ptr(), arg_.table_name_.length());
  checksum = ob_crc64(checksum, &arg_.consistency_level_, sizeof(arg_.consistency_level_));
  const uint64_t op_checksum = arg_.query_.get_checksum();
  checksum = ob_crc64(checksum, &op_checksum, sizeof(op_checksum));
  return checksum;
}

void ObTableQueryAsyncP::reset_ctx()
{
  result_row_count_ = 0;
  query_session_ = nullptr;
  ObTableApiProcessorBase::reset_ctx();
  result_.reset();
}

int ObTableQueryAsyncP::get_old_session(uint64_t sessid, ObTableQueryAsyncSession *&query_session)
{
  int ret = OB_SUCCESS;
  ObITableQueryAsyncSession *i_query_session = nullptr;
  if (OB_FAIL(MTL(ObTableQueryASyncMgr*)->get_query_session<ObTableQueryAsyncSession>(sessid, arg_.query_type_, i_query_session))) {
    LOG_WARN("fail to get query session", K(ret), K(sessid));
  } else if (OB_ISNULL(query_session = dynamic_cast<ObTableQueryAsyncSession *>(i_query_session))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to cast ObTableQueryAsyncSession", KP(i_query_session));
  } else {
    // hook processor's trans_desc_
    trans_param_.trans_desc_ = query_session->get_trans_desc();
    // use session trans_state_ which storage transaction state in query_next()
    trans_param_.trans_state_ptr_ = query_session->get_trans_state();
    query_session->set_session_type(arg_.entity_type_);
  }

  return ret;
}

int64_t ObTableQueryAsyncP::get_trans_timeout_ts()
{
  if (arg_.entity_type_ == ObTableEntityType::ET_HKV) {
    return INT_MAX64;
  }
  return get_timeout_ts();
}

int ObTableQueryAsyncP::init_tb_ctx(ObIAllocator* allocator,
                        ObTableConsistencyLevel consistency_level,
                        const table::ObTableEntityType &entity_type,
                        schema::ObSchemaGetterGuard& schema_guard,
                        table::ObTableApiCredential &credential,
                        bool tablegroup_req,
                        ObTableQueryAsyncCtx &query_ctx,
                        ObTableSingleQueryInfo& query_info,
                        const int64_t &timeout_ts,
                        table::ObTableCtx &ctx)
{
  int ret = OB_SUCCESS;
  bool is_weak_read = consistency_level == ObTableConsistencyLevel::EVENTUAL;
  ctx.set_scan(true);
  ctx.set_entity_type(entity_type);
  ctx.set_schema_cache_guard(&query_info.schema_cache_guard_);
  ctx.set_schema_guard(&schema_guard);
  ctx.set_simple_table_schema(query_info.simple_schema_);
  ctx.set_sess_guard(query_ctx.sess_guard_);
  ctx.set_is_tablegroup_req(tablegroup_req);

  ObObjectID tmp_object_id = OB_INVALID_ID;
  ObObjectID tmp_first_level_part_id = OB_INVALID_ID;
  ObTabletID real_tablet_id;
  if (tablegroup_req) {
    if (query_ctx.part_idx_ == OB_INVALID_INDEX && query_ctx.subpart_idx_ == OB_INVALID_INDEX) { // 非分区表
      real_tablet_id = query_info.simple_schema_->get_tablet_id();
    } else if (OB_FAIL(query_info.simple_schema_->get_part_id_and_tablet_id_by_idx(query_ctx.part_idx_,
                                                                                   query_ctx.subpart_idx_,
                                                                                   tmp_object_id,
                                                                                   tmp_first_level_part_id,
                                                                                   real_tablet_id))) {
      LOG_WARN("fail to get_part_id_and_tablet_id_by_idx", K(ret));
    }
  } else {
    // for table and hbase single cf：
    // use tablet_id pass by client as query tablet and
    // especially for global index query: its tablet is global index tablet
    real_tablet_id = query_ctx.index_tablet_id_;
  }
  ObExprFrameInfo *expr_frame_info = nullptr;
  if (OB_FAIL(ret)) {
  } else if (ctx.is_init()) {
    ret = OB_INIT_TWICE;
    LOG_INFO("tb ctx has been inited", K(ctx));
  } else if (OB_FAIL(ctx.init_common(credential, real_tablet_id, timeout_ts))) {
    LOG_WARN("fail to init table ctx common part", K(ret), K(query_info.simple_schema_->get_table_name()), K(ret));
  } else if (OB_FAIL(ctx.init_scan(query_info.query_, is_weak_read, query_ctx.index_table_id_))) {
    LOG_WARN("fail to init table ctx scan part", K(ret), K(query_info.simple_schema_->get_table_name()), K(query_info.table_id_));
  } else if (OB_FAIL(query_info.table_cache_guard_.init(&ctx))) {
    LOG_WARN("fail to init table cache guard", K(ret));
  } else if (OB_FAIL(query_info.table_cache_guard_.get_expr_info(&ctx, expr_frame_info))) {
    LOG_WARN("fail to get expr frame info from cache", K(ret));
  } else if (OB_FAIL(ObTableExprCgService::alloc_exprs_memory(ctx, *expr_frame_info))) {
    LOG_WARN("fail to alloc expr memory", K(ret));
  } else if (OB_FAIL(ctx.init_exec_ctx())) {
    LOG_WARN("fail to init exec ctx", K(ret), K(ctx));
  } else {
    ctx.set_init_flag(true);
    ctx.set_expr_info(expr_frame_info);
  }

  return ret;
}

template <typename ResultType>
int ObTableQueryAsyncP::generate_merge_result_iterator(ObIAllocator *allocator,
                                                      table::ObTableQuery &query,
                                                      common::ObIArray<table::ObTableQueryResultIterator *> &iterators_array,
                                                      ResultType &result,
                                                      table::ObTableQueryResultIterator*& result_iterator /* merge result iterator */)
{
  int ret = OB_SUCCESS;
  // Merge Iterator: Holds multiple underlying iterators, stored in its own heap
  ResultMergeIterator *merge_result_iter = nullptr;
  ObTableMergeFilterCompare *compare = nullptr;
  ObQueryFlag::ScanOrder scan_order = query.get_scan_order();
  if (OB_ISNULL(merge_result_iter = OB_NEWx(ResultMergeIterator,
                                              allocator,
                                              query,
                                              result /* Response serlize row*/))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to create merge_result_iter", K(ret));
  } else if (OB_FAIL(merge_result_iter->get_inner_result_iterators().assign(iterators_array))) {
    LOG_WARN("fail to assign results", K(ret));
  } else if (scan_order == ObQueryFlag::Reverse && OB_ISNULL(compare = OB_NEWx(ObTableHbaseRowKeyReverseCompare, &merge_result_iter->get_allocator()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create compare, alloc memory fail", K(ret));
  } else if (scan_order == ObQueryFlag::Forward && OB_ISNULL(compare = OB_NEWx(ObTableHbaseRowKeyDefaultCompare, &merge_result_iter->get_allocator()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create compare, alloc memory fail", K(ret));
  } else if (OB_FAIL(merge_result_iter->init(compare))) {
    LOG_WARN("fail to build merge_result_iter", K(ret));
  } else {
    result_iterator = merge_result_iter;
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(merge_result_iter)) {
    merge_result_iter->~ResultMergeIterator();
    allocator->free(merge_result_iter);
    merge_result_iter = nullptr;
  }
  return ret;
}

template int ObTableQueryAsyncP::generate_merge_result_iterator(ObIAllocator *allocator,
                                                      table::ObTableQuery &query,
                                                      common::ObIArray<table::ObTableQueryResultIterator *> &iterators_array,
                                                      ObTableQueryResult &result,
                                                      table::ObTableQueryResultIterator*& result_iterator /* merge result iterator */);
template int ObTableQueryAsyncP::generate_merge_result_iterator(ObIAllocator *allocator,
                                                      table::ObTableQuery &query,
                                                      common::ObIArray<table::ObTableQueryResultIterator *> &iterators_array,
                                                      ObTableQueryIterableResult &result,
                                                      table::ObTableQueryResultIterator*& result_iterator /* merge result iterator */);

int ObTableQueryAsyncP::get_table_result_iterator(ObIAllocator *allocator,
                                              ObTableQueryAsyncCtx &query_ctx,
                                              const ObString &arg_table_name,
                                              table::ObTableEntityType entity_type,
                                              table::ObTableQuery &query,
                                              transaction::ObTxDesc *txn_desc,
                                              transaction::ObTxReadSnapshot &tx_snapshot,
                                              table::ObTableQueryResult &result,
                                              table::ObTableQueryResultIterator*& result_iterator)
{
  int ret = OB_SUCCESS;

  ObTableQueryResultIterator* tmp_result_iter = nullptr;
  if (query_ctx.multi_cf_infos_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid query_ctx, empty multi_cf_infos", K(ret));
  } else {
    ObTableSingleQueryInfo *table_info = query_ctx.multi_cf_infos_.at(0);
    ObTableCtx& tb_ctx = table_info->tb_ctx_;

    ObTableApiExecutor *executor = nullptr;
    ObTableApiSpec *spec = nullptr;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(tb_ctx.init_trans(txn_desc, tx_snapshot))) {
      LOG_WARN("fail to init trans", K(ret), K(tb_ctx));
    } else if (OB_FAIL(table_info->table_cache_guard_.get_spec<TABLE_API_EXEC_SCAN>(&tb_ctx, spec))) {
      LOG_WARN("fail to get spec from cache", K(ret));
    } else if (OB_FAIL(spec->create_executor(tb_ctx, executor))) {
      LOG_WARN("fail to generate executor", K(ret));
    } else if (OB_FAIL(table_info->row_iter_.open(static_cast<ObTableApiScanExecutor*>(executor)))) {
      LOG_WARN("fail to open scan row iterator", K(ret));
    } else {
      if (OB_FAIL(ObTableQueryUtils::generate_query_result_iterator(*allocator, query, false, result, tb_ctx, tmp_result_iter))) {
        LOG_WARN("fail to generate table query result iterator", K(ret));
      } else if (OB_ISNULL(tmp_result_iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to create result iterable iter", K(ret));
      } else {
        tmp_result_iter->set_scan_result(&table_info->row_iter_);
      }
    }
  }

  if (OB_SUCC(ret)) {
    result_iterator = tmp_result_iter;
  } else if (OB_NOT_NULL(tmp_result_iter)) {
    tmp_result_iter->~ObTableQueryResultIterator();
    allocator->free(tmp_result_iter);
  }
  return ret;
}

template <typename ResultType>
int ObTableQueryAsyncP::get_inner_htable_result_iterator(ObIAllocator *allocator,
                                                  table::ObTableEntityType entity_type,
                                                  const ObString &arg_table_name,
                                                  ObTableQueryAsyncCtx &query_ctx,
                                                  ObTableQuery &query,
                                                  transaction::ObTxDesc *txn_desc,
                                                  transaction::ObTxReadSnapshot &tx_snapshot,
                                                  ResultType &result,
                                                  ObIArray<table::ObTableQueryResultIterator*>& iterator_array)
{
  int ret = OB_SUCCESS;
  ObTableApiExecutor *executor = nullptr;
  ObTableApiSpec *spec = nullptr;
  const ObIArray<ObString>& columns = query.htable_filter().get_columns();
  ObArray<std::pair<ObString, bool>> family_addfamily_flag_pairs;
  ObArray<ObString> real_columns;
  family_addfamily_flag_pairs.prepare_allocate_and_keep_count(8);
  real_columns.prepare_allocate_and_keep_count(8);
  if (OB_FAIL(process_columns(columns, family_addfamily_flag_pairs, real_columns))) {
    LOG_WARN("fail to process columns", K(ret), K(columns));
  }
  for (int i = 0; OB_SUCC(ret) && i < query_ctx.multi_cf_infos_.count(); ++i) {
    ObTableSingleQueryInfo* table_info = query_ctx.multi_cf_infos_.at(i);
    bool is_found = true;
    if (OB_ISNULL(table_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table info is NULL", K(ret));
    } else if (!family_addfamily_flag_pairs.empty()) { // Only when there is a qualifier parameter
      std::pair<ObString, bool> is_add_family;
      if (OB_FAIL(ObHTableUtils::check_family_existence_with_base_name(table_info->schema_cache_guard_.get_table_name_str(),
                                                                       arg_table_name,
                                                                       entity_type,
                                                                       family_addfamily_flag_pairs,
                                                                       is_add_family,
                                                                       is_found))) {
        LOG_WARN("fail to check family exist", K(ret));
      } else if (is_found && OB_FAIL(process_table_info(table_info,
                                                        family_addfamily_flag_pairs,
                                                        real_columns,
                                                        is_add_family))) {
        LOG_WARN("fail to process table info", K(ret));
      }
    }

    ObTableCtx &tb_ctx = table_info->tb_ctx_;
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (!is_found) {
      // ignore this table
    } else if (OB_FAIL(tb_ctx.init_trans(txn_desc, tx_snapshot))) {
      LOG_WARN("fail to init trans", K(ret), K(tb_ctx));
    } else if (OB_FAIL(table_info->table_cache_guard_.get_spec<TABLE_API_EXEC_SCAN>(&tb_ctx, spec))) {
      LOG_WARN("fail to get spec from cache", K(ret));
    } else if (OB_FAIL(spec->create_executor(tb_ctx, executor))) {
      LOG_WARN("fail to create executor", K(ret));
    } else if (OB_FAIL(table_info->row_iter_.open(static_cast<ObTableApiScanExecutor*>(executor)))) {
      LOG_WARN("fail to open scan row iterator", K(ret));
    } else {
      ObTableQueryResultIterator *result_iter = nullptr;
      // If it is a multi-CF scenario, the arg_table_name here must be the table group name
      // In the case of multiple column families (multi-CF), we need to use
      // ObTableQueryIterableResult from the table_info to store intermediate results.
      // However, in the case of a single column family (single CF), we directly
      // write the results to 'result'. If this is an internal call, this 'result'
      // is generally 'ObTableQueryResult'.
      //
      // There are exceptions, such as during BatchGet operations, where even
      // for single CF, the results will be placed in ObTableQueryIterableResult.
      bool is_tablegroup_req = ObHTableUtils::is_tablegroup_req(arg_table_name, entity_type);
      if (!is_tablegroup_req && OB_FAIL(ObTableQueryUtils::generate_htable_result_iterator(*allocator,
                                                                    table_info->query_,
                                                                    result,
                                                                    table_info->tb_ctx_,
                                                                    result_iter))) {
        LOG_WARN("fail to generate query result iterator", K(ret));
      } else if (is_tablegroup_req && OB_FAIL(ObTableQueryUtils::generate_htable_result_iterator(*allocator,
                                                                    table_info->query_,
                                                                    table_info->result_,
                                                                    table_info->tb_ctx_,
                                                                    result_iter))) {
        LOG_WARN("fail to generate query result iterator", K(ret));
      } else if (OB_ISNULL(result_iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to create result iterable iter");
      } else {
        result_iter->set_scan_result(&table_info->row_iter_);
        if (OB_FAIL(iterator_array.push_back(result_iter))) {
          LOG_WARN(" fail to push back result_iter to array_result", K(ret));
        }
      }

      if (OB_FAIL(ret) && OB_NOT_NULL(result_iter)) {
        result_iter->~ObTableQueryResultIterator();
        result_iter = nullptr;
      }
    }
  }
  return ret;
}

template int ObTableQueryAsyncP::get_inner_htable_result_iterator(ObIAllocator *allocator,
                                                  table::ObTableEntityType entity_type,
                                                  const ObString &arg_table_name,
                                                  ObTableQueryAsyncCtx &query_ctx,
                                                  ObTableQuery &query,
                                                  transaction::ObTxDesc *txn_desc,
                                                  transaction::ObTxReadSnapshot &tx_snapshot,
                                                  ObTableQueryResult &result,
                                                  ObIArray<table::ObTableQueryResultIterator*>& iterator_array);
template int ObTableQueryAsyncP::get_inner_htable_result_iterator(ObIAllocator *allocator,
                                                  table::ObTableEntityType entity_type,
                                                  const ObString &arg_table_name,
                                                  ObTableQueryAsyncCtx &query_ctx,
                                                  ObTableQuery &query,
                                                  transaction::ObTxDesc *txn_desc,
                                                  transaction::ObTxReadSnapshot &tx_snapshot,
                                                  ObTableQueryIterableResult &result,
                                                  ObIArray<table::ObTableQueryResultIterator*>& iterator_array);


int ObTableQueryAsyncP::process_columns(const ObIArray<ObString>& columns,
                                        ObArray<std::pair<ObString, bool>>& family_addfamily_flag_pairs,
                                        ObArray<ObString>& real_columns)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
    ObString column = columns.at(i);
    ObString real_column = column.after('.');
    ObString family = column.split_on('.');
    if (family.empty()) {
      // do nothing
    } else if (OB_FAIL(real_columns.push_back(real_column))) {
      LOG_WARN("fail to push column name", K(ret));
    } else if (OB_FAIL(family_addfamily_flag_pairs.push_back(std::make_pair(family, real_column.empty())))) {
      LOG_WARN("fail to push family name and addfamily flag", K(ret));
    }
  }
  return ret;
}

int ObTableQueryAsyncP::update_table_info_columns(ObTableSingleQueryInfo* table_info,
                                                  const ObArray<std::pair<ObString, bool>>& family_addfamily_flag_pairs,
                                                  const ObArray<ObString>& real_columns,
                                                  const std::pair<ObString, bool>& family_addfamily_flag)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table info is NULL", K(ret));
  } else {
    ObString family_name = family_addfamily_flag.first;
    table_info->query_.htable_filter().clear_columns();
    for (int i = 0; OB_SUCC(ret) && i < family_addfamily_flag_pairs.count(); ++i) {
      ObString curr_family = family_addfamily_flag_pairs.at(i).first;
      if (curr_family == family_name) {
        ObString real_column = real_columns.at(i);
        if (OB_FAIL(table_info->query_.htable_filter().add_column(real_column))) {
          LOG_WARN("fail to add column to htable_filter", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTableQueryAsyncP::process_table_info(ObTableSingleQueryInfo* table_info,
                                           const ObArray<std::pair<ObString, bool>>& family_addfamily_flag_pairs,
                                           const ObArray<ObString>& real_columns,
                                           const std::pair<ObString, bool>& family_addfamily_flag)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table info is NULL", K(ret));
  } else if (family_addfamily_flag_pairs.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("family with flag array is empty", K(ret));
  } else {
    if (family_addfamily_flag.second) {
      table_info->query_.htable_filter().clear_columns();
    } else if (OB_FAIL(update_table_info_columns(table_info, family_addfamily_flag_pairs, real_columns, family_addfamily_flag))) {
      LOG_WARN("fail to update table info columns", K(ret));
    }
  }
  return ret;
}

template <typename ResultType>
int ObTableQueryAsyncP::get_htable_result_iterator(ObIAllocator *allocator,
                                                  ObTableQueryAsyncCtx &query_ctx,
                                                  const ObString &arg_table_name,
                                                  ObTableEntityType entity_type,
                                                  ObTableQuery &query,
                                                  transaction::ObTxDesc *txn_desc,
                                                  transaction::ObTxReadSnapshot &tx_snapshot,
                                                  ResultType &result,
                                                  ObTableQueryResultIterator*& result_iterator)
{
  int ret = OB_SUCCESS;

  ObTableQueryResultIterator* tmp_result_iter = nullptr;
  if (query_ctx.multi_cf_infos_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid query_ctx, empty multi_cf_infos", K(ret));
  } else if (entity_type == ObTableEntityType::ET_HKV) {
    ObSEArray<ObTableQueryResultIterator*, 8> result_iters;
    if (OB_FAIL(get_inner_htable_result_iterator(allocator, entity_type, arg_table_name,
                                                query_ctx, query, txn_desc, tx_snapshot,
                                                result, result_iters))) {
      LOG_WARN("fail to get result iterators", K(ret));
    } else if (result_iters.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("fail to get result iterator", K(ret));
    } else if (result_iters.count() == 1 && !ObHTableUtils::is_tablegroup_req(arg_table_name, entity_type)) {
      tmp_result_iter = result_iters.at(0);
    } else if (OB_FAIL(generate_merge_result_iterator(allocator, query, result_iters, result, tmp_result_iter))) {
      LOG_WARN("fail to create merge result", K(ret));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid entity_type", K(ret), K(entity_type));
  }
  if (OB_SUCC(ret)) {
    result_iterator = tmp_result_iter;
  } else if (OB_NOT_NULL(tmp_result_iter)) {
    tmp_result_iter->~ObTableQueryResultIterator();
    allocator->free(tmp_result_iter);
  }

  return ret;
}

template int ObTableQueryAsyncP::get_htable_result_iterator(ObIAllocator *allocator,
                                                  ObTableQueryAsyncCtx &query_ctx,
                                                  const ObString &arg_table_name,
                                                  ObTableEntityType entity_type,
                                                  ObTableQuery &query,
                                                  transaction::ObTxDesc *txn_desc,
                                                  transaction::ObTxReadSnapshot &tx_snapshot,
                                                  ObTableQueryResult &result,
                                                  ObTableQueryResultIterator*& result_iterator);

template int ObTableQueryAsyncP::get_htable_result_iterator(ObIAllocator *allocator,
                                                  ObTableQueryAsyncCtx &query_ctx,
                                                  const ObString &arg_table_name,
                                                  ObTableEntityType entity_type,
                                                  ObTableQuery &query,
                                                  transaction::ObTxDesc *txn_desc,
                                                  transaction::ObTxReadSnapshot &tx_snapshot,
                                                  ObTableQueryIterableResult &result,
                                                  ObTableQueryResultIterator*& result_iterator);

int ObTableQueryAsyncP::execute_query() {
  int ret = OB_SUCCESS;
  ObCompressorType compressor_type = INVALID_COMPRESSOR;
  ObTableQueryResult *one_result = nullptr;
  if (ObTimeUtility::fast_current_time() > timeout_ts_) {
    ret = OB_TRANS_TIMEOUT;
    LOG_WARN("exceed operatiton timeout", K(ret));
  } else if (OB_FAIL(result_.deep_copy_property_names(query_session_->get_select_columns()))) {
    LOG_WARN("fail to deep copy property names to one result", K(ret), K(query_session_->get_query()));
  } else if (OB_FAIL(query_session_->get_result_iterator()->get_next_result(one_result))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next result", K(ret));
    } else {
      ret = OB_SUCCESS;
      result_.is_end_ = true;
    }
  } else if (query_session_->get_result_iterator()->has_more_result()) {
    result_.is_end_ = false;
  } else {
    // no more result
    result_.is_end_ = true;
  }

  // check if need compress the result
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCC(ret) &&
      OB_NOT_NULL(one_result) &&
      OB_TMP_FAIL(ObKVConfigUtil::get_compress_type(MTL_ID(),
                  one_result->get_result_size(),
                  compressor_type))) {
    LOG_WARN("fail to check compress config", K(tmp_ret), K(compressor_type));
  }
  this->set_result_compress_type(compressor_type);

  return ret;
}

int ObTableQueryAsyncP::init_read_trans(const ObTableConsistencyLevel consistency_level,
                                        const ObLSID &ls_id,
                                        int64_t timeout_ts,
                                        bool need_global_snapshot,
                                        TransState *trans_state)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(trans_state)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("trans_state is null", K(ret));
  } else if (FALSE_IT(trans_param_.trans_state_ptr_ = trans_state)) {
  } else if (OB_FAIL(trans_param_.init(true,
                                       consistency_level,
                                       ls_id,
                                       timeout_ts,
                                       need_global_snapshot))) {
    LOG_WARN("fail to init trans param", K(ret));
  } else if (OB_FAIL(ObTableTransUtils::init_read_trans(trans_param_))) {
    LOG_WARN("fail to start trans", K(ret), K_(trans_param));
  }
  return ret;
}

int ObTableQueryAsyncP::init_read_trans(const ObTableConsistencyLevel consistency_level,
                                        const uint64_t table_id,
                                        const common::ObIArray<common::ObTabletID> &tablet_ids,
                                        int64_t timeout_ts,
                                        ObTableTransParam &trans_param)
{
  int ret = OB_SUCCESS;

  ObLSID ls_id;
  bool need_global_sp = false;
  if (tablet_ids.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty tablet ids", K(ret));
  } else if (tablet_ids.count() > 1) {
    need_global_sp = true;
  } else {
    ObSchemaGetterGuard &schema_guard = exec_ctx_.get_schema_guard();
    const ObSimpleTableSchemaV2 *table_schema = nullptr;
    const ObTabletID &tablet_id = tablet_ids.at(0);
    uint64_t tenant_id = MTL_ID();
    bool is_cache_hit = false;
    if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id, table_id, table_schema))) {
      LOG_WARN("fail to get table schema", K(table_id));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null table schema", K(ret), K(table_id));
    } else if (OB_FAIL((OB_FAIL(GCTX.location_service_->get(tenant_id, tablet_id, 0,
                                                            is_cache_hit, ls_id))))) {
      LOG_WARN("fail to get ls id", K(ret), K(tenant_id), K(tablet_id));
    } else {
      need_global_sp = table_schema->is_global_index_table();
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(trans_param.init(true,
                                      consistency_level,
                                      ls_id,
                                      timeout_ts,
                                      need_global_sp))) {
    LOG_WARN("fail to init trans param", K(ret));
  } else if (OB_FAIL(ObTableTransUtils::init_read_trans(trans_param))) {
    LOG_WARN("fail to start trans", K(ret), K(trans_param));
  }
  return ret;
}

int ObTableQueryAsyncP::query_scan_with_init(ObIAllocator *allocator, ObTableQueryAsyncCtx &query_ctx, table::ObTableQuery &query)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Allocator is NULL", K(ret));
  } else if (OB_FAIL(arg_.query_.deep_copy(*allocator, query))) {  // 存储的 key range 是引用，所以这里需要深拷贝
    LOG_WARN("fail to deep copy query", K(ret), K(arg_.query_));
  } else {
    query_session_->set_req_start_time(common::ObTimeUtility::current_monotonic_time());
    common::ObArray<ObTableSingleQueryInfo*>& query_infos = query_ctx.multi_cf_infos_;
    for (int i = 0; OB_SUCC(ret) && i < query_infos.count(); ++i) {
      ObTableSingleQueryInfo* info = query_infos.at(i);
      if (OB_ISNULL(info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("info is NULL", K(ret));
      } else if (OB_FAIL(init_tb_ctx(allocator,
                                    arg_.consistency_level_,
                                    arg_.entity_type_,
                                    schema_guard_,
                                    credential_,
                                    is_tablegroup_req_,
                                    query_ctx,
                                    *info,
                                    get_timeout_ts(),
                                    info->tb_ctx_))) {
        LOG_WARN("fail to init tb_ctx", K(ret));
      } else {
        // async query wouldn't start_trans and read_lastest will be false
        // and ls_op will start_trans and it will reuse init_tb_ctx, we cannot set read_lastest to false when has dml operation,
        // otherwise, the modify will be invisible
        info->tb_ctx_.set_read_latest(false);
      }
    }

    if (OB_SUCC(ret)) {
      ObTableSingleQueryInfo* info = nullptr;
      if (query_infos.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get empty query_infos", K(ret));
      } else if (OB_ISNULL(info = query_infos.at(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table info is NULL", K(ret));
      } else if (OB_FAIL(query_session_->deep_copy_select_columns(
          query.get_select_columns(),
          info->tb_ctx_.get_query_col_names()))) {
        LOG_WARN("fail to deep copy select columns from table ctx", K(ret));
      } else if (OB_FAIL(init_read_trans(arg_.consistency_level_,
                                         info->tb_ctx_.get_ls_id(),
                                         info->tb_ctx_.get_timeout_ts(),
                                         info->tb_ctx_.need_dist_das(),
                                         query_session_->get_trans_state()))) { // use session trans_state_
        LOG_WARN("fail to start readonly transaction", K(ret), K(info->tb_ctx_));
      } else if (OB_FAIL(info->tb_ctx_.init_trans(get_trans_desc(), get_tx_snapshot()))) {
        LOG_WARN("fail to init trans", K(ret), K(info->tb_ctx_));
      } else {
        ObTableQueryResultIterator* result_iterator = nullptr;
        bool is_hkv = (arg_.entity_type_ == ObTableEntityType::ET_HKV);
        if (is_hkv && OB_FAIL(get_htable_result_iterator(allocator, query_ctx, arg_.table_name_, arg_.entity_type_, query,
                                                        get_trans_desc(), get_tx_snapshot(), result_, result_iterator))) {
          LOG_WARN("fail to get result iterator", K(ret));
        } else if (!is_hkv && OB_FAIL(get_table_result_iterator(allocator, query_ctx, arg_.table_name_, arg_.entity_type_, query,
                                                                get_trans_desc(), get_tx_snapshot(), result_, result_iterator))) {
          LOG_WARN("fail to get result iterator", K(ret));
        } else {
          query_session_->set_result_iterator(result_iterator);
          // save processor's trans_desc_ to query session
          query_session_->set_trans_desc(trans_param_.trans_desc_);
          if (OB_FAIL(execute_query())) {
            LOG_WARN("fail to execute query", K(ret));
          } else {
            stat_row_count_ = result_.get_row_count();
            result_.query_session_id_ = query_session_id_;
            is_full_table_scan_ = info->tb_ctx_.is_full_table_scan();
          }
        }
      }
    }
  }
  return ret;
}


int ObTableQueryAsyncP::query_scan_without_init(ObTableCtx &tb_ctx)
{
  int ret = OB_SUCCESS;
  ObTableQueryResultIterator *result_iter = query_session_->get_result_iterator();
  ObCompressorType compressor_type = INVALID_COMPRESSOR;
  OB_TABLE_START_AUDIT(credential_,
                       sess_guard_,
                       arg_.table_name_,
                       &audit_ctx_,
                       query_session_->get_query());

  if (OB_ISNULL(result_iter)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("unexpected null result iterator", K(ret));
  } else if (OB_FAIL(result_.deep_copy_property_names(query_session_->get_select_columns()))) {
    LOG_WARN("fail to deep copy property names to one result", K(ret), K(query_session_->get_query()));
  } else {
    ObTableQueryResult *query_result = nullptr;
    result_iter->set_one_result(&result_);  // set result_ as container
    if (ObTimeUtility::fast_current_time() > timeout_ts_) {
      ret = OB_TRANS_TIMEOUT;
      LOG_WARN("exceed operatiton timeout", K(ret));
    } else if (OB_FAIL(result_iter->get_next_result(query_result))) {
      if (OB_ITER_END == ret) {
        result_.is_end_ = true;  // set scan end
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to scan result", K(ret));
      }
    } else {
      result_.is_end_ = !result_iter->has_more_result();
      result_.query_session_id_ = query_session_id_;
      is_full_table_scan_ = tb_ctx.is_full_table_scan();
    }
    stat_row_count_ = result_.get_row_count();

    // check if need compress the result
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCC(ret) &&
        OB_NOT_NULL(query_result) &&
        OB_TMP_FAIL(ObKVConfigUtil::get_compress_type(MTL_ID(), query_result->get_result_size(), compressor_type))) {
      LOG_WARN("fail to check compress config", K(tmp_ret), K(compressor_type));
    }
    this->set_result_compress_type(compressor_type);
  }

  OB_TABLE_END_AUDIT(ret_code, ret,
                     snapshot, get_tx_snapshot(),
                     stmt_type, StmtType::T_KV_QUERY,
                     return_rows, result_.get_row_count(),
                     has_table_scan, true,
                     filter, (OB_ISNULL(result_iter) ? nullptr : result_iter->get_filter()));
  return ret;
}

int ObTableQueryAsyncP::modify_ret_for_session_not_exist(const ObQueryOperationType &query_type)
{
  int ret = OB_HASH_NOT_EXIST;
  int tmp_ret = OB_SUCCESS;
  // if OB_HASH_NOT_EXIST and query_type is QUERY_NEXT, get simple_table_schema and part type
  // if range part, change OB_HASH_NOT_EXIST to OB_KV_SESS_NOT_EXIST to inform client
  // else if key, keep ret code
  // if OB_HASH_NOT_EXIST and query_type is QUERY_END, do nothing
  // if OB_HASH_NOT_EXIST and query_type is QUERY_RENEW, keep ret code to inform client
  if (ObQueryOperationType::QUERY_NEXT == arg_.query_type_) {
    LOG_WARN("fail to get query session, query_next check part type", K(ret), K(arg_));
    if (OB_TMP_FAIL(init_schema_info(arg_.table_name_))) {
      ret = tmp_ret;
      LOG_WARN("fail to init schema info", K(ret), K(arg_.table_name_));
    } else if (OB_ISNULL(simple_table_schema_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("simple table schema is NULL", K(ret));
    } else if (share::schema::is_range_part(simple_table_schema_->get_part_option().get_part_func_type())) {
      ret = OB_KV_SESS_NOT_EXIST;
      LOG_WARN("fail to get query session, rebuild start query for range part table", K(ret));
    } else {
      LOG_WARN("fail to get query session, table is not range partitioned", K(ret));
    }
  } else if (ObQueryOperationType::QUERY_END == arg_.query_type_) {
    // overwrite ret
    LOG_WARN("fail to get query session, query_end do nothing", K(ret), K(arg_));
    ret = OB_SUCCESS;
    result_.is_end_ = true;
  } else {
    LOG_WARN("fail to get query session", K(ret), K_(arg));
  }
  return ret;
}

int ObTableQueryAsyncP::init_query_async_ctx(ObIAllocator *allocator,
                                            table::ObTableQuery &arg_query,
                                            schema::ObSchemaGetterGuard& schema_guard,
                                            const ObSEArray<const schema::ObSimpleTableSchemaV2*, 8> &table_schemas,
                                            uint64_t arg_tenant_id,
                                            uint64_t arg_table_id,
                                            const ObTabletID &arg_tablet_id,
                                            ObTableQueryAsyncCtx *query_ctx)
{
  int ret = OB_SUCCESS;
  auto& infos = query_ctx->multi_cf_infos_;
  query_ctx->index_table_id_ = arg_table_id;
  query_ctx->index_tablet_id_ = arg_tablet_id;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas.count(); ++i) {
    const schema::ObSimpleTableSchemaV2* table_schema = table_schemas.at(i);
    ObTableSingleQueryInfo* query_info = nullptr;
    if (OB_FAIL(generate_table_query_info(allocator, schema_guard, table_schema, arg_tenant_id, arg_table_id,
                                          arg_tablet_id, arg_query, query_ctx, query_info))) {
      LOG_WARN("fail to get table query info", K(ret));
    } else if (OB_FAIL(infos.push_back(query_info))) {
      LOG_WARN("fail to push query info", K(ret));
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(query_info)) {
      query_info->~ObTableSingleQueryInfo();
      allocator->free(query_info);
    }
  }
  return ret;
}

int ObTableQueryAsyncP::generate_table_query_info(ObIAllocator *allocator,
                                                  schema::ObSchemaGetterGuard& schema_guard,
                                                  const schema::ObSimpleTableSchemaV2* table_schema,
                                                  uint64_t arg_tenant_id,
                                                  uint64_t arg_table_id,
                                                  const ObTabletID &arg_tablet_id,
                                                  table::ObTableQuery &arg_query,
                                                  ObTableQueryAsyncCtx *query_ctx,
                                                  ObTableSingleQueryInfo *&info)
{
  int ret = OB_SUCCESS;

  ObTableSingleQueryInfo *tmp_info = nullptr;
  if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator is null", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table schema is null", K(ret));
  } else if (OB_ISNULL(tmp_info = OB_NEWx(ObTableSingleQueryInfo, allocator, *allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate query info", K(ret));
  } else {
    tmp_info->set_simple_schema(table_schema);
    if (OB_FAIL(tmp_info->get_schema_cache_guard().init(arg_tenant_id,
                                                        table_schema->get_table_id(),
                                                        table_schema->get_schema_version(),
                                                        schema_guard))) {
      LOG_WARN("fail to init schema cache guard", K(ret));
    } else {
      tmp_info->set_table_id(table_schema->get_table_id());
      tmp_info->set_schema_version(table_schema->get_schema_version());

      int64_t part_idx = OB_INVALID_INDEX;
      int64_t subpart_idx = OB_INVALID_INDEX;
      if (arg_table_id == table_schema->get_table_id() &&
        arg_tablet_id.is_valid() &&
        table_schema->is_partitioned_table() &&
        OB_FAIL(table_schema->get_part_idx_by_tablet(arg_tablet_id, part_idx, subpart_idx))) {
        LOG_WARN("Failed to get partition index by tablet", K(ret));
      } else if (OB_FAIL(arg_query.deep_copy(*allocator, tmp_info->query_))) {
        LOG_WARN("Failed to copy query to query_info", K(ret));
      } else {
        if (part_idx != OB_INVALID_INDEX) {
          query_ctx->part_idx_ = part_idx;
          query_ctx->subpart_idx_ = subpart_idx;
        }
        // Set the correct table_id for each scan range
        ObIArray<common::ObNewRange>& ranges = tmp_info->query_.get_scan_ranges();
        for (int64_t j = 0; j < ranges.count(); ++j) {
          ObNewRange& range = ranges.at(j);
          range.table_id_ = tmp_info->get_table_id();
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    info = tmp_info;
  } else if (OB_NOT_NULL(info)) {
    info->~ObTableSingleQueryInfo();
    allocator->free(info);
  }

  return ret;
}

int ObTableQueryAsyncP::process_query_start()
{
  int ret = OB_SUCCESS;
  OB_TABLE_START_AUDIT(credential_,
                       sess_guard_,
                       arg_.table_name_,
                       &audit_ctx_,
                       arg_.query_);
  ObTableQueryAsyncCtx *query_ctx = nullptr;
  ObSEArray<const schema::ObSimpleTableSchemaV2*, 8> table_schemas;
  if (FALSE_IT(query_ctx = &(query_session_->get_query_ctx()))) {
  } else if (OB_ISNULL(query_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid query_ctx", K(ret));
  } else if (OB_FAIL(ObTableQueryUtils::get_table_schemas(gctx_.schema_service_,
                                                          schema_guard_,
                                                          arg_.table_name_,
                                                          is_tablegroup_req_,
                                                          credential_.tenant_id_,
                                                          credential_.database_id_,
                                                          table_schemas))) {
    LOG_WARN("fail to get table schemas", K(ret));
  } else if (table_schemas.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is empty", K(ret));
  } else {
    if (query_session_->get_query().get_scan_order() == ObQueryFlag::Reverse) {
      lib::ob_sort(table_schemas.begin(), table_schemas.end(), [](const schema::ObSimpleTableSchemaV2* lhs,
                                                                            const schema::ObSimpleTableSchemaV2* rhs) {
        return lhs->get_table_name() > rhs->get_table_name();
      });
    } else {
      lib::ob_sort(table_schemas.begin(), table_schemas.end(), [](const schema::ObSimpleTableSchemaV2* lhs,
                                                                            const schema::ObSimpleTableSchemaV2* rhs) {
        return lhs->get_table_name() < rhs->get_table_name();
      });
    }
  }
  if (OB_FAIL(ret)){
  } else if (OB_FAIL(init_query_async_ctx(query_session_->get_allocator(),
                                          arg_.query_,
                                          schema_guard_,
                                          table_schemas,
                                          credential_.tenant_id_,
                                          arg_.table_id_,
                                          arg_.tablet_id_,
                                          query_ctx))) {
    LOG_WARN("fail to init query ctx", K(ret));
  } else if (query_ctx->multi_cf_infos_.empty() || OB_ISNULL(query_ctx->multi_cf_infos_.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("multi cf info is NULL", K(ret));
  } else if (OB_FAIL(check_mode_type(query_ctx->multi_cf_infos_.at(0)->get_schema_cache_guard()))) {
    LOG_WARN("fail to check mode type", K(ret));
  } else if (OB_SUCC(ret)) {
    ObTableApiSessGuard *tmp_sess_guard = nullptr;
    if(OB_ISNULL(tmp_sess_guard = OB_NEWx(ObTableApiSessGuard, query_session_->get_allocator()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate sess guard", K(ret));
    } else if (FALSE_IT(query_ctx->sess_guard_ = tmp_sess_guard)) {
    } else if (OB_FAIL(TABLEAPI_OBJECT_POOL_MGR->get_sess_info(credential_, *query_ctx->sess_guard_))) {
      LOG_WARN("Failed to get session info", K(ret), K(credential_));
    } else if (OB_ISNULL(query_ctx->sess_guard_->get_sess_node_val())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Session info is NULL", K(ret), K(credential_));
    } else {}
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(query_scan_with_init(query_session_->get_allocator(), *query_ctx, query_session_->get_query()))) {
    LOG_WARN("failed to process query start scan with init", K(ret), K(query_session_id_));
  } else {
    LOG_DEBUG("finish query start", K(ret), K(query_session_id_));
  }

  OB_TABLE_END_AUDIT(ret_code, ret,
                     snapshot, get_tx_snapshot(),
                     stmt_type, StmtType::T_KV_QUERY,
                     return_rows, result_.get_row_count(),
                     has_table_scan, true,
                     filter,
                     (OB_ISNULL(query_session_->get_result_iterator())
                              ? nullptr : query_session_->get_result_iterator()->get_filter()));
  return ret;
}

int ObTableQueryAsyncP::process_query_next()
{
  int ret = OB_SUCCESS;
  ObTableQueryAsyncCtx &query_ctx = query_session_->get_query_ctx();
  ObTableSingleQueryInfo *info = nullptr;
  if (query_ctx.multi_cf_infos_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("multi_cf_infos is empty", K(ret));
  } else if (OB_ISNULL(info = query_ctx.multi_cf_infos_.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("multi cf info is NULL", K(ret));
  } else if (OB_FAIL(query_scan_without_init(info->tb_ctx_))) {
    LOG_WARN("fail to query next scan without init", K(ret), K(query_session_id_));
  } else {
    LOG_DEBUG("finish query next", K(ret), K(query_session_id_));
  }
  return ret;
}

int ObTableQueryAsyncP::before_process()
{
  is_tablegroup_req_ = ObHTableUtils::is_tablegroup_req(arg_.table_name_, arg_.entity_type_);
  retry_policy_.allow_route_retry_ = arg_.server_can_retry();
  // In HBase model, scan range columns only for odp routing, useless in server
  if (arg_.entity_type_ == ObTableEntityType::ET_HKV) {
    arg_.query_.get_scan_range_columns().reset();
  }
  return ParentType::before_process();
}

int ObTableQueryAsyncP::process_query_end()
{
  int ret = OB_SUCCESS;
  result_.is_end_ = true;
  return ret;
}

int ObTableQueryAsyncP::try_process()
{
  int ret = OB_SUCCESS;
  // statis
  bool is_hkv = (ObTableEntityType::ET_HKV == arg_.entity_type_);
  bool is_hbase_op_valid = (OHOperationType::INVALID != arg_.hbase_op_type_);
  if (is_hkv) {
    if (is_hbase_op_valid) {
      switch (arg_.hbase_op_type_) {
        case OHOperationType::SCAN:
          stat_process_type_ = ObTableProccessType::TABLE_API_HBASE_SCAN;
          break;
        case OHOperationType::GET:
        case OHOperationType::EXISTS:
        case OHOperationType::GET_LIST:
        case OHOperationType::EXISTS_LIST:
          stat_process_type_ = ObTableProccessType::TABLE_API_HBASE_GET;
          break;
        default:
          break;
      }
    } else {
      stat_process_type_ = ObTableProccessType::TABLE_API_HBASE_QUERY_ASYNC;
    }
  } else {
    stat_process_type_ = ObTableProccessType::TABLE_API_TABLE_QUERY_ASYNC;
  }
  table_id_ = arg_.table_id_; // init move response need
  tablet_id_ = arg_.tablet_id_;
  if (is_new_try_process()) {
    if (OB_FAIL(new_try_process())) {
      LOG_WARN("fail to new try process", K(ret));
    }
  } else if (OB_FAIL(old_try_process())) {
    LOG_WARN("fail to old try process", K(ret));
  }
  return ret;
}


int ObTableQueryAsyncP::old_try_process()
{
  int ret = OB_SUCCESS;
  observer::ObReqTimeGuard req_timeinfo_guard; // 引用cache资源必须加ObReqTimeGuard
  if (OB_FAIL(ObTableQueryASyncMgr::check_query_type(arg_.query_type_))) {
    LOG_WARN("query type is invalid", K(ret), K(arg_.query_type_));
  } else if (OB_FAIL(MTL(ObTableQueryASyncMgr*)->get_session_id(query_session_id_, arg_.query_session_id_, arg_.query_type_))) {
    LOG_WARN("fail to get query session id", K(ret), K(arg_.query_session_id_));
  } else if (OB_FAIL(get_old_session(query_session_id_, query_session_))) {
    if (ret == OB_HASH_NOT_EXIST) {
      // overwrite ret
      ret = modify_ret_for_session_not_exist(arg_.query_type_);
    } else {
      LOG_WARN("fail to get query session", K(ret), K(query_session_id_));
    }
  } else if (FALSE_IT(timeout_ts_ = get_trans_timeout_ts())) {
  } else {
    WITH_CONTEXT(query_session_->get_memory_ctx()) {
      if (ObQueryOperationType::QUERY_START == arg_.query_type_) {
        set_timeout(query_session_->get_lease_timeout_period());
        ret = process_query_start();
      } else if (ObQueryOperationType::QUERY_NEXT == arg_.query_type_) {
        ret = process_query_next();
      } else if (ObQueryOperationType::QUERY_RENEW == arg_.query_type_) {
        result_.query_session_id_ = query_session_id_;
      } else if (ObQueryOperationType::QUERY_END == arg_.query_type_) {
        ret = process_query_end();
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("query execution failed, need rollback", K(ret));
        int tmp_ret = ret;
        if (OB_FAIL((MTL(ObTableQueryASyncMgr*))->destory_query_session(query_session_))) {
          LOG_WARN("faild to destory query session", K(ret));
        }
        ret = tmp_ret;
      } else if (result_.is_end_) {
        if (OB_FAIL((MTL(ObTableQueryASyncMgr*))->destory_query_session(query_session_))) {
          LOG_WARN("fail to destory query session", K(ret), K(query_session_id_));
        }
      } else {
        if (ObQueryOperationType::QUERY_START == arg_.query_type_) {
          if (OB_FAIL(query_session_->alloc_req_timeinfo())) {
            LOG_WARN("fail to allocate req time info", K(ret));
          } else {
            // start time > end time, end time == 0, reentrant cnt == 1
            query_session_->update_req_timeinfo_start_time();
          }
        }
        query_session_->set_in_use(false);
        query_session_->set_timeout_ts();
      }
    }
  }

  #ifndef NDEBUG
    // debug mode
    FLOG_INFO("[TABLE] execute query", K(ret), K_(arg), K(result_),
             K_(retry_count), K_(result_row_count));
  #else
    // release mode
    LOG_TRACE("[TABLE] execute query", K(ret), K_(arg), K_(timeout_ts), K_(retry_count), K(result_.is_end_),
              "receive_ts", get_receive_timestamp(), K_(result_row_count));
  #endif
  return ret;
}

int ObTableQueryAsyncP::new_try_process()
{
  int ret = OB_SUCCESS;
  ObModelGuard model_guard;
  ObIModel *model = nullptr;
  ObTableExecCtx *ctx = nullptr;
  exec_ctx_.set_table_name(arg_.table_name_);
  exec_ctx_.set_table_id(arg_.table_id_);
  exec_ctx_.set_timeout_ts(get_timeout_ts());
  exec_ctx_.set_audit_ctx(audit_ctx_);
  if (OB_FAIL(ObModelFactory::get_model_guard(allocator_, arg_.entity_type_, model_guard))) {
    LOG_WARN("fail to get model guard", K(ret), K(arg_.entity_type_));
  } else if (FALSE_IT(model = model_guard.get_model())) {
  } else if (OB_ISNULL(model)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("model is null", K(ret));
  } else if (OB_FAIL(model->prepare(exec_ctx_, arg_, result_, ctx))) {
    if (ret != OB_ITER_END) {
      if (ret == OB_HASH_NOT_EXIST) {
        // overwrite ret
        ret = modify_ret_for_session_not_exist(arg_.query_type_);
      } else {
        LOG_WARN("fail to prepare model", K(ret), K_(exec_ctx), K_(arg));
      }
    } else {
      result_.is_end_ = true;
    }
  } else if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ctx", K(ret));
  } else {
    if (ObQueryOperationType::QUERY_START == arg_.query_type_) {
      if (OB_FAIL(ctx->get_trans_param().init(true,
                                              arg_.consistency_level_,
                                              ctx->get_ls_id(),
                                              get_trans_timeout_ts(),
                                              !exec_ctx_.get_ls_id().is_valid()/*need_global_snapshot*/))) {
        LOG_WARN("fail to init trans param", K(ret));
      } else if (OB_FAIL(ObTableTransUtils::init_read_trans(ctx->get_trans_param()))) {
        LOG_WARN("fail to start trans", K(ret), K(ctx->get_trans_param()));
      } else if (!ctx->get_trans_param().tx_snapshot_.is_ls_snapshot()
                 && tablet_id_.is_valid()
                 && OB_FAIL(check_local_execute(tablet_id_))) {
        LOG_WARN("fail to check local execute", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(model->work(*ctx, arg_, result_, exec_ctx_))) {
        LOG_WARN("model fail to work", K(ret));
      } else if (OB_FAIL(model->after_work(*ctx, arg_, result_))) {
        LOG_WARN("model fail to after work", K(ret), K_(arg));
      }
    }

    if (OB_SUCC(ret) && ObQueryOperationType::QUERY_START == arg_.query_type_) {
      set_timeout(model->get_lease_timeout_period());
    }
    // record ob rows
    stat_row_count_ = exec_ctx_.get_stat_row_count();
  }

  if (OB_NOT_NULL(model) && OB_NOT_NULL(model->get_query_session())) {
    int64_t session_id = model->get_query_session()->get_session_id();
    if (OB_FAIL(ret)) {
      int tmp_ret = ret;
      if (OB_FAIL(MTL(ObTableQueryASyncMgr*)->destory_query_session(model->get_query_session()))) {
        LOG_WARN("faild to destory query session", K(ret), K(session_id));
      }
      ret = tmp_ret;
    } else if (result_.is_end_) {
      if (OB_FAIL(MTL(ObTableQueryASyncMgr*)->destory_query_session(model->get_query_session()))) {
        LOG_WARN("fail to destory query session", K(ret), K(session_id));
      }
    }
  }

  #ifndef NDEBUG
    // debug mode
    LOG_INFO("[TABLE] execute query", K(ret), K_(arg), K(result_),
             K_(retry_count), K_(result_row_count));
  #else
    // release mode
    LOG_TRACE("[TABLE] execute query", K(ret), K_(arg), K_(timeout_ts), K_(retry_count), K(result_.is_end_),
              "receive_ts", get_receive_timestamp(), K_(result_row_count));
  #endif

  if (ret == OB_ITER_END) {
    ret = OB_SUCCESS; // cover ret
  }
  return ret;
}

bool ObTableQueryAsyncP::is_new_try_process()
{
  return arg_.entity_type_ == ObTableEntityType::ET_HKV &&
         (!arg_.tablet_id_.is_valid() ||
         (arg_.tablet_id_.is_valid() && arg_.distribute_need_tablet_id())) &&
         TABLEAPI_OBJECT_POOL_MGR->is_support_distributed_execute();
}