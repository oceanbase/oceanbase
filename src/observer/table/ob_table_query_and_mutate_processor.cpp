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
#include "ob_table_query_and_mutate_processor.h"
#include "ob_table_rpc_processor_util.h"
#include "observer/ob_service.h"
#include "storage/tx_storage/ob_access_service.h"
#include "ob_table_end_trans_cb.h"
#include "sql/optimizer/ob_table_location.h"  // ObTableLocation
#include "lib/stat/ob_diagnose_info.h"
#include "lib/stat/ob_session_stat.h"
#include "ob_htable_utils.h"
#include "ob_table_cg_service.h"
#include "ob_htable_filter_operator.h"
#include "ob_table_op_wrapper.h"

using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::share;
using namespace oceanbase::sql;

ObTableQueryAndMutateP::ObTableQueryAndMutateP(const ObGlobalContext &gctx)
    :ObTableRpcProcessor(gctx),
     allocator_(ObModIds::TABLE_PROC, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
     tb_ctx_(allocator_)
{
}

int ObTableQueryAndMutateP::deserialize()
{
  arg_.query_and_mutate_.set_deserialize_allocator(&allocator_);
  arg_.query_and_mutate_.set_entity_factory(&default_entity_factory_);

  int ret = ParentType::deserialize();
  if (OB_SUCC(ret) && ObTableEntityType::ET_HKV == arg_.entity_type_) {
    // For HKV table, modify the timestamp value to be negative
    ObTableBatchOperation &mutations = arg_.query_and_mutate_.get_mutations();
    const int64_t N = mutations.count();
    for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
    {
      ObITableEntity *entity = nullptr;
      ObTableOperation &mutation = const_cast<ObTableOperation&>(mutations.at(i));
      if (OB_FAIL(mutation.get_entity(entity))) {
        LOG_WARN("failed to get entity", K(ret), K(i));
      } else if (OB_FAIL(ObTableRpcProcessorUtil::negate_htable_timestamp(*entity))) {
        LOG_WARN("failed to negate timestamp value", K(ret));
      }
    } // end for
  }
  return ret;
}

int ObTableQueryAndMutateP::check_arg()
{
  int ret = OB_SUCCESS;
  ObTableQuery &query = arg_.query_and_mutate_.get_query();
  ObHTableFilter &hfilter = query.htable_filter();
  ObTableBatchOperation &mutations = arg_.query_and_mutate_.get_mutations();
  if (!query.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table query request", K(ret), K(query));
  } else if (!hfilter.is_valid()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("QueryAndMutate only supports hbase model table for now", K(ret));
  } else if (mutations.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("should have at least one mutation operation", K(ret), K(mutations));
  } else {
    // these options are meaningless for QueryAndMutate users but we should control them internally
    query.set_batch(1);  // mutate for each row
    query.set_max_result_size(-1);

    hfilter.set_max_versions(1);
    hfilter.set_row_offset_per_column_family(0);
    hfilter.set_max_results_per_column_family(-1);
  }
  return ret;
}

void ObTableQueryAndMutateP::audit_on_finish()
{
  audit_record_.consistency_level_ = ObConsistencyLevel::STRONG; // todo: exact consistency
  audit_record_.return_rows_ = arg_.query_and_mutate_.return_affected_entity() ? result_.affected_entity_.get_row_count() : 0;
  audit_record_.table_scan_ = true; // todo: exact judgement
  audit_record_.affected_rows_ = result_.affected_rows_;
  audit_record_.try_cnt_ = retry_count_ + 1;
}

uint64_t ObTableQueryAndMutateP::get_request_checksum()
{
  uint64_t checksum = 0;
  checksum = ob_crc64(checksum, arg_.table_name_.ptr(), arg_.table_name_.length());
  const uint64_t op_checksum = arg_.query_and_mutate_.get_checksum();
  checksum = ob_crc64(checksum, &op_checksum, sizeof(op_checksum));
  checksum = ob_crc64(checksum, &arg_.binlog_row_image_type_, sizeof(arg_.binlog_row_image_type_));
  return checksum;
}

void ObTableQueryAndMutateP::reset_ctx()
{
  need_retry_in_queue_ = false;
  one_result_.reset();
  ObTableApiProcessorBase::reset_ctx();
}

ObTableAPITransCb *ObTableQueryAndMutateP::new_callback(rpc::ObRequest *req)
{
  UNUSED(req);
  return nullptr;
}

int ObTableQueryAndMutateP::get_tablet_ids(uint64_t table_id, ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  ObTabletID tablet_id = arg_.tablet_id_;
  share::schema::ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = NULL;
  if (!tablet_id.is_valid()) {
    const uint64_t tenant_id = MTL_ID();
    if (OB_FAIL(gctx_.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("failed to get schema guard", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
      LOG_WARN("failed to get table schema", K(ret), K(tenant_id), K(table_id), K(table_schema));
    } else if (!table_schema->is_partitioned_table()) {
      tablet_id = table_schema->get_tablet_id();
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("partitioned table not supported", K(ret), K(table_id));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(tablet_ids.push_back(tablet_id))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  return ret;
}

int ObTableQueryAndMutateP::init_tb_ctx(table::ObTableCtx &ctx,
                                        ObTableOperationType::Type op_type,
                                        const ObITableEntity &entity)
{
  int ret = OB_SUCCESS;
  ObExprFrameInfo *expr_frame_info = nullptr;
  ctx.set_entity(&entity);
  ctx.set_entity_type(arg_.entity_type_);
  ctx.set_operation_type(op_type);
  ctx.set_batch_operation(&arg_.query_and_mutate_.get_mutations());

  if (!ctx.is_init()) {
    if (OB_FAIL(ctx.init_common(credential_,
                                arg_.tablet_id_,
                                arg_.table_name_,
                                get_timeout_ts()))) {
      LOG_WARN("fail to init table ctx common part", K(ret), K(arg_.table_name_));
    } else {
      switch (op_type) {
        case ObTableOperationType::GET: {
          if (OB_FAIL(ctx.init_get())) {
            LOG_WARN("fail to init get ctx", K(ret), K(ctx));
          }
          break;
        }
        case ObTableOperationType::INSERT: {
          if (OB_FAIL(ctx.init_insert())) {
            LOG_WARN("fail to init insert ctx", K(ret), K(ctx));
          }
          break;
        }
        case ObTableOperationType::DEL: {
          if (OB_FAIL(ctx.init_delete())) {
            LOG_WARN("fail to init delete ctx", K(ret), K(ctx));
          }
          break;
        }
        case ObTableOperationType::UPDATE: {
          if (OB_FAIL(ctx.init_update())) {
            LOG_WARN("fail to init update ctx", K(ret), K(ctx));
          }
          break;
        }
        case ObTableOperationType::APPEND:
        case ObTableOperationType::INCREMENT:
        case ObTableOperationType::INSERT_OR_UPDATE: {
          if (OB_FAIL(ctx.init_insert_up())) {
            LOG_WARN("fail to init insert up ctx", K(ret), K(ctx));
          }
          break;
        }
        case ObTableOperationType::REPLACE: {
          if (OB_FAIL(ctx.init_replace())) {
            LOG_WARN("fail to init replace ctx", K(ret), K(ctx));
          }
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected operation type", "type", op_type);
          break;
        }
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(ctx.init_exec_ctx())) {
      LOG_WARN("fail to init exec ctx", K(ret), K(ctx));
    }
  }

  return ret;
}

int ObTableQueryAndMutateP::init_scan_tb_ctx(ObTableApiCacheGuard &cache_guard)
{
  int ret = OB_SUCCESS;
  ObExprFrameInfo *expr_frame_info = nullptr;
  const ObTableQuery &query = arg_.query_and_mutate_.get_query();
  bool is_weak_read = false;
  tb_ctx_.set_scan(true);

  if (tb_ctx_.is_init()) {
    LOG_INFO("tb ctx has been inited", K_(tb_ctx));
  } else if (OB_FAIL(tb_ctx_.init_common(credential_,
                                         arg_.tablet_id_,
                                         arg_.table_name_,
                                         get_timeout_ts()))) {
    LOG_WARN("fail to init table ctx common part", K(ret), K(arg_.table_name_));
  } else if (OB_FAIL(tb_ctx_.init_scan(query, is_weak_read))) {
    LOG_WARN("fail to init table ctx scan part", K(ret), K(arg_.table_name_));
  } else if (OB_FAIL(cache_guard.init(&tb_ctx_))) {
    LOG_WARN("fail to init cache guard", K(ret));
  } else if (OB_FAIL(cache_guard.get_expr_info(&tb_ctx_, expr_frame_info))) {
    LOG_WARN("fail to get expr frame info from cache", K(ret));
  } else if (OB_FAIL(ObTableExprCgService::alloc_exprs_memory(tb_ctx_, *expr_frame_info))) {
    LOG_WARN("fail to alloc expr memory", K(ret));
  } else if (OB_FAIL(tb_ctx_.init_exec_ctx())) {
    LOG_WARN("fail to init exec ctx", K(ret), K(tb_ctx_));
  } else {
    tb_ctx_.set_init_flag(true);
    tb_ctx_.set_expr_info(expr_frame_info);
  }

  return ret;
}

int ObTableQueryAndMutateP::generate_query_result(ObTableApiScanRowIterator &row_iter,
                                                  ObTableQueryResultIterator *&result_iter)
{
  int ret = OB_SUCCESS;
  const ObTableQuery &query = arg_.query_and_mutate_.get_query();
  ObHTableFilterOperator *htable_result_iter = nullptr;

  if (OB_FAIL(ObTableService::check_htable_query_args(query))) {
    LOG_WARN("fail to check htable query args", K(ret));
  } else if (OB_ISNULL(htable_result_iter = OB_NEWx(table::ObHTableFilterOperator,
                                                    (&allocator_),
                                                    query,
                                                    one_result_))) {
    LOG_WARN("fail to alloc htable query result iterator", K(ret));
  } else if (htable_result_iter->parse_filter_string(&allocator_)) {
    LOG_WARN("failed to parse htable filter string", K(ret));
  } else {
    result_iter = htable_result_iter;
    htable_result_iter->set_scan_result(&row_iter);
    ObHColumnDescriptor desc;
    const ObString &comment = tb_ctx_.get_table_schema()->get_comment_str();
    if (OB_FAIL(desc.from_string(comment))) {
      LOG_WARN("fail to parse hcolumn_desc from comment string", K(ret), K(comment));
    } else if (desc.get_time_to_live() > 0) {
      htable_result_iter->set_ttl(desc.get_time_to_live());
    }
  }

  return ret;
}

int ObTableQueryAndMutateP::execute_htable_delete()
{
  int ret = OB_SUCCESS;

  SMART_VAR(ObTableCtx, tb_ctx, allocator_) {
    ObTableApiSpec *spec = nullptr;
    ObTableApiExecutor *executor = nullptr;
    observer::ObReqTimeGuard req_timeinfo_guard; // 引用cache资源必须加ObReqTimeGuard
    ObTableApiCacheGuard cache_guard;
    ObTableBatchOperation &mutations = arg_.query_and_mutate_.get_mutations();
    const ObTableOperation &op = mutations.at(0);

    if (OB_FAIL(init_tb_ctx(tb_ctx,
                            ObTableOperationType::Type::DEL,
                            op.entity()))) {
      LOG_WARN("fail to init table ctx", K(ret));
    } else if (OB_FAIL(ObTableOpWrapper::get_or_create_spec<TABLE_API_EXEC_DELETE>(tb_ctx, cache_guard, spec))) {
      LOG_WARN("fail to get spec from cache", K(ret));
    } else if (OB_FAIL(spec->create_executor(tb_ctx, executor))) {
      LOG_WARN("fail to generate executor", K(ret), K(tb_ctx));
    } else if (OB_FAIL(tb_ctx.init_trans(get_trans_desc(), get_tx_snapshot()))) {
      LOG_WARN("fail to init trans", K(ret), K(tb_ctx));
    } else {
      ObHTableDeleteExecutor delete_executor(tb_ctx, static_cast<ObTableApiDeleteExecutor *>(executor));
      if (OB_FAIL(delete_executor.open())) {
        LOG_WARN("fail to open htable delete executor", K(ret));
      } else if (OB_FAIL(delete_executor.get_next_row())) {
        LOG_WARN("fail to call htable delete get_next_row", K(ret));
      }

      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = delete_executor.close())) {
        LOG_WARN("fail to close htable delete executor", K(tmp_ret));
        ret = COVER_SUCC(tmp_ret);
      }
    }

    if (OB_NOT_NULL(spec)) {
      spec->destroy_executor(executor);
      tb_ctx.set_expr_info(nullptr);
    }
  }

  return ret;
}

int ObTableQueryAndMutateP::execute_htable_put()
{
  int ret = OB_SUCCESS;
  ObTableBatchOperation &mutations = arg_.query_and_mutate_.get_mutations();
  int64_t N = mutations.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
    const ObTableOperation &op = mutations.at(i);
    if (OB_FAIL(execute_htable_put(op.entity()))) {
      LOG_WARN("fail to execute hatable insert", K(ret));
    }
  }

  return ret;
}

int ObTableQueryAndMutateP::execute_htable_put(const ObITableEntity &new_entity)
{
  int ret = OB_SUCCESS;

  SMART_VAR(ObTableCtx, tb_ctx, allocator_) {
    ObTableOperationResult op_result;
    if (OB_FAIL(init_tb_ctx(tb_ctx,
                            ObTableOperationType::Type::INSERT_OR_UPDATE,
                            new_entity))) {
      LOG_WARN("fail to init table ctx", K(ret));
    } else if (OB_FAIL(tb_ctx.init_trans(get_trans_desc(), get_tx_snapshot()))) {
      LOG_WARN("fail to init trans", K(ret), K(tb_ctx));
    } else if (OB_FAIL(ObTableOpWrapper::process_op<TABLE_API_EXEC_INSERT_UP>(tb_ctx, op_result))) {
      LOG_WARN("fail to process insert up op", K(ret));
    }
  }

  return ret;
}

class ObTableQueryAndMutateP::ColumnIdxComparator
{
public:
  bool operator()(const ColumnIdx &a, const ColumnIdx &b) const
  {
    return a.first.compare(b.first) < 0;
  }
};

int ObTableQueryAndMutateP::sort_qualifier(common::ObIArray<ColumnIdx> &columns,
                                           const table::ObTableBatchOperation &increment)
{
  int ret = OB_SUCCESS;
  const int64_t N = increment.count();
  if (N <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty increment", K(ret));
  }
  ObString htable_row;
  for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
  {
    const ObTableOperation &mutation = increment.at(i);
    const ObITableEntity &entity = mutation.entity();
    if (entity.get_rowkey_size() != 3) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("htable should be with 3 rowkey columns", K(ret), K(entity));
    } else {
      ObHTableCellEntity3 htable_cell(&entity);
      ObString row = htable_cell.get_rowkey();
      bool row_is_null = htable_cell.last_get_is_null();
      ObString qualifier = htable_cell.get_qualifier();
      bool qualifier_is_null = htable_cell.last_get_is_null();
      (void)htable_cell.get_timestamp();
      bool timestamp_is_null = htable_cell.last_get_is_null();
      if (row_is_null || timestamp_is_null || qualifier_is_null) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument for htable put", K(ret),
                 K(row_is_null), K(timestamp_is_null), K(qualifier_is_null));
      } else {
        if (0 == i) {
          htable_row = row;  // shallow copy
        } else if (htable_row != row) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("rowkey not the same", K(ret), K(row), K(htable_row));
          break;
        }
        if (OB_FAIL(columns.push_back(std::make_pair(qualifier, i)))) {
          LOG_WARN("failed to push back", K(ret));
          break;
        }
      }
    }
  } // end for
  if (OB_SUCC(ret)) {
    // sort qualifiers
    ColumnIdx *end = &columns.at(columns.count()-1);
    ++end;
    std::sort(&columns.at(0), end, ColumnIdxComparator());
  }
  if (OB_SUCC(ret)) {
    // check duplicated qualifiers
    for (int64_t i = 0; OB_SUCCESS == ret && i < N-1; ++i)
    {
      if (columns.at(i).first == columns.at(i+1).first) {
        ret = OB_ERR_PARAM_DUPLICATE;
        LOG_WARN("duplicated qualifiers", K(ret), "cq", columns.at(i).first, K(i));
      }
    } // end for
  }
  return ret;
}

int ObTableQueryAndMutateP::get_old_row(ObTableApiSpec &scan_spec, ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObTableApiExecutor *executor = nullptr;
  ObTableApiScanRowIterator row_iter;

  if (OB_FAIL(scan_spec.create_executor(tb_ctx_, executor))) {
    LOG_WARN("fail to generate executor", K(ret), K(tb_ctx_));
  } else if (OB_FAIL(row_iter.open(static_cast<ObTableApiScanExecutor*>(executor)))) {
    LOG_WARN("fail to open scan row iterator", K(ret));
  } else if (OB_FAIL(row_iter.get_next_row(row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next row", K(ret));
    } else {
      ret = OB_SUCCESS; // not exist
    }
  }

  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = row_iter.close())) {
    LOG_WARN("fail to close row iter", K(tmp_ret));
    ret = COVER_SUCC(tmp_ret);
  }

  if (OB_NOT_NULL(executor)) {
    scan_spec.destroy_executor(executor);
  }

  return ret;
}

int ObTableQueryAndMutateP::refresh_query_range(const ObObj &new_q_obj)
{
  int ret = OB_SUCCESS;
  const ObTableQuery &query = arg_.query_and_mutate_.get_query();
  const ObIArray<common::ObNewRange> &ranges = query.get_scan_ranges();
  if (ranges.count() != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid range count", K(ret), K(ranges.count()));
  } else {
    ObIArray<common::ObNewRange> &ctx_ranges = tb_ctx_.get_key_ranges();
    ctx_ranges.reset();
    ObNewRange new_range = ranges.at(0); // shawdow copy
    tb_ctx_.set_limit(1); // 设置limit 1，只获取最新版本的数据
    if (FALSE_IT(new_range.start_key_.get_obj_ptr()[ObHTableConstants::COL_IDX_Q] = new_q_obj)) {
      // 将mutate的qualifier作为新的扫描范围
    } else if (FALSE_IT(new_range.end_key_.get_obj_ptr()[ObHTableConstants::COL_IDX_Q] = new_q_obj)) {
      // 将mutate的qualifier作为新的扫描范围
    } else if (OB_FAIL(ctx_ranges.push_back(new_range))) {
      LOG_WARN("fail to push back new range", K(ret));
    }
  }

  return ret;
}

int ObTableQueryAndMutateP::add_to_results(const ObObj &rk,
                                           const ObObj &cq,
                                           const ObObj &ts,
                                           const ObObj &value)
{
  int ret = OB_SUCCESS;
  table::ObTableQueryResult &results = result_.affected_entity_;

  if (results.get_property_count() <= 0) {
    if (OB_FAIL(results.add_property_name(ObHTableConstants::ROWKEY_CNAME_STR))) {
      LOG_WARN("failed to copy name", K(ret));
    } else if (OB_FAIL(results.add_property_name(ObHTableConstants::CQ_CNAME_STR))) {
      LOG_WARN("failed to copy name", K(ret));
    } else if (OB_FAIL(results.add_property_name(ObHTableConstants::VERSION_CNAME_STR))) {
      LOG_WARN("failed to copy name", K(ret));
    } else if (OB_FAIL(results.add_property_name(ObHTableConstants::VALUE_CNAME_STR))) {
      LOG_WARN("failed to copy name", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ObObj objs[4];
    objs[0] = rk;
    objs[1] = cq;
    objs[2] = ts;
    int64_t timestamp = 0;
    objs[2].get_int(timestamp);
    objs[2].set_int(-timestamp);  // negate_htable_timestamp
    objs[3] = value;
    common::ObNewRow row(objs, 4);
    if (OB_FAIL(results.add_row(row))) {  // deep copy
      LOG_WARN("failed to add row to results", K(ret), K(row));
    }
  }
  return ret;
}

int ObTableQueryAndMutateP::generate_new_value(const ObNewRow *old_row,
                                               const ObITableEntity &src_entity,
                                               bool is_increment,
                                               ObTableEntity &new_entity)
{
  int ret = OB_SUCCESS;
  int64_t now_ms = -ObHTableUtils::current_time_millis();
  ObHTableCellEntity3 htable_cell(&src_entity);
  bool row_is_null = htable_cell.last_get_is_null();
  const ObString delta_str = htable_cell.get_value();
  int64_t delta_int = 0;
  bool v_is_null = htable_cell.last_get_is_null();
  if (row_is_null || v_is_null) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row is null or value is null", K(ret), K(row_is_null), K(v_is_null));
  } else if (is_increment && OB_FAIL(ObHTableUtils::java_bytes_to_int64(delta_str, delta_int))) {
    LOG_WARN("fail to convert bytes to integer", K(ret), K(delta_str));
  } else {
    ObString orig_str;
    int64_t new_ts = htable_cell.get_timestamp(); // default insert timestamp
    if (ObHTableConstants::LATEST_TIMESTAMP == new_ts) {
      new_ts = now_ms;
    }
    if (OB_NOT_NULL(old_row)) { // 旧行存在，构造新值（base + delta）
      if (ObHTableConstants::COL_IDX_V > old_row->count_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid cell count", K(ret), K(old_row->count_));
      } else {
        const ObObj &base_obj_t = old_row->get_cell(ObHTableConstants::COL_IDX_T);
        const ObObj &base_obj_v = old_row->get_cell(ObHTableConstants::COL_IDX_V);
        orig_str = base_obj_v.get_varbinary();
        int64_t orig_ts = base_obj_t.get_int();
        new_ts = min(orig_ts - 1, now_ms); // adapt hbase
        if (is_increment) {
          int64_t orig_int = 0;
          if (OB_FAIL(ObHTableUtils::java_bytes_to_int64(orig_str, orig_int))) {
            LOG_WARN("fail to convert bytes to integer", K(ret), K(orig_str));
          } else {
            delta_int += orig_int;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObObj new_k, new_q, new_t, new_v;
      // K
      new_k.set_varbinary(htable_cell.get_rowkey());
      // Q
      new_q.set_varbinary(htable_cell.get_qualifier());
      // T
      new_t.set_int(new_ts);
      // V
      if (is_increment) {
        char *bytes = static_cast<char*>(allocator_.alloc(sizeof(int64_t)));
        if (OB_ISNULL(bytes)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", K(ret), KP(bytes));
        } else if (OB_FAIL(ObHTableUtils::int64_to_java_bytes(delta_int, bytes))) {
          LOG_WARN("fail to convert bytes", K(ret), K(delta_int));
        } else {
          ObString v(sizeof(int64_t), bytes);
          new_v.set_varbinary(v);
        }
      } else {
        if (orig_str.empty()) {
          new_v.set_varbinary(delta_str);
        } else {
          int32_t total_len = orig_str.length() + delta_str.length();
          char *bytes = static_cast<char*>(allocator_.alloc(total_len));
          if (OB_ISNULL(bytes)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc memory", K(ret), KP(bytes));
          } else {
            MEMCPY(bytes, orig_str.ptr(), orig_str.length());
            MEMCPY(bytes + orig_str.length(), delta_str.ptr(), delta_str.length());
            ObString new_str(total_len, bytes);
            new_v.set_varbinary(new_str);
          }
        }
      }
      if (OB_SUCC(ret)) { // generate new entity
        if (OB_FAIL(new_entity.add_rowkey_value(new_k))) {
          LOG_WARN("fail to add rowkey value", K(ret), K(new_k));
        } else if (OB_FAIL(new_entity.add_rowkey_value(new_q))) {
          LOG_WARN("fail to add rowkey value", K(ret), K(new_q));
        } else if (OB_FAIL(new_entity.add_rowkey_value(new_t))) {
          LOG_WARN("fail to add rowkey value", K(ret), K(new_t));
        } else if (OB_FAIL(new_entity.set_property(ObHTableConstants::VALUE_CNAME_STR, new_v))) {
          LOG_WARN("fail to set property", K(ret), K(new_v));
        }
      }
      if (OB_SUCC(ret) && arg_.query_and_mutate_.return_affected_entity()) { // set return accected entity
        if (OB_FAIL(add_to_results(new_k, new_q, new_t, new_v))) {
          LOG_WARN("fail to add to results", K(ret), K(new_k), K(new_q), K(new_t), K(new_v));
        }
      }
    }
  }

  return ret;
}

// 1. 执行query获取到最新版本旧行的V
// 2. 基于旧V执行mutate
int ObTableQueryAndMutateP::execute_htable_increment(ObTableApiSpec &scan_spe)
{
  int ret = OB_SUCCESS;
  ObTableBatchOperation &mutations = arg_.query_and_mutate_.get_mutations();
  int64_t N = mutations.count();
  ObSEArray<ColumnIdx, OB_DEFAULT_SE_ARRAY_COUNT> columns;
  if (OB_FAIL(sort_qualifier(columns, mutations))) {
    LOG_WARN("fail to sort qualifier", K(ret), K(mutations));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); i++) {
    ObNewRow *old_row = nullptr;
    ObTableEntity new_entity;
    const ObTableOperation &op = mutations.at(columns.at(i).second);
    bool is_increment = ObTableOperationType::INCREMENT == op.type();
    const ObRowkey &rowkey = op.entity().get_rowkey();
    const ObObj &q_obj = rowkey.get_obj_ptr()[ObHTableConstants::COL_IDX_Q];  // column Q
    if (OB_FAIL(refresh_query_range(q_obj))) {
      LOG_WARN("fail to refresh query range", K(ret), K(q_obj));
    } else if (OB_FAIL(get_old_row(scan_spe, old_row))) { // 获取旧行，存在/不存在
      LOG_WARN("fail to get old row", K(ret));
    } else if (OB_FAIL(generate_new_value(old_row, op.entity(), is_increment, new_entity))) {
      LOG_WARN("fail to generate new value", K(ret), K(op.entity()), KP(old_row));
    } else if (OB_FAIL(execute_htable_insert(new_entity))) {
      LOG_WARN("fail to execute hatable insert", K(ret));
    }
  }

  return ret;
}

int ObTableQueryAndMutateP::execute_htable_insert(const ObITableEntity &new_entity)
{
  int ret = OB_SUCCESS;

  SMART_VAR(ObTableCtx, tb_ctx, allocator_) {
    ObTableApiSpec *spec = nullptr;
    ObTableApiExecutor *executor = nullptr;
    ObTableOperationResult op_result;
    if (OB_FAIL(init_tb_ctx(tb_ctx,
                            ObTableOperationType::Type::INSERT,
                            new_entity))) {
      LOG_WARN("fail to init table ctx", K(ret));
    } else if (OB_FAIL(tb_ctx.init_trans(get_trans_desc(), get_tx_snapshot()))) {
      LOG_WARN("fail to init trans", K(ret), K(tb_ctx));
    } else if (OB_FAIL(ObTableOpWrapper::process_op<TABLE_API_EXEC_INSERT>(tb_ctx, op_result))) {
      LOG_WARN("fail to process insert op", K(ret));
    }
  }

  return ret;
}

int ObTableQueryAndMutateP::try_process()
{
  int ret = OB_SUCCESS;
  // query_and_mutate request arg does not contain consisteny_level_
  // @see ObTableQueryAndMutateRequest
  const ObTableConsistencyLevel consistency_level = ObTableConsistencyLevel::STRONG;
  ObTableBatchOperation &mutations = arg_.query_and_mutate_.get_mutations();
  const ObTableOperation &mutation = mutations.at(0);
  int affected_rows = 0;
  observer::ObReqTimeGuard req_timeinfo_guard; // 引用cache资源必须加ObReqTimeGuard
  ObTableApiCacheGuard cache_guard;
  ObTableApiSpec *scan_spec = nullptr;

  if (OB_FAIL(init_scan_tb_ctx(cache_guard))) {
    LOG_WARN("fail to init scan table ctx", K(ret));
  } else if (OB_FAIL(start_trans(false, /* is_readonly */
                                 sql::stmt::T_UPDATE,
                                 consistency_level,
                                 tb_ctx_.get_ref_table_id(),
                                 tb_ctx_.get_ls_id(),
                                 get_timeout_ts()))) {
    LOG_WARN("fail to start readonly transaction", K(ret));
  } else if (OB_FAIL(tb_ctx_.init_trans(get_trans_desc(), get_tx_snapshot()))) {
    LOG_WARN("fail to init trans", K(ret), K(tb_ctx_));
  } else if (OB_FAIL(cache_guard.get_spec<TABLE_API_EXEC_SCAN>(&tb_ctx_, scan_spec))) {
    LOG_WARN("fail to get scan spec from cache", K(ret));
  } else if (ObTableOperationType::INCREMENT == mutation.type()) {
    stat_event_type_ = ObTableProccessType::TABLE_API_HBASE_INCREMENT;
    if (OB_FAIL(execute_htable_increment(*scan_spec))) {
      LOG_WARN("fail to execute hatable increment", K(ret));
    } else {
      affected_rows = 1;
    }
  } else if (ObTableOperationType::APPEND == mutation.type()) {
    stat_event_type_ = ObTableProccessType::TABLE_API_HBASE_APPEND;
    if (OB_FAIL(execute_htable_increment(*scan_spec))) {
      LOG_WARN("fail to execute hatable increment", K(ret));
    } else {
      affected_rows = 1;
    }
  } else {
    ObTableApiExecutor *executor = nullptr;
    ObTableQueryResultIterator *result_iterator = nullptr;
    ObTableApiScanRowIterator row_iter;
    if (OB_FAIL(scan_spec->create_executor(tb_ctx_, executor))) {
      LOG_WARN("fail to generate executor", K(ret), K(tb_ctx_));
    } else if (OB_FAIL(row_iter.open(static_cast<ObTableApiScanExecutor*>(executor)))) {
      LOG_WARN("fail to open scan row iterator", K(ret));
    } else if (OB_FAIL(generate_query_result(row_iter, result_iterator))) {
      LOG_WARN("fail to generate query result iterator", K(ret));
    } else {
      ObTableQueryResult *one_result = nullptr;
      // htable queryAndXXX only check one row
      ret = result_iterator->get_next_result(one_result);
      if (OB_FAIL(ret) && OB_ITER_END != ret) {
        LOG_WARN("fail to get one result", K(ret));
      } else {
        ret = OB_SUCCESS;
        one_result = &one_result_;  // empty result is OK for APPEND and INCREMENT
        switch(mutation.type()) {
          case ObTableOperationType::DEL: { // checkAndDelete
            stat_event_type_ = ObTableProccessType::TABLE_API_HBASE_CHECK_AND_DELETE;
            if (one_result->get_row_count() > 0) {  // not empty result means check passed
              if (OB_FAIL(execute_htable_delete())) {
                LOG_WARN("fail to execute hatable delete", K(ret));
              } else {
                affected_rows = 1;
              }
            }
            break;
          }
          case ObTableOperationType::INSERT_OR_UPDATE:  { // checkAndPut
            stat_event_type_ = ObTableProccessType::TABLE_API_HBASE_CHECK_AND_PUT;
            if (one_result->get_row_count() > 0) { // not empty result means check passed
              if (OB_FAIL(execute_htable_put())) {
                LOG_WARN("fail to execute hatable put", K(ret));
              } else {
                affected_rows = 1;
              }
            } else {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("insert or update with empty check result is not supported currently", K(ret));
            }
            break;
          }
          default: {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not supported mutation type", K(ret), "type", mutation.type());
            break;
          }
        }
      }

      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = row_iter.close())) {
        LOG_WARN("fail to close row iter", K(tmp_ret));
        ret = COVER_SUCC(tmp_ret);
      }
    }

    if (OB_NOT_NULL(executor)) {
      scan_spec->destroy_executor(executor);
      tb_ctx_.set_expr_info(nullptr);
    }
  }

  bool need_rollback_trans = (OB_SUCCESS != ret);
  int tmp_ret = ret;
  const bool use_sync = true;
  if (OB_FAIL(end_trans(need_rollback_trans, req_, get_timeout_ts(), use_sync))) {
    LOG_WARN("failed to end trans", K(ret), "rollback", need_rollback_trans);
  }
  ret = (OB_SUCCESS == tmp_ret) ? ret : tmp_ret;

  result_.affected_rows_ = affected_rows;

  // record events
  audit_row_count_ = 1;

  int64_t rpc_timeout = 0;
  if (NULL != rpc_pkt_) {
    rpc_timeout = rpc_pkt_->get_timeout();
  }
  #ifndef NDEBUG
    // debug mode
    LOG_INFO("[TABLE] execute query_and_mutate", K(ret), K_(arg), K(rpc_timeout),
             K_(retry_count));
  #else
    // release mode
    LOG_TRACE("[TABLE] execute query_and_mutate", K(ret), K_(arg),
              K(rpc_timeout), K_(retry_count),
              "receive_ts", get_receive_timestamp());
  #endif
  return ret;
}
