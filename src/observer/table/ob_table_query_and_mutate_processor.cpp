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
#include "ob_table_filter.h"
#include "ob_table_op_wrapper.h"

using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::share;
using namespace oceanbase::sql;

ObTableQueryAndMutateP::ObTableQueryAndMutateP(const ObGlobalContext &gctx)
    :ObTableRpcProcessor(gctx),
     allocator_(ObModIds::TABLE_PROC, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
     tb_ctx_(allocator_),
     default_entity_factory_("QueryAndMutateEntFac", MTL_ID()),
     end_in_advance_(false)
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
  } else if ((ObTableEntityType::ET_HKV == arg_.entity_type_) && !hfilter.is_valid()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "QueryAndMutate hbase model not set hfilter");
    LOG_WARN("QueryAndMutate hbase model should set hfilter", K(ret));
  } else if ((ObTableEntityType::ET_KV == arg_.entity_type_) && (1 != mutations.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tableapi query_and_mutate unexpected mutation count, expect 1", K(ret), K(mutations.count()));
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
  audit_record_.return_rows_ = result_.affected_entity_.get_row_count();
  audit_record_.table_scan_ = tb_ctx_.is_full_table_scan();
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
        case ObTableOperationType::APPEND: {
          if (OB_FAIL(ctx.init_append(false, /* returning_affected_entity_ */
                                      false /* returning_rowkey_ */))) {
            LOG_WARN("fail to init append ctx", K(ret), K(ctx));
          }
          break;
        }
        case ObTableOperationType::INCREMENT: {
          if (OB_FAIL(ctx.init_increment(false, /* returning_affected_entity_ */
                                         false /* returning_rowkey_ */))) {
            LOG_WARN("fail to init increment ctx", K(ret), K(ctx));
          }
          break;
        }
        case ObTableOperationType::INSERT_OR_UPDATE: {
          if (OB_FAIL(ctx.init_insert_up(false))) {
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
  tb_ctx_.set_entity_type(arg_.entity_type_);

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
    } else if (OB_FAIL(ObTableOpWrapper::process_insert_up_op(tb_ctx, op_result))) {
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
    lib::ob_sort(&columns.at(0), end, ColumnIdxComparator());
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
  } else if (OB_FAIL(row_iter.get_next_row(row, tb_ctx_.get_allocator()))) {
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
    ObObj &q_obj = const_cast<ObObj&>(new_q_obj); // 获取非const qualifier,为了varchar->varbinary
    if (FALSE_IT(q_obj.set_varbinary(q_obj.get_varchar()))) {
      // 将mutate的qualifier修改为varbinary
    } else if (FALSE_IT(new_range.start_key_.get_obj_ptr()[ObHTableConstants::COL_IDX_Q] = new_q_obj)) {
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
    } else if (OB_FAIL(ObTableOpWrapper::process_insert_op(tb_ctx, op_result))) {
      LOG_WARN("fail to process insert op", K(ret));
    }
  }

  return ret;
}

int ObTableQueryAndMutateP::execute_htable_mutation(ObTableQueryResultIterator *result_iterator,
                                                    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  ObTableBatchOperation &mutations = arg_.query_and_mutate_.get_mutations();
  const ObTableOperation &mutation = mutations.at(0);
  uint64_t table_id = tb_ctx_.get_ref_table_id();

  ObTableQueryResult *one_result = nullptr;
  // htable queryAndXXX only check one row
  ret = result_iterator->get_next_result(one_result);
  if (OB_FAIL(ret) && OB_ITER_END != ret) {
    LOG_WARN("fail to get one result", K(ret));
  } else {
    ret = OB_SUCCESS;
    one_result = &one_result_;  // empty result is OK for APPEND and INCREMENT
    bool expected_value_is_null = false;
    bool result_value_is_null = false;
    bool check_passed = false;
    if (OB_FAIL(check_expected_value_is_null(result_iterator, expected_value_is_null))) {
      LOG_WARN("fail to check whether expected value is null or not", K(ret));
    } else if ((one_result->get_row_count() > 0 && !expected_value_is_null)
                  || (one_result->get_row_count() == 0 && expected_value_is_null)) {
      check_passed = true;
    } else if (one_result->get_row_count() > 0 && expected_value_is_null) {
      if (OB_FAIL(check_result_value_is_null(one_result, result_value_is_null))) {
        LOG_WARN("fail to check whether result value is null or not", K(ret));
      } else if (result_value_is_null) {
        check_passed = true;
      }
    }
    if (OB_SUCC(ret) && check_passed) {
      switch(mutation.type()) {
        case ObTableOperationType::DEL: { // checkAndDelete
          stat_event_type_ = ObTableProccessType::TABLE_API_HBASE_CHECK_AND_DELETE;
          if (OB_FAIL(execute_htable_delete())) {
            LOG_WARN("fail to execute hatable delete", K(ret));
          } else {
            affected_rows = 1;
          }
          break;
        }
        case ObTableOperationType::INSERT_OR_UPDATE:  { // checkAndPut
          stat_event_type_ = ObTableProccessType::TABLE_API_HBASE_CHECK_AND_PUT;
          if (OB_FAIL(execute_htable_put())) {
            LOG_WARN("fail to execute hatable put", K(ret));
          } else {
            affected_rows = 1;
          }
          break;
        }
        default: {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "mutation type");
          LOG_WARN("not supported mutation type", K(ret), "type", mutation.type());
          break;
        }
      }
    }
  }

  return ret;
}

int ObTableQueryAndMutateP::get_rowkey_column_names(ObIArray<ObString> &names)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = tb_ctx_.get_table_schema();

  if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret), K(tb_ctx_));
  } else if (OB_FAIL(ObTableQueryUtils::get_rowkey_column_names(*table_schema, names))) {
    LOG_WARN("fail to get rowkey column names", K(ret));
  }

  return ret;
}
int ObTableQueryAndMutateP::check_expected_value_is_null(ObTableQueryResultIterator *result_iter, bool &is_null)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObHTableFilterParser filter_parser;
  hfilter::Filter *hfilter = nullptr;
  is_null = false;
  if (OB_ISNULL(result_iter)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("null result iterator", K(ret));
  } else {
    ObHTableFilterOperator *filter_op = dynamic_cast<ObHTableFilterOperator *>(result_iter);
    if (OB_ISNULL(filter_op)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("null filter operator", K(ret));
    } else {
      hfilter = filter_op->get_hfiter();
      if OB_ISNULL(hfilter) {
        // do nothing, filter string is empty, only htable key is specified
      } else {
        hfilter::CheckAndMutateFilter *query_and_mutate_filter = dynamic_cast<hfilter::CheckAndMutateFilter *>(hfilter);
        if (OB_ISNULL(query_and_mutate_filter)) {
          ret = OB_ERR_NULL_VALUE;
          LOG_WARN("null query and mutate fiter", K(ret));
        } else {
          is_null = query_and_mutate_filter->value_is_null();
        }
      }
    }
  }
  return ret;
}

int ObTableQueryAndMutateP::check_result_value_is_null(ObTableQueryResult *query_result, bool &is_null_value)
{
  int ret = OB_SUCCESS;
  const ObITableEntity *entity = nullptr;
  is_null_value = false;
  if (OB_ISNULL(query_result)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("null query result", K(ret));
  } else if (FALSE_IT(query_result->rewind())) {
  } else if (OB_FAIL(query_result->get_next_entity(entity))) {
    LOG_WARN("fail to get next entiry", K(ret));
  } else {
    ObHTableCellEntity2 htable_cell(entity);
    ObString value_str;
    if (OB_FAIL(htable_cell.get_value(value_str))) {
      LOG_WARN("fail to get value from htable cell", K(ret));
    } else if (value_str.empty()) {
      is_null_value = true;
    }
  }
  return ret;
}

int ObTableQueryAndMutateP::execute_one_mutation(ObTableQueryResult &one_result,
                                                 const ObIArray<ObString> &rk_names,
                                                 int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  ObTableBatchOperation &mutations = arg_.query_and_mutate_.get_mutations();
  const ObTableOperation &mutation = mutations.at(0);
  const ObITableEntity &mutate_entity = mutation.entity();
  const int64_t N = rk_names.count();

  bool is_check_and_insert = (mutation.type() == ObTableOperationType::INSERT);
  // 判断是否为check and insert 操作
  if (mutate_entity.get_rowkey_size() > 0 && !is_check_and_insert) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("do not set mutation rowkey in query_and_mutate, invalid mutation", K(ret), K(mutation));
  } else if (0 >= N) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowkey column names size is less than zero", K(ret));
  } else {
    const ObITableEntity *query_res_entity = nullptr;
    ObITableEntity *new_entity = nullptr;
    one_result.rewind();
    while (OB_SUCC(ret) && OB_SUCC(one_result.get_next_entity(query_res_entity))) {
      // 1. construct new entity
      if (is_check_and_insert) {
        end_in_advance_= true; // 若为check and insert, 则无需构造new_entity, 由于只插入一次, 故提前终止
      } else if (OB_ISNULL(new_entity = default_entity_factory_.alloc())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc entity", K(ret));
      } else if (OB_FAIL(new_entity->deep_copy_properties(allocator_, mutate_entity))) {
        LOG_WARN("fail to deep copy property", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
          ObObj key;
          if (OB_FAIL(query_res_entity->get_property(rk_names.at(i), key))) {
            LOG_WARN("fail to get property value", K(ret), K(rk_names.at(i)));
          } else if (OB_FAIL(new_entity->add_rowkey_value(key))) {
            LOG_WARN("fail to add rowkey value", K(ret));
          }
        }
      }

      // 2. execute
      if (OB_SUCC(ret)) {
        int64_t tmp_affect_rows = 0;
        switch (mutation.type()) {
          case ObTableOperationType::DEL: {
            if (OB_FAIL(process_dml_op<TABLE_API_EXEC_DELETE>(*new_entity, tmp_affect_rows))) {
              LOG_WARN("fail to execute table delete", K(ret));
            } else {
              affected_rows += tmp_affect_rows;
            }
            break;
          }
          case ObTableOperationType::UPDATE: {
            if (OB_FAIL(process_dml_op<TABLE_API_EXEC_UPDATE>(*new_entity, tmp_affect_rows))) {
              LOG_WARN("ail to execute table update", K(ret));
            } else {
              affected_rows += tmp_affect_rows;
            }
            break;
          }
          case ObTableOperationType::INCREMENT:
          case ObTableOperationType::APPEND: {
            ret = process_insert_up(*new_entity, tmp_affect_rows);
            if (OB_SUCC(ret)) {
              affected_rows += tmp_affect_rows;
            }
            break;
          }
          case ObTableOperationType::INSERT: { // 使用mutation上的entity执行insert
            ret = process_insert(mutate_entity, tmp_affect_rows);
            if (OB_SUCC(ret)) {
              affected_rows += tmp_affect_rows;
            }
            break;
          }
          default: {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "mutation type");
            LOG_WARN("not supported mutation type", K(ret), "type", mutation.type());
            break;
          }
        }
      }
    }
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }

  return ret;
}

// maybe scan multi result row, so mutate all result rows.
int ObTableQueryAndMutateP::execute_table_mutation(ObTableQueryResultIterator *result_iterator,
                                                   int64_t &affected_rows)
{
  int ret = OB_SUCCESS;

  ObSEArray<ObString, 8> rowkey_column_names;
  if (OB_FAIL(get_rowkey_column_names(rowkey_column_names))) {
    LOG_WARN("fail to get rowkey column names", K(ret));
  } else {
    stat_event_type_ = ObTableProccessType::TABLE_API_QUERY_AND_MUTATE;
    ObTableQueryResult *one_result = nullptr;
    while (OB_SUCC(ret) && !end_in_advance_) { // 在check and insert 情境下会有提前终止
      if (ObTimeUtility::current_time() > get_timeout_ts()) {
        ret = OB_TRANS_TIMEOUT;
        LOG_WARN("exceed operatiton timeout", K(ret));
      } else if (OB_FAIL(result_iterator->get_next_result(one_result))) { // one_result may include multi row
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next result", K(ret));
        }
      } else if (OB_FAIL(execute_one_mutation(*one_result, rowkey_column_names, affected_rows))) {
        LOG_WARN("fail to execute one mutation", K(ret), K(rowkey_column_names));
      } else if (arg_.query_and_mutate_.return_affected_entity()) {
        if (result_.affected_entity_.get_property_count() <= 0
            && OB_FAIL(result_.affected_entity_.add_all_property(*one_result))) {
          LOG_WARN("fail to add property", K(ret));
        } else if (OB_FAIL(result_.affected_entity_.add_all_row(*one_result))) {
          LOG_WARN("fail to add all rows", K(ret));
        }
      }
      if (OB_SUCC(ret) && !result_iterator->has_more_result()) {
        ret = OB_ITER_END;
      }
    }
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }

  return ret;
}

int ObTableQueryAndMutateP::try_process()
{
  int ret = OB_SUCCESS;
  // query_and_mutate request arg does not contain consisteny_level_
  // @see ObTableQueryAndMutateRequest
  const ObTableConsistencyLevel consistency_level = ObTableConsistencyLevel::STRONG;
  const ObTableQuery &query = arg_.query_and_mutate_.get_query();
  ObTableBatchOperation &mutations = arg_.query_and_mutate_.get_mutations();
  const ObTableOperation &mutation = mutations.at(0);
  observer::ObReqTimeGuard req_timeinfo_guard; // 引用cache资源必须加ObReqTimeGuard
  ObTableApiCacheGuard cache_guard;
  ObTableApiSpec *scan_spec = nullptr;
  int64_t affected_rows = 0;
  const bool is_hkv = (ObTableEntityType::ET_HKV == arg_.entity_type_);
  ObHTableLockHandle *lock_handle = nullptr;

  if (OB_FAIL(init_scan_tb_ctx(cache_guard))) {
    LOG_WARN("fail to init scan table ctx", K(ret));
  } else if (FALSE_IT(table_id_ = arg_.table_id_)) {
  } else if (FALSE_IT(tablet_id_ = arg_.tablet_id_)) {
  } else if (is_hkv && OB_FAIL(HTABLE_LOCK_MGR->acquire_handle(lock_handle))) {
    LOG_WARN("fail to get htable lock handle", K(ret));
  } else if (is_hkv && OB_FAIL(ObHTableUtils::lock_htable_row(table_id_, query, *lock_handle, ObHTableLockMode::EXCLUSIVE))) {
    LOG_WARN("fail to lock htable row", K(ret), K_(table_id), K(query));
  } else if (OB_FAIL(start_trans(false, /* is_readonly */
                                 sql::stmt::T_UPDATE,
                                 consistency_level,
                                 table_id_,
                                 tb_ctx_.get_ls_id(),
                                 get_timeout_ts()))) {
    LOG_WARN("fail to start readonly transaction", K(ret));
  } else if (OB_FAIL(tb_ctx_.init_trans(get_trans_desc(), get_tx_snapshot()))) {
    LOG_WARN("fail to init trans", K(ret), K(tb_ctx_));
  } else if (is_hkv && FALSE_IT(lock_handle->set_tx_id(get_trans_desc()->tid()))) {
  } else if (OB_FAIL(cache_guard.get_spec<TABLE_API_EXEC_SCAN>(&tb_ctx_, scan_spec))) {
    LOG_WARN("fail to get scan spec from cache", K(ret));
  } else if (is_hkv && ObTableOperationType::INCREMENT == mutation.type()) {
    stat_event_type_ = ObTableProccessType::TABLE_API_HBASE_INCREMENT;
    if (OB_FAIL(execute_htable_increment(*scan_spec))) {
      LOG_WARN("fail to execute hatable increment", K(ret));
    } else {
      affected_rows = 1;
    }
  } else if (is_hkv && ObTableOperationType::APPEND == mutation.type()) {
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
    } else if (OB_FAIL(ObTableQueryUtils::generate_query_result_iterator(allocator_,
                                                                         arg_.query_and_mutate_.get_query(),
                                                                         is_hkv,
                                                                         one_result_,
                                                                         tb_ctx_,
                                                                         result_iterator))) {
      LOG_WARN("fail to generate query result iterator", K(ret));
    } else if (FALSE_IT(result_iterator->set_scan_result(&row_iter))) {
      // do nothing
    } else if (is_hkv) {
      if (OB_FAIL(execute_htable_mutation(result_iterator, affected_rows))) {
        LOG_WARN("fail to execute htable mutation", K(ret));
      }
    } else { // tableapi
      if (OB_FAIL(execute_table_mutation(result_iterator, affected_rows))) {
       LOG_WARN("fail to execute table mutation", K(ret));
      }
    }

    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = row_iter.close())) {
      LOG_WARN("fail to close row iter", K(tmp_ret));
      ret = COVER_SUCC(tmp_ret);
    }

    if (OB_NOT_NULL(scan_spec)) {
      scan_spec->destroy_executor(executor);
      tb_ctx_.set_expr_info(nullptr);
    }
    ObTableQueryUtils::destroy_result_iterator(result_iterator);
  }

  bool need_rollback_trans = (OB_SUCCESS != ret);
  int tmp_ret = ret;
  const bool use_sync = true;
  if (OB_FAIL(end_trans(need_rollback_trans, req_, get_timeout_ts(), use_sync, lock_handle))) {
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
    LOG_INFO("[TABLE] execute query_and_mutate", K(ret), K(rpc_timeout), K_(retry_count));
  #else
    // release mode
    LOG_TRACE("[TABLE] execute query_and_mutate", K(ret), K(rpc_timeout), K_(retry_count),
              "receive_ts", get_receive_timestamp());
  #endif
  return ret;
}
