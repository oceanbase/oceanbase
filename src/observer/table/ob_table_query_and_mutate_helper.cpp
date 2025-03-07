/**
 * Copyright (c) 2024 OceanBase
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
#include "ob_table_query_and_mutate_helper.h"
#include "ob_htable_filter_operator.h"

using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::share;
using namespace oceanbase::sql;
using namespace oceanbase::sql::stmt;

int QueryAndMutateHelper::init_tb_ctx(ObTableCtx &ctx,
                                      ObTableOperationType::Type op_type,
                                      const ObITableEntity &entity)
{
  int ret = OB_SUCCESS;
  ObExprFrameInfo *expr_frame_info = nullptr;
  ctx.set_entity(&entity);
  ctx.set_entity_type(entity_type_);
  ctx.set_operation_type(op_type);
  if (ctx.is_htable()) {
    ctx.set_batch_operation(&query_and_mutate_.get_mutations().get_table_operations());
  }
  ctx.set_schema_guard(schema_guard_);
  ctx.set_simple_table_schema(simple_table_schema_);
  ctx.set_schema_cache_guard(schema_cache_guard_);
  ctx.set_sess_guard(sess_guard_);
  ctx.set_audit_ctx(&audit_ctx_);
  const int64_t timeout_ts = -1;

  if (!ctx.is_init()) {
    if (OB_FAIL(ctx.init_common(credential_, tablet_id_, timeout_ts_))) {
      LOG_WARN("fail to init table ctx common part", K(ret));
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

int QueryAndMutateHelper::init_scan_tb_ctx(ObTableApiCacheGuard &cache_guard)
{
  int ret = OB_SUCCESS;
  ObExprFrameInfo *expr_frame_info = nullptr;
  const ObTableQuery &query = query_and_mutate_.get_query();
  bool is_weak_read = false;
  tb_ctx_.set_scan(true);
  tb_ctx_.set_entity_type(entity_type_);
  tb_ctx_.set_schema_cache_guard(schema_cache_guard_);
  tb_ctx_.set_simple_table_schema(simple_table_schema_);
  tb_ctx_.set_schema_guard(schema_guard_);
  tb_ctx_.set_sess_guard(sess_guard_);
  if (tb_ctx_.is_init()) {
    LOG_INFO("tb ctx has been inited", K_(tb_ctx));
  } else if (OB_FAIL(tb_ctx_.init_common(const_cast<ObTableApiCredential&>(credential_),
                                         tablet_id_,
                                         timeout_ts_))) {
    LOG_WARN("fail to init table ctx common part", K(ret));
  } else if (OB_FAIL(tb_ctx_.init_scan(query, is_weak_read, table_id_))) {
    LOG_WARN("fail to init table ctx scan part", K(ret));
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

int QueryAndMutateHelper::execute_htable_delete(const ObTableOperation &table_operation, ObTableCtx *&tb_ctx)
{
  int ret = OB_SUCCESS;
  OB_TABLE_START_AUDIT(credential_,
                      *sess_guard_,
                      simple_table_schema_->get_table_name_str(),
                      &audit_ctx_, table_operation);
  const ObITableEntity &entity = table_operation.entity();
  observer::ObReqTimeGuard req_timeinfo_guard;  // 引用cache资源必须加ObReqTimeGuard
  ObTableApiCacheGuard cache_guard;
  ObTableApiExecutor *executor = nullptr;
  ObTableApiSpec *spec = nullptr;
  if (OB_ISNULL(tb_ctx)) {
    if (OB_ISNULL(tb_ctx = OB_NEWx(ObTableCtx, (&allocator_), allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create mutate table ctx", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (!tb_ctx->is_init()) {
      if (OB_FAIL(init_tb_ctx(*tb_ctx, ObTableOperationType::Type::DEL, entity))) {
        LOG_WARN("fail to init table ctx", K(ret));
      } else if (OB_FAIL(tb_ctx->init_trans(get_trans_desc(), get_tx_snapshot()))) {
        LOG_WARN("fail to init trans", K(ret), K(tb_ctx));
      }
    } else {
      tb_ctx->set_entity(&entity);
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObTableOpWrapper::get_or_create_spec<TABLE_API_EXEC_DELETE>(*tb_ctx, cache_guard, spec))) {
        LOG_WARN("fail to get spec from cache", K(ret));
      } else if (OB_FAIL(spec->create_executor(*tb_ctx, executor))) {
        LOG_WARN("fail to generate executor", K(ret), K(tb_ctx));
      } else {
        ObHTableDeleteExecutor delete_executor(*tb_ctx, static_cast<ObTableApiDeleteExecutor *>(executor), audit_ctx_);
        if (OB_FAIL(delete_executor.open())) {
          LOG_WARN("fail to open htable delete executor", K(ret));
        } else if (OB_FAIL(delete_executor.get_next_row_by_entity())) {
          LOG_WARN("fail to call htable delete get_next_row", K(ret));
        }

        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = delete_executor.close())) {
          LOG_WARN("fail to close htable delete executor", K(tmp_ret));
          ret = COVER_SUCC(tmp_ret);
        }
      }
    }
    if (OB_NOT_NULL(spec)) {
      spec->destroy_executor(executor);
    }
  }
  OB_TABLE_END_AUDIT(ret_code, ret,
                     snapshot, get_tx_snapshot(),
                     stmt_type, StmtType::T_KV_DELETE);
  return ret;
}

int QueryAndMutateHelper::execute_htable_put(const ObTableOperation &table_operation, ObTableCtx *&tb_ctx)
{
  int ret = OB_SUCCESS;
  OB_TABLE_START_AUDIT(credential_,
                      *sess_guard_,
                      simple_table_schema_->get_table_name_str(),
                      &audit_ctx_, table_operation);
  const ObITableEntity &entity = table_operation.entity();
  if (OB_ISNULL(tb_ctx)) {
    if (OB_ISNULL(tb_ctx = OB_NEWx(ObTableCtx, (&allocator_), allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create mutate table ctx", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (!tb_ctx->is_init()) {
      if (OB_FAIL(init_tb_ctx(*tb_ctx, ObTableOperationType::Type::INSERT_OR_UPDATE, entity))) {
        LOG_WARN("fail to init table ctx", K(ret));
      } else if (OB_FAIL(tb_ctx->init_trans(get_trans_desc(), get_tx_snapshot()))) {
        LOG_WARN("fail to init trans", K(ret), K(tb_ctx));
      }
    } else {
      tb_ctx->set_entity(&entity);
      if (OB_FAIL(tb_ctx->adjust_entity())) {
        LOG_WARN("fail to adjust entity", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObTableOperationResult op_result;
      if (OB_FAIL(ObTableOpWrapper::process_insert_up_op(*tb_ctx, op_result))) {
        LOG_WARN("fail to process insert up op", K(ret));
      }
    }
  }

  OB_TABLE_END_AUDIT(ret_code, ret,
                     snapshot, get_tx_snapshot(),
                     stmt_type, StmtType::T_KV_PUT);
  return ret;
}

class QueryAndMutateHelper::ColumnIdxComparator
{
public:
  bool operator()(const ColumnIdx &a, const ColumnIdx &b) const
  {
    return a.first.compare(b.first) < 0;
  }
};

int QueryAndMutateHelper::sort_qualifier(common::ObIArray<ColumnIdx> &columns,
                                         const ObTableBatchOperation &increment)
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

int QueryAndMutateHelper::add_to_results(const ObTableEntity &new_entity)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(query_and_mutate_result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null query and mutate result", K(ret));
  } else {
    ObTableQueryResult &results = query_and_mutate_result_->affected_entity_;

    if (results.get_property_count() <= 0) {
      if (OB_FAIL(results.append_property_names(new_entity.get_rowkey_names()))) {
        LOG_WARN("failed to deep copy name", K(ret), K(new_entity));
      } else if (OB_FAIL(results.append_property_names(new_entity.get_properties_names()))) {
        LOG_WARN("failed to deep copy name", K(ret), K(new_entity));
      }
    }
    if (OB_SUCC(ret)) {
      const ObIArray<ObObj> &rowkey_objs = new_entity.get_rowkey_objs();
      const ObIArray<ObObj> &properties_objs = new_entity.get_properties_values();
      int num = rowkey_objs.count() + properties_objs.count();
      ObObj properties_values[num];
      int i = 0;
      for (; i < rowkey_objs.count(); i++) {
        properties_values[i] = rowkey_objs.at(i);
      }
      for (; i < num; i++) {
        properties_values[i] = properties_objs.at(i - rowkey_objs.count());
      }
      common::ObNewRow row(properties_values, num);
      if (OB_FAIL(results.add_row(row))) {
        LOG_WARN("failed to add row to results", K(ret), K(row));
      }
    }
  }
  return ret;
}

int QueryAndMutateHelper::generate_new_value(const ObITableEntity *old_entity,
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
    if (OB_NOT_NULL(old_entity)) { // 旧行存在，构造新值（base + delta）
      ObObj base_obj_t, base_obj_v;
      if (OB_FAIL(old_entity->get_rowkey_value(ObHTableConstants::COL_IDX_T, base_obj_t))) {
        LOG_WARN("failed to get timestamp", K(ret), K(old_entity));
      } else if (OB_FAIL(old_entity->get_property(ObHTableConstants::VALUE_CNAME_STR, base_obj_v))) {
        LOG_WARN("failed to get value", K(ret), K(old_entity));
      } else {
        orig_str = base_obj_v.get_varbinary();
        int64_t orig_ts = -base_obj_t.get_int();
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
      ObObj new_k, new_q, new_t, new_v, new_ttl;
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
        bool cell_ttl_exist = false;
        int64_t ttl = INT64_MAX;
        if (OB_FAIL(new_entity.set_rowkey(ObHTableConstants::ROWKEY_CNAME_STR, new_k))) {
          LOG_WARN("fail to add rowkey value", K(ret), K(new_k));
        } else if (OB_FAIL(new_entity.set_rowkey(ObHTableConstants::CQ_CNAME_STR, new_q))) {
          LOG_WARN("fail to add rowkey value", K(ret), K(new_q));
        } else if (OB_FAIL(new_entity.set_rowkey(ObHTableConstants::VERSION_CNAME_STR, new_t))) {
          LOG_WARN("fail to add rowkey value", K(ret), K(new_t));
        } else if (OB_FAIL(new_entity.set_property(ObHTableConstants::VALUE_CNAME_STR, new_v))) {
          LOG_WARN("fail to set value property", K(ret), K(new_v));
        } else if (OB_FAIL(schema_cache_guard_->has_hbase_ttl_column(cell_ttl_exist))) {
          LOG_WARN("fail to check cell ttl", K(ret));
        } else if (cell_ttl_exist) {
          // TTL
          if (OB_FAIL(htable_cell.get_ttl(ttl))) {
            if (ret == OB_SEARCH_NOT_FOUND) {
              ret = OB_SUCCESS;
              if (OB_NOT_NULL(old_entity) &&
                  OB_FAIL(old_entity->get_property(ObHTableConstants::TTL_CNAME_STR, new_ttl))) {
                if (ret == OB_SEARCH_NOT_FOUND) {
                  ret = OB_SUCCESS;
                } else {
                  LOG_WARN("failed to get cell ttl", K(ret), K(old_entity));
                }
              }
            } else {
              LOG_WARN("failed to get cell ttl", K(ret), K(htable_cell));
            }
          } else if (!htable_cell.last_get_is_null()) {
            new_ttl.set_int(ttl);
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(new_entity.set_property(ObHTableConstants::TTL_CNAME_STR, new_ttl))) {
            LOG_WARN("fail to set ttl property", K(ret), K(new_ttl));
          }
        }
      }
      if (OB_SUCC(ret) && query_and_mutate_.return_affected_entity()) { // set return accected entity
        if (OB_FAIL(add_to_results(new_entity))) {
          LOG_WARN("fail to add to results", K(ret), K(new_k), K(new_q), K(new_t), K(new_v), K(new_ttl));
        }
      }
    }
  }

  return ret;
}

// 1. 执行query获取到最新版本旧行的V
// 2. 基于旧V执行mutate
int QueryAndMutateHelper::execute_htable_inc_or_append(ObTableQueryResult *result)
{
  int ret = OB_SUCCESS;
  const ObTableBatchOperation &mutations = query_and_mutate_.get_mutations();
  int64_t N = mutations.count();
  ObSEArray<ObITableEntity*, 8> entities;
  ObSEArray<ColumnIdx, OB_DEFAULT_SE_ARRAY_COUNT> columns;
  if (OB_FAIL(sort_qualifier(columns, mutations))) {
    LOG_WARN("fail to sort qualifier", K(ret), K(mutations));
  } else if (OB_FAIL(result->get_htable_all_entity(entities))) {
    LOG_WARN("fail to get all entity", K(ret));
  }
  ObITableEntity *old_entity = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); i++) {
    old_entity = nullptr;
    ObTableEntity new_entity;
    const ObTableOperation &op = mutations.at(columns.at(i).second);
    bool is_increment = ObTableOperationType::INCREMENT == op.type();
    const ObRowkey &rowkey = op.entity().get_rowkey();
    const ObObj &q_obj = rowkey.get_obj_ptr()[ObHTableConstants::COL_IDX_Q];  // column Q
    ObObj tmp_obj;
    for (int j = 0; OB_SUCC(ret) && j < entities.count(); j++) {
      if (OB_FAIL(entities.at(j)->get_rowkey_value(ObHTableConstants::COL_IDX_Q, tmp_obj))) {
        LOG_WARN("fail to get properties", K(ret));
      } else if (q_obj.get_string().compare(tmp_obj.get_string()) == 0) {
        old_entity = entities.at(j);
        break;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(generate_new_value(old_entity, op.entity(), is_increment, new_entity))) {
      LOG_WARN("fail to generate new value", K(ret), K(op.entity()), KP(old_entity));
    } else if (OB_FAIL(execute_htable_insert(new_entity))) {
      LOG_WARN("fail to execute hatable insert", K(ret));
    }
  }

  return ret;
}

int QueryAndMutateHelper::execute_htable_insert(const ObITableEntity &new_entity)
{
  int ret = OB_SUCCESS;
  ObTableOperation op;
  op.set_type(ObTableOperationType::Type::INSERT);
  op.set_entity(new_entity);
  OB_TABLE_START_AUDIT(credential_,
                       *sess_guard_,
                       simple_table_schema_->get_table_name_str(),
                       &audit_ctx_,
                       op);

  SMART_VAR(ObTableCtx, tb_ctx, allocator_) {
    ObTableApiSpec *spec = nullptr;
    ObTableApiExecutor *executor = nullptr;
    ObTableOperationResult op_result;
    if (OB_FAIL(init_tb_ctx(tb_ctx, ObTableOperationType::Type::INSERT, new_entity))) {
      LOG_WARN("fail to init table ctx", K(ret));
    } else if (OB_FAIL(tb_ctx.init_trans(get_trans_desc(), get_tx_snapshot()))) {
      LOG_WARN("fail to init trans", K(ret), K(tb_ctx));
    } else if (OB_FAIL(ObTableOpWrapper::process_insert_op(tb_ctx, op_result))) {
      LOG_WARN("fail to process insert op", K(ret));
    }
  }

  OB_TABLE_END_AUDIT(ret_code, ret,
                     snapshot, get_tx_snapshot(),
                     stmt_type, StmtType::T_KV_INSERT);
  return ret;
}

int QueryAndMutateHelper::execute_htable_mutation(ObTableQueryResultIterator *result_iterator)
{
  int ret = OB_SUCCESS;
  const ObTableBatchOperation &mutations = query_and_mutate_.get_mutations();
  const ObTableOperation &mutation = mutations.at(0);
  uint64_t table_id = tb_ctx_.get_ref_table_id();
  OB_TABLE_START_AUDIT(credential_,
                       *tb_ctx_.get_sess_guard(),
                       tb_ctx_.get_table_name(),
                       &audit_ctx_,
                       query_and_mutate_.get_query());

  ObTableQueryResult *one_result = nullptr;
  // htable queryAndXXX only check one row
  ret = result_iterator->get_next_result(one_result);
  OB_TABLE_END_AUDIT(ret_code, ret,
                    snapshot, get_tx_snapshot(),
                    stmt_type, StmtType::T_KV_QUERY,
                    return_rows, (OB_ISNULL(one_result) ? 0 : one_result->get_row_count()),
                    has_table_scan, true,
                    filter, (OB_ISNULL(result_iterator) ? nullptr : result_iterator->get_filter()));
  if (OB_FAIL(ret) && OB_ITER_END != ret) {
    LOG_WARN("fail to get one result", K(ret));
  } else {
    ret = OB_SUCCESS;  // override ret
    if (ObTableOperationType::INCREMENT == mutation.type() || ObTableOperationType::APPEND == mutation.type()) {
      if (OB_FAIL(execute_htable_inc_or_append(one_result))) {
        LOG_WARN("fail to execute hatable increment", K(ret));
      } else {
        set_result_affected_rows(1);
      }
    } else {
      one_result = &one_result_;  // empty result is OK for APPEND and INCREMENT
      bool expected_value_is_null = false;
      bool result_value_is_null = false;
      bool check_passed = false;
      if (OB_FAIL(check_expected_value_is_null(result_iterator, expected_value_is_null))) {
        LOG_WARN("fail to check whether expected value is null or not", K(ret));
      } else if ((one_result->get_row_count() > 0 && !expected_value_is_null) ||
                 (one_result->get_row_count() == 0 && expected_value_is_null)) {
        check_passed = true;
      } else if (one_result->get_row_count() > 0 && expected_value_is_null) {
        if (OB_FAIL(check_result_value_is_null(one_result, result_value_is_null))) {
          LOG_WARN("fail to check whether result value is null or not", K(ret));
        } else if (result_value_is_null) {
          check_passed = true;
        }
      }
      if (OB_SUCC(ret) && check_passed) {
        uint64_t N = mutations.count();
        ObTableCtx *put_tb_ctx = nullptr;
        ObTableCtx *del_tb_ctx = nullptr;
        for (int i = 0; i < N && OB_SUCC(ret); i++) {
          const ObTableOperation &op = mutations.at(i);
          switch (op.type()) {
            case ObTableOperationType::DEL: {  // checkAndDelete
              if (OB_FAIL(execute_htable_delete(op, del_tb_ctx))) {
                LOG_WARN("fail to execute hatable delete", K(ret), K(op), K(del_tb_ctx));
              }
              break;
            }
            case ObTableOperationType::INSERT_OR_UPDATE: {  // checkAndPut
              if (OB_FAIL(execute_htable_put(op, put_tb_ctx))) {
                LOG_WARN("fail to execute hatable insert", K(ret), K(op), K(del_tb_ctx));
              }
              break;
            }
            default: {
              ret = OB_NOT_SUPPORTED;
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "mutation type");
              LOG_WARN("not supported mutation type", K(ret), "type", op.type(), "index", i);
              break;
            }
          }
        }
        if (OB_SUCC(ret)) {
          set_result_affected_rows(1);
        }
        if (OB_NOT_NULL(put_tb_ctx)) {
          put_tb_ctx->~ObTableCtx();
        }
        if (OB_NOT_NULL(del_tb_ctx)) {
          del_tb_ctx->~ObTableCtx();
        }
      }
    }
  }
  return ret;
}

int QueryAndMutateHelper::get_rowkey_column_names(ObIArray<ObString> &names)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(schema_cache_guard_) || !schema_cache_guard_->is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema cache guard is NULL or not inited", K(ret), KP(schema_cache_guard_));
  } else if (OB_FAIL(ObTableQueryUtils::get_rowkey_column_names(*schema_cache_guard_, names))) {
    LOG_WARN("fail to get rowkey column names", K(ret));
  }

  return ret;
}

int QueryAndMutateHelper::check_expected_value_is_null(ObTableQueryResultIterator *result_iter, bool &is_null)
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
      hfilter = filter_op->get_filter();
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

int QueryAndMutateHelper::check_result_value_is_null(ObTableQueryResult *query_result, bool &is_null_value)
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

int QueryAndMutateHelper::execute_one_mutation(ObIAllocator &allocator,
                                               ObTableQueryResult &one_result,
                                               const ObIArray<ObString> &rk_names,
                                               int64_t &affected_rows,
                                               bool &end_in_advance)
{
  int ret = OB_SUCCESS;
  const ObTableBatchOperation &mutations = query_and_mutate_.get_mutations();
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
        end_in_advance = true; // 若为check and insert, 则无需构造new_entity, 由于只插入一次, 故提前终止
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
            ret = process_incr_or_append_op(*new_entity, tmp_affect_rows);
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
int QueryAndMutateHelper::execute_table_mutation(ObTableQueryResultIterator *result_iterator)
{
  int ret = OB_SUCCESS;

  ObSEArray<ObString, 8> rowkey_column_names;
  int64_t affected_rows = 0;
  if (OB_FAIL(get_rowkey_column_names(rowkey_column_names))) {
    LOG_WARN("fail to get rowkey column names", K(ret));
  } else {
    ObTableQueryResult *one_result = nullptr;
    bool end_in_advance = false;
    while (OB_SUCC(ret) && !end_in_advance) { // 在check and insert 情境下会有提前终止
      OB_TABLE_START_AUDIT(credential_,
                           *tb_ctx_.get_sess_guard(),
                           tb_ctx_.get_table_name(),
                           &audit_ctx_,
                           query_and_mutate_.get_query());
      if (ObTimeUtility::current_time() > get_timeout_ts()) {
        ret = OB_TRANS_TIMEOUT;
        LOG_WARN("exceed operatiton timeout", K(ret));
      } else if (OB_FAIL(result_iterator->get_next_result(one_result))) { // one_result may include multi row
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next result", K(ret));
        }
      }
      OB_TABLE_END_AUDIT(ret_code, ret,
                         snapshot, get_tx_snapshot(),
                         stmt_type, StmtType::T_KV_QUERY,
                         return_rows, (OB_ISNULL(one_result) ? 0 : one_result->get_row_count()),
                         has_table_scan, true,
                         filter, (OB_ISNULL(result_iterator) ? nullptr : result_iterator->get_filter()));

      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(execute_one_mutation(allocator_, *one_result, rowkey_column_names, affected_rows, end_in_advance))) {
        LOG_WARN("fail to execute one mutation", K(ret), K(rowkey_column_names));
      } else if (query_and_mutate_.return_affected_entity() && OB_NOT_NULL(query_and_mutate_result_)) {
        if (query_and_mutate_result_->affected_entity_.get_property_count() <= 0 &&
            OB_FAIL(query_and_mutate_result_->affected_entity_.deep_copy_property_names(one_result->get_select_columns()))) {
          LOG_WARN("fail to add property", K(ret));
        } else if (OB_FAIL(query_and_mutate_result_->affected_entity_.add_all_row(*one_result))) {
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

  if (OB_SUCC(ret)) {
    set_result_affected_rows(affected_rows);
  }

  return ret;
}

int QueryAndMutateHelper::execute_query_and_mutate()
{
  int ret = OB_SUCCESS;
  observer::ObReqTimeGuard req_timeinfo_guard; // 引用cache资源必须加ObReqTimeGuard
  ObTableApiCacheGuard cache_guard;
  ObTableApiSpec *scan_spec = nullptr;
  set_result_affected_rows(0);
  const ObTableBatchOperation &mutations = query_and_mutate_.get_mutations();
  const ObTableOperation &mutation = mutations.at(0);
  bool is_check_and_execute = query_and_mutate_.is_check_and_execute();
  if (OB_FAIL(init_scan_tb_ctx(cache_guard))) {
    LOG_WARN("fail to init scan table ctx", K(ret));
  } else if (OB_FAIL(tb_ctx_.init_trans(get_trans_desc(), get_tx_snapshot()))) {
    LOG_WARN("fail to init trans", K(ret), K(tb_ctx_));
  } else if (OB_FAIL(cache_guard.get_spec<TABLE_API_EXEC_SCAN>(&tb_ctx_, scan_spec))) {
    LOG_WARN("fail to get scan spec from cache", K(ret));
  } else {
    ObTableApiExecutor *executor = nullptr;
    ObTableQueryResultIterator *result_iterator = nullptr;
    ObTableApiScanRowIterator *row_iter = nullptr;
    if (OB_FAIL(ObTableQueryUtils::get_scan_row_interator(tb_ctx_, row_iter))) {
      LOG_WARN("fail to get scan row iterator", K(ret));
    } else if (OB_FAIL(scan_spec->create_executor(tb_ctx_, executor))) {
      LOG_WARN("fail to generate executor", K(ret), K(tb_ctx_));
    } else if (OB_FAIL(row_iter->open(static_cast<ObTableApiScanExecutor*>(executor)))) {
      LOG_WARN("fail to open scan row iterator", K(ret));
    } else if (OB_FAIL(ObTableQueryUtils::generate_query_result_iterator(allocator_,
                                                                         query_and_mutate_.get_query(),
                                                                         is_hkv_,
                                                                         one_result_,
                                                                         tb_ctx_,
                                                                         result_iterator))) {
      LOG_WARN("fail to generate query result iterator", K(ret));
    } else if (FALSE_IT(result_iterator->set_scan_result(row_iter))) {
      // do nothing
    } else if (is_hkv_) {
      if (OB_FAIL(execute_htable_mutation(result_iterator))) {
        LOG_WARN("fail to execute htable mutation", K(ret));
      }
    } else { // tableapi
      if (!is_check_and_execute) {
        if (OB_FAIL(execute_table_mutation(result_iterator))) {
          LOG_WARN("fail to execute table mutation", K(ret));
        }
      } else {
        if (OB_FAIL(check_and_execute(result_iterator))) {
          LOG_WARN("fail to execute check and execute", K(ret));
        }
      }
    }

    if (OB_NOT_NULL(row_iter)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = row_iter->close())) {
        LOG_WARN("fail to close row iter", K(tmp_ret));
        ret = COVER_SUCC(tmp_ret);
      }
    }

    if (OB_NOT_NULL(scan_spec)) {
      scan_spec->destroy_executor(executor);
      tb_ctx_.set_expr_info(nullptr);
    }
    ObTableQueryUtils::destroy_result_iterator(result_iterator);
  }
  return ret;
}

int QueryAndMutateHelper::check_and_execute(ObTableQueryResultIterator *result_iterator)
{
  int ret = OB_SUCCESS;
  const ObTableBatchOperation &mutations = query_and_mutate_.get_mutations();
  const ObITableEntity &mutate_entity = mutations.at(0).entity();
  ObTableQueryResult *one_result = nullptr;
  bool check_exists = query_and_mutate_.is_check_exists();
  bool rollback_when_check_failed = query_and_mutate_.rollback_when_check_failed();
  bool check_passed = false;
  int64_t affected_rows = 0;
  OB_TABLE_START_AUDIT(credential_,
                       *tb_ctx_.get_sess_guard(),
                       tb_ctx_.get_table_name(),
                       &audit_ctx_,
                       query_and_mutate_.get_query());

  if (OB_FAIL(result_iterator->get_next_result(one_result))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next result", K(ret));
    }
  }
  OB_TABLE_END_AUDIT(ret_code, ret,
                     snapshot, get_tx_snapshot(),
                     stmt_type, StmtType::T_KV_QUERY,
                     return_rows, (OB_ISNULL(one_result) ? 0 : one_result->get_row_count()),
                     has_table_scan, true,
                     filter, (OB_ISNULL(result_iterator) ? nullptr : result_iterator->get_filter()));

  if (OB_SUCC(ret)) {
    if (one_result->get_row_count() > 0 && check_exists) {
      check_passed = true;
    } else if (one_result->get_row_count() == 0 && !check_exists) {
      check_passed = true;
    }
  } else if (ret == OB_ITER_END) {
    check_passed = !check_exists;
    ret = OB_SUCCESS;
  } else {
    // do nothing
  }

  if (OB_SUCC(ret)) {
    if (!check_passed) { // check failed
      if (rollback_when_check_failed) {
        ObString op = ObString::make_string("CheckAndInsUp");
        ret = OB_KV_CHECK_FAILED;
        LOG_WARN("check failed and rollback", K(ret));
        LOG_USER_ERROR(OB_KV_CHECK_FAILED, op.length(), op.ptr());
      } else {
        // do nothing
      }
    } else { // check passed
      switch (mutations.at(0).type()) {
        case ObTableOperationType::INSERT_OR_UPDATE: {
          ret = process_insert_up(mutate_entity, affected_rows);
          break;
        }
        default: {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not supported mutation type", K(ret), "type", mutations.at(0).type());
          break;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    set_result_affected_rows(affected_rows);
  }

#ifndef NDEBUG
  // debug mode
  LOG_INFO("[TABLE] execute check and execute", K(ret), K(check_passed), K(affected_rows), K(mutations));
#else
  // release mode
  LOG_TRACE("[TABLE] execute check and execute", K(ret), K(check_passed), K(affected_rows), K(mutations));
#endif

  return ret;
}