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

#include "ob_redis_operator.h"
#include "src/observer/table/ob_table_query_and_mutate_helper.h"

using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace table
{
const ObString CommandOperator::COUNT_STAR_PROPERTY = "count(*)";

int CommandOperator::process_table_query_count(ObIAllocator &allocator, const ObTableQuery &query,
                                               int64_t &row_count)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObTableCtx, tb_ctx, allocator)
  {
    QUERY_ITER_START(redis_ctx_, query, tb_ctx, iter)
    row_count = 0;
    ObTableQueryResult *one_result = nullptr;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter->get_next_result(one_result))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next result", K(ret));
        }
      } else {
        row_count += one_result->get_row_count();
      }
    }
    QUERY_ITER_END(iter)
  }
  return ret;
}

// note: caller should start trans before
int CommandOperator::process_table_batch_op(const ObTableBatchOperation &req_ops,
                                            ResultFixedArray &results,
                                            bool returning_affected_entity /* = false*/,
                                            bool returning_rowkey /* = false*/)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(req_ops.count() == 0)) {
    // do nothing, return empty results
  } else {
    ObTableEntity result_entity;
    SMART_VAR(ObTableBatchCtx,
              batch_ctx,
              redis_ctx_.allocator_,
              const_cast<ObTableAuditCtx &>(*redis_ctx_.tb_ctx_.get_audit_ctx()))
    {
      if (OB_FAIL(results.init(req_ops.count()))) {
        LOG_WARN("fail to init results", K(ret), K(req_ops.count()));
      } else {
        batch_ctx.ops_ = &req_ops.get_table_operations();
        batch_ctx.entity_factory_ = redis_ctx_.entity_factory_;
        batch_ctx.result_entity_ = &result_entity;
        batch_ctx.results_ = &results;

        if (OB_FAIL(init_table_ctx(
                req_ops.at(0), batch_ctx.tb_ctx_, returning_affected_entity, returning_rowkey))) {
          LOG_WARN("fail to init table ctx", K(ret), K(redis_ctx_));
        } else if (OB_FAIL(init_batch_ctx(req_ops, batch_ctx))) {
          LOG_WARN("fail to init batch ctx", K(ret), K(redis_ctx_));
        } else if (OB_FAIL(batch_ctx.tb_ctx_.init_trans(redis_ctx_.trans_param_->trans_desc_,
                                                        redis_ctx_.trans_param_->tx_snapshot_))) {
          LOG_WARN("fail to init trans", K(ret));
        } else if (OB_FAIL(ObTableBatchService::execute(batch_ctx))) {
          LOG_WARN("fail to execute batch operation", K(batch_ctx), K(redis_ctx_));
        }
      }
    }
  }
  return ret;
}

// note: caller should start trans before
int CommandOperator::process_table_single_op(const table::ObTableOperation &op,
                                             ObTableOperationResult &op_res,
                                             bool returning_affected_entity /*= false*/,
                                             bool returning_rowkey /*= false*/)
{
  int ret = OB_SUCCESS;
  ObITableEntity *result_entity = nullptr;
  if (OB_ISNULL(redis_ctx_.trans_param_) || OB_ISNULL(redis_ctx_.entity_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("trans_param_ or entity_factory_ is null", K(ret), K(redis_ctx_.trans_param_));
  } else if (OB_ISNULL(result_entity = redis_ctx_.entity_factory_->alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc entity", K(ret));
  } else {
    op_res.set_entity(result_entity);
    ObNewRow *row = nullptr;
    SMART_VAR(ObTableCtx, tb_ctx, redis_ctx_.allocator_)
    {
      if (OB_FAIL(init_table_ctx(op, tb_ctx, returning_affected_entity, returning_rowkey))) {
        LOG_WARN("fail to init table ctx", K(ret), K(redis_ctx_));
      } else if (OB_FAIL(tb_ctx.init_trans(redis_ctx_.trans_param_->trans_desc_,
                                           redis_ctx_.trans_param_->tx_snapshot_))) {
        LOG_WARN("fail to init trans", K(ret), K(tb_ctx));
      }
      if (OB_SUCC(ret)) {
        switch (op.type()) {
          case ObTableOperationType::GET:
            ret = ObTableBatchService::process_get(op_temp_allocator_, tb_ctx, op_res);
            break;
          case ObTableOperationType::INSERT:
            ret = ObTableOpWrapper::process_insert_op(tb_ctx, op_res);
            break;
          case ObTableOperationType::DEL:
            ret = ObTableOpWrapper::process_op<TABLE_API_EXEC_DELETE>(tb_ctx, op_res);
            break;
          case ObTableOperationType::UPDATE:
            ret = ObTableOpWrapper::process_op<TABLE_API_EXEC_UPDATE>(tb_ctx, op_res);
            break;
          case ObTableOperationType::REPLACE:
            ret = ObTableOpWrapper::process_op<TABLE_API_EXEC_REPLACE>(tb_ctx, op_res);
            break;
          case ObTableOperationType::INSERT_OR_UPDATE:
            ret = ObTableOpWrapper::process_insert_up_op(tb_ctx, op_res);
            break;
          case ObTableOperationType::APPEND:
          case ObTableOperationType::INCREMENT:
            ret = ObTableOpWrapper::process_incr_or_append_op(tb_ctx, op_res);
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("unexpected operation type", "type", op.type());
            break;
        }
        // ObTableApiUtil::replace_ret_code(ret);
        if (OB_FAIL(ret)) {
          LOG_WARN("fail to execute single operation", K(ret), K(op));
        }
      }
    }
  }
  return ret;
}

int CommandOperator::init_batch_ctx(const ObTableBatchOperation &req_ops,
                                    ObTableBatchCtx &batch_ctx)
{
  int ret = OB_SUCCESS;
  batch_ctx.trans_param_ = redis_ctx_.trans_param_;
  batch_ctx.stat_event_type_ = redis_ctx_.stat_event_type_;
  batch_ctx.table_id_ = redis_ctx_.table_id_;
  batch_ctx.tablet_id_ = redis_ctx_.tablet_id_;
  batch_ctx.is_atomic_ = true;
  batch_ctx.is_readonly_ = req_ops.is_readonly();
  batch_ctx.is_same_type_ = req_ops.is_same_type();
  batch_ctx.is_same_properties_names_ = req_ops.is_same_properties_names();
  batch_ctx.entity_type_ = ObTableEntityType::ET_KV;
  batch_ctx.consistency_level_ = table::ObTableConsistencyLevel::STRONG;
  batch_ctx.credential_ = redis_ctx_.credential_;
  return ret;
}

int CommandOperator::init_scan_tb_ctx(ObTableApiCacheGuard &cache_guard, const ObTableQuery &query,
                                      ObTableCtx &tb_ctx)
{
  int ret = OB_SUCCESS;
  ObExprFrameInfo *expr_frame_info = nullptr;
  bool is_weak_read = (redis_ctx_.consistency_level_ == ObTableConsistencyLevel::EVENTUAL);
  tb_ctx.set_scan(true);
  tb_ctx.set_entity_type(ObTableEntityType::ET_KV);
  tb_ctx.set_schema_cache_guard(redis_ctx_.tb_ctx_.get_schema_cache_guard());
  tb_ctx.set_schema_guard(redis_ctx_.tb_ctx_.get_schema_guard());
  tb_ctx.set_simple_table_schema(redis_ctx_.tb_ctx_.get_simple_table_schema());
  tb_ctx.set_sess_guard(redis_ctx_.tb_ctx_.get_sess_guard());
  tb_ctx.set_need_dist_das(redis_ctx_.tb_ctx_.need_dist_das());
  tb_ctx.set_audit_ctx(&redis_ctx_.audit_ctx_);
  ObTabletID tablet_id = redis_ctx_.tablet_id_;
  ObNewRange range;

  if (tb_ctx.is_init()) {
    LOG_INFO("tb ctx has been inited", K(tb_ctx));
  } else if (OB_FAIL(query.get_scan_ranges().at(0, range))) {
    LOG_WARN("fail to get scan range", K(ret), K(query));
  } else if (redis_ctx_.tb_ctx_.need_dist_das()
             && OB_FAIL(tb_ctx.get_tablet_by_rowkey(range.start_key_, tablet_id))) {
    LOG_WARN("fail to get tablet by rowkey", K(ret), K(tb_ctx), K(range.start_key_));
  } else if (OB_FAIL(
                 tb_ctx.init_common(*redis_ctx_.credential_, tablet_id, redis_ctx_.timeout_ts_))) {
    LOG_WARN("fail to init table tb_ctx common part", K(ret), K(redis_ctx_));
  } else if (OB_FAIL(tb_ctx.init_scan(query, is_weak_read, tb_ctx.get_index_table_id()))) {
    LOG_WARN("fail to init table ctx scan part", K(ret));
  } else if (OB_FAIL(cache_guard.init(&tb_ctx))) {
    LOG_WARN("fail to init cache guard", K(ret));
  } else if (OB_FAIL(cache_guard.get_expr_info(&tb_ctx, expr_frame_info))) {
    LOG_WARN("fail to get expr frame info from cache", K(ret));
  } else if (OB_FAIL(ObTableExprCgService::alloc_exprs_memory(tb_ctx, *expr_frame_info))) {
    LOG_WARN("fail to alloc expr memory", K(ret));
  } else if (OB_FAIL(tb_ctx.init_exec_ctx())) {
    LOG_WARN("fail to init exec ctx", K(ret), K(tb_ctx));
  } else {
    tb_ctx.set_init_flag(true);
    tb_ctx.set_expr_info(expr_frame_info);
  }
  return ret;
}

// add member to request entity
int CommandOperator::build_hash_set_rowkey_entity(int64_t db, const ObString &key, bool is_data,
                                                  const ObString &member, ObITableEntity *&entity)
{
  int ret = OB_SUCCESS;
  ObRowkey rowkey;
  if (OB_ISNULL(redis_ctx_.entity_factory_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null entity_factory_", K(ret));
  } else if (OB_ISNULL(entity = redis_ctx_.entity_factory_->alloc())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null entity_factory_", K(ret));
  } else if (FALSE_IT(entity->set_allocator(&op_temp_allocator_))) {
  } else if (OB_FAIL(build_hash_set_rowkey(db, key, is_data, member, rowkey))) {
    LOG_WARN("fail to build start key", K(ret), K(db), K(key));
  } else if (OB_FAIL(entity->set_rowkey(rowkey))) {
    LOG_WARN("fail to set rowkey", K(ret));
  }
  return ret;
}

int CommandOperator::build_hash_set_rowkey(int64_t db, const ObString &key, bool is_data,
                                           bool is_min, common::ObRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  ObObj *obj_ptr = nullptr;
  if (OB_ISNULL(obj_ptr = static_cast<ObObj *>(redis_ctx_.allocator_.alloc(
                    sizeof(ObObj) * ObRedisUtil::COMPLEX_ROWKEY_NUM)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObObj", K(ret));
  } else {
    // rowkey: [db, rkey, is_data, member]
    obj_ptr[0].set_int(db);
    obj_ptr[1].set_varbinary(key);
    obj_ptr[2].set_tinyint(is_data);
    if (is_min) {
      obj_ptr[3].set_min_value();
    } else {
      obj_ptr[3].set_max_value();
    }
    rowkey.assign(obj_ptr, ObRedisUtil::COMPLEX_ROWKEY_NUM);
  }
  return ret;
}

int CommandOperator::build_hash_set_rowkey(int64_t db, const ObString &key, bool is_data,
                                           const ObString &member, common::ObRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  ObObj *obj_ptr = nullptr;
  if (OB_ISNULL(obj_ptr = static_cast<ObObj *>(redis_ctx_.allocator_.alloc(
                    sizeof(ObObj) * ObRedisUtil::COMPLEX_ROWKEY_NUM)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObObj", K(ret));
  } else {
    obj_ptr[0].set_int(db);
    obj_ptr[1].set_varbinary(key);
    obj_ptr[2].set_tinyint(is_data ? 1 : 0);
    obj_ptr[3].set_varbinary(member);
    rowkey.assign(obj_ptr, ObRedisUtil::COMPLEX_ROWKEY_NUM);
  }
  return ret;
}

int CommandOperator::init_table_ctx(const ObTableOperation &op, ObTableCtx &tb_ctx,
                                    bool returning_affected_entity /* = false*/,
                                    bool returning_rowkey /* = false*/)
{
  int ret = OB_SUCCESS;
  ObTableOperationType::Type op_type = op.type();
  tb_ctx.set_schema_cache_guard(redis_ctx_.tb_ctx_.get_schema_cache_guard());
  tb_ctx.set_schema_guard(redis_ctx_.tb_ctx_.get_schema_guard());
  tb_ctx.set_simple_table_schema(redis_ctx_.tb_ctx_.get_simple_table_schema());
  tb_ctx.set_sess_guard(redis_ctx_.tb_ctx_.get_sess_guard());
  tb_ctx.set_audit_ctx(&redis_ctx_.audit_ctx_);
  tb_ctx.set_entity(&op.entity());
  tb_ctx.set_entity_type(ObTableEntityType::ET_KV);
  tb_ctx.set_operation_type(op_type);
  tb_ctx.set_need_dist_das(redis_ctx_.tb_ctx_.need_dist_das());
  ObTabletID tablet_id = redis_ctx_.tablet_id_;
  if (redis_ctx_.tb_ctx_.need_dist_das()
      && OB_FAIL(tb_ctx.get_tablet_by_rowkey(op.entity().get_rowkey(), tablet_id))) {
    LOG_WARN("fail to get tablet by rowkey", K(ret), K(tb_ctx), K(op.entity().get_rowkey()));
  } else if (OB_FAIL(
                 tb_ctx.init_common(*redis_ctx_.credential_, tablet_id, redis_ctx_.timeout_ts_))) {
    LOG_WARN("fail to init table tb_ctx common part", K(ret), K(redis_ctx_));
  } else {
    switch (op_type) {
      case ObTableOperationType::GET: {
        if (OB_FAIL(tb_ctx.init_get())) {
          LOG_WARN("fail to init get tb_ctx", K(ret), K(tb_ctx));
        } else {
          tb_ctx.set_read_latest(false);
        }
        break;
      }
      case ObTableOperationType::PUT: {
        if (OB_FAIL(tb_ctx.init_put())) {
          LOG_WARN("fail to init put tb_ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::INSERT: {
        if (OB_FAIL(tb_ctx.init_insert())) {
          LOG_WARN("fail to init insert tb_ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::DEL: {
        if (OB_FAIL(tb_ctx.init_delete())) {
          LOG_WARN("fail to init delete tb_ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::UPDATE: {
        if (OB_FAIL(tb_ctx.init_update())) {
          LOG_WARN("fail to init update tb_ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::INSERT_OR_UPDATE: {
        if (OB_FAIL(tb_ctx.init_insert_up(false))) {
          LOG_WARN("fail to init insert up tb_ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::REPLACE: {
        if (OB_FAIL(tb_ctx.init_replace())) {
          LOG_WARN("fail to init replace tb_ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::APPEND: {
        if (OB_FAIL(tb_ctx.init_append(returning_affected_entity, returning_rowkey))) {
          LOG_WARN("fail to init append tb_ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::INCREMENT: {
        if (OB_FAIL(tb_ctx.init_increment(returning_affected_entity, returning_rowkey))) {
          LOG_WARN("fail to init increment tb_ctx", K(ret), K(tb_ctx));
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
  } else if (OB_FAIL(tb_ctx.init_exec_ctx())) {
    LOG_WARN("fail to init exec tb_ctx", K(ret), K(tb_ctx));
  }

  return ret;
}

int CommandOperator::build_range(const common::ObRowkey &start_key, const common::ObRowkey &end_key,
                                 ObNewRange *&range, bool inclusive_start /* = true */,
                                 bool inclusive_end /* = true */)
{
  int ret = OB_SUCCESS;
  range = nullptr;
  if (OB_ISNULL(range = OB_NEWx(ObNewRange, &op_temp_allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("ArgNodeContent cast to string invalid value", K(ret));
  } else {
    range->start_key_ = start_key;
    range->end_key_ = end_key;
    if (inclusive_start) {
      range->border_flag_.set_inclusive_start();
    }
    if (inclusive_end) {
      range->border_flag_.set_inclusive_end();
    }
  }
  return ret;
}

int CommandOperator::hashset_to_array(const common::hash::ObHashSet<ObString> &hash_set,
                                      ObIArray<ObString> &ret_arr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ret_arr.reserve(hash_set.size()))) {
    LOG_WARN("fail to reserve space", K(ret), K(hash_set.size()));
  }

  for (hash::ObHashSet<ObString>::const_iterator iter = hash_set.begin();
       OB_SUCC(ret) && iter != hash_set.end();
       iter++) {
    if (OB_FAIL(ret_arr.push_back(iter->first))) {
      LOG_WARN("fail to push back string", K(ret), K(iter->first));
    }
  }
  return ret;
}

int FilterStrBuffer::cmp_op_to_str(hfilter::CompareOperator cmp_op, ObString &str)
{
  int ret = OB_SUCCESS;
  switch (cmp_op) {
    case hfilter::CompareOperator::EQUAL: {
      str = ObString::make_string("=");
      break;
    }
    case hfilter::CompareOperator::GREATER: {
      str = ObString::make_string(">");
      break;
    }
    case hfilter::CompareOperator::GREATER_OR_EQUAL: {
      str = ObString::make_string(">=");
      break;
    }
    case hfilter::CompareOperator::LESS: {
      str = ObString::make_string("<");
      break;
    }
    case hfilter::CompareOperator::LESS_OR_EQUAL: {
      str = ObString::make_string("<=");
      break;
    }
    case hfilter::CompareOperator::NO_OP: {
      str = ObString::make_string("");
      break;
    }
    case hfilter::CompareOperator::NOT_EQUAL: {
      str = ObString::make_string("!=");
      break;
    }
    case hfilter::CompareOperator::IS: {
      str = ObString::make_string("is");
      break;
    }
    case hfilter::CompareOperator::IS_NOT: {
      str = ObString::make_string("is not");
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected compare operator type", K(ret), K(cmp_op));
    }
  }
  return ret;
}

// filter_str = TableCompareFilter(cmp_op, 'col_name:cmp_str')
int FilterStrBuffer::add_value_compare(hfilter::CompareOperator cmp_op, const ObString &col_name,
                                       const ObString &cmp_str)
{
  int ret = OB_SUCCESS;
  ObString filter_name = "TableCompareFilter";
  ObString op_str;
  if (OB_FAIL(cmp_op_to_str(cmp_op, op_str))) {
    LOG_WARN("fail to covert cmp op to str", K(ret), K(cmp_op));
  } else if (buffer_.reserve(filter_name.length() + op_str.length() + col_name.length()
                             + cmp_str.length() + 5)) {
    LOG_WARN("fail to reserve space for buffer_", K(ret));
  } else if (OB_FAIL(buffer_.append(filter_name.ptr(), filter_name.length()))) {
    LOG_WARN("fail to append buffer_", K(ret), K(filter_name));
  } else if (OB_FAIL(buffer_.append("("))) {
    LOG_WARN("fail to append buffer_", K(ret));
  } else if (OB_FAIL(buffer_.append(op_str.ptr(), op_str.length()))) {
    LOG_WARN("fail to append buffer_", K(ret), K(op_str));
  } else if (OB_FAIL(buffer_.append(", '"))) {
    LOG_WARN("fail to append buffer_", K(ret));
  } else if (OB_FAIL(buffer_.append(col_name.ptr(), col_name.length()))) {
    LOG_WARN("fail to append buffer_", K(ret), K(col_name));
  } else if (OB_FAIL(buffer_.append(":"))) {
    LOG_WARN("fail to append buffer_", K(ret));
  } else if (OB_FAIL(buffer_.append(cmp_str.ptr(), cmp_str.length()))) {
    LOG_WARN("fail to append buffer_", K(ret), K(cmp_str));
  } else if (OB_FAIL(buffer_.append("')"))) {
    LOG_WARN("fail to append buffer_", K(ret));
  }
  return ret;
}

int FilterStrBuffer::add_conjunction(bool is_and)
{
  return is_and ? buffer_.append(" && ") : buffer_.append(" || ");
}

}  // namespace table
}  // namespace oceanbase
