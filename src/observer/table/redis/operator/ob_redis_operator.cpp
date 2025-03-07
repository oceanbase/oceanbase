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
#include "lib/utility/ob_fast_convert.h"
#include "src/observer/table/ob_table_query_and_mutate_helper.h"
#include "src/observer/table/redis/ob_redis_rkey.h"
#include "observer/table/redis/cmd/ob_redis_cmd.h"
#include "observer/table/redis/operator/ob_redis_hash_operator.h"

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
                                               const ObRedisMeta *meta, int64_t &row_count)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObTableCtx, tb_ctx, allocator)
  {
    QUERY_ITER_START(redis_ctx_, query, tb_ctx, iter, meta)
    row_count = 0;
    ObTableQueryResult *one_result = nullptr;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter->get_next_result(one_result))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next result", K(ret), KP(meta), K(query));
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
                                            ObRedisMeta *meta /* = nullptr*/,
                                            uint8_t flags /* = RedisOpFlags::NONE*/,
                                            common::ObIAllocator *allocator /* = nullptr*/,
                                            ObITableEntityFactory* entity_factory /* = nullptr*/,
                                            ObIArray<ObTabletID> *tablet_ids /* = nullptr*/)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(req_ops.count() == 0)) {
    // do nothing, return empty results
  } else {
    ObTableEntity result_entity;
    common::ObIAllocator *batch_alloc = allocator != nullptr ? allocator : &redis_ctx_.allocator_;
    SMART_VAR(ObTableBatchCtx, batch_ctx, *batch_alloc, *redis_ctx_.audit_ctx_) {
      if (OB_FAIL(results.init(req_ops.count()))) {
        LOG_WARN("fail to init results", K(ret), K(req_ops.count()));
      } else {
        const common::ObIArray<ObTableOperation> &ops = req_ops.get_table_operations();
        ObITableEntityFactory &factory = entity_factory != nullptr ? *entity_factory : *redis_ctx_.entity_factory_;
        if (OB_NOT_NULL(tablet_ids) && OB_FAIL(batch_ctx.tablet_ids_.assign(*tablet_ids))) {
          LOG_WARN("fail to assign tablet ids", K(ret));
        } else if (OB_FAIL(init_table_ctx(req_ops.at(0), batch_ctx.tb_ctx_, meta, flags, true/*is_batch_ctx*/))) {
          LOG_WARN("fail to init table ctx", K(ret), K(redis_ctx_));
        } else if (OB_FAIL(init_batch_ctx(req_ops, flags, batch_ctx))) {
          LOG_WARN("fail to init batch ctx", K(ret), K(redis_ctx_));
        } else if (OB_FAIL(batch_ctx.tb_ctx_.init_trans(redis_ctx_.trans_param_->trans_desc_,
                                                        redis_ctx_.trans_param_->tx_snapshot_))) {
          LOG_WARN("fail to init trans", K(ret));
        } else if (OB_FAIL(ObTableBatchService::prepare_results(ops, factory, results))) {
          LOG_WARN("fail to prepare results", K(ret), K(ops));
        } else if (OB_FAIL(ObTableBatchService::execute(batch_ctx, ops, results))) {
          LOG_WARN("fail to execute batch operation", K(batch_ctx), K(redis_ctx_));
        }
      }
    }
  }
  return ret;
}

int CommandOperator::process_table_multi_batch_op(const ObTableMultiBatchRequest &req,
                                                  ObTableMultiBatchResult &result,
                                                  uint8_t flags /* = RedisOpFlags::NONE*/)
{
  int ret = OB_SUCCESS;
  ObTableEntity result_entity;
  const ObIArray<ObTableBatchOperation> &batch_ops = req.get_ops();

  if (batch_ops.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("batch operations is empty", KR(ret));
  }

  SMART_VAR(ObTableMultiBatchCtx, ctx, redis_ctx_.allocator_, *redis_ctx_.audit_ctx_, *redis_ctx_.entity_factory_) {
    if (OB_FAIL(init_table_ctx(batch_ops.at(0).get_table_operations().at(0),
                               ctx.tb_ctx_,
                               nullptr/*ObRedisMeta*/,
                               flags,
                               true/*is_batch_ctx*/))) {
      LOG_WARN("fail to init table ctx", K(ret), K(redis_ctx_));
    } else if (OB_FAIL(init_batch_ctx(batch_ops.at(0), flags, ctx))) {
      LOG_WARN("fail to init batch ctx", K(ret), K(redis_ctx_));
    } else if (OB_FAIL(ctx.tb_ctx_.init_trans(redis_ctx_.trans_param_->trans_desc_,
                                              redis_ctx_.trans_param_->tx_snapshot_))) {
      LOG_WARN("fail to init trans", K(ret));
    } else if (OB_FAIL(ObTableMultiBatchService::execute(ctx, req, result))) {
      LOG_WARN("fail to execute multi batch operation", K(ctx), K(redis_ctx_));
    }
  }

  return ret;
}

// note: caller should start trans before
int CommandOperator::process_table_single_op(const table::ObTableOperation &op,
                                             ObTableOperationResult &op_res,
                                             ObRedisMeta *meta /* = nullptr*/,
                                             uint8_t flags /* = RedisOpFlags::NONE*/,
                                             common::ObIAllocator *allocator /* = nullptr*/,
                                             ObITableEntityFactory *entity_factory /* = nullptr*/)
{
  int ret = OB_SUCCESS;
  ObITableEntity *result_entity = nullptr;
  ObITableEntityFactory* res_entity_factory = entity_factory != nullptr? entity_factory : redis_ctx_.entity_factory_;
  if (OB_ISNULL(redis_ctx_.trans_param_) || OB_ISNULL(res_entity_factory)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("trans_param_ or entity_factory_ is null", K(ret), K(redis_ctx_.trans_param_));
  } else if (OB_ISNULL(result_entity = res_entity_factory->alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc entity", K(ret));
  } else {
    op_res.set_entity(result_entity);
    ObNewRow *row = nullptr;
    common::ObIAllocator &process_alloc = allocator != nullptr ? *allocator : redis_ctx_.allocator_;
    SMART_VAR(ObTableCtx, tb_ctx, process_alloc) {
      if (OB_FAIL(init_table_ctx(op, tb_ctx, meta, flags))) {
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
          case ObTableOperationType::PUT:
            ret = ObTableOpWrapper::process_put_op(tb_ctx, op_res);
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("unexpected operation type", "type", op.type());
            break;
        }
        if (OB_FAIL(ret) && ret != OB_ITER_END) {
          LOG_WARN("fail to execute single operation", K(ret), K(op));
        }
      }
    }
  }
  return ret;
}

int CommandOperator::init_batch_ctx(const ObTableBatchOperation &req_ops,
                                    uint8_t flags,
                                    ObTableBatchCtx &batch_ctx)
{
  int ret = OB_SUCCESS;
  ObTableCtx &tb_ctx = batch_ctx.tb_ctx_;
  batch_ctx.trans_param_ = redis_ctx_.trans_param_;
  batch_ctx.is_atomic_ = !(flags & RedisOpFlags::BATCH_NOT_ATOMIC);
  batch_ctx.is_readonly_ = req_ops.is_readonly();
  batch_ctx.is_same_type_ = req_ops.is_same_type();
  batch_ctx.is_same_properties_names_ = req_ops.is_same_properties_names();
  batch_ctx.consistency_level_ = table::ObTableConsistencyLevel::STRONG;
  batch_ctx.credential_ = redis_ctx_.credential_;
  batch_ctx.returning_affected_entity_ = flags & RedisOpFlags::RETURN_AFFECTED_ENTITY;
  batch_ctx.returning_rowkey_ = flags & RedisOpFlags::RETURN_ROWKEY;
  batch_ctx.use_put_ = flags & RedisOpFlags::USE_PUT;
  if (batch_ctx.tablet_ids_.empty()) {
    if (tb_ctx.need_dist_das()) {
      for (int i = 0; OB_SUCC(ret) && i < req_ops.count(); ++i) {
        const ObRowkey &rowkey = req_ops.at(i).entity().get_rowkey();
        ObTabletID tablet_id;
        if (OB_FAIL(get_tablet_id_by_rowkey(tb_ctx, rowkey, tablet_id))) {
          LOG_WARN("fail to get tablet id by rowkey", K(ret), K(rowkey));
        } else if (OB_FAIL(batch_ctx.tablet_ids_.push_back(tablet_id))) {
          LOG_WARN("fail to push back tablet id", K(ret), K(tablet_id));
        }
      }
    } else if (OB_FAIL(batch_ctx.tablet_ids_.push_back(tb_ctx.get_tablet_id()))) {
      LOG_WARN("fail to push back tablet id", K(ret));
    }
  }
  return ret;
}

// add member to request entity
int CommandOperator::build_complex_type_rowkey_entity(int64_t db, const ObString &key, bool is_data,
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
  } else if (OB_FAIL(build_complex_type_rowkey(redis_ctx_.allocator_, db, key, is_data, member, rowkey))) {
    LOG_WARN("fail to build start key", K(ret), K(db), K(key));
  } else if (OB_FAIL(entity->set_rowkey(rowkey))) {
    LOG_WARN("fail to set rowkey", K(ret));
  }
  return ret;
}

int CommandOperator::build_complex_type_rowkey(int64_t db, const ObString &key,
                                           bool is_next_prefix, common::ObRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  ObObj *obj_ptr = nullptr;
  ObRedisRKey rkey(redis_ctx_.allocator_, key, true/*is_data*/, "");
  if (OB_ISNULL(obj_ptr = static_cast<ObObj *>(redis_ctx_.allocator_.alloc(
                    sizeof(ObObj) * ObRedisUtil::COMPLEX_ROWKEY_NUM)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObObj", K(ret));
  } else {
    // rowkey: [db, (key + is_data + member)]
    obj_ptr[ObRedisUtil::COL_IDX_DB].set_int(db);
    ObString rkey_str;
    if (!is_next_prefix) {
      if (OB_FAIL(rkey.encode(rkey_str))) {
        LOG_WARN("fail to encode rkey", K(ret), K(rkey));
      } else {
        obj_ptr[ObRedisUtil::COL_IDX_RKEY].set_varbinary(rkey_str);
      }
    } else {
      if (OB_FAIL(rkey.encode_next_prefix(rkey_str))) {
        LOG_WARN("fail to encode rkey", K(ret), K(rkey));
      } else {
        obj_ptr[ObRedisUtil::COL_IDX_RKEY].set_varbinary(rkey_str);
      }
    }
    rowkey.assign(obj_ptr, ObRedisUtil::COMPLEX_ROWKEY_NUM);
  }
  return ret;
}

int CommandOperator::build_complex_type_rowkey(
    ObIAllocator &allocator,
    int64_t db,
    const ObString &key,
    bool is_data,
    const ObString &member,
    common::ObRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  ObObj *obj_ptr = nullptr;
  ObRedisRKey rkey(allocator, key, is_data, member);
  ObString rkey_str;
  if (OB_ISNULL(obj_ptr = static_cast<ObObj *>(allocator.alloc(
                    sizeof(ObObj) * ObRedisUtil::COMPLEX_ROWKEY_NUM)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObObj", K(ret));
  } else if (OB_FAIL(rkey.encode(rkey_str))) {
    LOG_WARN("fail to encode rkey", K(ret), K(rkey));
  } else {
    obj_ptr[0].set_int(db);
    obj_ptr[1].set_varbinary(rkey_str);
    rowkey.assign(obj_ptr, ObRedisUtil::COMPLEX_ROWKEY_NUM);
  }
  return ret;
}

int CommandOperator::get_tablet_id_by_rowkey(ObTableCtx &tb_ctx, const ObRowkey &rowkey, ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObRowkey tmp_part_key;
  ObArenaAllocator tmp_alloc("RedisCmd", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  if (OB_FAIL(ObRedisRKeyUtil::gen_partition_key_by_rowkey(model_, tmp_alloc, rowkey, tmp_part_key))) {
    LOG_WARN("fail to get partition key", K(ret), K(rowkey));
  } else if (OB_FAIL(tb_ctx.get_tablet_by_rowkey(tmp_part_key, tablet_id))) {
    LOG_WARN("fail to get tablet by rowkey", K(ret), K(tb_ctx), K(tmp_part_key));
  }
  return ret;
}

int CommandOperator::init_table_ctx(const ObTableOperation &op,
                                    ObTableCtx &tb_ctx,
                                    ObRedisMeta *meta /* = nullptr*/,
                                    uint8_t flags /* = RedisOpFlags::NONE*/,
                                    bool is_batch_ctx /* = false */)
{
  int ret = OB_SUCCESS;
  ObTableOperationType::Type op_type = op.type();
  tb_ctx.set_schema_guard(redis_ctx_.redis_guard_.schema_guard_);
  tb_ctx.set_sess_guard(redis_ctx_.redis_guard_.sess_guard_);
  tb_ctx.set_audit_ctx(redis_ctx_.audit_ctx_);
  tb_ctx.set_entity(&op.entity());
  tb_ctx.set_entity_type(ObTableEntityType::ET_KV);
  tb_ctx.set_operation_type(op_type);
  tb_ctx.set_need_dist_das(redis_ctx_.need_dist_das_);
  bool is_redis_ttl = false;
  ObRedisTTLCtx *ttl_ctx = nullptr;
  ObTabletID tablet_id = redis_ctx_.tablet_id_;
  ObRedisTableInfo *tb_info = nullptr;
  if (OB_FAIL(redis_ctx_.try_get_table_info(tb_info))) {
    LOG_WARN("fail to try get table info", K(ret));
  } else if (OB_NOT_NULL(tb_info)) {
    tb_ctx.set_schema_cache_guard(tb_info->schema_cache_guard_);
    tb_ctx.set_simple_table_schema(tb_info->simple_schema_);
    if (OB_FAIL(tb_info->tablet_ids_.at(redis_ctx_.cur_rowkey_idx_, tablet_id))) {
      LOG_WARN("fail to get tablet id", K(ret), K(redis_ctx_.cur_rowkey_idx_), K(tb_info->tablet_ids_.count()));
    }
  } else {
    tb_ctx.set_schema_cache_guard(redis_ctx_.redis_guard_.schema_cache_guard_);
    tb_ctx.set_simple_table_schema(redis_ctx_.redis_guard_.simple_table_schema_);
    if (redis_ctx_.need_dist_das_ && !is_batch_ctx) {
      if (OB_FAIL(get_tablet_id_by_rowkey(tb_ctx, op.entity().get_rowkey(), tablet_id))) {
        LOG_WARN("fail to get tablet id by rowkey", K(ret), K(op.entity().get_rowkey()));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(tb_ctx.init_common(*redis_ctx_.credential_, tablet_id, redis_ctx_.timeout_ts_))) {
    LOG_WARN("fail to init table tb_ctx common part", K(ret), K(redis_ctx_));
  } else if (OB_ISNULL(ttl_ctx = OB_NEWx(ObRedisTTLCtx, &tb_ctx.get_allocator()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObRedisTTLCtx", K(ret));
  } else {
    ttl_ctx->set_return_meta(flags & RedisOpFlags::RETURN_REDIS_META);
    ttl_ctx->set_meta(meta);
    ttl_ctx->set_model(model_);
    tb_ctx.set_redis_ttl_ctx(ttl_ctx);
    // When it is not a get operation, the string cmd need to be processed through ttl.
    if (model_ == ObRedisModel::STRING && op_type != ObTableOperationType::GET) {
      tb_ctx.set_is_ttl_table(true);
      tb_ctx.set_ttl_definition(ObRedisUtil::STRING_TTL_DEFINITION);
    }
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
        } else {
          tb_ctx.set_skip_scan(flags & RedisOpFlags::DEL_SKIP_SCAN);
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
        if (OB_FAIL(tb_ctx.init_insert_up(flags & RedisOpFlags::USE_PUT))) {
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
        if (OB_FAIL(tb_ctx.init_append(
                flags & RedisOpFlags::RETURN_AFFECTED_ENTITY, flags & RedisOpFlags::RETURN_ROWKEY))) {
          LOG_WARN("fail to init append tb_ctx", K(ret), K(tb_ctx));
        }
        break;
      }
      case ObTableOperationType::INCREMENT: {
        if (OB_FAIL(tb_ctx.init_increment(
                flags & RedisOpFlags::RETURN_AFFECTED_ENTITY, flags & RedisOpFlags::RETURN_ROWKEY))) {
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
    LOG_WARN("alloc memory for ObNewRange failed", K(ret));
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

int CommandOperator::do_get_meta_entity(ObITableEntity &req_meta_entity,
                                        ObITableEntity *&res_meta_entity)
{
  int ret = OB_SUCCESS;
  ObTableOperation op = ObTableOperation::retrieve(req_meta_entity);
  ObTableOperationResult result;
  ObITableEntity *tmp_res_meta_entity = nullptr;
  if (OB_FAIL(process_table_single_op(op, result, nullptr/*meta*/, RedisOpFlags::RETURN_REDIS_META))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to get redis meta", K(ret), K(req_meta_entity));
    }
  } else if (OB_FAIL(result.get_entity(tmp_res_meta_entity))) {
    LOG_WARN("fail to get entity", K(ret), K(result));
  } else if (OB_ISNULL(tmp_res_meta_entity)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null entity", K(ret), K(tmp_res_meta_entity));
  } else {
    res_meta_entity = tmp_res_meta_entity;
  }
  return ret;
}

int CommandOperator::get_meta(int64_t db, const ObString &key, ObRedisModel model,
                              ObRedisMeta *&meta_info)
{
  int ret = OB_SUCCESS;

  ObITableEntity *get_meta_entity = nullptr;
  ObITableEntity *res_meta_entity = nullptr;
  if (OB_FAIL(ObRedisMetaUtil::create_redis_meta_by_model(op_temp_allocator_, model, meta_info))) {
    LOG_WARN("fail to decode meta by model", K(ret), K(model));
  } else if (OB_FAIL(meta_info->build_meta_rowkey(db, key, redis_ctx_, get_meta_entity))) {
    LOG_WARN("fail to gen entity with rowkey", K(ret));
  } else if (OB_FAIL(do_get_meta_entity(*get_meta_entity, res_meta_entity))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to get meta info", K(ret), KPC(get_meta_entity));
    }
  } else {
    if (OB_FAIL(meta_info->get_meta_from_entity(*res_meta_entity))) {
      LOG_WARN("fail get meta from entity", K(ret), KP(res_meta_entity));
    } else {
      // reuse entity
      redis_ctx_.entity_factory_->free(get_meta_entity);
      redis_ctx_.entity_factory_->free(res_meta_entity);
    }
  }

  if (OB_SUCC(ret)) {
    if (meta_info->is_expired()) {
      SERVER_LOG(INFO, "meta is expired", K(ret), KPC(meta_info));
      meta_info->reset();
      ret = OB_ITER_END;
    } else if (model == ObRedisModel::LIST && static_cast<ObRedisListMeta *>(meta_info)->count_ == 0) {
      SERVER_LOG(INFO, "list is empty, but not be del", K(ret), K(db), K(key), KPC(meta_info));
      meta_info->reset();
      ret = OB_ITER_END;
    }
  }

  return ret;
}

int CommandOperator::put_meta(int64_t db, const ObString &key, ObRedisModel model,
                              const ObRedisMeta &meta_info)
{
  int ret = OB_SUCCESS;
  ObITableEntity *put_meta_entity = nullptr;
  if (OB_FAIL(gen_meta_entity(db, key, model, meta_info, put_meta_entity))) {
    LOG_WARN("fail to generate meta entity", K(ret), K(db), K(key), K(model), K(meta_info));
  } else {
    ObTableOperation op = ObTableOperation::insert_or_update(*put_meta_entity);
    ObTableOperationResult result;
    if (OB_FAIL(process_table_single_op(op, result, nullptr/*meta*/, RedisOpFlags::RETURN_REDIS_META))) {
      LOG_WARN("fail to process table op", K(ret), K(op));
    }
  }

  return ret;
}

int CommandOperator::gen_meta_entity(int64_t db, const ObString &key, ObRedisModel model,
                                     const ObRedisMeta &meta_info, ObITableEntity *&put_meta_entity)
{
  int ret = OB_SUCCESS;
  put_meta_entity = nullptr;
  if (OB_FAIL(meta_info.build_meta_rowkey(db, key, redis_ctx_, put_meta_entity))) {
    LOG_WARN("fail to gen entity with rowkey", K(ret));
  } else if (OB_FAIL(meta_info.put_meta_into_entity(op_temp_allocator_, *put_meta_entity))) {
    LOG_WARN("fail to encode meta value", K(ret), K(meta_info));
  }

  return ret;
}

int CommandOperator::add_meta_select_columns(ObTableQuery &query)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(query.add_select_column(ObRedisUtil::DB_PROPERTY_NAME))) {
    LOG_WARN("fail to add select column", K(ret));
  } else if (OB_FAIL(query.add_select_column(ObRedisUtil::RKEY_PROPERTY_NAME))) {
    LOG_WARN("fail to add select column", K(ret));
  } else if (OB_FAIL(query.add_select_column(ObRedisUtil::EXPIRE_TS_PROPERTY_NAME))) {
    LOG_WARN("fail to add select column", K(ret));
  } else if (OB_FAIL(query.add_select_column(ObRedisUtil::INSERT_TS_PROPERTY_NAME))) {
    LOG_WARN("fail to add select column", K(ret));
  }
  if (model_ == ObRedisModel::LIST) {
    if (OB_FAIL(query.add_select_column(ObRedisUtil::IS_DATA_PROPERTY_NAME))) {
      LOG_WARN("fail to add select column", K(ret));
    }
  }
  return ret;
}

int CommandOperator::init_scan_tb_ctx(ObTableApiCacheGuard &cache_guard, const ObTableQuery &query,
                                      const ObRedisMeta *meta, ObTableCtx &tb_ctx)
{
  int ret = OB_SUCCESS;
  ObExprFrameInfo *expr_frame_info = nullptr;
  bool is_weak_read = (redis_ctx_.consistency_level_ == ObTableConsistencyLevel::EVENTUAL);
  tb_ctx.set_scan(true);
  tb_ctx.set_entity_type(ObTableEntityType::ET_KV);
  tb_ctx.set_schema_guard(redis_ctx_.redis_guard_.schema_guard_);
  tb_ctx.set_sess_guard(redis_ctx_.redis_guard_.sess_guard_);
  tb_ctx.set_need_dist_das(redis_ctx_.need_dist_das_);
  tb_ctx.set_audit_ctx(redis_ctx_.audit_ctx_);
  ObTabletID tablet_id = redis_ctx_.tablet_id_;
  // shallow copy
  ObTableQuery real_query = query;
  bool is_redis_ttl = false;
  ObRedisTableInfo *tb_info = nullptr;
  if (tb_ctx.is_init()) {
    SERVER_LOG(INFO, "tb ctx has been inited", K(tb_ctx));
  } else if (OB_FAIL(redis_ctx_.try_get_table_info(tb_info))) {
    LOG_WARN("fail to try get table info", K(ret));
  } else if (OB_NOT_NULL(tb_info)) {
    tb_ctx.set_schema_cache_guard(tb_info->schema_cache_guard_);
    tb_ctx.set_simple_table_schema(tb_info->simple_schema_);
    if (OB_FAIL(tb_info->tablet_ids_.at(redis_ctx_.cur_rowkey_idx_, tablet_id))) {
      LOG_WARN("fail to get tablet id", K(ret), K(redis_ctx_.cur_rowkey_idx_), K(tb_info->tablet_ids_.count()));
    }
  } else {
    tb_ctx.set_schema_cache_guard(redis_ctx_.redis_guard_.schema_cache_guard_);
    tb_ctx.set_simple_table_schema(redis_ctx_.redis_guard_.simple_table_schema_);
    ObNewRange range;
    if (OB_FAIL(query.get_scan_ranges().at(0, range))) {
      LOG_WARN("fail to get first range", K(ret), K(query));
    } else if (redis_ctx_.need_dist_das_) {
      if (OB_FAIL(get_tablet_id_by_rowkey(tb_ctx, range.start_key_, tablet_id))) {
        LOG_WARN("fail to get tablet id by rowkey", K(ret), K(range.start_key_));
      }
    }
  }

  if (tb_ctx.is_init()) {
    SERVER_LOG(INFO, "tb ctx has been inited", K(tb_ctx));
  } else if (!real_query.get_select_columns().empty()) {
    real_query.clear_select_columns();
    // to select meta info
    if (OB_FAIL(add_meta_select_columns(real_query))) {
      LOG_WARN("fail to add meta select columns", K(ret));
    }

    for (int i = 0; OB_SUCC(ret) && i < query.get_select_columns().count(); ++i) {
      if (OB_FAIL(real_query.add_select_column(query.get_select_columns().at(i)))) {
        LOG_WARN("fail to add select column", K(ret));
      }
    }
  }

  ObRedisTTLCtx *ttl_ctx = nullptr;
  if (OB_FAIL(ret) || tb_ctx.is_init()) {
  } else if (OB_ISNULL(ttl_ctx = OB_NEWx(ObRedisTTLCtx, &tb_ctx.get_allocator()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObRedisTTLCtx", K(ret));
  } else if (FALSE_IT(tb_ctx.set_redis_ttl_ctx(ttl_ctx))) {
  } else if (FALSE_IT(ttl_ctx->set_model(model_))) {
  } else if (OB_ISNULL(meta)) {
    // to make sure that meta is in first row (is_data = 0)
    real_query.set_scan_order(ObQueryFlag::Forward);
  } else {
    ObKvSchemaCacheGuard *cache_guard = tb_ctx.get_schema_cache_guard();
    ObKVAttr kv_attributes;
    if (OB_ISNULL(cache_guard)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("invalid null schema cache guard", K(ret));
    } else if (OB_FAIL(cache_guard->get_kv_attributes(kv_attributes))) {
      LOG_WARN("fail to get kv attributes", K(ret), K(*cache_guard));
    } else if (FALSE_IT(ttl_ctx->set_model(kv_attributes.redis_model_))) {
    } else if (FALSE_IT(tb_ctx.set_redis_ttl_ctx(ttl_ctx))) {
    } else if (OB_ISNULL(meta)) {
      // to make sure that meta is in first row (is_data = 0)
      real_query.set_scan_order(ObQueryFlag::Forward);
    } else {
      ttl_ctx->set_meta(meta);
    }
  }

  if (OB_FAIL(ret) || tb_ctx.is_init()) {
  } else if (OB_FAIL(
                 tb_ctx.init_common(*redis_ctx_.credential_, tablet_id, redis_ctx_.timeout_ts_))) {
    LOG_WARN("fail to init table tb_ctx common part", K(ret), K(redis_ctx_));
  } else if (OB_FAIL(tb_ctx.init_scan(real_query, is_weak_read, tb_ctx.get_index_table_id()))) {
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

int CommandOperator::insup_meta(int64_t db, const ObString &key, ObRedisModel model)
{
  int ret = OB_SUCCESS;
  ObRedisMeta *meta = nullptr;
  ObArenaAllocator tmp_allocator("RedisCmd", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObITableEntity *udpate_meta_entity = nullptr;
  if (OB_FAIL(ObRedisMetaUtil::create_redis_meta_by_model(op_temp_allocator_, model, meta))) {
    LOG_WARN("fail to decode meta by model", K(ret), K(model));
  } else {
    // meta is expire or not exist, update with new expire ts
    meta->set_expire_ts(INT64_MAX);
    meta->set_insert_ts(ObTimeUtility::fast_current_time());
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(meta->build_meta_rowkey(db, key, redis_ctx_, udpate_meta_entity))) {
      LOG_WARN("fail to gen entity with rowkey", K(ret), KPC(meta), K(db), K(key));
    } else if (OB_FAIL(meta->put_meta_into_entity(op_temp_allocator_, *udpate_meta_entity))) {
      LOG_WARN("fail to encode meta value", K(ret), KPC(meta));
    } else {
      ObTableOperationResult op_res;
      ObTableOperation op = ObTableOperation::insert_or_update(*udpate_meta_entity);
      if (OB_FAIL(process_table_single_op(op, op_res))) {
        LOG_WARN("fail to process table single op", K(ret), K(op));
      }
    }
  }

  return ret;
}

int CommandOperator::check_and_insup_meta(
  int64_t db,
  const ObString &key,
  ObRedisModel model,
  bool &do_insup,
  ObRedisMeta *&meta)
{
  int ret = OB_SUCCESS;
  meta = nullptr;
  ObArenaAllocator tmp_allocator("RedisCmd", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObITableEntity *udpate_meta_entity = nullptr;
  if (OB_FAIL(get_meta(db, key, model, meta))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to get meta", K(ret), K(db), K(key), K(model));
    }
  }
  do_insup = (ret == OB_ITER_END); // expired or not exists
  if (ret == OB_ITER_END) {
    // meta is expire or not exist, update with new expire ts
    ret = OB_SUCCESS;
    meta->set_expire_ts(INT64_MAX);
    meta->set_insert_ts(ObTimeUtility::fast_current_time());
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(meta->build_meta_rowkey(db, key, redis_ctx_, udpate_meta_entity))) {
      LOG_WARN("fail to gen entity with rowkey", K(ret), KPC(meta), K(db), K(key));
    } else if (OB_FAIL(meta->put_meta_into_entity(op_temp_allocator_, *udpate_meta_entity))) {
      LOG_WARN("fail to encode meta value", K(ret), KPC(meta));
    } else {
      ObTableOperationResult op_res;
      ObTableOperation op = ObTableOperation::insert_or_update(*udpate_meta_entity);
      if (OB_FAIL(process_table_single_op(op, op_res))) {
        LOG_WARN("fail to process table single op", K(ret), K(op));
      }
    }
  }

  return ret;
}

int CommandOperator::build_string_rowkey_entity(int64_t db, const ObString &key,
                                                ObITableEntity *&entity)
{
  int ret = OB_SUCCESS;
  ObObj *obj_ptr = nullptr;
  ObRowkey rowkey;
  if (OB_ISNULL(redis_ctx_.entity_factory_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null entity_factory_", K(ret));
  } else if (OB_ISNULL(entity = redis_ctx_.entity_factory_->alloc())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null entity_factory_", K(ret));
  } else if (OB_ISNULL(obj_ptr = static_cast<ObObj *>(redis_ctx_.allocator_.alloc(
                           sizeof(ObObj) * ObRedisUtil::STRING_ROWKEY_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObObj", K(ret));
  } else {
    obj_ptr[0].set_int(db);
    obj_ptr[1].set_varbinary(key);
    rowkey.assign(obj_ptr, ObRedisUtil::STRING_ROWKEY_SIZE);
    if (OB_FAIL(entity->set_rowkey(rowkey))) {
      LOG_WARN("fail to set rowkey", K(ret));
    }
  }
  return ret;
}

int CommandOperator::get_varbinary_from_entity(const ObITableEntity &entity,
                                               const ObString &col_name,
                                               ObString &value)
{
  int ret = OB_SUCCESS;
  ObObj value_obj;
  if (OB_FAIL(entity.get_property(col_name, value_obj))) {
    LOG_WARN("fail to get value obj", K(ret), K(entity));
  } else if (OB_FAIL(value_obj.get_varbinary(value))) {
    LOG_WARN("fail to get value from obj", K(ret), K(value_obj));
  }
  return ret;
}

int CommandOperator::add_complex_type_subkey_scan_range(int64_t db, const ObString &key, ObTableQuery &query)
{
  int ret = OB_SUCCESS;
  ObRowkey start_key;
  ObRowkey end_key;
  ObNewRange *range = nullptr;
  if (OB_FAIL(build_complex_type_rowkey(db, key, false/*is_next_prefix*/, start_key))) {
    LOG_WARN("fail to build start key", K(ret), K(db), K(key));
  } else if (OB_FAIL(build_complex_type_rowkey(db, key, true/*is_next_prefix*/, end_key))) {
    LOG_WARN("fail to build start key", K(ret), K(db), K(key));
  } else if (OB_FAIL(build_range(start_key, end_key, range, true, false))) {
    LOG_WARN("fail to build range", K(ret), K(start_key), K(end_key));
  } else if (OB_FAIL(query.add_scan_range(*range))) {
    LOG_WARN("fail to add scan range", K(ret));
  }

  return ret;
}

bool CommandOperator::is_incr_out_of_range(int64_t old_val, int64_t incr)
{
  return (incr > 0) ? (old_val > INT64_MAX - incr) : (old_val < INT64_MIN - incr);
}

bool CommandOperator::is_incrby_out_of_range(double old_val, double incr)
{
  return (incr > 0) ? (old_val > DBL_MAX - incr) : (old_val < (-DBL_MAX) - incr);
}

bool CommandOperator::is_incrby_out_of_range(long double old_val, long double incr)
{
  return (incr > 0) ? (old_val > DBL_MAX - incr) : (old_val < (-DBL_MAX) - incr);
}

// with deep copy
int CommandOperator::get_subkey_from_entity(
    ObIAllocator &allocator,
    const ObITableEntity &entity,
    ObString &subkey)
{
  int ret = OB_SUCCESS;
  ObObj rkey_obj;
  ObString encoded;
  ObString tmp_subkey;
  if (OB_FAIL(entity.get_property(ObRedisUtil::RKEY_PROPERTY_NAME, rkey_obj))) {
    LOG_WARN("fail to get member from result enrity", K(ret), K(entity));
  } else if (OB_FAIL(rkey_obj.get_varbinary(encoded))) {
    LOG_WARN("fail to get db num", K(ret), K(rkey_obj));
  } else if (ObRedisRKeyUtil::decode_subkey(encoded, tmp_subkey)) {
    LOG_WARN("fail to decode subkey", K(ret), K(encoded));
  } else if (OB_FAIL(ob_write_string(allocator, tmp_subkey, subkey))) {
    LOG_WARN("fail to write string", K(ret), K(tmp_subkey));
  }
  return ret;
}

int CommandOperator::get_insert_ts_from_entity(const ObITableEntity &entity, int64_t &insert_ts)
{
  int ret = OB_SUCCESS;
  ObObj insert_ts_obj;
  if (OB_FAIL(entity.get_property(ObRedisUtil::INSERT_TS_PROPERTY_NAME, insert_ts_obj))) {
    LOG_WARN("fail to get member from result enrity", K(ret), K(entity));
  } else if (OB_FAIL(insert_ts_obj.get_timestamp(insert_ts))) {
    LOG_WARN("fail to get insert ts", K(ret), K(insert_ts_obj));
  }
  return ret;
}

int CommandOperator::build_del_query(int64_t db, const ObString &key, ObTableQuery &query)
{
  int ret = OB_SUCCESS;
  ObObj *start_ptr = nullptr;
  ObObj *end_ptr = nullptr;
  ObNewRange *range = nullptr;
  ObRedisRKey rkey(redis_ctx_.allocator_, key, true/*is_data*/, "");
  ObString rkey_str;
  ObString next_rkey_str;
  if (OB_ISNULL(start_ptr =
                    static_cast<ObObj *>(op_temp_allocator_.alloc(sizeof(ObObj) * ObRedisUtil::COMPLEX_ROWKEY_NUM)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObObj", K(ret));
  } else if (OB_FAIL(ObRedisCtx::reset_objects(start_ptr, ObRedisUtil::COMPLEX_ROWKEY_NUM))) {
    LOG_WARN("fail to init object", K(ret));
  } else if (OB_ISNULL(end_ptr = static_cast<ObObj *>(
                           op_temp_allocator_.alloc(sizeof(ObObj) * ObRedisUtil::COMPLEX_ROWKEY_NUM)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObObj", K(ret));
  } else if (OB_FAIL(ObRedisCtx::reset_objects(end_ptr, ObRedisUtil::COMPLEX_ROWKEY_NUM))) {
    LOG_WARN("fail to init object", K(ret));
  } else if (OB_FAIL(rkey.encode(rkey_str))) {
    LOG_WARN("fail to encode rkey", K(ret), K(rkey));
  } else if (OB_FAIL(rkey.encode_next_prefix(next_rkey_str))) {
    LOG_WARN("fail to encode rkey", K(ret), K(rkey));
  } else {
    // pk: <db, rkey>
    start_ptr[ObRedisUtil::COL_IDX_DB].set_int(db);
    start_ptr[ObRedisUtil::COL_IDX_RKEY].set_varbinary(rkey_str);
    ObRowkey start_key(start_ptr, ObRedisUtil::COMPLEX_ROWKEY_NUM);
    end_ptr[ObRedisUtil::COL_IDX_DB].set_int(db);
    end_ptr[ObRedisUtil::COL_IDX_RKEY].set_varbinary(next_rkey_str);
    ObRowkey end_key(end_ptr, ObRedisUtil::COMPLEX_ROWKEY_NUM);
    if (OB_FAIL(build_range(start_key, end_key, range, true/*inclusive_start*/, false/*inclusive_end*/))) {
      LOG_WARN("fail to build range", K(ret));
    } else if (OB_FAIL(query.add_scan_range(*range))) {
      LOG_WARN("fail to add scan range", K(ret), KPC(range));
    }
  }
  return ret;
}

int CommandOperator::build_del_ops(ObRedisModel model,
                                   int db,
                                   const ObString &key,
                                   const ObTableQuery &query,
                                   ObTableBatchOperation &del_ops,
                                   ObRedisMeta *meta /*= nullptr*/)
{
  int ret = OB_SUCCESS;

  // query element
  SMART_VAR(ObTableCtx, tb_ctx, op_temp_allocator_) {
    QUERY_ITER_START(redis_ctx_, query, tb_ctx, iter, meta)
    ObTableQueryResult *one_result = nullptr;
    const ObITableEntity *result_entity = nullptr;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter->get_next_result(one_result))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next result", K(ret));
        }
      }
      one_result->rewind();
      while (OB_SUCC(ret)) {
        ObString member;
        if (OB_FAIL(one_result->get_next_entity(result_entity))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to get next result", K(ret));
          }
        } else if (OB_FAIL(get_subkey_from_entity(op_temp_allocator_, *result_entity, member))) {
          LOG_WARN("fail to get member from entity", K(ret), K(*result_entity));
        } else {
          ObITableEntity *del_data_entity = nullptr;
          if (OB_FAIL(build_complex_type_rowkey_entity(db, key, true /*not meta*/, member, del_data_entity))) {
            LOG_WARN("fail to gen entity with rowkey", K(ret));
          } else if (OB_FAIL(del_ops.del(*del_data_entity))) {
            LOG_WARN("fail to push back", K(ret));
          }
        }
      }
    }
    QUERY_ITER_END(iter)
  }
  return ret;
}

int CommandOperator::del_complex_key(
    ObRedisModel model,
    int64_t db,
    const ObString &key,
    bool del_meta,
    bool &is_exist)
{
  int ret = OB_SUCCESS;
  ObRedisMeta *meta = nullptr;
  ObTableQuery query;
  ObTableBatchOperation del_ops;
  is_exist = true;
  if (OB_FAIL(get_meta(db, key, model, meta))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to do list expire if needed", K(ret), K(key));
    } else {
      is_exist = false;
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(build_del_query(db, key, query))) {
    LOG_WARN("fail to build del query", K(ret), K(db), K(key));
  } else if (OB_FAIL(build_del_ops(model, db, key, query, del_ops, meta))) {
    LOG_WARN("fail to build del ops", K(ret), K(query));
  } else {
    // del data
    ResultFixedArray results(op_temp_allocator_);
    if (OB_FAIL(process_table_batch_op(del_ops, results))) {
      LOG_WARN("fail to process table batch op", K(ret), K(del_ops));
    } else if (del_meta) {
      // del meta
      if (OB_FAIL(fake_del_meta(model, db, key, meta))) {
        LOG_WARN("fail to delete complex type meta", K(db), K(key), KPC(meta));
      }
    }
  }
  return ret;
}

int CommandOperator::delete_results(const ResultFixedArray &results, const ObArray<ObRowkey> &rowkeys, int64_t &del_num)
{
  int ret = OB_SUCCESS;
  ObRedisMeta *meta = nullptr;
  del_num = 0;
  for (int i = 0; OB_SUCC(ret) && i < results.count(); ++i) {
    ObTableOperationResult cur_res;
    if (OB_FAIL(results.at(i, cur_res))) {
      LOG_WARN("fail to get result", K(ret), K(i));
    } else if (cur_res.get_return_rows() > 0) {
      ObTableOperation op;
      ObTableOperationResult del_result;
      ObITableEntity *del_entity = cur_res.get_entity();
      if (OB_ISNULL(del_entity)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("invalid null entity", K(ret));
      } else if (OB_FAIL(del_entity->set_rowkey(rowkeys.at(i)))) {
        LOG_WARN("fail to set rowkey", K(ret), K(i), K(rowkeys.at(i)));
      } else if (FALSE_IT(op = ObTableOperation::del(*del_entity))) {
      } else if (OB_FAIL(process_table_single_op(
              op,
              del_result,
              meta,
              RedisOpFlags::DEL_SKIP_SCAN))) {
        LOG_WARN("fail to del data", K(ret), K(op));
      } else {
        del_num += del_result.get_affected_rows();
      }
    }
  }
  return ret;
}

int CommandOperator::fake_del_meta(
  ObRedisModel model, int64_t db, const ObString &key, ObRedisMeta *meta_info/* = nullptr*/)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(meta_info)) {
    if (OB_FAIL(get_meta(db, key, model, meta_info))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("fail to get meta from key", K(ret), K(db), K(key));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObITableEntity *new_meta_entity = nullptr;
    ObTableOperationResult update_res;
    meta_info->set_expire_ts(meta_info->get_insert_ts());
    if (OB_FAIL(meta_info->build_meta_rowkey(db, key, redis_ctx_, new_meta_entity))) {
      LOG_WARN("fail to gen entity with rowkey", K(ret));
    } else if (OB_FAIL(meta_info->put_meta_into_entity(op_temp_allocator_, *new_meta_entity))) {
      LOG_WARN("fail to put meta into entity", K(ret), KPC(new_meta_entity));
    } else if (OB_FAIL(process_table_single_op(
        ObTableOperation::update(*new_meta_entity), update_res, meta_info, RedisOpFlags::DEL_SKIP_SCAN))) {
      LOG_WARN("fail to process table single del", K(ret), KPC(new_meta_entity));
    } else if (update_res.get_affected_rows() != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to delete meta row", K(ret), K(update_res.get_affected_rows()));
    }
  }
  if (ret == OB_ITER_END) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int CommandOperator::get_complex_type_count(int64_t db, const common::ObString &key, int64_t &total_count)
{
  int ret = OB_SUCCESS;
  ObTableQuery query;
  total_count = 0;
  if (OB_FAIL(add_complex_type_subkey_scan_range(db, key, query))) {
    LOG_WARN("fail to add hash set scan range", K(ret), K(db), K(key));
  } else if (OB_FAIL(process_table_query_count(op_temp_allocator_, query, nullptr, total_count))) {
    LOG_WARN("fail to process table query count", K(ret), K(db), K(key), K(query));
  }
  return ret;
}

int CommandOperator::fake_del_empty_key_meta(
    ObRedisModel model, int64_t db, const ObString &key, ObRedisMeta *meta_info/*= nullptr*/)
{
  int ret = OB_SUCCESS;
  // check whether key is empty
  bool is_empty = false;
  if (model == ObRedisModel::INVALID || model == ObRedisModel::STRING) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("model should be HASH/SET/ZSET/LIST", K(ret), K(model));
  } else if (model == ObRedisModel::LIST) {
    if (OB_ISNULL(meta_info)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("invalid null list meta", K(ret));
    } else {
      is_empty = reinterpret_cast<ObRedisListMeta *>(meta_info)->count_ == 0;
    }
  } else {
    ObTableQuery query;
    if (OB_FAIL(add_complex_type_subkey_scan_range(db, key, query))) {
      LOG_WARN("fail to add hash set scan range", K(ret), K(db), K(key));
    } else if (OB_FAIL(query.set_limit(2))) { // 2: include 1 meta 1 data
      LOG_WARN("fail to set query limit", K(ret), K(query));
    } else {
      ObArenaAllocator tmp_allocator("ObRedisCmd");
      SMART_VAR(ObTableCtx, tb_ctx, tmp_allocator) {
        QUERY_ITER_START(redis_ctx_, query, tb_ctx, iter, meta_info)
        ObTableQueryResult *one_result = nullptr;
        if (OB_FAIL(iter->get_next_result(one_result))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to get next result", K(ret));
          } else {
            is_empty = true;
          }
        }
        QUERY_ITER_END(iter)
      }
    }
  }

  // delete meta if key is empty
  if (is_empty && OB_SUCC(ret)) {
    if (OB_FAIL(fake_del_meta(model, db, key, meta_info))) {
      LOG_WARN("fail to delete complex type meta", K(db), K(key), K(meta_info));
    }
  }
  return ret;
}

int CommandOperator::init_tablet_ids_by_ops(const ObIArray<ObITableOp *> &ops)
{
  int ret = OB_SUCCESS;
  tablet_ids_.reuse();
  if (OB_FAIL(tablet_ids_.reserve(ops.count()))) {
    LOG_WARN("fail to reserve tablet id", K(ret), K(ops.count()));
  }
  for (int i = 0; OB_SUCC(ret) && i < ops.count(); ++i) {
    ObRedisOp *op = reinterpret_cast<ObRedisOp*>(ops.at(i));
    if (OB_ISNULL(op)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("invalid null op", K(ret), KP(op));
    } else if (OB_FAIL(tablet_ids_.push_back(op->tablet_id()))) {
      LOG_WARN("fail to push back tablet id", K(ret));
    }
  }
  return ret;
}

int CommandOperator::reply_batch_res(const ResultFixedArray &batch_res)
{
  int ret = OB_SUCCESS;
  ObRedisBatchCtx &group_ctx = reinterpret_cast<ObRedisBatchCtx &>(redis_ctx_);
  if (batch_res.count() != group_ctx.ops().count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("batch res count should be equal to ops count",
      K(ret), K(batch_res.count()), K(group_ctx.ops().count()));
  }
  for (int i = 0; OB_SUCC(ret) && i < batch_res.count(); ++i) {
    ObRedisOp *op = reinterpret_cast<ObRedisOp*>(group_ctx.ops().at(i));
    if (batch_res[i].get_return_rows() > 0) {
      ObString val;
      const ObITableEntity *res_entity = nullptr;
      if (OB_FAIL(batch_res[i].get_entity(res_entity))) {
        LOG_WARN("fail to get entity", K(ret), K(batch_res[i]));
      } else if (OB_FAIL(get_varbinary_from_entity(
                    *res_entity, ObRedisUtil::VALUE_PROPERTY_NAME, val))) {
        LOG_WARN("fail to get value from entity", K(ret), KPC(res_entity));
      } else if (OB_FAIL(op->response().set_res_bulk_string(val))) {
        LOG_WARN("fail to set bulk string", K(ret), K(val));
      }
    } else {
      if (OB_FAIL(op->response().set_fmt_res(ObRedisFmt::NULL_BULK_STRING))) {
        LOG_WARN("fail to set bulk string", K(ret));
      }
    }
  }
  return ret;
}

int CommandOperator::get_group_metas(ObIAllocator &allocator,
                                     ObRedisModel model,
                                     ObIArray<ObRedisMeta *> &metas)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObTabletID, ObTableBatchOperation::COMMON_BATCH_SIZE> tablet_ids;
  ObTableBatchOperation batch_op;
  ObRedisBatchCtx &group_ctx = reinterpret_cast<ObRedisBatchCtx &>(redis_ctx_);
  group_ctx.allocator_ = op_temp_allocator_;
  group_ctx.entity_factory_ = &op_entity_factory_;

  if (OB_FAIL(metas.reserve(group_ctx.ops().count()))) {
    LOG_WARN("fail to reserve array", K(ret), K(group_ctx.ops().count()));
  }

  for (int i = 0; OB_SUCC(ret) && i < group_ctx.ops().count(); ++i) {
    ObRedisOp *op = reinterpret_cast<ObRedisOp*>(group_ctx.ops().at(i));
    ObRedisMeta *meta_info = nullptr;
    if (OB_ISNULL(op)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("invalid null op", K(ret));
    } else {
      ObITableEntity *entity = nullptr;
      ObString key;
      if (OB_FAIL(op->get_key(key))) {
        LOG_WARN("fail to get key", K(ret), KPC(op));
      } else if (OB_FAIL(ObRedisMetaUtil::create_redis_meta_by_model(allocator, model, meta_info))) {
        LOG_WARN("fail to decode meta by model", K(ret), K(model));
      } else if (OB_FAIL(meta_info->build_meta_rowkey(op->db(), key, redis_ctx_, entity))) {
        LOG_WARN("fail to gen entity with rowkey", K(ret), K(op));
      } else if (OB_FAIL(batch_op.retrieve(*entity))) {
        LOG_WARN("fail to add get op", K(ret), KPC(entity));
      } else if (OB_FAIL(metas.push_back(meta_info))) {
        LOG_WARN("fail to push back meta info", K(ret), KPC(meta_info));
      } else if (OB_FAIL(tablet_ids.push_back(op->tablet_id()))) {
        LOG_WARN("fail to push back tablet id", K(ret));
      }
    }
  }

  ResultFixedArray batch_res(allocator);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(process_table_batch_op(
            batch_op,
            batch_res,
            nullptr /*meta*/,
            RedisOpFlags::RETURN_REDIS_META,
            &op_temp_allocator_,
            &op_entity_factory_,
            &tablet_ids))) {
      LOG_WARN("fail to process table batch op", K(ret));
    }
    for (int i = 0; OB_SUCC(ret) && i < batch_res.count(); ++i) {
      ObString val;
      ObRedisMeta *meta_info = metas.at(i);
      // if meta not exists or expired, set is_exist = false
      if (batch_res[i].get_return_rows() > 0) {
        ObITableEntity *res_entity = nullptr;
        if (OB_FAIL(batch_res[i].get_entity(res_entity))) {
          LOG_WARN("fail to get entity", K(ret), K(batch_res[i]));
        } else if (OB_FAIL(meta_info->get_meta_from_entity(*res_entity))) {
          LOG_WARN("fail get meta from entity", K(ret), KPC(res_entity));
        } else if (meta_info->is_expired()
          || (model == ObRedisModel::LIST && static_cast<ObRedisListMeta *>(meta_info)->count_ == 0)) {
          meta_info->reset();
          meta_info->set_is_exists(false);
        }
      } else {
        meta_info->reset();
        meta_info->set_is_exists(false);
      }
    }
  }

  return ret;
}

int CommandOperator::is_row_expire(const ObNewRow *old_row, const ObRedisMeta *meta, bool &is_old_row_expire)
{
  int ret = OB_SUCCESS;
  ObObj inset_ts_obj = old_row->get_cell(ObRedisUtil::COL_IDX_INSERT_TS);
  int64_t insert_ts = 0;
  if (OB_FAIL(inset_ts_obj.get_timestamp(insert_ts))) {
    LOG_WARN("fail to get insert_ts", K(ret), K(inset_ts_obj));
  } else if (insert_ts < meta->get_insert_ts()) {
    is_old_row_expire = true;
  }
  return ret;
}

int CommandOperator::do_group_complex_type_set()
{
  int ret = OB_SUCCESS;
  ObTableMultiBatchRequest *req; // use heap var to prevent stack too big
  ObRedisBatchCtx &group_ctx = reinterpret_cast<ObRedisBatchCtx &>(redis_ctx_);
  group_ctx.entity_factory_ = &op_entity_factory_;

  // 1. get metas
  ObArray<ObRedisMeta *> metas(OB_MALLOC_NORMAL_BLOCK_SIZE,
                              ModulePageAllocator(op_temp_allocator_, "RedisGHSet"));
  if (OB_ISNULL(req = OB_NEWx(ObTableMultiBatchRequest, (&op_temp_allocator_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc multi batch request", K(ret));
  } else if (OB_FAIL(get_group_metas(op_temp_allocator_, model_, metas))) {
    LOG_WARN("fail to get group metas", K(ret));
  } else if (OB_FAIL(req->get_ops().prepare_allocate(metas.count()))) {
    LOG_WARN("fail to reserve request ops", K(ret), K(metas.count()));
  }

  // 2. insup metas and datas
  int64_t cur_ts = ObTimeUtility::fast_current_time();
  for (int64_t i = 0; OB_SUCC(ret) && i < metas.count(); ++i) { // loop keys
    ObRedisOp *op = reinterpret_cast<ObRedisOp*>(group_ctx.ops().at(i));
    ObString key;
    ObRedisMeta *meta = metas.at(i);
    ObTableBatchOperation &batch_op = req->get_ops().at(i);
    batch_op.set_entity_factory(redis_ctx_.entity_factory_);
    // 2.1 insup meta
    if (OB_ISNULL(meta)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("invalid null meta", K(ret), K(i));
    } else if (OB_FAIL(op->get_key(key))) {
      LOG_WARN("fail to get key", K(ret));
    } else if (!meta->is_exists()) {
      ObITableEntity *meta_entity = nullptr;
      if (OB_FAIL(meta->build_meta_rowkey(op->db(), key, redis_ctx_, meta_entity))) {
        LOG_WARN("fail to gen entity with rowkey", K(ret), KPC(meta), K(op->db()), K(key));
      } else if (OB_FAIL(meta->put_meta_into_entity(op_temp_allocator_, *meta_entity))) {
        LOG_WARN("fail to encode meta value", K(ret), KPC(meta));
      } else if (OB_FAIL(batch_op.insert_or_update(*meta_entity))) {
        LOG_WARN("fail to add insert or update to batch", K(ret), KPC(meta_entity));
      }
    }

    if (OB_SUCC(ret)) {
      // 2.2 insup datas
      if (OB_FAIL(fill_set_batch_op(*op, req->get_tablet_ids(), batch_op))) {
        LOG_WARN("fail to fill batch op", K(ret), KPC(op));
      }
    }
  }

  // 3. do batch and return
  ObTableMultiBatchResult result(op_temp_allocator_);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(process_table_multi_batch_op(*req, result))) {
    LOG_WARN("fail to process table batch op", K(ret));
  } else if (metas.count() != result.get_results().count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected result count", K(ret), K(metas.count()), K(result.get_results().count()));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < result.get_results().count(); ++i) { // loop multi batch result
    int64_t insert_num = 0;
    ObTableBatchOperationResult &batch_result = result.get_results().at(i);
    ObRedisMeta *meta = metas.at(i);
    if (meta->is_exists()) {
      for (int64_t j = 0; j < batch_result.count(); j++) {  // loop batch result
        if (!batch_result.at(j).get_is_insertup_do_update()) {
          ++insert_num;
        } else {
          bool is_old_row_expire = false;
          const ObNewRow *old_row = batch_result.at(j).get_insertup_old_row();
          if (OB_ISNULL(old_row)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null old row", K(ret), KPC(old_row));
          } else if (OB_FAIL(is_row_expire(old_row, meta, is_old_row_expire))) {
            LOG_WARN("fail to check row is expired", K(ret), KPC(old_row));
          } else if (is_old_row_expire) {
            ++insert_num;
          }
        }
      }
    } else {
      insert_num = batch_result.count() - 1;
    }

    ObRedisOp *op = reinterpret_cast<ObRedisOp *>(group_ctx.ops().at(i));
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(op->response().set_res_int(insert_num))) {
      LOG_WARN("fail to set reponse", K(ret), K(insert_num));
    }
  }
  return ret;
}

// do_group_complex_type_subkey_exists
int CommandOperator::do_group_complex_type_subkey_exists(ObRedisModel model)
{
  int ret = OB_SUCCESS;
  model_ = model;
  ResultFixedArray batch_res(op_temp_allocator_);
  if (OB_FAIL(group_get_complex_type_data(ObRedisUtil::INSERT_TS_PROPERTY_NAME, batch_res))) {
    LOG_WARN("fail to group get complex type data", K(ret), K(ObRedisUtil::INSERT_TS_PROPERTY_NAME));
  }

  // reply
  ObRedisBatchCtx &group_ctx = reinterpret_cast<ObRedisBatchCtx &>(redis_ctx_);
  for (int i = 0; i < group_ctx.ops().count() && OB_SUCC(ret); ++i) {
    int reply = batch_res.at(i).get_return_rows() > 0 ? 1 : 0;
    ObRedisOp *op = reinterpret_cast<ObRedisOp*>(group_ctx.ops().at(i));
    if (OB_FAIL(op->response().set_res_int(reply/* field already exists */))) {
      LOG_WARN("fail to set response", K(ret), K(i));
    }
  }
  return ret;
}

int CommandOperator::fill_set_batch_op(const ObRedisOp &op,
                      ObIArray<ObTabletID> &tablet_ids,
                      ObTableBatchOperation &batch_op)
{
  int ret = OB_NOT_SUPPORTED;
  LOG_WARN("should call with specific CommandOperator, such as HashCommandOperator", K(ret));
  return ret;
}

int CommandOperator::group_get_complex_type_data(const ObString &property_name, ResultFixedArray &batch_res)
{
  int ret = OB_SUCCESS;
  ObRedisBatchCtx &group_ctx = reinterpret_cast<ObRedisBatchCtx &>(redis_ctx_);
  ObTableBatchOperation batch_op;
  group_ctx.entity_factory_ = &op_entity_factory_;

  for (int i = 0; OB_SUCC(ret) && i < group_ctx.ops().count(); ++i) {
    ObRedisOp *op = reinterpret_cast<ObRedisOp*>(group_ctx.ops().at(i));
    if (OB_ISNULL(op) || OB_ISNULL(op->cmd())) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("invalid null hget op", K(ret), KP(op), KP(op->cmd()));
    } else {
      RedisCommand *cmd = reinterpret_cast<RedisCommand*>(op->cmd());
      ObITableEntity *entity = nullptr;
      if (OB_FAIL(build_complex_type_rowkey_entity(op->db(), cmd->key(), true /*not meta*/, cmd->sub_key(), entity))) {
        LOG_WARN("fail to build rowkey entity", K(ret), K(cmd->sub_key()), K(op->db()), K(cmd->key()));
      } else if(OB_FAIL(entity->add_retrieve_property(property_name))) {
        LOG_WARN("fail to add retrive property", K(ret), KPC(entity));
      } else if (OB_FAIL(batch_op.retrieve(*entity))) {
        LOG_WARN("fail to add get op", K(ret), KPC(entity));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(init_tablet_ids_by_ops(group_ctx.ops()))) {
      LOG_WARN("fail to init tablet ids by ops", K(ret));
    } else if (OB_FAIL(process_table_batch_op(
            batch_op, batch_res, nullptr, RedisOpFlags::NONE, &op_temp_allocator_, &op_entity_factory_, &tablet_ids_))) {
      LOG_WARN("fail to process table batch op", K(ret));
    }
  }
  return ret;
}

ObTableOperation CommandOperator::put_or_insup(const ObITableEntity &entity)
{
  ObTableOperation op;
  if (can_use_put(entity)) {
    op.set_type(ObTableOperationType::PUT);
  } else {
    op.set_type(ObTableOperationType::INSERT_OR_UPDATE);
  }
  op.set_entity(&entity);
  return op;
}

bool CommandOperator::can_use_put(const ObITableEntity &entity)
{
  int64_t expect_property_num = 0;
  if (model_ == ObRedisModel::STRING || model_ == ObRedisModel::SET) {
    expect_property_num = ObRedisUtil::STRING_SET_PROPERTY_SIZE;
  } else if (model_ == ObRedisModel::HASH) {
    expect_property_num = ObRedisUtil::HASH_ZSET_PROPERTY_SIZE;
  }
  return model_ != ObRedisModel::ZSET && binlog_row_image_type_ == ObBinlogRowImage::MINIMAL
        && entity.get_properties_count() == expect_property_num;
}

int CommandOperator::put_or_insup(const ObITableEntity &entity, ObTableBatchOperation& batch_op)
{
  int ret = OB_SUCCESS;
  if (can_use_put(entity)) {
    if (OB_FAIL(batch_op.put(entity))) {
      LOG_WARN("fail to put entity", K(ret), K(entity));
    }
  } else {
    if (OB_FAIL(batch_op.insert_or_update(entity))) {
      LOG_WARN("fail to put entity", K(ret), K(entity));
    }
  }
  return ret;
}

/*******************/
/* FilterStrBuffer */
/*******************/

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

/*****************/
/* RedisKeyNode */
/*****************/

int RedisKeyNode::hash(uint64_t &res) const
{
  res = 0;
  res = murmurhash(&key_, sizeof(key_), res);
  res = murmurhash(&db_, sizeof(int64_t), res);
  return OB_SUCCESS;
}

}  // namespace table
}  // namespace oceanbase
