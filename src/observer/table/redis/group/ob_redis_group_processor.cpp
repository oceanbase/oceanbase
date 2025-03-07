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

#include "ob_redis_group_processor.h"
#include "share/table/redis/ob_redis_common.h"
#include "observer/table/redis/operator/ob_redis_hash_operator.h"
#include "observer/table/redis/operator/ob_redis_string_operator.h"
#include "observer/table/redis/operator/ob_redis_list_operator.h"
#include "observer/table/redis/operator/ob_redis_set_operator.h"
#include "observer/table/redis/operator/ob_redis_zset_operator.h"
#include "observer/table/group/ob_table_group_execute.h"

using namespace oceanbase::table;
using namespace oceanbase::common;

int ObRedisGroupOpProcessor::is_valid()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(group_ctx_) || OB_ISNULL(ops_) || OB_ISNULL(allocator_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null group ctx or ops", K(ret), KP(group_ctx_), KP(ops_));
  } else if (ops_->empty() || OB_ISNULL(ops_->at(0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ops_", K(ret), K(ops_->count()), KP(ops_->at(0)));
  }
  return ret;
}

int ObRedisGroupOpProcessor::set_group_need_dist_das(ObRedisBatchCtx &batch_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(is_valid())) {
    LOG_WARN("invalid processor", K(ret));
  } else {
    bool in_same_ls = true;
    ObLSID last_ls_id(ObLSID::INVALID_LS_ID);
    for (int i = 0; i < ops_->count() && OB_SUCC(ret) && in_same_ls; ++i) {
      const ObRedisOp *op = reinterpret_cast<const ObRedisOp *>(ops_->at(i));
      if (OB_ISNULL(op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid null redis cmd", K(ret));
      } else if (!last_ls_id.is_valid()) {
        last_ls_id = op->ls_id_;
      } else if (op->ls_id_ != last_ls_id) {
        in_same_ls = false;
      }
    }

    if (OB_SUCC(ret)) {
      batch_ctx.need_dist_das_ = !in_same_ls;
    }
  }
  return ret;
}

int ObRedisGroupOpProcessor::init_batch_ctx(ObRedisBatchCtx &batch_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(is_valid())) {
    LOG_WARN("invalid null group ctx", K(ret));
  } else if (OB_FAIL(set_group_need_dist_das(batch_ctx))) {
    LOG_WARN("fail to init cmd ctx", K(ret));
  } else {
    batch_ctx.entity_factory_ = &processor_entity_factory_;
    batch_ctx.timeout_ts_ = group_ctx_->timeout_ts_;
    batch_ctx.credential_ = &group_ctx_->credential_;
    batch_ctx.trans_param_ = group_ctx_->trans_param_;
    batch_ctx.ls_id_ = static_cast<ObRedisOp*>(ops_->at(0))->ls_id_;
    batch_ctx.redis_guard_.schema_guard_ = group_ctx_->schema_guard_;
    batch_ctx.redis_guard_.schema_cache_guard_ = group_ctx_->schema_cache_guard_;
    batch_ctx.redis_guard_.simple_table_schema_ = group_ctx_->simple_schema_;
    batch_ctx.redis_guard_.sess_guard_ = group_ctx_->sess_guard_;
    batch_ctx.audit_ctx_ = &group_ctx_->audit_ctx_;
    batch_ctx.tablet_id_ = ops_->at(0)->tablet_id();
    batch_ctx.consistency_level_ = ObTableConsistencyLevel::STRONG;
  }

  return ret;
}

int ObRedisGroupOpProcessor::end_trans(ObRedisBatchCtx &redis_ctx,
                                       bool need_snapshot,
                                       bool is_rollback)
{
  int ret = OB_SUCCESS;
  ObTableTransParam *trans_param = redis_ctx.trans_param_;

  if (OB_ISNULL(trans_param) || OB_ISNULL(group_ctx_) || OB_ISNULL(functor_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid null argument", K(ret), KP(trans_param), KP(group_ctx_), KP(functor_));
  } else if (need_snapshot) {  // read only cmd
    if (redis_ctx.trans_param_->lock_handle_ != nullptr) {
      HTABLE_LOCK_MGR->release_handle(*redis_ctx.trans_param_->lock_handle_);
    }
    ObTableTransUtils::release_read_trans(trans_param->trans_desc_);
  } else {
    trans_param->is_rollback_ = is_rollback;
    trans_param->req_ = nullptr;
    trans_param->use_sync_ = false;
    trans_param->create_cb_functor_ = functor_;
    if (OB_FAIL(ObTableTransUtils::end_trans(*trans_param))) {
      LOG_WARN("fail to end trans", K(ret), KPC(trans_param));
    }
  }

  return ret;
}

int ObRedisGroupOpProcessor::init(ObTableGroupCtx &group_ctx, ObIArray<ObITableOp*> *ops)
{
  int ret = OB_SUCCESS;
  ObRedisOp *op = static_cast<ObRedisOp*>(ops->at(0));

  if (OB_FAIL(ObITableOpProcessor::init(group_ctx, ops))) {
    LOG_WARN("fail to init processor", K(ret));
  } else {
    allocator_ = &group_ctx.allocator_;
    if (OB_NOT_NULL(op) && ObRedisHelper::is_read_only_command(op->cmd()->cmd_type())) {
      for (int64_t i = 0; i < ops->count(); i++) {
        op = static_cast<ObRedisOp*>(ops->at(i));
        op->update_outer_allocator(&group_ctx.allocator_);
      }
    }

    is_inited_ = true;
  }
  return ret;
}

int ObRedisGroupOpProcessor::process()
{
  int ret = OB_SUCCESS;
  ObRedisBatchCtx *batch_ctx_ptr = nullptr;
  if (OB_FAIL(is_valid())) {
    LOG_WARN("invalid processor", K(ret));
  } else if (OB_ISNULL(
      batch_ctx_ptr = OB_NEWx(ObRedisBatchCtx, (allocator_), *allocator_, &processor_entity_factory_, *ops_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc ObRedisBatchCtx", K(ret));
  } else {
    ObRedisBatchCtx &batch_ctx = *batch_ctx_ptr;
    const ObRedisOp &op = *reinterpret_cast<const ObRedisOp*>(ops_->at(0));
    const RedisCommand *redis_cmd = op.cmd();
    if (OB_ISNULL(redis_cmd)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("invalid null redis cmd", K(ret));
    } else if (OB_FAIL(init_batch_ctx(batch_ctx))) {
      LOG_WARN("fail to init cmd ctx", K(ret));
    } else if (OB_FAIL(ObRedisService::start_trans(batch_ctx, redis_cmd->need_snapshot()))) {
      LOG_WARN("fail to start trans", K(ret), K(batch_ctx));
    }

    if (OB_SUCC(ret)) {
      switch(redis_cmd->cmd_type()) {
        // hash
        case RedisCommandType::REDIS_COMMAND_HGET: {
          HashCommandOperator op(batch_ctx);
          ret = op.do_group_hget();
          break;
        }
        case RedisCommandType::REDIS_COMMAND_HSET: {
          HashCommandOperator op(batch_ctx);
          ret = op.do_group_complex_type_set();
          break;
        }
        case RedisCommandType::REDIS_COMMAND_HSETNX: {
          HashCommandOperator op(batch_ctx);
          ret = op.do_group_hsetnx();
          break;
        }
        case RedisCommandType::REDIS_COMMAND_HEXISTS: {
          CommandOperator op(batch_ctx);
          ret = op.do_group_complex_type_subkey_exists(ObRedisModel::HASH);
          break;
        }
        // string
        case RedisCommandType::REDIS_COMMAND_GET: {
          StringCommandOperator op(batch_ctx);
          ret = op.do_group_get();
          break;
        }
        case RedisCommandType::REDIS_COMMAND_SET: {
          StringCommandOperator op(batch_ctx);
          ret = op.do_group_set();
          break;
        }
        case RedisCommandType::REDIS_COMMAND_BITCOUNT: {
          StringCommandOperator op(batch_ctx);
          ret = op.do_group_analyze(&StringCommandOperator::analyze_bitcount, &op);
          break;
        }
        case RedisCommandType::REDIS_COMMAND_GETBIT: {
          StringCommandOperator op(batch_ctx);
          ret = op.do_group_analyze(&StringCommandOperator::analyze_getbit, &op);
          break;
        }
        case RedisCommandType::REDIS_COMMAND_GETRANGE: {
          StringCommandOperator op(batch_ctx);
          ret = op.do_group_analyze(&StringCommandOperator::analyze_getrange, &op);
          break;
        }
        case RedisCommandType::REDIS_COMMAND_STRLEN: {
          StringCommandOperator op(batch_ctx);
          ret = op.do_group_analyze(&StringCommandOperator::analyze_strlen, &op);
          break;
        }
        case RedisCommandType::REDIS_COMMAND_INCRBY:
        case RedisCommandType::REDIS_COMMAND_DECRBY:
        case RedisCommandType::REDIS_COMMAND_DECR:
        case RedisCommandType::REDIS_COMMAND_INCR: {
          StringCommandOperator op(batch_ctx);
          ret = op.do_group_incr();
          break;
        }
        case RedisCommandType::REDIS_COMMAND_INCRBYFLOAT: {
          StringCommandOperator op(batch_ctx);
          ret = op.do_group_incrbyfloat();
          break;
        }
        case RedisCommandType::REDIS_COMMAND_SETNX: {
          StringCommandOperator op(batch_ctx);
          ret = op.do_group_setnx();
          break;
        }
        case RedisCommandType::REDIS_COMMAND_APPEND: {
          StringCommandOperator op(batch_ctx);
          ret = op.do_group_append();
          break;
        }
        // list
        case RedisCommandType::REDIS_COMMAND_LPUSH:
        case RedisCommandType::REDIS_COMMAND_RPUSH:
        case RedisCommandType::REDIS_COMMAND_LPUSHX:
        case RedisCommandType::REDIS_COMMAND_RPUSHX: {
          ListCommandOperator op(batch_ctx);
          ret = op.do_group_push();
          break;
        }
        case RedisCommandType::REDIS_COMMAND_LPOP:
        case RedisCommandType::REDIS_COMMAND_RPOP: {
          ListCommandOperator op(batch_ctx);
          ret = op.do_group_pop();
          break;
        }
        // set
        case RedisCommandType::REDIS_COMMAND_SISMEMBER: {
          CommandOperator op(batch_ctx);
          ret = op.do_group_complex_type_subkey_exists(ObRedisModel::SET);
          break;
        }
        case RedisCommandType::REDIS_COMMAND_SADD: {
          SetCommandOperator op(batch_ctx);
          ret = op.do_group_complex_type_set();
          break;
        }
        // zset
        case RedisCommandType::REDIS_COMMAND_ZADD: {
          ZSetCommandOperator op(batch_ctx);
          ret = op.do_group_complex_type_set();
          break;
        }
        case RedisCommandType::REDIS_COMMAND_ZSCORE: {
          ZSetCommandOperator op(batch_ctx);
          ret = op.do_group_zscore();
          break;
        }
        default: {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("redis cmd do not support group commit", K(ret), K(redis_cmd->cmd_type()));
        }
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to process redis cmd", K(ret), K(redis_cmd->cmd_type()));
      }
    }

    bool is_rollback = (OB_SUCCESS != ret);
    if (OB_NOT_NULL(redis_cmd)) {
      int tmp_ret = OB_SUCCESS;
      // exec end_trans regardless of whether ret is OB_SUCCESS
      if (OB_SUCCESS != (tmp_ret = end_trans(batch_ctx, redis_cmd->need_snapshot(), is_rollback))) {
        LOG_WARN("fail to end trans", K(ret), K(is_rollback));
        ret = COVER_SUCC(tmp_ret);
      }
    }
  }

  return ret;
}
