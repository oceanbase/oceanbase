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
#include "ob_redis_service.h"
#include "ob_redis_command_factory.h"
#include "observer/table/ob_htable_utils.h"
#include "observer/table/redis/ob_redis_rkey.h"
#include "observer/table/group/ob_table_tenant_group.h"
#include "observer/table/group/ob_table_group_service.h"
#include "src/observer/table/redis/cmd/ob_redis_cmd.h"
#include "share/table/redis/ob_redis_error.h"

using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::sql;
using namespace oceanbase::rpc;
namespace oceanbase
{
namespace table
{
void ObTableRedisEndTransCb::callback(int cb_param)
{
  int ret = OB_SUCCESS;
  check_callback_timeout();
  if (OB_UNLIKELY(!has_set_need_rollback_)) {
    LOG_ERROR("is_need_rollback_ has not been set", K(has_set_need_rollback_), K(is_need_rollback_));
  } else if (OB_UNLIKELY(ObExclusiveEndTransCallback::END_TRANS_TYPE_INVALID == end_trans_type_)) {
    LOG_WARN("end trans type is invalid", K(cb_param), K(end_trans_type_));
  } else if (OB_NOT_NULL(tx_desc_)) {
    MTL(transaction::ObTransService *)->release_tx(*tx_desc_);
    tx_desc_ = NULL;
  }
  if (lock_handle_ != nullptr) {
    HTABLE_LOCK_MGR->release_handle(*lock_handle_);
  }
  this->handin();
  CHECK_BALANCE("[table redis async callback]");
  if (OB_FAIL(cb_param)) {
    // commit failed
    if (OB_NOT_NULL(result_)) {
      result_->reset();
    }
  }
  response_sender_.set_result(result_);
  if (OB_FAIL(response_sender_.response(cb_param))) {
    // overwrite ret
    LOG_WARN("failed to send redis response", K(ret), K(cb_param));
  } else {
    LOG_DEBUG("async send redis response", K(cb_param));
  }

  this->destroy_cb_if_no_ref();
}

int ObTableRedisEndTransCb::assign_execute_result(const ObITableResult &result)
{
  int ret = OB_SUCCESS;
  if (result.get_type() == ObTableResultType::REDIS_RESULT) {
    ObRedisResult *tmp_res = nullptr;
    if (OB_ISNULL(tmp_res = OB_NEWx(ObRedisResult, &allocator_, &allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret), K(result.get_type()));
    } else if (OB_FAIL(tmp_res->assign(reinterpret_cast<const ObRedisResult &>(result)))) {
      LOG_WARN("fail to assign redis result");
    } else {
      result_ = tmp_res;
    }
  } else if (result.get_type() == ObTableResultType::TABLE_OPERATION_RESULT) {
    ObTableOperationResult *tmp_res = nullptr;
    ObITableEntity *dest_entity = nullptr;
    if (OB_ISNULL(tmp_res = OB_NEWx(ObTableOperationResult, &allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret), K(result.get_type()));
    } else if (OB_ISNULL(dest_entity = OB_NEWx(ObTableEntity, &allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret), K(result.get_type()));
    } else {
      const ObITableEntity *src_entity = NULL;
      const ObTableOperationResult &op_res = reinterpret_cast<const ObTableOperationResult &>(result);
      if (OB_FAIL(op_res.get_entity(src_entity))) {
        LOG_WARN("failed to get entity", K(ret));
      } else if (OB_FAIL(dest_entity->deep_copy(allocator_, *src_entity))) {
        LOG_WARN("failed to copy entity", K(ret));
      } else {
        *tmp_res = reinterpret_cast<const ObTableOperationResult &>(result);
        tmp_res->set_entity(dest_entity);
        result_ = tmp_res;
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid result type", K(ret), K(result.get_type()));
  }
  return ret;
}

int ObTableRedisCbFunctor::init(ObRequest *req, const ObRedisResponse *response)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    if (OB_ISNULL(req)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("request is null", K(ret));
    } else if (OB_ISNULL(response)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("result is null", K(ret));
    } else {
      req_ = req;
      response_ = response;
      is_inited_ = true;
    }
  }

  return ret;
}

ObTableAPITransCb *ObTableRedisCbFunctor::new_callback()
{
  ObTableRedisEndTransCb *cb = nullptr;
  if (is_inited_) {
    cb = OB_NEW(ObTableRedisEndTransCb, ObMemAttr(MTL_ID(), "RedisTnCb"), req_);
    if (NULL != cb) {
      int ret = OB_SUCCESS;
      ObITableResult *result = nullptr;
      if (OB_ISNULL(response_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret));
      } else if (OB_FAIL(response_->get_result(result))) {
        LOG_WARN("fail to get result", K(ret), KPC(response_));
      } else if (OB_FAIL(cb->assign_execute_result(*result))) {
        LOG_WARN("fail to assign result", K(ret));
        cb->~ObTableRedisEndTransCb();
        cb = NULL;
        ob_free(cb);
      }
    }
  }
  return cb;
}

///////////////////////////////////////////////////////////////////////////////////
int ObRedisService::init_group_ctx(ObTableGroupCtx &group_ctx,
                                   const ObRedisSingleCtx &redis_ctx,
                                   ObRedisOp &redis_op,
                                   ObRedisCmdKey *key) {
  int ret = OB_SUCCESS;
  group_ctx.key_ = key;
  group_ctx.group_type_ = ObTableGroupType::TYPE_REDIS_GROUP;
  group_ctx.type_ = ObTableOperationType::Type::REDIS;
  group_ctx.entity_type_ = ObTableEntityType::ET_REDIS;
  group_ctx.credential_ = *redis_ctx.credential_;
  group_ctx.timeout_ts_ = redis_ctx.timeout_ts_;
  group_ctx.trans_param_ = redis_ctx.trans_param_;
  group_ctx.schema_cache_guard_ = redis_ctx.redis_guard_.schema_cache_guard_;
  group_ctx.schema_guard_ = redis_ctx.redis_guard_.schema_guard_;
  group_ctx.simple_schema_ = redis_ctx.redis_guard_.simple_table_schema_;
  group_ctx.sess_guard_ = redis_ctx.redis_guard_.sess_guard_;
  group_ctx.retry_count_ = redis_ctx.retry_count_;
  group_ctx.user_client_addr_ = redis_ctx.user_client_addr_;
  group_ctx.audit_ctx_.exec_timestamp_ = redis_ctx.audit_ctx_->exec_timestamp_;
  group_ctx.ls_id_ = redis_ctx.ls_id_;
  group_ctx.table_id_ = group_ctx.simple_schema_->get_table_id();
  group_ctx.schema_version_ = group_ctx.simple_schema_->get_schema_version();

  if (!redis_ctx.is_enable_group_op()) {
    ObTableRedisCbFunctor* functor = nullptr;
    functor = OB_NEWx(ObTableRedisCbFunctor, &redis_op.get_allocator());
    if (OB_FAIL(functor->init(redis_op.req_, &redis_op.response_))) {
      LOG_WARN("fail to init create batch execute callback functor", K(ret));
    }
    group_ctx.create_cb_functor_ = functor;
  }

  return ret;
}

int alloc_group_op(ObRedisOp *&redis_op)
{
  int ret = OB_SUCCESS;
  ObITableOp *op = nullptr;

  if (OB_FAIL(TABLEAPI_GROUP_COMMIT_MGR->alloc_op(ObTableGroupType::TYPE_REDIS_GROUP, op))) {
      LOG_WARN("fail to alloc op", K(ret));
  } else {
    redis_op = static_cast<ObRedisOp *>(op);
    if (OB_ISNULL(redis_op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("redis op is null", K(ret));
    }
  }

  return ret;
}

int ObRedisService::execute_cmd_single(ObRedisSingleCtx &ctx)
{
  int ret = OB_SUCCESS;

  RedisCommand *cmd = nullptr;
  ObHTableLockHandle *&trans_lock_handle = ctx.trans_param_->lock_handle_;
  ObString lock_key;
  REDIS_LOCK_MODE lock_mode;
  ObString fmt_err_msg;
  // process cmd in the old way
  if (OB_FAIL(ObRedisCommandFactory::gen_command(
          ctx.allocator_, ctx.request_.get_cmd_name(), ctx.request_.get_args(), fmt_err_msg, cmd))) {
    if (ret == OB_KV_REDIS_ERROR) {
      RESPONSE_REDIS_ERROR(ctx.response_, fmt_err_msg.ptr());
    }
    LOG_WARN(
        "gen command faild",
        K(ret),
        K(ctx.request_.get_cmd_name()),
        K(ctx.request_.get_args()));
  } else if (FALSE_IT(ctx.cmd_type_ = cmd->cmd_type())) {
  } else if (cmd->cmd_group() == ObRedisCmdGroup::GENERIC_CMD) {
    if (ctx.request_.get_args().empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected empty args", K(ret), K(ctx.request_.get_cmd_name()));
    } else {
      ObObj objs[ObRedisUtil::COMPLEX_ROWKEY_NUM];
      objs[ObRedisUtil::COL_IDX_DB].set_int(ctx.request_.get_db());
      objs[ObRedisUtil::COL_IDX_RKEY].set_varbinary(ctx.request_.get_args().at(0));
      ObRowkey cur_rowkey(objs, ObRedisUtil::COMPLEX_ROWKEY_NUM);
      if (OB_FAIL(ctx.init_cmd_ctx(cur_rowkey, ctx.request_.get_args()))) {
        LOG_WARN("fail to init cmd ctx", K(ret));
      } else {
        ctx.need_dist_das_ = !ctx.cmd_ctx_->get_in_same_ls();
      }
    }
  } else {
    ctx.need_dist_das_ = cmd->use_dist_das();
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(start_trans(ctx, cmd->need_snapshot()))) {
    LOG_WARN("fail to start trans", K(ret), K(ctx));
  } else if (OB_FALSE_IT(lock_mode = cmd->get_lock_mode())) {
  } else if (lock_mode != REDIS_LOCK_MODE::LOCK_FREE) {
    if (OB_ISNULL(trans_lock_handle) &&
        OB_FAIL(HTABLE_LOCK_MGR->acquire_handle(ctx.trans_param_->trans_desc_->tid(), trans_lock_handle))) {
      LOG_WARN("fail to get htable lock handle", K(ret), "tx_id", ctx.trans_param_->trans_desc_->tid());
    } else if (OB_FAIL(ObRedisHelper::get_lock_key(ctx.allocator_, ctx.request_, lock_key))) {
      LOG_WARN("fail to get lock key from entity", K(ret));
    } else if (OB_FAIL(ObHTableUtils::lock_redis_key(
                   ctx.table_id_,
                   lock_key,
                   *trans_lock_handle,
                   lock_mode == REDIS_LOCK_MODE::SHARED ? ObHTableLockMode::SHARED : ObHTableLockMode::EXCLUSIVE))) {
      LOG_WARN("fail to lock redis key", K(ret), K(ctx));
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(cmd->apply(ctx))) {
    LOG_WARN(
        "command apply failed",
        K(ret),
        K(ctx.request_.get_cmd_name()),
        K(ctx.request_.get_args()));
  }
  // NOTE: must be called after cmd->apply(), redis err_code cover to success
  ret = COVER_REDIS_ERROR(ret);

  bool is_rollback = (OB_SUCCESS != ret);
  if (OB_NOT_NULL(cmd)) {
    int tmp_ret = OB_SUCCESS;
    // cmd pointer need manual destruction
    cmd->~RedisCommand();
    // exec end_trans regardless of whether ret is OB_SUCCESS
    if (OB_SUCCESS != (tmp_ret = end_trans(ctx, cmd->need_snapshot(), is_rollback))) {
      LOG_WARN("fail to end trans", K(ret), K(is_rollback));
      ret = COVER_SUCC(tmp_ret);
    }
  }

  return ret;
}

int ObRedisService::execute_cmd_group(ObRedisSingleCtx &ctx)
{
  int ret = OB_SUCCESS;

  RedisCommand *cmd = nullptr;
  // process cmd with group
  ObTableGroupCtx group_ctx(ctx.allocator_);
  ObRedisOp *redis_op = nullptr;
  ObRedisCmdKey key;
  ObString fmt_err_msg;
  ObTableID table_id = ctx.redis_guard_.simple_table_schema_->get_table_id();
  if (OB_FAIL(alloc_group_op(redis_op))) {
    LOG_WARN("fail to alloc group op", K(ret));
  } else if (OB_FAIL(ObRedisCommandFactory::gen_group_command(
      *redis_op, ctx.request_.get_cmd_name(), ctx.request_.get_args(), fmt_err_msg, cmd))) {
    if (ret == OB_KV_REDIS_ERROR) {
      RESPONSE_REDIS_ERROR(ctx.response_, fmt_err_msg.ptr());
    }
    LOG_WARN(
        "gen command faild",
        K(ret),
        K(ctx.request_.get_cmd_name()),
        K(ctx.request_.get_args()));
  } else if (FALSE_IT(key.cmd_type_ = cmd->cmd_type())) {
  } else if (FALSE_IT(key.table_id_ = table_id)) {
  } else if (FALSE_IT(ctx.cmd_type_ = cmd->cmd_type())) {
  } else if (OB_FAIL(redis_op->init(ctx, cmd, ObTableGroupType::TYPE_REDIS_GROUP))) {
    LOG_WARN("fail to init redis op", K(ret));
  } else if (OB_FAIL(init_group_ctx(group_ctx, ctx, *redis_op, &key))) {
    LOG_WARN("fail to init group ctx", K(ret));
  } else if (OB_FAIL(ObTableGroupService::process(group_ctx, redis_op, !ctx.is_enable_group_op()))) {
    LOG_WARN(
        "fail to process group op",
        K(ret));  // can not K(ctx) or KPC_(group_single_op), cause req may have been free
  } else {
    ctx.did_async_commit_ = true;  // do not response packet anyway
  }

  if (OB_FAIL(ret)) {
    // ret != OB_SUCCESS mean we should response packet by the responser in rpc processor
    // and here we should free redis_op in hand
    if (OB_NOT_NULL(redis_op)) {
      TABLEAPI_GROUP_COMMIT_MGR->free_op(redis_op);
      redis_op = nullptr;
    }
  }
  // NOTE: Must be called to turn redis_err into success
  ret = COVER_REDIS_ERROR(ret);

  return ret;
}

int ObRedisService::execute(ObRedisSingleCtx &ctx)
{
  int ret = OB_SUCCESS;

  if (!ctx.valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("redis ctx is invalid", K(ret), K(ctx));
  } else if (ctx.is_cmd_support_group()) {
    ret = execute_cmd_group(ctx);
  } else {
    ret = execute_cmd_single(ctx);
  }

  return ret;
}

int ObRedisService::start_trans(ObRedisCtx &redis_ctx, bool need_snapshot)
{
  int ret = OB_SUCCESS;
  ObTableTransParam *trans_param = redis_ctx.trans_param_;

  if (OB_ISNULL(trans_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("trans param is null", K(ret));
  } else if (need_snapshot) {  // read only cmd
    if (OB_FAIL(trans_param->init(need_snapshot,
                                  redis_ctx.consistency_level_,
                                  redis_ctx.ls_id_,
                                  redis_ctx.timeout_ts_,
                                  redis_ctx.need_dist_das_))) {
      LOG_WARN("fail to init trans param", K(ret));
    } else if (OB_FAIL(ObTableTransUtils::init_read_trans(*trans_param))) {
      LOG_WARN("fail to init read trans", K(ret), KPC(trans_param));
    }
  } else {
    if (OB_FAIL(trans_param->init(need_snapshot,
                                  redis_ctx.consistency_level_,
                                  redis_ctx.ls_id_,
                                  redis_ctx.timeout_ts_,
                                  redis_ctx.need_dist_das_))) {
      LOG_WARN("fail to init trans param", K(ret));
    } else if (OB_FAIL(ObTableTransUtils::start_trans(*trans_param))) {
      LOG_WARN("fail to start trans", K(ret), KPC(trans_param));
    }
  }

  return ret;
}

int ObRedisService::end_trans(ObRedisSingleCtx &redis_ctx, bool need_snapshot, bool is_rollback)
{
  int ret = OB_SUCCESS;
  ObTableTransParam *trans_param = redis_ctx.trans_param_;

  if (OB_ISNULL(trans_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("trans param is null", K(ret));
  } else if (need_snapshot) {  // read only cmd
    if (redis_ctx.trans_param_->lock_handle_ != nullptr) {
      HTABLE_LOCK_MGR->release_handle(*redis_ctx.trans_param_->lock_handle_);
    }
    ObTableTransUtils::release_read_trans(trans_param->trans_desc_);
  } else {
    ObTableRedisCbFunctor functor;
    if (OB_FAIL(functor.init(redis_ctx.rpc_req_, &redis_ctx.response_))) {
      LOG_WARN("fail to init create batch execute callback functor", K(ret));
    } else {
      trans_param->is_rollback_ = is_rollback;
      trans_param->req_ = redis_ctx.rpc_req_;
      trans_param->use_sync_ = false;
      trans_param->create_cb_functor_ = &functor;
      if (OB_FAIL(ObTableTransUtils::end_trans(*trans_param))) {
        LOG_WARN("fail to end trans", K(ret), KPC(trans_param));
      }

      // maybe ObTableTransUtils::end_trans has been failed, but has been callback
      // so we need to set did_async_commit_
      if (trans_param->did_async_commit_) {
        redis_ctx.did_async_commit_ = true;
      }
    }
  }

  return ret;
}
ObTableProccessType ObRedisService::get_stat_process_type(const RedisCommandType &cmd_type)
{
  int ret = OB_SUCCESS;
  ObTableProccessType type = ObTableProccessType::TABLE_API_PROCESS_TYPE_INVALID;
  int iret = static_cast<int>(cmd_type + observer::ObTableProccessType::TABLE_API_REDIS_TYPE_OFFSET);
  if (iret <= observer::ObTableProccessType::TABLE_API_REDIS_TYPE_OFFSET
      || iret >= observer::ObTableProccessType::TABLE_API_REDIS_TYPE_MAX) {
    LOG_WARN("invalid redis type", K(cmd_type), K(iret), "max type", TABLE_API_REDIS_TYPE_MAX);
  } else {
    type = static_cast<observer::ObTableProccessType>(iret);
  }
  return type;
}

}
}
