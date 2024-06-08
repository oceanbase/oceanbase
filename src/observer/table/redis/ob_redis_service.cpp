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

using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::share;
using namespace oceanbase::sql;
using namespace oceanbase::rpc;

///////////////////////////////////////////////////////////////////////////////////
bool ObRedisCtx::valid() const
{
  bool valid = true;

  if (OB_ISNULL(stat_event_type_) || OB_ISNULL(trans_param_) || OB_ISNULL(entity_factory_) || OB_ISNULL(credential_)) {
    valid = false;
    int ret = OB_ERR_UNEXPECTED;
    LOG_WARN("redis ctx is not valid", KP_(stat_event_type), KP_(trans_param), KP_(entity_factory), KP_(credential));
  }
  return valid;
}

int ObRedisCtx::decode_request()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(request_.decode())) {
    LOG_WARN("fail to decode request", K(ret));
  }

  return ret;
}

///////////////////////////////////////////////////////////////////////////////////

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
    result_.set_err(cb_param);
    result_.set_affected_rows(0);
    result_entity_.reset();
  }
  if (OB_FAIL(response_sender_.response(cb_param))) {
    // overwrite ret
    LOG_WARN("failed to send redis response", K(ret), K(cb_param));
  } else {
    LOG_DEBUG("async send redis response", K(cb_param));
  }

  this->destroy_cb_if_no_ref();
}

int ObTableRedisEndTransCb::assign_execute_result(const ObTableOperationResult &result)
{
  int ret = OB_SUCCESS;
  const ObITableEntity *src_entity = NULL;
  if (OB_FAIL(result.get_entity(src_entity))) {
    LOG_WARN("failed to get entity", K(ret));
  } else if (OB_FAIL(result_entity_.deep_copy(allocator_, *src_entity))) {
    LOG_WARN("failed to copy entity", K(ret));
  } else {
    result_ = result;
    result_.set_entity(result_entity_);
  }
  return ret;
}

int ObTableRedisCbFunctor::init(ObRequest *req, const ObTableOperationResult *result)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    if (OB_ISNULL(req)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("request is null", K(ret));
    } else if (OB_ISNULL(result)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("result is null", K(ret));
    } else {
      req_ = req;
      result_ = result;
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
      if (OB_ISNULL(result_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret));
      } else if (OB_FAIL(cb->assign_execute_result(*result_))) {
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

int ObRedisService::execute(ObRedisCtx &ctx)
{
  int ret = OB_SUCCESS;
  RedisCommand *cmd = nullptr;
  ObHTableLockHandle *&trans_lock_handle = ctx.trans_param_->lock_handle_;
  ObString lock_key;
  REDIS_LOCK_MODE lock_mode;
  if (!ctx.valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("redis ctx is invalid", K(ret), K(ctx));
  } else if (OB_FAIL(ObRedisCommandFactory::gen_command(
                 ctx.allocator_, ctx.request_.get_command_type(), ctx.request_.get_args(), cmd))) {
    LOG_WARN("gen command faild",
             K(ret),
             K(ctx.request_.get_cmd_name()),
             K(ctx.request_.get_command_type()),
             K(ctx.request_.get_args()));
  } else if (FALSE_IT(ctx.tb_ctx_.set_need_dist_das(cmd->use_dist_das()))) {
  } else if (OB_FAIL(start_trans(ctx, cmd->need_snapshot()))) {
    LOG_WARN("fail to start trans", K(ret), K(ctx));
  } else if (OB_FALSE_IT(lock_mode = cmd->get_lock_mode())) {
  } else if (lock_mode != REDIS_LOCK_MODE::LOCK_FREE) {
    if (OB_ISNULL(trans_lock_handle) &&
        OB_FAIL(HTABLE_LOCK_MGR->acquire_handle(ctx.trans_param_->trans_desc_->tid(), trans_lock_handle))) {
      LOG_WARN("fail to get htable lock handle", K(ret), "tx_id", ctx.trans_param_->trans_desc_->tid());
    } else if (OB_FAIL(RedisOperationHelper::get_lock_key(ctx.allocator_, ctx.request_, lock_key))) {
      LOG_WARN("fail to get lock key from entity", K(ret), K(ctx.get_entity()));
    } else if (OB_FAIL(ObHTableUtils::lock_redis_key(
                   ctx.table_id_,
                   lock_key,
                   *trans_lock_handle,
                   lock_mode == REDIS_LOCK_MODE::SHARED ? ObHTableLockMode::SHARED : ObHTableLockMode::EXCLUSIVE))) {
      LOG_WARN("fail to lock redis key", K(ret), K(ctx));
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(cmd->apply(ctx))) {
    LOG_WARN("command apply faild", K(ret), K(ctx.request_.get_cmd_name()), K(ctx.request_.get_args()));
  }

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

int ObRedisService::start_trans(ObRedisCtx &redis_ctx, bool need_snapshot)
{
  int ret = OB_SUCCESS;
  ObTableTransParam *trans_param = redis_ctx.trans_param_;

  if (OB_ISNULL(trans_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("trans param is null", K(ret));
  } else if (need_snapshot) {  // read only cmd
    if (OB_FAIL(trans_param->init(redis_ctx.consistency_level_,
                                  redis_ctx.ls_id_,
                                  redis_ctx.timeout_ts_,
                                  redis_ctx.tb_ctx_.need_dist_das()))) {
      LOG_WARN("fail to init trans param", K(ret));
    } else if (OB_FAIL(ObTableTransUtils::init_read_trans(*trans_param))) {
      LOG_WARN("fail to init read trans", K(ret), KPC(trans_param));
    }
  } else {
    if (OB_FAIL(trans_param->init(need_snapshot,
                                  redis_ctx.consistency_level_,
                                  redis_ctx.ls_id_,
                                  redis_ctx.timeout_ts_,
                                  redis_ctx.tb_ctx_.need_dist_das()))) {
      LOG_WARN("fail to init trans param", K(ret));
    } else if (OB_FAIL(ObTableTransUtils::start_trans(*trans_param))) {
      LOG_WARN("fail to start trans", K(ret), KPC(trans_param));
    }
  }

  return ret;
}

int ObRedisService::end_trans(ObRedisCtx &redis_ctx, bool need_snapshot, bool is_rollback)
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
    if (OB_FAIL(functor.init(redis_ctx.rpc_req_, &redis_ctx.response_.get_result()))) {
      LOG_WARN("fail to init create batch execute callback functor", K(ret));
    } else {
      trans_param->is_rollback_ = is_rollback;
      trans_param->req_ = redis_ctx.rpc_req_;
      trans_param->use_sync_ = false;
      trans_param->create_cb_functor_ = &functor;
      if (OB_FAIL(ObTableTransUtils::end_trans(*trans_param))) {
        LOG_WARN("fail to end trans", K(ret), KPC(trans_param));
      }
    }
  }

  return ret;
}
