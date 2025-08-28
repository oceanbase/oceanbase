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
#include "ob_table_end_trans_cb.h"
#include "storage/tx/ob_trans_service.h"
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::rpc;

int ObTableExecuteCreateCbFunctor::init(ObRequest *req,
                                        const ObTableOperationResult *result,
                                        ObTableOperationType::Type op_type)
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
      op_type_ = op_type;
      is_inited_ = true;
    }
  }

  return ret;
}

ObTableAPITransCb* ObTableExecuteCreateCbFunctor::new_callback()
{
  ObTableExecuteEndTransCb *cb = nullptr;
  if (is_inited_) {
    cb = OB_NEW(ObTableExecuteEndTransCb,
                ObMemAttr(MTL_ID(), "TbExuTnCb"),
                req_,
                op_type_);
    if (NULL != cb) {
      int ret = OB_SUCCESS;
      if (OB_ISNULL(result_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret));
      } else if (OB_FAIL(cb->assign_execute_result(*result_))) {
        LOG_WARN("fail to assign result", K(ret));
        cb->~ObTableExecuteEndTransCb();
        cb = NULL;
        ob_free(cb);
      }
    }
  }
  return cb;
}

int ObTableBatchExecuteCreateCbFunctor::init(ObRequest *req,
                                             const ObTableBatchOperationResult *result,
                                             ObTableOperationType::Type op_type)
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
      op_type_ = op_type;
      is_inited_ = true;
    }
  }

  return ret;
}

ObTableAPITransCb* ObTableBatchExecuteCreateCbFunctor::new_callback()
{
  ObTableBatchExecuteEndTransCb *cb = nullptr;
  if (is_inited_) {
    cb = OB_NEW(ObTableBatchExecuteEndTransCb,
                ObMemAttr(MTL_ID(), "TbBchExuTnCb"),
                req_,
                op_type_);
    if (NULL != cb) {
      int ret = OB_SUCCESS;
      if (OB_ISNULL(result_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret));
      } else if (OB_FAIL(cb->assign_batch_execute_result(*result_))) {
        LOG_WARN("fail to assign result", K(ret));
        cb->~ObTableBatchExecuteEndTransCb();
        cb = NULL;
        ob_free(cb);
      }
    }
  }
  return cb;
}

int ObTableLSExecuteCreateCbFunctor::init(ObRequest *req)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    if (OB_ISNULL(req)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("request is null", K(ret));
    } else {
      ObTableLSExecuteEndTransCb *cb = OB_NEW(ObTableLSExecuteEndTransCb,
                                              ObMemAttr(MTL_ID(), "TbLsExuTnCb"), req);
      if (OB_ISNULL(cb)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memroy for ls execute callback", K(ret));
      } else {
        cb_ = cb;
        req_ = req;
        is_inited_ = true;
      }
    }
  }
  return ret;
}

ObTableAPITransCb* ObTableLSExecuteCreateCbFunctor::new_callback()
{
  ObTableAPITransCb *cb = nullptr;
  if (is_inited_) {
    cb = cb_;
  }
  return cb;
}

ObTableAPITransCb::ObTableAPITransCb()
    : tx_desc_(nullptr),
      lock_handle_(nullptr),
      ref_count_(2)
{
  create_ts_ = common::ObClockGenerator::getClock();
  if (ObCurTraceId::get_trace_id() != nullptr) {
    trace_id_ = *ObCurTraceId::get_trace_id();
  }
}

ObTableAPITransCb::~ObTableAPITransCb()
{
  LOG_DEBUG("[yzfdebug] ObTableAPITransCb destruct", K_(ref_count));
}

void ObTableAPITransCb::destroy_cb_if_no_ref()
{
  int32_t new_ref = ATOMIC_SAF(&ref_count_, 1);
  if (0 >= new_ref) {
    // @caution !!!
    destroy_cb();
  }
}

void ObTableAPITransCb::destroy_cb()
{
  this->~ObTableAPITransCb();
  ob_free(this);
}

void ObTableAPITransCb::set_lock_handle(ObHTableLockHandle *lock_handle)
{
  lock_handle_ = lock_handle;
}

// be called in callback() function
void ObTableAPITransCb::check_callback_timeout()
{
  int ret = OB_ERR_TOO_MUCH_TIME;
  const int64_t cur_ts = common::ObClockGenerator::getClock();
  const int64_t cost = cur_ts - create_ts_;
  const int64_t config_ts = GCONF.trace_log_slow_query_watermark; // default 1s
  if (cost > config_ts) {
    LOG_INFO("obkv trans callback cost too mush time", K(ret), K(cost), K(config_ts), K_(trace_id));
  }
}

////////////////////////////////////////////////////////////////
void ObTableExecuteEndTransCb::callback(int cb_param)
{
  int ret = OB_SUCCESS;
  check_callback_timeout();
  if (OB_UNLIKELY(!has_set_need_rollback_)) {
    LOG_ERROR("is_need_rollback_ has not been set",
              K(has_set_need_rollback_),
              K(is_need_rollback_));
  } else if (OB_UNLIKELY(ObExclusiveEndTransCallback::END_TRANS_TYPE_INVALID == end_trans_type_)) {
    LOG_WARN("end trans type is invalid", K(cb_param), K(end_trans_type_));
  } else if (OB_NOT_NULL(tx_desc_)) {
    MTL(transaction::ObTransService*)->release_tx(*tx_desc_);
    tx_desc_ = NULL;
  }
  if (lock_handle_ != nullptr) {
    HTABLE_LOCK_MGR->release_handle(*lock_handle_); 
  }
  this->handin();
  CHECK_BALANCE("[table async callback]");
  if (cb_param != OB_SUCCESS) {
    // commit failed
    result_.set_err(cb_param);
    result_.set_affected_rows(0);
    result_entity_.reset();
  }
  if (OB_FAIL(response_sender_.response(cb_param))) {
    LOG_WARN("failed to send response", K(ret), K(cb_param));
  } else {
    LOG_DEBUG("async send execute response", K(cb_param));
  }

  this->destroy_cb_if_no_ref();
}

void ObTableExecuteEndTransCb::callback(int cb_param, const transaction::ObTransID &trans_id)
{
  UNUSED(trans_id);
  this->callback(cb_param);
}

// when the operation is append/increment and returning_affected_entity is true, we will return the
// new values after append/increment to the client, so we need to deep copy the entity_result here.
int ObTableExecuteEndTransCb::assign_execute_result(const ObTableOperationResult &result)
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

////////////////////////////////////////////////////////////////
void ObTableBatchExecuteEndTransCb::callback(int cb_param)
{
  int ret = OB_SUCCESS;
  check_callback_timeout();
  if (OB_UNLIKELY(!has_set_need_rollback_)) {
    LOG_ERROR("is_need_rollback_ has not been set",
              K(has_set_need_rollback_),
              K(is_need_rollback_));
  } else if (OB_UNLIKELY(ObExclusiveEndTransCallback::END_TRANS_TYPE_INVALID == end_trans_type_)) {
    LOG_WARN("end trans type is invalid", K(cb_param), K(end_trans_type_));
  } else if (OB_NOT_NULL(tx_desc_)) {
    MTL(transaction::ObTransService*)->release_tx(*tx_desc_);
    tx_desc_ = NULL;
  }
  if (lock_handle_ != nullptr) {
    HTABLE_LOCK_MGR->release_handle(*lock_handle_); 
  }
  this->handin();
  CHECK_BALANCE("[table batch async callback]");
  if (cb_param != OB_SUCCESS) {
    result_.reset();
  }
  if (0 >= result_.count()) {
    // same result for all
    ObTableOperationResult single_op_result;
    single_op_result.set_entity(result_entity_);
    single_op_result.set_err(cb_param);
    single_op_result.set_type(table_operation_type_);
    if (OB_FAIL(result_.push_back(single_op_result))) {
      LOG_WARN("failed to add result", K(ret));  // @todo reset the connection
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(response_sender_.response(cb_param))) {
      LOG_WARN("failed to send response", K(ret), K(cb_param));
    } else {
      LOG_DEBUG("yzfdebug async send batch_execute response", K(cb_param));
    }
  }
  this->destroy_cb_if_no_ref();
}

void ObTableBatchExecuteEndTransCb::callback(int cb_param, const transaction::ObTransID &trans_id)
{
  UNUSED(trans_id);
  this->callback(cb_param);
}

int ObTableBatchExecuteEndTransCb::assign_batch_execute_result(const ObTableBatchOperationResult &result)
{
  int ret = OB_SUCCESS;
  result_.reset();
  ObTableOperationResult dest_result;
  int64_t N = result.count();
  for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
  {
    const ObTableOperationResult &src_result = result.at(i);
    if (OB_FAIL(dest_result.deep_copy(allocator_, entity_factory_, src_result))) {
      LOG_WARN("failed to deep copy result", K(ret));
    } else if (OB_FAIL(result_.push_back(dest_result))) {
      LOG_WARN("failed to push back", K(ret));
    }
  } // end for
  return ret;
}

void ObTableLSExecuteEndTransCb::callback(int cb_param)
{
  int ret = OB_SUCCESS;
  check_callback_timeout();
  if (OB_UNLIKELY(!has_set_need_rollback_)) {
    LOG_ERROR("is_need_rollback_ has not been set",
              K(has_set_need_rollback_),
              K(is_need_rollback_));
  } else if (OB_UNLIKELY(ObExclusiveEndTransCallback::END_TRANS_TYPE_INVALID == end_trans_type_)) {
    LOG_WARN("end trans type is invalid", K(cb_param), K(end_trans_type_));
  } else if (OB_NOT_NULL(tx_desc_)) {
    MTL(transaction::ObTransService*)->release_tx(*tx_desc_);
    tx_desc_ = NULL;
  }
  if (lock_handle_ != nullptr) {
    HTABLE_LOCK_MGR->release_handle(*lock_handle_); 
  }
  this->handin();
  CHECK_BALANCE("[table ls execute async callback]");
  if (cb_param != OB_SUCCESS) {
    result_.reset();
  }
  if (result_.empty()) {
    ObTableTabletOpResult tablet_result;
    ObTableSingleOpResult single_op_result;
    single_op_result.set_entity(result_entity_);
    single_op_result.set_errno(cb_param);
    if (OB_FAIL(tablet_result.push_back(single_op_result))) {
      LOG_WARN("failed to add single result", K(ret));
    } else if (OB_FAIL(result_.push_back(tablet_result))) {
      LOG_WARN("failed to add tablet result", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(response_sender_.response(cb_param))) {
      LOG_WARN("failed to send response", K(ret), K(cb_param));
    } else {
      LOG_DEBUG("send ls execute response", K(cb_param));
    }
  }

  free_dependent_results();
  this->destroy_cb_if_no_ref();
}

void ObTableLSExecuteEndTransCb::free_dependent_results()
{
  for (int64_t i = 0; i < dependent_results_.count(); i++) {
    if (OB_NOT_NULL(dependent_results_.at(i))) {
      if (is_alloc_from_pool_) {
        TABLEAPI_OBJECT_POOL_MGR->free_res(dependent_results_.at(i));
      } else {
        dependent_results_.at(i)->~ObTableLSOpResult();
      }
      dependent_results_.at(i) = nullptr;
    }
  }
}

void ObTableLSExecuteEndTransCb::callback(int cb_param, const transaction::ObTransID &trans_id)
{
  UNUSED(trans_id);
  this->callback(cb_param);
}
