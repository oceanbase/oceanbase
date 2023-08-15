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

#include "observer/table_load/ob_table_load_coordinator_trans.h"
#include "observer/table_load/ob_table_load_coordinator.h"
#include "observer/table_load/ob_table_load_trans_bucket_writer.h"
#include "sql/engine/cmd/ob_load_data_utils.h"
#include "observer/table_load/ob_table_load_table_ctx.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace lib;
using namespace table;

ObTableLoadCoordinatorTrans::ObTableLoadCoordinatorTrans(ObTableLoadTransCtx *trans_ctx, int32_t default_session_id)
  : trans_ctx_(trans_ctx),
    default_session_id_(default_session_id),
    trans_bucket_writer_(nullptr),
    ref_count_(0),
    is_dirty_(false),
    is_inited_(false)
{
}

ObTableLoadCoordinatorTrans::~ObTableLoadCoordinatorTrans()
{
  if (nullptr != trans_bucket_writer_) {
    trans_bucket_writer_->~ObTableLoadTransBucketWriter();
    trans_ctx_->allocator_.free(trans_bucket_writer_);
    trans_bucket_writer_ = nullptr;
  }
}

int ObTableLoadCoordinatorTrans::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadCoordinatorTrans init twice", KR(ret), KP(this));
  } else {
    if (OB_ISNULL(trans_bucket_writer_ =
                    OB_NEWx(ObTableLoadTransBucketWriter, (&trans_ctx_->allocator_), trans_ctx_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObTableLoadTransBucketWriter", KR(ret));
    } else if (OB_FAIL(trans_bucket_writer_->init())) {
      LOG_WARN("fail to init trans bucket writer", KR(ret));
    } else if (OB_FAIL(set_trans_status_inited())) {
      LOG_WARN("fail to set trans status inited", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadCoordinatorTrans::advance_trans_status(ObTableLoadTransStatusType trans_status)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(trans_ctx_->advance_trans_status(trans_status))) {
    LOG_WARN("fail to advance trans status", KR(ret), K(trans_status));
  } else {
    table_load_trans_status_to_string(trans_status,
                                      trans_ctx_->ctx_->job_stat_->coordinator.trans_status_);
  }
  return ret;
}

int ObTableLoadCoordinatorTrans::set_trans_status_error(int error_code)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(trans_ctx_->set_trans_status_error(error_code))) {
    LOG_WARN("fail to set trans status error", KR(ret));
  } else {
    table_load_trans_status_to_string(ObTableLoadTransStatusType::ERROR,
                                      trans_ctx_->ctx_->job_stat_->coordinator.trans_status_);
  }
  return ret;
}

int ObTableLoadCoordinatorTrans::set_trans_status_abort()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(trans_ctx_->set_trans_status_abort())) {
    LOG_WARN("fail to set trans status abort", KR(ret));
  } else {
    table_load_trans_status_to_string(ObTableLoadTransStatusType::ABORT,
                                      trans_ctx_->ctx_->job_stat_->coordinator.trans_status_);
  }
  return ret;
}

int ObTableLoadCoordinatorTrans::get_bucket_writer_for_write(
  ObTableLoadTransBucketWriter *&bucket_writer) const
{
  int ret = OB_SUCCESS;
  bucket_writer = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadCoordinatorTrans not init", KR(ret), KP(this));
  } else if (OB_FAIL(check_trans_status(ObTableLoadTransStatusType::RUNNING))) {
    LOG_WARN("fail to check trans status", KR(ret));
  } else {
    obsys::ObRLockGuard guard(trans_ctx_->rwlock_);
    if (OB_ISNULL(trans_bucket_writer_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null bucket writer", KR(ret));
    } else if (OB_UNLIKELY(trans_bucket_writer_->is_flush())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("trans bucket writer has flush", KR(ret));
    } else {
      bucket_writer = trans_bucket_writer_;
      bucket_writer->inc_ref_count();
    }
  }
  return ret;
}

int ObTableLoadCoordinatorTrans::get_bucket_writer_for_flush(
  ObTableLoadTransBucketWriter *&bucket_writer) const
{
  int ret = OB_SUCCESS;
  bucket_writer = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadCoordinatorTrans not init", KR(ret), KP(this));
  } else if (OB_FAIL(check_trans_status(ObTableLoadTransStatusType::FROZEN))) {
    LOG_WARN("fail to check trans status", KR(ret));
  } else {
    obsys::ObRLockGuard guard(trans_ctx_->rwlock_);
    if (OB_ISNULL(trans_bucket_writer_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null bucket writer", KR(ret));
    } else if (OB_UNLIKELY(trans_bucket_writer_->is_flush())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("trans bucket writer has flush", KR(ret));
    } else {
      trans_bucket_writer_->set_is_flush();
      bucket_writer = trans_bucket_writer_;
      bucket_writer->inc_ref_count();
    }
  }
  return ret;
}

void ObTableLoadCoordinatorTrans::put_bucket_writer(ObTableLoadTransBucketWriter *bucket_writer)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadCoordinatorTrans not init", KR(ret));
  } else if (OB_ISNULL(bucket_writer)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null bucket writer", KR(ret));
  } else {
    obsys::ObRLockGuard guard(trans_ctx_->rwlock_);
    OB_ASSERT(trans_bucket_writer_ == bucket_writer);
  }
  if (OB_SUCC(ret)) {
    if (0 == bucket_writer->dec_ref_count() && OB_FAIL(handle_write_done())) {
      LOG_WARN("fail to handle coordinator write done", KR(ret));
    }
  }
  if (OB_FAIL(ret)) {
    set_trans_status_error(ret);
  }
}

int ObTableLoadCoordinatorTrans::handle_write_done()
{
  int ret = OB_SUCCESS;
  if (ObTableLoadTransStatusType::FROZEN == trans_ctx_->get_trans_status()) {
    ObTableLoadCoordinator coordinator(trans_ctx_->ctx_);
    if (OB_FAIL(coordinator.init())) {
      LOG_WARN("fail to init coordinator", KR(ret));
    } else if (OB_FAIL(coordinator.finish_trans_peers(this))) {
      LOG_WARN("fail to finish trans peers", KR(ret));
    }
  }
  return ret;
}

}  // namespace observer
}  // namespace oceanbase
