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
                                      trans_ctx_->ctx_->job_stat_->coordinator_.trans_status_);
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
                                      trans_ctx_->ctx_->job_stat_->coordinator_.trans_status_);
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
                                      trans_ctx_->ctx_->job_stat_->coordinator_.trans_status_);
  }
  return ret;
}

}  // namespace observer
}  // namespace oceanbase
