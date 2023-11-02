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

#include "observer/table_load/ob_table_load_store_trans.h"
#include "observer/table_load/ob_table_load_store.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/ob_table_load_trans_store.h"
#include "sql/engine/cmd/ob_load_data_utils.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace lib;
using namespace table;

ObTableLoadStoreTrans::ObTableLoadStoreTrans(ObTableLoadTransCtx *trans_ctx)
  : trans_ctx_(trans_ctx),
    trans_store_(nullptr),
    trans_store_writer_(nullptr),
    ref_count_(0),
    is_dirty_(false),
    is_inited_(false)
{
}

ObTableLoadStoreTrans::~ObTableLoadStoreTrans()
{
  if (nullptr != trans_store_writer_) {
    trans_store_writer_->~ObTableLoadTransStoreWriter();
    trans_ctx_->allocator_.free(trans_store_writer_);
    trans_store_writer_ = nullptr;
  }
  if (nullptr != trans_store_) {
    trans_store_->~ObTableLoadTransStore();
    trans_ctx_->allocator_.free(trans_store_);
    trans_store_ = nullptr;
  }
}

int ObTableLoadStoreTrans::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadStoreTrans init twice", KR(ret), KP(this));
  } else {
    if (OB_ISNULL(trans_store_ =
                    OB_NEWx(ObTableLoadTransStore, (&trans_ctx_->allocator_), trans_ctx_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObTableLoadTransStore", KR(ret));
    } else if (OB_ISNULL(trans_store_writer_ = OB_NEWx(ObTableLoadTransStoreWriter,
                                                       (&trans_ctx_->allocator_), trans_store_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObTableLoadTransStoreWriter", KR(ret));
    } else if (OB_FAIL(trans_store_->init())) {
      LOG_WARN("fail to init trans store", KR(ret));
    } else if (OB_FAIL(trans_store_writer_->init())) {
      LOG_WARN("fail to init trans store writer", KR(ret));
    } else if (OB_FAIL(set_trans_status_inited())) {
      LOG_WARN("fail to set trans status inited", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadStoreTrans::advance_trans_status(ObTableLoadTransStatusType trans_status)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(trans_ctx_->advance_trans_status(trans_status))) {
    LOG_WARN("fail to advance trans status", KR(ret), K(trans_status));
  } else {
    table_load_trans_status_to_string(trans_status,
                                      trans_ctx_->ctx_->job_stat_->store.trans_status_);
  }
  return ret;
}

int ObTableLoadStoreTrans::set_trans_status_error(int error_code)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(trans_ctx_->set_trans_status_error(error_code))) {
    LOG_WARN("fail to set trans status error", KR(ret));
  } else {
    table_load_trans_status_to_string(ObTableLoadTransStatusType::ERROR,
                                      trans_ctx_->ctx_->job_stat_->store.trans_status_);
  }
  return ret;
}

int ObTableLoadStoreTrans::set_trans_status_abort()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(trans_ctx_->set_trans_status_abort())) {
    LOG_WARN("fail to set trans status abort", KR(ret));
  } else {
    table_load_trans_status_to_string(ObTableLoadTransStatusType::ABORT,
                                      trans_ctx_->ctx_->job_stat_->store.trans_status_);
  }
  return ret;
}

int ObTableLoadStoreTrans::get_store_writer(ObTableLoadTransStoreWriter *&store_writer) const
{
  int ret = OB_SUCCESS;
  store_writer = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStoreTrans not init", KR(ret), KP(this));
  } else {
    obsys::ObRLockGuard guard(trans_ctx_->rwlock_);
    if (OB_ISNULL(trans_store_writer_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null store writer", KR(ret));
    } else {
      store_writer = trans_store_writer_;
      store_writer->inc_ref_count();
    }
  }
  return ret;
}

void ObTableLoadStoreTrans::put_store_writer(ObTableLoadTransStoreWriter *store_writer)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStoreTrans not init", KR(ret));
  } else if (OB_ISNULL(store_writer)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null store", KR(ret));
  } else {
    obsys::ObRLockGuard guard(trans_ctx_->rwlock_);
    OB_ASSERT(trans_store_writer_ == store_writer);
  }
  if (OB_SUCC(ret)) {
    if (0 == store_writer->dec_ref_count() && OB_FAIL(handle_write_done())) {
      LOG_WARN("fail to handle store write done", KR(ret));
    }
  }
  if (OB_FAIL(ret)) {
    set_trans_status_error(ret);
  }
}

int ObTableLoadStoreTrans::handle_write_done()
{
  int ret = OB_SUCCESS;
  if (ObTableLoadTransStatusType::FROZEN == trans_ctx_->get_trans_status()) {
    if (OB_FAIL(set_trans_status_commit())) {
      LOG_WARN("fail to set trans status commit", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadStoreTrans::output_store(ObTableLoadTransStore *&trans_store)
{
  int ret = OB_SUCCESS;
  trans_store = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStoreTrans not init", KR(ret), KP(this));
  } else {
    obsys::ObWLockGuard guard(trans_ctx_->rwlock_);
    if (OB_ISNULL(trans_store_) || OB_ISNULL(trans_store_writer_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null store", KR(ret), KP_(trans_store), KP_(trans_store_writer));
    } else if (OB_UNLIKELY(0 != trans_store_writer_->get_ref_count())) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("store writer already be hold", KR(ret));
    } else if (OB_UNLIKELY(ObTableLoadTransStatusType::COMMIT != trans_ctx_->trans_status_)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("store is not committed", KR(ret));
    } else {
      trans_store = trans_store_;
      trans_store_ = nullptr;
    }
  }
  return ret;
}

}  // namespace observer
}  // namespace oceanbase
