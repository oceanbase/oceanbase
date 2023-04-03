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
 *
 * TransCtx manager
 */

#define USING_LOG_PREFIX  OBLOG

#define STAT_STR "[STAT] [TRANS_CTX] "
#define TCTX_STAT(level, format_str, ...) OBLOG_LOG(level, STAT_STR format_str, ##__VA_ARGS__)
#define TCTX_STAT_DEBUG(format_str, ...) TCTX_STAT(DEBUG, format_str, ##__VA_ARGS__)
#define TCTX_STAT_INFO(format_str, ...) TCTX_STAT(INFO, format_str, ##__VA_ARGS__)

#include "ob_log_trans_ctx_mgr.h"
#include "ob_log_utils.h"           // get_timestamp

#define DATA_PRINT(buf, size, pos, fmt, ...) \
  if (OB_SUCC(ret)) { \
    if (OB_FAIL(databuff_printf(buf, size, pos, fmt, ##__VA_ARGS__))) {\
      LOG_WARN("databuff_printf fail", KR(ret), K(size), K(pos)); \
    } \
  } \

using namespace oceanbase::common;
using namespace oceanbase::transaction;
namespace oceanbase
{
namespace libobcdc
{
void ObLogTransCtxMgr::Scanner::operator() (const TenantTransID &tenant_trans_id, TransCtx *trans_ctx)
{
  int ret = OB_SUCCESS;

  if (NULL != trans_ctx && NULL != buffer_ && buffer_size_ > 0 && tenant_trans_id.is_valid()) {
    if (pos_ >= buffer_size_) {
      LOG_WARN("print transaction information overflow", K(buffer_size_), K(pos_), K(tenant_trans_id), K(*trans_ctx));
    } else if (OB_FAIL(trans_ctx->lock())) {
      LOG_ERROR("lock trans_ctx fail", KR(ret), K(tenant_trans_id), K(*trans_ctx));
    } else if (trans_ctx->get_state() >= TransCtx::TRANS_CTX_STATE_PREPARED
        && trans_ctx->get_state() < TransCtx::TRANS_CTX_STATE_SEQUENCED) {
      int state = trans_ctx->get_state();
      char *ptr = buffer_;
      int64_t size = buffer_size_;
      const TransPartInfo *participants = trans_ctx->get_participants();
      int64_t participant_count = trans_ctx->get_participant_count();

      trans_count_[state]++;
      valid_trans_count_++;

      DATA_PRINT(ptr, size, pos_, "tenant_id:%lu;", tenant_trans_id.get_tenant_id());
      DATA_PRINT(ptr, size, pos_, "trans_id:%ld;", tenant_trans_id.get_tx_id().get_id());
      DATA_PRINT(ptr, size, pos_, "state:%s;", trans_ctx->print_state());
      DATA_PRINT(ptr, size, pos_, "ready_participant_count:%ld/%ld;",
          trans_ctx->get_ready_participant_count(), participant_count);
      DATA_PRINT(ptr, size, pos_, "participants:");

      for (int64_t index = 0; OB_SUCC(ret) && index < participant_count; index++) {
        DATA_PRINT(ptr, size, pos_, "%ld(%lu)",
            participants[index].tls_id_.get_ls_id().id(),
            participants[index].tls_id_.get_tenant_id());

        // Not NULL here, means the participant is ready
        if (NULL != participants[index].obj_) {
          DATA_PRINT(ptr, size, pos_, "[SEQ=%ld],",
              participants[index].obj_->get_checkpoint_seq());
        } else {
          DATA_PRINT(ptr, size, pos_, "[NOT_READY],");
        }
      }

      if (OB_SUCC(ret)) {
        if (participant_count > 0) {
          pos_--;
        }

        DATA_PRINT(ptr, size, pos_, ";");
      }

      if (OB_SUCC(ret)) {
        DATA_PRINT(ptr, size, pos_, "\n");
      }
    }

    (void)trans_ctx->unlock();
  }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

ObLogTransCtxMgr::ObLogTransCtxMgr() :
    inited_(false),
    map_(),
    need_sort_participant_(false),
    valid_trans_count_(0),
    created_trans_count_(0),
    last_created_trans_count_(0),
    sequenced_trans_count_(0),
    last_sequenced_trans_count_(0),
    last_stat_time_(0)
{
}

ObLogTransCtxMgr::~ObLogTransCtxMgr()
{
  destroy();
}

int ObLogTransCtxMgr::init(const int64_t max_cached_trans_ctx_count,
    const bool need_sort_participant)
{
  int ret = OB_SUCCESS;

  if (inited_) {
    ret = OB_INIT_TWICE;
  } else if (max_cached_trans_ctx_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(map_.init(max_cached_trans_ctx_count, BLOCK_SIZE, ObModIds::OB_LOG_TRANS_CTX))) {
    LOG_ERROR("init TransCtxMap fail", KR(ret), K(max_cached_trans_ctx_count));
  } else {
    inited_ = true;

    need_sort_participant_ = need_sort_participant;
    valid_trans_count_ = 0;
    (void)memset(trans_count_, 0, sizeof(trans_count_));
    created_trans_count_ = 0;
    last_created_trans_count_ = 0;
    sequenced_trans_count_ = 0;
    last_sequenced_trans_count_ = 0;
    last_stat_time_ = get_timestamp();
  }

  return ret;
}

void ObLogTransCtxMgr::destroy()
{
  inited_ = false;
  map_.destroy();

  need_sort_participant_ = false;
  valid_trans_count_ = 0;
  (void)memset(trans_count_, 0, sizeof(trans_count_));
  created_trans_count_ = 0;
  last_created_trans_count_ = 0;
  sequenced_trans_count_ = 0;
  last_sequenced_trans_count_ = 0;
  last_stat_time_ = 0;
}

int ObLogTransCtxMgr::get_trans_ctx(
    const uint64_t tenant_id,
    const transaction::ObTransID &tx_id,
    TransCtx *&trans_ctx,
    bool enable_create)
{
  int ret = OB_SUCCESS;
  TenantTransID key(tenant_id, tx_id);

  if (! inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid trans_id", KR(ret), K(key));
  } else if (OB_FAIL(map_.get(key, trans_ctx, enable_create))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("get TransCtx from map fail", KR(ret), K(key), K(enable_create));
    }
  } else {
    if (enable_create) {
      if (OB_FAIL(trans_ctx->init(this))) {
        LOG_ERROR("trans_ctx init fail", KR(ret));
      }
    }

    TCTX_STAT_DEBUG("get_trans_ctx", K(key), K(trans_ctx));
  }

  return ret;
}

int ObLogTransCtxMgr::revert_trans_ctx(TransCtx *trans_ctx)
{
  int ret = OB_SUCCESS;
  if (! inited_) {
    ret = OB_NOT_INIT;
  } else if (NULL == trans_ctx) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    TCTX_STAT_DEBUG("revert_trans_ctx", "key", trans_ctx->get_trans_id(), K(trans_ctx));

    if (OB_FAIL(map_.revert(trans_ctx))) {
      LOG_ERROR("revert TransCtx fail", KR(ret), K(trans_ctx));
    } else {
      // succ
      trans_ctx = NULL;
    }
  }
  return ret;
}

int ObLogTransCtxMgr::remove_trans_ctx(const uint64_t tenant_id, const transaction::ObTransID &tx_id)
{
  int ret = OB_SUCCESS;
  TenantTransID key(tenant_id, tx_id);

  if (! inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid trans_id", KR(ret), K(key));
  } else {
    TCTX_STAT_DEBUG("remove_trans_ctx", K(key));

    if (OB_FAIL(map_.remove(key))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_ERROR("remove TransCtx fail", KR(ret), K(key));
      }
    } else {
      // succ
    }
  }
  return ret;
}

int ObLogTransCtxMgr::update_stat_info(const int trans_state)
{
  int ret = OB_SUCCESS;
  static const int START_STATE = TransCtx::TRANS_CTX_STATE_INVALID + 1;
  static const int END_STATE = TransCtx::TRANS_CTX_STATE_MAX - 1;
  if (!TransCtx::is_state_valid(trans_state)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (START_STATE == trans_state) {
      ATOMIC_INC(&created_trans_count_);
      ATOMIC_INC(&valid_trans_count_);
      ATOMIC_INC(&(trans_count_[trans_state]));
    } else if (END_STATE == trans_state) {
      ATOMIC_DEC(&valid_trans_count_);
      ATOMIC_DEC(&(trans_count_[trans_state - 1]));
    } else {
      ATOMIC_INC(&(trans_count_[trans_state]));
      ATOMIC_DEC(&(trans_count_[trans_state - 1]));
      if (TransCtx::TRANS_CTX_STATE_SEQUENCED == trans_state) {
        ATOMIC_INC(&sequenced_trans_count_);
      }
    }
  }

  return ret;
}

void ObLogTransCtxMgr::print_stat_info()
{
  int64_t current_timestamp = get_timestamp();
  int64_t local_created_trans_count = ATOMIC_LOAD(&created_trans_count_);
  int64_t local_last_created_trans_count = ATOMIC_LOAD(&last_created_trans_count_);
  int64_t local_sequenced_trans_count = ATOMIC_LOAD(&sequenced_trans_count_);
  int64_t local_last_sequenced_trans_count = ATOMIC_LOAD(&last_sequenced_trans_count_);
  int64_t local_last_stat_time = last_stat_time_;
  int64_t delta_time = current_timestamp - local_last_stat_time;
  int64_t delta_create_count = local_created_trans_count - local_last_created_trans_count;
  int64_t delta_sequence_count = local_sequenced_trans_count - local_last_sequenced_trans_count;
  double create_tps = 0.0;
  double sequence_tps = 0.0;

  // Update the last statistics
  last_created_trans_count_ = local_created_trans_count;
  last_sequenced_trans_count_ = local_sequenced_trans_count;
  last_stat_time_ = current_timestamp;

  if (delta_time > 0) {
    create_tps = (double)(delta_create_count) * 1000000.0 / (double)delta_time;
    sequence_tps = (double)(delta_sequence_count) * 1000000.0 / (double) delta_time;
  }

  _LOG_INFO("[STAT] [PERF] CREATE_TRANS_TPS=%.3lf SEQ_TRANS_TPS=%.3lf CREATE_TRANS_COUNT=%ld "
      "SEQ_TRANS_COUNT=%ld",
      create_tps, sequence_tps, local_created_trans_count - local_last_created_trans_count,
      local_sequenced_trans_count - local_last_sequenced_trans_count);

  _LOG_INFO("[STAT] [TRANS_COUNT] TOTAL=%ld PREPARED=%ld PART_READY=%ld SEQ=%ld DATA_READY=%ld "
      "COMMITTED=%ld RECYCLE=%ld",
      valid_trans_count_,
      trans_count_[TransCtx::TRANS_CTX_STATE_PREPARED],
      trans_count_[TransCtx::TRANS_CTX_STATE_PARTICIPANT_READY],
      trans_count_[TransCtx::TRANS_CTX_STATE_SEQUENCED],
      trans_count_[TransCtx::TRANS_CTX_STATE_DATA_READY],
      trans_count_[TransCtx::TRANS_CTX_STATE_COMMITTED],
      trans_count_[TransCtx::TRANS_CTX_STATE_PARTICIPANT_REVERTED]);

  map_.print_state(STAT_STR);
}

int64_t ObLogTransCtxMgr::get_trans_count(const int trans_ctx_state) const
{
  int64_t ret_count = 0;

  if (TransCtx::is_state_valid(trans_ctx_state)) {
    ret_count = trans_count_[trans_ctx_state];
  } else {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "trans ctx state is invalid", K(trans_ctx_state));
  }

  return ret_count;
}

int ObLogTransCtxMgr::dump_pending_trans_info(char *buffer, const int64_t size, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (NULL != buffer && size > 0 && pos < size) {
    Scanner scanner;

    scanner.buffer_ = buffer;
    scanner.buffer_size_ = size;
    scanner.pos_ = pos;

    if (OB_FAIL(map_.for_each(scanner))) {
      LOG_ERROR("scan trans ctx map fail", KR(ret));
    } else {
      _LOG_INFO("[STAT] [PENDING_TRANS_COUNT] TOTAL=%ld PREPARED=%ld PART_READY=%ld ",
          scanner.valid_trans_count_,
          scanner.trans_count_[TransCtx::TRANS_CTX_STATE_PREPARED],
          scanner.trans_count_[TransCtx::TRANS_CTX_STATE_PARTICIPANT_READY]);

      DATA_PRINT(scanner.buffer_, scanner.buffer_size_, scanner.pos_, "[STAT] [TRANS_COUNT] "
          "TOTAL=%ld PREPARED=%ld PART_READY=%ld\n",
          scanner.valid_trans_count_,
          scanner.trans_count_[TransCtx::TRANS_CTX_STATE_PREPARED],
          scanner.trans_count_[TransCtx::TRANS_CTX_STATE_PARTICIPANT_READY]);

      pos = scanner.pos_;
    }
  } else {
    LOG_ERROR("invalid arguments", K(buffer), K(size), K(pos));
    ret = OB_INVALID_ARGUMENT;
  }

  return ret;
}

} // namespace libobcdc
} // namespace oceanbase
