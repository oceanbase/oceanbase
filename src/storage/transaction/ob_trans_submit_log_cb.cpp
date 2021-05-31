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

#include "ob_trans_submit_log_cb.h"
#include "lib/stat/ob_session_stat.h"
#include "common/ob_partition_key.h"
#include "share/ob_cluster_version.h"
#include "ob_trans_service.h"
#include "ob_trans_part_ctx.h"
#include "ob_trans_coord_ctx.h"

namespace oceanbase {
using namespace common;
using namespace storage;

namespace transaction {
int ObTransSubmitLogCb::init(
    ObTransService* ts, const ObPartitionKey& partition, const ObTransID& trans_id, ObTransCtx* ctx)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "ObTransSubmitLogCb inited twice");
    ret = OB_INIT_TWICE;
  } else if (NULL == ts || !partition.is_valid() || !trans_id.is_valid() || OB_ISNULL(ctx)) {
    TRANS_LOG(WARN, "invalid argument", KP(ts), K(partition), K(trans_id), KP(ctx));
    ret = OB_INVALID_ARGUMENT;
  } else {
    is_inited_ = true;
    ts_ = ts;
    partition_ = partition;
    trans_id_ = trans_id;
    log_type_ = storage::OB_LOG_UNKNOWN;
    submit_timestamp_ = ObClockGenerator::getRealClock();
    ctx_ = ctx;
  }

  return ret;
}

void ObTransSubmitLogCb::reset()
{
  ObITransSubmitLogCb::reset();
  is_inited_ = false;
  ts_ = NULL;
  partition_.reset();
  trans_id_.reset();
  log_type_ = storage::OB_LOG_UNKNOWN;
  submit_timestamp_ = 0;
  ctx_ = NULL;
  have_prev_trans_ = false;
  is_callbacking_ = false;
}

int ObTransSubmitLogCb::set_log_type(const int64_t log_type)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    TRANS_LOG(WARN, "ObTransSubmitLogCb not inited");
    ret = OB_NOT_INIT;
  } else if (!ObTransLogType::is_valid(log_type)) {
    TRANS_LOG(WARN, "invalid argument", K_(partition), K_(trans_id), K(log_type));
    ret = OB_INVALID_ARGUMENT;
  } else {
    log_type_ = log_type;
    // PREPARE and SP_TRANS_COMMIT log affect transaction's response time
    // clog has a high priority callback for these log
    if ((OB_LOG_TRANS_PREPARE & log_type) != 0 || OB_LOG_SP_TRANS_COMMIT == log_type) {
      is_high_priority_ = true;
    } else {
      is_high_priority_ = false;
    }
  }

  return ret;
}

int ObTransSubmitLogCb::set_real_submit_timestamp(const int64_t timestamp)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    TRANS_LOG(WARN, "ObTransSubmitLogCb not inited");
    ret = OB_NOT_INIT;
  } else if (timestamp <= 0) {
    TRANS_LOG(WARN, "invalid argument", K_(partition), K_(trans_id), K(timestamp));
    ret = OB_INVALID_ARGUMENT;
  } else {
    submit_timestamp_ = timestamp;
  }

  return ret;
}

int ObTransSubmitLogCb::on_success(const ObPartitionKey& partition_key, const clog::ObLogType clog_log_type,
    const uint64_t log_id, const int64_t timestamp, const bool batch_committed, const bool batch_first_participant)
{
  UNUSED(clog_log_type);
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObPartitionKey partition(partition_);
  ObTransID trans_id(trans_id_);
  const int64_t log_type = log_type_;
  bool alloc = false;
  const bool for_replay = false;
  const bool is_readonly = false;

  if (!is_inited_) {
    TRANS_LOG(WARN, "ObTransSubmitLogCb not inited");
    ret = OB_NOT_INIT;
  } else if (!partition_key.is_valid() || timestamp < 0) {
    TRANS_LOG(WARN, "invalid argument", K(partition_key), K(timestamp), K(log_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (!(partition_key == partition)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "partition not match", KR(ret), K(partition_key), K(partition), K(log_id));
  } else if (NULL == ctx_) {
    TRANS_LOG(ERROR, "ctx is null", K(partition), K(trans_id), K_(log_type), KP(ctx_));
    ret = OB_ERR_UNEXPECTED;
  } else if (ctx_->get_trans_id() != trans_id) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "transaction not match", KR(ret), K(partition_key), K(trans_id), K(log_id));
  } else {
    const int64_t used_time = ObClockGenerator::getRealClock() - submit_timestamp_;
    if (used_time >= ObServerConfig::get_instance().clog_sync_time_warn_threshold) {
      if (OB_LOG_TRANS_CLEAR == log_type) {
        if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
          TRANS_LOG(WARN, "transaction log sync use too much time", K(partition), K(trans_id), K(log_id), K(used_time));
        }
      } else {
        TRANS_LOG(WARN, "transaction log sync use too much time", K(partition), K(trans_id), K(log_id), K(used_time));
      }
    }
    ObTransStatistic::get_instance().add_clog_sync_time(partition_key.get_tenant_id(), used_time);
    ObTransStatistic::get_instance().add_clog_sync_count(partition_key.get_tenant_id(), 1);
    int64_t start = ObTimeUtility::fast_current_time();
    int64_t get_ctx = 0;
    int64_t callback = 0;
    int64_t revert_ctx = 0;
    ObPartTransCtx* part_ctx = static_cast<ObPartTransCtx*>(ctx_);

    if (OB_FAIL(part_ctx->get_partition_mgr()->acquire_ctx_ref(trans_id))) {
      TRANS_LOG(WARN, "get transaction context from hashmap error", K(ret), K(trans_id));
    } else {
      is_callbacking_ = true;
      get_ctx = ObTimeUtility::fast_current_time();
      // batch_commit don't support transaction dependency
      const bool batch_commit = (have_prev_trans_ ? false : batch_committed);
      bool can_call_coordinator = true;
#ifdef TRANS_ERROR
      const int random = (int)ObRandom::rand(1, 100);
      // mock clog write log fail: last participant don't callback coordinator randomly
      if (0 == random % 10) {
        can_call_coordinator = false;
      }
#endif
      if (OB_SUCC(ret) && batch_commit && batch_first_participant && can_call_coordinator) {
        const ObPartitionKey& coordinator = part_ctx->get_coordinator();
        ObTransCtx* tmp_ctx = NULL;
        // all participants has callbacked, it's time to callback coordinator
        if (OB_SUCCESS != (tmp_ret = ts_->get_coord_trans_ctx_mgr().get_trans_ctx(
                               coordinator, trans_id, for_replay, is_readonly, alloc, tmp_ctx))) {
          TRANS_LOG(
              WARN, "get transaction context error", K(tmp_ret), K(coordinator), K(partition), K(trans_id), K(log_id));
        } else {
          ObCoordTransCtx* coord_ctx = static_cast<ObCoordTransCtx*>(tmp_ctx);
          if (OB_SUCCESS != (tmp_ret = coord_ctx->handle_batch_commit_succ())) {
            TRANS_LOG(
                WARN, "handle batch commit success", K(tmp_ret), K(coordinator), K(partition), K(trans_id), K(log_id));
          }
          if (OB_SUCCESS != (tmp_ret = ts_->get_coord_trans_ctx_mgr().revert_trans_ctx(tmp_ctx))) {
            TRANS_LOG(
                WARN, "revert transaction context error", "ret", tmp_ret, K(coordinator), K(partition), K(trans_id));
            ret = tmp_ret;
          }
        }
      }
      // need acquire ref before callback
      if (part_ctx->need_to_post_redo_sync_task(log_type)) {
        // duplicated table need all lease avaiable replica sync and replay log
        if (OB_FAIL(part_ctx->retry_redo_sync_task(log_id, log_type, timestamp, true))) {
          TRANS_LOG(ERROR, "retry redo sync task error", K(ret), K(*part_ctx));
        }
      } else if (OB_FAIL(part_ctx->on_sync_log_success(log_type, log_id, timestamp, batch_commit))) {
        TRANS_LOG(WARN, "sync log success callback error", K(ret), K(partition), K(trans_id), K(log_id));
      } else if (part_ctx->is_enable_new_1pc()) {
        if (OB_SUCCESS != (tmp_ret = part_ctx->handle_2pc_local_msg_response(partition, trans_id, log_type))) {
          TRANS_LOG(WARN, "handle 2pc local msg response error", K(tmp_ret), K(partition), K(trans_id), K(log_type));
        }
      } else {
        // do nothing
      }
      callback = ObTimeUtility::fast_current_time();
      is_callbacking_ = false;
      if (OB_SUCCESS != (tmp_ret = part_ctx->get_partition_mgr()->release_ctx_ref(part_ctx))) {
        TRANS_LOG(WARN, "revert transaction context error", "ret", tmp_ret, K(partition), K(trans_id));
        ret = tmp_ret;
      }
      revert_ctx = ObTimeUtility::fast_current_time();
    }
    if (ObTimeUtility::fast_current_time() - start > 30 * 1000) {
      TRANS_LOG(WARN,
          "transaction log callback use too much time",
          "get_ctx_used",
          get_ctx - start,
          "callback_used",
          callback - get_ctx,
          "revert_ctx_used",
          ((revert_ctx > callback) ? (revert_ctx - callback) : -1),
          K(partition_key),
          K(log_id),
          K(log_type),
          K(timestamp));
    }
  }

  return ret;
}

int ObTransSubmitLogCb::on_submit_log_success(
    const bool with_need_update_verison, const uint64_t log_id, const int64_t log_timestamp)
{
  int ret = OB_SUCCESS;
  ObPartitionKey partition(partition_);
  ObTransID trans_id(trans_id_);

  if (!is_inited_) {
    TRANS_LOG(WARN, "ObTransSubmitLogCb not inited");
    ret = OB_NOT_INIT;
  } else if (!partition.is_valid() || !trans_id.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(partition), K(trans_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == ctx_) {
    TRANS_LOG(ERROR, "ctx is null, unexpected error", KP(ctx_));
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObPartTransCtx* part_ctx = static_cast<ObPartTransCtx*>(ctx_);
    if (OB_FAIL(part_ctx->on_submit_log_success(with_need_update_verison, log_id, log_timestamp))) {
      TRANS_LOG(WARN, "submit log success callback error", KR(ret), K(partition), K(trans_id));
    }
  }

  return ret;
}

int ObTransSubmitLogCb::on_submit_log_fail(const int retcode)
{
  int ret = OB_SUCCESS;
  ObPartitionKey partition(partition_);
  ObTransID trans_id(trans_id_);

  if (!is_inited_) {
    TRANS_LOG(WARN, "ObTransSubmitLogCb not inited");
    ret = OB_NOT_INIT;
  } else if (!partition.is_valid() || !trans_id.is_valid()) {
    TRANS_LOG(WARN, "invalid argument", K(partition), K(trans_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == ctx_) {
    TRANS_LOG(ERROR, "ctx is null, unexpected error", KP(ctx_));
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObPartTransCtx* part_ctx = static_cast<ObPartTransCtx*>(ctx_);
    if (OB_FAIL(part_ctx->on_submit_log_fail(retcode))) {
      TRANS_LOG(WARN, "submit log success callback error", KR(ret), K_(partition), K_(trans_id));
    }
  }

  return ret;
}

}  // namespace transaction
}  // namespace oceanbase
