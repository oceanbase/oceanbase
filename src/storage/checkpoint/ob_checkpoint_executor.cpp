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

#define USING_LOG_PREFIX STORAGE

#include "storage/ls/ob_ls.h"
#include "storage/checkpoint/ob_checkpoint_executor.h"
#include "storage/tx_storage/ob_checkpoint_service.h"
#include "storage/checkpoint/ob_data_checkpoint.h"
#include "share/ob_force_print_log.h"
#include "logservice/ob_log_base_type.h"

namespace oceanbase
{
using namespace palf;
using namespace logservice;
namespace storage
{
namespace checkpoint
{

ObCheckpointExecutor::ObCheckpointExecutor()
  : update_checkpoint_enabled_(false)
{
  reset();
}

ObCheckpointExecutor::~ObCheckpointExecutor()
{
  reset();
}

void ObCheckpointExecutor::reset()
{
  for (int i = 0; i < ObLogBaseType::MAX_LOG_BASE_TYPE; i++) {
    handlers_[i] = NULL;
  }
  ls_ = NULL;
  loghandler_ = NULL;
}

int ObCheckpointExecutor::register_handler(const ObLogBaseType &type,
                                           ObICheckpointSubHandler *handler)
{
  int ret = OB_SUCCESS;

  if (!is_valid_log_base_type(type) || NULL == handler) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(type), K(handler));
  } else {
    if (OB_NOT_NULL(handlers_[type])) {
      STORAGE_LOG(WARN, "repeat register into checkpoint_executor", K(ret), K(type), K(handler));
    } else {
      handlers_[type] = handler;
    }
  }

  return ret;
}

void ObCheckpointExecutor::unregister_handler(const ObLogBaseType &type)
{
  int ret = OB_SUCCESS;

  if (!is_valid_log_base_type(type)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(type));
  } else {
    handlers_[type] = NULL;
  }
}

int ObCheckpointExecutor::init(ObLS *ls, ObILogHandler *loghandler)
{
  int ret = OB_SUCCESS;
  ls_ = ls;
  loghandler_ = loghandler;
  return ret;
}

void ObCheckpointExecutor::start()
{
  online();
}

void ObCheckpointExecutor::online()
{
  ObSpinLockGuard guard(lock_);
  update_checkpoint_enabled_ = true;
}

void ObCheckpointExecutor::offline()
{
  ObSpinLockGuard guard(lock_);
  update_checkpoint_enabled_ = false;
}

void ObCheckpointExecutor::get_min_rec_log_ts(int &log_type,
                                              int64_t &min_rec_log_ts) const
{
  for (int i = 1; i < ObLogBaseType::MAX_LOG_BASE_TYPE; i++) {
    if (OB_NOT_NULL(handlers_[i])) {
      int64_t rec_log_ts = handlers_[i]->get_rec_log_ts();
      if (rec_log_ts > 0 && rec_log_ts < min_rec_log_ts) {
        min_rec_log_ts = rec_log_ts;
        log_type = i;
      }
    }
  }
}

inline void get_min_rec_log_ts_service_type_by_index_(int index, char* service_type)
{
  int ret = OB_SUCCESS;
  if (index == 0) {
    strncpy(service_type ,"MAX_DECIDED_LOG_TS", common::MAX_SERVICE_TYPE_BUF_LENGTH);
  } else if (OB_FAIL(log_base_type_to_string(ObLogBaseType(index),
                     service_type,
                     common::MAX_SERVICE_TYPE_BUF_LENGTH))) {
    STORAGE_LOG(WARN, "log_base_type_to_string failed", K(ret), K(index));
    strncpy(service_type ,"UNKNOWN_SERVICE_TYPE", common::MAX_SERVICE_TYPE_BUF_LENGTH);
  }
}

int ObCheckpointExecutor::update_clog_checkpoint()
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (update_checkpoint_enabled_) {
    ObFreezer *freezer = ls_->get_freezer();
    if (OB_NOT_NULL(freezer)) {
      int64_t checkpoint_ts = INT64_MAX;
      if (OB_FAIL(freezer->get_max_consequent_callbacked_log_ts(checkpoint_ts))) {
        STORAGE_LOG(WARN, "get_max_consequent_callbacked_log_ts failed", K(ret), K(freezer->get_ls_id()));
      } else {
        // used to record which handler provide the smallest rec_log_ts
        int min_rec_log_ts_service_type_index = 0;
        char service_type[common::MAX_SERVICE_TYPE_BUF_LENGTH];
        get_min_rec_log_ts(min_rec_log_ts_service_type_index, checkpoint_ts);
        get_min_rec_log_ts_service_type_by_index_(min_rec_log_ts_service_type_index, service_type);

        const int64_t checkpoint_ts_in_ls_meta = ls_->get_clog_checkpoint_ts();
        const share::ObLSID ls_id = ls_->get_ls_id();
        LSN clog_checkpoint_lsn;
        if (checkpoint_ts == checkpoint_ts_in_ls_meta) {
          STORAGE_LOG(INFO, "[CHECKPOINT] clog checkpoint no change", K(checkpoint_ts),
                      K(checkpoint_ts_in_ls_meta), K(ls_id), K(service_type));
        } else if (checkpoint_ts < checkpoint_ts_in_ls_meta) {
          if (min_rec_log_ts_service_type_index == 0) {
            STORAGE_LOG(INFO, "[CHECKPOINT] expexted when no log callbacked or replayed",
                        K(checkpoint_ts), K(checkpoint_ts_in_ls_meta), K(ls_id));
          } else {
            STORAGE_LOG(ERROR, "[CHECKPOINT] can not advance clog checkpoint", K(checkpoint_ts),
                        K(checkpoint_ts_in_ls_meta), K(ls_id), K(service_type));
          }
        } else if (OB_FAIL(loghandler_->locate_by_ts_ns_coarsely(checkpoint_ts, clog_checkpoint_lsn))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            STORAGE_LOG(WARN, "no file in disk", K(ret), K(ls_id), K(checkpoint_ts));
            ret = OB_SUCCESS;
          } else if (OB_NOT_INIT == ret) {
            STORAGE_LOG(WARN, "palf has been disabled", K(ret), K(checkpoint_ts), K(ls_->get_ls_id()));
            ret = OB_SUCCESS;
          } else {
            STORAGE_LOG(ERROR, "locate lsn by logts failed", K(ret), K(ls_id),
                        K(checkpoint_ts), K(checkpoint_ts_in_ls_meta));
          }
        } else if (OB_FAIL(ls_->set_clog_checkpoint(clog_checkpoint_lsn, checkpoint_ts))) {
          STORAGE_LOG(WARN, "set clog checkpoint failed", K(ret), K(clog_checkpoint_lsn), K(checkpoint_ts), K(ls_id));
        } else if (OB_FAIL(loghandler_->advance_base_lsn(clog_checkpoint_lsn))) {
          if (OB_NOT_INIT == ret) {
            STORAGE_LOG(WARN, "palf has been disabled", K(ret), K(clog_checkpoint_lsn), K(ls_->get_ls_id()));
            ret = OB_SUCCESS;
          } else {
            STORAGE_LOG(ERROR, "set base lsn failed", K(ret), K(clog_checkpoint_lsn), K(ls_id));
          }
        } else {
          FLOG_INFO("[CHECKPOINT] update clog checkpoint successfully",
                    K(clog_checkpoint_lsn), K(checkpoint_ts), K(ls_id),
                    K(service_type));
        }
      }
    } else {
      STORAGE_LOG(WARN, "freezer should not null", K(ls_->get_ls_id()));
    }
  } else {
    STORAGE_LOG(WARN, "update_checkpoint is not enabled", K(ls_->get_ls_id()));
  }

  return ret;
}

int ObCheckpointExecutor::advance_checkpoint_by_flush(int64_t recycle_ts) {
  int ret = OB_SUCCESS;

  ObSpinLockGuard guard(lock_);
  if (update_checkpoint_enabled_) {
    int tmp_ret = OB_SUCCESS;

    // calcu recycle_ts according to clog disk situation
    if (recycle_ts == 0) {
      LSN end_lsn;
      int64_t calcu_recycle_ts = INT64_MAX;
      if (OB_FAIL(loghandler_->get_end_lsn(end_lsn))) {
        STORAGE_LOG(WARN, "get end lsn failed", K(ret), K(ls_->get_ls_id()));
      } else {
        LSN clog_checkpoint_lsn = ls_->get_clog_base_lsn();
        LSN calcu_recycle_lsn = clog_checkpoint_lsn
              + ((end_lsn - clog_checkpoint_lsn) * CLOG_GC_PERCENT / 100);
        if (OB_FAIL(loghandler_->locate_by_lsn_coarsely(calcu_recycle_lsn, recycle_ts))) {
          STORAGE_LOG(WARN, "locate_by_lsn_coarsely failed", K(calcu_recycle_ts), K(calcu_recycle_lsn),
                        K(recycle_ts), K(ls_->get_ls_id()));
        } else {
          STORAGE_LOG(INFO, "advance checkpoint by flush to avoid clog disk full",
                      K(recycle_ts), K(end_lsn), K(clog_checkpoint_lsn),
                      K(calcu_recycle_lsn), K(ls_->get_ls_id()));
        }
      }
      // the log of end_log_lsn and the log of clog_checkpoint_lsn may be in a block
      if (recycle_ts < ls_->get_clog_checkpoint_ts()) {
        recycle_ts = INT64_MAX;
      }
    }

    if (OB_SUCC(ret)) {
      if (recycle_ts < ls_->get_clog_checkpoint_ts()) {
        ret = OB_NO_NEED_UPDATE;
        STORAGE_LOG(WARN, "recycle_ts should not smaller than checkpoint_log_ts",
                    K(recycle_ts), K(ls_->get_clog_checkpoint_ts()), K(ls_->get_ls_id()));
      } else {
        STORAGE_LOG(INFO, "start flush",
                    K(recycle_ts),
                    K(ls_->get_clog_checkpoint_ts()),
                    K(ls_->get_ls_id()));
        for (int i = 1; i < ObLogBaseType::MAX_LOG_BASE_TYPE; i++) {
          if (OB_NOT_NULL(handlers_[i])
              && OB_SUCCESS != (tmp_ret = (handlers_[i]->flush(recycle_ts)))) {
            STORAGE_LOG(WARN, "handler flush failed", K(recycle_ts), K(tmp_ret),
                        K(i), K(ls_->get_ls_id()));
          }
        }
      }
    }
  }

  return ret;
}

int ObCheckpointExecutor::get_checkpoint_info(ObIArray<ObCheckpointVTInfo> &checkpoint_array)
{
  int ret = OB_SUCCESS;
  checkpoint_array.reset();
  for (int i = 1; i < ObLogBaseType::MAX_LOG_BASE_TYPE; i++) {
    if (OB_NOT_NULL(handlers_[i])) {
      ObCheckpointVTInfo info;
      info.rec_log_ts = handlers_[i]->get_rec_log_ts();
      info.service_type = i;
      checkpoint_array.push_back(info);
    }
  }
  return ret;
}

int64_t ObCheckpointExecutor::get_cannot_recycle_log_size()
{
  int ret = OB_SUCCESS;
  int64_t cannot_recycle_log_size = 0;
  LSN end_lsn;
  if (OB_FAIL(loghandler_->get_end_lsn(end_lsn))) {
    STORAGE_LOG(WARN, "get end lsn failed", K(ret), K(ls_->get_ls_id()));
  } else {
    cannot_recycle_log_size =
      end_lsn.val_ - ls_->get_clog_base_lsn().val_;
  }
  return cannot_recycle_log_size;
}

int ObCheckpointExecutor::diagnose(CheckpointDiagnoseInfo &diagnose_info) const
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  int log_type_index = 0;
  diagnose_info.checkpoint_ = ls_->get_clog_checkpoint_ts();
  diagnose_info.min_rec_scn_ = INT64_MAX;
  get_min_rec_log_ts(log_type_index, diagnose_info.min_rec_scn_);
  ObLogBaseType log_type = static_cast<ObLogBaseType>(log_type_index);
  diagnose_info.log_type_ = log_type;
  return ret;
}
}  // namespace checkpoint
}  // namespace storage
}  // namespace oceanbase
