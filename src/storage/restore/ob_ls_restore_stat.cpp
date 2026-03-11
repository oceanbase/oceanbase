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
#include "ob_ls_restore_stat.h"

using namespace oceanbase;

//================================ObLSRestoreStat=======================================
int ObLSRestoreStat::init(const share::ObLSRestoreJobPersistKey &ls_key)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLSRestoreStat init twice", K(ret));
  } else {
    is_inited_ = true;
    ls_key_ = ls_key;
  }
  return ret;
}

int ObLSRestoreStat::set_total_tablet_cnt(const int64_t cnt)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mtx_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLSRestoreStat not init", K(ret));
  } else {
    total_tablet_cnt_ = cnt;
  }

  return ret;
}

int ObLSRestoreStat::inc_total_tablet_cnt()
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mtx_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLSRestoreStat not init", K(ret));
  } else {
    total_tablet_cnt_ += 1;
  }

  return ret;
}

int ObLSRestoreStat::dec_total_tablet_cnt()
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mtx_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLSRestoreStat not init", K(ret));
  } else {
    total_tablet_cnt_ -= 1;
  }

  return ret;
}

int ObLSRestoreStat::increase_total_bytes_by(const int64_t bytes)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mtx_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLSRestoreStat not init", K(ret));
  } else {
    total_bytes_ += bytes;
  }

  return ret;
}

int ObLSRestoreStat::decrease_total_bytes_by(const int64_t bytes)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mtx_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLSRestoreStat not init", K(ret));
  } else {
    total_bytes_ -= bytes;
  }

  return ret;
}

int ObLSRestoreStat::add_finished_tablet_cnt(const int64_t inc_finished_tablet_cnt)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mtx_);
  int64_t old_finished_tablet_cnt = get_finished_tablet_cnt_();
  int64_t new_finished_tablet_cnt = old_finished_tablet_cnt + inc_finished_tablet_cnt;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLSRestoreStat not init", K(ret));
  } else if (ObTimeUtility::current_time() - last_report_ts_ >= REPORT_INTERVAL) {
    if (OB_FAIL(do_report_finished_tablet_cnt_(new_finished_tablet_cnt))) {
      LOG_WARN("fail to report finished tablet cnt", K(ret), K_(ls_key));
    }
  }

  if (OB_SUCC(ret)) {
    unfinished_tablet_cnt_ = total_tablet_cnt_ - new_finished_tablet_cnt;
  }

  return ret;
}

int ObLSRestoreStat::report_unfinished_tablet_cnt(const int64_t unfinished_tablet_cnt)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mtx_);
  int64_t finished_tablet_cnt = total_tablet_cnt_ - unfinished_tablet_cnt;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLSRestoreStat not init", K(ret));
  } else if (total_tablet_cnt_ > 0 && finished_tablet_cnt > 0) {
    if (OB_FAIL(do_report_finished_tablet_cnt_(finished_tablet_cnt))) {
      LOG_WARN("fail to report finished tablet cnt", K(ret), K_(total_tablet_cnt), K(unfinished_tablet_cnt));
    }
  }

  if (OB_SUCC(ret)) {
    unfinished_tablet_cnt_ = unfinished_tablet_cnt;
  }

  return ret;
}

int ObLSRestoreStat::add_finished_bytes(const int64_t bytes)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mtx_);
  int64_t old_finished_bytes = get_finished_bytes();
  int64_t new_finished_bytes = old_finished_bytes + bytes;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLSRestoreStat not init", K(ret));
  } else if (ObTimeUtility::current_time() - last_report_ts_ >= REPORT_INTERVAL) {
    if (OB_FAIL(do_report_finished_bytes_(new_finished_bytes))) {
      LOG_WARN("fail to report finished tablet cnt", K(ret), K_(ls_key));
    }
  }

  if (OB_SUCC(ret)) {
    unfinished_tablet_cnt_ = total_tablet_cnt_ - new_finished_bytes;
  }

  return ret;
}

int ObLSRestoreStat::report_unfinished_bytes(const int64_t bytes)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mtx_);
  int64_t finished_bytes_ = total_bytes_ - bytes;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLSRestoreStat not init", K(ret));
  } else if (total_bytes_ > 0 && finished_bytes_ > 0) {
    if (OB_FAIL(do_report_finished_bytes_(finished_bytes_))) {
      LOG_WARN("fail to report finished tablet cnt", K(ret), K_(total_bytes), K_(unfinished_bytes));
    }
  }

  if (OB_SUCC(ret)) {
    unfinished_bytes_ = unfinished_bytes_;
  }

  return ret;
}

int ObLSRestoreStat::load_restore_stat()
{
  int ret = OB_SUCCESS;
  share::ObRestorePersistHelper helper;
  common::ObMySQLProxy *sql_proxy = nullptr;
  lib::ObMutexGuard guard(mtx_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLSRestoreStat not init", K(ret));
  } else if (OB_ISNULL(sql_proxy = GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql prxoy must not be null", K(ret));
  } else if (OB_FAIL(helper.init(ls_key_.tenant_id_, share::OBCG_STORAGE))) {
    LOG_WARN("fail to init restore table helper", K(ret), "tenant_id", ls_key_.tenant_id_);
  } else if (OB_FAIL(helper.get_ls_total_tablet_cnt(*sql_proxy, ls_key_, total_tablet_cnt_))) {
    LOG_WARN("fail to get ls total tablet cnt", K(ret), K_(ls_key));
  } else if (OB_FAIL(helper.get_ls_total_bytes(*sql_proxy, ls_key_, total_bytes_))) {
    LOG_WARN("fail to get ls total bytes", K(ret), K_(ls_key));
  }

  return ret;
}

void ObLSRestoreStat::reset()
{
  lib::ObMutexGuard guard(mtx_);
  ls_key_.reset();
  total_tablet_cnt_ = 0;
  unfinished_tablet_cnt_ = 0;
  total_bytes_ = 0;
  unfinished_bytes_ = 0;
  last_report_ts_ = 0;
  is_inited_ = false;
}

int ObLSRestoreStat::get_finished_tablet_cnt(int64_t &finished_tablet_cnt) const
{
  int ret = OB_SUCCESS;
  finished_tablet_cnt = 0;
  lib::ObMutexGuard guard(mtx_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLSRestoreStat not init", K(ret));
  } else {
    finished_tablet_cnt = total_tablet_cnt_ - unfinished_tablet_cnt_;
  }

  return ret;
}

int ObLSRestoreStat::do_report_finished_tablet_cnt_(const int64_t finished_tablet_cnt)
{
  // TODO:(wangxiaohui) 4.3, calculate total report time.
  int ret = OB_SUCCESS;
  share::ObRestorePersistHelper helper;
  common::ObMySQLProxy *sql_proxy = nullptr;
  if (finished_tablet_cnt < 0) {
  } else if (OB_ISNULL(sql_proxy = GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql prxoy must not be null", K(ret));
  } else if (OB_FAIL(helper.init(ls_key_.tenant_id_, share::OBCG_STORAGE))) {
    LOG_WARN("fail to init restore table helper", K(ret), "tenant_id", ls_key_.tenant_id_);
  } else if (OB_FAIL(helper.set_ls_finish_tablet_cnt(*sql_proxy, ls_key_, finished_tablet_cnt))) {
    LOG_WARN("fail to set ls finished tablet cnt", K(ret), K_(ls_key));
  } else {
    last_report_ts_ = ObTimeUtility::current_time();
  }

  return ret;
}

int64_t ObLSRestoreStat::get_finished_tablet_cnt_() const
{
  return total_tablet_cnt_ - unfinished_tablet_cnt_;
}

int ObLSRestoreStat::do_report_finished_bytes_(const int64_t finished_bytes)
{
  int ret = OB_SUCCESS;
  share::ObRestorePersistHelper helper;
  common::ObMySQLProxy *sql_proxy = nullptr;
  if (finished_bytes < 0) {
  } else if (OB_ISNULL(sql_proxy = GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql prxoy must not be null", K(ret));
  } else if (OB_FAIL(helper.init(ls_key_.tenant_id_, share::OBCG_STORAGE))) {
    LOG_WARN("fail to init restore table helper", K(ret), "tenant_id", ls_key_.tenant_id_);
  } else if (OB_FAIL(helper.set_ls_finish_bytes(*sql_proxy, ls_key_, finished_bytes))) {
    LOG_WARN("fail to set ls finished tablet cnt", K(ret), K_(ls_key));
  } else {
    last_report_ts_ = ObTimeUtility::current_time();
  }

  return ret;
}

int64_t ObLSRestoreStat::get_finished_bytes() const
{
  return total_bytes_ - unfinished_bytes_;
}


int ObLSRestoreStat::set_total_bytes(const int64_t bytes)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mtx_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLSRestoreStat not init", K(ret));
  } else {
    total_bytes_ = bytes;
  }

  return ret;
}

//================================ObLSRestoreResultMgr=======================================
ObLSRestoreResultMgr::ObLSRestoreResultMgr()
  : mtx_(common::ObLatchIds::OB_LS_RESTORE_RESULT_MGR_LOCK),
    result_(OB_SUCCESS),
    retry_cnt_(0),
    last_err_ts_(0),
    trace_id_(),
    failed_type_(RestoreFailedType::MAX_FAILED_TYPE)
{
}

void ObLSRestoreResultMgr::reset()
{
  lib::ObMutexGuard guard(mtx_);
  result_ = OB_SUCCESS;
  retry_cnt_ = 0;
  last_err_ts_ = 0;
  trace_id_.reset();
  failed_type_ = RestoreFailedType::MAX_FAILED_TYPE;
}

bool ObLSRestoreResultMgr::can_retry() const
{
  int64_t max_retry_cnt = OB_MAX_RESTORE_RETRY_TIMES;
#ifdef ERRSIM
  if (0 != GCONF.errsim_max_restore_retry_count) {
    max_retry_cnt = GCONF.errsim_max_restore_retry_count;
  }
#endif
  return retry_cnt_ < max_retry_cnt &&  can_retrieable_err(result_);
}

bool ObLSRestoreResultMgr::is_met_retry_time_interval()
{
  lib::ObMutexGuard guard(mtx_);
  bool bret = false;
  int64_t cur_ts = ObTimeUtility::current_time();
  if (last_err_ts_ + OB_MAX_LS_RESTORE_RETRY_TIME_INTERVAL <= cur_ts) {
    bret = true;
  }
  return bret;
}

void ObLSRestoreResultMgr::set_result(const int result, const share::ObTaskId &trace_id,
     const RestoreFailedType &failed_type)
{
  // update result_ conditions:
  // 1. result_ is OB_SUCCESS;
  // 2. result_ is retrieable err, which can be updated by newer retryable err or non-retryable err
  lib::ObMutexGuard guard(mtx_);
  if (OB_EAGAIN == result
     || OB_IO_LIMIT == result) {
  } else {
    if (retry_cnt_ >= OB_MAX_RESTORE_RETRY_TIMES) { // avoiding overwrite error code
    } else if (can_retrieable_err(result_) || OB_SUCCESS == result_) {
      result_ = result;
      trace_id_.set(trace_id);
      failed_type_ = failed_type;
    }
    retry_cnt_++;
    LOG_INFO("[RESTORE] set result", KPC(this), K(lbt()));
  }
  last_err_ts_ = ObTimeUtility::current_time();
}

int ObLSRestoreResultMgr::get_comment_str(const ObLSID &ls_id, const ObAddr &addr, ObHAResultInfo::Comment &comment) const
{
  ObHAResultInfo::FailedType type = RestoreFailedType::DATA_RESTORE_FAILED_TYPE == failed_type_ ?
                                    ObHAResultInfo::RESTORE_DATA : ObHAResultInfo::RESTORE_CLOG;
  ObHAResultInfo result_info(type, ls_id, addr, trace_id_, result_);
  return result_info.get_comment_str(comment);
}

bool ObLSRestoreResultMgr::can_retrieable_err(const int err) const
{
  bool bret = true;
  switch (err) {
    case OB_NOT_INIT :
    case OB_INVALID_ARGUMENT :
    case OB_ERR_UNEXPECTED :
    case OB_ERR_SYS :
    case OB_INIT_TWICE :
    case OB_CANCELED :
    case OB_NOT_SUPPORTED :
    case OB_TENANT_HAS_BEEN_DROPPED :
    case OB_SERVER_OUTOF_DISK_SPACE :
    case OB_OBJECT_NOT_EXIST :
    case OB_ARCHIVE_ROUND_NOT_CONTINUOUS :
    case OB_HASH_NOT_EXIST:
    case OB_TOO_MANY_PARTITIONS_ERROR:
    case OB_CANNOT_ACCESS_BACKUP_SET:
    case OB_OBJECT_STORAGE_PERMISSION_DENIED:
      bret = false;
      break;
    default:
      break;
  }
  return bret;
}
