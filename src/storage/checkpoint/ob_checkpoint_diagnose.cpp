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

#include "ob_checkpoint_diagnose.h"

namespace oceanbase
{
namespace storage
{
namespace checkpoint
{

void ObTraceInfo::init(const int64_t trace_id,
    const share::ObLSID &ls_id,
    const int64_t checkpoint_start_time)
{
  SpinWLockGuard lock(lock_);
  reset_without_lock_();
  trace_id_ = trace_id;
  ls_id_ = ls_id;
  strncpy(thread_name_, ob_get_tname(), oceanbase::OB_THREAD_NAME_BUF_LEN);
  checkpoint_start_time_ = checkpoint_start_time;
}

void ObTraceInfo::reset_without_lock_()
{
  TRANS_LOG(INFO, "trace info reset", KPC(this));
  trace_id_ = INVALID_TRACE_ID;
  freeze_clock_ = 0;
  ls_id_.reset();
  checkpoint_unit_diagnose_info_map_.reuse();
  memtable_diagnose_info_map_.reuse();
  checkpoint_start_time_ = 0;
  memset(thread_name_, 0, oceanbase::OB_THREAD_NAME_BUF_LEN);
  allocator_.clear();
}

bool ObTraceInfo::check_trace_id_(const int64_t trace_id)
{
  bool ret = true;
  if (trace_id != trace_id_) {
    LOG_WARN("trace_id not match", K(trace_id), KPC(this), K(lbt()));
    ret = false;
  }
  return ret;
}

bool ObTraceInfo::check_trace_id_(const ObCheckpointDiagnoseParam &param)
{
  bool ret = true;
  if (param.is_freeze_clock_) {
    if (param.freeze_clock_ != freeze_clock_ - 1
        || param.ls_id_ != ls_id_.id()) {
      LOG_WARN("freeze_clock not match", K(param), KPC(this), K(lbt()));
      ret = false;
    }
  } else {
    if (param.trace_id_ != trace_id_) {
      LOG_WARN("trace_id not match", K(param), KPC(this), K(lbt()));
      ret = false;
    }
  }
  return ret;
}

void ObTraceInfo::update_freeze_clock(const int64_t trace_id,
    const uint32_t freeze_clock)
{
  SpinWLockGuard lock(lock_);
  if (check_trace_id_(trace_id)) {
    freeze_clock_ = freeze_clock;
  }
}

int ObCheckpointDiagnoseMgr::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init ObCheckpointDiagnoseMgr twice", KR(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < MAX_TRACE_INFO_ARR_SIZE; i++) {
      const int64_t bucket_count = hash::cal_next_prime(100);
      if (OB_FAIL(trace_info_arr_[i].memtable_diagnose_info_map_.create(bucket_count, "CkptDgnMem", "CkptDgnMemNode", MTL_ID()))) {
        LOG_WARN("failed to create map", KR(ret));
      } else if (OB_FAIL(trace_info_arr_[i].checkpoint_unit_diagnose_info_map_.create(bucket_count, "CkptDgnMemCU", "CkptDgnCUNode", MTL_ID()))) {
        LOG_WARN("failed to create map", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObCheckpointDiagnoseMgr::acquire_trace_id(const share::ObLSID &ls_id,
    int64_t &trace_id)
{
  int ret = OB_SUCCESS;
  trace_id = INVALID_TRACE_ID;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCheckpointDiagnoseMgr not inited.", K(ret));
  } else if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls_id is invalid", KR(ret));
  } else {
    SpinWLockGuard lock(pos_lock_);
    if (max_trace_info_size_ > 0) {
      const int64_t start_time = ObTimeUtility::current_time();
      trace_id = ++last_pos_;
      trace_info_arr_[trace_id % MAX_TRACE_INFO_ARR_SIZE].init(trace_id, ls_id, start_time);
      reset_old_trace_infos_without_pos_lock_();
      LOG_INFO("acquire_trace_id", K(trace_id), K(ls_id));
    }
  }
  return ret;
}

int ObCheckpointDiagnoseMgr::update_freeze_clock(const share::ObLSID &ls_id,
    const int64_t trace_id,
    const int logstream_clock)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCheckpointDiagnoseMgr not inited.", K(ret));
  } else if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls_id is invalid", KR(ret));
  } else {
    // if trace_id is invalid, gen trace_id for checkpoint diagnose.
    int64_t tmp_trace_id = trace_id;
    if (checkpoint::INVALID_TRACE_ID == tmp_trace_id) {
      MTL(checkpoint::ObCheckpointDiagnoseMgr*)->acquire_trace_id(ls_id, tmp_trace_id);
    }
    if (checkpoint::INVALID_TRACE_ID != tmp_trace_id) {
      trace_info_arr_[tmp_trace_id % MAX_TRACE_INFO_ARR_SIZE].update_freeze_clock(tmp_trace_id, logstream_clock);
      LOG_INFO("update_freeze_clock", K(trace_info_arr_[tmp_trace_id % MAX_TRACE_INFO_ARR_SIZE]));
    }
  }
  return ret;
}

void UpdateScheduleDagInfo::operator()(ObCheckpointUnitDiagnoseInfo &info) const
{
  const int64_t start_time = ObTimeUtility::current_time();
  info.rec_scn_ = rec_scn_;
  info.start_scn_ = start_scn_;
  info.end_scn_ = end_scn_;
  info.create_flush_dag_time_ = start_time;
  TRANS_LOG(INFO, "update_schedule_dag_info", K(info), K(param_));
}
int ObCheckpointDiagnoseMgr::update_schedule_dag_info(const ObCheckpointDiagnoseParam &param,
    const share::SCN &rec_scn,
    const share::SCN &start_scn,
    const share::SCN &end_scn)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCheckpointDiagnoseMgr not inited.", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is invalid", KR(ret), K(param));
  } else {
    if (OB_FAIL(trace_info_arr_[param.trace_id_ % MAX_TRACE_INFO_ARR_SIZE].update_diagnose_info
          <ObCheckpointUnitDiagnoseInfo>(param, UpdateScheduleDagInfo(param, rec_scn, start_scn, end_scn)))) {
      LOG_WARN("failed to add_checkpoint_unit_diagnose_info", KR(ret), K(param));
    }
  }
  return ret;
}

DEF_UPDATE_TIME_FUNCTOR(UpdateMergeInfoForCheckpointUnit, ObCheckpointUnitDiagnoseInfo, merge_finish_time)
int ObCheckpointDiagnoseMgr::update_merge_info_for_checkpoint_unit(const ObCheckpointDiagnoseParam &param)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCheckpointDiagnoseMgr not inited.", KR(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is invalid", KR(ret), K(param));
  } else {
    if (OB_FAIL(trace_info_arr_[param.trace_id_ % MAX_TRACE_INFO_ARR_SIZE].update_diagnose_info
          <ObCheckpointUnitDiagnoseInfo>(param, UpdateMergeInfoForCheckpointUnit(param)))) {
      LOG_WARN("failed to update_checkpoint_unit_diagnose_info", KR(ret), K(param));

    }
  }
  return ret;

}

DEF_UPDATE_TIME_FUNCTOR(UpdateStartGcTimeForCheckpointUnit, ObCheckpointUnitDiagnoseInfo, start_gc_time)
int ObCheckpointDiagnoseMgr::update_start_gc_time_for_checkpoint_unit(const ObCheckpointDiagnoseParam &param)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCheckpointDiagnoseMgr not inited.", KR(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is invalid", KR(ret), K(param));
  } else {
    if (OB_FAIL(trace_info_arr_[param.trace_id_ % MAX_TRACE_INFO_ARR_SIZE].update_diagnose_info
          <ObCheckpointUnitDiagnoseInfo>(param, UpdateStartGcTimeForCheckpointUnit(param)))) {
      LOG_WARN("failed to update_checkpoint_unit_diagnose_info", KR(ret), K(param));

    }
  }
  return ret;
}

void UpdateFreezeInfo::operator()(ObMemtableDiagnoseInfo &info) const
{
  const int64_t start_time = ObTimeUtility::current_time();
  info.rec_scn_ = rec_scn_;
  info.start_scn_ = start_scn_;
  info.end_scn_ = end_scn_;
  info.occupy_size_ = occupy_size_;
  info.frozen_finish_time_ = start_time;
  TRANS_LOG(INFO, "update_freeze_info", K(info), K(param_));
}
int ObCheckpointDiagnoseMgr::update_freeze_info(const ObCheckpointDiagnoseParam &param,
    const share::SCN &rec_scn,
    const share::SCN &start_scn,
    const share::SCN &end_scn,
    const int64_t occupy_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCheckpointDiagnoseMgr not inited.", KR(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is invalid", KR(ret), K(param));
  } else {
    ObTraceInfo *trace_info_ptr = get_trace_info_for_memtable(param);
    if (OB_ISNULL(trace_info_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("trace_info is NULL", KR(ret), K(param));
    } else if (OB_FAIL(trace_info_ptr->update_diagnose_info<ObMemtableDiagnoseInfo>(param,
            UpdateFreezeInfo(param, rec_scn, start_scn, end_scn, occupy_size)))) {
      LOG_WARN("failed to add_memtable_diagnose_info", KR(ret), K(param));

    }
  }
  return ret;
}

DEF_UPDATE_TIME_FUNCTOR(UpdateScheduleDagTime, ObMemtableDiagnoseInfo, create_flush_dag_time)
int ObCheckpointDiagnoseMgr::update_schedule_dag_time(const ObCheckpointDiagnoseParam &param)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCheckpointDiagnoseMgr not inited.", KR(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is invalid", KR(ret), K(param));
  } else {
    ObTraceInfo *trace_info_ptr = get_trace_info_for_memtable(param);
    if (OB_ISNULL(trace_info_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("trace_info is NULL", KR(ret), K(param));
    } else if (OB_FAIL(trace_info_ptr->update_diagnose_info<ObMemtableDiagnoseInfo>(param,
            UpdateScheduleDagTime(param)))) {
      LOG_WARN("failed to update_memtable_diagnose_info", KR(ret), K(param));

    }
  }
  return ret;
}

DEF_UPDATE_TIME_FUNCTOR(UpdateReleaseTime, ObMemtableDiagnoseInfo, release_time)
int ObCheckpointDiagnoseMgr::update_release_time(const ObCheckpointDiagnoseParam &param)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCheckpointDiagnoseMgr not inited.", KR(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is invalid", KR(ret), K(param));
  } else {
    ObTraceInfo *trace_info_ptr = get_trace_info_for_memtable(param);
    if (OB_ISNULL(trace_info_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("trace_info is NULL", KR(ret), K(param));
    } else if (OB_FAIL(trace_info_ptr->update_diagnose_info<ObMemtableDiagnoseInfo>(param,
           UpdateReleaseTime(param)))) {
      LOG_WARN("failed to update_memtable_diagnose_info", KR(ret), K(param));

    }
  }
  return ret;
}

DEF_UPDATE_TIME_FUNCTOR(UpdateStartGCTimeForMemtable, ObMemtableDiagnoseInfo, start_gc_time)
int ObCheckpointDiagnoseMgr::update_start_gc_time_for_memtable(const ObCheckpointDiagnoseParam &param)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCheckpointDiagnoseMgr not inited.", KR(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is invalid", KR(ret), K(param));
  } else {
    ObTraceInfo *trace_info_ptr = get_trace_info_for_memtable(param);
    if (OB_ISNULL(trace_info_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("trace_info is NULL", KR(ret), K(param));
    } else if (OB_FAIL(trace_info_ptr->update_diagnose_info<ObMemtableDiagnoseInfo>(param,
           UpdateStartGCTimeForMemtable(param)))) {
      LOG_WARN("failed to update_memtable_diagnose_info", KR(ret), K(param));

    }
  }
  return ret;
}

void UpdateMergeInfoForMemtable::operator()(ObMemtableDiagnoseInfo &info) const
{
  info.merge_start_time_ = merge_start_time_;
  info.merge_finish_time_ = merge_finish_time_;
  info.occupy_size_ = occupy_size_;
  info.concurrent_cnt_ = concurrent_cnt_;
  TRANS_LOG(DEBUG, "update_merge_info", K(info), K(param_));
}
int ObCheckpointDiagnoseMgr::update_merge_info_for_memtable(const ObCheckpointDiagnoseParam &param,
    int64_t merge_start_time,
    int64_t merge_finish_time,
    int64_t occupy_size,
    int64_t concurrent_cnt)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCheckpointDiagnoseMgr not inited.", KR(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is invalid", KR(ret), K(param));
  } else {
    ObTraceInfo *trace_info_ptr = get_trace_info_for_memtable(param);
    if (OB_ISNULL(trace_info_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("trace_info is NULL", KR(ret), K(param));
    } else if (OB_FAIL(trace_info_ptr->update_diagnose_info<ObMemtableDiagnoseInfo>(param,
            UpdateMergeInfoForMemtable(param, merge_start_time, merge_finish_time, occupy_size, concurrent_cnt)))) {
      LOG_WARN("failed to update_memtable_diagnose_info", KR(ret), K(param));

    }
  }
  return ret;
}

int GetTraceInfoForMemtable::operator()(ObTraceInfo &trace_info) const
{
  if (param_.freeze_clock_ == trace_info.freeze_clock_ - 1
      && param_.ls_id_ == trace_info.ls_id_.id()) {
    ret_ = &trace_info;
  }
  return OB_SUCCESS;
}
ObTraceInfo* ObCheckpointDiagnoseMgr::get_trace_info_for_memtable(const ObCheckpointDiagnoseParam &param)
{
  ObTraceInfo *ret = NULL;
  if (param.is_freeze_clock_) {
    SpinRLockGuard lock(pos_lock_);
    for (int64_t i = first_pos_; i <= last_pos_; i++) {
      trace_info_arr_[i % MAX_TRACE_INFO_ARR_SIZE].read_trace_info(INVALID_TRACE_ID,
          GetTraceInfoForMemtable(param, ret));

      if (OB_NOT_NULL(ret)) {
        break;
      }
    }
  } else {
    ret = &(trace_info_arr_[param.trace_id_ % MAX_TRACE_INFO_ARR_SIZE]);
  }
  return ret;
}

int ObCheckpointDiagnoseMgr::update_max_trace_info_size(int64_t max_trace_info_size)
{
  int ret = OB_SUCCESS;
  if (max_trace_info_size < 0 || max_trace_info_size > MAX_TRACE_INFO_ARR_SIZE) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "max_trace_info_size invalid", K(ret), K(max_trace_info_size));
  } else {
    SpinWLockGuard lock(pos_lock_);
    max_trace_info_size_ = max_trace_info_size;
    reset_old_trace_infos_without_pos_lock_();
    TRANS_LOG(INFO, "max_trace_info_size update.", KPC(this));
  }
  return ret;
}

}
}
}
