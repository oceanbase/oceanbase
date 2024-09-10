//Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "storage/compaction/ob_compaction_schedule_iterator.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
using namespace storage;
using namespace common;
using namespace share;
namespace compaction
{
ObBasicMergeScheduleIterator::ObTabletArray::ObTabletArray()
  : tablet_idx_(0),
    array_(),
    is_inited_(false)
{
  array_.set_attr(ObMemAttr(MTL_ID(), "CompIter"));
}

int ObBasicMergeScheduleIterator::ObTabletArray::consume_tablet_id(ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet array is not init", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(tablet_idx_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tablet idx is invalid", KR(ret), KPC(this));
  } else if (tablet_idx_ >= count()) {
    ret = OB_ITER_END;
  } else {
    tablet_id = array_.at(tablet_idx_++);
  }
  return ret;
}

void ObBasicMergeScheduleIterator::ObTabletArray::to_string(char *buf, const int64_t buf_len, int64_t &pos) const
{
  J_OBJ_START();
  J_KV(K_(is_inited), K_(tablet_idx), "tablet_cnt", array_.count());
  if (!is_ls_iter_end()) {
    J_COMMA();
    J_KV("next_tablet", array_.at(tablet_idx_));
  }
  J_COMMA();
  J_KV(K_(array));
  J_OBJ_END();
}

ObBasicMergeScheduleIterator::ObBasicMergeScheduleIterator()
  : scan_finish_(false),
    merge_finish_(false),
    ls_idx_(-1),
    schedule_tablet_cnt_(0),
    max_batch_tablet_cnt_(0),
    cur_ls_handle_(),
    ls_ids_(),
    tablet_ids_()
{
  ls_ids_.set_attr(ObMemAttr(MTL_ID(), "CompIter"));
}

int ObBasicMergeScheduleIterator::init(const int64_t schedule_batch_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(schedule_batch_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(schedule_batch_size));
  } else if (!is_valid()) {
    ls_ids_.reuse();
    if (OB_FAIL(MTL(ObLSService *)->get_ls_ids(ls_ids_))) {
      LOG_WARN("failed to get all ls id", K(ret));
    } else {
      ls_idx_ = -1;
      tablet_ids_.reset();
      scan_finish_ = false;
      merge_finish_ = true;
      cur_ls_handle_.reset();
      schedule_tablet_cnt_ = 0;
      max_batch_tablet_cnt_ = schedule_batch_size;
      LOG_TRACE("build iter", K(ret), KPC(this));
    }
  } else { // iter is valid, no need to build, just set var to start cur batch
    (void) start_cur_batch();
  }
  return ret;
}

int ObBasicMergeScheduleIterator::get_next_ls(ObLSHandle &ls_handle)
{
  int ret = OB_SUCCESS;
  if (-1 == ls_idx_
    || tablet_ids_.is_ls_iter_end()) { // tablet iter end, need get next ls
    ++ls_idx_;
    cur_ls_handle_.reset();
    tablet_ids_.reset();
    LOG_TRACE("tablet iter end", K(ret), K(ls_idx_), K(tablet_ids_));
  }
  do {
    if (ls_idx_ >= ls_ids_.count()) {
      scan_finish_ = true;
      LOG_DEBUG("schedule all ls finish", K(ret), K_(ls_idx), K_(ls_ids));
      ret = OB_ITER_END;
    } else if (OB_FAIL(get_cur_ls_handle(ls_handle))) {
      if (OB_LS_NOT_EXIST == ret) {
        LOG_TRACE("ls not exist", K(ret), K(ls_idx_), K(ls_ids_[ls_idx_]));
        skip_cur_ls();
      } else {
        LOG_WARN("failed to get ls", K(ret), K(ls_idx_), K(ls_ids_[ls_idx_]));
      }
    } else {
      cur_ls_handle_ = ls_handle;
    }
    if (OB_SUCC(ret) && schedule_tablet_cnt_ >= max_batch_tablet_cnt_) {
      LOG_INFO("reach max batch tablet cnt, schedule next round", K(ret), K_(ls_idx), K_(ls_ids),
        K_(schedule_tablet_cnt), K_(max_batch_tablet_cnt), "tablet_cnt", tablet_ids_.count(),
        "tablet_idx", tablet_ids_.tablet_idx_);
      ret = OB_ITER_END;
    }
  } while (OB_LS_NOT_EXIST == ret);
  return ret;
}

#ifdef ERRSIM
void errsim_set_batch_cnt(
  const ObBasicMergeScheduleIterator::ObTabletArray &tablet_ids,
  int64_t &max_batch_tablet_cnt)
{
  int ret = OB_SUCCESS;
  ret = OB_E(EventTable::EN_COMPACTION_ITER_SET_BATCH_CNT) ret;
  if (OB_FAIL(ret)) {
    if (-ret <= 1) {
      max_batch_tablet_cnt = tablet_ids.array_.count();
    } else {
      max_batch_tablet_cnt = -ret;
    }
    FLOG_INFO("ERRSIM EN_COMPACTION_ITER_SET_BATCH_CNT", K(ret),
      K(max_batch_tablet_cnt), K(tablet_ids));
  }
}
#endif

int ObBasicMergeScheduleIterator::get_next_tablet(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!cur_ls_handle_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is unexpected null", K(ret), KPC(this));
  } else if (!tablet_ids_.is_inited_) {
    if (OB_FAIL(get_tablet_ids())) {
      LOG_WARN("failed to get tablet ids", K(ret));
    } else {
      tablet_ids_.mark_inited();
      LOG_TRACE("build iter in get_next_tablet", K(ret), K_(ls_idx), "ls_id", ls_ids_[ls_idx_], K(tablet_ids_));
#ifdef ERRSIM
      (void) errsim_set_batch_cnt(tablet_ids_, max_batch_tablet_cnt_);
#endif
    }
  }
  if (OB_FAIL(ret)) {
  } else if (schedule_tablet_cnt_ >= max_batch_tablet_cnt_) {
    ret = OB_ITER_END;
  } else {
    ObTabletID tablet_id;
    do {
      if (OB_FAIL(tablet_ids_.consume_tablet_id(tablet_id))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get tablet id", KR(ret), K_(tablet_ids));
        }
      } else if (OB_FAIL(get_tablet_handle(tablet_id, tablet_handle))) {
        if (OB_TABLET_NOT_EXIST == ret) {
          LOG_DEBUG("tablet not exist", K(ret), "ls_id", ls_ids_[ls_idx_], K(tablet_id), "tablet_cnt", tablet_ids_.count());
        } else {
          LOG_WARN("fail to get tablet", K(ret), K(tablet_ids_), K(tablet_id));
        }
      } else {
        tablet_handle.set_wash_priority(WashTabletPriority::WTP_LOW);
        schedule_tablet_cnt_++;
      }
    } while (OB_TABLET_NOT_EXIST == ret);
  }
  return ret;
}

void ObBasicMergeScheduleIterator::reset_basic_iter()
{
  scan_finish_ = false;
  merge_finish_ = false;
  ls_idx_ = 0;
  schedule_tablet_cnt_ = 0;
  ls_ids_.reuse();
  tablet_ids_.reset();
  cur_ls_handle_.reset();
}

bool ObBasicMergeScheduleIterator::is_valid() const
{
  return ls_ids_.count() > 0 && ls_idx_ >= 0 && cur_ls_handle_.is_valid()
    && (ls_idx_ < ls_ids_.count() - 1
      || (ls_idx_ == ls_ids_.count() - 1 && !tablet_ids_.is_ls_iter_end()));
    // have remain ls or have remain tablet
}

int64_t ObBasicMergeScheduleIterator::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(ls_idx), K_(ls_ids), K_(tablet_ids), K_(schedule_tablet_cnt), K_(max_batch_tablet_cnt));
  if (is_valid()) {
    J_COMMA();
    J_KV("cur_ls", ls_ids_.at(ls_idx_));
  }
  J_OBJ_END();
  return pos;
}


/************************************************ ObCompactionScheduleIterator ************************************************/
ObCompactionScheduleIterator::ObCompactionScheduleIterator(
    const bool is_major)
  : ObBasicMergeScheduleIterator(),
    is_major_(is_major),
    report_scn_flag_(false)
{
  ls_ids_.set_attr(ObMemAttr(MTL_ID(), "CompIter"));
}

int ObCompactionScheduleIterator::build_iter(const int64_t schedule_batch_size)
{
  int ret = OB_SUCCESS;
  bool need_reset_report_scn = !is_valid();

  if (OB_FAIL(init(schedule_batch_size))) {
    LOG_WARN("failed to inner build iter", K(ret));
  } else if (need_reset_report_scn) {
    report_scn_flag_ = false;
    if (REACH_TENANT_TIME_INTERVAL(CHECK_REPORT_SCN_INTERVAL)) {
      report_scn_flag_ = true;
    }
#ifdef ERRSIM
      report_scn_flag_ = true;
#endif
  }
  return ret;
}

void ObCompactionScheduleIterator::reset()
{
  reset_basic_iter();
  report_scn_flag_ = false;
}

int ObCompactionScheduleIterator::get_cur_ls_handle(ObLSHandle &ls_handle)
{
  int ret = MTL(ObLSService *)->get_ls(ls_ids_[ls_idx_], ls_handle, ObLSGetMod::COMPACT_MODE);
#ifdef ERRSIM
  if (OB_SUCC(ret) && ls_ids_[ls_idx_].id() > share::ObLSID::SYS_LS_ID) {
    ret = OB_E(EventTable::EN_COMPACTION_ITER_LS_NOT_EXIST) ret;
    if (OB_FAIL(ret)) {
      FLOG_INFO("ERRSIM EN_COMPACTION_ITER_LS_NOT_EXIST", KR(ret));
      ret = OB_LS_NOT_EXIST;
      ls_handle.reset();
    }
  }
#endif
  return ret;
}

#ifdef ERRSIM
void errsim_iter_invalid_tablet_id(int &ret, ObBasicMergeScheduleIterator::ObTabletArray &tablet_ids) {
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_COMPACTION_ITER_INVALID_TABLET_ID) ret;
    if (OB_FAIL(ret)) {
      FLOG_INFO("ERRSIM EN_COMPACTION_ITER_INVALID_TABLET_ID", KR(ret));
      common::ObSEArray<common::ObTabletID, 100> tmp_tablet_ids;
      int tmp_ret = OB_SUCCESS;
      const int64_t max_tablet_id = tablet_ids.array_.at(tablet_ids.count() -1).id();
      if (OB_TMP_FAIL(tmp_tablet_ids.assign(tablet_ids.array_))) {
        LOG_WARN_RET(tmp_ret, "failed to assign tablet_ids");
      } else {
        tablet_ids.reset();
        // push several invalid tablet id, rest tablet should be scheduled
        tmp_ret = tablet_ids.array_.push_back(ObTabletID(max_tablet_id + 100));
        for (int64_t i = 0; OB_SUCC(tmp_ret) && i < tmp_tablet_ids.count(); ++i) {
          if (i == tmp_tablet_ids.count() / 2) {
            tmp_ret = tablet_ids.array_.push_back(ObTabletID(max_tablet_id + 200));
          }
          if (OB_SUCC(tmp_ret)) {
            tmp_ret = tablet_ids.array_.push_back(tmp_tablet_ids.at(i));
          }
        }
        if (OB_SUCC(tmp_ret)) {
          tmp_ret = tablet_ids.array_.push_back(ObTabletID(max_tablet_id + 300));
        }
      }
      if (OB_SUCCESS == tmp_ret) {
        ret = OB_SUCCESS;
      }
    }
  }
}
#endif

int ObCompactionScheduleIterator::get_tablet_ids()
{
  tablet_ids_.reset();
  int ret = cur_ls_handle_.get_ls()->get_tablet_svr()->get_all_tablet_ids(is_major_/*except_ls_inner_tablet*/, tablet_ids_.array_);
#ifdef ERRSIM
  (void) errsim_iter_invalid_tablet_id(ret, tablet_ids_);
#endif
  return ret;
}

int ObCompactionScheduleIterator::get_tablet_handle(
  const ObTabletID &tablet_id, ObTabletHandle &tablet_handle)
{
  int ret = cur_ls_handle_.get_ls()->get_tablet_svr()->get_tablet(tablet_id, tablet_handle,  0/*timeout*/, storage::ObMDSGetTabletMode::READ_ALL_COMMITED);
#ifdef ERRSIM
  if (OB_SUCC(ret) && tablet_id.id() > ObTabletID::MIN_USER_TABLET_ID) {
    ret = OB_E(EventTable::EN_COMPACTION_ITER_TABLET_NOT_EXIST) ret;
    if (OB_FAIL(ret)) {
      FLOG_INFO("ERRSIM EN_COMPACTION_ITER_TABLET_NOT_EXIST", KR(ret));
      ret = OB_TABLET_NOT_EXIST;
      tablet_handle.reset();
    }
  }
#endif
  return ret;
}


} // namespace compaction
} // namespace oceanbase
