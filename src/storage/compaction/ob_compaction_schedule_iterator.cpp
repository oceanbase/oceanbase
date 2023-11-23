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

ObCompactionScheduleIterator::ObCompactionScheduleIterator(
    const bool is_major,
    ObLSGetMod mod)
    : mod_(mod),
      is_major_(is_major),
      scan_finish_(false),
      merge_finish_(false),
      report_scn_flag_(false),
      ls_idx_(-1),
      tablet_idx_(0),
      schedule_tablet_cnt_(0),
      max_batch_tablet_cnt_(0),
      ls_tablet_svr_(nullptr),
      ls_ids_(),
      tablet_ids_()
{
  ls_ids_.set_attr(ObMemAttr(MTL_ID(), "CompIter"));
  tablet_ids_.set_attr(ObMemAttr(MTL_ID(), "CompIter"));
}

int ObCompactionScheduleIterator::build_iter(const int64_t schedule_batch_size)
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
      tablet_idx_ = 0;
      tablet_ids_.reuse();
      scan_finish_ = false;
      merge_finish_ = true;
      ls_tablet_svr_ = nullptr;
      schedule_tablet_cnt_ = 0;
      report_scn_flag_ = false;
      max_batch_tablet_cnt_ = schedule_batch_size;
      if (REACH_TENANT_TIME_INTERVAL(CHECK_REPORT_SCN_INTERVAL)) {
        report_scn_flag_ = true;
      }
#ifdef ERRSIM
      report_scn_flag_ = true;
#endif
      LOG_TRACE("build iter", K(ret), KPC(this));
    }
  } else { // iter is invalid, no need to build, just set var to start cur batch
    (void) start_cur_batch();
  }
  return ret;
}

int ObCompactionScheduleIterator::get_next_ls(ObLSHandle &ls_handle)
{
  int ret = OB_SUCCESS;
  if (-1 == ls_idx_
    || tablet_idx_ >= tablet_ids_.count()) { // tablet iter end, need get next ls
    ++ls_idx_;
    ls_tablet_svr_ = nullptr;
    tablet_ids_.reuse();
    LOG_TRACE("tablet iter end", K(ret), K(ls_idx_), K(tablet_idx_), "tablet_cnt", tablet_ids_.count());
  }
  do {
     if (schedule_tablet_cnt_ >= max_batch_tablet_cnt_) {
      ret = OB_ITER_END;
     } else if (ls_idx_ >= ls_ids_.count()) {
      scan_finish_ = true;
      ret = OB_ITER_END;
    } else if (OB_FAIL(get_cur_ls_handle(ls_handle))) {
      if (OB_LS_NOT_EXIST == ret) {
        LOG_TRACE("ls not exist", K(ret), K(ls_idx_), K(ls_ids_[ls_idx_]));
        skip_cur_ls();
      } else {
        LOG_WARN("failed to get ls", K(ret), K(ls_idx_), K(ls_ids_[ls_idx_]));
      }
    } else {
      ls_tablet_svr_ = ls_handle.get_ls()->get_tablet_svr();
    }
  } while (OB_LS_NOT_EXIST == ret);
  return ret;
}

void ObCompactionScheduleIterator::reset()
{
  scan_finish_ = false;
  merge_finish_ = false;
  ls_idx_ = 0;
  tablet_idx_ = 0;
  schedule_tablet_cnt_ = 0;
  ls_ids_.reuse();
  tablet_ids_.reuse();
  ls_tablet_svr_ = nullptr;
  report_scn_flag_ = false;
}

bool ObCompactionScheduleIterator::is_valid() const
{
  return ls_ids_.count() > 0 && ls_idx_ >= 0 && nullptr != ls_tablet_svr_
    && (ls_idx_ < ls_ids_.count() - 1
      || (ls_idx_ == ls_ids_.count() - 1 && tablet_idx_ < tablet_ids_.count()));
    // have remain ls or have remain tablet
}

int ObCompactionScheduleIterator::get_next_tablet(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ls_tablet_svr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls tablet svr is unexpected null", K(ret), KPC(this));
  } else if (tablet_ids_.empty()) {
    if (OB_FAIL(get_tablet_ids())) {
      LOG_WARN("failed to get tablet ids", K(ret));
    } else {
      tablet_idx_ = 0; // for new ls, set tablet_idx_ = 0
      LOG_TRACE("build iter", K(ret), K_(ls_idx), "ls_id", ls_ids_[ls_idx_], K(tablet_ids_));
    }
  }
  if (OB_SUCC(ret)) {
    do {
      if (tablet_idx_ >= tablet_ids_.count()) {
        ret = OB_ITER_END;
      } else if (schedule_tablet_cnt_ >= max_batch_tablet_cnt_) {
        ret = OB_ITER_END;
      } else {
        const common::ObTabletID &tablet_id = tablet_ids_.at(tablet_idx_);
        if (OB_FAIL(get_tablet_handle(tablet_id, tablet_handle))) {
          if (OB_TABLET_NOT_EXIST == ret) {
            tablet_idx_++;
          } else {
            LOG_WARN("fail to get tablet", K(ret), K(tablet_idx_), K(tablet_id));
          }
        } else {
          tablet_handle.set_wash_priority(WashTabletPriority::WTP_LOW);
          tablet_idx_++;
          schedule_tablet_cnt_++;
        }
      }
    } while (OB_TABLET_NOT_EXIST == ret);
  }
  return ret;
}

int ObCompactionScheduleIterator::get_cur_ls_handle(ObLSHandle &ls_handle)
{
  int ret = MTL(ObLSService *)->get_ls(ls_ids_[ls_idx_], ls_handle, mod_);
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

int ObCompactionScheduleIterator::get_tablet_ids()
{
  int ret = ls_tablet_svr_->get_all_tablet_ids(is_major_/*except_ls_inner_tablet*/, tablet_ids_);
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_COMPACTION_ITER_INVALID_TABLET_ID) ret;
    if (OB_FAIL(ret)) {
      FLOG_INFO("ERRSIM EN_COMPACTION_ITER_INVALID_TABLET_ID", KR(ret));
      common::ObSEArray<common::ObTabletID, 100> tmp_tablet_ids;
      int tmp_ret = OB_SUCCESS;
      const int64_t max_tablet_id = tablet_ids_.at(tablet_ids_.count() -1).id();
      if (OB_TMP_FAIL(tmp_tablet_ids.assign(tablet_ids_))) {
        LOG_WARN_RET(tmp_ret, "failed to assign tablet_ids");
      } else {
        tablet_ids_.reset();
        // push several invalid tablet id, rest tablet should be scheduled
        tmp_ret = tablet_ids_.push_back(ObTabletID(max_tablet_id + 100));
        for (int64_t i = 0; OB_SUCC(tmp_ret) && i < tmp_tablet_ids.count(); ++i) {
          if (i == tmp_tablet_ids.count() / 2) {
            tmp_ret = tablet_ids_.push_back(ObTabletID(max_tablet_id + 200));
          }
          if (OB_SUCC(tmp_ret)) {
            tmp_ret = tablet_ids_.push_back(tmp_tablet_ids.at(i));
          }
        }
        if (OB_SUCC(tmp_ret)) {
          tmp_ret = tablet_ids_.push_back(ObTabletID(max_tablet_id + 300));
        }
      }
      if (OB_SUCCESS == tmp_ret) {
        ret = OB_SUCCESS;
      }
    }
  }
#endif
  return ret;
}

int ObCompactionScheduleIterator::get_tablet_handle(
  const ObTabletID &tablet_id, ObTabletHandle &tablet_handle)
{
  int ret = ls_tablet_svr_->get_tablet(tablet_id, tablet_handle,  0/*timeout*/, storage::ObMDSGetTabletMode::READ_ALL_COMMITED);
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

int64_t ObCompactionScheduleIterator::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(ls_idx), K_(ls_ids), K_(tablet_idx), K(tablet_ids_.count()), K_(schedule_tablet_cnt), K_(max_batch_tablet_cnt));
  if (is_valid()) {
    J_COMMA();
    J_KV("cur_ls", ls_ids_.at(ls_idx_));
    if (!tablet_ids_.empty() && tablet_idx_ > 0 && tablet_idx_ < tablet_ids_.count()) {
      J_COMMA();
      J_KV("next_tablet", tablet_ids_.at(tablet_idx_));
    }
  }
  J_OBJ_END();
  return pos;
}

} // namespace compaction
} // namespace oceanbase
