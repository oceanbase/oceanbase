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
#include "ob_tenant_compaction_progress.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/ls/ob_ls.h"
#include "storage/ob_sstable_struct.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/column_store/ob_column_oriented_sstable.h"

namespace oceanbase
{
using namespace storage;
using namespace share;
namespace compaction
{
bool ObCompactionProgress::is_valid() const
{
  bool bret = true;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id_ || merge_type_ <= INVALID_MERGE_TYPE || merge_type_ >= MERGE_TYPE_MAX
      || status_ >= ObIDag::DAG_STATUS_MAX || status_ < ObIDag::DAG_STATUS_INITING
      || data_size_ < 0 || unfinished_data_size_ < 0)) {
    bret = false;
  }
  return bret;
}

void ObCompactionProgress::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  merge_type_ = INVALID_MERGE_TYPE;
  merge_version_ = 0;
  status_ = ObIDag::DAG_STATUS_MAX;
  data_size_ = 0;
  unfinished_data_size_ = 0;
  original_size_ = 0;
  compressed_size_ = 0;
  start_time_ = 0;
  estimated_finish_time_ = 0;
  start_cg_idx_ = 0;
  end_cg_idx_ = 0;
}

bool ObTenantCompactionProgress::is_valid() const
{
  bool bret = true;
  if (OB_UNLIKELY(!ObCompactionProgress::is_valid()
      || total_tablet_cnt_ < 0
      || unfinished_tablet_cnt_ < 0)) {
    bret = false;
  }
  return bret;
}

ObTenantCompactionProgress & ObTenantCompactionProgress::operator=(const ObTenantCompactionProgress &other)
{
  is_inited_ = other.is_inited_;
  tenant_id_ = other.tenant_id_;
  merge_type_ = other.merge_type_;
  merge_version_ = other.merge_version_;
  status_ = other.status_;
  data_size_ = other.data_size_;
  unfinished_data_size_ = other.unfinished_data_size_;
  original_size_ = other.original_size_;
  compressed_size_ = other.compressed_size_;
  start_time_ = other.start_time_;
  estimated_finish_time_ = other.estimated_finish_time_;
  total_tablet_cnt_ = other.total_tablet_cnt_;
  unfinished_tablet_cnt_ = other.unfinished_tablet_cnt_;
  sum_time_guard_ = other.sum_time_guard_;
  start_cg_idx_ = other.start_cg_idx_;
  end_cg_idx_ = other.end_cg_idx_;
  real_finish_cnt_ = other.real_finish_cnt_;
  return *this;
}

bool ObTabletCompactionProgress::is_valid() const
{
  bool bret = true;
  if (OB_UNLIKELY(!ObCompactionProgress::is_valid()
      || ls_id_ < 0
      || tablet_id_ < 0
      || create_time_ <= 0)) {
    bret = false;
  }
  return bret;
}

void ObTabletCompactionProgress::reset()
{
  ObCompactionProgress::reset();
  ls_id_ = -1;
  tablet_id_ = 0;
  dag_id_.reset();
  progressive_merge_round_ = 0;
  create_time_ = 0;
}

bool ObDiagnoseTabletCompProgress::is_valid() const
{
  bool bret = true;
  if (OB_UNLIKELY(!ObCompactionProgress::is_valid()
    || create_time_ <= 0
    || (share::ObIDag::DAG_STATUS_NODE_RUNNING == status_
        && (start_time_ <= 0 || snapshot_version_ <= 0)))) {
    bret = false;
  }
  return bret;
}

void ObDiagnoseTabletCompProgress::reset()
{
  ObCompactionProgress::reset();
  is_suspect_abormal_ = false;
  dag_id_.reset();
  create_time_ = 0;
  latest_update_ts_ = 0;
  base_version_ = 0;
  snapshot_version_ = 0;
}

int ObTenantCompactionProgressMgr::mtl_init(ObTenantCompactionProgressMgr* &progress_mgr)
{
  return progress_mgr->init();
}

int ObTenantCompactionProgressMgr::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObInfoRingArray::init(SERVER_PROGRESS_MAX_CNT))) {
    STORAGE_LOG(WARN, "failed to init ObInfoRingArray", K(ret));
  }
  return ret;
}

void ObTenantCompactionProgressMgr::destroy()
{
  ObInfoRingArray::destroy();
}

int ObTenantCompactionProgressMgr::loop_major_sstable_(
    const int64_t merge_snapshot_version,
    int64_t &cnt,
    int64_t &size)
{
  int ret = OB_SUCCESS;
  common::ObTimeGuard timeguard("loop_major_sstable_to_calc_progress_size", 30 * 1000 * 1000); // 30s
  ObSharedGuard<ObLSIterator> ls_iter_guard;
  ObLS *ls = nullptr;
  if (OB_FAIL(MTL(ObLSService *)->get_ls_iter(ls_iter_guard, ObLSGetMod::COMPACT_MODE))) {
    LOG_WARN("failed to get ls iterator", K(ret));
  } else {
    while (OB_SUCC(ret)) { // loop all log_stream
      if (OB_FAIL(ls_iter_guard.get_ptr()->get_next(ls))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get ls", K(ret), KP(ls_iter_guard.get_ptr()));
        }
      } else if (OB_ISNULL(ls)){
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls is nullptr", K(ret), KPC(ls));
      } else if (ls->is_deleted()) {
        // do nothing
      } else {
        ObLSTabletIterator tablet_iter(ObMDSGetTabletMode::READ_WITHOUT_CHECK);
        const ObLSID &ls_id = ls->get_ls_id();
        if (OB_FAIL(ls->get_tablet_svr()->build_tablet_iter(tablet_iter))) {
          LOG_WARN("failed to build ls tablet iter", K(ret), K(ls));
        } else {
          ObTabletHandle tablet_handle;
          ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
          int tmp_ret = OB_SUCCESS;
          while (OB_SUCC(ret)) { // loop all tablets in ls
            if (OB_FAIL(tablet_iter.get_next_tablet(tablet_handle))) {
              if (OB_ITER_END == ret) {
                ret = OB_SUCCESS;
                break;
              } else {
                LOG_WARN("failed to get tablet", K(ret), K(ls_id), K(tablet_handle));
              }
            } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid tablet handle", K(ret), K(ls_id), K(tablet_handle));
            } else if (!tablet_handle.get_obj()->get_tablet_meta().tablet_id_.is_special_merge_tablet()) {
              ObSSTable *sstable = nullptr;
              if (OB_FAIL(tablet_handle.get_obj()->fetch_table_store(table_store_wrapper))) {
                LOG_WARN("faile to fetch table store", K(ret));
              } else if (OB_ISNULL(sstable = static_cast<ObSSTable *>(
                  table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(true/*last*/)))) {
                // do nothing
              } else if (sstable->get_snapshot_version() <= merge_snapshot_version) {
                // ATTENTION:
                // 1. it is hard to distinguish whether this major was generated by this compaction or whether it existed before the compaction,
                //    so maybe some more tablets will be calculated.
                // 2. for the major sstable generated by this compaction, the size will be calculated larger than the old major sstable.
                ++cnt;
                size += sstable->get_total_macro_block_count() * DEFAULT_MACRO_BLOCK_SIZE;
              }
            }
          } // end of while
        }
      }
    } // end of while
  }
  return ret;
}

int ObTenantCompactionProgressMgr::add_progress(const int64_t major_snapshot_version)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  SpinWLockGuard guard(lock_);
  if (OB_UNLIKELY(major_snapshot_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(major_snapshot_version));
  } else if (OB_FAIL(get_pos_(major_snapshot_version, pos))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("pos is invalid", K(ret), K(pos), K(major_snapshot_version));
    } else {
      ret = OB_SUCCESS; // clear OB_ENTRY_NOT_EXIST
      // force to finish previous progress
      for (int64_t i = 0; i < size(); ++i) {
        (void)finish_progress_(array_[i]);
      }
      ObTenantCompactionProgress progress;
      progress.merge_version_ = major_snapshot_version;
      progress.tenant_id_ = MTL_ID();
      progress.merge_type_ = MAJOR_MERGE;
      progress.start_time_ = ObTimeUtility::fast_current_time();
      progress.status_ = share::ObIDag::DAG_STATUS_INITING;
      if (OB_FAIL(ObInfoRingArray::add_no_lock(progress))) {
        LOG_WARN("failed to add progress", K(ret));
      } else {
        LOG_INFO("add_progress", K(ret), K(major_snapshot_version), K(progress), K(size()));
      }
    }
  }
  return ret;
}

// init data size
int ObTenantCompactionProgressMgr::init_progress(const int64_t major_snapshot_version)
{
  int ret = OB_SUCCESS;
  int64_t total_tablet_cnt = 0;
  int64_t occupy_size = 0;
  if (OB_UNLIKELY(major_snapshot_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(major_snapshot_version));
  } else if (OB_FAIL(loop_major_sstable_(
          major_snapshot_version,
          total_tablet_cnt,
          occupy_size))) {
    LOG_WARN("failed to get sstable info", K(ret));
  } else {
    int64_t pos = -1;
    SpinWLockGuard guard(lock_);
    if (OB_FAIL(get_pos_(major_snapshot_version, pos))) {
      LOG_WARN("pos is invalid", K(ret), K(pos), K(major_snapshot_version));
    } else if (share::ObIDag::DAG_STATUS_FINISH != array_[pos].status_) { // before init, major probably already finished
      array_[pos].is_inited_ = true;
      array_[pos].total_tablet_cnt_ = total_tablet_cnt;
      array_[pos].data_size_ += occupy_size;
      array_[pos].unfinished_tablet_cnt_ += array_[pos].total_tablet_cnt_;
      array_[pos].unfinished_data_size_ += array_[pos].data_size_;
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("success to init progress", K(ret), K(major_snapshot_version), K(array_[pos]));
    }
  }
  return ret;
}

int ObTenantCompactionProgressMgr::finish_progress_(ObTenantCompactionProgress &progress)
{
  int ret = OB_SUCCESS;
  if (share::ObIDag::DAG_STATUS_FINISH != progress.status_) {
    progress.unfinished_data_size_ = 0;
    progress.estimated_finish_time_ = ObTimeUtility::fast_current_time();
    progress.unfinished_tablet_cnt_ = 0;
    progress.status_ = share::ObIDag::DAG_STATUS_FINISH;
  }
  return ret;
}

int ObTenantCompactionProgressMgr::update_progress_status(
    const int64_t major_snapshot_version,
    share::ObIDag::ObDagStatus status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(major_snapshot_version < 0) || status < share::ObIDag::DAG_STATUS_INITING
      || status >= share::ObIDag::DAG_STATUS_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(major_snapshot_version), K(status));
  } else if (major_snapshot_version > 0) {
    int64_t pos = -1;
    SpinWLockGuard guard(lock_);
    if (OB_FAIL(get_pos_(major_snapshot_version, pos))) {
      LOG_WARN("pos is invalid", K(ret), K(pos), K(major_snapshot_version), K(status));
    } else if (share::ObIDag::DAG_STATUS_FINISH != array_[pos].status_) {
      if (share::ObIDag::DAG_STATUS_FINISH != status) {
        array_[pos].status_ = status;
      }
      if (share::ObIDag::DAG_STATUS_FINISH == status && OB_FAIL(finish_progress_(array_[pos]))) {
        LOG_WARN("failed to finish progress", K(ret), K(pos), K(major_snapshot_version), K(status));
      } else {
        LOG_DEBUG("success to update status", K(ret), K(pos), K(major_snapshot_version), K(status), K(array_[pos]));
      }
    }
  }
  return ret;
}

int ObTenantCompactionProgressMgr::get_pos_(const int64_t major_snapshot_version, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  pos = ObInfoRingArray::get_last_pos();
  int64_t loop_cnt = max_cnt_;
  while (OB_SUCC(ret) && 0 < loop_cnt) {
    if (array_[pos].merge_version_ == major_snapshot_version) {
      break;
    } else if (array_[pos].merge_version_ > major_snapshot_version) {
      LOG_DEBUG("merge_version is larger than major_snapshot_version", K(pos),
        "merge_version", array_[pos].merge_version_,
        K(major_snapshot_version));
      pos = pos == 0 ? max_cnt_ - 1 : pos - 1;
    } else {
      pos = -1;
      ret = OB_ENTRY_NOT_EXIST;
      LOG_DEBUG("entry not exits", K(ret), K(pos), K(major_snapshot_version));
      break;
    }
    --loop_cnt;
  }
  if (OB_SUCC(ret) && pos >= 0 && pos < SERVER_PROGRESS_MAX_CNT
      && array_[pos].merge_version_ != major_snapshot_version) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("major version is error", K(ret), K(pos), K(major_snapshot_version));
  }
  return ret;
}

int ObTenantCompactionProgressMgr::update_progress(
    const int64_t major_snapshot_version,
    const int64_t total_data_size_delta,
    const int64_t scanned_data_size_delta,
    const int64_t estimate_finish_time,
    const bool finish_flag,
    const ObCompactionTimeGuard *time_guard,
    const bool co_merge)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(major_snapshot_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(major_snapshot_version), K(scanned_data_size_delta));
  } else {
    int64_t pos = -1;
    SpinWLockGuard guard(lock_);
    if (OB_FAIL(get_pos_(major_snapshot_version, pos))) {
      LOG_WARN("pos is invalid", K(ret), K(pos), K(major_snapshot_version));
    } else if (share::ObIDag::DAG_STATUS_FINISH != array_[pos].status_) {
      if (finish_flag && !co_merge) {
        if (array_[pos].is_inited_ && OB_UNLIKELY(0 == array_[pos].unfinished_tablet_cnt_)) {
          if (REACH_TIME_INTERVAL(1000 * 1000)) {
            LOG_WARN("unfinished partition count is invalid", K(ret), K(array_[pos].unfinished_tablet_cnt_));
          }
        } else {
          array_[pos].unfinished_tablet_cnt_--;
        }
        array_[pos].real_finish_cnt_++;
      }

      array_[pos].data_size_ += total_data_size_delta;
      array_[pos].unfinished_data_size_ += total_data_size_delta;
      array_[pos].unfinished_data_size_ -= scanned_data_size_delta;
      if (nullptr != time_guard) {
        // ObCompactionTimeGuard don't have to_string
        const ObStorageCompactionTimeGuard *storage_time_guard = static_cast<const ObStorageCompactionTimeGuard *>(time_guard);
        array_[pos].sum_time_guard_.add_time_guard(*storage_time_guard);
      }

      if (array_[pos].is_inited_) {
        if (OB_UNLIKELY(array_[pos].data_size_ < 0)) {
          LOG_WARN("data size is invalid", K(ret), K(array_[pos].data_size_));
          array_[pos].data_size_ = 0;
        }
        if (OB_UNLIKELY(array_[pos].unfinished_data_size_ < 0)) {
          LOG_WARN("unfinished data size is invalid", K(ret), K(array_[pos].unfinished_data_size_));
          array_[pos].unfinished_data_size_ = 0;
        }
      }

      array_[pos].estimated_finish_time_ = MAX(array_[pos].estimated_finish_time_, estimate_finish_time);
      if (REACH_TIME_INTERVAL(FINISH_TIME_UPDATE_FROM_SCHEDULER_INTERVAL)) {
        const int64_t current_time = ObTimeUtility::fast_current_time();
        int64_t rest_time = 0;
        int64_t data_size = array_[pos].data_size_ < 0 ? 0 : array_[pos].data_size_;
        int64_t unfinished_data_size = array_[pos].unfinished_data_size_ < 0 ? 0 : array_[pos].unfinished_data_size_;
        const int64_t used_time = current_time - array_[pos].start_time_;
        if (0 != used_time) {
          const float work_ratio = (float)(data_size - unfinished_data_size) / used_time;
          if (fabs(work_ratio) > 1e-6) {
            rest_time = (int64_t)(unfinished_data_size / work_ratio);
          }
        }
        array_[pos].estimated_finish_time_ = MAX(array_[pos].estimated_finish_time_, current_time + rest_time);
      }
      if (ObPartitionMergeProgress::MAX_ESTIMATE_SPEND_TIME < array_[pos].estimated_finish_time_ - array_[pos].start_time_) {
        array_[pos].estimated_finish_time_ = array_[pos].start_time_ + ObPartitionMergeProgress::MAX_ESTIMATE_SPEND_TIME;
      }
      LOG_DEBUG("success to update progress", K(ret), K(total_data_size_delta), K(major_snapshot_version), K(finish_flag), K(total_data_size_delta),
          K(scanned_data_size_delta), K(array_[pos]));
    }
  }
  return ret;
}

int ObTenantCompactionProgressMgr::update_unfinish_tablet(const int64_t major_snapshot_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(major_snapshot_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(major_snapshot_version));
  } else {
    int64_t pos = -1;
    SpinWLockGuard guard(lock_);
    if (OB_FAIL(get_pos_(major_snapshot_version, pos))) {
      LOG_WARN("pos is invalid", K(ret), K(pos), K(major_snapshot_version));
    } else if (OB_UNLIKELY(0 == array_[pos].unfinished_tablet_cnt_)) {
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        LOG_WARN("unfinished partition count is invalid", K(ret), K(pos), K(array_[pos].unfinished_tablet_cnt_));
      }
    } else {
      array_[pos].unfinished_tablet_cnt_--;
      array_[pos].real_finish_cnt_++;
    }
  }
  return ret;
}

int ObTenantCompactionProgressMgr::update_compression_ratio(
    const int64_t major_snapshot_version,
    ObSSTableMergeInfo &info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(major_snapshot_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(major_snapshot_version));
  } else {
    int64_t pos = -1;
    SpinWLockGuard guard(lock_);
    if (OB_FAIL(get_pos_(major_snapshot_version, pos))) {
      LOG_WARN("pos is invalid", K(ret), K(pos), K(major_snapshot_version));
    } else {
      array_[pos].original_size_ += info.original_size_;
      array_[pos].compressed_size_ += info.compressed_size_;
    }
  }
  return ret;
}

/*
 * ObTenantCompactionProgressIterator implement
 * */

int ObTenantCompactionProgressIterator::open(const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  omt::TenantIdList all_tenants;
  all_tenants.set_label(ObModIds::OB_TENANT_ID_LIST);
  if (is_opened_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("The ObTabletCompactionProgressIterator has been opened", K(ret));
  } else if (!::is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else if (OB_SYS_TENANT_ID == tenant_id) { // sys tenant can get all tenants' info
    GCTX.omt_->get_tenant_ids(all_tenants);
  } else if (OB_FAIL(all_tenants.push_back(tenant_id))) {
    LOG_WARN("failed to push back tenant_id", K(ret), K(tenant_id));
  }
  for (int i = 0; OB_SUCC(ret) && i < all_tenants.size(); ++i) {
    if (!is_virtual_tenant_id(all_tenants[i])) { // skip virtual tenant
      MTL_SWITCH(all_tenants[i]) {
        if (OB_FAIL(MTL(ObTenantCompactionProgressMgr *)->get_list(progress_array_))) {
          LOG_WARN("failed to get compaction info", K(ret));
        }
      } else {
        if (OB_TENANT_NOT_IN_SERVER != ret) {
          STORAGE_LOG(WARN, "switch tenant failed", K(ret), K(all_tenants[i]));
        } else {
          ret = OB_SUCCESS;
          continue;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    cur_idx_ = 0;
    is_opened_ = true;
  }
  return ret;
}

void ObTenantCompactionProgressIterator::reset()
{
  progress_array_.reset();
  cur_idx_ = 0;
  is_opened_ = false;
}

int ObTenantCompactionProgressIterator::get_next_info(ObTenantCompactionProgress &info)
{
  int ret = OB_SUCCESS;
  if (!is_opened_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (cur_idx_ >= progress_array_.count()) {
    ret = OB_ITER_END;
  } else {
    info = progress_array_.at(cur_idx_);
    ++cur_idx_;
  }
  return ret;
}

/*
 * ObTabletCompactionProgressIterator implement
 * */

int ObTabletCompactionProgressIterator::open(const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  omt::TenantIdList all_tenants;
  all_tenants.set_label(ObModIds::OB_TENANT_ID_LIST);
  if (is_opened_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("The ObTabletCompactionProgressIterator has been opened", K(ret));
  } else if (!::is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else if (OB_SYS_TENANT_ID == tenant_id) { // sys tenant can get all tenants' info
    GCTX.omt_->get_tenant_ids(all_tenants);
  } else if (OB_FAIL(all_tenants.push_back(tenant_id))) {
    LOG_WARN("failed to push back tenant_id", K(ret), K(tenant_id));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < all_tenants.size(); ++i) {
    uint64_t tenant_id = all_tenants[i];
    if (!is_virtual_tenant_id(tenant_id)) { // skip virtual tenant
      MTL_SWITCH(tenant_id) {
        if (OB_FAIL(MTL(ObTenantDagScheduler *)->get_all_compaction_dag_info(allocator_, progress_array_))) {
          LOG_WARN("failed to get compaction info", K(ret));
        }
      } else {
        if (OB_TENANT_NOT_IN_SERVER != ret) {
          STORAGE_LOG(WARN, "switch tenant failed", K(ret), K(tenant_id));
        } else {
          ret = OB_SUCCESS;
          continue;
        }
      }
    }
  } // end for
  if (OB_SUCC(ret)) {
    cur_idx_ = 0;
    is_opened_ = true;
  }
  return ret;
}

void ObTabletCompactionProgressIterator::reset()
{
  progress_array_.reset();
  allocator_.reset();
  cur_idx_ = 0;
  is_opened_ = false;
}

int ObTabletCompactionProgressIterator::get_next_info(ObTabletCompactionProgress &info)
{
  int ret = OB_SUCCESS;
  if (!is_opened_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (cur_idx_ >= progress_array_.count()) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(progress_array_.at(cur_idx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("progress is null", K(ret), K(cur_idx_));
  } else {
    info = *progress_array_.at(cur_idx_);
    ++cur_idx_;
  }
  return ret;
}

} //compaction
} //oceanbase
