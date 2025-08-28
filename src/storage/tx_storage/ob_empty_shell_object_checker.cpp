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
#include "ob_empty_shell_object_checker.h"
#include "storage/ls/ob_ls.h"
#include "share/scheduler/ob_partition_auto_split_helper.h"
#include "share/ob_io_device_helper.h"

namespace oceanbase
{
using namespace share;
namespace storage
{

void ObDDLEmptyShellChecker::reset()
{
  ls_ = nullptr;
  last_check_normal_time_ = 0;
  (void)delayed_gc_tablet_infos_.destroy();
  is_inited_ = false;
}

int ObDDLEmptyShellChecker::init(storage::ObLS *ls)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arg", K(ret));
  } else if (OB_FAIL(delayed_gc_tablet_infos_.create(128/*bucket_num*/, lib::ObLabel("DDLDelayedGC")))) {
    STORAGE_LOG(WARN, "create ddl tablet delayed gc infos failed", K(ret));
  } else {
    ls_ = ls;
    last_check_normal_time_ = ObClockGenerator::getClock();
    is_inited_ = true;
  }
  return ret;
}

int ObDDLEmptyShellChecker::periodic_check_normal()
{
  int ret = OB_SUCCESS;
  const int64_t CHECK_NORMAL_INTERVAL = 7200 * 1000 * 1000L; // 2h
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_UNLIKELY(last_check_normal_time_ + CHECK_NORMAL_INTERVAL <= ObClockGenerator::getClock())) {
    ObSEArray<ObTabletID, 1> leak_tablets;
    DelayedGCTabletIterator iter = delayed_gc_tablet_infos_.begin();
    for (; iter != delayed_gc_tablet_infos_.end(); ++iter) {
      // override ret to find and release leak tablets more.
      ObTabletHandle tablet_handle;
      const ObTabletID &tablet_id = iter->first;
      const int64_t tag_deleted_us = iter->second;
      bool has_leak = false;
      if (OB_FAIL(ls_->get_tablet_svr()->get_tablet(tablet_id, tablet_handle, 
        ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US * 10, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
        if (OB_TABLET_NOT_EXIST == ret) {
          has_leak = true;
          STORAGE_LOG(WARN, "ddl delayed tablet info leak", K(ret), K(tablet_id), K(tag_deleted_us));
        } else {
          STORAGE_LOG(WARN, "get tablet failed", K(ret), K(tablet_id), K(tag_deleted_us));
        }
      } else if (OB_UNLIKELY(tablet_handle.get_obj()->is_empty_shell())) {
        ret = OB_ERR_UNEXPECTED;
        has_leak = true;
        STORAGE_LOG(WARN, "ddl delayed tablet info leak", K(ret), K(tablet_id), K(tag_deleted_us));
      }
      if (has_leak && OB_FAIL(leak_tablets.push_back(tablet_id))) {
        // override ret is expected.
        STORAGE_LOG(WARN, "push back failed", K(ret), K(tablet_id), K(tag_deleted_us));
      }
    }
    if (OB_UNLIKELY(!leak_tablets.empty())) {
      for (int64_t i = 0; i < leak_tablets.count(); i++) {
        // override ret to release leak tablets more.
        const ObTabletID &tablet_id = leak_tablets.at(i);
        if (OB_FAIL(delayed_gc_tablet_infos_.erase_refactored(tablet_id))) {
          STORAGE_LOG(WARN, "erase leak ddl tablet failed", K(ret), K(tablet_id));
        }
      }
    }
    last_check_normal_time_ = ObClockGenerator::getClock();
  }
  return ret;
}

int ObDDLEmptyShellChecker::check_disk_space_exceeds(
    const ObTabletID &tablet_id,
    bool &can_become_empty_shell)
{
  int ret = OB_SUCCESS;
  if (GCTX.is_shared_storage_mode()) {
    can_become_empty_shell = false;
  } else {
    const int64_t required_size = LOCAL_DEVICE_INSTANCE.get_total_block_size() * 0.1;
    if (OB_FAIL(LOCAL_DEVICE_INSTANCE.check_space_full(required_size, false/*alarm_if_space_full*/))) {
      if (OB_SERVER_OUTOF_DISK_SPACE == ret) {
        ret = OB_SUCCESS;
        can_become_empty_shell = true;
        STORAGE_LOG(INFO, "delete split src tablet when reaching data disk used limit", K(tablet_id), K(required_size));
      } else {
        STORAGE_LOG(WARN, "check data disk space full failed", K(ret), K(required_size));
      }
    }
  }
  return ret;
}

int ObDDLEmptyShellChecker::check_tablets_cnt_exceeds(
    const ObTabletID &tablet_id,
    bool &can_become_empty_shell)
{
  int ret = OB_SUCCESS;
  int64_t unused_real_auto_split_size = OB_INVALID_NUMERIC;
  int64_t unused_auto_split_size = 1;
  if (OB_FAIL(ObServerAutoSplitScheduler::check_tablet_creation_limit(0/*inc_tablet_cnt*/, 0.8/*safe_ratio*/, unused_auto_split_size, unused_real_auto_split_size))) {
    if (OB_TOO_MANY_PARTITIONS_ERROR == ret) {
      ret = OB_SUCCESS;
      can_become_empty_shell = true;
      STORAGE_LOG(INFO, "delete split src tablet when reaching unit tablet cnt limit", K(tablet_id));
    } else {
      STORAGE_LOG(WARN, "check tablet cnt reach limit failed", K(ret), K(tablet_id));
    }
  }
  return ret;
}

int ObDDLEmptyShellChecker::check_delay_deleted_time_exceeds(
    const ObTabletID &tablet_id,
    bool &can_become_empty_shell)
{
  int ret = OB_SUCCESS;
  int64_t tag_deleted_us = 0;
  int64_t DELAY_GC_INTERVAL = 0.5 * 3600 * 1000 * 1000L; // 0.5h
#ifdef ERRSIM
  if (DELAY_GC_INTERVAL != GCONF.errsim_delay_gc_interval) {
    DELAY_GC_INTERVAL = GCONF.errsim_delay_gc_interval;
  }
#endif
  if (OB_FAIL(delayed_gc_tablet_infos_.get_refactored(tablet_id, tag_deleted_us))) {
    if (OB_HASH_NOT_EXIST == ret) {
      if (OB_FAIL(delayed_gc_tablet_infos_.set_refactored(tablet_id, ObClockGenerator::getClock()))) {
        STORAGE_LOG(WARN, "update tablet tag deleted time failed", K(ret), K(tablet_id));
      }
    } else {
      STORAGE_LOG(WARN, "get refactored failed", K(ret), K(tablet_id));
    }
  } else if (ObClockGenerator::getClock() - tag_deleted_us >= DELAY_GC_INTERVAL) {
    can_become_empty_shell = true;
    STORAGE_LOG(INFO, "delete split src tablet when reaching predefined time limit", K(ret), K(tablet_id));
  } else {
    STORAGE_LOG(TRACE, "can not change to empty shell", K(tablet_id), K(tag_deleted_us));
  }
  return ret;
}

int ObDDLEmptyShellChecker::erase_tablet_record(
    const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    STORAGE_LOG(WARN, "not init", K(ret), K(tablet_id));
  } else if (OB_FAIL(delayed_gc_tablet_infos_.erase_refactored(tablet_id))) {
    if (OB_HASH_NOT_EXIST == ret ) {
      ret = OB_SUCCESS;
    } else {
      STORAGE_LOG(WARN, "erase failed", K(ret), K(tablet_id));
    }
  }
  return ret;
}

int ObDDLEmptyShellChecker::check_split_src_deleted_tablet(
    const ObTablet &tablet,
    const ObTabletCreateDeleteMdsUserData &user_data,
    bool &can_become_empty_shell,
    bool &need_retry)
{
  int ret = OB_SUCCESS;
  can_become_empty_shell = false;
  SCN decided_scn;
  const ObTabletID tablet_id(tablet.get_tablet_meta().tablet_id_);
  const share::ObLSID ls_id(tablet.get_tablet_meta().ls_id_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_UNLIKELY(!user_data.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "arguments are invalid", K(ret), K(user_data));
  } else if (ObTabletStatus::SPLIT_SRC_DELETED != user_data.get_tablet_status()) {
    // not split source tablet, ignore.
  } else if (OB_FAIL(ls_->get_max_decided_scn(decided_scn))) {
    STORAGE_LOG(WARN, "failed to get max decided scn", K(ret), K(user_data));
  } else if (decided_scn < user_data.delete_commit_scn_) {
    need_retry = true;
    if (REACH_THREAD_TIME_INTERVAL(1 * 1000 * 1000/*1s*/)) {
      STORAGE_LOG(INFO, "decided_scn is smaller than tablet delete commit scn",
        K(ls_id), K(tablet_id), K(user_data), K(decided_scn));
    }
  } else {
    int tmp_ret = OB_SUCCESS;
    if (!can_become_empty_shell) {
      if (OB_TMP_FAIL(check_disk_space_exceeds(tablet_id, can_become_empty_shell))) {
        STORAGE_LOG(WARN, "check data disk space full failed", K(tmp_ret), K(tablet_id));
      }
    }
    if (!can_become_empty_shell) {
      if (OB_TMP_FAIL(check_tablets_cnt_exceeds(tablet_id, can_become_empty_shell))) {
        STORAGE_LOG(WARN, "check tablet cnt reach limit failed", K(tmp_ret), K(tablet_id), K(user_data));
      }
    }
    if (!can_become_empty_shell) {
      if (OB_TMP_FAIL(check_delay_deleted_time_exceeds(tablet_id, can_become_empty_shell))) {
        STORAGE_LOG(WARN, "update tablet tag deleted time failed", K(tmp_ret), K(tablet_id));
      }
    }
    need_retry = !can_become_empty_shell ? true : need_retry;
  }
  (void)periodic_check_normal();
  return ret;
}

} // storage
} // oceanbase
