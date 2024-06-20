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

#ifndef OCEANBASE_STORAGE_OB_DATA_CHECKPOINT_H_
#define OCEANBASE_STORAGE_OB_DATA_CHECKPOINT_H_

#include <cstdint>
#include "storage/checkpoint/ob_common_checkpoint.h"
#include "lib/lock/ob_spin_lock.h"
#include "storage/checkpoint/ob_freeze_checkpoint.h"
#include "share/scn.h"
#include "share/ob_errno.h"


namespace oceanbase
{
namespace storage
{
class ObLS;
namespace checkpoint
{

class ObCheckpointIterator;

struct ObCheckpointDList
{
  ObCheckpointDList() {}
  ~ObCheckpointDList() {}

  void reset();
  bool is_empty() const;
  ObFreezeCheckpoint *get_header();
  int unlink(ObFreezeCheckpoint *ob_freeze_checkpoint);
  int insert(ObFreezeCheckpoint *ob_freeze_checkpoint, bool ordered = true);
  void get_iterator(ObCheckpointIterator &iterator);
  share::SCN get_min_rec_scn_in_list(bool ordered = true);
  ObFreezeCheckpoint *get_first_greater(const share::SCN rec_scn);
  int get_freezecheckpoint_info(
    ObIArray<checkpoint::ObFreezeCheckpointVTInfo> &freeze_checkpoint_array);
  int get_need_freeze_checkpoints(const share::SCN rec_scn,
                                  ObIArray<ObFreezeCheckpoint*> &freeze_checkpoints);

  ObDList<ObFreezeCheckpoint> checkpoint_list_;
};

// lock outside
class ObCheckpointIterator
{
public:
  ObCheckpointIterator() {}
  ~ObCheckpointIterator() {}

  void init(ObCheckpointDList *dlist);
  ObFreezeCheckpoint *get_next();
  bool has_next() const;

private:
  ObFreezeCheckpoint *cur_;
  ObFreezeCheckpoint *next_;
  ObCheckpointDList *dlist_;
};

// responsible for maintenance transaction checkpoint unit
class ObDataCheckpoint : public ObCommonCheckpoint
{
  friend class ObFreezeCheckpoint;
  friend class ObCheckpointLockGuard;

public:
  ObDataCheckpoint()
    : is_inited_(false),
      ls_(nullptr),
      new_create_list_(),
      active_list_(),
      prepare_list_(),
      ls_frozen_list_(),
      ls_freeze_finished_(true)
  {}
  ~ObDataCheckpoint() { ls_ = nullptr; }

  // used for virtual table
  static const uint64_t LS_DATA_CHECKPOINT_TABLET_ID = 40000;
  int init(ObLS *ls);
  int safe_to_destroy(bool &is_safe_destroy);
  ObCheckpointDList* get_checkpoint_list(const ObFreezeCheckpointLocation &location)
  {
    ObCheckpointDList *ret = NULL;
    switch (location) {
      case LS_FROZEN:
        ret = &ls_frozen_list_;
        break;
      case NEW_CREATE:
        ret = &new_create_list_;
        break;
      case ACTIVE:
        ret = &active_list_;
        break;
      case PREPARE:
        ret = &prepare_list_;
        break;
      default:
        break;
    }
    return ret;
  }

  share::SCN get_rec_scn();
  share::SCN get_active_rec_scn();
  // if min_rec_scn <= the input rec_scn
  // logstream freeze
  int flush(share::SCN recycle_scn, int64_t trace_id, bool need_freeze = true);
  // if min_rec_scn <= the input rec_scn
  // add ls_freeze task
  // logstream freeze optimization
  int ls_freeze(share::SCN rec_scn);
  // logstream_freeze schedule and minor merge schedule
  void road_to_flush(share::SCN rec_scn);
  // ObFreezeCheckpoint register into ObDataCheckpoint
  int add_to_new_create(ObFreezeCheckpoint *ob_freeze_checkpoint);
  // timer to tranfer freeze_checkpoint that rec_scn is stable from new_create_list to
  // active_list
  int check_can_move_to_active_in_newcreate();

  // judge logstream_freeze task if finished
  bool ls_freeze_finished();

  int get_freezecheckpoint_info(
    ObIArray<checkpoint::ObFreezeCheckpointVTInfo> &freeze_checkpoint_array);

  ObTabletID get_tablet_id() const;

  bool is_flushing() const;

  bool is_empty();

  static void set_tenant_freeze() { is_tenant_freeze_for_flush_ = true; }
  static void reset_tenant_freeze() { is_tenant_freeze_for_flush_ = false; }
  static bool is_tenant_freeze() { return is_tenant_freeze_for_flush_; }

private:
  // traversal prepare_list to flush memtable
  // case1: some memtable flush failed when ls freeze
  // case2: the memtable that tablet freeze
  int traversal_flush_();
  int unlink_(ObFreezeCheckpoint *ob_freeze_checkpoint);
  int finish_freeze(ObFreezeCheckpoint *ob_freeze_checkpoint);
  int insert_(ObFreezeCheckpoint *ob_freeze_checkpoint,
              ObCheckpointDList &dst,
              bool ordered = true);
  int transfer_(ObFreezeCheckpoint *ob_freeze_checkpoint,
                ObCheckpointDList &src,
                ObCheckpointDList &dst,
                ObFreezeCheckpointLocation location);

  int transfer_from_ls_frozen_to_active_without_src_lock_(ObFreezeCheckpoint *ob_freeze_checkpoint);
  int transfer_from_ls_frozen_to_new_created_without_src_lock_(ObFreezeCheckpoint *ob_freeze_checkpoint);
  int transfer_from_ls_frozen_to_prepare_without_src_lock_(ObFreezeCheckpoint *ob_freeze_checkpoint);
  int transfer_from_new_create_to_active_without_src_lock_(ObFreezeCheckpoint *ob_freeze_checkpoint);
  int transfer_from_new_create_to_prepare_(ObFreezeCheckpoint *ob_freeze_checkpoint);
  int transfer_from_active_to_prepare_(ObFreezeCheckpoint *ob_freeze_checkpoint);

  void pop_active_list_to_ls_frozen_(ObFreezeCheckpoint *last);
  void pop_new_create_to_ls_frozen_();
  void ls_frozen_to_active_(int64_t &last_time);
  void ls_frozen_to_prepare_(int64_t &last_time);
  void add_diagnose_info_for_ls_frozen_();
  void print_list_(ObCheckpointDList &list);
  void set_ls_freeze_finished_(bool is_finished);
  int get_need_flush_tablets_(const share::SCN recycle_scn,
                              common::ObIArray<ObTabletID> &flush_tablets);
  int freeze_base_on_needs_(const int64_t trace_id, share::SCN recycle_scn);
  int decide_freeze_clock_(ObFreezeCheckpoint *ob_freeze_checkpoint);

  static const int64_t LOOP_TRAVERSAL_INTERVAL_US = 1000L * 50;  // 50ms
  // when freeze memtable base on needs less than TABLET_FREEZE_PERCENT,
  // tablet_freeze will be instead of logstream_freeze
  // to relieve pressure for mini minor merge
  static const int64_t TABLET_FREEZE_PERCENT = 10;
  // when nums of memtables that wait to freeze less than MAX_FREEZE_CHECKPOINT_NUM.
  // logstream_freeze without get_need_flush_tablets
  static const int64_t MAX_FREEZE_CHECKPOINT_NUM = 50;
  bool is_inited_;
  ObLS *ls_;
  // new_create_list is unordered_list
  // active_list and prepare_list is ordered_list and order by rec_log_ts
  // improve computational efficiency of checkpoint

  // new created and rec_log_ts_not_stable ObFreezeCheckpoint in new_create_list
  // rec_log_ts_is_stable and not ready_for_flush ObFreezeCheckpoint in active_list
  // ready_for_flush ObFreezeCheckpoint in prepare_list
  ObCheckpointDList new_create_list_;
  ObCheckpointDList active_list_;
  ObCheckpointDList prepare_list_;
  // tmp_list for ls_freeze to improve performance
  // used when new_create_list_ -> active_list and active_list -> frozen_list
  ObCheckpointDList ls_frozen_list_;

  struct ObCheckpointLock
  {
    common::SpinRWLock ls_frozen_list_lock_;
    common::SpinRWLock new_create_list_lock_;
    common::SpinRWLock active_list_lock_;
    common::SpinRWLock prepare_list_lock_;
  } lock_;

  bool ls_freeze_finished_;

  static __thread bool is_tenant_freeze_for_flush_;
};

// list lock for DataChcekpoint
// flag:1~4: ObFreezeCheckpointLocation list lock
// flag:8: read/write lock
#define RLOCK(flag) ObCheckpointLockGuard lock_guard(*this, ~0x80 & flag, "[data_checkpoint]"); lock_guard.click(__FUNCTION__)
#define WLOCK(flag) ObCheckpointLockGuard lock_guard(*this, 0x80 | flag, "[data_checkpoint]"); lock_guard.click(__FUNCTION__)

class ObCheckpointLockGuard
{
public:
  ObCheckpointLockGuard(ObDataCheckpoint &data_checkpoint,
      uint8_t flag,
      const char *owner = "unknown",
      const int64_t warn_threshold = 50 * 1000 /* 50 ms */)
  : time_guard_(owner, warn_threshold),
    lock_(data_checkpoint.lock_),
    flag_(flag)
  {
    if (0 != (flag & LS_FROZEN)) {
      lock(flag, lock_.ls_frozen_list_lock_);
    }
    if (0 != (flag & NEW_CREATE)) {
      lock(flag, lock_.new_create_list_lock_);
    }
    if (0 != (flag & ACTIVE)) {
      lock(flag, lock_.active_list_lock_);
    }
    if (0 != (flag & PREPARE)) {
      lock(flag, lock_.prepare_list_lock_);
    }
  }

  ~ObCheckpointLockGuard()
  {
    if (0 != (flag_ & PREPARE)) {
      unlock(flag_, lock_.prepare_list_lock_);
    }
    if (0 != (flag_ & ACTIVE)) {
      unlock(flag_, lock_.active_list_lock_);
    }
    if (0 != (flag_ & NEW_CREATE)) {
      unlock(flag_, lock_.new_create_list_lock_);
    }
    if (0 != (flag_ & LS_FROZEN)) {
      unlock(flag_, lock_.ls_frozen_list_lock_);
    }
  }

  static void lock(uint8_t flag, common::SpinRWLock &lock)
  {
    int ret = OB_SUCCESS;
    do {
      if (0 != (flag & 0x80)) {
        ret = lock.wrlock();
      } else {
        ret = lock.rdlock();
      }
      if (OB_SUCCESS != ret) {
        STORAGE_LOG(ERROR, "failed to lock checkpoint lock", KR(ret), K(flag));
        usleep(100);
      }
    } while (OB_SUCCESS != ret);
  }

  static void unlock(uint8_t flag, common::SpinRWLock &lock)
  {
    if (0 != (flag & 0x80)) {
      lock.wrunlock();
    } else {
      lock.rdunlock();
    }
  }

  void click(const char *mod = NULL) { time_guard_.click(mod); }
private:
  ObTimeGuard time_guard_;
  ObDataCheckpoint::ObCheckpointLock &lock_;
  uint8_t flag_;
};

static const ObTabletID LS_DATA_CHECKPOINT_TABLET(ObDataCheckpoint::LS_DATA_CHECKPOINT_TABLET_ID);

}  // namespace checkpoint
}  // namespace storage
}  // namespace oceanbase
#endif
