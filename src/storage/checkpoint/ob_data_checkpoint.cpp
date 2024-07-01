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

#include "storage/checkpoint/ob_data_checkpoint.h"
#include "storage/tx_storage/ob_checkpoint_service.h"
#include "storage/ls/ob_ls.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/ls/ob_freezer.h"

namespace oceanbase
{
using namespace share;
namespace storage
{
namespace checkpoint
{

__thread bool ObDataCheckpoint::is_tenant_freeze_for_flush_ = false;

// ** ObCheckpointDList **
void ObCheckpointDList::reset()
{
  ObCheckpointIterator iterator;
  ObFreezeCheckpoint *ob_freeze_checkpoint = nullptr;
  get_iterator(iterator);
  while (iterator.has_next()) {
    ob_freeze_checkpoint = iterator.get_next();
    if (ob_freeze_checkpoint != checkpoint_list_.remove(ob_freeze_checkpoint)) {
      STORAGE_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "remove ob_freeze_checkpoint failed",
                                    K(*ob_freeze_checkpoint));
    } else {
      ob_freeze_checkpoint->location_ = OUT;
      ob_freeze_checkpoint->data_checkpoint_ = nullptr;
    }
  }
}

bool ObCheckpointDList::is_empty() const
{
  return checkpoint_list_.is_empty();
}

int ObCheckpointDList::unlink(ObFreezeCheckpoint *ob_freeze_checkpoint)
{
  int ret = OB_SUCCESS;
  if (ob_freeze_checkpoint != checkpoint_list_.remove(ob_freeze_checkpoint)) {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

int ObCheckpointDList::insert(ObFreezeCheckpoint *ob_freeze_checkpoint, bool ordered)
{
  int ret = OB_SUCCESS;
  if (ordered) {
    ObFreezeCheckpoint *next = get_first_greater(ob_freeze_checkpoint->get_rec_scn());
    if (!checkpoint_list_.add_before(next, ob_freeze_checkpoint)) {
      STORAGE_LOG(ERROR, "add_before failed");
      ret = OB_ERR_UNEXPECTED;
    }
  } else {
    if (!checkpoint_list_.add_last(ob_freeze_checkpoint)) {
      STORAGE_LOG(ERROR, "add_last failed");
      ret = OB_ERR_UNEXPECTED;
    }
  }
  return ret;
}

ObFreezeCheckpoint *ObCheckpointDList::get_header()
{
  return checkpoint_list_.get_header();
}

void ObCheckpointDList::get_iterator(ObCheckpointIterator &iterator)
{
  iterator.init(this);
}

SCN ObCheckpointDList::get_min_rec_scn_in_list(bool ordered)
{
  SCN min_rec_scn = SCN::max_scn();
  if (!checkpoint_list_.is_empty()) {
    ObFreezeCheckpoint *freeze_checkpoint = nullptr;
    if (ordered) {
      min_rec_scn = checkpoint_list_.get_first()->get_rec_scn();
      freeze_checkpoint = checkpoint_list_.get_first();
    } else {
      auto *header = checkpoint_list_.get_header();
      auto *cur = header->get_next();
      while (cur != header) {
        if (cur->get_rec_scn() < min_rec_scn) {
          min_rec_scn = cur->get_rec_scn();
          freeze_checkpoint = cur;
        }
        cur = cur->get_next();
      }
    }
    if (OB_NOT_NULL(freeze_checkpoint)) {
      STORAGE_LOG(DEBUG, "[CHECKPOINT] get_min_rec_scn_in_list", K(min_rec_scn),
                  K(*freeze_checkpoint));
    }
  }
  return min_rec_scn;
}

ObFreezeCheckpoint *ObCheckpointDList::get_first_greater(const SCN rec_scn)
{
  auto *cur = checkpoint_list_.get_header();
  if (!checkpoint_list_.is_empty()) {
    auto *prev = cur->get_prev();
    while (prev != checkpoint_list_.get_header() && prev->get_rec_scn() > rec_scn) {
      cur = prev;
      prev = cur->get_prev();
    }
  }
  return cur;
}

int ObCheckpointDList::get_need_freeze_checkpoints(const SCN rec_scn,
  ObIArray<ObFreezeCheckpoint*> &freeze_checkpoints)
{
  int ret = OB_SUCCESS;
  ObFreezeCheckpoint *head = checkpoint_list_.get_header();
  ObFreezeCheckpoint *cur = head->get_next();
  while (cur != head && cur->get_rec_scn() <= rec_scn) {
    if (OB_FAIL(freeze_checkpoints.push_back(cur))) {
      STORAGE_LOG(WARN, "push_back into freeze_checkpoints failed");
      break;
    } else {
      cur = cur->get_next();
    }
  }
  return ret;
}

int ObCheckpointDList::get_freezecheckpoint_info(
    ObIArray<checkpoint::ObFreezeCheckpointVTInfo> &freeze_checkpoint_array)
{
  int ret = OB_SUCCESS;
  ObCheckpointIterator iterator;
  this->get_iterator(iterator);
  while (iterator.has_next()) {
    auto ob_freeze_checkpoint = iterator.get_next();
    ObFreezeCheckpointVTInfo info;
    info.tablet_id = ob_freeze_checkpoint->get_tablet_id().id();
    info.rec_scn = ob_freeze_checkpoint->get_rec_scn();
    info.rec_scn_is_stable = ob_freeze_checkpoint->rec_scn_is_stable();
    info.location = ob_freeze_checkpoint->location_;
    freeze_checkpoint_array.push_back(info);
  }
  return ret;
}

// ** ObCheckpointIterator **
void ObCheckpointIterator::init(ObCheckpointDList *dlist)
{
  dlist_ = dlist;
  cur_ = dlist_->get_header();
  next_ = dlist_->get_header()->get_next();
}

ObFreezeCheckpoint *ObCheckpointIterator::get_next()
{
  cur_ = next_;
  next_ = cur_->get_next();
  return cur_;
}

bool ObCheckpointIterator::has_next() const
{
  return next_ != dlist_->get_header();
}

// ** ObDataCheckpoint *
int ObDataCheckpoint::init(ObLS *ls)
{
  ls_ = ls;
  is_inited_ = true;
  return OB_SUCCESS;
}

int ObDataCheckpoint::safe_to_destroy(bool &is_safe_destroy)
{
  int ret = OB_SUCCESS;

  is_safe_destroy = true;
  // avoid start ls_freeze again after waiting ls_freeze finish
  is_inited_ = false;
  // wait until ls_freeze finish
  while(!ls_freeze_finished_) {
    ob_usleep(1000 * 1000);
    if (REACH_TIME_INTERVAL(10 * 1000L * 1000L)) {
      STORAGE_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "ls freeze cost too much time", K(ls_->get_ls_id()));
      ret = OB_ERR_UNEXPECTED;
      break;
    }
  }

  WLOCK(LS_FROZEN | NEW_CREATE | ACTIVE | PREPARE);
  new_create_list_.reset();
  ls_frozen_list_.reset();
  active_list_.reset();
  prepare_list_.reset();

  if (OB_FAIL(ret)) {
    is_safe_destroy = false;
  }

  return ret;
}

SCN ObDataCheckpoint::get_rec_scn()
{
  RLOCK(LS_FROZEN | NEW_CREATE | ACTIVE | PREPARE);
  int ret = OB_SUCCESS;
  SCN min_rec_scn = SCN::max_scn();
  SCN tmp = SCN::max_scn();

  if ((tmp = new_create_list_.get_min_rec_scn_in_list(false)) < min_rec_scn) {
    min_rec_scn = tmp;
  }
  if ((tmp = active_list_.get_min_rec_scn_in_list()) < min_rec_scn) {
    min_rec_scn = tmp;
  }
  if ((tmp = ls_frozen_list_.get_min_rec_scn_in_list()) < min_rec_scn) {
    min_rec_scn = tmp;
  }
  if ((tmp = prepare_list_.get_min_rec_scn_in_list()) < min_rec_scn) {
    min_rec_scn = tmp;
  }
  return min_rec_scn;
}

SCN ObDataCheckpoint::get_active_rec_scn()
{
  RLOCK(NEW_CREATE | ACTIVE);
  int ret = OB_SUCCESS;
  SCN min_active_rec_scn = SCN::max_scn();
  SCN tmp = SCN::max_scn();
  if ((tmp = new_create_list_.get_min_rec_scn_in_list(false)) < min_active_rec_scn) {
    min_active_rec_scn = tmp;
  }
  if ((tmp = active_list_.get_min_rec_scn_in_list()) < min_active_rec_scn) {
    min_active_rec_scn = tmp;
  }
  return min_active_rec_scn;
}

int ObDataCheckpoint::flush(SCN recycle_scn, int64_t trace_id, bool need_freeze)
{
  int ret = OB_SUCCESS;
  if (need_freeze) {
    SCN active_rec_scn = get_active_rec_scn();
    if (active_rec_scn > recycle_scn) {
      STORAGE_LOG(INFO,
                  "skip flush data checkpoint cause active_rec_scn is larger than recycle_scn",
                  K(active_rec_scn),
                  K(recycle_scn));
    } else if (OB_FAIL(freeze_base_on_needs_(trace_id, recycle_scn))) {
      STORAGE_LOG(WARN, "freeze_base_on_needs failed",
                  K(ret), K(ls_->get_ls_id()), K(recycle_scn), K(trace_id));
    }
  } else if (OB_FAIL(traversal_flush_())) {
    STORAGE_LOG(WARN, "traversal_flush failed", K(ret), K(ls_->get_ls_id()));
  }

  return ret;
}

int ObDataCheckpoint::ls_freeze(SCN rec_scn)
{
  int ret = OB_SUCCESS;
  ObCheckPointService *checkpoint_srv = MTL(ObCheckPointService *);
  set_ls_freeze_finished_(false);
  if (OB_FAIL(checkpoint_srv->add_ls_freeze_task(this, rec_scn))) {
    STORAGE_LOG(WARN, "ls_freeze add task failed", K(ret));
    set_ls_freeze_finished_(true);
  }
  return ret;
}

void ObDataCheckpoint::set_ls_freeze_finished_(bool is_finished)
{
  ATOMIC_STORE(&ls_freeze_finished_, is_finished);
}

bool ObDataCheckpoint::ls_freeze_finished()
{
  return ATOMIC_LOAD(&ls_freeze_finished_);
}

ObTabletID ObDataCheckpoint::get_tablet_id() const
{
  return LS_DATA_CHECKPOINT_TABLET;
}

bool ObDataCheckpoint::is_flushing() const
{
  return !ls_freeze_finished_;
}

bool ObDataCheckpoint::is_empty()
{
  RLOCK(LS_FROZEN | NEW_CREATE | ACTIVE | PREPARE);

  return new_create_list_.is_empty() &&
    active_list_.is_empty() &&
    prepare_list_.is_empty() &&
    ls_frozen_list_.is_empty();
}

static inline bool task_reach_time_interval(int64_t i, int64_t &last_time)
{
  bool bret = false;
  int64_t cur_time = common::ObTimeUtility::fast_current_time();
  if (OB_UNLIKELY((i + *(&last_time)) < cur_time)) {
    last_time = cur_time;
    bret = true;
  }
  return bret;
}

void ObDataCheckpoint::print_list_(ObCheckpointDList &list)
{
  ObCheckpointIterator iterator;
  list.get_iterator(iterator);
  while (iterator.has_next()) {
    auto ob_freeze_checkpoint = iterator.get_next();
    STORAGE_LOG_RET(WARN, OB_SUCCESS, "the block obFreezecheckpoint is :", K(*ob_freeze_checkpoint));
  }
}

void ObDataCheckpoint::road_to_flush(SCN rec_scn)
{
  if (OB_UNLIKELY(!is_inited_)) {
    STORAGE_LOG_RET(WARN, OB_NOT_INIT, "ObDataCheckpoint not init", K(is_inited_));
  } else {
    STORAGE_LOG(INFO, "[Freezer] road_to_flush begin", K(ls_->get_ls_id()));
    // used to print log when stay at a cycle for a long time
    int64_t last_time = common::ObTimeUtility::fast_current_time();

    // new_create_list -> ls_frozen_list
    pop_new_create_to_ls_frozen_();
    last_time = common::ObTimeUtility::fast_current_time();
    STORAGE_LOG(INFO, "[Freezer] new_create_list to ls_frozen_list success",
                                                        K(ls_->get_ls_id()));
    // ls_frozen_list -> active_list
    ls_frozen_to_active_(last_time);
    STORAGE_LOG(INFO, "[Freezer] ls_frozen_list to active_list success",
                                                    K(ls_->get_ls_id()));
    // active_list -> ls_frozen_list
    ObFreezeCheckpoint *last = nullptr;
    {
      RLOCK(ACTIVE);
      last = active_list_.get_first_greater(rec_scn);
    }
    pop_active_list_to_ls_frozen_(last);
    STORAGE_LOG(INFO, "[Freezer] active_list to ls_frozen_list success",
                                                    K(ls_->get_ls_id()));
    // add diagnose info
    add_diagnose_info_for_ls_frozen_();

    // ls_frozen_list -> prepare_list
    last_time = common::ObTimeUtility::fast_current_time();
    ls_frozen_to_prepare_(last_time);
    STORAGE_LOG(INFO, "[Freezer] road_to_flush end", K(ls_->get_ls_id()));
  }
  set_ls_freeze_finished_(true);
}

void ObDataCheckpoint::pop_new_create_to_ls_frozen_()
{
  WLOCK(LS_FROZEN | NEW_CREATE);
  ObFreezeCheckpoint *cur = new_create_list_.get_header()->get_next();
  ObFreezeCheckpoint *next = nullptr;
  while (cur != new_create_list_.get_header()) {
    int ret = OB_SUCCESS;
    next = cur->get_next();
    {
      if (OB_FAIL(transfer_(cur, new_create_list_, ls_frozen_list_, LS_FROZEN))) {
        STORAGE_LOG(ERROR, "Transfer To Ls_Frozen Failed", K(ret));
      }
    }
    cur = next;
  }
}

void ObDataCheckpoint::pop_active_list_to_ls_frozen_(ObFreezeCheckpoint *last)
{
  WLOCK(LS_FROZEN | ACTIVE);
  ObFreezeCheckpoint *cur = active_list_.get_header()->get_next();
  ObFreezeCheckpoint *next = nullptr;
  while (cur != last) {
    int ret = OB_SUCCESS;
    next = cur->get_next();
    {
      if (OB_FAIL(transfer_(cur, active_list_, ls_frozen_list_, LS_FROZEN))) {
        STORAGE_LOG(ERROR, "Transfer To Ls_Frozen Failed", K(ret));
      }
    }
    cur = next;
  }
}

void ObDataCheckpoint::ls_frozen_to_active_(int64_t &last_time)
{
  int ret = OB_SUCCESS;
  bool ls_frozen_list_is_empty = false;
  do {
    {
      int64_t read_lock = LSLOCKALL;
      int64_t write_lock = 0;
      ObLSLockGuard lock_ls(ls_, ls_->lock_, read_lock, write_lock);

      if (OB_UNLIKELY(ls_->is_stopped())) {
        ret = OB_NOT_RUNNING;
        STORAGE_LOG(WARN, "ls stopped", K(ret), K_(ls_->ls_meta));
      } else if (OB_UNLIKELY(!(ls_->get_log_handler()->is_replay_enabled()))) {
        ret = OB_NOT_RUNNING;
        STORAGE_LOG(WARN, "log handler not enable replay, should not freeze", K(ret), K_(ls_->ls_meta));
      } else {
        // traversal list once
        WLOCK(LS_FROZEN);
        ObCheckpointIterator iterator;
        ls_frozen_list_.get_iterator(iterator);
        while (iterator.has_next()) {
          int ret = OB_SUCCESS;
          auto ob_freeze_checkpoint = iterator.get_next();
          if (ob_freeze_checkpoint->is_active_checkpoint()) {
            // avoid new active ob_freeze_checkpoint block minor merge
            // push back to new_create_list and wait next freeze
            if (OB_FAIL(transfer_from_ls_frozen_to_new_created_without_src_lock_(ob_freeze_checkpoint))) {
              STORAGE_LOG(
                  WARN, "ob_freeze_checkpoint move to new_created_list failed", K(ret), K(*ob_freeze_checkpoint));
            }
          } else if (ob_freeze_checkpoint->rec_scn_is_stable()) {
            if (OB_FAIL(transfer_from_ls_frozen_to_active_without_src_lock_(ob_freeze_checkpoint))) {
              STORAGE_LOG(WARN, "check can freeze failed", K(ret), K(*ob_freeze_checkpoint));
            }
          } else {
            // wait rec scn stable
          }
        }
        ls_frozen_list_is_empty = ls_frozen_list_.is_empty();
      }

      if (OB_NOT_RUNNING == ret && is_empty()) {
        ls_frozen_list_is_empty = true;
      }
    }

    if (!ls_frozen_list_is_empty) {
      ob_usleep(LOOP_TRAVERSAL_INTERVAL_US);
      if (task_reach_time_interval(3 * 1000 * 1000, last_time)) {
        STORAGE_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "cost too much time in ls_frozen_list_", K(ret), K(ls_->get_ls_id()));
        RLOCK(LS_FROZEN);
        print_list_(ls_frozen_list_);
      }
    } else {
      break;
    }
  } while (true);

  last_time = ObClockGenerator::getClock();
}

void ObDataCheckpoint::ls_frozen_to_prepare_(int64_t &last_time)
{
  int ret = OB_SUCCESS;
  bool ls_frozen_list_is_empty = false;
  do {
    {
      int64_t read_lock = LSLOCKALL;
      int64_t write_lock = 0;
      ObLSLockGuard lock_ls(ls_, ls_->lock_, read_lock, write_lock);

      if (OB_UNLIKELY(ls_->is_stopped())) {
        ret = OB_NOT_RUNNING;
        STORAGE_LOG(WARN, "ls stopped", K(ret), K_(ls_->ls_meta));
      } else if (OB_UNLIKELY(!(ls_->get_log_handler()->is_replay_enabled()))) {
        ret = OB_NOT_RUNNING;
        STORAGE_LOG(WARN, "log handler not enable replay, should not freeze", K(ret), K_(ls_->ls_meta));
      } else {
        // traversal list once
        WLOCK(LS_FROZEN);
        ObCheckpointIterator iterator;
        ls_frozen_list_.get_iterator(iterator);
        while (iterator.has_next()) {
          int tmp_ret = OB_SUCCESS;
          ObFreezeCheckpoint *ob_freeze_checkpoint = iterator.get_next();
          if (ob_freeze_checkpoint->ready_for_flush()) {
            if (OB_FAIL(ob_freeze_checkpoint->finish_freeze())) {
              STORAGE_LOG(WARN, "finish freeze failed", K(ret));
            }
          } else if (ob_freeze_checkpoint->is_active_checkpoint()) {
            // avoid active ob_freeze_checkpoint block minor merge
            // push back to active_list and wait next freeze
            if(OB_SUCCESS != (tmp_ret = (transfer_from_ls_frozen_to_active_without_src_lock_(ob_freeze_checkpoint)))) {
              STORAGE_LOG(WARN, "active ob_freeze_checkpoint move to active_list failed",
                          K(tmp_ret), K(*ob_freeze_checkpoint));
            }
          }
        }
        ls_frozen_list_is_empty = ls_frozen_list_.is_empty();
      }

      if (OB_NOT_RUNNING == ret && is_empty()) {
        ls_frozen_list_is_empty = true;
      }
    }

    if (!ls_frozen_list_is_empty) {
      ob_usleep(LOOP_TRAVERSAL_INTERVAL_US);
      if (task_reach_time_interval(3 * 1000 * 1000, last_time)) {
        STORAGE_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "cost too much time in ls_frozen_list_", K(ls_->get_ls_id()));
        RLOCK(LS_FROZEN);
        print_list_(ls_frozen_list_);
      }
    } else {
      break;
    }
    if (OB_EAGAIN == ret) {
      ob_usleep(100000);
    }
  } while (true);

  last_time = common::ObTimeUtility::fast_current_time();
}

int ObDataCheckpoint::check_can_move_to_active_in_newcreate()
{
  int ret = OB_SUCCESS;
  WLOCK(NEW_CREATE);
  if (!ls_freeze_finished_) {
    STORAGE_LOG(INFO, "skip check_can_move when ls freeze");
  } else {
    ObCheckpointIterator iterator;
    new_create_list_.get_iterator(iterator);
    while (iterator.has_next()) {
      auto ob_freeze_checkpoint = iterator.get_next();
      if (ob_freeze_checkpoint->rec_scn_is_stable()
          && OB_FAIL(transfer_from_new_create_to_active_without_src_lock_(ob_freeze_checkpoint))) {
        STORAGE_LOG(WARN, "check can freeze failed", K(ret));
        break;
      }
    }
  }

  return ret;
}

int ObDataCheckpoint::add_to_new_create(ObFreezeCheckpoint *ob_freeze_checkpoint)
{
  WLOCK(NEW_CREATE);
  int ret = OB_SUCCESS;

  if (OB_FAIL(insert_(ob_freeze_checkpoint, new_create_list_, false))) {
    STORAGE_LOG(ERROR, "Add To Active Failed");
  } else if (OB_FAIL(decide_freeze_clock_(ob_freeze_checkpoint))) {
    STORAGE_LOG(WARN, "fail to decide freeze_clock", K(ret));
  } else {
    ob_freeze_checkpoint->location_ = NEW_CREATE;
  }

  return ret;
}

int ObDataCheckpoint::decide_freeze_clock_(ObFreezeCheckpoint *ob_freeze_checkpoint)
{
  int ret = OB_SUCCESS;
  ObFreezer *freezer = nullptr;
  ObITabletMemtable *tablet_memtable = nullptr;

  if (OB_ISNULL(ls_) || OB_ISNULL(ob_freeze_checkpoint)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ls or ob_freeze_checkpoint cannot be null", K(ret), K(ls_), K(ob_freeze_checkpoint));
  } else if (FALSE_IT(freezer = ls_->get_freezer())) {
  } else if (OB_ISNULL(freezer)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "freezer cannot be null", K(ret));
  } else if (FALSE_IT(tablet_memtable = static_cast<ObITabletMemtable *>(ob_freeze_checkpoint))) {
  } else {
    // freeze_snapshot_version requires that two memtables of a tablet
    // cannot join in the same logstream_freeze task
    // otherwise freeze_snapshot_version of the old memtable will be too large
    (void)tablet_memtable->set_freeze_clock(freezer->get_freeze_clock());
  }

  return ret;
}

int ObDataCheckpoint::get_freezecheckpoint_info(
  ObIArray<checkpoint::ObFreezeCheckpointVTInfo> &freeze_checkpoint_array)
{
  int ret = OB_SUCCESS;
  freeze_checkpoint_array.reset();
  RLOCK(LS_FROZEN | NEW_CREATE | ACTIVE | PREPARE);
  if (OB_FAIL(new_create_list_.get_freezecheckpoint_info(
    freeze_checkpoint_array))) {
    STORAGE_LOG(ERROR, "iterator new_create_list fail",
      K(ls_->get_ls_id()));
  } else if (OB_FAIL(active_list_.get_freezecheckpoint_info(
    freeze_checkpoint_array))) {
    STORAGE_LOG(ERROR, "iterator active_list_list fail",
      K(ls_->get_ls_id()));
  } else if (OB_FAIL(prepare_list_.get_freezecheckpoint_info(
    freeze_checkpoint_array))) {
    STORAGE_LOG(ERROR, "iterator prepare_list_list fail",
      K(ls_->get_ls_id()));
  } else if (OB_FAIL(ls_frozen_list_.get_freezecheckpoint_info(
    freeze_checkpoint_array))) {
    STORAGE_LOG(ERROR, "iterator ls_frozen_list_list fail",
      K(ls_->get_ls_id()));
  }

  return ret;
}

int ObDataCheckpoint::traversal_flush_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  // Because prepare list is ordered based on rec_scn and we want to flush
  // based on the order of rec_scn. So we should can simply use a small
  // number for flush tasks.
  const int MAX_DATA_CHECKPOINT_FLUSH_COUNT = 10000;
  ObSEArray<ObTableHandleV2, BASIC_MEMSTORE_CNT> flush_tasks;

  {
    RLOCK(PREPARE);
    if (prepare_list_.is_empty()) {
      STORAGE_LOG(TRACE, "skip traversal_flush", K(ls_freeze_finished_),
                  K(prepare_list_.is_empty()), K(ls_->get_ls_id()));
    } else {
      ObCheckpointIterator iterator;
      prepare_list_.get_iterator(iterator);
      flush_tasks.reset();
      ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);

      while (OB_SUCC(ret)
             && iterator.has_next()
             && MAX_DATA_CHECKPOINT_FLUSH_COUNT >= flush_tasks.count()) {
        ObFreezeCheckpoint *ob_freeze_checkpoint = iterator.get_next();
        ObITabletMemtable *tablet_memtable = static_cast<ObITabletMemtable *>(ob_freeze_checkpoint);
        ObTableHandleV2 handle;
        if (OB_FAIL(handle.set_table(tablet_memtable, t3m, tablet_memtable->get_table_type()))) {
          STORAGE_LOG(WARN, "set table handle fail", K(ret), KPC(tablet_memtable));
        } else if (!tablet_memtable->get_is_flushed() && OB_FAIL(flush_tasks.push_back(handle))) {
          TRANS_LOG(WARN, "add table to flush tasks failed", KPC(tablet_memtable));
        }
      }
    }
  }

  // QUESTION1: IF ALWAYS FAILED, we will always schedule the first 10000
  if (0 < flush_tasks.count()) {
    for (int64_t i = 0; OB_SIZE_OVERFLOW != tmp_ret && i < flush_tasks.count(); i++) {
      ObITable *table = flush_tasks[i].get_table();
      ObITabletMemtable *tablet_memtable = static_cast<ObITabletMemtable *>(table);
      // Even if flush failed, we can continue to flush the next one except OB_SIZE_OVERFLOW
      if (OB_TMP_FAIL(tablet_memtable->flush(ls_->get_ls_id()))
          && tmp_ret != OB_NO_NEED_UPDATE) {
        STORAGE_LOG(WARN, "memtable flush failed", K(tmp_ret), K(ls_->get_ls_id()));
      }
    }

    STORAGE_LOG(INFO, "traversal_flush successfully", K(ls_->get_ls_id()), K(flush_tasks));
  }

  return ret;
}

int ObDataCheckpoint::unlink_(ObFreezeCheckpoint *ob_freeze_checkpoint)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ob_freeze_checkpoint)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "ob_freeze_checkpoint is NULL");
  } else {
    do {
      ObCheckpointDList *list = NULL;
      const ObFreezeCheckpointLocation location = ob_freeze_checkpoint->location_;
      if (OUT == location) {
        // has unlinked
      } else {
        WLOCK(location);
        // double check
        if (ob_freeze_checkpoint->location_ == location) {
          if (FALSE_IT(list = get_checkpoint_list(location))) {
          } else if (OB_ISNULL(list)) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(ERROR, "list is NULL", K(ret), KPC(ob_freeze_checkpoint));
          } else if (OB_FAIL(list->unlink(ob_freeze_checkpoint))) {
            STORAGE_LOG(ERROR, "failed to unlink from list", K(ret), KPC(ob_freeze_checkpoint));
          }
        } else {
          ret = OB_EAGAIN;
          STORAGE_LOG(WARN, "ob_freeze_checkpoint is not in list", KR(ret), K(location), KPC(ob_freeze_checkpoint));
        }

      }
    } while (OB_EAGAIN == ret);
  }
  return ret;
}

int ObDataCheckpoint::finish_freeze(ObFreezeCheckpoint *ob_freeze_checkpoint)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ob_freeze_checkpoint)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "ob_freeze_checkpoint is NULL");
  } else {
    do {
      switch (ob_freeze_checkpoint->location_) {
        case NEW_CREATE:
          if (OB_FAIL(transfer_from_new_create_to_prepare_(ob_freeze_checkpoint))) {
            if (OB_EAGAIN != ret) {
              STORAGE_LOG(ERROR, "failed to transfer_from_new_create_to_prepare", K(ret), KPC(ob_freeze_checkpoint));
            }
          }
          break;
        case ACTIVE:
          if (OB_FAIL(transfer_from_active_to_prepare_(ob_freeze_checkpoint))) {
            if (OB_EAGAIN != ret) {
              STORAGE_LOG(ERROR, "failed to transfer_from_active_to_prepare", K(ret), KPC(ob_freeze_checkpoint));
            }
          }
          break;
        case PREPARE:
        case OUT:
          break;
        case LS_FROZEN:
          if (OB_FAIL(transfer_from_ls_frozen_to_prepare_without_src_lock_(ob_freeze_checkpoint))) {
            if (OB_EAGAIN != ret) {
              STORAGE_LOG(ERROR, "failed to transfer_from_ls_frozen_to_prepare", K(ret));
            }
          }
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "Unknown ObFreezeCheckpoint State", KPC(ob_freeze_checkpoint));
          break;
      }
    } while (OB_EAGAIN == ret);
  }
  return ret;
}

int ObDataCheckpoint::insert_(ObFreezeCheckpoint *ob_freeze_checkpoint,
                              ObCheckpointDList &dst,
                              bool ordered)
{
  return dst.insert(ob_freeze_checkpoint, ordered);
}

int ObDataCheckpoint::transfer_(ObFreezeCheckpoint *ob_freeze_checkpoint,
                                ObCheckpointDList &src,
                                ObCheckpointDList &dst,
                                ObFreezeCheckpointLocation location)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(src.unlink(ob_freeze_checkpoint))) {
    STORAGE_LOG(ERROR, "Unlink From Dlist Failed");
  } else if (OB_FAIL(dst.insert(ob_freeze_checkpoint))) {
    STORAGE_LOG(ERROR, "Insert Into Dlist Failed");
  } else {
    ob_freeze_checkpoint->location_ = location;
  }
  return ret;
}

int ObDataCheckpoint::transfer_from_new_create_to_active_without_src_lock_(ObFreezeCheckpoint *ob_freeze_checkpoint)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ob_freeze_checkpoint)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "ob_freeze_checkpoint is NULL");
  } else if (NEW_CREATE != ob_freeze_checkpoint->location_) {
    ret = OB_EAGAIN;
    STORAGE_LOG(WARN, "ob_freeze_checkpoint is not in new_create", KR(ret), KPC(ob_freeze_checkpoint));
  } else {
    WLOCK(ACTIVE);
    if (OB_FAIL(transfer_(ob_freeze_checkpoint, new_create_list_, active_list_, ACTIVE))) {
      STORAGE_LOG(ERROR, "Transfer From NewCreate To Active Failed");
    }
  }
  return ret;
}

int ObDataCheckpoint::transfer_from_new_create_to_prepare_(ObFreezeCheckpoint *ob_freeze_checkpoint)
{
  int ret = OB_SUCCESS;
  WLOCK(NEW_CREATE);
  if (OB_ISNULL(ob_freeze_checkpoint)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "ob_freeze_checkpoint is NULL");
  } else if (NEW_CREATE != ob_freeze_checkpoint->location_) {
    ret = OB_EAGAIN;
    STORAGE_LOG(WARN, "ob_freeze_checkpoint is not in new_create", KR(ret), KPC(ob_freeze_checkpoint));
  } else {
    WLOCK(PREPARE);
    if (OB_FAIL(transfer_(ob_freeze_checkpoint, new_create_list_, prepare_list_, PREPARE))) {
      STORAGE_LOG(ERROR, "Transfer From NewCreate To Prepare Failed");
    }
  }
  return ret;
}

int ObDataCheckpoint::transfer_from_active_to_prepare_(ObFreezeCheckpoint *ob_freeze_checkpoint)
{
  int ret = OB_SUCCESS;
  WLOCK(ACTIVE);
  if (OB_ISNULL(ob_freeze_checkpoint)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "ob_freeze_checkpoint is NULL");
  } else if (ACTIVE != ob_freeze_checkpoint->location_) {
    ret = OB_EAGAIN;
    STORAGE_LOG(WARN, "ob_freeze_checkpoint is not in active", KR(ret), KPC(ob_freeze_checkpoint));
  } else {
    WLOCK(PREPARE);
    if (OB_FAIL(transfer_(ob_freeze_checkpoint, active_list_, prepare_list_, PREPARE))) {
      STORAGE_LOG(ERROR, "Transfer From Active To Frozen Failed");
    }
  }
  return ret;
}

int ObDataCheckpoint::transfer_from_ls_frozen_to_active_without_src_lock_(ObFreezeCheckpoint *ob_freeze_checkpoint)
{
  int ret = OB_SUCCESS;
  WLOCK(ACTIVE);
  if (OB_FAIL(transfer_(ob_freeze_checkpoint, ls_frozen_list_, active_list_, ACTIVE))) {
    STORAGE_LOG(ERROR, "Transfer From Active To Frozen Failed");
  }
  return ret;
}

int ObDataCheckpoint::transfer_from_ls_frozen_to_new_created_without_src_lock_(ObFreezeCheckpoint *ob_freeze_checkpoint)
{
  int ret = OB_SUCCESS;
  WLOCK(NEW_CREATE);
  if (OB_FAIL(transfer_(ob_freeze_checkpoint, ls_frozen_list_, new_create_list_, NEW_CREATE))) {
    STORAGE_LOG(ERROR, "Transfer From LS Frozen To New_Created Failed");
  }
  return ret;
}

int ObDataCheckpoint::transfer_from_ls_frozen_to_prepare_without_src_lock_(ObFreezeCheckpoint *ob_freeze_checkpoint)
{
  int ret = OB_SUCCESS;
  WLOCK(PREPARE);
  if (OB_FAIL(transfer_(ob_freeze_checkpoint, ls_frozen_list_, prepare_list_, PREPARE))) {
    STORAGE_LOG(ERROR, "Transfer From LS Frozen To Prepare Failed");
  }
  return ret;
}

int ObDataCheckpoint::get_need_flush_tablets_(const share::SCN recycle_scn,
                                              ObIArray<ObTabletID> &flush_tablets)
{
  int ret = OB_SUCCESS;
  RLOCK(NEW_CREATE | ACTIVE | PREPARE);
  ObSArray<ObFreezeCheckpoint*> need_freeze_checkpoints;
  if (OB_FAIL(new_create_list_.get_need_freeze_checkpoints(
    recycle_scn, need_freeze_checkpoints))) {
    STORAGE_LOG(WARN, "get_need_freeze_checkpoints failed", K(ret));
  } else if (OB_FAIL(active_list_.get_need_freeze_checkpoints(
    recycle_scn, need_freeze_checkpoints))) {
    STORAGE_LOG(WARN, "get_need_freeze_checkpoints failed", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < need_freeze_checkpoints.count(); i++) {
      if (OB_FAIL(flush_tablets.push_back(
        need_freeze_checkpoints[i]->get_tablet_id()))) {
        STORAGE_LOG(WARN, "get_flush_tablets failed", K(ret));
      }
    }
  }

  return ret;
}

int ObDataCheckpoint::freeze_base_on_needs_(const int64_t trace_id,
    share::SCN recycle_scn)
{
  int ret = OB_SUCCESS;
  if (get_rec_scn() <= recycle_scn) {
    if (is_tenant_freeze() || !is_flushing()) {
      int64_t wait_flush_num =
        new_create_list_.checkpoint_list_.get_size()
        + active_list_.checkpoint_list_.get_size();
      bool logstream_freeze = true;
      ObSArray<ObTabletID> need_flush_tablets;
      if (wait_flush_num > MAX_FREEZE_CHECKPOINT_NUM) {
        if (OB_FAIL(get_need_flush_tablets_(recycle_scn, need_flush_tablets))) {
          // do nothing
        } else {
          int need_flush_num = need_flush_tablets.count();
          logstream_freeze =
            need_flush_num * 100 / wait_flush_num > TABLET_FREEZE_PERCENT;
        }
      }

      const bool is_sync = false;
      const bool abs_timeout_ts = 0;  // async freeze do not need
      if (OB_FAIL(ret)) {
      } else if (logstream_freeze) {
        if (OB_FAIL(ls_->logstream_freeze(trace_id, is_sync, abs_timeout_ts))) {
          STORAGE_LOG(WARN, "minor freeze failed", K(ret), K(ls_->get_ls_id()));
        }
      } else if (OB_FAIL(ls_->tablet_freeze(trace_id, need_flush_tablets, is_sync))) {
        STORAGE_LOG(WARN, "batch tablet freeze failed", K(ret), K(ls_->get_ls_id()), K(need_flush_tablets));
      }
    }
  }
  return ret;
}

void ObDataCheckpoint::add_diagnose_info_for_ls_frozen_()
{
  ObCheckpointIterator iterator;
  RLOCK(LS_FROZEN);
  ls_frozen_list_.get_iterator(iterator);
  while (iterator.has_next()) {
    ObITabletMemtable *tablet_memtable = static_cast<ObITabletMemtable *>(iterator.get_next());
    if (tablet_memtable->is_data_memtable()) {
      if (!tablet_memtable->is_active_checkpoint()) {
        (static_cast<memtable::ObMemtable *>(tablet_memtable))
            ->report_memtable_diagnose_info(memtable::ObMemtable::AddCheckpointDiagnoseInfoForMemtable());
      }
    }
  }
}

}  // namespace checkpoint
}  // namespace storage
}  // namespace oceanbase
