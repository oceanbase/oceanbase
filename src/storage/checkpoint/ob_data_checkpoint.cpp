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

bool ObCheckpointDList::is_empty()
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
  while(is_flushing()) {
    ob_usleep(1000 * 1000);
    if (REACH_TIME_INTERVAL(10 * 1000L * 1000L)) {
      STORAGE_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "ls freeze cost too much time", K(ls_->get_ls_id()));
      ret = OB_ERR_UNEXPECTED;
      break;
    }
  }

  ObSpinLockGuard ls_frozen_list_guard(ls_frozen_list_lock_);
  ObSpinLockGuard guard(lock_);
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
  ObSpinLockGuard ls_frozen_list_guard(ls_frozen_list_lock_);
  ObSpinLockGuard guard(lock_);
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

int ObDataCheckpoint::flush(SCN recycle_scn, bool need_freeze)
{
  int ret = OB_SUCCESS;
  if (need_freeze) {
    if (OB_FAIL(freeze_base_on_needs_(recycle_scn))) {
      STORAGE_LOG(WARN, "freeze_base_on_needs failed",
                  K(ret), K(ls_->get_ls_id()), K(recycle_scn));
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
  ObSpinLockGuard guard(lock_);
  ls_freeze_finished_ = is_finished;
}

bool ObDataCheckpoint::ls_freeze_finished()
{
  ObSpinLockGuard guard(lock_);
  return ls_freeze_finished_;
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
  ObSpinLockGuard ls_frozen_list_guard(ls_frozen_list_lock_);
  ObSpinLockGuard guard(lock_);

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
    pop_range_to_ls_frozen_(new_create_list_.get_header(), new_create_list_);
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
      ObSpinLockGuard guard(lock_);
      last = active_list_.get_first_greater(rec_scn);
    }
    pop_range_to_ls_frozen_(last, active_list_);
    last_time = common::ObTimeUtility::fast_current_time();
    STORAGE_LOG(INFO, "[Freezer] active_list to ls_frozen_list success",
                                                    K(ls_->get_ls_id()));
    // ls_frozen_list -> prepare_list
    ls_frozen_to_prepare_(last_time);
    STORAGE_LOG(INFO, "[Freezer] road_to_flush end", K(ls_->get_ls_id()));
  }
  set_ls_freeze_finished_(true);
}

void ObDataCheckpoint::pop_range_to_ls_frozen_(ObFreezeCheckpoint *last, ObCheckpointDList &list)
{
  ObSpinLockGuard guard(lock_);
  ObFreezeCheckpoint *cur = list.get_header()->get_next();
  ObFreezeCheckpoint *next = nullptr;
  while (cur != last) {
    int ret = OB_SUCCESS;
    next = cur->get_next();
    {
      if (OB_FAIL(transfer_(cur, list, ls_frozen_list_, LS_FROZEN))) {
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
      int64_t read_lock = LSLOCKALL - LSLOCKLOGMETA;
      int64_t write_lock = 0;
      ObLSLockGuard lock_ls(ls_->lock_, read_lock, write_lock);

      if (OB_UNLIKELY(ls_->is_stopped_)) {
        ret = OB_NOT_RUNNING;
        STORAGE_LOG(WARN, "ls stopped", K(ret), K_(ls_->ls_meta));
      } else if (OB_UNLIKELY(!(ls_->get_log_handler()->is_replay_enabled()))) {
        ret = OB_NOT_RUNNING;
        STORAGE_LOG(WARN, "log handler not enable replay, should not freeze", K(ret), K_(ls_->ls_meta));
      } else {
        // traversal list once
        ObSpinLockGuard ls_frozen_list_guard(ls_frozen_list_lock_);
        ObCheckpointIterator iterator;
        ls_frozen_list_.get_iterator(iterator);
        while (iterator.has_next()) {
          int ret = OB_SUCCESS;
          auto ob_freeze_checkpoint = iterator.get_next();
          if (ob_freeze_checkpoint->is_active_checkpoint()) {
            ObSpinLockGuard guard(lock_);
            // avoid new active ob_freeze_checkpoint block minor merge
            // push back to new_create_list and wait next freeze
            if(OB_FAIL(transfer_from_ls_frozen_to_new_created_(ob_freeze_checkpoint))) {
              STORAGE_LOG(WARN, "ob_freeze_checkpoint move to new_created_list failed",
                          K(ret), K(*ob_freeze_checkpoint));
            }
          } else {
            ObSpinLockGuard guard(lock_);
            if (OB_FAIL(ob_freeze_checkpoint->check_can_move_to_active(true))) {
              STORAGE_LOG(WARN, "check can freeze failed", K(ret), K(*ob_freeze_checkpoint));
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
        STORAGE_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "cost too much time in ls_frozen_list_", K(ret), K(ls_->get_ls_id()));
        ObSpinLockGuard ls_frozen_list_guard(ls_frozen_list_lock_);
        print_list_(ls_frozen_list_);
      }
    } else {
      break;
    }
  } while (true);

  last_time = common::ObTimeUtility::fast_current_time();
}

void ObDataCheckpoint::ls_frozen_to_prepare_(int64_t &last_time)
{
  int ret = OB_SUCCESS;
  bool ls_frozen_list_is_empty = false;
  do {
    {
      int64_t read_lock = LSLOCKALL - LSLOCKLOGMETA;
      int64_t write_lock = 0;
      ObLSLockGuard lock_ls(ls_->lock_, read_lock, write_lock);

      if (OB_UNLIKELY(ls_->is_stopped_)) {
        ret = OB_NOT_RUNNING;
        STORAGE_LOG(WARN, "ls stopped", K(ret), K_(ls_->ls_meta));
      } else if (OB_UNLIKELY(!(ls_->get_log_handler()->is_replay_enabled()))) {
        ret = OB_NOT_RUNNING;
        STORAGE_LOG(WARN, "log handler not enable replay, should not freeze", K(ret), K_(ls_->ls_meta));
      } else {
        // traversal list once
        ObSpinLockGuard ls_frozen_list_guard(ls_frozen_list_lock_);
        ObCheckpointIterator iterator;
        ls_frozen_list_.get_iterator(iterator);
        while (iterator.has_next()) {
          int tmp_ret = OB_SUCCESS;
          auto ob_freeze_checkpoint = iterator.get_next();
          if (ob_freeze_checkpoint->ready_for_flush()) {
            if (OB_FAIL(ob_freeze_checkpoint->finish_freeze())) {
              STORAGE_LOG(WARN, "finish freeze failed", K(ret));
            }
          } else if (ob_freeze_checkpoint->is_active_checkpoint()) {
            // avoid active ob_freeze_checkpoint block minor merge
            // push back to active_list and wait next freeze
            ObSpinLockGuard guard(lock_);
            if(OB_SUCCESS != (tmp_ret = (transfer_from_ls_frozen_to_active_(ob_freeze_checkpoint)))) {
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
        ObSpinLockGuard ls_frozen_list_guard(ls_frozen_list_lock_);
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
  ObSpinLockGuard guard(lock_);
  if (!ls_freeze_finished_) {
    STORAGE_LOG(INFO, "skip check_can_move when ls freeze");
  } else {
    ObCheckpointIterator iterator;
    new_create_list_.get_iterator(iterator);
    while (iterator.has_next()) {
      auto ob_freeze_checkpoint = iterator.get_next();
      if (OB_FAIL(ob_freeze_checkpoint->check_can_move_to_active())) {
        STORAGE_LOG(WARN, "check can freeze failed", K(ret));
        break;
      }
    }
  }

  return ret;
}

int ObDataCheckpoint::add_to_new_create(ObFreezeCheckpoint *ob_freeze_checkpoint)
{
  ObSpinLockGuard guard(lock_);
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
  memtable::ObMemtable *memtable = nullptr;

  if (OB_ISNULL(ls_) || OB_ISNULL(ob_freeze_checkpoint)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ls or ob_freeze_checkpoint cannot be null", K(ret), K(ls_), K(ob_freeze_checkpoint));
  } else if (FALSE_IT(freezer = ls_->get_freezer())) {
  } else if (OB_ISNULL(freezer)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "freezer cannot be null", K(ret));
  } else if (FALSE_IT(memtable = static_cast<memtable::ObMemtable *>(ob_freeze_checkpoint))) {
  } else {
    // freeze_snapshot_version requires that two memtables of a tablet
    // cannot join in the same logstream_freeze task
    // otherwise freeze_snapshot_version of the old memtable will be too large
    (void)memtable->set_freeze_clock(freezer->get_freeze_clock());
  }

  return ret;
}

int ObDataCheckpoint::unlink_from_prepare(ObFreezeCheckpoint *ob_freeze_checkpoint)
{
  ObSpinLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  // for the node not in prepare_list
  // return OB_SUCCESS
  // the node will remove from list in the reset()
  if (ob_freeze_checkpoint->location_ == PREPARE) {
    if (OB_FAIL(unlink_(ob_freeze_checkpoint, prepare_list_))) {
      STORAGE_LOG(ERROR, "Unlink From Prepare Failed");
    } else {
      ob_freeze_checkpoint->location_ = OUT;
    }
  }
  return ret;
}

int ObDataCheckpoint::get_freezecheckpoint_info(
  ObIArray<checkpoint::ObFreezeCheckpointVTInfo> &freeze_checkpoint_array)
{
  int ret = OB_SUCCESS;
  freeze_checkpoint_array.reset();
  ObSpinLockGuard ls_frozen_list_guard(ls_frozen_list_lock_);
  ObSpinLockGuard guard(lock_);
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
  ObSEArray<ObTableHandleV2, 64> flush_tasks;

  {
    ObSpinLockGuard guard(lock_);
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
        memtable::ObMemtable *memtable = static_cast<memtable::ObMemtable *>(ob_freeze_checkpoint);
        ObTableHandleV2 handle(memtable, t3m, ObITable::TableType::DATA_MEMTABLE);
        if (!memtable->get_is_flushed()
            && OB_FAIL(flush_tasks.push_back(handle))) {
          TRANS_LOG(WARN, "add table to flush tasks failed", KPC(memtable));
        }
      }
    }
  }

  // QUESTION1: IF ALWAYS FAILED, we will always schedule the first 10000
  if (0 < flush_tasks.count()) {
    for (int64_t i = 0; OB_SIZE_OVERFLOW != tmp_ret && i < flush_tasks.count(); i++) {
      ObITable *table = flush_tasks[i].get_table();
      memtable::ObMemtable *memtable = static_cast<memtable::ObMemtable *>(table);
      // Even if flush failed, we can continue to flush the next one except OB_SIZE_OVERFLOW
      if (OB_TMP_FAIL(memtable->flush(ls_->get_ls_id()))
          && tmp_ret != OB_NO_NEED_UPDATE) {
        STORAGE_LOG(WARN, "memtable flush failed", K(tmp_ret), K(ls_->get_ls_id()));
      }
    }

    STORAGE_LOG(INFO, "traversal_flush successfully", K(ls_->get_ls_id()), K(flush_tasks));
  }

  return ret;
}

int ObDataCheckpoint::unlink_(ObFreezeCheckpoint *ob_freeze_checkpoint, ObCheckpointDList &src)
{
  return src.unlink(ob_freeze_checkpoint);
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
  if (OB_FAIL(unlink_(ob_freeze_checkpoint, src))) {
    STORAGE_LOG(ERROR, "Unlink From Dlist Failed");
  } else if (OB_FAIL(insert_(ob_freeze_checkpoint, dst))) {
    STORAGE_LOG(ERROR, "Insert Into Dlist Failed");
  } else {
    ob_freeze_checkpoint->location_ = location;
  }
  return ret;
}

int ObDataCheckpoint::transfer_from_new_create_to_active_(ObFreezeCheckpoint *ob_freeze_checkpoint)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(transfer_(ob_freeze_checkpoint, new_create_list_, active_list_, ACTIVE))) {
    STORAGE_LOG(ERROR, "Transfer From NewCreate To Active Failed");
  }
  return ret;
}

int ObDataCheckpoint::transfer_from_new_create_to_prepare_(ObFreezeCheckpoint *ob_freeze_checkpoint)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(transfer_(ob_freeze_checkpoint, new_create_list_, prepare_list_, PREPARE))) {
    STORAGE_LOG(ERROR, "Transfer From NewCreate To Prepare Failed");
  }
  return ret;
}

int ObDataCheckpoint::transfer_from_ls_frozen_to_active_(ObFreezeCheckpoint *ob_freeze_checkpoint)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(transfer_(ob_freeze_checkpoint, ls_frozen_list_, active_list_, ACTIVE))) {
    STORAGE_LOG(ERROR, "Transfer From Active To Frozen Failed");
  }
  return ret;
}

int ObDataCheckpoint::transfer_from_ls_frozen_to_new_created_(ObFreezeCheckpoint *ob_freeze_checkpoint)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(transfer_(ob_freeze_checkpoint, ls_frozen_list_, new_create_list_, NEW_CREATE))) {
    STORAGE_LOG(ERROR, "Transfer From LS Frozen To New_Created Failed");
  }
  return ret;
}

int ObDataCheckpoint::transfer_from_ls_frozen_to_prepare_(ObFreezeCheckpoint *ob_freeze_checkpoint)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(transfer_(ob_freeze_checkpoint, ls_frozen_list_, prepare_list_, PREPARE))) {
    STORAGE_LOG(ERROR, "Transfer From LS Frozen To Prepare Failed");
  }
  return ret;
}

int ObDataCheckpoint::transfer_from_active_to_prepare_(ObFreezeCheckpoint *ob_freeze_checkpoint)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(transfer_(ob_freeze_checkpoint, active_list_, prepare_list_, PREPARE))) {
    STORAGE_LOG(ERROR, "Transfer From Active To Frozen Failed");
  }
  return ret;
}

int ObDataCheckpoint::get_need_flush_tablets_(const share::SCN recycle_scn,
                                              ObIArray<ObTabletID> &flush_tablets)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
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

int ObDataCheckpoint::freeze_base_on_needs_(share::SCN recycle_scn)
{
  int ret = OB_SUCCESS;
  if (get_rec_scn() <= recycle_scn) {
    if (!is_flushing() && prepare_list_.is_empty()) {
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

      if (logstream_freeze) {
        if (OB_FAIL(ls_->logstream_freeze())) {
          STORAGE_LOG(WARN, "minor freeze failed", K(ret), K(ls_->get_ls_id()));
        }
      } else {
        for (int i = 0; OB_SUCC(ret) && i < need_flush_tablets.count(); i++) {
          if (OB_FAIL(ls_->tablet_freeze(need_flush_tablets[i]))) {
            STORAGE_LOG(WARN, "tablet freeze failed",
                        K(ret), K(ls_->get_ls_id()), K(need_flush_tablets[i]));
          }
        }
      }
    }
  }
  return ret;
}

}  // namespace checkpoint
}  // namespace storage
}  // namespace oceanbase
