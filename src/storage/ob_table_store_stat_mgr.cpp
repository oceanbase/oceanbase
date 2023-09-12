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
#include "ob_table_store_stat_mgr.h"
#include "lib/ob_errno.h"
#include "lib/allocator/ob_mod_define.h"
#include "share/ob_thread_mgr.h"

namespace oceanbase
{
using namespace common;
namespace storage
{
// ------------------ Statistic ------------------ //
bool ObMergeIterStat::is_valid() const
{
  return call_cnt_ >= 0 && output_row_cnt_ >= 0;
}

int ObMergeIterStat::add(const ObMergeIterStat& other)
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("self is invalid", K(ret), K(*this));
  } else if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("other is invalid", K(ret), K(other));
  } else {
    call_cnt_ += other.call_cnt_;
    output_row_cnt_ += other.output_row_cnt_;
  }
  return ret;
}

ObMergeIterStat & ObMergeIterStat::operator=(const ObMergeIterStat &other)
{
  if (this != &other) {
    MEMCPY(this, &other, sizeof(ObMergeIterStat));
  }
  return *this;
}

bool ObBlockAccessStat::is_valid() const
{
  return effect_read_cnt_ >= 0 && empty_read_cnt_ >= 0;
}

int ObBlockAccessStat::add(const ObBlockAccessStat& other)
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("self is invalid", K(ret), K(*this));
  } else if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("other is invalid", K(ret), K(other));
  } else {
    effect_read_cnt_ += other.effect_read_cnt_;
    empty_read_cnt_ += other.empty_read_cnt_;
  }
  return ret;
}

ObBlockAccessStat & ObBlockAccessStat::operator=(const ObBlockAccessStat &other)
{
  if (this != &other) {
    MEMCPY(this, &other, sizeof(ObBlockAccessStat));
  }
  return *this;
}

ObTableStoreStat::ObTableStoreStat()
{
  reset();
}

void ObTableStoreStat::reset()
{
  MEMSET(this, 0, sizeof(ObTableStoreStat));
}

void ObTableStoreStat::reuse()
{
  share::ObLSID ls_id = ls_id_;
  common::ObTabletID tablet_id = tablet_id_;
  common::ObTableID table_id = table_id_;
  MEMSET(this, 0, sizeof(ObTableStoreStat));
  ls_id_ = ls_id;
  tablet_id_ = tablet_id;
  table_id_ = table_id;
}

bool ObTableStoreStat::is_valid() const
{
  bool valid = true;
  if (row_cache_hit_cnt_ < 0 || row_cache_miss_cnt_ < 0 || row_cache_put_cnt_ < 0
      || bf_filter_cnt_ < 0 || bf_empty_read_cnt_ < 0 || bf_access_cnt_ < 0
      || block_cache_hit_cnt_ < 0 || block_cache_miss_cnt_ < 0
      || access_row_cnt_ < 0 || output_row_cnt_ < 0 || fuse_row_cache_hit_cnt_ < 0
      || fuse_row_cache_miss_cnt_ < 0 || fuse_row_cache_put_cnt_ < 0
      || macro_access_cnt_ < 0 || micro_access_cnt_ < 0 || pushdown_micro_access_cnt_ < 0
      || pushdown_row_access_cnt_ < 0 || pushdown_row_select_cnt_ < 0
      || !single_get_stat_.is_valid() || !multi_get_stat_.is_valid() || !index_back_stat_.is_valid()
      || !single_scan_stat_.is_valid() || !multi_scan_stat_.is_valid()
      || !exist_row_.is_valid() ||!get_row_.is_valid() || !scan_row_.is_valid()
      || logical_read_cnt_ < 0 || physical_read_cnt_ < 0) {
    valid = false;
  }
  return valid;
}

int ObTableStoreStat::add(const ObTableStoreStat& other)
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("self is invalid", K(ret), K(*this));
  } else if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("other is invalid", K(ret), K(other));
  } else if (other.ls_id_ != ls_id_ || other.tablet_id_ != tablet_id_ || other.table_id_ != table_id_) {
    ret = OB_NOT_THE_OBJECT;
    LOG_DEBUG("not the same table store", K(ret), K(other));
  } else {
    row_cache_hit_cnt_ += other.row_cache_hit_cnt_;
    row_cache_miss_cnt_ += other.row_cache_miss_cnt_;
    row_cache_put_cnt_ += other.row_cache_put_cnt_;
    bf_filter_cnt_ += other.bf_filter_cnt_;
    bf_empty_read_cnt_ += other.bf_empty_read_cnt_;
    bf_access_cnt_ += other.bf_access_cnt_;
    block_cache_hit_cnt_ += other.block_cache_hit_cnt_;
    block_cache_miss_cnt_ += other.block_cache_miss_cnt_;
    access_row_cnt_ += other.access_row_cnt_;
    output_row_cnt_ += other.output_row_cnt_;
    fuse_row_cache_hit_cnt_ += other.fuse_row_cache_hit_cnt_;
    fuse_row_cache_miss_cnt_ += other.fuse_row_cache_miss_cnt_;
    fuse_row_cache_put_cnt_ += other.fuse_row_cache_put_cnt_;
    macro_access_cnt_ += other.macro_access_cnt_;
    micro_access_cnt_ += other.micro_access_cnt_;
    pushdown_micro_access_cnt_ += other.pushdown_micro_access_cnt_;
    pushdown_row_access_cnt_ += other.pushdown_row_access_cnt_;
    pushdown_row_select_cnt_ += other.pushdown_row_select_cnt_;
    //ignore ret
    single_get_stat_.add(other.single_get_stat_);
    multi_get_stat_.add(other.multi_get_stat_);
    index_back_stat_.add(other.index_back_stat_);
    single_scan_stat_.add(other.single_scan_stat_);
    multi_scan_stat_.add(other.multi_scan_stat_);
    exist_row_.add(other.exist_row_);
    get_row_.add(other.get_row_);
    scan_row_.add(other.scan_row_);

    logical_read_cnt_ += other.logical_read_cnt_;
    physical_read_cnt_ += other.physical_read_cnt_;
  }
  return ret;
}

ObTableStoreStat &ObTableStoreStat::operator=(const ObTableStoreStat& other)
{
  if (this != &other) {
    MEMCPY(this, &other, sizeof(ObTableStoreStat));
  }
  return *this;
}

// ------------------ Iterator ------------------ //
ObTableStoreStatIterator::ObTableStoreStatIterator()
  : cur_idx_(0),
    is_opened_(false)
{
}

ObTableStoreStatIterator::~ObTableStoreStatIterator()
{
}

void ObTableStoreStatIterator::reset()
{
  cur_idx_ = 0;
  is_opened_ = false;
}

int ObTableStoreStatIterator::open()
{
  int ret = OB_SUCCESS;
  if (is_opened_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableStoreStatIterator has been opened", K(ret));
  } else {
    cur_idx_ = 0;
    is_opened_ = true;
  }
  return ret;
}

int ObTableStoreStatIterator::get_next_stat(ObTableStoreStat &stat)
{
  int ret = OB_SUCCESS;
  if (!is_opened_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableStoreStatIterator has not been opened", K(ret));
  } else if (OB_FAIL(ObTableStoreStatMgr::get_instance().get_table_store_stat(cur_idx_, stat))) {
  } else {
    ++cur_idx_;
  }
  return ret;
}

// ------------------ Manager ------------------ //
int ObTableStoreStatMgr::ReportTask::init(ObTableStoreStatMgr *stat_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stat_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(stat_mgr));
  } else {
    stat_mgr_ = stat_mgr;
  }
  return ret;
}

void ObTableStoreStatMgr::ReportTask::runTimerTask()
{
  if (OB_ISNULL(stat_mgr_)) {
    LOG_WARN_RET(OB_NOT_INIT, "ReportTask is not inited");
  } else {
    stat_mgr_->run_report_task();
  }
}

ObTableStoreStatMgr::ObTableStoreStatMgr()
  : is_inited_(false),
    lock_(),
    quick_map_(),
    cur_cnt_(0),
    limit_cnt_(0),
    lru_head_(NULL),
    lru_tail_(NULL),
    report_cursor_(0),
    pending_cursor_(0),
    report_task_()
{
}

ObTableStoreStatMgr::~ObTableStoreStatMgr()
{
  destroy();
}

void ObTableStoreStatMgr::stop()
{
  TG_STOP(lib::TGDefIDs::TableStatRpt);
}
void ObTableStoreStatMgr::wait()
{
  TG_WAIT(lib::TGDefIDs::TableStatRpt);
}
void ObTableStoreStatMgr::destroy()
{
  if (IS_INIT) {
    reset();
  }
}
void ObTableStoreStatMgr::reset()
{
  is_inited_ = false;
  TG_DESTROY(lib::TGDefIDs::TableStatRpt);
  report_task_.destroy();
  report_cursor_ = 0;
  pending_cursor_ = 0;
  lru_head_ = NULL;
  lru_tail_ = NULL;
  cur_cnt_ = 0;
  limit_cnt_ = 0;
  quick_map_.destroy();
}

int ObTableStoreStatMgr::init(const int64_t limit_cnt)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableStoreStatMgr has already been initiated", K(ret));
  } else if (limit_cnt <= 0 || limit_cnt > DEFAULT_MAX_CNT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(limit_cnt));
  } else if (OB_FAIL(quick_map_.create(limit_cnt, ObModIds::OB_TABLE_STORE_STAT_MGR, ObModIds::OB_TABLE_STORE_STAT_MGR))) {
    LOG_WARN("Fail to create merge info map, ", K(ret));
  } else if (OB_FAIL(report_task_.init(this))) {
    LOG_WARN("Fail to init report task, ", K(ret));
  } else if (OB_FAIL(TG_START(lib::TGDefIDs::TableStatRpt))) {
    LOG_WARN("Fail to init timer, ", K(ret));
  } else {
    for (int64_t i = 0; i < MAX_PENDDING_CNT; ++i) {
      stat_queue_[i].reset();
    }
    for (int64_t i = 0; i < DEFAULT_MAX_CNT; ++i) {
      stat_array_[i].reset();
      node_pool_[i].reset();
      // bind stat to node
      node_pool_[i].stat_ = &(stat_array_[i]);
    }
    limit_cnt_ = limit_cnt;
    if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::TableStatRpt, report_task_,
                            REPORT_TASK_INTERVAL_US, /*repeat*/true))) {
      LOG_WARN("schedule report task fail", K(ret));
    } else {
      LOG_INFO("schedule report task succeed");
      is_inited_ = true;
    }
  }
  if (!is_inited_) {
    reset();
  }
  return ret;
}

ObTableStoreStatMgr &ObTableStoreStatMgr::get_instance()
{
  static ObTableStoreStatMgr instance_;
  return instance_;
}

void ObTableStoreStatMgr::move_node_to_head(ObTableStoreStatNode *node)
{
  if (node == lru_head_) {
    // do nothing
  } else if (NULL == lru_head_) {
    node->pre_ = node->next_ = NULL;
    lru_head_ = lru_tail_ = node;
  } else {
    if (NULL != node->pre_) {
      node->pre_->next_ = node->next_;
    }
    if (NULL != node->next_) {
      node->next_->pre_ = node->pre_;
    }
    if (node == lru_tail_) {
      lru_tail_ = node->pre_;
    }
    node->pre_ = NULL;
    node->next_ = lru_head_;
    lru_head_->pre_ = node;
    lru_head_ = node;
  }
}

int ObTableStoreStatMgr::report_stat(const ObTableStoreStat &stat)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableStoreStatMgr hasn't been initiated", K(ret));
  } else if (!stat.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid stat", K(ret), K(stat));
  } else {
    uint64_t pending_cur = ATOMIC_LOAD(&pending_cursor_);
    uint64_t report_cur = ATOMIC_LOAD(&report_cursor_);
    if (((pending_cur + 1) % MAX_PENDDING_CNT) == report_cur) {
      // queue is full, direct return, ignore this stat
    } else {
      if (pending_cur != ATOMIC_CAS(&pending_cursor_, pending_cur, pending_cur + 1)) {
        // fail to advance the cursor, give up and ignore this stat
      } else {
        stat_queue_[pending_cur % MAX_PENDDING_CNT] = stat;
      }
    }
  }
  return ret;
}

int ObTableStoreStatMgr::get_table_store_stat(const int64_t idx, ObTableStoreStat &stat)
{
  UNUSED(idx);
  UNUSED(stat);
  int ret = OB_ITER_END;
  return ret;
}

void ObTableStoreStatMgr::run_report_task()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableStoreStatMgr hasn't been initiated", K(ret));
  } else {
    int64_t start = static_cast<int64_t>(ATOMIC_LOAD(&report_cursor_));
    int64_t end = static_cast<int64>(ATOMIC_LOAD(&pending_cursor_) % MAX_PENDDING_CNT);
    if (end == (start + 1) % MAX_PENDDING_CNT) {
      // sleep 100ms to give chance for the latest stat be assigned in report_stat
      ob_usleep(1000 * 100);
    }
    if (start == end) {
      // do nothing
    } else if (start < end) {
      for (int64_t i = start; i < end; ++i) {
        if(OB_FAIL(add_stat(stat_queue_[i]))) {
          LOG_WARN("report stat fail", K(ret), K(i), K(stat_queue_[i]));
        }
      }
      ATOMIC_STORE(&report_cursor_, end);
    } else {
      for (int64_t i = start; i < MAX_PENDDING_CNT - 1; ++i) {
        if(OB_FAIL(add_stat(stat_queue_[i]))) {
          LOG_WARN("report stat fail", K(ret), K(i), K(stat_queue_[i]));
        }
      }
      for (int64_t i = 0; i < end; ++i) {
        if(OB_FAIL(add_stat(stat_queue_[i]))) {
          LOG_WARN("report stat fail", K(ret), K(i), K(stat_queue_[i]));
        }
      }
      ATOMIC_STORE(&report_cursor_, end);
    }
  }
}

int ObTableStoreStatMgr::add_stat(const ObTableStoreStat &stat)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableStoreStatMgr hasn't been initiated", K(ret));
  } else if (!stat.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid stat", K(ret), K(stat));
  } else {
    ObTableStoreStatKey key(stat.table_id_, stat.tablet_id_);
    ObTableStoreStatNode *node = NULL;

    SpinWLockGuard guard(lock_);
    // retrieve node
    if (OB_FAIL(quick_map_.get_refactored(key, node))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        node = NULL;
        if (cur_cnt_ < limit_cnt_) { // has free node
          node = &(node_pool_[cur_cnt_++]);
        } else { // evict the tail node
          if (NULL == lru_tail_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("LRU tail is NULL", K(ret), K(stat), K(cur_cnt_));
          } else {
            ObTableStoreStatKey old_key(
                lru_tail_->stat_->table_id_,
                lru_tail_->stat_->tablet_id_);
            if (OB_FAIL(quick_map_.erase_refactored(old_key))) {
              LOG_WARN("add new node to hash map fail", K(ret), K(old_key));
            }
            node = lru_tail_;
            node->stat_->reset();
          }
        }

        if (OB_SUCC(ret)) {
          if (NULL == node) {
            LOG_WARN("node is NULL", K(ret), K(stat));
          } else {
            node->stat_->ls_id_ = stat.ls_id_;
            node->stat_->tablet_id_ = stat.tablet_id_;
            node->stat_->table_id_ = stat.table_id_;
            if (OB_FAIL(quick_map_.set_refactored(key, node))) {
              LOG_WARN("add new node to hash map fail", K(ret), K(key), K(*(node->stat_)));
            }
          }
        }
      } else {
        LOG_WARN("fail to get node", K(ret), K(key), K(stat));
      }
    }

    // update LRU
    if (OB_SUCC(ret)) {
      if (NULL == node) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("node is null", K(ret), K(stat), K(cur_cnt_));
      } else {
        // ignore ret
        node->stat_->add(stat);
        move_node_to_head(node);
      }
    }
  }
  return ret;
}
} // namespace oceanbase
} // namespace storage
