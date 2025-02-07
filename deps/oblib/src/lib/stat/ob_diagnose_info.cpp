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

#define USING_LOG_PREFIX COMMON

#include "lib/stat/ob_diagnose_info.h"
#include "lib/stat/ob_session_stat.h"
#include "lib/ash/ob_active_session_guard.h"
#include "share/ash/ob_active_sess_hist_list.h"
#include "lib/stat/ob_diagnostic_info_container.h"
#include "lib/stat/ob_diagnostic_info_guard.h"

namespace oceanbase
{
namespace common
{
/**
 * -----------------------------------------------------------ObLatchStat------------------------------------------------------
 */
ObLatchStat::ObLatchStat()
    : gets_(0),
      misses_(0),
      sleeps_(0),
      immediate_gets_(0),
      immediate_misses_(0),
      spin_gets_(0),
      wait_time_(0)
{
}

int ObLatchStat::add(const ObLatchStat &other)
{
  int ret = OB_SUCCESS;
  gets_ += other.gets_;
  misses_ += other.misses_;
  sleeps_ += other.sleeps_;
  immediate_gets_ += other.immediate_gets_;
  immediate_misses_ += other.immediate_misses_;
  spin_gets_ += other.spin_gets_;
  wait_time_ += other.wait_time_;
  return ret;
}

void ObLatchStat::reset()
{
  gets_ = 0;
  misses_ = 0;
  sleeps_ = 0;
  immediate_gets_ = 0;
  immediate_misses_ = 0;
  spin_gets_ = 0;
  wait_time_ = 0;
}

/**
 * ----------------------------------------------------------ObLatchStatArray-----------------------------------------------------
 */
ObLatchStatArray::ObLatchStatArray(ObIAllocator *allocator)
  : allocator_(allocator), items_()
{
}

ObLatchStatArray::~ObLatchStatArray()
{
  for (int64_t i = 0; i < ObLatchIds::LATCH_END; ++i) {
    if (OB_ISNULL(items_[i])) {
    } else {
      free_item(items_[i]);
      items_[i] = NULL;
    }
  }
}

int ObLatchStatArray::add(const ObLatchStatArray &other)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < ObLatchIds::LATCH_END && OB_SUCCESS == ret; ++i) {
    if (OB_ISNULL(other.get_item(i))) continue;
    auto *item = get_or_create_item(i);
    if (OB_NOT_NULL(item)) {
      ret = item->add(*other.get_item(i));
    } else {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      break;
    }
  }
  return ret;
}

void ObLatchStatArray::reset()
{
  for (int64_t i = 0; i < ObLatchIds::LATCH_END; ++i) {
    if (OB_ISNULL(items_[i])) {
    } else {
      items_[i]->reset();
    }
  }
}

static constexpr int NODE_NUM =
    common::hash::NodeNumTraits<ObLatchStat, common::OB_MALLOC_MIDDLE_BLOCK_SIZE>::NODE_NUM;
using LatchStatAlloc = hash::SimpleAllocer<ObLatchStat, NODE_NUM>;

LatchStatAlloc &get_latch_stat_alloc()
{
  struct Wrapper
  {
    Wrapper()
    {
      instance_.set_attr(SET_USE_500("LatchStat"));
      instance_.set_leak_check(false);
    }
    LatchStatAlloc instance_;
  };
  static Wrapper w;
  return w.instance_;
}

ObLatchStat *ObLatchStatArray::create_item()
{
  ObLatchStat *stat = NULL;
  lib::ObDisableDiagnoseGuard disable_diagnose_guard;
  if (OB_ISNULL(allocator_)) {
    stat = get_latch_stat_alloc().alloc();
  } else {
    stat = OB_NEWx(ObLatchStat, allocator_);
  }
  return stat;
}

void ObLatchStatArray::free_item(ObLatchStat *stat)
{
  lib::ObDisableDiagnoseGuard disable_diagnose_guard;
  if (OB_ISNULL(allocator_)) {
    get_latch_stat_alloc().free(stat);
  } else {
    stat->~ObLatchStat();
    allocator_->free(stat);
  }
}

/**
 * -------------------------------------------------------ObWaitEventHistory-------------------------------------------------------
 */
ObWaitEventHistoryIter::ObWaitEventHistoryIter()
  : items_(NULL),
    curr_(0),
    start_pos_(0),
    item_cnt_(0)
{
}

ObWaitEventHistoryIter::~ObWaitEventHistoryIter()
{
  reset();
}

int ObWaitEventHistoryIter::init(ObWaitEventDesc *items, const int64_t start_pos, int64_t item_cnt)
{
  int ret = OB_SUCCESS;
  if (NULL == items || start_pos < 0 || item_cnt < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    items_ = items;
    start_pos_ = start_pos;
    item_cnt_ = item_cnt;
    curr_ = 0;
  }
  return ret;
}

int ObWaitEventHistoryIter::get_next(ObWaitEventDesc *&item)
{
  int ret = OB_SUCCESS;
  if (curr_ >= item_cnt_ || curr_ >= SESSION_WAIT_HISTORY_CNT) {
    ret = OB_ITER_END;
  } else {
    item = &items_[(start_pos_ - curr_ + SESSION_WAIT_HISTORY_CNT) % SESSION_WAIT_HISTORY_CNT];
    curr_++;
    if (!item->is_valid()) {
      LOG_WARN("wait event desc is invalid", K(ret), K(item->event_no_));
      if (OB_FAIL(get_next(item))) {
        if (ret != OB_ITER_END) {
          LOG_WARN("failed to get next wait event desc");
        }
      }
    }

  }
  return ret;
}

void ObWaitEventHistoryIter::reset()
{
  items_ = NULL;
  curr_ = 0;
  start_pos_ = 0;
  item_cnt_ = 0;
}

ObWaitEventHistory::ObWaitEventHistory()
  : curr_pos_(0),
    item_cnt_(0),
    nest_cnt_(0),
    current_wait_(0)
{
  memset(items_, 0, sizeof(items_));
}

ObWaitEventHistory::~ObWaitEventHistory()
{
  reset();
}

int ObWaitEventHistory::push(const int64_t event_no, const uint64_t timeout_ms, const uint64_t p1, const uint64_t p2, const uint64_t p3)
{
  int ret = OB_SUCCESS;
  if (event_no < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (nest_cnt_ >= 1) {
    ret = OB_ARRAY_OUT_OF_RANGE;
  } else if (nest_cnt_ >= SESSION_WAIT_HISTORY_CNT) {
    ++nest_cnt_;
  } else {
    if (0 != nest_cnt_) {
      items_[curr_pos_].level_ = items_[current_wait_].level_ + 1;
      items_[curr_pos_].parent_ = current_wait_ - curr_pos_;
      current_wait_ = curr_pos_;
    } else {
      items_[curr_pos_].level_ = 0;
      items_[curr_pos_].parent_ = 0;
      current_wait_ = curr_pos_;
    }
    items_[curr_pos_].event_no_ = event_no;
    items_[curr_pos_].p1_ = p1;
    items_[curr_pos_].p2_ = p2;
    items_[curr_pos_].p3_ = p3;
    items_[curr_pos_].wait_end_time_ = 0;
    items_[curr_pos_].wait_time_ = 0;
    items_[curr_pos_].timeout_ms_ = timeout_ms;
    items_[curr_pos_].is_phy_ = OB_WAIT_EVENTS[event_no].is_phy_;
    if (items_[curr_pos_].is_phy_) {
      items_[curr_pos_].wait_begin_time_ = ObTimeUtility::current_time();
    }
    ++nest_cnt_;
    curr_pos_ = (curr_pos_ + 1) % SESSION_WAIT_HISTORY_CNT;
    if (item_cnt_ < SESSION_WAIT_HISTORY_CNT) {
      ++item_cnt_;
    }
  }
  return ret;
}

int ObWaitEventHistory::add(const ObWaitEventHistory &other)
{
  int64_t i = 0, j = 0, cnt = 0;
  int16_t N = SESSION_WAIT_HISTORY_CNT;
  int ret = OB_SUCCESS;
  if (other.item_cnt_ > 0) {
    ObWaitEventDesc tmp[SESSION_WAIT_HISTORY_CNT];
    memset(tmp, 0, sizeof(tmp));
    for (i = 0, j = 0; i < item_cnt_ && j < other.item_cnt_ && cnt < N && OB_SUCCESS == ret;) {
      ret = get_next_and_compare(i, j, cnt, other, tmp);
    }
    if (OB_SUCCESS != ret) {
    } else {
      if (cnt < N) {
        for (; i < item_cnt_ && cnt < N; ++i) {
          tmp[cnt++] = items_[(curr_pos_ - 1 - i + N) % N];
        }
        for (; j < other.item_cnt_ && cnt < N; ++j) {
          tmp[cnt++] = other.items_[(other.curr_pos_ - 1 - j + N) % N];
        }
      }

      for (i = cnt - 1; i >= 0; --i) {
        items_[cnt - i - 1] = tmp[i];
      }
      item_cnt_ = cnt;
      curr_pos_ = cnt % N;
    }
  }
  return ret;
}

int ObWaitEventHistory::get_next_and_compare(int64_t &iter_1, int64_t &iter_2, int64_t &cnt, const ObWaitEventHistory &other, ObWaitEventDesc *tmp)
{
  int64_t tmp_1 = iter_1;
  int64_t tmp_2 = iter_2;
  int ret = OB_SUCCESS;
  int tmp_ret = OB_ITER_END;
  int16_t N = SESSION_WAIT_HISTORY_CNT;

  if (iter_1 < 0 || iter_2 < 0 || cnt < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    for(; tmp_1 < item_cnt_; ++tmp_1) {
      if(0 == items_[(curr_pos_ - 1 - tmp_1 + N) % N].level_) {
        tmp_ret = OB_SUCCESS;
        break;
      }
    }
    if (OB_SUCCESS == tmp_ret) {
      tmp_ret = OB_ITER_END;
      for(; tmp_2 < other.item_cnt_; ++tmp_2) {
        if(0 == other.items_[(other.curr_pos_ - 1 - tmp_2 + N) % N].level_) {
          tmp_ret = OB_SUCCESS;
          break;
        }
      }
      if (OB_SUCCESS == tmp_ret) {
        if (items_[(curr_pos_ - 1 - tmp_1 + N) % N] > other.items_[(other.curr_pos_ - 1 - tmp_2 + N) % N]) {
          for (int64_t i = iter_1; i <= tmp_1 && cnt < N; i++) {
            tmp[cnt++] = items_[(curr_pos_ - 1 - i + N) % N];
          }
          iter_1 = tmp_1 + 1;
        } else {
          for (int64_t i = iter_2; i <= tmp_2 && cnt < N; i++) {
            tmp[cnt++] = other.items_[(other.curr_pos_ - 1 - i + N) % N];
          }
          iter_2 = tmp_2 + 1;
        }
      } else {
        iter_2 = other.item_cnt_;
      }
    } else {
      iter_1 = item_cnt_;
    }
  }
  return ret;
}

int ObWaitEventHistory::get_iter(ObWaitEventHistoryIter &iter)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(iter.init(items_, (curr_pos_ - 1 + SESSION_WAIT_HISTORY_CNT) % SESSION_WAIT_HISTORY_CNT, item_cnt_))) {
  }
  return ret;
}

int ObWaitEventHistory::get_last_wait(ObWaitEventDesc *&item)
{
  int ret = OB_SUCCESS;
  int16_t N = SESSION_WAIT_HISTORY_CNT;
  int64_t cnt = 0;
  if (0 == item_cnt_) {
    ret = OB_ITEM_NOT_SETTED;
  } else {
    while (cnt < item_cnt_ && 0 != items_[(curr_pos_ - cnt - 1 + N) % N].level_ && 0 != items_[(curr_pos_ - cnt - 1 + N) % N].wait_end_time_) {
      cnt++;
    }
    if (cnt < item_cnt_) {
      item = &items_[(curr_pos_ - cnt - 1 + N) % N];
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get last wait failed", K(cnt), K_(item_cnt), K_(curr_pos), K_(nest_cnt), K_(current_wait));
    }
  }
  return ret;
}

int ObWaitEventHistory::get_curr_wait(ObWaitEventDesc *&item)
{
  int ret = OB_SUCCESS;
  int16_t N = SESSION_WAIT_HISTORY_CNT;
  if (0 == item_cnt_) {
    ret = OB_ITEM_NOT_SETTED;
  } else {
    // get current waiting event or latest event
    item = &items_[(curr_pos_ - 1 + N) % N];
  }
  return ret;
}

int ObWaitEventHistory::get_accord_event(ObWaitEventDesc *&event_desc)
{
  int ret = OB_SUCCESS;

  if (0 != item_cnt_) {
    if (nest_cnt_ > SESSION_WAIT_HISTORY_CNT) {
      --nest_cnt_;
      ret = OB_ARRAY_OUT_OF_RANGE;
    } else {
      event_desc = &(items_[current_wait_]);
    }
  } else {
    ret = OB_ITEM_NOT_SETTED;
  }
  return ret;
}

int ObWaitEventHistory::calc_wait_time(ObWaitEventDesc *&event_desc)
{
  int ret = OB_SUCCESS;

  if (NULL != event_desc) {
    if (0 == event_desc->wait_time_ && 0 == event_desc->wait_end_time_) {
      if (event_desc->is_phy_ && 0 != event_desc->wait_begin_time_) {
        event_desc->wait_end_time_ = ObTimeUtility::current_time();
        event_desc->wait_time_ = event_desc->wait_end_time_ - event_desc->wait_begin_time_;
      }
    }
    if (0 != event_desc->level_) {
      items_[(current_wait_ + event_desc->parent_ + SESSION_WAIT_HISTORY_CNT) % SESSION_WAIT_HISTORY_CNT].wait_time_ += event_desc->wait_time_;
      if (0 == items_[(current_wait_ + event_desc->parent_ + SESSION_WAIT_HISTORY_CNT) % SESSION_WAIT_HISTORY_CNT].wait_begin_time_) {
        items_[(current_wait_ + event_desc->parent_ + SESSION_WAIT_HISTORY_CNT) % SESSION_WAIT_HISTORY_CNT].wait_begin_time_ = event_desc->wait_begin_time_;
      }
      items_[(current_wait_ + event_desc->parent_ + SESSION_WAIT_HISTORY_CNT) % SESSION_WAIT_HISTORY_CNT].wait_end_time_ = event_desc->wait_end_time_;
    } else {
      if (0 != event_desc->wait_time_) {
        curr_pos_ = (curr_pos_ - nest_cnt_ +1 + SESSION_WAIT_HISTORY_CNT) % SESSION_WAIT_HISTORY_CNT;
        item_cnt_ = item_cnt_ + 1 -nest_cnt_;
        nest_cnt_ = 0;
      } else {
        curr_pos_ = (curr_pos_ - nest_cnt_ + SESSION_WAIT_HISTORY_CNT) % SESSION_WAIT_HISTORY_CNT;
        item_cnt_ = item_cnt_ -nest_cnt_;
        nest_cnt_ = 0;
      }
    }
    current_wait_ = (current_wait_ + event_desc->parent_ + SESSION_WAIT_HISTORY_CNT) % SESSION_WAIT_HISTORY_CNT;
  }

  return ret;
}

void ObWaitEventHistory::reset()
{
  curr_pos_ = 0;
  item_cnt_ = 0;
  nest_cnt_ = 0;
  current_wait_ = 0;
  memset(items_, 0, sizeof(items_));
}

ObDiagnoseSessionInfo::ObDiagnoseSessionInfo()
    : curr_wait_(),
      max_wait_(NULL),
      total_wait_(NULL),
      event_history_(),
      event_stats_(),
      stat_add_stats_(),
      tenant_id_(0)
{
}

ObDiagnoseSessionInfo::~ObDiagnoseSessionInfo()
{
  reset();
}

int ObDiagnoseSessionInfo::add(ObDiagnoseSessionInfo &other)
{
  int ret = OB_SUCCESS;
  lock_.wrlock();
  if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    event_stats_.add(other.event_stats_);
    event_history_.add(other.event_history_);
    stat_add_stats_.add(other.stat_add_stats_);
    tenant_id_ = other.tenant_id_;
  }
  lock_.unlock();
  return ret;
}


void ObDiagnoseSessionInfo::reset()
{
  curr_wait_.reset();
  event_stats_.reset();
  event_history_.reset();
  stat_add_stats_.reset();
  max_wait_ = NULL;
  total_wait_ = NULL;
  tenant_id_ = 0;
}

ObWaitEventDesc &ObDiagnoseSessionInfo::get_curr_wait()
{
  int ret = OB_SUCCESS;
  ObWaitEventDesc *event_desc = NULL;
  if (OB_FAIL(event_history_.get_curr_wait(event_desc))) {
    event_desc = &curr_wait_;
  }
  return *event_desc;
}

int ObDiagnoseSessionInfo::inc_stat(const int16_t stat_no)
{
  int ret = OB_SUCCESS;
  // performance critical, do not lock this critical section
  ObStatEventAddStat *stat = stat_add_stats_.get(stat_no);
  if (NULL == stat) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    stat->stat_value_++;
  }
  return ret;
}

int ObDiagnoseSessionInfo::update_stat(const int16_t stat_no, const int64_t delta)
{
  int ret = OB_SUCCESS;
  // performance critical, do not lock this critical section
  ObStatEventAddStat *stat = stat_add_stats_.get(stat_no);
  if (NULL == stat) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    stat->stat_value_ += delta;
  }
  return ret;
}

int ObDiagnoseSessionInfo::set_tenant_id(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (0 < tenant_id && tenant_id < UINT32_MAX) {
    tenant_id_ = tenant_id;
  } else {
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

ObDiagnoseTenantInfo::ObDiagnoseTenantInfo(ObIAllocator *allocator)
  : event_stats_(),
    stat_add_stats_(),
    stat_set_stats_(),
    latch_stats_(allocator)
{
}

ObDiagnoseTenantInfo::~ObDiagnoseTenantInfo()
{
  reset();
}

void ObDiagnoseTenantInfo::add(const ObDiagnoseTenantInfo &other)
{
  event_stats_.add(other.event_stats_);
  stat_add_stats_.add(other.stat_add_stats_);
  stat_set_stats_.add(other.stat_set_stats_);
  latch_stats_.add(other.latch_stats_);
}

void ObDiagnoseTenantInfo::add_wait_event(const ObDiagnoseTenantInfo &other)
{
  event_stats_.add(other.event_stats_);
}

void ObDiagnoseTenantInfo::add_stat_event(const ObDiagnoseTenantInfo &other)
{
  stat_add_stats_.add(other.stat_add_stats_);
  stat_set_stats_.add(other.stat_set_stats_);
}

void ObDiagnoseTenantInfo::add_latch_stat(const ObDiagnoseTenantInfo &other)
{
  latch_stats_.add(other.latch_stats_);
}

void ObDiagnoseTenantInfo::reset()
{
  event_stats_.reset();
  stat_add_stats_.reset();
  stat_set_stats_.reset();
  latch_stats_.reset();
}

int ObDiagnoseTenantInfo::inc_stat(const int16_t stat_no)
{
  int ret = OB_SUCCESS;
  ObStatEventAddStat *stat = stat_add_stats_.get(stat_no);
  if (NULL == stat) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    stat->stat_value_++;
  }
  return ret;
}

int ObDiagnoseTenantInfo::update_stat(const int16_t stat_no, const int64_t delta)
{
  int ret = OB_SUCCESS;
  ObStatEventAddStat *stat = stat_add_stats_.get(stat_no);
  if (NULL == stat) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    stat->stat_value_ += delta;
  }
  return ret;
}

int ObDiagnoseTenantInfo::set_stat(const int16_t stat_no, const int64_t value)
{
  int ret = OB_SUCCESS;
  ObStatEventSetStat *stat = stat_set_stats_.get(stat_no - ObStatEventIds::STAT_EVENT_ADD_END -1);
  if (NULL == stat) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    stat->stat_value_ = value;
  }
  return ret;
}

int ObDiagnoseTenantInfo::get_stat(const int16_t stat_no, int64_t &value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObStatEventIds::STAT_EVENT_ADD_END == stat_no)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (stat_no < ObStatEventIds::STAT_EVENT_ADD_END) {
    ObStatEventAddStat *stat = stat_add_stats_.get(stat_no);
    if (OB_ISNULL(stat)) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      value = stat->stat_value_;
    }
  } else if (stat_no > ObStatEventIds::STAT_EVENT_ADD_END) {
    ObStatEventSetStat *stat = stat_set_stats_.get(stat_no - ObStatEventIds::STAT_EVENT_ADD_END - 1);
    if (OB_ISNULL(stat)) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      value = stat->stat_value_;
    }
  }
  return ret;
}

ObWaitEventGuard::ObWaitEventGuard(const int64_t event_no, const uint64_t timeout_ms,
    const int64_t p1, const int64_t p2, const int64_t p3, const bool is_atomic)
    : event_no_(0), di_(nullptr), is_atomic_(is_atomic)
{
  di_ = ObLocalDiagnosticInfo::get();
  if (OB_NOT_NULL(di_) && oceanbase::lib::is_diagnose_info_enabled() &&
      di_->get_ash_stat().is_active_session_) {    need_record_ = true;
    event_no_ = event_no;
    di_->begin_wait_event(event_no, timeout_ms, p1, p2, p3);
  } else {
    need_record_ = false;
  }
}

ObWaitEventGuard::~ObWaitEventGuard()
{
  if (need_record_) {
    ObDiagnosticInfo *curr_di = ObLocalDiagnosticInfo::get();
    if (di_ != curr_di) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "diagnositc info mismatch", K(curr_di), K(di_));
    }
    if (OB_NOT_NULL(curr_di)) {
      curr_di->end_wait_event(event_no_, OB_WAIT_EVENTS[event_no_].wait_class_ == ObWaitClassIds::IDLE);
    }
  }
}

ObMaxWaitGuard::ObMaxWaitGuard(ObWaitEventDesc *max_wait)
  : prev_wait_(NULL), di_(nullptr)
{
}

ObMaxWaitGuard::~ObMaxWaitGuard()
{
}

ObTotalWaitGuard::ObTotalWaitGuard(ObWaitEventStat *total_wait)
    : total_wait_(total_wait), di_(nullptr)
{
  di_ = ObLocalDiagnosticInfo::get();
  if (OB_NOT_NULL(di_)) {
    di_->reset_total_wait();
  }
}

ObTotalWaitGuard::~ObTotalWaitGuard()
{
  if (OB_UNLIKELY(di_ != ObLocalDiagnosticInfo::get())) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "diagnostic info switched wrongly.", KPC(di_),
        K(ObLocalDiagnosticInfo::get()));
    di_ = ObLocalDiagnosticInfo::get();
  }
  if (OB_NOT_NULL(total_wait_) && OB_NOT_NULL(di_)) {
    total_wait_->add(di_->get_total_wait());
  }
}

} /* namespace common */
} /* namespace oceanbase */
