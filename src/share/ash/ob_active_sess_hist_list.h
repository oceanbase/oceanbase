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

#ifndef _OB_SHARE_ASH_ACTIVE_SESSION_LIST_H_
#define _OB_SHARE_ASH_ACTIVE_SESSION_LIST_H_

#include "lib/lock/ob_tc_rwlock.h"
#include "lib/ash/ob_active_session_guard.h"

namespace oceanbase
{
namespace share
{
typedef lib::ObLockGuard<lib::ObMutex> LockGuard;

class ObActiveSessHistList
{
public:
  ObActiveSessHistList();
  ~ObActiveSessHistList() = default;

  static ObActiveSessHistList &get_instance();
  int init();
  int64_t get_ash_size() const { return ash_size_; }
  int resize_ash_size();
  TO_STRING_KV(K(size()), K_(ash_size), K_(ash_buffer));

  /*                    * -> write_idx
   * +------------------+--------------------+
   * | | | | | | | | | | | | | | | | | | | | |
   * +----------------+----------------------+
   *     read_idx  <- *
   *
   * we don't regard it as a circular buffer,
   * instead, regard it as an unlimited array goes to one direction forever
   * i.e. write_pos will increase for ever
   *
   * NOTICE: below function must be called with mutex_ or shared_ptr protection.
   */
  void add(ObActiveSessionStat &stat)
  {
    int64_t idx = ash_buffer_->append(stat);
    if (stat.event_no_) {
      // Once session can see fixup index, it surely can see fixup_buffer.
      stat.set_fixup_buffer(ash_buffer_);
      MEM_BARRIER();
      stat.set_fixup_index(idx);
    } else {
      // we cannot reset fixup buffer here. Because user thread would reset fixup buffer too. Causing a race condition.
      // Bottom line is that we cannot introduce any lock in worker thread.
      // Worst case is the fixup buffer never got release because user thread's wait event never ends.
    }
  }
  void set_read_pos(int64_t read_pos) { ash_buffer_->set_read_pos(read_pos); }
  int64_t write_pos() const { return ash_buffer_->write_pos(); }
  inline int64_t size() const { return ash_buffer_->size(); }
  inline int64_t free_slots_num() const { return ash_buffer_->free_slots_num(); }
  const ObActiveSessionStatItem &get(int64_t pos) const {
    return ash_buffer_->get(pos);
  }
public:
  class Iterator
  {
  public:
    Iterator() : ash_buffer_(), curr_(0), end_(0) {}
    explicit Iterator(const common::ObSharedGuard<ObAshBuffer> &ash_buffer,
             int64_t start,
             int64_t end)
        : ash_buffer_(),
          curr_(start),
          end_(end)
    {
      ash_buffer_ = ash_buffer;
    }
    Iterator(const Iterator &other)
       : curr_(other.curr_),
         end_(other.end_)
    {
      ash_buffer_ = other.ash_buffer_;
    }
    TO_STRING_KV(K_(ash_buffer), K_(curr), K_(end));
    bool has_next() const
    {
      bool bret = true;
      if (OB_UNLIKELY(nullptr == ash_buffer_.get_ptr() || ash_buffer_->write_pos() == 0 || curr_ < 0)) {
        bret = false;
      } else if (ash_buffer_->write_pos() - curr_ > ash_buffer_->size()) {  // write_pos is the next valid write position which is not written
        bret = false;
      } else if (curr_ < end_) {
        bret = false;
      }
      return bret;
    }
    const ObActiveSessionStatItem &next()
    {
      int64_t pos = curr_ % ash_buffer_->size();
      curr_--;
      return ash_buffer_->get(pos);
    }
    void init_with_sample_time_index(const int64_t &start, const int64_t &end)
    {
      if (OB_LIKELY(curr_ >= 0)) {
        std::pair<int64_t, int64_t> range = binary_search_sample_time_range(start, end);
        curr_ = range.second;
        end_ = range.first;
      }
      LOG_DEBUG("ash range for index", K(start), K(end), KPC(this));
    }
    private:
     std::pair<int64_t, int64_t> binary_search_sample_time_range(
         const int64_t &start, const int64_t &end) {
       int64_t right = sample_time_search_right_most(end);
       int64_t left = sample_time_search_left_most(start);
       return std::pair<int64_t, int64_t>{left, right};
     }
    int64_t sample_time_search_left_most(const int64_t left)
    {
      /**
       * Iteration of ash is in reverse order.
       * So below function aims to find the left most in the range [begin, end)
       * where sample_time is in ascending order.
       */
      int64_t begin = end_, end = curr_ + 1;
      int64_t middle = -1;
      while (begin < end) {
        middle = begin + ((end - begin) >> 1);
        const int64_t val = ash_buffer_->get(middle % ash_buffer_->size()).sample_time_;
        if (OB_LIKELY(is_valid(middle))) {
          if (val >= left) {
            end = middle;
          } else {
            begin = middle + 1;
          }
        } else {
          /**
           * If ash data produced so fast, ash write_pos would catch up with
           * current middle pos, which breaks the binary search's premise that
           * the target array is ascending sorted. In this case, we do the
           * binary search again. if binary search could be finished within 1
           * seconds(ash's default sample interval), this corner case could be
           * ended in constant time.
           */
          LOG_DEBUG("ash overwrite happened during binary search", K(begin),
                    K(middle), K(end), K(val), K(ash_buffer_->size()));
          // TODO(roland.qk): Adding sysstat counter to track this corner case.
          end = curr_ + 1;
          begin = ash_buffer_->write_pos() - ash_buffer_->size();
          OB_ASSERT(begin >= 0);
        }
      }
      return begin;
    }
    int64_t sample_time_search_right_most(const int64_t &right)
    {
      int64_t begin = sample_time_search_left_most(right);
      int64_t end = curr_;
      int64_t middle = -1;
      while (begin <= end) {
        middle = begin + ((end - begin) >> 1);
        const int64_t val = ash_buffer_->get(middle % ash_buffer_->size()).sample_time_;
        if (OB_LIKELY(is_valid(middle))) {
          if (val > right) {
            end = middle - 1;
          } else {
            begin = middle + 1;
          }
        } else {
          // ash ring buffer overwrite happened.
          LOG_DEBUG("ash overwrite happened during binary search", K(begin),
                    K(middle), K(end), K(val), K(ash_buffer_->size()));
          // TODO(roland.qk): Adding sysstat counter to track this corner case.
          end = curr_ + 1;
          begin = sample_time_search_left_most(right);
          OB_ASSERT(begin >= 0);
        }
      }
      return end;
    }
    bool is_valid(int64_t pos)
    {
      bool bret = true;
      if (OB_UNLIKELY(nullptr == ash_buffer_.get_ptr() || ash_buffer_->write_pos() == 0 || curr_ < 0)) {
        bret = false;
      } else if (ash_buffer_->write_pos() - pos > ash_buffer_->size()) {
        bret = false;
      } else if (pos < end_ || pos > curr_) {
        bret = false;
      }
      return bret;
    }
  private:
    common::ObSharedGuard<ObAshBuffer> ash_buffer_;
    int64_t curr_;
    int64_t end_;
  };

  class ReverseIterator
  {
  public:
    ReverseIterator() : ash_buffer_(), curr_(0), end_(0) {}
    explicit ReverseIterator(const common::ObSharedGuard<ObAshBuffer> &ash_buffer,
              int64_t start,
              int64_t end)
        : ash_buffer_(),
          curr_(start),
          end_(end)
    {
      ash_buffer_ = ash_buffer;
    }
    ReverseIterator(const ReverseIterator &other)
       : curr_(other.curr_),
         end_(other.end_)
    {
      ash_buffer_ = other.ash_buffer_;
    }
    bool has_next() const
    {
      bool bret = true;
      if (OB_UNLIKELY(nullptr == ash_buffer_.get_ptr() || ash_buffer_->write_pos() == 0 || curr_ < 0)) {
        bret = false;
      } else if (curr_ >= ash_buffer_->write_pos()) {  // write_pos is the next valid write position which is not written
        bret = false;
      } else if (curr_ > end_) {
        bret = false;
      }
      return bret;
    }
    const ObActiveSessionStatItem &next()
    {
      int64_t pos = curr_ % ash_buffer_->size();
      curr_++;
      return ash_buffer_->get(pos);
    }
    int64_t distance() const
    {
      return end_ - curr_ + 1;
    }
    TO_STRING_KV(K_(ash_buffer), K(ash_buffer_->write_pos()), K_(curr), K_(end));
  private:
    common::ObSharedGuard<ObAshBuffer> ash_buffer_;
    int64_t curr_;
    int64_t end_;
  };

  Iterator create_iterator()
  {
    // get hold of ash buffer.
    LockGuard lock(mutex_);
    common::ObSharedGuard<ObAshBuffer> ash_buffer = ash_buffer_;
    int64_t read_start = 0;
    int64_t read_end = 0;
    if (ash_buffer->write_pos() < ash_buffer->size()) {
      // buffer not full
      read_start = ash_buffer->write_pos() - 1;
      read_end = 0;
    } else {
      read_start = ash_buffer->write_pos() - 1;
      read_end = ash_buffer->write_pos() - ash_buffer->size();
    }
    return Iterator(ash_buffer, read_start, read_end);
  }

  ReverseIterator create_reverse_iterator_no_lock()
  {
    // get hold of ash buffer.
    common::ObSharedGuard<ObAshBuffer> ash_buffer = ash_buffer_;
    int64_t read_start = 0;
    int64_t read_end = 0;
    if (ash_buffer->write_pos() < ash_buffer->size()) {
      // buffer not full
      read_start = 0;
      read_end = ash_buffer->write_pos() - 1;
    } else {
      read_start = ash_buffer->write_pos() - ash_buffer->size();
      read_end = ash_buffer->write_pos() - 1;
    }
    return ReverseIterator(ash_buffer, read_start, read_end);
  }
  void lock() { mutex_.lock(); };
  void unlock() { mutex_.unlock(); };
private:
  int allocate_ash_buffer(int64_t ash_size, common::ObSharedGuard<ObAshBuffer> &ash_buffer);
  int64_t ash_size_;
  lib::ObMutex mutex_;
  common::ObSharedGuard<ObAshBuffer> ash_buffer_;
};

}
}
#endif
