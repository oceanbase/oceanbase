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

#include "lib/container/ob_array.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "lib/ash/ob_active_session_guard.h"

namespace oceanbase
{
namespace share
{

#define WR_ASH_SAMPLE_INTERVAL 10

class ObActiveSessHistList
{
public:
  ObActiveSessHistList() : write_pos_(0) {}
  ~ObActiveSessHistList() = default;

  static ObActiveSessHistList &get_instance();
  int init();
  int extend_list(int64_t new_size);
  TO_STRING_KV(K(size()), K_(write_pos));

  /*                    * -> write_idx
   * +------------------+--------------------+
   * | | | | | | | | | | | | | | | | | | | | |
   * +----------------+----------------------+
   *     read_idx  <- *
   *
   * we don't regard it as a circular buffer,
   * instead, regard it as an unlimited array goes to one direction forever
   * i.e. write_pos_ will increase for ever
   *
   * Note: no concurrent access to list_
   */
  void add(ActiveSessionStat &stat)
  {
    // TODO: optimize performance, eliminate '%'
    int64_t idx = (write_pos_++ + list_.size()) % list_.size();
    stat.id_ = write_pos_;
    MEMCPY(&list_[idx], &stat, sizeof(ActiveSessionStat));
    if (0 == write_pos_ % WR_ASH_SAMPLE_INTERVAL) {
      list_[idx].is_wr_sample_ = true;
    }
    stat.wait_time_ = 0;
    if (list_[idx].event_no_) {
      stat.set_last_stat(&list_[idx]); // for wait event time fixup
    } else {
      stat.set_last_stat(nullptr); // for wait event time fixup
    }
  }
  int64_t write_pos() const { return write_pos_; }
  inline int64_t size() const { return list_.size(); }
  const ActiveSessionStat &get(int64_t pos) const {
    return list_[pos];
  }
public:
  class Iterator
  {
  public:
    Iterator() : list_(nullptr), curr_(0), end_(0) {}
    Iterator(ObActiveSessHistList *list,
             int64_t start,
             int64_t end)
        : list_(list),
          curr_(start),
          end_(end)
    {}
    Iterator(const Iterator &other)
       : list_(other.list_),
         curr_(other.curr_),
         end_(other.end_)
    {}
    TO_STRING_KV(K_(list), K_(curr), K_(end));
    bool has_next() const
    {
      bool bret = true;
      if (OB_UNLIKELY(nullptr == list_ || list_->write_pos() == 0 || curr_ < 0)) {
        bret = false;
      } else if (list_->write_pos() - curr_ > list_->size()) {  // write_pos is the next valid write position which is not written
        bret = false;
      } else if (curr_ < end_) {
        bret = false;
      }
      return bret;
    }
    const ActiveSessionStat &next()
    {
      int64_t pos = curr_ % list_->size();
      curr_--;
      return list_->get(pos);
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
        const int64_t val = list_->get(middle % list_->size()).sample_time_;
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
                    K(middle), K(end), K(val), KPC(list_));
          // TODO(roland.qk): Adding sysstat counter to track this corner case.
          end = curr_ + 1;
          begin = list_->write_pos() - list_->size();
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
        const int64_t val = list_->get(middle % list_->size()).sample_time_;
        if (OB_LIKELY(is_valid(middle))) {
          if (val > right) {
            end = middle - 1;
          } else {
            begin = middle + 1;
          }
        } else {
          // ash ring buffer overwrite happened.
          LOG_DEBUG("ash overwrite happened during binary search", K(begin),
                    K(middle), K(end), K(val), KPC(list_));
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
      if (OB_UNLIKELY(nullptr == list_ || list_->write_pos() == 0 || curr_ < 0)) {
        bret = false;
      } else if (list_->write_pos() - pos > list_->size()) {
        bret = false;
      } else if (pos < end_ || pos > curr_) {
        bret = false;
      }
      return bret;
    }
  private:
    ObActiveSessHistList *list_;
    int64_t curr_;
    int64_t end_;
  };

  Iterator create_iterator()
  {
    int64_t read_start = 0;
    int64_t read_end = 0;
    if (write_pos_ < list_.size()) {
      // buffer not full
      read_start = write_pos_ - 1;
      read_end = 0;
    } else {
      read_start = write_pos_ - 1;
      read_end = write_pos_ - list_.size();
    }
    return Iterator(this, read_start, read_end);
  }
private:
  common::ObArray<ActiveSessionStat> list_;
  int64_t write_pos_; // where can write to when add an element
};

}
}
#endif
