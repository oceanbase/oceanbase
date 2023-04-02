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


class ObActiveSessHistList
{
public:
  ObActiveSessHistList() : write_pos_(0) {}
  ~ObActiveSessHistList() = default;

  static ObActiveSessHistList &get_instance();
  int init();
  int extend_list(int64_t new_size);

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
    bool has_next()
    {
      bool bret = true;
      if (OB_UNLIKELY(nullptr == list_)) {
        bret = false;
      } else if (list_->write_pos() - curr_ >= list_->size()) {
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
