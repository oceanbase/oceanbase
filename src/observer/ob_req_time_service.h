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

#ifndef OCEANBASE_REQ_TIME_SERVICE_H_
#define OCEANBASE_REQ_TIME_SERVICE_H_

#include "lib/lock/ob_spin_rwlock.h"
#include "lib/list/ob_dlink_node.h"
#include "lib/time/ob_time_utility.h"
#include "lib/thread_local/ob_tsi_factory.h"

namespace oceanbase
{
namespace observer
{
struct ObReqTimeInfo: public common::ObDLinkBase<ObReqTimeInfo>
{
  const static int64_t REQ_TIMEINFO_IDENTIFIER = 0;
  int64_t start_time_;
  int64_t end_time_;
  int64_t reentrant_cnt_;

  ObReqTimeInfo();
  ~ObReqTimeInfo();
  static ObReqTimeInfo &get_thread_local_instance()
  {
    thread_local ObReqTimeInfo req_timeinfo;
    return req_timeinfo;
  }
  void update_start_time()
  {
    if (0 == reentrant_cnt_) {
      // currrent_monotonic_time只保证时间不会退
      // 并不能保证时间一定递增，所以这里检测逻辑用start_time_ > end_time_
      if (OB_UNLIKELY(start_time_ > end_time_)) {
        SERVER_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "invalid start and end time", K(start_time_),
                   K(end_time_), K(this));
      }
      start_time_ = common::ObTimeUtility::current_monotonic_time();
    }
    ++reentrant_cnt_;
  }

  void update_end_time()
  {
    --reentrant_cnt_;
    if (0 == reentrant_cnt_) {
      // 原因同上
      if (OB_UNLIKELY(start_time_ < end_time_)) {
        SERVER_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "invalid start and end time", K(start_time_),
                   K(end_time_), K(this));
      }
      end_time_ = common::ObTimeUtility::current_monotonic_time();
    }
  }
};

class ObGlobalReqTimeService
{
public:
  ObGlobalReqTimeService()
    : lock_(common::ObLatchIds::OB_REQ_TIMEINFO_LIST_LOCK),
      time_info_list_() {}

  static ObGlobalReqTimeService &get_instance()
  {
    static ObGlobalReqTimeService THE_ONE;
    return THE_ONE;
  }

  int add_req_time_info(ObReqTimeInfo *time_info)
  {
    int ret = common::OB_SUCCESS;
    common::SpinWLockGuard lock_guard(lock_);
    if (!time_info_list_.add_last(time_info)) {
        ret = common::OB_ERR_UNEXPECTED;
        SERVER_LOG(ERROR, "failed to add last node", K(ret));
    }
    return ret;
  }

  int rm_req_time_info(ObReqTimeInfo *time_info)
  {
    int ret = common::OB_SUCCESS;
    common::SpinWLockGuard lock_guard(lock_);
    ObReqTimeInfo *deleted_info = time_info_list_.remove(time_info);
    if (OB_UNLIKELY(deleted_info != time_info)) {
      ret = common::OB_ERR_UNEXPECTED;
      SERVER_LOG(ERROR, "unexpected error", K(ret));
    }
    return ret;
  }

  int get_global_safe_timestamp(int64_t &safe_timestamp) const
  {
    int ret = common::OB_SUCCESS;
    int64_t current_time = common::ObTimeUtility::current_monotonic_time();
    safe_timestamp = current_time;
    common::SpinRLockGuard lock_guard(lock_);
    DLIST_FOREACH(cur, time_info_list_) {
      if (OB_ISNULL(cur)) {
        ret = common::OB_INVALID_ARGUMENT;
        SERVER_LOG(ERROR, "invalid null node", K(ret));
      } else {
        int64_t cur_start_time = cur->start_time_;
        int64_t cur_end_time = cur->end_time_;

        // start_time == end_time 线程可能在执行、也可能已经结束，这种情况取start_time作为安全时间
        // start_time > end_time 线程已经在执行，取start_time作为安全时间
        // start_ime == end_time == 0, 线程都没有执行过，这时候取current_time作为安全时间
        // start_time < end_time，线程已经结束，取current_time作为安全时间
        int64_t cur_safe_time = (cur_end_time > cur_start_time
                                || (cur_end_time == cur_start_time
                                    && 0 == cur_start_time))
                                ? current_time
                                : cur_start_time;
        safe_timestamp = MIN(safe_timestamp, cur_safe_time);
      }
    }
    return ret;
  }

  static void check_req_timeinfo();
private:
  common::SpinRWLock lock_;
  common::ObDList<ObReqTimeInfo> time_info_list_;
};

struct ObReqTimeGuard
{
  ObReqTimeGuard()
  {
    ObReqTimeInfo &req_timeinfo = observer::ObReqTimeInfo::get_thread_local_instance();
    req_timeinfo.update_start_time();
  }

  ~ObReqTimeGuard()
  {
    ObReqTimeInfo &req_timeinfo = observer::ObReqTimeInfo::get_thread_local_instance();
    req_timeinfo.update_end_time();
  }
};
} // end namespace observer
} // end namespace oceanbase

#endif // !OCEANBASE_REQ_TIME_SERVICE_H_
