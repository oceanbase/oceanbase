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

#ifndef OCEANBASE_ROOTSERVER_OB_UPDATE_RS_LIST_TASK_H_
#define OCEANBASE_ROOTSERVER_OB_UPDATE_RS_LIST_TASK_H_

#include "lib/lock/ob_spin_rwlock.h"
#include "lib/thread/ob_async_task_queue.h"
#include "lib/thread/ob_work_queue.h"
#include "share/ob_root_addr_agent.h"
#include "share/ob_rs_mgr.h"
#include "share/ob_cluster_role.h"        // ObClusterRole

namespace oceanbase
{
namespace share
{
class ObLSTableOperator;
}
namespace rootserver
{
class ObRootService;
class ObZoneManager;
class ObUpdateRsListTask : public share::ObAsyncTask
{
public:
  ObUpdateRsListTask();
  virtual ~ObUpdateRsListTask();

  int init(share::ObLSTableOperator &lst_operator,
           share::ObRootAddrAgent *addr_agent_,
           ObZoneManager &zone_mgr,
           common::SpinRWLock &lock,
           const bool force_update,
           const common::ObAddr &self_addr);
  int process();
  int process_without_lock();
  int64_t get_deep_copy_size() const;
  share::ObAsyncTask *deep_copy(char *buf, const int64_t buf_size) const;
  static int get_rs_list(share::ObLSTableOperator &lst,
                         const common::ObAddr &self_addr,
                         share::ObIAddrList &rs_list,
                         share::ObIAddrList &readonly_rs_list,
                         bool &rs_list_diff_memeber_list);
  static int check_rs_list_diff(const share::ObIAddrList &l, const share::ObIAddrList &r,
                         bool &different);
  static int check_rs_list_subset(const share::ObIAddrList &l, const share::ObIAddrList &r,
                           bool &is_subset);


private:
  int check_need_update(const share::ObIAddrList &rs_list,
                        const share::ObIAddrList &readonly_rs_list,
                        const common::ObClusterRole cluster_role,
                        const bool rs_list_diff_member_list,
                        bool &need_update,
                        bool &inner_need_update);

public:
  // encapsulate the operation of g_wait_cnt_
  static bool try_lock();
  static void unlock();
  static void clear_lock();

private:
  static volatile int64_t g_wait_cnt_;

private:
  bool inited_;
  share::ObLSTableOperator *lst_operator_;
  share::ObRootAddrAgent *root_addr_agent_;
  ObZoneManager *zone_mgr_;
  common::SpinRWLock *lock_;
  bool force_update_;
  common::ObAddr self_addr_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObUpdateRsListTask);
};

class ObUpdateRsListTimerTask :public common::ObAsyncTimerTask
{
public:
  const static int64_t RETRY_INTERVAL = 600 * 1000L * 1000L;  // 10min
  ObUpdateRsListTimerTask(ObRootService &rs);
  virtual ~ObUpdateRsListTimerTask() {}

  // interface of AsyncTask
  virtual int process() override;
  virtual int64_t get_deep_copy_size() const override { return sizeof(*this); }
  virtual ObAsyncTask *deep_copy(char *buf, const int64_t buf_size) const override;
private:
  ObRootService &rs_;
  DISALLOW_COPY_AND_ASSIGN(ObUpdateRsListTimerTask);
};

}//end namespace rootserver
}//end namespace oceanbase

#endif //OCEANBASE_ROOTSERVER_OB_UPDATE_RS_LIST_TASK_H_
