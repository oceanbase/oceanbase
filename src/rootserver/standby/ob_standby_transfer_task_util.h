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

#ifndef OCEANBASE_ROOTSERVER_OB_STANDBY_TRANSFER_TASK_UTIL_H
#define OCEANBASE_ROOTSERVER_OB_STANDBY_TRANSFER_TASK_UTIL_H

#include "share/balance/ob_balance_task_helper_operator.h"
#include "share/balance/ob_balance_task_table_operator.h"
#include "share/ob_tenant_info_proxy.h"

namespace oceanbase
{
namespace rootserver
{

class ObTransferTickShouldStop
{
public:
  virtual bool operator()() const = 0;
  virtual ~ObTransferTickShouldStop() = default;
};

class NoopTickShouldStop : public ObTransferTickShouldStop
{
public:
  bool operator()() const override { return false; }
};

class ObTransferTickResult
{
public:
  ObTransferTickResult() { reset_(); }
  int init(bool all_settled,
           const ObArray<share::ObBalanceTaskHelper> &helper_tasks,
           const share::ObBalanceTaskArray &balance_tasks,
           int64_t removed_count);
  bool is_all_settled() const { return all_settled_; }
  const ObArray<share::ObBalanceTaskHelper> &get_unsettled_helper_tasks() const { return unsettled_helper_tasks_; }
  const share::ObBalanceTaskArray &get_unsettled_balance_tasks() const { return unsettled_balance_tasks_; }
  int64_t get_removed_count() const { return removed_count_; }
  TO_STRING_KV(K_(all_settled), K_(unsettled_helper_tasks),
               K_(unsettled_balance_tasks), K_(removed_count));
private:
  void reset_()
  {
    all_settled_ = false;
    unsettled_helper_tasks_.reset();
    unsettled_balance_tasks_.reset();
    removed_count_ = 0;
  }
  bool all_settled_;
  ObArray<share::ObBalanceTaskHelper> unsettled_helper_tasks_;
  share::ObBalanceTaskArray unsettled_balance_tasks_;
  int64_t removed_count_;
};

class ObStandbyTransferTaskUtil
{
public:
  // preloaded_helper_tasks: if caller already scanned all helper tasks with
  // max_scn (e.g. background ObRecoveryLSService), pass them here to avoid a
  // redundant scan.  Otherwise leave nullptr — the function loads them internally
  // with max_scn.
  static int run_transfer_tick(
      common::ObISQLClient &proxy,
      share::ObAllTenantInfo &tenant_info,
      const bool do_gc,
      ObTransferTickResult &result,
      const common::ObIArray<share::ObBalanceTaskHelper> *preloaded_helper_tasks = nullptr,
      bool skip_balance_task_load = false,
      const ObTransferTickShouldStop &should_stop = NoopTickShouldStop());

private:
  static int check_transfer_begin_settled_(
      common::ObISQLClient &proxy,
      const share::ObBalanceTaskHelper &task,
      const share::ObAllTenantInfo &tenant_info,
      bool &settled);

  static int check_transfer_end_settled_(
      common::ObISQLClient &proxy,
      const share::ObBalanceTaskHelper &task,
      const share::ObAllTenantInfo &tenant_info,
      bool &settled);

  static int check_replay_after_find_(
      const share::ObBalanceTaskHelper &task,
      const share::ObAllTenantInfo &tenant_info,
      const share::SCN &target_scn,
      bool &settled);

  static int check_single_task_settled_(
      const share::ObLSID &src_ls,
      const share::ObLSID &dest_ls,
      const share::ObAllTenantInfo &tenant_info,
      bool &settled);

  static int process_helper_task_(
      common::ObISQLClient &proxy,
      const share::ObBalanceTaskHelper &task,
      const share::ObAllTenantInfo &tenant_info,
      const bool do_gc,
      bool &settled);

  static int resolve_transfer_begin_after_find_(
      common::ObISQLClient &proxy,
      const share::ObBalanceTaskHelper &task,
      const share::ObAllTenantInfo &tenant_info,
      share::SCN &target_scn,
      bool &settled);
};

} // namespace rootserver
} // namespace oceanbase

#endif /* OCEANBASE_ROOTSERVER_OB_STANDBY_TRANSFER_TASK_UTIL_H */
