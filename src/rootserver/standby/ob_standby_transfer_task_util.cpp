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

#define USING_LOG_PREFIX RS
#include "ob_standby_transfer_task_util.h"

#include "rootserver/ob_ls_service_helper.h"
#include "rootserver/ob_tenant_event_def.h"

namespace oceanbase
{
namespace rootserver
{

ERRSIM_POINT_DEF(ERRSIM_BLOCK_TRANSFER_BEGIN_REMOVE);

int ObTransferTickResult::init(bool all_settled,
    const ObArray<share::ObBalanceTaskHelper> &helper_tasks,
    const share::ObBalanceTaskArray &balance_tasks,
    int64_t removed_count)
{
  int ret = OB_SUCCESS;
  reset_();
  if (OB_FAIL(unsettled_helper_tasks_.assign(helper_tasks))) {
    LOG_WARN("fail to assign unsettled_helper_tasks", KR(ret),
        "helper_cnt", helper_tasks.count());
  } else if (OB_FAIL(unsettled_balance_tasks_.assign(balance_tasks))) {
    LOG_WARN("fail to assign unsettled_balance_tasks", KR(ret),
        "balance_cnt", balance_tasks.count());
    unsettled_helper_tasks_.reset();
  } else {
    all_settled_ = all_settled;
    removed_count_ = removed_count;
  }
  return ret;
}

int ObStandbyTransferTaskUtil::run_transfer_tick(
    common::ObISQLClient &proxy,
    share::ObAllTenantInfo &tenant_info,
    const bool do_gc,
    ObTransferTickResult &result,
    const common::ObIArray<share::ObBalanceTaskHelper> *preloaded_helper_tasks,
    bool skip_balance_task_load,
    const ObTransferTickShouldStop &should_stop)
{
  int ret = OB_SUCCESS;

  ObArray<share::ObBalanceTaskHelper> helper_tasks;
  share::ObBalanceTaskArray balance_tasks;
  ObArray<share::ObBalanceTaskHelper> unsettled_helpers;
  share::ObBalanceTaskArray unsettled_balances;
  int64_t removed_cnt = 0;

  // 1. load helper tasks (or use preloaded if caller already scanned).
  //    Foreground callers load with max_scn to see all rows; background
  //    ObRecoveryLSService passes preloaded helpers from its own scan (which
  //    uses readable_scn except for lossless failover, only GC-ing up to that
  //    point) via skip_balance_task_load=true to avoid a redundant scan.
  //
  //    preloaded_helper_tasks != nullptr:
  //      Background ObRecoveryLSService — it already loaded helpers with
  //      max_scn during its main scan, passes them here to avoid a second
  //      scan.  Also passes skip_balance_task_load=true (background thread
  //      only does GC, does not gate role transitions).
  //
  //    preloaded_helper_tasks == nullptr:
  //      Foreground switchover/failover (check_replay_readiness_ /
  //      wait_ls_balance_task_finish_) — one-shot call, loads helpers
  //      internally with max_scn.
  if (preloaded_helper_tasks != nullptr) {
    if (OB_FAIL(helper_tasks.assign(*preloaded_helper_tasks))) {
      LOG_WARN("fail to assign preloaded helper tasks", KR(ret));
    }
  } else if (OB_FAIL(share::ObBalanceTaskHelperTableOperator::load_tasks_order_by_scn(
          tenant_info.get_tenant_id(), proxy, SCN::max_scn(), helper_tasks))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      helper_tasks.reset();
    } else {
      LOG_WARN("failed to load helper tasks", KR(ret), "tenant_id", tenant_info.get_tenant_id());
    }
  }

  // 2. process helper tasks
  for (int64_t i = 0; OB_SUCC(ret) && i < helper_tasks.count()
       && !should_stop(); ++i) {
      const share::ObBalanceTaskHelper &task = helper_tasks.at(i);
      if (task.get_task_op().is_ls_alter()) {
        if (OB_FAIL(unsettled_helpers.push_back(task))) {
          LOG_WARN("fail to push back unsettled alter task", KR(ret), K(task));
        }
      } else {
        bool settled = false;
        if (OB_FAIL(process_helper_task_(proxy, task, tenant_info, do_gc, settled))) {
          LOG_WARN("failed to process helper task", KR(ret), K(task));
        } else if (!settled) {
          if (OB_FAIL(unsettled_helpers.push_back(task))) {
            LOG_WARN("fail to push back unsettled helper task", KR(ret), K(task));
          }
        } else if (do_gc) {
          // Single-row DELETE by operation_scn (PK) is atomic; no transaction wrapping needed.
          int tmp_ret = share::ObBalanceTaskHelperTableOperator::remove_task(
              tenant_info.get_tenant_id(), task.get_operation_scn(), proxy);
          if (OB_SUCCESS != tmp_ret && OB_ENTRY_NOT_EXIST != tmp_ret) {
            ret = tmp_ret;
            LOG_WARN("failed to remove task", KR(ret), "tenant_id", tenant_info.get_tenant_id(), K(task));
          } else if (OB_ENTRY_NOT_EXIST == tmp_ret) {
            LOG_INFO("task already removed by concurrent thread", K(task));
          } else {
            removed_cnt++;
            ROOTSERVICE_EVENT_ADD("standby_tenant", "remove_balance_task",
                "tenant_id" , tenant_info.get_tenant_id(),
                "task_type", task.get_task_op(),
                "task_scn", task.get_operation_scn().get_val_for_inner_table_field(),
                "switchover_status", tenant_info.get_switchover_status(),
                "src_ls", task.get_src_ls().id(),
                "dest_ls", task.get_dest_ls().id());
          }
        }
      }
  }

  // 3. load and process balance tasks.
  //    __all_balance_task_helper being empty does not mean there are no transfer
  //    tasks in flight. Example: tenant A (primary) starts a load-balance round
  //    that creates balance_task rows in TRANSFER state, but before it finishes,
  //    tenant A becomes a standby. Tenant B (standby) is promoted to primary.
  //    During promotion, tenant B must check __all_balance_task for historical
  //    TRANSFER rows — because the helper table may have already been cleaned by
  //    a previous round while the balance_task table still has entries that must
  //    be replayed before promotion can safely complete.
  //    Only the background thread can skip it because it processes
  //    helper rows and does not gate role transitions.
  if (OB_FAIL(ret)) {
  } else if (!skip_balance_task_load) {
    if (OB_FAIL(share::ObBalanceTaskTableOperator::load_need_transfer_task(
                   tenant_info.get_tenant_id(), balance_tasks, proxy))) {
      LOG_WARN("failed to load need transfer task", KR(ret), "tenant_id", tenant_info.get_tenant_id());
    } else if (helper_tasks.empty()
               && balance_tasks.count() > 0
               && !tenant_info.is_prepare_flashback_for_lossless_failover_to_primary_status()
               && tenant_info.get_sync_scn() != tenant_info.get_readable_scn()) {
      // Short-cut: helper empty + balance non-empty + sync != readable:
      // transfer rounds are done (helper table cleaned) but replay hasn't caught
      // up yet — mark all balance tasks unsettled, no per-task check.
      // Once sync catches up (sync == readable), fall through to per-task replay check.
      // Both paths benefit: check-only avoids pointless per-task check before
      // answering "not ready"; GC path passes them to next polling round.
      // Lossless flashback does not take this short-cut; it always checks per-task.
      if (OB_FAIL(unsettled_balances.assign(balance_tasks))) {
        LOG_WARN("fail to assign unsettled_balances", KR(ret));
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < balance_tasks.count()
           && !should_stop(); ++i) {
        const share::ObBalanceTask &task = balance_tasks.at(i);
        bool settled = false;
        if (OB_FAIL(check_single_task_settled_(
                task.get_src_ls_id(), task.get_dest_ls_id(), tenant_info, settled))) {
          LOG_WARN("failed to check balance task settled", KR(ret), K(task));
        } else if (!settled) {
          if (OB_FAIL(unsettled_balances.push_back(task))) {
            LOG_WARN("fail to push back unsettled balance task", KR(ret), K(task));
          }
        }
      }
    }
  }

  // 4. init result
  if (OB_SUCC(ret)) {
    bool all_settled = unsettled_helpers.empty() && unsettled_balances.empty();
    if (OB_FAIL(result.init(all_settled, unsettled_helpers, unsettled_balances, removed_cnt))) {
      LOG_WARN("fail to init tick result", KR(ret));
    }
  }
  return ret;
}

int ObStandbyTransferTaskUtil::check_single_task_settled_(
    const share::ObLSID &src_ls,
    const share::ObLSID &dest_ls,
    const share::ObAllTenantInfo &tenant_info,
    bool &settled)
{
  int ret = OB_SUCCESS;
  if (tenant_info.is_prepare_flashback_for_lossless_failover_to_primary_status()) {
    if (OB_FAIL(ObLSServiceHelper::check_transfer_task_replay_for_lossless_failover(
            src_ls, dest_ls, tenant_info, settled))) {
      LOG_WARN("fail to check replay for lossless failover", KR(ret), K(src_ls), K(dest_ls));
    }
  } else if (OB_FAIL(ObLSServiceHelper::check_transfer_task_replay(
                 src_ls, dest_ls, tenant_info, settled))) {
    LOG_WARN("fail to check transfer task replay", KR(ret), K(src_ls), K(dest_ls));
  }
  return ret;
}

int ObStandbyTransferTaskUtil::process_helper_task_(
    common::ObISQLClient &proxy,
    const share::ObBalanceTaskHelper &task,
    const share::ObAllTenantInfo &tenant_info,
    const bool do_gc,
    bool &settled)
{
  int ret = OB_SUCCESS;
  settled = false;
  if (!do_gc) {
    if (OB_FAIL(check_single_task_settled_(
            task.get_src_ls(), task.get_dest_ls(), tenant_info, settled))) {
      LOG_WARN("fail to check single task settled", KR(ret), K(task));
    }
  } else if (task.get_task_op().is_transfer_begin()) {
    if (OB_FAIL(check_transfer_begin_settled_(proxy, task, tenant_info, settled))) {
      LOG_WARN("fail to check transfer begin settled", KR(ret), K(task));
    }
  } else if (task.get_task_op().is_transfer_end()) {
    if (OB_FAIL(check_transfer_end_settled_(proxy, task, tenant_info, settled))) {
      LOG_WARN("fail to check transfer end settled", KR(ret), K(task));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected task op", KR(ret), K(task));
  }
  return ret;
}

int ObStandbyTransferTaskUtil::check_replay_after_find_(
    const share::ObBalanceTaskHelper &task,
    const share::ObAllTenantInfo &tenant_info,
    const share::SCN &target_scn,
    bool &settled)
{
  int ret = OB_SUCCESS;
  if (tenant_info.is_prepare_flashback_for_lossless_failover_to_primary_status()) {
    if (OB_FAIL(ObLSServiceHelper::check_transfer_task_replay_for_lossless_failover(
            task.get_src_ls(), task.get_dest_ls(), tenant_info, settled))) {
      LOG_WARN("failed to check transfer task replay for lossless failover",
          KR(ret), "tenant_id", tenant_info.get_tenant_id(), K(task));
    }
  } else if (settled) {
    if (OB_FAIL(ObLSServiceHelper::check_transfer_task_replay(
            tenant_info.get_tenant_id(), task.get_src_ls(), task.get_dest_ls(),
            target_scn, settled))) {
      LOG_WARN("failed to check transfer task settled",
          KR(ret), "tenant_id", tenant_info.get_tenant_id(), K(task));
    }
  }
  return ret;
}

int ObStandbyTransferTaskUtil::check_transfer_begin_settled_(
    common::ObISQLClient &proxy,
    const share::ObBalanceTaskHelper &task,
    const share::ObAllTenantInfo &tenant_info,
    bool &settled)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!task.is_valid() || !task.get_task_op().is_transfer_begin())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task));
  } else {
    share::SCN target_scn = task.get_operation_scn();
    if (OB_FAIL(resolve_transfer_begin_after_find_(proxy, task, tenant_info, target_scn, settled))) {
      LOG_WARN("fail to resolve transfer begin after find", KR(ret), K(task));
    }
    if (OB_FAIL(ret) || !settled) {
    } else if (OB_UNLIKELY(ERRSIM_BLOCK_TRANSFER_BEGIN_REMOVE)) {
      settled = false;
      LOG_WARN("ERRSIM_BLOCK_TRANSFER_BEGIN_REMOVE: block transfer begin remove",
          K(task), K(tenant_info));
    } else if (OB_FAIL(check_replay_after_find_(task, tenant_info, target_scn, settled))) {
      LOG_WARN("fail to check replay after find", KR(ret), K(task));
    } else if (settled) {
      FLOG_INFO("ls all replica replay to newest, can remove", K(task));
    } else if (!settled && REACH_THREAD_TIME_INTERVAL(10 * 1000 * 1000)) {
      LOG_WARN("can not remove ls balance task helper", K(task), K(tenant_info));
    }
  }
  return ret;
}

int ObStandbyTransferTaskUtil::resolve_transfer_begin_after_find_(
    common::ObISQLClient &proxy,
    const share::ObBalanceTaskHelper &task,
    const share::ObAllTenantInfo &tenant_info,
    share::SCN &target_scn,
    bool &settled)
{
  int ret = OB_SUCCESS;
  settled = true;
  share::ObBalanceTaskHelper transfer_end_task;
  if (OB_FAIL(share::ObBalanceTaskHelperTableOperator::try_find_transfer_end(
          tenant_info.get_tenant_id(), task.get_operation_scn(), task.get_src_ls(),
          task.get_dest_ls(), proxy, transfer_end_task))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to find transfer end task", KR(ret), "tenant_id", tenant_info.get_tenant_id(), K(task));
    } else if (FALSE_IT(ret = OB_SUCCESS)) {
    } else if (tenant_info.is_prepare_flashback_for_lossless_failover_to_primary_status()) {
    } else if (tenant_info.is_prepare_flashback_for_switch_to_primary_status()
               || tenant_info.is_prepare_flashback_for_failover_to_primary_status()) {
      if (tenant_info.get_sync_scn() != tenant_info.get_readable_scn()) {
        settled = false;
        LOG_WARN("There are transfer tasks in progress. Must wait for replay to newest",
            KR(ret), "tenant_id", tenant_info.get_tenant_id(), K(tenant_info), K(task));
      } else {
        target_scn = tenant_info.get_sync_scn();
        LOG_INFO("replay to newest, can remove transfer begin before switchover/failover to primary",
            K(tenant_info), K(task));
      }
    } else {
      settled = false;
      LOG_WARN("can not find transfer end task, can not end transfer begin task",
          KR(ret), K(tenant_info), K(task));
    }
  } else {
    LOG_INFO("has transfer end task, can remove transfer begin", K(task), K(transfer_end_task));
  }
  return ret;
}

int ObStandbyTransferTaskUtil::check_transfer_end_settled_(
    common::ObISQLClient &proxy,
    const share::ObBalanceTaskHelper &task,
    const share::ObAllTenantInfo &tenant_info,
    bool &settled)
{
  int ret = OB_SUCCESS;
  settled = false;
  if (OB_UNLIKELY(!task.is_valid() || !task.get_task_op().is_transfer_end())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task));
  } else {
    share::ObBalanceTaskHelper transfer_begin_task;
    if (OB_FAIL(share::ObBalanceTaskHelperTableOperator::try_find_transfer_begin(
            tenant_info.get_tenant_id(), task.get_operation_scn(), task.get_src_ls(),
            task.get_dest_ls(), proxy, transfer_begin_task))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        settled = true;
      } else {
        LOG_WARN("failed to find transfer begin task", KR(ret), "tenant_id", tenant_info.get_tenant_id(), K(task));
      }
    } else {
      settled = false;
      LOG_WARN("transfer_begin still exists, cannot remove transfer_end",
          K(task), K(transfer_begin_task));
    }
    if (OB_FAIL(ret) || !settled) {
    } else if (OB_FAIL(check_replay_after_find_(task, tenant_info,
            task.get_operation_scn(), settled))) {
      LOG_WARN("fail to check replay after find", KR(ret), K(task));
    }
  }
  return ret;
}

} // namespace rootserver
} // namespace oceanbase
