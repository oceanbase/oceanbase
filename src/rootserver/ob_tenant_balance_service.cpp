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

#define USING_LOG_PREFIX BALANCE
#include "ob_tenant_balance_service.h"
#include "share/schema/ob_schema_getter_guard.h"//ObSchemaGetGuard
#include "share/schema/ob_schema_struct.h"//ObTenantInfo
#include "share/schema/ob_multi_version_schema_service.h"//ObMultiSchemaService
#include "share/ob_unit_table_operator.h" //ObUnitTableOperator
#include "share/balance/ob_balance_job_table_operator.h"//ObBalanceJob
#include "share/balance/ob_balance_task_table_operator.h"//ObBalanceTask
#include "share/ob_primary_zone_util.h"//get_primary_zone
#include "share/rc/ob_tenant_base.h"//MTL
#include "rootserver/ob_ls_balance_helper.h"//ObLSBalanceTaskHelper
#include "rootserver/ob_ls_service_helper.h"//ObLSServiceHelper
#include "rootserver/ob_balance_ls_primary_zone.h"//ObBalanceLSPrimaryZone
#include "observer/ob_server_struct.h"//GCTX
#include "rootserver/ob_partition_balance.h" // partition balance
#include "storage/tablelock/ob_lock_utils.h" // ObInnerTableLockUtil
#include "share/ob_cluster_version.h"
#include "share/ob_share_util.h" // ObShareUtil
#include "share/transfer/ob_transfer_task_operator.h"

#define ISTAT(fmt, args...) FLOG_INFO("[TENANT_BALANCE] " fmt, ##args)
#define WSTAT(fmt, args...) FLOG_WARN("[TENANT_BALANCE] " fmt, ##args)


namespace oceanbase
{
using namespace common;
using namespace share;
using namespace transaction::tablelock;

namespace rootserver
{
//////////////ObTenantBalanceService
int ObTenantBalanceService::init()
{
  int ret = OB_SUCCESS;
  tenant_id_ = MTL_ID();
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("has inited", KR(ret));
  } else if (OB_FAIL(ObTenantThreadHelper::create("TBalance",
          lib::TGDefIDs::SimpleLSService, *this))) {
    LOG_WARN("failed to create thread", KR(ret));
  } else if (OB_FAIL(ObTenantThreadHelper::start())) {
    LOG_WARN("fail to start thread", KR(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

void ObTenantBalanceService::destroy()
{
  ObTenantThreadHelper::destroy();
  tenant_id_ = OB_INVALID_TENANT_ID;
  inited_ = false;
}

// enable_balance = true, enable_transfer = true: balance with LS dynamic change
// enable_balance = true, enable_transfer = false: balance without LS dynamic change
// enable_balance = false, enable_transfer does not take effect: do not balance
void ObTenantBalanceService::do_work()
{
  int ret = OB_SUCCESS;
  ISTAT("tenant balance thread", K(tenant_id_));
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(wait_tenant_schema_and_version_ready_(tenant_id_, DATA_VERSION_4_2_0_0))) {
    LOG_WARN("failed to wait tenant schema version ready", KR(ret), K(tenant_id_), K(DATA_CURRENT_VERSION));
  } else {
    int64_t idle_time_us = 10 * 1000 * 1000L;
    int tmp_ret = OB_SUCCESS;
    int64_t job_cnt = 0;
    int64_t last_partition_balance_time = ObTimeUtility::current_time();
    int64_t last_statistic_bg_stat_time = OB_INVALID_TIMESTAMP; // statistic once when thread starts
    int64_t last_statistic_schema_version = OB_INVALID_VERSION;
    ObTransferTaskID last_statistic_max_transfer_task_id;
    while (!has_set_stop()) {
      ObCurTraceId::init(GCONF.self_addr_);
      reset();
      DEBUG_SYNC(BEFORE_TENANT_BALANCE_SERVICE);
      if (OB_FAIL(gather_stat_())) {
        LOG_WARN("failed to gather stat", KR(ret));
      } else if (OB_FAIL(try_process_current_job(job_cnt))) {
        LOG_WARN("failed to process current job", KR(ret));
      } else if (0 == job_cnt && ObShareUtil::is_tenant_enable_rebalance(tenant_id_)) {
        //check ls status is match with __all_ls
        //TODO
        if (ObShareUtil::is_tenant_enable_transfer(tenant_id_)) {
          if (OB_FAIL(gather_ls_status_stat(tenant_id_, ls_array_))) {
            LOG_WARN("failed to gather ls status", KR(ret), K(tenant_id_), K(ls_array_));
          } else if (OB_FAIL(ls_balance_(job_cnt))) {
            LOG_WARN("failed to do ls balance", KR(ret));
          } else if (0 == job_cnt) {
            if (OB_FAIL(try_do_partition_balance_(last_partition_balance_time))) {
              LOG_WARN("try do partition balance failed", KR(ret), K(last_partition_balance_time));
            }
          }
        } else { // disable transfer
          ObTenantSchema tenant_schema_copy;
          if (OB_FAIL(get_tenant_schema(tenant_id_, tenant_schema_copy))) {
            LOG_WARN("failed to get tenant schema", KR(ret), K(tenant_id_));
          } else {
            bool is_balanced = false;
            bool need_execute_balance = true;
            ObTenantLSInfo tenant_info(GCTX.sql_proxy_, &tenant_schema_copy, tenant_id_);
            if (OB_FAIL(ObLSServiceHelper::balance_ls_group(need_execute_balance, tenant_info, is_balanced))) {
              LOG_WARN("failed to balance ls group", KR(ret));
            }
          }
        }
      }

      // separate statistic to avoid affecting balance jobs
      // statistics balance group status periodically when tenant schema version changes or transfer occurs
      if (OB_FAIL(ret)) {
      } else if (OB_TMP_FAIL(try_statistic_balance_group_status_(
          last_statistic_bg_stat_time,
          last_statistic_schema_version,
          last_statistic_max_transfer_task_id))) {
        LOG_WARN("try statistic balance group status failed", KR(tmp_ret), K(last_statistic_bg_stat_time),
            K(last_statistic_schema_version), K(last_statistic_max_transfer_task_id));
      }

      if (OB_FAIL(ret) && OB_NEED_WAIT != ret) {
        idle_time_us = 100 * 1000;
      } else {
        omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
        idle_time_us = tenant_config.is_valid() ? tenant_config->balancer_idle_time : 10 * 1000 * 1000;
      }
      ISTAT("finish one round", KR(ret), KR(tmp_ret), K_(tenant_id), K(job_cnt),
                K(primary_zone_num_), K(unit_group_array_),
                K(ls_array_), K(idle_time_us), K(last_partition_balance_time), K(last_statistic_bg_stat_time),
                K(last_statistic_schema_version), K(last_statistic_max_transfer_task_id),
                "enable_rebalance", ObShareUtil::is_tenant_enable_rebalance(tenant_id_),
                "enable_transfer", ObShareUtil::is_tenant_enable_transfer(tenant_id_));
      reset();
      idle(idle_time_us);
    }// end while
  }
}

int ObTenantBalanceService::gather_stat_primary_zone_num_and_units(
    const uint64_t &tenant_id,
    int64_t &primary_zone_num,
    ObIArray<share::ObSimpleUnitGroup> &unit_group_array)
{
  int ret = OB_SUCCESS;
  unit_group_array.reset();
  primary_zone_num = 0;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_) || OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP(GCTX.sql_proxy_), KP(GCTX.schema_service_));
  } else {
    //get primary zone
    share::schema::ObTenantSchema tenant_schema;
    ObArray<ObZone> primary_zone;
    if (OB_FAIL(get_tenant_schema(tenant_id, tenant_schema))) {
      LOG_WARN("failed to get tenant schema", KR(ret), K(tenant_id));
    } else if (!tenant_schema.is_normal()) {
      //already wait tenant ready, must be normal
      ret = OB_ERR_UNEXPECTED;
      WSTAT("tenant schema not ready is unexpected", KR(ret));
    } else if (OB_FAIL(ObLSServiceHelper::get_primary_zone_unit_array(&tenant_schema,
            primary_zone, unit_group_array))) {
      LOG_WARN("failed to get primary zone unit array", KR(ret), K(tenant_schema));
    } else {
      primary_zone_num = primary_zone.count();
    }
  }
  return ret;
}

int ObTenantBalanceService::gather_stat_()
{
  int ret = OB_SUCCESS;
  ATOMIC_SET(&loaded_, false);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(gather_stat_primary_zone_num_and_units(
      tenant_id_,
      primary_zone_num_,
      unit_group_array_))) {
    LOG_WARN("fail to execute gather_stat_primary_zone_num_and_units", KR(ret), K(tenant_id_));
  } else {
    ATOMIC_SET(&loaded_, true);
  }
  return ret;
}

int ObTenantBalanceService::gather_ls_status_stat(const uint64_t &tenant_id, share::ObLSStatusInfoArray &ls_array)
{
  int ret = OB_SUCCESS;
  ls_array.reset();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP(GCTX.sql_proxy_));
  } else {
    //get ls status info
    //must remove ls group id = 0, those ls no need balance, such as sys ls and duplicate ls
    ObLSStatusOperator status_op;
    ObLSAttrOperator ls_op(tenant_id, GCTX.sql_proxy_);
    share::ObLSAttrArray ls_attr_array;
    if (OB_FAIL(status_op.get_all_ls_status_by_order(tenant_id, ls_array, *GCTX.sql_proxy_))) {
      LOG_WARN("failed to get status by order", KR(ret), K(tenant_id));
    } else if (OB_FAIL(ls_op.get_all_ls_by_order(ls_attr_array))) {
      LOG_WARN("failed to get ls attr array", KR(ret));
    } else if (ls_attr_array.count() > ls_array.count()) {
      //only ls status has more ls, such as some ls is waitoffline
      ret = OB_NEED_WAIT;
      WSTAT("has ls need create", KR(ret), K(ls_attr_array));
    }
    int64_t attr_index = ls_attr_array.count() - 1;
    bool need_remove_ls = false;
    for (int64_t i = ls_array.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
      if (attr_index < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls attr array is unexpected", KR(ret), K(i), K(ls_attr_array));
      } else {
        const ObLSStatusInfo &status_info = ls_array.at(i);
        const ObLSAttr &ls_info = ls_attr_array.at(attr_index);
        need_remove_ls = false;
        if (status_info.ls_id_ == ls_info.get_ls_id()) {
          // check ls status and ls group id;
          attr_index--;
          if (status_info.ls_group_id_ != ls_info.get_ls_group_id()) {
            // need primary ls service work
            ret = OB_NEED_WAIT;
            WSTAT("has ls need modify ls group, can not balance", KR(ret),
                  K(ls_info), K(status_info));
          } else if (status_info.status_ != ls_info.get_ls_status() ||
                     (!status_info.ls_is_normal() &&
                      !status_info.ls_is_dropping())) {
            ret = OB_NEED_WAIT;
            WSTAT("ls status not ready, can not balance", KR(ret), K(ls_info),
                  K(status_info));
          } else if (0 == status_info.ls_group_id_ || status_info.ls_is_dropping()) {
            //ls has no ls group such as sys ls, or ls is in dropping, can not fallback, no need to takecare
            need_remove_ls = true;
          }
        } else if (status_info.ls_id_ > ls_info.get_ls_id()) {
          // ls must be in wait offline, ls in __all_ls_stauts but not in __all_ls, must be waitoffline, need remove
          need_remove_ls = true;
          if (!status_info.ls_is_wait_offline()) {
            ret = OB_ERR_UNEXPECTED;
            WSTAT("ls status not expected", KR(ret), K(status_info), K(ls_info),
                  K(ls_array), K(ls_attr_array));
          }
        } else {
          // ls in status can not large than in __all_ls by order
          ret = OB_NEED_WAIT;
          WSTAT("has ls need create, can not balance", KR(ret), K(ls_info),
                K(status_info));
        }
        if (OB_SUCC(ret) && need_remove_ls) {
          ISTAT("LS no need balance", "ls_status", ls_array.at(i));
          if (OB_FAIL(ls_array.remove(i))) {
            LOG_WARN("failed to remvoe no ls group ls", KR(ret), K(i));
          }
        }
      }
    }
  }
  return ret;
}

int ObTenantBalanceService::is_ls_balance_finished(const uint64_t &tenant_id, bool &is_finished)
{
  int ret = OB_SUCCESS;
  bool is_primary = true;
  is_finished = false;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant or not user tenant", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (ObAllTenantInfoProxy::is_primary_tenant(GCTX.sql_proxy_, tenant_id, is_primary)) {
    LOG_WARN("fail to execute is_primary_tenant", KR(ret), K(tenant_id));
  } else if (is_primary && ObShareUtil::is_tenant_enable_transfer(tenant_id)) {
    if (OB_FAIL(is_primary_tenant_ls_balance_finished_(tenant_id, is_finished))) {
      LOG_WARN("fail to execute is_primary_tenant_ls_balance_finished_", KR(ret), K(tenant_id));
    }
  } else {
    // standby & restore & primary tenant and enable_transfer=false
    if (OB_FAIL(is_standby_tenant_ls_balance_finished_(tenant_id, is_finished))) {
      LOG_WARN("fail to execute is_standby_tenant_ls_balance_finished_", KR(ret), K(tenant_id));
    }
  }
  LOG_TRACE("check whether the tenant has balanced ls", K(ret), K(tenant_id), K(is_primary), K(is_finished));
  return ret;
}

int  ObTenantBalanceService::is_primary_tenant_ls_balance_finished_(
    const uint64_t &tenant_id,
    bool &is_finished)
{
  int ret = OB_SUCCESS;
  int64_t job_cnt = 1;
  int64_t start_time = OB_INVALID_TIMESTAMP, finish_time = OB_INVALID_TIMESTAMP;
  ObBalanceJob job;
  ObLSBalanceTaskHelper ls_balance_helper;
  bool need_ls_balance = false;
  int64_t primary_zone_num = 0;
  share::ObLSStatusInfoArray ls_array;
  ObArray<share::ObSimpleUnitGroup> unit_group_array;
  is_finished = false;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant or not user tenant", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObBalanceJobTableOperator::get_balance_job(
      tenant_id, false, *GCTX.sql_proxy_, job, start_time, finish_time))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      job_cnt = 0;
    } else {
      LOG_WARN("fail to get balance job", KR(ret), K(tenant_id));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (0 != job_cnt) {
    is_finished= false;
  } else if (OB_FAIL(gather_ls_status_stat(tenant_id, ls_array))) {
    LOG_WARN("fail to execute gather_ls_status_stat", KR(ret), K(tenant_id));
  } else if (OB_FAIL(gather_stat_primary_zone_num_and_units(tenant_id, primary_zone_num, unit_group_array))) {
    LOG_WARN("fail to execute gather_stat_primary_zone_num_and_units", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ls_balance_helper.init(
      tenant_id, ls_array, unit_group_array, primary_zone_num, GCTX.sql_proxy_))) {
    LOG_WARN("failed to init ls balance helper", KR(ret), K(ls_array), K(unit_group_array),
        K(primary_zone_num), K(tenant_id));
  } else if (OB_FAIL(ls_balance_helper.check_need_ls_balance(need_ls_balance))) {
    LOG_WARN("failed to check_ls need balance", KR(ret));
  } else {
    is_finished = !need_ls_balance;
  }
  LOG_INFO("check whether the primary_tenant has balanced ls", KR(ret), K(tenant_id), K(ls_array),
      K(primary_zone_num), K(unit_group_array), K(need_ls_balance));
  return ret;
}

int ObTenantBalanceService::is_standby_tenant_ls_balance_finished_(
    const uint64_t &tenant_id,
    bool &is_finished)
{
  int ret = OB_SUCCESS;
  ObTenantSchema tenant_schema;
  is_finished = false;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant or not user tenant", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObTenantThreadHelper::get_tenant_schema(tenant_id, tenant_schema))) {
    LOG_WARN("failed to get tenant schema", KR(ret), K(tenant_id));
  } else {
    ObTenantLSInfo tenant_info(GCTX.sql_proxy_, &tenant_schema, tenant_id);
    bool need_execute_balance = false;
    if (OB_FAIL(ObLSServiceHelper::balance_ls_group(need_execute_balance, tenant_info, is_finished))) {
      LOG_WARN("failed to balance ls group", KR(ret), K(tenant_info));
    }
    LOG_INFO("check whether the non_primary_tenant has balanced ls", KR(ret), K(tenant_id), K(tenant_info));
  }
  return ret;
}

int ObTenantBalanceService::try_process_current_job(int64_t &job_cnt)
{
  int ret = OB_SUCCESS;
  job_cnt = 0;
  int64_t start_time = OB_INVALID_TIMESTAMP, finish_time = OB_INVALID_TIMESTAMP;
  ObBalanceJob job;
  bool job_need_cancel = false;
  ObSqlString comment;
  if (OB_UNLIKELY(!inited_ || !ATOMIC_LOAD(&loaded_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(inited_), K(loaded_));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(ObBalanceJobTableOperator::get_balance_job(
                 tenant_id_, false, *GCTX.sql_proxy_, job, start_time, finish_time))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      //NO JOB, need check current ls status
      ret = OB_SUCCESS;
      job_cnt = 0;
    } else {
      LOG_WARN("failed to get balance job", KR(ret), K(tenant_id_));
    }
  } else if (OB_FAIL(try_finish_current_job_(job, job_cnt))) {
    LOG_WARN("failed to finish current job", KR(ret), K(job));
  } else if (0 == job_cnt) {
    //You can check the status of the current ls to
    //decide whether you want to continue generating tasks
  } else if (job.get_job_status().is_canceling()) {
    //job already abort, no need to do
  } else if (OB_FAIL(check_ls_job_need_cancel_(job, job_need_cancel, comment))) {
    LOG_WARN("failed to check exist job need continue", KR(ret), K(job));
  } else if (job_need_cancel) {
    int tmp_ret = OB_SUCCESS;
    if (OB_FAIL(ObBalanceJobTableOperator::update_job_status(tenant_id_,
            job.get_job_id(), job.get_job_status(),
            ObBalanceJobStatus(ObBalanceJobStatus::BALANCE_JOB_STATUS_CANCELING),
            true, comment.string(),
            *GCTX.sql_proxy_))) {
      LOG_WARN("failed to update job status", KR(ret), K(tenant_id_), K(job), K(comment));
    }
  }
  return ret;
}

int ObTenantBalanceService::ls_balance_(int64_t &job_cnt)
{
  int ret = OB_SUCCESS;
  job_cnt = 0;
  ObLSBalanceTaskHelper ls_balance_helper;
  bool need_ls_balance = false;
  if (OB_UNLIKELY(!inited_ || !ATOMIC_LOAD(&loaded_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(inited_), K(loaded_));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP(GCTX.sql_proxy_));
  } else {
    //Build the current ls group, check if the ls group matches the primary_zone, unit_group
    //If it is a partition_balance task and ls_balance is currently required, the current task needs to be cancelled
    if (OB_FAIL(ls_balance_helper.init(tenant_id_, ls_array_, unit_group_array_,
                               primary_zone_num_, GCTX.sql_proxy_))) {
      LOG_WARN("failed to init ls balance helper", KR(ret), K(ls_array_), K(unit_group_array_),
                                                   K(primary_zone_num_), K(tenant_id_));
    } else if (OB_FAIL(ls_balance_helper.check_need_ls_balance(need_ls_balance))) {
      LOG_WARN("failed to check_ls need balance", KR(ret));
    }
  }

  if (OB_SUCC(ret) && need_ls_balance) {
    if (OB_FAIL(ls_balance_helper.generate_ls_balance_task())) {
      LOG_WARN("failed to generate task", KR(ret));
    } else if (OB_FAIL(persist_job_and_task_(
                   ls_balance_helper.get_balance_job(),
                   ls_balance_helper.get_balance_tasks()))) {
      LOG_WARN("failed to persist balance task and job", KR(ret),
      "job", ls_balance_helper.get_balance_job(),
      "tasks", ls_balance_helper.get_balance_tasks());
    } else {
      job_cnt = 1;
    }
  }
  ISTAT("finish ls balance", KR(ret), K(need_ls_balance),
           "job", ls_balance_helper.get_balance_job(), "tasks",
           ls_balance_helper.get_balance_tasks());
  return ret;
}

int ObTenantBalanceService::partition_balance_(bool enable_transfer)
{
  int ret = OB_SUCCESS;
  ObPartitionBalance partition_balance;
  int64_t active_unit_num = 0;
  if (OB_UNLIKELY(!inited_ || !ATOMIC_LOAD(&loaded_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(inited_), K(loaded_));
  } else if (OB_ISNULL(GCTX.sql_proxy_) || OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP(GCTX.sql_proxy_), KP(GCTX.schema_service_));
  } else if (OB_FAIL(get_active_unit_num_(active_unit_num))) {
    LOG_WARN("failed to get active unit num", KR(ret));
  } else if (OB_FAIL(partition_balance.init(tenant_id_, GCTX.schema_service_, GCTX.sql_proxy_,
          primary_zone_num_, active_unit_num,
          enable_transfer ? ObPartitionBalance::GEN_TRANSFER_TASK : ObPartitionBalance::GEN_BG_STAT))) {
    LOG_WARN("fail to init partition balance", KR(ret), K(tenant_id_), K(primary_zone_num_),
        K(active_unit_num), K(enable_transfer));
  } else if (OB_FAIL(partition_balance.process())) {
    LOG_WARN("fail to process partition_balance", KR(ret));
  } else if (partition_balance.get_balance_task().empty()) {
    ISTAT("partition balance generate empty task");
  } else if (OB_FAIL(persist_job_and_task_(partition_balance.get_balance_job(), partition_balance.get_balance_task()))) {
    LOG_WARN("fail to persist_job_and_task", KR(ret), "job", partition_balance.get_balance_job(), "tasks", partition_balance.get_balance_task());
  } else {
    ISTAT("partition balance generate task", "job", partition_balance.get_balance_job(), "tasks", partition_balance.get_balance_task());
  }
  return ret;
}

int ObTenantBalanceService::try_finish_current_job_(const share::ObBalanceJob &job, int64_t &job_cnt)
{
  int ret = OB_SUCCESS;
  int64_t task_cnt = 0;
  bool can_clean_job = false;
  if (OB_UNLIKELY(!inited_ || ! ATOMIC_LOAD(&loaded_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(inited_), K(loaded_));
  } else if (OB_UNLIKELY(!job.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("job is invalid", KR(ret), K(job));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (job.get_job_status().is_canceled() || job.get_job_status().is_success()) {
    can_clean_job = true;
  } else if (OB_FAIL(ObBalanceTaskTableOperator::get_job_task_cnt(tenant_id_, job.get_job_id(),
                                          task_cnt, *GCTX.sql_proxy_))) {
    LOG_WARN("failed to get job task cnt", KR(ret), K(job), K(tenant_id_));
  } else if (0 != task_cnt) {
    can_clean_job = false;
    ISTAT("job has not finish task, cannot finish", K(task_cnt), K(job));
  } else {
    ObBalanceJobStatus new_status;
    can_clean_job = true;
    ObString comment;
    if (job.get_job_status().is_doing()) {
      new_status = ObBalanceJobStatus(share::ObBalanceJobStatus::BALANCE_JOB_STATUS_COMPLETED);
    } else if (job.get_job_status().is_canceling()) {
      new_status = ObBalanceJobStatus(share::ObBalanceJobStatus::BALANCE_JOB_STATUS_CANCELED);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("job status not expected", KR(ret), K(job));
    }
    if (FAILEDx(ObBalanceJobTableOperator::update_job_status(tenant_id_, job.get_job_id(),
                         job.get_job_status(), new_status, false, comment,
                         *GCTX.sql_proxy_))) {
      LOG_WARN("failed to update job status", KR(ret), K(tenant_id_), K(job), K(new_status));
    }
  }
  if (OB_SUCC(ret)) {
    if (!can_clean_job) {
      job_cnt = 1;
    } else if (OB_FAIL(ObBalanceJobTableOperator::clean_job(tenant_id_, job.get_job_id(), *GCTX.sql_proxy_))) {
      LOG_WARN("failed to clean job", KR(ret), K(tenant_id_), K(job));
    } else {
      ISTAT("clean the job", K(task_cnt), K(job));
      job_cnt = 0;
    }
  }
  return ret;
}

int ObTenantBalanceService::get_active_unit_num_(int64_t &active_unit_num) const
{
  int ret = OB_SUCCESS;
  active_unit_num = 0;
  if (OB_UNLIKELY(!inited_ || ! ATOMIC_LOAD(&loaded_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(inited_), K(loaded_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < unit_group_array_.count(); ++i) {
      if (unit_group_array_.at(i).is_active()) {
        active_unit_num++;
      }
    }

  }
  return ret;
}

int ObTenantBalanceService::check_ls_job_need_cancel_(const share::ObBalanceJob &job,
                                bool &need_cancel,
                                ObSqlString &comment)
{
  int ret = OB_SUCCESS;
  need_cancel = false;
  comment.reset();
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || ! ATOMIC_LOAD(&loaded_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(inited_), K(loaded_));
  } else if (OB_UNLIKELY(!job.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("job is invalid", KR(ret), K(job));
  } else if (!ObShareUtil::is_tenant_enable_transfer(tenant_id_)) {
    need_cancel = true;
    if (OB_TMP_FAIL(comment.assign_fmt("Canceled due to tenant balance or transfer is disabled"))) {
      LOG_WARN("failed to assign fmt", KR(tmp_ret), K(job));
    }
    ISTAT("tenant balance or transfer is disabled, need cancel current job", K(job), K(comment),
        "enable_balance", ObShareUtil::is_tenant_enable_transfer(tenant_id_),
        "enable_transfer", ObShareUtil::is_tenant_enable_transfer(tenant_id_));
  } else if (job.get_primary_zone_num() != primary_zone_num_) {
    need_cancel = true;
    if (OB_TMP_FAIL(comment.assign_fmt("Canceled due to primary zone num change from %ld to %ld",
                      job.get_primary_zone_num(), primary_zone_num_))) {
      LOG_WARN("failed to assign fmt", KR(tmp_ret), K(job), K(primary_zone_num_));
    }
    ISTAT("primary zone num change, need cancel current job", K(primary_zone_num_), K(job), K(comment));
  } else {
    int64_t active_unit_num = 0;
    if (OB_FAIL(get_active_unit_num_(active_unit_num))) {
      LOG_WARN("failed to get active unit num", KR(ret));
    } else if (job.get_unit_group_num() != active_unit_num) {
      need_cancel = true;
      if (OB_TMP_FAIL(comment.assign_fmt("Canceled due to unit num change from %ld to %ld",
              job.get_unit_group_num(), active_unit_num))) {
        LOG_WARN("failed to assign fmt", KR(tmp_ret), K(job), K(active_unit_num));
      }
      ISTAT("unit group num change, need cancel current job",
      K(active_unit_num), K(job), K(unit_group_array_), K(comment));
    }
  }
  return ret;
}

void ObTenantBalanceService::reset()
{
  loaded_ = false;
  unit_group_array_.reset();
  ls_array_.reset();
  primary_zone_num_ = OB_INVALID_COUNT;

}

int ObTenantBalanceService::persist_job_and_task_(const share::ObBalanceJob &job,
                                                  ObArray<share::ObBalanceTask> &tasks)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || ! ATOMIC_LOAD(&loaded_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(inited_), K(loaded_));
  } else if (OB_UNLIKELY(!job.is_valid() || 0 == tasks.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("job or task is invalid", KR(ret), K(job), K(tasks));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(construct_dependency_of_each_task_(tasks))) {
    LOG_WARN("failed to generate dependency task", KR(ret), K(tasks));
  } else {
    common::ObMySQLTransaction trans;
    if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id_))) {
      LOG_WARN("failed to start trans", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(lock_and_check_balance_job_(trans, tenant_id_))) {
      LOG_WARN("lock and check balance job failed", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(ObBalanceJobTableOperator::insert_new_job(job, trans))) {
      LOG_WARN("failed to insert new job", KR(ret), K(job));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
      if (OB_FAIL(ObBalanceTaskTableOperator::insert_new_task(tasks.at(i),
                                                              trans))) {
        LOG_WARN("failed to insert new task", KR(ret), K(i), K(tasks));
      }
    }
    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

int ObTenantBalanceService::lock_and_check_balance_job_(
    common::ObMySQLTransaction &trans,
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObBalanceJob job;
  int64_t start_time = 0; // useless
  int64_t finish_time = 0; // useless
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObInnerTableLockUtil::lock_inner_table_in_trans(
      trans,
      tenant_id,
      OB_ALL_BALANCE_JOB_TID,
      EXCLUSIVE))) {
    LOG_WARN("lock inner table failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObBalanceJobTableOperator::get_balance_job(
      tenant_id,
      false,
      trans,
      job,
      start_time,
      finish_time))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      ISTAT("no job in table, check empty successfully", KR(ret), K(tenant_id));
    } else {
      LOG_WARN("failed to get balance job", KR(ret), K(tenant_id));
    }
  } else {
    ret = OB_ENTRY_EXIST;
    LOG_WARN("there should be no job in table", KR(ret), K(job));
  }
  if (OB_SUCC(ret)) {
    DEBUG_SYNC(AFTER_LOCK_ALL_BALANCE_JOB);
  }
  return ret;
}

#define SET_TASK_DEPENDENCY(ls_id)                                        \
  do {                                                                    \
    if (OB_FAIL(ret)) {                                                   \
    } else if (ls_id.is_valid()) {                                        \
      if (OB_SUCC(ls_task_map.get_refactored(ls_id, parent_task))) {      \
        overwrite = true;                                                 \
        if (OB_FAIL(task.get_parent_task_list().push_back(                \
                parent_task->get_balance_task_id()))) {              \
          LOG_WARN("failed to set parent", KR(ret), KPC(parent_task));    \
        } else if (OB_FAIL(parent_task->get_child_task_list().push_back(  \
                       task.get_balance_task_id()))) {               \
          LOG_WARN("failed to set child parent", KR(ret), K(task));       \
        }                                                                 \
      } else if (OB_HASH_NOT_EXIST == ret) {                              \
        overwrite = false;                                                \
        ret = OB_SUCCESS;                                                 \
      } else {                                                            \
        LOG_WARN("failed to get refactor", KR(ret), K(ls_id));            \
      }                                                                   \
      if (FAILEDx(ls_task_map.set_refactored(ls_id, &task, overwrite))) { \
        LOG_WARN("failed to set refactor", KR(ret), K(ls_id));            \
      }                                                                   \
    }                                                                     \
  } while (0)

int ObTenantBalanceService::construct_dependency_of_each_task_(
   ObArray<share::ObBalanceTask> &tasks)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(tasks.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tasks));
  } else {
    hash::ObHashMap<ObLSID, ObBalanceTask *> ls_task_map;
    ObBalanceTask *parent_task = NULL;
    bool overwrite = false;
    if (OB_FAIL(ls_task_map.create(tasks.count(), lib::ObLabel("BalTask")))) {
      LOG_WARN("failed to create hashmap", KR(ret), K(tasks));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
      ObBalanceTask &task = tasks.at(i);
      ObLSID src_ls = task.get_src_ls_id();
      ObLSID dest_ls = task.get_dest_ls_id();
      SET_TASK_DEPENDENCY(src_ls);
      if (src_ls != dest_ls) {
        //ls_alter maybe src_ls equal to dest_ls
        SET_TASK_DEPENDENCY(dest_ls);
      }
    }
  }
  return ret;
}

int ObTenantBalanceService::try_do_partition_balance_(int64_t &last_partition_balance_time)
{
  int ret = OB_SUCCESS;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!tenant_config.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant config is invalid", KR(ret), K_(tenant_id));
  } else {
    const int64_t curr_time = ObTimeUtility::current_time();
    const int64_t interval = tenant_config->partition_balance_schedule_interval;
    if (0 == interval) {
      // 0 means partition balance is disabled
    } else if (curr_time - last_partition_balance_time > interval) {
      if (OB_FAIL(partition_balance_(true/*need_balance*/))) { // generate transfer job
        LOG_WARN("failed to do partition balance",
            KR(ret), K(curr_time), K(last_partition_balance_time));
      } else {
        LOG_INFO("do partition balance successfully", KR(ret),
            K(interval), K(curr_time), K(last_partition_balance_time));
        last_partition_balance_time = curr_time;
      }
    }
  }
  return ret;
}

// when running normally, it will statistic balance group status every 10min when tenant schema version changes or transfer occurs
// when thread starts, it will try to statistic bg stat every 10s until it is successful
int ObTenantBalanceService::try_statistic_balance_group_status_(
    int64_t &last_statistic_bg_stat_time,
    int64_t &last_statistic_schema_version,
    ObTransferTaskID &last_statistic_max_transfer_task_id)
{
  int ret = OB_SUCCESS;
  const int64_t curr_time = ObTimeUtility::current_time();
  const int64_t STATISTIC_BG_STAT_INTERVAL = 600 * 1000 * 1000L; // 10min
  ObRefreshSchemaStatus schema_status;
  schema_status.tenant_id_ = tenant_id_;
  int64_t latest_tenant_schema_version = OB_INVALID_VERSION;
  ObTransferTaskID latest_max_transfer_task_id; // default -1
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(GCTX.sql_proxy_) || OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX has null ptr", KR(ret), KP(GCTX.sql_proxy_), KP(GCTX.schema_service_));
  } else if (last_statistic_schema_version > OB_INVALID_VERSION
      && curr_time - last_statistic_bg_stat_time < STATISTIC_BG_STAT_INTERVAL) {
    // no need to statistic because interval is not reached
  } else if (OB_FAIL(GCTX.schema_service_->get_schema_version_in_inner_table(
      *GCTX.sql_proxy_,
      schema_status,
      latest_tenant_schema_version))) {
    LOG_WARN("failed to get schema version in inner table", KR(ret), K(schema_status));
  } else if (OB_FAIL(ObTransferTaskOperator::get_max_task_id_from_history(
      *GCTX.sql_proxy_,
      tenant_id_,
      latest_max_transfer_task_id))) { // -1 when transfer history is empty
    LOG_WARN("get max transfer task if from history failed",
        KR(ret), K_(tenant_id), K(latest_max_transfer_task_id));
  } else if (latest_tenant_schema_version <= last_statistic_schema_version
      && latest_max_transfer_task_id <= last_statistic_max_transfer_task_id) {
    // no need to statistics because distribution of tablets is not changed
  } else if (OB_FAIL(partition_balance_(false/*need_balance*/))) { // just statistic balance group status
    LOG_WARN("failed to save balance group status",
        KR(ret), K(curr_time), K(last_statistic_bg_stat_time));
  } else {
    ISTAT("statistic balance group status successfully", K(curr_time), K(last_statistic_bg_stat_time),
        K(latest_tenant_schema_version), K(last_statistic_schema_version),
        K(latest_max_transfer_task_id), K(last_statistic_max_transfer_task_id));
    last_statistic_bg_stat_time = curr_time;
    last_statistic_schema_version = latest_tenant_schema_version;
    last_statistic_max_transfer_task_id = latest_max_transfer_task_id;
  }
  return ret;
}

#undef SET_TASK_DEPENDENCY
#undef ISTAT
#undef WSTAT
#undef STAT
}
}
