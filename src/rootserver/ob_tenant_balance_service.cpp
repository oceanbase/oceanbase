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
#include "rootserver/ob_ls_balance_helper.h"//ObLSBalanceTaskHelper
#include "share/balance/ob_balance_task_table_operator.h"//ObBalanceTask
#include "share/rc/ob_tenant_base.h"//MTL
#include "rootserver/ob_ls_service_helper.h"//ObLSServiceHelper
#include "rootserver/ob_ls_balance_helper.h"//ObLSBalanceTaskHelper
#include "rootserver/ob_transfer_partition_task.h"//ObTransferPartitionHelper
#include "rootserver/ob_partition_balance.h" // ObPartitionBalance
#include "observer/ob_server_struct.h"//GCTX
#include "rootserver/tenant_snapshot/ob_tenant_snapshot_util.h" //ObTenantSnapshotUtil
#include "rootserver/ob_balance_task_execute_service.h"//ObBalanceTaskExecuteService
#include "rootserver/balance/ob_object_balance_weight_mgr.h" // ObObjectBalanceWeightMgr
#include "storage/tablelock/ob_lock_utils.h" // ObInnerTableLockUtil
#include "share/transfer/ob_transfer_task_operator.h"
#include "rootserver/ob_balance_ls_primary_zone.h" // ObBalanceLSPrimaryZone

#define ISTAT(fmt, args...) FLOG_INFO("[TENANT_BALANCE] " fmt, ##args)
#define WSTAT(fmt, args...) FLOG_WARN("[TENANT_BALANCE] " fmt, ##args)
#define CHECK_OB_ARRAY_EQUAL(arr1, arr2, log_info) \
do { \
  if (OB_FAIL(ret)) { \
  } else if (arr1.count() != arr2.count()) { \
    ret = OB_NEED_RETRY; \
    LOG_WARN(log_info, KR(ret), K(arr1), K(arr2)); \
  } else { \
    for (int64_t i = 0; OB_SUCC(ret) && i < arr1.count(); ++i) { \
      if (arr1.at(i) != arr2.at(i)) { \
        ret = OB_NEED_RETRY; \
        LOG_WARN(log_info, KR(ret), K(arr1), K(arr2)); \
      } \
    } \
  } \
} while (0)


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
    //控制线程idle时间，如果处于suspend状态，可以多idle一段时间
    bool job_is_suspend = false;
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
      } else if (OB_FAIL(try_process_current_job(job_cnt, job_is_suspend))) {
        LOG_WARN("failed to process current job", KR(ret));
      } else if (0 == job_cnt) {
        if (OB_FAIL(transfer_partition_(job_cnt))) {
          LOG_WARN("failed to transfer partition", KR(ret));
        }
      }
      if (OB_SUCC(ret) && 0 == job_cnt) {
        const bool check_status = job_desc_.get_enable_rebalance();
        uint64_t user_data_version = 0;
        uint64_t meta_data_version = 0;
        if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, user_data_version))) {
          LOG_WARN("failed to get min data version", KR(ret), K(tenant_id_));
        } else if (OB_FAIL(GET_MIN_DATA_VERSION(gen_meta_tenant_id(tenant_id_), meta_data_version))) {
          LOG_WARN("failed to get min data version", KR(ret), K(tenant_id_));
        } else if (!ObShareUtil::check_compat_version_for_hetero_zone(meta_data_version)
            || !ObShareUtil::check_compat_version_for_hetero_zone(user_data_version)) {
          // need to check user tenant data version because new type of strategy in __all_balance_job will be used in 4420
          ISTAT("can not ls balance, wait meta tenant and user tenant upgrade to 4420", KR(ret), KDV(meta_data_version), KDV(user_data_version));
        } else if (OB_FAIL(gather_ls_status_stat(tenant_id_, ls_array_, check_status))) {
          LOG_WARN("failed to gather ls status stat", KR(ret), K(tenant_id_), K(check_status));
        } else if (OB_FAIL(ls_balance_(job_cnt))) {
          LOG_WARN("failed to do ls balance", KR(ret));
        }

        if (OB_SUCC(ret) && 0 == job_cnt) {
          if (ObShareUtil::is_tenant_enable_ls_leader_balance(tenant_id_)
            && OB_FAIL(ObBalanceLSPrimaryZone::try_adjust_user_ls_primary_zone(tenant_id_))) {
            LOG_WARN("failed to adjust user tenant primary zone", KR(ret), K(tenant_id_));
          } else if (ObShareUtil::is_tenant_enable_transfer(tenant_id_)
            && OB_FAIL(try_do_partition_balance_(last_partition_balance_time))) {
            LOG_WARN("try do partition balance failed", KR(ret), K(last_partition_balance_time));
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
      if (OB_SUCC(ret) && 1 == job_cnt && !job_is_suspend) {
        wakeup_balance_task_execute_();
      }

      if (OB_FAIL(ret) && OB_NEED_WAIT != ret) {
        idle_time_us = 100 * 1000;
      } else {
        omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
        int64_t tmp_time = 0;
        if (tenant_config.is_valid()) {
          tmp_time = tenant_config->balancer_idle_time;
        }
        idle_time_us = tenant_config.is_valid() ? tenant_config->balancer_idle_time : 10 * 1000 * 1000L;
        if (idle_time_us <= 0) {
          //防御性报错
          LOG_ERROR("balancer idle time is not valid", K(idle_time_us), K(tmp_time));
          idle_time_us = 10 * 1000 * 1000L;
        } else if (job_is_suspend) {
          idle_time_us = 5 * idle_time_us;
          ISTAT("job is suspend", K(idle_time_us));
        }
      }
      ISTAT("finish one round", KR(ret), KR(tmp_ret), K_(tenant_id), K(job_cnt),
                K(job_desc_),
                K(ls_array_), K(idle_time_us), K(last_partition_balance_time), K(last_statistic_bg_stat_time),
                K(last_statistic_schema_version), K(last_statistic_max_transfer_task_id),
                "enable_rebalance", ObShareUtil::is_tenant_enable_rebalance(tenant_id_),
                "enable_transfer", ObShareUtil::is_tenant_enable_transfer(tenant_id_));
      reset();
      idle(idle_time_us);
    }// end while
  }
}

void ObTenantBalanceService::wakeup_balance_task_execute_()
{
  int ret = OB_SUCCESS;
  ObBalanceTaskExecuteService *exe_service = MTL(ObBalanceTaskExecuteService*);
  if (OB_ISNULL(exe_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("balance task execute service is null", KR(ret));
  } else {
    exe_service->wakeup();
    LOG_INFO("wake balance task execute service");
  }
}

int ObTenantBalanceService::gather_tenant_balance_desc(
    const uint64_t &tenant_id,
    share::ObBalanceJobDesc &job_desc,
    ObIArray<share::ObUnit> &unit_array)
{
  int ret = OB_SUCCESS;
  unit_array.reset();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_) || OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP(GCTX.sql_proxy_), KP(GCTX.schema_service_));
  } else {
    //get primary zone
    share::schema::ObTenantSchema tenant_schema;
    ObArray<ObZone> locality_zone_list;
    ObArray<ObZone> primary_zone;
    if (OB_FAIL(get_tenant_schema(tenant_id, tenant_schema))) {
      LOG_WARN("failed to get tenant schema", KR(ret), K(tenant_id));
    } else if (OB_FAIL(ObLSServiceHelper::get_primary_zone_unit_array(&tenant_schema,
            primary_zone, unit_array, locality_zone_list))) {
      LOG_WARN("failed to get primary zone unit array", KR(ret), K(tenant_schema));
    } else {
      //1. get scale_out_factor
      uint64_t data_version = 0;
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
      int64_t ls_scale_out_factor = 1;
      bool enable_gts_standalone = false;
      if (OB_UNLIKELY(!tenant_config.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant config is invalid", KR(ret), K(tenant_id));
      } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
        LOG_WARN("failed to get min data version", KR(ret), K(tenant_id));
      } else if (MOCK_DATA_VERSION_4_2_5_1 > data_version
        || (DATA_VERSION_4_3_0_0 <= data_version && DATA_VERSION_4_4_1_0 > data_version)) {
        //not valid to use scale_out_factor
        ls_scale_out_factor = 1;
      } else {
        ls_scale_out_factor = tenant_config->ls_scale_out_factor;
      }
      if (OB_SUCC(ret)) {
        bool enable_transfer = ObShareUtil::is_tenant_enable_transfer(tenant_id);
        bool enable_rebalance = ObShareUtil::is_tenant_enable_rebalance(tenant_id);
        ObZoneUnitCntList zone_list;
        if (!ObShareUtil::check_compat_version_for_hetero_zone(data_version)) {
          enable_gts_standalone = false;
        } else {
          enable_gts_standalone = tenant_config->enable_gts_standalone;
        }
        ARRAY_FOREACH(locality_zone_list, idx) {
          ObZone &zone = locality_zone_list.at(idx);
          int64_t unit_num = 0;
          ObReplicaType replica_type = ObReplicaType::REPLICA_TYPE_MAX;
          //统计每个zone的可用的unit个数,以及unit的副本类型
          ARRAY_FOREACH(unit_array, j) {
            const ObUnit &unit = unit_array.at(j);
            if (unit.is_active_or_adding_status() && unit.zone_ == zone) {
              unit_num++;
              replica_type = unit.replica_type_;
            }
          }//end for check unit
          ObDisplayZoneUnitCnt zone_unit_cnt(zone, unit_num, replica_type);
          if (OB_FAIL(zone_list.push_back(zone_unit_cnt))) {
            LOG_WARN("failed to push back", KR(ret), K(idx));
          }
        }
        if (FAILEDx(job_desc.init_without_job(
            tenant_id,
            zone_list,
            primary_zone.count(),
            ls_scale_out_factor,
            enable_rebalance,
            enable_transfer,
            enable_gts_standalone))) {
          LOG_WARN("failed to init job_desc", KR(ret), "primary_zone_num", primary_zone.count(),
              K(ls_scale_out_factor), K(enable_rebalance), K(enable_transfer), K(enable_gts_standalone));
        }
      }
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
  } else if (OB_FAIL(gather_tenant_balance_desc(
      tenant_id_, job_desc_, unit_array_))) {
    LOG_WARN("fail to execute gather_tenant_balance_desc", KR(ret), K(tenant_id_));
  } else {
    ATOMIC_SET(&loaded_, true);
  }
  return ret;
}
/*
 * 现在均衡逻辑负责维护ls_status表的unit_list。之前的均衡获取到的ls_status是不包含dropping和wait_offline的日志流的，
 * */
int ObTenantBalanceService::gather_ls_status_stat(const uint64_t &tenant_id, share::ObLSStatusInfoArray &ls_array,
    const bool check_status_valid)
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
    ObLSStatusOperator status_op;
    if (OB_FAIL(status_op.get_all_ls_status_by_order(tenant_id, ls_array, *GCTX.sql_proxy_))) {
      LOG_WARN("failed to get status by order", KR(ret), K(tenant_id));
    } else if (check_status_valid) {
      if (OB_FAIL(check_ls_status_valid_balance(tenant_id, ls_array))) {
        LOG_WARN("failed to check status is valid", KR(ret), K(ls_array));
      }
    }
    LOG_INFO("gather ls status", KR(ret), K(ls_array), K(check_status_valid));
  }
  return ret;
}

int ObTenantBalanceService::check_ls_status_valid_balance(const uint64_t &tenant_id,
    share::ObLSStatusInfoArray &ls_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || ls_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id), K(ls_array));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP(GCTX.sql_proxy_));
  } else {
    //get ls status info
    ObLSAttrOperator ls_op(tenant_id, GCTX.sql_proxy_);
    share::ObLSAttrArray ls_attr_array;
    if (OB_FAIL(ls_op.get_all_ls_by_order(ls_attr_array))) {
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
        need_remove_ls = false;
        const ObLSStatusInfo &status_info = ls_array.at(i);
        const ObLSAttr &ls_info = ls_attr_array.at(attr_index);
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
          } else if (status_info.ls_is_dropping()) {
            //ls is in dropping, can not fallback, no need to takecare
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
  ObTenantRole::Role role = ObTenantRole::Role::PRIMARY_TENANT;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant or not user tenant", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (ObAllTenantInfoProxy::is_primary_tenant(GCTX.sql_proxy_, tenant_id, is_primary)) {
    LOG_WARN("fail to execute is_primary_tenant", KR(ret), K(tenant_id));
  } else if (!is_primary) {
    //其他的角色都统一成备库，没有特别的区别
    role = ObTenantRole::Role::STANDBY_TENANT;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(is_tenant_ls_balance_finished_(tenant_id, role, is_finished))) {
    LOG_WARN("fail to execute is_tenant_ls_balance_finished_", KR(ret), K(tenant_id), K(role));
  }

  LOG_TRACE("check whether the tenant has balanced ls", K(ret), K(tenant_id), K(is_primary), K(is_finished));
  return ret;
}

int ObTenantBalanceService::is_tenant_ls_balance_finished_(
    const uint64_t &tenant_id,
    const share::ObTenantRole::Role tenant_role,
    bool &is_finished)
{
  int ret = OB_SUCCESS;
  int64_t job_cnt = 1;
  int64_t start_time = OB_INVALID_TIMESTAMP, finish_time = OB_INVALID_TIMESTAMP;
  ObBalanceJob job;
  //系统租户有判断其他租户均衡完成的需求
  const uint64_t exe_tenant_id = is_valid_tenant_id(MTL_ID()) ? MTL_ID() : OB_SYS_TENANT_ID;
  ObArenaAllocator allocator("TntLSBalance" , OB_MALLOC_NORMAL_BLOCK_SIZE, exe_tenant_id);
  ObLSBalanceTaskHelper ls_balance_helper(allocator);
  bool need_ls_balance = false;
  share::ObLSStatusInfoArray ls_array;
  ObArray<share::ObUnit> unit_array;
  ObBalanceJobDesc job_desc;
  is_finished = false;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant or not user tenant", KR(ret), K(tenant_id));
  } else if (tenant_role != ObTenantRole::Role::PRIMARY_TENANT) {
    // skip checking balance job for non-primary tenant
    job_cnt = 0;
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
  } else if (OB_FAIL(gather_tenant_balance_desc(tenant_id, job_desc, unit_array))) {
    LOG_WARN("fail to execute gather_tenant_balance_desc", KR(ret), K(tenant_id));
  } else {
    bool check_status = true;
    if (ObTenantRole::Role::PRIMARY_TENANT != tenant_role) {
      check_status = false;
    } else {
      check_status = job_desc.get_enable_rebalance();
    }
    if (OB_FAIL(gather_ls_status_stat(tenant_id, ls_array, check_status))) {
      LOG_WARN("fail to execute gather_ls_status_stat", KR(ret), K(tenant_id), K(check_status));
    } else if (OB_FAIL(ls_balance_helper.init(
            tenant_id, ls_array, job_desc, unit_array,
            ObTenantRole(tenant_role), GCTX.sql_proxy_))) {
      LOG_WARN("failed to init ls balance helper", KR(ret), K(ls_array), K(tenant_id), K(tenant_role));
    } else if (OB_FAIL(ls_balance_helper.check_need_ls_balance(need_ls_balance))) {
      LOG_WARN("failed to check_ls need balance", KR(ret));
    } else {
      is_finished = !need_ls_balance;
    }
  }
  LOG_INFO("check whether the tenant has balanced ls", KR(ret), K(tenant_id), K(ls_array),
      K(job_desc), K(need_ls_balance));
  return ret;
}

int ObTenantBalanceService::try_process_current_job(int64_t &job_cnt, bool &job_is_suspend)
{
  int ret = OB_SUCCESS;
  job_cnt = 0;
  job_is_suspend = false;
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
  } else if (job.get_job_status().is_suspend()) {
    job_is_suspend = true;
    //in suspend, can not calcel, only doing need check cancel
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
  ObArenaAllocator allocator("TntLSBalance" , OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
  ObLSBalanceTaskHelper ls_balance_helper(allocator);
  ObTenantRole tenant_role(MTL_GET_TENANT_ROLE_CACHE());
  if (OB_UNLIKELY(!inited_ || !ATOMIC_LOAD(&loaded_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(inited_), K(loaded_));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP(GCTX.sql_proxy_));
  } else {
    //Build the current ls group, check if the ls group matches the primary_zone, unit_group
    //If it is a partition_balance task and ls_balance is currently required, the current task needs to be cancelled
    if (OB_FAIL(ls_balance_helper.init(tenant_id_, ls_array_,
            job_desc_, unit_array_, tenant_role, GCTX.sql_proxy_))) {
      LOG_WARN("failed to init ls balance helper", KR(ret), K(ls_array_), K(unit_array_),
          K(job_desc_), K(tenant_id_), K(tenant_role));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ls_balance_helper.generate_ls_balance_task(false/*only job*/))) {
      LOG_WARN("failed to generate task", KR(ret));
    } else if (!ls_balance_helper.need_ls_balance()) {
      job_cnt = 0;
    } else if (OB_FAIL(persist_job_and_task_(
        ls_array_,
        job_desc_,
        ls_balance_helper.get_balance_job(),
        ls_balance_helper.get_balance_tasks()))) {
      LOG_WARN("failed to persist balance task and job", KR(ret), K(ls_array_),
      "job", ls_balance_helper.get_balance_job(),
      "tasks", ls_balance_helper.get_balance_tasks());
    } else if (!ls_balance_helper.get_balance_job().get_balance_strategy().has_balance_task()) {
      //需要就地完成，并且结束掉这个job，TODO后面是进入下一个阶段
      if (OB_FAIL(ls_balance_helper.execute_job_without_task())) {
        LOG_WARN("failed to execute job without task", KR(ret), K(ls_balance_helper));
      }
    } else {
      job_cnt = 1;
    }
  }
  ISTAT("finish ls balance", KR(ret),
           "job", ls_balance_helper.get_balance_job(), "tasks",
           ls_balance_helper.get_balance_tasks(),
           "ls group op", ls_balance_helper.get_ls_group_op());
  return ret;
}

int ObTenantBalanceService::partition_balance_(bool enable_transfer)
{
  int ret = OB_SUCCESS;
  ObPartitionBalance partition_balance;
  if (OB_UNLIKELY(!inited_ || !ATOMIC_LOAD(&loaded_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(inited_), K(loaded_));
  } else if (OB_ISNULL(GCTX.sql_proxy_) || OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP(GCTX.sql_proxy_), KP(GCTX.schema_service_));
  } else if (OB_FAIL(partition_balance.init(tenant_id_, GCTX.schema_service_, GCTX.sql_proxy_,
          enable_transfer ? ObPartitionBalance::GEN_TRANSFER_TASK : ObPartitionBalance::GEN_BG_STAT))) {
    LOG_WARN("fail to init partition balance", KR(ret), K(tenant_id_), K(job_desc_), K(enable_transfer));
  } else if (OB_FAIL(partition_balance.process())) {
    LOG_WARN("fail to process partition_balance", KR(ret));
  } else if (partition_balance.get_balance_task().empty()) {
    ISTAT("partition balance generate empty task");
  } else if (OB_FAIL(persist_job_and_task_(
      ls_array_,
      job_desc_,
      partition_balance.get_balance_job(),
      partition_balance.get_balance_task()))) {
    LOG_WARN("fail to persist_job_and_task", KR(ret),
        "job", partition_balance.get_balance_job(), "tasks", partition_balance.get_balance_task());
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
  } else if (job.get_job_status().is_suspend()) {
    can_clean_job = false;
    if (REACH_THREAD_TIME_INTERVAL(10 * 1000 * 1000)) {
      //10s
      ISTAT("job is suspend, cannot finish", K(job));
    }
  } else if (OB_FAIL(ObBalanceTaskTableOperator::get_job_task_cnt(tenant_id_, job.get_job_id(),
                                          task_cnt, *GCTX.sql_proxy_))) {
    LOG_WARN("failed to get job task cnt", KR(ret), K(job), K(tenant_id_));
  } else if (0 != task_cnt) {
    can_clean_job = false;
    ISTAT("job has not finish task, cannot finish", K(task_cnt), K(job));
  } else if (job.get_job_type().is_balance_partition() && job.get_job_status().is_doing()) {
    // only the PARTITION_BALANCE job in DOING status is different from other job
    if (OB_FAIL(try_finish_doing_partition_balance_job_(job, can_clean_job))) {
      LOG_WARN("try finish partition balance job failed", KR(ret), K(job));
    }
  } else if (job.get_job_type().is_balance_ls() && job.get_job_status().is_doing()) {
    if (OB_FAIL(try_finish_doing_ls_balance_job_(job, can_clean_job))) {
      LOG_WARN("try finish ls balance job failed", KR(ret), K(job));
    }
  } else if (OB_FAIL(finish_doing_and_canceling_job_(job))) {
    LOG_WARN("finish doing and canceling job failed", KR(ret), K(job));
  } else {
    can_clean_job = true;
  }
  if (OB_FAIL(ret)) {
  } else if (!can_clean_job) {
    job_cnt = 1;
  } else if (OB_FAIL(ObBalanceJobTableOperator::clean_job(tenant_id_, job.get_job_id(), *GCTX.sql_proxy_))) {
    LOG_WARN("failed to clean job", KR(ret), K(tenant_id_), K(job));
  } else {
    ISTAT("clean the job", K(task_cnt), K(job));
    job_cnt = 0;
  }
  return ret;
}

int ObTenantBalanceService::finish_doing_and_canceling_job_(const ObBalanceJob &job)
{
  int ret = OB_SUCCESS;
  ObBalanceJobStatus new_status;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat failed", KR(ret));
  } else if (OB_UNLIKELY(!job.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid job", KR(ret), K(job));
  } else if (job.get_job_status().is_doing()) {
    new_status = ObBalanceJobStatus(share::ObBalanceJobStatus::BALANCE_JOB_STATUS_COMPLETED);
    if (OB_UNLIKELY(job.get_job_type().is_balance_partition() || job.get_job_type().is_balance_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition/ls balance in doing status can't be process by this func", KR(ret), K(job));
    }
  } else if (job.get_job_status().is_canceling()) {
    new_status = ObBalanceJobStatus(share::ObBalanceJobStatus::BALANCE_JOB_STATUS_CANCELED);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("job status not expected", KR(ret), K(job));
  }
  START_TRANSACTION(GCTX.sql_proxy_, tenant_id_);
  if (OB_SUCC(ret) && job.get_job_type().is_transfer_partition()) {
    //不管job是以什么状态结束的，校验transfer_partition_task没有这个job_id的任务
    if (OB_FAIL(try_finish_transfer_partition_(job, trans))) {
      LOG_WARN("try finish transfer partition task", KR(ret), K(job));
    }
  }
  if (FAILEDx(ObBalanceJobTableOperator::update_job_status(
      tenant_id_,
      job.get_job_id(),
      job.get_job_status(),
      new_status,
      false/*update_comment*/,
      ObString(),
      trans))) {
    LOG_WARN("failed to update job status", KR(ret), K(tenant_id_), K(job), K(new_status));
  }
  END_TRANSACTION(trans);
  return ret;
}

int ObTenantBalanceService::try_finish_doing_ls_balance_job_(
  const ObBalanceJob &job,
  bool &is_finished)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("TntLSBalance" , OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
  ObLSBalanceTaskHelper ls_balance_helper(allocator);
  ObTenantRole tenant_role(MTL_GET_TENANT_ROLE_CACHE());
  is_finished = false;
  ObBalanceJobStatus new_status;
  ObSqlString comment;
  const bool check_status = job_desc_.get_enable_rebalance();
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat failed", KR(ret));
  } else if (OB_UNLIKELY(!job.is_valid()
      || !job.get_job_type().is_balance_ls()
      || !job.get_job_status().is_doing())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid job", KR(ret), K(job));
  } else if (!job.get_balance_strategy().is_new_ls_balance_strategy()) {
    // case1: COMPLETED, compatible with old strategy
    new_status = ObBalanceJobStatus(share::ObBalanceJobStatus::BALANCE_JOB_STATUS_COMPLETED);
    is_finished = true;
  } else if (OB_FAIL(gather_ls_status_stat(tenant_id_, ls_array_, check_status))) {
    LOG_WARN("failed to gather ls status stat", KR(ret), K(tenant_id_), K(check_status));
  } else if (OB_FAIL(ls_balance_helper.init(
      tenant_id_,
      ls_array_,
      job_desc_,
      unit_array_,
      tenant_role,
      GCTX.sql_proxy_))) {
    LOG_WARN("failed to init ls balance helper", KR(ret), K(tenant_id_), K(ls_array_),
        K(unit_array_), K(job_desc_), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(ls_balance_helper.generate_ls_balance_task(false/*only job*/, job.get_job_id()))) {
    LOG_WARN("failed to generate task", KR(ret), K(job));
  } else if (!ls_balance_helper.need_ls_balance()) {
    // case2: COMPLETED, no need balance
    new_status = ObBalanceJobStatus(share::ObBalanceJobStatus::BALANCE_JOB_STATUS_COMPLETED);
    is_finished = true;
  } else { // need balance
    const ObBalanceStrategy &new_strategy = ls_balance_helper.get_balance_job().get_balance_strategy();
    if (!new_strategy.can_be_next_ls_balance_strategy(job.get_balance_strategy())) {
      // case3: CANCELING, strategy rollback
      new_status = ObBalanceJobStatus(ObBalanceJobStatus::BALANCE_JOB_STATUS_CANCELING);
      ISTAT("cancel ls balance job because strategy rollback", K(new_strategy), K(job));
      if (OB_FAIL(comment.assign_fmt("Canceled due to balance strategy rollback (new strategy:%s)", new_strategy.str()))) {
        LOG_WARN("assign failed", KR(ret), K(job), K(new_strategy));
      }
    } else if (!new_strategy.has_balance_task()) {
      // immediate execution
      if (OB_FAIL(ObBalanceJobTableOperator::update_job_balance_strategy(
          tenant_id_,
          job.get_job_id(),
          job.get_job_status(),
          job.get_balance_strategy(),
          new_strategy,
          *GCTX.sql_proxy_))) {
        LOG_WARN("update job balane strategy failed", KR(ret), K(tenant_id_), K(job), K(new_strategy));
      } else if (OB_FAIL(ls_balance_helper.execute_job_without_task())) {
        LOG_WARN("failed to execute job without task", KR(ret), K(ls_balance_helper));
      } else {
        // case4: DOING, balance job without task
        new_status = ObBalanceJobStatus(ObBalanceJobStatus::BALANCE_JOB_STATUS_DOING);
        ISTAT("update ls balance strategy without task successfully", K(job), K(new_status),
            K(new_strategy), "old_strategy", job.get_balance_strategy());
      }
    } else if (OB_FAIL(update_job_and_insert_new_tasks_(
        job,
        new_strategy,
        ls_balance_helper.get_balance_tasks()))) {
      LOG_WARN("update job and insert new tasks failed", KR(ret), K(job),
          K(new_strategy), "balance_tasks", ls_balance_helper.get_balance_tasks());
    } else {
      // case5: DOING, balance job with task
      new_status = ObBalanceJobStatus(ObBalanceJobStatus::BALANCE_JOB_STATUS_DOING);
      ISTAT("update ls balance strategy with task successfully", K(job), K(new_status),
          K(new_strategy), "old_strategy", job.get_balance_strategy());
    }
  }
  if (OB_FAIL(ret) || new_status == job.get_job_status()) {
    // skip
  } else if (OB_FAIL(ObBalanceJobTableOperator::update_job_status(
      tenant_id_,
      job.get_job_id(),
      job.get_job_status(),
      new_status,
      !comment.empty()/*update_comment*/,
      comment.string(),
      *GCTX.sql_proxy_))) {
    LOG_WARN("failed to update job status", KR(ret), K(tenant_id_), K(job), K(new_status));
  } else {
    ISTAT("update ls balance job status successfully",
        "job_id", job.get_job_id(), K(new_status), "old_status", job.get_job_status());
  }
  return ret;
}

//在balance_job结束时，可能存在transfer partition任务的残留，这里残留有两种情况：
//1. cancel 时，需要一把把没有处理完成的任务全都回滚掉。
//2. complete时，可能也会有残留的任务，例如生成了LS_SPLIT + ALTER + MERGE任务
//用户在split任务执行成功后，就把对应的分区删除掉了，后续的merge任务的part_list中就看不到这个分区了，所以在整个balance_job结束掉后，就会存在transfer_partition任务的残留，这种我们也回滚成WAITING状态，等待下一轮结束掉:
int ObTenantBalanceService::try_finish_transfer_partition_(
    const share::ObBalanceJob &job, common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || ! ATOMIC_LOAD(&loaded_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(inited_), K(loaded_));
  } else if (OB_UNLIKELY(!job.is_valid() || !job.get_job_type().is_transfer_partition())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("job is invalid", KR(ret), K(job));
  } else if (job.get_job_status().is_canceling()) {
    //取消所有的关联的transfer partition任务
    if (OB_FAIL(ObTransferPartitionTaskTableOperator::rollback_all_to_waitting(
            tenant_id_, job.get_job_id(), trans))) {
      LOG_WARN("failed to rollback task", KR(ret), K(tenant_id_), K(job));
    }
  } else if (job.get_job_status().is_doing()) {
    ObArray<ObTransferPartitionTask> task_array;
    if (OB_FAIL(ObTransferPartitionTaskTableOperator::load_all_balance_job_task(
            tenant_id_, job.get_job_id(), task_array, trans))) {
      LOG_WARN("failed to load balance job task", KR(ret), K(tenant_id_), K(job));
    } else if (task_array.count() > 0) {
      ObString comment("Need retry, partition may be dropped");
      ISTAT("Job is finish, has transfer task doing, rollback such task",
          K(task_array), K(job));
      ObTransferPartList part_list;
      //通过分析，这部分遗留下来的transfer partition任务一定是doing状态的
      for (int64_t i = 0; OB_SUCC(ret) && i < task_array.count(); ++i) {
        const ObTransferPartitionTask &task = task_array.at(i);
        if (OB_UNLIKELY(!task.get_task_status().is_doing())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("task must be doing", KR(ret), K(task));
        } else if (OB_FAIL(part_list.push_back(task.get_part_info()))) {
          LOG_WARN("failed to push back", KR(ret), K(i), K(task));
        }
      }
      if (FAILEDx(ObTransferPartitionTaskTableOperator::
                      rollback_from_doing_to_waiting(tenant_id_,
                      job.get_job_id(), part_list, comment, trans))) {
        LOG_WARN("failed to rollback balance job", KR(ret), K(job), K(part_list));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("job status is not expected", KR(ret), K(job));
  }
  return ret;
}

int ObTenantBalanceService::check_ls_job_need_cancel_(
    const share::ObBalanceJob &job,
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
  } else if (job.get_job_type().is_transfer_partition()) {
    //手动transfer partition任务只需要看 enable_transfer 和 没有在升级状态中 即可
    if (!ObShareUtil::is_tenant_enable_transfer(tenant_id_)) {
      need_cancel = true;
      if (OB_TMP_FAIL(comment.assign("Canceled due to tenant transfer being disabled or tenant being in upgrade mode"))) {
        LOG_WARN("failed to assign fmt", KR(tmp_ret), K(job));
      }
      ISTAT("tenant transfer is disabled or tenant is in upgrade mode; need cancel current job", K(job), K(comment));
    }
  } else if (OB_FAIL(check_if_need_cancel_by_job_desc_(job, need_cancel, comment))) {
    LOG_WARN("check if need cancel by job desc failed", KR(ret), K(job), K(need_cancel), K(comment));
  }

  if (OB_FAIL(ret) || need_cancel) {
    // skip
  } else if (job.is_timeout()) {
    need_cancel = true;
    if (OB_TMP_FAIL(comment.assign("Canceled due to timeout"))) {
      LOG_WARN("assign failed", KR(tmp_ret), K(job));
    }
    ISTAT("cancel job due to timeout", K(job),
        K(comment), "current_time", ObTimeUtility::current_time());
  }
  return ret;
}

#define CANCEL_REASON_BOOL(variable_str, old_bool, new_bool)                                     \
  do {                                                                                           \
    need_cancel = true;                                                                          \
    const char *old_value_str = old_bool ? "true" : "false";                                     \
    const char *new_value_str = new_bool ? "true" : "false";                                     \
    if (OB_TMP_FAIL(comment.assign_fmt("Canceled due to tenant %s changing from %s to %s",       \
        variable_str, old_value_str, new_value_str))) {                                          \
      LOG_WARN("failed to assign fmt", KR(tmp_ret), K(variable_str), K(old_bool), K(new_bool));  \
    }                                                                                            \
  } while(0)

int ObTenantBalanceService::check_if_need_cancel_by_job_desc_(
    const share::ObBalanceJob &job,
    bool &need_cancel,
    common::ObSqlString &comment)
{
  int ret = OB_SUCCESS;
  need_cancel = false;
  comment.reset();
  int tmp_ret = OB_SUCCESS;
  uint64_t meta_data_version = 0;
  ObBalanceJobDesc old_job_desc;
  bool is_job_desc_same = true;
  ObSqlString diff_str;
  if (OB_UNLIKELY(!inited_ || !ATOMIC_LOAD(&loaded_) || !job_desc_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(inited_), K(loaded_), K(job_desc_));
  } else if (OB_UNLIKELY(!job.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("job is invalid", KR(ret), K(job));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(gen_meta_tenant_id(job.get_tenant_id()), meta_data_version))) {
    LOG_WARN("failed to get min data version", KR(ret), K(tenant_id_));
  } else if (!ObShareUtil::check_compat_version_for_hetero_zone(meta_data_version)) {
    // case 1
    need_cancel = true;
    if (OB_TMP_FAIL(comment.assign_fmt("Canceled due to meta tenant data_version < 4.4.2.0"))) {
      LOG_WARN("assign_fmt failed", KR(tmp_ret), KDV(meta_data_version));
    }
  } else if (OB_FAIL(ObBalanceJobDescOperator::get_balance_job_desc(
      job.get_tenant_id(),
      job.get_job_id(),
      *GCTX.sql_proxy_,
      old_job_desc))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // case 2
      ret = OB_SUCCESS;
      need_cancel = true;
      if (OB_TMP_FAIL(comment.assign_fmt("Canceled due to job description not exist"))) {
        LOG_WARN("assign_fmt failed", KR(tmp_ret));
      }
    } else {
      LOG_WARN("get balance job desc failed", KR(ret), K(job));
    }
  } else if (OB_FAIL(old_job_desc.compare(job_desc_, is_job_desc_same, diff_str))) {
    LOG_WARN("compare faield", KR(ret), K(old_job_desc), K(job_desc_));
  } else if (!is_job_desc_same) {
    // case 3
    need_cancel = true;
    if (OB_TMP_FAIL(comment.assign_fmt("Canceled due to %s", diff_str.ptr()))) {
      LOG_WARN("assign_fmt failed", KR(tmp_ret), K(diff_str));
    }
  }
  if (OB_SUCC(ret) && need_cancel) {
    ISTAT("need cancel current job", K(need_cancel), K(comment), K(job), K(job_desc_), K(old_job_desc));
  }
  return ret;
}

void ObTenantBalanceService::reset()
{
  ATOMIC_SET(&loaded_, false);
  ls_array_.reset();
  job_desc_.reset();
  unit_array_.reset();

}

int ObTenantBalanceService::persist_job_and_task_(
    const share::ObLSStatusInfoArray &ls_array,
    const share::ObBalanceJobDesc &job_desc,
    const share::ObBalanceJob &job,
    ObArray<share::ObBalanceTask> &tasks)
{
  int ret = OB_SUCCESS;
  ObConflictCaseWithClone case_to_check(ObConflictCaseWithClone::TRANSFER);
  if (OB_UNLIKELY(!job.is_valid() || ls_array.empty() || !job_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("job or task is invalid", KR(ret), K(job), K(ls_array), K(job_desc), K(tasks));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP(GCTX.sql_proxy_));
  } else {
    const uint64_t tenant_id = job.get_tenant_id();
    common::ObMySQLTransaction trans;
    if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id))) {
      LOG_WARN("failed to start trans", KR(ret), K(tenant_id));
    } else if (OB_FAIL(persist_job_and_task_in_trans_(ls_array, job_desc, job, tasks, trans))) {
      LOG_WARN("failed to persist job and task in trans", KR(ret), K(job_desc), K(job), K(tasks));
    }
    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
    if (FAILEDx(ObBalanceJobDescOperator::insert_balance_job_desc(
        tenant_id,
        job.get_job_id(),
        job_desc,
        *GCTX.sql_proxy_))) {
      LOG_WARN("insert balance job desc failed", KR(ret), K(tenant_id), K(job), K(job_desc));
    }
  }
  return ret;
}

int ObTenantBalanceService::persist_job_and_task_in_trans_(
    const share::ObLSStatusInfoArray &ls_array,
    const share::ObBalanceJobDesc &job_desc,
    const share::ObBalanceJob &job,
    ObArray<share::ObBalanceTask> &tasks,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  //job 是unit_list相关的时候，task是空的
  ObConflictCaseWithClone case_to_check(ObConflictCaseWithClone::TRANSFER);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(inited_));
  } else if (OB_UNLIKELY(!job.is_valid() || ls_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job), K(ls_array));
  } else if (job.get_balance_strategy().has_balance_task()) {
    if (OB_UNLIKELY(0 == tasks.count())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("task is invalid", KR(ret), K(job));
    } else if (OB_FAIL(construct_dependency_of_each_task_(tasks))) {
      LOG_WARN("failed to generate dependency task", KR(ret), K(tasks));
    }
  }
  const uint64_t tenant_id = job.get_tenant_id();
  if (FAILEDx(lock_and_check_balance_job(trans, tenant_id))) {
    LOG_WARN("lock and check balance job failed", KR(ret), K_(tenant_id));
  } else {
    //由于ls_array_是在锁外获取，所以可能会存在没有获取到最新状态的问题，在锁内做二次校验
    //TODO 是否需要检验primary_zone和unit_num，目前看不需要，这些随时都有可能被修改
    //只能保证最终一致性
    share::ObLSStatusInfoArray tmp_ls_array;
    const bool check_status = job_desc.get_enable_rebalance();
    if (OB_FAIL(gather_ls_status_stat(tenant_id, tmp_ls_array, check_status))) {
      LOG_WARN("failed to get ls status array", KR(ret), K(tenant_id), K(check_status));
    } else {
      CHECK_OB_ARRAY_EQUAL(tmp_ls_array, ls_array, "ls status info change, need retry");
    }
  }
  if (FAILEDx(ObBalanceJobTableOperator::insert_new_job(job, trans))) {
    LOG_WARN("failed to insert new job", KR(ret), K(job));
  } else if (OB_FAIL(ObTenantSnapshotUtil::check_tenant_not_in_cloning_procedure(tenant_id_, case_to_check))) {
      LOG_WARN("fail to check whether tenant is cloning", KR(ret), K_(tenant_id), K(case_to_check));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
    if (OB_FAIL(ObBalanceTaskTableOperator::insert_new_task(tasks.at(i),
                                                            trans))) {
      LOG_WARN("failed to insert new task", KR(ret), K(i), K(tasks));
    }
  }
  return ret;
}
int ObTenantBalanceService::lock_and_check_balance_job(
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
      EXCLUSIVE, false))) {
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
  if (OB_UNLIKELY(tasks.count() <= 0)) {
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
  } else if (OB_FAIL(ObObjectBalanceWeightMgr::try_clear_tenant_expired_obj_weight(tenant_id_))) {
    LOG_WARN("try clear tenant expired obj weight failed", KR(ret), K(tenant_id_));
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
int ObTenantBalanceService::transfer_partition_(int64_t &job_cnt)
{
  int ret = OB_SUCCESS;
  job_cnt = 0;
  uint64_t data_version = 0;
  if (OB_UNLIKELY(!inited_ || !ATOMIC_LOAD(&loaded_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(inited_), K(loaded_));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, data_version))) {
    LOG_WARN("failed to get min data version", KR(ret), K(tenant_id_));
  } else if (DATA_VERSION_4_2_1_2 > data_version
      || (data_version >= DATA_VERSION_4_3_0_0 && DATA_VERSION_4_3_1_0 > data_version)) {
    LOG_TRACE("no need do transfer partition", KDV(data_version));
  } else if (!ObShareUtil::is_tenant_enable_transfer(tenant_id_)) {
    LOG_TRACE("can not transfer partition due to transfer being disabled or tenant being in upgrade mode.");
  } else {
    ObTransferPartitionHelper tp_help(tenant_id_, GCTX.sql_proxy_);
    const bool check_status = job_desc_.get_enable_rebalance();
    bool has_job = true;

    if (OB_FAIL(tp_help.build(has_job))) {
      LOG_WARN("failed to build transfer task info", KR(ret));
    } else if (!has_job) {
      LOG_INFO("no transfer partition job");
    } else if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ptr is null", KR(ret), KP(GCTX.sql_proxy_));
    } else if (OB_FAIL(gather_ls_status_stat(tenant_id_, ls_array_, check_status))) {
      LOG_WARN("failed to gather ls status stat", KR(ret), K(tenant_id_));
    } else {
      START_TRANSACTION(GCTX.sql_proxy_, tenant_id_)
      if (FAILEDx(tp_help.process_in_trans(ls_array_, trans))) {
        LOG_WARN("failed to process transfer partition", KR(ret), K(ls_array_));
      } else if (0 == tp_help.get_balance_tasks().count()) {
        job_cnt = 0;
      } else if (OB_FAIL(persist_job_and_task_in_trans_(
          ls_array_,
          job_desc_,
          tp_help.get_balance_job(),
          tp_help.get_balance_tasks(),
          trans))) {
        LOG_WARN("failed to persist job and task", KR(ret), "job",
                 tp_help.get_balance_job(), "tasks",
                 tp_help.get_balance_tasks(),
                 "job_desc", job_desc_);
      } else {
        job_cnt = 1;
      }
      END_TRANSACTION(trans)

      // transfer partition does not rely on job_desc
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCC(ret) && OB_TMP_FAIL(ObBalanceJobDescOperator::insert_balance_job_desc(
          tenant_id_,
          tp_help.get_balance_job().get_job_id(),
          job_desc_,
          *GCTX.sql_proxy_))) {
        LOG_WARN("insert balance job desc failed", KR(tmp_ret), K(tenant_id_),
            K(job_desc_), "job", tp_help.get_balance_job());
      }
    }
  }

  ISTAT("finish transfer partition", KR(ret), K(job_cnt),
      "enable transfer", ObShareUtil::is_tenant_enable_transfer(tenant_id_));
  return ret;
}

int ObTenantBalanceService::trigger_partition_balance(
    const uint64_t tenant_id,
    const int64_t balance_timeout)
{
  int ret = OB_SUCCESS;
  // get ls_array before precheck and double check it when persisting job to make sure ls status hasn't changed
  ObLSStatusInfoArray ls_array;
  ObPartitionBalance partition_balance;
  bool is_supported = false;
  ObArray<ObUnit> unit_array_useless;
  ObBalanceJobDesc job_desc;
  bool check_status = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantBalanceService not init", KR(ret), K(tenant_id), K(balance_timeout));
  } else if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not user tenant", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObBalanceStrategy::check_compat_version(tenant_id, is_supported))) {
    LOG_WARN("check compat version failed", KR(ret), K(tenant_id), K(is_supported));
  } else if (!is_supported) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("trigger partition balance not support", K(tenant_id), K(is_supported));
  } else if (OB_FAIL(precheck_for_trigger_(tenant_id))) {
    LOG_WARN("precheck failed", KR(ret), K(tenant_id), K(balance_timeout));
  } else if (OB_FAIL(gather_tenant_balance_desc(tenant_id, job_desc, unit_array_useless))) {
    LOG_WARN("fail to execute gather_tenant_balance_desc", KR(ret), K(tenant_id));
  } else if (FALSE_IT(check_status = job_desc.get_enable_rebalance())) {
  } else if (OB_FAIL(gather_ls_status_stat(tenant_id, ls_array, check_status))) {
    LOG_WARN("fail to execute gather_ls_status_stat", KR(ret), K(tenant_id), K(check_status));
  } else if (OB_FAIL(init_partition_balance_for_trigger_(tenant_id, partition_balance))) {
    LOG_WARN("init partition balance failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(partition_balance.process(ObBalanceJobID(), balance_timeout))) { // gen new job
    LOG_WARN("fail to process partition_balance", KR(ret));
  } else if (partition_balance.get_balance_task().empty()) {
    ret = OB_PARTITION_ALREADY_BALANCED;
    ISTAT("partitions are already balanced", KR(ret), K(tenant_id));
    LOG_USER_ERROR(OB_PARTITION_ALREADY_BALANCED, "no need to trigger partition balance");
  } else if (OB_FAIL(persist_job_and_task_(
      ls_array,
      job_desc,
      partition_balance.get_balance_job(),
      partition_balance.get_balance_task()))) {
    LOG_WARN("persist job and task in trans failed", KR(ret), K(ls_array),
        "balance_job", partition_balance.get_balance_job(),
        "balance_task", partition_balance.get_balance_task());
    if (OB_ENTRY_EXIST == ret || OB_NEED_RETRY == ret) {
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "balance job is in progress, trigger partition balance is");
    }
  } else {
    ISTAT("trigger partition balance successfully",
        "balance_job", partition_balance.get_balance_job(),
        "balance_task", partition_balance.get_balance_task());
  }
  return ret;
}

// 1. check enable_rebalance and enable_transfer
// 2. check current balance job and whether ls is balanced
// 3. check transfer partition task
int ObTenantBalanceService::precheck_for_trigger_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  bool has_transfer_partition_task = false;
  bool other_balance_job_finished = false;
  if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_) || OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX has null ptr", KR(ret), KP(GCTX.sql_proxy_), KP(GCTX.schema_service_));
  } else if (!ObShareUtil::is_tenant_enable_rebalance(tenant_id)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("rebalance is disabled, tirgger partition balance is not allowed", KR(ret), K(tenant_id));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "enable_rebalance is disabled, trigger partition balance is");
  } else if (!ObShareUtil::is_tenant_enable_transfer(tenant_id)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("transfer is disabled, tirgger partition balance is not allowed", KR(ret), K(tenant_id));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "enable_transfer is disabled, trigger partition balance is");
  } else if (OB_FAIL(is_ls_balance_finished(tenant_id, other_balance_job_finished))) { // include checking current balance job
    LOG_WARN("check ls balance finished failed", KR(ret), K(tenant_id));
  } else if (!other_balance_job_finished) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("balance job is in progress, trigger partition balance is not allowed", KR(ret), K(tenant_id));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "balance job is in progress, trigger partition balance is");
  } else {
    ObTransferPartitionHelper tp_help(tenant_id, GCTX.sql_proxy_);
    if (OB_FAIL(tp_help.build(has_transfer_partition_task))) {
      LOG_WARN("failed to build transfer task info", KR(ret));
    } else if (has_transfer_partition_task) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("Transfer partition tasks are in progress, trigger partition balance is not allowed", KR(ret), K(tenant_id));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "transfer partition tasks are in progress, trigger partition balance is");
    }
  }
  return ret;
}

int ObTenantBalanceService::init_partition_balance_for_trigger_(
    const uint64_t tenant_id,
    ObPartitionBalance &partition_balance)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not user tenant", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_) || OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX has null ptr", KR(ret), KP(GCTX.sql_proxy_), KP(GCTX.schema_service_));
  } else if (OB_UNLIKELY(partition_balance.is_inited())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition_balance has inited", KR(ret), K(tenant_id));
  } else if (OB_FAIL(partition_balance.init(
      tenant_id,
      GCTX.schema_service_,
      GCTX.sql_proxy_,
      ObPartitionBalance::GEN_TRANSFER_TASK))) {
    LOG_WARN("init partition balance failed", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObTenantBalanceService::check_inner_stat_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !ATOMIC_LOAD(&loaded_))) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("incorrect state", KR(ret), K(inited_), K(loaded_));
  } else if (OB_ISNULL(GCTX.sql_proxy_) || OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX has null ptr", KR(ret), KP(GCTX.sql_proxy_), KP(GCTX.schema_service_));
  }
  return ret;
}

int ObTenantBalanceService::try_finish_doing_partition_balance_job_(
    const ObBalanceJob &job,
    bool &is_finished)
{
  int ret = OB_SUCCESS;
  is_finished = false;
  ObPartitionBalance partition_balance;
  ObBalanceJobStatus new_status;
  ObSqlString comment;

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat failed", KR(ret));
  } else if (OB_UNLIKELY(!job.is_valid()
      || !job.get_job_type().is_balance_partition()
      || !job.get_job_status().is_doing())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid job", KR(ret), K(job));
  } else if (OB_FAIL(partition_balance.init(
      tenant_id_,
      GCTX.schema_service_,
      GCTX.sql_proxy_,
      ObPartitionBalance::GEN_TRANSFER_TASK))) {
    LOG_WARN("init failed", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(partition_balance.process(job.get_job_id()))) { // use old job_id
    LOG_WARN("fail to process partition_balance", KR(ret));
  } else if (partition_balance.get_balance_task().empty()) {
    new_status = ObBalanceJobStatus(ObBalanceJobStatus::BALANCE_JOB_STATUS_COMPLETED);
    is_finished = true;
  } else {
    const ObBalanceStrategy &new_strategy = partition_balance.get_balance_job().get_balance_strategy();
    if (!new_strategy.can_be_next_partition_balance_strategy(job.get_balance_strategy())) {
      new_status = ObBalanceJobStatus(ObBalanceJobStatus::BALANCE_JOB_STATUS_CANCELING);
      ISTAT("cancel partition balance job because strategy rollback", K(new_strategy), K(job));
      if (OB_FAIL(comment.assign("Canceled because partition schema changed during job"))) {
        LOG_WARN("assign failed", KR(ret), K(job));
      }
    } else if (OB_FAIL(update_job_and_insert_new_tasks_(
        job,
        new_strategy,
        partition_balance.get_balance_task()))) {
      LOG_WARN("update job and insert new tasks failed", KR(ret), K(job),
          K(new_strategy), "balance_tasks", partition_balance.get_balance_task());
    } else {
      new_status = ObBalanceJobStatus(ObBalanceJobStatus::BALANCE_JOB_STATUS_DOING);
      ISTAT("update partition balance strategy success", K(job), K(new_status),
          K(new_strategy), "old_strategy", job.get_balance_strategy());
    }
  }
  if (OB_FAIL(ret)
      || !new_status.is_valid()
      || new_status == job.get_job_status()) {
    // skip
  } else if (OB_FAIL(ObBalanceJobTableOperator::update_job_status(
      tenant_id_,
      job.get_job_id(),
      job.get_job_status(),
      new_status,
      !comment.empty()/*update_comment*/,
      comment.string(),
      *GCTX.sql_proxy_))) {
    LOG_WARN("failed to update job status", KR(ret), K(tenant_id_), K(job), K(new_status));
  } else {
    ISTAT("update partition balance job status successfully",
        "job_id", job.get_job_id(), K(new_status), "old_status", job.get_job_status());
  }
  return ret;
}

int ObTenantBalanceService::update_job_and_insert_new_tasks_(
    const ObBalanceJob &old_job,
    const ObBalanceStrategy &new_strategy,
    ObArray<share::ObBalanceTask> &new_tasks)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat failed", KR(ret));
  } else if (OB_UNLIKELY(!new_strategy.is_valid() || new_tasks.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(new_strategy), K(new_tasks));
  } else if (OB_FAIL(construct_dependency_of_each_task_(new_tasks))) {
    LOG_WARN("failed to generate dependency task", KR(ret), K(new_tasks));
  } else {
    START_TRANSACTION(GCTX.sql_proxy_, tenant_id_);
    if (FAILEDx(ObBalanceJobTableOperator::update_job_balance_strategy(
        tenant_id_,
        old_job.get_job_id(),
        old_job.get_job_status(),
        old_job.get_balance_strategy(),
        new_strategy,
        trans))) {
      LOG_WARN("update job balane strategy failed", KR(ret), K(tenant_id_), K(old_job), K(new_strategy));
    } else {
      ARRAY_FOREACH(new_tasks, idx) {
        const ObBalanceTask &task = new_tasks.at(idx);
        if (OB_FAIL(ObBalanceTaskTableOperator::insert_new_task(task, trans))) {
          LOG_WARN("insert new task failed", KR(ret), K(task));
        }
      }
    }
    END_TRANSACTION(trans);
  }
  return ret;
}

#undef SET_TASK_DEPENDENCY
#undef ISTAT
#undef WSTAT
#undef STAT
}
}
