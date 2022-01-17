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
#include "rootserver/backup/ob_backup_backupset.h"
#include "rootserver/ob_zone_manager.h"
#include "rootserver/ob_server_manager.h"
#include "rootserver/ob_root_balancer.h"
#include "rootserver/ob_rebalance_task_mgr.h"
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "share/backup/ob_backup_lease_info_mgr.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/backup/ob_backup_operator.h"
#include "share/backup/ob_backup_backupset_operator.h"
#include "share/backup/ob_extern_backup_info_mgr.h"
#include "share/backup/ob_tenant_backup_task_updater.h"
#include "share/backup/ob_backup_meta_store.h"
#include "lib/container/ob_iarray.h"
#include <algorithm>

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;

namespace oceanbase {
namespace rootserver {

/* ObBackupBackupsetIdling */

int64_t ObBackupBackupsetIdling::get_idle_interval_us()
{
  const int64_t idle_interval_us = GCONF._backup_idle_time;
  return idle_interval_us;
}

/* ObBackupBackupsetLoadBalancer */

ObBackupBackupsetLoadBalancer::ObBackupBackupsetLoadBalancer()
    : is_inited_(false), random_(), zone_mgr_(NULL), server_mgr_(NULL)
{}

ObBackupBackupsetLoadBalancer::~ObBackupBackupsetLoadBalancer()
{}

int ObBackupBackupsetLoadBalancer::init(ObZoneManager& zone_mgr, ObServerManager& server_mgr)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_SUCCESS;
    LOG_WARN("backup backupset load balancer init twice", KR(ret));
  } else {
    zone_mgr_ = &zone_mgr;
    server_mgr_ = &server_mgr;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupBackupsetLoadBalancer::get_next_server(common::ObAddr& server)
{
  int ret = OB_SUCCESS;
  char backup_region[MAX_REGION_LENGTH] = "";
  ObServerManager::ObServerArray server_list;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(GCONF.backup_region.copy(backup_region, sizeof(backup_region)))) {
    LOG_WARN("failed to copy backup region", KR(ret));
  } else if (OB_FAIL(get_all_server_in_region(backup_region, server_list))) {
    LOG_WARN("failed to get all server in region", KR(ret));
  } else if (server_list.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("server list is empty", KR(ret));
  } else if (OB_FAIL(choose_server(server_list, server))) {
    LOG_WARN("failed to choose server");
  }
  return ret;
}

int ObBackupBackupsetLoadBalancer::get_all_server_in_region(
    const common::ObRegion& region, common::ObIArray<ObAddr>& server_list)
{
  int ret = OB_SUCCESS;
  ObArray<ObZone> zone_list;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup validate load balancer do not init", KR(ret));
  } else if (OB_FAIL(get_zone_list(region, zone_list))) {
    LOG_WARN("failed to get zone list", KR(ret), K(region));
  } else {
    int64_t alive_server_count = 0;
    ObArray<ObAddr> tmp_server_list;
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_list.count(); ++i) {
      const ObZone& zone = zone_list.at(i);
      if (OB_FAIL(server_mgr_->get_alive_servers(zone, tmp_server_list))) {
        LOG_WARN("failed to get alive servers", KR(ret), K(zone));
      } else if (OB_FAIL(server_mgr_->get_alive_server_count(zone, alive_server_count))) {
        LOG_WARN("failed to get alive server count", KR(ret), K(zone));
      } else if (alive_server_count != tmp_server_list.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("err unexpected", KR(ret), K(alive_server_count), KR(tmp_server_list.count()));
      } else if (OB_FAIL(append(server_list, tmp_server_list))) {
        LOG_WARN("failed to add array", KR(ret));
      }
    }
  }
  return ret;
}

int ObBackupBackupsetLoadBalancer::get_zone_list(
    const common::ObRegion& region, common::ObIArray<common::ObZone>& zone_list)
{
  int ret = OB_SUCCESS;
  if (region.is_empty()) {
    if (OB_FAIL(zone_mgr_->get_zone(zone_list))) {
      LOG_WARN("failed to get zone list", KR(ret));
    }
  } else {
    if (OB_FAIL(zone_mgr_->get_zone(region, zone_list))) {
      LOG_WARN("failed to get zone list in region", KR(ret), K(region));
    }
  }
  return ret;
}

int ObBackupBackupsetLoadBalancer::choose_server(
    const common::ObIArray<common::ObAddr>& server_list, common::ObAddr& server)
{
  int ret = OB_SUCCESS;
  bool choose = false;
  const int64_t server_cnt = server_list.count();
  while (!choose) {
    int64_t res = random_.get(0, server_cnt - 1);
    common::ObAddr tmp_server;
    tmp_server = server_list.at(res);
    server = tmp_server;
    choose = true;
    LOG_INFO("choose server to do validation task success", K(server));
  }
  return ret;
}

/*  ObBackupBackupset */

ObBackupBackupset::ObBackupBackupset()
    : is_inited_(false),
      extern_device_error_(false),
      sql_proxy_(NULL),
      zone_mgr_(NULL),
      server_mgr_(NULL),
      root_balancer_(NULL),
      rebalance_task_mgr_(NULL),
      rpc_proxy_(NULL),
      schema_service_(NULL),
      backup_lease_service_(NULL),
      idling_(stop_),
      check_time_map_(),
      inner_table_version_(OB_BACKUP_INNER_TABLE_VMAX)
{}

ObBackupBackupset::~ObBackupBackupset()
{}

int ObBackupBackupset::init(ObMySQLProxy& sql_proxy, ObZoneManager& zone_mgr, ObServerManager& server_mgr,
    ObRootBalancer& root_balancer, ObRebalanceTaskMgr& task_mgr, obrpc::ObSrvRpcProxy& rpc_proxy,
    share::schema::ObMultiVersionSchemaService& schema_service, share::ObIBackupLeaseService& backup_lease_service)
{
  int ret = OB_SUCCESS;
  const int64_t thread_num = 1;
  const int64_t MAX_BUCKET = 1024;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup backupset init twice", KR(ret));
  } else if (OB_FAIL(create(thread_num, "ObBackupBackupset"))) {
    LOG_WARN("failed to create thread", KR(ret), K(thread_num));
  } else if (OB_FAIL(check_time_map_.create(MAX_BUCKET, "BACKUP_BACKUP"))) {
    LOG_WARN("failed to create check time map", KR(ret));
  } else {
    sql_proxy_ = &sql_proxy;
    zone_mgr_ = &zone_mgr;
    server_mgr_ = &server_mgr;
    root_balancer_ = &root_balancer;
    rebalance_task_mgr_ = &task_mgr;
    rpc_proxy_ = &rpc_proxy;
    schema_service_ = &schema_service;
    backup_lease_service_ = &backup_lease_service;
    inner_table_version_ = OB_BACKUP_INNER_TABLE_VMAX;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupBackupset::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(ObReentrantThread::start())) {
    LOG_WARN("backup backupset start failed", KR(ret));
  }
  return ret;
}

int ObBackupBackupset::idle()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(idling_.idle())) {
    LOG_WARN("idle failed", KR(ret));
  }
  return ret;
}

void ObBackupBackupset::wakeup()
{
  if (IS_NOT_INIT) {
    LOG_WARN("backup backupset do not init");
  } else {
    idling_.wakeup();
  }
}

void ObBackupBackupset::stop()
{
  if (IS_NOT_INIT) {
    LOG_WARN("backup backupset do not init");
  } else {
    ObReentrantThread::stop();
    idling_.wakeup();
  }
}

void ObBackupBackupset::run3()
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> job_ids;
  ObCurTraceId::init(GCONF.self_addr_);

  while (!stop_) {
    job_ids.reset();

    if (OB_FAIL(check_can_do_work())) {
      LOG_WARN("failed to check can do work", KR(ret));
    } else if (OB_FAIL(get_all_jobs(job_ids))) {
      LOG_WARN("failed to get all jobs", KR(ret));
    } else if (OB_FAIL(do_work(job_ids))) {
      LOG_WARN("failed to work", KR(ret));
    }

    if (OB_FAIL(idle())) {
      LOG_WARN("idle failed", KR(ret));
    } else {
      continue;
    }
  }
}

int ObBackupBackupset::get_check_time(const uint64_t tenant_id, int64_t& check_time)
{
  int ret = OB_SUCCESS;
  check_time = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get lease time get invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_time_map_.get_refactored(tenant_id, check_time))) {
    LOG_WARN("failed to get check time", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObBackupBackupset::update_check_time(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  const int flag = 1;
  const int64_t check_time = ObTimeUtility::current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get lease time get invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_time_map_.set_refactored(tenant_id, check_time, flag))) {
    LOG_WARN("failed to set check time", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObBackupBackupset::insert_check_time(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("insert check time get invalid argument", KR(ret));
  } else {
    int64_t tmp_check_time = 0;
    hash_ret = check_time_map_.get_refactored(tenant_id, tmp_check_time);
    if (OB_SUCCESS == hash_ret) {
      // do nothing
    } else if (OB_HASH_NOT_EXIST == hash_ret) {
      if (OB_FAIL(update_check_time(tenant_id))) {
        LOG_WARN("failed to update check time", KR(ret), K(tenant_id));
      }
    } else {
      ret = hash_ret;
      LOG_WARN("failed to get lease time", KR(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObBackupBackupset::check_can_do_work()
{
  int ret = OB_SUCCESS;
  bool can_do_work = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(backup_lease_service_->get_lease_status(can_do_work))) {
    LOG_WARN("failed to get lease status", KR(ret));
  } else if (!can_do_work) {
    ret = OB_RS_STATE_NOT_ALLOW;
    LOG_WARN("leader may switch, can not do work", KR(ret));
  } else if (is_valid_backup_inner_table_version(inner_table_version_) &&
             inner_table_version_ >= OB_BACKUP_INNER_TABLE_V3) {
    // inner table version is new enough
  } else if (OB_FAIL(ObBackupInfoOperator::get_inner_table_version(*sql_proxy_, inner_table_version_))) {
    LOG_WARN("Failed to get inner table version", K(ret));
  } else if (inner_table_version_ < OB_BACKUP_INNER_TABLE_V3) {
    ret = OB_EAGAIN;
    LOG_INFO("inner table version is too old, waiting backup inner table upgrade", K(ret), K(inner_table_version_));
  }
  return ret;
}

int ObBackupBackupset::do_work(const common::ObIArray<int64_t>& job_ids)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (job_ids.empty()) {
    LOG_INFO("no backup backupset job to do");
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < job_ids.count(); ++i) {
      const int64_t job_id = job_ids.at(i);
      if (OB_SUCCESS != (tmp_ret = do_job(job_id))) {
        LOG_WARN("failed to do job", KR(tmp_ret), K(job_id));
      }
    }
  }
  return ret;
}

int ObBackupBackupset::get_all_jobs(common::ObIArray<int64_t>& job_ids)
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupBackupsetJobInfo> job_list;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(ObBackupBackupsetOperator::get_all_task_items(job_list, *sql_proxy_))) {
    LOG_WARN("failed to get all task items", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < job_list.count(); ++i) {
      const ObBackupBackupsetJobInfo& job_info = job_list.at(i);
      if (OB_FAIL(job_ids.push_back(job_info.job_id_))) {
        LOG_WARN("failed to push back job id", KR(ret), K(job_info));
      }
    }
  }
  return ret;
}

int ObBackupBackupset::do_all_tenant_tasks(const common::ObIArray<ObTenantBackupBackupsetTaskInfo>& task_infos)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (task_infos.empty()) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < task_infos.count(); ++i) {
      const ObTenantBackupBackupsetTaskInfo& task_info = task_infos.at(i);
      const uint64_t tenant_id = task_info.tenant_id_;
      const int64_t job_id = task_info.job_id_;
      const int64_t backup_set_id = task_info.backup_set_id_;
      const int64_t copy_id = task_info.copy_id_;
      if (OB_SYS_TENANT_ID == tenant_id) {
        continue;
      } else if (OB_FAIL(do_tenant_task(tenant_id, job_id, backup_set_id, copy_id))) {
        LOG_WARN("failed to do tenant task", KR(ret), K(task_info));
      }
    }
  }
  return ret;
}

int ObBackupBackupset::do_tenant_task(
    const uint64_t tenant_id, const int64_t job_id, const int64_t backup_set_id, const int64_t copy_id)
{
  int ret = OB_SUCCESS;
  bool is_dropped = false;
  ObTenantBackupBackupset tenant_backup_backupset;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(check_tenant_is_dropped(tenant_id, is_dropped))) {
    LOG_WARN("failed to check tenant is dropped", KR(ret), K(tenant_id));
  } else if (OB_FAIL(tenant_backup_backupset.init(is_dropped,
                 tenant_id,
                 job_id,
                 backup_set_id,
                 copy_id,
                 *sql_proxy_,
                 *zone_mgr_,
                 *server_mgr_,
                 *root_balancer_,
                 *rebalance_task_mgr_,
                 *this,
                 *rpc_proxy_,
                 *backup_lease_service_))) {
    LOG_WARN("failed to init tenant backup backupset", KR(ret), K(tenant_id));
  } else if (OB_FAIL(tenant_backup_backupset.do_task())) {
    LOG_WARN("failed to do backup backupset task for tenant", KR(ret), K(tenant_id));
  }

  return ret;
}

int ObBackupBackupset::filter_success_backup_task(common::ObIArray<ObTenantBackupTaskItem>& backup_tasks)
{
  int ret = OB_SUCCESS;
  ObBackupDest backup_dest;
  char backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
  ObArray<ObTenantBackupTaskItem> tmp_tasks;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(GCONF.backup_dest.copy(backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to copy backup backup dest", KR(ret));
  } else if (OB_FAIL(backup_dest.set(backup_dest_str))) {
    LOG_WARN("failed to set backup dest", KR(ret), K(backup_dest_str));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_tasks.count(); ++i) {
      const ObTenantBackupTaskItem& task_info = backup_tasks.at(i);
      if (task_info.compatible_ <= OB_BACKUP_COMPATIBLE_VERSION_V1 ||
          task_info.compatible_ >= OB_BACKUP_COMPATIBLE_VERSION_MAX) {
        // backup backup do not deal with old compatible version
        continue;
      } else if (!task_info.backup_dest_.is_root_path_equal(backup_dest)) {
        // only backup backup those dest same with GCONF.backup_dest
        continue;
      } else if (OB_SUCCESS != task_info.result_) {
        continue;
      } else if (OB_FAIL(tmp_tasks.push_back(task_info))) {
        LOG_WARN("failed to push back", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      backup_tasks.reset();
      if (OB_FAIL(append(backup_tasks, tmp_tasks))) {
        LOG_WARN("failed to add array", KR(ret));
      }
    }
  }
  return ret;
}

int ObBackupBackupset::get_all_backup_set_id(const uint64_t tenant_id, const share::ObBackupBackupsetJobInfo& job_info,
    common::ObArray<int64_t>& backup_set_ids, common::ObArray<ObTenantBackupBackupsetTaskInfo>& no_need_schedule_tasks)
{
  int ret = OB_SUCCESS;
  no_need_schedule_tasks.reset();
  ObArray<ObTenantBackupTaskItem> backup_tasks;
  ObArray<ObTenantBackupBackupsetTaskItem> backup_backupset_tasks;
  const ObBackupBackupsetType& type = job_info.type_;
  const int64_t backup_set_id = job_info.backup_set_id_;
  char backup_dest_str[share::OB_MAX_BACKUP_DEST_LENGTH] = "";
  ObBackupDest dest;
  bool is_continuous = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (!job_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("job info is not valid", KR(ret), K(job_info));
  } else if (OB_FAIL(get_dst_backup_dest_str(job_info, backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to get dst backup dest str", KR(ret));
  } else if (OB_FAIL(dest.set(backup_dest_str))) {
    LOG_WARN("failed to set backup dest", KR(ret));
  } else if (OB_FAIL(ObBackupTaskHistoryOperator::get_tenant_backup_tasks(tenant_id, *sql_proxy_, backup_tasks))) {
    LOG_WARN("failed to get tenant backup tasks", KR(ret), K(tenant_id));
  } else if (OB_FAIL(filter_success_backup_task(backup_tasks))) {
    LOG_WARN("failed to filter success backup task", KR(ret));
  } else if (backup_tasks.empty()) {
    LOG_INFO("no backup task no be backed up", K(tenant_id));
  } else if (OB_FAIL(check_backup_tasks_is_continuous(tenant_id, job_info, backup_tasks, is_continuous))) {
    LOG_WARN("failed to check backup tasks is continuous", KR(ret), K(tenant_id), K(job_info), K(backup_tasks));
  } else if (!is_continuous) {
    ret = OB_BACKUP_BACKUP_CAN_NOT_START;
    LOG_WARN("backup task is not continuous", KR(ret));
  } else if (OB_FAIL(ObTenantBackupBackupsetHistoryOperator::get_task_items_with_same_dest(
                 tenant_id, dest, backup_backupset_tasks, *sql_proxy_))) {
    LOG_WARN("failed to get tenant backup backupset task history tasks", KR(ret), K(tenant_id));
  } else {
    CompareHistoryBackupTaskItem cmp_backup;
    CompareHistoryBackupBackupsetTaskItem cmp_backup_backupset;
    std::sort(backup_tasks.begin(), backup_tasks.end(), cmp_backup);
    std::sort(backup_backupset_tasks.begin(), backup_backupset_tasks.end(), cmp_backup_backupset);
    switch (type.type_) {
      case ObBackupBackupsetType::ALL_BACKUP_SET: {
        if (OB_FAIL(get_full_backup_set_id_list(job_info,
                backup_backupset_tasks,
                backup_tasks,
                tenant_id,
                backup_set_id,
                backup_set_ids,
                no_need_schedule_tasks))) {
          LOG_WARN("failed to get full backup set id list", KR(ret), K(tenant_id), K(backup_set_id));
        }
        break;
      }
      case ObBackupBackupsetType::SINGLE_BACKUP_SET: {
        if (OB_FAIL(get_single_backup_set_id_list(job_info,
                backup_backupset_tasks,
                backup_tasks,
                tenant_id,
                backup_set_id,
                backup_set_ids,
                no_need_schedule_tasks))) {
          LOG_WARN("failed to get inc backup set id list", KR(ret), K(tenant_id), K(backup_set_id));
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("err unexpected", K(tenant_id), K(backup_set_id));
        break;
    }
  }
  return ret;
}

// 1,2  1Full 2Inc
// 3,4  3Full 4Inc
// 5,6  5Full 6Inc
// The user specifies 6 inputs, 1, 2, 3, 4, 5, 6
int ObBackupBackupset::get_full_backup_set_id_list(const share::ObBackupBackupsetJobInfo& job_info,
    const ObArray<ObTenantBackupBackupsetTaskItem>& backup_backupset_tasks,
    const ObArray<ObTenantBackupTaskItem>& backup_tasks, const uint64_t tenant_id, const int64_t backup_set_id,
    common::ObArray<int64_t>& backup_set_ids, common::ObArray<ObTenantBackupBackupsetTaskInfo>& no_need_schedule_tasks)
{
  int ret = OB_SUCCESS;
  backup_set_ids.reset();
  no_need_schedule_tasks.reset();
  BackupArrayIter iter;
  int64_t smallest_backup_set_id_in_bb = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(find_backup_task_lower_bound(backup_tasks, backup_set_id, iter))) {
    LOG_WARN("failed to find backup task lower bound", KR(ret), K(backup_set_id));
  } else if (iter->backup_set_id_ > backup_set_id) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("backup set id do not exist", KR(ret), K(backup_set_id), K(*iter));
  } else {
    if (backup_backupset_tasks.empty()) {
      smallest_backup_set_id_in_bb = 0;
    } else {
      // if no success task, the smallest_backup_set_id_in_bb is 0
      for (int64_t i = 0; OB_SUCC(ret) && i < backup_backupset_tasks.count(); ++i) {
        const ObTenantBackupBackupsetTaskItem& task = backup_backupset_tasks.at(i);
        if (OB_SUCCESS == task.result_) {
          smallest_backup_set_id_in_bb = task.backup_set_id_;
          break;
        }
      }
    }

    while (OB_SUCC(ret) && iter >= backup_tasks.begin()) {
      bool exists = false;
      const int64_t tmp_id = iter->backup_set_id_;
      for (int64_t i = 0; OB_SUCC(ret) && i < backup_backupset_tasks.count(); ++i) {
        const ObTenantBackupBackupsetTaskInfo& info = backup_backupset_tasks.at(i);
        if (info.backup_set_id_ == tmp_id && OB_SUCCESS == info.result_) {
          exists = true;
          if (OB_FAIL(no_need_schedule_tasks.push_back(info))) {
            LOG_WARN("failed to push backup tasks", KR(ret), K(info));
          }
          break;
        }
      }
      if (tmp_id == smallest_backup_set_id_in_bb) {
        break;
      }

      if (OB_SUCC(ret) && !exists && !iter->is_mark_deleted_) {
        bool deleted_before = false;
        if (OB_FAIL(check_backup_backup_has_been_deleted_before(
                job_info, tmp_id, OB_START_INCARNATION, tenant_id, deleted_before))) {
          LOG_WARN("failed to check backup backup has been deleted before", KR(ret));
        } else if (deleted_before) {
          LOG_INFO("backup backup has been deleted before", K(tmp_id), K(tenant_id));
          // do nothing
        } else if (OB_FAIL(backup_set_ids.push_back(tmp_id))) {
          LOG_WARN("failed to push back backup set id", KR(ret), K(tmp_id));
        }
      }
      --iter;
    }
    std::sort(backup_set_ids.begin(), backup_set_ids.end());
  }
  return ret;
}

int ObBackupBackupset::check_backup_tasks_is_continuous(const uint64_t tenant_id,
    const share::ObBackupBackupsetJobInfo& job_info, const common::ObArray<share::ObTenantBackupTaskItem>& backup_tasks,
    bool& is_continuous)
{
  int ret = OB_SUCCESS;
  is_continuous = true;
  bool is_same_with_gconf = false;
  CompareHistoryBackupTaskItem cmp_backup;
  ObArray<ObTenantBackupTaskItem> backup_list;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(check_dest_same_with_gconf(job_info, is_same_with_gconf))) {
    LOG_WARN("failed to check dest same with gconf", KR(ret), K(job_info));
  } else if (!is_same_with_gconf) {
    // only check job at gconf
  } else if (OB_FAIL(backup_list.assign(backup_tasks))) {
    LOG_WARN("failed to assign backup tasks", KR(ret));
  } else {
    std::sort(backup_list.begin(), backup_list.end(), cmp_backup);
    for (int64_t i = backup_list.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
      const ObTenantBackupTaskItem& info = backup_list.at(i);
      if (OB_SUCCESS != info.result_) {
        continue;
      } else if (ObBackupType::FULL_BACKUP == info.backup_type_.type_) {
        // I I F I I I
        // 2 3 4 5 6 7
        // how to identify the relation of backup set id 3 and 4
        continue;
      } else {
        bool prev_inc_exist = false;
        const int64_t prev_inc_backup_set_id = info.prev_inc_backup_set_id_;
        for (int64_t j = i - 1; OB_SUCC(ret) && j >= 0; --j) {
          const ObTenantBackupTaskItem& prev_info = backup_list.at(j);
          if (prev_info.backup_set_id_ == prev_inc_backup_set_id) {
            prev_inc_exist = true;
            break;
          }
        }
        if (OB_SUCC(ret) && !prev_inc_exist) {
          if (0 == i) {
            bool has_backup_set_copies = false;
            ObArray<ObBackupSetFileInfo> backup_set_files;
            // if backup tasks do not have full backup set id, we need to check if backup backup
            // already has this backup set id, if not, the backup backup is not continuous
            if (OB_FAIL(ObBackupSetFilesOperator::get_backup_set_file_info_copies(
                    tenant_id, OB_START_INCARNATION, prev_inc_backup_set_id, *sql_proxy_, backup_set_files))) {
              LOG_WARN("failed to get backup set file info copies", KR(ret), K(tenant_id), K(prev_inc_backup_set_id));
            } else {
              for (int64_t k = 0; OB_SUCC(ret) && k < backup_set_files.count(); ++k) {
                const ObBackupSetFileInfo& file_info = backup_set_files.at(k);
                if (file_info.copy_id_ > 0 && OB_SUCCESS == file_info.result_) {
                  has_backup_set_copies = true;
                  break;
                }
              }
            }
            if (OB_SUCC(ret) && !has_backup_set_copies) {
              is_continuous = false;
              LOG_WARN("backup set is not continuous", KR(ret), K(backup_list));
              break;
            }
          } else {
            is_continuous = false;
            LOG_WARN("backup set is not continuous", KR(ret), K(backup_list));
            break;
          }
        }
      }
    }
  }
  return ret;
}

int ObBackupBackupset::get_single_backup_set_id_list(const share::ObBackupBackupsetJobInfo& job_info,
    const ObArray<ObTenantBackupBackupsetTaskItem>& backup_backupset_tasks,
    const ObArray<ObTenantBackupTaskItem>& backup_tasks, const uint64_t tenant_id, const int64_t backup_set_id,
    common::ObArray<int64_t>& backup_set_ids, common::ObArray<ObTenantBackupBackupsetTaskInfo>& no_need_schedule_tasks)
{
  int ret = OB_SUCCESS;
  backup_set_ids.reset();
  BackupArrayIter iter;
  bool is_same_with_gconf = false;
  bool already_backup_backupset = false;
  bool deleted_before = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(find_backup_task_lower_bound(backup_tasks, backup_set_id, iter))) {
    LOG_WARN("failed to find backup task lower bound", KR(ret), K(backup_set_id));
  } else if (has_independ_inc_backup_set(iter->compatible_)) {
    if (OB_FAIL(backup_set_ids.push_back(backup_set_id))) {
      LOG_WARN("failed to push back backupset id", KR(ret));
    }
  } else if (iter->backup_set_id_ != backup_set_id) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("backup set id do not exist", KR(ret), K(backup_set_id), K(*iter), K(backup_tasks));
  } else if (OB_FAIL(check_already_backup_backupset(
                 backup_backupset_tasks, backup_set_id, already_backup_backupset, no_need_schedule_tasks))) {
    LOG_WARN("failed to check already backup backupset", KR(ret), K(backup_set_id), K(backup_backupset_tasks));
  } else if (already_backup_backupset) {
    LOG_WARN("backup set already backup backup", K(backup_set_id), K(backup_backupset_tasks));
  } else if (OB_FAIL(check_dest_same_with_gconf(job_info, is_same_with_gconf))) {
    LOG_WARN("failed to check dest same with gconf", KR(ret), K(job_info));
  } else if (OB_FAIL(check_backup_backup_has_been_deleted_before(
                 job_info, backup_set_id, OB_START_INCARNATION, tenant_id, deleted_before))) {
    LOG_WARN("failed to check backup backup has been deleted before", KR(ret), K(backup_set_id), K(tenant_id));
  } else if (deleted_before) {
    LOG_INFO("backup backup has been deleted before, no need backup backup", K(tenant_id), K(backup_set_id));
  } else {
    ObBackupType::BackupType backup_type = iter->backup_type_.type_;
    if (backup_backupset_tasks.empty()) {
      if (ObBackupType::FULL_BACKUP != backup_type && is_same_with_gconf) {
        ret = OB_INVALID_BACKUP_SET_ID;
        LOG_WARN("the first backup backupset you backup backup must be full backup set id",
            KR(ret),
            K(backup_tasks),
            K(backup_backupset_tasks),
            K(*iter));
      }
    } else {
      --iter;
      int64_t prev_backup_set_id = 0;
      while (OB_SUCC(ret) && iter >= backup_tasks.begin()) {
        if (OB_SUCCESS == iter->result_) {
          prev_backup_set_id = iter->backup_set_id_;
          break;
        } else {
          --iter;
        }
      }
      if (0 != prev_backup_set_id) {
        bool exists = false;
        for (int64_t i = 0; OB_SUCC(ret) && i < backup_backupset_tasks.count(); ++i) {
          const ObTenantBackupBackupsetTaskInfo& info = backup_backupset_tasks.at(i);
          if (info.backup_set_id_ == prev_backup_set_id && OB_SUCCESS == info.result_) {
            exists = true;
            break;
          }
        }
        if (OB_SUCC(ret) && !exists) {
          ret = OB_INVALID_BACKUP_SET_ID;
          LOG_WARN("backup set id not exist", K(ret), K(prev_backup_set_id));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(backup_set_ids.push_back(backup_set_id))) {
        LOG_WARN("failed to push back", KR(ret), K(backup_set_id));
      }
    }
  }
  return ret;
}

int ObBackupBackupset::check_backup_backup_has_been_deleted_before(const share::ObBackupBackupsetJobInfo& info,
    const int64_t backup_set_id, const int64_t incarnation, const uint64_t tenant_id, bool& deleted_before)
{
  int ret = OB_SUCCESS;
  deleted_before = false;
  ObBackupBackupCopyIdLevel copy_id_level = OB_BB_COPY_ID_LEVEL_MAX;
  ObArray<ObBackupSetFileInfo> backup_set_file_infos;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(get_job_copy_id_level(info, copy_id_level))) {
    LOG_WARN("failed to get job copy id level", KR(ret), K(info));
  } else if (OB_FAIL(ObBackupSetFilesOperator::get_backup_set_file_info_copy_list(
                 info.tenant_id_, incarnation, backup_set_id, copy_id_level, *sql_proxy_, backup_set_file_infos))) {
    LOG_WARN("failed to get backup set file info copy list", KR(ret), K(tenant_id), K(backup_set_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_set_file_infos.count(); ++i) {
      ObBackupDest backup_dest;
      const ObBackupSetFileInfo& file_info = backup_set_file_infos.at(i);
      if (OB_FAIL(file_info.get_backup_dest(backup_dest))) {
        LOG_WARN("failed to get backup dest", KR(ret), K(file_info));
      } else if (info.backup_dest_.is_root_path_equal(backup_dest)) {
        if (ObBackupFileStatus::BACKUP_FILE_DELETING == file_info.file_status_ ||
            ObBackupFileStatus::BACKUP_FILE_DELETED == file_info.file_status_) {
          deleted_before = true;
          break;
        }
      }
    }
  }
  return ret;
}

int ObBackupBackupset::check_already_backup_backupset(
    const common::ObArray<share::ObTenantBackupBackupsetTaskItem>& backup_backupset_tasks, const int64_t backup_set_id,
    bool& already_backup_backupset, common::ObArray<share::ObTenantBackupBackupsetTaskInfo>& no_need_schedule_tasks)
{
  int ret = OB_SUCCESS;
  no_need_schedule_tasks.reset();
  already_backup_backupset = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (backup_backupset_tasks.empty()) {
    already_backup_backupset = false;
  } else if (backup_set_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret), K(backup_set_id));
  } else {
    CompareBackupBackupsetTaskBackupSetId cmp_backup_backupset;
    typedef ObArray<share::ObTenantBackupBackupsetTaskInfo>::const_iterator Iter;
    Iter iter = std::lower_bound(
        backup_backupset_tasks.begin(), backup_backupset_tasks.end(), backup_set_id, cmp_backup_backupset);
    if (iter != backup_backupset_tasks.end()) {
      already_backup_backupset = backup_set_id == iter->backup_set_id_ && OB_SUCCESS == iter->result_;
      if (already_backup_backupset) {
        if (OB_FAIL(no_need_schedule_tasks.push_back(*iter))) {
          LOG_WARN("failed to push back", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObBackupBackupset::find_backup_task_lower_bound(const common::ObArray<share::ObTenantBackupTaskItem>& tasks,
    const int64_t backup_set_id, BackupArrayIter& output_iter)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (tasks.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tasks should not be empty", KR(ret), K(tasks));
  } else {
    CompareBackupTaskBackupSetId cmp;
    BackupArrayIter iter = std::lower_bound(tasks.begin(), tasks.end(), backup_set_id, cmp);
    if (iter == tasks.end()) {
      --iter;
    } else if (iter != tasks.begin() && iter->backup_set_id_ > backup_set_id) {
      --iter;
    }
    output_iter = iter;
  }
  return ret;
}

int ObBackupBackupset::get_largest_already_backup_backupset_id(
    const common::ObIArray<share::ObTenantBackupBackupsetTaskItem>& tasks, int64_t& backup_set_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (tasks.empty()) {
    backup_set_id = 0;
  } else {
    backup_set_id = tasks.at(tasks.count() - 1).backup_set_id_;
  }
  return ret;
}

int ObBackupBackupset::get_all_tenants(
    const share::ObBackupBackupsetJobInfo& job_info, common::ObIArray<SimpleBackupBackupsetTenant>& tenant_list)
{
  int ret = OB_SUCCESS;
  tenant_list.reset();
  ObBackupTaskHistoryUpdater updater;
  const bool is_smaller_equal = ObBackupBackupsetType::ALL_BACKUP_SET == job_info.type_.type_;
  const int64_t backup_set_id = job_info.backup_set_id_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (!job_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret), K(job_info));
  } else {
    if (OB_SYS_TENANT_ID == job_info.tenant_id_) {
      ObArray<uint64_t> tenant_id_list;
      if (OB_FAIL(ObBackupTaskHistoryOperator::get_all_tenant_backup_set_matches(
              backup_set_id, is_smaller_equal, *sql_proxy_, tenant_id_list))) {
        LOG_WARN("failed to get all tenant backup set matches", KR(ret), K(backup_set_id), K(is_smaller_equal));
      } else {
        SimpleBackupBackupsetTenant tenant;
        for (int64_t i = 0; OB_SUCC(ret) && i < tenant_id_list.count(); ++i) {
          tenant.reset();
          const uint64_t tenant_id = tenant_id_list.at(i);
          bool is_dropped = true;
          if (OB_FAIL(check_tenant_is_dropped(tenant_id, is_dropped))) {
            LOG_WARN("failed to check tenant is dropped", KR(ret), K(tenant_id));
          } else {
            tenant.tenant_id_ = tenant_id;
            tenant.is_dropped_ = is_dropped;
            if (OB_FAIL(tenant_list.push_back(tenant))) {
              LOG_WARN("failed to push back tenant id", KR(ret), K(tenant));
            } else if (OB_FAIL(insert_check_time(tenant_id))) {
              LOG_WARN("failed to insert check time", KR(ret), K(tenant_id));
            }
          }
        }
      }
    } else {
      SimpleBackupBackupsetTenant tenant;
      bool is_dropped = true;
      const uint64_t tenant_id = job_info.tenant_id_;
      if (OB_FAIL(check_tenant_is_dropped(tenant_id, is_dropped))) {
        LOG_WARN("failed to check tenant is dropped", KR(ret), K(tenant_id));
      } else {
        tenant.tenant_id_ = tenant_id;
        tenant.is_dropped_ = is_dropped;
        if (OB_FAIL(tenant_list.push_back(tenant))) {
          LOG_WARN("failed to push back tenant id", KR(ret), K(tenant));
        } else if (OB_FAIL(insert_check_time(tenant_id))) {
          LOG_WARN("failed to insert check time", KR(ret), K(tenant_id));
        }
      }
    }
  }
  return ret;
}

int ObBackupBackupset::get_all_job_tasks(
    const share::ObBackupBackupsetJobInfo& job_info, common::ObIArray<ObTenantBackupBackupsetTaskInfo>& task_infos)
{
  int ret = OB_SUCCESS;
  const int64_t job_id = job_info.job_id_;
  ObArray<SimpleBackupBackupsetTenant> tenant_list;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(get_all_tenants(job_info, tenant_list))) {
    LOG_WARN("failed to get all tenant ids", KR(ret), K(job_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_list.count(); ++i) {
      const SimpleBackupBackupsetTenant& tenant = tenant_list.at(i);
      ObArray<ObTenantBackupBackupsetTaskInfo> tenant_task_infos;
      if (OB_FAIL(ObTenantBackupBackupsetOperator::get_task_items(tenant, job_id, tenant_task_infos, *sql_proxy_))) {
        LOG_WARN("failed to get task items", KR(ret), K(job_id), K(tenant));
      } else if (OB_FAIL(append(task_infos, tenant_task_infos))) {
        LOG_WARN("failed to add array", KR(ret));
      }
    }
  }
  return ret;
}

int ObBackupBackupset::get_all_job_tasks_from_history(const share::ObBackupBackupsetJobInfo& job_info,
    common::ObIArray<share::ObTenantBackupBackupsetTaskInfo>& task_infos)
{
  int ret = OB_SUCCESS;
  task_infos.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (!job_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret), K(job_info));
  } else if (OB_FAIL(ObTenantBackupBackupsetHistoryOperator::get_all_job_tasks(
                 job_info.job_id_, false /*for_update*/, task_infos, *sql_proxy_))) {
    LOG_WARN("failed to get all job tasks", KR(ret), K(job_info));
  }
  return ret;
}

int ObBackupBackupset::get_current_round_tasks(
    const share::ObBackupBackupsetJobInfo& job_info, common::ObIArray<ObTenantBackupBackupsetTaskInfo>& task_infos)
{
  int ret = OB_SUCCESS;
  task_infos.reset();
  ObArray<SimpleBackupBackupsetTenant> tenant_list;
  const int64_t job_id = job_info.job_id_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(get_all_tenants(job_info, tenant_list))) {
    LOG_WARN("failed to get all tenant ids", KR(ret), K(job_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_list.count(); ++i) {
      const SimpleBackupBackupsetTenant& tenant = tenant_list.at(i);
      const uint64_t tenant_id = tenant.tenant_id_;
      ObArray<ObTenantBackupBackupsetTaskInfo> tenant_task_infos;
      ObTenantBackupBackupsetTaskInfo smallest_task;
      if (OB_SYS_TENANT_ID == tenant_id) {
        continue;
      } else if (OB_FAIL(ObTenantBackupBackupsetOperator::get_unfinished_task_items(
                     tenant, job_id, tenant_task_infos, *sql_proxy_))) {
        LOG_WARN("failed to get task items", KR(ret), K(job_id), K(tenant));
      } else if (tenant_task_infos.empty()) {
        // do nothing
      } else if (OB_FAIL(find_smallest_unfinished_task(tenant_task_infos, smallest_task))) {
        LOG_WARN("failed to find smallest unfinished tasks", KR(ret));
      } else if (OB_FAIL(task_infos.push_back(smallest_task))) {
        LOG_WARN("failed to push back task info", KR(ret), K(smallest_task));
      }
    }
  }
  return ret;
}

int ObBackupBackupset::find_smallest_unfinished_task(
    const common::ObIArray<ObTenantBackupBackupsetTaskInfo>& task_infos,
    ObTenantBackupBackupsetTaskInfo& smallest_task_info)
{
  int ret = OB_SUCCESS;
  smallest_task_info.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (task_infos.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task infos should not be empty", KR(ret));
  } else {
    int64_t smallest_backup_set_id = INT64_MAX;
    for (int64_t i = 0; OB_SUCC(ret) && i < task_infos.count(); ++i) {
      const ObTenantBackupBackupsetTaskInfo& task_info = task_infos.at(i);
      const int64_t backup_set_id = task_info.backup_set_id_;
      if (backup_set_id < smallest_backup_set_id) {
        smallest_backup_set_id = backup_set_id;
        smallest_task_info = task_info;
      }
    }
  }
  return ret;
}

int ObBackupBackupset::check_tenant_is_dropped(const uint64_t tenant_id, bool& is_dropped)
{
  int ret = OB_SUCCESS;
  is_dropped = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check tenant exist get invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service_->check_if_tenant_has_been_dropped(tenant_id, is_dropped))) {
    LOG_WARN("failed tocheck if tenant has been dropped", K(ret), K(tenant_id));
  }
  return ret;
}

int ObBackupBackupset::check_all_tenant_task_finished(
    const share::ObBackupBackupsetJobInfo& job_info, bool& all_finished)
{
  int ret = OB_SUCCESS;
  all_finished = true;
  ObArray<ObTenantBackupBackupsetTaskInfo> task_infos;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(get_all_job_tasks(job_info, task_infos))) {
    LOG_WARN("failed to get all job tasks", KR(ret), K(job_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < task_infos.count(); ++i) {
      const ObTenantBackupBackupsetTaskInfo& tenant_task_info = task_infos.at(i);
      if (OB_SYS_TENANT_ID == tenant_task_info.tenant_id_) {
        continue;
      } else if (ObTenantBackupBackupsetTaskInfo::FINISH != tenant_task_info.task_status_) {
        all_finished = false;
        break;
      }
    }
  }
  return ret;
}

int ObBackupBackupset::move_job_info_to_history(const int64_t job_id, const int64_t result)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObBackupBackupsetJobInfo job_info;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    LOG_WARN("failed to start trans", KR(ret));
  } else {
    if (OB_FAIL(ObBackupBackupsetOperator::get_task_item(job_id, job_info, trans))) {
      LOG_WARN("failed to get task item", KR(ret), K(job_id));
    } else {
      job_info.result_ = OB_SUCCESS == job_info.result_ ? result : job_info.result_;
      if (OB_FAIL(ObBackupBackupsetHistoryOperator::insert_job_item(job_info, trans))) {
        LOG_WARN("failed to insert job info to history", KR(ret), K(job_id));
      } else if (OB_FAIL(ObBackupBackupsetOperator::remove_job_item(job_id, trans))) {
        LOG_WARN("failed to remove job item", KR(ret), K(job_id));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true /*commit*/))) {
        LOG_WARN("failed to commit", KR(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false /* commit*/))) {
        LOG_WARN("failed to rollback trans", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObBackupBackupset::update_cluster_data_backup_info(const ObBackupBackupsetJobInfo& job_info)
{
  int ret = OB_SUCCESS;
  ObTenantBackupTaskInfo backup_task_info;
  ObBackupTaskHistoryUpdater updater;
  const int64_t backup_set_id = job_info.backup_set_id_;
  ObClusterBackupDest src, dst;
  char dst_backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
  const bool for_update = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(updater.init(*sql_proxy_))) {
    LOG_WARN("failed to init backup task history updater", KR(ret));
  } else if (OB_FAIL(updater.get_original_tenant_backup_task(
                 OB_SYS_TENANT_ID, backup_set_id, for_update, backup_task_info))) {
    LOG_WARN("failed to get tenant backup task info", KR(ret), K(backup_set_id), K(OB_SYS_TENANT_ID));
  } else if (OB_FAIL(get_dst_backup_dest_str(job_info, dst_backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to get dst backup dest str", KR(ret));
  } else if (OB_FAIL(src.set(backup_task_info.backup_dest_, OB_START_INCARNATION))) {
    LOG_WARN("failed to set src backup dest", KR(ret), K(backup_task_info));
  } else if (OB_FAIL(dst.set(dst_backup_dest_str, OB_START_INCARNATION))) {
    LOG_WARN("failed to set dst backup dest", KR(ret), K(dst_backup_dest_str));
  } else if (OB_FAIL(do_extern_cluster_backup_info(job_info, src, dst))) {
    LOG_WARN("failed to do extern cluster backup info", KR(ret), K(job_info), K(src), K(dst));
  }
  return ret;
}

int ObBackupBackupset::do_extern_cluster_backup_info(const share::ObBackupBackupsetJobInfo& job_info,
    const share::ObClusterBackupDest& src, const share::ObClusterBackupDest& dst)
{
  int ret = OB_SUCCESS;
  ObExternBackupInfoMgr src_mgr, dst_mgr;
  int64_t dst_end_backup_set_id = 0;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  const int64_t backup_set_id = job_info.backup_set_id_;
  ObArray<ObExternBackupInfo> src_extern_backup_infos, dst_extern_backup_infos;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(src_mgr.init(tenant_id, src, *backup_lease_service_))) {
    LOG_WARN("failed to init src extern backup info mgr", KR(ret), K(src));
  } else if (OB_FAIL(dst_mgr.init(tenant_id, dst, *backup_lease_service_))) {
    LOG_WARN("failed to init dst extern backup info mgr", KR(ret), K(dst));
  } else if (OB_FAIL(src_mgr.get_extern_backup_infos(src_extern_backup_infos))) {
    LOG_WARN("failed to get src extern backup infos", KR(ret), K(src));
  } else if (OB_FAIL(dst_mgr.get_extern_backup_infos(dst_extern_backup_infos))) {
    LOG_WARN("failed to get dst extern backup infos", KR(ret), K(dst));
  } else {
    if (!dst_extern_backup_infos.empty()) {
      dst_end_backup_set_id = dst_extern_backup_infos.at(dst_extern_backup_infos.count() - 1).inc_backup_set_id_;
    } else {
      dst_end_backup_set_id = 0;
    }
    int64_t src_start_id = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < src_extern_backup_infos.count(); ++i) {
      const ObExternBackupInfo& extern_info = src_extern_backup_infos.at(i);
      if (dst_end_backup_set_id + 1 == extern_info.inc_backup_set_id_) {
        src_start_id = i;
        break;
      }
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("do extern cluster backup info failed", KR(ret));
    } else {
      for (int64_t i = src_start_id; OB_SUCC(ret) && i < src_extern_backup_infos.count(); ++i) {
        const ObExternBackupInfo& extern_info = src_extern_backup_infos.at(i);
        if (OB_FAIL(dst_mgr.upload_backup_info(extern_info, true /*search*/))) {
          LOG_WARN("failed to upload backup info", KR(ret));
        } else if (backup_set_id == extern_info.inc_backup_set_id_) {
          break;
        }
      }
    }
  }
  return ret;
}

int ObBackupBackupset::do_job(const int64_t job_id)
{
  int ret = OB_SUCCESS;
  ObBackupBackupsetJobInfo job_info;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(get_job_info(job_id, job_info))) {
    LOG_WARN("failed to get job status", KR(ret), K(job_id));
  } else if (OB_FAIL(check_can_do_work())) {
    LOG_WARN("failed to check can do work", KR(ret));
  } else {
    switch (job_info.job_status_) {
      case ObBackupBackupsetJobInfo::SCHEDULE: {
        if (OB_FAIL(do_schedule(job_info))) {
          LOG_WARN("failed to do schedule", KR(ret), K(job_info));
        }
        break;
      }
      case ObBackupBackupsetJobInfo::BACKUP: {
        if (OB_FAIL(do_backup(job_info))) {
          LOG_WARN("failed to do backup", KR(ret), K(job_info));
        }
        break;
      }
      case ObBackupBackupsetJobInfo::CLEAN: {
        if (OB_FAIL(do_clean(job_info))) {
          LOG_WARN("failed to do clean", KR(ret), K(job_info));
        }
        break;
      }
      case ObBackupBackupsetJobInfo::CANCEL: {
        if (OB_FAIL(do_cancel(job_info))) {
          LOG_WARN("failed to do cancel", KR(ret), K(job_info));
        }
        break;
      }
      case ObBackupBackupsetJobInfo::FINISH: {
        if (OB_FAIL(do_finish(job_info))) {
          LOG_WARN("failed to do finish", KR(ret), K(job_info));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("status is not valid", KR(ret));
        break;
      }
    }
    LOG_INFO("doing backup backupset", KR(ret), K(job_info));
  }
  return ret;
}

int ObBackupBackupset::get_job_info(const int64_t job_id, share::ObBackupBackupsetJobInfo& job_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(ObBackupBackupsetOperator::get_task_item(job_id, job_info, *sql_proxy_))) {
    LOG_WARN("failed to get job info", KR(ret));
  }
  return ret;
}

int ObBackupBackupset::update_job_info(const share::ObBackupBackupsetJobInfo& job_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(ObBackupBackupsetOperator::report_job_item(job_info, *sql_proxy_))) {
    LOG_WARN("failed to report job item", KR(ret), K(job_info));
  }
  return ret;
}

int ObBackupBackupset::do_schedule(const ObBackupBackupsetJobInfo& job_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObArray<SimpleBackupBackupsetTenant> tenant_list;
  const int64_t job_id = job_info.job_id_;
  const int64_t backup_set_id = job_info.backup_set_id_;
  bool backup_set_id_is_valid = true;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (!job_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("job info is not valid", KR(ret), K(job_info));
  } else if (OB_FAIL(get_all_tenants(job_info, tenant_list))) {
    LOG_WARN("failed to get all tenant ids", KR(ret), K(backup_set_id));
  } else if (OB_FAIL(check_can_do_work())) {
    LOG_WARN("failed to check can do work", KR(ret));
  } else {
    CompareTenantId cmp;
    std::sort(tenant_list.begin(), tenant_list.end(), cmp);
    if (OB_FAIL(inner_schedule_tenant_task(job_info, tenant_list))) {
      LOG_WARN("failed to inner schedule tenant task", KR(ret), K(job_info));
    } else if (OB_FAIL(update_cluster_data_backup_info(job_info))) {
      LOG_WARN("failed to update backup meta info", KR(ret));
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_INVALID_BACKUP_SET_ID == ret || OB_BACKUP_BACKUP_CAN_NOT_START == ret ||
        OB_BACKUP_BACKUP_REACH_MAX_BACKUP_TIMES == ret) {
      tmp_ret = ret;
      ret = OB_SUCCESS;  // override the return code to report job
      backup_set_id_is_valid = false;
    }
  }

  if (OB_SUCC(ret)) {
    ObBackupBackupsetJobInfo new_job_info = job_info;
    int64_t task_count = 0;
    if (OB_FAIL(ObTenantBackupBackupsetOperator::get_job_task_count(job_info, *sql_proxy_, task_count))) {
      LOG_WARN("failed to get job task count", KR(ret), K(job_info));
    }
    if (backup_set_id_is_valid) {
      new_job_info.job_status_ = 0 == task_count ? ObBackupBackupsetJobInfo::FINISH : ObBackupBackupsetJobInfo::BACKUP;
      new_job_info.result_ = 0 == task_count ? OB_BACKUP_BACKUP_REACH_COPY_LIMIT : OB_SUCCESS;
      new_job_info.comment_ = 0 == task_count ? OB_BACKUP_BACKUP_REACH_COPY_LIMIT__USER_ERROR_MSG : "";
    } else {
      new_job_info.job_status_ = ObBackupBackupsetJobInfo::FINISH;
      new_job_info.result_ = tmp_ret;
      new_job_info.comment_ = common::ob_strerror(tmp_ret);
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(update_job_info(new_job_info))) {
      LOG_WARN("failed to update job status", KR(ret), K(job_id), K(job_info));
    } else {
      ROOTSERVICE_EVENT_ADD("backup_backupset",
          "start_job",
          "tenant_id",
          job_info.tenant_id_,
          "job_id",
          job_info.job_id_,
          "backup_set_id",
          job_info.backup_set_id_,
          "job_status",
          job_info.get_status_str(job_info.job_status_));
      wakeup();
    }
  }
  return ret;
}

int ObBackupBackupset::do_backup(const ObBackupBackupsetJobInfo& job_info)
{
  int ret = OB_SUCCESS;
  ObArray<ObTenantBackupBackupsetTaskInfo> task_infos;
  bool all_finished = false;
  bool is_available = true;
  const int64_t job_id = job_info.job_id_;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (!job_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("job info is not valid", KR(ret), K(job_info));
  } else if (OB_FAIL(check_can_do_work())) {
    LOG_WARN("failed to check can do work", KR(ret));
  } else if (OB_FAIL(get_current_round_tasks(job_info, task_infos))) {
    LOG_WARN("failed to get all doing job tasks", KR(ret), K(job_info));
  } else {
    DEBUG_SYNC(BEFORE_CHECK_BACKUP_TASK_DATA_AVAILABLE);
    if (OB_FAIL(do_if_src_data_unavailable(job_info, is_available))) {
      LOG_WARN("failed to do if src data unavailable", KR(ret), K(job_info));
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(do_all_tenant_tasks(task_infos))) {
      LOG_WARN("failed to do all tenant tasks", KR(ret));
    } else if (OB_FAIL(check_all_tenant_task_finished(job_info, all_finished))) {
      LOG_WARN("failed to check all tenant task finish", KR(ret), K(job_info));
    }

    if (OB_SUCC(ret) && all_finished) {
      ObBackupBackupsetJobInfo new_job_info = job_info;
      new_job_info.result_ = is_available ? OB_SUCCESS : OB_DATA_SOURCE_NOT_EXIST;
      new_job_info.job_status_ = ObBackupBackupsetJobInfo::CLEAN;
      if (OB_FAIL(update_job_info(new_job_info))) {
        LOG_WARN("failed to update job status", KR(ret), K(job_id), K(job_info));
      } else {
        wakeup();
      }
    }
  }
  return ret;
}

int ObBackupBackupset::do_cancel(const ObBackupBackupsetJobInfo& job_info)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  const int64_t job_id = job_info.job_id_;
  ObArray<ObTenantBackupBackupsetTaskInfo> task_infos;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (!job_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("job info is not valid", KR(ret), K(job_info));
  } else if (OB_FAIL(get_all_job_tasks(job_info, task_infos))) {
    LOG_WARN("failed to get all job tasks", KR(ret), K(job_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < task_infos.count(); ++i) {
      ObTenantBackupBackupsetTaskInfo& task_info = task_infos.at(i);
      task_info.task_status_ = ObTenantBackupBackupsetTaskInfo::CANCEL;
      task_info.result_ = OB_CANCELED;
      const uint64_t tenant_id = task_info.tenant_id_;
      const int64_t job_id = task_info.job_id_;
      const int64_t backup_set_id = task_info.backup_set_id_;
      const int64_t copy_id = task_info.copy_id_;
      SimpleBackupBackupsetTenant tenant;
      bool is_dropped = false;
      tenant.tenant_id_ = tenant_id;
      if (OB_FAIL(check_tenant_is_dropped(tenant_id, is_dropped))) {
        LOG_WARN("failed to check tenant is dropped", KR(ret), K(tenant_id));
      } else if (FALSE_IT(tenant.is_dropped_ = is_dropped)) {
        // do nothing
      } else if (OB_FAIL(ObTenantBackupBackupsetOperator::report_task_item(tenant, task_info, *sql_proxy_))) {
        LOG_WARN("failed to set tenant backup backupset task info to cancel", KR(ret), K(task_info));
      } else if (OB_FAIL(do_tenant_task(tenant_id, job_id, backup_set_id, copy_id))) {
        LOG_WARN("failed to do tenant task", KR(ret), K(tenant_id), K(job_id), K(backup_set_id));
      }
    }
    if (OB_FAIL(ret)) {
      if (ObBackupUtils::is_extern_device_error(ret)) {
        LOG_WARN("extern device error, ignore error code, must update inner table", KR(ret));
        ret = OB_SUCCESS;
      }
    }
    if (OB_SUCC(ret)) {
      ObBackupBackupsetJobInfo new_job_info = job_info;
      new_job_info.job_status_ = ObBackupBackupsetJobInfo::CLEAN;
      new_job_info.result_ = OB_CANCELED;
      new_job_info.comment_ = OB_CANCELED__USER_ERROR_MSG;
      if (OB_FAIL(update_job_info(new_job_info))) {
        LOG_WARN("failed to update job status", KR(ret), K(job_id), K(job_info));
      } else {
        wakeup();
      }
    }
  }
  return ret;
}

int ObBackupBackupset::do_clean(const share::ObBackupBackupsetJobInfo& job_info)
{
  int ret = OB_SUCCESS;
  const int64_t job_id = job_info.job_id_;
  ObArray<ObTenantBackupBackupsetTaskInfo> task_infos;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (!job_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("job info is not valid", KR(ret), K(job_info));
  } else if (OB_FAIL(get_all_job_tasks(job_info, task_infos))) {
    LOG_WARN("failed to get all job tasks", KR(ret), K(job_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < task_infos.count(); ++i) {
      ObTenantBackupBackupsetTaskInfo& task_info = task_infos.at(i);
      SimpleBackupBackupsetTenant tenant;
      tenant.tenant_id_ = task_info.tenant_id_;
      bool is_dropped = false;
      bool all_deleted = false;
      if (OB_SYS_TENANT_ID == tenant.tenant_id_) {
        task_info.task_status_ = ObTenantBackupBackupsetTaskInfo::FINISH;
        if ((OB_FAIL(ObTenantBackupBackupsetOperator::report_task_item(tenant, task_info, *sql_proxy_)))) {
          LOG_WARN("failed to report task item", KR(ret), K(tenant), K(task_info));
        }
      } else {
        if (OB_FAIL(check_tenant_is_dropped(task_info.tenant_id_, is_dropped))) {
          LOG_WARN("failed to check tenant is dropped", KR(ret), K(tenant));
        } else if (FALSE_IT(tenant.is_dropped_ = is_dropped)) {
          // do nothing
        } else if (OB_FAIL(ObPGBackupBackupsetOperator::remove_all_pg_tasks(tenant, *sql_proxy_))) {
          LOG_WARN("failed to remove pg tasks", KR(ret), K(tenant), K(task_info));
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObBackupBackupsetJobInfo new_job_info = job_info;
      new_job_info.job_status_ = ObBackupBackupsetJobInfo::FINISH;
      if (OB_FAIL(update_job_info(new_job_info))) {
        LOG_WARN("failed to update job status", KR(ret), K(job_id), K(job_info));
      } else {
        wakeup();
      }
    }
  }
  return ret;
}

int ObBackupBackupset::do_finish(const ObBackupBackupsetJobInfo& job_info)
{
  int ret = OB_SUCCESS;
  ObArray<ObTenantBackupBackupsetTaskInfo> task_infos;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (!job_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("job info is not valid", KR(ret), K(job_info));
  } else if (OB_FAIL(get_all_job_tasks(job_info, task_infos))) {
    LOG_WARN("failed to get all job tasks", KR(ret), K(job_info));
  } else if (OB_FAIL(check_can_do_work())) {
    LOG_WARN("failed to check can do work", KR(ret));
  } else {
    CompareBBTaskTenantIdLarger cmp;
    std::sort(task_infos.begin(), task_infos.end(), cmp);
    DEBUG_SYNC(BEFORE_BACKUP_BACKUPSET_FINISH);
    for (int64_t i = 0; OB_SUCC(ret) && i < task_infos.count(); ++i) {
      ObTenantBackupBackupsetTaskInfo& info = task_infos.at(i);
      const uint64_t tenant_id = info.tenant_id_;
      const int64_t job_id = info.job_id_;
      const int64_t backup_set_id = info.backup_set_id_;
      const int64_t copy_id = info.copy_id_;
      ObTenantBackupBackupsetTaskInfo task_info;
      SimpleBackupBackupsetTenant tenant;
      tenant.tenant_id_ = tenant_id;
      bool is_dropped = false;
      ObMySQLTransaction trans;
      if (OB_FAIL(check_tenant_is_dropped(tenant_id, is_dropped))) {
        LOG_WARN("failed to check tenant is dropped", KR(ret), K(tenant_id));
      } else if (FALSE_IT(tenant.is_dropped_ = is_dropped)) {
        // do nothing
      } else if (OB_FAIL(do_tenant_task(tenant_id, job_id, backup_set_id, copy_id))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          LOG_WARN("tenant may be dropped", KR(ret));
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to do tenant task", KR(ret), K(job_id), K(tenant_id), K(backup_set_id));
        }
      } else if (OB_FAIL(trans.start(sql_proxy_))) {
        LOG_WARN("failed to start trans", KR(ret));
      } else {
        if (OB_FAIL(ObTenantBackupBackupsetOperator::get_task_item(tenant, job_id, backup_set_id, task_info, trans))) {
          LOG_WARN("failed to get tenant task info", KR(ret), K(tenant_id), K(job_id));
        } else if (OB_SYS_TENANT_ID != task_info.tenant_id_ &&
                   ObTenantBackupBackupsetTaskInfo::FINISH != task_info.task_status_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tenant backup backupset task status is not finish", KR(ret), K(task_info));
        } else if (OB_FAIL(construct_sys_tenant_backup_backupset_info(info))) {
          LOG_WARN("failed to construct sys tenant backup backupset info", KR(ret), K(info));
        } else if (OB_FAIL(ObTenantBackupBackupsetHistoryOperator::insert_task_item(task_info, trans))) {
          LOG_WARN("failed to insert task item to history", KR(ret));
        } else if (OB_FAIL(ObTenantBackupBackupsetOperator::remove_task_item(tenant, job_id, backup_set_id, trans))) {
          LOG_WARN("failed to remove task item", KR(ret));
        } else if (OB_FAIL(update_tenant_backup_set_file_info(info, trans))) {
          LOG_WARN("failed to update tenant backup set file info", KR(ret), K(info));
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(trans.end(true /*commit*/))) {
            LOG_WARN("failed to commit", KR(ret));
          }
        } else {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = trans.end(false /* commit*/))) {
            LOG_WARN("failed to rollback trans", K(tmp_ret));
          }
        }
      }
    }

#ifdef ERRSIM
    ret = E(EventTable::EN_BACKUP_BACKUPSET_EXTERN_INFO_ERROR) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      LOG_WARN("errsim set extern backup backupset info error", KR(ret));
    }
#endif

    if (OB_SUCC(ret)) {
      bool is_complete = true;
      int64_t result = OB_SUCCESS;
      ObArray<SimpleBackupBackupsetTenant> missing_tenants;
      if (OB_FAIL(check_job_tasks_is_complete(job_info, missing_tenants, is_complete))) {
        LOG_WARN("failed to check job tasks is complete", KR(ret), K(job_info));
      } else if (FALSE_IT(result = is_complete ? OB_SUCCESS : OB_TENANT_HAS_BEEN_DROPPED)) {
        // do nothing
      } else if (!is_complete && OB_FAIL(do_if_job_tasks_is_not_complete(job_info, result))) {
        LOG_WARN("failed to do if job tasks if not complete", KR(ret), K(job_info));
      } else if (OB_FAIL(deal_with_missing_tenants(job_info, missing_tenants))) {
        LOG_WARN("failed to deal with missing tenants", KR(ret));
      } else {
        if (OB_FAIL(move_job_info_to_history(job_info.job_id_, result))) {
          LOG_WARN("failed to move job info to history", KR(ret), K(job_info));
        } else {
          ROOTSERVICE_EVENT_ADD("backup_backupset",
              "finish_job",
              "tenant_id",
              job_info.tenant_id_,
              "job_id",
              job_info.job_id_,
              "backup_set_id",
              job_info.backup_set_id_,
              "job_status",
              job_info.get_status_str(job_info.job_status_));
          wakeup();
        }
      }
    }

    if (OB_FAIL(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (ObBackupUtils::is_extern_device_error(ret)) {
        if (OB_SUCCESS != (tmp_ret = inner_do_finish_if_device_error(job_info))) {
          LOG_WARN("failed to inner do finish if device error", KR(tmp_ret), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObBackupBackupset::check_dest_same_with_gconf(const share::ObBackupBackupsetJobInfo& job_info, bool& is_same)
{
  int ret = OB_SUCCESS;
  is_same = false;
  ObBackupDest backup_dest;
  char backup_backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(GCONF.backup_backup_dest.copy(backup_backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to copy backup backup dest", KR(ret));
  } else if (0 == strlen(backup_backup_dest_str)) {
    is_same = false;
  } else if (OB_FAIL(backup_dest.set(backup_backup_dest_str))) {
    LOG_WARN("failed to set backup dest", KR(ret), K(backup_backup_dest_str));
  } else if (job_info.backup_dest_.is_root_path_equal(backup_dest)) {
    is_same = true;
  }
  return ret;
}

int ObBackupBackupset::get_job_copy_id_level(
    const share::ObBackupBackupsetJobInfo& job_info, share::ObBackupBackupCopyIdLevel& copy_id_level)
{
  int ret = OB_SUCCESS;
  copy_id_level = OB_BB_COPY_ID_LEVEL_MAX;
  bool is_same_with_gconf = false;
  const bool is_tenant_level = job_info.is_tenant_level();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(check_dest_same_with_gconf(job_info, is_same_with_gconf))) {
    LOG_WARN("failed to check dest same with gconf", KR(ret), K(job_info));
  } else {
    if (!is_tenant_level) {
      if (is_same_with_gconf) {
        copy_id_level = OB_BB_COPY_ID_LEVEL_CLUSTER_GCONF_DEST;
      } else {
        copy_id_level = OB_BB_COPY_ID_LEVEL_CLUSTER_USER_DEST;
      }
    } else {
      if (is_same_with_gconf) {
        copy_id_level = OB_BB_COPY_ID_LEVEL_TENANT_GCONF_DEST;
      } else {
        copy_id_level = OB_BB_COPY_ID_LEVEL_TENANT_USER_DEST;
      }
    }
  }
  return ret;
}

int ObBackupBackupset::do_if_src_data_unavailable(const share::ObBackupBackupsetJobInfo& job_info, bool& is_available)
{
  int ret = OB_SUCCESS;
  ObArray<ObTenantBackupBackupsetTaskInfo> all_task_infos;
  ObArray<ObTenantBackupBackupsetTaskInfo> unavailable_tasks;
  ObArray<ObTenantBackupBackupsetTaskInfo> available_tasks;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(get_all_job_tasks(job_info, all_task_infos))) {
    LOG_WARN("failed to get all job tasks", KR(ret), K(job_info));
  } else if (OB_FAIL(check_src_data_available(all_task_infos, unavailable_tasks, available_tasks, is_available))) {
    LOG_WARN("failed to check data available", KR(ret), K(all_task_infos));
  } else if (!is_available) {
    if (OB_FAIL(update_tenant_tasks_result(unavailable_tasks, OB_DATA_SOURCE_NOT_EXIST))) {
      LOG_WARN("failed to update tenant task result", KR(ret), K(job_info));
    } else if (OB_FAIL(update_tenant_tasks_result(available_tasks, OB_CANCELED))) {
      LOG_WARN("failed to update tenant task result", KR(ret), K(job_info));
    } else {
      ROOTSERVICE_EVENT_ADD("backup_backupset",
          "data_not_available",
          "tenant_id",
          job_info.tenant_id_,
          "job_id",
          job_info.job_id_,
          "backup_set_id",
          job_info.backup_set_id_,
          "job_status",
          job_info.get_status_str(job_info.job_status_));
    }
  }

  return ret;
}

int ObBackupBackupset::check_src_data_available(const common::ObArray<share::ObTenantBackupBackupsetTaskInfo>& tasks,
    common::ObArray<share::ObTenantBackupBackupsetTaskInfo>& unavailable_tasks,
    common::ObArray<share::ObTenantBackupBackupsetTaskInfo>& available_tasks, bool& is_available)
{
  int ret = OB_SUCCESS;
  unavailable_tasks.reset();
  available_tasks.reset();
  is_available = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (tasks.empty()) {
    // do nothing
  } else {
    const int64_t original_copy_id = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
      ObBackupSetFileInfo file_info;
      const ObTenantBackupBackupsetTaskInfo& task = tasks.at(i);
      if (OB_FAIL(ObBackupSetFilesOperator::get_tenant_backup_set_file_info(task.tenant_id_,
              task.backup_set_id_,
              task.incarnation_,
              original_copy_id,
              false /*for_update*/,
              *sql_proxy_,
              file_info))) {
        LOG_WARN("failed to get tenant backup set file infos", KR(ret), K(task));
      } else if (ObBackupFileStatus::BACKUP_FILE_AVAILABLE != file_info.file_status_) {
        if (OB_FAIL(unavailable_tasks.push_back(task))) {
          LOG_WARN("failed to push back", KR(ret), K(task));
        }
        is_available = false;
      } else {
        if (OB_FAIL(available_tasks.push_back(task))) {
          LOG_WARN("failed to push back", KR(ret), K(task));
        }
      }
    }
  }
  return ret;
}

int ObBackupBackupset::update_tenant_tasks_result(
    const common::ObArray<share::ObTenantBackupBackupsetTaskInfo>& tasks, const int result)
{
  int ret = OB_SUCCESS;
  ObArray<ObTenantBackupBackupsetTaskInfo> task_infos;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(task_infos.assign(tasks))) {
    LOG_WARN("failed to assign array", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < task_infos.count(); ++i) {
      ObTenantBackupBackupsetTaskInfo& info = task_infos.at(i);
      info.result_ = result;
      info.task_status_ = ObTenantBackupBackupsetTaskInfo::FINISH;
      SimpleBackupBackupsetTenant tenant;
      tenant.tenant_id_ = info.tenant_id_;
      bool is_dropped = false;
      if (OB_FAIL(check_tenant_is_dropped(info.tenant_id_, is_dropped))) {
        LOG_WARN("failed to check tenant is dropped", KR(ret), K(info));
      } else if (FALSE_IT(tenant.is_dropped_ = is_dropped)) {
        // do nothing
      } else if (OB_FAIL(ObTenantBackupBackupsetOperator::report_task_item(tenant, info, *sql_proxy_))) {
        LOG_WARN("failed to report task item", KR(ret), K(tenant), K(info));
      }
    }
  }
  return ret;
}

int ObBackupBackupset::inner_do_finish_if_device_error(const share::ObBackupBackupsetJobInfo& job_info)
{
  int ret = OB_SUCCESS;
  ObArray<ObTenantBackupBackupsetTaskInfo> task_infos;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (!job_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("job info is not valid", KR(ret), K(job_info));
  } else if (OB_FAIL(get_all_job_tasks(job_info, task_infos))) {
    LOG_WARN("failed to get all job tasks", KR(ret), K(job_info));
  } else if (OB_FAIL(check_can_do_work())) {
    LOG_WARN("failed to check can do work", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < task_infos.count(); ++i) {
      ObMySQLTransaction trans;
      ObTenantBackupBackupsetTaskInfo& info = task_infos.at(i);
      const uint64_t tenant_id = info.tenant_id_;
      const int64_t job_id = info.job_id_;
      const int64_t backup_set_id = info.backup_set_id_;
      ObTenantBackupBackupsetTaskInfo task_info;
      SimpleBackupBackupsetTenant tenant;
      tenant.tenant_id_ = tenant_id;
      bool is_dropped = false;
      if (OB_FAIL(check_tenant_is_dropped(tenant_id, is_dropped))) {
        LOG_WARN("failed to check tenant is dropped", KR(ret), K(tenant_id));
      } else if (FALSE_IT(tenant.is_dropped_ = is_dropped)) {
        // do nothing
      } else if (OB_FAIL(trans.start(sql_proxy_))) {
        LOG_WARN("failed to start trans", KR(ret));
      } else {
        if (OB_FAIL(ObTenantBackupBackupsetOperator::get_task_item(tenant, job_id, backup_set_id, task_info, trans))) {
          LOG_WARN("failed to get tenant task info", KR(ret), K(tenant_id), K(job_id));
        } else if (OB_SYS_TENANT_ID != task_info.tenant_id_ &&
                   ObTenantBackupBackupsetTaskInfo::FINISH != task_info.task_status_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tenant backup backupset task status is not finish", KR(ret), K(task_info));
        } else if (OB_FAIL(construct_sys_tenant_backup_backupset_info(info))) {
          LOG_WARN("failed to construct sys tenant backup backupset info", KR(ret), K(info));
        } else if (OB_FAIL(ObTenantBackupBackupsetHistoryOperator::insert_task_item(
                       task_info, *sql_proxy_))) {  // underline use insert update
          LOG_WARN("failed to insert task item to history", KR(ret));
        } else if (OB_FAIL(ObTenantBackupBackupsetOperator::remove_task_item(tenant, job_id, backup_set_id, trans))) {
          LOG_WARN("failed to remove task item", KR(ret));
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(trans.end(true /*commit*/))) {
            LOG_WARN("failed to commit", K(ret));
          }
        } else {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = trans.end(false /* commit*/))) {
            LOG_WARN("failed to rollback trans", K(tmp_ret));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(move_job_info_to_history(job_info.job_id_, OB_IO_ERROR))) {
        LOG_WARN("failed to move job info to history", KR(ret), K(job_info));
      } else {
        wakeup();
      }
    }
  }
  return ret;
}

int ObBackupBackupset::inner_schedule_tenant_task(
    const share::ObBackupBackupsetJobInfo& job_info, const common::ObArray<SimpleBackupBackupsetTenant>& tenant_list)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (!job_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup backupset job info is not valid", KR(ret), K(job_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_list.count(); ++i) {
      const SimpleBackupBackupsetTenant& tenant = tenant_list.at(i);
      const uint64_t tenant_id = tenant.tenant_id_;
      ObArray<int64_t> all_backup_set_ids;
      ObArray<int64_t> filtered_backup_set_ids;
      ObArray<ObTenantBackupBackupsetTaskInfo> no_need_schedule_tasks;
      if (OB_FAIL(get_all_backup_set_id(tenant_id, job_info, all_backup_set_ids, no_need_schedule_tasks))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          LOG_INFO("tenant do not have such backup set id", K(tenant_id), K(job_info));
        } else {
          LOG_WARN("failed to get all backup set id of tenant", KR(ret), K(tenant_id), K(job_info));
        }
      } else if (all_backup_set_ids.empty()) {
        // do nothing
      } else if (OB_FAIL(filter_backup_set_ids(tenant_id, job_info, all_backup_set_ids, filtered_backup_set_ids))) {
        LOG_WARN("failed to filter backup set ids", KR(ret), K(tenant_id), K(job_info));
        ;
      } else if (filtered_backup_set_ids.empty()) {
        LOG_INFO("no backup set id to", K(job_info), K(tenant_id));
      } else if (OB_FAIL(inner_schedule_tenant_backupset_task(job_info, tenant, filtered_backup_set_ids))) {
        LOG_WARN("faied to inner schedule tenant backupset task", KR(ret), K(job_info), K(tenant_id));
      }
    }
  }

  LOG_INFO("finish inner schedule tenant task", K(ret), K(job_info), K(tenant_list));
  return ret;
}

int ObBackupBackupset::deal_with_no_need_schedule_tasks(const share::ObBackupBackupsetJobInfo& job_info,
    const share::SimpleBackupBackupsetTenant& tenant,
    const common::ObArray<ObTenantBackupBackupsetTaskInfo>& no_need_schedule_tasks)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (!job_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup backupset job info is not valid", KR(ret), K(job_info));
  } else if (no_need_schedule_tasks.empty()) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < no_need_schedule_tasks.count(); ++i) {
      const ObTenantBackupBackupsetTaskInfo& info = no_need_schedule_tasks.at(i);
      ObTenantBackupBackupsetTaskInfo new_task_info = info;
      new_task_info.job_id_ = job_info.job_id_;
      new_task_info.task_status_ = ObTenantBackupBackupsetTaskInfo::FINISH;
      if (OB_FAIL(ObTenantBackupBackupsetOperator::insert_task_item(tenant, new_task_info, *sql_proxy_))) {
        LOG_WARN("failed to insert tenant backup backupset task info", KR(ret), K(new_task_info));
      }
    }
  }
  return ret;
}

int ObBackupBackupset::filter_backup_set_ids(const uint64_t tenant_id, const share::ObBackupBackupsetJobInfo& info,
    const common::ObArray<int64_t>& backup_set_ids, common::ObArray<int64_t>& filtered_backup_set_ids)
{
  int ret = OB_SUCCESS;
  filtered_backup_set_ids.reset();
  const int64_t max_backup_times = info.max_backup_times_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (tenant_id == OB_INVALID_ID || !info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret), K(tenant_id), K(info));
  } else {
    if (max_backup_times == -1) {
      if (OB_FAIL(filtered_backup_set_ids.assign(backup_set_ids))) {
        LOG_WARN("failed to assign", KR(ret));
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < backup_set_ids.count(); ++i) {
        const int64_t backup_set_id = backup_set_ids.at(i);
        int64_t copy_count = 0;
        ObBackupSetFileInfo src_file_info;
        if (OB_FAIL(ObBackupSetFilesOperator::get_tenant_backup_set_file_info(tenant_id,
                backup_set_id,
                info.incarnation_,
                0 /*copy_id*/,
                false /*for_update*/,
                *sql_proxy_,
                src_file_info))) {
          LOG_WARN("failed to get tenant backup set file info", KR(ret), K(tenant_id), K(backup_set_id));
        } else if (0 != src_file_info.result_) {
          // do not deal with backup set id whose result is not 0
        } else if (OB_FAIL(ObBackupSetFilesOperator::get_all_cluster_level_backup_set_copy_count(
                       tenant_id, info.incarnation_, backup_set_id, *sql_proxy_, copy_count))) {
          LOG_WARN("failed to get backup set file info copy count", KR(ret), K(tenant_id), K(info), K(backup_set_id));
        } else if (copy_count >= max_backup_times) {
          LOG_INFO(
              "backup set id already reached max backup times", K(backup_set_id), K(max_backup_times), K(copy_count));
        } else if (OB_FAIL(filtered_backup_set_ids.push_back(backup_set_id))) {
          LOG_WARN("failed to push back", KR(ret), K(backup_set_id));
        }
      }
    }
  }
  return ret;
}

int ObBackupBackupset::inner_schedule_tenant_backupset_task(const share::ObBackupBackupsetJobInfo& job_info,
    const SimpleBackupBackupsetTenant& tenant, const common::ObIArray<int64_t>& backup_set_ids)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = tenant.tenant_id_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_set_ids.count(); ++i) {
      bool exist = true;
      int64_t copy_id = 1;
      const int64_t backup_set_id = backup_set_ids.at(i);
      ObTenantBackupBackupsetTaskInfo task_info;
      bool is_backup_set_id_valid = true;
      if (!job_info.is_tenant_level() && OB_SYS_TENANT_ID != tenant.tenant_id_) {
        if (OB_FAIL(
                check_backup_set_id_valid_for_normal_tenant(job_info, tenant, backup_set_id, is_backup_set_id_valid))) {
          LOG_WARN("failed to check backup set id valid for normal tenant", KR(ret), K(tenant), K(backup_set_id));
        } else if (!is_backup_set_id_valid) {
          continue;
        }
      }

      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(ObTenantBackupBackupsetOperator::get_task_item(
                     tenant, job_info.job_id_, backup_set_id, task_info, *sql_proxy_))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          LOG_INFO("tenant backup backupset task do not exist", KR(ret), K(tenant_id));
          exist = false;
          ret = OB_SUCCESS;
        }
      }

      if (OB_SUCC(ret) && !exist) {
        bool need_insert = true;
        if (!job_info.is_tenant_level()) {
          if (OB_SYS_TENANT_ID == tenant_id && OB_FAIL(calc_backup_set_copy_id(job_info, backup_set_id, copy_id))) {
            LOG_WARN(
                "failed to get tenant backup backup task copy id", KR(ret), K(job_info), K(backup_set_id), K(copy_id));
          } else if (OB_SYS_TENANT_ID != tenant_id &&
                     OB_FAIL(get_backup_set_copy_id(job_info, backup_set_id, copy_id))) {
            LOG_WARN("failed to get backup set copy id", KR(ret), K(job_info), K(backup_set_id), K(copy_id));
          }
        } else {
          if (OB_FAIL(calc_backup_set_copy_id(job_info, backup_set_id, copy_id))) {
            LOG_WARN("failed to calc backup set copy id", KR(ret), K(job_info), K(backup_set_id));
          }
        }
        if (OB_FAIL(ret)) {
          // do nothing
        } else if (OB_FAIL(build_tenant_task_info(tenant_id, backup_set_id, copy_id, job_info, task_info))) {
          LOG_WARN("failed to build tenant task info", KR(ret), K(tenant_id), K(job_info));
        } else if (OB_FAIL(ObTenantBackupBackupsetOperator::insert_task_item(tenant, task_info, *sql_proxy_))) {
          LOG_WARN("failed to insert tenant backup backupset task info", KR(ret), K(task_info));
        } else if (OB_FAIL(check_need_insert_backup_set_file_info(task_info, need_insert))) {
          LOG_WARN("failed to check need insert backup set file info", KR(ret), K(task_info));
        } else if (need_insert && OB_FAIL(insert_tenant_backup_set_file_info(task_info))) {
          LOG_WARN("failed to insert tenant backup set file info", KR(ret), K(task_info));
        } else {
          LOG_INFO("inner schedule tenant task success", K(job_info), K(task_info));
        }
      }
    }
  }
  return ret;
}

int ObBackupBackupset::check_backup_set_id_valid_for_normal_tenant(const share::ObBackupBackupsetJobInfo& job_info,
    const share::SimpleBackupBackupsetTenant& tenant, const int64_t backup_set_id, bool& is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  SimpleBackupBackupsetTenant sys_tenant;
  sys_tenant.tenant_id_ = OB_SYS_TENANT_ID;
  sys_tenant.is_dropped_ = false;
  ObTenantBackupBackupsetTaskInfo sys_task_info;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (job_info.is_tenant_level()) {
    // do nothing
  } else if (OB_FAIL(ObTenantBackupBackupsetOperator::get_task_item(
                 sys_tenant, job_info.job_id_, backup_set_id, sys_task_info, *sql_proxy_))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      is_valid = false;
      LOG_INFO("backup set id not exist for normal tenant", K(tenant), K(backup_set_id), K(job_info));
    } else {
      LOG_WARN("failed to get task item", KR(ret), K(sys_tenant), K(job_info), K(backup_set_id));
    }
  } else {
    is_valid = !sys_task_info.is_mark_deleted_;
  }
  return ret;
}

int ObBackupBackupset::check_need_insert_backup_set_file_info(
    const share::ObTenantBackupBackupsetTaskInfo& task_info, bool& need_insert)
{
  int ret = OB_SUCCESS;
  need_insert = true;
  const uint64_t tenant_id = task_info.tenant_id_;
  const int64_t backup_set_id = task_info.backup_set_id_;
  const int64_t copy_id = task_info.copy_id_;
  const bool for_update = false;
  ObBackupSetFileInfo file_info;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (!task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret), K(task_info));
  } else if (OB_FAIL(ObBackupSetFilesOperator::get_tenant_backup_set_file_info(
                 tenant_id, backup_set_id, copy_id, for_update, *sql_proxy_, file_info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get tenant backup set file info", KR(ret), K(tenant_id), K(backup_set_id), K(copy_id));
    }
  } else {
    need_insert =
        !(ObBackupFileStatus::BACKUP_FILE_AVAILABLE == file_info.file_status_ && OB_SUCCESS == file_info.result_);
  }
  return ret;
}

int ObBackupBackupset::calc_backup_set_copy_id(
    const share::ObBackupBackupsetJobInfo& job_info, const int64_t backup_set_id, int64_t& copy_id)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_CALC_BACKUP_BACKUPSET_COPY_ID);
  copy_id = 0;
  int64_t max_copy_id = 0;
  const int64_t max_backup_times = job_info.max_backup_times_;
  ObArray<ObBackupSetFileInfo> file_infos;
  const bool is_tenant_level = job_info.is_tenant_level();
  ObBackupBackupCopyIdLevel copy_id_level = OB_BB_COPY_ID_LEVEL_MAX;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (backup_set_id < 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tenant backup backup task copy id get invalid arugment", KR(ret), K(job_info), K(backup_set_id));
  } else if (OB_FAIL(get_job_copy_id_level(job_info, copy_id_level))) {
    LOG_WARN("failed to get job copy id level", KR(ret), K(job_info));
  } else if (OB_FAIL(ObBackupSetFilesOperator::get_backup_set_file_info_copy_list(
                 job_info.tenant_id_, OB_START_INCARNATION, backup_set_id, copy_id_level, *sql_proxy_, file_infos))) {
    LOG_WARN("failed to get backup set file info copy list", KR(ret));
  } else if (file_infos.empty()) {
    if (OB_FAIL(get_backup_backup_start_copy_id(copy_id_level, copy_id))) {
      LOG_WARN("failed to get backup backup start copy id", KR(ret), K(copy_id_level));
    } else {
      LOG_INFO("tasks is empty", K(backup_set_id), K(copy_id));
    }
  } else {
    bool has_same = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < file_infos.count(); ++i) {
      const ObBackupSetFileInfo& file_info = file_infos.at(i);
      ObBackupDest backup_dest;
      if (OB_FAIL(backup_dest.set(file_info.backup_dest_.ptr()))) {
        LOG_WARN("failed to set backup dest", KR(ret));
      } else if (backup_dest.is_root_path_equal(job_info.backup_dest_)) {
        if (ObBackupFileStatus::BACKUP_FILE_INCOMPLETE == file_info.file_status_) {
          copy_id = file_info.copy_id_;
          has_same = true;
          LOG_INFO("copy id can be reused", K(job_info), K(backup_set_id));
          break;
        } else if (ObBackupFileStatus::BACKUP_FILE_DELETING == file_info.file_status_) {
          ret = OB_BACKUP_BACKUP_CAN_NOT_START;
          LOG_WARN("backup backup is deleting", KR(ret), K(file_info));
        } else if (ObBackupFileStatus::BACKUP_FILE_DELETED == file_info.file_status_) {
          ret = OB_BACKUP_BACKUP_CAN_NOT_START;
          LOG_WARN("backup backup has been deleted", KR(ret), K(file_info));
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected file status", KR(ret), K(file_info));
        }
      }
      if (file_info.copy_id_ > max_copy_id) {
        max_copy_id = file_info.copy_id_;
      }
    }
    if (OB_SUCC(ret) && !has_same) {
      int64_t valid_bb_count = 0;
      if (OB_FAIL(get_valid_backup_backup_file_info_count(file_infos, valid_bb_count))) {
        LOG_WARN("failed to get non incomplete file info count", KR(ret));
      } else if (max_backup_times >= 0 && !is_tenant_level && valid_bb_count >= max_backup_times) {
        ret = OB_BACKUP_BACKUP_REACH_MAX_BACKUP_TIMES;
        LOG_WARN("reached max backup backuppiece time limit", KR(ret), K(valid_bb_count), K(job_info));
      } else {
        copy_id = max_copy_id + 1;
        if (!is_tenant_level && copy_id >= OB_TENANT_GCONF_DEST_START_COPY_ID) {
          ret = OB_BACKUP_BACKUP_CAN_NOT_START;
          LOG_ERROR("can not start backup backup", K(backup_set_id), K(copy_id));
        }
      }
    }
  }
  return ret;
}

int ObBackupBackupset::get_valid_backup_backup_file_info_count(
    const common::ObArray<share::ObBackupSetFileInfo>& file_infos, int64_t& count)
{
  int ret = OB_SUCCESS;
  count = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < file_infos.count(); ++i) {
      const ObBackupSetFileInfo& file_info = file_infos.at(i);
      if (OB_SUCCESS == file_info.result_) {
        ++count;
      }
    }
  }
  return ret;
}

int ObBackupBackupset::get_backup_set_copy_id(
    const share::ObBackupBackupsetJobInfo& job_info, const int64_t backup_set_id, int64_t& copy_id)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_GET_BACKUP_BACKUPSET_COPY_ID);
  SimpleBackupBackupsetTenant tenant;
  tenant.tenant_id_ = OB_SYS_TENANT_ID;
  tenant.is_dropped_ = false;
  copy_id = 0;
  ObTenantBackupBackupsetTaskItem sys_info;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(ObTenantBackupBackupsetOperator::get_task_item(
                 tenant, job_info.job_id_, backup_set_id, sys_info, *sql_proxy_))) {
    LOG_WARN("failed to get task item", KR(ret), K(tenant));
  } else {
    copy_id = sys_info.copy_id_;
  }
  return ret;
}

int ObBackupBackupset::build_tenant_task_info(const uint64_t tenant_id, const int64_t backup_set_id,
    const int64_t copy_id, const share::ObBackupBackupsetJobInfo& job_info,
    share::ObTenantBackupBackupsetTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  share::ObTenantBackupTaskInfo backup_info;
  char src_backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
  char dst_backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(get_tenant_backup_task_info(tenant_id, backup_set_id, backup_info))) {
    LOG_WARN("failed to get tenant backup task info", KR(ret));
  } else if (OB_FAIL(backup_info.backup_dest_.get_backup_dest_str(src_backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to get src backup dest str", KR(ret));
  } else if (OB_FAIL(get_dst_backup_dest_str(job_info, dst_backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
    LOG_WARN("failed to get dst backup dest str", KR(ret));
  } else if (OB_FAIL(task_info.src_backup_dest_.set(src_backup_dest_str))) {
    LOG_WARN("failed to set src backup dest", KR(ret), K(src_backup_dest_str));
  } else if (OB_FAIL(task_info.dst_backup_dest_.set(dst_backup_dest_str))) {
    LOG_WARN("failed to set dst backup dest", KR(ret), K(dst_backup_dest_str));
  } else {
    task_info.tenant_id_ = tenant_id;
    task_info.job_id_ = job_info.job_id_;
    task_info.incarnation_ = OB_START_INCARNATION;
    task_info.backup_set_id_ = backup_set_id;
    task_info.copy_id_ = copy_id;
    task_info.backup_type_ = backup_info.backup_type_;
    task_info.snapshot_version_ = backup_info.snapshot_version_;
    task_info.prev_full_backup_set_id_ = backup_info.prev_full_backup_set_id_;
    task_info.prev_inc_backup_set_id_ = backup_info.prev_inc_backup_set_id_;
    task_info.prev_backup_data_version_ = backup_info.prev_backup_data_version_;
    task_info.input_bytes_ = 0;
    task_info.output_bytes_ = 0;
    task_info.cluster_id_ = backup_info.cluster_id_;
    task_info.cluster_version_ = backup_info.cluster_version_;
    task_info.backup_data_version_ = backup_info.backup_data_version_;
    task_info.backup_schema_version_ = backup_info.backup_schema_version_;
    task_info.task_status_ = ObTenantBackupBackupsetTaskInfo::GENERATE;
    task_info.start_ts_ = ObTimeUtility::current_time();
    task_info.end_ts_ = 0;
    task_info.compatible_ = backup_info.compatible_;
    task_info.total_pg_count_ = backup_info.finish_pg_count_;
    task_info.finish_pg_count_ = 0;
    task_info.total_partition_count_ = backup_info.finish_partition_count_;
    task_info.finish_partition_count_ = 0;
    task_info.total_macro_block_count_ = backup_info.finish_macro_block_count_;
    task_info.finish_macro_block_count_ = 0;
    task_info.result_ = OB_SUCCESS;
    task_info.encryption_mode_ = backup_info.encryption_mode_;
    task_info.passwd_ = backup_info.passwd_;
    task_info.start_replay_log_ts_ = backup_info.start_replay_log_ts_;
    task_info.date_ = backup_info.date_;
  }
  return ret;
}

int ObBackupBackupset::get_tenant_backup_task_info(
    const uint64_t tenant_id, const int64_t backup_set_id, share::ObTenantBackupTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  ObBackupTaskHistoryUpdater updater;
  const bool for_update = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(updater.init(*sql_proxy_))) {
    LOG_WARN("failed to init backup task history updater", KR(ret));
  } else if (OB_FAIL(updater.get_original_tenant_backup_task(tenant_id, backup_set_id, for_update, task_info))) {
    LOG_WARN("failed to get tenant backup task", KR(ret), K(tenant_id), K(backup_set_id));
  }
  return ret;
}

int ObBackupBackupset::get_dst_backup_dest_str(
    const share::ObBackupBackupsetJobInfo& job_info, char* buf, const int64_t buf_size) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(job_info.backup_dest_.get_backup_dest_str(buf, buf_size))) {
    LOG_WARN("failed to get backup dest str", KR(ret));
  }
  return ret;
}

int ObBackupBackupset::construct_sys_tenant_backup_backupset_info(share::ObTenantBackupBackupsetTaskInfo& info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_SYS_TENANT_ID != info.tenant_id_) {
    // do nothing
  } else {
    info.task_status_ = ObTenantBackupBackupsetTaskInfo::FINISH;
    info.end_ts_ = ObTimeUtility::current_time();
  }
  return ret;
}

int ObBackupBackupset::insert_tenant_backup_set_file_info(const share::ObTenantBackupBackupsetTaskInfo& info)
{
  int ret = OB_SUCCESS;
  ObBackupSetFileInfo file_info;
  char backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(ObBackupSetFilesOperator::get_tenant_backup_set_file_info(info.tenant_id_,
                 info.backup_set_id_,
                 info.incarnation_,
                 0 /*copy_id*/,
                 false /*for_update*/,
                 *sql_proxy_,
                 file_info))) {
    LOG_WARN("failed to get src tenant backup set file info", KR(ret), K(info));
  } else {
    file_info.copy_id_ = info.copy_id_;
    file_info.status_ = ObBackupSetFileInfo::DOING;
    file_info.file_status_ = ObBackupFileStatus::BACKUP_FILE_COPYING;
    file_info.start_time_ = ObTimeUtility::current_time();
    file_info.end_time_ = 0;
    file_info.input_bytes_ = 0;
    file_info.output_bytes_ = 0;
    if (OB_FAIL(info.dst_backup_dest_.get_backup_dest_str(backup_dest_str, sizeof(backup_dest_str)))) {
      LOG_WARN("failed to get backup dest str", KR(ret), K(info));
    } else if (OB_FAIL(file_info.backup_dest_.assign(backup_dest_str))) {
      LOG_WARN("failed to assign backup dest str", KR(ret), K(file_info));
    } else if (OB_FAIL(ObBackupSetFilesOperator::insert_tenant_backup_set_file_info(file_info, *sql_proxy_))) {
      LOG_WARN("failed to insert tenant backup set file info", KR(ret), K(file_info));
    }
  }
  return ret;
}

int ObBackupBackupset::update_tenant_backup_set_file_info(
    const share::ObTenantBackupBackupsetTaskInfo& info, common::ObISQLClient& client)
{
  int ret = OB_SUCCESS;
  ObBackupSetFileInfo src_file_info;
  ObBackupSetFileInfo dst_file_info;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(ObBackupSetFilesOperator::get_tenant_backup_set_file_info(info.tenant_id_,
                 info.backup_set_id_,
                 info.incarnation_,
                 info.copy_id_,
                 true /*for_update*/,
                 client,
                 src_file_info))) {
    LOG_WARN("failed to get src tenant backup set file info", KR(ret), K(info));
  } else {
    dst_file_info = src_file_info;
    dst_file_info.input_bytes_ = 0 == info.result_ ? src_file_info.output_bytes_ : 0;
    dst_file_info.output_bytes_ = 0 == info.result_ ? src_file_info.output_bytes_ : 0;
    dst_file_info.status_ = 0 == info.result_ ? ObBackupSetFileInfo::SUCCESS : ObBackupSetFileInfo::FAILED;
    dst_file_info.file_status_ =
        0 == info.result_ ? ObBackupFileStatus::BACKUP_FILE_AVAILABLE : ObBackupFileStatus::BACKUP_FILE_INCOMPLETE;
    dst_file_info.end_time_ = ObTimeUtility::current_time();
    if (OB_FAIL(ObBackupSetFilesOperator::update_tenant_backup_set_file(src_file_info, dst_file_info, client))) {
      LOG_WARN("failed to update tenant backup set file info", KR(ret), K(dst_file_info));
    }
  }
  return ret;
}

int ObBackupBackupset::update_extern_backup_set_file_info(const ObBackupSetFileInfo& backup_set_file_info)
{
  int ret = OB_SUCCESS;
  ObExternBackupSetFileInfoMgr extern_backup_set_file_info_mgr;
  ObClusterBackupDest backup_dest;
  const uint64_t tenant_id = backup_set_file_info.tenant_id_;
  const bool is_backup_backup = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("root backup do not init", K(ret));
  } else if (!backup_set_file_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update extern backup set file info get invalid argument", K(ret), K(backup_set_file_info));
  } else if (OB_FAIL(backup_dest.set(backup_set_file_info.backup_dest_.ptr(), backup_set_file_info.incarnation_))) {
    LOG_WARN("failed to set backup dest", K(ret), K(backup_set_file_info));
  } else if (OB_FAIL(extern_backup_set_file_info_mgr.init(
                 tenant_id, backup_dest, is_backup_backup, *backup_lease_service_))) {
    LOG_WARN("failed to init extern backup set file info mgr", K(ret), K(backup_set_file_info));
  } else if (OB_FAIL(extern_backup_set_file_info_mgr.update_backup_set_file_info(backup_set_file_info))) {
    LOG_WARN("failed to add backup set file info", K(ret), K(backup_set_file_info));
  } else if (OB_FAIL(extern_backup_set_file_info_mgr.upload_backup_set_file_info())) {
    LOG_WARN("failed to upload backup set file info", K(ret), K(backup_set_file_info));
  }
  return ret;
}

int ObBackupBackupset::check_job_tasks_is_complete(const share::ObBackupBackupsetJobInfo& job_info,
    common::ObArray<share::SimpleBackupBackupsetTenant>& missing_tenants, bool& is_complete)
{
  int ret = OB_SUCCESS;
  is_complete = true;
  ObArray<ObTenantBackupBackupsetTaskInfo> task_infos;
  ObArray<SimpleBackupBackupsetTenant> tenant_list;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (!job_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret), K(job_info));
  } else if (OB_FAIL(get_all_tenants(job_info, tenant_list))) {
    LOG_WARN("failed to get all tenants", KR(ret), K(job_info));
  } else if (OB_FAIL(get_all_job_tasks_from_history(job_info, task_infos))) {
    LOG_WARN("failed to get all job tasks", KR(ret), K(job_info));
  } else if (OB_FAIL(
                 inner_check_job_tasks_is_complete(job_info, task_infos, tenant_list, missing_tenants, is_complete))) {
    LOG_WARN("failed to inner check job tasks is complete", KR(ret), K(task_infos), K(tenant_list));
  } else {
    LOG_INFO("do check job tasks is complete", K(is_complete), K(task_infos), K(tenant_list));
  }
  return ret;
}

int ObBackupBackupset::inner_check_job_tasks_is_complete(const share::ObBackupBackupsetJobInfo& job_info,
    const common::ObArray<ObTenantBackupBackupsetTaskInfo>& task_infos,
    const common::ObArray<SimpleBackupBackupsetTenant>& tenant_list,
    common::ObArray<share::SimpleBackupBackupsetTenant>& missing_tenants, bool& is_complete)
{
  int ret = OB_SUCCESS;
  ObArray<ObTenantBackupBackupsetTaskInfo> sys_task_infos;
  missing_tenants.reset();
  is_complete = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(filter_all_sys_job_tasks(task_infos, sys_task_infos))) {
    LOG_WARN("failed to filter all sys job tasks", KR(ret), K(task_infos));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_list.count(); ++i) {
      const SimpleBackupBackupsetTenant& tenant = tenant_list.at(i);
      const uint64_t tenant_id = tenant.tenant_id_;
      bool exist = false;
      for (int64_t j = 0; OB_SUCC(ret) && j < task_infos.count(); ++j) {
        const ObTenantBackupBackupsetTaskInfo& task_info = task_infos.at(j);
        if (task_info.tenant_id_ == tenant_id) {
          exist = true;
          break;
        }
      }
      if (OB_SUCC(ret) && !exist) {
        bool real_missing = false;
        if (OB_FAIL(check_tenant_task_real_missing(job_info, tenant, sys_task_infos, real_missing))) {
          LOG_WARN("failed to check tenant task real missing", KR(ret));
        } else if (!real_missing) {
          LOG_INFO("tenant not real missing, maybe done before", K(tenant));
        } else if (OB_FAIL(missing_tenants.push_back(tenant))) {
          LOG_WARN("failed to push back", KR(ret), K(tenant));
        } else {
          is_complete = false;
        }
      }
    }
  }
  return ret;
}

int ObBackupBackupset::filter_all_sys_job_tasks(
    const common::ObIArray<share::ObTenantBackupBackupsetTaskInfo>& task_infos,
    common::ObIArray<share::ObTenantBackupBackupsetTaskInfo>& sys_task_infos)
{
  int ret = OB_SUCCESS;
  sys_task_infos.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < task_infos.count(); ++i) {
      const ObTenantBackupBackupsetTaskInfo& task_info = task_infos.at(i);
      if (OB_FAIL(sys_task_infos.push_back(task_info))) {
        LOG_WARN("failed to push back task info", KR(ret));
      }
    }
  }
  return ret;
}

int ObBackupBackupset::check_tenant_task_real_missing(const share::ObBackupBackupsetJobInfo& job_info,
    const share::SimpleBackupBackupsetTenant& missing_tenant,
    const common::ObIArray<share::ObTenantBackupBackupsetTaskInfo>& sys_task_infos, bool& real_missing)
{
  int ret = OB_SUCCESS;
  real_missing = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sys_task_infos.count(); ++i) {
      const ObTenantBackupBackupsetTaskInfo& sys_info = sys_task_infos.at(i);
      if (OB_FAIL(inner_check_tenant_task_real_missing(job_info, missing_tenant, sys_info, real_missing))) {
        LOG_WARN(
            "failed to inner check tenant task real missing", KR(ret), K(missing_tenant), K(job_info), K(sys_info));
      } else if (real_missing) {
        LOG_INFO("tenant real missing", K(job_info), K(missing_tenant));
        break;
      }
    }
  }
  return ret;
}

// 1. check if the normal tenant origin backup_set_id exist, if not exist, real_missing is false
// 2. get the normal tenant all backup_set_id copies
// 3. check the correponding backup_set_files info exist
int ObBackupBackupset::inner_check_tenant_task_real_missing(const share::ObBackupBackupsetJobInfo& job_info,
    const share::SimpleBackupBackupsetTenant& missing_tenant,
    const share::ObTenantBackupBackupsetTaskInfo& sys_task_info, bool& real_missing)
{
  int ret = OB_SUCCESS;
  real_missing = false;
  const uint64_t missing_tenant_id = missing_tenant.tenant_id_;
  const int64_t backup_set_id = sys_task_info.backup_set_id_;
  ObBackupSetFileInfo origin_info;
  ObArray<ObBackupSetFileInfo> backup_copies;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(ObBackupSetFilesOperator::get_tenant_backup_set_file_info(missing_tenant_id,
                 backup_set_id,
                 OB_START_INCARNATION,
                 0 /*copy_id*/,
                 false /*for_update*/,
                 *sql_proxy_,
                 origin_info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get tenant backup set file info", KR(ret), K(missing_tenant_id), K(backup_set_id));
    }
  } else if (OB_FAIL(ObBackupSetFilesOperator::get_backup_set_file_info_copies(
                 missing_tenant_id, OB_START_INCARNATION, backup_set_id, *sql_proxy_, backup_copies))) {
    LOG_WARN("failed to get backup set file info copies", KR(ret), K(backup_set_id));
  } else if (backup_copies.empty()) {
    real_missing = true;
  } else {
    bool found = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_copies.count(); ++i) {
      const ObBackupSetFileInfo& copy = backup_copies.at(i);
      ObBackupDest backup_dest;
      if (OB_FAIL(backup_dest.set(copy.backup_dest_.ptr()))) {
        LOG_WARN("failed to set backup dest", KR(ret));
      } else if (backup_dest.is_root_path_equal(job_info.backup_dest_)) {
        found = true;
        real_missing = copy.file_status_ != ObBackupFileStatus::BACKUP_FILE_AVAILABLE;
        break;
      }
    }
    if (!found) {
      real_missing = true;
    }
  }

  return ret;
}

int ObBackupBackupset::do_if_job_tasks_is_not_complete(
    const share::ObBackupBackupsetJobInfo& job_info, const int64_t result)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else {
    ObArray<ObTenantBackupBackupsetTaskItem> task_items;
    if (OB_FAIL(ObTenantBackupBackupsetHistoryOperator::get_all_sys_job_tasks(
            job_info.job_id_, true /*for_update*/, task_items, *sql_proxy_))) {
      LOG_WARN("failed to get all sys job tasks", KR(ret), K(job_info));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < task_items.count(); ++i) {
        ObTenantBackupBackupsetTaskItem& task = task_items.at(i);
        task.result_ = result;
        if (OB_FAIL(trans.start(sql_proxy_))) {
          LOG_WARN("failed to start trans", KR(ret));
        } else if (OB_FAIL(ObTenantBackupBackupsetHistoryOperator::report_task_item(task, trans))) {
          LOG_WARN("failed to report task item", KR(ret), K(task));
        } else if (OB_FAIL(update_tenant_backup_set_file_info(task, trans))) {
          LOG_WARN("failed to update tenant backup set file info", KR(ret), K(task));
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(trans.end(true /*commit*/))) {
            LOG_WARN("failed to commit", KR(ret));
          }
        } else {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = trans.end(false /* commit*/))) {
            LOG_WARN("failed to rollback trans", K(tmp_ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObBackupBackupset::deal_with_missing_tenants(const share::ObBackupBackupsetJobInfo& job_info,
    const common::ObArray<share::SimpleBackupBackupsetTenant>& missing_tenants)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (missing_tenants.empty()) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < missing_tenants.count(); ++i) {
      const SimpleBackupBackupsetTenant& tenant = missing_tenants.at(i);
      const uint64_t tenant_id = tenant.tenant_id_;
      const ObBackupFileStatus::STATUS& file_status = ObBackupFileStatus::BACKUP_FILE_COPYING;
      ObArray<ObBackupSetFileInfo> file_array;
      ObMySQLTransaction trans;
      if (OB_FAIL(trans.start(sql_proxy_))) {
        LOG_WARN("failed to start trans", KR(ret));
      } else {
        if (OB_FAIL(ObBackupSetFilesOperator::get_backup_set_info_with_file_status(
                tenant_id, OB_START_INCARNATION, true /*for_update*/, file_status, trans, file_array))) {
          LOG_WARN("failed to get backup set info with file status", KR(ret), K(tenant_id), K(file_status));
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < file_array.count(); ++j) {
            const ObBackupSetFileInfo& src_info = file_array.at(j);
            ObBackupSetFileInfo dst_info = src_info;
            dst_info.file_status_ = ObBackupFileStatus::BACKUP_FILE_INCOMPLETE;
            dst_info.status_ = ObBackupSetFileInfo::FAILED;
            dst_info.result_ = OB_TENANT_HAS_BEEN_DROPPED;
            if (OB_FAIL(ObBackupSetFilesOperator::update_tenant_backup_set_file(src_info, dst_info, trans))) {
              LOG_WARN("failed to update tenant backup set file", KR(ret));
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(trans.end(true /*commit*/))) {
            LOG_WARN("failed to commit", KR(ret));
          }
        } else {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = trans.end(false /* commit*/))) {
            LOG_WARN("failed to rollback trans", K(tmp_ret));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObArray<ObTenantBackupBackupsetTaskInfo> sys_tasks;
      if (OB_FAIL(ObTenantBackupBackupsetHistoryOperator::get_all_sys_job_tasks(
              job_info.job_id_, false /*for_update*/, sys_tasks, *sql_proxy_))) {
        LOG_WARN("failed to get all sys job tasks", KR(ret), K(job_info));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < sys_tasks.count(); ++i) {
          const ObTenantBackupBackupsetTaskInfo& sys_task = sys_tasks.at(i);
          ObBackupSetFileInfo sys_file_info;
          ObBackupSetFileInfo new_sys_file_info;
          if (OB_FAIL(ObBackupSetFilesOperator::get_tenant_backup_set_file_info(sys_task.tenant_id_,
                  sys_task.backup_set_id_,
                  sys_task.incarnation_,
                  sys_task.copy_id_,
                  true /*for_update*/,
                  *sql_proxy_,
                  sys_file_info))) {
            LOG_WARN("failed to get tenant backup set file info", KR(ret));
          } else {
            new_sys_file_info = sys_file_info;
            new_sys_file_info.file_status_ = ObBackupFileStatus::BACKUP_FILE_INCOMPLETE;
            new_sys_file_info.status_ = ObBackupSetFileInfo::FAILED;
            new_sys_file_info.result_ = OB_TENANT_HAS_BEEN_DROPPED;
            if (OB_FAIL(ObBackupSetFilesOperator::update_tenant_backup_set_file(
                    sys_file_info, new_sys_file_info, *sql_proxy_))) {
              LOG_WARN("failed to update tenant backup set file", KR(ret));
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(refill_missing_bb_tasks_history_for_clean(job_info, missing_tenants))) {
        LOG_WARN("failed to refill bb tasks history for clean", KR(ret), K(job_info), K(missing_tenants));
      } else {
        LOG_INFO("refill missing bb tasks history for clean", K(job_info), K(missing_tenants));
      }
    }
  }
  return ret;
}

int ObBackupBackupset::refill_missing_bb_tasks_history_for_clean(const share::ObBackupBackupsetJobInfo& job_info,
    const common::ObArray<share::SimpleBackupBackupsetTenant>& missing_tenants)
{
  int ret = OB_SUCCESS;
  const int64_t job_id = job_info.job_id_;
  const int64_t result = OB_TENANT_HAS_BEEN_DROPPED;
  ObArray<ObTenantBackupBackupsetTaskInfo> sys_tasks;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(ObTenantBackupBackupsetHistoryOperator::get_all_sys_job_tasks(
                 job_info.job_id_, false /*for_update*/, sys_tasks, *sql_proxy_))) {
    LOG_WARN("failed to get all sys job tasks", KR(ret), K(job_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sys_tasks.count(); ++i) {
      const ObTenantBackupBackupsetTaskInfo& sys_task = sys_tasks.at(i);
      const int64_t backup_set_id = sys_task.backup_set_id_;
      const int64_t copy_id = sys_task.copy_id_;
      for (int64_t j = 0; OB_SUCC(ret) && j < missing_tenants.count(); ++j) {
        const SimpleBackupBackupsetTenant& missing_tenant = missing_tenants.at(j);
        const uint64_t tenant_id = missing_tenant.tenant_id_;
        ObBackupSetFileInfo origin_backup_set_file_info;
        ObTenantBackupBackupsetTaskInfo bb_task_info;
        if (OB_FAIL(ObBackupSetFilesOperator::get_tenant_backup_set_file_info(tenant_id,
                backup_set_id,
                0 /*copy_id*/,
                false /*for_update*/,
                *sql_proxy_,
                origin_backup_set_file_info))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to get tenant backup set file info", KR(ret), K(tenant_id), K(backup_set_id));
          }
        } else if (OB_FAIL(origin_backup_set_file_info.convert_to_backup_backup_task_info(
                       job_id, copy_id, result, job_info.backup_dest_, bb_task_info))) {
          LOG_WARN("failed to convert to backup task info", KR(ret));
        } else if (OB_FAIL(do_external_backup_set_file_info(bb_task_info,
                       ObBackupSetFileInfo::FAILED,
                       ObBackupFileStatus::BACKUP_FILE_INCOMPLETE,
                       result))) {
          LOG_WARN("failed to do external backup set file info", KR(ret), K(bb_task_info));
        } else if (OB_FAIL(ObTenantBackupBackupsetHistoryOperator::insert_task_item(bb_task_info, *sql_proxy_))) {
          LOG_WARN("failed to report task item", KR(ret), K(bb_task_info));
        }
      }
    }
  }
  return ret;
}

int ObBackupBackupset::do_external_backup_set_file_info(const share::ObTenantBackupBackupsetTaskInfo& task_info,
    const share::ObBackupSetFileInfo::BackupSetStatus& status, const share::ObBackupFileStatus::STATUS& file_status,
    const int64_t result)
{
  int ret = OB_SUCCESS;
  ObClusterBackupDest src, dst;
  int64_t full_backup_set_id = 0;
  int64_t inc_backup_set_id = 0;
  const uint64_t tenant_id = task_info.tenant_id_;
  const int64_t copy_id = task_info.copy_id_;
  ObBackupBackupHelper helper;
  if (0 == task_info.prev_full_backup_set_id_ && 0 == task_info.prev_inc_backup_set_id_) {
    full_backup_set_id = task_info.backup_set_id_;
    inc_backup_set_id = task_info.backup_set_id_;
  } else {
    full_backup_set_id = task_info.prev_full_backup_set_id_;
    inc_backup_set_id = task_info.backup_set_id_;
  }
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(src.set(task_info.src_backup_dest_, OB_START_INCARNATION))) {
    LOG_WARN("failed to set src cluster backup dest", KR(ret), K(task_info));
  } else if (OB_FAIL(dst.set(task_info.dst_backup_dest_, OB_START_INCARNATION))) {
    LOG_WARN("failed to set dst cluster backup dest", KR(ret), K(task_info));
  } else if (OB_FAIL(helper.init(src, dst, tenant_id, *backup_lease_service_))) {
    LOG_WARN("failed to init backup backup helper", KR(ret), K(src), K(dst), K(tenant_id));
  } else if (OB_FAIL(helper.do_extern_single_backup_set_info(
                 status, file_status, full_backup_set_id, inc_backup_set_id, copy_id, task_info.date_, result))) {
    LOG_WARN("failed to do extern single backup set info", KR(ret), K(src), K(dst));
  } else if (OB_FAIL(helper.do_extern_backup_set_file_info(
                 status, file_status, task_info.backup_set_id_, copy_id, result))) {
    LOG_WARN("failed to do extern backup set file info", KR(ret), K(src), K(dst));
  } else if (OB_FAIL(helper.sync_extern_backup_set_file_info())) {
    LOG_WARN("failed to sync extern backup set file info", KR(ret), K(src), K(dst), K(tenant_id));
  }
  return ret;
}

/*  ObTenantBackupBackupset */

ObTenantBackupBackupset::ObTenantBackupBackupset()
    : is_inited_(false),
      is_dropped_(false),
      tenant_id_(OB_INVALID_ID),
      job_id_(-1),
      backup_set_id_(-1),
      copy_id_(-1),
      sql_proxy_(NULL),
      zone_mgr_(NULL),
      server_mgr_(NULL),
      root_balancer_(NULL),
      rebalance_task_mgr_(NULL),
      backup_backupset_(NULL),
      rpc_proxy_(NULL),
      src_backup_dest_(),
      dst_backup_dest_(),
      backup_lease_service_(NULL)
{}

ObTenantBackupBackupset::~ObTenantBackupBackupset()
{}

int ObTenantBackupBackupset::init(const bool is_dropped, const uint64_t tenant_id, const int64_t job_id,
    const int64_t backup_set_id, const int64_t copy_id, ObMySQLProxy& sql_proxy, ObZoneManager& zone_mgr,
    ObServerManager& server_mgr, ObRootBalancer& root_balancer, ObRebalanceTaskMgr& task_mgr,
    ObBackupBackupset& backup_backupset, obrpc::ObSrvRpcProxy& rpc_proxy,
    share::ObIBackupLeaseService& backup_lease_service)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tenant backup backupset init twice", KR(ret));
  } else {
    is_dropped_ = is_dropped;
    tenant_id_ = tenant_id;
    job_id_ = job_id;
    backup_set_id_ = backup_set_id;
    copy_id_ = copy_id;
    sql_proxy_ = &sql_proxy;
    zone_mgr_ = &zone_mgr;
    server_mgr_ = &server_mgr;
    root_balancer_ = &root_balancer;
    rebalance_task_mgr_ = &task_mgr;
    backup_backupset_ = &backup_backupset;
    rpc_proxy_ = &rpc_proxy;
    backup_lease_service_ = &backup_lease_service;
    is_inited_ = true;
  }
  return ret;
}

int ObTenantBackupBackupset::do_task()
{
  int ret = OB_SUCCESS;
  ObTenantBackupBackupsetTaskInfo task_info;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (OB_FAIL(get_tenant_task_info(task_info))) {
    LOG_WARN("failed to get tenant status", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(do_with_status(task_info))) {
    LOG_WARN("failed to do with status", KR(ret), K(task_info));
  }
  return ret;
}

int ObTenantBackupBackupset::do_with_status(const ObTenantBackupBackupsetTaskInfo& task_info)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else {
    switch (task_info.task_status_) {
      case ObTenantBackupBackupsetTaskInfo::GENERATE: {
        if (do_generate(task_info)) {
          LOG_WARN("failed to do generate", KR(ret), K(task_info));
        }
        break;
      }
      case ObTenantBackupBackupsetTaskInfo::BACKUP: {
        if (do_backup(task_info)) {
          LOG_WARN("failed to do backup backupset", KR(ret), K(task_info));
        }
        break;
      }
      case ObTenantBackupBackupsetTaskInfo::CANCEL: {
        if (do_cancel(task_info)) {
          LOG_WARN("failed to do cancel", KR(ret), K(task_info));
        }
        break;
      }
      case ObTenantBackupBackupsetTaskInfo::FINISH: {
        if (do_finish(task_info)) {
          LOG_WARN("failed to do finish", KR(ret), K(task_info));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tenant status", KR(ret), K(task_info));
        break;
      }
    }
    LOG_INFO("tenant doing backup backupset", KR(ret), K(task_info));
  }
  return ret;
}

int ObTenantBackupBackupset::do_generate(const share::ObTenantBackupBackupsetTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  const int64_t backup_set_id = task_info.backup_set_id_;
  ObClusterBackupDest backup_dest;
  ObTenantBackupTaskInfo backup_task_info;
  ObArray<ObPGKey> pg_key_list;
  int64_t full_backup_set_id = -1;
  int64_t inc_backup_set_id = -1;
  int64_t total_partition_count = 0;
  int64_t total_macro_block_count = 0;
  bool meta_file_complete = true;
  bool need_generate = true;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (OB_FAIL(check_need_generate_pg_tasks(task_info, need_generate))) {
    LOG_WARN("failed to check need generate pg tasks", KR(ret));
  } else if (!need_generate) {
    LOG_INFO("no need to generate pg tasks", K(task_info));
  } else if (OB_FAIL(get_tenant_backup_task_info(backup_set_id, backup_task_info))) {
    LOG_WARN("failed to get backup set id info", KR(ret));
  } else if (!backup_task_info.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup task info is not valida", KR(ret), K(backup_task_info));
  } else if (OB_FAIL(get_backup_set_id_info(backup_task_info, full_backup_set_id, inc_backup_set_id))) {
    LOG_WARN("failed to get backup set id info", KR(ret));
  } else if (OB_FAIL(backup_dest.set(backup_task_info.backup_dest_, OB_START_INCARNATION))) {
    LOG_WARN("failed to set cluster backup dest", KR(ret), K(task_info));
  } else if (OB_FAIL(fetch_all_pg_list(backup_dest,
                 full_backup_set_id,
                 inc_backup_set_id,
                 backup_task_info.date_,
                 backup_task_info.compatible_,
                 pg_key_list))) {
    LOG_WARN("failed to fetch all pg list", KR(ret), K(backup_dest), K(task_info), K(backup_task_info));
  } else if (OB_FAIL(check_meta_file_complete(backup_task_info,
                 full_backup_set_id,
                 inc_backup_set_id,
                 pg_key_list,
                 total_partition_count,
                 total_macro_block_count,
                 meta_file_complete))) {
    LOG_WARN("failed to check meta file is complete", KR(ret), K(backup_task_info), K(pg_key_list));
  } else if (!meta_file_complete) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta file is not complete", KR(ret));
  } else if (OB_FAIL(generate_backup_backupset_task(copy_id_, backup_set_id, pg_key_list))) {
    LOG_WARN("failed to generate backup backup task", KR(ret));
  }

  if (OB_SUCC(ret)) {
    ObTenantBackupBackupsetTaskInfo new_task_info = task_info;
    new_task_info.total_pg_count_ = pg_key_list.count();
    new_task_info.total_partition_count_ = total_partition_count;
    new_task_info.total_macro_block_count_ = total_macro_block_count;
    if (need_generate) {
      if (meta_file_complete) {
        new_task_info.task_status_ = ObTenantBackupBackupsetTaskInfo::BACKUP;
        new_task_info.result_ = OB_SUCCESS;
      } else {
        new_task_info.task_status_ = ObTenantBackupBackupsetTaskInfo::FINISH;
        new_task_info.result_ = OB_ERR_UNEXPECTED;
      }
    } else {
      new_task_info.task_status_ = ObTenantBackupBackupsetTaskInfo::FINISH;
      new_task_info.result_ = OB_SUCCESS;
    }
    if (OB_FAIL(update_tenant_task_info(new_task_info))) {
      LOG_WARN("failed to update tenant status", KR(ret), K(tenant_id_), K(new_task_info));
    } else {
      root_balancer_->wakeup();
    }
  }
  return ret;
}

int ObTenantBackupBackupset::do_backup(const share::ObTenantBackupBackupsetTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  bool finished = false;
  ObClusterBackupDest src, dst;
  int64_t full_backup_set_id = 0;
  int64_t inc_backup_set_id = 0;
  ObTenantBackupTaskInfo backup_task_info;
  const int64_t backup_set_id = task_info.backup_set_id_;
  ;
  ObExternBackupInfo::ExternBackupInfoStatus status = ObExternBackupInfo::DOING;
  int64_t check_time_interval = CHECK_TIME_INTERVAL;
  ObBackupBackupHelper helper;
#ifdef ERRSIM
  int64_t tmp_interval = GCONF.backup_backupset_retry_interval;
  if (0 != tmp_interval) {
    check_time_interval = tmp_interval;
  }
#endif
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (OB_FAIL(get_tenant_backup_task_info(backup_set_id, backup_task_info))) {
    LOG_WARN("failed to get backup set id info", KR(ret));
  } else if (!backup_task_info.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup task info is not valida", KR(ret), K(backup_task_info));
  } else if (OB_FAIL(get_backup_set_id_info(backup_task_info, full_backup_set_id, inc_backup_set_id))) {
    LOG_WARN("failed to get backup set id info", KR(ret));
  } else if (OB_FAIL(src.set(task_info.src_backup_dest_, OB_START_INCARNATION))) {
    LOG_WARN("failed to set src cluster backup dest", KR(ret), K(task_info));
  } else if (OB_FAIL(dst.set(task_info.dst_backup_dest_, OB_START_INCARNATION))) {
    LOG_WARN("failed to set dst cluster backup dest", KR(ret), K(task_info));
  } else if (OB_FAIL(do_extern_data_backup_info(status, src, dst, full_backup_set_id, inc_backup_set_id))) {
    LOG_WARN("failed to do extern data backup info", KR(ret));
  } else if (OB_FAIL(helper.init(src, dst, tenant_id_, *backup_lease_service_))) {
    LOG_WARN("failed to init backup backup helper", KR(ret), K(src), K(dst), K_(tenant_id));
  } else if (OB_FAIL(helper.do_extern_single_backup_set_info(ObBackupSetFileInfo::DOING,
                 ObBackupFileStatus::BACKUP_FILE_COPYING,
                 full_backup_set_id,
                 inc_backup_set_id,
                 copy_id_,
                 backup_task_info.date_,
                 OB_SUCCESS))) {
    LOG_WARN("failed to do extern single backup set file info", KR(ret), K(src), K(dst));
  } else if (OB_FAIL(helper.do_extern_backup_set_file_info(ObBackupSetFileInfo::DOING,
                 ObBackupFileStatus::BACKUP_FILE_COPYING,
                 task_info.backup_set_id_,
                 copy_id_,
                 OB_SUCCESS))) {
    LOG_WARN("failed to do extern backup set file info", KR(ret), K(src), K(dst));
  }

  DEBUG_SYNC(BACKUP_BACKUPSET_COPYING);

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(check_backup_backupset_task_finished(finished))) {
    LOG_WARN("failed to check backup backupset task finished", KR(ret));
  } else if (!finished && OB_FAIL(check_doing_pg_tasks(task_info))) {
    LOG_WARN("failed to check doing pg tasks", KR(ret), K(task_info));
  }

  if (OB_SUCC(ret) && !finished) {
    if (REACH_TIME_INTERVAL(check_time_interval)) {
      if (OB_FAIL(deal_with_finished_failed_pg_task(task_info))) {
        LOG_WARN("failed to deal with finished failed pg task", KR(ret));
      }
    }
  }

  if (OB_SUCC(ret) && finished) {
    ObTenantBackupBackupsetTaskInfo new_task_info = task_info;
    new_task_info.task_status_ = ObTenantBackupBackupsetTaskInfo::FINISH;
    if (OB_FAIL(update_tenant_task_info(new_task_info))) {
      LOG_WARN("failed to update tenant status", KR(ret), K(tenant_id_), K(new_task_info));
    } else {
      backup_backupset_->wakeup();
    }
  }
  return ret;
}

int ObTenantBackupBackupset::do_cancel(const share::ObTenantBackupBackupsetTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = task_info.tenant_id_;
  const int64_t job_id = task_info.job_id_;
  const int64_t backup_set_id = task_info.backup_set_id_;
  ObArray<ObPGBackupBackupsetTaskInfo> tasks;
  SimpleBackupBackupsetTenant tenant;
  tenant.is_dropped_ = is_dropped_;
  tenant.tenant_id_ = tenant_id;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (OB_FAIL(
                 ObPGBackupBackupsetOperator::get_doing_pg_tasks(tenant, job_id, backup_set_id, tasks, *sql_proxy_))) {
    LOG_WARN("failed to get one doing pg tasks", KR(ret), K(tenant), K(job_id), K(backup_set_id));
  } else {
    LOG_INFO("cancel backup backupset task", K(tenant_id_));
    for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
      obrpc::ObCancelTaskArg rpc_arg;
      const ObPGBackupBackupsetTaskInfo& task = tasks.at(i);
      const ObAddr& task_server = task.server_;
      rpc_arg.task_id_ = task.trace_id_;
      if (OB_FAIL(rpc_proxy_->to(task_server).cancel_sys_task(rpc_arg))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          LOG_INFO("task may not excute on server", K(rpc_arg), K(task_server));
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to cancel sys task", K(ret), K(rpc_arg));
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObTenantBackupBackupsetTaskInfo new_task_info = task_info;
      new_task_info.task_status_ = ObTenantBackupBackupsetTaskInfo::FINISH;
      if (OB_FAIL(update_tenant_task_info(new_task_info))) {
        LOG_WARN("failed to update tenant status", KR(ret), K(tenant_id_), K(new_task_info));
      } else {
        backup_backupset_->wakeup();
      }
    }
  }
  return ret;
}

int ObTenantBackupBackupset::do_finish(const share::ObTenantBackupBackupsetTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  ObClusterBackupDest src;
  ObClusterBackupDest dst;
  ObTenantBackupTaskInfo backup_task_info;
  ObPGBackupBackupsetTaskStat pg_stat;
  int64_t full_backup_set_id = -1;
  int64_t inc_backup_set_id = -1;
  ObBackupBackupHelper helper;
  ObExternBackupInfo::ExternBackupInfoStatus status =
      OB_SUCCESS == task_info.result_ ? ObExternBackupInfo::SUCCESS : ObExternBackupInfo::FAILED;
  ObBackupSetFileInfo::BackupSetStatus backup_set_status =
      OB_SUCCESS == task_info.result_ ? ObBackupSetFileInfo::SUCCESS : ObBackupSetFileInfo::FAILED;
  ObBackupFileStatus::STATUS file_status = OB_SUCCESS == task_info.result_ ? ObBackupFileStatus::BACKUP_FILE_AVAILABLE
                                                                           : ObBackupFileStatus::BACKUP_FILE_INCOMPLETE;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (OB_SYS_TENANT_ID == task_info.tenant_id_) {
    // do nothing
  } else if (OB_FAIL(get_finished_pg_stat(task_info, pg_stat))) {
    LOG_WARN("failed to get finished pg stat", KR(ret), K(task_info));
  } else if (OB_FAIL(get_tenant_backup_task_info(task_info.backup_set_id_, backup_task_info))) {
    LOG_WARN("failed to get tenant backup task info", KR(ret), K(task_info));
  } else if (OB_FAIL(get_backup_set_id_info(backup_task_info, full_backup_set_id, inc_backup_set_id))) {
    LOG_WARN("failed to get backup set id info", KR(ret), K(backup_task_info));
  } else if (OB_FAIL(src.set(task_info.src_backup_dest_, OB_START_INCARNATION))) {
    LOG_WARN("failed to set src cluster backup dest", KR(ret), K(task_info));
  } else if (OB_FAIL(dst.set(task_info.dst_backup_dest_, OB_START_INCARNATION))) {
    LOG_WARN("failed to set src cluster backup dest", KR(ret), K(task_info));
  } else if (OB_FAIL(transfer_pg_list(src,
                 dst,
                 full_backup_set_id,
                 inc_backup_set_id,
                 backup_task_info.date_,
                 backup_task_info.compatible_))) {
    LOG_WARN("failed to transfer pg list", KR(ret), K(src), K(dst), K(task_info));
  } else if (OB_FAIL(transfer_tenant_locality_info(src,
                 dst,
                 full_backup_set_id,
                 inc_backup_set_id,
                 backup_task_info.date_,
                 backup_task_info.compatible_))) {
    LOG_WARN("failed to transfer tenant locality info", KR(ret), K(src), K(dst));
  } else if (OB_FAIL(do_backup_set_info(src,
                 dst,
                 full_backup_set_id,
                 inc_backup_set_id,
                 backup_task_info.date_,
                 backup_task_info.compatible_))) {
    LOG_WARN("failed to do backup set info", KR(ret), K(src), K(dst));
  } else if (OB_FAIL(transfer_backup_set_meta(src,
                 dst,
                 full_backup_set_id,
                 inc_backup_set_id,
                 backup_task_info.date_,
                 backup_task_info.compatible_))) {
    LOG_WARN("failed to transfer backup set meta", KR(ret), K(src), K(dst));
  } else if (OB_FAIL(do_extern_data_backup_info(status, src, dst, full_backup_set_id, inc_backup_set_id))) {
    LOG_WARN("failed to do data backup info", KR(ret));
  } else if (OB_FAIL(helper.init(src, dst, tenant_id_, *backup_lease_service_))) {
    LOG_WARN("failed to init backup backup helper", KR(ret), K(src), K(dst), K_(tenant_id));
  } else if (OB_FAIL(helper.do_extern_single_backup_set_info(backup_set_status,
                 file_status,
                 full_backup_set_id,
                 inc_backup_set_id,
                 copy_id_,
                 backup_task_info.date_,
                 OB_SUCCESS))) {
    LOG_WARN("failed to do extern single backup set info", KR(ret), K(dst));
  } else if (OB_FAIL(helper.do_extern_backup_set_file_info(
                 backup_set_status, file_status, task_info.backup_set_id_, copy_id_, task_info.result_))) {
    LOG_WARN("failed to do extern backup set info", KR(ret), K(dst));
  } else if (OB_FAIL(helper.sync_extern_backup_set_file_info())) {
    LOG_WARN("failed to sync extern backup set file info", KR(ret), K(src), K(dst));
  }
  if (OB_SUCC(ret)) {
    ObTenantBackupBackupsetTaskInfo update_task_info = task_info;
    update_task_info.end_ts_ = ObTimeUtility::current_time();
    update_task_info.finish_pg_count_ = task_info.total_pg_count_;
    update_task_info.finish_partition_count_ = pg_stat.finish_partition_count_;
    update_task_info.finish_macro_block_count_ = pg_stat.finish_macro_block_count_;
    update_task_info.input_bytes_ = backup_task_info.output_bytes_;
    update_task_info.output_bytes_ = backup_task_info.output_bytes_;
    if (OB_FAIL(update_tenant_task_info(update_task_info))) {
      LOG_WARN("failed to update tenant task info", KR(ret), K(update_task_info));
    } else {
      LOG_INFO("tenant backup backupset task finished", K(tenant_id_), K(task_info));
    }
  }
  return ret;
}

int ObTenantBackupBackupset::check_need_generate_pg_tasks(
    const share::ObTenantBackupBackupsetTaskInfo& task_info, bool& need_generate)
{
  int ret = OB_SUCCESS;
  need_generate = true;
  const uint64_t tenant_id = task_info.tenant_id_;
  const int64_t backup_set_id = task_info.backup_set_id_;
  const int64_t copy_id = task_info.copy_id_;
  const bool for_update = false;
  ObBackupSetFileInfo file_info;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (!task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret), K(task_info));
  } else if (OB_FAIL(ObBackupSetFilesOperator::get_tenant_backup_set_file_info(
                 tenant_id, backup_set_id, copy_id, for_update, *sql_proxy_, file_info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get tenant backup set file info", KR(ret), K(tenant_id), K(backup_set_id), K(copy_id));
    }
  } else {
    need_generate =
        !(ObBackupFileStatus::BACKUP_FILE_AVAILABLE == file_info.file_status_ && OB_SUCCESS == file_info.result_);
  }
  return ret;
}

int ObTenantBackupBackupset::check_doing_pg_tasks(const share::ObTenantBackupBackupsetTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  int64_t prev_check_time = 0;
  const uint64_t tenant_id = task_info.tenant_id_;
  ObArray<ObPGBackupBackupsetTaskInfo> doing_pg_tasks;
  SimpleBackupBackupsetTenant tenant;
  tenant.is_dropped_ = is_dropped_;
  tenant.tenant_id_ = tenant_id;
  int64_t check_time_interval = CHECK_TIME_INTERVAL;
#ifdef ERRSIM
  int64_t tmp_interval = GCONF.backup_backupset_retry_interval;
  if (0 != tmp_interval) {
    check_time_interval = tmp_interval;
  }
#endif
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (!task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check doing pg task get invalid argument", KR(ret), K(task_info));
  } else if (OB_ISNULL(backup_backupset_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("backup backupset is null", KR(ret), KP(backup_backupset_));
  } else if (OB_FAIL(backup_backupset_->get_check_time(tenant_id, prev_check_time))) {
    LOG_WARN("failed to get check time", KR(ret), K(task_info));
  } else if (ObTimeUtility::current_time() - prev_check_time < check_time_interval) {
    // do nothing
  } else if (OB_FAIL(backup_backupset_->update_check_time(tenant_id))) {
    LOG_WARN("failed to update check time", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObPGBackupBackupsetOperator::get_doing_pg_tasks(
                 tenant, task_info.job_id_, task_info.backup_set_id_, doing_pg_tasks, *sql_proxy_))) {
    LOG_WARN("failed to get doing pg tasks", KR(ret), K(tenant), K(task_info));
  } else {
    ObArray<ObPGBackupBackupsetTaskInfo> fake_doing_tasks;
    for (int64_t i = 0; OB_SUCC(ret) && i < doing_pg_tasks.count(); ++i) {
      const ObPGBackupBackupsetTaskInfo& pg_task = doing_pg_tasks.at(i);
      bool actual_doing = true;
      if (OB_FAIL(check_doing_pg_task(pg_task, actual_doing))) {
        LOG_WARN("failed to check doing pg task", KR(ret), K(pg_task));
      } else if (actual_doing) {
        // do nothing if actual doing
      } else if (OB_FAIL(fake_doing_tasks.push_back(pg_task))) {
        LOG_WARN("failed to push back pg task", KR(ret), K(pg_task));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(reset_doing_pg_tasks(fake_doing_tasks))) {
        LOG_WARN("failed to reset doing pg tasks", KR(ret), K(fake_doing_tasks));
      }
    }
  }
  return ret;
}

int ObTenantBackupBackupset::check_doing_pg_task(
    const share::ObPGBackupBackupsetTaskInfo& task_info, bool& actual_doing)
{
  int ret = OB_SUCCESS;
  actual_doing = false;
  bool in_task_mgr = false;
  bool in_progress = false;
  bool finished = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (!task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check doing pg task get invalid argument", KR(ret), K(task_info));
  } else if (OB_FAIL((check_in_rebalance_task_mgr(task_info, in_task_mgr)))) {
    LOG_WARN("failed to check in rebalance task mgr", KR(ret), K(task_info));
  } else if (in_task_mgr) {
    actual_doing = true;
  } else if (OB_FAIL(check_task_in_progress(task_info, in_progress))) {
    LOG_WARN("failed to check task in progress", KR(ret), K(task_info));
  } else if (in_progress) {
    actual_doing = true;
  } else if (OB_FAIL(check_pg_task_finished(task_info, finished))) {
    LOG_WARN("failed to check pg task finished", KR(ret), K(task_info));
  } else if (finished) {
    actual_doing = true;
  }
  return ret;
}

int ObTenantBackupBackupset::check_in_rebalance_task_mgr(
    const share::ObPGBackupBackupsetTaskInfo& task_info, bool& exists)
{
  int ret = OB_SUCCESS;
  exists = true;
  ObPartitionKey pkey;
  ObBackupBackupsetTaskInfo mock_info;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (OB_FAIL(pkey.init(task_info.table_id_, task_info.partition_id_, 0))) {
    LOG_WARN("failed to init pkey", KR(ret), K(task_info));
  } else if (OB_FAIL(mock_info.init_task_key(task_info.job_id_,
                 task_info.backup_set_id_,
                 pkey.get_table_id(),
                 pkey.get_partition_id(),
                 RebalanceKeyType::BACKUP_MANAGER_KEY))) {
    LOG_WARN("failed to build task key", KR(ret), K(task_info), K(pkey));
  } else if (OB_FAIL(rebalance_task_mgr_->check_rebalance_task_exist(mock_info, task_info.server_, exists))) {
    LOG_WARN("failed to get schedule task", KR(ret), K(mock_info), K(task_info));
  }
  return ret;
}

int ObTenantBackupBackupset::check_task_in_progress(
    const share::ObPGBackupBackupsetTaskInfo& task_info, bool& in_progress)
{
  int ret = OB_SUCCESS;
  in_progress = true;
  obrpc::Bool res = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", K(ret));
  } else if (ObPGBackupBackupsetTaskInfo::DOING != task_info.status_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check task in progress get invalid argument", KR(ret), K(task_info));
  } else {
    share::ObServerStatus server_status;
    const common::ObAddr& dest = task_info.server_;
    bool is_exist = false;
    if (OB_FAIL(server_mgr_->is_server_exist(dest, is_exist))) {
      LOG_WARN("failed to check server exist", KR(ret));
    } else if (!is_exist) {
      in_progress = false;
      FLOG_INFO("backup backupset server is not exist", KR(ret), K(dest));
    } else if (OB_FAIL(server_mgr_->get_server_status(dest, server_status))) {
      LOG_WARN("fail to get server status", KR(ret), K(dest));
    } else if (!server_status.is_active() || !server_status.in_service()) {
      in_progress = false;
      FLOG_INFO("server status may not active or in service", K(dest));
    } else if (OB_FAIL(rpc_proxy_->to(dest).check_migrate_task_exist(task_info.trace_id_, res))) {
      LOG_WARN("failed to check task", KR(ret), K(task_info));
    } else if (!res) {
      in_progress = false;
    }
    LOG_DEBUG("check backup backupset task in progress", K(ret), K(server_status), K(res), K(is_exist), K(in_progress));
  }
  return ret;
}

int ObTenantBackupBackupset::check_pg_task_finished(const share::ObPGBackupBackupsetTaskInfo& task_info, bool& finished)
{
  int ret = OB_SUCCESS;
  finished = false;
  SimpleBackupBackupsetTenant tenant;
  tenant.tenant_id_ = tenant_id_;
  tenant.is_dropped_ = is_dropped_;
  ObPGBackupBackupsetTaskInfo cur_info;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", K(ret));
  } else if (OB_FAIL(ObPGBackupBackupsetOperator::get_pg_task(tenant,
                 task_info.job_id_,
                 task_info.backup_set_id_,
                 task_info.table_id_,
                 task_info.partition_id_,
                 cur_info,
                 *sql_proxy_))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      finished = true;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get doing pg task", KR(ret), K(tenant), K(task_info));
    }
  } else if (ObPGBackupBackupsetTaskInfo::FINISH == cur_info.status_) {
    finished = true;
  }
  return ret;
}

int ObTenantBackupBackupset::reset_doing_pg_tasks(const ObIArray<ObPGBackupBackupsetTaskInfo>& task_infos)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (task_infos.empty()) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < task_infos.count(); ++i) {
      const ObPGBackupBackupsetTaskInfo& task_info = task_infos.at(i);
      if (OB_FAIL(inner_reset_doing_pg_tasks(task_info))) {
        LOG_WARN("failed to inner reset doing pg tasks", KR(ret), K(task_info));
      }
    }
  }
  return ret;
}

int ObTenantBackupBackupset::inner_reset_doing_pg_tasks(const share::ObPGBackupBackupsetTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  SimpleBackupBackupsetTenant tenant;
  tenant.is_dropped_ = is_dropped_;
  tenant.tenant_id_ = tenant_id_;
  ObArray<ObPGBackupBackupsetTaskInfo> same_trace_id_tasks;
  ObArray<ObPGBackupBackupsetTaskInfo> reset_task_infos;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (OB_FAIL(get_same_trace_id_tasks(task_info, same_trace_id_tasks))) {
    LOG_WARN("failed to get same trace id tasks", KR(ret), K(task_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < same_trace_id_tasks.count(); ++i) {
      const ObPGBackupBackupsetTaskInfo& info = same_trace_id_tasks.at(i);
      ObPGBackupBackupsetTaskInfo reset_info = info;
      reset_info.status_ = ObPGBackupBackupsetTaskInfo::PENDING;
      reset_info.trace_id_.reset();
      reset_info.server_.reset();
      reset_info.finish_partition_count_ = 0;
      reset_info.finish_partition_count_ = 0;
      reset_info.result_ = 0;
      if (OB_FAIL(reset_task_infos.push_back(reset_info))) {
        LOG_WARN("failed to push back reset info", KR(ret), K(reset_info));
      }
    }
    if (OB_SUCC(ret)) {
      if (reset_task_infos.empty()) {
        // do nothing
      } else if (OB_FAIL(ObPGBackupBackupsetOperator::batch_report_task(tenant, reset_task_infos, *sql_proxy_))) {
        LOG_WARN("failed to batch report task", KR(ret), K(tenant), K(reset_task_infos));
      } else {
        root_balancer_->wakeup();
      }
    }
  }
  return ret;
}

int ObTenantBackupBackupset::get_same_trace_id_tasks(const share::ObPGBackupBackupsetTaskInfo& task_info,
    common::ObIArray<share::ObPGBackupBackupsetTaskInfo>& task_infos)
{
  int ret = OB_SUCCESS;
  task_infos.reset();
  SimpleBackupBackupsetTenant tenant;
  tenant.is_dropped_ = is_dropped_;
  tenant.tenant_id_ = tenant_id_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (!task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pg task info is not valid", KR(ret), K(task_info));
  } else if (OB_FAIL(
                 ObPGBackupBackupsetOperator::get_same_trace_id_tasks(tenant, task_info, task_infos, *sql_proxy_))) {
    LOG_WARN("failed to get same trace id tasks", KR(ret), K(task_info));
  }
  return ret;
}

int ObTenantBackupBackupset::deal_with_finished_failed_pg_task(const share::ObTenantBackupBackupsetTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  const int64_t job_id = task_info.job_id_;
  const int64_t backup_set_id = task_info.backup_set_id_;
  SimpleBackupBackupsetTenant tenant;
  tenant.is_dropped_ = is_dropped_;
  tenant.tenant_id_ = tenant_id_;
  ObArray<ObPGBackupBackupsetTaskInfo> task_infos;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (OB_FAIL(ObPGBackupBackupsetOperator::get_failed_pg_tasks(
                 tenant, job_id, backup_set_id, task_infos, *sql_proxy_))) {
    LOG_WARN("failed to get failed pg tasks", KR(ret), K(tenant), K(job_id), K(backup_set_id));
  } else if (task_infos.empty()) {
    // do nothing
  } else {
    DEBUG_SYNC(BEFORE_DEAL_WITH_FAILED_BACKUP_BACKUPSET_TASK);
    for (int64_t i = 0; OB_SUCC(ret) && i < task_infos.count(); ++i) {
      ObPGBackupBackupsetTaskInfo& reset_info = task_infos.at(i);
      reset_info.status_ = ObPGBackupBackupsetTaskInfo::PENDING;
      reset_info.trace_id_.reset();
      reset_info.server_.reset();
      reset_info.finish_partition_count_ = 0;
      reset_info.finish_partition_count_ = 0;
      reset_info.result_ = 0;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObPGBackupBackupsetOperator::batch_report_task(tenant, task_infos, *sql_proxy_))) {
        LOG_WARN("failed to batch report tasks", KR(ret), K(tenant), K(task_infos));
      } else {
        root_balancer_->wakeup();
      }
    }
  }
  return ret;
}

int ObTenantBackupBackupset::check_meta_file_complete(const share::ObTenantBackupTaskInfo& backup_info,
    const int64_t full_backup_set_id, const int64_t inc_backup_set_id, const common::ObIArray<common::ObPGKey>& pg_list,
    int64_t& total_partition_count, int64_t& total_macro_block_count, bool& complete)
{
  int ret = OB_SUCCESS;
  ObClusterBackupDest backup_dest;
  ObBackupBaseDataPathInfo path_info;
  ObBackupMetaFileStore meta_store;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (OB_FAIL(backup_dest.set(backup_info.backup_dest_, OB_START_INCARNATION))) {
    LOG_WARN("failed to set cluster backup dest", KR(ret));
  } else if (OB_FAIL(path_info.set(backup_dest,
                 backup_info.tenant_id_,
                 full_backup_set_id,
                 inc_backup_set_id,
                 backup_info.date_,
                 backup_info.compatible_))) {
    LOG_WARN("failed to set backup base data path info", KR(ret));
  } else if (OB_FAIL(meta_store.init(path_info))) {
    LOG_WARN("failed to init backup meta file store", KR(ret), K(path_info));
  } else if (OB_FAIL(check_pg_list_complete(
                 meta_store, pg_list, total_partition_count, total_macro_block_count, complete))) {
    LOG_WARN("failed to check pg list is complete", KR(ret), K(pg_list));
  }
  return ret;
}

int ObTenantBackupBackupset::check_pg_list_complete(share::ObBackupMetaFileStore& meta_store,
    const common::ObIArray<common::ObPGKey>& pg_key_list, int64_t& total_partition_count,
    int64_t& total_macro_block_count, bool& complete)
{
  int ret = OB_SUCCESS;
  complete = false;
  total_partition_count = 0;
  total_macro_block_count = 0;
  hash::ObHashSet<ObPGKey> pg_set;
  const int64_t MAX_PG_SET_SIZE = 10000;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (OB_FAIL(pg_set.create(MAX_PG_SET_SIZE))) {
    LOG_WARN("failed to create hash set", KR(ret));
  } else if (OB_FAIL(convert_array_to_set(pg_key_list, pg_set))) {
    LOG_WARN("failed to convert array to set", KR(ret));
  } else {
    while (OB_SUCC(ret) && pg_set.size() > 0) {
      ObBackupMeta backup_meta;
      if (OB_FAIL(meta_store.next(backup_meta))) {
        if (OB_ITER_END == ret) {
          LOG_INFO("iterator ended", K(ret));
        } else {
          LOG_WARN("failed to get next backup meta", KR(ret));
        }
      } else {
        const ObPGKey& pg_key = backup_meta.partition_group_meta_.pg_key_;
        if (OB_FAIL(pg_set.erase_refactored(pg_key))) {
          if (OB_HASH_NOT_EXIST == ret) {
            // 前面可能erase过了
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to erase pg key", KR(ret), K(pg_key), K(pg_set.size()));
          }
        } else {
          total_macro_block_count += backup_meta.get_total_macro_block_count();
          total_partition_count += backup_meta.get_total_partition_count();
        }
      }
    }
    if (pg_set.size() > 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pg meta is not complete", KR(ret), K(pg_set.size()));
      hash::ObHashSet<ObPGKey>::const_iterator iter;
      for (iter = pg_set.begin(); OB_SUCC(ret) && iter != pg_set.end(); ++iter) {
        LOG_INFO("missing pg key", K(iter->first));
      }
    }
    if (OB_SUCC(ret)) {
      if (0 == pg_set.size()) {
        complete = true;
      }
    } else {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        complete = true;
      } else {
        complete = false;
      }
    }
  }
  return ret;
}

int ObTenantBackupBackupset::get_tenant_backup_task_info(
    const int64_t backup_set_id, share::ObTenantBackupTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  ObBackupTaskHistoryUpdater updater;
  const bool for_update = false;
  task_info.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (OB_FAIL(updater.init(*sql_proxy_))) {
    LOG_WARN("failed to init backup task history updater", KR(ret));
  } else if (OB_FAIL(updater.get_original_tenant_backup_task(tenant_id_, backup_set_id, for_update, task_info))) {
    LOG_WARN("failed to get tenant backup task", KR(ret), K(tenant_id_), K(backup_set_id));
  }
  return ret;
}

int ObTenantBackupBackupset::get_backup_set_id_info(
    const share::ObTenantBackupTaskInfo& task_info, int64_t& full_backup_set_id, int64_t& inc_backup_set_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else {
    if (0 == task_info.prev_full_backup_set_id_ && 0 == task_info.prev_inc_backup_set_id_) {
      full_backup_set_id = task_info.backup_set_id_;
      inc_backup_set_id = task_info.backup_set_id_;
    } else {
      full_backup_set_id = task_info.prev_full_backup_set_id_;
      inc_backup_set_id = task_info.backup_set_id_;
    }
  }
  return ret;
}

int ObTenantBackupBackupset::fetch_all_pg_list(const share::ObClusterBackupDest& src_backup_dest,
    const int64_t full_backup_set_id, const int64_t inc_backup_set_id, const int64_t backup_snapshot_version,
    const int64_t compatible, common::ObIArray<common::ObPGKey>& pg_keys)
{
  int ret = OB_SUCCESS;
  pg_keys.reset();
  ObArray<ObPGKey> sys_pg_keys;
  ObArray<ObPGKey> normal_pg_keys;
  ObExternPGListMgr extern_pg_list_mgr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (OB_FAIL(extern_pg_list_mgr.init(tenant_id_,
                 full_backup_set_id,
                 inc_backup_set_id,
                 src_backup_dest,
                 backup_snapshot_version,
                 compatible,
                 *backup_lease_service_))) {
    LOG_WARN("failed to init extern pg list mgr", KR(ret));
  } else if (OB_FAIL(extern_pg_list_mgr.get_sys_pg_list(sys_pg_keys))) {
    LOG_WARN("failed to get sys pg list", KR(ret));
  } else if (OB_FAIL(extern_pg_list_mgr.get_normal_pg_list(normal_pg_keys))) {
    LOG_WARN("failed to get normal pg list", KR(ret));
  } else if (OB_FAIL(append(pg_keys, sys_pg_keys))) {
    LOG_WARN("failed to add sys pg key array", KR(ret));
  } else if (OB_FAIL(append(pg_keys, normal_pg_keys))) {
    LOG_WARN("failed to add norml pg key array", KR(ret));
  }
  return ret;
}

int ObTenantBackupBackupset::generate_backup_backupset_task(
    const int64_t copy_id, const int64_t backup_set_id, const common::ObIArray<common::ObPGKey>& pg_keys)
{
  int ret = OB_SUCCESS;
  SimpleBackupBackupsetTenant tenant;
  tenant.tenant_id_ = tenant_id_;
  tenant.is_dropped_ = is_dropped_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (pg_keys.empty()) {
    LOG_INFO("no task need to generate", K(pg_keys.count()));
  } else {
    ObArray<ObPGBackupBackupsetTaskInfo> task_infos;
    for (int64_t i = 0; OB_SUCC(ret) && i < pg_keys.count(); ++i) {
      const ObPGKey& pg_key = pg_keys.at(i);
      ObPGBackupBackupsetTaskInfo task_info;
      if (OB_FAIL(inner_generate_backup_backupset_task(copy_id, backup_set_id, pg_key, task_info))) {
        LOG_WARN("failed to generate pg backup backupset task", KR(ret), K(backup_set_id), K(pg_key));
      } else if (OB_FAIL(task_infos.push_back(task_info))) {
        LOG_WARN("failed to push back task info", KR(ret), K(task_info));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObPGBackupBackupsetOperator::batch_report_task(tenant, task_infos, *sql_proxy_))) {
        LOG_WARN("failed to batch report tasks", KR(ret));
      }
    }
  }
  return ret;
}

int ObTenantBackupBackupset::inner_generate_backup_backupset_task(const int64_t copy_id, const int64_t backup_set_id,
    const common::ObPGKey& pg_key, share::ObPGBackupBackupsetTaskInfo& pg_task_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (!pg_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("inner generate backup backupset task", KR(ret));
  } else {
    pg_task_info.tenant_id_ = tenant_id_;
    pg_task_info.job_id_ = job_id_;
    pg_task_info.incarnation_ = OB_START_INCARNATION;
    pg_task_info.backup_set_id_ = backup_set_id;
    pg_task_info.copy_id_ = copy_id;
    pg_task_info.table_id_ = pg_key.get_table_id();
    pg_task_info.partition_id_ = pg_key.get_partition_id();
    pg_task_info.status_ = ObPGBackupBackupsetTaskInfo::PENDING;
    pg_task_info.total_partition_count_ = 0;
    pg_task_info.finish_partition_count_ = 0;
    pg_task_info.total_macro_block_count_ = 0;
    pg_task_info.finish_macro_block_count_ = 0;
    pg_task_info.result_ = 0;
  }
  return ret;
}

int ObTenantBackupBackupset::get_tenant_task_info(share::ObTenantBackupBackupsetTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  SimpleBackupBackupsetTenant tenant;
  tenant.tenant_id_ = tenant_id_;
  tenant.is_dropped_ = is_dropped_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (OB_FAIL(ObTenantBackupBackupsetOperator::get_task_item(
                 tenant, job_id_, backup_set_id_, task_info, *sql_proxy_))) {
    LOG_WARN("failed to get task item", KR(ret), K(tenant), K(job_id_), K(backup_set_id_));
  }
  return ret;
}

int ObTenantBackupBackupset::update_tenant_task_info(const share::ObTenantBackupBackupsetTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  SimpleBackupBackupsetTenant tenant;
  tenant.is_dropped_ = is_dropped_;
  tenant.tenant_id_ = tenant_id_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (OB_FAIL(ObTenantBackupBackupsetOperator::report_task_item(tenant, task_info, *sql_proxy_))) {
    LOG_WARN("failed to report tenant task info", KR(ret), K(task_info));
  }
  return ret;
}

int ObTenantBackupBackupset::transfer_backup_set_meta(const share::ObClusterBackupDest& src,
    const share::ObClusterBackupDest& dst, const int64_t full_backup_set_id, const int64_t inc_backup_set_id,
    const int64_t backup_date, const int64_t compatible)
{
  int ret = OB_SUCCESS;
  ObBackupPath src_path;
  ObBackupPath dst_path;
  ObStorageUtil util(false);
  ObBackupBaseDataPathInfo src_path_info;
  ObBackupBaseDataPathInfo dst_path_info;
  ObArenaAllocator allocator(ObModIds::BACKUP);
  ObArray<ObString> file_names;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (OB_FAIL(
                 src_path_info.set(src, tenant_id_, full_backup_set_id, inc_backup_set_id, backup_date, compatible))) {
    LOG_WARN("failed to set backup base data path info",
        KR(ret),
        K(src),
        K(tenant_id_),
        K(full_backup_set_id),
        K(inc_backup_set_id));
  } else if (OB_FAIL(
                 dst_path_info.set(dst, tenant_id_, full_backup_set_id, inc_backup_set_id, backup_date, compatible))) {
    LOG_WARN("failed to set backup base data path info",
        KR(ret),
        K(dst),
        K(tenant_id_),
        K(full_backup_set_id),
        K(inc_backup_set_id));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_data_inc_backup_set_path(src_path_info, src_path))) {
    LOG_WARN("failed to get tenant data inc backup set path", KR(ret), K(src_path_info));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_data_inc_backup_set_path(dst_path_info, dst_path))) {
    LOG_WARN("failed to get tenant data inc backup set path", KR(ret), K(dst_path_info));
  } else if (OB_FAIL(util.list_files(
                 src_path.get_obstr(), src_path_info.dest_.get_storage_info(), allocator, file_names))) {
    LOG_WARN("failed to list files", KR(ret), K(src_path));
  } else {
    ObBackupPath src_file_path;
    ObBackupPath dst_file_path;
    for (int64_t i = 0; OB_SUCC(ret) && i < file_names.count(); ++i) {
      src_file_path.reset();
      dst_file_path.reset();
      ObString& file_name = file_names.at(i);
      if (!file_name.prefix_match(OB_STRING_BACKUP_META_INDEX) && !file_name.prefix_match(OB_STRING_BACKUP_META_FILE) &&
          !file_name.prefix_match(OB_STRING_TENANT_DIAGNOSE_INFO)) {
        LOG_INFO("skip non meta data file", K(file_name));
      } else if (OB_FAIL(src_file_path.init(src_path.get_obstr()))) {
        LOG_WARN("failed to init file base path", KR(ret), K(src_file_path));
      } else if (OB_FAIL(src_file_path.join(file_name))) {
        LOG_WARN("failed to init file path", KR(ret), K(file_name));
      } else if (OB_FAIL(dst_file_path.init(dst_path.get_obstr()))) {
        LOG_WARN("failed to init file base path", KR(ret), K(dst_file_path));
      } else if (OB_FAIL(dst_file_path.join(file_name))) {
        LOG_WARN("failed to init file path", KR(ret), K(file_name));
      } else if (OB_FAIL(transfer_single_file(src_file_path.get_obstr(),
                     src_path_info.dest_.get_storage_info(),
                     dst_file_path.get_obstr(),
                     dst_path_info.dest_.get_storage_info()))) {
        LOG_WARN("failed to transfer single file", KR(ret));
      } else {
        LOG_INFO("transfer backup set meta", K(tenant_id_));
      }
    }
  }
  return ret;
}

int ObTenantBackupBackupset::transfer_single_file(const common::ObString& src_uri,
    const common::ObString& src_storage_info, const common::ObString& dst_uri, const common::ObString& dst_storage_info)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObStorageUtil util(false);
  int64_t file_length = 0;
  char* buf = NULL;
  int64_t read_size = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (OB_FAIL(util.get_file_length(src_uri, src_storage_info, file_length))) {
    LOG_WARN("failed to get file length", KR(ret), K(src_uri), K(src_storage_info));
  } else if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(file_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", KR(ret), K(file_length));
  } else if (OB_FAIL(util.read_single_file(src_uri, src_storage_info, buf, file_length, read_size))) {
    LOG_WARN("failed to read single file", KR(ret), K(src_uri), K(src_storage_info));
  } else if (file_length != read_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file length do not equal read size", KR(ret), K(file_length), K(read_size));
  } else if (OB_FAIL(util.write_single_file(dst_uri, dst_storage_info, buf, read_size))) {
    LOG_WARN("failed to write single file", KR(ret), K(dst_uri), K(dst_storage_info));
  }
  return ret;
}

int ObTenantBackupBackupset::upload_pg_list(const share::ObClusterBackupDest& backup_dest,
    const int64_t full_backup_set_id, const int64_t inc_backup_set_id, const int64_t backup_snapshot_version,
    const int64_t compatible, const common::ObIArray<common::ObPGKey>& pg_keys)
{
  int ret = OB_SUCCESS;
  ObExternPGListMgr mgr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (OB_FAIL(mgr.init(tenant_id_,
                 full_backup_set_id,
                 inc_backup_set_id,
                 backup_dest,
                 backup_snapshot_version,
                 compatible,
                 *backup_lease_service_))) {
    LOG_WARN("failed to init extern pg list mgr",
        KR(ret),
        K(backup_dest),
        K(full_backup_set_id),
        K(inc_backup_set_id),
        K(compatible));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pg_keys.count(); ++i) {
      const ObPGKey& pg_key = pg_keys.at(i);
      if (OB_FAIL(mgr.add_pg_key(pg_key))) {
        LOG_WARN("failed to add pg key to extern pg list mgr", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(mgr.upload_pg_list())) {
        LOG_WARN("failed to upload pg list", KR(ret));
      }
    }
  }
  return ret;
}

int ObTenantBackupBackupset::transfer_pg_list(const share::ObClusterBackupDest& src,
    const share::ObClusterBackupDest& dst, const int64_t full_backup_set_id, const int64_t inc_backup_set_id,
    const int64_t backup_date, const int64_t compatible)
{
  int ret = OB_SUCCESS;
  ObArray<ObPartitionKey> pg_keys;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (!src.is_valid() || !dst.is_valid() || src.is_same(dst)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cluster backup dest is not valid", KR(ret), K(src), K(dst));
  } else if (OB_FAIL(fetch_all_pg_list(src, full_backup_set_id, inc_backup_set_id, backup_date, compatible, pg_keys))) {
    LOG_WARN("failed to fetch all pg list", KR(ret), K(src), K(full_backup_set_id), K(inc_backup_set_id));
  } else if (OB_FAIL(upload_pg_list(dst, full_backup_set_id, inc_backup_set_id, backup_date, compatible, pg_keys))) {
    LOG_WARN("failed to upload pg list", KR(ret), K(dst), K(full_backup_set_id), K(inc_backup_set_id));
  }
  return ret;
}

int ObTenantBackupBackupset::transfer_tenant_locality_info(const share::ObClusterBackupDest& src,
    const share::ObClusterBackupDest& dst, const int64_t full_backup_set_id, const int64_t inc_backup_set_id,
    const int64_t backup_date, const int64_t compatible)
{
  int ret = OB_SUCCESS;
  ObExternTenantLocalityInfo info;
  ObExternTenantLocalityInfoMgr src_mgr;
  ObExternTenantLocalityInfoMgr dst_mgr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (!src.is_valid() || !dst.is_valid() || src.is_same(dst) || backup_date <= 0 ||
             compatible < OB_BACKUP_COMPATIBLE_VERSION_V1 || compatible > OB_BACKUP_COMPATIBLE_VERSION_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cluster backup dest is not valid", KR(ret), K(src), K(dst));
  } else if (OB_FAIL(src_mgr.init(tenant_id_,
                 full_backup_set_id,
                 inc_backup_set_id,
                 src,
                 backup_date,
                 compatible,
                 *backup_lease_service_))) {
    LOG_WARN("failed to init extern tenant locality info mgr", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(dst_mgr.init(tenant_id_,
                 full_backup_set_id,
                 inc_backup_set_id,
                 dst,
                 backup_date,
                 compatible,
                 *backup_lease_service_))) {
    LOG_WARN("failed to init extern tenant locality info mgr", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(src_mgr.get_extern_tenant_locality_info(info))) {
    LOG_WARN("failed to get extern tenant locality info", KR(ret));
  } else if (OB_FAIL(dst_mgr.upload_tenant_locality_info(info))) {
    LOG_WARN("failed to upload tenant locality info", KR(ret), K(info));
  }
  return ret;
}

int ObTenantBackupBackupset::do_backup_set_info(const share::ObClusterBackupDest& src,
    const share::ObClusterBackupDest& dst, const int64_t full_backup_set_id, const int64_t inc_backup_set_id,
    const int64_t backup_date, const int64_t compatible)
{
  int ret = OB_SUCCESS;
  bool dst_exist = false;
  ObExternBackupSetInfoMgr src_mgr;
  ObExternBackupSetInfoMgr dst_mgr;
  ObArray<ObExternBackupSetInfo> src_infos;
  ObArray<ObExternBackupSetInfo> dst_infos;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (OB_FAIL(src_mgr.init(tenant_id_,
                 full_backup_set_id,
                 inc_backup_set_id,
                 src,
                 backup_date,
                 compatible,
                 *backup_lease_service_))) {
    LOG_WARN("failed to init extern backup set info mgr", KR(ret), K(tenant_id_), K(src));
  } else if (OB_FAIL(dst_mgr.init(tenant_id_,
                 full_backup_set_id,
                 inc_backup_set_id,
                 dst,
                 backup_date,
                 compatible,
                 *backup_lease_service_))) {
    LOG_WARN("failed to init extern backup set info mgr", KR(ret), K(tenant_id_), K(dst));
  } else if (OB_FAIL(src_mgr.get_extern_backup_set_infos(src_infos))) {
    LOG_WARN("failed to get extern backup set infos", KR(ret), K(tenant_id_), K(src));
  } else if (OB_FAIL(dst_mgr.get_extern_backup_set_infos(dst_infos))) {
    LOG_WARN("failed to get extern backup set infos", KR(ret), K(tenant_id_), K(dst));
  } else if (OB_FAIL(check_backup_info_exist(dst_infos, inc_backup_set_id, dst_exist))) {
    LOG_WARN("failed to check backup info exist", KR(ret));
  } else if (dst_exist) {
    // do nothing
  } else {
    int64_t index = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < src_infos.count(); ++i) {
      if (src_infos.at(i).backup_set_id_ == inc_backup_set_id) {
        index = i;
        break;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(dst_mgr.upload_backup_set_info(src_infos[index]))) {
        LOG_WARN("failed to upload backup set info", KR(ret));
      }
    }
  }
  return ret;
}

int ObTenantBackupBackupset::check_backup_info_exist(
    const common::ObIArray<ObExternBackupSetInfo>& infos, const int64_t backup_set_id, bool& exist)
{
  int ret = OB_SUCCESS;
  exist = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < infos.count(); ++i) {
      const ObExternBackupSetInfo& info = infos.at(i);
      if (backup_set_id == info.backup_set_id_) {
        exist = true;
        break;
      }
    }
  }
  return ret;
}

int ObTenantBackupBackupset::convert_array_to_set(
    const common::ObIArray<common::ObPGKey>& pg_list, hash::ObHashSet<common::ObPGKey>& pg_set)
{
  int ret = OB_SUCCESS;
  pg_set.clear();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pg_list.count(); ++i) {
      const common::ObPGKey& pg_key = pg_list.at(i);
      if (OB_FAIL(pg_set.set_refactored(pg_key))) {
        LOG_WARN("failed to set pg key", KR(ret), K(pg_key));
      }
    }
  }
  return ret;
}

int ObTenantBackupBackupset::check_backup_backupset_task_finished(bool& finished)
{
  int ret = OB_SUCCESS;
  finished = false;
  int64_t finished_count = 0;
  ObTenantBackupBackupsetTaskInfo task_info;
  SimpleBackupBackupsetTenant tenant;
  tenant.tenant_id_ = tenant_id_;
  tenant.is_dropped_ = is_dropped_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (OB_FAIL(get_tenant_task_info(task_info))) {
    LOG_WARN("failed to get tenant task info", KR(ret));
  } else if (OB_FAIL(ObPGBackupBackupsetOperator::get_finished_task_count(tenant,
                 task_info.job_id_,
                 task_info.backup_set_id_,
                 task_info.copy_id_,
                 finished_count,
                 *sql_proxy_))) {
    LOG_WARN("failed to get finished pg task count", KR(ret), K(task_info));
  } else {
    finished = finished_count == task_info.total_pg_count_;
    LOG_INFO("pg backup backupset task status", K(finished_count), K(task_info));
  }
  return ret;
}

int ObTenantBackupBackupset::get_finished_pg_stat(
    const share::ObTenantBackupBackupsetTaskInfo& task_info, ObPGBackupBackupsetTaskStat& stat)
{
  int ret = OB_SUCCESS;
  stat.reset();
  const int64_t copy_id = task_info.copy_id_;
  SimpleBackupBackupsetTenant tenant;
  tenant.tenant_id_ = tenant_id_;
  tenant.is_dropped_ = is_dropped_;
  ObArray<ObPGBackupBackupsetTaskInfo> task_infos;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (OB_FAIL(ObPGBackupBackupsetOperator::get_finished_tasks(
                 tenant, job_id_, backup_set_id_, copy_id, task_infos, *sql_proxy_))) {
    LOG_WARN("failed to get finished tasks", KR(ret), K(tenant_id_), K(backup_set_id_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < task_infos.count(); ++i) {
      const ObPGBackupBackupsetTaskInfo& task_info = task_infos.at(i);
      if (ObPGBackupBackupsetTaskInfo::FINISH != task_info.status_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("status is not finish", KR(ret), K(task_info));
      } else {
        stat.finish_partition_count_ += task_info.finish_partition_count_;
        stat.finish_macro_block_count_ += task_info.finish_macro_block_count_;
      }
    }
  }
  return ret;
}

int ObTenantBackupBackupset::do_extern_data_backup_info(const ObExternBackupInfo::ExternBackupInfoStatus& status,
    const share::ObClusterBackupDest& src, const share::ObClusterBackupDest& dst, const int64_t full_backup_set_id,
    const int64_t inc_backup_set_id)
{
  int ret = OB_SUCCESS;
  bool src_exist = false;
  ObExternBackupInfoMgr src_mgr, dst_mgr;
  ObArray<ObExternBackupInfo> src_extern_backup_infos, dst_extern_backup_infos;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (OB_FAIL(src_mgr.init(tenant_id_, src, *backup_lease_service_))) {
    LOG_WARN("failed to init extern backup info mgr", KR(ret), K(tenant_id_), K(src));
  } else if (OB_FAIL(dst_mgr.init(tenant_id_, dst, *backup_lease_service_))) {
    LOG_WARN("failed to init extern backup info mgr", KR(ret), K(tenant_id_), K(dst));
  } else if (OB_FAIL(src_mgr.get_extern_backup_infos(src_extern_backup_infos))) {
    LOG_WARN("failed to get extern backup infos", KR(ret));
  } else if (OB_FAIL(dst_mgr.get_extern_backup_infos(dst_extern_backup_infos))) {
    LOG_WARN("failed to get extern backup infos", KR(ret));
  } else if (OB_FAIL(
                 check_backup_info_exist(src_extern_backup_infos, full_backup_set_id, inc_backup_set_id, src_exist))) {
    LOG_WARN("failed to check backup info exist", KR(ret));
  } else if (!src_exist) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src backup info should exist", KR(ret), K(full_backup_set_id), K(inc_backup_set_id));
  } else {
    ObExternBackupInfo extern_info;
    for (int64_t i = 0; OB_SUCC(ret) && i < src_extern_backup_infos.count(); ++i) {
      const ObExternBackupInfo& info = src_extern_backup_infos.at(i);
      if (info.full_backup_set_id_ == full_backup_set_id && info.inc_backup_set_id_ == inc_backup_set_id) {
        extern_info = info;
        break;
      }
    }
    if (OB_SUCC(ret)) {
      extern_info.status_ = status;
      if (OB_FAIL(dst_mgr.upload_backup_info(extern_info, true /*search*/))) {
        LOG_WARN("failed to upload backup info", KR(ret), K(extern_info));
      }
    }
  }
  return ret;
}

int ObTenantBackupBackupset::check_backup_info_exist(const common::ObIArray<ObExternBackupInfo>& extern_backup_infos,
    const int64_t full_backup_set_id, const int64_t inc_backup_set_id, bool& exist)
{
  int ret = OB_SUCCESS;
  exist = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (extern_backup_infos.empty()) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < extern_backup_infos.count(); ++i) {
      const ObExternBackupInfo& info = extern_backup_infos.at(i);
      if (info.full_backup_set_id_ == full_backup_set_id && info.inc_backup_set_id_ == inc_backup_set_id) {
        exist = true;
        break;
      }
    }
  }
  return ret;
}

/* ObPartitionBackupBackupset */

ObPartitionBackupBackupset::ObPartitionBackupBackupset()
    : is_inited_(false),
      sql_proxy_(NULL),
      zone_mgr_(NULL),
      server_mgr_(NULL),
      rebalance_task_mgr_(NULL),
      load_balancer_()
{}

ObPartitionBackupBackupset::~ObPartitionBackupBackupset()
{}

int ObPartitionBackupBackupset::init(
    ObMySQLProxy& sql_proxy, ObZoneManager& zone_mgr, ObServerManager& server_mgr, ObRebalanceTaskMgr& task_mgr)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_SUCCESS;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_FAIL(load_balancer_.init(zone_mgr, server_mgr))) {
    LOG_WARN("failed to init load balancer", KR(ret));
  } else {
    sql_proxy_ = &sql_proxy;
    zone_mgr_ = &zone_mgr;
    server_mgr_ = &server_mgr;
    rebalance_task_mgr_ = &task_mgr;
    is_inited_ = true;
  }
  return ret;
}

int ObPartitionBackupBackupset::partition_backup_backupset(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t job_id = -1;
  int64_t backup_set_id = -1;
  ObBackupBackupsetJobInfo job_info;
  ObTenantBackupTaskInfo backup_info;
  ObTenantBackupBackupsetTaskInfo backup_backup_info;
  ObArray<ObPGBackupBackupsetTaskInfo> task_infos;
  SimpleBackupBackupsetTenant tenant;
  tenant.tenant_id_ = tenant_id;
  tenant.is_dropped_ = false;
  bool exists = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (OB_FAIL(get_one_job(job_id))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_DEBUG("no job to do", KR(ret));
      ret = OB_SUCCESS;
    }
  } else if (OB_SYS_TENANT_ID == tenant_id && OB_FAIL(get_one_tenant_if_sys(job_id, tenant, exists))) {
    LOG_WARN("failed to get one tenant id if sys", KR(ret), K(job_id));
  } else if (!exists) {
    LOG_INFO("no dropped tenant to do");
  } else if (OB_FAIL(get_smallest_unfinished_task(job_id, tenant, backup_set_id))) {
    if (OB_ENTRY_NOT_EXIST == ret || OB_CANCELED == ret) {
      LOG_INFO("no need to do task", KR(ret));
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(get_pending_tasks(tenant, job_id, backup_set_id, task_infos))) {
    LOG_WARN("failed to get pending pg tasks", KR(ret), K(tenant_id));
  } else if (task_infos.empty()) {
    // do nothing
  } else if (OB_FAIL(get_tenant_backup_task(tenant.tenant_id_, backup_set_id, backup_info))) {
    LOG_WARN("failed to get tenant backup task", KR(ret), K(tenant_id), K(backup_set_id));
  } else if (OB_FAIL(get_tenant_backup_backupset_task(tenant, job_id, backup_set_id, backup_backup_info))) {
    LOG_WARN("failed to get tenant backup backupset task", KR(ret), K(tenant_id));
  } else if (OB_FAIL(send_backup_backupset_rpc(tenant, backup_info, backup_backup_info, task_infos))) {
    LOG_WARN("failed to backup backupset", KR(ret), K(tenant), K(backup_info), K(backup_backup_info));
  }
  return ret;
}

int ObPartitionBackupBackupset::get_one_job(int64_t& job_id)
{
  int ret = OB_SUCCESS;
  job_id = -1;
  ObArray<ObBackupBackupsetJobInfo> job_infos;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition backup backupset do not init", KR(ret));
  } else if (OB_FAIL(ObBackupBackupsetOperator::get_all_task_items(job_infos, *sql_proxy_))) {
    LOG_WARN("failed to get all task items", KR(ret));
  } else if (job_infos.empty()) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    job_id = job_infos.at(0).job_id_;
  }
  return ret;
}

int ObPartitionBackupBackupset::get_one_tenant_if_sys(
    const int64_t job_id, SimpleBackupBackupsetTenant& tenant, bool& exists)
{
  int ret = OB_SUCCESS;
  exists = true;
  ObArray<ObTenantBackupBackupsetTaskInfo> tasks;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition backup backupset do not init", KR(ret));
  } else if (OB_FAIL(ObTenantBackupBackupsetOperator::get_sys_unfinished_task_items(job_id, tasks, *sql_proxy_))) {
    LOG_WARN("failed to get sys unfinished task items", KR(ret), K(job_id));
  } else if (tasks.empty()) {
    exists = false;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
      const ObTenantBackupBackupsetTaskInfo& task = tasks.at(i);
      if (OB_SYS_TENANT_ID == task.tenant_id_) {
        continue;
      } else {
        tenant.tenant_id_ = task.tenant_id_;
        tenant.is_dropped_ = true;
        exists = true;
        break;
      }
    }
  }
  return ret;
}

int ObPartitionBackupBackupset::get_smallest_unfinished_task(
    const int64_t job_id, const SimpleBackupBackupsetTenant& tenant, int64_t& backup_set_id)
{
  int ret = OB_SUCCESS;
  backup_set_id = INT64_MAX;
  ObArray<ObTenantBackupBackupsetTaskInfo> task_infos;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition backup backupset do not init", KR(ret));
  } else if (OB_FAIL(
                 ObTenantBackupBackupsetOperator::get_unfinished_task_items(tenant, job_id, task_infos, *sql_proxy_))) {
    LOG_WARN("failed to get unfinished task items", KR(ret), K(tenant), K(job_id));
  } else if (task_infos.empty()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_INFO("all task is finished", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < task_infos.count(); ++i) {
      const ObTenantBackupBackupsetTaskInfo& task_info = task_infos.at(i);
      if (ObTenantBackupBackupsetTaskInfo::CANCEL == task_info.task_status_) {
        ret = OB_CANCELED;
        LOG_WARN("tenant backup backupset task is cancelled", KR(ret), K(task_info));
      } else if (task_info.backup_set_id_ < backup_set_id) {
        backup_set_id = task_info.backup_set_id_;
      }
    }
  }
  return ret;
}

int ObPartitionBackupBackupset::get_tenant_backup_task(
    const uint64_t tenant_id, const int64_t backup_set_id, share::ObTenantBackupTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  ObBackupTaskHistoryUpdater updater;
  const bool for_update = false;
  task_info.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition backup backupset do not init", KR(ret));
  } else if (OB_FAIL(updater.init(*sql_proxy_))) {
    LOG_WARN("failed to init backup task history updater", KR(ret));
  } else if (OB_FAIL(updater.get_original_tenant_backup_task(tenant_id, backup_set_id, for_update, task_info))) {
    LOG_WARN("failed to get tenant back", KR(ret), K(tenant_id), K(backup_set_id));
  }
  return ret;
}

int ObPartitionBackupBackupset::get_tenant_backup_backupset_task(const SimpleBackupBackupsetTenant& tenant,
    const int64_t job_id, const int64_t backup_set_id, share::ObTenantBackupBackupsetTaskInfo& backup_backup_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition backup backupset do not init", KR(ret));
  } else if (OB_FAIL(ObTenantBackupBackupsetOperator::get_task_item(
                 tenant, job_id, backup_set_id, backup_backup_info, *sql_proxy_))) {
    LOG_WARN("failed to get backup backupset task info", KR(ret), K(tenant), K(backup_set_id));
  }
  return ret;
}

int ObPartitionBackupBackupset::get_pending_tasks(const SimpleBackupBackupsetTenant& tenant, const int64_t job_id,
    const int64_t backup_set_id, common::ObIArray<share::ObPGBackupBackupsetTaskInfo>& task_infos)
{
  int ret = OB_SUCCESS;
  int64_t limit = BATCH_TASK_COUNT;
#ifdef ERRSIM
  const int64_t tmp_limit = GCONF.backup_backupset_batch_count;
  if (0 != tmp_limit) {
    limit = tmp_limit;
  }
#endif
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (OB_FAIL(ObPGBackupBackupsetOperator::get_pending_tasks_with_limit(
                 tenant, job_id, backup_set_id, limit, task_infos, *sql_proxy_))) {
    LOG_WARN("failed to get pending pg tasks", KR(ret), K(job_id), K(backup_set_id), K(tenant));
  }
  return ret;
}

int ObPartitionBackupBackupset::send_backup_backupset_rpc(const share::SimpleBackupBackupsetTenant& tenant,
    const share::ObTenantBackupTaskInfo& backup_info, const share::ObTenantBackupBackupsetTaskInfo& backup_backup_info,
    const common::ObIArray<share::ObPGBackupBackupsetTaskInfo>& old_task_infos)
{
  int ret = OB_SUCCESS;
  ObAddr addr;
  ObBackupBackupsetTask task;
  ObArray<rootserver::ObBackupBackupsetTaskInfo> task_infos;
  ObArray<share::ObPGBackupBackupsetTaskInfo> pg_task_infos;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (OB_FAIL(load_balancer_.get_next_server(addr))) {
    LOG_WARN("failed to get next server", KR(ret));
  } else if (OB_FAIL(build_pg_backup_backupset_task_infos(addr, old_task_infos, pg_task_infos))) {
    LOG_WARN("failed to build pg backup backupset task infos", KR(ret));
  } else if (OB_FAIL(
                 build_backup_backupset_task_infos(addr, backup_info, backup_backup_info, pg_task_infos, task_infos))) {
    LOG_WARN("failed to build task infos", KR(ret));
  } else {
    if (OB_FAIL(task.build(task_infos, addr, tenant.is_dropped_, ""))) {
      LOG_WARN("failed to build backup backupset task", KR(ret), K(tenant));
    } else if (OB_FAIL(ObPGBackupBackupsetOperator::batch_report_task(tenant, pg_task_infos, *sql_proxy_))) {
      LOG_WARN("failed to write to inner table", KR(ret), K(tenant));
    } else if (OB_FAIL(rebalance_task_mgr_->add_task(task))) {
      LOG_WARN("failed to add task to rebalance task mgr", KR(ret));
    }
  }
  return ret;
}

int ObPartitionBackupBackupset::build_backup_backupset_arg(const common::ObAddr& addr,
    const share::ObTenantBackupTaskInfo& backup_info, const share::ObTenantBackupBackupsetTaskInfo& backup_backup_info,
    const share::ObPGBackupBackupsetTaskInfo& pg_task_info, share::ObBackupBackupsetArg& arg)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition backup backupset do not init", KR(ret));
  } else if (OB_FAIL(GCONF.cluster.copy(arg.cluster_name_, OB_MAX_CLUSTER_NAME_LENGTH))) {
    LOG_WARN("failed to copy cluster name", KR(ret));
  } else if (OB_FAIL(databuff_printf(arg.src_uri_header_,
                 OB_MAX_URI_HEADER_LENGTH,
                 "%s",
                 backup_backup_info.src_backup_dest_.root_path_))) {
    LOG_WARN("failed to databuff printf", KR(ret), K(backup_backup_info));
  } else if (OB_FAIL(databuff_printf(
                 arg.src_storage_info_, OB_MAX_URI_LENGTH, "%s", backup_backup_info.src_backup_dest_.storage_info_))) {
    LOG_WARN("failed to databuff printf", KR(ret), K(backup_backup_info));
  } else if (OB_FAIL(databuff_printf(arg.dst_uri_header_,
                 OB_MAX_URI_HEADER_LENGTH,
                 "%s",
                 backup_backup_info.dst_backup_dest_.root_path_))) {
    LOG_WARN("failed to databuff printf", KR(ret), K(backup_backup_info));
  } else if (OB_FAIL(databuff_printf(
                 arg.dst_storage_info_, OB_MAX_URI_LENGTH, "%s", backup_backup_info.dst_backup_dest_.storage_info_))) {
    LOG_WARN("failed to databuff printf", KR(ret), K(backup_backup_info));
  } else {
    arg.job_id_ = pg_task_info.job_id_;
    arg.tenant_id_ = pg_task_info.tenant_id_;
    arg.cluster_id_ = GCONF.cluster_id;
    arg.incarnation_ = OB_START_INCARNATION;
    arg.backup_set_id_ = pg_task_info.backup_set_id_;
    arg.copy_id_ = pg_task_info.copy_id_;
    arg.backup_data_version_ = backup_info.backup_data_version_;
    arg.backup_schema_version_ = backup_info.backup_schema_version_;
    arg.prev_full_backup_set_id_ = backup_info.prev_full_backup_set_id_;
    arg.prev_inc_backup_set_id_ = backup_info.prev_inc_backup_set_id_;
    arg.pg_key_ = common::ObPGKey(pg_task_info.table_id_, pg_task_info.partition_id_, 0);
    arg.server_ = addr;
    arg.backup_type_ = backup_info.backup_type_.type_;
    arg.delete_input_ = false;
    arg.cluster_version_ = backup_info.cluster_version_;
    arg.backup_date_ = backup_info.date_;
    arg.compatible_ = backup_info.compatible_;
  }
  return ret;
}

int ObPartitionBackupBackupset::build_backup_backupset_task_infos(const common::ObAddr& addr,
    const share::ObTenantBackupTaskInfo& backup_info, const share::ObTenantBackupBackupsetTaskInfo& backup_backup_info,
    const common::ObIArray<share::ObPGBackupBackupsetTaskInfo>& pg_task_infos,
    common::ObIArray<rootserver::ObBackupBackupsetTaskInfo>& task_infos)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pg_task_infos.count(); ++i) {
      const share::ObPGBackupBackupsetTaskInfo& pg_task_info = pg_task_infos.at(i);
      ObReplicaMember replica_member = ObReplicaMember(addr, ObTimeUtility::current_time(), REPLICA_TYPE_FULL, 0);
      rootserver::ObBackupBackupsetTaskInfo task_info;
      share::ObBackupBackupsetArg backup_backupset_arg;
      if (OB_FAIL(
              build_backup_backupset_arg(addr, backup_info, backup_backup_info, pg_task_info, backup_backupset_arg))) {
        LOG_WARN("failed to build backup backupset arg", KR(ret));
      } else if (OB_FAIL(task_info.build(replica_member, backup_backupset_arg))) {
        LOG_WARN("failed to build backup backupset task info", KR(ret));
      } else if (OB_FAIL(task_infos.push_back(task_info))) {
        LOG_WARN("failed to push back backup backupset task info", KR(ret));
      }
    }
  }
  return ret;
}

int ObPartitionBackupBackupset::build_pg_backup_backupset_task_infos(const common::ObAddr& server,
    const common::ObIArray<share::ObPGBackupBackupsetTaskInfo>& task_infos,
    common::ObIArray<share::ObPGBackupBackupsetTaskInfo>& new_task_infos)
{
  int ret = OB_SUCCESS;
  share::ObTaskId trace_id;
  trace_id.set(*ObCurTraceId::get_trace_id());
  share::ObPGBackupBackupsetTaskInfo new_task_info;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition backup backupset do not init", KR(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("build pg backup backupset task infos get invalid argument", KR(ret), K(server));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < task_infos.count(); ++i) {
      new_task_info.reset();
      const share::ObPGBackupBackupsetTaskInfo& task_info = task_infos.at(i);
      new_task_info = task_info;
      new_task_info.server_ = server;
      new_task_info.trace_id_ = trace_id;
      new_task_info.status_ = ObPGBackupBackupsetTaskInfo::DOING;
      if (OB_FAIL(new_task_infos.push_back(new_task_info))) {
        LOG_WARN("failed to push back pg backup backupset task info", KR(ret));
      }
    }
  }
  return ret;
}

ObBackupBackupHelper::ObBackupBackupHelper()
    : is_inited_(false), src_(), dst_(), tenant_id_(OB_INVALID_ID), backup_lease_service_(NULL)
{}

ObBackupBackupHelper::~ObBackupBackupHelper()
{}

int ObBackupBackupHelper::init(const share::ObClusterBackupDest& src, const share::ObClusterBackupDest& dst,
    const uint64_t tenant_id, share::ObIBackupLeaseService& backup_lease_service)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", KR(ret), K(tenant_id));
  } else {
    is_inited_ = true;
    src_ = src;
    dst_ = dst;
    tenant_id_ = tenant_id;
    backup_lease_service_ = &backup_lease_service;
  }
  return ret;
}

int ObBackupBackupHelper::do_extern_backup_set_file_info(const share::ObBackupSetFileInfo::BackupSetStatus& status,
    const share::ObBackupFileStatus::STATUS& file_status, const int64_t backup_set_id, const int64_t copy_id,
    const int64_t result)
{
  int ret = OB_SUCCESS;
  ObExternBackupSetFileInfoMgr backup_mgr, backup_backup_mgr;
  ObArray<ObBackupSetFileInfo> backup_infos, bb_infos;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backup helper do not init", KR(ret));
  } else if (OB_FAIL(backup_mgr.init(tenant_id_, src_, false /*is_backup_backup*/, *backup_lease_service_))) {
    LOG_WARN("failed to init extern backup set file info mgr", KR(ret), K_(tenant_id), K_(src));
  } else if (OB_FAIL(backup_backup_mgr.init(tenant_id_, src_, true /*is_backup_backup*/, *backup_lease_service_))) {
    LOG_WARN("failed to init extern backup set file info mgr", KR(ret), K_(tenant_id), K_(src));
  } else if (OB_FAIL(backup_mgr.get_backup_set_file_infos(backup_infos))) {
    LOG_WARN("failed to get backup set file infos", KR(ret));
  } else if (OB_FAIL(backup_backup_mgr.get_backup_set_file_infos(bb_infos))) {
    LOG_WARN("failed to get backup set file infos", KR(ret));
  } else {
    bool src_exist = false;
    ObBackupSetFileInfo b_info;
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_infos.count(); ++i) {
      b_info = backup_infos.at(i);
      if (b_info.backup_set_id_ == backup_set_id) {
        src_exist = true;
        break;
      }
    }
    if (!src_exist) {
      ret = OB_ERR_SYS;
      LOG_WARN("do not exist src backup info", K(backup_set_id), K(backup_infos));
    }
    if (OB_SUCC(ret) && src_exist) {
      bool dst_exist = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < bb_infos.count(); ++i) {
        const ObBackupSetFileInfo& bb_info = bb_infos.at(i);
        if (b_info.backup_set_id_ == bb_info.backup_set_id_ && bb_info.copy_id_ == copy_id) {
          dst_exist = true;
          break;
        }
      }
      if (OB_SUCC(ret) && !dst_exist) {
        b_info.copy_id_ = copy_id;
        b_info.status_ = status;
        b_info.file_status_ = ObBackupFileStatus::BACKUP_FILE_COPYING;
        b_info.result_ = result;
        if (OB_FAIL(backup_backup_mgr.add_backup_set_file_info(b_info))) {
          LOG_WARN("failed to add backup set file info", KR(ret), K(b_info));
        }
      }
    }
    if (OB_SUCC(ret)) {
      b_info.copy_id_ = copy_id;
      b_info.status_ = status;
      b_info.file_status_ = file_status;
      b_info.result_ = result;
      char backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
      if (OB_FAIL(dst_.dest_.get_backup_dest_str(backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
        LOG_WARN("failed to get backup dest str", KR(ret));
      } else if (OB_FAIL(b_info.backup_dest_.assign(backup_dest_str))) {
        LOG_WARN("failed to assign backup dest str", KR(ret));
      } else if (OB_FAIL(backup_backup_mgr.update_backup_set_file_info(b_info))) {
        LOG_WARN("failed to update backup set file info", KR(ret), K(b_info));
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(backup_backup_mgr.upload_backup_set_file_info())) {
      LOG_WARN("failed to upload backup set file info", KR(ret));
    }
  }
  return ret;
}

int ObBackupBackupHelper::do_extern_single_backup_set_info(const share::ObBackupSetFileInfo::BackupSetStatus& status,
    const share::ObBackupFileStatus::STATUS& file_status, const int64_t full_backup_set_id,
    const int64_t inc_backup_set_id, const int64_t copy_id, const int64_t backup_date, const int64_t result)
{
  int ret = OB_SUCCESS;
  ObExternSingleBackupSetInfoMgr src_mgr;
  ObExternSingleBackupSetInfoMgr dst_mgr;
  ObBackupSetFileInfo file_info;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backup helper do not init", KR(ret));
  } else if (OB_FAIL(src_mgr.init(
                 tenant_id_, full_backup_set_id, inc_backup_set_id, backup_date, src_, *backup_lease_service_))) {
    LOG_WARN("failed to init src extern backup set info mgr", KR(ret));
  } else if (OB_FAIL(dst_mgr.init(
                 tenant_id_, full_backup_set_id, inc_backup_set_id, backup_date, dst_, *backup_lease_service_))) {
    LOG_WARN("failed to init src extern backup set info mgr", KR(ret));
  } else if (OB_FAIL(src_mgr.get_extern_backup_set_file_info(file_info))) {
    LOG_WARN("failed to get extern backup set file infos", KR(ret));
  } else {
    file_info.copy_id_ = copy_id;
    file_info.status_ = status;
    file_info.file_status_ = file_status;
    file_info.result_ = result;
    char backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = "";
    if (OB_FAIL(dst_.dest_.get_backup_dest_str(backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
      LOG_WARN("failed to get backup dest str", KR(ret));
    } else if (OB_FAIL(file_info.backup_dest_.assign(backup_dest_str))) {
      LOG_WARN("failed to assign backup dest str", KR(ret));
    } else if (OB_FAIL(dst_mgr.upload_backup_set_file_info(file_info))) {
      LOG_WARN("failed to upload backup set file info", KR(ret), K(file_info));
    }
  }
  return ret;
}

int ObBackupBackupHelper::sync_extern_backup_set_file_info()
{
  int ret = OB_SUCCESS;
  ObExternBackupSetFileInfoMgr src_bb_mgr, dst_bb_mgr;
  ObArray<ObBackupSetFileInfo> file_infos;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backup helper do not init", KR(ret));
  } else if (OB_FAIL(src_bb_mgr.init(tenant_id_, src_, true /*is_backup_backup*/, *backup_lease_service_))) {
    LOG_WARN("failed to init extern backup set file info mgr", KR(ret), K_(tenant_id), K_(src));
  } else if (OB_FAIL(dst_bb_mgr.init(tenant_id_, dst_, true /*is_backup_backup*/, *backup_lease_service_))) {
    LOG_WARN("failed to init extern backup set file info mgr", KR(ret), K_(tenant_id), K_(dst));
  } else if (OB_FAIL(src_bb_mgr.get_backup_set_file_infos(file_infos))) {
    LOG_WARN("failed to get backup set file infos", KR(ret));
  } else if (FALSE_IT(dst_bb_mgr.clear())) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < file_infos.count(); ++i) {
      const ObBackupSetFileInfo& info = file_infos.at(i);
      if (OB_FAIL(dst_bb_mgr.add_backup_set_file_info(info))) {
        LOG_WARN("failed to add backup set file info", KR(ret), K(info));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(dst_bb_mgr.upload_backup_set_file_info())) {
        LOG_WARN("failed to upload backup set file info", KR(ret));
      }
    }
  }
  return ret;
}

}  // end namespace rootserver
}  // end namespace oceanbase
