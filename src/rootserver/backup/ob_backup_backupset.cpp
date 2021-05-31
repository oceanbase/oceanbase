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
      } else if (OB_FAIL(add_array(server_list, tmp_server_list))) {
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
      sql_proxy_(NULL),
      zone_mgr_(NULL),
      server_mgr_(NULL),
      root_balancer_(NULL),
      rebalance_task_mgr_(NULL),
      schema_service_(NULL),
      backup_lease_service_(NULL),
      idling_(stop_)
{}

ObBackupBackupset::~ObBackupBackupset()
{}

int ObBackupBackupset::init(ObMySQLProxy& sql_proxy, ObZoneManager& zone_mgr, ObServerManager& server_mgr,
    ObRootBalancer& root_balancer, ObRebalanceTaskMgr& task_mgr,
    share::schema::ObMultiVersionSchemaService& schema_service, share::ObIBackupLeaseService& backup_lease_service)
{
  int ret = OB_SUCCESS;
  const int64_t thread_num = 1;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup backupset init twice", KR(ret));
  } else if (OB_FAIL(create(thread_num, "ObBackupBackupset"))) {
    LOG_WARN("failed to create thread", KR(ret), K(thread_num));
  } else {
    sql_proxy_ = &sql_proxy;
    zone_mgr_ = &zone_mgr;
    server_mgr_ = &server_mgr;
    root_balancer_ = &root_balancer;
    rebalance_task_mgr_ = &task_mgr;
    schema_service_ = &schema_service;
    backup_lease_service_ = &backup_lease_service;
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
      if (OB_SYS_TENANT_ID == tenant_id) {
        continue;
      } else if (OB_FAIL(do_tenant_task(tenant_id, job_id, backup_set_id))) {
        LOG_WARN("failed to do tenant task", KR(ret), K(task_info));
      }
    }
  }
  return ret;
}

int ObBackupBackupset::do_tenant_task(const uint64_t tenant_id, const int64_t job_id, const int64_t backup_set_id)
{
  int ret = OB_SUCCESS;
  ObTenantBackupBackupset tenant_backup_backupset;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(tenant_backup_backupset.init(tenant_id,
                 job_id,
                 backup_set_id,
                 *sql_proxy_,
                 *zone_mgr_,
                 *server_mgr_,
                 *root_balancer_,
                 *rebalance_task_mgr_,
                 *this,
                 *backup_lease_service_))) {
    LOG_WARN("failed to init tenant backup backupset", KR(ret), K(tenant_id));
  } else if (OB_FAIL(tenant_backup_backupset.do_task())) {
    LOG_WARN("failed to do backup backupset task for tenant", KR(ret), K(tenant_id));
  }

  return ret;
}

int ObBackupBackupset::get_all_backup_set_id(
    const uint64_t tenant_id, const share::ObBackupBackupsetJobInfo& job_info, common::ObArray<int64_t>& backup_set_ids)
{
  int ret = OB_SUCCESS;
  ObArray<ObTenantBackupTaskItem> backup_tasks;
  ObArray<ObTenantBackupBackupsetTaskItem> backup_backupset_tasks;
  const ObBackupBackupsetType& type = job_info.type_;
  const int64_t backup_set_id = job_info.backup_set_id_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (!job_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("job info is not valid", KR(ret), K(job_info));
  } else if (OB_FAIL(ObBackupTaskHistoryOperator::get_tenant_backup_tasks(tenant_id, *sql_proxy_, backup_tasks))) {
    LOG_WARN("failed to get tenant backup tasks", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObTenantBackupBackupsetHistoryOperator::get_task_items(
                 tenant_id, OB_START_COPY_ID, backup_backupset_tasks, *sql_proxy_))) {
    LOG_WARN("failed to get tenant backup backupset task history tasks", KR(ret), K(tenant_id));
  } else if (backup_backupset_tasks.count() > backup_tasks.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task count is not valid", KR(ret), K(backup_backupset_tasks.count()), K(backup_tasks.count()));
  } else {
    CompareHistoryBackupTaskItem cmp_backup;
    CompareHistoryBackupBackupsetTaskItem cmp_backup_backupset;
    std::sort(backup_tasks.begin(), backup_tasks.end(), cmp_backup);
    std::sort(backup_backupset_tasks.begin(), backup_backupset_tasks.end(), cmp_backup_backupset);
    switch (type.type_) {
      case ObBackupBackupsetType::ALL_BACKUP_SET: {
        if (OB_FAIL(get_full_backup_set_id_list(backup_backupset_tasks, backup_tasks, backup_set_id, backup_set_ids))) {
          LOG_WARN("failed to get full backup set id list", KR(ret), K(tenant_id), K(backup_set_id));
        }
        break;
      }
      case ObBackupBackupsetType::SINGLE_BACKUP_SET: {
        if (OB_FAIL(
                get_single_backup_set_id_list(backup_backupset_tasks, backup_tasks, backup_set_id, backup_set_ids))) {
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
int ObBackupBackupset::get_full_backup_set_id_list(
    const ObArray<ObTenantBackupBackupsetTaskItem>& backup_backupset_tasks,
    const ObArray<ObTenantBackupTaskItem>& backup_tasks, const int64_t backup_set_id,
    common::ObArray<int64_t>& backup_set_ids)
{
  int ret = OB_SUCCESS;
  backup_set_ids.reset();
  BackupArrayIter iter;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(find_backup_task_lower_bound(backup_tasks, backup_set_id, iter))) {
    LOG_WARN("failed to find backup task lower bound", KR(ret), K(backup_set_id));
  } else if (iter->backup_set_id_ != backup_set_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup set id do not exist", KR(ret), K(backup_set_id), K(*iter));
  } else {
    while (OB_SUCC(ret) && iter >= backup_tasks.begin()) {
      bool exists = false;
      const int64_t tmp_id = iter->backup_set_id_;
      for (int64_t i = 0; OB_SUCC(ret) && i < backup_backupset_tasks.count(); ++i) {
        const ObTenantBackupBackupsetTaskInfo& info = backup_backupset_tasks.at(i);
        if (info.backup_set_id_ == tmp_id) {
          exists = true;
          break;
        }
      }
      if (OB_SUCC(ret) && !exists && !iter->is_mark_deleted_) {
        if (OB_FAIL(backup_set_ids.push_back(tmp_id))) {
          LOG_WARN("failed to push back backup set id", KR(ret), K(tmp_id));
        }
      }
      --iter;
    }
    std::sort(backup_set_ids.begin(), backup_set_ids.end());
  }
  return ret;
}

// 1,2  1Full 2Inc
// 3,4  3Full 4Inc
// 5,6  5Full 6Inc
// The user specifies 6 and outputs 5, 6
// If the user specifies 6 but 5 has been backed up, output 6
int ObBackupBackupset::get_single_backup_set_id_list(
    const ObArray<ObTenantBackupBackupsetTaskItem>& backup_backupset_tasks,
    const ObArray<ObTenantBackupTaskItem>& backup_tasks, const int64_t backup_set_id,
    common::ObArray<int64_t>& backup_set_ids)
{
  int ret = OB_SUCCESS;
  backup_set_ids.reset();
  BackupArrayIter iter;
  ObArray<int64_t> tmp_backup_set_ids;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(find_backup_task_lower_bound(backup_tasks, backup_set_id, iter))) {
    LOG_WARN("failed to find backup task lower bound", KR(ret), K(backup_set_id));
  } else if (iter->backup_set_id_ != backup_set_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup set id do not exist", KR(ret), K(backup_set_id), K(*iter));
  } else if (iter->is_mark_deleted_) {
    LOG_INFO("backup set id is mark deleted, no need backup backupset", KR(ret), K(*iter));
  } else {
    while (OB_SUCC(ret) && iter >= backup_tasks.begin()) {
      if (OB_FAIL(tmp_backup_set_ids.push_back(iter->backup_set_id_))) {
        LOG_WARN("failed to push back backup set id", KR(ret));
      } else {
        if (iter->backup_type_.type_ == ObBackupType::FULL_BACKUP) {
          break;
        } else {
          --iter;
        }
      }
    }
    std::sort(tmp_backup_set_ids.begin(), tmp_backup_set_ids.end());
    // Remove the backup set id that has been backed up
    // TODO use hash map to accelerate
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_backup_set_ids.count(); ++i) {
      const int64_t tmp_id = tmp_backup_set_ids.at(i);
      bool exists = false;
      for (int64_t j = 0; OB_SUCC(ret) && j < backup_backupset_tasks.count(); ++j) {
        const ObTenantBackupBackupsetTaskInfo& info = backup_backupset_tasks.at(j);
        if (info.backup_set_id_ == tmp_id) {
          exists = true;
          break;
        }
      }
      if (OB_SUCC(ret) && !exists) {
        if (OB_FAIL(backup_set_ids.push_back(tmp_id))) {
          LOG_WARN("failed to push back backup set id", KR(ret), K(tmp_id));
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
    } else if (iter != tasks.end() && iter->backup_set_id_ > backup_set_id) {
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

int ObBackupBackupset::get_all_tenant_ids(const int64_t backup_set_id, common::ObIArray<uint64_t>& tenant_ids)
{
  int ret = OB_SUCCESS;
  tenant_ids.reset();
  ObBackupTaskHistoryUpdater updater;
  ObArray<ObTenantBackupTaskInfo> backup_tasks;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(updater.init(*sql_proxy_))) {
    LOG_WARN("failed to init backup task hitory updater", KR(ret));
  } else if (OB_FAIL(updater.get_tenant_backup_tasks(backup_set_id, backup_tasks))) {
    LOG_WARN("failed to get tenant backup tasks of backup_set_id", KR(ret), K(backup_set_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_tasks.count(); ++i) {
      const ObTenantBackupTaskInfo& task_info = backup_tasks.at(i);
      const uint64_t tenant_id = task_info.tenant_id_;
      if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
        LOG_WARN("failed to push back tenant id", KR(ret), K(tenant_id));
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
  ObArray<uint64_t> tenant_ids;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(get_all_tenant_ids(job_info.backup_set_id_, tenant_ids))) {
    LOG_WARN("failed to get all tenant ids", KR(ret), K(job_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
      const uint64_t tenant_id = tenant_ids.at(i);
      ObArray<ObTenantBackupBackupsetTaskInfo> tenant_task_infos;
      if (OB_FAIL(ObTenantBackupBackupsetOperator::get_task_items(
              tenant_id, job_id, OB_START_COPY_ID, tenant_task_infos, *sql_proxy_))) {
        LOG_WARN("failed to get task items", KR(ret), K(job_id), K(tenant_id));
      } else if (OB_FAIL(add_array(task_infos, tenant_task_infos))) {
        LOG_WARN("failed to add array", KR(ret));
      }
    }
  }
  return ret;
}

int ObBackupBackupset::get_current_round_tasks(
    const share::ObBackupBackupsetJobInfo& job_info, common::ObIArray<ObTenantBackupBackupsetTaskInfo>& task_infos)
{
  int ret = OB_SUCCESS;
  task_infos.reset();
  ObArray<uint64_t> tenant_ids;
  const int64_t job_id = job_info.job_id_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(get_all_tenant_ids(job_info.backup_set_id_, tenant_ids))) {
    LOG_WARN("failed to get all tenant ids", KR(ret), K(job_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
      const uint64_t tenant_id = tenant_ids.at(i);
      ObArray<ObTenantBackupBackupsetTaskInfo> tenant_task_infos;
      ObTenantBackupBackupsetTaskInfo smallest_task;
      if (OB_SYS_TENANT_ID == tenant_id) {
        continue;
      } else if (OB_FAIL(ObTenantBackupBackupsetOperator::get_unfinished_task_items(
                     tenant_id, job_id, OB_START_COPY_ID, tenant_task_infos, *sql_proxy_))) {
        LOG_WARN("failed to get task items", KR(ret), K(job_id), K(tenant_id));
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
    int64_t smallest_backup_set_id = INT_MAX64;
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

int ObBackupBackupset::move_job_info_to_history(const int64_t job_id)
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
    } else if (OB_FAIL(ObBackupBackupsetHistoryOperator::insert_job_item(job_info, trans))) {
      LOG_WARN("failed to insert job info to history", KR(ret), K(job_id));
    } else if (OB_FAIL(ObBackupBackupsetOperator::remove_job_item(job_id, trans))) {
      LOG_WARN("failed to remove job item", KR(ret), K(job_id));
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
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(updater.init(*sql_proxy_))) {
    LOG_WARN("failed to init backup task history updater", KR(ret));
  } else if (OB_FAIL(updater.get_tenant_backup_task(OB_SYS_TENANT_ID, backup_set_id, backup_task_info))) {
    LOG_WARN("failed to get tenant backup task info", KR(ret), K(backup_set_id), K(OB_SYS_TENANT_ID));
  } else if (OB_FAIL(get_dst_backup_dest_str(dst_backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
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
          LOG_WARN("failed to do schedule", KR(ret), K(job_info));
        }
        break;
      }
      case ObBackupBackupsetJobInfo::CANCEL: {
        if (OB_FAIL(do_cancel(job_info))) {
          LOG_WARN("failed to do schedule", KR(ret), K(job_info));
        }
        break;
      }
      case ObBackupBackupsetJobInfo::FINISH: {
        if (OB_FAIL(do_finish(job_info))) {
          LOG_WARN("failed to do schedule", KR(ret), K(job_info));
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

int ObBackupBackupset::update_job_info(const int64_t job_id, const share::ObBackupBackupsetJobInfo& job_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(ObBackupBackupsetOperator::report_job_item(job_info, *sql_proxy_))) {
    LOG_WARN("failed to report job item", KR(ret), K(job_id));
  }
  return ret;
}

int ObBackupBackupset::do_schedule(const ObBackupBackupsetJobInfo& job_info)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  const int64_t job_id = job_info.job_id_;
  const int64_t backup_set_id = job_info.backup_set_id_;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (!job_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("job info is not valid", KR(ret), K(job_info));
  } else if (OB_FAIL(get_all_tenant_ids(backup_set_id, tenant_ids))) {
    LOG_WARN("failed to get all tenant ids", KR(ret), K(backup_set_id));
  } else if (OB_FAIL(check_can_do_work())) {
    LOG_WARN("failed to check can do work", KR(ret));
  } else if (OB_FAIL(inner_schedule_tenant_task(job_info, tenant_ids))) {
    LOG_WARN("failed to inner schedule tenant task", KR(ret), K(job_info));
  }

  if (OB_SUCC(ret)) {
    ObBackupBackupsetJobInfo new_job_info = job_info;
    new_job_info.job_status_ = ObBackupBackupsetJobInfo::BACKUP;
    if (OB_FAIL(update_job_info(job_id, new_job_info))) {
      LOG_WARN("failed to update job status", KR(ret), K(job_id), K(job_info));
    } else {
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
    if (OB_FAIL(do_all_tenant_tasks(task_infos))) {
      LOG_WARN("failed to do all tenant tasks", KR(ret));
    } else if (OB_FAIL(check_all_tenant_task_finished(job_info, all_finished))) {
      LOG_WARN("failed to check all tenant task finish", KR(ret), K(job_info));
    }

    if (OB_SUCC(ret) && all_finished) {
      ObBackupBackupsetJobInfo new_job_info = job_info;
      new_job_info.job_status_ = ObBackupBackupsetJobInfo::FINISH;
      if (OB_FAIL(update_job_info(job_id, new_job_info))) {
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

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (!job_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("job info is not valid", KR(ret), K(job_info));
  } else {
    // do something here

    if (OB_SUCC(ret)) {
      ObBackupBackupsetJobInfo new_job_info = job_info;
      new_job_info.job_status_ = ObBackupBackupsetJobInfo::FINISH;
      if (OB_FAIL(update_job_info(job_id, new_job_info))) {
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
    for (int64_t i = 0; OB_SUCC(ret) && i < task_infos.count(); ++i) {
      ObTenantBackupBackupsetTaskInfo& info = task_infos.at(i);
      const uint64_t tenant_id = info.tenant_id_;
      const int64_t job_id = info.job_id_;
      const int64_t backup_set_id = info.backup_set_id_;
      ObTenantBackupBackupsetTaskInfo task_info;
      if (OB_FAIL(do_tenant_task(tenant_id, job_id, backup_set_id))) {
        LOG_WARN("failed to do tenant task", KR(ret), K(job_id), K(tenant_id), K(backup_set_id));
      } else if (OB_FAIL(ObTenantBackupBackupsetOperator::get_task_item(
                     tenant_id, job_id, backup_set_id, OB_START_COPY_ID, task_info, *sql_proxy_))) {
        LOG_WARN("failed to get tenant task info", KR(ret), K(tenant_id), K(job_id));
      } else if (OB_SYS_TENANT_ID != task_info.tenant_id_ &&
                 ObTenantBackupBackupsetTaskInfo::FINISH != task_info.task_status_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant backup backupset task status is not finish", KR(ret), K(task_info));
      } else if (OB_FAIL(construct_sys_tenant_backup_backupset_info(info))) {
        LOG_WARN("failed to construct sys tenant backup backupset info", KR(ret), K(info));
      } else if (OB_FAIL(ObTenantBackupBackupsetHistoryOperator::insert_task_item(task_info, *sql_proxy_))) {
        LOG_WARN("failed to insert task item to history", KR(ret));
      } else if (OB_FAIL(ObTenantBackupBackupsetOperator::remove_task_item(
                     tenant_id, job_id, backup_set_id, OB_START_COPY_ID, *sql_proxy_))) {
        LOG_WARN("failed to remove task item", KR(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(update_cluster_data_backup_info(job_info))) {
        LOG_WARN("failed to update backup meta info", KR(ret));
      } else if (OB_FAIL(move_job_info_to_history(job_info.job_id_))) {
        LOG_WARN("failed to move job info to history", KR(ret), K(job_info));
      } else {
        wakeup();
      }
    }
  }
  return ret;
}

int ObBackupBackupset::inner_schedule_tenant_task(
    const share::ObBackupBackupsetJobInfo& job_info, const common::ObIArray<uint64_t>& tenant_ids)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (!job_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup backupset job info is not valid", KR(ret), K(job_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
      const uint64_t tenant_id = tenant_ids.at(i);
      ObArray<int64_t> backup_set_ids;
      if (OB_FAIL(get_all_backup_set_id(tenant_id, job_info, backup_set_ids))) {
        LOG_WARN("failed to get all backup set id of tenant", KR(ret), K(tenant_id), K(job_info));
      } else if (OB_FAIL(inner_schedule_tenant_backupset_task(job_info, tenant_id, backup_set_ids))) {
        LOG_WARN("faied to inner schedule tenant backupset task", KR(ret), K(job_info), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObBackupBackupset::inner_schedule_tenant_backupset_task(const share::ObBackupBackupsetJobInfo& job_info,
    const uint64_t tenant_id, const common::ObIArray<int64_t>& backup_set_ids)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_set_ids.count(); ++i) {
      bool exist = true;
      const int64_t backup_set_id = backup_set_ids.at(i);
      ObTenantBackupBackupsetTaskInfo task_info;
      if (OB_FAIL(ObTenantBackupBackupsetOperator::get_task_item(
              tenant_id, job_info.job_id_, backup_set_id, OB_START_COPY_ID, task_info, *sql_proxy_))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          LOG_INFO("tenant backup backupset task do not exist", KR(ret), K(tenant_id));
          exist = false;
          ret = OB_SUCCESS;
        }
      }

      if (OB_SUCC(ret) && !exist) {
        if (OB_FAIL(build_tenant_task_info(tenant_id, backup_set_id, job_info, task_info))) {
          LOG_WARN("failed to build tenant task info", KR(ret), K(tenant_id), K(job_info));
        } else if (OB_FAIL(ObTenantBackupBackupsetOperator::insert_task_item(task_info, *sql_proxy_))) {
          LOG_WARN("failed to insert tenant backup backupset task info", KR(ret), K(task_info));
        } else {
          LOG_INFO("inner schedule tenant task success", K(job_info), K(task_info));
        }
      }
    }
  }
  return ret;
}

int ObBackupBackupset::build_tenant_task_info(const uint64_t tenant_id, const int64_t backup_set_id,
    const share::ObBackupBackupsetJobInfo& job_info, share::ObTenantBackupBackupsetTaskInfo& task_info)
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
  } else if (OB_FAIL(get_dst_backup_dest_str(dst_backup_dest_str, OB_MAX_BACKUP_DEST_LENGTH))) {
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
    task_info.copy_id_ = OB_START_COPY_ID;
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
  }
  return ret;
}

int ObBackupBackupset::get_tenant_backup_task_info(
    const uint64_t tenant_id, const int64_t backup_set_id, share::ObTenantBackupTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  ObBackupTaskHistoryUpdater updater;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(updater.init(*sql_proxy_))) {
    LOG_WARN("failed to init backup task history updater", KR(ret));
  } else if (OB_FAIL(updater.get_tenant_backup_task(tenant_id, backup_set_id, task_info))) {
    LOG_WARN("failed to get tenant backup task", KR(ret), K(tenant_id), K(backup_set_id));
  }
  return ret;
}

int ObBackupBackupset::get_dst_backup_dest_str(char* buf, const int64_t buf_size) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup backupset do not init", KR(ret));
  } else if (OB_FAIL(GCONF.backup_backup_dest.copy(buf, buf_size))) {
    LOG_WARN("failed to copy backup backup dest", KR(ret));
  } else {
    LOG_INFO("backup backup dest is", K(buf));
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

/*  ObTenantBackupBackupset */

ObTenantBackupBackupset::ObTenantBackupBackupset()
    : is_inited_(false),
      tenant_id_(OB_INVALID_ID),
      job_id_(-1),
      backup_set_id_(-1),
      sql_proxy_(NULL),
      zone_mgr_(NULL),
      server_mgr_(NULL),
      root_balancer_(NULL),
      rebalance_task_mgr_(NULL),
      backup_backupset_(NULL),
      src_backup_dest_(),
      dst_backup_dest_(),
      backup_lease_service_(NULL)
{}

ObTenantBackupBackupset::~ObTenantBackupBackupset()
{}

int ObTenantBackupBackupset::init(const uint64_t tenant_id, const int64_t job_id, const int64_t backup_set_id,
    ObMySQLProxy& sql_proxy, ObZoneManager& zone_mgr, ObServerManager& server_mgr, ObRootBalancer& root_balancer,
    ObRebalanceTaskMgr& task_mgr, ObBackupBackupset& backup_backupset,
    share::ObIBackupLeaseService& backup_lease_service)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tenant backup backupset init twice", KR(ret));
  } else {
    tenant_id_ = tenant_id;
    job_id_ = job_id;
    backup_set_id_ = backup_set_id;
    sql_proxy_ = &sql_proxy;
    zone_mgr_ = &zone_mgr;
    server_mgr_ = &server_mgr;
    root_balancer_ = &root_balancer;
    rebalance_task_mgr_ = &task_mgr;
    backup_backupset_ = &backup_backupset;
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
      case ObTenantBackupBackupsetTaskInfo::FINISH: {
        if (do_finish(task_info)) {
          LOG_WARN("failed to do finish", KR(ret), K(task_info));
        }
        break;
      }
      case ObTenantBackupBackupsetTaskInfo::CANCEL: {
        if (do_cancel(task_info)) {
          LOG_WARN("failed to do cancel", KR(ret), K(task_info));
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
  } else if (OB_FAIL(backup_dest.set(backup_task_info.backup_dest_, OB_START_INCARNATION))) {
    LOG_WARN("failed to set cluster backup dest", KR(ret), K(task_info));
  } else if (OB_FAIL(fetch_all_pg_list(backup_dest, full_backup_set_id, inc_backup_set_id, pg_key_list))) {
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
  } else if (OB_FAIL(generate_backup_backupset_task(backup_set_id, pg_key_list))) {
    LOG_WARN("failed to generate backup backup task", KR(ret));
  }

  if (OB_SUCC(ret)) {
    ObTenantBackupBackupsetTaskInfo new_task_info = task_info;
    new_task_info.total_pg_count_ = pg_key_list.count();
    new_task_info.total_partition_count_ = total_partition_count;
    new_task_info.total_macro_block_count_ = total_macro_block_count;
    new_task_info.task_status_ =
        meta_file_complete ? ObTenantBackupBackupsetTaskInfo::BACKUP : ObTenantBackupBackupsetTaskInfo::FINISH;
    new_task_info.result_ = meta_file_complete ? OB_SUCCESS : OB_ERR_UNEXPECTED;
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
  } else if (OB_FAIL(check_backup_backupset_task_finished(finished))) {
    LOG_WARN("failed to check backup backupset task finished", KR(ret));
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
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else {
    LOG_INFO("cancel backup backupset task", K(tenant_id_));

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
  ObExternBackupInfo::ExternBackupInfoStatus status =
      OB_SUCCESS == task_info.result_ ? ObExternBackupInfo::SUCCESS : ObExternBackupInfo::FAILED;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (OB_FAIL(get_finished_pg_stat(pg_stat))) {
    LOG_WARN("failed to get finished pg stat", KR(ret));
  } else if (OB_FAIL(get_tenant_backup_task_info(task_info.backup_set_id_, backup_task_info))) {
    LOG_WARN("failed to get tenant backup task info", KR(ret), K(task_info));
  } else if (OB_FAIL(get_backup_set_id_info(backup_task_info, full_backup_set_id, inc_backup_set_id))) {
    LOG_WARN("failed to get backup set id info", KR(ret), K(backup_task_info));
  } else if (OB_FAIL(src.set(task_info.src_backup_dest_, OB_START_INCARNATION))) {
    LOG_WARN("failed to set src cluster backup dest", KR(ret), K(task_info));
  } else if (OB_FAIL(dst.set(task_info.dst_backup_dest_, OB_START_INCARNATION))) {
    LOG_WARN("failed to set src cluster backup dest", KR(ret), K(task_info));
  } else if (OB_FAIL(transfer_pg_list(src, dst, full_backup_set_id, inc_backup_set_id))) {
    LOG_WARN("failed to transfer pg list", KR(ret), K(src), K(dst));
  } else if (OB_FAIL(transfer_tenant_locality_info(src, dst, full_backup_set_id, inc_backup_set_id))) {
    LOG_WARN("failed to transfer tenant locality info", KR(ret), K(src), K(dst));
  } else if (OB_FAIL(do_backup_set_info(src, dst, full_backup_set_id, inc_backup_set_id))) {
    LOG_WARN("failed to do backup set info", KR(ret), K(src), K(dst));
  } else if (OB_FAIL(transfer_backup_set_meta(src, dst, full_backup_set_id, inc_backup_set_id))) {
    LOG_WARN("failed to transfer backup set meta", KR(ret), K(src), K(dst));
  } else if (OB_FAIL(do_extern_data_backup_info(status, src, dst, full_backup_set_id, inc_backup_set_id))) {
    LOG_WARN("failed to do data backup info", KR(ret));
  } else {
    ObTenantBackupBackupsetTaskInfo update_task_info = task_info;
    update_task_info.end_ts_ = ObTimeUtility::current_time();
    update_task_info.finish_pg_count_ = task_info.total_pg_count_;
    update_task_info.finish_partition_count_ = pg_stat.finish_partition_count_;
    update_task_info.finish_macro_block_count_ = pg_stat.finish_macro_block_count_;
    if (OB_FAIL(update_tenant_task_info(update_task_info))) {
      LOG_WARN("failed to update tenant task info", KR(ret), K(update_task_info));
    } else {
      LOG_INFO("tenant backup backupset task finished", K(tenant_id_), K(task_info));
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
  } else if (OB_FAIL(path_info.set(backup_dest, backup_info.tenant_id_, full_backup_set_id, inc_backup_set_id))) {
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
    while (OB_SUCC(ret)) {
      ObBackupMeta backup_meta;
      if (OB_FAIL(meta_store.next(backup_meta))) {
        if (OB_ITER_END == ret) {
          LOG_INFO("iterator ended", K(ret));
        } else {
          LOG_WARN("failed to get next backup meta", KR(ret));
        }
      } else if (backup_meta.partition_group_meta_.partitions_.count() != backup_meta.partition_meta_list_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("backup meta partition count do not match", KR(ret));
      } else {
        const ObPGKey& pg_key = backup_meta.partition_group_meta_.pg_key_;
        if (OB_FAIL(pg_set.erase_refactored(pg_key))) {
          LOG_WARN("failed to erase pg key", KR(ret), K(pg_key));
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
    if (OB_FAIL(ret)) {
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
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (OB_FAIL(updater.init(*sql_proxy_))) {
    LOG_WARN("failed to init backup task history updater", KR(ret));
  } else if (OB_FAIL(updater.get_tenant_backup_task(tenant_id_, backup_set_id, task_info))) {
    LOG_WARN("failed to get tenant backup task", KR(ret), K(tenant_id_));
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
    const int64_t full_backup_set_id, const int64_t inc_backup_set_id, common::ObIArray<common::ObPGKey>& pg_keys)
{
  int ret = OB_SUCCESS;
  pg_keys.reset();
  ObArray<ObPGKey> sys_pg_keys;
  ObArray<ObPGKey> normal_pg_keys;
  ObExternPGListMgr extern_pg_list_mgr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (OB_FAIL(extern_pg_list_mgr.init(
                 tenant_id_, full_backup_set_id, inc_backup_set_id, src_backup_dest, *backup_lease_service_))) {
    LOG_WARN("failed to init extern pg list mgr", KR(ret));
  } else if (OB_FAIL(extern_pg_list_mgr.get_sys_pg_list(sys_pg_keys))) {
    LOG_WARN("failed to get sys pg list", KR(ret));
  } else if (OB_FAIL(extern_pg_list_mgr.get_normal_pg_list(normal_pg_keys))) {
    LOG_WARN("failed to get normal pg list", KR(ret));
  } else if (OB_FAIL(add_array(pg_keys, sys_pg_keys))) {
    LOG_WARN("failed to add sys pg key array", KR(ret));
  } else if (OB_FAIL(add_array(pg_keys, normal_pg_keys))) {
    LOG_WARN("failed to add norml pg key array", KR(ret));
  }
  return ret;
}

int ObTenantBackupBackupset::generate_backup_backupset_task(
    const int64_t backup_set_id, const common::ObIArray<common::ObPGKey>& pg_keys)
{
  int ret = OB_SUCCESS;
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
      if (OB_FAIL(inner_generate_backup_backupset_task(backup_set_id, pg_key, task_info))) {
        LOG_WARN("failed to generate pg backup backupset task", KR(ret), K(backup_set_id), K(pg_key));
      } else if (OB_FAIL(task_infos.push_back(task_info))) {
        LOG_WARN("failed to push back task info", KR(ret), K(task_info));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObPGBackupBackupsetOperator::batch_report_task(task_infos, *sql_proxy_))) {
        LOG_WARN("failed to batch report tasks", KR(ret));
      }
    }
  }
  return ret;
}

int ObTenantBackupBackupset::inner_generate_backup_backupset_task(
    const int64_t backup_set_id, const common::ObPGKey& pg_key, share::ObPGBackupBackupsetTaskInfo& pg_task_info)
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
    pg_task_info.copy_id_ = OB_START_COPY_ID;
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
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (OB_FAIL(ObTenantBackupBackupsetOperator::get_task_item(
                 tenant_id_, job_id_, backup_set_id_, OB_START_COPY_ID, task_info, *sql_proxy_))) {
    LOG_WARN("failed to get task item", KR(ret), K(tenant_id_), K(job_id_), K(backup_set_id_));
  }
  return ret;
}

int ObTenantBackupBackupset::update_tenant_task_info(const share::ObTenantBackupBackupsetTaskInfo& task_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (OB_FAIL(ObTenantBackupBackupsetOperator::report_task_item(task_info, *sql_proxy_))) {
    LOG_WARN("failed to report tenant task info", KR(ret), K(task_info));
  }
  return ret;
}

int ObTenantBackupBackupset::transfer_backup_set_meta(const share::ObClusterBackupDest& src,
    const share::ObClusterBackupDest& dst, const int64_t full_backup_set_id, const int64_t inc_backup_set_id)
{
  int ret = OB_SUCCESS;
  ObBackupPath src_path;
  ObBackupPath dst_path;
  ObStorageUtil util(true);
  ObBackupBaseDataPathInfo src_path_info;
  ObBackupBaseDataPathInfo dst_path_info;
  ObArenaAllocator allocator(ObModIds::BACKUP);
  ObArray<ObString> file_names;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (OB_FAIL(src_path_info.set(src, tenant_id_, full_backup_set_id, inc_backup_set_id))) {
    LOG_WARN("failed to set backup base data path info",
        KR(ret),
        K(src),
        K(tenant_id_),
        K(full_backup_set_id),
        K(inc_backup_set_id));
  } else if (OB_FAIL(dst_path_info.set(dst, tenant_id_, full_backup_set_id, inc_backup_set_id))) {
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
    const int64_t full_backup_set_id, const int64_t inc_backup_set_id, const common::ObIArray<common::ObPGKey>& pg_keys)
{
  int ret = OB_SUCCESS;
  ObExternPGListMgr mgr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (OB_FAIL(
                 mgr.init(tenant_id_, full_backup_set_id, inc_backup_set_id, backup_dest, *backup_lease_service_))) {
    LOG_WARN("failed to init extern pg list mgr", KR(ret), K(backup_dest), K(full_backup_set_id), K(inc_backup_set_id));
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
    const share::ObClusterBackupDest& dst, const int64_t full_backup_set_id, const int64_t inc_backup_set_id)
{
  int ret = OB_SUCCESS;
  ObArray<ObPartitionKey> pg_keys;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (!src.is_valid() || !dst.is_valid() || src.is_same(dst)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cluster backup dest is not valid", KR(ret), K(src), K(dst));
  } else if (OB_FAIL(fetch_all_pg_list(src, full_backup_set_id, inc_backup_set_id, pg_keys))) {
    LOG_WARN("failed to fetch all pg list", KR(ret), K(src), K(full_backup_set_id), K(inc_backup_set_id));
  } else if (OB_FAIL(upload_pg_list(dst, full_backup_set_id, inc_backup_set_id, pg_keys))) {
    LOG_WARN("failed to upload pg list", KR(ret), K(dst), K(full_backup_set_id), K(inc_backup_set_id));
  }
  return ret;
}

int ObTenantBackupBackupset::transfer_tenant_locality_info(const share::ObClusterBackupDest& src,
    const share::ObClusterBackupDest& dst, const int64_t full_backup_set_id, const int64_t inc_backup_set_id)
{
  int ret = OB_SUCCESS;
  ObExternTenantLocalityInfo info;
  ObExternTenantLocalityInfoMgr src_mgr;
  ObExternTenantLocalityInfoMgr dst_mgr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (!src.is_valid() || !dst.is_valid() || src.is_same(dst)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cluster backup dest is not valid", KR(ret), K(src), K(dst));
  } else if (OB_FAIL(src_mgr.init(tenant_id_, full_backup_set_id, inc_backup_set_id, src, *backup_lease_service_))) {
    LOG_WARN("failed to init extern tenant locality info mgr", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(dst_mgr.init(tenant_id_, full_backup_set_id, inc_backup_set_id, dst, *backup_lease_service_))) {
    LOG_WARN("failed to init extern tenant locality info mgr", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(src_mgr.get_extern_tenant_locality_info(info))) {
    LOG_WARN("failed to get extern tenant locality info", KR(ret));
  } else if (OB_FAIL(dst_mgr.upload_tenant_locality_info(info))) {
    LOG_WARN("failed to upload tenant locality info", KR(ret), K(info));
  }
  return ret;
}

int ObTenantBackupBackupset::do_backup_set_info(const share::ObClusterBackupDest& src,
    const share::ObClusterBackupDest& dst, const int64_t full_backup_set_id, const int64_t inc_backup_set_id)
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
  } else if (OB_FAIL(src_mgr.init(tenant_id_, full_backup_set_id, src, *backup_lease_service_))) {
    LOG_WARN("failed to init extern backup set info mgr", KR(ret), K(tenant_id_), K(src));
  } else if (OB_FAIL(dst_mgr.init(tenant_id_, full_backup_set_id, dst, *backup_lease_service_))) {
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
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (OB_FAIL(get_tenant_task_info(task_info))) {
    LOG_WARN("failed to get tenant task info", KR(ret));
  } else if (OB_FAIL(ObPGBackupBackupsetOperator::get_finished_task_count(task_info.tenant_id_,
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

int ObTenantBackupBackupset::get_finished_pg_stat(ObPGBackupBackupsetTaskStat& stat)
{
  int ret = OB_SUCCESS;
  stat.reset();
  int64_t copy_id = OB_START_COPY_ID;
  ObArray<ObPGBackupBackupsetTaskInfo> task_infos;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (OB_FAIL(ObPGBackupBackupsetOperator::get_finished_tasks(
                 tenant_id_, job_id_, backup_set_id_, copy_id, task_infos, *sql_proxy_))) {
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
  int64_t copy_id = OB_START_COPY_ID;
  ObTenantBackupTaskInfo backup_info;
  ObTenantBackupBackupsetTaskInfo backup_backup_info;
  ObArray<ObPGBackupBackupsetTaskInfo> task_infos;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (OB_SYS_TENANT_ID == tenant_id) {
    // do nothing
  } else if (OB_FAIL(get_one_job(job_id))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_DEBUG("no job to do", KR(ret));
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(get_smallest_unfinished_task(job_id, tenant_id, copy_id, backup_set_id))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_INFO("no need to do task", KR(ret));
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(get_pending_tasks(tenant_id, job_id, backup_set_id, copy_id, task_infos))) {
    LOG_WARN("failed to get pending pg tasks", KR(ret), K(tenant_id));
  } else if (task_infos.empty()) {
    // do nothing
  } else if (OB_FAIL(get_tenant_backup_task(tenant_id, backup_set_id, backup_info))) {
    LOG_WARN("failed to get tenant backup task", KR(ret), K(tenant_id), K(backup_set_id));
  } else if (OB_FAIL(get_tenant_backup_backupset_task(
                 tenant_id, job_id, backup_set_id, OB_START_COPY_ID, backup_backup_info))) {
    LOG_WARN("failed to get tenant backup backupset task", KR(ret), K(tenant_id));
  } else if (OB_FAIL(send_backup_backupset_rpc(backup_info, backup_backup_info, task_infos))) {
    LOG_WARN("failed to backup backupset", KR(ret), K(tenant_id), K(backup_info), K(backup_backup_info));
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

int ObPartitionBackupBackupset::get_smallest_unfinished_task(
    const int64_t job_id, const uint64_t tenant_id, const int64_t copy_id, int64_t& backup_set_id)
{
  int ret = OB_SUCCESS;
  backup_set_id = INT_MAX64;
  ObArray<ObTenantBackupBackupsetTaskInfo> task_infos;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition backup backupset do not init", KR(ret));
  } else if (OB_FAIL(ObTenantBackupBackupsetOperator::get_unfinished_task_items(
                 tenant_id, job_id, copy_id, task_infos, *sql_proxy_))) {
    LOG_WARN("failed to get unfinished task items", KR(ret), K(tenant_id), K(job_id), K(copy_id));
  } else if (task_infos.empty()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_INFO("all task is finished", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < task_infos.count(); ++i) {
      const ObTenantBackupBackupsetTaskInfo& task_info = task_infos.at(i);
      if (task_info.backup_set_id_ < backup_set_id) {
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
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition backup backupset do not init", KR(ret));
  } else if (OB_FAIL(updater.init(*sql_proxy_))) {
    LOG_WARN("failed to init backup task history updater", KR(ret));
  } else if (OB_FAIL(updater.get_tenant_backup_task(tenant_id, backup_set_id, task_info))) {
    LOG_WARN("failed to get tenant back", KR(ret), K(tenant_id), K(backup_set_id));
  }
  return ret;
}

int ObPartitionBackupBackupset::get_tenant_backup_backupset_task(const uint64_t tenant_id, const int64_t job_id,
    const int64_t backup_set_id, const int64_t copy_id, share::ObTenantBackupBackupsetTaskInfo& backup_backup_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition backup backupset do not init", KR(ret));
  } else if (OB_FAIL(ObTenantBackupBackupsetOperator::get_task_item(
                 tenant_id, job_id, backup_set_id, copy_id, backup_backup_info, *sql_proxy_))) {
    LOG_WARN("failed to get backup backupset task info", KR(ret), K(tenant_id), K(backup_set_id));
  }
  return ret;
}

int ObPartitionBackupBackupset::get_pending_tasks(const uint64_t tenant_id, const int64_t job_id,
    const int64_t backup_set_id, const int64_t copy_id,
    common::ObIArray<share::ObPGBackupBackupsetTaskInfo>& task_infos)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup backupset do not init", KR(ret));
  } else if (OB_FAIL(ObPGBackupBackupsetOperator::get_pending_tasks(
                 tenant_id, job_id, backup_set_id, copy_id, task_infos, *sql_proxy_))) {
    LOG_WARN("failed to get pending pg tasks", KR(ret), K(job_id), K(backup_set_id), K(tenant_id), K(copy_id));
  }
  return ret;
}

int ObPartitionBackupBackupset::send_backup_backupset_rpc(const share::ObTenantBackupTaskInfo& backup_info,
    const share::ObTenantBackupBackupsetTaskInfo& backup_backup_info,
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
    if (OB_FAIL(task.build(task_infos, addr, ""))) {
      LOG_WARN("failed to build backup backupset task", KR(ret));
    } else if (OB_FAIL(ObPGBackupBackupsetOperator::batch_report_task(pg_task_infos, *sql_proxy_))) {
      LOG_WARN("failed to write to inner table", KR(ret));
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

}  // end namespace rootserver
}  // end namespace oceanbase
