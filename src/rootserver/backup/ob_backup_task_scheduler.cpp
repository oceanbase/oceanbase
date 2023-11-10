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

#include "ob_backup_task_scheduler.h"
#include "ob_backup_service.h"
#include "rootserver/ob_root_service.h"
#include "lib/lock/ob_mutex.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/oblog/ob_log_module.h"
#include "share/ob_rpc_struct.h"
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/ob_zone_table_operation.h"
#include "share/ob_zone_info.h"
#include "share/ob_unit_table_operator.h"
#include "share/backup/ob_backup_server_mgr.h"
#include "share/ob_srv_rpc_proxy.h"

namespace oceanbase

{
using namespace common;
using namespace lib;
using namespace obrpc;
using namespace share;
namespace rootserver
{
ObBackupTaskSchedulerQueue::ObBackupTaskSchedulerQueue()
  : is_inited_(false),
    mutex_(common::ObLatchIds::BACKUP_LOCK),
    max_size_(0),
    tenant_stat_map_("BackUpTMap"),
    server_stat_map_("BackUpSMap"),
    task_allocator_(),
    wait_list_(),
    schedule_list_(),
    task_map_(),
    rpc_proxy_(nullptr),
    task_scheduler_(nullptr),
    sql_proxy_(nullptr)
{
}

ObBackupTaskSchedulerQueue::~ObBackupTaskSchedulerQueue()
{
  reset();
}

void ObBackupTaskSchedulerQueue::reset()
{
  int tmp_ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  while (!wait_list_.is_empty()) {
    ObBackupScheduleTask *t = wait_list_.remove_first();
    if (NULL != t) {
      if (OB_SUCCESS != (tmp_ret = clean_tenant_ref_(t->get_tenant_id()))) {
        LOG_WARN_RET(tmp_ret, "fail to clean tenant ref", K(tmp_ret), KPC(t));
      }
      if (OB_SUCCESS != (tmp_ret = clean_task_map(t->get_task_key()))) {
        LOG_WARN_RET(tmp_ret, "fail to clean task map", K(tmp_ret), KPC(t));
      }
      t->~ObBackupScheduleTask();
      task_allocator_.free(t);
      t = nullptr;
    }
  }
  while (!schedule_list_.is_empty()) {
    ObBackupScheduleTask *t = schedule_list_.remove_first();
    if (NULL != t) {
      if (OB_SUCCESS != (tmp_ret = clean_server_ref_(t->get_dst(), t->get_type()))) {
        LOG_WARN_RET(tmp_ret, "fail to clean server ref", K(tmp_ret), KPC(t));
      } 
      if (OB_SUCCESS != (tmp_ret = clean_tenant_ref_(t->get_tenant_id()))) {
        LOG_WARN_RET(tmp_ret, "fail to clean tenant ref", K(tmp_ret), KPC(t));
      } 
      if (OB_SUCCESS != (tmp_ret = clean_task_map(t->get_task_key()))) {
        LOG_WARN_RET(tmp_ret, "fail to clean task map", K(tmp_ret), KPC(t));
      }
      t->~ObBackupScheduleTask();
      task_allocator_.free(t);
      t = nullptr;
    }
  }
  task_map_.clear();
  tenant_stat_map_.reuse();
  server_stat_map_.reuse();
}

int ObBackupTaskSchedulerQueue::init(
    const int64_t bucket_num, 
    obrpc::ObSrvRpcProxy *rpc_proxy,
    ObBackupTaskScheduler *task_scheduler,
		const int64_t max_size,
    common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  const char *OB_BACKUP_TASK_SCHEDULER = "backupTaskScheduler";
  const ObMemAttr attr(MTL_ID(), OB_BACKUP_TASK_SCHEDULER);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup scheduler queue init twice", K(ret));
  } else if (bucket_num <= 0 || nullptr == rpc_proxy || nullptr == task_scheduler) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(bucket_num), K(rpc_proxy), K(task_scheduler));
  } else if (OB_FAIL(tenant_stat_map_.init(ObBackupTaskScheduler::MAX_BACKUP_TASK_QUEUE_LIMIT))) {
    LOG_WARN("init tenant stat failed", K(ret));
  } else if (OB_FAIL(server_stat_map_.init(ObBackupTaskScheduler::MAX_BACKUP_TASK_QUEUE_LIMIT))) {
    LOG_WARN("init server stat failed", K(ret));
  } else if (OB_FAIL(task_map_.create(bucket_num, attr))) {
    LOG_WARN("fail to init task map", K(ret), K(bucket_num));
  } else if (OB_FAIL(task_allocator_.init(ObMallocAllocator::get_instance(), OB_MALLOC_MIDDLE_BLOCK_SIZE, attr))) {
    LOG_WARN("fail to init task allocator", K(ret));
  } else {
    max_size_ = max_size;
    task_allocator_.set_label(OB_BACKUP_TASK_SCHEDULER);
    rpc_proxy_ = rpc_proxy;
    task_scheduler_ = task_scheduler;
    sql_proxy_ = &sql_proxy;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupTaskSchedulerQueue::push_task(const ObBackupScheduleTask &task)
{
  ObMutexGuard guard(mutex_);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup scheduler queue not inited", K(ret));
  } else if (task_scheduler_->has_set_stop()) {
  } else if (get_task_cnt_() >= max_size_) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("task scheduler queue is full, cant't push task", K(ret), K(get_task_cnt_()));
  } else if (OB_FAIL(check_push_unique_task_(task))) {
    LOG_WARN("fail to check unique task", K(ret), K(task));
  } else {
    void *raw_ptr = nullptr;
    ObBackupScheduleTask *new_task = nullptr;
    const int64_t task_deep_copy_size = task.get_deep_copy_size();
    if (nullptr == (raw_ptr = (task_allocator_.alloc(task_deep_copy_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate task", K(ret), K(task_deep_copy_size));
    } else if (OB_FAIL(task.clone(raw_ptr, new_task))) {
      LOG_WARN("fail to clone new task", K(ret), K(task));
    } else if (OB_UNLIKELY(nullptr == new_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new task ptr is null", K(ret));
    } else if (OB_FAIL(set_tenant_stat_(new_task->get_tenant_id()))) {
      LOG_WARN("fail to set tenant stat", K(ret));
    } else {
      if (ObBackupTaskStatus::Status::DOING == new_task->get_status().status_) {
        if (!schedule_list_.add_last(new_task)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to add task in schedule_list", K(ret), "task_key", new_task->get_task_key());
        } else if (OB_FAIL(set_server_stat_(new_task->get_dst(), new_task->get_type()))) {
          LOG_WARN("failed to set server stat", K(ret), "dst", new_task->get_dst());
        } else if (OB_FAIL(new_task->set_schedule(new_task->get_dst()))) {
          LOG_WARN("fail to set schedule", K(ret), K(new_task));
        } 
      } else if (!wait_list_.add_last(new_task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to add task in wait_list", K(ret), "task_key", new_task->get_task_key());
      } 
      if (OB_SUCC(ret) && OB_FAIL(task_map_.set_refactored(new_task->get_task_key(), new_task))) {
        LOG_WARN("fail to set refacotred a new task in task map", K(ret), "task_key", new_task->get_task_key());
      } else {
        LOG_INFO("succeed add task into backup task scheduler", K(ret), K(new_task));
      }

      if (OB_FAIL(ret)) {
        int tmp_ret = OB_SUCCESS;
        if (new_task->get_dst().is_valid()) {
          schedule_list_.remove(new_task);
          if (OB_SUCCESS != (tmp_ret = clean_server_ref_(new_task->get_dst(), new_task->get_type()))) {
            LOG_ERROR("fail to clean server ref", K(ret), KPC(new_task));
          }
        } else {
          wait_list_.remove(new_task);
        }
        if (OB_SUCCESS != (tmp_ret = clean_tenant_ref_(new_task->get_tenant_id()))) {
          // use tmp_ret to avoid error code coverage
          LOG_ERROR("fail to clean tenant ref", K(tmp_ret), K(new_task));
        }
        if (nullptr != new_task) {
          new_task->~ObBackupScheduleTask();
          new_task = nullptr;
        } 
        if (nullptr != raw_ptr) {
          task_allocator_.free(raw_ptr);
          raw_ptr = nullptr;
        }
      }
    }
  }
  return ret;
}

int ObBackupTaskSchedulerQueue::check_push_unique_task_(const ObBackupScheduleTask &task)
{
  int ret = OB_SUCCESS;
  const ObBackupScheduleTaskKey &task_key = task.get_task_key();
  ObBackupScheduleTask *temp_task = nullptr;
  if (OB_FAIL(task_map_.get_refactored(task_key, temp_task))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to check task in task map", K(ret), K(task_key));
    }
  } else if (OB_UNLIKELY(nullptr == temp_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("temp task ptr is nullptr", K(ret), KP(temp_task));
  } else {
    ret = OB_ENTRY_EXIST;
    LOG_INFO("task already exist", K(task_key), "task", *temp_task);
  }
  return ret;
}

int ObBackupTaskSchedulerQueue::dump_statistics()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup scheduler queue not init", K(ret));
  } else {
    ObMutexGuard guard(mutex_);
    LOG_INFO("backup scheduler task statistics",
      "waiting_task_cnt",
      get_wait_task_cnt_(),
      "executing_task_cnt",
      get_in_schedule_task_cnt_());
    ObServerBackupScheduleTaskStatMap::HashTable::const_iterator sit = server_stat_map_.get_hash_table().begin();
    for (; sit != server_stat_map_.get_hash_table().end(); ++sit) {
      LOG_INFO("server task", "stat", sit->v_);
    }
    ObTenantBackupScheduleTaskStatMap::HashTable::const_iterator tit = tenant_stat_map_.get_hash_table().begin();
    for (; tit != tenant_stat_map_.get_hash_table().end(); ++tit) {
      LOG_INFO("tenant task", "stat", tit->v_);
    }
  }
  return ret;
}

int ObBackupTaskSchedulerQueue::pop_task(ObBackupScheduleTask *&output_task, common::ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObBackupScheduleTask *task = nullptr;
  output_task = nullptr;
  ObArray<ObBackupZone> backup_zone;
  ObArray<ObBackupRegion> backup_region;
  ObArray<ObBackupServer> all_servers;
  ObAddr dst;
  bool can_schedule = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup scheduler queue not init", K(ret));
  } else if (OB_FAIL(get_backup_region_and_zone_(backup_zone, backup_region))) { 
    LOG_WARN("fail to get backup region and zone", K(ret));
  } else if (OB_FAIL(get_all_servers_(backup_zone, backup_region, all_servers))) {
    LOG_WARN("fail to get zone servers", K(ret), K(backup_zone), K(backup_region));
  } else {
    ObMutexGuard guard(mutex_);
    DLIST_FOREACH(t, wait_list_)
    {
      if (!backup_zone.empty() || !backup_region.empty()) {
  // TODO(zeyong): when backup zone and backup region scheme is ready, adjust this code in 4.3
  // only backup ls task need the defensive operation
        ObArray<common::ObAddr> empty_block_server;
        if (!t->can_execute_on_any_server() && BackupJobType::BACKUP_BACKUP_DATA_JOB == t->get_type()) {
          ObBackupDataLSTask *tmp_task = static_cast<ObBackupDataLSTask *>(t);
          tmp_task->set_optional_servers_(empty_block_server);
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(choose_dst_(*t, all_servers, dst, can_schedule))) {
          LOG_WARN("fail to choose servers from backup zone or backup region", K(ret), KPC(t));
        }
      } else {
        if (!t->can_execute_on_any_server()) {
          const ObIArray<ObBackupServer> &optional_servers =t->get_optional_servers();
          if (optional_servers.empty()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("optional servers is empty", K(ret), KPC(t));
          } else if (OB_FAIL(choose_dst_(*t, optional_servers, dst, can_schedule))) {
            LOG_WARN("fail to choose servers from optional servers", K(ret), KPC(t));
          }
        } else {
          if (OB_FAIL(choose_dst_(*t, all_servers, dst, can_schedule))) {
            LOG_WARN("fail to choose servers from all servers", K(ret), KPC(t));
          }
        }
      }
      if (OB_SUCC(ret) && can_schedule) {
        task = t;
        break;
      }
    }

    if (OB_SUCC(ret) && nullptr != task) {
      if (!dst.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error dst", K(ret), KPC(task), K(dst));
      } else if (OB_FAIL(task->set_schedule(dst))) {
        LOG_WARN("fail to set schedule", K(ret), K(dst), KPC(task));
      } else if (OB_FAIL(set_server_stat_(dst, task->get_type()))) {
        LOG_WARN("set server stat faled", K(ret), K(dst));
      } else {
        wait_list_.remove(task);
        if (OB_FAIL(task->update_dst_and_doing_status(*sql_proxy_))) {
          LOG_WARN("fail to update task dst in internal table", K(ret), KPC(task), K(dst));
        } else if (!schedule_list_.add_last(task)) { // This step must be successful
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("fail to add task to schedule list", K(ret), KPC(task));
        } 
        if (OB_FAIL(ret)) {
          int tmp_ret = OB_SUCCESS;
          //if ret != OB_SUCCESS, clean_server_ref_ and add_last must execute
          if (OB_SUCCESS != (tmp_ret = clean_server_ref_(dst, task->get_type()))) {
            LOG_ERROR("fail to clean server ref", K(ret), KPC(task));
          }
          if (!wait_list_.add_last(task)) {
            LOG_ERROR("fail to add task to wait list", KPC(task));
          } 
        }
      }
      if (OB_FAIL(ret)) {
        task->clear_schedule();
        task = nullptr;
      }
    }

    if (OB_FAIL(ret) || OB_ISNULL(task)) {
    } else {
      void *raw_ptr = nullptr;
      const int64_t task_deep_copy_size = task->get_deep_copy_size();
      if (OB_ISNULL(raw_ptr = allocator.alloc(task_deep_copy_size))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate task", K(ret));
      } else if (OB_FAIL(task->clone(raw_ptr, output_task))) {
        LOG_WARN("fail to clone input task", K(ret));
      } else if (OB_ISNULL(output_task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("input task ptr is null", K(ret));
      } else {
        task->set_executor_time(ObTimeUtility::current_time());
      }
      raw_ptr = nullptr;
    }
  }
  return ret;
}

int ObBackupTaskSchedulerQueue::get_backup_region_and_zone_(
    ObIArray<ObBackupZone> &backup_zone,
    ObIArray<ObBackupRegion> &backup_region)
{
  int ret = OB_SUCCESS;
  // TODO(zeyong) redefine backup region and backup zone in 4.3
  return ret;
}

int ObBackupTaskSchedulerQueue::get_all_servers_(
    const ObIArray<ObBackupZone> &backup_zone,
    const ObIArray<ObBackupRegion> &backup_region,
    ObIArray<ObBackupServer> &servers)
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupZone> all_zones;
  share::ObBackupServerMgr server_mgr;
  if (!servers.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(servers));
  } else if (OB_FAIL(get_all_zones_(backup_zone, backup_region, all_zones))) {
    LOG_WARN("failed to get all zones", K(ret), K(backup_zone), K(backup_region));
  } else if (OB_FAIL(server_mgr.init(task_scheduler_->get_exec_tenant_id(), *sql_proxy_))) {
    LOG_WARN("fail to init server operator", K(ret));
  } else {
    const bool force_update = false;
    ObArray<ObAddr> tmp_server_list;
    for (int64_t i = 0; OB_SUCC(ret) && i < all_zones.count(); ++i) {
      tmp_server_list.reuse();
      const ObZone &zone = all_zones.at(i).zone_;
      const int64_t priority = all_zones.at(i).priority_;
      if (OB_FAIL(server_mgr.get_alive_servers(force_update, zone, tmp_server_list))) {
        LOG_WARN("failed to get alive servers", KR(ret), K(zone));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < tmp_server_list.count(); ++j) {
          const ObAddr &server = tmp_server_list.at(j);
          ObBackupServer backup_server;
          if (OB_FAIL(backup_server.set(server, priority))) {
            LOG_WARN("failed to set backup server", K(ret), K(server), K(priority));
          } else if (OB_FAIL(servers.push_back(backup_server))) {
            LOG_WARN("failed to push server", K(ret), K(backup_server));
          }
        }
      }
    }
    LOG_DEBUG("get all alternative servers", K(backup_zone), K(backup_region), K(servers));
  }
  return ret;
}

int ObBackupTaskSchedulerQueue::get_all_zones_(
    const ObIArray<ObBackupZone> &backup_zone,
    const ObIArray<ObBackupRegion> &backup_region,
    ObIArray<ObBackupZone> &zones)
{
  int ret = OB_SUCCESS;
  ObArray<common::ObZone> zone_list;
  ObArray<ObZone> tmp_zones;
  if (!zones.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(zones));
  } else if (!backup_zone.empty()) {
    if (OB_FAIL(append(zones, backup_zone))) {
      LOG_WARN("failed to append backup zone to zones", K(ret), K(backup_zone));
    }
  } else if (!backup_region.empty()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_region.count(); ++i) {
      const ObRegion &region = backup_region.at(i).region_;
      const int64_t priority = backup_region.at(i).priority_;
      if (OB_FAIL(get_zone_list_from_region_(region, tmp_zones))) {
        LOG_WARN("fail to get zones", K(ret), K(region));
      } else {
        for (int j = 0; OB_SUCC(ret) && j < tmp_zones.count(); ++j) {
          const ObZone &zone = tmp_zones.at(j);
          ObBackupZone tmp_zone;
          if (OB_FAIL(tmp_zone.set(zone.str(), priority))) {
            LOG_WARN("failed to set zone", K(ret), K(zone), K(priority));
          } else if (OB_FAIL(zones.push_back(tmp_zone))) {
            LOG_WARN("failed to push zone", K(ret), K(tmp_zone));
          }
        }
      }
    }
  } else {
    if (OB_FAIL(get_tenant_zone_list_(task_scheduler_->get_exec_tenant_id(), tmp_zones))) {
      LOG_WARN("fail to get zone list of tenant", K(ret), "tenant_id", task_scheduler_->get_exec_tenant_id());
    } else {
      // priority = 0 to express all the zone has the same priority
      int64_t priority = 0;
      for (int j = 0; OB_SUCC(ret) && j < tmp_zones.count(); ++j) {
        const ObZone &zone = tmp_zones.at(j);
        ObBackupZone tmp_zone;
        if (OB_FAIL(tmp_zone.set(zone.str(), priority))) {
          LOG_WARN("failed to set zone", K(ret), K(zone));
        } else if (OB_FAIL(zones.push_back(tmp_zone))) {
          LOG_WARN("failed to push zone", K(ret), K(tmp_zone));
        }
      }
    }
  } 
  return ret;
}

int ObBackupTaskSchedulerQueue::get_zone_list_from_region_(const common::ObRegion &region, ObIArray<ObZone> &zone_list)
{
  int ret = OB_SUCCESS;
  ObArray<common::ObZone> tmp_zone_list;
  common::ObRegion tmp_region;
  if (region.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("region is empty", K(region), K(ret));
  } else if (OB_FAIL(get_tenant_zone_list_(task_scheduler_->get_exec_tenant_id(), tmp_zone_list))) {
    LOG_WARN("fail to get zone list of tenant", K(ret), "tenant_id", task_scheduler_->get_exec_tenant_id());
  }
  SMART_VAR(share::ObZoneInfo, info) {
    ARRAY_FOREACH_X(tmp_zone_list, i, cur, OB_SUCC(ret)) {
      const common::ObZone &zone = tmp_zone_list.at(i);
      info.reset();
      info.zone_ = zone;
      if (OB_FAIL(share::ObZoneTableOperation::load_zone_info(*sql_proxy_, info))) {
        LOG_WARN("fail to load zone info", K(ret));
      } else if (!info.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid zone info", K(ret), K(info));
      } else if (OB_FAIL(info.get_region(tmp_region))) {
        LOG_WARN("fail to get region", K(ret));
      } else if (region == tmp_region) {
        if (OB_FAIL(zone_list.push_back(zone))) {
          LOG_WARN("fail to push backup zone", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObBackupTaskSchedulerQueue::get_tenant_zone_list_(const uint64_t tenant_id, ObIArray<common::ObZone> &zone_list)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObUnit> units;
  share::ObUnitTableOperator unit_op;
  if (OB_FAIL(unit_op.init(*sql_proxy_))) {
    LOG_WARN("fail to init unit table operator", K(ret));
  } else if (OB_FAIL(unit_op.get_units_by_tenant(tenant_id, units))) {
    LOG_WARN("fail to get units by tenant", K(ret), K(tenant_id));
  } else if (units.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("units must not be empty", K(ret), K(tenant_id));
  }
  ARRAY_FOREACH_X(units, i, cur, OB_SUCC(ret)) {
    const share::ObUnit &unit = units.at(i);
    if (OB_FAIL(zone_list.push_back(unit.zone_))) {
      LOG_WARN("fail to push back zone", K(ret), K(unit));
    }
  }
  return ret;
}

int ObBackupTaskSchedulerQueue::choose_dst_(
    const ObBackupScheduleTask &task, 
    const ObIArray<ObBackupServer> &servers,
    ObAddr &dst,
    bool &can_schedule)
{
  int ret = OB_SUCCESS;
  can_schedule = false;
  dst.reset();
  ObArray<ObAddr> alternative_servers;
  if(servers.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("servers is empty", K(ret), K(servers));
  } else if (OB_FAIL(get_alternative_servers_(task, servers, alternative_servers))) {
    LOG_WARN("failed to get alternative servers", K(ret), K(task), K(alternative_servers));
  } else if (alternative_servers.empty()) { // servers are busy, wait for next turn
  } else if (OB_FAIL(get_dst_(alternative_servers, dst))) {
    LOG_WARN("failed to get a dst", K(ret), K(alternative_servers));
  } else if (!dst.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dst is error", K(ret), K(dst), K(task));
  } else {
    can_schedule = true;
  }
  return ret;
}

int ObBackupTaskSchedulerQueue::get_alternative_servers_(    
    const ObBackupScheduleTask &task, 
    const ObIArray<ObBackupServer> &servers,
    ObArray<ObAddr> &alternative_servers) 
{
  int ret = OB_SUCCESS;
  int64_t tmp_priority = OB_INVALID_ID;
  if (servers.empty() || !alternative_servers.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task), K(servers), K(alternative_servers));
  } else if (!task.can_execute_on_any_server()) {
    bool has_intersection = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < servers.count(); ++i) {
      const ObAddr &server = servers.at(i).server_;
      const int64_t &priority = servers.at(i).priority_;
      for (int64_t j = 0; OB_SUCC(ret) && j < task.get_optional_servers().count(); ++j) {
        const ObAddr &task_server = task.get_optional_servers().at(j).server_;
        if (server == task_server) {
          has_intersection = true;
          bool can_dst = false;
          if (OB_FAIL(check_server_can_become_dst_(server, task.get_type(), can_dst))) {
            LOG_WARN("fail to check server can become dst", K(ret), K(server));
          } else if (!can_dst) {
          } else if (OB_INVALID_ID == tmp_priority || tmp_priority == priority) {
            if (OB_FAIL(alternative_servers.push_back(server))) {
              LOG_WARN("failed to push server into choosed server list", K(ret), K(server));
            } else {
              tmp_priority = priority;
              break;
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && !has_intersection && REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("alternative servers array is empty", K(ret), K(alternative_servers));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < servers.count(); ++i) {
      const ObAddr &server = servers.at(i).server_;
      const int64_t &priority = servers.at(i).priority_;
      bool can_dst = false;
      if (OB_FAIL(check_server_can_become_dst_(server, task.get_type(), can_dst))) {
        LOG_WARN("fail to check server can become dst", K(ret), K(server));
      } else if (!can_dst) {
      } else if (OB_INVALID_ID == tmp_priority || tmp_priority == priority) {
        if (OB_FAIL(alternative_servers.push_back(server))) {
          LOG_WARN("failed to push server into choosed server list", K(ret), K(server));
        } else {
          tmp_priority = priority;
        }
      } 
    }
  }
  return ret;
}

int ObBackupTaskSchedulerQueue::get_dst_(const ObIArray<ObAddr> &alternative_servers, ObAddr &dst) 
{
  int ret = OB_SUCCESS;
  if (alternative_servers.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alternative servers is empty", K(ret), K(alternative_servers));
  } else {
    int server_count = alternative_servers.count();
    ObRandom random_;
    int64_t res = random_.get(0,server_count -1);
    if (res < 0 || res >= server_count) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected res", K(ret), K(res));
    } else {
      dst = alternative_servers.at(res);
    }
  }
  return ret;
}

int ObBackupTaskSchedulerQueue::check_server_can_become_dst_(
	  const ObAddr &server, 
    const BackupJobType &type,
    bool &can_dst)
{
  int ret = OB_SUCCESS;
  can_dst = false;
  ObBackupServerStatKey key;
  ObServerBackupScheduleTaskStatMap::Item *server_stat = nullptr;
  if (OB_FAIL(key.init(server, type))) {
    LOG_WARN("fail to init ObBackupServerStatKey", K(ret));
  } else if (OB_FAIL(server_stat_map_.locate(key, server_stat))) {
    LOG_WARN("fail to get server stat item", K(ret), K(key));
  } else if (OB_UNLIKELY(nullptr == server_stat)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null tenant stat ptr", K(ret), KP(server_stat));
  } else if (server_stat->v_.in_schedule_task_cnt_ >= ObBackupTaskScheduler::BACKUP_TASK_CONCURRENCY) {
    if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
      LOG_INFO("server is busy, task can't schedule on the server", K(server));
    }
  } else {
    can_dst = true;
  }
  return ret;
}

int ObBackupTaskSchedulerQueue::set_server_stat_(const ObAddr &dst, const BackupJobType &type)
{
  int ret = OB_SUCCESS;
  ObBackupServerStatKey key;
  if (OB_UNLIKELY(!dst.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dst));
  } else if (OB_FAIL(key.init(dst, type))) {
    LOG_WARN("fail to init ObBackupServerStatKey", K(ret), K(dst), K(type));
  } else {
    ObServerBackupScheduleTaskStatMap::Item *server_stat = NULL;
    if (OB_FAIL(server_stat_map_.locate(key, server_stat))) {
      LOG_WARN("fail to locate server stat", K(ret), K(key));
    } else if (OB_UNLIKELY(nullptr == server_stat)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("server stat is null", K(ret), KP(server_stat));
    } else {
      server_stat->ref();
      server_stat->v_.in_schedule_task_cnt_++;
      LOG_INFO("server in schedule task cnt increase", K(key));
    }
  }
  return ret;
}

int ObBackupTaskSchedulerQueue::set_tenant_stat_(const int64_t &tenant_id) 
{
  int ret = OB_SUCCESS;
  if (tenant_id == OB_INVALID_TENANT_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    ObTenantBackupScheduleTaskStatMap::Item *tenant_stat = NULL;
    if (OB_FAIL(tenant_stat_map_.locate(tenant_id, tenant_stat))) {
      LOG_WARN("fail to locate tenant stat", K(ret), K(tenant_id));
    } else if (OB_UNLIKELY(nullptr == tenant_stat)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant stat is null", K(ret), KP(tenant_stat));
    } else {
      tenant_stat->ref();
      tenant_stat->v_.task_cnt_++;
      LOG_INFO("tenant stat task cnt increase", K(tenant_id));
    }
  }
  return ret;
}

int ObBackupTaskSchedulerQueue::clean_server_ref_(const ObAddr &dst, const BackupJobType &type)
{
  int ret = OB_SUCCESS;
  ObBackupServerStatKey key;
  if (OB_UNLIKELY(!dst.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dst));
  } else if (OB_FAIL(key.init(dst, type))) {
    LOG_WARN("fail to init ObBackupServerStatKey", K(ret));
  } else {
    ObServerBackupScheduleTaskStatMap::Item *server_stat = NULL;
    if (OB_FAIL(server_stat_map_.locate(key, server_stat))) {
      LOG_ERROR("fail to locate server stat", K(ret), K(key));
    } else if (OB_UNLIKELY(nullptr == server_stat)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("server stat is null", K(ret), KP(server_stat));
    } else {
      server_stat->v_.in_schedule_task_cnt_--;
      server_stat->unref();
      LOG_INFO("server task cnt decrease", K(key));
    }
  }
  return ret;
}

int ObBackupTaskSchedulerQueue::clean_tenant_ref_(const int64_t &tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    ObTenantBackupScheduleTaskStatMap::Item *tenant_stat = NULL;
    if (OB_FAIL(tenant_stat_map_.locate(tenant_id, tenant_stat))) {
      LOG_ERROR("fail to locate tenant stat", K(ret), K(tenant_id));
    } else if (nullptr == tenant_stat) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant stat is null", K(ret), KP(tenant_stat));
    } else {
      tenant_stat->v_.task_cnt_--;
      tenant_stat->unref();
      LOG_INFO("tenant task cnt decrease", K(tenant_id));
    }
  }
  return ret;
}

int ObBackupTaskSchedulerQueue::clean_task_map(const ObBackupScheduleTaskKey &task_key) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(task_map_.erase_refactored(task_key))) {
    if (ret == OB_HASH_NOT_EXIST) {
      LOG_WARN("remove a non-existent task", K(ret));
    } else {
      LOG_ERROR("fail to remove task from map", K(ret));
    }
  }
  return ret;
}

int ObBackupTaskSchedulerQueue::execute_over(const ObBackupScheduleTask &task, const share::ObHAResultInfo &result_info)
{
  int ret = OB_SUCCESS;
  ObBackupScheduleTask *tmp_task = nullptr;
  ObIBackupJobScheduler *job = nullptr;
  ObAddr dst = task.get_dst();
  bool can_remove = false;
  bool in_schedule = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!dst.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task's dst is not valid", K(ret), K(task));
	} else if (OB_FAIL(task_scheduler_->get_backup_job(task.get_type(), job))) {
    LOG_WARN("failed to get backup service", K(ret));
  } else if (nullptr == job) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("job is nullptr", K(ret), K(task));
  } else {
    ObMutexGuard guard(mutex_);
    if (task_scheduler_->has_set_stop()) {
    } else if (OB_FAIL(get_schedule_task_(task, tmp_task))) { // if task not exist in map ,tmp_task = nullptr
      LOG_WARN("get schedule task failed", K(ret));
    } else if (nullptr == tmp_task) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("in schedule list task not found", K(task));
    } else if (OB_FAIL(job->handle_execute_over(tmp_task, result_info, can_remove))) {
      LOG_WARN("failed to handle execute over", K(ret), K(task));
    } else if (!can_remove) {
    } else if (OB_FAIL(remove_task_(tmp_task, in_schedule))) {
      LOG_WARN("remove task failed", K(ret), K(task));
    }
  }
  return ret;
}

int ObBackupTaskSchedulerQueue::reload_task(const ObArray<ObBackupScheduleTask *> &need_reload_tasks)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup scheduler queue not inited", K(ret));
  } else if (need_reload_tasks.empty()) {
  } else {
    for (int j = 0; OB_SUCC(ret) && j < need_reload_tasks.count(); ++j) {
      ObBackupScheduleTask *task = need_reload_tasks.at(j);
      if (nullptr == task) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("nullptr task", K(ret));
      } else if (OB_FAIL(push_task(*task))) {
        if (OB_ENTRY_EXIST == ret) {
          ret = OB_SUCCESS;
          LOG_INFO("task exist, no need to reload", KPC(task));
        } else {
          LOG_WARN("failed to push task", K(ret));
        }
      } 
    }
  } 
  return ret;
}

int ObBackupTaskSchedulerQueue::cancel_tasks(
    const BackupJobType &type, 
    const uint64_t job_id, 
    const uint64_t tenant_id) 
{
  ObMutexGuard guard(mutex_);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup scheduler queue not inited", K(ret)); 
  } else {
    ObBackupScheduleTask *tmp_task = nullptr;
    bool in_schedule = false;
    DLIST_FOREACH_REMOVESAFE(t, wait_list_) {
      if (nullptr == t) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task is nullptr", K(ret), KPC(t));
      } else if (t->get_type() == type && t->get_job_id() == job_id && t->get_tenant_id() == tenant_id) {
        in_schedule = false;
        if (OB_FAIL(get_wait_task_(*t, tmp_task))) {
          LOG_WARN("fail to get task from wait list", K(ret), KPC(t));
        } else if (nullptr == tmp_task) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("task not in wait list", K(ret), KPC(t));
        } else if (OB_FAIL(remove_task_(tmp_task, in_schedule))) {
          LOG_WARN("remove task failed", K(ret), KPC(tmp_task));
        }
      }
    }
    DLIST_FOREACH_REMOVESAFE(t, schedule_list_) {
      if (nullptr == t) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task is nullptr", K(ret), KPC(t));
      } else if (t->get_type() == type && t->get_job_id() == job_id && t->get_tenant_id() == tenant_id) {
        in_schedule = true;
        if (OB_FAIL(get_schedule_task_(*t, tmp_task))) {
          LOG_WARN("fail to get task from wait list", K(ret), KPC(t));
        } else if (nullptr == tmp_task) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("task not in schedule list", K(ret), KPC(t));
        } else if (OB_FAIL(t->cancel(*rpc_proxy_))) {
          LOG_WARN("fail to cancel task in server", K(ret), KPC(t));
        } else if(OB_FAIL(remove_task_(tmp_task, in_schedule))) {
          LOG_WARN("remove task failed", K(ret), KPC(tmp_task));
        }
      }
    }
  }
  return ret;
}

int ObBackupTaskSchedulerQueue::cancel_tasks(
    const BackupJobType &type, 
    const uint64_t tenant_id) 
{
  ObMutexGuard guard(mutex_);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup scheduler queue not inited", K(ret)); 
  } else {
    ObBackupScheduleTask *tmp_task = nullptr;
    bool in_schedule = false;
    DLIST_FOREACH_REMOVESAFE(t, wait_list_) {
      if (nullptr == t) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task is nullptr", K(ret), KPC(t));
      } else if (t->get_type() == type && t->get_tenant_id() == tenant_id) {
        in_schedule = false;
        if (OB_FAIL(get_wait_task_(*t, tmp_task))) {
          LOG_WARN("fail to get task from wait list", K(ret), KPC(t));
        } else if (nullptr == tmp_task) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("task not in wait list", K(ret), KPC(t));
        } else if (OB_FAIL(remove_task_(tmp_task, in_schedule))) {
          LOG_WARN("remove task failed", K(ret), KPC(tmp_task));
        }
      }
    }
    DLIST_FOREACH_REMOVESAFE(t, schedule_list_) {
      if (nullptr == t) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task is nullptr", K(ret), KPC(t));
      } else if (t->get_type() == type && t->get_tenant_id() == tenant_id) {
        in_schedule = true;
        if (OB_FAIL(get_schedule_task_(*t, tmp_task))) {
          LOG_WARN("fail to get task from wait list", K(ret), KPC(t));
        } else if (nullptr == tmp_task) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("task not in schedule list", K(ret), KPC(t));
        } else if (OB_FAIL(t->cancel(*rpc_proxy_))) {
          LOG_WARN("fail to cancel task in server", K(ret), KPC(t));
        } else if(OB_FAIL(remove_task_(tmp_task, in_schedule))) {
          LOG_WARN("remove task failed", K(ret), KPC(tmp_task));
        }
      }
    }
  }
  return ret;
}

int ObBackupTaskSchedulerQueue::get_all_tasks(
    ObIAllocator &allocator, 
    ObIArray<ObBackupScheduleTask *> &tasks)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    LOG_WARN("queue not init", K(ret));
  } else if (!tasks.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schedule_tasks not empty", K(ret));
  } else {
    ObMutexGuard guard(mutex_);
    DLIST_FOREACH_X(t, schedule_list_, OB_SUCC(ret)) {
      void *raw_ptr = nullptr;
      ObBackupScheduleTask *task = nullptr;
      if (nullptr == (raw_ptr = allocator.alloc(t->get_deep_copy_size()))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate task", K(ret));
      } else if (OB_FAIL(t->clone(raw_ptr, task))) {
        LOG_WARN("fail to clone input task", K(ret));
      } else if (nullptr == task) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("input task ptr is null", K(ret));
      } else if (OB_FAIL(tasks.push_back(task))) {
        LOG_WARN("push back fail", K(ret));
      }
    }
    DLIST_FOREACH_X(t, wait_list_, OB_SUCC(ret)) {
      void *raw_ptr = nullptr;
      ObBackupScheduleTask *task = nullptr;
      if (nullptr == (raw_ptr = allocator.alloc(t->get_deep_copy_size()))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate task", K(ret));
      } else if (OB_FAIL(t->clone(raw_ptr, task))) {
        LOG_WARN("fail to clone input task", K(ret));
      } else if (nullptr == task) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("input task ptr is null", K(ret));
      } else if (OB_FAIL(tasks.push_back(task))) {
        LOG_WARN("push back fail", K(ret));
      }
    }
  }
  return ret;
}

int ObBackupTaskSchedulerQueue::check_task_exist(const ObBackupScheduleTaskKey key, bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  ObBackupScheduleTask *temp_task = nullptr;
  if (OB_FAIL(task_map_.get_refactored(key, temp_task))) {
    if (OB_HASH_NOT_EXIST == ret) {
      is_exist = false;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to check task in task map", K(ret), K(key));
    }
  } else if (OB_UNLIKELY(nullptr == temp_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("temp task ptr is nullptr", K(ret), KP(temp_task));
  } else {
    is_exist = true;
  }
  return ret;
}

int ObBackupTaskSchedulerQueue::get_schedule_task_(
    const ObBackupScheduleTask &input_task, 
    ObBackupScheduleTask *&task)
{
  int ret = OB_SUCCESS;
  task = nullptr;
  const ObBackupScheduleTaskKey &task_key = input_task.get_task_key();
  ObBackupScheduleTask *sample_task = nullptr;
  if (OB_FAIL(task_map_.get_refactored(task_key, sample_task))) {
    if (OB_HASH_NOT_EXIST == ret) {
      task = nullptr;
    } else {
      LOG_WARN("get task from map failed", K(ret), K(input_task));
    }
  } else if (OB_UNLIKELY(nullptr == sample_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null task from map", K(ret));
  } else if (!sample_task->in_schedule()) { 
    task = nullptr;
  } else if (sample_task->get_dst() != input_task.get_dst()) {
    task = nullptr;
  } else if (sample_task->get_task_key() != input_task.get_task_key()) {
    task = nullptr;
  } else if (sample_task->get_task_id() != input_task.get_task_id()) {
    task = nullptr;
  } else if (!(sample_task->get_trace_id() == input_task.get_trace_id())) {
    task = nullptr;
  } else {
    task = sample_task;
  }
  return ret;
}

int ObBackupTaskSchedulerQueue::get_wait_task_(
    const ObBackupScheduleTask &input_task, 
    ObBackupScheduleTask *&task)
{
  int ret = OB_SUCCESS;
  task = nullptr;
  const ObBackupScheduleTaskKey &task_key = input_task.get_task_key();
  ObBackupScheduleTask *sample_task = nullptr;
  if (OB_FAIL(task_map_.get_refactored(task_key, sample_task))) {
    if (OB_HASH_NOT_EXIST == ret) {
      task = nullptr;
    } else {
      LOG_WARN("get task from map failed", K(ret), K(input_task));
    }
  } else if (OB_UNLIKELY(nullptr == sample_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null task from map", K(ret));
  } else if (sample_task->in_schedule()) {
    task = nullptr;
  } else if (sample_task->get_task_key() != input_task.get_task_key()) {
    task = nullptr;
  } else if (sample_task->get_task_id() != input_task.get_task_id()) {
    task = nullptr;
  } else {
    task = sample_task;
  }
  return ret;
}

int ObBackupTaskSchedulerQueue::remove_task_(ObBackupScheduleTask *task, const bool &in_schedule)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task pointer", K(ret), K(task));
  } else if (!in_schedule) {
    wait_list_.remove(task);
  } else {
    schedule_list_.remove(task);
    if (OB_SUCCESS != (tmp_ret = clean_server_ref_(task->get_dst(), task->get_type()))) {
      LOG_ERROR("fail to clean server ref", K(ret), KPC(task));
    }
  }
  // clean the reference in tenant stat maps
  if (OB_SUCC(ret)) {
    if (OB_SUCCESS != (tmp_ret = clean_tenant_ref_(task->get_tenant_id()))) {
      LOG_ERROR("fail to clean tenant ref", K(ret), KPC(task));     
    }
    if (OB_SUCCESS != (tmp_ret = clean_task_map(task->get_task_key()))) {
      LOG_ERROR("fail to clean task map", K(ret), KPC(task));
    }
    LOG_INFO("succeed remove task from backup scheduler mgr", KPC(task));
    task->~ObBackupScheduleTask();
    task_allocator_.free(task);
    task = nullptr;
  }
  return ret;
}

int ObBackupTaskSchedulerQueue::get_schedule_tasks(
    ObIArray<ObBackupScheduleTask *> &schedule_tasks, 
    ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    LOG_WARN("queue not init", K(ret));
  } else if (!schedule_tasks.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schedule_tasks not empty", K(ret));
  } else {
    ObMutexGuard guard(mutex_);
    DLIST_FOREACH_X(t, schedule_list_, OB_SUCC(ret)) {
      void *raw_ptr = nullptr;
      ObBackupScheduleTask * task = nullptr;
      if (nullptr == (raw_ptr = allocator.alloc(t->get_deep_copy_size()))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate task", K(ret));
      } else if (OB_FAIL(t->clone(raw_ptr, task))) {
        LOG_WARN("fail to clone input task", K(ret));
      } else if (nullptr == task) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("input task ptr is null", K(ret));
      } else if (OB_FAIL(schedule_tasks.push_back(task))) {
        LOG_WARN("push back fail", K(ret));
      }
    }
  }
  return ret;
}

// ObBackupTaskScheduler
ObBackupTaskScheduler::ObBackupTaskScheduler()
  : is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    queue_(),
    rpc_proxy_(nullptr),
    sql_proxy_(nullptr),
    schema_service_(nullptr),
    backup_srv_array_()
{
}

int ObBackupTaskScheduler::init()
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  obrpc::ObSrvRpcProxy *rpc_proxy = GCTX.srv_rpc_proxy_;
  share::schema::ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy should not be NULL", K(ret), KP(sql_proxy));
  } else if (OB_ISNULL(rpc_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rpc_proxy should not be NULL", K(ret), KP(rpc_proxy));
  } else if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scheam service can not be NULL", K(ret), KP(schema_service));
  } else if (OB_UNLIKELY(nullptr == rpc_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(rpc_proxy));
  } else if (OB_FAIL(create("BACKUP_SCHE", *this, ObWaitEventIds::BACKUP_TASK_SCHEDULER_COND_WAIT))) {
    LOG_WARN("create backup task scheduler thread failed", K(ret));
  } else {
    tenant_id_ = MTL_ID();
    rpc_proxy_ = rpc_proxy;
    schema_service_ = schema_service;
    if (OB_FAIL(queue_.init(MAX_BACKUP_TASK_QUEUE_LIMIT, rpc_proxy_, this, MAX_BACKUP_TASK_QUEUE_LIMIT, *sql_proxy))) {
      LOG_WARN("init rebalance task queue failed", K(ret), LITERAL_K(MAX_BACKUP_TASK_QUEUE_LIMIT));
    } else {
      sql_proxy_ = sql_proxy;
      is_inited_ = true;
    }
  }
  return ret;
}

void ObBackupTaskScheduler::destroy()
{
  reuse();
  rpc_proxy_ = nullptr;
  sql_proxy_ = nullptr;
  schema_service_ = nullptr;
  is_inited_ = false;
  ObBackupBaseService::destroy();
}

int ObBackupTaskScheduler::switch_to_leader()
{
  reuse();
  return ObBackupBaseService::switch_to_leader();
}

void ObBackupTaskScheduler::switch_to_follower_forcedly()
{
  ObBackupBaseService::switch_to_follower_forcedly();
  reuse();
}

int ObBackupTaskScheduler::switch_to_follower_gracefully()
{
  int ret = ObBackupBaseService::switch_to_follower_gracefully();
  reuse();
  return ret;
}

int ObBackupTaskScheduler::resume_leader()
{
  reuse();
  return ObBackupBaseService::resume_leader();
}

void ObBackupTaskScheduler::run2()
{
  int ret = OB_SUCCESS;
  ObCurTraceId::init(GCONF.self_addr_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    LOG_INFO("backup task Scheduler start");
    int64_t last_dump_time = ObTimeUtility::current_time();
    int64_t last_check_alive_ts = ObTimeUtility::current_time();
    int64_t last_reload_task_ts = ObTimeUtility::current_time();
    bool reload_flag = false;
    while (!has_set_stop()) {
      bool is_normal = false;
      if (!is_meta_tenant(tenant_id_)) { // only meta tenant has backup task need to schedule
        set_idle_time(ObBackupBaseService::OB_MAX_IDLE_TIME);
        idle();
      } else if (OB_FAIL(check_tenant_status_normal_(is_normal))) {
        LOG_WARN("fail to chaeck tenant status normal", K(ret));
      } else if (is_normal) {
        dump_statistics_(last_dump_time);
        if (OB_FAIL(check_leader())) {
          LOG_WARN("fail to check leader", K(ret));
        } else if (OB_FAIL(reload_task_(last_reload_task_ts, reload_flag))) {
          LOG_WARN("failed to reload task", K(ret));
        } else {
          // error code has no effect between pop_and_send_task() and check_alive()
          if (OB_FAIL(pop_and_send_task_())) {
            LOG_WARN("fail to pop and send task", K(ret));
          }
          if (OB_FAIL(check_alive_(last_check_alive_ts, reload_flag))) {
            LOG_WARN("fail to check alive", K(ret));
          }
        }

        if (0 == queue_.get_task_cnt()) {
          set_idle_time(60*1000*1000);
          idle();
        }
      }
    }
    LOG_INFO("backup task scheduler stop");
  }
}

int ObBackupTaskScheduler::check_tenant_status_normal_(bool &is_normal)
{
  int ret = OB_SUCCESS;
  is_normal = false;
  share::schema::ObSchemaGetterGuard guard;
  const share::schema::ObSimpleTenantSchema *tenant_info = nullptr;
  if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id_, guard))) {
    LOG_WARN("fail to get schema guard", K(ret), K(tenant_id_));
  } else if (OB_FAIL(guard.get_tenant_info(tenant_id_, tenant_info))) {
    LOG_WARN("fail to get tenant info", K(ret), K(tenant_id_));
  } else if (OB_NOT_NULL(tenant_info) && tenant_info->is_normal()) {
    is_normal = true;
  }
  return ret;
}

int ObBackupTaskScheduler::reload_task_(int64_t &last_reload_task_ts, bool &reload_flag)
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  const int64_t MAX_CHECK_TIME_INTERVAL = 10 * 60 * 1000 * 1000L;
  common::ObArenaAllocator allocator;
  ObArray<ObBackupScheduleTask *> need_reload_tasks;
  reload_flag = false;
  ARRAY_FOREACH(backup_srv_array_, i) {
    need_reload_tasks.reset();
    ObBackupService *backup_service = backup_srv_array_.at(i);
    if (!backup_service->can_schedule() || now > MAX_CHECK_TIME_INTERVAL + last_reload_task_ts) {
      if (OB_FAIL(backup_service->get_need_reload_task(allocator, need_reload_tasks))) {
        LOG_WARN("failed to get need reload tasks", K(ret));
      } else if (need_reload_tasks.empty()) {
      } else if (OB_FAIL(queue_.reload_task(need_reload_tasks))) {
        LOG_WARN("failed to reload task", K(ret), K(need_reload_tasks));
      } else {
        LOG_INFO("succeed reload tasks", K(need_reload_tasks));
      }

      if (OB_SUCC(ret)) {
        backup_service->enable_backup();
        last_reload_task_ts = now;
        reload_flag = true;
        backup_service->wakeup();
      } else {
        reload_flag = false;
        backup_service->disable_backup();
      }
      ARRAY_FOREACH(need_reload_tasks, i) {
        ObBackupScheduleTask *task = need_reload_tasks.at(i);
        if (nullptr != task) {
          task->~ObBackupScheduleTask();
          task = nullptr;
        }
      }
    }
  }
  if (OB_SUCC(ret) && reload_flag) {
    last_reload_task_ts = now;
  }
  return ret;
}

int ObBackupTaskScheduler::pop_and_send_task_()
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator;
  ObBackupScheduleTask *task = nullptr;
  if (OB_FAIL(queue_.pop_task(task, allocator))) {
    LOG_WARN("pop_task failed", K(ret));
  }
  // execute task
  if (OB_SUCC(ret) && nullptr != task) {
    if (OB_FAIL(execute_task_(*task))) {
      LOG_WARN("send task to execute failed", K(ret), KPC(task));
    } 
    if (nullptr != task) {
      task->~ObBackupScheduleTask();
      task = nullptr;
    }
  }
  return ret;
}

int ObBackupTaskScheduler::check_alive_(int64_t &last_check_task_on_server_ts, bool &reload_flag)
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupScheduleTask *> schedule_tasks;
  ObArenaAllocator allocator;
  share::ObBackupServerMgr server_mgr;
  ObServerStatus server_status;
  Bool res = false;
  bool force_update = false;
  const int64_t now = ObTimeUtility::current_time();
  int64_t backup_task_keep_alive_interval = 0;
  int64_t backup_task_keep_alive_timeout = 0;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (!tenant_config.is_valid()) {
    backup_task_keep_alive_interval = 10_s;
    backup_task_keep_alive_timeout = 10_min;
  } else {
    backup_task_keep_alive_interval = tenant_config->_backup_task_keep_alive_interval;
    backup_task_keep_alive_timeout = tenant_config->_backup_task_keep_alive_timeout;
  }

  if ((now <= backup_task_keep_alive_interval + last_check_task_on_server_ts) && !reload_flag) {
  } else if (OB_FAIL(queue_.get_schedule_tasks(schedule_tasks, allocator))) {
    LOG_WARN("get scheduelr tasks error", K(ret));
  } else if (OB_FAIL(server_mgr.init(get_exec_tenant_id(), *sql_proxy_))) {
    LOG_WARN("fail to init server mgr", K(ret));
  } else {
    last_check_task_on_server_ts = now;
    for (int64_t i = 0; OB_SUCC(ret) && i < schedule_tasks.count(); ++i) {
      bool is_exist = true;
      ObBackupScheduleTask *task = schedule_tasks.at(i);
      const ObAddr dst = task->get_dst();
      share::ObServerInfoInTable server_info;
      obrpc::ObBackupCheckTaskArg check_task_arg;
      check_task_arg.tenant_id_ = task->get_tenant_id();
      check_task_arg.trace_id_ = task->get_trace_id();
      if ((now - task->get_generate_time() < backup_task_keep_alive_interval) && !reload_flag) {
        // no need to check alive, wait next turn
      } else if (OB_FAIL(server_mgr.is_server_exist(dst, force_update, is_exist))) {
        LOG_WARN("fail to check server exist", K(ret), K(dst));
      } else if (!is_exist) {
        LOG_WARN("backup dest server is not exist", K(ret), K(dst));
      } else if (OB_FAIL(server_mgr.get_server_status(dst, force_update, server_status))) {
        LOG_WARN("fail to get server status", K(ret), K(dst));
      } else if (!server_status.is_active() || !server_status.in_service()) {
        is_exist = false;
        LOG_WARN("server status may not active or in service", K(ret), K(dst));
      } else if (OB_FAIL(rpc_proxy_->to(dst).check_backup_task_exist(check_task_arg, res))) {
        if (now - task->get_executor_time() > backup_task_keep_alive_timeout) {
          is_exist = false;
        }
        LOG_WARN("fail to check task", K(ret), KPC(task));
      } else if (!res) {
        is_exist = false;
      }
      if (!is_exist) {
        LOG_INFO("task not on server, need remove", KPC(task));
        const int rc = OB_REBALANCE_TASK_NOT_IN_PROGRESS;
        ObHAResultInfo result_info(ObHAResultInfo::ROOT_SERVICE,
                                   task->get_dst(),
                                   task->get_trace_id(),
                                   rc);
        if (OB_FAIL(execute_over(*task, result_info))) {
          LOG_WARN("do execute over failed", K(ret), KPC(task));
        }
      }
      ROOTSERVICE_EVENT_ADD("backup", "check_backup_task_alive",
            "is_exist", is_exist ? "true" : "false",
            "job_id", task->get_job_id(),
            "tenant_id", task->get_tenant_id(),
            "ls_id", task->get_ls_id(),
            "dst", task->get_dst(),
            "trace_id", task->get_trace_id());
      task->~ObBackupScheduleTask();
      task = nullptr;
    }
    reload_flag = false;
  }
  return ret;
}

int ObBackupTaskScheduler::execute_task_(const ObBackupScheduleTask &task)
{
  int ret = OB_SUCCESS;
  THIS_WORKER.set_timeout_ts(INT64_MAX);
  LOG_INFO("execute task", K(task));
  if (OB_FAIL(do_execute_(task))) {
    LOG_WARN("fail to execute task", K(ret), K(task));
  }
  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    ObHAResultInfo result_info(ObHAResultInfo::ROOT_SERVICE,
                               task.get_dst(),
                               task.get_trace_id(),
                               ret);
    if (OB_SUCCESS != (tmp_ret = execute_over(task, result_info))) {
      LOG_WARN("do execute over failed", K(tmp_ret), K(result_info), K(task));
    }
  }
  return ret;
}

int ObBackupTaskScheduler::execute_over(const ObBackupScheduleTask &input_task, const share::ObHAResultInfo &result_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(queue_.execute_over(input_task, result_info))) {
    LOG_WARN("failed to execute over", K(ret), K(result_info));
  } else {
    wakeup();
  }
  return ret;
}

int ObBackupTaskScheduler::get_all_tasks(ObIAllocator &allocator, ObIArray<ObBackupScheduleTask *> &tasks)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(queue_.get_all_tasks(allocator, tasks))) {
    LOG_WARN("failed to get all tasks", K(ret));
  }
  return ret;
}

int ObBackupTaskScheduler::do_execute_(const ObBackupScheduleTask &task)
{
  int ret = OB_SUCCESS;
  // check dst server avaiable
  const ObAddr &online_server = task.get_dst();
  bool is_exist = false;
  bool in_service = false;
  const bool force_update = false;
  share::ObBackupServerMgr server_mgr;
  share::ObServerStatus server_status;
  if (OB_FAIL(server_mgr.init(get_exec_tenant_id(), *sql_proxy_))) {
    LOG_WARN("fail to init server mgr", K(ret));
  } else if (OB_FAIL(server_mgr.is_server_exist(online_server, force_update, is_exist))) {
    LOG_WARN("check server alive failed", K(ret), K(online_server));
  } else if (!is_exist) {
    ret = OB_REBALANCE_TASK_CANT_EXEC;
    LOG_WARN("dst server not exist", K(ret), K(online_server));
  } else if (OB_FAIL(server_mgr.get_server_status(online_server, force_update, server_status))) {
    LOG_WARN("fail to get server status", K(ret), K(online_server));
  } else if (!server_status.is_active() || !server_status.in_service()) {
    ret = OB_REBALANCE_TASK_CANT_EXEC;
    LOG_WARN("server status may not active or in service, task can't execute", K(ret), K(online_server));
  } else if (OB_UNLIKELY(nullptr == rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null ptr", K(ret), KP(rpc_proxy_));
  } else if (OB_FAIL(task.execute(*rpc_proxy_))) {
    LOG_WARN("fail to execute task", K(ret), K(task));
  }
  FLOG_INFO("execute send backup scheduler task", K(ret), K(task));
  return ret;
}

int ObBackupTaskScheduler::add_task(const ObBackupScheduleTask &task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup task scheduler not inited", K(ret));
  } else if (OB_FAIL(queue_.push_task(task))) {
    if (OB_ENTRY_EXIST != ret) {
      LOG_WARN("add task failed", K(ret), K(task));
    } else {
      LOG_WARN("task already exists", K(ret), K(task));
    }
  } else {
    wakeup();
    LOG_INFO("backup task scheduler add task success", K(ret), K(task));
  }
  return ret;
}

void ObBackupTaskScheduler::dump_statistics_(int64_t &last_dump_time)
{
  int tmp_ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  if (now > last_dump_time + TASK_SCHEDULER_STATISTIC_INTERVAL) {
    // record scheduler execution log periodically
    last_dump_time = now;
    if (OB_SUCCESS != (tmp_ret = queue_.dump_statistics())) {
      LOG_WARN_RET(tmp_ret, "fail to dump statistics", K(tmp_ret));
    }
  }
}

int ObBackupTaskScheduler::cancel_tasks(const BackupJobType &type, const uint64_t job_id, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
	if (OB_FAIL(queue_.cancel_tasks(type, job_id, tenant_id))) {
    LOG_WARN("remove task group failed", K(ret), K(job_id), K(tenant_id));
  } 
  return ret;
}

int ObBackupTaskScheduler::cancel_tasks(const BackupJobType &type, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
	if (OB_FAIL(queue_.cancel_tasks(type, tenant_id))) {
    LOG_WARN("remove task group failed", K(ret), K(tenant_id));
  } 
  return ret;
}

int ObBackupTaskScheduler::check_task_exist(const ObBackupScheduleTaskKey key, bool &is_exist)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(queue_.check_task_exist(key, is_exist))) {
    LOG_WARN("failed to check task exist", K(ret), K(key));
  }
  return ret;
}

int ObBackupTaskScheduler::reuse()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    queue_.reset();
  }
  return ret;
}

int ObBackupTaskScheduler::get_backup_job(const BackupJobType &type, ObIBackupJobScheduler *&job)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(scheduler_mtx_);
  ARRAY_FOREACH(backup_srv_array_, i) {
    ObBackupService *srv = backup_srv_array_.at(i);
    if (OB_ISNULL(srv)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("backup service must not be null", K(ret));
    } else if (OB_NOT_NULL(job = srv->get_scheduler(type))) {
      if (job->get_job_type() != type) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("backup job not match job type", K(ret));
      } else {
        break;
      }
    }
  }

  if (OB_FAIL(ret)) {
    job = nullptr;
  }
  return ret;
}

int ObBackupTaskScheduler::register_backup_srv(ObBackupService &srv)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(scheduler_mtx_);
  ARRAY_FOREACH(backup_srv_array_, i) {
    if (&srv == backup_srv_array_.at(i)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("duplicate register", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(backup_srv_array_.push_back(&srv))) {
    LOG_WARN("failed to push backup backup service", K(ret));
  }
  return ret;
}

}  // namespace rootserver
}  // namespace oceanbase
