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

#define USING_LOG_PREFIX RS_LB
#include "ob_partition_backup.h"
#include "ob_balance_info.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_se_array_iterator.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/config/ob_server_config.h"
#include "share/ob_multi_cluster_util.h"
#include "ob_rebalance_task.h"
#include "ob_rebalance_task_mgr.h"
#include "ob_root_utils.h"
#include "rootserver/ob_zone_manager.h"
#include "rootserver/ob_partition_disk_balancer.h"
#include "rootserver/ob_balance_group_container.h"
#include "rootserver/ob_balance_group_data.h"
#include "observer/ob_server_struct.h"
#include "share/backup/ob_backup_operator.h"
#include "share/backup/ob_tenant_backup_task_updater.h"

using namespace oceanbase::common;
using namespace oceanbase::rootserver;
using namespace oceanbase::share;
using namespace oceanbase::rootserver::balancer;

ObPartitionBackup::ObPartitionBackup()
    : is_inited_(false),
      config_(NULL),
      schema_service_(NULL),
      sql_proxy_(NULL),
      pt_operator_(NULL),
      task_mgr_(NULL),
      zone_mgr_(NULL),
      tenant_stat_(NULL),
      origin_tenant_stat_(NULL),
      server_mgr_(NULL),
      check_stop_provider_(NULL),
      pg_task_updater_(),
      start_task_id_(0),
      end_task_id_(0)
{}

int ObPartitionBackup::init(common::ObServerConfig& cfg, share::schema::ObMultiVersionSchemaService& schema_service,
    common::ObMySQLProxy& sql_proxy, share::ObPartitionTableOperator& pt_operator, ObRebalanceTaskMgr& task_mgr,
    ObZoneManager& zone_mgr, TenantBalanceStat& tenant_stat, ObServerManager& server_mgr,
    share::ObCheckStopProvider& check_stop_provider)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(pg_task_updater_.init(sql_proxy))) {
    LOG_WARN("failed to init pg task updater", K(ret));
  } else {
    config_ = &cfg;
    schema_service_ = &schema_service;
    sql_proxy_ = &sql_proxy;
    pt_operator_ = &pt_operator;
    task_mgr_ = &task_mgr;
    zone_mgr_ = &zone_mgr;
    origin_tenant_stat_ = &tenant_stat;
    tenant_stat_ = &tenant_stat;
    server_mgr_ = &server_mgr;
    check_stop_provider_ = &check_stop_provider;

    is_inited_ = true;
  }
  return ret;
}

int ObPartitionBackup::partition_backup(int64_t& task_cnt, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObArray<ObPGBackupTaskInfo> pg_tasks;
  ObArenaAllocator allocator(ObModIds::BACKUP);
  ObArray<ObRegion> detected_region;
  ObPartitionBackupProvider provider;
  ObTenantBackupTaskInfo task_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition backup do not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition backup get invalid argument", K(ret), K(tenant_id));
  } else if (OB_SYS_TENANT_ID == tenant_id) {
    // do nothing
  } else if (OB_FAIL(get_backup_infos(tenant_id, task_info, pg_tasks))) {
    LOG_WARN("failed to get pg backup task", K(ret), K(tenant_id));
  } else if (pg_tasks.empty()) {
    // do nothing
  } else if (OB_FAIL(get_detected_region(tenant_id, detected_region))) {
    LOG_WARN("failed to get detected region", K(ret), K(tenant_id));
  } else if (OB_FAIL(provider.init(detected_region, task_info, allocator, *zone_mgr_, *server_mgr_))) {
    LOG_WARN("failed to init partition backup provider", K(ret), K(detected_region));
  } else if (OB_FAIL(prepare_backup_task(pg_tasks, provider, allocator))) {
    LOG_WARN("failed to prepare backup task", K(ret), K(pg_tasks));
  } else if (FALSE_IT(reset_task_id_range())) {
  } else if (OB_FAIL(backup_pg(tenant_id, task_cnt, provider))) {
    LOG_WARN("failed to backup pg", K(ret));
  }
  return ret;
}

int ObPartitionBackup::get_backup_infos(const uint64_t tenant_id, share::ObTenantBackupTaskInfo& task_info,
    common::ObIArray<share::ObPGBackupTaskInfo>& pg_tasks)
{
  int ret = OB_SUCCESS;
  task_info.reset();
  pg_tasks.reset();
  const int64_t start_ts = ObTimeUtil::current_time();
  ObTenantBackupTaskUpdater updater;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition backup do not init", K(ret));
  } else if (OB_FAIL(updater.init(*sql_proxy_))) {
    LOG_WARN("failed to init tenant backup task updater", K(ret), K(tenant_id));
  } else if (OB_FAIL(updater.get_tenant_backup_task(tenant_id, task_info))) {
    LOG_WARN("failed to get tenant backup task", K(ret), K(tenant_id));
  } else if (ObTenantBackupTaskInfo::DOING != task_info.status_ &&
             ObTenantBackupTaskInfo::CANCEL != task_info.status_) {
    // do nothing
  } else if (OB_FAIL(pg_task_updater_.get_pending_pg_task(
                 task_info.tenant_id_, task_info.incarnation_, task_info.backup_set_id_, pg_tasks))) {
    LOG_WARN("failed to get doing pg task", K(ret), K(task_info));
  } else if (!task_info.is_result_succeed()) {
    if (OB_FAIL(cancel_pending_pg_tasks(task_info, pg_tasks))) {
      LOG_WARN("failed to cancel pending pg tasks", K(ret), K(task_info));
    }
  }

  LOG_INFO("get pg backup task",
      "cost ts",
      ObTimeUtil::current_time() - start_ts,
      K(ret),
      K(tenant_id),
      K(pg_tasks.count()),
      K(task_info));
  return ret;
}

int ObPartitionBackup::check_pg_backup_task(const ObPGBackupTaskInfo& pg_task, bool& need_add)
{
  // TODO() fix it later
  int ret = OB_SUCCESS;
  need_add = true;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition backup do not init", K(ret));
  } else if (!pg_task.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check pg backup task alive get invalid argument", K(ret));
  } else if (ObPGBackupTaskInfo::FINISH == pg_task.status_ && OB_SUCCESS == pg_task.result_) {
    need_add = false;
  } else if (ObPGBackupTaskInfo::FINISH == pg_task.status_) {
    // may need reschedule
    need_add = false;
  } else if (ObPGBackupTaskInfo::DOING == pg_task.status_) {
    // check server or task still alive
    need_add = false;
  } else if (ObPGBackupTaskInfo::PENDING == pg_task.status_) {
    need_add = true;
  }
  return ret;
}

int ObPartitionBackup::get_detected_region(const uint64_t tenant_id, common::ObIArray<ObRegion>& detected_region)
{
  int ret = OB_SUCCESS;
  ObBackupInfoManager info_manager;
  ObArray<uint64_t> tenant_ids;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition backup do not init", K(ret));
  } else if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
    LOG_WARN("failed to push tenant id into array", K(ret), K(tenant_id));
  } else if (OB_FAIL(info_manager.init(tenant_ids, *sql_proxy_))) {
    LOG_WARN("failed to init info manager", K(ret), K(tenant_ids));
  } else if (OB_FAIL(info_manager.get_detected_region(tenant_id, detected_region))) {
    LOG_WARN("failed to get detected region", K(ret), K(tenant_ids));
  }
  return ret;
}

int ObPartitionBackup::prepare_backup_task(
    const ObIArray<ObPGBackupTaskInfo>& pg_tasks, ObPartitionBackupProvider& provider, ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtil::current_time();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition backup do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pg_tasks.count(); ++i) {
      ObPartitionInfo partition_info;
      partition_info.set_allocator(&allocator);
      const ObPGBackupTaskInfo& pg_task = pg_tasks.at(i);
      if (OB_FAIL(pt_operator_->get(pg_task.table_id_, pg_task.partition_id_, partition_info))) {
        LOG_WARN("failed to get partition info", K(ret), K(pg_task));
      } else if (OB_FAIL(provider.add_backup_replica_info(partition_info))) {
        LOG_WARN("failed to add pg backup task into provider", K(ret), K(partition_info));
      }
    }
  }
  LOG_INFO("prepare backup task", "cost ts", ObTimeUtil::current_time() - start_ts, K(ret));
  return ret;
}

int ObPartitionBackup::backup_pg(const uint64_t tenant_id, int64_t& task_cnt, ObPartitionBackupProvider& provider)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtil::current_time();
  const int64_t HALF_TASK_QUEUE_LIMIT = task_mgr_->TASK_QUEUE_LIMIT / 2;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition backup do not init", K(ret));
  } else if (OB_FAIL(provider.prepare_choose_src())) {
    LOG_WARN("failed to prepare choose src", K(ret));
  } else {
    LOG_INFO("prepare choose src", "cost ts", ObTimeUtil::current_time() - start_ts);
    ObBackupTask task;
    ObArray<ObBackupTaskInfo> task_info;
    while (OB_SUCC(ret)) {
      task_info.reset();
      task.reset();
      const int64_t task_info_cnt = task_mgr_->get_high_priority_task_info_cnt();
      if (task_info_cnt > HALF_TASK_QUEUE_LIMIT) {
        LOG_INFO("task mgr task info cnt may full", K(task_info_cnt), K(HALF_TASK_QUEUE_LIMIT));
        break;
      } else if (OB_FAIL(get_task_id_range(tenant_id))) {
        LOG_WARN("failed to get task id range", K(ret));
      } else if (OB_FAIL(provider.generate_batch_backup_task(start_task_id_, task_info))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to generate batch backup task", K(ret));
        }
      } else {
        ++start_task_id_;
      }

      if (OB_SUCC(ret) && task_info.count() > 0) {
        const ObAddr& server = task_info.at(0).get_dest().get_server();
        LOG_INFO("backup task info", K(task_info.count()), K(server));
        if (OB_FAIL(task.build(task_info, server, ""))) {
          LOG_WARN("fail to build migrate task", K(ret));
        } else if (OB_FAIL(batch_update_pg_task_info(task_info, task.get_task_id()))) {
          LOG_WARN("failed to batch update pg task addr", K(ret), K(task_info));
        } else {
          DEBUG_SYNC(PARTITION_BACKUP_TASK_BEFORE_ADD_TASK_IN_MGR);
          if (OB_FAIL(task_mgr_->add_task(task, task_cnt))) {
            LOG_WARN("fail to add task", K(ret));
          } else {
          }  // no more to do
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(check_stop())) {
          LOG_WARN("balancer stop", K(ret));
        }
      }
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObPartitionBackup::batch_update_pg_task_info(const ObIArray<ObBackupTaskInfo>& task_info, const ObTaskId& task_id)
{
  int ret = OB_SUCCESS;
  ObArray<ObPartitionKey> pkeys;
  const int64_t MAX_BATCH_CNT = 1024;
  int64_t report_idx = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition backup do not init", K(ret));
  } else if (task_info.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("batch update pg task addr get invalid argument", K(ret), K(task_info));
  } else {
    const ObAddr& addr = task_info.at(0).get_dest().get_server();
    const ObReplicaType& replica_type = task_info.at(0).get_dest().member_.get_replica_type();
    const ObPGBackupTaskInfo::BackupStatus status = ObPGBackupTaskInfo::DOING;
    for (int64_t i = 0; OB_SUCC(ret) && i < task_info.count(); ++i) {
      const ObPartitionKey& pkey = task_info.at(i).get_partition_key();
      if (OB_FAIL(pkeys.push_back(pkey))) {
        LOG_WARN("failed to push pkey into array", K(ret), K(pkey));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(pg_task_updater_.update_pg_task_info(pkeys, addr, task_id, replica_type, status))) {
        LOG_WARN("failed to update pg backup task addr", K(ret), K(pkeys), K(addr), K(status));
      }
    }
  }
  return ret;
}

int ObPartitionBackup::get_task_id_range(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_TASK_ID_INTERVAL = 100;
  ObBackupInfoManager info_manager;
  ObArray<uint64_t> tenant_ids;
  ObBaseBackupInfoStruct info;
  ObBackupItemTransUpdater updater;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition backup do not init", K(ret));
  } else if (start_task_id_ < end_task_id_) {
    // do nothing
    LOG_INFO("start task id  smaller than end task id, skip it", K(start_task_id_), K(end_task_id_));
  } else if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
    LOG_WARN("failed to push tenant id into array", K(ret), K(tenant_id));
  } else if (OB_FAIL(info_manager.init(tenant_ids, *sql_proxy_))) {
    LOG_WARN("failed to init info manager", K(ret), K(tenant_ids));
  } else if (OB_FAIL(updater.start(*sql_proxy_))) {
    LOG_WARN("failed to start trans", K(ret), K(tenant_id));
  } else {
    if (OB_FAIL(info_manager.get_backup_info(tenant_id, updater, info))) {
      LOG_WARN("failed to get backup info", K(ret), K(tenant_id));
    } else if (!info.backup_status_.is_doing_status()) {
      // do nothing
    } else {
      start_task_id_ = info.backup_task_id_;
      end_task_id_ = start_task_id_ + MAX_TASK_ID_INTERVAL;
      info.backup_task_id_ = end_task_id_;
      if (OB_FAIL(info_manager.update_backup_info(tenant_id, info, updater))) {
        LOG_WARN("failed to update backup info", K(ret), K(info), K(tenant_id));
      }
    }
    int tmp_ret = OB_SUCCESS;
    tmp_ret = updater.end(OB_SUCC(ret));
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
  }
  return ret;
}

void ObPartitionBackup::reset_task_id_range()
{
  start_task_id_ = 0;
  end_task_id_ = 0;
}

int ObPartitionBackup::cancel_pending_pg_tasks(
    const ObTenantBackupTaskInfo& task_info, const ObIArray<share::ObPGBackupTaskInfo>& pg_tasks)
{
  int ret = OB_SUCCESS;
  ObArray<ObPartitionKey> pkeys;
  ObArray<int32_t> results;
  const ObPGBackupTaskInfo::BackupStatus status = ObPGBackupTaskInfo::FINISH;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup do not init", K(ret));
  } else if (!task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check doing pg task get invalid argument", K(ret), K(task_info));
  } else {
    ObPartitionKey pkey;
    for (int64_t i = 0; OB_SUCC(ret) && i < pg_tasks.count(); ++i) {
      pkey.reset();
      const ObPGBackupTaskInfo& pg_backup_task = pg_tasks.at(i);
      if (OB_FAIL(pkey.init(pg_backup_task.table_id_, pg_backup_task.partition_id_, 0))) {
        LOG_WARN("failed to init pkey", K(ret), K(pg_backup_task));
      } else if (OB_FAIL(pkeys.push_back(pkey))) {
        LOG_WARN("failed to push pkey into array", K(ret), K(pkey), K(pg_backup_task));
      } else if (OB_FAIL(results.push_back(OB_CANCELED))) {
        LOG_WARN("failed to push result into array", K(ret), K(pkey), K(pg_backup_task));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(pg_task_updater_.update_status_and_result(pkeys, results, status))) {
        LOG_WARN("failed to update status and result", K(ret), K(pkeys));
      }
    }
  }
  return ret;
}

ObBackupElement::ObBackupElement() : replica_(), region_()
{}

int ObBackupElement::assign(const ObBackupElement& element)
{
  int ret = OB_SUCCESS;
  if (!element.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup element is invalid", K(ret), K(element));
  } else if (OB_FAIL(replica_.assign(element.replica_))) {
    LOG_WARN("failed to assgin replica", K(ret), K(element));
  } else {
    region_ = element.region_;
  }
  return ret;
}

void ObBackupElement::reset()
{
  replica_.reset();
  region_.reset();
}

bool ObBackupElement::is_valid() const
{
  return replica_.is_valid() && !region_.is_empty();
}

ObReplicaBackupElement::ObReplicaBackupElement() : replica_element_(), choose_element_(NULL)
{}

void ObReplicaBackupElement::reset()
{
  replica_element_.reset();
  choose_element_ = NULL;
}

int ObReplicaBackupElement::assign(const ObReplicaBackupElement& element)
{
  int ret = OB_SUCCESS;
  if (!element.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("replica backup element is invalid", K(ret), K(element));
  } else if (OB_FAIL(replica_element_.assign(element.replica_element_))) {
    LOG_WARN("failed to assign replica backup element", K(ret));
  } else {
    choose_element_ = element.choose_element_;
  }
  return ret;
}

bool ObReplicaBackupElement::is_valid() const
{
  return replica_element_.count() > 0;
}

ObPartitionBackupProvider::ObPartitionBackupProvider()
    : is_inited_(false),
      detected_region_(),
      task_info_(),
      all_replica_elements_(),
      allocator_(NULL),
      zone_mgr_(NULL),
      addr_array_(),
      iter_index_(0),
      map_iter_(),
      server_mgr_(NULL)
{}

int ObPartitionBackupProvider::init(const common::ObIArray<common::ObRegion>& detected_region,
    const ObTenantBackupTaskInfo& task_info, common::ObIAllocator& allocator, ObZoneManager& zone_mgr,
    ObServerManager& server_mgr)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition backup provider init twice", K(ret));
  } else if (!task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup provider init get invalid argument", K(ret), K(task_info));
  } else if (OB_FAIL(detected_region_.assign(detected_region))) {
    LOG_WARN("failed to assign detected region", K(ret));
  } else if (OB_FAIL(all_replica_elements_.create(MAX_BUCKET_NUM, common::ObModIds::BACKUP))) {
    LOG_WARN("failed to create all replica elements map", K(ret));
  } else {
    task_info_ = task_info;
    allocator_ = &allocator;
    zone_mgr_ = &zone_mgr;
    server_mgr_ = &server_mgr;
    is_inited_ = true;
  }
  return ret;
}

int ObPartitionBackupProvider::add_backup_replica_info(const share::ObPartitionInfo& partition_info)
{
  int ret = OB_SUCCESS;
  ObRegion region;
  ObArray<ObBackupElement> backup_element_array;
  ObArray<ObBackupElement> tmp_backup_element_array;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition backup provider do not init", K(ret));
  } else if (!partition_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition backup provider get invalid argument", K(ret), K(partition_info));
  } else {
    const ObPartitionKey pkey(partition_info.get_table_id(), partition_info.get_partition_id(), 0);
    const ObIArray<ObPartitionReplica>& replica_array = partition_info.get_replicas_v2();
    for (int64_t i = 0; OB_SUCC(ret) && i < replica_array.count(); ++i) {
      ObBackupElement backup_element;
      bool is_exist = false;
      ObServerStatus server_status;
      const ObPartitionReplica& replica = replica_array.at(i);
      if (!replica.is_in_service() || !replica.in_member_list_ || replica.is_restore_) {
        LOG_INFO("this type replica can not backup, skip it", K(replica));
      } else if (OB_FAIL(server_mgr_->is_server_exist(replica.server_, is_exist))) {
        LOG_WARN("failed to check server is exist", K(ret), K(replica));
      } else if (!is_exist) {
        LOG_INFO("server do not exist", K(replica));
      } else if (OB_FAIL(server_mgr_->get_server_status(replica.server_, server_status))) {
        LOG_WARN("failed to get server status", K(ret), K(replica));
      } else if (!server_status.is_active() || !server_status.in_service()) {
        LOG_INFO("server not active or in service", K(ret), K(replica));
      } else if (OB_FAIL(zone_mgr_->get_region(replica.zone_, region))) {
        LOG_WARN("failed to get region", K(ret), K(replica));
      } else {
        backup_element.replica_ = replica;
        backup_element.region_ = region;
        if (OB_FAIL(tmp_backup_element_array.push_back(backup_element))) {
          LOG_WARN("failed to push backup element into array", K(ret), K(backup_element));
        }
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < detected_region_.count(); ++i) {
      const ObRegion& region = detected_region_.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < tmp_backup_element_array.count(); ++j) {
        const ObBackupElement& element = tmp_backup_element_array.at(j);
        if (element.region_ == region) {
          bool can_become = false;
          if (OB_FAIL(check_can_become_dest(element, can_become))) {
            LOG_WARN("failed to check can become dest", K(ret), K(can_become));
          } else if (!can_become) {
            // do nothing
          } else if (OB_FAIL(backup_element_array.push_back(element))) {
            LOG_WARN("failed to push element into array", K(ret), K(element));
          }
        }
      }
      if (OB_SUCC(ret) && !backup_element_array.empty()) {
        break;
      }
    }

    if (OB_SUCC(ret) && backup_element_array.empty()) {
      // not find same region
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_backup_element_array.count(); ++i) {
        const ObBackupElement& element = tmp_backup_element_array.at(i);
        bool can_become = false;
        if (!element.replica_.is_strong_leader()) {
          if (OB_FAIL(check_can_become_dest(element, can_become))) {
            LOG_WARN("failed to check can become dest", K(ret), K(can_become));
          } else if (!can_become) {
            // do nothing
          } else if (OB_FAIL(backup_element_array.push_back(element))) {
            LOG_WARN("failed to push element into array", K(ret), K(element));
          }
        }
      }
    }

    if (OB_SUCC(ret) && backup_element_array.empty()) {
      // put leader in it
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_backup_element_array.count(); ++i) {
        const ObBackupElement& element = tmp_backup_element_array.at(i);
        bool can_become = false;
        if (!element.replica_.is_strong_leader()) {
          // do nothing
        } else if (OB_FAIL(check_can_become_dest(element, can_become))) {
          LOG_WARN("failed to check can become dest", K(ret), K(can_become));
        } else if (!can_become) {
          // do nothing
        } else if (OB_FAIL(backup_element_array.push_back(element))) {
          LOG_WARN("failed to push element into array", K(ret), K(element));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (backup_element_array.empty()) {
        bool is_in_merge = false;
        if (OB_FAIL(zone_mgr_->is_in_merge(is_in_merge))) {
          LOG_WARN("failed to check is in merge", K(ret));
        } else if (is_in_merge) {
          // do nothing
        } else {
          bool has_suitable_replica = false;
          for (int64_t i = 0; i < tmp_backup_element_array.count(); ++i) {
            const ObBackupElement& backup_element = tmp_backup_element_array.at(i);
            if (backup_element.replica_.data_version_ >= task_info_.backup_data_version_) {
              has_suitable_replica = true;
              break;
            }
          }

          if (!has_suitable_replica) {
            FLOG_INFO("not suitable replica, need wait", K(tmp_backup_element_array), K(pkey), K(partition_info));
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("backup element array should not be empty", K(ret), K(backup_element_array), K(is_in_merge));
          }
        }
      } else {
        void* buf = NULL;
        ObReplicaBackupElement* replica_backup_element = NULL;
        if (OB_ISNULL(buf = allocator_->alloc(sizeof(ObReplicaBackupElement)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc buf", K(ret));
        } else {
          replica_backup_element = new (buf) ObReplicaBackupElement();
          if (OB_FAIL(replica_backup_element->replica_element_.assign(backup_element_array))) {
            LOG_WARN("failed to assign backup element array", K(ret), K(backup_element_array));
          } else if (OB_FAIL(all_replica_elements_.set_refactored(pkey, replica_backup_element))) {
            LOG_WARN("failed to set replica backup element into mao", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObPartitionBackupProvider::prepare_choose_src()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition backup do not init", K(ret));
  } else if (OB_FAIL(inner_prepare_choose_src())) {
    LOG_WARN("failed to do inner prepare choose src", K(ret));
  } else {
    map_iter_ = all_replica_elements_.begin();
  }
  return ret;
}

int ObPartitionBackupProvider::inner_prepare_choose_src()
{
  int ret = OB_SUCCESS;
  addr_array_.reset();
  const int64_t MAX_ADDR_BUCKET_NUM = 64;
  hash::ObHashMap<ObAddr, int64_t> addr_num_map;
  int hash_ret = OB_SUCCESS;
  const int32_t flag = 1;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition backup provider do not init", K(ret));
  } else if (OB_FAIL(addr_num_map.create(MAX_ADDR_BUCKET_NUM, ObModIds::BACKUP))) {
    LOG_WARN("failed to create addr num map", K(ret));
  } else {
    for (ReplicaElementMap::iterator iter = all_replica_elements_.begin();
         OB_SUCC(ret) && iter != all_replica_elements_.end();
         ++iter) {
      const ObReplicaBackupElement* backup_element = iter->second;
      for (int64_t i = 0; OB_SUCC(ret) && i < backup_element->replica_element_.count(); ++i) {
        const ObBackupElement& element = backup_element->replica_element_.at(i);
        int64_t count = 0;
        hash_ret = addr_num_map.get_refactored(element.replica_.server_, count);
        if (OB_SUCCESS == hash_ret || OB_HASH_NOT_EXIST == hash_ret) {
          ++count;
        } else {
          ret = hash_ret;
          LOG_WARN("get addr num map get unexpected results", K(ret));
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(addr_num_map.set_refactored(element.replica_.server_, count, flag))) {
            LOG_WARN("failed to set addr num into map", K(ret), K(element));
          }
        }
      }
    }

    for (ReplicaElementMap::iterator iter = all_replica_elements_.begin();
         OB_SUCC(ret) && iter != all_replica_elements_.end();
         ++iter) {
      ObReplicaBackupElement* backup_element = iter->second;
      int64_t max_count = 0;
      int64_t count = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < backup_element->replica_element_.count(); ++i) {
        const ObBackupElement& element = backup_element->replica_element_.at(i);
        hash_ret = addr_num_map.get_refactored(element.replica_.server_, count);
        if (OB_SUCCESS != hash_ret) {
          ret = hash_ret;
          LOG_WARN("get addr num map get unexpected result", K(ret), K(element));
        } else {
          if (count > max_count) {
            backup_element->choose_element_ = &element;
            max_count = count;
          } else if (max_count == count) {
            if (OB_ISNULL(backup_element->choose_element_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("backup element choose element should not NULL", K(ret), K(*backup_element));
            } else if (backup_element->choose_element_->replica_.is_strong_leader()) {
              backup_element->choose_element_ = &element;
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      LOG_INFO("dump task info", K(task_info_));
      for (hash::ObHashMap<ObAddr, int64_t>::iterator iter = addr_num_map.begin();
           OB_SUCC(ret) && iter != addr_num_map.end();
           ++iter) {
        const ObAddr& addr = iter->first;
        if (OB_FAIL(addr_array_.push_back(addr))) {
          LOG_WARN("failed to push addr into array", K(ret), K(addr));
        } else {
          LOG_INFO("backup choose addr ", K(iter->first), "count", iter->second);
        }
      }
    }
  }
  return ret;
}

int ObPartitionBackupProvider::generate_batch_backup_task(
    const int64_t backup_task_id, ObIArray<ObBackupTaskInfo>& backup_task)
{
  int ret = OB_SUCCESS;
  ObPhysicalBackupArg physical_backup_arg;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition backup provider do not init", K(ret));
  } else if (iter_index_ == addr_array_.count()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(build_physical_backup_arg(backup_task_id, physical_backup_arg))) {
    LOG_WARN("failed to build physical backup arg", K(ret));
  } else {
    const ObAddr& addr = addr_array_.at(iter_index_);
    for (; OB_SUCC(ret) && backup_task.count() <= MAX_TASK_NUM && map_iter_ != all_replica_elements_.end();
         ++map_iter_) {
      ObReplicaBackupElement* backup_element = map_iter_->second;
      const ObBackupElement* choose_element = backup_element->choose_element_;
      if (OB_ISNULL(choose_element)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("choose element should not be NULL", K(ret), KP(choose_element));
      } else if (addr != choose_element->replica_.server_) {
        // do nothing
      } else {
        ObBackupTaskInfo task_info;
        const ObPartitionReplica& replica = choose_element->replica_;
        ObPartitionKey key(replica.table_id_, replica.partition_id_, 0);
        ObReplicaMember src = ObReplicaMember(
            replica.server_, replica.member_time_us_, replica.replica_type_, replica.get_memstore_percent());
        OnlineReplica dst;
        dst.member_ = ObReplicaMember(
            replica.server_, ObTimeUtility::current_time(), replica.replica_type_, replica.get_memstore_percent());
        dst.unit_id_ = replica.unit_id_;
        dst.zone_ = replica.zone_;
        dst.member_.set_region(choose_element->region_);
        int64_t transmit_data_size = replica.data_size_;
        int64_t cluster_id = OB_INVALID_ID;

        if (FALSE_IT(task_info.set_transmit_data_size(transmit_data_size))) {
          // do nothing
        } else if (OB_FAIL(task_info.build(key, dst, src, physical_backup_arg))) {
          LOG_WARN("failed to build backup task info", K(ret));
        } else if (OB_FAIL(backup_task.push_back(task_info))) {
          LOG_WARN("failed to push task info into array", K(ret), K(task_info));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (map_iter_ == all_replica_elements_.end()) {
        ++iter_index_;
        map_iter_ = all_replica_elements_.begin();
      }
    }
  }
  return ret;
}

int ObPartitionBackupProvider::build_physical_backup_arg(const int64_t backup_task_id, share::ObPhysicalBackupArg& arg)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("partition backup provider do not init", K(ret));
  } else {
    arg.backup_data_version_ = task_info_.backup_data_version_;
    arg.backup_schema_version_ = task_info_.backup_schema_version_;
    arg.backup_set_id_ = task_info_.backup_set_id_;
    arg.incarnation_ = task_info_.incarnation_;
    arg.tenant_id_ = task_info_.tenant_id_;
    arg.prev_full_backup_set_id_ = task_info_.prev_full_backup_set_id_;
    arg.prev_inc_backup_set_id_ = task_info_.prev_inc_backup_set_id_;
    arg.task_id_ = backup_task_id;
    arg.backup_type_ = task_info_.backup_type_.type_;
    arg.prev_data_version_ = task_info_.prev_backup_data_version_;
    arg.backup_snapshot_version_ = task_info_.snapshot_version_;
    MEMCPY(arg.storage_info_, task_info_.backup_dest_.storage_info_, sizeof(task_info_.backup_dest_.storage_info_));
    MEMCPY(arg.uri_header_, task_info_.backup_dest_.root_path_, sizeof(task_info_.backup_dest_.root_path_));
  }
  return ret;
}

int ObPartitionBackupProvider::check_can_become_dest(const ObBackupElement& element, bool& can_become)
{
  int ret = OB_SUCCESS;
  HEAP_VAR(ObZoneInfo, zone_info)
  {
    int64_t zone_last_merge_version = 0;
    can_become = true;
    if (!is_inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("partition backup provider do not init", K(ret));
    } else if (!element.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("check can become dest get invaid argument", K(ret), K(element));
    } else if (!ObReplicaTypeCheck::is_replica_with_ssstore(element.replica_.replica_type_)) {
      can_become = false;
    } else if (OB_FAIL(zone_mgr_->get_zone(element.replica_.zone_, zone_info))) {
      LOG_WARN("failed to get zone info", K(ret), K(element));
    } else if (zone_info.last_merged_version_.value_ >= task_info_.backup_data_version_ &&
               element.replica_.data_version_ >= task_info_.backup_data_version_) {
      // can become
    } else {
      can_become = false;
    }
  }
  return ret;
}
