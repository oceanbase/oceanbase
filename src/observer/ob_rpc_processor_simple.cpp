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

#define USING_LOG_PREFIX SERVER

#include "observer/ob_rpc_processor_simple.h"

#include "lib/io/ob_io_manager.h"
#include "lib/allocator/page_arena.h"
#include "share/ob_autoincrement_service.h"
#include "share/config/ob_config_manager.h"
#include "share/stat/ob_opt_stat_manager.h"
#include "share/ob_server_blacklist.h"
#include "share/rc/ob_context.h"
#include "share/rc/ob_tenant_base.h"
#include "share/cache/ob_cache_name_define.h"
#include "storage/ob_partition_service.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/ob_partition_storage.h"
#include "storage/ob_pg_storage.h"
#include "storage/blocksstable/ob_store_file.h"
#include "clog/ob_partition_log_service.h"
#include "rootserver/ob_root_service.h"
#include "sql/plan_cache/ob_plan_cache_manager.h"
#include "sql/ob_sql.h"
#include "observer/ob_service.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/mysql/ob_diag.h"
#include "observer/mysql/ob_mysql_request_manager.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/omt/ob_tenant_node_balancer.h"
#include "storage/ob_file_system_util.h"
#include "storage/ob_partition_group.h"

namespace oceanbase {
using namespace common;
using namespace clog;
using namespace storage;
using namespace transaction;
using namespace memtable;
using namespace share;
using namespace sql;
using namespace obmysql;
using namespace omt;

namespace observer {

int ObErrorP::process()
{
  if (ret_ == OB_SUCCESS) {
    LOG_ERROR("should not return success in error packet", K(ret_));
  }
  return ret_;
}

int ObErrorP::deserialize()
{
  return OB_SUCCESS;
}

int ObRpcCheckBackupSchuedulerWorkingP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.root_service_)) {
    LOG_ERROR("invalid argument", K(gctx_.root_service_));
  } else {
    ret = gctx_.root_service_->check_backup_scheduler_working(result_);
  }
  return ret;
}

int ObRpcValidateBackupBatchP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->validate_backup_batch(arg_);
  }
  return ret;
}

int ObRpcSetConfigP::process()
{
  LOG_INFO("process set config", K(arg_));
  GCONF.add_extra_config(arg_.ptr());
  GCTX.config_mgr_->reload_config();
  return OB_SUCCESS;
}

int ObRpcGetConfigP::process()
{
  return OB_SUCCESS;
}

int ObRpcNotifyTenantServerUnitResourceP::process()
{
  int ret = OB_SUCCESS;
  if (arg_.is_delete_) {
    if (OB_FAIL(ObTenantNodeBalancer::get_instance().try_notify_drop_tenant(arg_.tenant_id_))) {
      LOG_WARN("fail to try drop tenant", K(ret), K(arg_));
    }
  } else {
    if (OB_FAIL(ObTenantNodeBalancer::get_instance().notify_create_tenant(arg_))) {
      LOG_WARN("failed to notify update tenant", K(ret), K_(arg));
    }
  }
  return ret;
}

int ObReachPartitionLimitP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->reach_partition_limit(arg_);
  }
  return ret;
}

int ObCheckFrozenVersionP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->check_frozen_version(arg_);
  }
  return ret;
}

int ObRpcAddTenantTmpP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.omt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.omt_), K(ret));
  } else {
    if (OB_FAIL(gctx_.omt_->add_tenant(arg_, MIN_TENANT_QUOTA, MIN_TENANT_QUOTA))) {
      if (ret == OB_TENANT_EXIST) {
        // tenant has added before, we just return success.
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("add tenant tmp fail", "tenant_id", arg_, K(ret));
      }
    }
  }
  return ret;
}

int ObGetMinSSTableSchemaVersionP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->get_min_sstable_schema_version(arg_, result_);
  }
  return ret;
}

// oceanbase service provided
int ObRpcCreatePartitionP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->create_partition(arg_);
  }
  return ret;
}

int ObRpcCreatePartitionBatchP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->create_partition_batch(arg_, result_);
  }
  return ret;
}

int ObCheckUniqueIndexRequestP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arguments", K(ret), KP(gctx_.ob_service_));
  } else {
    ret = gctx_.ob_service_->check_unique_index_request(arg_);
  }
  return ret;
}

int ObCalcColumnChecksumRequestP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arguments", K(ret), KP(gctx_.ob_service_));
  } else {
    ret = gctx_.ob_service_->calc_column_checksum_request(arg_);
  }
  return ret;
}

int ObCheckSingleReplicaMajorSSTableExistP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arguments", K(ret), KP(gctx_.ob_service_));
  } else {
    ret = gctx_.ob_service_->check_single_replica_major_sstable_exist(arg_);
  }
  return ret;
}

int ObCheckSingleReplicaMajorSSTableExistWithTimeP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(gctx_.ob_service_));
  } else {
    ret = gctx_.ob_service_->check_single_replica_major_sstable_exist(arg_, result_);
  }
  return ret;
}

int ObCheckAllReplicaMajorSSTableExistP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arguments", K(ret), KP(gctx_.ob_service_));
  } else {
    ret = gctx_.ob_service_->check_all_replica_major_sstable_exist(arg_);
  }
  return ret;
}

int ObCheckAllReplicaMajorSSTableExistWithTimeP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arguments", K(ret), KP(gctx_.ob_service_));
  } else {
    ret = gctx_.ob_service_->check_all_replica_major_sstable_exist(arg_, result_);
  }
  return ret;
}

int ObRpcFetchRootPartitionP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->fetch_root_partition(result_);
  }
  return ret;
}

int ObRpcBroadcastRsListP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->broadcast_rs_list(arg_);
  }
  return ret;
}

int ObRpcAddReplicaP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->add_replica(arg_, arg_.task_id_);
  }
  return ret;
}

int ObRpcGetTenantLogArchiveStatusP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->get_tenant_log_archive_status(arg_, result_);
  }
  return ret;
}

int ObRpcRebuildReplicaP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->rebuild_replica(arg_, arg_.task_id_);
  }
  return ret;
}

int ObRpcRestoreReplicaP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->restore_replica(arg_, arg_.task_id_);
  }
  return ret;
}

int ObRpcPhysicalRestoreReplicaP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->physical_restore_replica(arg_, arg_.task_id_);
  }
  return ret;
}

int ObRpcChangeReplicaP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->change_replica(arg_, arg_.task_id_);
  }
  return ret;
}

int ObRpcRemoveNonPaxosReplicaP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->remove_non_paxos_replica(arg_);
  }
  return ret;
}

int ObRpcRemoveMemberP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->remove_member(arg_);
  }
  return ret;
}

int ObRpcMigrateReplicaP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->migrate_replica(arg_, arg_.task_id_);
  }
  return ret;
}

int ObRpcAddReplicaBatchP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->add_replica_batch(arg_);
  }
  return ret;
}

int ObRpcRebuildReplicaBatchP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->rebuild_replica_batch(arg_);
  }
  return ret;
}

int ObRpcCopySSTableBatchP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->copy_sstable_batch(arg_);
  }
  return ret;
}

int ObRpcBackupReplicaBatchP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->backup_replica_batch(arg_);
  }
  return ret;
}

int ObRpcCheckBackupTaskExistP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(ret), K(gctx_.ob_service_));
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("check backup task exist rpc do not supported", K(ret));
  }
  return ret;
}

int ObRpcGetRoleP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.root_service_)) {
    LOG_ERROR("invalid argument", K(gctx_.root_service_));
  } else {
    ret = gctx_.ob_service_->get_root_server_status(result_);
  }
  return ret;
}

int ObRpcCheckMigrateTaskExistP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(ret), K(gctx_.ob_service_));
  } else {
    bool is_exist = false;
    ret = gctx_.ob_service_->check_migrate_task_exist(arg_, is_exist);
    result_ = is_exist;
  }
  return ret;
}

int ObRpcChangeReplicaBatchP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->change_replica_batch(arg_);
  }
  return ret;
}

int ObRpcRemoveNonPaxosReplicaBatchP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->remove_non_paxos_replica_batch(arg_, result_);
  }
  return ret;
}

int ObRpcRemoveMemberBatchP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->remove_member_batch(arg_, result_);
  }
  return ret;
}

int ObRpcModifyQuorumBatchP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->modify_quorum_batch(arg_, result_);
  }
  return ret;
}

int ObRpcMigrateReplicaBatchP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->migrate_replica_batch(arg_);
  }
  return ret;
}

int ObRpcStandbyCutdataBatchTaskP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), KR(ret));
  } else {
    ret = gctx_.ob_service_->standby_cutdata_batch_task(arg_);
  }
  return ret;
}

int ObRpcMinorFreezeP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->minor_freeze(arg_, result_);
  }
  return ret;
}

int ObRpcCheckSchemaVersionElapsedP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(ret), K(gctx_.ob_service_));
  } else {
    ret = gctx_.ob_service_->check_schema_version_elapsed(arg_, result_);
  }
  return ret;
}

int ObRpcCheckCtxCreateTimestampElapsedP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(ret), K(gctx_.ob_service_));
  } else {
    ret = gctx_.ob_service_->check_ctx_create_timestamp_elapsed(arg_, result_);
  }
  return ret;
}

int ObRpcGetMemberListP::process()
{
  return OB_SUCCESS;
}

int ObRpcSwitchLeaderP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->switch_leader(arg_);
  }
  return ret;
}

int ObRpcBatchSwitchRsLeaderP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), KR(ret));
  } else {
    ret = gctx_.ob_service_->batch_switch_rs_leader(arg_);
  }
  return ret;
}

int ObRpcSwitchLeaderListAsyncP::process()
{
  int ret = OB_SUCCESS;
  // ignore result_
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else if (OB_FAIL(gctx_.ob_service_->switch_leader_list(arg_))) {
    LOG_WARN("failed to switch leader list", K(ret), K(arg_));
  }
  return ret;
}

int ObRpcSwitchLeaderListP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else if (OB_FAIL(gctx_.ob_service_->switch_leader_list(arg_))) {
    LOG_WARN("failed to switch leader list", K(ret), K(arg_));
  }
  return ret;
}

int ObRpcGetLeaderCandidatesP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->get_leader_candidates(arg_, result_);
  }
  return ret;
}

int ObRpcGetLeaderCandidatesAsyncP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else if (OB_FAIL(gctx_.ob_service_->get_leader_candidates(arg_, result_))) {
    LOG_WARN("fail to get leader candidates", K(ret));
  }
  return ret;
}

int ObRpcGetLeaderCandidatesAsyncV2P::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else if (OB_FAIL(gctx_.ob_service_->get_leader_candidates_v2(arg_, result_))) {
    LOG_WARN("fail to get leader candidates", K(ret));
  }
  return ret;
}

int ObRpcGetPartitionCountP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else if (OB_FAIL(gctx_.ob_service_->get_partition_count(result_))) {
    LOG_WARN("failed to get partition count", K(ret));
  }
  return ret;
}

int ObRpcSwitchSchemaP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->switch_schema(arg_);
  }
  return ret;
}

int ObRpcRefreshMemStatP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->refresh_memory_stat();
  }
  return ret;
}

int ObRpcBootstrapP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->bootstrap(arg_);
  }
  return ret;
}

int ObRpcIsEmptyServerP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->is_empty_server(arg_, result_);
  }
  return ret;
}

int ObRpcCheckDeploymentModeP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(ret), K(gctx_.ob_service_));
  } else {
    ret = gctx_.ob_service_->check_deployment_mode_match(arg_, result_);
  }
  return ret;
}

int ObRpcGetPartitionStatP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->get_partition_stat(result_);
  }
  return ret;
}

int ObRpcSyncAutoincValueP::process()
{
  return ObAutoincrementService::get_instance().refresh_sync_value(arg_);
}

int ObRpcClearAutoincCacheP::process()
{
  return ObAutoincrementService::get_instance().clear_autoinc_cache(arg_);
}

int ObDumpMemtableP::process()
{
  int ret = OB_SUCCESS;
  ObIPartitionStorage* storage = NULL;
  ObTablesHandle stores_handle;
  ObIPartitionGroupGuard guard;
  ObPGPartitionGuard pg_partition_guard;
  TRANS_LOG(INFO, "dump_memtable");
  if (OB_FAIL(gctx_.par_ser_->get_partition(arg_, guard)) || NULL == guard.get_partition_group()) {
    COMMON_LOG(WARN, "get_partition_storage fail", "pkey", arg_);
    ret = OB_ENTRY_NOT_EXIST;
  } else if (OB_FAIL(guard.get_partition_group()->get_pg_partition(arg_, pg_partition_guard))) {
    COMMON_LOG(WARN, "get pg partition fail", K(ret), "pkey", arg_);
  } else if (OB_ISNULL(storage = pg_partition_guard.get_pg_partition()->get_storage())) {
    COMMON_LOG(WARN, "get_partition_storage fail", "pkey", arg_);
  } else if (OB_FAIL(storage->get_all_tables(stores_handle))) {
    COMMON_LOG(WARN, "get_all_stores fail", K(ret), "pkey", arg_);
  } else {
    const ObIArray<ObITable*>& stores = stores_handle.get_tables();
    for (int64_t i = 0; OB_SUCC(ret) && i < stores.count(); ++i) {
      ObITable* store = stores.at(i);
      if (NULL != store && store->is_memtable()) {
        mkdir("/tmp/dump_memtable/", S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
        ((ObMemtable*)store)->dump2text("/tmp/dump_memtable/memtable.txt");
      }
    }
  }
  return ret;
}

int ObHaltPrewarmP::process()
{
  int ret = OB_SUCCESS;
  ObPartitionService* ps = gctx_.par_ser_;
  TRANS_LOG(INFO, "halt_prewarm");
  if (OB_UNLIKELY(NULL == ps)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "partition_service is null");
  } else if (OB_FAIL(ps->halt_all_prewarming())) {
    COMMON_LOG(WARN, "halt_all_prewarming fail", K(ret));
  }

  return ret;
}

int ObHaltPrewarmAsyncP::process()
{
  int ret = OB_SUCCESS;
  ObPartitionService* ps = gctx_.par_ser_;
  TRANS_LOG(INFO, "halt_prewarm");
  if (OB_UNLIKELY(NULL == ps)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "partition_service is null");
  } else if (OB_FAIL(ps->halt_all_prewarming(arg_))) {
    COMMON_LOG(WARN, "halt_all_prewarming fail", K(ret));
  }

  return ret;
}

int ObForceSetAsSingleReplicaP::process()
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;
  ObPartitionLogService* log_service = NULL;
  TRANS_LOG(INFO, "set_as_single_replica", "pkey", to_cstring(arg_));
  if (OB_FAIL(gctx_.par_ser_->get_partition(arg_, guard)) || NULL == (partition = guard.get_partition_group()) ||
      NULL == (log_service = reinterpret_cast<ObPartitionLogService*>(partition->get_log_service()))) {
    COMMON_LOG(WARN, "get_partition fail");
    ret = OB_ENTRY_NOT_EXIST;
  } else if (OB_FAIL(log_service->force_set_as_single_replica())) {
    COMMON_LOG(WARN, "set_as_single_replica fail", K(ret));
  }
  return ret;
}

int ObForceRemoveReplicaP::process()
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  TRANS_LOG(INFO, "force_remove_replica", "pkey", to_cstring(arg_));
  if (OB_FAIL(gctx_.par_ser_->remove_partition(arg_))) {
    COMMON_LOG(WARN, "remove_partition fail");
  }
  return ret;
}

int ObForceSetReplicaNumP::process()
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;
  ObPartitionLogService* log_service = NULL;
  TRANS_LOG(
      INFO, "set_replica_num", "pkey", to_cstring(arg_.partition_key_), "replica_num", to_cstring(arg_.replica_num_));
  if (OB_FAIL(gctx_.par_ser_->get_partition(arg_.partition_key_, guard)) ||
      NULL == (partition = guard.get_partition_group()) ||
      NULL == (log_service = reinterpret_cast<ObPartitionLogService*>(partition->get_log_service()))) {
    COMMON_LOG(WARN, "get_partition fail");
    ret = OB_ENTRY_NOT_EXIST;
  } else if (OB_FAIL(log_service->force_set_replica_num(arg_.replica_num_))) {
    COMMON_LOG(WARN, "set_as_single_replica fail", K(ret));
  }
  return ret;
}

int ObForceResetParentP::process()
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;
  ObPartitionLogService* log_service = NULL;
  TRANS_LOG(INFO, "force_reset_parent", "pkey", to_cstring(arg_));
  if (OB_FAIL(gctx_.par_ser_->get_partition(arg_, guard)) || NULL == (partition = guard.get_partition_group()) ||
      NULL == (log_service = reinterpret_cast<ObPartitionLogService*>(partition->get_log_service()))) {
    COMMON_LOG(WARN, "get_partition fail");
    ret = OB_ENTRY_NOT_EXIST;
  } else if (OB_FAIL(log_service->force_reset_parent())) {
    COMMON_LOG(WARN, "force_reset_parent fail", K(ret));
  }
  return ret;
}

int ObForceSetParentP::process()
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;
  ObPartitionLogService* log_service = NULL;
  TRANS_LOG(
      INFO, "ObForceSetParentP", "pkey", to_cstring(arg_.partition_key_), "parent", to_cstring(arg_.parent_addr_));
  if (OB_FAIL(gctx_.par_ser_->get_partition(arg_.partition_key_, guard)) ||
      NULL == (partition = guard.get_partition_group()) ||
      NULL == (log_service = reinterpret_cast<ObPartitionLogService*>(partition->get_log_service()))) {
    COMMON_LOG(WARN, "get_partition fail");
    ret = OB_ENTRY_NOT_EXIST;
  } else if (OB_FAIL(log_service->force_set_parent(arg_.parent_addr_))) {
    COMMON_LOG(WARN, "ObForceSetParentP fail", K(ret));
  }
  return ret;
}

int ObReportReplicaP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->report_replica();
  }
  return ret;
}

int ObReportSingleReplicaP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->report_replica(arg_);
  }
  return ret;
}

int ObFlushCacheP::process()
{
  int ret = OB_SUCCESS;
  switch (arg_.cache_type_) {
    case CACHE_TYPE_PLAN: {
      ObPlanCacheManager* pcm = NULL;
      if (OB_ISNULL(gctx_.sql_engine_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
      } else if (NULL == (pcm = gctx_.sql_engine_->get_plan_cache_manager())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("invalid argument", K(pcm), K(ret));
      } else if (arg_.is_all_tenant_) {  // flush all tenant cache
        ret = pcm->flush_all_plan_cache();
      } else {  // flush appointed tenant cache
        ret = pcm->flush_plan_cache(arg_.tenant_id_);
      }
      break;
    }
    case CACHE_TYPE_SQL_AUDIT: {
      if (arg_.is_all_tenant_) {  // flush all tenant sql audit
        TenantIdList id_list(16);
        if (OB_ISNULL(GCTX.omt_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null of omt", K(ret));
        } else {
          GCTX.omt_->get_tenant_ids(id_list);
        }
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCC(ret)) {
          for (int64_t i = 0; i < id_list.size(); i++) {  // ignore ret
            FETCH_ENTITY(TENANT_SPACE, id_list.at(i))
            {
              ObMySQLRequestManager* req_mgr = MTL_GET(ObMySQLRequestManager*);
              if (nullptr == req_mgr) {
                // do-nothing
                // virtual tenant such as 50x do not maintain tenant local object, hence req_mgr could be null.
              } else {
                req_mgr->clear_queue();
              }
            }
            tmp_ret = ret;
          }
        }
        ret = tmp_ret;
      } else {  // flush specified tenant sql audit
        FETCH_ENTITY(TENANT_SPACE, arg_.tenant_id_)
        {
          ObMySQLRequestManager* req_mgr = MTL_GET(ObMySQLRequestManager*);
          if (nullptr == req_mgr) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to get request manager", K(ret), K(req_mgr));
          } else {
            req_mgr->clear_queue();
          }
        }
      }
      break;
    }
    case CACHE_TYPE_PL_OBJ: {
      ObPlanCacheManager* pcm = NULL;
      if (OB_ISNULL(gctx_.sql_engine_) || OB_ISNULL(pcm = gctx_.sql_engine_->get_plan_cache_manager())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("invalid aargument", K(ret));
      } else if (arg_.is_all_tenant_) {
        ret = pcm->flush_all_pl_cache();
      } else {
        ret = pcm->flush_pl_cache(arg_.tenant_id_);
      }
      break;
    }
    case CACHE_TYPE_PS_OBJ: {
      ObPlanCacheManager* pcm = NULL;
      if (OB_ISNULL(gctx_.sql_engine_) || OB_ISNULL(pcm = gctx_.sql_engine_->get_plan_cache_manager())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("invalid aargument", K(ret));
      } else if (arg_.is_all_tenant_) {
        ret = pcm->flush_all_ps_cache();
      } else {
        ret = pcm->flush_ps_cache(arg_.tenant_id_);
      }
      break;
    }
    case CACHE_TYPE_SCHEMA: {
      // this option is only used for upgrade now
      if (arg_.is_all_tenant_) {
        if (OB_FAIL(common::ObKVGlobalCache::get_instance().erase_cache(OB_SCHEMA_CACHE_NAME))) {
          LOG_WARN("clear kv cache  failed", K(ret));
        } else {
          LOG_INFO("success erase kvcache", K(ret), K(OB_SCHEMA_CACHE_NAME));
        }
      } else {
        if (OB_FAIL(common::ObKVGlobalCache::get_instance().erase_cache(arg_.tenant_id_, OB_SCHEMA_CACHE_NAME))) {
          LOG_WARN("clear kv cache failed", K(ret));
        } else {
          LOG_INFO("success erase kvcache", K(ret), K(arg_.tenant_id_), K(OB_SCHEMA_CACHE_NAME));
        }
      }
      break;
    }
    case CACHE_TYPE_ALL:
    case CACHE_TYPE_COLUMN_STAT:
    case CACHE_TYPE_BLOCK_INDEX:
    case CACHE_TYPE_BLOCK:
    case CACHE_TYPE_ROW:
    case CACHE_TYPE_BLOOM_FILTER:
    case CACHE_TYPE_LOCATION:
    case CACHE_TYPE_CLOG:
    case CACHE_TYPE_ILOG: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("cache type not supported flush", "type", arg_.cache_type_, K(ret));
    } break;
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid cache type", "type", arg_.cache_type_);
    }
  }
  return ret;
}

int ObRecycleReplicaP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->recycle_replica();
  }
  return ret;
}

int ObClearLocationCacheP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->clear_location_cache();
  }
  return ret;
}

int ObDropReplicaP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->drop_replica(arg_);
  }
  return ret;
}

int ObSetDSActionP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->set_ds_action(arg_);
  }
  return ret;
}

int ObRequestHeartbeatP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->request_heartbeat(result_);
  }
  return ret;
}

int ObUpdateClusterInfoP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->update_cluster_info(arg_);
  }
  return ret;
}
int ObBroadcastSysSchemaP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->broadcast_sys_schema(arg_);
  }
  return ret;
}

int ObCheckPartitionTableP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->check_partition_table();
  }
  return ret;
}

int ObRpcGetMemberListAndLeaderP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->get_member_list_and_leader(arg_, result_);
  }
  return ret;
}

int ObRpcGetMemberListAndLeaderV2P::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->get_member_list_and_leader_v2(arg_, result_);
  }
  return ret;
}

int ObRpcBatchGetMemberListAndLeaderP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->batch_get_member_list_and_leader(arg_, result_);
  }
  return ret;
}

int ObRpcCheckNeedOffineReplicaP::process()
{
  int ret = OB_SUCCESS;
  ObPartitionService* ps = gctx_.par_ser_;
  if (OB_UNLIKELY(NULL == ps)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "partition_service is null");
  } else if (OB_FAIL(ps->check_has_need_offline_replica(arg_, result_))) {
    COMMON_LOG(WARN, "failed to check have need offline replica", KR(ret));
  }
  return ret;
}

int ObRpcCheckFlashbackInfoDumpP::process()
{
  int ret = OB_SUCCESS;
  ObPartitionService* ps = gctx_.par_ser_;
  if (OB_ISNULL(ps)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "partition_service is null", KR(ret));
  }
  return ret;
}

int ObRpcBatchGetRoleP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->batch_get_role(arg_, result_);
  }
  return ret;
}

int ObSyncPGPartitionMTP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->sync_pg_partition_table(arg_);
  }
  return ret;
}

int ObSyncPartitionTableP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->sync_partition_table(arg_);
  }
  return ret;
}

int ObCheckDanglingReplicaExistP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->check_dangling_replica_exist(arg_);
  }
  return ret;
}

int ObGetDiagnoseArgsP::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(GCTX.diag_->refresh_passwd(passwd_))) {
    LOG_ERROR("refresh passwd fail", K(ret));
  } else {
    int64_t pos = 0;
    if (OB_FAIL(databuff_printf(argsbuf_,
            sizeof(argsbuf_),
            pos,
            "-h127.0.0.1 -P%ld -u@diag -p%s",
            GCONF.mysql_port.get(),
            passwd_.ptr()))) {
      LOG_ERROR("construct arguments fail", K(ret));
    } else {
      result_.assign_ptr(argsbuf_, static_cast<ObString::obstr_size_t>(STRLEN(argsbuf_)));
    }
  }
  return ret;
}

int ObRpcSetTPP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->set_tracepoint(arg_);
  }
  return ret;
}
int ObCancelSysTaskP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->cancel_sys_task(arg_.task_id_);
  }
  return ret;
}

int ObSetDiskValidP::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIOManager::get_instance().reset_disk_error())) {
    LOG_WARN("reset_disk_error failed", K(ret));
  }
  return ret;
}

int ObAddDiskP::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_FILE_SYSTEM.add_disk(arg_.diskgroup_name_, arg_.disk_path_, arg_.alias_name_))) {
    LOG_WARN("failed to add disk", K(ret), K(arg_));
  }
  return ret;
}

int ObDropDiskP::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_FILE_SYSTEM.drop_disk(arg_.diskgroup_name_, arg_.alias_name_))) {
    LOG_WARN("failed to drop disk", K(ret), K(arg_));
  }
  return ret;
}

int ObForceSwitchILogFileP::process()
{
  int ret = OB_SUCCESS;
  ObICLogMgr* clog_mgr = NULL;
  TRANS_LOG(INFO, "force_switch_ilog_file");
  if (NULL == (clog_mgr = gctx_.par_ser_->get_clog_mgr())) {
    ret = OB_ENTRY_NOT_EXIST;
    COMMON_LOG(WARN, "get_clog_mgr failed", K(ret));
    //} else if (OB_FAIL(clog_mgr->force_switch_ilog_file())) {
    //  COMMON_LOG(WARN, "force_switch_ilog_file failed", K(ret));
  }
  return ret;
}

int ObForceSetAllAsSingleReplicaP::process()
{
  int ret = OB_SUCCESS;
  ObPartitionService* partition_service = gctx_.par_ser_;
  TRANS_LOG(INFO, "force_set_all_as_single_replica");
  if (NULL == partition_service) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "partition_service is null");
  } else {
    storage::ObIPartitionGroupIterator* partition_iter = NULL;
    if (NULL == (partition_iter = partition_service->alloc_pg_iter())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(ERROR, "partition_mgr alloc_scan_iter failed", K(ret));
    } else {
      storage::ObIPartitionGroup* partition = NULL;
      ObIPartitionLogService* pls = NULL;
      while (OB_SUCC(ret)) {
        int tmp_ret = OB_SUCCESS;
        if (OB_FAIL(partition_iter->get_next(partition)) || NULL == partition) {
          TRANS_LOG(INFO, "get_next failed or partition is NULL", K(ret));
        } else if (!partition->is_valid() || (NULL == (pls = partition->get_log_service()))) {
          TRANS_LOG(INFO, "partition is valid or pls is NULL", "partition_key", partition->get_partition_key());
        } else if (OB_SUCCESS != (tmp_ret = pls->force_set_as_single_replica())) {
          TRANS_LOG(WARN,
              "force_set_as_single_replica failed",
              K(ret),
              K(tmp_ret),
              "partition_key",
              partition->get_partition_key());
        }
      }
    }
    if (NULL != partition_iter) {
      partition_service->revert_pg_iter(partition_iter);
      partition_iter = NULL;
    }
  }
  return ret;
}

int ObRpcBatchSplitPartitionP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->split_partition(arg_, result_);
  }
  return ret;
}
int ObRpcSplitPartitionP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->split_partition(arg_, result_);
  }
  return ret;
}

int ObQueryMaxDecidedTransVersionP::process()
{
  int ret = OB_SUCCESS;

  const obrpc::ObQueryMaxDecidedTransVersionRequest& arg = arg_;
  obrpc::ObQueryMaxDecidedTransVersionResponse& result = result_;
  ObPartitionService* partition_service = gctx_.par_ser_;
  int64_t min_trans_version = INT64_MAX;
  ObPartitionKey pkey;

  if (OB_ISNULL(partition_service)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    for (int64_t index = 0; OB_SUCC(ret) && index < arg.partition_array_.count(); index++) {
      const common::ObPartitionKey partition_key = arg.partition_array_[index];
      storage::ObIPartitionGroupGuard guard;
      int64_t tmp_value = 0;
      if (OB_FAIL(partition_service->get_partition(partition_key, guard))) {
        STORAGE_LOG(WARN, "get_partition failed", K(ret), K(partition_key));
      } else if (OB_ISNULL(guard.get_partition_group())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "partition is NULL", K(ret), K(partition_key));
      } else if ((guard.get_partition_group()->get_pg_storage().is_restore())) {
        // skip partitions that in restore
      } else if (OB_FAIL(guard.get_partition_group()->get_max_decided_trans_version(tmp_value))) {
        STORAGE_LOG(WARN, "get_max_decided_trans_version failed", K(ret), K(partition_key));
      } else if (tmp_value < arg.last_max_decided_trans_version_) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR,
            "global_max_decided_trans_version_ change smaller, unexpected",
            K(ret),
            K(arg),
            K(tmp_value),
            K(partition_key));
      } else if (tmp_value < min_trans_version) {
        min_trans_version = tmp_value;
        pkey = partition_key;
      }
    }
  }

  if (OB_SUCC(ret)) {
    result.trans_version_ = min_trans_version;
    result.pkey_ = pkey;
  }
  result.ret_value_ = ret;
  ret = OB_SUCCESS;

  return ret;
}

int ObQueryIsValidMemberP::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  const obrpc::ObQueryIsValidMemberRequest& arg = arg_;
  const common::ObAddr& addr = arg_.self_addr_;
  obrpc::ObQueryIsValidMemberResponse& response = result_;
  ObPartitionService* partition_service = gctx_.par_ser_;

  if (OB_ISNULL(partition_service)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    for (int64_t index = 0; OB_SUCC(ret) && index < arg.partition_array_.count(); index++) {
      const common::ObPartitionKey& partition_key = arg.partition_array_[index];
      bool is_valid_member = true;
      storage::ObIPartitionGroupGuard guard;
      storage::ObIPartitionGroup* partition = NULL;
      ObIPartitionLogService* pls = NULL;
      if (OB_SUCCESS != (tmp_ret = partition_service->get_partition(partition_key, guard))) {
        STORAGE_LOG(WARN, "get_partition failed", K(ret), K(partition_key));
      } else if (NULL == (partition = guard.get_partition_group()) || !partition->is_valid() ||
                 (NULL == (pls = partition->get_log_service()))) {
        tmp_ret = OB_PARTITION_NOT_EXIST;
      } else if (OB_SUCCESS != (tmp_ret = pls->is_valid_member(addr, is_valid_member))) {
        if (REACH_TIME_INTERVAL(100 * 1000)) {
          STORAGE_LOG(WARN, "is_valid_member failed", K(ret), K(partition_key), K(addr));
        }
      } else {
      }

      if (OB_FAIL(response.partition_array_.push_back(partition_key))) {
        STORAGE_LOG(WARN, "response partition_array_ push_back failed", K(ret), K(partition_key));
      } else if (OB_FAIL(response.ret_array_.push_back(tmp_ret))) {
        STORAGE_LOG(WARN, "response ret_array push_back failed", K(ret), K(partition_key), K(tmp_ret));
      } else if (OB_FAIL(response.candidates_status_.push_back(is_valid_member))) {
        STORAGE_LOG(WARN, "response candidates_status_ push_back failed", K(ret), K(partition_key));
      } else {
        // do nothing
      }
    }
  }

  response.ret_value_ = ret;
  ret = OB_SUCCESS;
  return ret;
}

int ObQueryMaxFlushedILogIdP::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  const obrpc::ObQueryMaxFlushedILogIdRequest& arg = arg_;
  obrpc::ObQueryMaxFlushedILogIdResponse& response = result_;
  ObPartitionService* partition_service = gctx_.par_ser_;

  if (OB_ISNULL(partition_service)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    for (int64_t index = 0; OB_SUCC(ret) && index < arg.partition_array_.count(); index++) {
      const common::ObPartitionKey& partition_key = arg.partition_array_[index];
      uint64_t max_flushed_ilog_id = 0;
      ObIPartitionGroupGuard guard;
      ObIPartitionGroup* partition = NULL;
      ObPartitionLogService* log_service = NULL;
      if (OB_SUCCESS != (tmp_ret = partition_service->get_partition(partition_key, guard)) ||
          NULL == (partition = guard.get_partition_group()) ||
          NULL == (log_service = reinterpret_cast<ObPartitionLogService*>(partition->get_log_service()))) {
        COMMON_LOG(WARN, "get partition fail", K(tmp_ret), K(partition_key), K(partition), K(log_service), K(index));
      } else if (OB_SUCCESS != (tmp_ret = log_service->query_max_flushed_ilog_id(max_flushed_ilog_id))) {
        COMMON_LOG(WARN,
            "query_max_flushed_ilog_id fail",
            K(tmp_ret),
            K(partition_key),
            K(partition),
            K(log_service),
            K(index));
      } else if (OB_FAIL(response.partition_array_.push_back(partition_key))) {
        COMMON_LOG(WARN, "response partition_array_ push_back failed", K(ret), K(partition_key), K(index));
      } else if (OB_FAIL(response.max_flushed_ilog_ids_.push_back(max_flushed_ilog_id))) {
        COMMON_LOG(WARN, "response partition_array_ push_back failed", K(ret), K(partition_key), K(index));
      } else {
        // do nothing
      }
    }
  }
  COMMON_LOG(INFO,
      "ObQueryMaxFlushedILogIdP",
      K(arg),
      "partition count",
      arg.partition_array_.count(),
      K(response),
      K(tmp_ret),
      K(ret));
  response.err_code_ = ret;
  ret = OB_SUCCESS;

  return ret;
}

int ObUpdateLocalStatCacheP::process()
{
  int ret = OB_SUCCESS;
  ObOptStatManager& stat_manager = ObOptStatManager::get_instance();
  if (OB_FAIL(stat_manager.add_refresh_stat_task(arg_))) {
    LOG_WARN("failed to update local statistic cache", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObForceDisableBlacklistP::process()
{
  int ret = OB_SUCCESS;
  share::ObServerBlacklist::get_instance().disable_blacklist();
  COMMON_LOG(INFO, "disable_blacklist finished", K(ret));
  return ret;
}

int ObForceEnableBlacklistP::process()
{
  int ret = OB_SUCCESS;
  share::ObServerBlacklist::get_instance().enable_blacklist();
  COMMON_LOG(INFO, "enable_blacklist finished", K(ret));
  return ret;
}

int ObForceClearBlacklistP::process()
{
  int ret = OB_SUCCESS;
  share::ObServerBlacklist::get_instance().clear_blacklist();
  COMMON_LOG(INFO, "clear_black_list finished", K(ret));
  return ret;
}

int ObBatchTranslatePartitionP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->batch_set_member_list(arg_, result_);
  }
  return ret;
}

int ObBatchWaitLeaderP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->batch_wait_leader(arg_, result_);
  }
  return ret;
}

int ObBatchWriteCutdataClogP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), KR(ret));
  } else {
    ret = gctx_.ob_service_->batch_write_cutdata_clog(arg_, result_);
  }
  return ret;
}

int ObCheckPartitionLogP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->check_partition_log(arg_, result_);
  }
  return ret;
}

int ObStopPartitionWriteP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->stop_partition_write(arg_, result_);
  }
  return ret;
}

int ObEstimatePartitionRowsP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else {
    ret = gctx_.ob_service_->estimate_partition_rows(arg_, result_);
  }
  return ret;
}

int ObGetWRSInfoP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(ret));
  } else if (OB_FAIL(gctx_.ob_service_->get_wrs_info(arg_, result_))) {
    LOG_WARN("failed to get cluster info", K(ret));
  }
  return ret;
}

int ObHaGtsPingRequestP::process()
{
  int ret = OB_SUCCESS;
  ObPartitionService* ps = gctx_.par_ser_;
  if (OB_UNLIKELY(NULL == ps)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition_service is NULL", K(ret), K(arg_));
  } else if (OB_FAIL(ps->handle_ha_gts_ping_request(arg_, result_))) {
    LOG_WARN("handle_ha_gts_ping_request failed", K(ret), K(arg_), K(result_));
  }
  return ret;
}

int ObHaGtsGetRequestP::process()
{
  int ret = OB_SUCCESS;
  ObPartitionService* ps = gctx_.par_ser_;
  if (OB_UNLIKELY(NULL == ps)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition_service is NULL", K(ret), K(arg_));
  } else if (OB_FAIL(ps->handle_ha_gts_get_request(arg_))) {
    LOG_WARN("handle_ha_get_gts_request failed", K(ret), K(arg_));
  }
  return ret;
}

int ObHaGtsGetResponseP::process()
{
  int ret = OB_SUCCESS;
  ObPartitionService* ps = gctx_.par_ser_;
  if (OB_UNLIKELY(NULL == ps)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition_service is NULL", K(ret), K(arg_));
  } else if (OB_FAIL(ps->handle_ha_gts_get_response(arg_))) {
    LOG_WARN("handle_ha_gts_get_response failed", K(ret), K(arg_));
  } else {
    // do nothing
  }
  return ret;
}

int ObHaGtsHeartbeatP::process()
{
  int ret = OB_SUCCESS;
  ObPartitionService* ps = gctx_.par_ser_;
  if (OB_UNLIKELY(NULL == ps)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition_service is NULL", K(ret), K(arg_));
  } else if (OB_FAIL(ps->handle_ha_gts_heartbeat(arg_))) {
    LOG_WARN("handle_ha_gts_heartbeat failed", K(ret), K(arg_));
  } else {
    // do nothing
  }
  return ret;
}

int ObGetTenantSchemaVersionP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arguments", K(ret), KP(gctx_.ob_service_));
  } else {
    ret = gctx_.ob_service_->get_tenant_refreshed_schema_version(arg_, result_);
  }
  return ret;
}

int ObHaGtsUpdateMetaP::process()
{
  int ret = OB_SUCCESS;
  ObPartitionService* ps = gctx_.par_ser_;
  const obrpc::ObHaGtsUpdateMetaRequest& arg = arg_;
  obrpc::ObHaGtsUpdateMetaResponse& response = result_;
  if (OB_UNLIKELY(NULL == ps)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition_service is NULL", K(ret), K(arg));
  } else if (OB_FAIL(ps->handle_ha_gts_update_meta(arg, response))) {
    LOG_WARN("handle_ha_gts_update_meta failed", K(ret), K(arg));
  } else {
    // do nothing
  }
  return ret;
}

int ObHaGtsChangeMemberP::process()
{
  int ret = OB_SUCCESS;
  ObPartitionService* ps = gctx_.par_ser_;
  const obrpc::ObHaGtsChangeMemberRequest& arg = arg_;
  obrpc::ObHaGtsChangeMemberResponse& response = result_;
  if (OB_UNLIKELY(NULL == ps)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition_service is NULL", K(ret), K(arg));
  } else if (OB_FAIL(ps->handle_ha_gts_change_member(arg, response))) {
    LOG_WARN("handle_ha_gts_change_member failed", K(ret), K(arg));
  } else {
    // do nothing
  }
  return ret;
}

int ObSetMemberListBatchP::process()
{
  int ret = OB_SUCCESS;
  ObPartitionService* ps = static_cast<ObPartitionService*>(gctx_.par_ser_);
  if (OB_UNLIKELY(NULL == ps)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition_service is NULL", K(ret), K(arg_));
  } else if (OB_FAIL(ps->set_member_list(arg_, result_))) {
    LOG_WARN("persist member list failed", K(ret), K(arg_), K(result_));
  }
  return ret;
}

int ObRpcBatchGetProtectionLevelP::process()
{
  int ret = OB_SUCCESS;
  ObPartitionService* ps = static_cast<ObPartitionService*>(gctx_.par_ser_);
  if (OB_UNLIKELY(NULL == ps)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition_service is NULL", K(ret), K(arg_));
  } else if (OB_FAIL(ps->batch_get_protection_level(arg_, result_))) {
    LOG_WARN("get protection level failed", K(ret), K(arg_), K(result_));
  }
  return ret;
}
int ObGetRemoteTenantGroupStringP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid argument", K(ret));
  } else {
    ret = gctx_.ob_service_->get_tenant_group_string(result_);
  }
  return ret;
}

int ObUpdateTenantMemoryP::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(omt::ObTenantNodeBalancer::get_instance().update_tenant_memory(arg_))) {
    LOG_WARN("failed to update tenant memory", K(ret), K_(arg));
  }
  return ret;
}

int ObRpcGetMasterRSP::process()
{
  int ret = OB_SUCCESS;
  bool found = false;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("observer is null", K(ret));
  } else if (OB_FAIL(gctx_.ob_service_->get_master_root_server(result_))) {
    LOG_WARN("failed to get master root server", K(ret));
  }
  return ret;
}

int ObCheckPhysicalFlashbackSUCCP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("observer is null", K(ret));
  } else if (OB_FAIL(gctx_.ob_service_->check_physical_flashback_succ(arg_, result_))) {
    LOG_WARN("failed to check physical flashback", K(ret), K_(arg), K_(result));
  }
  return ret;
}

int ObForceSetServerListP::process()
{
  int ret = OB_SUCCESS;
  ObPartitionService* partition_service = gctx_.par_ser_;
  TRANS_LOG(INFO, "force_set_server_list");
  if (NULL == partition_service) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "partition_service is NULL");
  } else {
    storage::ObIPartitionGroupIterator* partition_iter = NULL;
    if (NULL == (partition_iter = partition_service->alloc_pg_iter())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(ERROR, "partition_mgr alloc_scan_iter failed", K(ret));
    } else {
      storage::ObIPartitionGroup* partition = NULL;
      ObIPartitionLogService* pls = NULL;
      while (OB_SUCC(ret)) {
        int tmp_ret = OB_SUCCESS;
        if (OB_FAIL(partition_iter->get_next(partition)) || NULL == partition) {
          TRANS_LOG(INFO, "get_next failed or partition is NULL", K(ret));
        } else if (!partition->is_valid() || (NULL == (pls = partition->get_log_service()))) {
          TRANS_LOG(INFO, "partition is invalid or pls is NULL", "partition_key", partition->get_partition_key());
        } else if (OB_SUCCESS != (tmp_ret = pls->force_set_server_list(arg_.server_list_, arg_.replica_num_))) {
          TRANS_LOG(WARN,
              "force_set_server_list failed",
              K(ret),
              K(tmp_ret),
              "partition_key",
              partition->get_partition_key());
        }
      }
    }

    if (NULL != partition_iter) {
      partition_service->revert_pg_iter(partition_iter);
      partition_iter = NULL;
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObRenewInZoneHbP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("observer is null", K(ret));
  } else if (OB_FAIL(gctx_.ob_service_->renew_in_zone_hb(arg_, result_))) {
    LOG_WARN("failed to check physical flashback", K(ret), K(arg_), K(result_));
  }
  return ret;
}

int ObKillPartTransCtxP::process()
{
  LOG_INFO("kill_part_trans_ctx rpc is called", K(arg_));
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg_));
  } else if (OB_ISNULL(gctx_.par_ser_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("gctx partition service is null");
  } else if (OB_FAIL(gctx_.par_ser_->get_trans_service()->kill_part_trans_ctx(arg_.partition_key_, arg_.trans_id_))) {
    LOG_WARN("failed to kill part trans ctx", K(ret), K(arg_));
  }
  return ret;
}

}  // end of namespace observer
}  // end of namespace oceanbase
