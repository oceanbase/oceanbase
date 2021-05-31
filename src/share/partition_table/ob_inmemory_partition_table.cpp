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

#define USING_LOG_PREFIX SHARE_PT

#include "ob_inmemory_partition_table.h"

#include "lib/container/ob_se_array.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"
namespace oceanbase {
namespace share {
using namespace common;
using namespace share;

ObInMemoryPartitionTable::ObInMemoryPartitionTable(ObIPartPropertyGetter& prop_getter)
    : ObIPartitionTable(prop_getter), inited_(false), partition_info_(), mutex_(), rs_list_change_cb_(NULL)
{
  partition_info_.set_table_id(combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID));
  partition_info_.set_partition_id(ALL_CORE_TABLE_PARTITION_ID);
}

ObInMemoryPartitionTable::~ObInMemoryPartitionTable()
{}

int ObInMemoryPartitionTable::init(ObIRsListChangeCb& rs_list_change_cb)
{
  int ret = OB_SUCCESS;
  if (is_inited()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    rs_list_change_cb_ = &rs_list_change_cb;
    inited_ = true;
  }
  return ret;
}

void ObInMemoryPartitionTable::reuse()
{
  lib::ObMutexGuard guard(mutex_);
  partition_info_.get_replicas_v2().reuse();
}

int ObInMemoryPartitionTable::get(const uint64_t table_id, const int64_t partition_id, ObPartitionInfo& partition_info,
    const bool need_fetch_faillist, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID != cluster_id) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("get location with cluster_id not supported", K(ret), K(cluster_id));
  } else if (combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID) != table_id ||
             ALL_CORE_TABLE_PARTITION_ID != partition_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KT(table_id), K(partition_id));
  } else {
    const bool filter_flag_replica = true;
    if (OB_FAIL(inner_get(table_id, partition_id, filter_flag_replica, partition_info))) {
      LOG_WARN("inner_get failed", KT(table_id), K(partition_id), K(filter_flag_replica), K(ret));
    }
    if (!need_fetch_faillist) {
      // no faillist info is needed, reset
      for (int64_t i = 0; OB_SUCC(ret) && i < partition_info.get_replicas_v2().count(); ++i) {
        partition_info.get_replicas_v2().at(i).reset_fail_list();
      }
    }
  }
  check_leader();
  return ret;
}

int ObInMemoryPartitionTable::prefetch_by_table_id(const uint64_t tenant_id, const uint64_t start_table_id,
    const int64_t start_partition_id, ObIArray<ObPartitionInfo>& partition_infos, const bool need_fetch_faillist)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_SYS_TENANT_ID != tenant_id || start_table_id != combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID) ||
             start_partition_id != ALL_CORE_TABLE_PARTITION_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(tenant_id), K(ret), K(start_table_id), K(start_partition_id));
  } else {
    const bool filter_flag_replica = true;
    const uint64_t table_id = combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID);
    const int64_t partition_id = ALL_CORE_TABLE_PARTITION_ID;
    ObPartitionInfo partition_info;
    if (OB_FAIL(inner_get(table_id, partition_id, filter_flag_replica, partition_info))) {
      LOG_WARN("get failed", KT(table_id), K(partition_id), K(filter_flag_replica), K(ret));
    } else {
      partition_info.reset_row_checksum();
      if (!need_fetch_faillist) {
        // no faillist info is needed, reset
        for (int64_t i = 0; OB_SUCC(ret) && i < partition_info.get_replicas_v2().count(); ++i) {
          partition_info.get_replicas_v2().at(i).reset_fail_list();
        }
      }
      if (OB_FAIL(partition_infos.push_back(partition_info))) {
        LOG_WARN("push_back failed", K(ret));
      }
    }
  }
  check_leader();
  return ret;
}

int ObInMemoryPartitionTable::prefetch(const uint64_t tenant_id, const uint64_t start_table_id,
    const int64_t start_partition_id, ObIArray<ObPartitionInfo>& partition_infos, bool ignore_row_checksum,
    const bool need_fetch_faillist)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  UNUSED(ignore_row_checksum);
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_SYS_TENANT_ID != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(tenant_id), K(ret));
  } else if (OB_INVALID_ID == start_table_id || start_partition_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid start_table_id or invalid start_partition_id", KT(start_table_id), K(start_partition_id), K(ret));
  } else {
    const bool filter_flag_replica = false;
    if (OB_FAIL(inner_prefetch(tenant_id,
            start_table_id,
            start_partition_id,
            filter_flag_replica,
            partition_infos,
            need_fetch_faillist))) {
      LOG_WARN("inner_prefetch failed",
          K(tenant_id),
          KT(start_table_id),
          K(start_partition_id),
          K(filter_flag_replica),
          K(ret));
    }
  }
  check_leader();
  return ret;
}

int ObInMemoryPartitionTable::prefetch(const uint64_t pt_table_id, const int64_t pt_partition_id,
    const uint64_t start_table_id, const int64_t start_partition_id, ObIArray<ObPartitionInfo>& partition_infos,
    const bool need_fetch_faillist)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (pt_table_id != combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_CORE_META_TABLE_TID) || 0 != pt_partition_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid pt_table_id or pt_partition_id", KT(pt_table_id), K(pt_partition_id), K(ret));
  } else if (OB_INVALID_ID == start_table_id || start_partition_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid start_table_id or invalid start_partition_id", KT(start_table_id), K(start_partition_id), K(ret));
  } else {
    const bool filter_flag_replica = false;
    const uint64_t tenant_id = OB_SYS_TENANT_ID;
    if (OB_FAIL(inner_prefetch(tenant_id,
            start_table_id,
            start_partition_id,
            filter_flag_replica,
            partition_infos,
            need_fetch_faillist))) {
      LOG_WARN("inner_prefetch failed",
          K(tenant_id),
          KT(start_table_id),
          K(start_partition_id),
          K(filter_flag_replica),
          K(ret));
    }
  }
  check_leader();
  return ret;
}

int ObInMemoryPartitionTable::batch_fetch_partition_infos(const common::ObIArray<common::ObPartitionKey>& keys,
    common::ObIAllocator& allocator, common::ObArray<ObPartitionInfo*>& partitions, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  if (1 != keys.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid keys cnt", K(ret), "cnt", keys.count());
  } else if (OB_INVALID_ID != cluster_id) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("get location with cluster_id not supported", K(ret), K(cluster_id));
  } else {
    ObPartitionInfo* partition = NULL;
    const ObPartitionKey& key = keys.at(0);
    if (OB_FAIL(ObPartitionInfo::alloc_new_partition_info(allocator, partition))) {
      LOG_WARN("fail to alloc partition", K(ret), K(key));
    } else if (OB_FAIL(get(key.get_table_id(), key.get_partition_id(), *partition))) {
      LOG_WARN("fail to get partition", K(ret), K(key));
    } else if (OB_FAIL(partitions.push_back(partition))) {
      LOG_WARN("fail to push back partition", K(ret), K(key));
    }
  }
  return ret;
}

int ObInMemoryPartitionTable::batch_execute(const ObIArray<ObPartitionReplica>& replicas)
{
  int ret = OB_SUCCESS;
  if (replicas.count() <= 0) {
    // nothing to do
  } else {
    FOREACH_CNT_X(replica, replicas, OB_SUCC(ret))
    {
      if (OB_ISNULL(replica) || OB_ALL_CORE_TABLE_TID != extract_pure_id(replica->table_id_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("get invalid replica", K(ret), K(replica));
      } else if (replica->is_remove_) {
        if (OB_FAIL(remove(replica->table_id_, replica->partition_id_, GCTX.self_addr_))) {
          LOG_WARN(
              "fail to remove replica", K(ret), "table_id", replica->table_id_, "partition id", replica->partition_id_);
        }
      } else {
        if (OB_FAIL(update(*replica))) {
          LOG_WARN(
              "fail to update replica", K(ret), "table_id", replica->table_id_, "partition id", replica->partition_id_);
        }
      }
    }
  }
  return ret;
}

int ObInMemoryPartitionTable::batch_report_with_optimization(
    const ObIArray<ObPartitionReplica>& replicas, const bool with_role)
{
  UNUSED(replicas);
  UNUSED(with_role);
  return OB_NOT_SUPPORTED;
}

int ObInMemoryPartitionTable::batch_report_partition_role(
    const common::ObIArray<share::ObPartitionReplica>& pkey_array, const ObRole new_role)
{
  UNUSED(pkey_array);
  UNUSED(new_role);
  return OB_NOT_SUPPORTED;
}

int ObInMemoryPartitionTable::remove(const uint64_t table_id, const int64_t partition_id, const ObAddr& server)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID) != table_id ||
             ALL_CORE_TABLE_PARTITION_ID != partition_id || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KT(table_id), K(partition_id), K(server));
  } else {
    const ObPartitionReplica* replica = NULL;
    if (OB_FAIL(partition_info_.find(server, replica))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("no replica found", K(server));
      } else {
        LOG_WARN("find server replica failed", K(ret), K(server));
      }
    } else if (NULL == replica) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL replica", K(ret));
    } else {
      LOG_INFO("remove replica", "replica", *replica);
      if (OB_FAIL(partition_info_.remove(server))) {
        LOG_WARN("remove server replica failed", K(ret), K(server), K_(partition_info));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(rs_list_change_cb_->submit_update_rslist_task())) {
        LOG_WARN("submit_update_rslist_task failed", K(ret));
      }
    }
  }
  check_leader();
  return ret;
}

int ObInMemoryPartitionTable::update(const ObPartitionReplica& replica)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!replica.is_valid() || combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID) != replica.table_id_ ||
             ALL_CORE_TABLE_PARTITION_ID != replica.partition_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(replica));
  } else {
    ObPartitionReplica new_replica;
    if (OB_FAIL(new_replica.assign(replica))) {
      LOG_WARN("failed to assign new_replica", K(ret));
    } else {
      // disable column checksum to simplify memory management
      new_replica.row_checksum_.column_count_ = 0;
      new_replica.row_checksum_.column_checksum_array_ = NULL;

      if (new_replica.is_strong_leader()) {
        if (replica.data_version_ <= 0) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("data_version must > 0 for leader replica", K(ret), K(replica));
        } else if (OB_FAIL(update_leader_replica(new_replica))) {
          LOG_WARN("update leader replica failed", K(ret), K(replica));
        }
      } else {
        const ObPartitionReplica* old = NULL;
        if (OB_FAIL(partition_info_.find(new_replica.server_, old))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("find server replica failed", K(ret), "server", new_replica.server_);
          }
        } else if (NULL == old) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL replica", K(ret));
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(update_follower_replica(new_replica))) {
            LOG_WARN("update follower replica failed", K(ret), K(replica));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(partition_info_.update_replica_status())) {
        LOG_WARN("update replica type failed", K(ret), K_(partition_info));
      } else if (OB_FAIL(rs_list_change_cb_->submit_update_rslist_task())) {
        LOG_WARN("submit_update_rslist_task failed", K(ret));
      }
    }

    LOG_INFO("update partition replica", K(ret), K(replica), K_(partition_info));
  }

  if (OB_CHECKSUM_ERROR == ret) {
    int tmp_ret = OB_SUCCESS;
    if (OB_ISNULL(merge_error_cb_)) {
      // nothing todo
    } else if (OB_SUCCESS != (tmp_ret = merge_error_cb_->submit_merge_error_task())) {
      LOG_WARN("fail to report checksum error", K(tmp_ret));
    }
  }
  check_leader();
  return ret;
}

int ObInMemoryPartitionTable::set_unit_id(
    const uint64_t table_id, const int64_t partition_id, const ObAddr& server, const uint64_t unit_id)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID) != table_id ||
             ALL_CORE_TABLE_PARTITION_ID != partition_id || !server.is_valid() || OB_INVALID_ID == unit_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KT(table_id), K(partition_id), K(server), K(unit_id));
  } else if (OB_FAIL(partition_info_.set_unit_id(server, unit_id))) {
    LOG_WARN("partition_info set_unit_id failed", K(server), K(unit_id), K(ret));
  }
  check_leader();
  return ret;
}

int ObInMemoryPartitionTable::update_rebuild_flag(
    const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server, const bool rebuild)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID) != table_id ||
             ALL_CORE_TABLE_PARTITION_ID != partition_id || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KT(table_id), K(partition_id), K(server));
  } else {
    const ObPartitionReplica* replica = NULL;
    if (OB_FAIL(partition_info_.find(server, replica))) {
      LOG_WARN("find replica failed", K(ret), K(server));
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      }
    } else if (OB_ISNULL(replica)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL replica", K(ret));
    } else {
      const_cast<ObPartitionReplica*>(replica)->rebuild_ = rebuild;
    }
  }
  LOG_INFO("update inmemory partition rebuild flag", K(ret), K(server), K(rebuild), K_(partition_info));
  check_leader();
  return ret;
}

int ObInMemoryPartitionTable::update_fail_list(const uint64_t table_id, const int64_t partition_id,
    const common::ObAddr& server, const ObPartitionReplica::FailList& fail_list)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID) != table_id ||
             ALL_CORE_TABLE_PARTITION_ID != partition_id || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KT(table_id), K(partition_id), K(server));
  } else {
    const ObPartitionReplica* replica = NULL;
    if (OB_FAIL(partition_info_.find(server, replica))) {
      LOG_WARN("find replica failed", K(ret), K(server));
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      }
    } else if (OB_ISNULL(replica)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL replica", K(ret));
    } else if (OB_FAIL(const_cast<ObPartitionReplica*>(replica)->set_faillist(fail_list))) {
      LOG_WARN("failed to set fail list", K(ret), K(fail_list), K(replica));
    }
  }
  LOG_INFO("update inmemory partition fail list", K(ret), K(server), K(fail_list), K_(partition_info));
  check_leader();
  return ret;
}

int ObInMemoryPartitionTable::handover_partition(
    const common::ObPGKey& pg_key, const common::ObAddr& src_addr, const common::ObAddr& dest_addr)
{
  SHARE_PT_LOG(INFO,
      "handover partition will do nothing, maybe fast migrate __all_core_table",
      K(pg_key),
      K(src_addr),
      K(dest_addr));
  return OB_SUCCESS;  // do nothing
}

int ObInMemoryPartitionTable::replace_partition(
    const ObPartitionReplica& replica, const common::ObAddr& src_addr, const common::ObAddr& dest_addr)
{
  SHARE_PT_LOG(INFO,
      "replace partition will do nothing, maybe fast recover __all_core_table",
      K(replica),
      K(src_addr),
      K(dest_addr));
  return OB_SUCCESS;  // do nothing
}

int ObInMemoryPartitionTable::set_original_leader(
    const uint64_t table_id, const int64_t partition_id, const bool is_original_leader)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID) != table_id ||
             ALL_CORE_TABLE_PARTITION_ID != partition_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KT(table_id), K(partition_id));
  } else {
    // this function is useless, when rs change, this message will lost,
    // we implement it just to make it consistent with persistent partition table
    const ObPartitionReplica* leader = NULL;
    if (OB_FAIL(partition_info_.find_leader_v2(leader))) {
      LOG_WARN("fail to find leader", K(ret), K(partition_info_));
    } else if (OB_ISNULL(leader)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid leader", K(ret), K(leader), K(partition_info_));
    } else {
      const_cast<ObPartitionReplica*>(leader)->is_original_leader_ = is_original_leader;
    }
  }
  check_leader();
  return ret;
}

int ObInMemoryPartitionTable::inner_get(const uint64_t table_id, const int64_t partition_id,
    const bool filter_flag_replica, ObPartitionInfo& partition_info)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID) != table_id ||
             ALL_CORE_TABLE_PARTITION_ID != partition_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KT(table_id), K(partition_id));
  } else {
    partition_info.set_table_id(table_id);
    partition_info.set_partition_id(partition_id);
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_info_.get_replicas_v2().count(); ++i) {
      if (!partition_info_.get_replicas_v2().at(i).is_flag_replica() || !filter_flag_replica) {
        if (OB_FAIL(partition_info.add_replica(partition_info_.get_replicas_v2().at(i)))) {
          // one more checksum operation than push back directly
          LOG_WARN("add replica to array failed", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObInMemoryPartitionTable::inner_prefetch(const uint64_t tenant_id, const uint64_t start_table_id,
    const int64_t start_partition_id, const bool filter_flag_replica, ObIArray<ObPartitionInfo>& partition_infos,
    const bool need_fetch_faillist)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_SYS_TENANT_ID != tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(tenant_id), K(ret));
  } else if (start_table_id > combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID) ||
             (start_table_id == combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID) &&
                 start_partition_id >= ALL_CORE_TABLE_PARTITION_ID)) {
    // do nothing
    //  some one else will process
    // ret = OB_ERR_UNEXPECTED;
    // LOG_WARN("should not be here", K(ret), K(start_table_id), K(start_partition_id));
  } else {
    const uint64_t table_id = combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID);
    const int64_t partition_id = ALL_CORE_TABLE_PARTITION_ID;
    ObPartitionInfo partition_info;
    if (OB_FAIL(inner_get(table_id, partition_id, filter_flag_replica, partition_info))) {
      LOG_WARN("get failed", KT(table_id), K(partition_id), K(filter_flag_replica), K(ret));
    } else {
      partition_info.reset_row_checksum();
      if (!need_fetch_faillist) {
        // no faillist info is needed, reset
        for (int64_t i = 0; OB_SUCC(ret) && i < partition_info.get_replicas_v2().count(); ++i) {
          partition_info.get_replicas_v2().at(i).reset_fail_list();
        }
      }
      if (OB_FAIL(partition_infos.push_back(partition_info))) {
        LOG_WARN("push_back failed", K(ret));
      }
    }
  }
  return ret;
}

int ObInMemoryPartitionTable::update_follower_replica(const ObPartitionReplica& replica)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!replica.is_valid() || !is_follower(replica.role_) ||
             combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID) != replica.table_id_ ||
             ALL_CORE_TABLE_PARTITION_ID != replica.partition_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(replica));
  } else {
    ObPartitionReplica new_replica;
    if (OB_FAIL(new_replica.assign(replica))) {
      LOG_WARN("failed to assign new_replica", K(ret));
    } else {
      if (!replica.is_flag_replica()) {
        const ObPartitionReplica* inner_replica = NULL;
        if (OB_FAIL(partition_info_.find(new_replica.server_, inner_replica))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("find server replica failed", K(ret), "server", new_replica.server_);
          }
        } else if (NULL == inner_replica) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL replica", K(ret));
        } else {
          new_replica.to_leader_time_ = inner_replica->to_leader_time_;
          if (OB_INVALID_ID == replica.unit_id_) {
            new_replica.unit_id_ = inner_replica->unit_id_;
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(partition_info_.add_replica(new_replica))) {
        LOG_WARN("add replica failed", K(ret), K(replica));
      } else {
        LOG_INFO("add follower replica for __all_core_table", K(new_replica));
      }
    }
  }

  LOG_INFO("inmemory partition table add replica", K(ret), K(replica));
  return ret;
}

int ObInMemoryPartitionTable::update_leader_replica(const ObPartitionReplica& replica)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAddr, ObPartitionReplica::DEFAULT_REPLICA_COUNT> members;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!replica.is_valid() || !replica.is_strong_leader() || replica.data_version_ <= 0 ||
             combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID) != replica.table_id_ ||
             ALL_CORE_TABLE_PARTITION_ID != replica.partition_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(replica));
  } else if (GCONF.self_addr_ != replica.server_) {
    ret = OB_NOT_MASTER;
    LOG_WARN("get invalid replica info, leader should be self", "self", GCONF.self_addr_, K(replica));
  } else if (OB_FAIL(prop_getter_->get_leader_member(replica.partition_key(), members))) {
    LOG_WARN("get leader member failed", K(ret));
    ObPartitionReplica follower_replica;
    if (OB_FAIL(follower_replica.assign(replica))) {
      LOG_WARN("failed to assign follower_replica", K(ret));
    } else {
      follower_replica.role_ = FOLLOWER;
      // reset ret to avoid infinity retry.
      // TODO : only NOT_LEADER error can do this?
      if (OB_FAIL(update_follower_replica(follower_replica))) {
        LOG_WARN("update follower replica failed", K(ret), K(replica));
      }
    }
  } else {
    int64_t max_to_leader_time = 0;
    ObPartitionInfo::ReplicaArray& all_replicas = partition_info_.get_replicas_v2();
    for (int64_t i = 0; OB_SUCC(ret) && i < all_replicas.count(); ++i) {
      if ((all_replicas.at(i).is_strong_leader()) && all_replicas.at(i).server_ != replica.server_) {
        LOG_INFO("update replica role to FOLLOWER", "replica", all_replicas.at(i));
        all_replicas.at(i).role_ = FOLLOWER;
      }
      if (max_to_leader_time < all_replicas.at(i).to_leader_time_) {
        max_to_leader_time = all_replicas.at(i).to_leader_time_;
      }
    }  // end for
    if (OB_SUCC(ret)) {
      if (max_to_leader_time > replica.to_leader_time_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN(
            "get invalid replica info. leader should the max to_leader_time", K(ret), K(replica), K(partition_info_));
      }
    }

    if (OB_SUCC(ret)) {
      ObPartitionReplica new_replica;
      const ObPartitionReplica* inner_replica = NULL;
      if (OB_FAIL(new_replica.assign(replica))) {
        LOG_WARN("failed to assign new_replica", K(ret));
      } else if (OB_FAIL(partition_info_.find(new_replica.server_, inner_replica))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("find server failed", K(ret), "server", new_replica.server_);
        }
      } else if (NULL == inner_replica) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL replica", K(ret));
      } else {
        if (OB_INVALID_ID == replica.unit_id_) {
          new_replica.unit_id_ = inner_replica->unit_id_;
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(partition_info_.add_replica(new_replica))) {
          LOG_WARN("add replica failed", K(ret), K(replica));
        } else {
          LOG_INFO("add leader replica for __all_core_table", K(new_replica));
        }
      }
    }
  }
  return ret;
}

int ObInMemoryPartitionTable::check_leader()
{
  int ret = OB_SUCCESS;
  const ObPartitionReplica* leader = NULL;
  if (OB_FAIL(partition_info_.find_leader_v2(leader))) {
    LOG_WARN("fail to get leader", K(ret), K(partition_info_));
  } else if (OB_ISNULL(leader)) {
    ret = OB_NOT_MASTER;
    LOG_WARN("get invalid leader", K(ret), K(partition_info_));
  } else if (leader->server_ != GCONF.self_addr_) {
    ret = OB_NOT_MASTER;
    LOG_WARN("leader should be self", K(ret), K(*leader), K(partition_info_));
  }
  if (OB_FAIL(ret)) {
    // force report;
    LOG_WARN("__all_core_table with no leader, try report it", K(ret), K(partition_info_));
    // if (partition_info_.replica_count() > 0) {
    //  if (OB_FAIL(rs_list_change_cb_->submit_report_replica(partition_info_.get_replicas_v2().at(0).partition_key())))
    //  {
    //    LOG_WARN("fail to force observer report replica", K(ret));
    //  }
    //} else {
    //  if (OB_FAIL(rs_list_change_cb_->submit_report_replica())) {
    //    LOG_WARN("fail to force observer report replica", K(ret));
    //  }
    //}
  }
  return ret;
}

}  // end namespace share
}  // end namespace oceanbase
