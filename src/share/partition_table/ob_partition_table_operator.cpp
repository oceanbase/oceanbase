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

#include "ob_partition_table_operator.h"

#include "lib/stat/ob_diagnose_info.h"
#include "share/config/ob_server_config.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/ob_rs_mgr.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "common/ob_timeout_ctx.h"
#include "rootserver/ob_root_utils.h"
namespace oceanbase {
namespace share {
using namespace common;
using namespace obrpc;
ObPartitionTableOperator::ObPartitionTableOperator(ObIPartPropertyGetter& prop_getter)
    : ObIPartitionTable(prop_getter),
      inited_(false),
      root_meta_table_(&inmemory_table_),
      inmemory_table_(prop_getter),
      rpc_table_(prop_getter),
      persistent_table_(prop_getter)
{}

ObPartitionTableOperator::~ObPartitionTableOperator()
{}

int ObPartitionTableOperator::init(ObISQLClient& sql_proxy, common::ObServerConfig* config /*=NULL*/)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("partition table operator has already inited", K(ret));
  } else if (OB_FAIL(persistent_table_.init(sql_proxy, config))) {
    LOG_WARN("init persistent partition table failed", K(ret));
  } else {
    root_meta_table_ = &inmemory_table_;
    inited_ = true;
  }
  return ret;
}

int ObPartitionTableOperator::set_use_memory_table(ObIRsListChangeCb& rs_list_change_cb)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (!inmemory_table_.is_inited()) {
      if (OB_FAIL(inmemory_table_.init(rs_list_change_cb))) {
        LOG_WARN("inmemory_table_ init failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      root_meta_table_ = &inmemory_table_;
    }
  }
  return ret;
}

int ObPartitionTableOperator::set_callback_for_rs(ObIRsListChangeCb& rs_list_change_cb, ObIMergeErrorCb& merge_error_cb)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(set_use_memory_table(rs_list_change_cb))) {
    LOG_WARN("fail to set use memory table", K(ret));
  } else {
    inmemory_table_.set_merge_error_cb(&merge_error_cb);
    persistent_table_.set_merge_error_cb(&merge_error_cb);
    rpc_table_.set_merge_error_cb(&merge_error_cb);
  }
  return ret;
}

int ObPartitionTableOperator::set_callback_for_obs(ObCommonRpcProxy& rpc_proxy, ObRsMgr& rs_mgr, ObServerConfig& config)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(set_use_rpc_table(rpc_proxy, rs_mgr, config))) {
    LOG_WARN("fail to set user rpc table", K(ret));
  } else {
    inmemory_table_.set_merge_error_cb(NULL);
    persistent_table_.set_merge_error_cb(NULL);
    rpc_table_.set_merge_error_cb(NULL);
  }
  return ret;
}
int ObPartitionTableOperator::set_use_rpc_table(ObCommonRpcProxy& rpc_proxy, ObRsMgr& rs_mgr, ObServerConfig& config)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (!rpc_table_.is_inited()) {
      if (OB_FAIL(rpc_table_.init(rpc_proxy, rs_mgr, config))) {
        LOG_WARN("rpc_table_ init failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      inmemory_table_.reuse();
      root_meta_table_ = &rpc_table_;
    }
  }
  return ret;
}

int ObPartitionTableOperator::get(const uint64_t table_id, const int64_t partition_id, ObPartitionInfo& partition_info,
    const bool need_fetch_faillist, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  ObIPartitionTable* pt = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_valid_key(table_id, partition_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KT(table_id), K(partition_id));
  } else if (OB_FAIL(get_partition_table(table_id, pt))) {
    LOG_WARN("get partition table failed", K(ret), K(table_id));
  } else if (OB_ISNULL(pt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL partition table", K(ret));
  } else {
    partition_info.reuse();
    partition_info.set_table_id(table_id);
    partition_info.set_partition_id(partition_id);
    ObTimeoutCtx ctx;
    if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
    } else if (OB_FAIL(pt->get(table_id, partition_id, partition_info, need_fetch_faillist, cluster_id))) {
      LOG_WARN("get partition info failed", K(ret), KT(table_id), K(partition_id));
    }
  }
  if (OB_SUCC(ret)) {
    int64_t now = ObTimeUtility::current_time();
    EVENT_INC(PARTITION_TABLE_OPERATOR_GET_COUNT);
    EVENT_ADD(PARTITION_TABLE_OPERATOR_GET_TIME, now - start_time);
  }
  return ret;
}

int ObPartitionTableOperator::prefetch_by_table_id(const uint64_t tenant_id, const uint64_t start_table_id,
    const int64_t start_partition_id, ObIArray<ObPartitionInfo>& partition_infos, const bool need_fetch_faillist)
{
  int ret = OB_SUCCESS;
  ObIPartitionTable* pt = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(tenant_id), K(ret));
  } else if (OB_INVALID_ID == start_table_id || OB_INVALID_INDEX == start_partition_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid start_table_id or invalid start_partition_id", K(start_table_id), K(start_partition_id), K(ret));
  } else if (OB_FAIL(get_partition_table(start_table_id, pt))) {
    LOG_WARN("get partition table failed", K(ret), K(start_table_id));
  } else if (OB_ISNULL(pt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL partition table", K(ret));
  } else if (OB_FAIL(pt->prefetch_by_table_id(
                 tenant_id, start_table_id, start_partition_id, partition_infos, need_fetch_faillist))) {
    LOG_WARN("partition_table prefetch failed", K(tenant_id), K(start_table_id), K(start_partition_id), K(ret));
  }
  LOG_DEBUG(
      "prefetch by table_id", K(ret), K(tenant_id), K(start_table_id), K(start_partition_id), K(need_fetch_faillist));
  return ret;
}

int ObPartitionTableOperator::prefetch(const uint64_t tenant_id, const uint64_t start_table_id,
    const int64_t start_partition_id, ObIArray<ObPartitionInfo>& partition_infos, bool ignore_row_checksum,
    const bool need_fetch_faillist)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(tenant_id), K(ret));
  } else if (OB_INVALID_ID == start_table_id || OB_INVALID_INDEX == start_partition_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid start_table_id or invalid start_partition_id", K(start_table_id), K(start_partition_id), K(ret));
  } else {
    // only sys tenant has partition in root_meta_table
    if (OB_SYS_TENANT_ID == tenant_id) {
      if (OB_FAIL(root_meta_table_->prefetch(tenant_id,
              start_table_id,
              start_partition_id,
              partition_infos,
              ignore_row_checksum,
              need_fetch_faillist))) {
        LOG_WARN("root_meta_table prefetch failed", K(tenant_id), K(start_table_id), K(start_partition_id), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObIPartitionTable* pt = static_cast<ObIPartitionTable*>(&persistent_table_);
      if (OB_FAIL(pt->prefetch(tenant_id,
              start_table_id,
              start_partition_id,
              partition_infos,
              ignore_row_checksum,
              need_fetch_faillist))) {
        LOG_WARN("persistent_table prefetch failed", K(tenant_id), K(start_table_id), K(start_partition_id), K(ret));
      }
    }
  }
  return ret;
}

int ObPartitionTableOperator::prefetch(const uint64_t pt_table_id, const int64_t pt_partition_id,
    const uint64_t start_table_id, const int64_t start_partition_id, ObIArray<ObPartitionInfo>& partition_infos,
    const bool need_fetch_faillist)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == pt_table_id || !ObIPartitionTable::is_partition_table(pt_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid pt_table_id", KT(pt_table_id), K(ret));
  } else if (OB_INVALID_INDEX == pt_partition_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid pt_partition_id", K(pt_partition_id), K(ret));
  } else if (OB_INVALID_ID == start_table_id || OB_INVALID_INDEX == start_partition_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid start_table_id or invalid start_partition_id", K(start_table_id), K(start_partition_id), K(ret));
  } else {
    ObIPartitionTable* pt = NULL;
    if (combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_CORE_META_TABLE_TID) == pt_table_id) {
      pt = root_meta_table_;
    } else {
      pt = static_cast<ObIPartitionTable*>(&persistent_table_);
    }
    if (OB_ISNULL(pt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pt should not be null", K(ret));
    } else if (OB_FAIL(pt->prefetch(pt_table_id,
                   pt_partition_id,
                   start_table_id,
                   start_partition_id,
                   partition_infos,
                   need_fetch_faillist))) {
      LOG_WARN(
          "pt prefetch failed", KT(pt_table_id), K(pt_partition_id), KT(start_table_id), K(start_partition_id), K(ret));
    }
  }
  return ret;
}

// guarantee batch tasks belong to the same partition meta table
// 1.__all_core_table/__all_root_table location_cache refresh are processed separately
// 2.sys partitons and user partitions are not in the same batch
// 3.user tables from different tenants are not in the same batch
// elements of partitions need to be destructed
int ObPartitionTableOperator::batch_fetch_partition_infos(const common::ObIArray<common::ObPartitionKey>& keys,
    common::ObIAllocator& allocator, common::ObArray<ObPartitionInfo*>& partitions,
    const int64_t cluster_id /*=OB_INVALID_ID*/)
{
  int ret = OB_SUCCESS;
  if (keys.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid key cnt", K(ret));
  } else {
    // pre check
    for (int64_t i = 0; OB_SUCC(ret) && i < keys.count(); i++) {
      uint64_t cur_table_id = keys.at(i).get_table_id();
      if (is_virtual_table(cur_table_id)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid table id", K(ret), K(cur_table_id));
      } else if (i > 0) {
        uint64_t pre_table_id = keys.at(i - 1).get_table_id();
        if (OB_ALL_CORE_TABLE_TID == extract_pure_id(cur_table_id) ||
            OB_ALL_ROOT_TABLE_TID == extract_pure_id(cur_table_id)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("should process alone", K(ret), K(cur_table_id), K(pre_table_id));
        } else if (is_sys_table(cur_table_id) != is_sys_table(pre_table_id)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("should fetch sys and user tables partitions in different batch ",
              K(ret),
              K(cur_table_id),
              K(pre_table_id));
        } else if (!is_sys_table(cur_table_id) && extract_tenant_id(cur_table_id) != extract_tenant_id(pre_table_id)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fetch different tenants' user partitions", K(ret), K(cur_table_id), K(pre_table_id));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObIPartitionTable* pt = NULL;
      uint64_t table_id = keys.at(0).get_table_id();
      if (OB_FAIL(get_partition_table(table_id, pt))) {
        LOG_WARN("get partition table failed", K(ret), K(table_id));
      } else if (OB_ISNULL(pt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL partition table", K(ret));
      } else if (OB_FAIL(pt->batch_fetch_partition_infos(keys, allocator, partitions, cluster_id))) {
        LOG_WARN("fail to batch update", K(ret));
      }
    }
  }
  return ret;
}

// batch_execute is invoked only by ObPartitionTableUpdater,
// all replicas in argument replicas are from the same tenant
// replicas fall into the following three categories:
// 1. __all_virtual_core_meta_table,__all_core_table
// 2. __all_root_table
// 3. __all_tenant_meta_table
int ObPartitionTableOperator::batch_execute(const ObIArray<ObPartitionReplica>& replicas)
{
  int ret = OB_SUCCESS;
  const int64_t begin = ObTimeUtility::current_time();
  ObIPartitionTable* pt = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (replicas.count() <= 0) {
    // do nothing
  } else {
    int64_t table_id = replicas.at(0).table_id_;
    // __all_core_table and __all_root_table use the same task queue,
    // but their partition tables are not the same,
    // we need to process these task separately
    if (OB_ALL_CORE_TABLE_TID == extract_pure_id(table_id) || OB_ALL_ROOT_TABLE_TID == extract_pure_id(table_id)) {
      ObSEArray<ObPartitionReplica, 1> tmp_replicas;
      FOREACH_CNT_X(replica, replicas, OB_SUCC(ret))
      {
        tmp_replicas.reuse();
        if (OB_ISNULL(replica)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL partition table", K(ret), K(replica));
        } else if (OB_FAIL(get_partition_table(replica->table_id_, pt))) {
          LOG_WARN("fail to get partition table", K(ret), "table_id", replica->table_id_);
        } else if (OB_ISNULL(pt)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL partition table", K(ret), K(pt));
        } else if (OB_FAIL(tmp_replicas.push_back(*replica))) {
          LOG_WARN("fail to push back replica", K(ret), K(*replica));
        } else if (OB_FAIL(pt->batch_execute(tmp_replicas))) {
          LOG_WARN("fail to batch execute", K(ret));
        }
      }
    } else {
      if (OB_FAIL(get_partition_table(replicas.at(0).table_id_, pt))) {
        LOG_WARN("get partition table failed", K(ret), "table_id", replicas.at(0).table_id_);
      } else if (OB_ISNULL(pt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL partition table", K(ret));
      } else if (OB_FAIL(pt->batch_execute(replicas))) {
        LOG_WARN("fail to batch update", K(ret));
      }
    }
  }
  LOG_DEBUG("batch update partition replicas", K(ret), "time_used", ObTimeUtility::current_time() - begin);
  return ret;
}

int ObPartitionTableOperator::batch_report_partition_role(
    const common::ObIArray<share::ObPartitionReplica>& replica_array, const common::ObRole new_role)
{
  int ret = OB_SUCCESS;
  ObIPartitionTable* pt = nullptr;
  const int64_t begin = ObTimeUtility::current_time();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (replica_array.count() <= 0) {
    // do nothing
  } else {
    const uint64_t table_id = replica_array.at(0).get_table_id();
    if (OB_FAIL(get_partition_table(table_id, pt))) {
      LOG_WARN("fail to get partition table", KR(ret), K(table_id));
    } else if (OB_UNLIKELY(nullptr == pt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null partition table", KR(ret));
    } else if (OB_FAIL(pt->batch_report_partition_role(replica_array, new_role))) {
      LOG_WARN("fail to batch report partition role", KR(ret), K(replica_array));
    }
    LOG_INFO("batch report partition role", KR(ret), K(new_role), "time_used", ObTimeUtility::current_time() - begin);
  }
  return ret;
}

/* batch_report_with_optimization is a func optimized for the following two categories:
 * 1 follower replicas
 * 2 leader replicas of the user table partition without role column reports
 */
int ObPartitionTableOperator::batch_report_with_optimization(
    const ObIArray<ObPartitionReplica>& replicas, const bool with_role)
{
  int ret = OB_SUCCESS;
  const int64_t begin = ObTimeUtility::current_time();
  ObIPartitionTable* pt = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (replicas.count() <= 0) {
    // do nothing
  } else {
    int64_t table_id = replicas.at(0).table_id_;
    // __all_core_table and __all_root_table use the same task queue,
    // but their partition tables are not the same,
    // we need to process these task separately
    if (OB_ALL_CORE_TABLE_TID == extract_pure_id(table_id) || OB_ALL_ROOT_TABLE_TID == extract_pure_id(table_id)) {
      if (OB_FAIL(batch_execute(replicas))) {
        LOG_WARN("fail to batch report follower", K(ret), K(table_id));
      }
    } else {
      FOREACH_CNT_X(replica, replicas, OB_SUCC(ret))
      {
        if (OB_ISNULL(replica)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL partition table", K(ret), K(replica));
        } else if ((replica->is_leader_like() && (with_role || replica->need_force_full_report())) ||
                   replica->is_remove_) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("replica status invalid", K(ret), K(replica), K(with_role));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(get_partition_table(replicas.at(0).table_id_, pt))) {
        LOG_WARN("get partition table failed", K(ret), "table_id", replicas.at(0).table_id_);
      } else if (OB_ISNULL(pt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL partition table", K(ret));
      } else if (OB_FAIL(pt->batch_report_with_optimization(replicas, with_role))) {
        LOG_WARN("fail to batch report follower", K(ret));
      }
    }
  }
  LOG_INFO("batch report follower", K(ret), "time_used", ObTimeUtility::current_time() - begin);
  return ret;
}

int ObPartitionTableOperator::update(const ObPartitionReplica& replica)
{
  int ret = OB_SUCCESS;
  const int64_t begin = ObTimeUtility::current_time();
  ObIPartitionTable* pt = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!replica.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(replica));
  } else if (OB_FAIL(get_partition_table(replica.table_id_, pt))) {
    LOG_WARN("get partition table failed", K(ret), "table_id", replica.table_id_);
  } else if (OB_ISNULL(pt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL partition table", K(ret));
  } else if (OB_FAIL(pt->update(replica))) {
    LOG_WARN("update replica failed", K(ret), K(replica));
  }
  LOG_INFO("update partition replica", K(ret), "time_used", ObTimeUtility::current_time() - begin, K(replica));
  return ret;
}

int ObPartitionTableOperator::remove(const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server)
{
  int ret = OB_SUCCESS;
  ObIPartitionTable* pt = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_valid_key(table_id, partition_id) || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KT(table_id), K(partition_id), K(server));
  } else if (OB_FAIL(get_partition_table(table_id, pt))) {
    LOG_WARN("get partition table failed", K(ret), K(table_id));
  } else if (OB_ISNULL(pt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL partition table", K(ret));
  } else if (OB_FAIL(pt->remove(table_id, partition_id, server))) {
    LOG_WARN("remove partition failed", K(ret), KT(table_id), K(partition_id), K(server));
  }
  return ret;
}

int ObPartitionTableOperator::set_unit_id(
    const uint64_t table_id, const int64_t partition_id, const ObAddr& server, const uint64_t unit_id)
{
  int ret = OB_SUCCESS;
  ObIPartitionTable* pt = NULL;
  ObTimeoutCtx ctx;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_valid_key(table_id, partition_id) || !server.is_valid() || common::OB_INVALID_ID == unit_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KT(table_id), K(partition_id), K(server), K(unit_id));
  } else if (OB_FAIL(get_partition_table(table_id, pt))) {
    LOG_WARN("get partition table failed", K(ret), K(table_id));
  } else if (OB_ISNULL(pt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL partition table", K(ret));
  } else if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
    LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
  } else if (OB_FAIL(pt->set_unit_id(table_id, partition_id, server, unit_id))) {
    LOG_WARN("set unit id failed", KT(table_id), K(partition_id), K(server), K(unit_id), K(ret));
  }
  return ret;
}

int ObPartitionTableOperator::update_rebuild_flag(
    const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server, const bool rebuild)
{
  int ret = OB_SUCCESS;
  ObIPartitionTable* pt = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_valid_key(table_id, partition_id) || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KT(table_id), K(partition_id), K(server));
  } else if (OB_FAIL(get_partition_table(table_id, pt))) {
    LOG_WARN("get partition table failed", K(ret), K(table_id));
  } else if (OB_ISNULL(pt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL partition table", K(ret));
  } else if (OB_FAIL(pt->update_rebuild_flag(table_id, partition_id, server, rebuild))) {
    LOG_WARN("update rebuild flag failed", KT(table_id), K(partition_id), K(server), K(rebuild), K(ret));
  }
  LOG_INFO("update rebuild flag", K(ret), K(table_id), K(partition_id), K(server), K(rebuild));
  return ret;
}

int ObPartitionTableOperator::update_fail_list(const uint64_t table_id, const int64_t partition_id,
    const common::ObAddr& server, const ObPartitionReplica::FailList& fail_list)
{
  int ret = OB_SUCCESS;
  ObIPartitionTable* pt = NULL;
  ObPartitionInfo part_info;
  ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
  part_info.set_allocator(&allocator);
  ObPartitionReplica::FailList new_fail_list;
  const bool need_fetch_faillist = true;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_valid_key(table_id, partition_id) || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_id), K(partition_id), K(server));
  } else if (OB_FAIL(get_partition_table(table_id, pt))) {
    LOG_WARN("get partition table failed", K(ret), K(table_id));
  } else if (OB_ISNULL(pt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL partition table", K(ret));
  } else if (OB_FAIL(pt->get(table_id, partition_id, part_info, need_fetch_faillist))) {
    LOG_WARN("failed to get partition_info", K(ret));
  } else {
    ObPartitionInfo::ReplicaArray& replicas = part_info.get_replicas_v2();
    bool found = false;
    int64_t index = 0;
    for (; OB_SUCC(ret) && index < replicas.count(); ++index) {
      if (replicas.at(index).server_ == server) {
        found = true;
        break;
      }
    }  // end for
    if (!found || index >= replicas.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to find replica", K(ret), K(found), K(index), K(replicas));
    } else if (OB_FAIL(copy_assign(new_fail_list, fail_list))) {
      LOG_WARN("failed to copy", K(ret), K(new_fail_list), K(fail_list));
    } else if (OB_FAIL(replicas.at(index).process_faillist(new_fail_list))) {
      LOG_WARN("failed to process fail list", K(ret), K(new_fail_list), K(index), "replica", replicas.at(index));
    } else {
      LOG_DEBUG("success process fail list", K(new_fail_list));
    }
  }
  if (OB_FAIL(ret)) {
    // nothing
  } else if (OB_FAIL(pt->update_fail_list(table_id, partition_id, server, new_fail_list))) {
    LOG_WARN("update rebuild flag failed", K(ret), K(table_id), K(partition_id), K(server), K(new_fail_list));
  }
  LOG_INFO("update fail list", K(ret), K(table_id), K(partition_id), K(server), K(new_fail_list));
  return ret;
}

int ObPartitionTableOperator::set_original_leader(
    const uint64_t table_id, int64_t partition_id, const bool is_original_leader)
{
  int ret = OB_SUCCESS;
  ObIPartitionTable* pt = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_valid_key(table_id, partition_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KT(table_id), K(partition_id));
  } else if (OB_FAIL(get_partition_table(table_id, pt))) {
    LOG_WARN("get partition table failed", K(ret), K(table_id));
  } else if (OB_ISNULL(pt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL partition table", K(ret));
  } else if (OB_FAIL(pt->set_original_leader(table_id, partition_id, is_original_leader))) {
    LOG_WARN("set set_original_leader failed", K(ret), KT(table_id), K(partition_id), K(is_original_leader));
  }
  return ret;
}

int ObPartitionTableOperator::handover_partition(
    const common::ObPGKey& pg_key, const common::ObAddr& src_addr, const common::ObAddr& dest_addr)
{
  int ret = OB_SUCCESS;
  ObIPartitionTable* pt = nullptr;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!pg_key.is_valid() || !src_addr.is_valid() || !dest_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pg_key), K(src_addr), K(dest_addr));
  } else if (OB_FAIL(get_partition_table(pg_key.get_table_id(), pt))) {
    LOG_WARN("fail to get partition table", K(ret), K(pg_key));
  } else if (OB_ISNULL(pt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition table is null", K(ret), K(pg_key));
  } else if (OB_FAIL(pt->handover_partition(pg_key, src_addr, dest_addr))) {
    LOG_WARN("fail to handover partition, may fail fast migrate", K(ret), K(pg_key));
  }
  return ret;
}

int ObPartitionTableOperator::replace_partition(
    const ObPartitionReplica& replica, const common::ObAddr& src_addr, const common::ObAddr& dest_addr)
{
  int ret = OB_SUCCESS;
  ObIPartitionTable* pt = nullptr;
  const ObPGKey pg_key = replica.partition_key();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!replica.is_valid() || !src_addr.is_valid() || !dest_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pg_key), K(replica), K(src_addr), K(dest_addr));
  } else if (OB_FAIL(get_partition_table(pg_key.get_table_id(), pt))) {
    LOG_WARN("fail to get partition table", K(ret), K(pg_key));
  } else if (OB_ISNULL(pt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition table is null", K(ret), K(pg_key));
  } else if (OB_FAIL(pt->replace_partition(replica, src_addr, dest_addr))) {
    LOG_WARN("fail to handover partition, may fail fast migrate", K(ret), K(pg_key));
  }
  return ret;
}

int ObPartitionTableOperator::get_partition_table(const uint64_t table_id, ObIPartitionTable*& partition_table)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_id));
  } else if (combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID) == table_id) {
    partition_table = root_meta_table_;
  } else {
    partition_table = static_cast<ObIPartitionTable*>(&persistent_table_);
  }
  return ret;
}
}  // end namespace share
}  // end namespace oceanbase
