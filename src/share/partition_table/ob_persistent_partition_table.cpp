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

#include "ob_persistent_partition_table.h"

#include "lib/time/ob_time_utility.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/config/ob_server_config.h"
#include "share/ob_dml_sql_splicer.h"
#include "observer/ob_server_struct.h"
#include "storage/ob_partition_service.h"
#include "ob_partition_info.h"
#include "ob_replica_filter.h"
#include "ob_partition_table_proxy.h"
#include "observer/ob_server_struct.h"
namespace oceanbase {
namespace share {
using namespace common;
using namespace share;

ObPersistentPartitionTable::ObPersistentPartitionTable(ObIPartPropertyGetter& prop_getter)
    : ObIPartitionTable(prop_getter), inited_(false), sql_proxy_(nullptr), config_(nullptr)
{}

ObPersistentPartitionTable::~ObPersistentPartitionTable()
{}

int ObPersistentPartitionTable::init(ObISQLClient& sql_proxy, ObServerConfig* config)
{
  int ret = OB_SUCCESS;
  if (is_inited()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("int twice", K(ret));
  } else {
    sql_proxy_ = &sql_proxy;
    config_ = config;
    inited_ = true;
  }
  return ret;
}

int ObPersistentPartitionTable::get(const uint64_t table_id, const int64_t partition_id,
    ObPartitionInfo& partition_info, const bool need_fetch_faillist, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_valid_key(table_id, partition_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KT(table_id), K(partition_id));
  } else if (NULL == partition_info.get_allocator()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition info's allocator must set", K(ret), K(partition_info.get_allocator()));
  } else {
    const bool filter_flag_replica = true;
    if (OB_FAIL(get_partition_info(
            table_id, partition_id, filter_flag_replica, partition_info, need_fetch_faillist, cluster_id))) {
      LOG_WARN(
          "get_partition_info failed", K(cluster_id), KT(table_id), K(partition_id), K(filter_flag_replica), K(ret));
    }
  }
  return ret;
}

int ObPersistentPartitionTable::prefetch_by_table_id(const uint64_t tenant_id, const uint64_t start_table_id,
    const int64_t start_partition_id, ObIArray<ObPartitionInfo>& partition_infos, const bool need_fetch_faillist)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(tenant_id), K(ret));
  } else if (OB_INVALID_ID == start_table_id || OB_INVALID_INDEX == start_partition_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid start_table_id or invalid start_partition_id", K(start_table_id), K(start_partition_id), K(ret));
  } else {
    ObPartitionTableProxyFactory factory(*sql_proxy_, merge_error_cb_, config_);
    ObPartitionTableProxy* proxy = NULL;
    const bool filter_flag_replica = true;
    int64_t fetch_count = GCONF.partition_table_scan_batch_count;
    if (OB_FAIL(factory.get_proxy(start_table_id, proxy))) {
      LOG_WARN("get partition table proxy failed", K(ret), K(start_table_id));
    } else if (NULL == proxy) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL proxy", K(ret), KT(start_table_id));
    } else if (OB_FAIL(proxy->fetch_by_table_id(tenant_id,
                   start_table_id,
                   start_partition_id,
                   filter_flag_replica,
                   fetch_count,
                   partition_infos,
                   need_fetch_faillist))) {
      LOG_WARN("fetch_tenant_partition_infos failed",
          K(tenant_id),
          K(start_table_id),
          K(start_partition_id),
          K(filter_flag_replica),
          K(fetch_count),
          K(ret));
    }
  }
  return ret;
}

int ObPersistentPartitionTable::prefetch(const uint64_t tenant_id, const uint64_t start_table_id,
    const int64_t start_partition_id, ObIArray<ObPartitionInfo>& partition_infos, bool ignore_row_checksum,
    const bool need_fetch_faillist)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(tenant_id), K(ret));
  } else if (OB_INVALID_ID == start_table_id || OB_INVALID_INDEX == start_partition_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid start_table_id or invalid start_partition_id", K(start_table_id), K(start_partition_id), K(ret));
  } else {
    ObSEArray<ObPartitionTableProxy*, OB_DEFAULT_SE_ARRAY_COUNT> proxies;
    ObPartitionTableProxyFactory factory(*sql_proxy_, merge_error_cb_, config_);
    const bool filter_flag_replica = false;
    if (OB_FAIL(factory.get_tenant_proxies(tenant_id, proxies))) {
      LOG_WARN("get_tenant_proxies failed", K(tenant_id), K(ret));
    } else {
      int64_t fetch_count = GCONF.partition_table_scan_batch_count;
      FOREACH_CNT_X(proxy, proxies, OB_SUCCESS == ret)
      {
        if (NULL == *proxy) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL proxy", K(ret));
        } else if (OB_FAIL((*proxy)->fetch_partition_infos(tenant_id,
                       start_table_id,
                       start_partition_id,
                       filter_flag_replica,
                       fetch_count,
                       partition_infos,
                       ignore_row_checksum,
                       false,
                       need_fetch_faillist))) {
          LOG_WARN("fetch_tenant_partition_infos failed",
              K(tenant_id),
              K(start_table_id),
              K(start_partition_id),
              K(filter_flag_replica),
              K(fetch_count),
              K(ret));
        }
      }
    }
  }
  return ret;
}

int ObPersistentPartitionTable::prefetch(const uint64_t pt_table_id, const int64_t pt_partition_id,
    const uint64_t start_table_id, const int64_t start_partition_id, ObIArray<ObPartitionInfo>& partition_infos,
    const bool need_fetch_faillist)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == pt_table_id || pt_partition_id < 0 || OB_INVALID_ID == start_table_id ||
             OB_INVALID_INDEX == start_partition_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(pt_table_id), K(pt_partition_id), K(start_table_id), K(start_partition_id), K(ret));
  } else {
    ObPartitionTableProxyFactory factory(*sql_proxy_, merge_error_cb_, config_);
    ObPartitionTableProxy* proxy = NULL;
    int64_t fetch_count = GCONF.partition_table_scan_batch_count;
    if (OB_FAIL(factory.get_proxy_of_partition_table(pt_table_id, proxy))) {
      LOG_WARN("get partition table proxy failed", K(ret), KT(pt_table_id));
    } else if (NULL == proxy) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL proxy", K(ret), KT(pt_table_id));
    } else if (OB_FAIL(proxy->fetch_partition_infos_pt(pt_table_id,
                   pt_partition_id,
                   start_table_id,
                   start_partition_id,
                   fetch_count,
                   partition_infos,
                   need_fetch_faillist))) {
      LOG_WARN("fetch_partition_infos failed",
          KT(pt_table_id),
          K(pt_partition_id),
          KT(start_table_id),
          K(start_partition_id),
          K(fetch_count),
          K(ret));
    }
  }

  return ret;
}

int ObPersistentPartitionTable::remove(const uint64_t table_id, const int64_t partition_id, const ObAddr& server)
{
  int ret = OB_SUCCESS;
  ObPartitionTableProxy* proxy = NULL;
  ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
  ObPartitionInfo partition;
  partition.set_allocator(&allocator);
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_valid_key(table_id, partition_id) || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KT(table_id), K(partition_id), K(server));
  } else {
    ObPartitionTableProxyFactory factory(*sql_proxy_, merge_error_cb_, config_);
    if (OB_FAIL(factory.get_proxy(table_id, proxy))) {
      LOG_WARN("get partition table proxy failed", K(ret), KT(table_id));
    } else if (NULL == proxy) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL proxy", K(ret), KT(table_id));
    } else {
      const bool filter_flag_replica = false;
      if (OB_FAIL(get_partition_info(table_id, partition_id, filter_flag_replica, partition))) {
        LOG_WARN("get partition info failed", K(ret), KT(table_id), K(partition_id), K(filter_flag_replica));
      } else {
        const ObPartitionReplica* replica = NULL;
        if (OB_FAIL(partition.find(server, replica))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("find server failed", K(ret), K(server));
          }
        } else {
          if (OB_FAIL(proxy->remove(table_id, partition_id, server))) {
            LOG_WARN("remove partition failed", K(ret), KT(table_id), K(partition_id));
          }
        }
      }
    }
  }
  LOG_INFO("remove partition", K(ret), KT(table_id), K(partition_id), K(server));
  return ret;
}

int ObPersistentPartitionTable::batch_execute(const ObIArray<ObPartitionReplica>& replicas)
{
  int ret = OB_SUCCESS;
  const int64_t EXECUTE_TIMEOUT_US = 2L * 1000 * 1000;
  int64_t start_time = ObTimeUtil::current_time();
  ObPartitionTableProxy* proxy = NULL;
  ObTimeoutCtx timeout_ctx;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(replicas.count() <= 0)) {
    // nothing to do
  } else {
    int64_t stmt_timeout = EXECUTE_TIMEOUT_US;
    ObPartitionTableProxyFactory factory(*sql_proxy_, merge_error_cb_, config_);
    if (OB_FAIL(factory.get_proxy(replicas.at(0).table_id_, proxy))) {
      LOG_WARN("get partition table proxy failed", K(ret), "table_id", replicas.at(0).table_id_);
    } else if (NULL == proxy) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL proxy", K(ret), K(replicas.at(0)));
    } else if (OB_FAIL(timeout_ctx.set_trx_timeout_us(stmt_timeout * (replicas.count() + 2)))) {
      // the start trans and end trans stmt are taken into consideration
      LOG_WARN("fail to set trx timeout", K(ret), K(replicas));
    } else if (OB_FAIL(timeout_ctx.set_timeout(stmt_timeout))) {
      LOG_WARN("set timeout context failed", K(ret));
    } else if (OB_FAIL(proxy->start_trans())) {
      LOG_WARN("fail to start trans", K(ret));
    } else {
      ObPartitionTableProxy* double_check_proxy = NULL;
      FOREACH_CNT_X(replica, replicas, OB_SUCC(ret))
      {
        if (OB_ISNULL(replica)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get invalid replica", K(ret), K(replica));
        } else if (OB_FAIL(factory.get_proxy(replica->table_id_, double_check_proxy))) {
          LOG_WARN("fail to get proxy", K(ret), K(replica));
        } else if (double_check_proxy != proxy) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("error! get wrong proxy, should not be batch execute", K(ret), K(proxy), K(double_check_proxy));
        } else if (OB_FAIL(timeout_ctx.set_timeout(EXECUTE_TIMEOUT_US))) {
          LOG_WARN("set timeout context failed", K(ret));
        } else if (OB_FAIL(execute(proxy, *replica))) {
          LOG_WARN("fail to update replica", K(ret));
        }
      }
    }
    if (proxy != NULL && proxy->is_trans_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = timeout_ctx.set_timeout(stmt_timeout))) {
        LOG_WARN("set timeout context failed", K(tmp_ret));
      }
      int trans_ret = proxy->end_trans(ret);
      if (OB_SUCCESS != trans_ret) {
        LOG_WARN("end trans failed", K(trans_ret));
        ret = OB_SUCCESS == ret ? trans_ret : ret;
      } else {
        LOG_DEBUG("batch execute end trans", K(ret), "end trans ret", trans_ret);
      }
    }
  }
  int64_t cost = ObTimeUtil::current_time() - start_time;
  LOG_DEBUG("batch execute", K(ret), K(cost), "batch count", replicas.count());
  return ret;
}

int ObPersistentPartitionTable::batch_report_partition_role(
    const common::ObIArray<share::ObPartitionReplica>& replica_array, const common::ObRole new_role)
{
  int ret = OB_SUCCESS;
  const int64_t EXECUTE_TIMEOUT_US = 2L * 1000 * 1000;
  ObPartitionTableProxy* proxy = nullptr;
  ObTimeoutCtx timeout_ctx;
  if (OB_UNLIKELY(!is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(nullptr == GCTX.par_ser_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition service ptr is null", KR(ret));
  } else if (OB_UNLIKELY(replica_array.count() <= 0)) {
    // bypass
  } else {
    const int64_t stmt_timeout = EXECUTE_TIMEOUT_US;
    const uint64_t table_id = replica_array.at(0).get_table_id();
    ObPartitionTableProxyFactory factory(*sql_proxy_, merge_error_cb_, config_);
    if (OB_FAIL(factory.get_proxy(table_id, proxy))) {
      LOG_WARN("get partition table proxy failed", KR(ret), K(table_id));
    } else if (OB_UNLIKELY(nullptr == proxy)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL proxy", K(ret), K(table_id));
    } else if (OB_FAIL(timeout_ctx.set_trx_timeout_us(stmt_timeout * (replica_array.count() + 2)))) {
      LOG_WARN("fail to set trx timeout", KR(ret), K(replica_array));
    } else if (OB_FAIL(timeout_ctx.set_timeout(stmt_timeout))) {
      LOG_WARN("set timeout context failed", KR(ret));
    } else if (OB_FAIL(proxy->start_trans())) {
      LOG_WARN("fail to start trans", KR(ret));
    } else {
      ObPartitionTableProxy* double_check_proxy = nullptr;
      for (int64_t i = 0; OB_SUCC(ret) && i < replica_array.count(); ++i) {
        const share::ObPartitionReplica& replica = replica_array.at(i);
        if (OB_FAIL(factory.get_proxy(replica.get_table_id(), double_check_proxy))) {
          LOG_WARN("fail to get proxy", KR(ret), "table_id", replica.get_table_id());
        } else if (double_check_proxy != proxy) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("error! get wrong proxy, should not be batch execute", KR(ret), K(proxy), K(double_check_proxy));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(timeout_ctx.set_timeout(stmt_timeout * replica_array.count()))) {
          LOG_WARN("set timeout context failed", KR(ret));
        } else if (OB_FAIL(proxy->batch_report_partition_role(replica_array, new_role))) {
          LOG_WARN("fail to batch report role", KR(ret), K(replica_array));
        }
      }
    }
    if (proxy != nullptr && proxy->is_trans_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = timeout_ctx.set_timeout(stmt_timeout))) {
        LOG_WARN("set timeout context failed", KR(tmp_ret));
      }
      int trans_ret = proxy->end_trans(ret);
      if (OB_SUCCESS != trans_ret) {
        LOG_WARN("end trans failed", K(trans_ret));
        ret = OB_SUCCESS == ret ? trans_ret : ret;
      }
    }
  }
  return ret;
}

int ObPersistentPartitionTable::batch_report_with_optimization(
    const ObIArray<ObPartitionReplica>& replicas, const bool with_role)
{
  int ret = OB_SUCCESS;
  const int64_t EXECUTE_TIMEOUT_US = 2L * 1000 * 1000;
  int64_t start_time = ObTimeUtil::current_time();
  ObPartitionTableProxy* proxy = NULL;
  ObTimeoutCtx timeout_ctx;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(replicas.count() <= 0)) {
    // nothing to do
  } else {
    int64_t stmt_timeout = EXECUTE_TIMEOUT_US;
    ObPartitionTableProxyFactory factory(*sql_proxy_, merge_error_cb_, config_);
    if (OB_FAIL(factory.get_proxy(replicas.at(0).table_id_, proxy))) {
      LOG_WARN("get partition table proxy failed", K(ret), "table_id", replicas.at(0).table_id_);
    } else if (NULL == proxy) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL proxy", K(ret), K(replicas.at(0)));
    } else if (OB_FAIL(timeout_ctx.set_trx_timeout_us(stmt_timeout * (replicas.count() + 2)))) {
      // the start trans and end trans stmt are taken into consideration
      LOG_WARN("fail to set trx timeout", K(ret), K(replicas));
    } else if (OB_FAIL(timeout_ctx.set_timeout(stmt_timeout))) {
      LOG_WARN("set timeout context failed", K(ret));
    } else if (OB_FAIL(proxy->start_trans())) {
      LOG_WARN("fail to start trans", K(ret));
    } else {
      ObPartitionTableProxy* double_check_proxy = NULL;
      FOREACH_CNT_X(replica, replicas, OB_SUCC(ret))
      {
        if (OB_ISNULL(replica)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get invalid replica", K(ret), K(replica));
        } else if (OB_ALL_CORE_TABLE_TID == extract_pure_id(replica->table_id_) ||
                   OB_ALL_ROOT_TABLE_TID == extract_pure_id(replica->table_id_) ||
                   (replica->is_leader_like() && (with_role || replica->need_force_full_report())) ||
                   replica->is_remove_) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid replica", K(ret), K(replica));
        } else if (OB_FAIL(factory.get_proxy(replica->table_id_, double_check_proxy))) {
          LOG_WARN("fail to get proxy", K(ret), K(replica));
        } else if (double_check_proxy != proxy) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("error! get wrong proxy, should not be batch execute", K(ret), K(proxy), K(double_check_proxy));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(timeout_ctx.set_timeout(stmt_timeout * replicas.count()))) {
      LOG_WARN("set timeout context failed", K(ret));
    } else if (OB_FAIL(proxy->batch_report_with_optimization(replicas, with_role))) {
      LOG_WARN("fail to batch report follower", K(ret));
    }
    if (proxy != NULL && proxy->is_trans_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = timeout_ctx.set_timeout(stmt_timeout))) {
        LOG_WARN("set timeout context failed", K(tmp_ret));
      }
      int trans_ret = proxy->end_trans(ret);
      if (OB_SUCCESS != trans_ret) {
        LOG_WARN("end trans failed", K(trans_ret));
        ret = OB_SUCCESS == ret ? trans_ret : ret;
      } else {
        LOG_DEBUG("batch execute end trans", K(ret), "end trans ret", trans_ret);
      }
    }
  }
  int64_t cost = ObTimeUtil::current_time() - start_time;
  LOG_DEBUG("batch report follower", K(ret), K(cost), "batch count", replicas.count());
  return ret;
}

int ObPersistentPartitionTable::execute(ObPartitionTableProxy* proxy, const ObPartitionReplica& replica)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(proxy) || OB_ISNULL(GCTX.par_ser_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(proxy), K(GCTX.par_ser_));
  } else {
    if (replica.is_remove_) {
      if (OB_FAIL(proxy->remove(replica.table_id_, replica.partition_id_, GCTX.self_addr_))) {
        LOG_WARN(
            "fail to remove partition", K(ret), "table_id", replica.table_id_, "paritition id", replica.partition_id_);
      }
    } else if (!replica.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invaid replica for update", K(ret), K(replica));
    } else {
      ObPartitionReplica new_replica;
      if (OB_FAIL(new_replica.assign(replica))) {
        LOG_WARN("failed to assign new_replica", K(ret));
      } else {
        ObRole new_role = INVALID_ROLE;
        if (replica.is_leader_like()) {
          if (replica.data_version_ < 0) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("data_version must > 0 for leader replica", K(ret), K(replica));
          } else if (replica.to_leader_time_ <= 0) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("change to leader time must > 0 for leader replica", K(ret), K(replica));
          } else if (OB_FAIL(GCTX.par_ser_->get_role_for_partition_table(replica.partition_key(), new_role))) {
            ret = OB_SUCCESS;
            new_replica.role_ = FOLLOWER;
          } else if (is_follower(new_role)) {
            new_replica.role_ = FOLLOWER;
          } else {
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(proxy->update_replica(new_replica))) {
          LOG_WARN("fail to update replica", K(ret), K(replica));
        }
      }
    }
  }
  return ret;
}

int ObPersistentPartitionTable::update(const ObPartitionReplica& replica)
{
  int ret = OB_SUCCESS;
  ObPartitionTableProxy* proxy = NULL;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!replica.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(replica));
  } else {
    ObPartitionTableProxyFactory factory(*sql_proxy_, merge_error_cb_, config_);
    if (OB_FAIL(factory.get_proxy(replica.table_id_, proxy))) {
      LOG_WARN("get partition table proxy failed", K(ret), "table_id", replica.table_id_);
    } else if (NULL == proxy) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL proxy", K(ret), "table_id", replica.table_id_);
    } else {
      ObPartitionInfo partition;
      ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
      partition.set_allocator(&allocator);

      const bool lock_replica =
          (replica.is_leader_like() || OB_ALL_ROOT_TABLE_TID == extract_pure_id(replica.table_id_));
      if (lock_replica) {
        if (OB_FAIL(proxy->start_trans())) {
          LOG_WARN("start transaction failed", K(ret));
        }
      }

      const bool filter_flag_replica = false;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(proxy->fetch_partition_info(
                     lock_replica, replica.table_id_, replica.partition_id_, filter_flag_replica, partition))) {
        LOG_WARN(
            "get partition failed", K(ret), K_(replica.table_id), K_(replica.partition_id), K(filter_flag_replica));
      } else {
        ObPartitionReplica new_replica;
        // replica's life cycle is larger than new_replica
        // get unit_id if replica exist
        const ObPartitionReplica* inner_replica = NULL;
        if (OB_FAIL(new_replica.assign(replica))) {
          LOG_WARN("failed to assign new_replica", K(ret));
        } else if (OB_FAIL(partition.find(replica.server_, inner_replica))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("find server replica failed", K(ret), "server", replica.server_);
          }
        } else if (NULL == inner_replica) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL replica", K(ret));
        } else {
          new_replica.is_original_leader_ = inner_replica->is_original_leader_;
          if (is_follower(new_replica.role_)) {
            new_replica.to_leader_time_ = inner_replica->to_leader_time_;
          }
          if (OB_INVALID_ID == new_replica.unit_id_) {
            new_replica.unit_id_ = inner_replica->unit_id_;
          }
        }

        if (OB_SUCC(ret)) {
          if (new_replica.is_leader_like()) {
            if (!new_replica.is_flag_replica()) {
              if (new_replica.data_version_ <= 0) {
                ret = OB_INVALID_ARGUMENT;
                LOG_WARN("data_version must > 0 for leader replica", K(ret), K(new_replica));
              } else if (new_replica.to_leader_time_ <= 0) {
                ret = OB_INVALID_ARGUMENT;
                LOG_WARN("change to leader time must > 0 for leader replica", K(ret), K(new_replica));
              }
            } else {
              // flag replica with only the role of leader, to_leader_time_ and data_version_ are not set
              // flag is set only by rs,observer shall not set the flag replica
              // observer will update the partition table asynchronously by invoking
              // ObPGPartitionMTUpdateOperator::batch_update
            }
            if (OB_FAIL(ret)) {
              // nothing todo
            } else if (OB_FAIL(update_leader_replica(*proxy, partition, new_replica))) {
              LOG_WARN("update leader replica failed", K(ret), K(new_replica));
            }
          } else {
            if (OB_FAIL(update_follower_replica(*proxy, partition, new_replica))) {
              LOG_WARN("update follower replica failed", K(ret), K(new_replica));
            }
          }
        }
      }

      if (proxy->is_trans_started()) {
        int trans_ret = proxy->end_trans(ret);
        if (OB_SUCCESS != trans_ret) {
          LOG_WARN("end trans failed", K(trans_ret));
          ret = OB_SUCCESS == ret ? trans_ret : ret;
        }
      }
    }
  }
  return ret;
}

int ObPersistentPartitionTable::set_unit_id(
    const uint64_t table_id, const int64_t partition_id, const ObAddr& server, const uint64_t unit_id)
{
  int ret = OB_SUCCESS;
  ObPartitionTableProxy* proxy = NULL;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_valid_key(table_id, partition_id) || !server.is_valid() || OB_INVALID_ID == unit_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KT(table_id), K(partition_id), K(server), K(unit_id), K(ret));
  } else {
    ObPartitionTableProxyFactory factory(*sql_proxy_, merge_error_cb_, config_);
    if (OB_FAIL(factory.get_proxy(table_id, proxy))) {
      LOG_WARN("get partition table failed", K(ret), KT(table_id));
    } else if (NULL == proxy) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL proxy", K(ret), KT(table_id));
    } else if (OB_FAIL(proxy->set_unit_id(table_id, partition_id, server, unit_id))) {
      LOG_WARN("set unit failed", K(ret), KT(table_id), K(partition_id), K(server), K(unit_id));
    }
  }
  return ret;
}

int ObPersistentPartitionTable::update_rebuild_flag(
    const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server, const bool rebuild)
{
  int ret = OB_SUCCESS;
  ObPartitionTableProxy* proxy = NULL;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_valid_key(table_id, partition_id) || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KT(table_id), K(partition_id), K(server), K(ret));
  } else {
    ObPartitionTableProxyFactory factory(*sql_proxy_, merge_error_cb_, config_);
    if (OB_FAIL(factory.get_proxy(table_id, proxy))) {
      LOG_WARN("get partition table failed", K(ret), KT(table_id));
    } else if (NULL == proxy) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL proxy", K(ret), KT(table_id));
    } else if (OB_FAIL(proxy->update_rebuild_flag(table_id, partition_id, server, rebuild))) {
      LOG_WARN("update rebuild flag failed", K(ret), KT(table_id), K(partition_id), K(server), K(rebuild));
    }
  }
  return ret;
}

int ObPersistentPartitionTable::update_fail_list(const uint64_t table_id, const int64_t partition_id,
    const common::ObAddr& server, const ObPartitionReplica::FailList& fail_list)
{
  int ret = OB_SUCCESS;
  ObPartitionTableProxy* proxy = NULL;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_valid_key(table_id, partition_id) || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KT(table_id), K(partition_id), K(server), K(ret));
  } else {
    ObPartitionTableProxyFactory factory(*sql_proxy_, merge_error_cb_, config_);
    if (OB_FAIL(factory.get_proxy(table_id, proxy))) {
      LOG_WARN("get partition table failed", K(ret), KT(table_id));
    } else if (NULL == proxy) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL proxy", K(ret), KT(table_id));
    } else if (OB_FAIL(proxy->update_fail_list(table_id, partition_id, server, fail_list))) {
      LOG_WARN("update rebuild flag failed", K(ret), KT(table_id), K(partition_id), K(server), K(fail_list));
    }
  }
  return ret;
}

int ObPersistentPartitionTable::handover_partition(
    const common::ObPGKey& pg_key, const common::ObAddr& src_addr, const common::ObAddr& dest_addr)
{
  int ret = OB_SUCCESS;
  ObPartitionTableProxy* proxy = NULL;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!pg_key.is_valid() || !src_addr.is_valid() || !dest_addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pg_key), K(src_addr), K(dest_addr));
  } else {
    ObPartitionTableProxyFactory factory(*sql_proxy_, merge_error_cb_, config_);
    if (OB_FAIL(factory.get_proxy(pg_key.get_table_id(), proxy))) {
      LOG_WARN("get partition table failed", K(ret), K(pg_key));
    } else if (NULL == proxy) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL proxy", K(ret), K(pg_key));
    } else if (OB_FAIL(proxy->handover_partition(pg_key, src_addr, dest_addr))) {
      LOG_WARN("handover partition failed", K(ret), K(pg_key), K(src_addr), K(dest_addr));
    }
  }
  return ret;
}

int ObPersistentPartitionTable::replace_partition(
    const ObPartitionReplica& replica, const common::ObAddr& src_addr, const common::ObAddr& dest_addr)
{
  int ret = OB_SUCCESS;
  ObPartitionTableProxy* proxy = NULL;
  const ObPGKey pg_key = replica.partition_key();
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!replica.is_valid() || !src_addr.is_valid() || !dest_addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(pg_key), K(replica), K(src_addr), K(dest_addr));
  } else {
    ObPartitionTableProxyFactory factory(*sql_proxy_, merge_error_cb_, config_);
    if (OB_FAIL(factory.get_proxy(pg_key.get_table_id(), proxy))) {
      LOG_WARN("get partition table failed", K(ret), K(pg_key));
    } else if (NULL == proxy) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL proxy", K(ret), K(pg_key));
    } else if (OB_FAIL(proxy->replace_partition(replica, src_addr, dest_addr))) {
      LOG_WARN("handover partition failed", K(ret), K(pg_key), K(replica), K(src_addr), K(dest_addr));
    }
  }
  return ret;
}

int ObPersistentPartitionTable::set_original_leader(
    const uint64_t table_id, const int64_t partition_id, const bool is_original_leader)
{
  int ret = OB_SUCCESS;
  ObPartitionTableProxy* proxy = NULL;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_valid_key(table_id, partition_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KT(table_id), K(partition_id), K(is_original_leader));
  } else {
    ObPartitionTableProxyFactory factory(*sql_proxy_, merge_error_cb_, config_);
    if (OB_FAIL(factory.get_proxy(table_id, proxy))) {
      LOG_WARN("get partition table failed", K(ret), KT(table_id));
    } else if (NULL == proxy) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL proxy", K(ret), KT(table_id));
    } else if OB_FAIL (proxy->set_original_leader(table_id, partition_id, is_original_leader)) {
      LOG_WARN("update original leader flag failed", K(ret), KT(table_id), K(partition_id), K(is_original_leader));
    }
  }
  return ret;
}

int ObPersistentPartitionTable::update_follower_replica(
    ObPartitionTableProxy& pt_proxy, ObPartitionInfo& partition, const ObPartitionReplica& replica)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!partition.is_valid() || !replica.is_valid() || !is_follower(replica.role_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(partition), K(replica));
  } else if (OB_FAIL(update_replica(pt_proxy, partition, replica))) {
    LOG_WARN("update replica failed", K(ret), K(partition), K(replica));
  }
  return ret;
}

int ObPersistentPartitionTable::update_replica(
    ObPartitionTableProxy& pt_proxy, ObPartitionInfo& partition, const ObPartitionReplica& replica)
{
  int ret = OB_SUCCESS;
  bool replace = false;
  const ObPartitionReplica* old = NULL;
  // FIXME : remove replace flag, because log replica always first inserted
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!partition.is_valid() || !replica.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(partition), K(replica));
  } else if (OB_FAIL(partition.find(replica.server_, old))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("find server replica failed", K(ret), "server", replica.server_);
    } else {
      LOG_INFO("insert new replica", K(replica));
      replace = !replica.is_flag_replica();
      if (OB_FAIL(pt_proxy.update_replica(replica, replace))) {
        LOG_WARN("insert replica failed", K(ret), K(replica));
      }
    }
  } else if (NULL == old) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL replica", K(ret));
  } else {
    bool need_update = false;
    if (replica.is_flag_replica()) {
      LOG_WARN("log replica not the first inserted replica, ignore", K(ret), K(replica));
    } else if (OB_FAIL(check_need_update(*old, replica, need_update))) {
      LOG_WARN("check need update failed", K(ret), "old", *old, K(replica));
    } else if (need_update) {
      LOG_INFO("update replica", "old_replica", *old, K(replica));
      replace = true;
      if (OB_FAIL(partition.add_replica(replica))) {
        LOG_WARN("partition add replica failed", K(ret), K(partition), K(replica));
      } else if (OB_FAIL(pt_proxy.update_replica(replica, replace))) {
        LOG_WARN("update replica failed", K(ret), K(replica));
      }
    }
  }
  return ret;
}

int ObPersistentPartitionTable::get_partition_info(const uint64_t table_id, const int64_t partition_id,
    const bool filter_flag_replica, ObPartitionInfo& partition_info, const bool need_fetch_faillist,
    const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  const bool lock_replica = false;
  ObPartitionTableProxy* proxy = NULL;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_valid_key(table_id, partition_id) || NULL == partition_info.get_allocator()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN(
        "invalid argument", K(ret), KT(table_id), K(partition_id), "allocator", OB_P(partition_info.get_allocator()));
  } else {
    ObPartitionTableProxyFactory factory(*sql_proxy_, merge_error_cb_, config_);
    if (OB_FAIL(factory.get_proxy(table_id, proxy))) {
      LOG_WARN("get partition table proxy failed", K(ret), KT(table_id));
    } else if (NULL == proxy) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL proxy", K(ret), KT(table_id));
    } else if (OB_FAIL(proxy->fetch_partition_info(lock_replica,
                   table_id,
                   partition_id,
                   filter_flag_replica,
                   partition_info,
                   need_fetch_faillist,
                   cluster_id))) {
      LOG_WARN(
          "get partition table failed", K(ret), K(cluster_id), KT(table_id), K(partition_id), K(filter_flag_replica));
    }
  }
  return ret;
}

int ObPersistentPartitionTable::batch_fetch_partition_infos(const common::ObIArray<common::ObPartitionKey>& keys,
    common::ObIAllocator& allocator, common::ObArray<ObPartitionInfo*>& partitions, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  ObPartitionTableProxy* proxy = NULL;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (keys.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid key cnt", K(ret));
  } else {
    uint64_t table_id = keys.at(0).get_table_id();
    ObPartitionTableProxyFactory factory(*sql_proxy_, merge_error_cb_, config_);
    if (OB_FAIL(factory.get_proxy(table_id, proxy))) {
      LOG_WARN("get partition table proxy failed", K(ret), KT(table_id));
    } else if (NULL == proxy) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL proxy", K(ret), KT(table_id));
    } else if (OB_FAIL(proxy->batch_fetch_partition_infos(keys, allocator, partitions, cluster_id))) {
      LOG_WARN("batch get partition table failed", K(ret), K(cluster_id), "cnt", keys.count());
    }
  }
  return ret;
}

int ObPersistentPartitionTable::update_leader_replica(
    ObPartitionTableProxy& pt_proxy, ObPartitionInfo& partition, const ObPartitionReplica& replica)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAddr, ObPartitionReplica::DEFAULT_REPLICA_COUNT> members;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!partition.is_valid() || !replica.is_valid() || !replica.is_leader_like()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(partition), K(replica));
  } else if (!replica.is_restore_leader() &&
             OB_FAIL(prop_getter_->get_leader_member(replica.partition_key(), members))) {
    LOG_WARN("get leader member failed", K(ret), K(replica));
    ObPartitionReplica follower_replica;
    if (OB_FAIL(follower_replica.assign(replica))) {
      LOG_WARN("failed to assign follower_replica", K(ret));
    } else {
      follower_replica.role_ = FOLLOWER;
      // reset ret to avoid infinity retry.
      // TODO : only NOT_LEADER error can do this?
      if (OB_FAIL(update_follower_replica(pt_proxy, partition, follower_replica))) {
        LOG_WARN("update follower replica failed", K(ret), K(replica));
      }
    }
  } else {
    ObPartitionInfo::ReplicaArray& all_replicas = partition.get_replicas_v2();
    // replicas not in leader member
    ObSEArray<ObAddr, ObPartitionReplica::DEFAULT_REPLICA_COUNT> to_remove;
    for (int64_t i = 0; OB_SUCC(ret) && i < all_replicas.count(); ++i) {
      if (partition.is_leader_like(i) && all_replicas.at(i).server_ != replica.server_) {
        LOG_INFO("update replica role to FOLLOWER", "replica", all_replicas.at(i));
        if (OB_FAIL(
                pt_proxy.set_to_follower_role(replica.table_id_, replica.partition_id_, all_replicas.at(i).server_))) {
          LOG_WARN("update replica role to follower failed", K(ret), "replica", all_replicas.at(i));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(update_replica(pt_proxy, partition, replica))) {
        LOG_WARN("add replica failed", K(ret), K(replica));
      }
    }
  }
  return ret;
}
}  // end namespace share
}  // end namespace oceanbase
