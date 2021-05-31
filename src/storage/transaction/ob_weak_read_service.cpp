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

#define USING_LOG_PREFIX TRANS

#include "ob_weak_read_service.h"

#include "storage/ob_partition_service.h"   // ObPartitionService
#include "share/rc/ob_context.h"            // WITH_CONTEXT
#include "share/rc/ob_tenant_base.h"        // MTL_GET
#include "share/config/ob_server_config.h"  // ObServerConfig
#include "lib/container/ob_array.h"         // ObArray
#include "lib/thread/ob_thread_name.h"
#include "observer/omt/ob_multi_tenant.h"  // TenantIdList
#include "ob_weak_read_util.h"             //ObWeakReadUtil

namespace oceanbase {
using namespace common;
using namespace storage;
using namespace obrpc;
using namespace share;

namespace transaction {
int ObWeakReadService::init(const rpc::frame::ObReqTransport* transport)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(wrs_rpc_.init(transport, *this))) {
    LOG_WARN("init tenant map fail", KR(ret));
  } else {
    server_version_epoch_tstamp_ = 0;
    inited_ = true;
    LOG_INFO("[WRS] weak read service init succ");
  }
  return ret;
}

void ObWeakReadService::destroy()
{
  LOG_INFO("[WRS] weak read service begin destroy");
  if (inited_) {
    stop();
    wait();
    inited_ = false;
    server_version_epoch_tstamp_ = 0;
  }
  LOG_INFO("[WRS] weak read service destroy succ");
}

int ObWeakReadService::start()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(share::ObThreadPool::start())) {
    LOG_ERROR("Weak Read Service thread start error", KR(ret));
  } else {
    LOG_INFO("[WRS] weak read service thread start");
  }
  return ret;
}

void ObWeakReadService::stop()
{
  LOG_INFO("[WRS] weak read service thread stop");
  share::ObThreadPool::stop();
}

void ObWeakReadService::wait()
{
  LOG_INFO("[WRS] weak read service thread wait");
  share::ObThreadPool::wait();
}

int ObWeakReadService::get_server_version(const uint64_t tenant_id, int64_t& version) const
{
  int ret = OB_SUCCESS;
  // switch tenant
  FETCH_ENTITY(TENANT_SPACE, tenant_id)
  {
    ObTenantWeakReadService* twrs = MTL_GET(ObTenantWeakReadService*);
    if (OB_ISNULL(twrs)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("MTL_GET ObTenantWeakReadService object is NULL", K(twrs), K(tenant_id), KR(ret));
    } else {
      version = twrs->get_server_version();
    }
  }
  else
  {
    LOG_WARN("change tenant context fail when get weak read service server version", KR(ret), K(tenant_id));
  }

  if (OB_SUCC(ret) && version <= 0) {
    int old_ret = ret;
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR(
        "get server version succ, but version not bigger than zero", K(ret), K(old_ret), K(tenant_id), K(version));
  }
  LOG_DEBUG("[WRS] get_server_version", K(ret), K(tenant_id), K(version));

  return ret;
}

int ObWeakReadService::get_cluster_version(const uint64_t tenant_id, int64_t& version)
{
  int ret = OB_SUCCESS;
  // switch tenant
  FETCH_ENTITY(TENANT_SPACE, tenant_id)
  {
    ObTenantWeakReadService* twrs = MTL_GET(ObTenantWeakReadService*);
    if (OB_ISNULL(twrs)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("MTL_GET ObTenantWeakReadService object is NULL", K(twrs), K(tenant_id), KR(ret));
    } else if (OB_FAIL(twrs->get_cluster_version(version))) {
      LOG_WARN("get tenant weak read service cluster version fail", KR(ret), K(tenant_id));
    } else {
      // success
    }
  }
  else
  {
    LOG_WARN("change tenant context fail when get weak read service cluster version", KR(ret), K(tenant_id));
  }

  if (OB_SUCC(ret) && version <= 0) {
    int old_ret = ret;
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR(
        "get cluster version succ, but version not bigger than zero", K(ret), K(old_ret), K(tenant_id), K(version));
  }
  LOG_DEBUG("[WRS] get_cluster_version", K(ret), K(tenant_id), K(version));

  return ret;
}

void ObWeakReadService::check_server_can_start_service(bool& can_start_service, int64_t& min_wrs) const
{
  int ret = OB_SUCCESS;
  int64_t safe_weak_read_snapshot = INT64_MAX;
  ObPartitionKey pkey;
  ObPartitionService& ps = ObPartitionService::get_instance();
  if (OB_FAIL(ps.check_can_start_service(can_start_service, safe_weak_read_snapshot, pkey))) {
    LOG_WARN("partition service check can start service error", KR(ret));
  } else if (!can_start_service) {
    if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      LOG_INFO("[WRS] [NOTICE] server can not start service",
          K(can_start_service),
          K(safe_weak_read_snapshot),
          "delta",
          ObTimeUtility::current_time() - safe_weak_read_snapshot,
          K(pkey));
    }
  } else {
    LOG_INFO("[WRS] [NOTICE] server can start service",
        K(can_start_service),
        K(safe_weak_read_snapshot),
        "delta",
        ObTimeUtility::current_time() - safe_weak_read_snapshot,
        K(pkey));
  }
  if (OB_SUCC(ret)) {
    min_wrs = safe_weak_read_snapshot;
  }
}

void ObWeakReadService::run1()
{
  int ret = OB_SUCCESS;
  int64_t total_time = 0;
  int64_t loop_count = 0;
  int64_t refresh_interval = 0;
  lib::set_thread_name("WeakReadSvr");
  while (!has_set_stop()) {
    refresh_interval = ObWeakReadUtil::replica_keepalive_interval();
    int64_t start_time = ObTimeUtility::current_time();
    int64_t valid_user_part_count = 0, skip_user_part_count = 0;
    int64_t valid_inner_part_count = 0, skip_inner_part_count = 0;
    loop_count++;
    if (OB_FAIL(update_server_version_epoch_tstamp_(start_time))) {
      LOG_WARN("update server version epoch timestmap fail", KR(ret), K(start_time));
    } else if (OB_FAIL(scan_all_partitions_(
                   valid_user_part_count, skip_user_part_count, valid_inner_part_count, skip_inner_part_count))) {
      LOG_WARN("scan all partition fail", KR(ret));
    } else if (OB_FAIL(handle_all_tenant_())) {
      LOG_WARN("handle all tenants fail", KR(ret));
    } else {
      // success
    }

    int64_t time_used = ObTimeUtility::current_time() - start_time;
    total_time += time_used;
    DEBUG_SYNC(BLOCK_CALC_WRS);
    if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      LOG_INFO("[WRS] weak read service task statistics",
          "avg_time",
          total_time / loop_count,
          K(valid_user_part_count),
          K(skip_user_part_count),
          K(valid_inner_part_count),
          K(skip_inner_part_count),
          K(refresh_interval));
      total_time = 0;
      loop_count = 0;
    }

    if (refresh_interval > time_used) {
      usleep(refresh_interval - time_used);
    }
  }
}

int ObWeakReadService::update_server_version_epoch_tstamp_(const int64_t cur_time)
{
  if (cur_time > server_version_epoch_tstamp_) {
    server_version_epoch_tstamp_ = cur_time;
  } else {
    server_version_epoch_tstamp_++;
  }
  return OB_SUCCESS;
}

// scan all partitions
int ObWeakReadService::scan_all_partitions_(int64_t& valid_user_part_count, int64_t& skip_user_part_count,
    int64_t& valid_inner_part_count, int64_t& skip_inner_part_count)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupIterator* iter = NULL;
  valid_user_part_count = 0;
  skip_user_part_count = 0;
  valid_inner_part_count = 0;
  skip_inner_part_count = 0;
  ObPartitionService& ps = ObPartitionService::get_instance();
  // partitions of tenant not exist in local server
  ObArray<ObPartitionKey> no_tenant_part_arr;
  if (OB_ISNULL(iter = ps.alloc_pg_iter())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc partition scan iter", KR(ret));
  } else {
    int64_t start_time = ObTimeUtility::current_time();
    while (OB_SUCCESS == ret) {
      ObIPartitionGroup* partition = NULL;
      bool need_skip = false;
      bool is_user_part = false;
      if (OB_FAIL(iter->get_next(partition))) {
        if (OB_ITER_END == ret) {
          // iteration end
        } else {
          LOG_WARN("iterate next partition fail", KR(ret));
        }
      } else if (OB_ISNULL(partition)) {
        ret = OB_PARTITION_NOT_EXIST;
        LOG_WARN("iterate partition fail", K(partition));
      } else if (OB_FAIL(handle_partition_(*partition, need_skip, is_user_part))) {
        if (OB_TENANT_NOT_IN_SERVER == ret) {
          (void)no_tenant_part_arr.push_back(partition->get_partition_key());
        } else {
          // handle partition fail, all except OB_TENANT_NOT_IN_SERVER here
          // just skip this partition
          need_skip = true;
          LOG_WARN("handle partition fail", "pkey", partition->get_partition_key(), K(ret), K(need_skip));
        }
        ret = OB_SUCCESS;
      } else {
        // success
      }

      if (OB_SUCCESS == ret) {
        if (is_user_part) {
          if (need_skip) {
            skip_user_part_count++;
          } else {
            valid_user_part_count++;
          }
        } else {
          if (need_skip) {
            skip_inner_part_count++;
          } else {
            valid_inner_part_count++;
          }
        }
      }

      int64_t cur_time = ObTimeUtility::current_time();
      if (cur_time - start_time > 10000) {
        // too many partitions, time cost too much in single scan
        // need sleep to prevent CPU cost too much
        const uint32_t sleep_time = 1000;  // 1ms
        usleep(sleep_time);                // 1ms
        start_time = cur_time + sleep_time;
      }
    }  // while

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }

  if (OB_SUCCESS == ret) {
    if (no_tenant_part_arr.count() > 0 && REACH_TIME_INTERVAL(1 * 1000 * 1000L)) {
      LOG_WARN("[WRS] partition still exists, but tenant not exist",
          "count",
          no_tenant_part_arr.count(),
          K(no_tenant_part_arr));
    }
  }

  if (NULL != iter) {
    ps.revert_pg_iter(iter);
    iter = NULL;
  }

  return ret;
}

int ObWeakReadService::handle_partition_(ObIPartitionGroup& part, bool& need_skip, bool& is_user_part)
{
  int ret = OB_SUCCESS;
  int64_t version = 0;
  ObTenantWeakReadService* twrs = NULL;
  const ObPartitionKey& pkey = part.get_partition_key();
  uint64_t tenant_id = pkey.get_tenant_id();
  ObPartitionService& ps = ObPartitionService::get_instance();

  // tenant maybe not exist, pass second parameter to ignore WARN log
  int64_t max_stale_time =
      ObWeakReadUtil::max_stale_time_for_weak_consistency(tenant_id, ObWeakReadUtil::IGNORE_TENANT_EXIST_WARN);
  // generate partition level weak read snapshot version
  if (OB_FAIL(
          ps.generate_partition_weak_read_snapshot_version(part, need_skip, is_user_part, version, max_stale_time))) {
    LOG_WARN("generate partition weak read snapshot version fail", K(ret), K(pkey));
  } else {
    // switch tenant, get tenant weak read service
    FETCH_ENTITY(TENANT_SPACE, tenant_id)
    {
      if (OB_ISNULL(twrs = MTL_GET(ObTenantWeakReadService*))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("MTL_GET ObTenantWeakReadService object is NULL", KR(ret), K(tenant_id));
      } else if (OB_FAIL(twrs->update_server_version_with_part_info(
                     server_version_epoch_tstamp_, need_skip, is_user_part, version))) {
        LOG_WARN("update tenant server version with partition info fail",
            KR(ret),
            K(server_version_epoch_tstamp_),
            K(need_skip),
            K(is_user_part),
            K(version));
      }
    }
    else if (OB_TENANT_NOT_IN_SERVER == ret)
    {
      LOG_DEBUG("tenant not in server, but partition still exists", KR(ret), K(tenant_id), K(pkey));
    }
    else
    {
      LOG_WARN("change tenant context fail when update server version", KR(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObWeakReadService::handle_all_tenant_()
{
  int ret = OB_SUCCESS;
  omt::TenantIdList all_tenants;
  bool need_print = false;
  static const int64_t PRINT_INTERVAL = 500 * 1000L;  // print all tenants info every 500ms

  // conditions below, tenant weak read snapshot version = current timestamp - max_stale_time
  // weak read retry can support read success
  // 1. not partitions in local server
  // 2. all partitions offline
  // 3. all partitions delay too much or in invalid status, need skip all
  // 4. all partitions in migration, and delay more than 500ms

  if (REACH_TIME_INTERVAL(PRINT_INTERVAL)) {
    need_print = true;
  } else {
    need_print = false;
  }

  if (OB_ISNULL(GCTX.omt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("omt is NULL", KR(ret));
  } else {
    // get tenant ids
    all_tenants.set_label(ObModIds::OB_TENANT_ID_LIST);
    GCTX.omt_->get_tenant_ids(all_tenants);

    for (int64_t index = 0; OB_SUCCESS == ret && index < all_tenants.size(); index++) {
      uint64_t tenant_id = all_tenants[index];

      // skip virtual tenant
      if (!is_virtual_tenant_id(tenant_id)) {
        // switch tenant context
        FETCH_ENTITY(TENANT_SPACE, tenant_id)
        {
          ObTenantWeakReadService* twrs = MTL_GET(ObTenantWeakReadService*);
          if (OB_ISNULL(twrs)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("MTL_GET ObTenantWeakReadService is NULL", KR(ret), K(twrs), K(tenant_id));
          } else if (OB_FAIL(twrs->generate_server_version(server_version_epoch_tstamp_, need_print))) {
            LOG_WARN("generate server version for tenant fail",
                KR(ret),
                K(tenant_id),
                K(index),
                K(server_version_epoch_tstamp_),
                KPC(twrs));
          } else {
            // success
          }
        }
        else if (OB_TENANT_NOT_IN_SERVER == ret)
        {
          LOG_WARN("tenant not in server", KR(ret), K(tenant_id));
        }
        else
        {
          LOG_WARN("change tenant context fail", KR(ret), K(tenant_id), K(index), K(all_tenants));
        }

        // rewrite ret, do all partitions
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

void ObWeakReadService::process_get_cluster_version_rpc(const uint64_t tenant_id,
    const obrpc::ObWrsGetClusterVersionRequest& req, obrpc::ObWrsGetClusterVersionResponse& res)
{
  int ret = OB_SUCCESS;
  int64_t version = 0;
  ObTenantWeakReadService* twrs = NULL;

  FETCH_ENTITY(TENANT_SPACE, tenant_id)
  {
    if (OB_ISNULL(twrs = MTL_GET(ObTenantWeakReadService*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("MTL_GET ObTenantWeakReadService is NULL", KR(ret), K(twrs), K(tenant_id));
    } else if (OB_FAIL(twrs->process_get_cluster_version_rpc(version))) {
      LOG_WARN(
          "tenant weak read service process get cluster version RPC fail", KR(ret), K(tenant_id), K(req), KPC(twrs));
    } else {
      // success
    }
  }
  else
  {
    LOG_WARN("change tenant context fail when process get cluster version RPC", KR(ret), K(tenant_id), K(req));
  }

  // set response
  res.set(ret, version);
}

void ObWeakReadService::process_cluster_heartbeat_rpc(
    const uint64_t tenant_id, const obrpc::ObWrsClusterHeartbeatRequest& req, obrpc::ObWrsClusterHeartbeatResponse& res)
{
  int ret = OB_SUCCESS;
  ObTenantWeakReadService* twrs = NULL;

  FETCH_ENTITY(TENANT_SPACE, tenant_id)
  {
    if (OB_ISNULL(twrs = MTL_GET(ObTenantWeakReadService*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("MTL_GET ObTenantWeakReadService is NULL", KR(ret), K(twrs), K(tenant_id));
    } else if (OB_FAIL(twrs->process_cluster_heartbeat_rpc(req.req_server_,
                   req.version_,
                   req.valid_part_count_,
                   req.total_part_count_,
                   req.generate_timestamp_))) {
      LOG_WARN("tenant weak read service process cluster heartbeat RPC fail", KR(ret), K(tenant_id), K(req), KPC(twrs));
    } else {
      // success
    }
  }
  else
  {
    LOG_WARN("change tenant context fail when process cluster heartbeat RPC", KR(ret), K(tenant_id), K(req));
  }

  // set respnse
  res.set(ret);
}

void ObWeakReadService::process_cluster_heartbeat_rpc_cb(const uint64_t tenant_id, const obrpc::ObRpcResultCode& rcode,
    const obrpc::ObWrsClusterHeartbeatResponse& res, const common::ObAddr& dst)
{
  int ret = OB_SUCCESS;
  ObTenantWeakReadService* twrs = NULL;

  FETCH_ENTITY(TENANT_SPACE, tenant_id)
  {
    if (OB_ISNULL(twrs = MTL_GET(ObTenantWeakReadService*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("MTL_GET ObTenantWeakReadService is NULL", KR(ret), K(twrs), K(tenant_id));
    } else {
      // get response ret
      int ret_code = rcode.rcode_;
      if (OB_SUCCESS == ret_code) {
        ret_code = res.err_code_;
      }
      // handle response ret
      twrs->process_cluster_heartbeat_rpc_cb(rcode, dst);
    }
  }
  else
  {
    LOG_WARN("change tenant context fail when process cluster heartbeat rpc cb",
        KR(ret),
        K(tenant_id),
        K(res),
        K(rcode),
        K(dst));
  }
}

}  // namespace transaction
}  // namespace oceanbase
