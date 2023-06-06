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

#ifndef __SQL_ENG_PX_TARGET_MGR_H__
#define __SQL_ENG_PX_TARGET_MGR_H__

#include "share/ob_thread_pool.h"
#include "share/ob_define.h"
#include "ob_px_tenant_target_monitor.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/hash/ob_link_hashmap.h"
#include "ob_px_rpc_proxy.h"
#include "share/ob_alive_server_tracer.h"

namespace oceanbase
{
using namespace share;
namespace sql
{
typedef common::ObIntWarp ObPxTenantInfo;
typedef common::LinkHashNode<ObPxTenantInfo> ObPxTenantInfoNode;
typedef common::LinkHashValue<ObPxTenantInfo> ObPxTenantInfoValue;

class ObPxResInfo : public ObPxTenantInfoValue
{
public:
  ObPxResInfo() {}
  ~ObPxResInfo() {}
  void destroy();
  ObPxTenantTargetMonitor* get_target_monitor() { return &target_monitor_; }
private:
  bool is_inited_;
  ObPxTenantTargetMonitor target_monitor_;
};

class ObPxInfoAlloc
{
public:
  static ObPxResInfo *alloc_value() { return NULL; }
  static void free_value(ObPxResInfo *info)
  {
    if (NULL != info) {
      info->~ObPxResInfo();
      ob_free(info);
      info = NULL;
    }
  }
  static ObPxTenantInfoNode *alloc_node(ObPxResInfo *p)
  {
    UNUSED(p);
    return OB_NEW(ObPxTenantInfoNode, "PxTargetMgr");
  }
  static void free_node(ObPxTenantInfoNode *node)
  {
    if (NULL != node) {
      OB_DELETE(ObPxTenantInfoNode, "PxTargetMgr", node);
      node = NULL;
    }
  }
};

typedef common::ObLinkHashMap<ObPxTenantInfo, ObPxResInfo, ObPxInfoAlloc> ObPxInfoMap;

class ObPxGlobalResGather
{
public:
  ObPxGlobalResGather(ObPxRpcFetchStatResponse &result) : result_(result) {}
  ~ObPxGlobalResGather() {}
  int operator()(hash::HashMapPair<ObAddr, ServerTargetUsage> &entry)
  {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(result_.push_peer_target_usage(entry.first, entry.second.get_peer_used()))) {
      COMMON_LOG(WARN, "push_back peer_used failed", K(ret));
    }
    return ret;
  }
  ObPxRpcFetchStatResponse &result_;
};

class ObPxTargetMgr
    : public share::ObThreadPool
{

#define PX_REFRESH_TARGET_INTERVEL_US (500 * 1000)
#define PX_MAX_ALIVE_SERVER_NUM (2000)

public:
  ObPxTargetMgr() { reset(); }
  ~ObPxTargetMgr() { destroy(); }
  int init(const common::ObAddr &server,
           share::ObIAliveServerTracer &server_tracer);
  void reset();
  int start();
  void stop();
  void wait();
  void destroy();
  void run1();
public:
  static ObPxTargetMgr &get_instance();

  // for target_mgr or monitor
  int add_tenant(uint64_t tenant_id);
  int delete_tenant(uint64_t tenant_id);
  int set_parallel_servers_target(uint64_t tenant_id, int64_t parallel_servers_target);
  int get_parallel_servers_target(uint64_t tenant_id, int64_t &parallel_servers_target);
  int get_parallel_session_count(uint64_t tenant_id, int64_t &parallel_session_count);

  // for rpc
  int is_leader(uint64_t tenant_id,  bool &is_leader);
  int get_version(uint64_t tenant_id, uint64_t &version);
  int update_peer_target_used(uint64_t tenant_id, const ObAddr &server, int64_t peer_used, uint64_t version);
  int gather_global_target_usage(uint64_t tenant_id, ObPxGlobalResGather &gather);
  int reset_leader_statistics(uint64_t tenant_id);
  
  // for px_admission
  int apply_target(uint64_t tenant_id, hash::ObHashMap<ObAddr, int64_t> &worker_map,
                   int64_t wait_time_us, int64_t session_target, int64_t req_cnt,
                   int64_t &admit_count, uint64_t &admit_version);
  int release_target(uint64_t tenant_id, hash::ObHashMap<ObAddr, int64_t> &worker_map, uint64_t admit_version);

  // for virtual_table iter
  int get_all_tenant(common::ObSEArray<uint64_t, 4> &tenant_array);
  int get_all_target_info(uint64_t tenant_id, common::ObIArray<ObPxTargetInfo> &target_info_array);
private:
  bool is_inited_;
  bool is_running_;
  common::ObAddr server_;
  share::ObIAliveServerTracer *server_tracer_;
  ObPxInfoMap px_info_map_; // 如果考虑删租户就要加锁
  hash::ObHashSet<ObAddr> alive_server_set_;
};

class ObPxResRefreshFunctor
{
public:
  ObPxResRefreshFunctor() : need_refresh_all_(true) {}
  ~ObPxResRefreshFunctor() {}
  bool operator()(const ObPxTenantInfo &px_tenant_info, ObPxResInfo *px_res_info)
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(px_res_info)) {
      ret = common::OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "px_res_info is null", K(ret), K(px_tenant_info));
    } else if (OB_ISNULL(px_res_info->get_target_monitor())) {
      ret = common::OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "target_monitor is null", K(ret), K(px_tenant_info));
    } else if (OB_FAIL(px_res_info->get_target_monitor()->refresh_statistics(need_refresh_all_))) {
      COMMON_LOG(WARN, "target monitor refresh statistics failed", K(ret), K(px_tenant_info), KPC(px_res_info->get_target_monitor()));
    }
    //外面需要遍历所有的租户，此处不能返回false
    return true;
  }
  
  void set_need_refresh_all(bool need_refresh_all) { need_refresh_all_ = need_refresh_all; }

  bool need_refresh_all_;
};

#define OB_PX_TARGET_MGR (::oceanbase::sql::ObPxTargetMgr::get_instance())

}
}

#endif /* __SQL_ENG_PX_RESOURCE_MGR_H__ */
//// end of header file
