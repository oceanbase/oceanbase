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

#ifndef OCEANBASE_GTS_OB_HA_GTS_SOURCE_H_
#define OCEANBASE_GTS_OB_HA_GTS_SOURCE_H_

#include "common/ob_member_list.h"
#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "lib/task/ob_timer.h"
#include "share/ob_gts_info.h"
#include "share/ob_gts_table_operator.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_thread_pool.h"
#include "ob_ha_gts_define.h"
#include "storage/transaction/ob_trans_define.h"

namespace oceanbase {
namespace obrpc {
class ObSrvRpcProxy;
}
namespace common {
class ObMySQLProxy;
}
namespace gts {
class ObHaGtsSource : public share::ObThreadPool {
public:
  ObHaGtsSource();
  ~ObHaGtsSource();

public:
  int init(obrpc::ObSrvRpcProxy* rpc_proxy, common::ObMySQLProxy* sql_proxy, const common::ObAddr& self_addr);
  void reset();
  void destroy();
  int start();
  void stop();
  void wait();

public:
  // if gts does not support this tenant, return OB_NOT_SUPPORTED;
  // if stc <= tenant_cache.srr_, return tenant_cache.gts_;
  // others, return OB_EAGAIN;
  int get_gts(const uint64_t tenant_id, const transaction::MonotonicTs stc, int64_t& gts) const;
  int trigger_gts();
  int handle_get_response(const obrpc::ObHaGtsGetResponse& response);
  // refresh GTS thread
  void run1();
  // work for loading GTSInfo thread
  void runTimerTask();
  void refresh_gts(const uint64_t gts_id, const common::ObAddr& gts_server);

private:
  class ObTenantCache {
  public:
    ObTenantCache()
    {
      reset();
    }
    ~ObTenantCache()
    {
      reset();
    }

  public:
    void reset()
    {
      srr_.reset();
      gts_ = common::OB_INVALID_TIMESTAMP;
    }
    void set_srr_and_gts(const transaction::MonotonicTs srr, const int64_t gts)
    {
      srr_ = srr;
      gts_ = gts;
    }
    transaction::MonotonicTs get_srr() const
    {
      return srr_;
    }
    int64_t get_gts() const
    {
      return gts_;
    }

  private:
    transaction::MonotonicTs srr_;
    int64_t gts_;
  };

  struct ObTenantLease {
    uint64_t tenant_id_;
    transaction::MonotonicTs lease_begin_;
    transaction::MonotonicTs lease_end_;
    TO_STRING_KV(K(tenant_id_), K(lease_begin_), K(lease_end_));
  };
  typedef common::ObSEArray<ObTenantLease, 16> ObTenantLeaseArray;

  class ObGtsMeta {
  public:
    ObGtsMeta()
    {
      reset();
    }
    ~ObGtsMeta()
    {
      destroy();
    }

  public:
    void reset()
    {
      tenant_lease_array_.reset();
      member_list_.reset();
    }
    void destroy()
    {
      tenant_lease_array_.destroy();
      member_list_.reset();
    }
    const ObTenantLeaseArray& get_tenant_lease_array() const
    {
      return tenant_lease_array_;
    }
    ObTenantLeaseArray& get_tenant_lease_array()
    {
      return tenant_lease_array_;
    }
    const common::ObMemberList& get_member_list() const
    {
      return member_list_;
    }
    void set_member_list(const common::ObMemberList& member_list)
    {
      member_list_ = member_list;
    }
    void set_tenant_lease_array(const ObTenantLeaseArray& tenant_lease_array)
    {
      int ret = OB_SUCCESS;
      if (OB_FAIL(tenant_lease_array_.assign(tenant_lease_array))) {
        STORAGE_LOG(WARN, "tenant_lease_array_ assign failed", K(ret));
      }
    }
    TO_STRING_KV(K(tenant_lease_array_), K(member_list_));

  private:
    ObTenantLeaseArray tenant_lease_array_;
    common::ObMemberList member_list_;
  };

  class ObHaGtsSourceTask : public common::ObTimerTask {
  public:
    ObHaGtsSourceTask() : gts_source_(NULL)
    {}
    ~ObHaGtsSourceTask()
    {}

  public:
    int init(ObHaGtsSource* gts_source);
    virtual void runTimerTask();

  private:
    ObHaGtsSource* gts_source_;
  };

  typedef common::ObLinearHashMap<ObGtsID, ObGtsMeta> ObGtsMetaMap;
  typedef common::ObLinearHashMap<ObTenantID, ObTenantCache> ObTenantCacheMap;
  typedef common::ObSEArray<uint64_t, 16> ObTenantArray;
  typedef common::ObSEArray<uint64_t, 16> ObGtsIDArray;
  typedef common::ObSEArray<ObGtsMeta, 4> ObGtsMetaArray;
  typedef common::ObSEArray<common::ObGtsTenantInfo, 16> ObGtsTenantInfoArray;
  class GetTenantArrayFunctor;
  class ModifyTenantCacheFunctor;
  class RefreshGtsFunctor;
  class ModifyGtsMetaFunctor;
  const static int64_t TIMER_TASK_INTERVAL = 1 * 1000 * 1000;
  const static uint64_t REFRESHING_FLAG = (1ULL << 63);
  const static int64_t RETRY_TRIGGER_INTERVAL = 10 * 1000;

private:
  void refresh_gts_();
  void load_gts_meta_map_();
  int get_gts_meta_array_(ObGtsIDArray& gts_id_array, ObGtsMetaArray& gts_meta_array);
  int handle_gts_meta_array_(const ObGtsIDArray& gts_id_array, const ObGtsMetaArray& gts_meta_array);
  static int64_t get_refresh_ts_(const int64_t refresh_ts);
  static bool is_refreshing_(const int64_t refresh_ts);
  static int64_t set_refreshing_(const int64_t refresh_ts);
  static int64_t reset_refreshing_(const int64_t refresh_ts);

private:
  bool is_inited_;
  ObGtsMetaMap gts_meta_map_;
  ObTenantCacheMap tenant_cache_map_;
  ObHaGtsSourceTask task_;
  common::ObTimer timer_;
  obrpc::ObSrvRpcProxy* rpc_proxy_;
  share::ObGtsTableOperator gts_table_operator_;
  common::ObAddr self_addr_;
  int64_t latest_refresh_ts_;
  int64_t latest_trigger_ts_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObHaGtsSource);
};
}  // namespace gts
}  // namespace oceanbase

#endif  // OCEANBASE_GTS_OB_HA_GTS_SOURCE_H_
