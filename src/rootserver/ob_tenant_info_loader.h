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

#ifndef OCEANBASE_ROOTSERVER_OB_TENANT_INFO_LOADER_H
#define OCEANBASE_ROOTSERVER_OB_TENANT_INFO_LOADER_H

#include "lib/thread/ob_reentrant_thread.h"//ObRsReentrantThread
#include "lib/utility/ob_print_utils.h" //TO_STRING_KV
#include "share/ob_tenant_info_proxy.h"//ObAllTenantInfo
#include "lib/lock/ob_spin_rwlock.h" //lock
#include "rootserver/ob_tenant_role_transition_service.h"//ObTenantRoleTransitionConstants

namespace oceanbase {
namespace common
{
class ObMySQLProxy;
}
namespace share
{
class SCN;
}
namespace rootserver
{

class ObAllTenantInfoCache
{
public:
  ObAllTenantInfoCache()
     : lock_(),
       tenant_info_(),
       last_sql_update_time_(OB_INVALID_TIMESTAMP),
       dump_tenant_info_interval_(DUMP_TENANT_INFO_INTERVAL),
       ora_rowscn_(0) {}
  ~ObAllTenantInfoCache() {}
  int get_tenant_info(share::ObAllTenantInfo &tenant_info);
  int get_tenant_info(share::ObAllTenantInfo &tenant_info, int64_t &last_sql_update_time, int64_t &ora_rowscn);
  int refresh_tenant_info(const uint64_t tenant_id, common::ObMySQLProxy *sql_proxy, bool &refreshed);
  void reset();
  void set_refresh_interval_for_sts();
  int update_tenant_info_cache(const int64_t new_ora_rowscn, const ObAllTenantInfo &new_tenant_info, bool &refreshed);
private:
  const static int64_t DUMP_TENANT_INFO_INTERVAL = 3 * 1000 * 1000; // 3s

public:
  TO_STRING_KV(K_(last_sql_update_time), K_(tenant_info), K_(ora_rowscn));
  DECLARE_TO_YSON_KV;

private:
  common::SpinRWLock lock_;
  share::ObAllTenantInfo tenant_info_;
  int64_t last_sql_update_time_;
  common::ObTimeInterval dump_tenant_info_interval_;
  int64_t ora_rowscn_;
  DISALLOW_COPY_AND_ASSIGN(ObAllTenantInfoCache);
};

/*description:
 * Periodically cache tenant info.*/
class ObTenantInfoLoader : public share::ObReentrantThread
{
public:
 ObTenantInfoLoader()
     : is_inited_(false),
       tenant_id_(common::OB_INVALID_TENANT_ID),
       tenant_info_cache_(),
       sql_proxy_(nullptr),
       broadcast_times_(0),
       rpc_update_times_(0),
       sql_update_times_(0),
       last_rpc_update_time_us_(OB_INVALID_TIMESTAMP),
       dump_tenant_info_cache_update_action_interval_(DUMP_TENANT_INFO_CACHE_UPDATE_ACTION_INTERVAL) {}
 ~ObTenantInfoLoader() {}
 static int mtl_init(ObTenantInfoLoader *&ka);
 int init();
 void destroy();
 int start();
 void stop();
 void wait();
 void wakeup();
 virtual int blocking_run() {
   BLOCKING_RUN_IMPLEMENT();
 }
 virtual void run2() override;
 int get_tenant_info(share::ObAllTenantInfo &tenant_info);
 /**
  * @description:
  *    get valid sts, only return sts refreshed after specified_time
  * @param[in] specified_time sts refreshed after specified_time
  * @param[out] standby_scn sts
  * @return return code
  *    OB_NEED_WAIT possible reason
  *       1. tenant info cache is not refreshed, need wait
  *       2. tenant info cache is old, need wait
  *       3. sts can not work for current tenant status
  */
 int get_valid_sts_after(const int64_t specified_time_us, share::SCN &standby_scn);
 int refresh_tenant_info();
 int update_tenant_info_cache(const int64_t new_ora_rowscn, const ObAllTenantInfo &new_tenant_info);
 bool need_refresh(const int64_t refresh_time_interval_us);
private:
 bool is_sys_ls_leader_();
 void broadcast_tenant_info_content_();
 void dump_tenant_info_(
      const int64_t sql_update_cost_time,
      const bool is_sys_ls_leader,
      const int64_t broadcast_cost_time,
      const int64_t end_time_us,
      int64_t &last_dump_time_us);
 bool act_as_standby_();

public:
 TO_STRING_KV(K_(is_inited), K_(tenant_id), K_(tenant_info_cache), K_(broadcast_times), K_(rpc_update_times), K_(sql_update_times), K_(last_rpc_update_time_us));

private:
  const static int64_t DUMP_TENANT_INFO_CACHE_UPDATE_ACTION_INTERVAL = 1 * 1000 * 1000; // 1s

private:
  bool is_inited_;
  uint64_t tenant_id_;
  ObAllTenantInfoCache tenant_info_cache_;
  common::ObMySQLProxy *sql_proxy_;
  uint64_t broadcast_times_;
  uint64_t rpc_update_times_;
  uint64_t sql_update_times_;
  int64_t last_rpc_update_time_us_;
  common::ObTimeInterval dump_tenant_info_cache_update_action_interval_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantInfoLoader);
};

} // namespace rootserver
} // namespace oceanbase

#endif /* !OCEANBASE_ROOTSERVER_OB_TENANT_INFO_LOADER_H */