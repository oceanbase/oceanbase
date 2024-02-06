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

class ObRecoveryLSService;
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
  int refresh_tenant_info(const uint64_t tenant_id, common::ObMySQLProxy *sql_proxy, bool &content_changed);
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
  friend class ObRecoveryLSService;
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
 /**
  * @description:
  *    get tenant standby scn.
  *       for SYS/META/user tenant: use GTS/STS cache as readable_scn
  * @param[out] readable_scn
  */
 int get_readable_scn(share::SCN &readable_scn);

 /**
  * @description:
  *    get tenant replayable_scn.
  *       for SYS/META tenant: there isn't replayable_scn
  *       for user tenant: get replayable_scn from __all_tenant_info cache
  * @param[out] replayable_scn
  */
 int get_replayable_scn(share::SCN &replayable_scn);

  /**
  * @description:
  *    get tenant sync_scn.
  *       for SYS/META tenant: there isn't sync_scn
  *       for user tenant: get sync_scn from __all_tenant_info cache
  * @param[out] sync_scn
  */
 int get_sync_scn(share::SCN &sync_scn);

 /**
  * @description:
  *    get tenant is_standby_normal_status
  *       for SYS/META tenant: return false
  *       for user tenant: return tenant_info.is_standby() && tenant_info.is_normal_status()
  * @param[out] is_standby_normal_status
  */
 int check_is_standby_normal_status(bool &is_standby_normal_status);

 /**
  * @description:
  *    get tenant is_primary_normal_status
  *       for SYS/META tenant: return true
  *       for user tenant: return tenant_info.is_primary() && tenant_info.is_normal_status()
  * @param[out] is_primary_normal_status
  */
 int check_is_primary_normal_status(bool &is_primary_normal_status);

 int refresh_tenant_info();
 int update_tenant_info_cache(const int64_t new_ora_rowscn, const ObAllTenantInfo &new_tenant_info);
 bool need_refresh(const int64_t refresh_time_interval_us);
 int get_max_ls_id(uint64_t &tenant_id, ObLSID &max_ls_id);

protected:
 /**
  * @description:
  *    get tenant info
  *       for SYS/META tenant: do not support
  *       for user tenant: tenant_info
  * @param[out] tenant_info
  */
 int get_tenant_info(share::ObAllTenantInfo &tenant_info);

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
