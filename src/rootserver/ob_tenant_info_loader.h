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
#include "share/ob_service_name_proxy.h"//ObServiceName
#include "lib/lock/ob_spin_rwlock.h" //lock
#include "rootserver/standby/ob_tenant_role_transition_service.h"//ObTenantRoleTransitionConstants

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
       ora_rowscn_(0),
       is_data_version_crossed_(false),
       finish_data_version_(0)
  {
    data_version_barrier_scn_.set_min();
  }
  ~ObAllTenantInfoCache() {}
  int get_tenant_info(share::ObAllTenantInfo &tenant_info);
  int get_tenant_info(share::ObAllTenantInfo &tenant_info, int64_t &last_sql_update_time, int64_t &ora_rowscn);
  int get_tenant_info(share::ObAllTenantInfo &tenant_info,
                      int64_t &last_sql_update_time, int64_t &ora_rowscn,
                      uint64_t &finish_data_version, share::SCN &data_version_barrier_scn);
  int refresh_tenant_info(const uint64_t tenant_id, common::ObMySQLProxy *sql_proxy, bool &content_changed);
  void reset();
  void set_refresh_interval_for_sts();
  int update_tenant_info_cache(const int64_t new_ora_rowscn, const ObAllTenantInfo &new_tenant_info,
                               const uint64_t new_finish_data_version,
                               const share::SCN &new_data_version_barrier_scn, bool &refreshed);
  int is_data_version_crossed(bool &is_data_version_crossed, share::SCN &data_version_barrier_scn);

private:
  bool is_tenant_info_valid_();
  int assign_new_tenant_info_(const int64_t new_ora_rowscn, const ObAllTenantInfo &new_tenant_info,
                              const uint64_t new_finish_data_version,
                              const share::SCN &new_data_version_barrier_scn, bool &assigned);
  int query_new_finish_data_version_(const uint64_t tenant_id, common::ObMySQLProxy *sql_proxy,
                                     uint64_t &finish_data_version,
                                     share::SCN &data_version_barrier_scn);
  const static int64_t DUMP_TENANT_INFO_INTERVAL = 3 * 1000 * 1000; // 3s

public:
  TO_STRING_KV(K_(last_sql_update_time), K_(tenant_info), K_(ora_rowscn),
               K_(is_data_version_crossed), K_(finish_data_version), K_(data_version_barrier_scn));
  DECLARE_TO_YSON_KV;

private:
  common::SpinRWLock lock_;
  share::ObAllTenantInfo tenant_info_;
  int64_t last_sql_update_time_;
  common::ObTimeInterval dump_tenant_info_interval_;
  int64_t ora_rowscn_;
  bool is_data_version_crossed_;
  uint64_t finish_data_version_;
  share::SCN data_version_barrier_scn_;
  DISALLOW_COPY_AND_ASSIGN(ObAllTenantInfoCache);
};

class ObAllServiceNamesCache
{
public:
  ObAllServiceNamesCache()
    : lock_(),
      tenant_id_(OB_INVALID_TENANT_ID),
      epoch_(0),
      all_service_names_(),
      dump_service_names_interval_(DUMP_SERVICE_NAMES_INTERVAL),
      last_refresh_time_(OB_INVALID_TIMESTAMP),
      is_service_name_enabled_(false) {}
  ~ObAllServiceNamesCache() {}
  int init(const uint64_t tenant_id);
  int refresh_service_name();
  int update_service_name(const int64_t epoch, const common::ObIArray<share::ObServiceName> &service_name_list);
  bool need_refresh();
  int check_if_the_service_name_is_stopped(const ObServiceNameString &service_name_str) const;
  int get_service_name(const ObServiceNameID &service_name_id, ObServiceName &service_name) const;
  void reset();
private:
  static constexpr int64_t DUMP_SERVICE_NAMES_INTERVAL = 5 * 1000L * 1000L; // 5s
  static constexpr int64_t REFRESH_INTERVAL = 2 * 1000L * 1000L; // 2s
  common::SpinRWLock lock_;
  uint64_t tenant_id_;
  int64_t epoch_;
  ObArray<ObServiceName> all_service_names_;
  common::ObTimeInterval dump_service_names_interval_;
  int64_t last_refresh_time_;
  bool is_service_name_enabled_;
  DISALLOW_COPY_AND_ASSIGN(ObAllServiceNamesCache);
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
       dump_tenant_info_cache_update_action_interval_(DUMP_TENANT_INFO_CACHE_UPDATE_ACTION_INTERVAL),
       service_names_cache_() {}
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
 int check_if_sts_is_ready(bool &is_ready);
 /**
  * @description:
  *    get tenant standby scn.
  *       for SYS/META/user tenant: use GTS/STS cache as readable_scn
  * @param[out] readable_scn
  */
 int get_readable_scn(share::SCN &readable_scn);

 /**
  * @description:
  *    get tenant's global replayable_scn.
  *       for SYS/META tenant: there isn't replayable_scn
  *       for user tenant: get replayable_scn for tenant, which is directly retrieved from
  *                        __all_tenant_info cache
  * @param[out] replayable_scn
  */
 int get_global_replayable_scn(share::SCN &replayable_scn);

 /**
  * @description:
  *    get tenant's local replayable_scn.
  *       for SYS/META tenant: there isn't replayable_scn
  *       for user tenant: get replayable_scn for current machine, which may be smaller than the
  *                        global replayable_scn if current machine's data version has been synced
  *    by using this interface, we can ensure the log stream will never replay new version log in
  *    old data version
  * @param[out] replayable_scn
  */
 int get_local_replayable_scn(share::SCN &replayable_scn);
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
  *    get tenant recovery_until_scn.
  *       for SYS/META tenant: there isn't recovery_until_scn
  *       for user tenant: get recovery_until_scn from __all_tenant_info cache
  * @param[out] recovery_until_scn
  */
 int get_recovery_until_scn(share::SCN &recovery_until_scn);

   /**
  * @description:
  *    get tenant restore_data_mode.
  *       for SYS/META tenant: return normal restore data mode
  *       for user tenant: get restore_data_mode from __all_tenant_info cache
  * @param[out] restore_data_mode
  */
 int get_restore_data_mode(share::ObRestoreDataMode &restore_data_mode);

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
 int check_is_prepare_flashback_for_switch_to_primary_status(bool &is_prepare);

 int refresh_tenant_info();
 int refresh_service_name();
 int update_service_name(const int64_t epoch, const common::ObIArray<share::ObServiceName> &service_name_list);
 int update_tenant_info_cache(const int64_t new_ora_rowscn, const ObAllTenantInfo &new_tenant_info,
                              const uint64_t new_finish_data_version,
                              const share::SCN &new_data_version_barrier_scn);
 bool need_refresh(const int64_t refresh_time_interval_us);
 int get_max_ls_id(uint64_t &tenant_id, ObLSID &max_ls_id);
 int get_switchover_epoch(int64_t &switchover_epoch);
 /**
  * @description:
  *    check if service_status of the given service_name is STOPPED or STOPPING
  * @param[in] service_name_str service_name string
  * @return return code
  *         OB_SERVICE_NAME_NOT_FOUND service_name is not found, cannot check its service_status
  *         OB_SERVICE_STOPPED service_status is STOPPED or STOPPING
  *         OB_SUCCESS service_name exists and its service_status is STARTED
  *         others
  */
 int check_if_the_service_name_is_stopped(const ObServiceNameString &service_name_str) const
 {
  return service_names_cache_.check_if_the_service_name_is_stopped(service_name_str);
 }
 bool get_service_name(const ObServiceNameID &service_name_id, ObServiceName &service_name) const
 {
  return service_names_cache_.get_service_name(service_name_id, service_name);
 }


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
  ObAllServiceNamesCache service_names_cache_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantInfoLoader);
};

} // namespace rootserver
} // namespace oceanbase

#endif /* !OCEANBASE_ROOTSERVER_OB_TENANT_INFO_LOADER_H */
