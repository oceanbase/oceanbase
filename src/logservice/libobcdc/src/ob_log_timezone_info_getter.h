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
 *
 * TimeZone Info Getter
 */

#ifndef OCEANBASE_LIBOBCDC_TIMEZONE_INFO_GETTER_H__
#define OCEANBASE_LIBOBCDC_TIMEZONE_INFO_GETTER_H__

#include "lib/mysqlclient/ob_mysql_proxy.h"               // ObMySQLProxy
#include "lib/timezone/ob_timezone_info.h"                // ObTZInfoMap
#include "lib/lock/ob_spin_rwlock.h"                      // ObSpinRWLock
#include "common/ob_queue_thread.h"                       // ObCond
#include "ob_log_instance.h"                              //TCTX

namespace oceanbase
{
namespace omt
{
class ObTenantTimezoneGuard;
}
namespace libobcdc
{
class ObCDCTenantTimeZoneInfo;
///////////////////////////////////// IObCDCTimeZoneInfoGetter /////////////////////////////////
class IObCDCTimeZoneInfoGetter
{
public:
  virtual ~IObCDCTimeZoneInfoGetter() {}

public:
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void mark_stop_flag() = 0;

  // Init by ObLogTenant
  virtual int init_tenant_tz_info(const uint64_t tenant_id) = 0;

  virtual int get_tenant_tz_info(
      const uint64_t tenant_id,
      ObCDCTenantTimeZoneInfo *&tenant_tz_info) = 0;

  virtual void remove_tenant_tz_info(const uint64_t tenant_id) = 0;
  /// Refresh timezone info until successful (try refreshing several times)
  virtual int refresh_tenant_timezone_info_until_succ(const uint64_t tenant_id) = 0;
};


///////////////////////////////////// ObCDCTimeZoneInfoGetter /////////////////////////////////
// OP(init/refresh/get) of ObCDCTenantTimeZoneInfo should be thread safe by invoker.
struct ObCDCTenantTimeZoneInfo
{
  ObCDCTenantTimeZoneInfo() :
      is_inited_(false),
      tenant_id_(OB_INVALID_TENANT_ID),
      timezone_info_version_(OB_INVALID_VERSION),
      tz_info_wrap_(),
      tz_info_map_() {}
  ~ObCDCTenantTimeZoneInfo() { destroy(); }

  int init(const uint64_t tenant_id);
  void destroy();
  int set_time_zone(const ObString &timezone_str);
  OB_INLINE bool is_inited() const { return is_inited_; }
  OB_INLINE bool is_valid() const
  { return is_inited_ && timezone_info_version_ > OB_INVALID_VERSION; }
  OB_INLINE bool need_update_tz_info(int64_t target_tz_info_version) const
  { return target_tz_info_version > timezone_info_version_; }
  void update_timezone_info_version(const int64_t timezone_info_version)
  { ATOMIC_STORE(&timezone_info_version_, timezone_info_version); }

  OB_INLINE const common::ObTimeZoneInfoWrap &get_tz_wrap() const { return tz_info_wrap_; }
  OB_INLINE const common::ObTZInfoMap &get_tz_map() const { return tz_info_map_; }
  OB_INLINE const common::ObTimeZoneInfo *get_timezone_info() const
  { return tz_info_wrap_.get_time_zone_info(); }
  TO_STRING_KV(K_(tenant_id), K_(is_inited),  K_(timezone_info_version), K_(tz_info_wrap));

  bool                        is_inited_;
  uint64_t                    tenant_id_;
  // 2_2_6 branch start: Oracle time zone related data types: internal table dependency split to tenant
  // 1. If the low version of OB upgrades to 226, if the low version imports a time zone table, then the post script will split the time zone related table under the tenant
  // 2. If the low version does not import the time zone table, do nothing
  int64_t                     timezone_info_version_;
  common::ObTimeZoneInfoWrap  tz_info_wrap_;
  common::ObTZInfoMap         tz_info_map_;
};

// for init interface OTTZ_MGR.tenant_tz_map_getter_
// return an refreshed tz_map_wrap
int get_tenant_tz_map_function(
    const uint64_t tenant_id,
    common::ObTZMapWrap &tz_map_wrap);

class IObLogErrHandler;
class IObLogSysTableHelper;
typedef common::hash::ObHashMap<uint64_t, ObCDCTenantTimeZoneInfo*> ObLogTZInfoMap;
class ObCDCTimeZoneInfoGetter : public IObCDCTimeZoneInfoGetter
{
  static const int64_t SLEEP_TIME_ON_SCHEMA_FAIL = 500 * 1000;
  static const int64_t QUERY_TIMEZONE_INFO_VERSION_INTERVAL = 100 * 1000 * 1000;

public:
  virtual ~ObCDCTimeZoneInfoGetter();
  static ObCDCTimeZoneInfoGetter &get_instance();

public:
  int init(
      const char *timezone_str,
      common::ObMySQLProxy &mysql_proxy,
      IObLogSysTableHelper &systable_helper,
      IObLogErrHandler &err_handler);
  void destroy();

public:
  virtual int start();
  virtual void stop();
  virtual void mark_stop_flag() { ATOMIC_STORE(&stop_flag_, true); }

  // Init by ObLogTenant initialisation
  virtual int init_tenant_tz_info(const uint64_t tenant_id) override;

  virtual int get_tenant_tz_info(
      const uint64_t tenant_id,
      ObCDCTenantTimeZoneInfo *&tenant_tz_info) override;

  virtual void remove_tenant_tz_info(const uint64_t tenant_id);

  virtual int refresh_tenant_timezone_info_until_succ(const uint64_t tenant_id) override;

  // for OTTZ_MGR, may accquire tenant not in obcdc whitelist
  int get_tenant_timezone_map(const uint64_t tenant_id,
      common::ObTZMapWrap &tz_map_wrap);
  void revert_tenant_tz_info_(ObCDCTenantTimeZoneInfo *tenant_tz_info);

private:
  ObCDCTimeZoneInfoGetter();
  static void *tz_thread_func_(void *args);
  void tz_routine();

  OB_INLINE bool need_fetch_timezone_info_by_tennat_() const
  { return GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2260; }
  OB_INLINE uint64_t get_exec_tenant_id(const uint64_t tenant_id) const
  { return need_fetch_timezone_info_by_tennat_() ? tenant_id : OB_SYS_TENANT_ID; }
  bool need_fetch_tz_info_online_() const;

  int create_tenant_tz_info_(
      const uint64_t tenant_id,
      ObCDCTenantTimeZoneInfo *&tenant_tz_info);

  int refresh_all_tenant_timezone_info_();

  int refresh_tenant_timezone_info_until_succ_(const uint64_t tenant_id, ObCDCTenantTimeZoneInfo &tenant_tz_info);

  // 1. Check the version first, if the version has not changed, then do not refresh
  // 2. Refresh only if the version has changed
  //
  // @retval OB_SUCCESS         Success
  // @retval OB_ENTRY_NOT_EXIST tenant not exist
  // @retval other_error_code   Fail
  int refresh_tenant_timezone_info_based_on_version_(const uint64_t tenant_id, ObCDCTenantTimeZoneInfo &oblog_tz_info);

  // refresh tenant timezone_info from local timezone.info file.
  int refresh_tenant_timezone_info_from_local_file_(
      const uint64_t tenant_id,
      common::ObTZInfoMap &tz_info_map);

  // 1. For versions below 226, there is one global copy of the timezone internal table and only one timezone_verison
  // 2. From version 226, the timezone internal table is split into tenants, each with a timezone_verison; if timezone_version is not available, the tenant has not imported a timezone table
  //
  // @retval OB_SUCCESS         Success
  // @retval OB_ENTRY_NOT_EXIST tenant not exist
  // @retval other_error_code   Fail
  int query_timezone_info_version_(const uint64_t tenant_id,
      int64_t &timezone_info_version);

  int refresh_tenant_timezone_info_map_(const uint64_t tenant_id, ObCDCTenantTimeZoneInfo &tenant_tz_info);
  // export timezone_info_map and demp to local file.
  int export_timezone_info_(common::ObTZInfoMap &tz_info_map);
  // import timezone_info from local file and convert to ObTZInfoMap
  int import_timezone_info_(common::ObTZInfoMap &tz_info_map);
  int load_tzinfo_from_file_(char *buf, const int64_t buf_len);

private:
  static const int64_t TENANT_TZ_INFO_VALUE_SIZE = sizeof(ObCDCTenantTimeZoneInfo);
  static const int MAP_BUCKET_NUM = 4;
private:
  bool                  inited_;
  pthread_t             tz_tid_;
  common::ObCond        tz_cond_;
  volatile bool         stop_flag_ CACHE_ALIGNED;

  common::ObMySQLProxy  *mysql_proxy_;

  IObLogSysTableHelper  *systable_helper_;
  IObLogErrHandler      *err_handler_;

  common::SpinRWLock    lock_;
  // save for init tz_info_wrap
  const char            *timezone_str_;
  ObLogTZInfoMap        oblog_tz_info_map_;
  ObSliceAlloc          allocator_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObCDCTimeZoneInfoGetter);
};
} // namespace libobcdc
} // namespace oceanbase
#endif
