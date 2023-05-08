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
#include "lib/lock/ob_spin_lock.h"                        // ObSpinLock
#include "common/ob_queue_thread.h"                       // ObCond
#include "ob_log_tenant_mgr.h"                            // ObLogTenantMgr
#include "ob_log_instance.h"                              //TCTX

namespace oceanbase
{
namespace omt
{
class ObTenantTimezoneGuard;
}
namespace libobcdc
{
///////////////////////////////////// IObLogTimeZoneInfoGetter /////////////////////////////////
class IObLogTimeZoneInfoGetter
{
public:
  virtual ~IObLogTimeZoneInfoGetter() {}

public:
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void mark_stop_flag() = 0;

  // Init by ObLogTenant initialisation
  virtual int init_tz_info_wrap(
      const uint64_t tenant_id,
      int64_t &tz_info_map_version,
      common::ObTZInfoMap &tz_info_map,
      common::ObTimeZoneInfoWrap &tz_info_wrap) = 0;

  /// Refresh timezone info until successful (try refreshing several times)
  virtual int fetch_tenant_timezone_info_util_succ(
      const uint64_t tenant_id,
      common::ObTZInfoMap *tz_info_map) = 0;
};


///////////////////////////////////// ObLogTimeZoneInfoGetter /////////////////////////////////

class IObLogErrHandler;
class IObLogSysTableHelper;
class IObLogTenantMgr;

class ObLogTimeZoneInfoGetter : public IObLogTimeZoneInfoGetter
{
  static const int64_t SLEEP_TIME_ON_SCHEMA_FAIL = 500 * 1000;
  static const int64_t QUERY_TIMEZONE_INFO_VERSION_INTERVAL = 100 * 1000 * 1000;

public:
  ObLogTimeZoneInfoGetter();
  virtual ~ObLogTimeZoneInfoGetter();

public:
  int init(
      const char *timezone_str,
      common::ObMySQLProxy &mysql_proxy,
      IObLogSysTableHelper &systable_helper,
      IObLogTenantMgr &tenant_mgr,
      IObLogErrHandler &err_handler);
  void destroy();

public:
  virtual int start();
  virtual void stop();
  virtual void mark_stop_flag() { ATOMIC_STORE(&stop_flag_, true); }

  // Init by ObLogTenant initialisation
  virtual int init_tz_info_wrap(
      const uint64_t tenant_id,
      int64_t &tz_info_map_version,
      common::ObTZInfoMap &tz_info_map,
      common::ObTimeZoneInfoWrap &tz_info_wrap);

  virtual int fetch_tenant_timezone_info_util_succ(
      const uint64_t tenant_id,
      common::ObTZInfoMap *tz_info_map);

  // for init interface OTTZ_MGR.tenant_tz_map_getter_
  static int get_tenant_timezone_map(
      const uint64_t tenant_id,
      common::ObTZMapWrap &tz_map_wrap);

private:
  static void *tz_thread_func_(void *args);
  void tz_routine();

  // 1. local maintenance of timezone info version
  // 2. Periodically query all_zone table - time_zone_info_version:
  // update timezone info when changes occur
  // otherwise not updated (updating timezone info involves multiple table joins)
  int query_timezone_info_version_and_update_();

  OB_INLINE bool need_fetch_timezone_info_by_tennat_() const { return true; }

  int refresh_tenant_timezone_info_(const uint64_t tenant_id);
  // 1. Check the version first, if the version has not changed, then do not refresh
  // 2. Refresh only if the version has changed
  //
  // @retval OB_SUCCESS         Success
  // @retval OB_ENTRY_NOT_EXIST tenant not exist
  // @retval other_error_code   Fail
  int refresh_tenant_timezone_info_based_on_version_(const uint64_t tenant_id);

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
  int query_timezone_info_version_(
      const uint64_t tenant_id,
      int64_t &timezone_info_version);

  // refresh timezone info
  int refresh_timezone_info_();
  // fetch timezone info by SQL
  int fetch_tenant_timezone_info_(const uint64_t tenant_id, common::ObTZInfoMap *tz_info_map);
  int refresh_all_tenant_timezone_info_();
  // export timezone_info_map and demp to local file.
  int export_timezone_info_(common::ObTZInfoMap &tz_info_map);
  // import timezone_info from local file and convert to ObTZInfoMap
  int import_timezone_info_(common::ObTZInfoMap &tz_info_map);
  int load_tzinfo_from_file_(char *buf, const int64_t buf_len);
  // currently refresh tz_info while using online_refresh_mode(data_dict won't refresh tz_info even
  // if in intergate_mode)
  bool is_online_tz_info_available() const { return is_online_refresh_mode(TCTX.refresh_mode_); };

private:
  bool                  inited_;
  pthread_t             tz_tid_;
  common::ObCond        tz_cond_;
  volatile bool         stop_flag_ CACHE_ALIGNED;

  common::ObMySQLProxy  *mysql_proxy_;

  IObLogSysTableHelper  *systable_helper_;
  IObLogErrHandler      *err_handler_;

  common::ObSpinLock    lock_;
  IObLogTenantMgr       *tenant_mgr_;
  // save for init tz_info_wrap
  const char            *timezone_str_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLogTimeZoneInfoGetter);
};
} // namespace libobcdc
} // namespace oceanbase
#endif
