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

#ifndef OCEANBASE_SHARE_OB_FREEZE_INFO_PROXY_H_
#define OCEANBASE_SHARE_OB_FREEZE_INFO_PROXY_H_

#include "lib/ob_define.h"
#include "share/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/string/ob_sql_string.h"
#include "lib/time/ob_time_utility.h"
#include "lib/queue/ob_fixed_queue.h"
#include "common/storage/ob_freeze_define.h"
#include "share/ob_cluster_version.h"
#include "share/scn.h"
#include "share/ob_tenant_id_schema_version.h"

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
class ObMySQLTransaction;
class ObISQLClient;
class ObAddr;
namespace sqlclient
{
class ObMySQLResult;
}
}
namespace share
{
/*
 * the columns of __all_freeze_info are as follows:
 * | frozen_scn | schema_version | data_version |
 * we make sure the row_id of __all_freeze_info equals to frozen_scn
 */
struct ObFreezeInfo
{
  ObFreezeInfo()
    : frozen_scn_(),
      schema_version_(INVALID_SCHEMA_VERSION), 
      data_version_(0)
  {}
  ObFreezeInfo(const SCN &frozen_scn,
                       const int64_t schema_version,
                       const int64_t data_version)
    : schema_version_(schema_version),
      data_version_(data_version)
  {
    frozen_scn_ = frozen_scn;
  }

  void assign(const ObFreezeInfo &other)
  {
    frozen_scn_ = other.frozen_scn_;
    schema_version_ = other.schema_version_;
    data_version_ = other.data_version_;
  }

  void reset()
  {
    frozen_scn_.reset();
    schema_version_ = INVALID_SCHEMA_VERSION;
    data_version_ = 0;
  }

  void set_initial_value(const int64_t data_version)
  {
    schema_version_ = ORIGIN_SCHEMA_VERSION;
    data_version_ = data_version;
    frozen_scn_ = share::SCN::base_scn();
  }

  bool is_valid() const
  {
    return (frozen_scn_.is_valid())
           && (schema_version_ > INVALID_SCHEMA_VERSION);
  }

  bool operator ==(const ObFreezeInfo &other) const
  {
    return ((this == &other)
            || ((this->frozen_scn_ == other.frozen_scn_)
            && (this->schema_version_ == other.schema_version_)
            && (this->data_version_ == other.data_version_)));
  }

  TO_STRING_KV(N_FROZEN_VERSION, frozen_scn_, K_(schema_version),
               K_(data_version));

  static const int64_t INVALID_SCHEMA_VERSION = 0;
  static const int64_t ORIGIN_SCHEMA_VERSION = 1;

  static const uint64_t ZERO_FROZEN_SCN_VAL = 0;
  static const uint64_t INITIAL_FROZEN_SCN_VAL = 1;
  static const uint64_t INVALID_FROZEN_SCN_VAL = UINT64_MAX;

  SCN frozen_scn_;
  int64_t schema_version_;
  int64_t data_version_;

  OB_UNIS_VERSION(1);
};

class ObFreezeInfoProxy
{
public:
  ObFreezeInfoProxy(int64_t tenant_id) : tenant_id_(tenant_id) {}
  virtual ~ObFreezeInfoProxy() {}

public:
  int get_freeze_info(common::ObISQLClient &sql_proxy,
                      const SCN &frozen_scn,
                      ObFreezeInfo &frozen_status);

  // not include initial_freeze_info
  int get_all_freeze_info(common::ObISQLClient &sql_proxy,
                          common::ObIArray<ObFreezeInfo> &frozen_statuses);

  int get_freeze_info_larger_or_equal_than(
      common::ObISQLClient &sql_proxy,
      const SCN &frozen_scn,
      common::ObIArray<ObFreezeInfo> &frozen_statuses);

  int get_frozen_scn_larger_or_equal_than(
      common::ObISQLClient &sql_proxy,
      const SCN &frozen_scn,
      common::ObIArray<uint64_t> &frozen_scn_vals);

  // get the maximum frozen_scn which is smaller than or equal to the given @compaction_scn
  int get_max_frozen_scn_smaller_or_equal_than(
      common::ObISQLClient &sql_proxy,
      const SCN &compaction_scn,
      SCN &max_frozen_scn);

  int set_freeze_info(common::ObISQLClient &sql_proxy,
                      const ObFreezeInfo &frozen_status);

  // This function will query __all_freeze_info by sql_proxy to get following info:
  // 1. get min frozen_scn, as @min_frozen_scn
  // 2. get all frozen status whose frozen_scn is larger than @frozen_scn
  int get_min_major_available_and_larger_info(common::ObISQLClient &sql_proxy,
                                              const SCN &frozen_scn,
                                              SCN &min_frozen_scn,
                                              common::ObIArray<ObFreezeInfo> &frozen_statuses);

  // batch delete freeze info:
  // frozen_scn <= upper_frozen_scn && frozen_scn > 1
  int batch_delete(common::ObISQLClient &sql_proxy,
                   const SCN &upper_frozen_scn);

  // for frozen_status whose frozen_scn is less than @frozen_scn, get the one with biggest frozen_scn
  int get_frozen_info_less_than(common::ObISQLClient &sql_proxy,
                                const SCN &frozen_scn,
                                ObFreezeInfo &frozen_status);

  // get frozen_status whose frozen_scn <= @frozen_scn
  // If @get_all = true, means 'get all matched'; Else, only get one record with highest frozen_scn.
  int get_frozen_info_less_than(common::ObISQLClient &sql_proxy,
                                const SCN &frozen_scn,
                                common::ObIArray<ObFreezeInfo> &frozen_status_arr,
                                bool get_all = true);

  // get frozen_status of max frozen_scn
  int get_max_freeze_info(common::ObISQLClient &sql_proxy,
                          ObFreezeInfo &frozen_status);

  int get_freeze_schema_info(common::ObISQLClient &sql_proxy,
                            const uint64_t tenant_id,
                            const SCN &frozen_scn,
                            TenantIdAndSchemaVersion &schema_version_info);

private:
  int get_min_major_available_and_larger_info_inner_(common::ObISQLClient &sql_proxy,
                                                     const SCN &frozen_scn,
                                                     SCN &min_frozen_scn,
                                                     common::ObIArray<ObFreezeInfo> &frozen_statuses);
  
  int construct_frozen_status_(common::sqlclient::ObMySQLResult &result,
                               ObFreezeInfo &frozen_status);
private:
  uint64_t tenant_id_;
};

} // end namespace share
} // end namespace oceanbase

#endif // OCEANBASE_SHARE_OB_FREEZE_INFO_PROXY_H_
