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

#ifndef OCEANBASE_RS_OB_FREEZE_INFO_PROXY_H_
#define OCEANBASE_RS_OB_FREEZE_INFO_PROXY_H_

#include "lib/ob_define.h"
#include "share/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/string/ob_sql_string.h"
#include "lib/time/ob_time_utility.h"
#include "lib/queue/ob_fixed_queue.h"
#include "common/storage/ob_freeze_define.h"
#include "share/ob_cluster_version.h"

namespace oceanbase {
namespace common {
class ObMySQLProxy;
class ObMySQLTransaction;
class ObISQLClient;
class ObAddr;
namespace sqlclient {
class ObMySQLResult;
}
}  // namespace common
namespace share {
const int64_t OB_INVALID_SCHEMA_VERSION = -1;
enum ObSnapShotType {
  SNAPSHOT_FOR_MAJOR = 0,
  SNAPSHOT_FOR_CREATE_INDEX = 1,
  SNAPSHOT_FOR_MULTI_VERSION = 2,
  SNAPSHOT_FOR_RESTORE_POINT = 3,
  SNAPSHOT_FOR_BACKUP_POINT = 4,
  MAX_SNAPSHOT_TYPE,
};

/* __all_freeze_info is a table logically saved in __all_core_table,
 * it has four columns which are frozen_version, frozen_timestamp, freeze_status, schema_version,
 * freeze_status has three values: FREEZE_PREPARE, FREEZE_COMMIT and FREEZE_ABORT
 * its logical structure is as follows:
 * |  frozen_version  |  frozen_timestamp  |  frozen_status  |  schema_version  |
 * we stipulate row_id of __all_freeze_info equals to (frozen_version - 1)
 */

struct ObSimpleFrozenStatus {
  ObSimpleFrozenStatus()
      : frozen_version_(0), frozen_timestamp_(storage::ObFrozenStatus::INVALID_TIMESTAMP), cluster_version_(0)
  {}
  ObSimpleFrozenStatus(const int64_t& frozen_version, const int64_t frozen_timestamp, const int64_t cluster_version)
      : frozen_version_(frozen_version), frozen_timestamp_(frozen_timestamp), cluster_version_(cluster_version)
  {}

  void reset()
  {
    frozen_version_ = 0;
    frozen_timestamp_ = storage::ObFrozenStatus::INVALID_TIMESTAMP;
    cluster_version_ = 0;
  }
  bool is_valid() const
  {
    return frozen_version_ > 0 && frozen_timestamp_ > common::OB_INVALID_TIMESTAMP;
  }
  ObSimpleFrozenStatus& operator=(const storage::ObFrozenStatus& other)
  {
    this->frozen_version_ = other.frozen_version_;
    this->frozen_timestamp_ = other.frozen_timestamp_;
    this->cluster_version_ = other.cluster_version_;
    return *this;
  }
  bool operator==(const ObSimpleFrozenStatus& other) const
  {
    return ((this == &other) ||
            (this->frozen_version_ == other.frozen_version_ && this->frozen_timestamp_ == other.frozen_timestamp_ &&
                this->cluster_version_ == other.cluster_version_));
  }
  TO_STRING_KV(N_FROZEN_VERSION, frozen_version_, K_(frozen_timestamp), K_(cluster_version));

  int64_t frozen_version_;
  int64_t frozen_timestamp_;
  int64_t cluster_version_;

  OB_UNIS_VERSION(1);
};

struct TenantIdAndSchemaVersion {
  OB_UNIS_VERSION(1);

public:
  TenantIdAndSchemaVersion() : tenant_id_(common::OB_INVALID_TENANT_ID), schema_version_(0)
  {}
  TenantIdAndSchemaVersion(const int64_t tenant_id, const int64_t schema_version)
      : tenant_id_(tenant_id), schema_version_(schema_version)
  {}
  TO_STRING_KV(K_(tenant_id), K_(schema_version));
  void reset()
  {
    tenant_id_ = common::OB_INVALID_TENANT_ID;
    schema_version_ = 0;
  }
  bool operator==(const TenantIdAndSchemaVersion& other) const
  {
    return ((this == &other) || (tenant_id_ == other.tenant_id_ && schema_version_ == other.schema_version_));
  }
  bool is_valid() const
  {
    return tenant_id_ > 0 && schema_version_ > 0;
  }
  int assign(const TenantIdAndSchemaVersion& other)
  {
    int ret = common::OB_SUCCESS;
    tenant_id_ = other.tenant_id_;
    schema_version_ = other.schema_version_;
    return ret;
  }
  uint64_t tenant_id_;
  int64_t schema_version_;
};

class ObFreezeInfoProxy {
public:
  ObFreezeInfoProxy()
  {}
  virtual ~ObFreezeInfoProxy()
  {}
  static const char* OB_ALL_FREEZE_INFO_TNAME;

public:
  // freeze status
  static const int64_t INVALID_TIMESTAMP = storage::ObFrozenStatus::INVALID_TIMESTAMP;
  static const int64_t INVALID_FROZEN_VERSION = storage::ObFrozenStatus::INVALID_FROZEN_VERSION;
  static const int64_t INVALID_SCHEMA_VERSION = storage::ObFrozenStatus::INVALID_SCHEMA_VERSION;

public:
  // major_version = 0: Get latest freeze info
  int get_freeze_info(
      common::ObISQLClient& sql_proxy, const int64_t major_version, ObSimpleFrozenStatus& frozen_status);

  int get_freeze_info_v2(common::ObISQLClient& sql_proxy, const int64_t input_frozen_version,
      storage::ObFrozenStatus& frozen_status, common::ObIArray<TenantIdAndSchemaVersion>& frozen_schema_versions);
  int set_freeze_info(common::ObMySQLTransaction& trans, const storage::ObFrozenStatus& frozen_status);
  int get_freeze_info_v2(common::ObISQLClient& sql_proxy, const int64_t tenant_id, const int64_t major_version,
      storage::ObFrozenStatus& frozen_status);
  int get_freeze_info_inner(
      common::ObISQLClient& sql_proxy, const int64_t major_version, storage::ObFrozenStatus& frozen_status);

  int get_freeze_info_larger_than_v2(common::ObISQLClient& sql_proxy, const int64_t tenant_id,
      const int64_t major_version, common::ObIArray<storage::ObFrozenStatus>& frozen_statuses);
  int get_freeze_info_larger_than_v2(common::ObISQLClient& sql_proxy, const int64_t major_version,
      common::ObIArray<ObSimpleFrozenStatus>& frozen_statuses);

  // This function will query __all_core_table's __all_freeze_info by sql_proxy to get following info:
  // 1. get min major version
  // 2. get all frozen status larger than major version
  int get_min_major_available_and_larger_info(common::ObISQLClient& sql_proxy, const int64_t major_version,
      int64_t& min_major_version, common::ObIArray<ObSimpleFrozenStatus>& frozen_statuses);

  int get_min_major_version_available(common::ObISQLClient& sql_proxy, int64_t& major_version);

  int update_frozen_schema(common::ObMySQLProxy& client, const storage::ObFrozenStatus& frozen_status);
  // Get tenant freeze schema info
  // If tenant_id = 0, means get all tenant freeze schema info
  // major_version should not be 0.
  int get_freeze_schema_v2(common::ObISQLClient& sql_proxy, const uint64_t tenant_id, const int64_t major_version,
      common::ObIArray<TenantIdAndSchemaVersion>& schema_version_infos);

  int update_frozen_schema_v2(common::ObMySQLProxy& client, const int64_t frozen_version,
      const common::ObIArray<TenantIdAndSchemaVersion>& id_versions);
  int get_frozen_status_without_schema_version_v2(
      common::ObISQLClient& client, common::ObIArray<storage::ObFrozenStatus>& frozen_status);
  int get_max_frozen_version_with_schema(common::ObISQLClient& sql_proxy, int64_t& frozen_version);
  // batch delete freeze info record: frozen_timestamp < min_frozen_timestamp && frover_version < min_frozen_version
  int batch_delete(
      common::ObMySQLProxy& sql_proxy, const int64_t min_frozen_timestamp, const int64_t min_frozen_version);
  int batch_delete_frozen_info_larger_than(common::ObISQLClient& proxy, const int64_t max_frozen_version);
  int batch_delete_frozen_schema_larger_than(common::ObISQLClient& proxy, const int64_t max_fronze_version);
  int get_max_frozen_status_with_schema_version_v2(
      common::ObISQLClient& sql_proxy, ObSimpleFrozenStatus& frozen_status);

  int get_max_frozen_status_with_integrated_schema_version_v2(
      common::ObISQLClient& sql_proxy, ObSimpleFrozenStatus& frozen_status);

  int get_freeze_info_larger_than_mock(common::ObISQLClient& sql_proxy, const int64_t major_version,
      common::ObIArray<storage::ObFrozenStatus>& frozen_statuses);

  int get_schema_info_larger_than(common::ObISQLClient& sql_proxy, const int64_t snapshot_ts,
      common::ObIArray<TenantIdAndSchemaVersion>& schema_versions);

  int get_schema_info_less_than(common::ObISQLClient& sql_proxy, const int64_t snapshot_ts,
      common::ObIArray<TenantIdAndSchemaVersion>& schema_versions);
  int get_frozen_info_less_than(
      common::ObISQLClient& sql_proxy, const int64_t snapshot_ts, ObSimpleFrozenStatus& frozen_status);
  int fix_tenant_schema(common::ObISQLClient& sql_proxy, const int64_t frozen_version, const int64_t tenant_id,
      const int64_t schema_version);
  int get_frozen_status_less_than(common::ObISQLClient& sql_proxy, const int64_t tenant_id,
      const int64_t schema_version, ObSimpleFrozenStatus& frozen_status);

private:
  int get_freeze_info_larger_than_inner(common::ObISQLClient& sql_proxy, const int64_t major_version,
      common::ObIArray<storage::ObFrozenStatus>& frozen_statuses);
  int get_min_major_available_and_larger_info_inner(common::ObISQLClient& sql_proxy, const int64_t major_version,
      int64_t& min_major_version, common::ObIArray<storage::ObFrozenStatus>& frozen_statuses);

  int get_max_frozen_status_with_schema_version_inner(
      common::ObISQLClient& sql_proxy, storage::ObFrozenStatus& frozen_status);
  int get_frozen_status_without_schema_version_inner(
      common::ObISQLClient& client, common::ObIArray<storage::ObFrozenStatus>& frozen_status);
  int get_max_frozen_status_with_schema_version_v2_inner(
      common::ObISQLClient& sql_proxy, common::ObSqlString& sql, ObSimpleFrozenStatus& frozen_status);

private:
  inline bool is_valid_frozen_version_(int64_t frozen_version)
  {
    return INVALID_FROZEN_VERSION != frozen_version && frozen_version > 0;
  }
  inline bool is_valid_freeze_status_(common::ObFreezeStatus& freeze_status)
  {
    return (common::INIT_STATUS == freeze_status || common::PREPARED_SUCCEED == freeze_status ||
            common::COMMIT_SUCCEED == freeze_status);
  }
  inline bool is_valid_frozen_timestamp_(int64_t frozen_timestamp)
  {
    return (INVALID_TIMESTAMP != frozen_timestamp && frozen_timestamp >= 0);
  }
  inline bool is_valid_schema_version_(int64_t schema_version)
  {
    return (INVALID_SCHEMA_VERSION != schema_version && schema_version > 0);
  }
  inline int64_t generate_new_frozen_timestamp_(int64_t old_ts)
  {
    int64_t cur_ts = common::ObTimeUtility::current_time();
    return ((cur_ts > old_ts) ? cur_ts : (old_ts + 1));
  }

private:
  // table name and column name
  static const char* FROZEN_VERSION_CNAME;
  static const char* FROZEN_TIMESTAMP_CNAME;
  static const char* FREEZE_STATUS_CNAME;
  static const char* SCHEMA_VERSION_CNAME;
  static const char* CLUSTER_VERSION_CNAME;
  // misc
  static const int64_t MAX_COLUMN_VALUE_STR_LEN = 64;
  static const int64_t MAX_RETRY_CNT = 5;
};

}  // end namespace share
}  // end namespace oceanbase
#endif
