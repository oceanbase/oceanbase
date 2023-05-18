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

#ifndef OCEANBASE_SHARE_OB_SCHEMA_STATUS_PROXY_H_
#define OCEANBASE_SHARE_OB_SCHEMA_STATUS_PROXY_H_

#include "share/ob_define.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/container/ob_iarray.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
class ObISQLClient;
class ObAddr;
namespace sqlclient
{
class ObMySQLResult;
}
}
namespace share
{

class ObSchemaStatusUpdater
{
public:
  ObSchemaStatusUpdater(share::schema::ObRefreshSchemaStatus schema_status)
    : schema_status_(schema_status) {}
  virtual ~ObSchemaStatusUpdater() {}

  int operator() (common::hash::HashMapPair<uint64_t, share::schema::ObRefreshSchemaStatus> &entry);
private:
  share::schema::ObRefreshSchemaStatus schema_status_;
  DISALLOW_COPY_AND_ASSIGN(ObSchemaStatusUpdater);
};

// dodge the bug :
// all operation of __all_core_table must be single partition transaction
class ObSchemaStatusProxy
{
public:
  static const char *OB_ALL_SCHEMA_STATUS_TNAME;
  static const char *TENANT_ID_CNAME;
  static const char *SNAPSHOT_TIMESTAMP_CNAME;
  static const char *READABLE_SCHEMA_VERSION_CNAME;
  static const char *CREATED_SCHEMA_VERSION_CNAME;
  static const int64_t TENANT_SCHEMA_STATUS_BUCKET_NUM = 100;
public:
  ObSchemaStatusProxy(common::ObISQLClient &sql_proxy)
    : sql_proxy_(sql_proxy),
      schema_status_cache_(),
      is_inited_(false) {}
  virtual ~ObSchemaStatusProxy() {}

  int init();

  int nonblock_get(const uint64_t refresh_tenant_id,
      share::schema::ObRefreshSchemaStatus &refresh_schema_status);

  int get_refresh_schema_status(
      const uint64_t tenant_id,
      share::schema::ObRefreshSchemaStatus &refresh_schema_status);

  int get_refresh_schema_status(
      common::ObIArray<share::schema::ObRefreshSchemaStatus> &refresh_schema_status_array);

  int load_refresh_schema_status();

  int del_tenant_schema_status(const uint64_t tenant_id);

  int set_tenant_schema_status(
    const share::schema::ObRefreshSchemaStatus &refresh_schema_status);

  int update_schema_status(const share::schema::ObRefreshSchemaStatus &cur_schema_status);
  int load_refresh_schema_status(
    const uint64_t refresh_tenant_id,
    schema::ObRefreshSchemaStatus &refresh_schema_status);

private:
  int check_inner_stat();
private:
  common::ObISQLClient &sql_proxy_;
  common::hash::ObHashMap<uint64_t, share::schema::ObRefreshSchemaStatus, common::hash::ReadWriteDefendMode> schema_status_cache_;
  bool is_inited_;
};

} // end namespace share
} // end namespace oceanbase
#endif
