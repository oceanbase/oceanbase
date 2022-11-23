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

#ifndef OCEANBASE_RS_OB_TENANT_ID_SCHEMA_VERSION_H_
#define OCEANBASE_RS_OB_TENANT_ID_SCHEMA_VERSION_H_

#include "lib/ob_define.h"
#include "share/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/string/ob_sql_string.h"
#include "lib/time/ob_time_utility.h"
#include "lib/queue/ob_fixed_queue.h"
#include "common/storage/ob_freeze_define.h"
#include "share/ob_cluster_version.h"

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
class ObMySQLTransaction;
class ObISQLClient;
namespace sqlclient
{
class ObMySQLResult;
}
}
namespace share
{
const int64_t OB_INVALID_SCHEMA_VERSION = -1;

struct TenantIdAndSchemaVersion
{
public:
  TenantIdAndSchemaVersion() : tenant_id_(common::OB_INVALID_TENANT_ID), schema_version_(0) {}
  TenantIdAndSchemaVersion(const int64_t tenant_id, const int64_t schema_version) :
      tenant_id_(tenant_id), schema_version_(schema_version) {}
  TO_STRING_KV(K_(tenant_id), K_(schema_version));
  void reset() { tenant_id_ = common::OB_INVALID_TENANT_ID; schema_version_ = 0; }
  bool operator == (const TenantIdAndSchemaVersion &other) const
  {
    return ((this == &other) || (tenant_id_ == other.tenant_id_ && schema_version_ == other.schema_version_));
  }
  bool is_valid() const { return tenant_id_ > 0 && schema_version_ > 0; }
  int assign(const TenantIdAndSchemaVersion &other)
  {
    int ret = common::OB_SUCCESS;
    tenant_id_ = other.tenant_id_;
    schema_version_ = other.schema_version_;
    return ret;
  }
  uint64_t tenant_id_;
  int64_t schema_version_;

  OB_UNIS_VERSION(1);
};

} // end namespace share
} // end namespace oceanbase
#endif
