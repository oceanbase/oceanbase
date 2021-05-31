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

#define USING_LOG_PREFIX RS_LB

#include "ob_balancer_interface.h"
#include "lib/string/ob_string.h"
#include "lib/allocator/ob_allocator.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/ob_primary_zone_util.h"
#include "ob_balance_info.h"

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::rootserver::balancer;

bool BalancerStringUtil::has_prefix(const ObString& str, const ObString& prefix)
{
  bool ret = false;
  if (str.length() >= prefix.length()) {
    ret = (0 == STRNCMP(str.ptr(), prefix.ptr(), prefix.length()));
  }
  return ret;
}

int BalancerStringUtil::substract_extra_suffix(const ObString& str, ObString& prefix, ObString& suffix)
{
  int ret = OB_SUCCESS;
  if (str.length() < 3) {
    prefix = str;
    suffix = ObString();
  } else {
    int64_t idx = str.length() - 1;
    // Ends with _t
    if ('_' == str[idx - 1] && 't' == str[idx]) {
      prefix = ObString(str.length() - 2, str.ptr());
      suffix = ObString(2, str.ptr() + str.length() - 2);
    } else {
      // If it does not end with _t,
      // ignore this pattern and leave it to continue to check whether it ends with "_number"
      prefix = str;
      suffix = ObString();
    }
  }
  return ret;
}

int BalancerStringUtil::substract_numeric_suffix(const ObString& str, ObString& prefix, int64_t& digit_len)
{
  int ret = OB_SUCCESS;
  int digit = 0;
  int underscore = 0;
  int64_t prefix_len = 0;
  int64_t idx = str.length() - 1;
  while (idx > 0) {
    if (0 != isdigit(str[idx])) {
      digit++;
      idx--;
    } else {
      if ('_' == str[idx]) {
        underscore++;
        prefix_len = idx;
      }
      break;
    }
  }
  if (digit > 0 && underscore > 0 && idx > 0) {
    digit_len = digit;
    prefix = ObString(prefix_len, str.ptr());
  } else {
    digit_len = 0;
    prefix = str;
  }
  return ret;
}

int StatFinderUtil::get_partition_entity_schemas_by_tg_idx(ITenantStatFinder& stat_finder,
    ObSchemaGetterGuard& schema_guard, uint64_t tenant_id, int64_t tablegroup_idx,
    ObIArray<const ObPartitionSchema*>& partition_entity_schemas)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tids;
  if (OB_FAIL(stat_finder.get_partition_entity_ids_by_tg_idx(tablegroup_idx, tids))) {
    LOG_WARN("fail get table ids in tg by tablegroup_idx", K(tablegroup_idx), K(tenant_id), K(ret));
  } else {
    FOREACH_X(tid, tids, OB_SUCC(ret))
    {
      if (!is_new_tablegroup_id(*tid)) {
        const ObTableSchema* schema = nullptr;
        if (OB_FAIL(schema_guard.get_table_schema(*tid, schema))) {
          LOG_WARN("fail get table schema", K(tenant_id), K(*tid), K(tids), K(ret));
        } else if (OB_ISNULL(schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null ptr", K(schema), K(ret));
        } else if (OB_FAIL(partition_entity_schemas.push_back(schema))) {
          LOG_WARN("fail push back schema", K(tablegroup_idx), K(tenant_id), K(ret));
        }
      } else {
        const ObTablegroupSchema* schema = nullptr;
        if (OB_FAIL(schema_guard.get_tablegroup_schema(*tid, schema))) {
          LOG_WARN("fail to get tablegroup schema", K(ret), K(*tid));
        } else if (OB_ISNULL(schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null ptr", K(schema), K(ret));
        } else if (OB_FAIL(partition_entity_schemas.push_back(schema))) {
          LOG_WARN("fail push back schema", K(tablegroup_idx), K(tenant_id), K(ret));
        }
      }
    }
  }
  return ret;
}

int StatFinderUtil::get_need_balance_table_schemas_in_tenant(share::schema::ObSchemaGetterGuard& schema_guard,
    uint64_t tenant_id, common::ObIArray<const share::schema::ObTableSchema*>& tables)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tids;
  if (OB_FAIL(schema_guard.get_table_ids_in_tenant(tenant_id, tids))) {
    LOG_WARN("fail get table ids in tenant", K(tenant_id), K(ret));
  } else {
    FOREACH_X(tid, tids, OB_SUCC(ret))
    {
      const ObTableSchema* schema = NULL;
      if (OB_FAIL(schema_guard.get_table_schema(*tid, schema))) {
        LOG_WARN("fail get table schema", K(tenant_id), K(*tid), K(ret));
      } else if (OB_ISNULL(schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null ptr", K(schema), K(*tid), K(ret));
      } else if (OB_RECYCLEBIN_SCHEMA_ID == extract_pure_id(schema->get_database_id())) {
        // The table in the recycle bin does not participate in load balancing
      } else if (!schema->has_self_partition()) {
        // Table schema without partition
      } else if (OB_FAIL(tables.push_back(schema))) {
        LOG_WARN("fail push back schema", K(tenant_id), K(*tid), K(ret));
      }
    }
  }
  return ret;
}

int TenantSchemaGetter::get_all_pg_idx(
    const common::ObPartitionKey& pkey, const int64_t all_tg_idx, int64_t& all_pg_idx)
{
  UNUSED(pkey);
  UNUSED(all_tg_idx);
  all_pg_idx = 0;  // dummy
  return OB_SUCCESS;
}

int TenantSchemaGetter::get_all_tg_idx(uint64_t tablegroup_id, uint64_t table_id, int64_t& all_tg_idx)
{
  UNUSED(tablegroup_id);
  UNUSED(table_id);
  all_tg_idx = 0;  // dummy
  return OB_SUCCESS;
}

int TenantSchemaGetter::get_primary_partition_unit(
    const common::ObZone& zone, const int64_t all_tg_idx, const int64_t part_idx, uint64_t& unit_id)
{
  UNUSED(zone);
  UNUSED(all_tg_idx);
  UNUSED(part_idx);
  UNUSED(unit_id);
  return OB_NOT_IMPLEMENT;
}

int TenantSchemaGetter::get_partition_entity_ids_by_tg_idx(
    const int64_t tablegroup_idx, common::ObIArray<uint64_t>& tids)
{
  UNUSED(tablegroup_idx);
  UNUSED(tids);
  return OB_NOT_IMPLEMENT;
}

int TenantSchemaGetter::get_partition_group_data_size(
    const common::ObZone& zone, const int64_t all_tg_idx, const int64_t part_idx, int64_t& data_size)
{
  UNUSED(zone);
  UNUSED(all_tg_idx);
  UNUSED(part_idx);
  UNUSED(data_size);
  return OB_NOT_IMPLEMENT;
}

int TenantSchemaGetter::get_gts_switch(bool& on)
{
  UNUSED(on);
  return OB_NOT_IMPLEMENT;
}

int TenantSchemaGetter::get_primary_partition_key(const int64_t all_pg_idx, common::ObPartitionKey& pkey)
{
  UNUSED(all_pg_idx);
  UNUSED(pkey);
  return OB_NOT_IMPLEMENT;
}
