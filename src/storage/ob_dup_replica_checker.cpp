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

#define USING_LOG_PREFIX STORAGE

#include "ob_dup_replica_checker.h"
#include "common/ob_partition_key.h"
#include "lib/net/ob_addr.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "storage/ob_locality_manager.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace share::schema;
namespace storage {
int ObDupReplicaChecker::init(
    share::schema::ObMultiVersionSchemaService* schema_service, storage::ObLocalityManager* locality_manager)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(nullptr == schema_service || nullptr == locality_manager)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    is_inited_ = true;
    schema_service_ = schema_service;
    locality_manager_ = locality_manager;
  }
  return ret;
}

int ObDupReplicaChecker::get_table_high_primary_zone_array(share::schema::ObSchemaGetterGuard& schema_guard,
    const share::schema::ObTableSchema& table_schema, common::ObIArray<common::ObZone>& primary_zone_array)
{
  int ret = OB_SUCCESS;
  primary_zone_array.reset();
  ObSchema base_schema;
  ObPrimaryZone primary_zone_schema(&base_schema);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == locality_manager_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("locality manager ptr is null", K(ret));
  } else if (OB_FAIL(table_schema.get_primary_zone_inherit(schema_guard, primary_zone_schema))) {
    LOG_WARN("fail to get primary zone inherit", K(ret));
  } else {
    const common::ObIArray<ObZoneScore>& zone_score_array = primary_zone_schema.get_primary_zone_array();
    if (zone_score_array.count() <= 0) {
      // primary zone array is empty, directly return
    } else {
      const ObZoneScore& sample_zone = zone_score_array.at(0);
      for (int64_t i = 0; OB_SUCC(ret) && i < zone_score_array.count(); ++i) {
        const ObZoneScore& this_zone = zone_score_array.at(i);
        if (sample_zone.score_ == this_zone.score_) {
          if (OB_FAIL(primary_zone_array.push_back(this_zone.zone_))) {
            LOG_WARN("fail to push back", K(ret));
          }
        } else {
          break;
        }
      }
    }
  }
  return ret;
}

int ObDupReplicaChecker::get_table_high_primary_region_array(share::schema::ObSchemaGetterGuard& schema_guard,
    const share::schema::ObTableSchema& table_schema, common::ObIArray<common::ObRegion>& primary_region_array)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObZone, 7> primary_zone_array;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(get_table_high_primary_zone_array(schema_guard, table_schema, primary_zone_array))) {
    LOG_WARN("fail to get table hight primary zone array", K(ret));
  } else if (OB_UNLIKELY(nullptr == locality_manager_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("locality manager ptr is null", K(ret));
  } else {
    primary_region_array.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < primary_zone_array.count(); ++i) {
      const common::ObZone& this_zone = primary_zone_array.at(i);
      common::ObRegion this_region;
      if (OB_FAIL(locality_manager_->get_noempty_zone_region(this_zone, this_region))) {
        LOG_WARN("fail to get zone region", K(ret));
      } else if (has_exist_in_array(primary_region_array, this_region)) {
        // bypass, already exist
      } else if (OB_FAIL(primary_region_array.push_back(this_region))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObDupReplicaChecker::get_dup_replica_type(
    const common::ObPartitionKey& pkey, const common::ObAddr& server, DupReplicaType& dup_replica_type)
{
  return get_dup_replica_type(pkey.get_table_id(), server, dup_replica_type);
}

int ObDupReplicaChecker::get_dup_replica_type(
    const uint64_t table_id, const common::ObAddr& server, DupReplicaType& dup_replica_type)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTableSchema* table_schema = nullptr;
  const uint64_t fetch_tenant_id = is_inner_table(table_id) ? OB_SYS_TENANT_ID : extract_tenant_id(table_id);
  const bool check_formal = extract_pure_id(table_id) > OB_MAX_CORE_TABLE_ID;  // avoid circular dependency
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!is_valid_id(table_id) || !server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_id), K(server));
  } else if (OB_UNLIKELY(nullptr == schema_service_ || nullptr == locality_manager_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service ptr or locality manager is null", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_full_schema_guard(fetch_tenant_id, schema_guard, check_formal))) {
    if (OB_TENANT_SCHEMA_NOT_FULL != ret) {
      LOG_WARN("fail to get schema guard", K(ret));
    }
  } else if (OB_FAIL(schema_guard.get_table_schema(table_id, table_schema))) {
    LOG_WARN("fail to get table schema", K(ret));
  } else if (nullptr == table_schema) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_DEBUG("table not exist", K(ret), K(table_id));
  } else if (ObDuplicateScope::DUPLICATE_SCOPE_NONE == table_schema->get_duplicate_scope()) {
    // table is NOT duplicated table
    dup_replica_type = DupReplicaType::NON_DUP_REPLICA;
  } else if (ObDuplicateScope::DUPLICATE_SCOPE_CLUSTER == table_schema->get_duplicate_scope()) {
    // table is cluster duplicated table
    dup_replica_type = DupReplicaType::DUP_REPLICA;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table replicate scope",
        K(ret),
        "table_id",
        table_schema->get_table_id(),
        "duplicate_scope",
        table_schema->get_duplicate_scope());
  }
  return ret;
}
}  // namespace storage
}  // namespace oceanbase
