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

#define USING_LOG_PREFIX SHARE_SCHEMA

#include "ob_table_iter.h"

#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_print_utils.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_mgr.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
using namespace oceanbase::common;

ObTenantTableIterator::ObTenantTableIterator()
  : is_inited_(false),
    cur_table_idx_(0),
    table_ids_()
{
}

int ObTenantTableIterator::init(ObMultiVersionSchemaService *schema_service,
                                const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  // allow init twice
  if (OB_ISNULL(schema_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema_service is null", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(tenant_id), K(ret));
  } else {
    table_ids_.reuse();
    cur_table_idx_ = 0;
    if (OB_FAIL(get_table_ids(schema_service, tenant_id))) {
      LOG_WARN("get_table_ids failed", K(tenant_id), K(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTenantTableIterator::next(uint64_t &table_id)
{
  int ret = OB_SUCCESS;
  if (cur_table_idx_ < table_ids_.count()) {
    table_id = table_ids_.at(cur_table_idx_);
    ++cur_table_idx_;
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObTenantTableIterator::get_table_ids(ObMultiVersionSchemaService *schema_service,
                                         const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_service) || OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(schema_service), K(tenant_id));
  } else {
    ObSchemaGetterGuard guard;
    if (OB_FAIL(schema_service->get_tenant_schema_guard(tenant_id, guard))) {
      LOG_WARN("get_schema_guard failed", K(ret));
    } else if (OB_FAIL(guard.get_table_ids_in_tenant(tenant_id, table_ids_))) {
      LOG_WARN("get_table_ids_in_tenant failed", K(tenant_id), K(ret));
    }
  }
  return ret;
}

ObTenantIterator::ObTenantIterator()
  : is_inited_(false),
    cur_tenant_idx_(0),
    tenant_ids_()
{
}

int ObTenantIterator::init(ObMultiVersionSchemaService &schema_service)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(get_tenant_ids(schema_service))) {
      LOG_WARN("get_tenant_ids failed", K(ret));
  } else {
    cur_tenant_idx_ = 0;
    is_inited_ = true;
  }
  return ret;
}

int ObTenantIterator::init(ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids_))) {
    LOG_WARN("get_tenant_ids failed", K(ret));
  } else {
    cur_tenant_idx_ = 0;
    is_inited_ = true;
  }
  return ret;
}

int ObTenantIterator::next(uint64_t &tenant_id)
{
  int ret = OB_SUCCESS;
  if (cur_tenant_idx_ < tenant_ids_.count()) {
    tenant_id = tenant_ids_.at(cur_tenant_idx_);
    ++cur_tenant_idx_;
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObTenantIterator::get_tenant_ids(ObMultiVersionSchemaService &schema_service)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard guard;
  if (OB_FAIL(schema_service.get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
    LOG_WARN("get_schema_guard failed", K(ret));
  } else if (OB_FAIL(guard.get_tenant_ids(tenant_ids_))) {
    LOG_WARN("get_tenant_ids failed", K(ret));
  }
  return ret;
}

int ObTenantPartitionEntityIterator::get_partition_entity_id_array(
    ObSchemaGetterGuard &schema_guard,
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    cur_idx_ = 0;
    entity_id_array_.reset();
    common::ObArray<const ObSimpleTableSchemaV2 *> table_schemas;
    if (OB_FAIL(schema_guard.get_table_schemas_in_tenant(
            tenant_id, table_schemas))) {
      LOG_WARN("fail to get table schemas in tenant", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas.count(); ++i) {
        const ObSimpleTableSchemaV2 *table_schema = table_schemas.at(i);
        if (OB_UNLIKELY(nullptr == table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table schema ptr is null", K(ret));
        } else if (table_schema->has_partition()) {
          if (OB_FAIL(entity_id_array_.push_back(table_schema->get_table_id()))) {
            LOG_WARN("fail to push back", K(ret));
          }
        } else {} // There is no partition, no push is required
      }
    }
  }
  return ret;
}

int ObTenantPartitionEntityIterator::init(
    ObSchemaGetterGuard &schema_guard,
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    entity_id_array_.reset();
    cur_idx_ = 0;
    if (OB_FAIL(get_partition_entity_id_array(schema_guard, tenant_id))) {
      LOG_WARN("fail to get partition entity id array", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTenantPartitionEntityIterator::next(
    uint64_t &partition_entity_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (cur_idx_ >= entity_id_array_.count()) {
    ret = OB_ITER_END;
  } else {
    partition_entity_id = entity_id_array_.at(cur_idx_++);
  }
  return ret;
}

}//end of namespace schema
}//end of namespace share
}//end of namespace oceanbase
