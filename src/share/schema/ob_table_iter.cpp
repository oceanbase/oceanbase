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

namespace oceanbase {
namespace share {
namespace schema {
using namespace oceanbase::common;

// only used in calculate_startup_progress for get all table_id
// after refreshed schema since restart of rootserver
// so, this usage might be replaced with fetch all table ids, then
// iterator all these table_ids.
//
ObTableIterator::ObTableIterator()
    : cur_tenant_table_id_(OB_MIN_ID, OB_MIN_ID), cur_table_idx_(0), schema_guard_(NULL), tenant_iter_()
{}

int ObTableIterator::init(ObSchemaGetterGuard* schema_guard)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  if (NULL != schema_guard_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(schema_guard_), K(ret));
  } else if (OB_ISNULL(schema_guard)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(schema_guard), K(ret));
  } else if (schema_guard->is_tenant_schema_guard()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("fetch all table ids with tenant schema guard not allowed", K(ret));
  } else if (OB_FAIL(tenant_iter_.init(*schema_guard))) {
    LOG_WARN("fail to init tenant iterator", K(ret));
  } else if (OB_FAIL(tenant_iter_.next(tenant_id))) {
    LOG_WARN("fail to iter next tenant", K(ret));
  } else if (OB_SYS_TENANT_ID != tenant_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else {
    schema_guard_ = schema_guard;
    cur_tenant_table_id_.tenant_id_ = tenant_id;
    cur_tenant_table_id_.table_id_ = OB_MIN_ID;
  }
  return ret;
}

int ObTableIterator::next(uint64_t& table_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_guard_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("table iterator not init", K_(schema_guard), K(ret));
  } else if (cur_table_idx_ >= cache_table_array_.count()) {
    if (OB_FAIL(next_batch_tables())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next batch", K(ret));
      }
    } else {
      cur_table_idx_ = 0;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (cur_table_idx_ >= cache_table_array_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_INFO("get no tables when not iter end", K(ret));
  } else {
    cur_tenant_table_id_ = cache_table_array_.at(cur_table_idx_);
    table_id = cur_tenant_table_id_.table_id_;
    ++cur_table_idx_;
  }
  return ret;
}

int ObTableIterator::next_batch_tables()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_guard_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K_(schema_guard));
  } else {
    cache_table_array_.reset();
    if (OB_FAIL(schema_guard_->batch_get_next_table(
            cur_tenant_table_id_, CACHE_TABLE_ARRAY_CAPACITY, cache_table_array_))) {
      if (OB_ITER_END == ret) {
        uint64_t tenant_id = OB_INVALID_TENANT_ID;
        if (OB_FAIL(tenant_iter_.next(tenant_id))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to iter next tenant", K(ret));
          }
        } else if (OB_INVALID_TENANT_ID == tenant_id) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
        } else {
          cur_tenant_table_id_.tenant_id_ = tenant_id;
          cur_tenant_table_id_.table_id_ = OB_MIN_ID;
          ret = next_batch_tables();
        }
      } else {
        LOG_WARN("fail to get next batch", K(ret), K(cur_tenant_table_id_));
      }
    }
  }
  return ret;
}

int64_t ObTableIterator::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(K_(cur_tenant_table_id), K_(cur_table_idx), "cache_table_array size", cache_table_array_.count());
  return pos;
}

ObTenantTableIterator::ObTenantTableIterator() : is_inited_(false), cur_table_idx_(0), table_ids_()
{}

int ObTenantTableIterator::init(ObMultiVersionSchemaService* schema_service, const uint64_t tenant_id)
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

int ObTenantTableIterator::next(uint64_t& table_id)
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

int ObTenantTableIterator::get_table_ids(ObMultiVersionSchemaService* schema_service, const uint64_t tenant_id)
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

ObTenantIterator::ObTenantIterator() : is_inited_(false), cur_tenant_idx_(0), tenant_ids_()
{}

int ObTenantIterator::init(ObMultiVersionSchemaService& schema_service)
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

int ObTenantIterator::init(ObSchemaGetterGuard& schema_guard)
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

int ObTenantIterator::next(uint64_t& tenant_id)
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

int ObTenantIterator::get_tenant_ids(ObMultiVersionSchemaService& schema_service)
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
    ObSchemaGetterGuard& schema_guard, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    cur_idx_ = 0;
    entity_id_array_.reset();
    common::ObArray<const ObSimpleTableSchemaV2*> table_schemas;
    common::ObArray<const ObSimpleTablegroupSchema*> tablegroup_schemas;
    if (OB_FAIL(schema_guard.get_table_schemas_in_tenant(tenant_id, table_schemas))) {
      LOG_WARN("fail to get table schemas in tenant", K(ret));
    } else if (OB_FAIL(schema_guard.get_tablegroup_schemas_in_tenant(tenant_id, tablegroup_schemas))) {
      LOG_WARN("fail to get tablegroup schemas in tenant", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas.count(); ++i) {
        const ObSimpleTableSchemaV2* table_schema = table_schemas.at(i);
        if (OB_UNLIKELY(nullptr == table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table schema ptr is null", K(ret));
        } else if (table_schema->has_partition() && !table_schema->get_binding()) {
          // has partition, and is table of nonbinding
          if (OB_FAIL(entity_id_array_.push_back(table_schema->get_table_id()))) {
            LOG_WARN("fail to push back", K(ret));
          }
        } else {
        }  // There is no partition, or the partition binding is on the tablegroup, no push is required
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < tablegroup_schemas.count(); ++i) {
        const ObSimpleTablegroupSchema* tg_schema = tablegroup_schemas.at(i);
        if (OB_UNLIKELY(nullptr == tg_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablegroup schema ptr is null", K(ret));
        } else if (tg_schema->get_binding()) {
          if (OB_FAIL(entity_id_array_.push_back(tg_schema->get_tablegroup_id()))) {
            LOG_WARN("fail to push back", K(ret));
          }
        } else {
        }  // nonbinding table group
      }
    }
  }
  return ret;
}

int ObTenantPartitionEntityIterator::init(ObSchemaGetterGuard& schema_guard, const uint64_t tenant_id)
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

int ObTenantPartitionEntityIterator::next(uint64_t& partition_entity_id)
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

}  // end of namespace schema
}  // end of namespace share
}  // end of namespace oceanbase
