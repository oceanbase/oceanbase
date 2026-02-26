/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX BALANCE

#include "rootserver/balance/ob_object_balance_weight_mgr.h"
#include "rootserver/ob_tenant_event_def.h"           // TENANT_EVENT

namespace oceanbase
{
using namespace share;
using namespace common;
using namespace tenant_event;

namespace rootserver
{
int ObObjectBalanceWeightMgr::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  const int64_t MAP_BUCKET_NUM = 128;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(obj_weight_map_.create(
      MAP_BUCKET_NUM,
      lib::ObLabel("ObjWeightMap"),
      ObModIds::OB_HASH_NODE,
      tenant_id))) {
    LOG_WARN("create map for part weight map failed", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    loaded_ = false;
    inited_ = true;
  }
  return ret;
}

int ObObjectBalanceWeightMgr::load()
{
  int ret = OB_SUCCESS;
  ObArray<ObObjectBalanceWeight> obj_weights;
  bool is_supported_version = true;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(check_version_(tenant_id_, is_supported_version))) {
    LOG_WARN("check version failed", KR(ret), K(tenant_id_));
  } else if (OB_UNLIKELY(!is_supported_version)) {
    // skip
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (FALSE_IT(obj_weights.set_tenant_id(tenant_id_))) {
  } else if (OB_FAIL(ObObjectBalanceWeightOperator::get_by_tenant(
      *GCTX.sql_proxy_,
      tenant_id_,
      obj_weights))) { // TODO: split into small batches
    LOG_WARN("get by tenant failed", KR(ret), K(tenant_id_));
  } else {
    ARRAY_FOREACH(obj_weights, idx) {
      const ObObjectBalanceWeightKey &obj_key = obj_weights.at(idx).get_obj_key();
      int64_t obj_weight = obj_weights.at(idx).get_weight();
      if (OB_FAIL(obj_weight_map_.set_refactored(obj_key, obj_weight))) {
        LOG_WARN("set refactored failed", KR(ret), K(obj_key), K(obj_weight));
      }
    }
    if (OB_SUCC(ret)) {
      loaded_ = true;
    }
  }
  return ret;
}

int ObObjectBalanceWeightMgr::check_inner_stat_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || !loaded_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_), K(loaded_), K(tenant_id_));
  }
  return ret;
}

int ObObjectBalanceWeightMgr::try_clear_tenant_expired_obj_weight(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObArray<ObObjectBalanceWeight> tenant_obj_weights;
  ObArray<ObObjectBalanceWeightKey> expired_obj_keys;
  schema::ObSchemaGetterGuard schema_guard;
  bool is_supported_version = true;
  const int64_t begin_ts = ObTimeUtil::current_time();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_version_(tenant_id, is_supported_version))) {
    LOG_WARN("check version failed", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!is_supported_version)) {
    // do nothing
  } else if (OB_ISNULL(GCTX.schema_service_) || OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null ptr in GCTX", KR(ret), KP(GCTX.schema_service_), KP(GCTX.sql_proxy_));
  } else if (FALSE_IT(tenant_obj_weights.set_tenant_id(tenant_id))) {
  } else if (FALSE_IT(expired_obj_keys.set_tenant_id(tenant_id))) {
  } else if (OB_FAIL(ObObjectBalanceWeightOperator::get_by_tenant(
      *GCTX.sql_proxy_,
      tenant_id,
      tenant_obj_weights))) { // TODO: split into small batches
    LOG_WARN("get by tenant failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get tenant schema guard fail", KR(ret), K(tenant_id));
  } else { // get tenant scheme_guard after get_by_tenant to make sure the schema is newer
    ARRAY_FOREACH(tenant_obj_weights, idx) {
      bool is_expired = false;
      const ObObjectBalanceWeightKey &obj_key = tenant_obj_weights.at(idx).get_obj_key();
      if (OB_FAIL(check_if_obj_weight_is_expired_(schema_guard, obj_key, is_expired))) {
        LOG_WARN("check and clear expired obj weight failed", KR(ret), K(obj_key));
      } else if (!is_expired) {
        continue;
      } else if (OB_FAIL(expired_obj_keys.push_back(obj_key))) {
        LOG_WARN("push back failed", KR(ret), K(obj_key), K(expired_obj_keys));
      }
    }
    if (OB_FAIL(ret) || expired_obj_keys.empty()) {
      // do nothing
    } else if (OB_FAIL(ObObjectBalanceWeightOperator::batch_remove(*GCTX.sql_proxy_, expired_obj_keys))) {
      LOG_WARN("batch remove failed", KR(ret), K(expired_obj_keys));
    } else {
      LOG_INFO("remove expired obj weight successfully", K(expired_obj_keys));
      ARRAY_FOREACH(expired_obj_keys, j) {
        const int64_t end_ts = ObTimeUtil::current_time();
        const ObObjectBalanceWeightKey &obj_key = expired_obj_keys.at(j);
        TENANT_EVENT(tenant_id, DBMS_BALANCE, CLEAR_EXPIRED_BALANCE_WEIGHT, end_ts, ret, end_ts - begin_ts,
          obj_key.get_table_id(), obj_key.get_part_id(), obj_key.get_subpart_id());
      }
    }
  }
  return ret;
}

int ObObjectBalanceWeightMgr::check_if_obj_weight_is_expired_(
    ObSchemaGetterGuard &schema_guard,
    const ObObjectBalanceWeightKey &obj_key,
    bool &is_expired)
{
  int ret = OB_SUCCESS;
  is_expired = false;
  const ObSimpleTablegroupSchema *tablegroup_schema = nullptr;
  const ObSimpleTableSchemaV2 *table_schema = nullptr;
  ObObjectID object_id;
  ObTabletID tablet_id;
  const int64_t begin_ts = ObTimeUtil::current_time();
  if (OB_UNLIKELY(!schema_guard.is_inited())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("schema_guard is not inited", KR(ret));
  } else if (OB_UNLIKELY(!obj_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(obj_key));
  } else if (OB_FAIL(schema_guard.get_tablegroup_schema(
      obj_key.get_tenant_id(),
      obj_key.get_table_id(),
      tablegroup_schema))) { // the tablegroup_id is also recorded in the table_id column
    LOG_WARN("get tablegroup schema failed", KR(ret), K(obj_key));
  } else if (OB_NOT_NULL(tablegroup_schema)) {
    if (!tablegroup_schema->is_sharding_none()) {
      is_expired = true;
    } else {
      // tablegroup level weight is valid, do nothing
    }
  } else if (OB_FAIL(schema_guard.get_simple_table_schema(
      obj_key.get_tenant_id(),
      obj_key.get_table_id(),
      table_schema))) {
    LOG_WARN("get simple table schema failed", KR(ret), K(obj_key));
  } else if (OB_ISNULL(table_schema)) {
    is_expired = true;
  } else if (OB_INVALID_ID == obj_key.get_part_id()) {
    // obj_key is table level, it is valid
  } else if (OB_UNLIKELY(PARTITION_LEVEL_ONE != table_schema->get_part_level())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported part level", KR(ret), KPC(table_schema));
  } else if (OB_FAIL(table_schema->get_tablet_id_by_object_id(obj_key.get_part_id(), tablet_id))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      is_expired = true;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("get tablet id by object id failed", KR(ret), K(obj_key));
    }
  } else {
    // partition level object weight is valid, do nothing
  }

  // double check to prevent the local schema guard is not new enough
  if (OB_SUCC(ret) && is_expired) {
    ObSimpleTableSchemaV2 *latest_table_schema = NULL;
    is_expired = false;
    ObArenaAllocator allocator;
    if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null ptr in GCTX", KR(ret), KP(GCTX.sql_proxy_));
    } else if (OB_FAIL(ObSchemaUtils::get_latest_table_schema(
        *GCTX.sql_proxy_,
        allocator,
        obj_key.get_tenant_id(),
        obj_key.get_table_id(),
        latest_table_schema))) {
      if (OB_TABLE_NOT_EXIST == ret) {
        is_expired = true;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get latest table schema", KR(ret), K(obj_key));
      }
    } else if (OB_ISNULL(latest_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema is null", KR(ret), K(obj_key));
    } else if (OB_INVALID_ID != obj_key.get_part_id()
        && OB_FAIL(latest_table_schema->get_tablet_id_by_object_id(obj_key.get_part_id(), tablet_id))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        is_expired = true;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get tablet id by object id failed", KR(ret), K(obj_key));
      }
    }
  }
  return ret;
}

//TODO: support subpart later
int ObObjectBalanceWeightMgr::get_obj_weight(
    const common::ObObjectID &table_id,
    const common::ObObjectID &part_id,
    const common::ObObjectID &subpart_id,
    int64_t &obj_weight)
{
  int ret = OB_SUCCESS;
  obj_weight = 0;
  int64_t table_weight = 0;
  int64_t part_weight = 0;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat failed", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_id(table_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table_id", KR(ret), K(table_id), K(part_id), K(subpart_id));
  } else if (OB_UNLIKELY(is_valid_id(subpart_id))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("balance weight of subpartition is not supported",
        KR(ret), K(table_id), K(part_id), K(subpart_id));
  } else if (OB_FAIL(inner_get_obj_weight_(table_id, OB_INVALID_ID, OB_INVALID_ID, table_weight))) {
    LOG_WARN("inner get table weight failed", KR(ret), K(table_id));
  } else if (is_valid_id(part_id)
      && OB_FAIL(inner_get_obj_weight_(table_id, part_id, OB_INVALID_ID, part_weight))) {
    LOG_WARN("inner get part weight failed", KR(ret), K(table_id));
  } else {
    obj_weight = (part_weight > 0) ? part_weight : table_weight;
  }
  return ret;
}

int ObObjectBalanceWeightMgr::inner_get_obj_weight_(
    const common::ObObjectID &table_id,
    const common::ObObjectID &part_id,
    const common::ObObjectID &subpart_id,
    int64_t &obj_weight)
{
  int ret = OB_SUCCESS;
  obj_weight = 0;
  ObObjectBalanceWeightKey obj_key;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat failed", KR(ret));
  } else if (OB_FAIL(obj_key.init(tenant_id_, table_id, part_id, subpart_id))) {
    LOG_WARN("init failed", KR(ret), K(tenant_id_), K(table_id), K(part_id), K(subpart_id));
  } else if (OB_FAIL(obj_weight_map_.get_refactored(obj_key, obj_weight))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      obj_weight = 0;
    } else {
      LOG_WARN("get_refactored failed", KR(ret), K(obj_key));
    }
  }
  return ret;
}

int ObObjectBalanceWeightMgr::check_version_(const uint64_t tenant_id, bool &is_supported)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  is_supported = true;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get tenant data version", KR(ret), K(tenant_id), K(data_version));
  } else if (data_version < MOCK_DATA_VERSION_4_2_5_2
      || (data_version >= DATA_VERSION_4_3_0_0
          && data_version < DATA_VERSION_4_4_1_0)) {
    is_supported = false;
    LOG_INFO("not support version", KR(ret), K(tenant_id), K(data_version), K(is_supported));
  }
  return ret;
}

// the tablegroup_id is also recorded in the table_id column
int ObObjectBalanceWeightMgr::get_tablegroup_weight(
  const common::ObObjectID &tablegroup_id,
  int64_t &weight)
{
  int ret = OB_SUCCESS;
  weight = 0;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat failed", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_id(tablegroup_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablegroup_id", KR(ret), K(tablegroup_id));
  } else if (OB_FAIL(inner_get_obj_weight_(tablegroup_id, OB_INVALID_ID, OB_INVALID_ID, weight))) {
    LOG_WARN("inner get obj weight failed", KR(ret), K(tablegroup_id));
  }
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase