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

#define USING_LOG_PREFIX SHARE

#include "ob_object_balance_weight_operator.h"

namespace oceanbase
{
using namespace common;
namespace share
{

int ObObjectBalanceWeightKey::init(
    const uint64_t tenant_id,
    const ObObjectID &table_id,
    const ObObjectID &part_id,
    const ObObjectID &subpart_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || OB_INVALID_ID == table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id),
        K(table_id), K(part_id), K(subpart_id));
  } else {
    tenant_id_ = tenant_id;
    table_id_ = table_id;
    part_id_ = part_id;
    subpart_id_ = subpart_id;
  }
  return ret;
}

int ObObjectBalanceWeightKey::init_tablegroup_key(
    const uint64_t tenant_id,
    const ObObjectID &tablegroup_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || !is_valid_id(tablegroup_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(tablegroup_id));
  } else {
    tenant_id_ = tenant_id;
    table_id_ = tablegroup_id;
    part_id_ = OB_INVALID_ID;
    subpart_id_ = OB_INVALID_ID;
  }
  return ret;
}

int ObObjectBalanceWeightKey::assign(const ObObjectBalanceWeightKey &other)
{
  int ret = OB_SUCCESS;
  if (&other != this) {
    tenant_id_ = other.tenant_id_;
    table_id_ = other.table_id_;
    part_id_ = other.part_id_;
    subpart_id_ = other.subpart_id_;
  }
  return ret;
}

void ObObjectBalanceWeightKey::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  table_id_ = OB_INVALID_ID;
  part_id_ = OB_INVALID_ID;
  subpart_id_ = OB_INVALID_ID;
}

bool ObObjectBalanceWeightKey::is_valid() const
{
  return is_valid_tenant_id(tenant_id_)
      && table_id_ != OB_INVALID_ID;
}

bool ObObjectBalanceWeightKey::operator == (const ObObjectBalanceWeightKey &other) const
{
  return other.tenant_id_ == tenant_id_
      && other.table_id_ == table_id_
      && other.part_id_ == part_id_
      && other.subpart_id_ == subpart_id_;
}

bool ObObjectBalanceWeightKey::operator != (const ObObjectBalanceWeightKey &other) const
{
  return !(other == *this);
}

uint64_t ObObjectBalanceWeightKey::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
  hash_val = murmurhash(&table_id_, sizeof(table_id_), hash_val);
  hash_val = murmurhash(&part_id_, sizeof(part_id_), hash_val);
  hash_val = murmurhash(&subpart_id_, sizeof(subpart_id_), hash_val);
  return hash_val;
}

int ObObjectBalanceWeightKey::hash(uint64_t &hash_val) const
{
  hash_val = hash();
  return OB_SUCCESS;
}

int ObObjectBalanceWeight::init(
    const uint64_t tenant_id,
    const ObObjectID &table_id,
    const ObObjectID &part_id,
    const ObObjectID &subpart_id,
    const int64_t weight)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || OB_INVALID_ID == table_id
      || weight <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id),
        K(table_id), K(part_id), K(subpart_id), K(weight));
  } else if (OB_FAIL(obj_key_.init(tenant_id, table_id, part_id, subpart_id))) {
    LOG_WARN("assign failed", KR(ret),  K(tenant_id),
        K(table_id), K(part_id), K(subpart_id));
  } else {
    weight_ = weight;
  }
  return ret;
}

int ObObjectBalanceWeight::init_tablegroup_weight(
    const uint64_t tenant_id,
    const ObObjectID &tablegroup_id,
    const int64_t weight)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || !is_valid_id(tablegroup_id)
      || weight <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(tablegroup_id), K(weight));
  } else if (OB_FAIL(obj_key_.init_tablegroup_key(tenant_id, tablegroup_id))) {
    LOG_WARN("assign failed", KR(ret), K(tenant_id), K(tablegroup_id));
  } else {
    weight_ = weight;
  }
  return ret;
}

int ObObjectBalanceWeight::assign(const ObObjectBalanceWeight &other)
{
  int ret = OB_SUCCESS;
  if (&other != this) {
    if (OB_FAIL(obj_key_.assign(other.obj_key_))) {
      LOG_WARN("assign failed", KR(ret), K(other));
    } else {
      weight_ = other.weight_;
    }
  }
  return ret;
}

void ObObjectBalanceWeight::reset()
{
  obj_key_.reset();
  weight_ = 0;
}

bool ObObjectBalanceWeight::is_valid() const
{
  return obj_key_.is_valid() && weight_ > 0;
}


int ObObjectBalanceWeightOperator::update(
    ObISQLClient &client,
    const ObObjectBalanceWeight &obj_balance_weight)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  if (OB_UNLIKELY(!obj_balance_weight.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(obj_balance_weight));
  } else if (OB_FAIL(fill_dml_with_key_(obj_balance_weight.get_obj_key(), dml))) {
    LOG_WARN("fill dml splicer failed", KR(ret), K(obj_balance_weight));
  } else if (OB_FAIL(dml.add_column("weight", obj_balance_weight.get_weight()))) {
    LOG_WARN("add column failed", KR(ret), K(obj_balance_weight));
  } else {
    int64_t affected_rows = 0;
    ObDMLExecHelper exec(client, obj_balance_weight.get_obj_key().get_tenant_id());
    if (OB_FAIL(exec.exec_insert_update(OB_ALL_OBJECT_BALANCE_WEIGHT_TNAME, dml, affected_rows))) {
      LOG_WARN("execute insert update failed", KR(ret), K(obj_balance_weight));
    } else if (OB_UNLIKELY(affected_rows < 0 || affected_rows > 2)) {
      // affected_rows = 2 when on duplicate key update
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected affected_rows", KR(ret), K(affected_rows));
    }
  }
  return ret;
}

int ObObjectBalanceWeightOperator::fill_dml_with_key_(
    const ObObjectBalanceWeightKey &obj_key,
    share::ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!obj_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(obj_key));
  } else if (OB_FAIL(dml.add_pk_column("table_id", obj_key.get_table_id()))
      || OB_FAIL(dml.add_pk_column("partition_id", obj_key.get_part_id()))
      || OB_FAIL(dml.add_pk_column("subpartition_id", obj_key.get_subpart_id()))) {
    LOG_WARN("fill dml spliter failed", KR(ret), K(obj_key));
  }
  return ret;
}

int ObObjectBalanceWeightOperator::remove(
    ObISQLClient &client,
    const ObObjectBalanceWeightKey &obj_key)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  if (OB_UNLIKELY(!obj_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(obj_key));
  } else if (OB_FAIL(fill_dml_with_key_(obj_key, dml))) {
    LOG_WARN("fill dml splicer failed", KR(ret), K(obj_key));
  } else {
    int64_t affected_rows = 0;
    ObDMLExecHelper exec(client, obj_key.get_tenant_id());
    if (OB_FAIL(exec.exec_delete(OB_ALL_OBJECT_BALANCE_WEIGHT_TNAME, dml, affected_rows))) {
      LOG_WARN("execute insert update failed", KR(ret), K(obj_key));
    } else if (0 == affected_rows) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("object balance weight not exist", KR(ret), K(obj_key));
    } else if (OB_UNLIKELY(affected_rows < 0 || affected_rows >= 2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected affected_rows", KR(ret), K(affected_rows));
    }
  }
  return ret;
}

int ObObjectBalanceWeightOperator::batch_remove(
    ObISQLClient &client,
    const ObIArray<ObObjectBalanceWeightKey> &obj_keys)
{
  int ret = OB_SUCCESS;
  if (obj_keys.empty()) {
    // do nothing
  } else {
    const int64_t BATCH_SIZE = GCONF.tablet_meta_table_scan_batch_count;
    int64_t start_idx = 0;
    int64_t end_idx = min(BATCH_SIZE, obj_keys.count());
    while (OB_SUCC(ret) && (start_idx < end_idx)) {
      if (OB_FAIL(inner_batch_remove_by_sql_(client, obj_keys, start_idx, end_idx))) {
        LOG_WARN("fail to inner batch remove", KR(ret), K(obj_keys), K(start_idx), K(end_idx));
      } else {
        start_idx = end_idx;
        end_idx = min(start_idx + BATCH_SIZE, obj_keys.count());
      }
    }
  }
  return ret;
}

int ObObjectBalanceWeightOperator::inner_batch_remove_by_sql_(
    ObISQLClient &client,
    const ObIArray<ObObjectBalanceWeightKey> &obj_keys,
    const int64_t start_idx,
    const int64_t end_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(obj_keys.count() <= 0
      || start_idx < 0
      || start_idx >= end_idx
      || end_idx > obj_keys.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), "obj_keys count", obj_keys.count(), K(start_idx), K(end_idx));
  } else {
    int64_t affected_rows = 0;
    ObSqlString sql;
    ObDMLSqlSplicer dml;
    const uint64_t tenant_id = obj_keys.at(0).get_tenant_id();
    for (int64_t idx = start_idx; OB_SUCC(ret) && (idx < end_idx); ++idx) {
      const ObObjectBalanceWeightKey &obj_key = obj_keys.at(idx);
      if (OB_UNLIKELY(!obj_key.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid obj_key", KR(ret), K(obj_key));
      } else if (OB_FAIL(fill_dml_with_key_(obj_key, dml))) {
        LOG_WARN("fail to fill dml splicer", KR(ret), K(obj_key));
      } else if (OB_FAIL(dml.finish_row())) {
        LOG_WARN("fail to finish row", KR(ret), K(obj_key));
      }
    }
    if (FAILEDx(dml.splice_batch_delete_sql(OB_ALL_OBJECT_BALANCE_WEIGHT_TNAME, sql))) {
      LOG_WARN("fail to splice batch delete sql", KR(ret), K(sql));
    } else if (OB_FAIL(client.write(tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(sql));
    }
  }
  return ret;
}


int ObObjectBalanceWeightOperator::get_by_tenant(
    ObISQLClient &client,
    const uint64_t tenant_id,
    ObIArray<ObObjectBalanceWeight> &obj_balance_weights)
{
  int ret = OB_SUCCESS;
  obj_balance_weights.reset();
  ObSqlString sql;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invlid arg", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("select * from %s", OB_ALL_OBJECT_BALANCE_WEIGHT_TNAME))) {
    LOG_WARN("assign fmt failed", KR(ret), K(tenant_id));
  } else {
    HEAP_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(client.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("failed to read", KR(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get sql result", KR(ret));
      } else {
        ObObjectBalanceWeight obj_weight;
        while(OB_SUCC(ret)) {
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              break;
            } else {
              LOG_WARN("failed to next", KR(ret), K(sql));
            }
          } else if (OB_FAIL(construct_obj_balance_weight_(tenant_id, *result, obj_weight))) {
            LOG_WARN("construct obj balance weight failed", KR(ret), K(tenant_id), K(sql));
          } else if (OB_FAIL(obj_balance_weights.push_back(obj_weight))) {
            LOG_WARN("push back failed", KR(ret), K(obj_weight));
          }
        }//end while

      }
    }
  }
  return ret;
}

int ObObjectBalanceWeightOperator::construct_obj_balance_weight_(
    const uint64_t tenant_id,
    common::sqlclient::ObMySQLResult &result,
    ObObjectBalanceWeight &obj_weight)
{
  int ret = OB_SUCCESS;
  obj_weight.reset();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(tenant_id));
  } else {
    ObObjectID table_id = OB_INVALID_ID;
    ObObjectID part_id = OB_INVALID_ID;
    ObObjectID subpart_id = OB_INVALID_ID;
    int64_t weight = 0;
    EXTRACT_INT_FIELD_MYSQL(result, "table_id", table_id, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "partition_id", part_id, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "subpartition_id", subpart_id, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "weight", weight, int64_t);
    if (FAILEDx(obj_weight.init(tenant_id, table_id, part_id, subpart_id, weight))) {
      LOG_WARN("init failed", KR(ret), K(tenant_id),
          K(table_id), K(part_id), K(subpart_id), K(weight));
    }
  }
  return ret;
}

} // end namespace share
} // end namespace oceanbase