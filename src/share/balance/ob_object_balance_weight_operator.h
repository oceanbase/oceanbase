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

#ifndef OCEANBASE_SHARE_OB_OBJECT_BALANCE_WEIGHT_OPERATOR_H_
#define OCEANBASE_SHARE_OB_OBJECT_BALANCE_WEIGHT_OPERATOR_H_

#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace share
{
// part_id and subpart_id can be OB_INVALID_ID
class ObObjectBalanceWeightKey
{
public:
  ObObjectBalanceWeightKey()
      : tenant_id_(OB_INVALID_TENANT_ID),
        table_id_(OB_INVALID_ID),
        part_id_(OB_INVALID_ID),
        subpart_id_(OB_INVALID_ID) {}
  ~ObObjectBalanceWeightKey() {}
  int init(
      const uint64_t tenant_id,
      const ObObjectID &table_id,
      const ObObjectID &part_id,
      const ObObjectID &subpart_id);
  int init_tablegroup_key(const uint64_t tenant_id, const ObObjectID &tablegroup_id);
  int assign(const ObObjectBalanceWeightKey &other);
  void reset();
  bool is_valid() const;
  bool operator == (const ObObjectBalanceWeightKey &other) const;
  bool operator != (const ObObjectBalanceWeightKey &other) const;
  int hash(uint64_t &hash_val) const;
  uint64_t hash() const;
  uint64_t get_tenant_id() const { return tenant_id_; }
  ObObjectID get_table_id() const { return table_id_; }
  ObObjectID get_part_id() const { return part_id_; }
  ObObjectID get_subpart_id() const { return subpart_id_; }

  TO_STRING_KV(K_(tenant_id), K_(table_id), K_(part_id), K_(subpart_id));
private:
  uint64_t tenant_id_;
  ObObjectID table_id_;
  ObObjectID part_id_;
  ObObjectID subpart_id_;
};

class ObObjectBalanceWeight
{
public:
  ObObjectBalanceWeight()
      : obj_key_(),
        weight_(0) {}
  ~ObObjectBalanceWeight() {}
  int init(
      const uint64_t tenant_id,
      const ObObjectID &table_id,
      const ObObjectID &part_id,
      const ObObjectID &subpart_id,
      const int64_t weight);
  int init_tablegroup_weight(
      const uint64_t tenant_id,
      const ObObjectID &tablegroup_id,
      const int64_t weight);
  int assign(const ObObjectBalanceWeight &other);
  void reset();
  bool is_valid() const;
  const ObObjectBalanceWeightKey &get_obj_key() const { return obj_key_; }
  int64_t get_weight() const { return weight_; }
  TO_STRING_KV(K_(obj_key), K_(weight));
private:
  ObObjectBalanceWeightKey obj_key_;
  int64_t weight_;
};

class ObObjectBalanceWeightOperator
{
public:
  static int update(
      ObISQLClient &client,
      const ObObjectBalanceWeight &obj_balance_weight);

  static int remove(
      ObISQLClient &client,
      const ObObjectBalanceWeightKey &obj_key);

  static int batch_remove(
      ObISQLClient &client,
      const ObIArray<ObObjectBalanceWeightKey> &obj_keys);

  static int get_by_tenant(
      ObISQLClient &client,
      const uint64_t tenant_id,
      ObIArray<ObObjectBalanceWeight> &obj_balance_weights);

private:
  static int fill_dml_with_key_(
      const ObObjectBalanceWeightKey &obj_key,
      share::ObDMLSqlSplicer &dml);
  static int construct_obj_balance_weight_(
      const uint64_t tenant_id,
      common::sqlclient::ObMySQLResult &result,
      ObObjectBalanceWeight &obj_weight);
  static int inner_batch_remove_by_sql_(
      ObISQLClient &client,
      const ObIArray<ObObjectBalanceWeightKey> &obj_keys,
      const int64_t start_idx,
      const int64_t end_idx);
};

} // end of share
} // end of oceanbase
#endif // OCEANBASE_SHARE_OB_OBJECT_BALANCE_WEIGHT_OPERATOR_H_