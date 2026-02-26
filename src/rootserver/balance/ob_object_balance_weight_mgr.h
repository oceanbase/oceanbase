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

#ifndef OCEANBASE_SHARE_OB_OBJECT_BALANCE_WEIGHT_MGR_H_
#define OCEANBASE_SHARE_OB_OBJECT_BALANCE_WEIGHT_MGR_H_

#include "share/balance/ob_object_balance_weight_operator.h" // ObObjectBalanceWeightKey

namespace oceanbase
{
namespace rootserver
{
class ObObjectBalanceWeightMgr
{
public:
  ObObjectBalanceWeightMgr()
    : inited_(false),
      loaded_(false),
      tenant_id_(OB_INVALID_TENANT_ID),
      obj_weight_map_() {}
  ~ObObjectBalanceWeightMgr() {}

  int init(const uint64_t tenant_id);
  void destroy() { obj_weight_map_.destroy(); }
  int load();
  int get_obj_weight(
      const common::ObObjectID &table_id,
      const common::ObObjectID &part_id,
      const common::ObObjectID &subpart_id,
      int64_t &obj_weight);
  int get_tablegroup_weight(
      const common::ObObjectID &tablegroup_id,
      int64_t &weight);

  static int try_clear_tenant_expired_obj_weight(const uint64_t tenant_id);

private:
  int check_inner_stat_();
  int inner_get_obj_weight_(
      const common::ObObjectID &table_id,
      const common::ObObjectID &part_id,
      const common::ObObjectID &subpart_id,
      int64_t &obj_weight);

  static int check_if_obj_weight_is_expired_(
      ObSchemaGetterGuard &schema_guard,
      const ObObjectBalanceWeightKey &obj_key,
      bool &is_expired);
  static int check_version_(const uint64_t tenant_id, bool &is_supported);

private:
  bool inited_;
  bool loaded_;
  uint64_t tenant_id_;
  hash::ObHashMap<ObObjectBalanceWeightKey, int64_t> obj_weight_map_;
};


} // end namespace rootserver
} // end namespace oceanbase

#endif