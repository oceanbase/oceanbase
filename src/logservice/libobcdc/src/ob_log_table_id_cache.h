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

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_TABLE_ID_CACHE_H_
#define OCEANBASE_LIBOBCDC_OB_LOG_TABLE_ID_CACHE_H_

#include "lib/hash/ob_linear_hash_map.h"        // ObLinearHashMap

namespace oceanbase
{
namespace libobcdc
{
// Cache of Global General Index
struct TableID
{
  uint64_t table_id_;

  TableID(const uint64_t table_id) : table_id_(table_id) {}

  int64_t hash() const
  {
    return static_cast<int64_t>(table_id_);
  }

  int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return OB_SUCCESS;
  }

  bool operator== (const TableID &other) const
  {
    return table_id_ == other.table_id_;
  }

  TO_STRING_KV(K_(table_id));
};

// Record table_id
// 1. for the primary table, record itself
// 2. For an index table, record table_id of its primary table
struct TableInfo
{
  uint64_t table_id_;

  TableInfo() { reset(); }
  ~TableInfo() { reset(); }

  void reset();
  int init(const uint64_t table_id);

  TO_STRING_KV(K_(table_id));
};
struct TableInfoEraserByTenant
{
  uint64_t tenant_id_;
  bool is_global_normal_index_;

  explicit TableInfoEraserByTenant(const uint64_t id, const bool is_global_normal_index)
    : tenant_id_(id), is_global_normal_index_(is_global_normal_index) {}
  bool operator()(const TableID &table_id_key, TableInfo &tb_info);
};

// Global General Index Cache
typedef common::ObLinearHashMap<TableID, TableInfo> GIndexCache;
// TableIDCache, records master table, unique index table, global unique index table_id, used to filter tables within a partition group
typedef common::ObLinearHashMap<TableID, TableInfo> TableIDCache;
}
}

#endif
