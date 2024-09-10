/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "lib/oblog/ob_log_module.h"
#include "sql/engine/join/hash_join/ob_hash_join_struct.h"
#include "share/ob_define.h"

namespace oceanbase
{
namespace sql
{

class ObJoinFilterStoreRow : public ObHJStoredRow
{
public:
  inline uint64_t get_join_filter_hash_value(const RowMeta &row_meta, uint16_t hash_id) const;
  inline void set_join_filter_hash_value(const RowMeta &row_meta, uint16_t hash_id,
                                         uint64_t hash_value);
};

inline uint64_t ObJoinFilterStoreRow::get_join_filter_hash_value(const RowMeta &row_meta,
                                                                 uint16_t hash_id) const
{
  return (reinterpret_cast<uint64_t *>(get_extra_payload(row_meta)))[hash_id];
}

inline void ObJoinFilterStoreRow::set_join_filter_hash_value(const RowMeta &row_meta,
                                                             uint16_t hash_id, uint64_t hash_value)
{
  (reinterpret_cast<uint64_t *>(get_extra_payload(row_meta)))[hash_id] = hash_value;
}

} // namespace sql

} // namespace oceanbase
