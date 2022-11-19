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

#ifndef OB_HASH_PARTITIONING_BASIC_H_
#define OB_HASH_PARTITIONING_BASIC_H_

#include "share/datum/ob_datum_funcs.h"
#include "sql/engine/sort/ob_sort_basic_info.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"

namespace oceanbase
{
namespace sql
{

static const uint64_t DEFAULT_PART_HASH_VALUE = 99194853094755497L;
struct ObHashPartStoredRow : public sql::ObChunkDatumStore::StoredRow
{
  const static int64_t HASH_VAL_BIT = 63;
  const static int64_t HASH_VAL_MASK = UINT64_MAX >> (64 - HASH_VAL_BIT);
  struct ExtraInfo
  {
    uint64_t hash_val_:HASH_VAL_BIT;
    uint64_t is_match_:1;
  };
  ExtraInfo &get_extra_info()
  {
    static_assert(sizeof(ObHashPartStoredRow) == sizeof(sql::ObChunkDatumStore::StoredRow),
        "sizeof StoredJoinRow must be the save with StoredRow");
    return *reinterpret_cast<ExtraInfo *>(get_extra_payload());
  }
  const ExtraInfo &get_extra_info() const
  { return *reinterpret_cast<const ExtraInfo *>(get_extra_payload()); }

  uint64_t get_hash_value() const
  { return get_extra_info().hash_val_; }
  void set_hash_value(const uint64_t hash_val)
  { get_extra_info().hash_val_ = hash_val & HASH_VAL_MASK; }
  bool is_match() const { return get_extra_info().is_match_; }
  void set_is_match(bool is_match) { get_extra_info().is_match_ = is_match; }
  static uint64_t get_hash_mask() { return HASH_VAL_MASK; }
};

enum InputSide
{
  LEFT = 0,
  RIGHT = 1,
};

}  // namespace sql
}  // namespace oceanbase

#endif /* OB_HASH_PARTITIONING_BASIC_H_ */
