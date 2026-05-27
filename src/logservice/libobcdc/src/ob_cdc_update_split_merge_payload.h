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

#ifndef OCEANBASE_LIBOBCDC_UPDATE_SPLIT_MERGE_PAYLOAD_H_
#define OCEANBASE_LIBOBCDC_UPDATE_SPLIT_MERGE_PAYLOAD_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_se_array.h"
#include "lib/utility/ob_unify_serialize.h"
#include "ob_log_binlog_record.h"

namespace oceanbase
{
namespace libobcdc
{

struct MergeOldColValue
{
  OB_UNIS_VERSION(1);
public:
  MergeOldColValue() : data_(), origin_(static_cast<uint8_t>(REDO)) {}
  MergeOldColValue(const common::ObString &data, const uint8_t origin) : data_(data), origin_(origin) {}

  TO_STRING_KV(K_(data), K_(origin));

  common::ObString data_;
  uint8_t origin_;
};

struct MergeOldColsPayload
{
  OB_UNIS_VERSION(1);
public:
  MergeOldColsPayload() : old_cols_() {}

  int init_from_delete(IBinlogRecord &del_data, common::ObIAllocator &allocator);
  int apply_to_insert(IBinlogRecord &ins_data) const;

  TO_STRING_KV(K_(old_cols));

  common::ObSEArray<MergeOldColValue, 2> old_cols_;
};

} // namespace libobcdc
} // namespace oceanbase

#endif
