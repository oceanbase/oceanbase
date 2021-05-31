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

#define USING_LOG_PREFIX SQL_ENG
#include "lib/allocator/ob_malloc.h"
#include "lib/utility/utility.h"
#include "common/row/ob_row.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/aggregate/ob_distinct.h"

namespace oceanbase {
using namespace common;

namespace sql {
ObDistinct::ObDistinct(common::ObIAllocator& alloc)
    : ObSingleChildPhyOperator(alloc), distinct_columns_(alloc), is_block_mode_(false)
{}

ObDistinct::~ObDistinct()
{}

void ObDistinct::reset()
{
  distinct_columns_.reset();
  ObSingleChildPhyOperator::reset();
}

void ObDistinct::reuse()
{
  distinct_columns_.reuse();
  ObSingleChildPhyOperator::reuse();
}

int ObDistinct::add_distinct_column(const int64_t column_index, ObCollationType cs_type)
{
  ObDistinctColumn distinct_column;
  distinct_column.index_ = column_index;
  distinct_column.cs_type_ = cs_type;
  return distinct_columns_.push_back(distinct_column);
}

int64_t ObDistinct::to_string_kv(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(N_DISTINCT, distinct_columns_);
  return pos;
}
OB_SERIALIZE_MEMBER((ObDistinct, ObSingleChildPhyOperator), distinct_columns_, is_block_mode_);

}  // namespace sql
}  // namespace oceanbase
