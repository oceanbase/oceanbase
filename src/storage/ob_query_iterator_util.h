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

#ifndef OCEANBASE_STORAGE_OB_QUERY_ITERATOR_UTIL_
#define OCEANBASE_STORAGE_OB_QUERY_ITERATOR_UTIL_

#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_se_array.h"
#include "common/row/ob_row.h"
#include "sql/engine/expr/ob_expr.h"

namespace oceanbase {
namespace storage {
struct ObPadCol {

  int64_t idx_;
  common::ObObjMeta meta_type_;
  int32_t data_length_;

  TO_STRING_KV("idx", idx_, "type", meta_type_, "data_length", data_length_);
};

typedef common::ObSEArray<ObPadCol, common::OB_ROW_MAX_COLUMNS_COUNT> PadColArray;

int pad_column(const common::ObAccuracy accuracy, common::ObIAllocator& padding_alloc, common::ObObj& cell);

int pad_column(const common::ObAccuracy accuracy, sql::ObEvalCtx& ctx, sql::ObExpr* expr);
}  // namespace storage
}  // namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_QUERY_ITERATOR_UTIL_
