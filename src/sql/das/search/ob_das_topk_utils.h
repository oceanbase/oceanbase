/**
 * Copyright (c) 2025 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SQL_OB_DAS_TOPK_UTILS_H_
#define OCEANBASE_SQL_OB_DAS_TOPK_UTILS_H_

#include "lib/container/ob_heap.h"
#include "lib/hash/ob_hashmap.h"
#include "sql/das/ob_das_ir_define.h"

namespace oceanbase
{
namespace sql
{

struct ObDASTopKItem
{
  ObDASTopKItem() : score_(0.0), cache_idx_(-1) {}
  ObDASTopKItem(const double score, const int64_t idx) : score_(score), cache_idx_(idx) {}
  ~ObDASTopKItem() = default;
  TO_STRING_KV(K_(score), K_(cache_idx));
  double score_;
  int64_t cache_idx_;
};

struct ObDASTopKItemCmp
{
  bool operator()(const ObDASTopKItem &a, const ObDASTopKItem &b) const
  {
    return a.score_ > b.score_;
  }
  int get_error_code() { return OB_SUCCESS; }
};

typedef common::ObBinaryHeap<ObDASTopKItem, ObDASTopKItemCmp> ObDASTopKHeap;
typedef hash::ObHashMap<ObDocIdExt, ObDASTopKItem> ObDASTopKHashMap;

} // namespace sql
} // namesapce oceanbase

#endif // OCEANBASE_SQL_OB_DAS_TOPK_UTILS_H_