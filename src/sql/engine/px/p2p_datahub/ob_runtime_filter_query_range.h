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

#pragma once

#include "lib/ob_define.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_fixed_array.h"
#include "common/object/ob_object.h"

namespace oceanbase
{
namespace sql
{
// for runtime filter extract query range,
// we need to maintain the runtime filter info in GI operator
struct ObPxRFStaticInfo
{
  OB_UNIS_VERSION(1);

public:
  ObPxRFStaticInfo() : is_inited_(false), is_shared_(false), p2p_dh_ids_()
  {}
  int init(const ObIArray<int64_t> &p2p_dh_ids, bool is_shared);
  int assign(const ObPxRFStaticInfo &other);
  inline void destroy() {
    p2p_dh_ids_.reset();
  }

public:
  bool is_inited_;
  bool is_shared_; // whether the RF is shared in sql level
  ObSEArray<int64_t, 2> p2p_dh_ids_; // 2 ids, for in msg && range msg
  TO_STRING_KV(K(is_inited_), K(p2p_dh_ids_), K(is_shared_));
};

struct ObPxQueryRangeInfo
{
  OB_UNIS_VERSION(1);

public:
  static const int64_t MAX_IN_FILTER_QUERY_RANGE_COUNT = 128;
public:
  ObPxQueryRangeInfo(common::ObIAllocator &allocator)
      : table_id_(OB_INVALID_ID), range_column_cnt_(-1), prefix_col_idxs_(allocator),
        prefix_col_obj_metas_(allocator)
  {}
  int init(int64_t table_id, int64_t range_column_cnt, const ObIArray<int64_t> &prefix_col_idxs,
           const ObIArray<ObObjMeta> &prefix_col_obj_metas);
  int assign(const ObPxQueryRangeInfo &other);
  inline bool can_extract() const { return 0 < prefix_col_idxs_.count() ; }
  inline void destroy() {
    prefix_col_idxs_.reset();
    prefix_col_obj_metas_.reset();
  }
public:
  int64_t table_id_;
  // if the coloumn extracted by runtime filter is less than
  // the origin column count, range_column_cnt_ tells we how many columns we need
  // to fill with (min, max)
  int64_t range_column_cnt_;
  // the sequence of join key may be different from the index range column,
  // here we maintain the mapping info for these different for output query range.
  // for example
  // join key: A.c1=B.c1 && A.c2=B.c2; the index of table B: (c2, c1, c3)
  // then the prefix_col_idxs_ = [1, 0],
  // and we should fill (min, max) for c3
  ObFixedArray<int64_t, common::ObIAllocator> prefix_col_idxs_;
  ObFixedArray<ObObjMeta, common::ObIAllocator> prefix_col_obj_metas_;
  TO_STRING_KV(K_(table_id), K_(range_column_cnt), K_(prefix_col_idxs), K_(prefix_col_obj_metas));
};

} //end name space sql

} //end name space oceanbase
