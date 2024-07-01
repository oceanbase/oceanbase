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

#include "lib/ob_define.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"

namespace oceanbase
{
namespace lib
{
  class MemoryContext;
}

namespace sql
{
struct ObSortVecOpContext;
struct ObCompactRow;
struct RowMeta;
class ObPushDownTopNFilterMsg;
struct ObPushDownTopNFilterInfo;
class ObExecContext;
struct ObSortFieldCollation;
class ObExprTopNFilterContext;

// ob_sort_vec_op_impl.ipp is an ipp with variable template
// move the topn code to the ob_pd_topn_sort_filter.cpp to accelerate the compile speed
class ObPushDownTopNFilter
{
public:
  ObPushDownTopNFilter()
      : enabled_(false), need_update_(false), msg_set_(false), mem_context_(nullptr),
        pd_topn_filter_info_(nullptr), pd_topn_filter_msg_(nullptr), topn_filter_ctx_(nullptr)
  {}
  ~ObPushDownTopNFilter();
  void destroy();

  // for vec2.0
  inline int init(const ObSortVecOpContext &ctx, lib::MemoryContext &mem_context);
  int init(const ObPushDownTopNFilterInfo *pd_topn_filter_info, uint64_t tenant_id,
           const ObIArray<ObSortFieldCollation> *sort_collations, ObExecContext *exec_ctx,
           lib::MemoryContext &mem_context, bool use_rich_format = false);

  int update_filter_data(ObCompactRow *compact_row, const RowMeta *row_meta_);
  int update_filter_data(ObChunkDatumStore::StoredRow *store_row);

  inline bool enabled() { return enabled_; }
  inline void set_need_update(bool flag) { need_update_ = flag; }
  inline bool need_update() { return need_update_; }

private:
  int create_pd_topn_filter_ctx(const ObPushDownTopNFilterInfo *pd_topn_filter_info,
                                ObExecContext *exec_ctx, bool use_rich_format,
                                int64_t px_seq_id);
  // publish topn msg to consumer
  int publish_topn_msg();

private:
  bool enabled_;
  bool need_update_;
  bool msg_set_;
  lib::MemoryContext mem_context_;
  const ObPushDownTopNFilterInfo *pd_topn_filter_info_;
  ObPushDownTopNFilterMsg *pd_topn_filter_msg_;
  // for local topn filter, topn_filter_ctx_ is not null
  // for global topn fitler, topn_filter_ctx_ is null
  ObExprTopNFilterContext *topn_filter_ctx_;
};

} // end namespace sql
} // end namespace oceanbase
