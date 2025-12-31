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

#ifndef OB_STORAGE_BLOCK_ROW_STORE_H_
#define OB_STORAGE_BLOCK_ROW_STORE_H_

#include "common/object/ob_object.h"
#include "lib/container/ob_bitmap.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "storage/access/ob_where_optimizer.h"

namespace oceanbase
{

namespace blocksstable
{
class ObIMicroBlockRowScanner;
class ObMicroBlockDecoder;
}
namespace storage
{
struct ObTableAccessContext;
struct ObTableAccessParam;
struct ObTableIterParam;
struct ObStoreRow;
struct ObTableScanStoreStat;

struct ObFilterResult
{
public:
  ObFilterResult() :
      filter_start_(0),
      bitmap_(nullptr)
  {}
  bool test(int64_t row_id) const
  {
    bool not_filtered = true;
    if (nullptr == bitmap_ || bitmap_->test(row_id - filter_start_)) {
    } else {
      not_filtered = false;
    }
    return not_filtered;
  }
  int64_t filter_start_;
  const ObBitmap *bitmap_;
  TO_STRING_KV(K_(filter_start), KP_(bitmap));
};

class ObBlockRowStore
{
public:
  ObBlockRowStore(ObTableAccessContext &context);
  virtual ~ObBlockRowStore();
  virtual void reset();
  virtual void reuse();
  virtual int init(const ObTableAccessParam &param, common::hash::ObHashSet<int32_t> *agg_col_mask = nullptr);
  int open(ObTableIterParam &iter_param);
  OB_INLINE bool is_valid() const { return is_inited_; }
  OB_INLINE bool is_disabled() const { return disabled_; }
  OB_INLINE void disable() { disabled_ = true; }
  OB_INLINE virtual bool can_refresh() const { return !is_aggregated_in_prefetch_; }
  OB_INLINE void set_aggregated_in_prefetch() { is_aggregated_in_prefetch_ = true; }
  // for blockscan
  OB_INLINE bool filter_pushdown() const { return pd_filter_info_.is_pd_filter_; }
  OB_INLINE bool filter_is_null() const { return pd_filter_info_.is_pd_filter_ && nullptr == pd_filter_info_.filter_; }
  OB_INLINE sql::ObPushdownFilterExecutor *get_pd_filter()
  { return pd_filter_info_.filter_; }
  OB_INLINE sql::PushdownFilterInfo &get_pd_filter_info()
  { return pd_filter_info_; }
  OB_INLINE int reorder_filter()
  {
    return nullptr != where_optimizer_ ?
        where_optimizer_->reorder_row_filter() : OB_SUCCESS;
  }
  OB_INLINE bool disable_bypass() const
  {
    return nullptr != where_optimizer_ ?
        where_optimizer_->is_disable_bypass() : false;
  }
  virtual bool is_end() const { return false; }
  virtual bool is_empty() const { return true; }
  virtual int reuse_for_refresh_table()
  {
    disabled_ = false;
    return OB_SUCCESS;
  }
  VIRTUAL_TO_STRING_KV(K_(is_inited), K_(disabled), K_(is_aggregated_in_prefetch), K_(pd_filter_info));
protected:
  bool is_inited_;
  sql::PushdownFilterInfo pd_filter_info_;
  ObTableAccessContext &context_;
  const ObTableIterParam *iter_param_;
  bool is_aggregated_in_prefetch_;
private:
  bool disabled_;
  ObWhereOptimizer *where_optimizer_;
};

}
}
#endif //OB_STORAGE_BLOCK_ROW_STORE_H_
