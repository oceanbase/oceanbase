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
#include "storage/ob_table_store_stat_mgr.h"

namespace oceanbase
{

namespace sql
{
class ObPushdownFilterExecutor;
class ObBlackFilterExecutor;
}
namespace blocksstable
{
class ObIMicroBlockRowScanner;
class ObMicroBlockDecoder;
class ObStorageDatum;
}
namespace storage
{
struct ObTableAccessContext;
struct ObTableAccessParam;
struct ObTableIterParam;
struct ObStoreRow;
struct PushdownFilterInfo
{
  PushdownFilterInfo() :
      is_pd_filter_(false),
      start_(-1),
      end_(-1),
      col_capacity_(0),
      col_buf_(nullptr),
      datum_buf_(nullptr),
      filter_(nullptr) {}
  bool is_pd_filter_;
  int64_t start_;
  int64_t end_;
  int64_t col_capacity_;
  // TODO remove col_buf_ later
  common::ObObj *col_buf_;
  blocksstable::ObStorageDatum *datum_buf_;
  sql::ObPushdownFilterExecutor *filter_;
};

class ObBlockRowStore
{
public:
  ObBlockRowStore(ObTableAccessContext &context);
  virtual ~ObBlockRowStore();
  virtual void reset();
  virtual void reuse();
  virtual int init(const ObTableAccessParam &param);
  int open();
  OB_INLINE bool is_valid() const { return is_inited_; }
  OB_INLINE bool is_disabled() const { return disabled_; }
  OB_INLINE void disable() { disabled_ = true; }
  // for blockscan
  OB_INLINE void reset_blockscan() { can_blockscan_ = false; filter_applied_ = false; }
  OB_INLINE bool can_blockscan() const { return can_blockscan_; }
  OB_INLINE bool filter_applied() const { return filter_applied_; }
  OB_INLINE bool filter_is_null() const { return pd_filter_info_.is_pd_filter_ && nullptr == pd_filter_info_.filter_; }
  int apply_blockscan(
      blocksstable::ObIMicroBlockRowScanner &micro_scanner,
      const int64_t row_count,
      const bool can_pushdown,
      ObTableStoreStat &table_store_stat);
  int get_result_bitmap(const common::ObBitmap *&bitmap);
  virtual bool is_end() const { return false; }
  virtual bool is_empty() const { return true; }
  virtual int filter_micro_block_batch(
      blocksstable::ObMicroBlockDecoder &block_reader,
      sql::ObPushdownFilterExecutor *parent,
      sql::ObBlackFilterExecutor &filter,
      common::ObBitmap &result_bitmap)
  {
    UNUSEDx(block_reader, parent, filter, result_bitmap);
    return common::OB_NOT_SUPPORTED;
  }
  VIRTUAL_TO_STRING_KV(K_(is_inited),  K_(can_blockscan), K_(filter_applied), K_(disabled));
protected:
  int filter_micro_block(
      const int64_t row_count,
      blocksstable::ObIMicroBlockRowScanner &micro_scanner,
      sql::ObPushdownFilterExecutor *parent,
      sql::ObPushdownFilterExecutor *filter);
  bool is_inited_;
  PushdownFilterInfo pd_filter_info_;
  ObTableAccessContext &context_;
private:
  bool can_blockscan_;
  bool filter_applied_;
  bool disabled_;
};

}
}
#endif //OB_STORAGE_BLOCK_ROW_STORE_H_
