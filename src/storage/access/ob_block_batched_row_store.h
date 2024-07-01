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

#ifndef OB_STORAGE_OB_BLOCK_BATCHED_ROW_STORE_H_
#define OB_STORAGE_OB_BLOCK_BATCHED_ROW_STORE_H_
#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/container/ob_bitmap.h"
#include "share/schema/ob_table_param.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "ob_block_row_store.h"

namespace oceanbase
{
namespace common
{
  class ObDatum;
}
namespace sql
{
class ObEvalCtx;
}
namespace blocksstable
{
struct ObDatumRow;
class ObIMicroBlockReader;
}
namespace storage
{
class ObBlockBatchedRowStore : public ObBlockRowStore {
public:
  enum IterEndState
  {
    PROCESSING = 0x00,
    // limit/offset end
    LIMIT_ITER_END = 0x01,
    ITER_END = 0x02,
  };

  ObBlockBatchedRowStore(
      const int64_t batch_size,
      sql::ObEvalCtx &eval_ctx,
      ObTableAccessContext &context);
  virtual ~ObBlockBatchedRowStore();
  virtual void reset() override;
  virtual int init(const ObTableAccessParam &param) override;
  virtual int fill_row(blocksstable::ObDatumRow &out_row) = 0;
  virtual int fill_rows(
      const int64_t group_idx,
      blocksstable::ObIMicroBlockRowScanner *scanner,
      int64_t &begin_index,
      const int64_t end_index,
      const ObFilterResult &res) = 0;
  virtual int fill_rows(const int64_t group_idx, const int64_t row_count) = 0;
  virtual int reuse_capacity(const int64_t capacity);

  virtual bool is_end() const override final
  { return iter_end_flag_ != IterEndState::PROCESSING; }
  virtual void set_end() { iter_end_flag_ = ITER_END; }
  virtual void set_limit_end() { iter_end_flag_ = LIMIT_ITER_END; }
  OB_INLINE int64_t get_batch_size() { return batch_size_; }
  OB_INLINE int64_t get_row_capacity() const { return row_capacity_; }
  INHERIT_TO_STRING_KV("ObBlockRowStore", ObBlockRowStore, K_(iter_end_flag), K_(batch_size), K_(row_capacity));
protected:
  int get_row_ids(
      blocksstable::ObIMicroBlockReader *reader,
      int64_t &begin_index,
      const int64_t end_index,
      int64_t &row_count,
      const bool can_limit,
      const ObFilterResult &res);
  IterEndState iter_end_flag_;
  int64_t batch_size_;
  int64_t row_capacity_;
  const char **cell_data_ptrs_;
  int32_t *row_ids_;
  uint32_t *len_array_;
  sql::ObEvalCtx &eval_ctx_;
};

} /* namespace storage */
} /* namespace oceanbase */

#endif //OB_STORAGE_OB_BLOCK_BATCHED_ROW_STORE_H_
