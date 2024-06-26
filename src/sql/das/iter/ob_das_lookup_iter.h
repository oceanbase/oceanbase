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

#ifndef OBDEV_SRC_SQL_DAS_ITER_OB_DAS_LOOKUP_ITER_H_
#define OBDEV_SRC_SQL_DAS_ITER_OB_DAS_LOOKUP_ITER_H_

#include "sql/das/iter/ob_das_iter.h"
#include "lib/utility/ob_tracepoint.h"
namespace oceanbase
{
using namespace common;
namespace sql
{

class ObDASBaseCtDef;
class ObDASBaseRtDef;
class ObDASScanCtDef;
class ObDASScanRtDef;
struct ObDASLookupIterParam : public ObDASIterParam
{
public:
  ObDASLookupIterParam(bool is_global_index)
    : ObDASIterParam(is_global_index ? DAS_ITER_GLOBAL_LOOKUP : DAS_ITER_LOCAL_LOOKUP),
      default_batch_row_count_(0),
      index_ctdef_(nullptr),
      index_rtdef_(nullptr),
      lookup_ctdef_(nullptr),
      lookup_rtdef_(nullptr),
      index_table_iter_(nullptr),
      data_table_iter_(nullptr),
      rowkey_exprs_(nullptr)
  {}
  int64_t default_batch_row_count_;
  const ObDASBaseCtDef *index_ctdef_;
  ObDASBaseRtDef *index_rtdef_;
  const ObDASScanCtDef *lookup_ctdef_;
  ObDASScanRtDef *lookup_rtdef_;
  ObDASIter *index_table_iter_;
  ObDASIter *data_table_iter_;
  const ExprFixedArray *rowkey_exprs_;

  virtual bool is_valid() const override
  {
    return ObDASIterParam::is_valid()
        && index_table_iter_ != nullptr && data_table_iter_ != nullptr
        && index_ctdef_ != nullptr && index_rtdef_ != nullptr
        && lookup_ctdef_ != nullptr && lookup_rtdef_ != nullptr && rowkey_exprs_ != nullptr;
  }
};

class ObDASLookupIter : public ObDASIter
{
public:
  ObDASLookupIter(const ObDASIterType type = ObDASIterType::DAS_ITER_INVALID)
    : ObDASIter(type),
      index_ctdef_(nullptr),
      index_rtdef_(nullptr),
      lookup_ctdef_(nullptr),
      lookup_rtdef_(nullptr),
      rowkey_exprs_(),
      index_table_iter_(nullptr),
      data_table_iter_(nullptr),
      lookup_rowkey_cnt_(0),
      lookup_row_cnt_(0),
      state_(INDEX_SCAN),
      index_end_(false),
      default_batch_row_count_(0),
      lookup_memctx_()
  {}
  virtual ~ObDASLookupIter() {}

  INHERIT_TO_STRING_KV("ObDASIter", ObDASIter, K_(state), K_(index_end), K_(default_batch_row_count));

protected:
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override;
  virtual int inner_release() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;
  virtual void reset_lookup_state();
  virtual int add_rowkey() = 0;
  virtual int add_rowkeys(int64_t count) = 0;
  virtual int do_index_lookup() = 0;
  virtual int check_index_lookup() = 0;

protected:
  const ObDASBaseCtDef *index_ctdef_;
  ObDASBaseRtDef *index_rtdef_;
  const ObDASScanCtDef *lookup_ctdef_;
  ObDASScanRtDef *lookup_rtdef_;
  common::ObSEArray<ObExpr*, 2>  rowkey_exprs_;
  ObDASIter *index_table_iter_;
  ObDASIter *data_table_iter_;
  int64_t lookup_rowkey_cnt_;
  int64_t lookup_row_cnt_;
  int build_lookup_range(ObNewRange &range);
  int build_trans_info_datum(const ObExpr *trans_info_expr, ObDatum *&datum_ptr);
  common::ObArenaAllocator &get_arena_allocator() { return lookup_memctx_->get_arena_allocator(); }

private:
  enum LookupState : uint32_t
  {
    INDEX_SCAN,
    DO_LOOKUP,
    OUTPUT_ROWS,
    FINISHED
  };

  LookupState state_;
  bool index_end_;
  int64_t default_batch_row_count_;
  lib::MemoryContext lookup_memctx_;
};

}  // namespace sql
}  // namespace oceanbase

#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_LOOKUP_ITER_H_ */
