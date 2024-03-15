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

struct ObDASLookupIterParam : public ObDASIterParam
{
  int64_t default_batch_row_count_;
  bool can_retry_;
  const ObExpr *calc_part_id_;
  const ObDASScanCtDef *lookup_ctdef_;
  ObDASScanRtDef *lookup_rtdef_;
  const ExprFixedArray *rowkey_exprs_;
  ObTableID ref_table_id_;
  ObDASIter *index_table_iter_;
  ObDASIter *data_table_iter_;

  virtual bool is_valid() const override
  {
    return ObDASIterParam::is_valid() &&
        index_table_iter_ != nullptr && data_table_iter_ != nullptr && calc_part_id_ != nullptr &&
        lookup_ctdef_ != nullptr && lookup_rtdef_ != nullptr && rowkey_exprs_ != nullptr;
  }
};

class ObDASLookupIter : public ObDASIter
{
public:
  ObDASLookupIter()
    : calc_part_id_(nullptr),
      lookup_ctdef_(nullptr),
      lookup_rtdef_(nullptr),
      rowkey_exprs_(nullptr),
      index_table_iter_(nullptr),
      data_table_iter_(nullptr),
      lookup_rowkey_cnt_(0),
      lookup_row_cnt_(0),
      can_retry_(false),
      state_(INDEX_SCAN),
      index_end_(false),
      default_batch_row_count_(0),
      iter_alloc_(nullptr)
  {}
  virtual ~ObDASLookupIter() {}

  INHERIT_TO_STRING_KV("ObDASIter", ObDASIter, K_(state), K_(index_end), K(lookup_ctdef_->ref_table_id_));

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
  const ObExpr *calc_part_id_;
  const ObDASScanCtDef *lookup_ctdef_;
  ObDASScanRtDef *lookup_rtdef_;
  const ExprFixedArray *rowkey_exprs_;
  ObDASIter *index_table_iter_;
  ObDASIter *data_table_iter_;
  int64_t lookup_rowkey_cnt_;
  int64_t lookup_row_cnt_;
  bool can_retry_;
  int build_lookup_range(ObNewRange &range);
  int build_trans_info_datum(const ObExpr *trans_info_expr, ObDatum *&datum_ptr);

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
  common::ObArenaAllocator *iter_alloc_;
  char iter_alloc_buf_[sizeof(common::ObArenaAllocator)];
};

class ObDASGlobalLookupIter : public ObDASLookupIter
{
public:
  ObDASGlobalLookupIter()
    : ObDASLookupIter()
  {}
  virtual ~ObDASGlobalLookupIter() {}

protected:
  virtual int add_rowkey() override;
  virtual int add_rowkeys(int64_t count) override;
  virtual int do_index_lookup() override;
  virtual int check_index_lookup() override;
};

}  // namespace sql
}  // namespace oceanbase



#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_LOOKUP_ITER_H_ */
