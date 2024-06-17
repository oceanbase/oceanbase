/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SQL_ENGINE_INDEX_LOOKUP_OP_IMPL_H_
#define OCEANBASE_SQL_ENGINE_INDEX_LOOKUP_OP_IMPL_H_
#include "sql/ob_sql_utils.h"


class ObTableScanOp;
namespace oceanbase
{
namespace sql
{
/*  ObIndexLookupOpImpl is the base class for table scan with index back.
*   It is an abstract class of ObLocalIndexLookupOp and ObGlobalIndexLookupOpImpl.
*   The ObGlobalIndexLookupOpImpl is located in ob_table_scan_op.h
*   The ObLocalIndexLookupOp is located in ob_das_scan_op.h
*/
class ObIndexLookupOpImpl
{
protected:
  enum LookupType : int32_t
  {
    LOCAL_INDEX = 0,
    GLOBAL_INDEX
  };
  enum LookupState : int32_t
  {
    INDEX_SCAN,
    DO_LOOKUP,
    OUTPUT_ROWS,
    FINISHED,
    AUX_LOOKUP
  };
public:
  ObIndexLookupOpImpl(LookupType lookup_type, const int64_t default_batch_row_count);
  virtual ~ObIndexLookupOpImpl() = default;
  virtual int get_next_row();
  virtual int get_next_rows(int64_t &count, int64_t capacity);
  virtual void do_clear_evaluated_flag() = 0;
  virtual int reset_lookup_state() = 0;
  virtual int get_next_row_from_index_table() = 0;
  virtual int get_next_rows_from_index_table(int64_t &count, int64_t capacity) = 0;
  virtual int process_data_table_rowkey() = 0;
  virtual int process_data_table_rowkeys(const int64_t size, const ObBitVector *skip) = 0;
  virtual int do_index_lookup() = 0;
  virtual int get_next_row_from_data_table() = 0;
  virtual int get_next_rows_from_data_table(int64_t &count, int64_t capacity) = 0;
  virtual int check_lookup_row_cnt() = 0;

  virtual ObEvalCtx & get_eval_ctx() = 0;
  virtual const ExprFixedArray & get_output_expr() = 0;
  int build_trans_datum(ObExpr *expr, ObEvalCtx *eval_ctx, ObIAllocator &alloc, ObDatum *&datum_ptr);

protected:
  LookupType lookup_type_;
  int64_t default_batch_row_count_;
  LookupState state_;         // index lookup state
  bool index_end_;            // if index reach iterator end
  int64_t lookup_rowkey_cnt_; // number of rows fetched from index table
  int64_t lookup_row_cnt_;
};
} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_INDEX_LOOKUP_OP_IMPL_H_ */
