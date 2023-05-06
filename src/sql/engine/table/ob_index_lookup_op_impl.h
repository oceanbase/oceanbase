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
    FINISHED
  };
public:
  ObIndexLookupOpImpl(LookupType lookup_type, const int64_t default_batch_row_count);
  virtual ~ObIndexLookupOpImpl() = default;
  virtual int get_next_row();
  virtual int get_next_rows(int64_t &count, int64_t capacity);
  virtual void do_clear_evaluated_flag() = 0;
  virtual int get_next_row_from_index_table() = 0;
  virtual int process_data_table_rowkey() = 0;
  virtual int process_data_table_rowkeys(const int64_t size, const ObBitVector *skip) = 0;
  virtual bool is_group_scan() const = 0;
  virtual int init_group_range(int64_t cur_group_idx, int64_t group_size) = 0;
  virtual int do_index_lookup() = 0;
  virtual int get_next_row_from_data_table() = 0;
  virtual int get_next_rows_from_data_table(int64_t &count, int64_t capacity) = 0;
  virtual int process_next_index_batch_for_row() = 0;
  virtual int process_next_index_batch_for_rows(int64_t &count) = 0;
  virtual bool need_next_index_batch() const = 0;
  virtual int check_lookup_row_cnt() = 0;
  virtual int do_index_table_scan_for_rows(const int64_t max_row_cnt,
                                           const int64_t start_group_idx,
                                           const int64_t default_row_batch_cnt) = 0;
  virtual void update_state_in_output_rows_state(int64_t &count) = 0;
  virtual void update_states_in_finish_state() = 0;
  virtual void update_states_after_finish_state() = 0;
  // The following function distinguishes between the global index back and the local index back.
  // For Local index, it will return 0
  // For Global index, it will return the property
  virtual int64_t get_index_group_cnt() const = 0;
  virtual int64_t get_lookup_group_cnt() const = 0;
  virtual void inc_index_group_cnt() = 0;
  virtual void inc_lookup_group_cnt() = 0;

  virtual ObEvalCtx & get_eval_ctx() = 0;
  virtual const ExprFixedArray & get_output_expr() = 0;
  virtual int switch_index_table_and_rowkey_group_id() { return OB_SUCCESS; }
  int build_trans_datum(ObExpr *expr, ObEvalCtx *eval_ctx, ObIAllocator &alloc, ObDatum *&datum_ptr);

protected:
  LookupType lookup_type_;
  int64_t default_batch_row_count_;
  LookupState state_;         // index lookup state
  bool index_end_;            // if index reach iterator end
  int64_t index_group_cnt_;   // number of groups fetched from index table
  int64_t lookup_group_cnt_;  // number of groups fetched from lookup table
  int64_t lookup_rowkey_cnt_; // number of rows fetched from index table
  int64_t lookup_row_cnt_;
};
} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_INDEX_LOOKUP_OP_IMPL_H_ */
