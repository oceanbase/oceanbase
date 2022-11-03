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

#ifndef _OB_FAKE_TABLE_H
#define _OB_FAKE_TABLE_H 1
#include "sql/engine/ob_phy_operator.h"
#include "lib/container/ob_array.h"
#include "lib/allocator/page_arena.h"
namespace oceanbase
{
namespace sql
{
namespace test
{
static common::ObArenaAllocator alloc_;
// fake table to feed input for testing
class ObFakeTable: public ObPhyOperator
{
public:
  const static int64_t COL0_RAND_STR = 0;
  const static int64_t COL1_ROW_ID = 1;
  const static int64_t COL2_ROW_ID_MOD_2 = 2;
  const static int64_t COL3_ROW_ID_MOD_3 = 3;
  const static int64_t COL4_ROW_ID_DIV_2 = 4;
  const static int64_t COL5_ROW_ID_DIV_3 = 5;
  const static int64_t COL6_SUM_COL2_COL3 = 6;
  const static int64_t COL7_SUM_COL5_COL5 = 7;
  const static int64_t COL8_ROW_ID_OR_NULL = 8;
  const static int64_t COL9_ROW_ID_DIV_2_MULTIPLY_2 = 9;
  const static int64_t COL10_ROW_ID_DIV_3_MULTIPLY_3 = 10;
  const static int64_t COL11_ROW_ID_MULTIPLY_3_DIV_COUNT = 11;
  const static int64_t COL12_RAND_INT = 12;
  const static int64_t COL13_RAND_INT = 13;
  const static int64_t COL14_RAND_INT = 14;
  const static int64_t COL15_RAND_INT = 15;
protected:
  class ObFakeTableCtx: public ObPhyOperatorCtx
  {
    friend class ObFakeTable;
  public:
    ObFakeTableCtx(ObExecContext &ctx)
        : ObPhyOperatorCtx(ctx),
          get_count_(0)
    {
      buf_[0] = '\0';
    }
    virtual ~ObFakeTableCtx() {}
    virtual void destroy() { return ObPhyOperatorCtx::destroy_base(); }
  private:
    static const int VARCHAR_CELL_BUF_SIZE = 32;
  private:
    int64_t get_count_;
    char buf_[VARCHAR_CELL_BUF_SIZE];
  };

public:
  ObFakeTable();
  virtual ~ObFakeTable();
  virtual ObPhyOperatorType get_type() const { return PHY_INVALID; }
  void set_row_count(const int64_t count);
  void reset() {}
  void reuse() {}

  virtual ObPhyOperator *get_child(int32_t child_idx) const;
  virtual int32_t get_child_num() const;
  virtual int set_child(int32_t child_idx, ObPhyOperator &child_operator);
  virtual int inner_open(ObExecContext &exec_ctx) const;
  virtual int64_t to_string(char* buf, const int64_t buf_len) const;
  virtual void set_segments_local_merge_sort(int64_t n_segments_local_merge_sort) { n_segments_local_merge_sort_ = n_segments_local_merge_sort; }
protected:
  virtual int inner_get_next_row(ObExecContext &exec_ctx, const common::ObNewRow *&row) const;
  virtual int init_op_ctx(ObExecContext &ctx) const;
private:
  // types and constants
  //static const int COLUMN_COUNT = 16;
  static const int COLUMN_COUNT = 32;
private:
  // disallow copy
  ObFakeTable(const ObFakeTable &other);
  ObFakeTable& operator=(const ObFakeTable &other);
  // function members
  int cons_cur_row(ObFakeTableCtx *table_ctx, const int64_t row_idx) const;
  int cons_varchar_cell(ObFakeTableCtx *table_ctx, common::ObObj &cell) const;
  int cons_random_int_cell(common::ObObj &cell) const;
  int rand_int(int max) const;
  virtual int inner_create_operator_ctx(ObExecContext &exec_ctx, ObPhyOperatorCtx *&op_ctx) const;

protected:
  int64_t row_count_;
  int64_t n_segments_local_merge_sort_;
};
} // end namespace test
} // end namespace sql
} // end namespace oceanbase

#endif /* _OB_FAKE_TABLE_H */
