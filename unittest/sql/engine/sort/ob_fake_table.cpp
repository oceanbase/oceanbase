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

#include "ob_fake_table.h"
#include "lib/utility/utility.h"
#include "lib/allocator/ob_malloc.h"
#include "sql/engine/ob_exec_context.h"
//using namespace oceanbase::sql;
//using namespace oceanbase::sql::test;
//using namespace oceanbase::common;

namespace oceanbase
{
using namespace common;
namespace sql
{
namespace test
{
ObFakeTable::ObFakeTable()
    : ObPhyOperator(alloc_), row_count_(0), n_segments_local_merge_sort_(0)
{
  set_column_count(COLUMN_COUNT);
}

ObFakeTable::~ObFakeTable()
{
}

void ObFakeTable::set_row_count(const int64_t count)
{
  row_count_ = count;
}

ObPhyOperator *ObFakeTable::get_child(int32_t child_idx) const
{
  UNUSED(child_idx);
  return NULL;
}

int32_t ObFakeTable::get_child_num() const
{
  return 0;
}

int ObFakeTable::set_child(int32_t child_idx, ObPhyOperator &child_operator)
{
  UNUSED(child_idx);
  UNUSED(child_operator);
  return OB_SUCCESS;
}

int ObFakeTable::inner_open(ObExecContext &exec_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = init_op_ctx(exec_ctx))) {
    _OB_LOG(WARN, "failed to create fake table ctx, ret=%d", ret);
  }
  return ret;
}

int ObFakeTable::inner_create_operator_ctx(ObExecContext &exec_ctx, ObPhyOperatorCtx *&op_ctx) const
{
  int ret = OB_SUCCESS;
  ObFakeTableCtx *table_ctx = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObFakeTableCtx, exec_ctx, get_id(), get_type(), table_ctx))) {
    _OB_LOG(WARN, "failed to create fake table ctx, ret=%d", ret);
  } else {
    op_ctx = table_ctx;
  }
  return ret;
}

int ObFakeTable::init_op_ctx(ObExecContext &ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx *op_ctx = NULL;
  if (OB_SUCCESS != (ret = inner_create_operator_ctx(ctx, op_ctx))) {
    _OB_LOG(WARN, "create operator context failed, ret=%d", ret);
  } else if (OB_SUCCESS != (ret = op_ctx->create_cur_row(get_column_count(), projector_, projector_size_))) {
    _OB_LOG(WARN, "create cur_row failed, ret=%d", ret);
  }
  return ret;
}


int ObFakeTable::inner_get_next_row(ObExecContext &exec_ctx, const ObNewRow *&row) const
{
  int ret = OB_SUCCESS;
  ObFakeTableCtx *table_ctx = NULL;
  if (NULL == (table_ctx = GET_PHY_OPERATOR_CTX(ObFakeTableCtx, exec_ctx, get_id()))) {
    _OB_LOG(WARN, "failed to get table ctx, ret=%d", ret);
  } else if (table_ctx->get_count_ < row_count_) {
    if (n_segments_local_merge_sort_ <= 0) {
      if (OB_SUCCESS != (ret = cons_cur_row(table_ctx, table_ctx->get_count_))) {
        _OB_LOG(WARN, "failed to cons current row, err=%d", ret);
      } else {
        row = &table_ctx->cur_row_;
        ++table_ctx->get_count_;
      }
    } else {
      int64_t count_per = row_count_ / n_segments_local_merge_sort_;
      int64_t get_count = table_ctx->get_count_ % count_per;
      if (table_ctx->get_count_ >= (n_segments_local_merge_sort_ - 1) * count_per) {
        get_count = table_ctx->get_count_ - (n_segments_local_merge_sort_ - 1) * count_per;
      }
      if (OB_SUCCESS != (ret = cons_cur_row(table_ctx, get_count))) {
        _OB_LOG(WARN, "failed to cons current row, err=%d", ret);
      } else {
        row = &table_ctx->cur_row_;
        ++table_ctx->get_count_;
      }
    }
  } else {
    _OB_LOG(INFO, "end of table");
    ret = OB_ITER_END;
  }
  return ret;
}
/***************************************************************************************************
  c0       | c1      | c2        | c3        | c4        | c5         | c6    | c7   |
-----------------------------------------------------------------------------------------------------
  rand str | row_idx | row_idx%2 | row_idx%3 | row_idx/2 | row_idx/3  | c1+c2 |c3+c4 |
***************************************************************************************************/
int ObFakeTable::cons_cur_row(ObFakeTableCtx *table_ctx, const int64_t row_idx) const
{
  int ret = OB_SUCCESS;
  int64_t c2_val = 0, c3_val = 0, c4_val = 0, c5_val = 0, c6_val = 0;
  ObObj cell;
  if (OB_SUCCESS != (ret = cons_varchar_cell(table_ctx, cell))) {
    _OB_LOG(WARN, "failed to cons varchar cell");
  } else {
    // column 0: varchar
    table_ctx->cur_row_.cells_[COL0_RAND_STR] = cell;
    // column 1: int, row_idx
    cell.set_int(row_idx);
    table_ctx->cur_row_.cells_[COL1_ROW_ID] = cell;
    // column 2: int, row_idx % 2
    cell.set_int(c2_val = row_idx % 2);
    table_ctx->cur_row_.cells_[COL2_ROW_ID_MOD_2] = cell;
    // column 3: int, row_idx % 3
    cell.set_int(c3_val = row_idx % 3);
    table_ctx->cur_row_.cells_[COL3_ROW_ID_MOD_3] = cell;
    // column 4 int, row_idx / 2, e.g. 0,0,1,1,2,2,3,3,...
    cell.set_int(c4_val = row_idx / 2);
    table_ctx->cur_row_.cells_[COL4_ROW_ID_DIV_2] = cell;
    // column 5 int, row_idx / 3, e.g. 0,0,0,1,1,1,2,2,2,...
    cell.set_int(c5_val = row_idx / 3);
    table_ctx->cur_row_.cells_[COL5_ROW_ID_DIV_3] = cell;
    // column 6 int, c2+c3
    cell.set_int(c6_val = c2_val + c3_val);
    table_ctx->cur_row_.cells_[COL6_SUM_COL2_COL3] = cell;
    // column 7 int, c4+c5
    cell.set_int(c6_val = c4_val + c5_val);
    table_ctx->cur_row_.cells_[COL7_SUM_COL5_COL5] = cell;
    // column 8: column with null values, value is null for each even row
    (0 == row_idx % 2) ? cell.set_null() : cell.set_int(row_idx);
    table_ctx->cur_row_.cells_[COL8_ROW_ID_OR_NULL] = cell;
    // column 9 int, row_idx/2*2, e.g. 0,0,2,2,4,4,6,6,...
    cell.set_int(row_idx / 2 * 2);
    table_ctx->cur_row_.cells_[COL9_ROW_ID_DIV_2_MULTIPLY_2] = cell;
    // column 10 int, row_idx/3*3, e.g. 0,0,0,3,3,3,6,6,6,...
    cell.set_int(row_idx / 3 * 3);
    table_ctx->cur_row_.cells_[COL10_ROW_ID_DIV_3_MULTIPLY_3] = cell;
    // column 11 int, row_index_ * 3 / row_count_
    cell.set_int(row_idx * 3 / row_count_);
    table_ctx->cur_row_.cells_[COL11_ROW_ID_MULTIPLY_3_DIV_COUNT] = cell;
    // column 12-15: int, random data
    for (int64_t cell_idx = COL12_RAND_INT; cell_idx < COLUMN_COUNT; ++cell_idx) {
      if (OB_SUCCESS != (ret = cons_random_int_cell(cell))) {
        break;
      } else {
        table_ctx->cur_row_.cells_[cell_idx] = cell;
      }
    }
  }
  return ret;
}

#define A_Z_CNT   (26)
#define GER_CNT   (4)   // ä, ö, ü, ß.
int ObFakeTable::cons_varchar_cell(ObFakeTableCtx *table_ctx, ObObj &cell) const
{
  int ret = OB_SUCCESS;
  int charnum = rand_int(ObFakeTableCtx::VARCHAR_CELL_BUF_SIZE - 5) + 4;
  for (int i = 0; i < charnum; ++i) {
    int rand = (i < charnum - 1) ? rand_int(A_Z_CNT + 2 * GER_CNT) : rand_int(A_Z_CNT);
    if (rand < A_Z_CNT) {
      table_ctx->buf_[i] = (char)('A' + rand);
    } else {
      table_ctx->buf_[i++] = (char)(0xC3);
      switch (rand) {
        case 26:
        case 27:
          table_ctx->buf_[i] = (char)(0xA4); break; // ä
        case 28:
        case 29:
          table_ctx->buf_[i] = (char)(0xB6); break; // ö
        case 30:
        case 31:
          table_ctx->buf_[i] = (char)(0xBC); break; // ü
        case 32:
        case 33:
          table_ctx->buf_[i] = (char)(0x9F); break; // ß
        default: break;
      }

    }
  }
  table_ctx->buf_[charnum] = 0;
  ObString varchar;
  varchar.assign_ptr(table_ctx->buf_, charnum);
  cell.set_varchar(varchar);
  cell.set_collation_type(common::CS_TYPE_UTF8MB4_BIN);
//  printf("**** rand varchar: %s\n", table_ctx->buf_);
  return ret;
}

int ObFakeTable::cons_random_int_cell(ObObj &cell) const
{
  int ret = OB_SUCCESS;
  cell.set_int(rand());
  return ret;
}

inline int ObFakeTable::rand_int(int max) const
{
  double fmax = max;
  int j = (int) (fmax * (rand() / (RAND_MAX + 1.0)));
  return j;
}

int64_t ObFakeTable::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "FakeTableForTesting\n");
  return pos;
}
} // end namespace test
} // end namespace sql
} // end namespace oceanbase
