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

#ifndef OCEANBASE_JOIN_JOIN_DATA_GENERATOR_H_
#define OCEANBASE_JOIN_JOIN_DATA_GENERATOR_H_

#include <functional>
#include "sql/engine/ob_phy_operator.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
namespace sql
{
class JoinDataGenerator : public ObPhyOperator
{
public:
  const static int64_t CELL_CNT = 4;

  const static int64_t ROW_ID_CELL = 0;
  const static int64_t IDX_CELL = 1;
  const static int64_t RAND_CELL = 2;
  const static int64_t STR_CELL = 3;

  const static int64_t INT_VARCHAR_BUF_SIZE = 10;

  class Ctx : public ObPhyOperatorCtx
  {
  public:
    Ctx(ObExecContext &c) : ObPhyOperatorCtx(c) {}
    virtual void destroy() { return ObPhyOperatorCtx::destroy_base(); }
  };

  explicit JoinDataGenerator(common::ObIAllocator &alloc) : ObPhyOperator(alloc)
  {
    type_ = PHY_TABLE_SCAN;
  }

  virtual int set_child(int32_t, ObPhyOperator &) { return common::OB_SUCCESS; }
  virtual ObPhyOperator *get_child(int32_t) const { return NULL; }
  virtual int32_t get_child_num() const { return 0; }
  virtual int inner_get_next_row(ObExecContext &, const common::ObNewRow *&) const { return common::OB_SUCCESS; }
  int close(ObExecContext &) const { return common::OB_SUCCESS; }

  ~JoinDataGenerator()
  {
    if (str_) {
      delete []str_;
      str_ = NULL;
    }
    if (projector_) {
      delete []projector_;
      projector_ = NULL;
    }
  }

  // row_cnt_ idx_cnt_func_ must set before init
  int test_init()
  {
    int ret = OB_SUCCESS;
	if (NULL == projector_) {
		projector_ = new int32_t[CELL_CNT];
		projector_size_ = CELL_CNT;
		for (int i = 0; i < CELL_CNT; i++) {
			projector_[i] = i;
		}
	}
    set_column_count(CELL_CNT);

	row_id_ = 0;
    if (reverse_) {
      row_id_ = row_cnt_ - 1;
    }
    str_ = new char[string_size_];
    memset(str_, 'x', string_size_);
    row_.cells_ = cells_;
    row_.count_ = CELL_CNT;
    row_.projector_size_ = projector_size_;
    row_.projector_ = projector_;
    return ret;
  }

  virtual int init_op_ctx(ObExecContext &ec) const override
  {
    Ctx *ctx = NULL;
    return CREATE_PHY_OPERATOR_CTX(Ctx, ec, get_id(), get_type(), ctx);
  }

  virtual int inner_open(ObExecContext &ec) const override
  {
    return init_op_ctx(ec);
  }

  void next_row_id() const
  {
    row_id_ += !reverse_ ? 1 : -1;
    idx_ = 0;
    idx_cnt_ = 0;
  }

  virtual int get_next_row(ObExecContext &, const common::ObNewRow *&row) const override
  {
    while (true) {
      if (row_id_ < 0 || row_id_ >= row_cnt_) {
        row = NULL;
        return iter_end_ret_;
      }
      if (idx_cnt_ == 0) {
        idx_cnt_ = idx_cnt_func_(row_id_, row_cnt_);
      }
      if (idx_ < idx_cnt_) {
        construct_row();
        row = &row_;
        idx_++;
        return OB_SUCCESS;
      }
      if (idx_ >= idx_cnt_) {
        next_row_id();
      }
    }
    return OB_SUCCESS;
  }

  void construct_row() const
  {

    int64_t rand = murmurhash64A(&row_id_, sizeof(row_id_), 0);
    rand = murmurhash64A(&idx_, sizeof(idx_), (uint64_t)rand);
    ObObj *objs[] = { &cells_[ROW_ID_CELL], &cells_[IDX_CELL], &cells_[RAND_CELL] };
    int64_t ints[] = { row_id_val_(row_id_), idx_val_(idx_), rand };
    if (!gen_varchar_cell_) {
      for (int64_t i = 0; i < ARRAYSIZEOF(objs); i++) {
        objs[i]->set_int(ints[i]);
      }
    } else {
      char *bufs[] = {
        &int_varchar_buf_[ROW_ID_CELL * INT_VARCHAR_BUF_SIZE],
        &int_varchar_buf_[IDX_CELL * INT_VARCHAR_BUF_SIZE],
        &int_varchar_buf_[RAND_CELL * INT_VARCHAR_BUF_SIZE]
      };

      for (int64_t i = 0; i < ARRAYSIZEOF(objs); i++) {
        // '0' flag ignored with precision and ‘%d’ gnu_printf format
        snprintf(bufs[i], INT_VARCHAR_BUF_SIZE, "%.*ld", (int)INT_VARCHAR_BUF_SIZE, ints[i]);
        objs[i]->set_varchar(bufs[i], INT_VARCHAR_BUF_SIZE);
      }
    }
    cells_[STR_CELL].set_varchar(str_, string_size_);
  }

public:
  bool gen_varchar_cell_ = false;
  int64_t row_cnt_ = 1;
  bool reverse_ = false;
  int string_size_ = 64;
  typedef std::function<int64_t(const int64_t, const int64_t)> IdxCntFunc;
  IdxCntFunc idx_cnt_func_ = [](const int64_t, const int64_t) { return 1; };
  typedef std::function<int64_t(const int64_t)> IdValFunc;
  IdValFunc row_id_val_ = [](const int64_t v) { return v; };
  IdValFunc idx_val_ = [](const int64_t v) { return v; };
  int iter_end_ret_ = OB_ITER_END;

private:
  mutable common::ObNewRow row_;
  mutable common::ObObj cells_[CELL_CNT];
  mutable int64_t row_id_ = 0;
  mutable int64_t idx_ = 0;
  mutable int64_t idx_cnt_ = 0;
  mutable char *str_ = NULL;
  mutable char int_varchar_buf_[CELL_CNT * INT_VARCHAR_BUF_SIZE];
};

} // end sql
} // end oceanbase

#endif // OCEANBASE_JOIN_JOIN_DATA_GENERATOR_H_
