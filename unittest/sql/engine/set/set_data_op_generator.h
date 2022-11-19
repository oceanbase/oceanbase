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

#ifndef OCEANBASE_SET_DATA_GENERATOR_OP_H_
#define OCEANBASE_SET_DATA_GENERATOR_OP_H_

#include <functional>
#include "sql/engine/ob_operator.h"
#include "sql/engine/set/ob_hash_set_op.h"
#include "sql/engine/ob_exec_context.h"

#include "lib/container/ob_array.h"
#include "common/row/ob_row_desc.h"
#include "common/rowkey/ob_rowkey.h"
#include "lib/utility/ob_hang_fatal_error.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/basic/ob_hash_partitioning_infrastructure_op.h"

const int LEFT_HASH_SEED = 1000000007;
const int RIGHT_HASH_SEED = 100007;

namespace oceanbase
{
namespace sql
{

class SetDataGeneratorSpec;
class SetDataGeneratorOp;
static const SetDataGeneratorSpec &get_my_spec(const SetDataGeneratorOp &op);

class SetDataGeneratorSpec : public ObOpSpec
{
OB_UNIS_VERSION_V(1);
public:
  SetDataGeneratorSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type) : ObOpSpec(alloc, type)
  {
  }
};

OB_SERIALIZE_MEMBER((SetDataGeneratorSpec, ObOpSpec));

class SetDataGeneratorOp : public ObOperator
{
public:
  SetDataGeneratorOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input, ObString op_id)
                     : ObOperator(exec_ctx, spec, input), op_id_(op_id)
  {
    for (int i = 0; i != 1000; ++i) {
      str_buf_[i] = 's';
    }
  }
  //void set_column_num(const int num) { columns_num_ = num; } 暂定3列

  int test_init()
  {
    int ret = OB_SUCCESS;
    //set_column_count(columns_num_);
    row_id_ = 0;
    return ret;
  }

  void destroy()
  {
    ObOperator::destroy();
  }

  /*int open()
  {
    int ret = OB_SUCCESS;
    opened_ = true;
    return ret;
  }*/

  int convert_row(
    const common::ObIArray<ObExpr *> &src_exprs, const common::ObIArray<ObExpr *> &dst_exprs)
  {
    int ret = OB_SUCCESS;
    if (dst_exprs.count() != src_exprs.count()) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "unexpected status: exprs is not match", K(ret), K(src_exprs.count()),
        K(dst_exprs.count()));
    } else {
      // do nothing
    }
    return ret;
  }
  void init_expr(int64_t init_pos)
  {
    int64_t pos = init_pos;
    if (op_id_ == ObString("RIGHT_OP")) {
      pos += 1000000;
    }
    if (cells_.count() == 0) {
      for (int64_t i = 0; i < columns_num_; ++i) {
        ObExpr *expr = static_cast<ObExpr*>(alloc_.alloc(sizeof(ObExpr)));
        ASSERT_EQ(OB_SUCCESS, cells_.push_back(expr));
        expr->frame_idx_ = 0;
        expr->datum_off_ = pos;
        pos += sizeof(ObDatum) * eval_ctx_.max_batch_size_;
        expr->eval_info_off_  = pos;
        pos += sizeof(ObEvalInfo);
        expr->eval_flags_off_ = pos;
        pos += ObBitVector::memory_size(eval_ctx_.max_batch_size_);
        expr->pvt_skip_off_  = pos;
        expr->batch_result_ = (spec_.batch_size_ > 0);
        expr->batch_idx_mask_ = (expr->batch_result_ ? UINT64_MAX : 0);
        expr->datum_meta_.type_ = ObIntType;
        ObDatum *datums = expr->locate_batch_datums(eval_ctx_);
        for (int64_t j = 0; j < eval_ctx_.max_batch_size_; j++) {
          datums[j].ptr_ = eval_ctx_.frames_[0] + pos;
          pos += (3 == i ? 522 : 8);
        }
      }
    }
    SetDataGeneratorSpec &op_spec = const_cast<SetDataGeneratorSpec &> (get_spec());
    if (op_spec.output_.count() == 0) {
      ASSERT_EQ(OB_SUCCESS, op_spec.output_.init(3));
      for (int64_t i = 0; i < columns_num_; ++i) {
        ObExpr *expr = cells_[i];
        ASSERT_EQ(OB_SUCCESS, op_spec.output_.push_back(expr));
      }
    }
  }

  void gen_row(int64_t row_id)
  {
    ObDatum *expr_datum_0 = &cells_.at(0)->locate_expr_datum(eval_ctx_);
    int id = row_id / 2;
    int key = id * 100 + static_cast<int>(static_cast<int64_t>(hash(id)) % 91);
    expr_datum_0->set_int(key);
    cells_.at(0)->get_eval_info(eval_ctx_).evaluated_ = true;
    /*int64_t max_size = 512;
    if (enable_big_row_ && row_id > 0 && random() % 100000 < 5) {
      max_size = 1 << 20;
    }*/
    ObDatum *expr_datum_1 = &cells_.at(1)->locate_expr_datum(eval_ctx_);
    expr_datum_1->set_null();
    cells_.at(1)->get_eval_info(eval_ctx_).evaluated_ = true;

    int64_t size = 300;
    ObDatum *expr_datum_2 = &cells_.at(2)->locate_expr_datum(eval_ctx_);
    expr_datum_2->set_string(str_buf_, (int)size);
    cells_.at(2)->get_eval_info(eval_ctx_).evaluated_ = true;
  }

  void gen_batch(int64_t &row_id, const int64_t batch_size)
  {
    ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
    guard.set_batch_idx(0);
    ObDatumVector datum0 = cells_.at(0)->locate_expr_datumvector(eval_ctx_);
    ObDatumVector datum1 = cells_.at(1)->locate_expr_datumvector(eval_ctx_);
    ObDatumVector datum2 = cells_.at(2)->locate_expr_datumvector(eval_ctx_);
    for (int64_t i = 0; i < batch_size; ++i) {
      int id = (row_id + i) / 2;
      int key = (id) * 100 + static_cast<int>(static_cast<int64_t>(hash((id))) % 91);
      datum0.at(i).set_int(key);
      cells_.at(0)->get_eval_info(eval_ctx_).evaluated_ = true;

      datum1.at(i).set_null();
      cells_.at(1)->get_eval_info(eval_ctx_).evaluated_ = true;

      int64_t size = 300;
      datum2.at(i).set_string(str_buf_, (int)size);
      cells_.at(2)->get_eval_info(eval_ctx_).evaluated_ = true;
    }
    if (row_id + batch_size <= row_cnt_) {
      brs_.size_ = batch_size;
      row_id += batch_size;
    } else {
      brs_.size_ = row_cnt_ - row_id;
      row_id += brs_.size_;
    }
  }



  uint64_t hash(int64_t value)
  {
    uint64_t seed = 0;
    if (op_id_ == ObString("RIGHT_OP")) {
      seed =static_cast<uint64_t>(RIGHT_HASH_SEED);
    } else {
      seed = static_cast<uint64_t>(LEFT_HASH_SEED);
    }
    uint64_t res = (value * seed);
    return res;
  }
  /*int inner_get_next_row()
  {
    int ret = OB_SUCCESS;
    gen_row(row_cnt_);
    if (row_cnt_ <= 0) {
      return iter_end_ret_;
    } else if (OB_FAIL(convert_row(cells_, MY_SPEC.output_))) { //将cur_expr写入到output中
      OB_LOG(WARN, "copy current row failed", K(ret));
    } else {
      row_cnt_--;
    }
    char *frame = eval_ctx_.frames_[0];
    ObDatum *tmp_datum = (ObDatum *)(frame + 72);
    return ret;
  }*/

  int inner_get_next_row()
  {
    static int add =0;
    int ret = OB_SUCCESS;
    gen_row(row_id_);
    if (row_id_ < 0 || row_id_ >= row_cnt_) {
      return iter_end_ret_;
    } else if (OB_FAIL(convert_row(cells_, MY_SPEC.output_))) { //将cur_expr写入到output中
      OB_LOG(WARN, "copy current row failed", K(ret));
    } else {
      ++add;
      ++row_id_;
    }
    char *frame = eval_ctx_.frames_[0];
    ObDatum *tmp_datum = (ObDatum *)(frame + 72);
    return ret;
  }

  int inner_get_next_batch(const int64_t max_row_cnt)
  {
    int ret = OB_SUCCESS;
    gen_batch(row_id_, max_row_cnt);
    if (row_id_ < 0 || row_id_ >= row_cnt_) {
      brs_.end_ = true;
    }
    return ret;
  }

  const SetDataGeneratorSpec &get_spec() const
  {
    return static_cast<const SetDataGeneratorSpec &>(spec_);
  }

  int64_t row_cnt_ = 1;
  int string_size_ = 64;
  typedef std::function<int64_t(const int64_t, const int64_t)> IdxCntFunc;
  IdxCntFunc idx_cnt_func_ = [](const int64_t, const int64_t) { return 1; };
  typedef std::function<int64_t(const int64_t)> IdValFunc;
  IdValFunc row_id_val_ = [](const int64_t v) { return v; };
  IdValFunc idx_val_ = [](const int64_t v) { return v; };
  int iter_end_ret_ = OB_ITER_END;
  const static int columns_num_=3;


private:
  ObString op_id_;
  int64_t row_id_ = 0;
  ObSEArray<ObExpr *, columns_num_> cells_;
  ObSEArray<ObExpr *, columns_num_> ver_cells_;
  const static int64_t BUF_SIZE = 2 << 20;
  char str_buf_[BUF_SIZE];
  bool enable_big_row_ = false;
  ObArenaAllocator alloc_;
  ObArenaAllocator eval_res_;
  ObArenaAllocator eval_tmp_;
  ObExecContext exec_ctx_;
  //ObEvalCtx eval_ctx_;
};

const static SetDataGeneratorSpec &get_my_spec(const SetDataGeneratorOp &op)
{
  return op.get_spec();
}

} // end sql
} // end oceanbase

#endif // OCEANBASE_SET_DATA_GENERATOR_OP_H_
