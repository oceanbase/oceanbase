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

#define USING_LOG_PREFIX SQL_ENG
#include <gtest/gtest.h>
#define private public
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "sql/engine/expr/ob_expr_column_conv.h"
#include "lib/utility/utility.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
using namespace common;
using namespace sql;

namespace sql
{

static const int64_t BUF_SIZE = 1024 * 1024; // 1MB buffer

typedef int (*EvalFunc) (const ObExpr &expr,
  ObEvalCtx &ctx,
  ObDatum &expr_datum);
typedef int (*EvalBatchFunc) (const ObExpr &expr,
       ObEvalCtx &ctx,
       const ObBitVector &skip,
       const int64_t size);
typedef int (*EvalVectorFunc) (const ObExpr &expr,
        ObEvalCtx &ctx,
        const ObBitVector &skip,
        const EvalBound &bound);
typedef int (*EvalEnumSetFunc) (const ObExpr &expr,
         const common::ObIArray<common::ObString> &str_values,
         const uint64_t cast_mode,
         ObEvalCtx &ctx,
         ObDatum &expr_datum);
struct ObExpr42x
{
  OB_UNIS_VERSION(2);

  uint64_t magic_{0x12345678};
  ObExprOperatorType type_{T_INT};
  ObDatumMeta datum_meta_{};
  common::ObObjMeta obj_meta_{};
  int32_t max_length_{34234};
  common::ObObjDatumMapType obj_datum_map_{};
  union {
    struct {
      uint64_t batch_result_:1;
      uint64_t is_called_in_sql_:1;
      uint64_t is_static_const_:1;
      uint64_t is_boolean_:1;
      uint64_t is_dynamic_const_:1;
      uint64_t need_stack_check_:1;
    };
    uint64_t flag_{3423354};
  };
  union {
    EvalFunc eval_func_{nullptr};
    sql::serializable_function ser_eval_func_;
  };
  union {
    EvalBatchFunc eval_batch_func_{nullptr};
    sql::ser_eval_batch_function ser_eval_batch_func_;
  };
  union {
    void **inner_functions_{nullptr};
    sql::serializable_function *ser_inner_functions_;
  };
  uint32_t inner_func_cnt_{0};
  ObExpr **args_{nullptr};
  uint32_t arg_cnt_{0};
  ObExpr **parents_{nullptr};
  uint32_t parent_cnt_{0};
  uint32_t frame_idx_{1};
  uint32_t datum_off_{9};
  uint32_t eval_info_off_{7};
  uint32_t dyn_buf_header_offset_{8};
  uint32_t res_buf_off_{2};
  uint32_t res_buf_len_{3};
  uint32_t eval_flags_off_{4};
  uint32_t pvt_skip_off_{5};
  uint32_t expr_ctx_id_{6};
  union {
    uint64_t extra_{10};
    struct {
      int64_t div_calc_scale_          :   16;
      int64_t is_error_div_by_zero_    :    1;
      int64_t reserved_                :   47;
    };
  };
  ObExprBasicFuncs *basic_funcs_{nullptr};
  uint64_t batch_idx_mask_{11};
  ObIExprExtraInfo *extra_info_{nullptr};
  int64_t local_session_var_id_{12};
};

OB_DEF_SERIALIZE(ObExpr42x)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              type_,
              datum_meta_,
              obj_meta_,
              max_length_,
              obj_datum_map_,
              ser_eval_func_,
              serialization::make_ser_carray(ser_inner_functions_, inner_func_cnt_),
              serialization::make_ser_carray(args_, arg_cnt_),
              serialization::make_ser_carray(parents_, parent_cnt_),
              frame_idx_,
              datum_off_,
              res_buf_off_,
              res_buf_len_,
              expr_ctx_id_,
              extra_);

  LST_DO_CODE(OB_UNIS_ENCODE,
              eval_info_off_,
              flag_,
              ser_eval_batch_func_,
              eval_flags_off_,
              pvt_skip_off_);

  if (OB_SUCC(ret)) {
    ObExprOperatorType type = T_INVALID;
    if (nullptr != extra_info_) {
      type = extra_info_->type_;
    }
    // Add a type before extra_info to determine whether extra_info is empty
    OB_UNIS_ENCODE(type)
    if (OB_FAIL(ret)) {
    } else if (T_INVALID != type) {
      OB_UNIS_ENCODE(*extra_info_);
    }
    OB_UNIS_ENCODE(dyn_buf_header_offset_)
    OB_UNIS_ENCODE(local_session_var_id_);
  }

  return ret;
}

int ObExpr42x::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t version = 0;
  int64_t len = 0;
  OB_UNIS_DECODE(version);
  OB_UNIS_DECODE(len);
  LST_DO_CODE(OB_UNIS_DECODE,
              type_,
              datum_meta_,
              obj_meta_,
              max_length_,
              obj_datum_map_,
              ser_eval_func_,
              serialization::make_ser_carray(ser_inner_functions_, inner_func_cnt_),
              serialization::make_ser_carray(args_, arg_cnt_),
              serialization::make_ser_carray(parents_, parent_cnt_),
              frame_idx_,
              datum_off_,
              res_buf_off_,
              res_buf_len_,
              expr_ctx_id_,
              extra_);

  LST_DO_CODE(OB_UNIS_DECODE,
              eval_info_off_,
              flag_,
              ser_eval_batch_func_,
              eval_flags_off_,
              pvt_skip_off_);
  if (0 == eval_info_off_ && OB_SUCC(ret)) {
    // compatible with 3.0, ObExprDatum::flag_ is ObEvalInfo
    eval_info_off_ = datum_off_ + sizeof(ObDatum);
  }

  if (OB_SUCC(ret)) {
    ObExprOperatorType type = T_INVALID;
    // Add a type before extra_info to determine whether extra_info is empty
    OB_UNIS_DECODE(type);
    if (OB_FAIL(ret)) {
    } else if (T_INVALID != type) {
      OZ (ObExprExtraInfoFactory::alloc(CURRENT_CONTEXT->get_arena_allocator(),
                                        type, extra_info_));
      CK (OB_NOT_NULL(extra_info_));
      OB_UNIS_DECODE(*extra_info_);
    }
  }

  if (OB_SUCC(ret)) {
    basic_funcs_ = ObDatumFuncs::get_basic_func(datum_meta_.type_, datum_meta_.cs_type_, datum_meta_.scale_,
                                                lib::is_oracle_mode(), obj_meta_.has_lob_header());
    CK(NULL != basic_funcs_);
  }
  if (batch_result_) {
    batch_idx_mask_ = UINT64_MAX;
  }
  OB_UNIS_DECODE(dyn_buf_header_offset_);
  if (version == 3) {
    uint32_t vector_header_off = 0;
    uint32_t offset_off = 0;
    uint32_t len_arr_off = 0;
    uint32_t cont_buf_off = 0;
    uint32_t null_bitmap_off = 0;
    uint16_t vec_value_tc = 0;
    sql::ser_eval_vector_function ser_eval_vector_func = nullptr;
    LST_DO_CODE(OB_UNIS_DECODE,
      vector_header_off,
      offset_off,
      len_arr_off,
      cont_buf_off,
      null_bitmap_off,
      vec_value_tc,
      ser_eval_vector_func);
  }
  OB_UNIS_DECODE(local_session_var_id_);
  return ret;
}

class TestExprSerializeCompat : public ::testing::Test
{
public:
  TestExprSerializeCompat() {}
  virtual ~TestExprSerializeCompat() {}
  virtual void SetUp() {}
  virtual void TearDown() {}
};

void fill_expr(ObExpr &expr)
{
  expr.type_ = T_INT;
  expr.datum_meta_.type_ = ObIntType;
  expr.datum_meta_.cs_type_ = CS_TYPE_BINARY;
  expr.datum_meta_.scale_ = 12;
  expr.obj_meta_.set_type(ObIntType);
  expr.max_length_ = UINT32_MAX;
  expr.obj_datum_map_ = OBJ_DATUM_STRING;
  expr.batch_result_ = true;
  expr.is_called_in_sql_ = true;
  expr.is_static_const_ = true;
  expr.is_boolean_ = true;
  expr.is_dynamic_const_ = true;
  expr.need_stack_check_ = true;
  expr.ser_eval_func_ = nullptr;
  expr.ser_eval_batch_func_ = nullptr;
  expr.ser_inner_functions_ = nullptr;
  expr.inner_func_cnt_ = 0;
  expr.args_ = nullptr;
  expr.arg_cnt_ = 0;
  expr.flag_ = 44;
  expr.frame_idx_ = 1;
  expr.datum_off_ = 1;
  expr.eval_info_off_ = 1;
  expr.dyn_buf_header_offset_ = 1;
  expr.res_buf_off_ = 1;
  expr.res_buf_len_ = 1;
  expr.eval_flags_off_ = 1;
  expr.pvt_skip_off_ = 1;
  expr.expr_ctx_id_ = 1;
  expr.extra_ = 1;
  expr.local_session_var_id_ = 1;
  expr.batch_idx_mask_ = 1;
  expr.extra_info_ = nullptr;
  expr.basic_funcs_ = nullptr;
  expr.batch_idx_mask_ = 0;
  expr.extra_info_ = nullptr;
  expr.local_session_var_id_ = OB_INVALID_INDEX_INT64;
}

void verify_basic_fields_equal(const ObExpr42x &expr1, const ObExpr &expr2)
{
  ASSERT_EQ(expr1.type_, expr2.type_);
  ASSERT_EQ(expr1.datum_meta_.type_, expr2.datum_meta_.type_);
  ASSERT_EQ(expr1.datum_meta_.cs_type_, expr2.datum_meta_.cs_type_);
  ASSERT_EQ(expr1.datum_meta_.scale_, expr2.datum_meta_.scale_);
  ASSERT_EQ(expr1.obj_meta_.get_type(), expr2.obj_meta_.get_type());
  ASSERT_EQ(expr1.max_length_, expr2.max_length_);
  ASSERT_EQ(expr1.obj_datum_map_, expr2.obj_datum_map_);
  ASSERT_EQ(expr1.flag_, expr2.flag_);
  ASSERT_EQ(expr1.frame_idx_, expr2.frame_idx_);
  ASSERT_EQ(expr1.datum_off_, expr2.datum_off_);
  ASSERT_EQ(expr1.eval_info_off_, expr2.eval_info_off_);
  ASSERT_EQ(expr1.dyn_buf_header_offset_, expr2.dyn_buf_header_offset_);
  ASSERT_EQ(expr1.res_buf_off_, expr2.res_buf_off_);
  ASSERT_EQ(expr1.res_buf_len_, expr2.res_buf_len_);
  ASSERT_EQ(expr1.eval_flags_off_, expr2.eval_flags_off_);
  ASSERT_EQ(expr1.pvt_skip_off_, expr2.pvt_skip_off_);
  ASSERT_EQ(expr1.expr_ctx_id_, expr2.expr_ctx_id_);
  ASSERT_EQ(expr1.extra_, expr2.extra_);
  ASSERT_EQ(expr1.local_session_var_id_, expr2.local_session_var_id_);
}

TEST_F(TestExprSerializeCompat, test_42x_to_master)
{
  ObExpr42x expr_42x;
  ObExpr expr_master;

  char buf[BUF_SIZE];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, expr_42x.serialize(buf, sizeof(buf), pos));

  int64_t deserialize_pos = 0;
  ASSERT_EQ(OB_SUCCESS, expr_master.deserialize(buf, pos, deserialize_pos));

  verify_basic_fields_equal(expr_42x, expr_master);
}

TEST_F(TestExprSerializeCompat, test_master_to_42x)
{
  ObExpr expr_master;
  ObExpr42x expr_42x;
  fill_expr(expr_master);

  char buf[BUF_SIZE];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, expr_master.serialize(buf, sizeof(buf), pos));

  int64_t deserialize_pos = 0;
  ASSERT_EQ(OB_SUCCESS, expr_42x.deserialize(buf, pos, deserialize_pos));

  verify_basic_fields_equal(expr_42x, expr_master);
}

} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}