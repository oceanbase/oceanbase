
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

#define USING_LOG_PREFIX STORAGE

#include <gtest/gtest.h>
#define protected public
#define private public
#include "ob_cs_encoding_test_base.h"
#include "storage/blocksstable/cs_encoding/ob_cs_encoding_util.h"
#include "storage/blocksstable/cs_encoding/ob_micro_block_cs_decoder.h"

namespace oceanbase
{
namespace blocksstable
{

using namespace common;
using namespace storage;
using namespace share::schema;

using ::testing::Bool;
using ::testing::Combine;

#define ENABLE_CASE_CHECK true

class ObPdFilterTestBase : public ObCSEncodingTestBase, public ::testing::Test
{
public:
  ObPdFilterTestBase()
    : abnormal_filter_type_(AbnormalFilterType::NOT_ABNORMAL)
  {}
  virtual ~ObPdFilterTestBase() {}
  virtual void SetUp() {}
  virtual void TearDown()
  {
    reuse();
    abnormal_filter_type_ = AbnormalFilterType::NOT_ABNORMAL;
  }

  void set_obj_collation(ObObj &obj, const ObObjType &column_type);
  void set_obj_meta_collation(ObObjMeta &obj_meta, const ObObjType &column_type);
  void setup_obj(ObObj& obj, int64_t column_idx, int64_t seed);
  void setup_obj(ObObj& obj, int64_t column_idx);

  void init_filter(
    sql::ObWhiteFilterExecutor &filter,
    const ObIArray<ObObj> &filter_objs,
    sql::ObExpr *expr_buf,
    sql::ObExpr **expr_p_buf,
    ObDatum *datums,
    void *datum_buf,
    const ObObjMeta &col_meta);
  void init_in_filter(
    sql::ObWhiteFilterExecutor &filter,
    const ObIArray<ObObj> &filter_objs,
    sql::ObExpr *expr_buf,
    sql::ObExpr **expr_p_buf,
    ObDatum *datums,
    void *datum_buf,
    const ObObjMeta &col_meta);

  template<typename T>
  int build_decimal_filter_ref(const int64_t ref_cnt, const T *ref_arr,
                               const int64_t col_offset, ObArray<ObObj> &ref_objs);
  int build_integer_filter_ref(const int64_t ref_cnt, const int64_t *ref_arr, const int64_t col_offset,
                               ObArray<ObObj> &ref_objs, const bool use_row_gen = false);
  int build_string_filter_ref(const char *str_buf, const int64_t str_len, const ObObjType &obj_type,
                              ObArray<ObObj> &ref_objs);

  int build_white_filter(sql::ObPushdownOperator &pd_operator,
                         sql::PushdownFilterInfo &pd_filter_info,
                         sql::ObPushdownWhiteFilterNode &pd_filter_node,
                         ObWhiteFilterExecutor *&white_filter,
                         ObBitmap *&res_bitmap,
                         const ObWhiteFilterOperatorType &op_type,
                         const int64_t row_cnt,
                         const int64_t col_cnt);

  int check_column_store_white_filter(const ObWhiteFilterOperatorType &op_type,
                                      const int64_t row_cnt,
                                      const int64_t col_cnt,
                                      const int64_t col_offset,
                                       const ObObjMeta &col_meta,
                                      const ObIArray<ObObj> &ref_objs,
                                      ObMicroBlockCSDecoder &decoder,
                                      const int64_t res_count);

public:
   enum AbnormalFilterType
   {
      NOT_ABNORMAL = 0,
      WIDER_WIDTH,
      OPPOSITE_SIGN
   };
   AbnormalFilterType abnormal_filter_type_;
};

void ObPdFilterTestBase::set_obj_collation(ObObj &obj, const ObObjType &column_type)
{
  set_obj_meta_collation(obj.meta_, column_type);
}

void ObPdFilterTestBase::set_obj_meta_collation(ObObjMeta &obj_meta, const ObObjType &column_type)
{
  if (ObVarcharType == column_type || ObCharType == column_type || ObHexStringType == column_type
      || ObNVarchar2Type == column_type || ObNCharType == column_type || ObTextType == column_type) {
    obj_meta.cs_level_ = CS_LEVEL_IMPLICIT;
    obj_meta.cs_type_ = CS_TYPE_UTF8MB4_GENERAL_CI;
  } else {
    obj_meta.set_type(column_type);
  }

  switch (column_type) {
    case ObCharType:
      obj_meta.set_char();
      break;
    case ObVarcharType:
      obj_meta.set_varchar();
      break;
    default:
      break;
  }
}

void ObPdFilterTestBase::setup_obj(ObObj& obj, int64_t column_idx, int64_t seed)
{
  ObObjMeta column_meta = row_generate_.column_list_.at(column_idx).col_type_;
  obj.copy_meta_type(column_meta);
  ObObjType column_type = column_meta.get_type();
  row_generate_.set_obj(column_type, row_generate_.column_list_.at(column_idx).col_id_, seed, obj, 0);
  set_obj_collation(obj, column_type);
}

void ObPdFilterTestBase::setup_obj(ObObj& obj, int64_t column_idx)
{
  ObObjMeta column_meta = row_generate_.column_list_.at(column_idx).col_type_;
  ObObjType column_type = column_meta.get_type();
  bool is_integer = (ob_obj_type_class(column_type) == ObObjTypeClass::ObIntTC) || (ob_obj_type_class(column_type) == ObObjTypeClass::ObUIntTC);
  if (is_integer) {
    if (abnormal_filter_type_ == AbnormalFilterType::WIDER_WIDTH) {
      if (ob_obj_type_class(column_type) == ObObjTypeClass::ObIntTC) {
        column_meta.set_int();
      } else {
        column_meta.set_uint64();
      }
    } else if (abnormal_filter_type_ == AbnormalFilterType::OPPOSITE_SIGN) {
      if (ob_obj_type_class(column_type) == ObObjTypeClass::ObIntTC) {
        column_meta.set_uint64();
      } else {
        column_meta.set_int();
      }
    }
  }
  column_type = column_meta.get_type();
  obj.copy_meta_type(column_meta);
  set_obj_collation(obj, column_type);
}

int ObPdFilterTestBase::build_integer_filter_ref(
    const int64_t ref_cnt,
    const int64_t *ref_arr,
    const int64_t col_offset,
    ObArray<ObObj> &ref_objs,
    const bool use_row_gen)
{
  int ret = OB_SUCCESS;
  if (ref_cnt < 1 || OB_ISNULL(ref_arr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ref_cnt));
  } else if (OB_FAIL(ref_objs.reserve(ref_cnt))) {
    LOG_WARN("fail to reserve", KR(ret), K(ref_cnt));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && (i < ref_cnt); ++i) {
      if (use_row_gen) {
        ObObj ref_obj;
        setup_obj(ref_obj, col_offset, ref_arr[i]/*actual is ref_seed_arr*/);
        if (OB_FAIL(ref_objs.push_back(ref_obj))) {
          LOG_WARN("fail to push back", KR(ret), K(i));
        }
      } else {
        ObObj ref_obj(ref_arr[i]);
        setup_obj(ref_obj, col_offset);
        if (OB_FAIL(ref_objs.push_back(ref_obj))) {
          LOG_WARN("fail to push back", KR(ret), K(i));
        }
      }
    }
  }
  return ret;
}

template<typename T>
int ObPdFilterTestBase::build_decimal_filter_ref(
    const int64_t ref_cnt,
    const T *ref_arr,
    const int64_t col_offset,
    ObArray<ObObj> &ref_objs)
{
  int ret = OB_SUCCESS;
  if (ref_cnt < 1 || OB_ISNULL(ref_arr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ref_cnt));
  } else if (OB_FAIL(ref_objs.reserve(ref_cnt))) {
    LOG_WARN("fail to reserve", KR(ret), K(ref_cnt));
  } else {
    const int32_t int_bytes = sizeof(T);
    char *buf = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && (i < ref_cnt); ++i) {
      if (OB_ISNULL(buf = (char *)allocator_.alloc(int_bytes))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "fail to alloc memory");
      } else {
        ObObj ref_obj;
        MEMCPY(buf, &ref_arr[i], int_bytes);
        ref_obj.set_decimal_int(int_bytes, 0, reinterpret_cast<ObDecimalInt *>(buf));
        if (OB_FAIL(ref_objs.push_back(ref_obj))) {
          LOG_WARN("fail to push back", KR(ret), K(i));
        }
      }
    }
  }

  return ret;
}

int ObPdFilterTestBase::build_string_filter_ref(
    const char *str_buf,
    const int64_t str_len,
    const ObObjType &obj_type,
    ObArray<ObObj> &ref_objs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(str_buf) || OB_UNLIKELY(str_len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(str_len));
  } else {
    ObObj ref_obj;
    ref_obj.meta_.set_char();
    set_obj_collation(ref_obj, obj_type);
    ref_obj.set_char_value(str_buf, str_len);
    if (OB_FAIL(ref_objs.push_back(ref_obj))) {
      LOG_WARN("fail to push back", KR(ret));
    }
  }
  return ret;
}

int ObPdFilterTestBase::build_white_filter(
    sql::ObPushdownOperator &pd_operator,
    sql::PushdownFilterInfo &pd_filter_info,
    sql::ObPushdownWhiteFilterNode &pd_filter_node,
    ObWhiteFilterExecutor *&white_filter,
    ObBitmap *&res_bitmap,
    const ObWhiteFilterOperatorType &op_type,
    const int64_t row_cnt,
    const int64_t col_cnt)
{
  int ret = OB_SUCCESS;
  if (row_cnt < 1 || col_cnt < 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(col_cnt), K(row_cnt));
  } else {
    // build pd_filter_info
    void *storage_datum_buf = allocator_.alloc(sizeof(ObStorageDatum) * col_cnt);
    pd_filter_info.datum_buf_ = new (storage_datum_buf) ObStorageDatum [col_cnt]();
    pd_filter_info.col_capacity_ = col_cnt;
    pd_filter_info.start_ = 0;
    pd_filter_info.count_ = row_cnt;

    // build white_filter_executor
    pd_filter_node.op_type_ = op_type;

    ObIAllocator* allocator_ptr = &allocator_;
    white_filter = OB_NEWx(ObWhiteFilterExecutor, allocator_ptr, allocator_, pd_filter_node, pd_operator);

    if (OB_ISNULL(white_filter)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate white filter", KR(ret));
    } else if (OB_FAIL(white_filter->init_bitmap(row_cnt, res_bitmap))) {
      LOG_WARN("fail to init bitmap", KR(ret), K(row_cnt));
    } else if (OB_ISNULL(res_bitmap)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result bitmap should not be null", KR(ret), K(row_cnt));
    }
  }
  return ret;
}

// col_meta is may not equal to param_meta
void ObPdFilterTestBase::init_filter(
    sql::ObWhiteFilterExecutor &filter,
    const ObIArray<ObObj> &filter_objs,
    sql::ObExpr *expr_buf,
    sql::ObExpr **expr_p_buf,
    ObDatum *datums,
    void *datum_buf,
    const ObObjMeta &col_meta)
{
  int count = filter_objs.count();
  ObWhiteFilterOperatorType op_type = filter.filter_.get_op_type();
  if (sql::WHITE_OP_NU == op_type || sql::WHITE_OP_NN == op_type) {
    count = 1;
  }

  filter.filter_.expr_ = new (expr_buf) ObExpr();
  filter.filter_.expr_->arg_cnt_ = count + 1;
  filter.filter_.expr_->args_ = expr_p_buf;
  ASSERT_EQ(OB_SUCCESS, filter.datum_params_.init(count));

  for (int64_t i = 0; i <= count; ++i) {
    filter.filter_.expr_->args_[i] = new (expr_buf + 1 + i) ObExpr();
    if (i < count) {
      if (sql::WHITE_OP_NU == op_type || sql::WHITE_OP_NN == op_type) {
        filter.filter_.expr_->args_[i]->obj_meta_.set_null();
        filter.filter_.expr_->args_[i]->datum_meta_.type_ = ObNullType;
      } else {
        filter.filter_.expr_->args_[i]->obj_meta_ = filter_objs.at(i).get_meta();
        filter.filter_.expr_->args_[i]->datum_meta_.type_ = filter_objs.at(i).get_meta().get_type();
        datums[i].ptr_ = reinterpret_cast<char *>(datum_buf) + i * 128;
        datums[i].from_obj(filter_objs.at(i));
        ASSERT_EQ(OB_SUCCESS, filter.datum_params_.push_back(datums[i]));
        if (filter.is_null_param(datums[i], filter_objs.at(i).get_meta())) {
          filter.null_param_contained_ = true;
        }
      }
    } else {
      filter.filter_.expr_->args_[i]->type_ = T_REF_COLUMN;
      filter.filter_.expr_->args_[i]->obj_meta_ = col_meta;
      filter.filter_.expr_->args_[i]->datum_meta_.type_ = col_meta.get_type();
    }
  }
  filter.cmp_func_ = get_datum_cmp_func(col_meta, filter.filter_.expr_->args_[0]->obj_meta_);
}

// col_meta is may not equal to param_meta
void ObPdFilterTestBase::init_in_filter(
    sql::ObWhiteFilterExecutor &filter,
    const ObIArray<ObObj> &filter_objs,
    sql::ObExpr *expr_buf,
    sql::ObExpr **expr_p_buf,
    ObDatum *datums,
    void *datum_buf,
    const ObObjMeta &col_meta)
{
  int count = filter_objs.count();
  ASSERT_TRUE(count > 0);
  filter.filter_.expr_ = new (expr_buf) ObExpr();
  filter.filter_.expr_->arg_cnt_ = 2;
  filter.filter_.expr_->args_ = expr_p_buf;
  filter.filter_.expr_->args_[0] = new (expr_buf + 1) ObExpr();
  filter.filter_.expr_->args_[1] = new (expr_buf + 2) ObExpr();
  filter.filter_.expr_->inner_func_cnt_ = count;
  filter.filter_.expr_->args_[1]->args_ = expr_p_buf + 2;

  ObObjMeta obj_meta = filter_objs.at(0).get_meta();
  sql::ObExprBasicFuncs *basic_funcs = ObDatumFuncs::get_basic_func(
    obj_meta.get_type(), obj_meta.get_collation_type(), obj_meta.get_scale(), false, obj_meta.has_lob_header());
  sql::ObExprBasicFuncs *col_basic_funcs = ObDatumFuncs::get_basic_func(
    col_meta.get_type(), col_meta.get_collation_type(), col_meta.get_scale(), false, col_meta.has_lob_header());
  ObDatumCmpFuncType cmp_func = get_datum_cmp_func(col_meta, obj_meta);
  ObDatumCmpFuncType cmp_func_rev = get_datum_cmp_func(obj_meta, col_meta);

  filter.filter_.expr_->args_[0]->type_ = T_REF_COLUMN;
  filter.filter_.expr_->args_[0]->obj_meta_ = col_meta;
  filter.filter_.expr_->args_[0]->datum_meta_.type_ = col_meta.get_type();
  filter.filter_.expr_->args_[0]->basic_funcs_ = col_basic_funcs;

  ASSERT_EQ(OB_SUCCESS, filter.datum_params_.init(count));
  ASSERT_EQ(OB_SUCCESS, filter.param_set_.create(count * 2));
  filter.param_set_.set_hash_and_cmp_func(basic_funcs->murmur_hash_v2_, basic_funcs->null_first_cmp_);
  for (int64_t i = 0; i < count; ++i) {
    filter.filter_.expr_->args_[1]->args_[i] = new (expr_buf + 3 + i) ObExpr();
    filter.filter_.expr_->args_[1]->args_[i]->obj_meta_ = obj_meta;
    filter.filter_.expr_->args_[1]->args_[i]->datum_meta_.type_ = obj_meta.get_type();
    filter.filter_.expr_->args_[1]->args_[i]->basic_funcs_ = basic_funcs;
    datums[i].ptr_ = reinterpret_cast<char *>(datum_buf) + i * 128;
    datums[i].from_obj(filter_objs.at(i));
    if (!filter.is_null_param(datums[i], filter_objs.at(i).get_meta())) {
      ASSERT_EQ(OB_SUCCESS, filter.add_to_param_set_and_array(datums[i], filter.filter_.expr_->args_[1]->args_[i]));
    }
  }
  std::sort(filter.datum_params_.begin(), filter.datum_params_.end(),
            [cmp_func] (const ObDatum datum1, const ObDatum datum2) {
                int cmp_ret = 0;
                cmp_func(datum1, datum2, cmp_ret);
                return cmp_ret < 0;
            });
  filter.cmp_func_ = cmp_func;
  filter.cmp_func_rev_ = cmp_func_rev;
  filter.param_set_.set_hash_and_cmp_func(col_basic_funcs->murmur_hash_v2_, filter.cmp_func_rev_);
}

int ObPdFilterTestBase::check_column_store_white_filter(
    const ObWhiteFilterOperatorType &op_type,
    const int64_t row_cnt,
    const int64_t col_cnt,
    const int64_t col_offset,
    const ObObjMeta &col_meta,
    const ObIArray<ObObj> &ref_objs,
    ObMicroBlockCSDecoder &decoder,
    const int64_t res_count)
{
  int ret = OB_SUCCESS;
  sql::ObPushdownWhiteFilterNode pd_filter_node(allocator_);
  sql::PushdownFilterInfo pd_filter_info;
  sql::ObExecContext exec_ctx(allocator_);
  sql::ObEvalCtx eval_ctx(exec_ctx);
  sql::ObPushdownExprSpec expr_spec(allocator_);
  sql::ObPushdownOperator pd_operator(eval_ctx, expr_spec);
  ObWhiteFilterExecutor *white_filter = nullptr;
  common::ObBitmap *res_bitmap = nullptr;

  if (row_cnt < 1 || col_cnt < 1 || col_offset < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(row_cnt), K(col_cnt), K(col_offset), K(ref_objs.count()));
  } else if (OB_FAIL(build_white_filter(pd_operator, pd_filter_info, pd_filter_node, white_filter, res_bitmap,
    op_type, row_cnt, col_cnt))) {
    LOG_WARN("fail to build white filter", KR(ret));
  } else if (OB_ISNULL(white_filter) || OB_ISNULL(res_bitmap)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (ref_objs.count() < 1 && op_type != sql::WHITE_OP_NU && op_type != sql::WHITE_OP_NN) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(op_type));
  } else {
    white_filter->col_offsets_.init(1);
    white_filter->col_params_.init(1);
    ObColumnParam col_param(allocator_);
    col_param.set_meta_type(col_meta);
    white_filter->col_params_.push_back(&col_param);
    white_filter->col_offsets_.push_back(col_offset);
    white_filter->n_cols_ = 1;

    int count = ref_objs.count();
    ObWhiteFilterOperatorType op_type = pd_filter_node.get_op_type();
    if (sql::WHITE_OP_NU == op_type || sql::WHITE_OP_NN == op_type) {
      count = 1;
    }
    int count_expr = WHITE_OP_IN == op_type ? count + 3 : count + 2;
    int count_expr_p = WHITE_OP_IN == op_type ? count + 2 : count + 1;
    sql::ObExpr *expr_buf = reinterpret_cast<sql::ObExpr *>(allocator_.alloc(sizeof(sql::ObExpr) * count_expr));
    sql::ObExpr **expr_p_buf = reinterpret_cast<sql::ObExpr **>(allocator_.alloc(sizeof(sql::ObExpr*) * count_expr_p));
    void *datum_buf = allocator_.alloc(sizeof(int8_t) * 128 * count);
    ObDatum datums[count];
    EXPECT_TRUE(OB_NOT_NULL(expr_buf));
    EXPECT_TRUE(OB_NOT_NULL(expr_p_buf));

    if (WHITE_OP_IN == op_type) {
      init_in_filter(*white_filter, ref_objs, expr_buf, expr_p_buf, datums, datum_buf, col_meta);
    } else {
      init_filter(*white_filter, ref_objs, expr_buf, expr_p_buf, datums, datum_buf, col_meta);
    }

    if (OB_UNLIKELY(2 > white_filter->filter_.expr_->arg_cnt_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected filter expr", K(ret), K(white_filter->filter_.expr_->arg_cnt_));
    } else if (OB_FAIL(decoder.filter_pushdown_filter(nullptr, *white_filter, pd_filter_info, *res_bitmap))) {
      LOG_WARN("fail to filter pushdown filter", KR(ret));
    } else {
      EXPECT_EQ(res_count, res_bitmap->popcnt());
    }

    if (nullptr != expr_buf) {
      allocator_.free(expr_buf);
    }
    if (nullptr != expr_p_buf) {
      allocator_.free(expr_p_buf);
    }
    if (nullptr != datum_buf) {
      allocator_.free(datum_buf);
    }
  }
  return ret;
}

}
}

#define integer_type_filter_normal_check(flag, op_type, round, ref_cnt, res_arr) \
  need_check = flag & enable_check; \
  if (need_check) { \
    int64_t tmp_ref_arr[ref_cnt]; \
    ObArray<ObObj> ref_objs; \
    for (int64_t i = 0; i < round; ++i) { \
      ref_objs.reset(); \
      if (ref_cnt > 0) { \
        int64_t start_idx = ref_cnt * i; \
        for (int64_t j = start_idx; (j < ref_cnt * (i + 1)); ++j) { \
          tmp_ref_arr[j - start_idx] = ref_arr[j]; \
        } \
        ASSERT_EQ(OB_SUCCESS, build_integer_filter_ref(ref_cnt, tmp_ref_arr, col_offset, ref_objs, false)); \
      } \
      ASSERT_EQ(OB_SUCCESS, check_column_store_white_filter(op_type, row_cnt, col_cnt, \
        col_offset, col_descs_[col_offset].col_type_, ref_objs, decoder, res_arr[i])) << "round: " << i << std::endl; \
    } \
  } \

#define decimal_type_filter_normal_check(flag, op_type, round, ref_cnt, res_arr) \
  need_check = flag & enable_check; \
  if (need_check) { \
    std::decay<decltype(*ref_arr)>::type  tmp_ref_arr[ref_cnt]; \
    ObArray<ObObj> ref_objs; \
    for (int64_t i = 0; i < round; ++i) { \
      ref_objs.reset(); \
      if (ref_cnt > 0) { \
        int64_t start_idx = ref_cnt * i; \
        for (int64_t j = start_idx; (j < ref_cnt * (i + 1)); ++j) { \
          tmp_ref_arr[j - start_idx] = ref_arr[j]; \
        } \
        ASSERT_EQ(OB_SUCCESS, build_decimal_filter_ref(ref_cnt, tmp_ref_arr, col_offset, ref_objs)); \
      } \
      ASSERT_EQ(OB_SUCCESS, check_column_store_white_filter(op_type, row_cnt, col_cnt, \
        col_offset, col_descs_[col_offset].col_type_, ref_objs, decoder, res_arr[i])) << "round: " << i << std::endl; \
    } \
  } \


#define raw_type_filter_normal_check(flag, op_type, round, ref_cnt, res_arr) \
  need_check = flag & enable_check; \
  if (need_check) { \
    int64_t tmp_ref_seed_arr[ref_cnt]; \
    ObArray<ObObj> ref_objs; \
    for (int64_t i = 0; i < round; ++i) { \
      ref_objs.reset(); \
      if (ref_cnt > 0) { \
        int64_t start_idx = ref_cnt * i; \
        for (int64_t j = start_idx; (j < ref_cnt * (i + 1)); ++j) { \
          tmp_ref_seed_arr[j - start_idx] = ref_seed_arr[j]; \
        } \
        ASSERT_EQ(OB_SUCCESS, build_integer_filter_ref(ref_cnt, tmp_ref_seed_arr, col_offset, ref_objs, true)); \
      } \
      ASSERT_EQ(OB_SUCCESS, check_column_store_white_filter(op_type, row_cnt, col_cnt, \
        col_offset, col_descs_[col_offset].col_type_, ref_objs, decoder, res_arr[i])) << "round: " << i << std::endl; \
    } \
  } \

#define string_type_filter_normal_check(flag, op_type, round, ref_cnt, res_arr) \
  need_check = flag & enable_check; \
  if (need_check) { \
    std::pair<int64_t, int64_t> tmp_ref_arr[ref_cnt]; \
    ObArray<ObObj> ref_objs; \
    for (int64_t i = 0; i < round; ++i) { \
      ref_objs.reset(); \
      if (ref_cnt > 0) { \
        int64_t start_idx = ref_cnt * i; \
        for (int64_t j = start_idx; (j < ref_cnt * (i + 1)); ++j) { \
          const int64_t cur_idx = j - start_idx; \
          tmp_ref_arr[cur_idx] = ref_arr[j]; \
          ASSERT_EQ(OB_SUCCESS, build_string_filter_ref(char_data_arr[tmp_ref_arr[cur_idx].first], tmp_ref_arr[cur_idx].second, col_types[col_offset], ref_objs)); \
        } \
      } \
      ASSERT_EQ(OB_SUCCESS, check_column_store_white_filter(op_type, row_cnt, col_cnt, \
        col_offset, col_descs_[col_offset].col_type_, ref_objs, decoder, res_arr[i])); \
    } \
  } \


  #define HANDLE_TRANSFORM() \
  ObMicroBlockDesc micro_block_desc; \
  ObMicroBlockHeader *header = nullptr; \
  ASSERT_EQ(OB_SUCCESS, build_micro_block_desc(encoder, micro_block_desc, header)); \
  ASSERT_EQ(OB_SUCCESS, full_transform_check_row(header, micro_block_desc, row_arr, row_cnt, true)); \
  ASSERT_EQ(OB_SUCCESS, part_transform_check_row(header, micro_block_desc, row_arr, row_cnt, true)); \
  LOG_INFO(">>>>>>>>>>FINISH DECODER<<<<<<<<<<<"); \
  LOG_INFO(">>>>>>>>>>START PD FILTER<<<<<<<<<<<"); \
  ObMicroBlockData full_transformed_data; \
  ObMicroBlockCSDecoder decoder; \
  ASSERT_EQ(OB_SUCCESS, init_cs_decoder(header, micro_block_desc, full_transformed_data, decoder));
