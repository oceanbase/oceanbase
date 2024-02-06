
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
#include "storage/blocksstable/cs_encoding/ob_micro_block_cs_encoder.h"

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
    : enable_abnormal_filter_type_(false)
  {}
  virtual ~ObPdFilterTestBase() {}
  virtual void SetUp() {}
  virtual void TearDown()
  {
    reuse();
    enable_abnormal_filter_type_ = false;
  }

  void set_obj_collation(ObObj &obj, const ObObjType &column_type);
  void set_obj_meta_collation(ObObjMeta &obj_meta, const ObObjType &column_type);
  void setup_obj(ObObj& obj, int64_t column_idx, int64_t seed);
  void setup_obj(ObObj& obj, int64_t column_idx);

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
                                      const ObObjType &col_type,
                                      const ObIArray<ObObj> &ref_objs,
                                      ObMicroBlockCSDecoder &decoder,
                                      const int64_t res_count);

public:
  bool enable_abnormal_filter_type_;

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
  if (enable_abnormal_filter_type_ && is_integer) {
    if (ob_obj_type_class(column_type) == ObObjTypeClass::ObIntTC) {
      column_meta.set_int();
    } else {
      column_meta.set_uint64();
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

int ObPdFilterTestBase::check_column_store_white_filter(
    const ObWhiteFilterOperatorType &op_type,
    const int64_t row_cnt,
    const int64_t col_cnt,
    const int64_t col_offset,
    const ObObjType &col_type,
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
    ObObjMeta col_meta;
    if (ref_objs.count() < 1) {
      set_obj_meta_collation(col_meta, col_type);
      col_param.set_meta_type(col_meta);
    } else {
      col_param.set_meta_type(ref_objs.at(0).meta_);
    }
    white_filter->col_params_.push_back(&col_param);
    white_filter->col_offsets_.push_back(col_offset);
    white_filter->n_cols_ = 1;

    int arg_cnt = ref_objs.count() + 1;
    if (sql::WHITE_OP_NU == op_type || sql::WHITE_OP_NN == op_type) {
      arg_cnt = 2;
    }

    void *expr_ptr = allocator_.alloc(sizeof(sql::ObExpr));
    MEMSET(expr_ptr, '\0', sizeof(sql::ObExpr));
    void *expr_ptr_arr = allocator_.alloc(sizeof(sql::ObExpr*) * arg_cnt);
    MEMSET(expr_ptr_arr, '\0', sizeof(sql::ObExpr*) * arg_cnt);
    void *expr_arr = allocator_.alloc(sizeof(sql::ObExpr) * arg_cnt);
    MEMSET(expr_arr, '\0', sizeof(sql::ObExpr) * arg_cnt);
    EXPECT_TRUE(OB_NOT_NULL(expr_ptr));
    EXPECT_TRUE(OB_NOT_NULL(expr_ptr_arr));
    EXPECT_TRUE(OB_NOT_NULL(expr_arr));

    white_filter->filter_.expr_ = reinterpret_cast<sql::ObExpr *>(expr_ptr);
    white_filter->filter_.expr_->arg_cnt_ = arg_cnt;
    white_filter->filter_.expr_->args_ = reinterpret_cast<sql::ObExpr **>(expr_ptr_arr);

    ObDatum datums[arg_cnt];
    white_filter->datum_params_.init(arg_cnt);
    const int64_t datum_buf_size = sizeof(int8_t) * 128 * arg_cnt;
    void *datum_buf = allocator_.alloc(datum_buf_size);
    MEMSET(datum_buf, '\0', datum_buf_size);
    EXPECT_TRUE(OB_NOT_NULL(datum_buf));

    for (int64_t i = 0; OB_SUCC(ret) && (i < arg_cnt); ++i) {
      white_filter->filter_.expr_->args_[i] = reinterpret_cast<sql::ObExpr *>(expr_arr) + i;
      if (i < arg_cnt - 1) {
        if (sql::WHITE_OP_NU == op_type || sql::WHITE_OP_NN == op_type) {
          white_filter->filter_.expr_->args_[i]->obj_meta_.set_null();
          white_filter->filter_.expr_->args_[i]->datum_meta_.type_ = ObNullType;
        } else {
          white_filter->filter_.expr_->args_[i]->obj_meta_ = ref_objs.at(i).get_meta();
          white_filter->filter_.expr_->args_[i]->datum_meta_.type_ = ref_objs.at(i).get_meta().get_type();
          datums[i].ptr_ = reinterpret_cast<char *>(datum_buf) + i * 128;
          if (OB_FAIL(datums[i].from_obj(ref_objs.at(i)))) {
            LOG_WARN("fail to handle datum from obj", KR(ret), K(i), K(ref_objs.at(i)));
          } else if (OB_FAIL(white_filter->datum_params_.push_back(datums[i]))) {
            LOG_WARN("fail to push back", KR(ret), K(i), K(datums[i]));
          }
        }
      } else {
        white_filter->filter_.expr_->args_[i]->type_ = T_REF_COLUMN;
      }
    }
    if (OB_UNLIKELY(2 > white_filter->filter_.expr_->arg_cnt_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected filter expr", K(ret), K(white_filter->filter_.expr_->arg_cnt_));
    } else {
      white_filter->cmp_func_ = get_datum_cmp_func(white_filter->filter_.expr_->args_[0]->obj_meta_, white_filter->filter_.expr_->args_[0]->obj_meta_);
    }

    if (OB_SUCC(ret) ) {
      if (sql::WHITE_OP_IN == white_filter->get_op_type()) {
        if (OB_FAIL(white_filter->init_obj_set())) {
          LOG_WARN("fail to init obj_set", KR(ret));
        }
      }

      if (FAILEDx(decoder.filter_pushdown_filter(nullptr, *white_filter, pd_filter_info, *res_bitmap))) {
        LOG_WARN("fail to filter pushdown filter", KR(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (res_count != res_bitmap->popcnt()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result count not match", K(res_count), K(res_bitmap->popcnt()), K(ref_objs));
        // ::ob_abort();
      }
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
        col_offset, col_types[col_offset], ref_objs, decoder, res_arr[i])) << "round: " << i << std::endl; \
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
        col_offset, col_types[col_offset], ref_objs, decoder, res_arr[i])) << "round: " << i << std::endl; \
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
        col_offset, col_types[col_offset], ref_objs, decoder, res_arr[i])) << "round: " << i << std::endl; \
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
        col_offset, col_types[col_offset], ref_objs, decoder, res_arr[i])); \
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
