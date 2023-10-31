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

#include "ob_cs_decoding_util.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;

// one-dimensitional array, 4 elements
#define INIT_FILTER_OP_FUNCS_4(func_arr, producer, produce_func) \
  func_arr[0] = producer<0>::produce_func(); \
  func_arr[1] = producer<1>::produce_func(); \
  func_arr[2] = producer<2>::produce_func(); \
  func_arr[3] = producer<3>::produce_func(); \

// two-dimensitional array, 2*4
#define INIT_FILTER_OP_FUNCS_2P4(func_arr, producer, produce_func) \
  func_arr[0][0] = producer<false, 0>::produce_func(); \
  func_arr[1][0] = producer<true, 0>::produce_func(); \
  func_arr[0][1] = producer<false, 1>::produce_func(); \
  func_arr[1][1] = producer<true, 1>::produce_func(); \
  func_arr[0][2] = producer<false, 2>::produce_func(); \
  func_arr[1][2] = producer<true, 2>::produce_func(); \
  func_arr[0][3] = producer<false, 3>::produce_func(); \
  func_arr[1][3] = producer<true, 3>::produce_func(); \

// two-dimensitional array, 4*6
#define INIT_FILTER_OP_FUNCS_4P6(func_arr, producer, produce_func) \
  func_arr[0][op_type] = producer<0>::produce_func(static_cast<sql::ObWhiteFilterOperatorType>(op_type)); \
  func_arr[1][op_type] = producer<1>::produce_func(static_cast<sql::ObWhiteFilterOperatorType>(op_type)); \
  func_arr[2][op_type] = producer<2>::produce_func(static_cast<sql::ObWhiteFilterOperatorType>(op_type)); \
  func_arr[3][op_type] = producer<3>::produce_func(static_cast<sql::ObWhiteFilterOperatorType>(op_type)); \

// three-dimensitional array, 2*2*4
#define INIT_FILTER_OP_FUNCS_2P2P4(func_arr, producer, produce_func) \
  func_arr[0][0][0] = producer<false, false, 0>::produce_func(); \
  func_arr[1][0][0] = producer<true, false, 0>::produce_func(); \
  func_arr[0][1][0] = producer<false, true, 0>::produce_func(); \
  func_arr[1][1][0] = producer<true, true, 0>::produce_func(); \
  func_arr[0][0][1] = producer<false, false, 1>::produce_func(); \
  func_arr[1][0][1] = producer<true, false, 1>::produce_func(); \
  func_arr[0][1][1] = producer<false, true, 1>::produce_func(); \
  func_arr[1][1][1] = producer<true, true, 1>::produce_func(); \
  func_arr[0][0][2] = producer<false, false, 2>::produce_func(); \
  func_arr[1][0][2] = producer<true, false, 2>::produce_func(); \
  func_arr[0][1][2] = producer<false, true, 2>::produce_func(); \
  func_arr[1][1][2] = producer<true, true, 2>::produce_func(); \
  func_arr[0][0][3] = producer<false, false, 3>::produce_func(); \
  func_arr[1][0][3] = producer<true, false, 3>::produce_func(); \
  func_arr[0][1][3] = producer<false, true, 3>::produce_func(); \
  func_arr[1][1][3] = producer<true, true, 3>::produce_func(); \

// four-dimensitional array, 2*2*4*6
#define INIT_FILTER_OP_FUNCS_2P2P4P6(func_arr, producer, produce_func) \
  func_arr[0][0][0][op_type] = producer<false, false, 0>::produce_func(static_cast<sql::ObWhiteFilterOperatorType>(op_type)); \
  func_arr[1][0][0][op_type] = producer<true, false, 0>::produce_func(static_cast<sql::ObWhiteFilterOperatorType>(op_type)); \
  func_arr[0][1][0][op_type] = producer<false, true, 0>::produce_func(static_cast<sql::ObWhiteFilterOperatorType>(op_type)); \
  func_arr[1][1][0][op_type] = producer<true, true, 0>::produce_func(static_cast<sql::ObWhiteFilterOperatorType>(op_type)); \
  func_arr[0][0][1][op_type] = producer<false, false, 1>::produce_func(static_cast<sql::ObWhiteFilterOperatorType>(op_type)); \
  func_arr[1][0][1][op_type] = producer<true, false, 1>::produce_func(static_cast<sql::ObWhiteFilterOperatorType>(op_type)); \
  func_arr[0][1][1][op_type] = producer<false, true, 1>::produce_func(static_cast<sql::ObWhiteFilterOperatorType>(op_type)); \
  func_arr[1][1][1][op_type] = producer<true, true, 1>::produce_func(static_cast<sql::ObWhiteFilterOperatorType>(op_type)); \
  func_arr[0][0][2][op_type] = producer<false, false, 2>::produce_func(static_cast<sql::ObWhiteFilterOperatorType>(op_type)); \
  func_arr[1][0][2][op_type] = producer<true, false, 2>::produce_func(static_cast<sql::ObWhiteFilterOperatorType>(op_type)); \
  func_arr[0][1][2][op_type] = producer<false, true, 2>::produce_func(static_cast<sql::ObWhiteFilterOperatorType>(op_type)); \
  func_arr[1][1][2][op_type] = producer<true, true, 2>::produce_func(static_cast<sql::ObWhiteFilterOperatorType>(op_type)); \
  func_arr[0][0][3][op_type] = producer<false, false, 3>::produce_func(static_cast<sql::ObWhiteFilterOperatorType>(op_type)); \
  func_arr[1][0][3][op_type] = producer<true, false, 3>::produce_func(static_cast<sql::ObWhiteFilterOperatorType>(op_type)); \
  func_arr[0][1][3][op_type] = producer<false, true, 3>::produce_func(static_cast<sql::ObWhiteFilterOperatorType>(op_type)); \
  func_arr[1][1][3][op_type] = producer<true, true, 3>::produce_func(static_cast<sql::ObWhiteFilterOperatorType>(op_type)); \

ObCSFilterFunctionFactory::ObCSFilterFunctionFactory()
{
  INIT_FILTER_OP_FUNCS_2P4(integer_bt_null_funcs_, ObCSIntegerFilterFuncProducerWithNull, produce_integer_bt_tranverse_with_null);
  INIT_FILTER_OP_FUNCS_2P4(integer_in_null_funcs_, ObCSIntegerFilterFuncProducerWithNull, produce_integer_in_tranverse_with_null);
  INIT_FILTER_OP_FUNCS_2P4(dict_ref_sort_bt_funcs_, ObCSDictRefFilterFuncProducer, produce_dict_ref_sort_bt_tranverse);
  INIT_FILTER_OP_FUNCS_2P4(dict_scan_ref_funcs_, ObCSDictRefFilterFuncProducer, produce_dict_tranverse_ref);

  INIT_FILTER_OP_FUNCS_2P2P4(integer_bt_funcs_, ObCSIntegerFilterFuncProducer, produce_integer_bt_tranverse);
  INIT_FILTER_OP_FUNCS_2P2P4(integer_in_funcs_, ObCSIntegerFilterFuncProducer, produce_integer_in_tranverse);

  for (int32_t op_type = 0; op_type < 6; ++op_type) {
    INIT_FILTER_OP_FUNCS_2P2P4P6(integer_cmp_funcs_, ObCSIntegerFilterFuncProducer, produce_integer_cmp_tranverse);
    INIT_FILTER_OP_FUNCS_4P6(dict_val_cmp_funcs_, ObCSDictFilterFuncProducer, produce_dict_val_cmp_tranverse);
  }

  INIT_FILTER_OP_FUNCS_4(dict_val_bt_funcs_, ObCSDictFilterFuncProducer, produce_dict_val_bt_tranverse);
  INIT_FILTER_OP_FUNCS_4(dict_val_in_funcs_, ObCSDictFilterFuncProducer, produce_dict_val_in_tranverse);
}

ObCSFilterFunctionFactory &ObCSFilterFunctionFactory::instance()
{
  static ObCSFilterFunctionFactory ret;
  return ret;
}

ObMultiDimArray_T<cs_filter_is_less_than, 2, 4, 2, 4> ObCSDecodingUtil::less_than_funcs;

template <int L_IS_SIGNED, int32_t L_WIDTH_TAG, int R_IS_SIGNED, int32_t R_WIDTH_TAG>
struct CSFilterLessThanFuncInit
{
  bool operator()()
  {
    ObCSDecodingUtil::less_than_funcs[L_IS_SIGNED][L_WIDTH_TAG][R_IS_SIGNED][R_WIDTH_TAG]
        = &(ObCSFilterCommonFunction<L_IS_SIGNED, L_WIDTH_TAG, R_IS_SIGNED, R_WIDTH_TAG>::less_than_func);
    return true;
  }
};

bool InitCSFilterLessThanFunc()
{
  return ObNDArrayIniter<CSFilterLessThanFuncInit, 2, 4, 2, 4>::apply();
}

bool cs_filter_less_than_func_inited = InitCSFilterLessThanFunc();


}  // end namespace blocksstable
}  // end namespace oceanbase
