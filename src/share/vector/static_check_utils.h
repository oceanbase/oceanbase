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

#ifndef OCEANBASE_SHARE_VECTOR_STATIC_CHECK_UTILS_H_
#define OCEANBASE_SHARE_VECTOR_STATIC_CHECK_UTILS_H_

// define TYPE_CHECK
// eg:  TYPE_CHECK_DEF(is_fixed_type, int8_t, int16_t, int32_t, int64_t,
//                     uint8_t, uint16_t, uint32_t, uint64_t,
//                     double, float);
//
// static_assert(is_fixed_type<T>::value, "invalid type");

template <typename T, typename... Args>
struct exist_type {
  static const bool value = false;
};

template <typename T, typename First, typename... Rest>
struct exist_type<T, First, Rest...> {
  static const bool value = std::is_same<T, First>::value || exist_type<T, Rest...>::value;
};

// #define TYPE_CHECKER_DEF(checker_name, ...)                          \
// template <typename T>                                                \
// struct checker_name {                                                \
//   static constexpr bool value = exist_type<T, ##__VA_ARGS__>::value; \
// };

// define VALUE_CHECK
// eg: DEF_CHECK_VALUE(VecValueTypeClass, is_decimal_tc,
//                     VEC_TC_DECIMAL_INT32,  VEC_TC_DECIMAL_INT64,
//                     VEC_TC_DECIMAL_INT128,  VEC_TC_DECIMAL_INT256,
//                     VEC_TC_DECIMAL_INT512);
//
// if (is_decimal_int<T>::value) { ... }
//

template <typename T, T v, T... args>
struct exist_value {
 static constexpr bool value = false;
};

template <typename T, T v, T First, T... Rest>
struct exist_value<T, v, First, Rest...> {
 static constexpr bool value = (v == First) || exist_value<T, v, Rest...>::value;
};

#define VALUE_CHECKER_DEF(type, checker_name, ...)                         \
template <type v>                                                          \
struct checker_name {                                                      \
    static constexpr bool value = exist_value<type, v, ##__VA_ARGS__>;     \
}; \                                                                       \

#endif // OCEANBASE_SHARE_VECTOR_STATIC_CHECK_UTILS_H_
