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

#ifndef _OB_YSON_H
#define _OB_YSON_H 1
#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"  // for databuff_printf etc.
#include "lib/json/ob_yson_encode.h"     // TO_YSON_KV
// YSON: Yet Another Binary JSON
// difference with lib/utility/ob_uni_serialization:
// 1. Self-explanatory
// 2. need faster encoding, don't care decoding
#include "lib/ob_name_id_def.h"  // for NAME()

namespace oceanbase
{
namespace yson
{
// YSON to Text
int databuff_print_elements(char *buf, const int64_t buf_len, int64_t &pos,
                            const char *yson_buf, const int64_t yson_buf_len, bool in_array = false);

}  // yson
namespace common
{
struct ObYsonToString
{
  ObYsonToString(char* yson_buf, int64_t yson_buf_len)
      :yson_buf_(yson_buf),
       yson_buf_len_(yson_buf_len) {}
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    (void)::oceanbase::common::databuff_printf(buf, buf_len, pos, "{");
    (void)::oceanbase::yson::databuff_print_elements(buf, buf_len, pos, yson_buf_, yson_buf_len_);
    (void)::oceanbase::common::databuff_printf(buf, buf_len, pos, "}");
    return pos;
  }
private:
  char *yson_buf_;
  int64_t yson_buf_len_;
};

// define template <...> databuff_print_id_value(buf, buf_len, pos, ...)
#define PRINT_ID_VALUE_TEMPLATE_TYPE(N) CAT(typename T, N)
#define PRINT_ID_VALUE_ARG_PAIR(N) oceanbase::yson::ElementKeyType CAT(key, N), const CAT(T, N) &CAT(obj, N)
#define PRINT_ID_VALUE_ONE(N) if (OB_SUCC(ret)) {                       \
    ret = ::oceanbase::common::databuff_print_json_kv(buf, buf_len, pos, NAME(CAT(key, N)), CAT(obj,N)); \
  }

#define J_COMMA_WITH_RET \
    if (OB_FAIL(ret)) { \
    } else if (OB_FAIL(J_COMMA())) { \
    } else {}

#define DEFINE_PRINT_ID_VALUE(N)                                        \
  template < LST_DO_(N, PRINT_ID_VALUE_TEMPLATE_TYPE, (,), PROC_ONE, ONE_TO_HUNDRED) > \
  int databuff_print_id_value(char *buf, const int64_t buf_len, int64_t& pos, \
                              LST_DO_(N, PRINT_ID_VALUE_ARG_PAIR, (,), PROC_ONE, ONE_TO_HUNDRED) \
                              )                                         \
  {                                                                     \
    int ret = OB_SUCCESS;                            \
    LST_DO_(N, PRINT_ID_VALUE_ONE, (J_COMMA_WITH_RET), PROC_ONE, ONE_TO_HUNDRED);                 \
    return ret;                                                         \
  }

// TO_STRING_AND_YSON
#define TO_STRING_AND_YSON(...)                                         \
  int to_yson(char *buf, const int64_t buf_len, int64_t &pos) const     \
  {                                                                     \
    return oceanbase::yson::databuff_encode_elements(buf, buf_len, pos, __VA_ARGS__); \
  }                                                                     \
  DECLARE_TO_STRING                                                     \
  {                                                                     \
    int64_t pos = 0;                                                    \
    J_OBJ_START();                                                      \
    ::oceanbase::common::databuff_print_id_value(buf, buf_len, pos, __VA_ARGS__);  \
    J_OBJ_END();                                                        \
    return pos;                                                         \
  }

#define DEFINE_TO_STRING_AND_YSON(T, ...)                               \
  int T::to_yson(char *buf, const int64_t buf_len, int64_t &pos) const  \
  {                                                                     \
    return oceanbase::yson::databuff_encode_elements(buf, buf_len, pos, __VA_ARGS__); \
  }                                                                     \
  DEF_TO_STRING(T)                                                      \
  {                                                                     \
    int64_t pos = 0;                                                    \
    J_OBJ_START();                                                      \
    ::oceanbase::common::databuff_print_id_value(buf, buf_len, pos, __VA_ARGS__);  \
    J_OBJ_END();                                                        \
    return pos;                                                         \
  }

// TO_YSON_KV
#define TO_YSON_KV(...)                                                 \
  DECLARE_TO_YSON_KV                                                    \
  {                                                                     \
    return oceanbase::yson::databuff_encode_elements(buf, buf_len, pos, __VA_ARGS__); \
  }
#define VIRTUAL_TO_YSON_KV(...) virtual TO_YSON_KV(__VA_ARGS__)
#define DEFINE_TO_YSON_KV(T, ...) \
  int T::to_yson(char *buf, const int64_t buf_len, int64_t &pos) const  \
  {                                                                     \
    return oceanbase::yson::databuff_encode_elements(buf, buf_len, pos, __VA_ARGS__); \
  }

DEFINE_PRINT_ID_VALUE(1);
DEFINE_PRINT_ID_VALUE(2);
DEFINE_PRINT_ID_VALUE(3);
DEFINE_PRINT_ID_VALUE(4);
DEFINE_PRINT_ID_VALUE(5);
DEFINE_PRINT_ID_VALUE(6);
DEFINE_PRINT_ID_VALUE(7);
DEFINE_PRINT_ID_VALUE(8);
DEFINE_PRINT_ID_VALUE(9);
DEFINE_PRINT_ID_VALUE(10);
DEFINE_PRINT_ID_VALUE(11);
DEFINE_PRINT_ID_VALUE(12);
DEFINE_PRINT_ID_VALUE(13);
DEFINE_PRINT_ID_VALUE(14);
DEFINE_PRINT_ID_VALUE(15);
DEFINE_PRINT_ID_VALUE(16);
DEFINE_PRINT_ID_VALUE(17);
DEFINE_PRINT_ID_VALUE(18);
DEFINE_PRINT_ID_VALUE(19);
DEFINE_PRINT_ID_VALUE(20);
DEFINE_PRINT_ID_VALUE(21);
DEFINE_PRINT_ID_VALUE(22);
DEFINE_PRINT_ID_VALUE(23);
DEFINE_PRINT_ID_VALUE(24);
DEFINE_PRINT_ID_VALUE(25);
DEFINE_PRINT_ID_VALUE(26);
DEFINE_PRINT_ID_VALUE(27);
DEFINE_PRINT_ID_VALUE(28);
DEFINE_PRINT_ID_VALUE(29);
DEFINE_PRINT_ID_VALUE(30);
DEFINE_PRINT_ID_VALUE(31);
DEFINE_PRINT_ID_VALUE(32);

} // end namespace common
} // end namespace oceanbase

#endif /* _OB_YSON_H */
