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

#ifndef OCEANBASE_TRANSACTION_OB_TX_ON_DEMAND_PRINT_HEADER
#define OCEANBASE_TRANSACTION_OB_TX_ON_DEMAND_PRINT_HEADER

namespace oceanbase
{
namespace transaction
{

#define IMPL_ON_DEMAND_PRINT_FUNC(ClassName) \
  int ClassName::on_demand_print_(char *buf, const int64_t buf_len, int64_t &pos) const

#define DECLARE_ON_DEMAND_TO_STRING \
  int on_demand_print_(char *buf, const int64_t buf_len, int64_t &pos) const;

#define ON_DEMAND_TO_STRING_KV_(args...) DEFINE_ON_DEMAND_TO_STRING_(J_KV(args))

#define DEFINE_ON_DEMAND_TO_STRING_(body) \
  DECLARE_ON_DEMAND_TO_STRING             \
  DECLARE_TO_STRING_                      \
  {                                       \
    int64_t pos = 0;                      \
    J_OBJ_START();                        \
    body;                                 \
    on_demand_print_(buf, buf_len, pos);  \
    J_OBJ_END();                          \
    return pos;                           \
  }

#define DECLARE_TO_STRING_ int64_t to_string_(char *buf, const int64_t buf_len) const
#define OBJ_TO_STR(obj) #obj

#define ON_DEMAND_START_PRINT(prefix_name) \
  BUF_PRINTF(" " OBJ_TO_STR(<prefix_name>-{))

#define ON_DEMAND_END_PRINT(postfix_name) \
  BUF_PRINTF(OBJ_TO_STR(}-<postfix_name>))

#define TX_KV_PRINT_WITH_ERR(print_condition, name, obj, separator)                     \
  if (print_condition) {                                                              \
    int tmp_ret = OB_SUCCESS;                                                         \
    int tmp_pos = pos;                                                                \
    if (OB_TMP_FAIL(common::databuff_print_json_kv(buf, buf_len, pos, #name, obj))) { \
      (void)common::databuff_print_kv(buf, buf_len, pos, #name, tmp_ret);             \
    }                                                                                 \
    BUF_PRINTF(separator); \
  }

#define TX_PRINT_FUNC_WITH_ERR(print_condition, func, separator)                              \
  if (print_condition) {                                                              \
    int tmp_ret = OB_SUCCESS;                                                         \
    int tmp_pos = pos;                                                                \
    if (OB_TMP_FAIL(func(buf, buf_len, pos))) { \
      (void)common::databuff_print_kv(buf, buf_len, pos, #func, tmp_ret);             \
    }                                                                                 \
    BUF_PRINTF(separator); \
  }
} // namespace transaction
} // namespace oceanbase

#endif
