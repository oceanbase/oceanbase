//Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_SHARE_COMPACTION_COMPACTION_INFO_PARAM_H_
#define OB_SHARE_COMPACTION_COMPACTION_INFO_PARAM_H_

namespace oceanbase
{
namespace compaction
{

#define DEFINE_COMPACITON_INFO_ADD_KV(n)                                       \
  template <LOG_TYPENAME_TN##n>                                                \
  void ADD_COMPACTION_INFO_PARAM(char *buf, const int64_t buf_size,            \
                                 LOG_PARAMETER_KV##n) {                        \
    int64_t __pos = strlen(buf);                                               \
    int ret = OB_SUCCESS;                                                      \
    SIMPLE_TO_STRING_##n                                                       \
    if (__pos < 0) {                                                           \
      __pos = 0;                                                               \
    } else if (__pos > 0) {                                                    \
      if (__pos >= buf_size) {                                                 \
        __pos = buf_size - 1;                                                  \
      } else {                                                                 \
        buf[__pos - 1] = ';';                                                  \
      }                                                                        \
    }                                                                          \
    buf[__pos] = '\0';                                                         \
  }

#define SIMPLE_TO_STRING(n)                                                                       \
    if (OB_FAIL(ret)) {                                                                          \
    } else if (OB_FAIL(::oceanbase::common::logdata_print_key_obj(buf, buf_size - 1, __pos, key##n, false, obj##n))) { \
    } else if (__pos + 1 >= buf_size) {                                                          \
    } else {                                                                                     \
      buf[__pos++] = ',';                                                                        \
    }

#define SIMPLE_TO_STRING_1  SIMPLE_TO_STRING(1)

#define SIMPLE_TO_STRING_2                                                                    \
    SIMPLE_TO_STRING_1                                                                        \
    SIMPLE_TO_STRING(2)

#define SIMPLE_TO_STRING_3                                                                    \
    SIMPLE_TO_STRING_2                                                                        \
    SIMPLE_TO_STRING(3)

#define SIMPLE_TO_STRING_4                                                                    \
    SIMPLE_TO_STRING_3                                                                        \
    SIMPLE_TO_STRING(4)

#define SIMPLE_TO_STRING_5                                                                    \
    SIMPLE_TO_STRING_4                                                                        \
    SIMPLE_TO_STRING(5)

#define SIMPLE_TO_STRING_6                                                                   \
    SIMPLE_TO_STRING_5                                                                        \
    SIMPLE_TO_STRING(6)

#define SIMPLE_TO_STRING_7                                                                    \
    SIMPLE_TO_STRING_6                                                                        \
    SIMPLE_TO_STRING(7)

#define SIMPLE_TO_STRING_8                                                                    \
    SIMPLE_TO_STRING_7                                                                        \
    SIMPLE_TO_STRING(8)

DEFINE_COMPACITON_INFO_ADD_KV(1)
DEFINE_COMPACITON_INFO_ADD_KV(2)
DEFINE_COMPACITON_INFO_ADD_KV(3)
DEFINE_COMPACITON_INFO_ADD_KV(4)
DEFINE_COMPACITON_INFO_ADD_KV(5)
DEFINE_COMPACITON_INFO_ADD_KV(6)
DEFINE_COMPACITON_INFO_ADD_KV(7)
DEFINE_COMPACITON_INFO_ADD_KV(8)

} // namespace compaction
} // namespace oceanbase

#endif // OB_SHARE_COMPACTION_COMPACTION_INFO_PARAM_H_
