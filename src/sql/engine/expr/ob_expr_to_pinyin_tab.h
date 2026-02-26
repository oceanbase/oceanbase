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
#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_TO_PINYIN_TABLE_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_TO_PINYIN_TABLE_

#include <cstdint>
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace sql
{
// The content of `begin` and `end` in `PINYIN_TABLE` comes from the file `cldr-common-33.0.zip:common/collation/zh.xml:35~1558`;
# define PINYIN_COUNT 1502
struct PinyinPair{
  uint64_t begin;
  uint64_t end;
  ObString pinyin;
};
extern PinyinPair PINYIN_TABLE[PINYIN_COUNT];
const uint64_t OptionCnt = 7;
const char* const OptionStr[OptionCnt] = {"Full", "Cap", "First_Cap", "All_Cap",
                                          "Initial", "Cap_Initial", "All_Cap_Initial"};
enum ModeOption {
  Full = 0,
  Cap,
  First_Cap,
  All_Cap,
  Initial,
  Cap_Initial,
  All_Cap_Initial,
};
} // end namespace sql
} // end namespace oceanbase
#endif /* OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_TO_PINYIN_ */
