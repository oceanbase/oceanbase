/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE_FTS

#include "ob_ik_surrogate_processor.h"

#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
namespace storage
{
int ObIKSurrogateProcessor::do_process(TokenizeContext &ctx,
                                       const char *ch,
                                       const uint8_t char_len,
                                       const ObFTCharUtil::CharType type)
{
  int ret = OB_SUCCESS;
  if (ObFTCharUtil::CharType::SURROGATE_HIGH == type) {
    high_offset_ = ctx.get_cursor();
    low_offset = ctx.get_end_cursor();
  } else if (ObFTCharUtil::CharType::SURROGATE_LOW == type && has_high()) {
    // out pair and reset
    low_offset = ctx.get_end_cursor();
    if (OB_FAIL(ctx.add_token(ctx.fulltext(),
                              high_offset_,
                              low_offset - high_offset_,
                              1,
                              ObIKTokenType::IK_SURROGATE_TOKEN))) {
      LOG_WARN("Fail to add token", K(ret));
    }
    reset();
  } else if (has_high()) { // so char is not surrogate
    // add high as the single char token
    if (OB_FAIL(ctx.add_token(ctx.fulltext(),
                              high_offset_,
                              low_offset - high_offset_,
                              1,
                              ObIKTokenType::IK_SURROGATE_TOKEN))) {
      LOG_WARN("Fail to add token", K(ret));
    }
    reset();
  } else if (ObFTCharUtil::CharType::SURROGATE_LOW == type) { // only a low surrogate, ignore
    reset();
  } else {
  }

  if (OB_FAIL(ret)) {
  } else if (ctx.is_last() && has_high()
             && OB_FAIL(ctx.add_token(ctx.fulltext(),
                                      high_offset_,
                                      low_offset - high_offset_,
                                      1,
                                      ObIKTokenType::IK_SURROGATE_TOKEN))) {
    LOG_WARN("Fail to add last token", K(ret));
  } else if (ctx.is_last() && has_high()) { // Succeed to add the last token
    reset();
  }
  return ret;
}
} //  namespace storage
} //  namespace oceanbase
