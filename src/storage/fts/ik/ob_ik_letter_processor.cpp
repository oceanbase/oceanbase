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

#include "storage/fts/ik/ob_ik_letter_processor.h"

#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "storage/fts/ik/ob_ik_char_util.h"
#include "storage/fts/ik/ob_ik_processor.h"
#include "storage/fts/ik/ob_ik_token.h"

namespace oceanbase
{
namespace storage
{
ObIKLetterProcessor::ObIKLetterProcessor() {}

int ObIKLetterProcessor::do_process(TokenizeContext &ctx,
                                    const char *ch,
                                    const uint8_t char_len,
                                    const ObFTCharUtil::CharType type)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(process_english_letter(ctx, ch, char_len, type))) {
    LOG_WARN("Fail to process english letter", K(ret));
  } else if (OB_FAIL(process_arabic_letter(ctx, ch, char_len, type))) {
    LOG_WARN("Fail to process arabic letter", K(ret));
  } else if (OB_FAIL(process_mix_letter(ctx, ch, char_len, type))) {
    LOG_WARN("Fail to process mix letter", K(ret));
  }
  return ret;
}

int ObIKLetterProcessor::process_english_letter(TokenizeContext &ctx,
                                                const char *ch,
                                                const uint8_t char_len,
                                                const ObFTCharUtil::CharType type)
{
  int ret = OB_SUCCESS;

  if (-1 == english_start_) {
    if (ObFTCharUtil::CharType::ENGLISH_LETTER == type) {
      english_start_ = ctx.get_cursor();
      english_end_ = ctx.get_end_cursor();
      english_char_cnt_++;
    }
  } else {
    if (ObFTCharUtil::CharType::ENGLISH_LETTER == type) {
      english_end_ = ctx.get_end_cursor();
      english_char_cnt_++;
    } else {
      if (OB_FAIL(ctx.add_token(ctx.fulltext(),
                                english_start_,
                                english_end_ - english_start_,
                                english_char_cnt_,
                                ObIKTokenType::IK_ENGLISH_TOKEN))) {
        LOG_WARN("fail to add token", K(ret));
      } else {
        reset_english_state();
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (ctx.is_last() && (-1 != english_start_ && -1 != english_end_)) {
      if (OB_FAIL(ctx.add_token(ctx.fulltext(),
                                english_start_,
                                english_end_ - english_start_,
                                english_char_cnt_,
                                ObIKTokenType::IK_ENGLISH_TOKEN))) {
        LOG_WARN("Fail to add token", K(ret));
      } else {
        reset_english_state();
      }
    }
  }

  return ret;
}

int ObIKLetterProcessor::process_arabic_letter(TokenizeContext &ctx,
                                               const char *ch,
                                               const uint8_t char_len,
                                               const ObFTCharUtil::CharType type)
{
  int ret = OB_SUCCESS;

  bool is_connector = false;

  if (-1 == arabic_start_) {
    if (ObFTCharUtil::CharType::ARABIC_LETTER == type) {
      arabic_start_ = ctx.get_cursor();
      arabic_end_ = ctx.get_end_cursor();
      arabic_char_cnt_++;
    }
  } else if (ObFTCharUtil::CharType::ARABIC_LETTER == type) {
    arabic_end_ = ctx.get_end_cursor();
    // count previous count
    arabic_char_cnt_ += arabic_connect_cnt_;
    arabic_char_cnt_++;
    arabic_connect_cnt_ = 0;
  } else if (OB_FAIL(
                 ObFTCharUtil::check_num_connector(ctx.collation(), ch, char_len, is_connector))) {
    LOG_WARN("Fail to check is num connector", K(ret));
  } else if (ObFTCharUtil::CharType::USELESS == type && is_connector) {
    // only if connector follows  and is followed by an arabic number we count it
    arabic_connect_cnt_++;
  } else {
    if (is_connector) {
      arabic_connect_cnt_ = 0;
    }
    if (OB_FAIL(ctx.add_token(ctx.fulltext(),
                              arabic_start_,
                              arabic_end_ - arabic_start_,
                              arabic_char_cnt_ + arabic_connect_cnt_,
                              ObIKTokenType::IK_ARABIC_TOKEN))) {
      LOG_WARN("Fail to add token", K(ret));
    } else {
      reset_arabic_state();
    }
  }

  if (OB_SUCC(ret)) {
    if (ctx.is_last() && (arabic_start_ != -1 && arabic_end_ != -1)) {
      if (is_connector) {
        arabic_connect_cnt_ = 0;
      }
      if (OB_FAIL(ctx.add_token(ctx.fulltext(),
                                arabic_start_,
                                arabic_end_ - arabic_start_,
                                arabic_char_cnt_ + arabic_connect_cnt_,
                                ObIKTokenType::IK_ARABIC_TOKEN))) {
        LOG_WARN("fail to add token", K(ret));
      } else {
        reset_arabic_state();
      }
    }
  }

  return ret;
}

int ObIKLetterProcessor::process_mix_letter(TokenizeContext &ctx,
                                            const char *ch,
                                            const uint8_t char_len,
                                            const ObFTCharUtil::CharType type)
{
  int ret = OB_SUCCESS;

  bool is_mix_char = (ObFTCharUtil::CharType::ARABIC_LETTER == type
                      || ObFTCharUtil::CharType::ENGLISH_LETTER == type);
  if (mix_start_ == -1) {
    if (is_mix_char) {
      mix_start_ = ctx.get_cursor();
      mix_end_ = ctx.get_end_cursor();
      mix_char_cnt_++;
    }
  } else {
    bool is_connector;
    if (is_mix_char) {
      mix_end_ = ctx.get_end_cursor();
      this->mix_char_cnt_++;
    } else if (OB_FAIL(ObFTCharUtil::check_letter_connector(ctx.collation(),
                                                            ch,
                                                            char_len,
                                                            is_connector))) {
      LOG_WARN("Fail to check is letter connector", K(ret));
    } else if (ObFTCharUtil::CharType::USELESS == type && is_connector) {
      this->mix_end_ = ctx.get_end_cursor();
      this->mix_char_cnt_++;
    } else {
      if (OB_FAIL(ctx.add_token(ctx.fulltext(),
                                mix_start_,
                                mix_end_ - mix_start_,
                                mix_char_cnt_,
                                ObIKTokenType::IK_MIX_TOKEN))) {
        LOG_WARN("Fail to add token", K(ret));
      } else {
        reset_mix_state();
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (ctx.is_last() && (mix_start_ != -1 && mix_end_ != -1)) {
      if (OB_FAIL(ctx.add_token(ctx.fulltext(),
                                mix_start_,
                                mix_end_ - mix_start_,
                                mix_char_cnt_,
                                ObIKTokenType::IK_MIX_TOKEN))) {
        LOG_WARN("Fail to add token", K(ret));
      } else {
        reset_mix_state();
      }
    }
  }

  return ret;
}

void ObIKLetterProcessor::reset_english_state()
{
  english_start_ = -1;
  english_end_ = -1;
  english_char_cnt_ = 0;
}

void ObIKLetterProcessor::reset_arabic_state()
{
  arabic_start_ = -1;
  arabic_end_ = -1;
  arabic_char_cnt_ = 0;
  arabic_connect_cnt_ = 0;
}

void ObIKLetterProcessor::reset_mix_state()
{
  mix_start_ = -1;
  mix_end_ = -1;
  mix_char_cnt_ = 0;
}
} //  namespace storage
} //  namespace oceanbase
