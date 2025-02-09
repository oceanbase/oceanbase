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

#include "storage/fts/ik/ob_ik_quantifier_processor.h"

#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "storage/fts/dict/ob_ft_dict_hub.h"
#include "storage/fts/ik/ob_ik_char_util.h"
#include "storage/fts/ik/ob_ik_token.h"

namespace oceanbase
{
namespace storage
{
int ObIKQuantifierProcessor::do_process(TokenizeContext &ctx,
                                        const char *ch,
                                        const uint8_t char_len,
                                        const ObFTCharUtil::CharType type)
{
  int ret = OB_SUCCESS;
  // it do have an order, process number first
  if (OB_FAIL(process_CN_number(ctx, ch, char_len, type))) {
    LOG_WARN("Fail to process CN number", K(ret));
  } else if (OB_FAIL(process_CN_count(ctx, ch, char_len, type))) {
    LOG_WARN("Fail to process CN count", K(ret));
  }
  return ret;
}

int ObIKQuantifierProcessor::process_CN_number(TokenizeContext &ctx,
                                               const char *ch,
                                               const uint8_t char_len,
                                               const ObFTCharUtil::CharType type)
{
  int ret = OB_SUCCESS;
  bool is_cn_number = false;

  if (ObFTCharUtil::CharType::CHINESE == type
      && OB_FAIL(ObFTCharUtil::check_cn_number(ctx.collation(), ch, char_len, is_cn_number))) {
    LOG_WARN("Fail to check is cn number", K(ret));
  } else if (start_ == -1 && end_ == -1) {
    if (is_cn_number) {
      start_ = ctx.get_cursor();
      end_ = ctx.get_end_cursor();
      quan_char_cnt_++;
    } else {
      // pass
    }
  } else if (is_cn_number) {
    // start has been set
    end_ = ctx.get_end_cursor();
    quan_char_cnt_++;
  } else if (OB_FAIL(output_num_token(ctx))) {
    LOG_WARN("Failed to output number token", K(ret));
  } else {
    reset();
  }

  if (OB_SUCC(ret)) {
    if (ctx.is_last() && (start_ != -1 && end_ != -1)) {
      if (OB_FAIL(output_num_token(ctx))) {
        LOG_WARN("Failed to output number token", K(ret));
      }
    }
    if (ctx.is_last()) {
      reset();
    }
  } else {
    reset();
  }
  return ret;
}

int ObIKQuantifierProcessor::process_CN_count(TokenizeContext &ctx,
                                              const char *ch,
                                              const uint8_t char_len,
                                              const ObFTCharUtil::CharType type)
{
  int ret = OB_SUCCESS;

  if (need_count_scan(ctx)) {
    if (ObFTCharUtil::CharType::CHINESE == type) {
      // handle existing
      if (!count_hits_.empty()) {
        for (ObDATrieHit &hit : count_hits_) {
          if (OB_FAIL(ret)) {
            break;
          } else if (OB_FAIL(quan_dict_.match({char_len, ch}, hit))) {
            LOG_WARN("Failed to match quantifier.", K(ret));
          } else if (hit.is_match()) {
            if (OB_FAIL(ctx.add_token(ctx.fulltext(),
                                      hit.start_pos_,
                                      hit.end_pos_ - hit.start_pos_,
                                      hit.char_cnt_,
                                      ObIKTokenType::IK_COUNT_TOKEN))) {
              LOG_WARN("Failed to add token", K(ret));
            } else {
              // add ok
            }
          } else if (hit.is_prefix()) {
            // wait another match
          } else if (hit.is_unmatch()) {
            count_hits_.erase(hit);
          } else {
            ret = OB_UNEXPECT_INTERNAL_ERROR;
            LOG_WARN("Reached an imposible state.");
          }
        }
      } else {
        // pass
      }

      // handle new
      if (OB_FAIL(ret)) {
        // already logged
      } else {
        // find dict

        ObDATrieHit hit(&quan_dict_, ctx.get_cursor());
        if (OB_FAIL(quan_dict_.match({char_len, ch}, hit))) {
          LOG_WARN("Fail to match", K(ret));
        } else if (hit.is_match()) {
          if (OB_FAIL(ctx.add_token(ctx.fulltext(),
                                    hit.start_pos_,
                                    hit.end_pos_ - hit.start_pos_,
                                    hit.char_cnt_,
                                    ObIKTokenType::IK_COUNT_TOKEN))) {
            LOG_WARN("Failed to add token", K(ret));
          } else if (OB_FAIL(count_hits_.push_back(hit))) {
            LOG_WARN("Failed to push hit list", K(ret));
          }
        } else if (hit.is_prefix()) {
          if (OB_FAIL(count_hits_.push_back(hit))) {
            LOG_WARN("Failed to push hit list", K(ret));
          }
        } else if (hit.is_unmatch()) {
          count_hits_.clear();
        } else {
          ret = OB_UNEXPECT_INTERNAL_ERROR;
          LOG_WARN("Reached an imposible state.", K(ret));
        }
      }
    } else {
      count_hits_.clear();
    }

    if (OB_FAIL(ret)) {
      // already logged
    } else if (ctx.is_last()) {
      count_hits_.clear();
    } else {
    }
  } else {
  }
  return ret;
}

bool ObIKQuantifierProcessor::need_count_scan(TokenizeContext &ctx)
{
  if ((start_ != -1 && end_ != -1) || !count_hits_.empty()) {
    return true;
  } else {
    // find a near count
    if (!ctx.token_list().is_empty()) {
      ObIKToken &token = ctx.token_list().tokens().get_last();
      if ((token.type_ == ObIKTokenType::IK_CNNUM_TOKEN
           || token.type_ == ObIKTokenType::IK_ARABIC_TOKEN)
          && token.offset_ + token.length_ == ctx.get_cursor()) {
        return true;
      }
    }
  }
  return false;
}

int ObIKQuantifierProcessor::output_num_token(TokenizeContext &ctx)
{
  int ret = OB_SUCCESS;
  if (start_ > -1 && end_ > -1) {
    ret = ctx.add_token(ctx.fulltext(),
                        start_,
                        end_ - start_,
                        quan_char_cnt_,
                        ObIKTokenType::IK_CNNUM_TOKEN);
  }
  return ret;
}

void ObIKQuantifierProcessor::reset()
{
  start_ = -1;
  end_ = -1;
  quan_char_cnt_ = 0;
}

} //  namespace storage
} //  namespace oceanbase
