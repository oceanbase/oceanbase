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

#include "storage/fts/ik/ob_ik_processor.h"

#include "lib/allocator/ob_allocator.h"
#include "lib/charset/ob_charset.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "storage/fts/ik/ob_ik_arbitrator.h"
#include "storage/fts/ik/ob_ik_char_util.h"
#include "storage/fts/ik/ob_ik_token.h"

namespace oceanbase
{
namespace storage
{
int ObIIKProcessor::process(TokenizeContext &ctx)
{
  int ret = OB_SUCCESS;

  ObFTCharUtil::CharType type;
  const char *ch = nullptr;
  uint8_t char_len = 0;

  if (OB_FAIL(ctx.current_char_and_type(ch, char_len, type))) {
    if (OB_LIKELY(OB_ITER_END == ret)) {
    } else {
      LOG_WARN("Failed to get current char and type", K(ret));
    }
  } else if (OB_FAIL(do_process(ctx, ch, char_len, type))) {
    LOG_WARN("Failed to do process char", K(ret));
  }
  return ret;
}

TokenizeContext::TokenizeContext(ObIAllocator &allocator)
    : coll_type_(CS_TYPE_INVALID), fulltext_(nullptr), fulltext_len_(0), cursor_(0),
      next_char_len_(0), handle_size_(0), is_smart_(false), token_list_(allocator),
      results_(allocator), result_idx_(0), cs_(nullptr), well_formed_len_(nullptr),
      cs_type_(CHARSET_INVALID), buffer_start_cursor_(0)
{
}

int TokenizeContext::init(ObCollationType coll_type,
                          const char *fulltext,
                          int64_t fulltext_len,
                          bool is_smart)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == fulltext || fulltext_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the are invalid arguments", K(ret), K(coll_type), KP(fulltext), K(fulltext_len));
  } else {
    coll_type_ = coll_type;
    fulltext_ = fulltext;
    fulltext_len_ = fulltext_len;
    is_smart_ = is_smart;
    cs_type_ = ObCharset::charset_type_by_coll(coll_type);

    if (OB_UNLIKELY(nullptr == (cs_ = static_cast<const ObCharsetInfo *>(ObCharset::get_charset(coll_type))))) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported charset or collation", K(ret), K(coll_type));
    } else if (OB_UNLIKELY(nullptr == cs_->cset)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the cset is null", K(ret));
    } else if (OB_UNLIKELY(nullptr == cs_->cset->well_formed_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the well_formed_len is null", K(ret));
    } else {
      well_formed_len_ = cs_->cset->well_formed_len;
      if (OB_FAIL(prepare_next_char())) {
        LOG_WARN("Failed to prepare next char", K(ret));
      }
    }
  }
  return ret;
}

int TokenizeContext::reuse_context(const char *fulltext, int64_t fulltext_len)
{
  int ret = OB_SUCCESS;
  fulltext_ = fulltext;
  fulltext_len_ = fulltext_len;
  cursor_ = 0;
  buffer_start_cursor_ = 0;
  if (OB_FAIL(reset_resource())) {
    LOG_WARN("Failed to reset resource", K(ret));
  } else if (OB_FAIL(prepare_next_char())) {
    LOG_WARN("Failed to prepare next char", K(ret));
  }
  return ret;
}

int TokenizeContext::reset_resource()
{
  handle_size_ = 0;
  results_.reuse();
  result_idx_ = 0;
  token_list_.reuse();
  return OB_SUCCESS;
}

int TokenizeContext::current_char_and_type(const char *&ch, uint8_t &char_len, ObFTCharUtil::CharType &type)
{
  int ret = OB_SUCCESS;
  if (cursor_ >= fulltext_len_) {
    ret = OB_ITER_END;
  } else {
    ch = fulltext_ + cursor_;
    char_len = next_char_len_;
    type = next_char_type_;
  }
  return ret;
}

int TokenizeContext::prepare_next_char()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObFTCharUtil::classify_first_valid_char(cs_type_,
                                                      fulltext_ + cursor_,
                                                      fulltext_len_ - cursor_,
                                                      well_formed_len_,
                                                      cs_,
                                                      next_char_len_,
                                                      next_char_type_))) {
    if (OB_LIKELY(OB_ITER_END == ret)) {
    } else {
      LOG_WARN("Failed to classify first valid char", K(ret));
    }
  }
  return ret;
}

int TokenizeContext::step_next()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cursor_ >= fulltext_len_)) {
    ret = OB_ITER_END;
  } else if (cursor_ + next_char_len_ >= fulltext_len_) {
    cursor_ = fulltext_len_;
    next_char_len_ = 0;
    ret = OB_ITER_END;
  } else if (cursor_ < fulltext_len_ && 0 == next_char_len_) {
    // should not happen
    ret = OB_UNEXPECT_INTERNAL_ERROR;
    LOG_WARN("Unexpected error", K(ret));
  } else {
    cursor_ += next_char_len_;
    handle_size_++;
    if (OB_FAIL(prepare_next_char())) {
      if (OB_LIKELY(OB_ITER_END == ret)) {
        fulltext_len_ = cursor_;
      } else {
        LOG_WARN("Failed to prepare next char", K(ret));
      }
    }
  }
  return ret;
}

ObCollationType TokenizeContext::collation() const { return coll_type_; }

int64_t TokenizeContext::get_end_cursor() const { return cursor_ + next_char_len_; }

const char *TokenizeContext::fulltext() const { return fulltext_; }

int64_t TokenizeContext::fulltext_len() const { return fulltext_len_; }

int64_t TokenizeContext::get_cursor() const { return cursor_; }

bool TokenizeContext::is_last() const { return cursor_ + next_char_len_ >= fulltext_len_; }

bool TokenizeContext::iter_end() const { return cursor_ >= fulltext_len_; }

bool TokenizeContext::is_smart() const { return is_smart_; }

bool TokenizeContext::is_results_exhaust() const { return result_idx_ >= results_.count(); }

int TokenizeContext::add_token(const char *fulltext,
                               int64_t offset,
                               int64_t length,
                               int64_t char_cnt,
                               ObIKTokenType type)
{
  int ret = OB_SUCCESS;
  ObIKToken token;
  token.ptr_ = fulltext;
  token.length_ = length;
  token.offset_ = offset;
  token.char_cnt_ = char_cnt;
  token.type_ = type;
  if (OB_FAIL(token_list_.add_token(token))) {
    LOG_WARN("Failed to add token to result list", K(ret));
  }
  return ret;
}

TokenizeContext::~TokenizeContext()
{
  token_list_.reset();
  results_.reset();
}

int TokenizeContext::get_next_token(const char *&word,
                                    int64_t &word_len,
                                    int64_t &offset,
                                    int64_t &char_cnt)
{
  int ret = OB_SUCCESS;
  if (result_idx_ < results_.count()) {
    ObIKToken &token = results_.at(result_idx_);
    ++result_idx_;
    if (result_idx_ < results_.count()) {
      if (OB_FAIL(compound(token))) {
        LOG_WARN("Failed to compound", K(ret));
      } else {
        // pass
      }
    }
    if (OB_SUCC(ret)) {
      word = token.ptr_;
      word_len = token.length_;
      offset = token.offset_;
      char_cnt = token.char_cnt_;
    }
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

int TokenizeContext::compound(ObIKToken &token)
{
  int ret = OB_SUCCESS;
  ObFastSegmentArray<ObIKToken> &list = results_;
  if (is_smart_) {
    if (result_idx_ < list.count()) {
      if (ObIKTokenType::IK_ARABIC_TOKEN == token.type_) {
        ObIKToken &next = list.at(result_idx_);
        bool append = false;

        if (ObIKTokenType::IK_CNNUM_TOKEN == next.type_) {
          // handle eng num + chn num
          if (token.offset_ + token.length_ == next.offset_) {
            append = true;
            token.length_ += next.length_;
            token.char_cnt_ += next.char_cnt_;
            token.type_ = ObIKTokenType::IK_CNNUM_TOKEN;
          }
        } else if (ObIKTokenType::IK_COUNT_TOKEN == next.type_) {
          // handle eng num + chn count
          if (token.offset_ + token.length_ == next.offset_) {
            append = true;
            token.length_ += next.length_;
            token.char_cnt_ += next.char_cnt_;
            token.type_ = ObIKTokenType::IK_CNQUAN_TOKEN;
          }
        } else {
          // pass
        }
        if (append) {
          ++result_idx_;
        }
      }
      // There may be another round of append
      if (OB_SUCC(ret) && result_idx_ < list.count()) {
        ObIKToken &next = list.at(result_idx_);
        bool append = false;
        if (ObIKTokenType::IK_COUNT_TOKEN == next.type_) {
          if (token.offset_ + token.length_ == next.offset_) {
            append = true;
            token.length_ += next.length_;
            token.type_ = ObIKTokenType::IK_CNQUAN_TOKEN;
          }
        }
        if (append) {
          ++result_idx_;
        }
      }
    }
  } else {
    // nothing todo, just return
  }
  return ret;
}

} // namespace storage

} // namespace oceanbase