/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SHARE

#include "share/text_analysis/ob_token_stream.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace share
{

ObTextTokenizer::ObTextTokenizer()
  : ObITokenStream(),
    input_doc_(nullptr),
    cs_(nullptr),
    iter_end_(false),
    is_inited_(false)
{
}

void ObTextTokenizer::reset()
{
  input_doc_ = nullptr;
  cs_ = nullptr;
  iter_end_ = false;
  is_inited_ = false;
}

int ObTextTokenizer::open(const ObDatum &document, const ObCharsetInfo *cs)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double init", K(ret), K_(is_inited), K_(iter_end), K_(cs), KPC_(input_doc));
  } else if (OB_ISNULL(cs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Charset info is nullptr", K(ret), K(cs));
  } else if (OB_UNLIKELY(document.is_outrow())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("out row document not supported for tokenizer yet", K(ret), K(document));
  } else {
    input_doc_ = &document;
    cs_ = cs;
    if (document.is_null() || 0 == document.len_) {
      iter_end_ = true;
    } else if (OB_FAIL(inner_open(document, cs_))) {
      LOG_WARN("failed to open document for tokenization", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

ObTextWhitespaceTokenizer::ObTextWhitespaceTokenizer()
  : ObTextTokenizer(),
    curr_token_ptr_(nullptr),
    trav_pos_(0)
{
}

void ObTextWhitespaceTokenizer::reset()
{
  curr_token_ptr_ = nullptr;
  trav_pos_ = 0;
  ObTextTokenizer::reset();
}

int ObTextWhitespaceTokenizer::inner_open(const ObDatum &document, const ObCharsetInfo *cs)
{
  int ret = OB_SUCCESS;
  curr_token_ptr_ = nullptr;
  trav_pos_ = 0;
  return ret;
}

int ObTextWhitespaceTokenizer::get_next(ObDatum &next_token, int64_t &token_freq)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (iter_end_) {
    ret = OB_ITER_END;
  } else {
    const char *doc = input_doc_->ptr_;
    const uint32_t doc_len = get_input_buf_len();
    int64_t token_len = 0;
    // to next non-whitespace pos
    while (OB_SUCC(ret) && found_delimiter()) {
      const int64_t c_len = ob_mbcharlen_ptr(cs_, doc + trav_pos_, doc + doc_len);
      trav_pos_ += c_len;
      if (trav_pos_ >= doc_len || 0 == c_len) {// if char is invalid, just skip the rest of document
        iter_end_ = true;
        ret = OB_ITER_END;
      }
    }

    if (OB_SUCC(ret)) {
      curr_token_ptr_ = get_trav_ptr();
    }

    // to next whitespace pos
    while (OB_SUCC(ret) && !found_delimiter()) {
      const int64_t c_len = ob_mbcharlen_ptr(cs_, doc + trav_pos_, doc + doc_len);
      trav_pos_ += c_len;
      token_len += c_len;
      if (trav_pos_ >= doc_len || 0 == c_len) {
        iter_end_ = true;
        if (0 == token_len) {
          ret = OB_ITER_END;
        }
        break;
      }
    }

    if (OB_SUCC(ret)) {
      next_token.set_string(curr_token_ptr_, static_cast<uint32_t>(token_len));
      token_freq = 1;
      LOG_DEBUG("[TEXT ANALYSIS] got next token", K(ret), K(next_token), K(next_token.get_string()),
          K(input_doc_->len_), K_(trav_pos));
    }
  }
  return ret;
}

bool ObTextWhitespaceTokenizer::found_delimiter()
{
  bool found = false;
  const char *curr_char = input_doc_->ptr_ + trav_pos_;
  found = ob_isspace(cs_, *curr_char) || ob_iscntrl(cs_, *curr_char) || ob_ispunct(cs_, *curr_char);
  return found;
}

ObTokenNormalizer::ObTokenNormalizer()
  : in_stream_(nullptr),
    cs_(nullptr),
    is_inited_(false)
{
}

void ObTokenNormalizer::reset()
{
  in_stream_ = nullptr;
  cs_ = nullptr;
  is_inited_ = false;
}

void ObTokenNormalizer::reuse()
{
  if (nullptr != in_stream_) {
    in_stream_->reuse();
  }
}

int ObTokenNormalizer::init(const ObCharsetInfo *cs, ObITokenStream &in_stream)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double initialization", K(ret));
  } else if (OB_ISNULL(cs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argment, charset info is nullptr", K(ret), KP(cs));
  } else {
    in_stream_ = &in_stream;
    cs_ = cs;
    if (OB_FAIL(inner_init(cs_, in_stream))) {
      LOG_WARN("failed to inner init token normalizer", K(ret));
    }
    is_inited_ = true;
  }
  return ret;
}

int ObTokenStopWordNormalizer::get_next(ObDatum &next_token, int64_t &token_freq)
{
  int ret = OB_SUCCESS;
  bool found_next_valid_token = false;
  token_freq = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  }

  while (OB_SUCC(ret) && !found_next_valid_token) {
    // Only filter out pure punctuation / control mark tokens for now
    if (OB_FAIL(in_stream_->get_next(next_token, token_freq))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get next token from in stream", K(ret), KPC_(in_stream));
      }
    } else if (OB_FAIL(filter_special_marks(next_token, found_next_valid_token))) {
      LOG_WARN("failed to filter special marks", K(ret), K(next_token), KP_(cs), KPC_(in_stream));
    } else if (!found_next_valid_token) {
      next_token.reset();
    }
  }
  return ret;
}

int ObTokenStopWordNormalizer::filter_special_marks(const ObDatum &check_token, bool &is_valid)
{
  int ret = OB_SUCCESS;
  const int64_t token_len = check_token.len_;
  const char *token = check_token.ptr_;
  int64_t special_mark_cnt = 0;
  for (int64_t i = 0; i < token_len; ++i) {
    const char *character = token + i;
    if (ob_ispunct(cs_, *character) || ob_iscntrl(cs_, *character)) {
      special_mark_cnt++;
    }
  }
  is_valid = !(special_mark_cnt == token_len);
  return ret;
}

ObBasicEnglishNormalizer::ObBasicEnglishNormalizer()
  : ObTokenNormalizer(),
    norm_allocator_("TxtTokGrpFilter", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID())
{
}

void ObBasicEnglishNormalizer::reset()
{
  norm_allocator_.reset();
  ObTokenNormalizer::reset();
}

void ObBasicEnglishNormalizer::reuse()
{
  norm_allocator_.reuse();
  ObTokenNormalizer::reuse();
}

int ObBasicEnglishNormalizer::get_next(ObDatum &next_token, int64_t &token_freq)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    next_token.reset();
    token_freq = 0;
    norm_allocator_.reuse();
    ObDatum tmp_datum;
    bool found_alnum = false;
    uint32_t norm_token_len = 0;
    const char *norm_token_ptr = nullptr;
    while (OB_SUCC(ret) && !found_alnum) {
      tmp_datum.reset();
      if (OB_FAIL(in_stream_->get_next(tmp_datum, token_freq))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to get next datum", K(ret));
        }
      } else {
        // trim leading and trailing non-alnum characters
        const char *token = tmp_datum.ptr_;
        const uint32_t raw_token_len = tmp_datum.len_;
        uint32_t first_alnum_pos = 0;
        uint32_t last_alnum_pos = raw_token_len - 1;
        for (uint32_t i = 0; i < raw_token_len; ++i) {
          const char *character = token + i;
          if (ob_isalnum(cs_, *character)) {
            first_alnum_pos = i;
            found_alnum = true;
            break;
          }
        }

        for (int32_t i = raw_token_len - 1; 0 <= i && i < raw_token_len && i >= first_alnum_pos; --i) {
          const char *character = token + i;
          if (ob_isalnum(cs_, *character)) {
            last_alnum_pos = i;
            found_alnum = true;
            break;
          }
        }

        if (!found_alnum) {
          // skip
        } else if (OB_UNLIKELY(last_alnum_pos < first_alnum_pos)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected alnum char pos", K(ret), K(last_alnum_pos), K(first_alnum_pos));
        } else {
          norm_token_len = last_alnum_pos - first_alnum_pos + 1;
          norm_token_ptr = token + first_alnum_pos;
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(!found_alnum)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected unfounded alnum in token", K(ret), K(tmp_datum));
    } else {
      ObString norm_alnum_token(norm_token_len, norm_token_ptr);
      ObString norm_lower_token;
      if (OB_FAIL(ObCharset::tolower(cs_, norm_alnum_token, norm_lower_token, norm_allocator_))) {
        LOG_WARN("norm token to lower case failed", K(ret), K_(cs), K(norm_alnum_token));
      } else {
        next_token.set_string(norm_lower_token);
      }
    }
  }
  return ret;
}


ObTextTokenGroupNormalizer::ObTextTokenGroupNormalizer()
  : ObTokenNormalizer(),
    token_allocator_("TxtTokGrpFilter", OB_MALLOC_MIDDLE_BLOCK_SIZE, MTL_ID()),
    grouping_map_(),
    map_iter_(),
    map_end_iter_(),
    in_stream_iter_end_(false)
{
}

void ObTextTokenGroupNormalizer::reset()
{
  grouping_map_.destroy();
  in_stream_iter_end_ = false;
  token_allocator_.reset();
  ObTokenNormalizer::reset();
}

void ObTextTokenGroupNormalizer::reuse()
{
  grouping_map_.reuse();
  in_stream_iter_end_ = false;
  token_allocator_.reuse();
  ObTokenNormalizer::reuse();
}

int ObTextTokenGroupNormalizer::inner_init(const ObCharsetInfo *cs, ObITokenStream &in_stream)
{
  // TODO: use resaonable initialize bucket cnt
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments, charset info is nullptr", K(ret), KP(cs));
  } else if (OB_FAIL(grouping_map_.create(
      DEFAULT_HASH_MAP_BUCKET_CNT,
      "TxtTokGrpHash",
      "TxtTokGrpHash",
      MTL_ID()))) {
    LOG_WARN("failed to create grouping hash map", K(ret), K(MTL_ID()));
  }
  return ret;
}

int ObTextTokenGroupNormalizer::get_next(ObDatum &next_token, int64_t &token_freq)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!in_stream_iter_end_ && OB_FAIL(build_grouping_map())) {
    LOG_WARN("failed to build text token grouping map", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (map_iter_ == map_end_iter_) {
      ret = OB_ITER_END;
    } else {
      next_token.set_string(map_iter_->first);
      token_freq = map_iter_->second;
      ++map_iter_;
    }
  }
  return ret;
}

int ObTextTokenGroupNormalizer::build_grouping_map()
{
  int ret = OB_SUCCESS;
  ObDatum token;
  if (OB_UNLIKELY(in_stream_iter_end_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("already iterated in stream", K(ret), K_(in_stream_iter_end), KPC_(in_stream));
  }

  while (OB_SUCC(ret)) {
    int64_t grouped_token_freq = 0;
    int64_t curr_token_freq = 0;
    if (OB_FAIL(in_stream_->get_next(token, curr_token_freq))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get next token from in stream", K(ret), KPC_(in_stream));
      }
    } else {
      ObString token_string = token.get_string();
      int hash_ret = grouping_map_.get_refactored(token_string, grouped_token_freq);
      if (OB_HASH_NOT_EXIST == hash_ret) {
        ObString copied_token_string;
        if (OB_FAIL(ob_write_string(token_allocator_, token_string, copied_token_string))) {
          LOG_WARN("failed to copy token string", K(ret), K(token_string));
        } else if (OB_FAIL(grouping_map_.set_refactored(copied_token_string, curr_token_freq))) {
          LOG_WARN("failed to put first token in grouping map", K(ret), K(token), K(copied_token_string));
        }
      } else if (OB_SUCCESS == hash_ret) {
        // add token_freq in hash map directly, to avoid deep copy token string
        *grouping_map_.get(token_string) += curr_token_freq;
      } else {
        ret = hash_ret;
        LOG_WARN("failed to get value from grouping map", K(ret), K(token), K(token_string));
      }
    }
  }

  if (OB_UNLIKELY(ret != OB_ITER_END)) {
    LOG_WARN("failed to iterate in token stream", K(ret), KPC_(in_stream));
  } else {
    ret = OB_SUCCESS;
    map_end_iter_ = grouping_map_.end();
    map_iter_ = grouping_map_.begin();
    in_stream_iter_end_ = true;
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
