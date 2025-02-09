/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE_FTS

#include "ob_ngram_ft_parser.h"
#include "storage/fts/ob_fts_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::plugin;

namespace oceanbase
{
namespace storage
{

#define true_word_char(ctype, character) ((ctype) & (_MY_U | _MY_L | _MY_NMR) || (character) == '_')


ObNgramFTParser::ObNgramFTParser()
  : cs_(nullptr),
    start_(nullptr),
    next_(nullptr),
    end_(nullptr),
    c_nums_(0),
    ngram_token_size_(plugin::ObFTParserParam::NGRAM_TOKEN_SIZE),
    is_inited_(false)
{}

ObNgramFTParser::~ObNgramFTParser()
{
  reset();
}

void ObNgramFTParser::reset()
{
  cs_ = nullptr;
  start_ = nullptr;
  next_ = nullptr;
  end_ = nullptr;
  c_nums_ = 0;
  ngram_token_size_ = plugin::ObFTParserParam::NGRAM_TOKEN_SIZE;
  is_inited_ = false;
}

int ObNgramFTParser::init(ObFTParserParam *param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), KPC(param), KPC(this));
  } else if (OB_ISNULL(param)
      || OB_ISNULL(param->cs_)
      || OB_ISNULL(param->fulltext_)
      || OB_UNLIKELY(0 >= param->ft_length_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KPC(param));
  } else {
    cs_ = param->cs_;
    start_ = param->fulltext_;
    next_ = start_;
    end_ = start_ + param->ft_length_;
    c_nums_ = 0;
    ngram_token_size_ = param->ngram_token_size_;
    is_inited_ = true;
  }
  if (OB_FAIL(ret) && OB_UNLIKELY(!is_inited_)) {
    reset();
  }
  return ret;
}

int ObNgramFTParser::get_next_token(
    const char *&word,
    int64_t &word_len,
    int64_t &char_len,
    int64_t &word_freq)
{
  int ret = OB_SUCCESS;
  word = nullptr;
  word_len = 0;
  char_len = 0;
  word_freq = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ngram ft parser isn't initialized", K(ret), K(is_inited_));
  } else {
    int64_t c_nums = c_nums_;
    const char *start = start_;
    const char *next = next_;
    const char *end = end_;
    const ObCharsetInfo *cs = cs_;
    if (next >= end) {
      ret = OB_ITER_END;
    } else {
      do {
        const int64_t c_len = ob_mbcharlen_ptr(cs, next, end);
        if (next + c_len > end || 0 == c_len) { // if char is invalid, just skip the rest of doc.
          ret = OB_ITER_END;
          break;
        } else {
          int ctype;
          cs->cset->ctype(cs, &ctype, (uchar *)next, (uchar *)end);
          if (1 == c_len && (' ' == *next || !true_word_char(ctype, *next))) {
            start = next + 1;
            next = start;
            c_nums = 0;
            if (next == end) {
              ret = OB_ITER_END;
            }
            continue;
          }
          next += c_len;
          ++c_nums;
        }
        if (ngram_token_size_ == c_nums) {
          word = start;
          word_len = next - start;
          char_len = c_nums;
          word_freq = 1;
          start += ob_mbcharlen_ptr(cs, start, end);
          c_nums = ngram_token_size_ - 1;
          break;
        } else if (next >= end) {
          ret = OB_ITER_END;
          break;
        }
      } while (OB_SUCC(ret) && next < end);
    }
    if (OB_ITER_END == ret || OB_SUCCESS == ret) {
      start_ = start;
      next_ = next;
      end_ = end;
      c_nums_ = c_nums;
    }
    LOG_DEBUG("next word", K(ret), K(ObString(word_len, word)), KP(start_), KP(next_), KP(end_));
  }
  return ret;
}

ObNgramFTParserDesc::ObNgramFTParserDesc()
  : is_inited_(false)
{
}

int ObNgramFTParserDesc::init(ObPluginParam *param)
{
  is_inited_ = true;
  return OB_SUCCESS;
}

int ObNgramFTParserDesc::deinit(ObPluginParam *param)
{
  reset();
  return OB_SUCCESS;
}

int ObNgramFTParserDesc::segment(
    ObFTParserParam *param,
    ObITokenIterator *&iter) const
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ngram ft parser desc hasn't be initialized", K(ret), K(is_inited_));
  } else if (OB_ISNULL(param) || OB_ISNULL(param->fulltext_) || OB_UNLIKELY(!param->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(param));
  } else if (OB_ISNULL(buf = param->allocator_->alloc(sizeof(ObNgramFTParser)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate ngram ft parser", K(ret));
  } else {
    ObNgramFTParser *parser = new (buf) ObNgramFTParser();
    if (OB_FAIL(parser->init(param))) {
      LOG_WARN("fail to init ngram fulltext parser", K(ret), KPC(param));
      param->allocator_->free(parser);
    } else {
      iter = parser;
    }
  }
  return ret;
}

void ObNgramFTParserDesc::free_token_iter(
    ObFTParserParam *param,
    ObITokenIterator *&iter) const
{
  if (OB_NOT_NULL(iter)) {
    abort_unless(nullptr != param);
    abort_unless(nullptr != param->allocator_);
    iter->~ObITokenIterator();
    param->allocator_->free(iter);
  }
}

int ObNgramFTParserDesc::get_add_word_flag(ObAddWordFlag &flag) const
{
  int ret = OB_SUCCESS;
  flag.set_casedown();
  flag.set_groupby_word();
  return ret;
}

} // end namespace storage
} // end namespace oceanbase
