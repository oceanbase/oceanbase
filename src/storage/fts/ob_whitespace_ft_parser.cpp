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

#include "storage/fts/ob_whitespace_ft_parser.h"

#include "storage/fts/ob_whitespace_ft_parser.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_string.h"
#include "storage/fts/ob_fts_literal.h"
#include "storage/fts/ob_fts_parser_property.h"
#include "storage/fts/ob_fts_struct.h"

#define USING_LOG_PREFIX STORAGE_FTS

using namespace oceanbase::common;
using namespace oceanbase::plugin;

namespace oceanbase
{
namespace storage
{

#define true_word_char(ctype, character) ((ctype) & (_MY_U | _MY_L | _MY_NMR) || (character) == '_')

ObSpaceFTParser::ObSpaceFTParser()
  : cs_(nullptr),
    start_(nullptr),
    next_(nullptr),
    end_(nullptr),
    is_inited_(false)
{}

ObSpaceFTParser::~ObSpaceFTParser()
{
  reset();
}

void ObSpaceFTParser::reset()
{
  cs_ = nullptr;
  start_ = nullptr;
  next_ = nullptr;
  end_ = nullptr;
  is_inited_ = false;
}

int ObSpaceFTParser::init(ObFTParserParam *param)
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
    is_inited_ = true;
  }
  if (OB_FAIL(ret) && OB_UNLIKELY(!is_inited_)) {
    reset();
  }
  return ret;
}

int ObSpaceFTParser::get_next_token(const char *&word,
                                    int64_t &word_len,
                                    int64_t &char_len,
                                    int64_t &word_freq) // word_freq is always 1 ?
{
  int ret = OB_SUCCESS;
  int mbl = 0;
  word = nullptr;
  word_len = 0;
  char_len = 0;
  word_freq = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("space ft parser isn't initialized", K(ret), K(is_inited_));
  } else {
    const char *start = start_;
    const char *next = next_;
    const char *end = end_;
    const ObCharsetInfo *cs = cs_;
    do {
      while (next < end) {
        int ctype;
        mbl = cs->cset->ctype(cs, &ctype, (uchar *)next, (uchar *)end);
        if (true_word_char(ctype, *next)) {
          break;
        }
        next += mbl > 0 ? mbl : (mbl < 0 ? -mbl : 1);
      }
      if (next >= end) {
        ret = OB_ITER_END;
      } else {
        int64_t c_nums = 0;
        start = next;
        while (next < end) {
          int ctype;
          mbl = cs->cset->ctype(cs, &ctype, (uchar *)next, (uchar *)end);
          if (!true_word_char(ctype, *next)) {
            break;
          }
          ++c_nums;
          next += mbl > 0 ? mbl : (mbl < 0 ? -mbl : 1);
        }
        if (0 < c_nums) {
          word = start;
          word_len = next - start;
          char_len = c_nums;
          word_freq = 1;
          start = next;
          break;
        } else {
          start = next;
        }
      }
    } while (OB_SUCC(ret) && next < end);
    if (OB_ITER_END == ret || OB_SUCCESS == ret) {
      start_ = start;
      next_ = next;
      end_ = end;
    }
    LOG_DEBUG("next word", K(ObString(word_len, word)), KP(start_), KP(next_), KP(end_));
  }
  return ret;
}

ObWhiteSpaceFTParserDesc::ObWhiteSpaceFTParserDesc()
  : is_inited_(false)
{
}

int ObWhiteSpaceFTParserDesc::init(ObPluginParam *param)
{
  is_inited_ = true;
  return OB_SUCCESS;
}

int ObWhiteSpaceFTParserDesc::deinit(ObPluginParam *param)
{
  reset();
  return OB_SUCCESS;
}

int ObWhiteSpaceFTParserDesc::segment(
    ObFTParserParam *param,
    ObITokenIterator *&iter) const
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("default ft parser desc hasn't be initialized", K(ret), K(is_inited_));
  } else if (OB_ISNULL(param) || OB_ISNULL(param->fulltext_) || OB_UNLIKELY(!param->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(param));
  } else if (OB_ISNULL(buf = param->allocator_->alloc(sizeof(ObSpaceFTParser)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate space ft parser", K(ret));
  } else {
    ObSpaceFTParser *parser = new (buf) ObSpaceFTParser();
    if (OB_FAIL(parser->init(param))) {
      LOG_WARN("fail to init whitespace fulltext parser", K(ret), KPC(param));
      param->allocator_->free(parser);
    } else {
      iter = parser;
    }
  }
  return ret;
}

void ObWhiteSpaceFTParserDesc::free_token_iter(
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

int ObWhiteSpaceFTParserDesc::get_add_word_flag(ObAddWordFlag &flag) const
{
  int ret = OB_SUCCESS;
  flag.set_min_max_word();
  flag.set_stop_word();
  flag.set_casedown();
  flag.set_groupby_word();
  return ret;
}

} // end namespace storage
} // end namespace oceanbase
