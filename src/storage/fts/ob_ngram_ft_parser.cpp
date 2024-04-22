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

#include "lib/string/ob_string.h"
#include "ob_ngram_ft_parser.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace storage
{

#define true_word_char(ctype, character) ((ctype) & (_MY_U | _MY_L | _MY_NMR) || (character) == '_')

/*static*/ int ObNgramFTParser::segment(
  lib::ObFTParserParam *param,
  const char *fulltext,
  const int64_t ft_len)
{
  int ret = OB_SUCCESS;
  int64_t c_nums = 0;
  const char *start = fulltext;
  const char *next = start;
  const char *end = start + ft_len;
  if (OB_ISNULL(param) || OB_ISNULL(fulltext) || OB_UNLIKELY(ft_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(param), KP(fulltext), K(ft_len));
  } else {
    const ObCharsetInfo *cs = param->cs_;
    while (OB_SUCC(ret) && next < end) {
      const int64_t c_len = ob_mbcharlen_ptr(cs, next, end);
      if (next + c_len > end || 0 == c_len) { // if char is invalid, just skip the rest of doc.
        break;
      } else {
        int ctype;
        cs->cset->ctype(cs, &ctype, (uchar *)next, (uchar *)end);
        if (1 == c_len && (' ' == *next || !true_word_char(ctype, *next))) {
          start = next + 1;
          next = start;
          c_nums = 0;
          continue;
        }
        next += c_len;
        ++c_nums;
      }
      if (NGRAM_TOKEN_SIZE == c_nums) {
        if (OB_FAIL(add_word(param, start, next - start))) {
          LOG_WARN("fail to add word", K(ret), KP(param), KP(start), KP(next));
        } else {
          start += ob_mbcharlen_ptr(cs, start, end);
          c_nums = NGRAM_TOKEN_SIZE - 1;
        }
      }
    }
  }
  return ret;
}

/*static*/ int ObNgramFTParser::add_word(
    lib::ObFTParserParam *param,
    const char *word,
    int64_t word_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param)
      || OB_ISNULL(word)
      || OB_UNLIKELY(0 >= word_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KPC(param), KP(word), K(word_len));
  } else if (OB_FAIL(param->add_word(param, word, word_len))) {
    LOG_WARN("fail to add word", K(ret), KPC(param), K(ObString(word_len, word)));
  }
  return ret;
}

ObNgramFTParserDesc::ObNgramFTParserDesc()
  : is_inited_(false)
{
}

int ObNgramFTParserDesc::init(lib::ObPluginParam *param)
{
  is_inited_ = true;
  return OB_SUCCESS;
}

int ObNgramFTParserDesc::deinit(lib::ObPluginParam *param)
{
  reset();
  return OB_SUCCESS;
}

int ObNgramFTParserDesc::segment(lib::ObFTParserParam *param) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ngram ft parser desc hasn't be initialized", K(ret), K(is_inited_));
  } else if (OB_ISNULL(param) || OB_ISNULL(param->fulltext_) || OB_UNLIKELY(!param->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(param));
  } else if (OB_FAIL(ObNgramFTParser::segment(param, param->fulltext_, param->ft_length_))) {
    LOG_WARN("fail to segment words for fulltext by ngram", K(ret), KPC(param),
        K(param->fulltext_), K(param->ft_length_));
  }
  return ret;
}

} // end namespace storage
} // end namespace oceanbase
