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
#include "storage/fts/ob_whitespace_ft_parser.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace storage
{

#define true_word_char(ctype, character) ((ctype) & (_MY_U | _MY_L | _MY_NMR) || (character) == '_')

int ObSpaceFTParser::segment(
    lib::ObFTParserParam *param,
    const char *ft,
    const int64_t ft_len)
{
  int ret = OB_SUCCESS;
  const char *start = ft;
  const char *next = start;
  const char *end = start + ft_len;
  int mbl = 0;
  if (OB_ISNULL(param) || OB_ISNULL(param->cs_) || OB_ISNULL(ft) || OB_UNLIKELY(0 >= ft_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KPC(param), KP(ft), K(ft_len));
  } else {
    const ObCharsetInfo *cs = param->cs_;
    while (OB_SUCC(ret) && next < end) {
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
        if (0 < c_nums && OB_FAIL(add_word(param, start, next - start, c_nums))) {
          LOG_WARN("fail to add word", K(ret), KPC(param), KP(start), K(next));
        } else {
          start = next;
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObSpaceFTParser::add_word(
    lib::ObFTParserParam *param,
    const char *word,
    const int64_t word_len,
    const int64_t char_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param)
      || OB_ISNULL(word)
      || OB_UNLIKELY(0 >= word_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KPC(param), KP(word), K(word_len));
  } else if (OB_FAIL(param->add_word(param, word, word_len, char_cnt))) {
    LOG_WARN("fail to add word", K(ret), KPC(param), K(char_cnt),  K(ObString(word_len, word)));
  }
  return ret;
}

ObWhiteSpaceFTParserDesc::ObWhiteSpaceFTParserDesc()
  : is_inited_(false)
{
}

int ObWhiteSpaceFTParserDesc::init(lib::ObPluginParam *param)
{
  is_inited_ = true;
  return OB_SUCCESS;
}

int ObWhiteSpaceFTParserDesc::deinit(lib::ObPluginParam *param)
{
  reset();
  return OB_SUCCESS;
}

int ObWhiteSpaceFTParserDesc::segment(lib::ObFTParserParam *param) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("default ft parser desc hasn't be initialized", K(ret), K(is_inited_));
  } else if (OB_ISNULL(param) || OB_ISNULL(param->fulltext_) || OB_UNLIKELY(!param->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(param));
  } else if (OB_FAIL(ObSpaceFTParser::segment(param, param->fulltext_, param->ft_length_))) {
    LOG_WARN("fail to segment words for fulltext by spaces", K(ret), KPC(param),
        K(param->fulltext_), K(param->ft_length_));
  }
  return ret;
}

} // end namespace storage
} // end namespace oceanbase
