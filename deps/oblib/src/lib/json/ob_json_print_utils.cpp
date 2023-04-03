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

#define USING_LOG_PREFIX LIB
#include "lib/json/ob_json_print_utils.h"
#include "lib/ob_define.h"

namespace oceanbase
{
namespace json
{
using namespace common;

int ObStdJsonConvertor::init(const char *json, char *buf, const int64_t buf_size)
{
  // reset
  *this = ObStdJsonConvertor();

  int ret = OB_SUCCESS;
  if (OB_ISNULL(json) || OB_ISNULL(buf) || OB_UNLIKELY(0 == buf_size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arugment", KP(json), KP(buf), K(buf_size));
  } else {
    json_ = json;
    buf_ = buf;
    buf_size_ = buf_size;
  }
  return ret;
}

int ObStdJsonConvertor::convert(int64_t &out_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(json_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init");
  } else {
    out_len = 0;
    const char *p = json_;
    const char *begin = json_;
    bool in_string = false;
    if (OB_ISNULL(p)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("json_ is NULL", K(ret));
    } else {}
    while (OB_SUCC(ret) && *p) {
      if (in_string) {
        if ('\\' == *p && backslash_escape_) {
          if (*(p + 1)) {
            p++;
          } else {}
        } else if ('"' == *p) {
          in_string = false;
          if (OB_FAIL(output(p, begin, out_len))) {
            LOG_WARN("fail to output", K(ret));
          } else {}
        } else {}
      } else {
        switch (*p) {
          case '"': {
            in_string = true;
            break;
          }
          case '[':
          case ']':
          case ',':
          case '{':
          case '}': {
            if (OB_FAIL(output(p, begin, out_len))) {
              LOG_WARN("fail to output", K(ret));
            } else {}
            break;
          }
          case ':': {
            if (OB_FAIL(quoted_output(p, begin, out_len))) {
              LOG_WARN("fail to quoted output", K(ret));
            } else {}
            break;
          }
          default: {
          }
        }
      }
      p++;
    }
    if (OB_SUCC(ret) && begin < p) {
      if (OB_FAIL(output(p - 1, begin, out_len))) {
        LOG_WARN("fail to output", K(ret));
      } else {}
    }
  }
  return ret;
}

int ObStdJsonConvertor::output(const char *p, const char *&begin, int64_t &out_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(p < begin)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("p < begin", K(ret), KP(p), KP(begin));
  } else {
    int64_t len = p - begin + 1;
    if (len > buf_size_ - pos_) {
      len = buf_size_ - pos_;
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("buf not enouth", K_(buf_size), "json_len", strlen(json_));
    } else {}
    // copy to %buf_, even buffer not enough
    if (len > 0) {
      MEMCPY(buf_ + pos_, begin, len);
      pos_ += len;
      out_len += len;
      begin = p + 1;
    } else {}
  }
  return ret;
}

int ObStdJsonConvertor::add_quote_mark(int64_t &out_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(pos_ >= buf_size_)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buf not enouth", K_(buf_size), "json_len", strlen(json_));
  } else {
    buf_[pos_++] = '"';
    out_len++;
  }
  return ret;
}

int ObStdJsonConvertor::quoted_output(const char *p, const char *&begin, int64_t &out_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(p) || OB_UNLIKELY(p < begin) || OB_UNLIKELY(':' != *p)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("p is NULL or p < begin or ':' != *p", K(ret), KP(p), KP(begin));
  } else {
    const char *name_begin = begin;
    const char *name_end = p - 1;
    if (OB_ISNULL(name_begin) || OB_ISNULL(name_end)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("name_begin or name_end is NULL", K(ret), KP(name_begin), KP(name_end));
    } else {
      // trim
      while (name_begin <= name_end && isspace(*name_begin)) {
        name_begin++;
      }
      while (name_begin <= name_end && isspace(*name_end)) {
        name_end--;
      }
      if (name_begin > name_end) { // no valid name
        if (OB_FAIL(output(p, begin, out_len))) {
          LOG_WARN("fail to output", K(ret));
        } else {}
      } else {
        if (name_begin > begin) {
          if (OB_FAIL(output(name_begin - 1, begin, out_len))) {
            LOG_WARN("fail to output", K(ret));
          } else {}
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(add_quote_mark(out_len))) {
          LOG_WARN("fail to add quote mark", K(ret));
        } else if (OB_FAIL(output(name_end, begin, out_len))) {
          LOG_WARN("fail to add output", K(ret));
        } else if (OB_FAIL(add_quote_mark(out_len))) {
          LOG_WARN("fail to add quote mark", K(ret));
        } else if (OB_FAIL(output(p, begin, out_len))) {
          LOG_WARN("fail to add output", K(ret));
        } else {}
      }
    }
  }

  return ret;
}

} // end namespace json
} // end namespace oceanbase

