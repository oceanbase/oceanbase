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

#define USING_LOG_PREFIX SHARE

#include <string.h>
#include <ctype.h>
#include "ob_kv_parser.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace share
{

int ObKVParser::match(int sym)
{
  int ret = OB_SUCCESS;
  if (sym != token_) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ret = get_token();
  }
  return ret;
}

int ObKVParser::emit(int sym)
{
  int ret = OB_SUCCESS;
  if (sym != token_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(sym), K_(token), K(ret));
  } else {
    if (sym == SYM_VALUE && NULL != cb_) {
      if (OB_FAIL(cb_->match(key_buf_, value_buf_))) {
        LOG_WARN("fail match callback", K_(key_buf), K_(value_buf));
      }
    }
    if (OB_SUCC(ret)) {
      ret = get_token();
    }
  }
  return ret;
}

int ObKVParser::get_token()
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    if ('\0' == *cur_) { // NOTE: When encountering 0, always end, ignore data_length
      token_ = SYM_END;
      break;
    } else if (data_length_ <= cur_ - data_) {
      token_ = SYM_END;
      break;
    } else if (SYM_KV_SEP == *cur_) {
      cur_++;
      token_ = SYM_KV_SEP;
      break;
    } else if (SYM_PAIR_SEP == *cur_) {
      cur_++;
      token_ = SYM_PAIR_SEP;
      break;
    } else if (isspace(*cur_)) {
      if (allow_space_) {
        cur_++; // Skip the spaces before and after the token
      } else {
        ret = OB_INVALID_ARGUMENT;
      }
    } else if (isprint(*cur_)) {
      const char *start = cur_;
      while (isprint(*cur_) && !isspace(*cur_) && SYM_KV_SEP != *cur_ && SYM_PAIR_SEP != *cur_) {
        cur_++;
      }
      if (cur_ - start >= MAX_TOKEN_SIZE) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("token size is too large", "actual", cur_ - start, K(ret));
      } else {
        if (SYM_KV_SEP == token_) {
          // value part
          STRNCPY(value_buf_, start, cur_ - start);
          value_buf_[cur_ - start] = '\0';
        } else {
          // key part
          STRNCPY(key_buf_, start, cur_ - start);
          key_buf_[cur_ - start] = '\0';
        }
        token_ = SYM_VALUE;
      }
      break;
    } else {
      // Unknown character
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unexpected char", "c", *cur_, K(ret));
    }
  }
  return ret;
}

int ObKVParser::parse(const char *data)
{
  int64_t data_length = STRLEN(data);
  return parse(data, data_length);
}

int ObKVParser::parse(const char *data, int64_t data_length)
{
  int ret = OB_SUCCESS;
  bool finish = false;
  const int64_t length = data_length + 1;
  if (OB_ISNULL(data) || data_length <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null data ptr", K(ret));
  } else if (OB_ISNULL(data_ = static_cast<char*>(allocator_.alloc(length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret), K(length));
  } else {
    MEMSET(data_, '\0', length);
    STRNCPY(data_, data, data_length);
    cur_ = data_;
    data_length_ = length;
    if (OB_FAIL(get_token())) {
      LOG_WARN("failed to  get_token", K(ret));
    }
  }
  while (OB_SUCC(ret) && !finish) {
    if (SYM_END == token_) {
      if (OB_FAIL(match(SYM_END))) {
        LOG_WARN("fail match SYM_END");
      } else {
        finish = true;
      }
    } else if (SYM_VALUE == token_) {
      if (OB_FAIL(kv_pair())) {
        LOG_WARN("fail match kv");
      }
    } else if (SYM_PAIR_SEP == token_) {
      if (OB_FAIL(match(SYM_PAIR_SEP))) {
        LOG_WARN("fail match SYM_PAIR_SEP");
      } else if (OB_FAIL(kv_pair())) {
        LOG_WARN("fail match kv");
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("failed to parse", K(ret), K(token_));
    }
  }
  if (OB_SUCC(ret) && NULL != cb_) {
      if (false == cb_->check()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("fail check parsed data", K(ret));
      }
  }
  return ret;
}

int ObKVParser::kv_pair()
{
  int ret = OB_SUCCESS;
  switch (token_) {
    case SYM_VALUE:
      if (OB_FAIL(match(SYM_VALUE))) {
        LOG_WARN("fail match key");
      } else if (OB_FAIL(match(SYM_KV_SEP))) {
        LOG_WARN("fail match kv seperator");
      } else if (OB_FAIL(emit(SYM_VALUE))) {
        LOG_WARN("fail match value");
      }
      break;
    default:
      ret = OB_INVALID_ARGUMENT;
      break;
  }
  return ret;
}


}/* ns share*/
}/* ns oceanbase */
