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

#include "lib/alloc/alloc_struct.h"
#include "lib/ob_define.h"
#include "lib/coro/co_var.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_fast_convert.h"

using namespace oceanbase;
using namespace lib;
using namespace common;

bool ObLabel::operator==(const ObLabel& other) const
{
  bool bret = false;
  if (v_ != 0 && other.v_ != 0) {
    if (is_str_ && other.is_str_ && str_[0] == other.str_[0]) {
      if (0 == STRCMP(str_, other.str_)) {
        bret = true;
      }
    } else if (!is_str_ && !other.is_str_) {
      if (mod_id_ == other.mod_id_) {
        bret = true;
      }
    }
  }
  return bret;
}

ObLabel::operator const char*() const
{
  const char* str = nullptr;
  if (is_str_) {
    str = str_;
  } else {
    static constexpr int len = 32;
    static CoVar<char[len]> buf;
    snprintf(&buf[0], len, "%ld", mod_id_);
    str = &buf[0];
  }
  return str;
}

int64_t ObLabel::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  (void)common::logdata_printf(buf, buf_len, pos, "%s", (const char*)(*this));
  return pos;
}

int64_t ObMemAttr::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  (void)common::logdata_printf(buf,
      buf_len,
      pos,
      "tenant_id=%ld, label=%s, ctx_id=%ld, prio=%d",
      tenant_id_,
      (const char*)label_,
      ctx_id_,
      prio_);
  return pos;
}

void Label::fmt(char* buf, int64_t buf_len, int64_t& pos, const char* str)
{
  if (OB_UNLIKELY(pos >= buf_len)) {
  } else {
    int64_t len = snprintf(buf + pos, buf_len - pos, "%s", str);
    if (len < buf_len - pos) {
      pos += len;
    } else {
      pos = buf_len;
    }
  }
}

void Label::fmt(char* buf, int64_t buf_len, int64_t& pos, int64_t digit)
{
  if (OB_UNLIKELY(pos >= buf_len)) {
  } else {
    ObFastFormatInt ff(digit);
    fmt(buf, buf_len, pos, ff.str());
  }
}