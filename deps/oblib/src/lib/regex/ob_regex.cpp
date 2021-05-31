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
#include <stdlib.h>
#include "lib/oblog/ob_log.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/regex/ob_regex.h"

namespace oceanbase {
namespace common {
ObRegex::ObRegex() : init_(false), match_(NULL), reg_(), nmatch_(0)
{}

ObRegex::~ObRegex()
{
  if (true == init_) {
    destroy();
  }
}

int ObRegex::init(const char* pattern, int flags)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(true == init_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("already inited", K(ret), K(this));
  } else if (OB_ISNULL(pattern)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param pattern", K(ret), K(pattern));
  } else {
    int tmp_ret = regcomp(&reg_, pattern, flags);
    if (OB_UNLIKELY(0 != tmp_ret)) {
      ret = OB_ERR_REGEXP_ERROR;
      LOG_WARN("fail to regcomp", K(ret), K(tmp_ret));
    } else {
      nmatch_ = reg_.re_nsub + 1;
      ObMemAttr attr;
      attr.label_ = ObModIds::OB_REGEX;
      match_ = (regmatch_t*)ob_malloc(sizeof(regmatch_t) * nmatch_, attr);
      if (OB_ISNULL(match_)) {
        ret = OB_ERR_REGEXP_ERROR;
        LOG_WARN("fail to create the regmatch object", K(ret));
        regfree(&reg_);
      } else {
        init_ = true;
      }
    }
  }
  return ret;
}

int ObRegex::match(const char* text, int flags, bool& is_match)
{
  int ret = OB_SUCCESS;
  is_match = false;
  if (OB_UNLIKELY(!init_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K(this));
  } else if (OB_ISNULL(text)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param text", K(ret), K(text));
  } else {
    int tmp_ret = regexec(&reg_, text, nmatch_, match_, flags);
    if (REG_NOMATCH == tmp_ret) {
      is_match = false;
    } else if (0 == tmp_ret) {
      is_match = true;
    } else {
      ret = OB_ERR_REGEXP_ERROR;
      const static int64_t REG_ERR_MSG_BUF_LEN = 512;
      char reg_err_msg[REG_ERR_MSG_BUF_LEN];
      size_t err_msg_len = regerror(tmp_ret, &reg_, reg_err_msg, REG_ERR_MSG_BUF_LEN);
      LOG_WARN("fail to run match func: regexec", K(ret), K(tmp_ret), K(err_msg_len), K(reg_err_msg));
    }
  }
  return ret;
}

void ObRegex::destroy()
{
  if (init_) {
    regfree(&reg_);
    if (NULL != match_) {
      ob_free(match_);
      match_ = NULL;
    }
    nmatch_ = 0;
    init_ = false;
  }
}
}  // namespace common
}  // namespace oceanbase
