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

#ifndef OCEANBASE_LIB_REGEX_OB_REGEX_
#define OCEANBASE_LIB_REGEX_OB_REGEX_

#include <regex.h>
#include "lib/ob_define.h"

namespace oceanbase {
namespace common {
class ObRegex {
public:
  ObRegex();
  virtual ~ObRegex();

public:
  int init(const char* pattern, int flags);
  int match(const char* text, int flags, bool& is_match);
  void destroy();
  inline const regmatch_t* get_match() const
  {
    return match_;
  }
  inline int64_t get_match_count() const
  {
    return static_cast<int64_t>(nmatch_);
  }

private:
  bool init_;
  regmatch_t* match_;
  regex_t reg_;
  size_t nmatch_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRegex);
};
}  // namespace common
}  // namespace oceanbase

#endif  // OCEANBASE_LIB_REGEX_OB_REGEX_
