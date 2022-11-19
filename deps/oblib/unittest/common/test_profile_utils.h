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

#ifndef OCEANBASE_COMMON_TEST_PROFILE_UTILS_H
#define OCEANBASE_COMMON_TEST_PROFILE_UTILS_H
#include "lib/utility/ob_macro_utils.h"
namespace oceanbase
{
namespace common
{
const char* const FMT_STR = "%s%ld";
const char* const MODEL_STR = "Copyright 2014 Alibaba Inc. All Rights Reserved. ";

class TestProfileUtils
{
public:
  TestProfileUtils() {}
  ~TestProfileUtils() {}
  static int build_string(char *str_buf,
                          int64_t str_buf_length,
                          int64_t str_length)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(str_buf)
        || OB_UNLIKELY(str_buf_length < str_length)) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid argument", K(ret), K(str_buf), K(str_buf_length), K(str_length));
    } else {
      memset(str_buf, '\0', str_buf_length);
      int64_t model_size = static_cast<int64_t>(strlen(MODEL_STR));
      if (str_length < model_size) {
        strncat(str_buf, MODEL_STR, model_size);
      } else {
        int64_t pos = 0;
        for (int64_t i = 0; i < str_length / model_size; i++) {
          strcat(str_buf + pos, MODEL_STR);
          pos = pos + model_size;
        }
        int64_t left_size = str_buf_length - strlen(str_buf) - 1;
        strncat(str_buf + pos, MODEL_STR, left_size);
      }
    }
    return ret;

  }
private:
  DISALLOW_COPY_AND_ASSIGN(TestProfileUtils);
};
}
}
#endif

