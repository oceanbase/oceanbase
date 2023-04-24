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

#ifndef _OCEANBASE_TESTBENCH_UTILS_H_
#define _OCEANBASE_TESTBENCH_UTILS_H_

#include "lib/oblog/ob_log.h"

namespace oceanbase
{
  int __attribute__((weak)) split(const char *str, const char *delimiter, const int expect_res_cnt, const char **res, int &res_cnt)
  {
    int ret = OB_SUCCESS;
    res_cnt = 0;

    if ((OB_ISNULL(str) || OB_UNLIKELY(0 == strlen(str))) ||
        (OB_ISNULL(delimiter) || OB_UNLIKELY(0 == strlen(delimiter))))
    {
      ret = OB_INVALID_ARGUMENT;
      TESTBENCH_LOG(WARN, "split invalid argument", KCSTRING(str), KCSTRING(delimiter));
    }
    else
    {
      char *rest = const_cast<char *>(str);
      char *token;
      while ((token = strtok_r(rest, delimiter, &rest)))
      {
        TESTBENCH_LOG(INFO, "get split token", KCSTRING(token));
        *res++ = token;
        ++res_cnt;
      }
    }
    if (expect_res_cnt > 0 && res_cnt != expect_res_cnt)
    {
      ret = OB_INVALID_ARGUMENT;
      TESTBENCH_LOG(WARN, "split result count does not match expected count", K(expect_res_cnt), K(res_cnt));
    }

    return ret;
  }
}

#endif