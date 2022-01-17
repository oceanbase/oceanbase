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

#ifndef TBSYS_TIMEUTIL_H_
#define TBSYS_TIMEUTIL_H_

#include <stdint.h>
#include <time.h>
#include <sys/time.h>
#include <stdio.h>
#include <string.h>

namespace oceanbase {
namespace obsys {

/**
 * @brief Simple encapsulation of linux time operation
 */
class CTimeUtil {
public:
  /**
   * ms timestamp
   */
  static int64_t getTime();
  /**
   * get current time
   */
  static int64_t getMonotonicTime();
  /**
   * format int into 20080101101010
   */
  static char* timeToStr(time_t t, char* dest);
  /**
   * format string to time(local)
   */
  // static int strToTime(char *str);
};

}  // namespace obsys
}  // namespace oceanbase

#endif
