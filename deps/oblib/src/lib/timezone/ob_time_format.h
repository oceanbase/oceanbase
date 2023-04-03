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

#ifndef OCEANBASE_LIB_FILE_TIME_FORMAT
#define OCEANBASE_LIB_FILE_TIME_FORMAT

#include <stdint.h>

namespace oceanbase
{
namespace common
{

//enum enum_ob_timestamp_type
//{
//  OB_TIMESTAMP_NONE= -2, OB_TIMESTAMP_ERROR= -1,
//  OB_TIMESTAMP_DATE= 0, OB_TIMESTAMP_DATETIME= 1, OB_TIMESTAMP_TIME= 2
//};

//typedef struct
//{
//  /*
//    this format is different since in the struct tm,
//    the range of month is [0, 11], here, the month range
//    is [1, 12], that's different
//   */
//  uint32_t year, month, day, hour, minute, second, usecond;  // usecond is microsecond.
//  bool     neg;
//} OB_TIME;

}
}

#endif
