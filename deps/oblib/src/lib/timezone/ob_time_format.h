/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
