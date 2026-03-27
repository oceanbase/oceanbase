/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_COMMON_ZEROFILL_INFO_
#define OCEANBASE_COMMON_ZEROFILL_INFO_

#include "common/ob_accuracy.h"

namespace oceanbase
{
namespace common
{
struct ObZerofillInfo
{
public:
  ObZerofillInfo(const bool zf, const ObLength len) : max_length_(len), need_zerofill_(zf) {}
  ObZerofillInfo() : max_length_(0), need_zerofill_(false) {}
public:
  ObLength max_length_;
  bool need_zerofill_;
};

}/* ns common*/
}/* ns oceanbase */

#endif /* OCEANBASE_COMMON_ZEROFILL_INFO_ */
//// end of header file

