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

