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

#ifndef OCEANBASE_OB_SERVER_UTILS_
#define OCEANBASE_OB_SERVER_UTILS_

#include "lib/allocator/ob_allocator.h"
#include "lib/string/ob_string.h"

namespace oceanbase {
using common::ObIAllocator;
using common::ObString;

namespace observer {
class ObServerUtils {
public:
  // Get the server's ipstr.
  // @param [in] allocator.
  // @param [out] server's ip str.
  // @return the error code.
  static int get_server_ip(ObIAllocator* allocator, ObString& ipstr);

private:
  DISALLOW_COPY_AND_ASSIGN(ObServerUtils);
};
}  // namespace observer
}  // namespace oceanbase
#endif  // OCEANBASE_OB_SERVER_UTILS_
