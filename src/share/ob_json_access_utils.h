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
 * This file contains implementation for lob_access_utils.
 */

#ifndef OCEANBASE_SHARE_OB_JSON_ACCESS_UTILS_
#define OCEANBASE_SHARE_OB_JSON_ACCESS_UTILS_

#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace common {
class ObIJsonBase;
class ObIAllocator;
}
namespace share
{
class ObJsonWrapper
{
public:
  static int get_raw_binary(common::ObIJsonBase *j_base, common::ObString &result, common::ObIAllocator *allocator);
};

} // end namespace share
} // end namespace oceanbase
#endif
