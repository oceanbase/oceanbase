/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
