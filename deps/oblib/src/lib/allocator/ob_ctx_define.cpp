/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "lib/allocator/ob_ctx_define.h"

namespace oceanbase
{
namespace common
{
ObCtxAttrCenter &ObCtxAttrCenter::instance()
{
  static ObCtxAttrCenter instance;
  return instance;
}
} // end of namespace common
} // end of namespace oceanbase
