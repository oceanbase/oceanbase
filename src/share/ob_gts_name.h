/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_COMMON_OB_GTS_NAME_H_
#define OCEANBASE_COMMON_OB_GTS_NAME_H_

#include "lib/string/ob_fixed_length_string.h"

namespace oceanbase
{
namespace common
{
typedef ObFixedLengthString<MAX_GTS_NAME_LENGTH> ObGtsName;
} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_COMMON_OB_GTS_NAME_H_
