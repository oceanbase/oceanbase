/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_COMMON_OB_ZONE_H_
#define OCEANBASE_COMMON_OB_ZONE_H_

#include "lib/string/ob_fixed_length_string.h"

namespace oceanbase
{
namespace common
{
typedef ObFixedLengthString<MAX_ZONE_LENGTH> ObZone;
typedef ObFixedLengthString<MAX_ZONE_LIST_LENGTH> ObPriZone;
}//end namespace common
}//end namespace oceanbase
#endif //OCEANBASE_COMMON_OB_ZONE_H_

