/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_COMMON_OB_IDC_H_
#define OCEANBASE_COMMON_OB_IDC_H_

#include "lib/string/ob_fixed_length_string.h"

namespace oceanbase
{
namespace common
{
typedef ObFixedLengthString<MAX_REGION_LENGTH> ObIDC;
}//end namespace common
}//end namespace oceanbase
#endif //OCEANBASE_COMMON_OB_REGION_H_

