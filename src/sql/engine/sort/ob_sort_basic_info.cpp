/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG

#include "ob_sort_basic_info.h"
namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER(ObSortFieldCollation, field_idx_, cs_type_, is_ascending_, null_pos_, is_not_null_);

} // end namespace sql
} // end namespace oceanbase
