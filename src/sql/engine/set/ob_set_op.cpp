/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/set/ob_set_op.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObSetSpec::ObSetSpec(ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type),
    is_distinct_(false),
    set_exprs_(alloc),
    sort_collations_(alloc),
    sort_cmp_funs_(alloc)
{
}

OB_SERIALIZE_MEMBER((ObSetSpec, ObOpSpec),
    is_distinct_,
    set_exprs_,
    sort_collations_,
    sort_cmp_funs_);

} // end namespace sql
} // end namespace oceanbase
