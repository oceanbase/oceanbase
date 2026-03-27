/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/aggregate/ob_distinct_op.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObDistinctSpec::ObDistinctSpec(ObIAllocator &alloc, const ObPhyOperatorType type)
  : ObOpSpec(alloc, type),
  distinct_exprs_(alloc),
  cmp_funcs_(alloc),
  is_block_mode_(false),
  by_pass_enabled_(false)
{}

OB_SERIALIZE_MEMBER((ObDistinctSpec, ObOpSpec),
                    distinct_exprs_,
                    cmp_funcs_,
                    is_block_mode_,
                    by_pass_enabled_);


} // end namespace sql
} // end namespace oceanbase

