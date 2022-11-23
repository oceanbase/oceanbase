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
