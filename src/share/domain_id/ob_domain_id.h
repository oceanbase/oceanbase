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

#ifndef OCEANBASE_SHARE_DOMAIN_ID_DEFINE_H_
#define OCEANBASE_SHARE_DOMAIN_ID_DEFINE_H_

#include "deps/oblib/src/lib/allocator/ob_allocator_v2.h"
#include "deps/oblib/src/lib/container/ob_iarray.h"
#include "deps/oblib/src/lib/container/ob_se_array.h"
#include "sql/ob_sql_context.h"

namespace oceanbase
{
namespace share
{

typedef common::ObSEArray<uint64_t, 2> DomainIdxs;


}
}

#endif