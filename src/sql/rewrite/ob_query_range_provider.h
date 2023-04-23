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

#ifndef OCEANBASE_SQL_REWRITE_QUERY_RANGE_PROVIDER_
#define OCEANBASE_SQL_REWRITE_QUERY_RANGE_PROVIDER_

#include "lib/container/ob_array.h"
#include "lib/container/ob_se_array.h"
#include "common/ob_range.h"

namespace oceanbase
{
namespace common
{
struct ObDataTypeCastParams;
}
namespace sql
{
struct ColumnItem;
typedef common::ObSEArray<common::ObNewRange *, 1> ObQueryRangeArray;
typedef common::ObSEArray<common::ObNewRange, 4, common::ModulePageAllocator, true> ObRangesArray;
typedef common::ObSEArray<ColumnItem, 16, common::ModulePageAllocator, true> ColumnArray;

class ObQueryRangeProvider
{
public:
  virtual int get_tablet_ranges(ObQueryRangeArray &ranges,
                                bool &all_single_value_ranges,
                                const common::ObDataTypeCastParams &dtc_params) = 0;

  // to string
  virtual int64_t to_string(char *buf, const int64_t buf_len) const = 0;
};
}
}
#endif //OCEANBASE_SQL_REWRITE_QUERY_RANGE_PROVIDER_
//// end of header file



