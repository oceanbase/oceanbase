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

#ifndef OCEANBASE_COMMON_OB_ROW_UTIL_
#define OCEANBASE_COMMON_OB_ROW_UTIL_

#include "common/row/ob_row.h"
#include "common/rowkey/ob_rowkey.h"

namespace oceanbase
{
namespace common
{

class ObRowUtil
{
public:
  static int convert(const common::ObString &compact_row, ObNewRow &row);
  static int convert(const char *compact_row, int64_t buf_len, ObNewRow &row);
  static int compare_row(const ObNewRow &lrow, const ObNewRow &rrow, int &cmp);
};

} // end namespace common
} // end namespace oceanbase

#endif /* OCEANBASE_COMMON_OB_ROW_UTIL_ */
