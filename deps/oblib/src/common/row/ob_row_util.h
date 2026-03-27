/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
