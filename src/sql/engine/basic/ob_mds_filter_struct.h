/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_BASIC_OB_MDS_FILTER_STRUCT_H_
#define OCEANBASE_SQL_ENGINE_BASIC_OB_MDS_FILTER_STRUCT_H_

#include "ob_pushdown_filter.h"

namespace oceanbase
{
namespace sql
{

class ObIMDSFilterExecutor
{
public:
  ObIMDSFilterExecutor() = default;
  virtual ~ObIMDSFilterExecutor() = default;

  // for single column mds filter executor, like ttl filter/base version filter, return the column offset in read info column index array
  virtual int64_t get_col_offset(const bool is_cg = false) const { return -1; }
  virtual int get_filter_val_meta(common::ObObjMeta &obj_meta) const { return OB_NOT_SUPPORTED; }

  virtual int filter(const blocksstable::ObStorageDatum *datums, int64_t count, bool &filtered) const = 0;

  DECLARE_PURE_VIRTUAL_TO_STRING;

  static ObIMDSFilterExecutor *cast(ObPushdownFilterExecutor *executor);
};


}
} // namespace oceanbase

#endif
