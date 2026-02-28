/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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

  // for single column mds filter executor, like ttl filter/base version filter, return the column index
  virtual int64_t get_col_idx() const { return -1; }
  virtual int get_filter_val_meta(common::ObObjMeta &obj_meta) const { return OB_NOT_SUPPORTED; }

  virtual int filter(const blocksstable::ObDatumRow &row, bool &filtered) const = 0;
  virtual int filter(const blocksstable::ObStorageDatum *datums, int64_t count, bool &filtered) const = 0;

  DECLARE_PURE_VIRTUAL_TO_STRING;

  static ObIMDSFilterExecutor *cast(ObPushdownFilterExecutor *executor);
};


}
} // namespace oceanbase

#endif
