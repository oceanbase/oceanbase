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

#define USING_LOG_PREFIX STORAGE

#include "ob_new_column_cs_decoder.h"
#include "storage/access/ob_aggregate_base.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;

int ObNewColumnCSDecoder::get_aggregate_result(
    const ObColumnCSDecoderCtx &ctx,
    const int32_t *row_ids,
    const int64_t row_cap,
    storage::ObAggCellBase &agg_cell) const
{
  UNUSEDx(row_ids, row_cap);
  int ret = OB_SUCCESS;
  ObStorageDatum datum;
  if (OB_FAIL(common_decoder_.decode(ctx.get_col_param(), ctx.get_allocator(), datum))) {
    LOG_WARN("Failed to decode datum", K(ret), K(ctx.get_col_param()[0].get_orig_default_value()));
  } else if (OB_FAIL(agg_cell.eval(datum))) {
    LOG_WARN("Failed to eval datum", K(ret), K(datum));
  }
  LOG_DEBUG("[NEW_COLUMN_DECODE] get aggregate(min/max) result", K(datum), K(lbt()));
  return ret;
}

} // end of namespace oceanbase
} // end of namespace oceanbase
