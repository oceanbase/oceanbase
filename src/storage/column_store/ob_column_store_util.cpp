/**
 * Copyright (c) 2022 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_column_store_util.h"
#include "storage/memtable/ob_memtable.h"

namespace oceanbase
{
namespace storage
{
void ObCSDatumRange::set_datum_range(const int64_t start, const int64_t end)
{
  datums_[0].set_int(start);
  datums_[1].set_int(end);
  cs_datum_range_.start_key_.datums_ = &datums_[0];
  cs_datum_range_.start_key_.datum_cnt_ = 1;
  cs_datum_range_.set_left_closed();
  cs_datum_range_.end_key_.datums_ = &datums_[1];
  cs_datum_range_.end_key_.datum_cnt_ = 1;
  cs_datum_range_.set_right_closed();
}

} /* storage */
} /* oceanbase */
