/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "ob_column_store_util.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/access/ob_table_access_param.h"

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
