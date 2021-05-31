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

#include "ob_sstable_row_getter.h"
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

namespace oceanbase {
namespace storage {

ObSSTableRowGetter::ObSSTableRowGetter() : ObSSTableRowIterator(), has_prefetched_(false)
{}

ObSSTableRowGetter::~ObSSTableRowGetter()
{}

int ObSSTableRowGetter::get_handle_cnt(const void* query_range, int64_t& read_handle_cnt, int64_t& micro_handle_cnt)
{
  int ret = OB_SUCCESS;
  if (NULL == query_range) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(ret));
  } else {
    read_handle_cnt = 1;
    micro_handle_cnt = 2;
  }
  return ret;
}

int ObSSTableRowGetter::prefetch_read_handle(ObSSTableReadHandle& read_handle)
{
  int ret = OB_SUCCESS;
  if (has_prefetched_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(get_read_handle(*rowkey_, read_handle))) {
    STORAGE_LOG(WARN, "Fail to get read handle, ", K(ret), K(*rowkey_));
  } else {
    read_handle.range_idx_ = 0;
    has_prefetched_ = true;
  }
  return ret;
}

int ObSSTableRowGetter::fetch_row(ObSSTableReadHandle& read_handle, const ObStoreRow*& store_row)
{
  return get_row(read_handle, store_row);
}

void ObSSTableRowGetter::reset()
{
  ObSSTableRowIterator::reset();
  has_prefetched_ = false;
}

void ObSSTableRowGetter::reuse()
{
  ObSSTableRowIterator::reuse();
  has_prefetched_ = false;
}

int ObSSTableRowGetter::get_range_count(const void* query_range, int64_t& range_count) const
{
  int ret = OB_SUCCESS;
  UNUSED(query_range);
  range_count = 1;
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
