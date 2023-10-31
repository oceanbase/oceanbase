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
#define USING_LOG_PREFIX STORAGE
#include "ob_co_sstable_row_getter.h"

namespace oceanbase
{
namespace storage
{
void ObCOSSTableRowGetter::reset()
{
  has_fetched_ = false;
  nop_pos_ = nullptr;
  prefetcher_.reset();
  read_handle_.reset();
  ObCGSSTableRowGetter::reset();
}

void ObCOSSTableRowGetter::reuse()
{
  has_fetched_ = false;
  nop_pos_ = nullptr;
  prefetcher_.reuse();
  read_handle_.reset();
  ObCGSSTableRowGetter::reuse();
}

int ObCOSSTableRowGetter::inner_open(
    const ObTableIterParam &iter_param,
    ObTableAccessContext &access_ctx,
    ObITable *table,
    const void *query_range)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObCGSSTableRowGetter::init(
              iter_param, access_ctx, prefetcher_, table, query_range))) {
    LOG_WARN("Fail to init sstable cg getter", K(ret));
  } else {
    read_handle_.rowkey_ = static_cast<const blocksstable::ObDatumRowkey *>(query_range);
    read_handle_.range_idx_ = 0;
    read_handle_.is_get_ = true;
    if (OB_FAIL(prefetcher_.single_prefetch(read_handle_))) {
      LOG_WARN("ObCOSSTableRowGetter prefetch failed", K(ret));
    } else {
      is_inited_ = true;
    }
  }

  if (IS_NOT_INIT) {
    reset();
  }
  return ret;
}

int ObCOSSTableRowGetter::inner_get_next_row(const blocksstable::ObDatumRow *&store_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (has_fetched_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(prefetcher_.lookup_in_index_tree(read_handle_, true))) {
    LOG_WARN("Fail to prefetch", K(ret), K_(read_handle));
  } else if (OB_FAIL(fetch_row(read_handle_, nop_pos_, store_row))) {
    LOG_WARN("Fail to fetch row", K(ret));
  } else {
    has_fetched_ = true;
    nop_pos_ = nullptr;
  }
  return ret;
}

}
}
