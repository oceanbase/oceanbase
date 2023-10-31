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
#include "ob_co_sstable_row_multi_getter.h"

namespace oceanbase
{
namespace storage
{

void ObCOSSTableRowMultiGetter::reset()
{
  prefetcher_.reset();
  ObCGSSTableRowGetter::reset();
}

void ObCOSSTableRowMultiGetter::reuse()
{
  prefetcher_.reuse();
  ObCGSSTableRowGetter::reuse();
}

int ObCOSSTableRowMultiGetter::inner_open(
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
    if (OB_FAIL(prefetcher_.multi_prefetch())) {
      LOG_WARN("Fail to multi prefetch data", K(ret));
    } else {
      is_inited_ = true;
    }
  }

  if (IS_NOT_INIT) {
    reset();
  }
  return ret;
}

int ObCOSSTableRowMultiGetter::inner_get_next_row(const blocksstable::ObDatumRow *&store_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObCOSSTableRowMultiGetter has not been inited", K(ret), KP(this));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(prefetcher_.multi_prefetch())) {
        LOG_WARN("Fail to prefetch micro block", K(ret), K_(prefetcher));
      } else if (prefetcher_.fetch_rowkey_idx_ >= prefetcher_.prefetch_rowkey_idx_) {
        if (OB_LIKELY(prefetcher_.is_prefetch_end())) {
          ret = OB_ITER_END;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Current fetch handle idx exceed prefetching idx", K(ret), K_(prefetcher));
        }
      } else if (!prefetcher_.current_read_handle().cur_prefetch_end_) {
        continue;
      } else if (OB_FAIL(fetch_row(prefetcher_.current_read_handle(), nullptr, store_row))) {
        if (OB_LIKELY(OB_ITER_END == ret)) {
          prefetcher_.mark_cur_rowkey_fetched(prefetcher_.current_read_handle());
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("Fail to fetch row", K(ret));
        }
      } else {
        prefetcher_.mark_cur_rowkey_fetched(prefetcher_.current_read_handle());
        break;
      }
    }
  }
  return ret;
}

}
}
