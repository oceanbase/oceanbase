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
#include "ob_store_row_iterator.h"
#include "ob_dml_param.h"
#include "storage/access/ob_table_access_context.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
namespace storage
{

ObStoreRowIterator::~ObStoreRowIterator()
{
}

void ObStoreRowIterator::reuse()
{
  is_sstable_iter_ = false;
}

void ObStoreRowIterator::reset()
{
  is_sstable_iter_ = false;
}

int ObStoreRowIterator::init(
    const ObTableIterParam &iter_param,
    storage::ObTableAccessContext &access_ctx,
    ObITable *table,
    const void *query_range)
{
  int ret = OB_SUCCESS;
  is_sstable_iter_ = table->is_sstable();
  if (is_sstable_iter_) {
    block_row_store_ = access_ctx.block_row_store_;
  }
  if (OB_FAIL(inner_open(iter_param, access_ctx, table, query_range))) {
    STORAGE_LOG(WARN, "Failed to inner open ObStoreRowIterator", K(ret), K(iter_param), K(access_ctx));
  }
  return ret;
}

int ObStoreRowIterator::get_next_row(const ObDatumRow *&store_row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_get_next_row(store_row))) {
  }
  return ret;
}

}
}
