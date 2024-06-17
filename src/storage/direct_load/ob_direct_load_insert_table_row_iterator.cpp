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

#include "storage/direct_load/ob_direct_load_insert_table_row_iterator.h"
#include "storage/direct_load/ob_direct_load_lob_builder.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;
using namespace table;

/**
 * ObDirectLoadInsertTableRowIterator
 */

ObDirectLoadInsertTableRowIterator::ObDirectLoadInsertTableRowIterator()
  : insert_tablet_ctx_(nullptr),
    sql_statistics_(nullptr),
    lob_builder_(nullptr),
    lob_allocator_(ObModIds::OB_LOB_ACCESS_BUFFER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    is_inited_(false)
{
}

ObDirectLoadInsertTableRowIterator::~ObDirectLoadInsertTableRowIterator() {}

int ObDirectLoadInsertTableRowIterator::inner_init(
  ObDirectLoadInsertTabletContext *insert_tablet_ctx,
  ObTableLoadSqlStatistics *sql_statistics,
  ObDirectLoadLobBuilder &lob_builder)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == insert_tablet_ctx || !insert_tablet_ctx->is_valid() ||
                  (insert_tablet_ctx->get_online_opt_stat_gather() && nullptr == sql_statistics))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(insert_tablet_ctx), KP(sql_statistics));
  } else {
    insert_tablet_ctx_ = insert_tablet_ctx;
    sql_statistics_ = sql_statistics;
    lob_builder_ = &lob_builder;
  }
  return ret;
}

int ObDirectLoadInsertTableRowIterator::get_next_row(const ObDatumRow *&result_row)
{
  int ret = OB_SUCCESS;
  ret = get_next_row(false, result_row);
  if (ret != OB_ITER_END && ret != OB_SUCCESS) {
    LOG_WARN("fail to get next row", KR(ret));
  }
  return ret;
}

int ObDirectLoadInsertTableRowIterator::get_next_row(const bool skip_lob, const blocksstable::ObDatumRow *&result_row)
{
  int ret = OB_SUCCESS;
  result_row = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadInsertTableRowIterator not init", KR(ret), KP(this));
  } else {
    ObDatumRow *datum_row = nullptr;
    if (OB_FAIL(inner_get_next_row(datum_row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to do inner get next row", KR(ret));
      }
    } else if (insert_tablet_ctx_->get_online_opt_stat_gather() &&
               OB_FAIL(insert_tablet_ctx_->get_table_ctx()->update_sql_statistics(*sql_statistics_,
                                                                                  *datum_row))) {
      LOG_WARN("fail to update sql statistics", KR(ret));
    } else if ((insert_tablet_ctx_->has_lob_storage() && !skip_lob) && OB_FAIL(handle_lob(*datum_row))) {
      LOG_WARN("fail to handle lob", KR(ret));
    } else {
      result_row = datum_row;
    }
  }
  return ret;
}

int ObDirectLoadInsertTableRowIterator::handle_lob(ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  lob_allocator_.reuse();
  if (OB_FAIL(lob_builder_->append_lob(lob_allocator_, datum_row))) {
    LOG_WARN("fail to append lob", KR(ret), K(datum_row));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
