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

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/plan/ob_table_load_normal_row_handler.h"
#include "observer/table_load/ob_table_load_error_row_handler.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/plan/ob_table_load_plan.h"
#include "observer/table_load/plan/ob_table_load_table_op.h"
#include "storage/direct_load/ob_direct_load_external_row.h"
#include "storage/direct_load/ob_direct_load_multiple_datum_row.h"

namespace oceanbase
{
namespace observer
{
using namespace blocksstable;
using namespace common;
using namespace sql;
using namespace storage;

/**
 * ObTableLoadNormalRowHandler
 */

ObTableLoadNormalRowHandler::ObTableLoadNormalRowHandler(ObTableLoadTableOp *table_op)
  : ObTableLoadTableDMLRowHandler(table_op)
{
}

int ObTableLoadNormalRowHandler::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    int ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadNormalRowHandler init twice", KR(ret), KP(this));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObTableLoadNormalRowHandler::handle_insert_row(const ObTabletID &tablet_id,
                                                   const ObDirectLoadDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadNormalRowHandler not init", KR(ret), KP(this));
  } else if (OB_FAIL(push_insert_row(tablet_id, datum_row))) {
    LOG_WARN("fail to push insert row", KR(ret));
  }
  return ret;
}

int ObTableLoadNormalRowHandler::handle_insert_row(const ObTabletID &tablet_id,
                                                   const ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadNormalRowHandler not init", KR(ret), KP(this));
  } else if (OB_FAIL(push_insert_row(tablet_id, datum_row))) {
    LOG_WARN("fail to push insert row", KR(ret));
  }
  return ret;
}

int ObTableLoadNormalRowHandler::handle_insert_batch(const ObTabletID &tablet_id,
                                                     const ObBatchDatumRows &datum_rows)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadNormalRowHandler not init", KR(ret), KP(this));
  } else if (0 == datum_rows.row_count_) {
    // do nothing
  } else if (OB_FAIL(push_insert_batch(tablet_id, datum_rows))) {
    LOG_WARN("fail to push insert batch", KR(ret));
  }
  return ret;
}

int ObTableLoadNormalRowHandler::handle_delete_row(const ObTabletID &tablet_id,
                                                   const storage::ObDirectLoadDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadNormalRowHandler not init", KR(ret), KP(this));
  } else if (OB_FAIL(push_delete_row(tablet_id, datum_row))) {
    LOG_WARN("fail to push insert batch", KR(ret));
  }
  return ret;
}

int ObTableLoadNormalRowHandler::handle_update_row(const ObTabletID &tablet_id,
                                                   const ObDirectLoadDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(push_update_row(tablet_id, datum_row))) {
    LOG_WARN("fail to push update row", KR(ret));
  }
  return ret;
}

int ObTableLoadNormalRowHandler::handle_update_row(const ObTabletID &tablet_id,
                                                   ObArray<const ObDirectLoadExternalRow *> &rows,
                                                   const ObDirectLoadExternalRow *&row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(rows.count() < 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret));
  } else if (OB_FAIL(push_update_row(tablet_id, rows, row))) {
    LOG_WARN("fail to push update row", K(ret));
  }
  return ret;
}

int ObTableLoadNormalRowHandler::handle_update_row(
  ObArray<const ObDirectLoadMultipleDatumRow *> &rows, const ObDirectLoadMultipleDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(push_update_row(rows, row))) {
    LOG_WARN("fail to push update row", K(ret));
  }
  return ret;
}

int ObTableLoadNormalRowHandler::handle_update_row(const ObTabletID &tablet_id,
                                                   const ObDirectLoadDatumRow &old_row,
                                                   const ObDirectLoadDatumRow &new_row,
                                                   const ObDirectLoadDatumRow *&result_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadNormalRowHandler not init", KR(ret), KP(this));
  } else if (OB_FAIL(push_update_row(tablet_id, old_row, new_row, result_row))) {
    LOG_WARN("fail to push update row", KR(ret));
  }
  return ret;
}

int ObTableLoadNormalRowHandler::handle_insert_delete_conflict(const ObTabletID &tablet_id,
                                                                const ObDirectLoadDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadNormalRowHandler not init", KR(ret), KP(this));
  } else if (OB_FAIL(push_insert_delete_conflict(tablet_id, datum_row))) {
    LOG_WARN("fail to push insert delete conflict", KR(ret));
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
