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
#pragma once

#include "lib/container/ob_array.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace common
{
class ObTabletID;
} // namespace common
namespace blocksstable
{
class ObDatumRow;
class ObBatchDatumRows;
} // namespace blocksstable
namespace storage
{
class ObDirectLoadDatumRow;
class ObDirectLoadExternalRow;
class ObDirectLoadMultipleDatumRow;

class ObDirectLoadDMLRowHandler
{
public:
  ObDirectLoadDMLRowHandler() = default;
  virtual ~ObDirectLoadDMLRowHandler() = default;

  /**
   * handle rows direct insert into sstable
   */
  // ObDirectLoadDatumRow without multi version cols
  virtual int handle_insert_row(const ObTabletID &tablet_id,
                                const ObDirectLoadDatumRow &datum_row) = 0;
  virtual int handle_delete_row(const ObTabletID &tablet_id,
                                const ObDirectLoadDatumRow &datum_row) = 0;
  // ObDatumRow with multi version cols
  // 堆表导入insert非向量化接口使用
  virtual int handle_insert_row(const ObTabletID &tablet_id,
                                const blocksstable::ObDatumRow &datum_row) = 0;
  // ObBatchDatumRows with multi version cols
  // 堆表导入insert向量化接口使用
  virtual int handle_insert_batch(const ObTabletID &tablet_id,
                                  const blocksstable::ObBatchDatumRows &datum_rows) = 0;

  /**
   * handle rows with the same primary key in the imported data
   */
  virtual int handle_update_row(const ObTabletID &tablet_id,
                                const ObDirectLoadDatumRow &row) = 0;
  virtual int handle_update_row(const ObTabletID &tablet_id,
                                common::ObArray<const ObDirectLoadExternalRow *> &rows,
                                const ObDirectLoadExternalRow *&row) = 0;
  virtual int handle_update_row(common::ObArray<const ObDirectLoadMultipleDatumRow *> &rows,
                                const ObDirectLoadMultipleDatumRow *&row) = 0;

  /**
   * handle rows with the same primary key between the imported data and the original data
   */
  virtual int handle_update_row(const ObTabletID &tablet_id,
                                const ObDirectLoadDatumRow &old_row,
                                const ObDirectLoadDatumRow &new_row,
                                const ObDirectLoadDatumRow *&result_row) = 0;

  DECLARE_PURE_VIRTUAL_TO_STRING;
};

} // namespace storage
} // namespace oceanbase
