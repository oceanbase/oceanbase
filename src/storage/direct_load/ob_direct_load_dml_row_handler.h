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

#include "storage/blocksstable/ob_datum_row.h"
#include "storage/direct_load/ob_direct_load_external_row.h"
#include "storage/direct_load/ob_direct_load_multiple_datum_row.h"

namespace oceanbase
{
namespace storage
{

class ObDirectLoadDMLRowHandler
{
public:
  ObDirectLoadDMLRowHandler() = default;
  virtual ~ObDirectLoadDMLRowHandler() = default;
  // handle rows direct insert into sstable
  virtual int handle_insert_row(const blocksstable::ObDatumRow &row) = 0;
  // handle rows with the same primary key in the imported data
  virtual int handle_update_row(const blocksstable::ObDatumRow &row) = 0;
  virtual int handle_update_row(common::ObArray<const ObDirectLoadExternalRow *> &rows,
                                const ObDirectLoadExternalRow *&row) = 0;
  virtual int handle_update_row(common::ObArray<const ObDirectLoadMultipleDatumRow *> &rows,
                                const ObDirectLoadMultipleDatumRow *&row) = 0;
  // handle rows with the same primary key between the imported data and the original data
  virtual int handle_update_row(const blocksstable::ObDatumRow &old_row,
                                const blocksstable::ObDatumRow &new_row,
                                const blocksstable::ObDatumRow *&result_row) = 0;
  DECLARE_PURE_VIRTUAL_TO_STRING;
};

} // namespace storage
} // namespace oceanbase
