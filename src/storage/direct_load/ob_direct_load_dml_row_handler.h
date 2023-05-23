// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

#pragma once

#include "storage/blocksstable/ob_datum_row.h"

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
  // handle rows with the same primary key between the imported data and the original data
  virtual int handle_update_row(const blocksstable::ObDatumRow &old_row,
                                const blocksstable::ObDatumRow &new_row,
                                const blocksstable::ObDatumRow *&result_row) = 0;
  DECLARE_PURE_VIRTUAL_TO_STRING;
};

} // namespace storage
} // namespace oceanbase
