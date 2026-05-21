/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_OB_LIST_FILE_
#define OCEANBASE_OBSERVER_OB_LIST_FILE_
#include "share/ob_virtual_table_scanner_iterator.h"
#include "common/ob_range.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace common
{
class ObString;
}
namespace observer
{
class ObExternalLocationListFile : public common::ObVirtualTableScannerIterator
{
public:
  ObExternalLocationListFile();
  virtual ~ObExternalLocationListFile();
  virtual int inner_open();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
private:
  int resolve_param(uint64_t &location_id,
                    ObString &sub_path,
                    ObString &pattern,
                    share::schema::ObExternalFilePatternType &pattern_type);
  int fill_row_cells(uint64_t location_id,
                     const ObString &sub_path,
                     const ObString &pattern,
                     const share::schema::ObExternalFilePatternType &pattern_type,
                     const ObString &file_url,
                     int64_t file_size);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExternalLocationListFile);
};
}
}
#endif /* OCEANBASE_OBSERVER_OB_LIST_FILE_ */
