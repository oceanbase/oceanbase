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

#ifndef OCEANBASE_OBSERVER_OB_LIST_FILE_
#define OCEANBASE_OBSERVER_OB_LIST_FILE_
#include "lib/container/ob_se_array.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "common/ob_range.h"

namespace oceanbase
{
namespace common
{
class ObString;
}
namespace observer
{
class ObListFile : public common::ObVirtualTableScannerIterator
{
public:
  ObListFile();
  virtual ~ObListFile();
  virtual int inner_open();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
private:
  int resolve_param(uint64_t &location_id, ObString &sub_path, ObString &pattern);
  int fill_row_cells(uint64_t location_id,
                     const ObString &sub_path,
                     const ObString &pattern,
                     const ObString &file_url,
                     int64_t file_size);
private:
  DISALLOW_COPY_AND_ASSIGN(ObListFile);
};
}
}
#endif /* OCEANBASE_OBSERVER_OB_LIST_FILE_ */
