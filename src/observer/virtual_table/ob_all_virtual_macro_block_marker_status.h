/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_MACRO_BLOCK_MARKER_STATUS_H_
#define SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_MACRO_BLOCK_MARKER_STATUS_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"
#include "common/row/ob_row.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
namespace oceanbase
{
namespace observer
{

class ObAllVirtualMacroBlockMarkerStatus: public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualMacroBlockMarkerStatus();
  virtual ~ObAllVirtualMacroBlockMarkerStatus();

  int init (const blocksstable::ObMacroBlockMarkerStatus &marker_status);
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();

private:
  char svr_ip_[common::MAX_IP_ADDR_LENGTH];
  char comment_[common::MAX_TABLE_COMMENT_LENGTH];
  blocksstable::ObMacroBlockMarkerStatus marker_status_;
  bool is_end_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualMacroBlockMarkerStatus);
};


}// observer
}// oceanbase
#endif /* SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_MACRO_BLOCK_MARKER_STATUS_H_ */
