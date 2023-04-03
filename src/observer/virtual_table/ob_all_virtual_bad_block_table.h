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

#ifndef OB_ALL_VIRTUAL_BAD_BLOCK_TABLE_H
#define OB_ALL_VIRTUAL_BAD_BLOCK_TABLE_H

#include "storage/blocksstable/ob_block_manager.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"
#include "common/row/ob_row.h"

namespace oceanbase
{
namespace observer
{

class ObVirtualBadBlockTable : public ObVirtualTableScannerIterator
{
public:
  ObVirtualBadBlockTable();
  virtual ~ObVirtualBadBlockTable();
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
  virtual void reset() override;
  int init(const common::ObAddr &addr);
private:
  enum BAD_BLOCK_COLUMN
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    DISK_ID,
    STORE_FILE_PATH,
    MACRO_BLOCK_INDEX,
    ERROR_TYPE,
    ERROR_MSG,
    CHECK_TIME
  };
  bool is_inited_;
  int64_t cursor_;
  common::ObAddr addr_;
  common::ObArray<blocksstable::ObBadBlockInfo> bad_block_infos_;
  char ip_buf_[common::MAX_IP_ADDR_LENGTH];
  DISALLOW_COPY_AND_ASSIGN(ObVirtualBadBlockTable);
};


}// namespace observer
}// namespace oceanbase

#endif /* !OB_ALL_VIRTUAL_BAD_BLOCK_TABLE_H */
