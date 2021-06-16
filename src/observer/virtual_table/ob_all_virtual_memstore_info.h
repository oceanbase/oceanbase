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

#ifndef OB_ALL_VIRTUAL_MEMSTORE_INFO_H_
#define OB_ALL_VIRTUAL_MEMSTORE_INFO_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"
#include "common/row/ob_row.h"
#include "lib/container/ob_se_array.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;
class ObPGPartitionIterator;
}  // namespace storage
namespace memtable {
class ObMemtable;
}
namespace observer {
class ObAllVirtualMemstoreInfo : public common::ObVirtualTableScannerIterator {
public:
  ObAllVirtualMemstoreInfo();
  virtual ~ObAllVirtualMemstoreInfo();

public:
  virtual int inner_get_next_row(common::ObNewRow*& row);
  virtual void reset();
  inline void set_partition_service(storage::ObPartitionService* partition_service)
  {
    partition_service_ = partition_service;
  }
  inline void set_addr(common::ObAddr& addr)
  {
    addr_ = addr;
  }

private:
  int make_this_ready_to_read();
  int get_next_memtable(memtable::ObMemtable*& mt);

private:
  storage::ObPartitionService* partition_service_;
  common::ObAddr addr_;
  common::ObPartitionKey pkey_;
  storage::ObPGPartitionIterator* ptt_iter_;
  char ip_buf_[common::OB_IP_STR_BUFF];
  char mem_version_buf_[common::MAX_VERSION_LENGTH];
  storage::ObTablesHandle tables_handle_;
  common::ObArray<memtable::ObMemtable*> memtables_;
  int64_t memtable_array_pos_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualMemstoreInfo);
};

}  // namespace observer
}  // namespace oceanbase
#endif /* OB_ALL_VIRTUAL_MEMSTORE_INFO_H */
