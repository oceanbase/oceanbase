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

#ifndef SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SERVER_H_
#define SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SERVER_H_

#include "share/ob_virtual_table_scanner_iterator.h"  // ObVirtualTableScannerIterator

namespace oceanbase
{
namespace observer
{
class ObAllVirtualServer : public common::ObVirtualTableScannerIterator
{
  enum COLUMN_ID_LIST
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    ZONE,
    SQL_PORT,
    CPU_CAPACITY,
    CPU_CAPACITY_MAX,
    CPU_ASSIGNED,
    CPU_ASSIGNED_MAX,
    MEM_CAPACITY,
    MEM_ASSIGNED,
    DATA_DISK_CAPACITY,
    DATA_DISK_IN_USE,
    DATA_DISK_HEALTH_STATUS,
    DATA_DISK_ABNORMAL_TIME,
    LOG_DISK_CAPACITY,
    LOG_DISK_ASSIGNED,
    LOG_DISK_IN_USE,
    SSL_CERT_EXPIRED_TIME,
    MEMORY_LIMIT,
    DATA_DISK_ALLOCATED
  };

public:
  ObAllVirtualServer();
  virtual ~ObAllVirtualServer();
  int init(common::ObAddr &addr);
  virtual int inner_open();
  virtual int inner_get_next_row(common::ObNewRow *&row);

private:
  char ip_buf_[common::OB_IP_STR_BUFF];
  common::ObAddr addr_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualServer);
};

}
}
#endif /* SRC_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SERVER_H_ */
