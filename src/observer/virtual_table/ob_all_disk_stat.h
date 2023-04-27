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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_DISK_STAT_TABLE_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_DISK_STAT_TABLE_

#include "share/ob_virtual_table_scanner_iterator.h"


namespace oceanbase
{
namespace common
{
class ObObj;
}

namespace observer
{

class ObInfoSchemaDiskStatTable : public common::ObVirtualTableScannerIterator
{
public:
  ObInfoSchemaDiskStatTable();
  virtual ~ObInfoSchemaDiskStatTable();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  inline void set_addr(common::ObAddr &addr) {addr_ = &addr;}
  virtual int set_ip(common::ObAddr *addr);

private:
  enum DISK_COLUMN
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TOTAL_SIZE,
    USED_SIZE,
    FREE_SIZE,
    IS_DISK_VALID,
    DISK_ERROR_BEGIN_TS,
    ALLOCATED_SIZE
  };
  common::ObAddr *addr_;
  common::ObString ipstr_;
  int32_t port_;
  bool is_end_;
  DISALLOW_COPY_AND_ASSIGN(ObInfoSchemaDiskStatTable);
};

}
}
#endif /* OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_DISK_STAT_TABLE */
