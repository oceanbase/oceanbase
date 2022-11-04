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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SERVER_SCHEMA_INFO_H_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SERVER_SCHEMA_INFO_H_


#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "common/row/ob_row.h"

namespace oceanbase
{
namespace observer
{
class ObAllVirtualServerSchemaInfo: public common::ObVirtualTableScannerIterator
{
public:
  explicit ObAllVirtualServerSchemaInfo(share::schema::ObMultiVersionSchemaService &schema_service)
             : schema_service_(schema_service), tenant_ids_(), idx_(0) {}
  virtual ~ObAllVirtualServerSchemaInfo() {}
public:
  virtual int inner_open();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  void destroy();
private:
  const static int64_t DEFAULT_TENANT_NUM = 10;
  char ip_buffer_[OB_MAX_SERVER_ADDR_SIZE];
  share::schema::ObMultiVersionSchemaService &schema_service_;
  ObSEArray<uint64_t, DEFAULT_TENANT_NUM> tenant_ids_;
  int64_t idx_;
}; //class ObAllVirtualServerSchemaInfo
}//namespace observer
}//namespace oceanbase
#endif //OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SERVER_SCHEMA_INFO_H_
