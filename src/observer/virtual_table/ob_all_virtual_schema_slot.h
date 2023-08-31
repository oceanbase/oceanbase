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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SCHEMA_SLOT_H_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SCHEMA_SLOT_H_


#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "common/row/ob_row.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_se_array.h"
#include "lib/string/ob_sql_string.h"

namespace oceanbase
{
namespace observer
{
class ObAllVirtualSchemaSlot: public common::ObVirtualTableScannerIterator
{
  enum COLUMN_ID_LIST
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    SLOT_ID,
    SCHEMA_VERSION,
    SCHEMA_COUNT,
    REF_CNT,
    REF_INFO,
    ALLOCATOR_IDX,
  };
public:
  explicit ObAllVirtualSchemaSlot(share::schema::ObMultiVersionSchemaService &schema_service)
             : tenant_idx_(OB_INVALID_INDEX), slot_idx_(0), 
               tenant_ids_(), schema_service_(schema_service), schema_slot_infos_() {}
  virtual ~ObAllVirtualSchemaSlot() {}
public:
  virtual int inner_open();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  void reset(common::ObIAllocator &allocator, common::ObIArray<ObSchemaSlot> &schema_slot_infos);
  int get_next_tenant_slot_info(ObSchemaSlot &schema_slot);

private:
  int64_t tenant_idx_;// tenant_iterator
  int64_t slot_idx_;// slot iterator
  const static int64_t DEFAULT_TENANT_NUM = 10;
  const static int64_t DEFAULT_SLOT_NUM = 128;
  char ip_buffer_[OB_MAX_SERVER_ADDR_SIZE];
  common::ObSEArray<uint64_t, DEFAULT_TENANT_NUM> tenant_ids_;
  share::schema::ObMultiVersionSchemaService &schema_service_;
  common::ObSEArray<ObSchemaSlot, DEFAULT_SLOT_NUM> schema_slot_infos_;// specified tenant's all slot info
}; //class ObAllVirtualSchemaSlot
}//namespace observer
}//namespace oceanbase
#endif //OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SCHEMA_SLOT_H_
