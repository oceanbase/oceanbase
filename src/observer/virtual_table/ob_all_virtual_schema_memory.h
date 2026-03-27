/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SCHEMA_MEMORY_H_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SCHEMA_MEMORY_H_


#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "common/row/ob_row.h"

namespace oceanbase
{
class ObSchemaMemory;
namespace observer
{
class ObAllVirtualSchemaMemory: public common::ObVirtualTableScannerIterator
{
  enum COLUMN_ID_LIST
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    ALLOCATOR_TYPE,
    USED_SCHEMA_MGR_CNT,
    FREE_SCHEMA_MGR_CNT,
    MEM_USED,
    MEM_TOTAL,
    ALLOCATOR_IDX,
  };
public:
  explicit ObAllVirtualSchemaMemory(share::schema::ObMultiVersionSchemaService &schema_service)
             : tenant_idx_(OB_INVALID_INDEX), mem_idx_(0),
               schema_service_(schema_service), tenant_ids_() {}
  virtual ~ObAllVirtualSchemaMemory() {}
public:
  virtual int inner_open();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  int get_next_tenant_mem_info(ObSchemaMemory &schema_mem);
private:
  int64_t tenant_idx_;
  int64_t mem_idx_;
  const static int64_t DEFAULT_TENANT_NUM = 10;
  const static int64_t DEFAULT_ALLOCATOR_COUNT = 2;
  char ip_buffer_[OB_MAX_SERVER_ADDR_SIZE];
  share::schema::ObMultiVersionSchemaService &schema_service_;
  common::ObSEArray<ObSchemaMemory, DEFAULT_ALLOCATOR_COUNT> schema_mem_infos_;
  common::ObSEArray<uint64_t, DEFAULT_TENANT_NUM> tenant_ids_;
}; //class ObAllVirtualServerSchemaMem
}//namespace observer
}//namespace oceanbase
#endif //OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SCHEMA_MEM_H_
