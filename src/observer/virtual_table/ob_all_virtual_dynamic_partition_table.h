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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_ALL_VIRTUAL_DYNAMIC_PARTITION_TABLE_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_ALL_VIRTUAL_DYNAMIC_PARTITION_TABLE_

#include "share/ob_virtual_table_scanner_iterator.h"

namespace oceanbase
{
namespace observer
{
class ObAllVirtualDynamicPartitionTable : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualDynamicPartitionTable();
  virtual ~ObAllVirtualDynamicPartitionTable();
  int init(share::schema::ObMultiVersionSchemaService *schema_service_);
  void destroy();
  virtual int inner_open();
  virtual int inner_get_next_row(common::ObNewRow *&row);
private:
  int next_tenant_();
  int build_new_row_(const int64_t tenant_schema_version,
                     const share::schema::ObTableSchema &table_schema,
                     common::ObNewRow *&row);
private:
  enum {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    TENANT_SCHEMA_VERSION,
    DATABASE_NAME,
    TABLE_NAME,
    TABLE_ID,
    MAX_HIGH_BOUND_VAL,
    ENABLE,
    TIME_UNIT,
    PRECREATE_TIME,
    EXPIRE_TIME,
    TIME_ZONE,
    BIGINT_PRECISION
  };
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualDynamicPartitionTable);
private:
  bool is_inited_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  common::ObArray<uint64_t> tenant_ids_;
  common::ObArray<uint64_t> cur_tenant_table_ids_;
  int64_t tenant_idx_;
  int64_t table_idx_;
};
} // namespace observer
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_VIRTUAL_TABLE_ALL_VIRTUAL_DYNAMIC_PARTITION_TABLE_
