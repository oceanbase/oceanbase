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

#ifndef OCEANBASE_VIRTUAL_TABLE_OB_ITERATE_PRIVATE_VIRTUAL_TABLE_H_
#define OCEANBASE_VIRTUAL_TABLE_OB_ITERATE_PRIVATE_VIRTUAL_TABLE_H_

#include "lib/string/ob_sql_string.h"
#include "observer/virtual_table/ob_agent_table_base.h" // ObAgentTableBase

namespace oceanbase
{
namespace observer
{

// Iterate tables of sys/meta tenants:
// 1. base table should be system table & in tenant space & is cluster private.
// 2. `tenant_id` should be prefix of base table's primary key.
// 3. for data of base table:
//    - user tenant's data records in its meta tenant.
//    - meta tenant's data records in itself or sys tenant.(control by meta_record_in_sys)
//    - sys tenant's data records in itself.
class ObIteratePrivateVirtualTable : public ObAgentTableBase
{
public:
  ObIteratePrivateVirtualTable();
  virtual ~ObIteratePrivateVirtualTable();

  int init(const uint64_t base_table_id,
      const bool meta_record_in_sys,
      const share::schema::ObTableSchema *index_table,
      const ObVTableScanParam &scan_param);

  virtual int do_open() override;
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
  virtual int try_convert_row(const ObNewRow *input_row, ObNewRow *&row);
  virtual int inner_close() override;
private:
  virtual int init_non_exist_map_item(
      MapItem &item, const share::schema::ObColumnSchemaV2 &col) override;
  virtual int setup_inital_rowkey_condition(
      common::ObSqlString &cols, common::ObSqlString &vals) override;
  virtual int add_extra_condition(common::ObSqlString &sql) override;

  bool check_tenant_in_range_(const uint64_t tenant_id, const common::ObNewRange &range);
  int next_tenant_();
  uint64_t get_exec_tenant_id_(const uint64_t tenant_id);
  virtual int set_convert_func(convert_func_t &func,
                               const share::schema::ObColumnSchemaV2 &col,
                               const share::schema::ObColumnSchemaV2 &base_col) override;
private:
  int64_t tenant_idx_;
  uint64_t cur_tenant_id_;
  // if meta_record_in_sys_ = true: fetch meta tenant's data from sys tenant.
  // if meta_record_in_sys_ = false: fetch meta tenant's data from itself.
  bool meta_record_in_sys_;
  common::ObArray<uint64_t, common::ObWrapperAllocator> tenants_;
  common::ObSqlString sql_;
};

} // end namespace observer
} // end namespace oceanbase

#endif // OCEANBASE_VIRTUAL_TABLE_OB_ITERATE_PRIVATE_VIRTUAL_TABLE_H_
