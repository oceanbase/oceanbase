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

#ifndef OCEANBASE_VIRTUAL_TABLE_OB_ITERATE_VIRTUAL_TABLE_H_
#define OCEANBASE_VIRTUAL_TABLE_OB_ITERATE_VIRTUAL_TABLE_H_

#include "ob_agent_table_base.h"
#include "lib/string/ob_sql_string.h"

namespace oceanbase
{
namespace observer
{

// Iterate tables of all tenants
class ObIterateVirtualTable : public ObAgentTableBase
{
public:
  ObIterateVirtualTable();
  virtual ~ObIterateVirtualTable();

  int init(const uint64_t base_table_id,
           const share::schema::ObTableSchema *index_table,
           const ObVTableScanParam &scan_param);

  virtual int do_open() override;
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
  virtual int inner_close() override;
private:
  virtual int init_non_exist_map_item(MapItem &item,
      const share::schema::ObColumnSchemaV2 &col) override;

  virtual int setup_inital_rowkey_condition(
      common::ObSqlString &cols, common::ObSqlString &vals);
  virtual int add_extra_condition(common::ObSqlString &sql) override;

  bool check_tenant_in_range(const uint64_t tenant_id, const common::ObNewRange &range);
  int next_tenant();

  virtual int change_column_value(const MapItem &item,
                                  ObIAllocator &allocator,
                                  ObObj &new_value) override;

  int str_to_int(const ObString &str, int64_t &value);

private:
  int64_t tenant_idx_;
  uint64_t cur_tenant_id_;
  common::ObArray<uint64_t, common::ObWrapperAllocator> tenants_;
  common::ObSqlString sql_;
};

} // end namespace observer
} // end namespace oceanbase

#endif // OCEANBASE_VIRTUAL_TABLE_OB_ITERATE_VIRTUAL_TABLE_H_
