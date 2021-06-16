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

namespace oceanbase {
namespace observer {

// Iterate tables of all tenants
class ObIterateVirtualTable : public ObAgentTableBase {
public:
  ObIterateVirtualTable();
  virtual ~ObIterateVirtualTable();

  int init(const uint64_t base_table_id, const bool record_real_tenant_id,
      const share::schema::ObTableSchema* index_table, const ObVTableScanParam& scan_param);

  virtual int do_open() override;
  virtual int inner_get_next_row(common::ObNewRow*& row) override;
  virtual int inner_close() override;

  int set_column_name_with_tenant_id(const char* column_name);

private:
  virtual int init_non_exist_map_item(MapItem& item, const share::schema::ObColumnSchemaV2& col) override;

  virtual int setup_inital_rowkey_condition(common::ObSqlString& cols, common::ObSqlString& vals);
  virtual int add_extra_condition(common::ObSqlString& sql) override;

  bool check_tenant_in_range(const uint64_t tenant_id, const common::ObNewRange& range);
  int next_tenant();
  bool is_real_tenant_id() const;

  virtual int change_column_value(const MapItem& item, ObIAllocator& allocator, ObObj& new_value) override;

  virtual int deal_with_column_with_tenant_id(
      const MapItem& item, ObIAllocator& allocator, bool decode, ObObj& new_value);

  int str_to_int(const ObString& str, int64_t& value);

private:
  int64_t tenant_idx_;
  uint64_t cur_tenant_id_;
  // For sys table under user tenant, record real tenant id (effective_tenant_id()) in
  // tenant_id field or zero. SYS tenant always record real tenant id (OB_SYS_TENANT_ID).
  bool record_real_tenant_id_;
  common::ObArray<uint64_t, common::ObWrapperAllocator> tenants_;
  common::ObSqlString sql_;
  static const int64_t DEFAULT_COLUMN_NUM = 5;
  ObSEArray<ObString, DEFAULT_COLUMN_NUM> columns_with_tenant_id_;
};

}  // end namespace observer
}  // end namespace oceanbase

#endif  // OCEANBASE_VIRTUAL_TABLE_OB_ITERATE_VIRTUAL_TABLE_H_
