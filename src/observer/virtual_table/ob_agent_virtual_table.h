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

#ifndef OCEANBASE_VIRTUAL_TABLE_OB_AGENT_VIRTUAL_TABLE_H_
#define OCEANBASE_VIRTUAL_TABLE_OB_AGENT_VIRTUAL_TABLE_H_


#include "lib/allocator/page_arena.h"
#include "ob_agent_table_base.h"

namespace oceanbase
{
namespace observer
{
// mysql mode system table access agent for oracle tenant.
class ObAgentVirtualTable : public ObAgentTableBase
{
public:
  ObAgentVirtualTable();
  virtual ~ObAgentVirtualTable();

  int init(
      const uint64_t pure_table_id,
      const bool sys_tenant_base_table,
      const share::schema::ObTableSchema *index_table,
      const ObVTableScanParam &scan_param,
      const bool only_sys_data,
      const lib::Worker::CompatMode &mode = lib::Worker::CompatMode::ORACLE);

  virtual int do_open() override;
  virtual int inner_get_next_row(common::ObNewRow *&row) override;

private:
  virtual int set_convert_func(convert_func_t &func,
      const share::schema::ObColumnSchemaV2 &col,
      const share::schema::ObColumnSchemaV2 &base_col) override;

  virtual int init_non_exist_map_item(MapItem &item,
      const share::schema::ObColumnSchemaV2 &col) override;

  virtual int add_extra_condition(common::ObSqlString &sql) override;

  int should_add_tenant_condition(bool &need, const uint64_t tenant_id) const;

private:
  // 切sys前被代理的普通租户ID
  uint64_t general_tenant_id_;
  // agent table Compat Mode
  lib::Worker::CompatMode mode_;
  // 表明是否这张代理表只查系统租户下的数据
  bool only_sys_data_;
  DISALLOW_COPY_AND_ASSIGN(ObAgentVirtualTable);
};

} // end namespace observer
} // end namespace oceanbase

#endif // OCEANBASE_VIRTUAL_TABLE_OB_AGENT_VIRTUAL_TABLE_H_
