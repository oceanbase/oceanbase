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

#include "sql/resolver/ddl/ob_rename_table_stmt.h"

namespace oceanbase
{

using namespace share::schema;
using namespace common;


namespace sql
{

ObRenameTableStmt::ObRenameTableStmt(common::ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_RENAME_TABLE)
{
}

ObRenameTableStmt::ObRenameTableStmt()
    : ObDDLStmt(stmt::T_RENAME_TABLE)
{
}

ObRenameTableStmt::~ObRenameTableStmt()
{
}

int ObRenameTableStmt::add_rename_table_item(const obrpc::ObRenameTableItem &rename_table_item){
  int ret = OB_SUCCESS;
  if (OB_FAIL(rename_table_arg_.rename_table_items_.push_back(rename_table_item))) {
    SQL_RESV_LOG(WARN, "failed to add rename table item to rename table arg!", K(ret));
  }
  return ret;
}

int ObRenameTableStmt::get_rename_table_table_ids(
    common::ObIArray<share::schema::ObObjectStruct> &object_ids) const
{
  int ret = OB_SUCCESS;
  const common::ObIArray<obrpc::ObRenameTableItem> &tmp_items=rename_table_arg_.rename_table_items_;
  for (int64_t i = 0; i < tmp_items.count() && OB_SUCC(ret); ++i) {
    share::schema::ObObjectStruct tmp_struct(share::schema::ObObjectType::TABLE,
                                             tmp_items.at(i).origin_table_id_);
    if (OB_FAIL(object_ids.push_back(tmp_struct))) {
      SQL_RESV_LOG(WARN, "failed to add rename table item to rename table arg!", K(ret));
    }
  }
  return ret;
}


} //namespace sql
}

