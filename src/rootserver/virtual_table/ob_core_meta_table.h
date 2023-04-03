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

#ifndef OCEANBASE_ROOTSERVER_OB_CORE_META_TABLE_H_
#define OCEANBASE_ROOTSERVER_OB_CORE_META_TABLE_H_

#include "share/ob_virtual_table_projector.h"

namespace oceanbase
{
namespace share
{
class ObLSReplica;
class ObLSTableOperator;
namespace schema
{
class ObTableSchema;
class ObColumnSchemaV2;
}
}
namespace rootserver
{
class ObCoreMetaTable : public common::ObVirtualTableProjector
{
public:
  ObCoreMetaTable();
  virtual ~ObCoreMetaTable();

  int init(share::ObLSTableOperator &lst_operator,
           share::schema::ObSchemaGetterGuard *schema_guard);

  virtual int inner_get_next_row(common::ObNewRow *&row);
private:
  int get_full_row(const share::schema::ObTableSchema *table,
                   const share::ObLSReplica &replica,
                   common::ObIArray<Column> &columns);
  bool inited_;
  share::ObLSTableOperator *lst_operator_;
  share::schema::ObSchemaGetterGuard *schema_guard_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObCoreMetaTable);
};

}//end namespace rootserver
}//end namespace oceanbase

#endif //OCEANBASE_ROOTSERVER_OB_CORE_META_TABLE_H_
