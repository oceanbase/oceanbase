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

#ifndef OCEANBASE_ROOTSERVER_OB_ALL_VIRTUAL_CORE_INNER_TABLE_H_
#define OCEANBASE_ROOTSERVER_OB_ALL_VIRTUAL_CORE_INNER_TABLE_H_

#include "share/ob_virtual_table_projector.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"

namespace oceanbase
{
namespace share
{
class ObCoreTableProxy;
namespace schema
{
class ObTableSchema;
class ObColumnSchemaV2;
class ObSchemaGetterGuard;
}
}
namespace rootserver
{
class ObVritualCoreInnerTable : public common::ObVirtualTableProjector
{
public:
  ObVritualCoreInnerTable();
  virtual ~ObVritualCoreInnerTable();

  int init(common::ObMySQLProxy &sql_proxy,
           const char *table_name, const uint64_t table_id,
           share::schema::ObSchemaGetterGuard *schema_guard);

  virtual int inner_open();
  virtual int inner_get_next_row(common::ObNewRow *&row);
private:
  int get_full_row(const uint64_t tenant_id,
                   const share::schema::ObTableSchema *table,
                   const share::ObCoreTableProxy &core_table,
                   common::ObIArray<Column> &columns);

  bool inited_;
  common::ObMySQLProxy *sql_proxy_;
  const char *table_name_;
  uint64_t table_id_;
  share::schema::ObSchemaGetterGuard *schema_guard_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObVritualCoreInnerTable);
};

}//end namespace rootserver
}//end namespace oceanbase

#endif //OCEANBASE_ROOTSERVER_OB_ALL_VIRTUAL_CORE_INNER_TABLE_H_
