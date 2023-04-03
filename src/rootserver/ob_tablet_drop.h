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

#ifndef OB_TABLE_DROP_H
#define OB_TABLE_DROP_H

#include "ob_rs_async_rpc_proxy.h" //async rpc
#include "share/ls/ob_ls_table_operator.h"

namespace oceanbase
{
namespace rpc
{
  class ObBatchRemoveTabletArg;
}
namespace rootserver
{

class ObTabletDrop
{
public:
  ObTabletDrop(
      const uint64_t tenant_id,
      ObMySQLTransaction &trans,
      int64_t schema_version)
                : tenant_id_(tenant_id),
                  trans_(trans),
                  allocator_("TbtDrop"),
                  schema_version_(schema_version),
                  inited_(false) {}
  virtual ~ObTabletDrop();
  int init();
  int execute();
  void reset();
  // drop tablets in some table:
  // 1. one of which is data table, other are its local indexes,
  // 2. or all are local indexes of a table
  //
  // @param [in] table_schema, table schema for dropping tablets,
  // 1. the first is data table, others are its local indexes.
  // 2. or all are local indexes of a table
  int add_drop_tablets_of_table_arg(
      const common::ObIArray<const share::schema::ObTableSchema*> &schemas);
  int get_ls_from_table(const share::schema::ObTableSchema &table_schema,
                        common::ObIArray<share::ObLSID> &assign_ls_id_array);
private:
  int drop_tablet_(
      const common::ObIArray<const share::schema::ObTableSchema *> &table_schema_ptr_array,
      const share::ObLSID &ls_key,
      const int64_t i, 
      const int64_t j);
private:
  const uint64_t tenant_id_;
  ObMySQLTransaction &trans_;
  ObArenaAllocator allocator_;
  common::hash::ObHashMap<share::ObLSID, common::ObIArray<ObTabletID>*> args_map_;
  int64_t schema_version_;
  bool inited_;
  const int64_t MAP_BUCKET_NUM = 1024;
};
}
}



#endif /* !OB_TABLE_DROP_H */
