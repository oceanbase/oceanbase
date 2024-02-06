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

#ifndef OB_TABLE_CREATOR_H
#define OB_TABLE_CREATOR_H

#include "rootserver/ob_tablet_creator.h"
#include "share/tablet/ob_tablet_info.h" // ObTabletTablePair

#include "share/tablet/ob_tablet_to_ls_operator.h"

namespace oceanbase
{
namespace share
{
class SCN;
}
namespace rootserver
{
class ObTableCreator
{
public:
  ObTableCreator(
      const uint64_t tenant_id,
      const share::SCN &frozen_scn,
      ObMySQLTransaction &trans)
                : tenant_id_(tenant_id),
                  tablet_creator_(tenant_id, frozen_scn, trans),
                  trans_(trans),
                  ls_id_array_(),
                  inited_(false) {}
  virtual ~ObTableCreator();
  int init(const bool need_tablet_cnt_check);
  int execute();
  void reset();

  // create tablets in some tables, one of which is data table, others are its local indexes
  //
  // @param [in] schemas, tables schema for creating tablets, the first is data table, others are its local indexes
  int add_create_tablets_of_tables_arg(
      const common::ObIArray<const share::schema::ObTableSchema*> &schemas,
      const common::ObIArray<share::ObLSID> &ls_id_array);

  // create tablets for local aux tables(include local_index/aux_lob_table), which are belong to a data table.
  //
  // @param [in] schemas, indexes/aux_lob_table schema for creating tablets
  // @param [in] data_table_schema, data table schema of indexes
  int add_create_tablets_of_local_aux_tables_arg(
      const common::ObIArray<const share::schema::ObTableSchema*> &schemas,
      const share::schema::ObTableSchema *data_table_schema,
      const common::ObIArray<share::ObLSID> &ls_id_array);

  // create tablets of hidden table from original table, used by ddl table redefinition
  int add_create_bind_tablets_of_hidden_table_arg(
      const share::schema::ObTableSchema &orig_table_schema,
      const share::schema::ObTableSchema &hidden_table_schema,
      const common::ObIArray<share::ObLSID> &ls_id_array);

  // create tablets in a table
  //
  // @param [in] table_schema, table schema for creating tablets
  int add_create_tablets_of_table_arg(
      const share::schema::ObTableSchema &table_schema,
      const common::ObIArray<share::ObLSID> &ls_id_array);
private:
  int add_create_tablets_of_tables_arg_(
      const common::ObIArray<const share::schema::ObTableSchema*> &schemas,
      const share::schema::ObTableSchema *data_table_schema,
      const common::ObIArray<share::ObLSID> &ls_id_array);
  int generate_create_tablet_arg_(
      const common::ObIArray<const share::schema::ObTableSchema*> &schemas,
      const ObTableSchema &data_table_schema,
      const lib::Worker::CompatMode &mode,
      const share::ObLSID &ls_id,
      common::ObIArray<share::ObTabletTablePair> &pairs,
      const int64_t part_idx,
      const int64_t subpart_idx,
      const bool is_create_bind_hidden_tablets);
  int get_tablet_list_str_(
      const share::schema::ObTableSchema &table_schema,
      ObSqlString &tablet_list);
private:
  const uint64_t tenant_id_;
  ObTabletCreator tablet_creator_;
  ObMySQLTransaction &trans_;
  common::ObArray<share::ObLSID> ls_id_array_;
  common::ObArray<share::ObTabletToLSInfo> tablet_infos_;
  bool inited_;
};
}
}


#endif /* !OB_TABLE_CREATOR_H */
