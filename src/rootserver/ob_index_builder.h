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

#ifndef OCEANBASE_ROOTSERVER_OB_INDEX_BUILDER_H_
#define OCEANBASE_ROOTSERVER_OB_INDEX_BUILDER_H_

#include "lib/container/ob_array.h"
#include "share/ob_ddl_task_executor.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_struct.h"
#include "ob_ddl_service.h"
#include "share/ob_index_builder_util.h"

namespace oceanbase
{

namespace common
{
class ObMySQLProxy;
}

namespace share
{
class ObReplicaFilterHolder;
class SCN;
namespace schema
{
class ObMultiVersionSchemaService;
class ObTableSchema;
}
}

namespace obrpc
{
class ObSrvRpcProxy;
}

namespace rootserver
{
class ObZoneManager;
class ObDDLService;
class ObDDLTaskRecord;

class ObIndexBuilder
{
public:
  explicit ObIndexBuilder(ObDDLService &ddl_service);
  virtual ~ObIndexBuilder();

  int create_index(const obrpc::ObCreateIndexArg &arg,
                   obrpc::ObAlterTableRes &res);
  int drop_index(const obrpc::ObDropIndexArg &arg, obrpc::ObDropIndexRes &res);

  // Check and update local index status.
  // if not all index table updated return OB_EAGAIN.
  int do_create_index(
      const obrpc::ObCreateIndexArg &arg,
      obrpc::ObAlterTableRes &res);
  int do_create_global_index(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const obrpc::ObCreateIndexArg &arg,
      const share::schema::ObTableSchema &table_schema,
      obrpc::ObAlterTableRes &res);
  int do_create_local_index(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const obrpc::ObCreateIndexArg &arg,
      const share::schema::ObTableSchema &table_schema,
      obrpc::ObAlterTableRes &res);
  int generate_schema(const obrpc::ObCreateIndexArg &arg,
                      share::schema::ObTableSchema &data_schema,
                      const bool global_index_without_column_info,
                      const bool generate_id,
                      share::schema::ObTableSchema &index_schema);
  int generate_container_schema(
      obrpc::ObCreateIndexArg &arg,
      const ObTableSchema &data_schema,
      const share::schema::ObTableSchema &index_schema,
      const bool generate_id);
  int submit_drop_index_task(
      common::ObMySQLTransaction &trans,
      const share::schema::ObTableSchema &data_schema,
      const share::schema::ObTableSchema &index_schema,
      const int64_t schema_version,
      const obrpc::ObDropIndexArg &arg,
      common::ObIAllocator &allocator,
      ObDDLTaskRecord &task_record,
      const ObTableSchema *container_schema = nullptr,
      const ObTableSchema *second_container_schema = nullptr);
  int submit_build_index_task(common::ObMySQLTransaction &trans,
                              const obrpc::ObCreateIndexArg &arg,
                              const share::schema::ObTableSchema *data_schema,
                              const common::ObIArray<common::ObTabletID> *inc_data_tablet_ids,
                              const common::ObIArray<common::ObTabletID> *del_data_tablet_ids,
                              const share::schema::ObTableSchema *index_schema,
                              const int64_t parallelism,
                              const int64_t group_id,
                              const uint64_t tenant_data_version,
                              common::ObIAllocator &allocator,
                              ObDDLTaskRecord &task_record,
                              const ObTableSchema *container_schema = nullptr,
                              const ObTableSchema *second_container_schema = nullptr);
private:
  typedef common::ObArray<std::pair<int64_t, common::ObString> > OrderFTColumns;
  int create_container_table(obrpc::ObCreateIndexArg &arg,
                             ObDDLSQLTransaction &trans,
                             ObArenaAllocator &allocator,
                             const ObTableSchema &index_schema,
                             const ObTableSchema &new_table_schema,
                             const uint64_t offset,
                             const uint64_t tenant_data_version,
                             const bool is_global_index = false);
  class FulltextColumnOrder
  {
  public:
    FulltextColumnOrder() {}
    ~FulltextColumnOrder() {}

    bool operator()(const std::pair<int64_t, common::ObString> &left,
                    const std::pair<int64_t, common::ObString> &right) const
    {
      return left.first < right.first;
    }
  };

  int set_basic_infos(const obrpc::ObCreateIndexArg &arg,
                      const share::schema::ObTableSchema &data_schema,
                      share::schema::ObTableSchema &schema);
  int set_index_table_columns(const obrpc::ObCreateIndexArg &arg,
                              const share::schema::ObTableSchema &data_schema,
                              share::schema::ObTableSchema &schema,
                              const share::ObIndexTableType index_table_type = share::ObIndexTableType::INDEX_TABLE);
  int set_index_table_options(const obrpc::ObCreateIndexArg &arg,
                              const share::schema::ObTableSchema &data_schema,
                              share::schema::ObTableSchema &schema);

  bool is_final_index_status(const share::schema::ObIndexStatus index_status) const;

  int create_index_column_group(const obrpc::ObCreateIndexArg &arg,
                                share::schema::ObTableSchema &index_table_schema);

private:
  ObDDLService &ddl_service_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObIndexBuilder);
};
}//end namespace rootserver
}//end namespace oceanbase

#endif //OCEANBASE_ROOTSERVER_OB_INDEX_BUILDER_H_
