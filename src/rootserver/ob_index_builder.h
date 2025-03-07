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
struct ObCreateDDLTaskParam;

class ObIndexBuilder
{
public:
  explicit ObIndexBuilder(ObDDLService &ddl_service);
  virtual ~ObIndexBuilder();

  int create_index(const obrpc::ObCreateIndexArg &arg,
                   obrpc::ObAlterTableRes &res);
  int drop_index(const obrpc::ObDropIndexArg &const_arg, obrpc::ObDropIndexRes &res);
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
  int submit_drop_index_task(
      common::ObMySQLTransaction &trans,
      const share::schema::ObTableSchema &data_schema,
      const common::ObIArray<share::schema::ObTableSchema> &index_schemas,
      const obrpc::ObDropIndexArg &arg,
      const common::ObIArray<common::ObTabletID> *inc_data_tablet_ids,
      const common::ObIArray<common::ObTabletID> *del_data_tablet_ids,
      common::ObIAllocator &allocator,
      bool &task_has_exist,
      ObDDLTaskRecord &task_record);
  int submit_drop_vec_index_task(
      common::ObMySQLTransaction &trans,
      const share::schema::ObTableSchema &data_schema,
      const common::ObIArray<share::schema::ObTableSchema> &index_schemas,
      const obrpc::ObDropIndexArg &arg,
      common::ObIAllocator &allocator,
      bool &task_has_exist,
      ObDDLTaskRecord &task_record);
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
                              const int64_t new_fetched_snapshot = 0);
  int submit_rebuild_index_task(common::ObMySQLTransaction &trans,
                                const obrpc::ObRebuildIndexArg &arg,
                                const share::schema::ObTableSchema *data_schema,
                                const common::ObIArray<common::ObTabletID> *inc_data_tablet_ids,
                                const common::ObIArray<common::ObTabletID> *del_data_tablet_ids,
                                const share::schema::ObTableSchema *index_schema,
                                const int64_t parallelism,
                                const int64_t group_id,
                                const uint64_t tenant_data_version,
                                common::ObIAllocator &allocator,
                                ObDDLTaskRecord &task_record);
  int drop_index_on_failed(const obrpc::ObDropIndexArg &arg, obrpc::ObDropIndexRes &res);
private:
  int recognize_vec_hnsw_index_schemas(
      const common::ObIArray<share::schema::ObTableSchema> &index_schemas,
      const bool is_vec_inner_drop,
      int64_t &index_ith,
      int64_t &rowkey_vid_ith,
      int64_t &vid_rowkey_ith,
      int64_t &domain_index_ith,
      int64_t &index_id_ith,
      int64_t &snapshot_data_ith);
  int recognize_vec_ivf_index_schemas(
      const common::ObIArray<share::schema::ObTableSchema> &index_schemas,
      const bool is_vec_inner_drop,
      int64_t &index_ith,
      int64_t &centroid_ith,
      int64_t &cid_vector_ith,
      int64_t &rowkey_cid_ith,
      int64_t &sq_meta_ith,
      int64_t &pq_centroid_ith,
      int64_t &pq_code_ith);
  int recognize_fts_or_multivalue_index_schemas(
      const common::ObIArray<share::schema::ObTableSchema> &index_schemas,
      const bool is_parent_task_dropping_fts,
      const bool is_parent_task_dropping_multivalue,
      int64_t &index_ith,
      int64_t &aux_doc_word_ith,
      int64_t &aux_rowkey_doc_ith,
      int64_t &domain_index_ith,
      int64_t &aux_doc_rowkey_ith,
      int64_t &aux_multivalue_ith);
  int set_basic_infos(const obrpc::ObCreateIndexArg &arg,
                      const share::schema::ObTableSchema &data_schema,
                      share::schema::ObTableSchema &schema);
  int set_global_index_auto_partition_infos(const share::schema::ObTableSchema &data_schema,
                                            share::schema::ObTableSchema &schema);
  int set_index_table_columns(const obrpc::ObCreateIndexArg &arg,
                              const share::schema::ObTableSchema &data_schema,
                              share::schema::ObTableSchema &schema);
  int set_index_table_options(const obrpc::ObCreateIndexArg &arg,
                              const share::schema::ObTableSchema &data_schema,
                              share::schema::ObTableSchema &schema);

  bool is_final_index_status(const share::schema::ObIndexStatus index_status) const;
  int set_local_index_partition_schema(const share::schema::ObTableSchema &data_schema,
                                       share::schema::ObTableSchema &index_schema);

  int check_has_none_shared_index_tables_for_fts_or_multivalue_index_(
      const uint64_t tenant_id,
      const uint64_t data_table_id,
      share::schema::ObSchemaGetterGuard &schema_guard,
      bool &has_fts_or_multivalue_index);
  int check_has_none_shared_index_tables_for_vector_index_(
      const uint64_t tenant_id,
      const uint64_t data_table_id,
      share::schema::ObSchemaGetterGuard &schema_guard,
      bool &has_none_share_vector_index);
  bool ignore_error_code_for_domain_index(
      const int ret,
      const obrpc::ObDropIndexArg &arg,
      const share::schema::ObTableSchema *index_schema = nullptr);
  int set_index_table_column_store_if_need(share::schema::ObTableSchema &table_schema);
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
