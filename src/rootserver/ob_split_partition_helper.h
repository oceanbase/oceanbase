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

#ifndef OCEANBASE_ROOTSERVER_OB_SPLIT_PARTITION_HELPER_H_
#define OCEANBASE_ROOTSERVER_OB_SPLIT_PARTITION_HELPER_H_

#include "rootserver/ob_ddl_service.h"
#include "rootserver/ob_tablet_creator.h"

namespace oceanbase
{
namespace obrpc
{
struct ObFreezeSplitSrcTabletArg;
struct ObFreezeSplitSrcTabletRes;
}

namespace rootserver
{

class ObSplitPartitionHelper
{
public:
  ObSplitPartitionHelper(
      ObDDLSQLTransaction &trans,
      share::schema::ObSchemaGetterGuard &schema_guard,
      ObIAllocator &allocator,
      const uint64_t tenant_id,
      const uint64_t tenant_data_version,
      const share::ObDDLType split_type,
      const common::ObIArray<share::schema::ObTableSchema *> &new_table_schemas,
      const common::ObIArray<share::schema::ObTableSchema *> &upd_table_schemas,
      const common::ObIArray<const share::schema::ObTableSchema *> &inc_table_schemas,
      const int64_t parallelism)
    : trans_(trans), schema_guard_(schema_guard), allocator_(allocator), tenant_id_(tenant_id), tenant_data_version_(tenant_data_version), split_type_(split_type),
      new_table_schemas_(new_table_schemas), upd_table_schemas_(upd_table_schemas), inc_table_schemas_(inc_table_schemas), parallelism_(parallelism),
      ls_id_(), leader_addr_(), src_tablet_ids_(), dst_tablet_ids_(), start_src_arg_(), start_dst_arg_(), task_id_(0),
      tablet_creator_(), data_end_scn_(), end_autoinc_seqs_() {}
  ~ObSplitPartitionHelper();

  int execute(ObDDLTaskRecord &task_record);
  static int clean_split_src_and_dst_tablet(
      const obrpc::ObCleanSplittedTabletArg &arg,
      const int64_t auto_part_size,
      const int64_t new_schema_version,
      ObMySQLTransaction &trans);
  static int check_allow_split(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const share::schema::ObTableSchema &table_schema);
  static int freeze_split_src_tablet(
      const obrpc::ObFreezeSplitSrcTabletArg &arg,
      obrpc::ObFreezeSplitSrcTabletRes &res,
      const int64_t abs_timeout_us);
  static int get_split_src_tablet_id_if_any(const share::schema::ObTableSchema &table_schema, ObTabletID &tablet_id);
  static int check_enable_global_index_auto_split(const share::schema::ObTableSchema &data_table_schema, bool &enable_auto_split, int64_t &auto_part_size);

private:
  static int prepare_start_args_(
      const uint64_t tenant_id,
      const common::ObIArray<share::schema::ObTableSchema *> &new_table_schemas,
      const common::ObIArray<share::schema::ObTableSchema *> &upd_table_schemas,
      const common::ObIArray<const share::schema::ObTableSchema *> &inc_table_schemas,
      share::ObLSID &ls_id,
      ObAddr &leader_addr,
      ObIArray<ObTabletID> &src_tablet_ids,
      ObIArray<ObArray<ObTabletID>> &dst_tablet_ids,
      storage::ObTabletSplitMdsArg &start_src_arg,
      storage::ObTabletSplitMdsArg &start_dst_arg,
      int64_t &task_id,
      ObIAllocator &allocator,
      ObMySQLTransaction &trans);
  static int prepare_dst_tablet_creator_(
      const uint64_t tenant_id,
      const uint64_t tenant_data_version,
      const share::ObLSID &ls_id,
      const ObAddr &leader_addr,
      const ObIArray<ObTabletID> &src_tablet_ids,
      const ObIArray<ObArray<ObTabletID>> &dst_tablet_ids,
      const ObIArray<const share::schema::ObTableSchema *> &inc_table_schemas,
      const share::schema::ObTableSchema &main_src_table_schema,
      const share::ObDDLType split_type,
      ObIAllocator &allocator,
      ObTabletCreator *&tablet_creator,
      ObMySQLTransaction &trans);
  static int insert_dst_tablet_to_ls_and_table_history_(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const ObIArray<ObArray<ObTabletID>> &dst_tablet_ids,
      const ObIArray<const share::schema::ObTableSchema *> &inc_table_schemas,
      ObMySQLTransaction &trans);
  static int delete_src_tablet_to_ls_and_table_history_(
      const uint64_t tenant_id,
      const ObIArray<ObTabletID> &dst_tablet_ids,
      const int64_t new_schema_version,
      ObMySQLTransaction &trans);
  static int create_ddl_task_(
      const uint64_t tenant_id,
      const int64_t task_id,
      const share::ObDDLType split_type,
      const ObIArray<const share::schema::ObTableSchema *> &inc_table_schemas,
      const int64_t parallelism,
      const share::ObLSID &ls_id,
      const uint64_t tenant_data_version,
      ObIAllocator &allocator,
      ObDDLTaskRecord &task_record,
      ObMySQLTransaction &trans);
  static int start_src_(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const ObAddr &leader_addr,
      const ObIArray<ObTabletID> &src_tablet_ids,
      const storage::ObTabletSplitMdsArg &start_src_arg,
      share::SCN &data_end_scn,
      ObIArray<std::pair<uint64_t, uint64_t>> &end_autoinc_seqs,
      ObMySQLTransaction &trans);
  static int start_dst_(
      const uint64_t tenant_id,
      const uint64_t tenant_data_version,
      const share::ObLSID &ls_id,
      const ObAddr &leader_addr,
      const ObIArray<const share::schema::ObTableSchema *> &inc_table_schemas,
      const ObIArray<ObArray<ObTabletID>> &dst_tablet_ids,
      const share::SCN &data_end_scn,
      const ObIArray<std::pair<uint64_t, uint64_t>> &end_autoinc_seqs,
      const int64_t task_id,
      storage::ObTabletSplitMdsArg &start_dst_arg,
      ObTabletCreator *&tablet_creator,
      ObMySQLTransaction &trans);
  static int check_mem_usage_for_split_(
      const uint64_t tenant_id,
      const int64_t dst_tablets_number);
  static int check_tenant_leader_distributed_(
      const uint64_t tenant_id,
      bool &is_leader_distributed);

private:
  ObDDLSQLTransaction &trans_;
  share::schema::ObSchemaGetterGuard &schema_guard_;
  ObIAllocator &allocator_;
  uint64_t tenant_id_;
  uint64_t tenant_data_version_;
  share::ObDDLType split_type_;
  const static int64_t MYSQL_MAX_NUM_TABLETS_IN_TABLE = 8L * 1024L;
  const static int64_t ORACLE_MAX_NUM_TABLETS_IN_TABLE = 1024L * 1024L - 1L;
  const static int64_t OB_MAX_SPLIT_PER_ROUND = 8L * 1024L;
  const static int64_t MEMORY_USAGE_SPLIT_PER_DST = 8L * 1024L * 1024L;
  const common::ObIArray<share::schema::ObTableSchema *> &new_table_schemas_;
  const common::ObIArray<share::schema::ObTableSchema *> &upd_table_schemas_;
  const common::ObIArray<const share::schema::ObTableSchema *> &inc_table_schemas_;
  int64_t parallelism_;

  // prepared before start_src
  ObLSID ls_id_;
  ObAddr leader_addr_;
  ObArray<ObTabletID> src_tablet_ids_;
  ObArray<ObArray<ObTabletID>> dst_tablet_ids_;
  ObTabletSplitMdsArg start_src_arg_;
  ObTabletSplitMdsArg start_dst_arg_;
  int64_t task_id_;

  // prepared before start_dst
  ObTabletCreator *tablet_creator_;
  share::SCN data_end_scn_;
  ObArray<std::pair<uint64_t, uint64_t>> end_autoinc_seqs_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSplitPartitionHelper);
};

} // namespace rootserver
} // namespace oceanbase
#endif  // OCEANBASE_ROOTSERVER_OB_SPLIT_PARTITION_HELPER_H_
