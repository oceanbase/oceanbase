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

#ifndef OCEANBASE_ROOTSERVER_OB_PARTITION_EXCHANGE_H_
#define OCEANBASE_ROOTSERVER_OB_PARTITION_EXCHANGE_H_

#include "lib/container/ob_array.h"
#include "share/ob_ddl_task_executor.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_struct.h"
#include "storage/tablet/ob_tablet_binding_helper.h"

namespace oceanbase
{
namespace share
{
class AutoincParam;
namespace schema
{
class ObTableSchema;
}
}
namespace rootserver
{
class ObDDLService;
class ObDDLSQLTransaction;
class ObPartitionExchange final
{
public:
  typedef std::pair<share::ObLSID, common::ObTabletID> LSTabletID;
  explicit ObPartitionExchange(ObDDLService &ddl_service, const uint64_t data_version);
  ~ObPartitionExchange();
  int check_and_exchange_partition(const obrpc::ObExchangePartitionArg &arg, obrpc::ObAlterTableRes &res, ObSchemaGetterGuard &schema_guard);
private:
  int check_partition_exchange_conditions_(const obrpc::ObExchangePartitionArg &arg, const ObTableSchema &base_table_schema, const ObTableSchema &inc_table_schema, const bool is_oracle_mode, ObSchemaGetterGuard &schema_guard);
  int do_exchange_partition_(const obrpc::ObExchangePartitionArg &arg, obrpc::ObAlterTableRes &res, const ObTableSchema &base_table_schema, const ObTableSchema &inc_table_schema, const bool is_oracle_mode, ObSchemaGetterGuard &schema_guard);
  int lock_exchange_data_table_and_partition_(const uint64_t tenant_id, const ObTableSchema &partitioned_table_schema, const ObTableSchema &non_partitioned_table_schema, const common::ObTabletID &tablet_id, ObDDLSQLTransaction &trans);
  int check_data_table_partition_exchange_conditions_(const ObTableSchema &base_table_schema, const ObTableSchema &inc_table_schema, const ObString &exchange_partition_name, const ObPartitionLevel exchange_partition_level,const bool is_oracle_mode);
  // table level conditions that need to be checked for partition exchange in mysql mode and oracle mode
  int check_table_conditions_in_common_(const ObTableSchema &base_table_schema, const ObTableSchema &inc_table_schema, const ObString &exchange_partition_name, const ObPartitionLevel exchange_partition_level, const bool is_oracle_mode);
  // table level conditions that need to be checked for partition exchange in mysql mode
  int check_table_conditions_in_mysql_mode_(const ObTableSchema &base_table_schema, const ObTableSchema &inc_table_schema);
  // table level conditions that need to be checked for partition exchange in oracle mode
  int check_table_conditions_in_oracle_mode_(const ObTableSchema &base_table_schema, const ObTableSchema &inc_table_schema);
  int check_partition_and_table_tablespace_(const ObTableSchema &base_table_schema, const ObTableSchema &inc_table_schema, const ObString &exchange_partition_name, const ObPartitionLevel exchange_partition_level, const bool is_oracle_mode);
  int check_table_index_infos_(const ObTableSchema &base_table_schema, const ObTableSchema &inc_table_schema, const bool is_oracle_mode);
  int check_table_lob_infos_(const ObTableSchema &base_table_schema, const ObTableSchema &inc_table_schema, const bool is_oracle_mode);
  int check_table_rowkey_infos_(const ObTableSchema &base_table_schema, const ObTableSchema &inc_table_schema, const bool is_oracle_mode);
  int compare_two_rowkey_info_(const common::ObRowkeyInfo &l_rowkey_info, const common::ObRowkeyInfo &r_rowkey_info, bool &is_equal);
  int check_table_column_groups_(const ObTableSchema &base_table_schema, const ObTableSchema &inc_table_schema, const bool is_oracle_mode);
  int compare_two_column_group_schema_(const ObTableSchema &base_table_schema, const ObTableSchema &inc_table_schema, const ObColumnGroupSchema &base_cg_schema, const ObColumnGroupSchema &inc_cg_schema, const bool is_oracle_mode, bool &is_equal);
  // verify if the check constraints of two tables meet the requirements
  int check_table_constraints_(const ObTableSchema &base_table_schema, const ObTableSchema &inc_table_schema, const bool is_oracle_mode);
  int check_table_all_column_conditions_(const ObTableSchema &base_table_schema, const ObTableSchema &inc_table_schema, const bool is_oracle_mode);
  int check_column_level_conditions_(const ObColumnSchemaV2 *base_table_col_schema, const ObColumnSchemaV2 *inc_table_col_schema, const bool is_aux_table_column, const bool is_oracle_mode);
  int check_column_conditions_in_common_(const ObColumnSchemaV2 *base_table_col_schema, const ObColumnSchemaV2 *inc_table_col_schema, const bool is_oracle_mode);
  int check_column_conditions_in_mysql_mode_(const ObColumnSchemaV2 *base_table_col_schema, const ObColumnSchemaV2 *inc_table_col_schema, const bool is_aux_table_column);
  int check_column_conditions_in_oracle_mode_(const ObColumnSchemaV2 *base_table_col_schema, const ObColumnSchemaV2 *inc_table_col_schema, const bool is_aux_table_column);
  int check_generate_column_conditions_(const ObColumnSchemaV2 *base_table_col_schema, const ObColumnSchemaV2 *inc_table_col_schema, bool &is_equal);
  int check_column_flags_(const ObColumnSchemaV2 *base_table_col_schema, const ObColumnSchemaV2 *inc_table_col_schema, bool &is_equal);
  int check_column_default_value_(const ObColumnSchemaV2 *base_table_col_schema, const ObColumnSchemaV2 *inc_table_col_schema, const bool is_oracle_mode, bool &is_equal);
  int compare_default_value_(ObObj &l_value, ObObj &r_value, const bool is_oracle_mode, bool &is_equal);
  int get_next_pair_column_schema_(ObTableSchema::const_column_iterator &base_iter_begin,
                                   ObTableSchema::const_column_iterator &base_iter_end,
                                   ObTableSchema::const_column_iterator &inc_iter_begin,
                                   ObTableSchema::const_column_iterator &inc_iter_end,
                                   const bool is_oracle_mode,
                                   ObColumnSchemaV2 *&base_table_col_schema,
                                   ObColumnSchemaV2 *&inc_table_col_schema);
  int get_next_need_check_column_(ObTableSchema::const_column_iterator &iter_begin, ObTableSchema::const_column_iterator &iter_end, const bool is_oracle_mode, ObColumnSchemaV2 *&table_col_schema);
  int set_global_storage_index_unusable_(const uint64_t tenant_id,
                                         const ObTableSchema &partitioned_data_table_schema,
                                         const ObTableSchema &non_partitioned_data_table_schema,
                                         ObDDLOperator &ddl_operator,
                                         ObDDLSQLTransaction &trans,
                                         ObSchemaGetterGuard &schema_guard);
  int get_data_partition_and_index_(const ObTableSchema &partitioned_data_table_schema, const ObString &data_part_name, const ObPartition *&data_part, int64_t &data_partition_index);
  int get_data_subpartition_and_index_(const ObTableSchema &partitioned_data_table_schema,
                                       const ObString &data_subpart_name,
                                       const ObPartition *&data_part,
                                       const ObSubPartition *&data_subpart,
                                       int64_t &data_partition_index,
                                       int64_t &data_subpartition_index);
  int exchange_data_table_partition_(const uint64_t tenant_id,
                                     const ObTableSchema &partitioned_table_schema,
                                     const ObTableSchema &non_partitioned_table_schema,
                                     const ObPartition &part,
                                     const bool is_oracle_mode,
                                     ObDDLOperator &ddl_operator,
                                     ObDDLSQLTransaction &trans,
                                     ObSchemaGetterGuard &schema_guard);
  int exchange_data_table_subpartition_(const uint64_t tenant_id,
                                        const ObTableSchema &partitioned_table_schema,
                                        const ObTableSchema &non_partitioned_table_schema,
                                        const ObPartition &part,
                                        const ObSubPartition &subpart,
                                        const bool is_oracle_mode,
                                        ObDDLOperator &ddl_operator,
                                        ObDDLSQLTransaction &trans,
                                        ObSchemaGetterGuard &schema_guard);
  int exchange_auxiliary_table_partition_(const uint64_t tenant_id,
                                          const int64_t  ori_data_partition_index,
                                          const ObPartition &ori_data_part,
                                          const bool is_oracle_mode,
                                          ObDDLOperator &ddl_operator,
                                          ObDDLSQLTransaction &trans,
                                          ObSchemaGetterGuard &schema_guard);
  int exchange_auxiliary_table_subpartition_(const uint64_t tenant_id,
                                             const int64_t  ori_data_partition_index,
                                             const int64_t  ori_data_subpartition_index,
                                             const ObPartition &ori_data_part,
                                             const ObSubPartition &ori_data_subpart,
                                             const bool is_oracle_mode,
                                             ObDDLOperator &ddl_operator,
                                             ObDDLSQLTransaction &trans,
                                             ObSchemaGetterGuard &schema_guard);
  int exchange_partition_map_relationship_(const uint64_t tenant_id,
                                           const ObPartition &part,
                                           const ObTableSchema &partitioned_table_schema,
                                           const ObTableSchema &non_partitioned_table_schema,
                                           const bool is_oracle_mode,
                                           ObDDLOperator &ddl_operator,
                                           ObDDLSQLTransaction &trans,
                                           ObSchemaGetterGuard &schema_guard);
  int exchange_subpartition_map_relationship_(const uint64_t tenant_id,
                                              const ObPartition &part,
                                              const ObSubPartition &subpart,
                                              const ObTableSchema &partitioned_table_schema,
                                              const ObTableSchema &non_partitioned_table_schema,
                                              const bool is_oracle_mode,
                                              ObDDLOperator &ddl_operator,
                                              ObDDLSQLTransaction &trans,
                                              ObSchemaGetterGuard &schema_guard);
  int update_exchange_table_non_schema_attributes_(const uint64_t tenant_id,
                                                   const int64_t old_partition_id,
                                                   const int64_t new_partition_id,
                                                   const bool is_exchange_subpartition,
                                                   const ObTableSchema &partitioned_table_schema,
                                                   const ObTableSchema &non_partitioned_table_schema,
                                                   const ObIArray<uint64_t> &exchange_table_ids,
                                                   const ObIArray<ObTabletID> &exchange_tablet_ids,
                                                   const bool is_oracle_mode,
                                                   ObDDLOperator &ddl_operator,
                                                   ObDDLSQLTransaction &trans,
                                                   ObSchemaGetterGuard &schema_guard);
  int update_exchange_table_level_attributes_(const uint64_t tenant_id,
                                              const ObIArray<uint64_t> &exchange_table_ids,
                                              const ObIArray<ObTabletID> &exchange_tablet_ids,
                                              ObTableSchema &partitioned_table_schema,
                                              ObTableSchema &non_partitioned_table_schema,
                                              ObDDLSQLTransaction &trans);
  int update_table_to_tablet_id_mapping_(const uint64_t tenant_id,
                                         const ObIArray<uint64_t> &table_ids,
                                         const ObIArray<ObTabletID> &tablet_ids,
                                         ObDDLSQLTransaction &trans);
  int refresh_table_schema_version_(const uint64_t tenant_id, ObTableSchema &table_schema);
  int update_table_attribute_(const ObTableSchema &table_schema,
                              ObDDLSQLTransaction &trans);
  int push_data_table_schema_version_(const uint64_t tenant_id,
                                      const ObTableSchema &table_schema,
                                      const common::ObString *ddl_stmt_str,
                                      const uint64_t exchange_data_table_id,
                                      int64_t &new_schema_version,
                                      ObDDLSQLTransaction &trans);
  int get_local_storage_index_and_lob_table_schemas_(const ObTableSchema &table_schema,
                                                     const bool is_pt_schema,
                                                     const bool is_oracle_mode,
                                                     ObIArray<const ObTableSchema*> &table_schemas,
                                                     ObSchemaGetterGuard &schema_guard);
  int check_auxiliary_schema_conditions_(const ObTableSchema *table_schema, const bool is_oracle_mode);
  int compare_column_extended_type_info_(const common::ObIArray<common::ObString> &l_extended_type_info,
                                         const common::ObIArray<common::ObString> &r_extended_type_info,
                                         bool &is_equal);
  bool in_supported_table_type_white_list_(const ObTableSchema &table_schema);
  // generate corresponding auxiliary table mapping that need to exchange partitions
  bool in_find_same_aux_table_retry_white_list_(const int ret_code);
  int generate_auxiliary_table_mapping_(const ObTableSchema &partitioned_data_table_schema,
                                        const ObTableSchema &non_partitioned_data_table_schema,
                                        const ObString &exchange_partition_name,
                                        const ObPartitionLevel exchange_partition_level,
                                        const bool is_oracle_mode,
                                        ObSchemaGetterGuard &schema_guard);
  int generate_local_storage_index_and_lob_table_mapping_(const ObTableSchema &partitioned_table_schema,
                                                          ObIArray<const ObTableSchema*> &non_partitioned_table_schemas,
                                                          const ObString &exchange_partition_name,
                                                          const ObPartitionLevel exchange_partition_level,
                                                          const bool is_oracle_mode,
                                                          ObIArray<bool> &used_nt_schema_flag);
  int generate_local_storage_index_table_mapping_in_mysql_mode_(const ObTableSchema &partitioned_table_schema,
                                                                ObIArray<const ObTableSchema*> &non_partitioned_table_schemas,
                                                                const ObString &exchange_partition_name,
                                                                const ObPartitionLevel exchange_partition_level,
                                                                ObIArray<bool> &used_nt_schema_flag,
                                                                bool &find_related_nt_schema);
  int generate_local_storage_index_table_mapping_in_oracle_mode_(const ObTableSchema &partitioned_table_schema,
                                                                 ObIArray<const ObTableSchema*> &non_partitioned_table_schemas,
                                                                 const ObString &exchange_partition_name,
                                                                 const ObPartitionLevel exchange_partition_level,
                                                                 ObIArray<bool> &used_nt_schema_flag,
                                                                 bool &find_related_nt_schema);
  int generate_lob_table_mapping_(const ObTableSchema &partitioned_table_schema,
                                  ObIArray<const ObTableSchema*> &non_partitioned_table_schemas,
                                  const ObString &exchange_partition_name,
                                  const ObPartitionLevel exchange_partition_level,
                                  const bool is_oracle_mode,
                                  ObIArray<bool> &used_nt_schema_flag,
                                  bool &find_related_nt_schema);
  int update_index_status_(const uint64_t tenant_id,
                           const uint64_t table_id,
                           const share::schema::ObIndexStatus status,
                           const bool in_offline_ddl_white_list,
                           ObDDLOperator &ddl_operator,
                           ObDDLSQLTransaction &trans,
                           ObSchemaGetterGuard &schema_guard);
  // Register MDS for read and write defense verification after single table ddl
  int build_single_table_rw_defensive_(const uint64_t tenant_id,
                                       const ObArray<common::ObTabletID> &tablet_ids,
                                       const int64_t schema_version,
                                       ObDDLSQLTransaction &trans);
  int build_modify_tablet_binding_args_v1_(const uint64_t tenant_id,
                                           const ObArray<ObTabletID> &tablet_ids,
                                           const int64_t schema_version,
                                           ObIArray<ObBatchUnbindTabletArg> &modify_args,
                                           ObDDLSQLTransaction &trans);
  int get_tablets_(const uint64_t tenant_id,
                   const ObArray<common::ObTabletID> &tablet_ids,
                   ObIArray<LSTabletID> &tablets,
                   ObDDLSQLTransaction &trans);
  int adapting_cdc_changes_in_exchange_partition_(const uint64_t tenant_id,
                                                  const uint64_t partitioned_table_id,
                                                  const uint64_t non_partitioned_table_id,
                                                  ObDDLSQLTransaction &trans);
  int update_autoinc_column_information_(const uint64_t tenant_id,
                                         const ObTableSchema &partitioned_table_schema,
                                         const ObTableSchema &non_partitioned_table_schema,
                                         ObDDLOperator &ddl_operator,
                                         ObDDLSQLTransaction &trans);
  int update_identity_column_information_(const uint64_t tenant_id,
                                          const ObTableSchema &partitioned_table_schema,
                                          const ObTableSchema &non_partitioned_table_schema,
                                          const bool is_oracle_mode,
                                          ObDDLOperator &ddl_operator,
                                          ObDDLSQLTransaction &trans,
                                          ObSchemaGetterGuard &schema_guard);
  int sync_exchange_partition_stats_info_(const uint64_t tenant_id,
                                          const uint64_t new_table_id,
                                          const uint64_t new_stat_level,
                                          const int64_t old_partition_id,
                                          const int64_t new_partition_id,
                                          const ObTabletID &tablet_id,
                                          const ObTableSchema &orig_table_schema,
                                          ObDDLSQLTransaction &trans);
  int update_table_all_monitor_modified_(const uint64_t tenant_id, const uint64_t new_table_id, const ObTabletID &tablet_id, const ObTableSchema &orig_table_schema, ObDDLSQLTransaction &trans);
  int get_object_id_from_partition_schema_(ObPartitionSchema &partition_schema, const bool get_subpart_only, int64_t &object_id);
private:
  ObDDLService &ddl_service_;
  uint64_t data_version_;
  common::hash::ObHashMap<uint64_t, uint64_t> used_pt_nt_id_map_;
  common::hash::ObHashMap<uint64_t, ObTabletID> used_table_to_tablet_id_map_;
  common::ObSArray<uint64_t> unused_pt_index_id_;
  common::ObSArray<uint64_t> unused_nt_index_id_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObPartitionExchange);
};

struct ObChangeTabletToTableArg final
{
  OB_UNIS_VERSION_V(1);
public:
  ObChangeTabletToTableArg() : tenant_id_(OB_INVALID_TENANT_ID), ls_id_(), base_table_id_(OB_INVALID_ID), inc_table_id_(OB_INVALID_ID), tablet_ids_(), table_ids_() {}
  ~ObChangeTabletToTableArg() {}
  bool is_valid() const;
  int assign(const ObChangeTabletToTableArg &other);
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  uint64_t base_table_id_; // PT table, always with the large amount of data.
  uint64_t inc_table_id_; // NT table, with the incremental data.
   // tablet ids of data table, index table, lob meta/piece table.
  common::ObSArray<ObTabletID> tablet_ids_;
  // the table ids corresponding to the tablet ids.
  common::ObSArray<uint64_t> table_ids_;
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(base_table_id), K_(inc_table_id), K_(tablet_ids), K_(table_ids));
};

}//end namespace rootserver
}//end namespace oceanbase

#endif //OCEANBASE_ROOTSERVER_OB_PARTITION_EXCHANGE_H_
