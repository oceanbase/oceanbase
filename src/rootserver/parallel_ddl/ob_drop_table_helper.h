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

#ifndef OCEANBASE_ROOTSERVER_OB_DROP_TABLE_HELPER_H_
#define OCEANBASE_ROOTSERVER_OB_DROP_TABLE_HELPER_H_

#include "rootserver/parallel_ddl/ob_ddl_helper.h"
#include "share/ob_tablet_autoincrement_service.h"

namespace oceanbase {
namespace share {
namespace schema {
class ObMultiVersionSchemaService;
}
} // namespace share
namespace obrpc {
class ObDropTableArg;
class ObDDLRes;
} // namespace obrpc
namespace rootserver {
class ObDropTableHelper : public ObDDLHelper {
public:
  ObDropTableHelper(share::schema::ObMultiVersionSchemaService *schema_service,
                    const uint64_t tenant_id,
                    const obrpc::ObDropTableArg &arg,
                    obrpc::ObDropTableRes &res,
                    ObDDLSQLTransaction *external_trans = nullptr);
  virtual ~ObDropTableHelper();
  TO_STRING_KV(K_(arg),
               K_(res),
               K_(table_items),
               K_(existing_table_items),
               K_(database_ids),
               K_(table_schemas),
               K_(drop_table_ids),
               K_(mock_fk_parent_table_schemas),
               K_(dep_objs),
               K_(ddl_stmt_str),
               K_(err_table_list),
               K_(tablet_autoinc_cleaner));
private:
  virtual int init_() override;
  virtual int lock_objects_() override;
  virtual int generate_schemas_() override;
  virtual int calc_schema_version_cnt_() override;
  virtual int operate_schemas_() override;
  virtual int operation_before_commit_() override;
  virtual int clean_on_fail_commit_() override;
  virtual int construct_and_adjust_result_(int &return_ret) override;

  int pre_check_();

  int lock_tables_();
  int check_legitimacy_();
  int gen_mock_fk_parent_table_schemas_();
  int drop_schemas_();

private:
  int lock_databases_by_name_();
  int lock_tables_by_name_();
  int lock_databases_by_id_();
  int prefetch_table_schemas_();
  int lock_objects_by_id_();
  int gen_mock_fk_parent_tables_for_drop_fks_(const ObIArray<const ObForeignKeyInfo*> &fk_infos,
                                              ObArray<ObMockFKParentTableSchema> &new_mock_fk_parent_table_schemas);
  int gen_mock_fk_parent_table_for_drop_table_(const ObIArray<ObForeignKeyInfo> &fk_infos,
                                               const ObForeignKeyInfo &violated_fk_info,
                                               const ObTableSchema &table_schema,
                                               ObArray<ObMockFKParentTableSchema> &mock_fk_parent_table_schemas);
  int collect_aux_table_schemas_(const ObTableSchema &table_schema, ObIArray<const ObTableSchema *> &aux_table_schemas);
  int calc_schema_version_cnt_for_table_(const ObTableSchema &table_schema, bool to_recyclebin);
  int calc_schema_version_cnt_for_dep_objs_();
  int calc_schema_version_cnt_for_sequence_(const ObTableSchema &table_schema);
  int lock_fk_tables_by_id_(const ObTableSchema &table_schema);
  int lock_aux_tables_by_id_(const ObTableSchema &table_schema);
  int lock_triggers_by_id_(const ObTableSchema &table_schema);
  int lock_sequences_by_id_(const ObTableSchema &table_schema);
  int lock_rls_by_id_(const ObTableSchema &table_schema);
  int lock_audits_by_id_(const ObTableSchema &table_schema);
  int lock_sensitive_rules_by_id_(const ObTableSchema &table_schema);
  int add_table_to_tablet_autoinc_cleaner_(const ObTableSchema &table_schema);
  int construct_drop_table_sql_(const ObTableSchema &table_schema, const obrpc::ObTableItem &table_item);
  int drop_table_(const ObTableSchema &table_schema, const ObString *ddl_stmt_str);
  int drop_table_to_recyclebin_(const ObTableSchema &table_schema, const ObString *ddl_stmt_str);
  int drop_triggers_(const ObTableSchema &table_schema);
  int drop_trigger_(const ObTriggerInfo &trigger_info, const ObTableSchema &table_schema);
  int drop_trigger_to_recyclebin_(const ObTriggerInfo &trigger_info);
  int drop_obj_privs_(const uint64_t obj_id, const ObObjectType obj_type);
  int drop_sequences_(const ObTableSchema &table_schema);
  int drop_sequence_(const ObColumnSchemaV2 &column_schema);
  int drop_rls_object_(const ObTableSchema &table_schema);
  int drop_sensitive_column_(const ObTableSchema &table_schema);
  int modify_dep_obj_status_(const int64_t idx);
  int deal_with_mock_fk_parent_tables_(const int64_t idx);
  int deal_with_mock_fk_parent_table_(ObMockFKParentTableSchema &mock_fk_parent_table_schema);
  int create_mock_fk_parent_table_(const ObMockFKParentTableSchema &mock_fk_parent_table_schema);
  int drop_mock_fk_parent_table_(const ObMockFKParentTableSchema &mock_fk_parent_table_schema);
  int alter_mock_fk_parent_table_(ObMockFKParentTableSchema &mock_fk_parent_table_schema);
  int sync_version_for_cascade_mock_fk_parent_table_(const ObIArray<uint64_t> &mock_fk_parent_table_ids);
  bool is_to_recyclebin_(const ObTableSchema &table_schema);
  int log_table_not_exist_msg_(const obrpc::ObTableItem &table_item);
private:
  const obrpc::ObDropTableArg &arg_;
  obrpc::ObDropTableRes &res_;
  ObSArray<obrpc::ObTableItem> table_items_;
  ObArray<obrpc::ObTableItem> existing_table_items_; // only store table items which exsit
  ObArray<uint64_t> database_ids_; // if database id is invalid, this table does not exist, skip it
  ObArray<const ObTableSchema*> table_schemas_; // only store table schemas which exsit
  DropTableIdHashSet drop_table_ids_;
  ObArray<ObArray<ObMockFKParentTableSchema>> mock_fk_parent_table_schemas_;
  ObArray<ObArray<std::pair<uint64_t, share::schema::ObObjectType>>> dep_objs_;
  ObSqlString ddl_stmt_str_;
  ObSqlString err_table_list_;
  ObTabletAutoincCacheCleaner tablet_autoinc_cleaner_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObDropTableHelper);
};

} // namespace rootserver
} // namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_DROP_TABLE_HELPER_H_
