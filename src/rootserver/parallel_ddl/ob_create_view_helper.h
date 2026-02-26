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
 #ifndef OCEANBASE_ROOTSERVER_OB_CREATE_VIEW_HELPER_H_
 #define OCEANBASE_ROOTSERVER_OB_CREATE_VIEW_HELPER_H_

 #include "rootserver/parallel_ddl/ob_ddl_helper.h"

 namespace oceanbase
 {
 namespace share
 {
 namespace schema
 {
 class ObMultiVersionSchemaService;
 }
 }
 namespace obrpc
 {
 class ObCreateTableArg;
 class ObCreateTableRes;
 }
 namespace rootserver
 {
 class ObCreateViewHelper : public ObDDLHelper
 {
 public:
   ObCreateViewHelper(
     share::schema::ObMultiVersionSchemaService *schema_service,
     const uint64_t tenant_id,
     const obrpc::ObCreateTableArg &arg,
     obrpc::ObCreateTableRes &res);
   virtual ~ObCreateViewHelper();

 private:
   int init_();
   virtual int lock_objects_() override;
   int lock_and_check_database_();
   int lock_and_check_view_name_();
   int lock_object_id_();
   int check_parallel_ddl_conflict_();
   virtual int generate_schemas_() override;
   virtual int calc_schema_version_cnt_() override;
   int drop_schemas_();
   int drop_rls_schemas_();
   int drop_trigger_schemas_();
   int drop_obj_privs_();
   int modify_obj_status_();
   int drop_table_();
   int create_schemas_();
   int print_view_expanded_definition_();
   int create_table_();
   int insert_schema_object_dependency_();
   int check_max_dependency_version_(const common::ObIArray<uint64_t> &obj_ids,
                                     const common::ObIArray<ObSchemaIdVersion> &versions);
   int restore_obj_privs_();
   int handle_error_info_();
   static bool dep_compare_func_(const std::pair<uint64_t, share::schema::ObObjectType> &a,
                                 const std::pair<uint64_t, share::schema::ObObjectType> &b)
                                 { return a.first > b.first
                                          || (a.first == b.first && a.second > b.second); }
   virtual int operate_schemas_() override;
   virtual int clean_on_fail_commit_() override;
   virtual int operation_before_commit_() override;
   virtual int construct_and_adjust_result_(int &return_ret) override;
 private:
   const obrpc::ObCreateTableArg &arg_;
   obrpc::ObCreateTableRes &res_;
   ObTableSchema* new_table_schema_;
   ObString ddl_stmt_str_;
   const ObDatabaseSchema* database_schema_;
   uint64_t orig_table_id_;
   const ObTableSchema* orig_table_schema_;
   ObArray<std::pair<uint64_t, share::schema::ObObjectType>> dep_objs_;
   ObArray<const ObTableSchema*> dep_views_;
   ObArray<const ObRlsPolicySchema*> rls_policy_schemas_;
   ObArray<const ObRlsGroupSchema*> rls_group_schemas_;
   ObArray<const ObRlsContextSchema*> rls_context_schemas_;
   ObArray<const ObTriggerInfo*> trigger_infos_;
   ObArray<ObObjPriv> obj_privs_;
   ObArray<std::pair<ObRawObjPrivArray, ObRawObjPrivArray>> raw_obj_privs_;
 private:
   DISALLOW_COPY_AND_ASSIGN(ObCreateViewHelper);
 };

 } // end namespace rootserver
 } // end namespace oceanbase


 #endif//OCEANBASE_ROOTSERVER_OB_CREATE_VIEW_HELPER_H_
