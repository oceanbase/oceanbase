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

 #define USING_LOG_PREFIX RS
 #include "rootserver/parallel_ddl/ob_create_view_helper.h"
 #include "sql/resolver/ddl/ob_create_view_resolver.h"
 #include "share/schema/ob_multi_version_schema_service.h"
 #include "share/schema/ob_table_sql_service.h"
 #include "share/schema/ob_rls_sql_service.h"
 #include "share/schema/ob_trigger_sql_service.h"
 #include "share/schema/ob_priv_sql_service.h"
 #include "share/ob_rpc_struct.h"
 #include "pl/ob_pl_persistent.h"
 using namespace oceanbase::lib;
 using namespace oceanbase::common;
 using namespace oceanbase::share;
 using namespace oceanbase::share::schema;
 using namespace oceanbase::rootserver;

 ObCreateViewHelper::ObCreateViewHelper(
     share::schema::ObMultiVersionSchemaService *schema_service,
     const uint64_t tenant_id,
     const obrpc::ObCreateTableArg &arg,
     obrpc::ObCreateTableRes &res)
   : ObDDLHelper(schema_service, tenant_id, "[parallel create view]"),
     arg_(arg),
     res_(res),
     new_table_schema_(nullptr),
     ddl_stmt_str_(),
     database_schema_(nullptr),
     orig_table_id_(OB_INVALID_ID),
     orig_table_schema_(nullptr),
     dep_objs_(),
     dep_views_(),
     rls_policy_schemas_(),
     rls_group_schemas_(),
     rls_context_schemas_(),
     trigger_infos_(),
     obj_privs_(),
     raw_obj_privs_()
 {}

 ObCreateViewHelper::~ObCreateViewHelper()
 {
 }


 int ObCreateViewHelper::init_()
 {
   int ret = OB_SUCCESS;
   uint64_t data_version = OB_INVALID_VERSION;
   if (OB_FAIL(check_inner_stat_())) {
     LOG_WARN("fail to check inner stat", KR(ret));
   } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, data_version))) {
     LOG_WARN("fail to get min data version", KR(ret), K_(tenant_id));
   } else if (OB_UNLIKELY(data_version >= DATA_VERSION_4_4_0_0
                          && data_version < DATA_VERSION_4_4_1_0)) {
     ret = OB_NOT_SUPPORTED;
     LOG_WARN("parallel create view is not supported before 4.4.1.0", KR(ret), K_(tenant_id), K(data_version));
   } else if (OB_UNLIKELY(USER_VIEW != arg_.schema_.get_table_type())) {
     ret = OB_NOT_SUPPORTED;
     LOG_WARN("not support table type", KR(ret), K(arg_.schema_.get_table_type()));
   } else if (OB_UNLIKELY(OB_INVALID_ID != arg_.schema_.get_table_id())) {
     ret = OB_NOT_SUPPORTED;
     LOG_WARN("create view with table_id in 4.x is not supported",
              KR(ret), K_(tenant_id), "table_id", arg_.schema_.get_table_id());
   } else if (OB_UNLIKELY(OB_INVALID_ID != arg_.schema_.get_tablespace_id())) {
     ret = OB_NOT_SUPPORTED;
     LOG_WARN("create view with tablespace_id in 4.x is not supported",
              KR(ret), K_(tenant_id), "tablespace_id", arg_.schema_.get_tablespace_id());
   } else if (OB_UNLIKELY(OB_INVALID_ID != arg_.schema_.get_tablegroup_id())) {
     ret = OB_NOT_SUPPORTED;
     LOG_WARN("create view with tablegroup_id in 4.x is not supported",
              KR(ret), K_(tenant_id), "tablegroup_id", arg_.schema_.get_tablegroup_id());
   } else if (OB_UNLIKELY(arg_.schema_.is_duplicate_table())) {
     ret = OB_NOT_SUPPORTED;
     LOG_WARN("create duplicate view in 4.x is not supported",
              KR(ret), K_(tenant_id), K(arg_.schema_.is_duplicate_table()));
   } else if (OB_UNLIKELY(PARTITION_LEVEL_ZERO != arg_.schema_.get_part_level())) {
     ret = OB_NOT_SUPPORTED;
     LOG_WARN("create view with partition in 4.x is not supported",
              KR(ret), K_(tenant_id), K(arg_.schema_.get_part_level()));
   } else if (OB_UNLIKELY(arg_.schema_.is_materialized_view())) {
     ret = OB_NOT_SUPPORTED;
     LOG_WARN("create materialized view in 4.x is not supported",
              KR(ret), K_(tenant_id), K(arg_.schema_.is_materialized_view()));
   }
   return ret;
 }

 int ObCreateViewHelper::lock_objects_()
 {
   int ret = OB_SUCCESS;
   DEBUG_SYNC(BEFORE_PARALLEL_DDL_LOCK);
   if (OB_FAIL(check_inner_stat_())) {
     LOG_WARN("fail to check inner stat", KR(ret));
   } else if (OB_FAIL(lock_and_check_database_())) {
     LOG_WARN("fail to lock database name", KR(ret));
   } else if (OB_FAIL(lock_and_check_view_name_())) {
     LOG_WARN("fail to lock view name", KR(ret));
   } else if (OB_FAIL(lock_object_id_())) {
     LOG_WARN("fail to lock object id", KR(ret));
   } else if (OB_FAIL(check_parallel_ddl_conflict_())) {
     LOG_WARN("fail to check parallel ddl conflict", KR(ret));
   }
   DEBUG_SYNC(AFTER_PARALLEL_DDL_LOCK);
   RS_TRACE(lock_objects);
   return ret;
 }

 int ObCreateViewHelper::lock_and_check_database_()
 {
   int ret = OB_SUCCESS;
   const ObString &database_name = arg_.db_name_;
   uint64_t database_id = OB_INVALID_ID;
   if (OB_FAIL(check_inner_stat_())) {
     LOG_WARN("fail to check inner stat", KR(ret));
   } else if (OB_FAIL(add_lock_object_by_database_name_(database_name, transaction::tablelock::SHARE))) {
     LOG_WARN("fail to lock database by name", KR(ret), K_(tenant_id), K(database_name));
   } else if (OB_FAIL(lock_databases_by_name_())) {
     LOG_WARN("fail to lock databses by name", KR(ret), K_(tenant_id));
   } else if (OB_FAIL(check_database_legitimacy_(database_name, database_id))) {
     LOG_WARN("fail to check databse legitimacy", KR(ret), K(database_name),K_(tenant_id));
   } else if (OB_UNLIKELY(database_id != arg_.schema_.get_database_id())) {
     ret = OB_ERR_PARALLEL_DDL_CONFLICT;
     LOG_WARN("database_id not consistent", KR(ret), K(database_id), K(arg_.schema_.get_database_id()));
   } else {
     (void) const_cast<ObTableSchema&>(arg_.schema_).set_database_id(database_id);
   }
   return ret;
 }

 /* check view name confilict
 -- !arg.if_not_exist
     view name should not exist
 -- arg.if_not_exist && arg.is_alter_view
     view name must exist and be a view
 -- arg.if_not_exist && !arg.is_alter_view
     view name could exist or not
     if view name exist, it must be a view

 -- oracle mode
    check oracle object exist with name
    get table id with name
 -- mysql mode
    get mock table id with name
    get table id with name
 */

 int ObCreateViewHelper::lock_and_check_view_name_()
 {
   int ret = OB_SUCCESS;
   const ObString &database_name = arg_.db_name_;
   const ObString &table_name = arg_.schema_.get_table_name();
   const uint64_t database_id = arg_.schema_.get_database_id();
   if (OB_FAIL(check_inner_stat_())) {
     LOG_WARN("fail to check to inner stat", KR(ret));
   } else if (OB_FAIL(add_lock_object_by_name_(database_name, table_name, share::schema::TABLE_SCHEMA,
              transaction::tablelock::EXCLUSIVE))) {
     LOG_WARN("fail to lock object by table name", KR(ret), K_(tenant_id), K(database_name), K(table_name));
   } else if (OB_FAIL(lock_existed_objects_by_name_())) {
     LOG_WARN("fail to lock objects by name", KR(ret), K_(tenant_id));
   }
   const ObTableSchema* table_schema = nullptr;
   uint64_t synonym_id = OB_INVALID_ID;
   ObTableType table_type = MAX_TABLE_TYPE;
   int64_t schema_version = OB_INVALID_VERSION;
   const uint64_t session_id = arg_.schema_.get_session_id();
   bool is_oracle_mode = false;
   if (OB_FAIL(ret)) {
     // do nothing
   } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(tenant_id_, is_oracle_mode))) {
     LOG_WARN("fail to check is oracle mode", KR(ret));
   } else if (is_oracle_mode) {
     if (arg_.is_alter_view_) {
       ret = OB_NOT_SUPPORTED;
       LOG_WARN("not support alter view in oracle mode", KR(ret), K_(tenant_id));
     } else if (OB_FAIL(latest_schema_guard_.check_oracle_object_exist(
                database_id, session_id, table_name, TABLE_SCHEMA,
                INVALID_ROUTINE_TYPE, arg_.if_not_exist_))) {
       LOG_WARN("fail to check oracle object exist", KR(ret), K(database_id), K(session_id), K(table_name), K_(arg_.if_not_exist));
     } else if (arg_.if_not_exist_) {
       // create or replace view
       // could be no object exist
       // or exist in table space, should check table type
       if (OB_FAIL(latest_schema_guard_.get_table_id(database_id, session_id, table_name,
                                                     orig_table_id_, table_type, schema_version))) {
         LOG_WARN("fail to get table id", KR(ret), K_(tenant_id), K(database_id), K(session_id), K(table_name));
       } else if (OB_INVALID_ID != orig_table_id_) {
         if (USER_VIEW == table_type
             || (GCONF.enable_sys_table_ddl && SYSTEM_VIEW == table_type)) {
           // do nothing
         } else if (SYSTEM_VIEW == table_type) {
           ret = OB_OP_NOT_ALLOW;
           LOG_WARN("not allowed to replace sys view when enable_sys_table_ddl is false", KR(ret), K(table_type));
           LOG_USER_ERROR(OB_OP_NOT_ALLOW, "replace sys view when enable_sys_table_ddl is false");
         } else {
           ret = OB_ERR_EXIST_OBJECT;
           LOG_WARN("name is already used by an existing object", KR(ret), K(table_name));
         }
       }
     } else {
       // create view
       // no object exist when !arg_.if_not_exist_
     }
   } else {
     uint64_t mock_table_id = OB_INVALID_ID;
     if (OB_FAIL(latest_schema_guard_.get_mock_fk_parent_table_id(database_id,
                        table_name, mock_table_id))) {
       LOG_WARN("fail to get mock table id", KR(ret), K_(tenant_id), K(database_id), K(table_name));
     } else if (OB_UNLIKELY(OB_INVALID_ID != mock_table_id)) {
       if (arg_.is_alter_view_) {
         ret = OB_ERR_WRONG_OBJECT;
         ObCStringHelper helper;
         LOG_USER_ERROR(OB_ERR_WRONG_OBJECT,
             helper.convert(database_name),
             helper.convert(table_name), "VIEW");
         LOG_WARN("table exist", KR(ret), K_(tenant_id), K(database_id), K(table_name));
       } else {
         ret = OB_ERR_TABLE_EXIST;
         LOG_USER_ERROR(OB_ERR_TABLE_EXIST, arg_.schema_.get_table_name_str().length(),
                    arg_.schema_.get_table_name_str().ptr());
         LOG_WARN("mock table exist", KR(ret), K_(tenant_id), K(database_id), K(session_id), K(table_name),
                                 K(mock_table_id), K(schema_version), K(arg_.if_not_exist_));
       }
     } else if (OB_FAIL(latest_schema_guard_.get_table_id(database_id, session_id, table_name,
                                                   orig_table_id_, table_type, schema_version))) {
       LOG_WARN("fail to get table id", KR(ret), K_(tenant_id), K(database_id), K(session_id), K(table_name));
     } else if (OB_INVALID_ID == orig_table_id_) {
       // view not exist
       // alter view asks for existed view
       if (arg_.is_alter_view_) {
         ret = OB_TABLE_NOT_EXIST;
         ObCStringHelper helper;
         LOG_USER_ERROR(OB_TABLE_NOT_EXIST,
                        helper.convert(database_name),
                        helper.convert(table_name));
       }
     } else {
       // view should not exist when create view
       if (!arg_.if_not_exist_) {
         ret = OB_ERR_TABLE_EXIST;
         LOG_USER_ERROR(OB_ERR_TABLE_EXIST, arg_.schema_.get_table_name_str().length(),
                    arg_.schema_.get_table_name_str().ptr());
         LOG_WARN("table exist", KR(ret), K_(tenant_id), K(database_id), K(session_id), K(table_name),
                               K_(orig_table_id), K(schema_version), K(arg_.if_not_exist_));
       // create or replace / alter view need to check schema type is USER/SYSTEM VIEW
       } else if (USER_VIEW == table_type
                  || (GCONF.enable_sys_table_ddl && SYSTEM_VIEW == table_type)) {
         // do nothing
       } else if (SYSTEM_VIEW == table_type) {
         ret = OB_OP_NOT_ALLOW;
         LOG_WARN("not allowed to replace sys view when enable_sys_table_ddl is false", KR(ret), K(table_type));
         LOG_USER_ERROR(OB_OP_NOT_ALLOW, "replace sys view when enable_sys_table_ddl is false");
       } else {
         ret = OB_ERR_WRONG_OBJECT;
         ObCStringHelper helper;
         LOG_USER_ERROR(OB_ERR_WRONG_OBJECT,
                        helper.convert(database_name),
                        helper.convert(table_name), "VIEW");
         LOG_WARN("table exist", KR(ret), K_(tenant_id), K(database_id), K(table_name));
       }
     }
   }
   return ret;
 }

 int ObCreateViewHelper::lock_object_id_()
 {
   int ret = OB_SUCCESS;
   if (OB_FAIL(check_inner_stat_())) {
     LOG_WARN("fail to check inner stat", KR(ret));
   } else if (OB_FAIL(add_lock_object_by_id_(arg_.schema_.get_database_id(),
              share::schema::DATABASE_SCHEMA, transaction::tablelock::SHARE))) {
     LOG_WARN("fail to add lock databse id", KR(ret), K_(tenant_id), K(arg_.schema_.get_database_id()));
   } else if (OB_INVALID_ID != orig_table_id_
              && OB_FAIL(add_lock_object_by_id_(orig_table_id_, VIEW_SCHEMA, transaction::tablelock::EXCLUSIVE))) {
     LOG_WARN("fail to add lock object", KR(ret));
   } else if (OB_FAIL(lock_existed_objects_by_id_())) {
     LOG_WARN("fail to lock existed objects by id", KR(ret));
   } else if (OB_INVALID_ID != orig_table_id_) {
     if (OB_FAIL(latest_schema_guard_.get_table_schema(orig_table_id_, orig_table_schema_))) {
       LOG_WARN("fail to get table schema", KR(ret), K_(tenant_id), K_(orig_table_id));
     } else if (OB_ISNULL(orig_table_schema_)) {
       ret = OB_ERR_UNEXPECTED;
       LOG_WARN("orig table schema is null", KR(ret));
     }
   }
   if (OB_SUCC(ret)) {
     for (int64_t i = 0; OB_SUCC(ret) && (i < arg_.dep_infos_.count()); ++i) {
       const ObDependencyInfo &dep = arg_.dep_infos_.at(i);
       ObSchemaType schema_type = transfer_obj_type_to_schema_type_for_dep_(dep.get_ref_obj_type());
       if (OB_UNLIKELY(OB_MAX_SCHEMA == schema_type)) {
         ret = OB_INVALID_ARGUMENT;
         LOG_WARN("invalid obj type", KR(ret), K(dep.get_ref_obj_type()));
       } else if (OB_FAIL(add_lock_object_by_id_(dep.get_ref_obj_id(), schema_type, transaction::tablelock::SHARE))) {
         LOG_WARN("fail to add lock object", KR(ret));
       }
     }

     if (OB_SUCC(ret)) {
       for (int64_t i = 0; OB_SUCC(ret) && (i < arg_.based_schema_object_infos_.count()); ++i) {
         const ObBasedSchemaObjectInfo &info = arg_.based_schema_object_infos_.at(i);
         if (is_inner_pl_udt_id(info.schema_id_) || is_inner_pl_object_id(info.schema_id_)) {
           // do nothing
         } else if (OB_FAIL(add_lock_object_by_id_(info.schema_id_,
                                                   info.schema_type_,
                                                   transaction::tablelock::SHARE))) {
           LOG_WARN("fail to lock based object schema id", KR(ret), K_(tenant_id));
         }
       }
     }
   }
   ObArray<std::pair<uint64_t, share::schema::ObObjectType>> dep_objs_before_lock;
   if (OB_FAIL(ret)) {
   } else if (OB_NOT_NULL(orig_table_schema_)) {
     if (OB_FAIL(ObDependencyInfo::collect_all_dep_objs(tenant_id_, orig_table_schema_->get_table_id(),
                                                        *sql_proxy_, dep_objs_before_lock))) {
       LOG_WARN("fail to collect dep obj", KR(ret), K_(tenant_id), K(orig_table_schema_->get_table_id()));
     } else {
       for (int64_t i = 0; OB_SUCC(ret) && i < dep_objs_before_lock.count(); ++i) {
         ObSchemaType schema_type = transfer_obj_type_to_schema_type_for_dep_(dep_objs_before_lock.at(i).second);
         if (ObObjectType::VIEW == dep_objs_before_lock.at(i).second) {
           if (OB_FAIL(add_lock_object_by_id_(dep_objs_before_lock.at(i).first,
                                              VIEW_SCHEMA, transaction::tablelock::EXCLUSIVE))) {
             LOG_WARN("fail to add lock object", KR(ret));
           }
         }
       }
     }
     const ObIArray<uint64_t> &trigger_list = orig_table_schema_->get_trigger_list();
     for (int64_t i = 0; OB_SUCC(ret) && i < trigger_list.count(); ++i) {
       if (OB_FAIL(add_lock_object_by_id_(trigger_list.at(i), TRIGGER_SCHEMA, transaction::tablelock::EXCLUSIVE))) {
         LOG_WARN("fail to add lock object", KR(ret));
       }
     }
     const ObIArray<uint64_t> &rls_policy_list = orig_table_schema_->get_rls_policy_ids();
     for (int64_t i = 0; OB_SUCC(ret) && i < rls_policy_list.count(); ++i) {
       if (OB_FAIL(add_lock_object_by_id_(rls_policy_list.at(i), RLS_POLICY_SCHEMA, transaction::tablelock::EXCLUSIVE))) {
         LOG_WARN("fail to add lock object", KR(ret));
       }
     }
     const ObIArray<uint64_t> &rls_group_list = orig_table_schema_->get_rls_group_ids();
     for (int64_t i = 0; OB_SUCC(ret) && i < rls_group_list.count(); ++i) {
       if (OB_FAIL(add_lock_object_by_id_(rls_group_list.at(i), RLS_GROUP_SCHEMA, transaction::tablelock::EXCLUSIVE))) {
         LOG_WARN("fail to add lock object", KR(ret));
       }
     }
     const ObIArray<uint64_t> &rls_context_list = orig_table_schema_->get_rls_context_ids();
     for (int64_t i = 0; OB_SUCC(ret) && i < rls_context_list.count(); ++i) {
       if (OB_FAIL(add_lock_object_by_id_(rls_context_list.at(i), RLS_CONTEXT_SCHEMA, transaction::tablelock::EXCLUSIVE))) {
         LOG_WARN("fail to add lock object", KR(ret));
       }
     }
   }

   if (FAILEDx(add_lock_table_udt_id_(arg_.schema_))) {
     LOG_WARN("fail to add lock table udt id", KR(ret));
   }

   if (FAILEDx(lock_existed_objects_by_id_())) {
     LOG_WARN("fail to lock objects by id", KR(ret));
   } else if (OB_NOT_NULL(orig_table_schema_)) {
     if (OB_FAIL(ObDependencyInfo::collect_all_dep_objs(tenant_id_, orig_table_schema_->get_table_id(), *sql_proxy_, dep_objs_))) {
       LOG_WARN("fail to collect dep obj", KR(ret), K_(tenant_id), K(orig_table_schema_->get_table_id()));
     } else if (dep_objs_.count() != dep_objs_before_lock.count()) {
       ret = OB_ERR_PARALLEL_DDL_CONFLICT;
       LOG_WARN("dep objs count not consistent", KR(ret), K(dep_objs_.count()), K(dep_objs_before_lock.count()));
     } else {
       lib::ob_sort(dep_objs_before_lock.begin(), dep_objs_before_lock.end(), dep_compare_func_);
       lib::ob_sort(dep_objs_.begin(), dep_objs_.end(), dep_compare_func_);
       for (int64_t i = 0; OB_SUCC(ret) && i < dep_objs_.count(); ++i) {
         if (dep_objs_before_lock.at(i).first != dep_objs_.at(i).first) {
           ret = OB_ERR_PARALLEL_DDL_CONFLICT;
           LOG_WARN("dep obj in double check not in exist list", KR(ret),
                    K(dep_objs_.at(i).first), K(dep_objs_.at(i).second),
                    K(dep_objs_before_lock.at(i).first), K(dep_objs_before_lock.at(i).second));
         } else if (dep_objs_before_lock.at(i).second != dep_objs_.at(i).second) {
           ret = OB_ERR_UNEXPECTED;
           LOG_WARN("different obj type with same obj id is unexpected", KR(ret),
                    K(dep_objs_.at(i).first), K(dep_objs_.at(i).second),
                    K(dep_objs_before_lock.at(i).first), K(dep_objs_before_lock.at(i).second));
         }
       }
     }
   }
   return ret;
 }

 int ObCreateViewHelper::check_parallel_ddl_conflict_()
 {
   int ret = OB_SUCCESS;
   if (OB_FAIL(check_inner_stat_())) {
     LOG_WARN("fail to check inner stat", KR(ret));
   } else if (OB_FAIL(ObDDLHelper::check_parallel_ddl_conflict_(arg_.based_schema_object_infos_))) {
     LOG_WARN("fail to check parallel ddl conflict", KR(ret), K_(arg_.based_schema_object_infos));
   } else if (OB_FAIL(check_table_udt_exist_(arg_.schema_))) {
     LOG_WARN("fail to check table udt exist", KR(ret));
   } else {
     bool exist = true;
     ObArray<uint64_t> table_ids;
     ObArray<uint64_t> routine_ids;
     ObArray<uint64_t> synonym_ids;
     ObArray<uint64_t> package_ids;
     ObArray<uint64_t> sequence_ids;
     ObArray<uint64_t> type_ids;
     for (int64_t i = 0; OB_SUCC(ret) && (i < arg_.dep_infos_.count()); ++i) {
       const ObDependencyInfo &dep = arg_.dep_infos_.at(i);
       if (is_inner_pl_object_id(dep.get_ref_obj_id())
           || is_inner_pl_udt_id(dep.get_ref_obj_id())) {
         // do nothing
         // pl inner object should check in sys tenant
       } else {
         switch (dep.get_ref_obj_type()) {
           case ObObjectType::TABLE:
           case ObObjectType::VIEW:
            // create view v1 as select * from odps_catalog.default.table;
            // catalog table should not check
            // because  resolver gen table schema temporarily
            // this schema will not store in rs do not need
            // to check check_max_dependency_version_
            if (!is_external_object_id(dep.get_ref_obj_id())) {
              if (OB_FAIL(table_ids.push_back(dep.get_ref_obj_id()))) {
                LOG_WARN("fail to push back table id", KR(ret), K(dep));
              }
            }
             break;
           case ObObjectType::PROCEDURE:
           case ObObjectType::FUNCTION:
             if (OB_FAIL(routine_ids.push_back(dep.get_ref_obj_id()))) {
               LOG_WARN("fail to push back routine id", KR(ret), K(dep));
             }
             break;
           case ObObjectType::SEQUENCE:
             if (OB_FAIL(sequence_ids.push_back(dep.get_ref_obj_id()))) {
               LOG_WARN("fail to push back sequence id", KR(ret), K(dep));
             }
           case ObObjectType::SYNONYM:
             if (OB_FAIL(synonym_ids.push_back(dep.get_ref_obj_id()))) {
               LOG_WARN("fail to push back synonym id", KR(ret), K(dep));
             }
             break;
           case ObObjectType::PACKAGE:
           case ObObjectType::PACKAGE_BODY:
             if (OB_FAIL(package_ids.push_back(dep.get_ref_obj_id()))) {
               LOG_WARN("fail to push back package id", KR(ret), K(dep));
             }
             break;
           case ObObjectType::TYPE:
           case ObObjectType::TYPE_BODY:
             if (OB_FAIL(type_ids.push_back(dep.get_ref_obj_id()))) {
               LOG_WARN("fail to push back type id", KR(ret), K(dep));
             }
             break;
           default:
             ret = OB_NOT_SUPPORTED;
             LOG_WARN("unexpected obj type", KR(ret), K(dep));
             break;
         }
       }
     }
   #ifndef CHECK_MAX_DEPENDENCY_VERSION
   #define CHECK_MAX_DEPENDENCY_VERSION(SCHEMA_TYPE) \
     ObArray<ObSchemaIdVersion> SCHEMA_TYPE##_schema_versions; \
     if (OB_FAIL(ret)) { \
     } else if (0 == SCHEMA_TYPE##_ids.count()) { \
     } else if (OB_FAIL(latest_schema_guard_.get_##SCHEMA_TYPE##_schema_versions(SCHEMA_TYPE##_ids, SCHEMA_TYPE##_schema_versions))) { \
       LOG_WARN("failed to get " #SCHEMA_TYPE " schema versions", KR(ret), K_(tenant_id), K(SCHEMA_TYPE##_ids)); \
     } else if (OB_FAIL(check_max_dependency_version_(SCHEMA_TYPE##_ids, SCHEMA_TYPE##_schema_versions))) { \
       LOG_WARN("fail to check max dependency version", KR(ret), K(SCHEMA_TYPE##_ids), K(SCHEMA_TYPE##_schema_versions)); \
     }
     CHECK_MAX_DEPENDENCY_VERSION(table)
     CHECK_MAX_DEPENDENCY_VERSION(routine)
     CHECK_MAX_DEPENDENCY_VERSION(synonym)
     CHECK_MAX_DEPENDENCY_VERSION(package)
     CHECK_MAX_DEPENDENCY_VERSION(type)
     CHECK_MAX_DEPENDENCY_VERSION(sequence)
   #undef CHECK_MAX_DEPENDENCY_VERSION
   #endif
   }
   return ret;
 }
 int ObCreateViewHelper::check_max_dependency_version_(const common::ObIArray<uint64_t> &obj_ids,
                                                       const common::ObIArray<ObSchemaIdVersion> &versions)
 {
   int ret = OB_SUCCESS;
   LOG_TRACE("check dep version", K(obj_ids), K(versions));
   if (OB_UNLIKELY(obj_ids.count() != versions.count())) {
     ret = OB_ERR_PARALLEL_DDL_CONFLICT;
     LOG_WARN("obj_ids' count not match versions' count", KR(ret), K(obj_ids.count()), K(versions.count()),
                                                          K(obj_ids), K(versions));
   } else {
     for (uint64_t i = 0; OB_SUCC(ret) && i < versions.count(); ++i) {
       if (versions.at(i).get_schema_version() > arg_.schema_.get_max_dependency_version()) {
         ret = OB_ERR_PARALLEL_DDL_CONFLICT;
         LOG_WARN("table schema version larger than max dependency version", KR(ret),
                  K(versions.at(i).get_schema_version()),
                  K(arg_.schema_.get_max_dependency_version()));
       }
     }
   }
   return ret;
 }

 int ObCreateViewHelper::generate_schemas_()
 {
   int ret = OB_SUCCESS;
   ObIDGenerator id_generator;
   const uint64_t object_cnt = 1;
   uint64_t object_id = OB_INVALID_ID;
   void *new_table_ptr = nullptr;
   if (OB_FAIL(check_inner_stat_())) {
     LOG_WARN("fail to check inner stat", KR(ret));
   } else if (OB_FAIL(gen_object_ids_(object_cnt, id_generator))) {
     LOG_WARN("fail to gen object id", KR(ret), K(object_cnt));
   } else if (OB_FAIL(id_generator.next(object_id))) {
     LOG_WARN("fail to get next object id", KR(ret));
   } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator_, arg_.schema_, new_table_schema_))) {
     LOG_WARN("fail to allocate new table schema", KR(ret), K_(tenant_id));
   } else {
     new_table_schema_->set_table_id(object_id);
   }
   if (FAILEDx(print_view_expanded_definition_())) {
     LOG_WARN("fail to print view expanded definition", KR(ret));
   }
   if (OB_SUCC(ret) && OB_NOT_NULL(orig_table_schema_)) {
     const uint64_t orig_table_id = orig_table_schema_->get_table_id();
     if (OB_FAIL(latest_schema_guard_.get_obj_privs(orig_table_id, ObObjectType::TABLE, obj_privs_))) {
       LOG_WARN("fail to get obj privs", KR(ret), K_(tenant_id), K(orig_table_id));
     } else {
       for (int64_t i = 0; OB_SUCC(ret) && i < obj_privs_.count(); ++i) {
         std::pair<ObRawObjPrivArray, ObRawObjPrivArray> raw_obj_priv;
         ObObjPriv &obj_priv = obj_privs_.at(i);
         if (OB_FAIL(ObPrivPacker::raw_option_obj_priv_from_pack(obj_priv.get_obj_privs(), raw_obj_priv.first))) {
           LOG_WARN("fail to raw option obj priv from pack", KR(ret), K(obj_priv));
         } else if (OB_FAIL(ObPrivPacker::raw_no_option_obj_priv_from_pack(obj_priv.get_obj_privs(), raw_obj_priv.second))) {
           LOG_WARN("fail to raw no option obj priv from pack", KR(ret), K(obj_priv));
         } else if (OB_FAIL(raw_obj_privs_.push_back(raw_obj_priv))) {
           LOG_WARN("fail to push back raw obj priv", KR(ret));
         }
       }
     }
     const ObIArray<uint64_t> &trigger_list = orig_table_schema_->get_trigger_list();
     for (int64_t i = 0; OB_SUCC(ret) && i < trigger_list.count(); ++i) {
       const ObTriggerInfo* trigger_info = nullptr;
       if (OB_FAIL(latest_schema_guard_.get_trigger_info(trigger_list.at(i), trigger_info))) {
         LOG_WARN("fail to get trigger info", KR(ret), K_(tenant_id), K(trigger_list.at(i)));
       } else if (OB_ISNULL(trigger_info)) {
         ret = OB_ERR_PARALLEL_DDL_CONFLICT;
         LOG_WARN("trigger info is null, may be dropped", KR(ret));
       } else if (OB_UNLIKELY(trigger_info->is_in_recyclebin())) {
         ret = OB_ERR_UNEXPECTED;
         LOG_WARN("trigger is in recyclebin", KR(ret), KP(trigger_info));
       } else if (OB_FAIL(trigger_infos_.push_back(trigger_info))) {
         LOG_WARN("fail to push back trigger info", KR(ret));
       }
     }
     const ObIArray<uint64_t> &rls_policy_list = orig_table_schema_->get_rls_policy_ids();
     for (int64_t i = 0; OB_SUCC(ret) && i < rls_policy_list.count(); ++i) {
       const ObRlsPolicySchema* rls_policy = nullptr;
       if (OB_FAIL(latest_schema_guard_.get_rls_policys(rls_policy_list.at(i), rls_policy))) {
         LOG_WARN("fail to get rls policy", KR(ret), K_(tenant_id), K(rls_policy_list.at(i)));
       } else if (OB_ISNULL(rls_policy)) {
         ret = OB_ERR_PARALLEL_DDL_CONFLICT;
         LOG_WARN("rls policy is null, may be dropped", KR(ret));
       } else if (OB_FAIL(rls_policy_schemas_.push_back(rls_policy))) {
         LOG_WARN("fail to push back rls policy", KR(ret));
       }
     }
     const ObIArray<uint64_t> &rls_group_list = orig_table_schema_->get_rls_group_ids();
     for (int64_t i = 0; OB_SUCC(ret) && i < rls_group_list.count(); ++i) {
       const ObRlsGroupSchema* rls_group = nullptr;
       if (OB_FAIL(latest_schema_guard_.get_rls_groups(rls_policy_list.at(i), rls_group))) {
         LOG_WARN("fail to get rls group", KR(ret), K_(tenant_id), K(rls_group_list.at(i)));
       } else if (OB_ISNULL(rls_group)) {
         ret = OB_ERR_PARALLEL_DDL_CONFLICT;
         LOG_WARN("rls group is null, may be dropped", KR(ret));
       } else if (OB_FAIL(rls_group_schemas_.push_back(rls_group))) {
         LOG_WARN("fail to push back rls group", KR(ret));
       }
     }
     const ObIArray<uint64_t> &rls_context_list = orig_table_schema_->get_rls_context_ids();
     for (int64_t i = 0; OB_SUCC(ret) && i < rls_context_list.count(); ++i) {
       const ObRlsContextSchema* rls_context = nullptr;
       if (OB_FAIL(latest_schema_guard_.get_rls_contexts(rls_context_list.at(i), rls_context))) {
         LOG_WARN("fail to get rls context", KR(ret), K_(tenant_id), K(rls_policy_list.at(i)));
       } else if (OB_ISNULL(rls_context)) {
         ret = OB_ERR_PARALLEL_DDL_CONFLICT;
         LOG_WARN("rls context is null, may be dropped", KR(ret));
       } else if (OB_FAIL(rls_context_schemas_.push_back(rls_context))) {
         LOG_WARN("fail to push back rls context", KR(ret));
       }
     }
     for (int64_t i = 0; OB_SUCC(ret) && i < dep_objs_.count(); ++i) {
       if (ObObjectType::VIEW == dep_objs_.at(i).second) {
         const ObTableSchema* view_schema = nullptr;
         if (OB_FAIL(latest_schema_guard_.get_table_schema(dep_objs_.at(i).first, view_schema))) {
           LOG_WARN("fail to get table schema", KR(ret), K_(tenant_id), K(dep_objs_.at(i).first));
         } else if (OB_ISNULL(view_schema)) {
           ret = OB_ERR_PARALLEL_DDL_CONFLICT;
         } else if (ObObjectStatus::INVALID == view_schema->get_object_status()) {
           // do nothing
         } else if (OB_FAIL(dep_views_.push_back(view_schema))) {
           LOG_WARN("fail to push back view schema", KR(ret));
         }
       }
     }
   }
   RS_TRACE(generate_schemas);
   return ret;
 }

 int ObCreateViewHelper::print_view_expanded_definition_()
 {
   int ret = OB_SUCCESS;
   char *buf = nullptr;
   int64_t buf_len = OB_MAX_VARCHAR_LENGTH;
   int64_t pos = 0;
   bool is_oracle_mode = false;
   if (OB_ISNULL(buf = static_cast<char*>(allocator_.alloc(buf_len)))) {
     ret = OB_ALLOCATE_MEMORY_FAILED;
     LOG_WARN("fail to allocate memory", KR(ret), K(buf_len));
   } else if (OB_ISNULL(new_table_schema_)) {
     ret = OB_ERR_UNEXPECTED;
     LOG_WARN("new_table_schema_ is null", KR(ret));
   } else if (OB_FAIL(latest_schema_guard_.get_database_schema(arg_.schema_.get_database_id(), database_schema_))) {
     LOG_WARN("fail to get database schema", KR(ret));
   } else if (OB_ISNULL(database_schema_)) {
     ret = OB_ERR_UNEXPECTED;
     LOG_WARN("database schema is null", KR(ret), K(arg_.schema_.get_database_id()));
   } else if (OB_FAIL(new_table_schema_->check_if_oracle_compat_mode(is_oracle_mode))) {
     LOG_WARN("fail to check oracle mode", KR(ret));
   } else if (OB_FAIL(databuff_printf(buf, buf_len, pos,
              is_oracle_mode ? "CREATE%s %sVIEW \"%s\".\"%s\" AS %.*s;"
                             : "CREATE%s %sVIEW `%s`.`%s` AS %.*s;",
              arg_.if_not_exist_ ? " OR REPLACE" : "",
              new_table_schema_->is_materialized_view() ? "MATERIALIZED " : "",
              database_schema_->get_database_name(),
              new_table_schema_->get_table_name(),
              new_table_schema_->get_view_schema().get_view_definition_str().length(),
              new_table_schema_->get_view_schema().get_view_definition_str().ptr()))) {
     LOG_WARN("fail to print view definition", KR(ret));
   } else {
     ddl_stmt_str_.assign_ptr(buf, static_cast<int32_t>(pos));
   }
   return ret;
 }

 int ObCreateViewHelper::calc_schema_version_cnt_()
 {
   int ret = OB_SUCCESS;
   if (OB_FAIL(check_inner_stat_())) {
     LOG_WARN("fail to check inner stat", KR(ret));
   } else {
     schema_version_cnt_ = 0;
     if (OB_NOT_NULL(orig_table_schema_)) {
       // drop trigger
       schema_version_cnt_ += trigger_infos_.count();
       // modify status
       schema_version_cnt_ += dep_views_.count();
       // drop privs
       schema_version_cnt_ += obj_privs_.count();
       // drop rls
       schema_version_cnt_ += rls_policy_schemas_.count();
       schema_version_cnt_ += rls_group_schemas_.count();
       schema_version_cnt_ += rls_context_schemas_.count();
       // drop orig view
       schema_version_cnt_ ++;
       // restore privs
       for (int64_t i = 0; i < raw_obj_privs_.count(); ++i) {
         if (raw_obj_privs_.at(i).first.count() > 0) {
           schema_version_cnt_ ++;
         }
         if (raw_obj_privs_.at(i).second.count() > 0) {
           schema_version_cnt_ ++;
         }
       }
     }
     // create view
     schema_version_cnt_ ++;
     // 1503
     schema_version_cnt_ ++;
   }
   return ret;
 }

 int ObCreateViewHelper::create_schemas_()
 {
   int ret = OB_SUCCESS;
   if (OB_FAIL(check_inner_stat_())) {
     LOG_WARN("fail to check inner stat", KR(ret));
   } else if (OB_FAIL(create_table_())) {
     LOG_WARN("fail to create table", KR(ret));
   } else if (OB_FAIL(insert_schema_object_dependency_())) {
     LOG_WARN("fail to insert schema object dependency", KR(ret));
   } else if (OB_FAIL(restore_obj_privs_())) {
     LOG_WARN("fail to restore obj privs", KR(ret));
   } else if (OB_FAIL(handle_error_info_())) {
     LOG_WARN("fail to handle error info", KR(ret));
   } else {
     int64_t last_schema_version = OB_INVALID_VERSION;
     ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
     ObSchemaService *schema_service_impl = schema_service_->get_schema_service();
     ObSchemaVersionGenerator *tsi_generator = GET_TSI(TSISchemaVersionGenerator);
     if (OB_ISNULL(tsi_generator)) {
       ret = OB_ERR_UNEXPECTED;
       LOG_WARN("tsi generator is null", KR(ret));
     } else if (OB_ISNULL(schema_service_impl)) {
       ret = OB_ERR_UNEXPECTED;
       LOG_WARN("schema service must not by null", KR(ret));
     } else if (OB_FAIL(tsi_generator->get_current_version(last_schema_version))) {
       LOG_WARN("fail to get end version", KR(ret), K_(tenant_id), K_(arg));
     } else if (OB_UNLIKELY(last_schema_version <= 0)) {
       ret = OB_ERR_UNEXPECTED;
       LOG_WARN("last schema version is invalid", KR(ret), K_(tenant_id), K(last_schema_version));
     } else if (OB_FAIL(ddl_operator.insert_ori_schema_version(get_trans_(), tenant_id_, new_table_schema_->get_table_id(), last_schema_version))) {
       LOG_WARN("fail to insert ori schema version", KR(ret), K_(tenant_id), K(last_schema_version));
     }
   }
   RS_TRACE(create_schemas);
   return ret;
 }

 int ObCreateViewHelper::create_table_()
 {
   int ret = OB_SUCCESS;
   ObSchemaService *schema_service_impl = nullptr;
   if (OB_FAIL(check_inner_stat_())) {
     LOG_WARN("fail to check inner stat", KR(ret));
   } else if (OB_ISNULL(schema_service_impl = schema_service_->get_schema_service())) {
     ret = OB_ERR_UNEXPECTED;
     LOG_WARN("schema_service impl is null", KR(ret));
   } else if (OB_ISNULL(new_table_schema_)) {
     ret = OB_ERR_UNEXPECTED;
     LOG_WARN("new_table_schema_ is null");
   } else {
     int64_t new_schema_version = OB_INVALID_VERSION;
     if (OB_FAIL(schema_service_->gen_new_schema_version(tenant_id_, new_schema_version))) {
       LOG_WARN("fail to gen new schema version", KR(ret), K_(tenant_id));
     } else if (FALSE_IT(new_table_schema_->set_schema_version(new_schema_version))) {
     } else if (OB_FAIL(schema_service_impl->get_table_sql_service().create_table(
                        *new_table_schema_,
                        get_trans_(),
                        &ddl_stmt_str_,
                        false /*need sync schema version*/,
                        false /*is truncate table*/))) {
       LOG_WARN("fail to create table", KR(ret), KPC(new_table_schema_));
     }
   }
   return ret;
 }

 int ObCreateViewHelper::insert_schema_object_dependency_()
 {
   int ret = OB_SUCCESS;
   if (OB_FAIL(check_inner_stat_())) {
     LOG_WARN("fail to check inner stat", KR(ret));
   } else if (OB_ISNULL(new_table_schema_)) {
     ret = OB_ERR_UNEXPECTED;
     LOG_WARN("new table schema is null", KR(ret));
   } else {
     for (int64_t i = 0; OB_SUCC(ret) && i < arg_.dep_infos_.count(); ++i) {
       ObDependencyInfo dep;
       if (OB_FAIL(dep.assign(arg_.dep_infos_.at(i)))) {
         LOG_WARN("fail to assign dependency info", KR(ret));
       } else {
         dep.set_tenant_id(tenant_id_);
         dep.set_dep_obj_id(new_table_schema_->get_table_id());
         dep.set_dep_obj_owner_id(new_table_schema_->get_table_id());
         dep.set_schema_version(new_table_schema_->get_schema_version());
         if (OB_FAIL(dep.insert_schema_object_dependency(get_trans_()))) {
           LOG_WARN("fail to insert schema object dependency", KR(ret), K(dep));
         }
       }
     }
   }
   return ret;
 }
 int ObCreateViewHelper::drop_schemas_()
 {
   int ret = OB_SUCCESS;
   if (OB_FAIL(check_inner_stat_())) {
     LOG_WARN("fail to check inner stat", KR(ret));
   } else if (OB_ISNULL(orig_table_schema_)) {
     // do nothing
   } else if (OB_FAIL(drop_trigger_schemas_())) {
     LOG_WARN("fail to drop trigger schemas", KR(ret));
   } else if (OB_FAIL(modify_obj_status_())) {
     LOG_WARN("fail to modify obj status", KR(ret));
   } else if (OB_FAIL(drop_obj_privs_())) {
     LOG_WARN("fail to drop obj privs", KR(ret));
   } else if (OB_FAIL(drop_rls_schemas_())) {
     LOG_WARN("fail to drop rls schemas", KR(ret));
   } else if (OB_FAIL(drop_table_())) {
     LOG_WARN("fail to drop table", KR(ret));
   }
   RS_TRACE(drop_schemas);
   return ret;
 }

 int ObCreateViewHelper::modify_obj_status_()
 {
   int ret = OB_SUCCESS;
   ObSchemaService* schema_service = nullptr;
   if (OB_FAIL(check_inner_stat_())) {
     LOG_WARN("fail to check inner stat", KR(ret));
   } else if (OB_ISNULL(schema_service = schema_service_->get_schema_service())) {
     ret = OB_ERR_UNEXPECTED;
     LOG_WARN("schema_service is null", KR(ret));
   } else {
     for (int64_t i = 0; OB_SUCC(ret) && i < dep_views_.count(); ++i) {
       const ObTableSchema *view_schema = dep_views_.at(i);
       int64_t new_schema_version = OB_INVALID_VERSION;
       if (OB_ISNULL(view_schema)) {
         ret = OB_ERR_UNEXPECTED;
         LOG_WARN("view schema is null", KR(ret));
       } else if (OB_FAIL(schema_service_->gen_new_schema_version(tenant_id_, new_schema_version))) {
         LOG_WARN("fail to gen new schema version", KR(ret));
       } else {
         ObObjectStatus new_status = ObObjectStatus::INVALID;
         const bool update_object_status_ignore_version = false;
         ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
         HEAP_VAR(ObTableSchema, new_dep_view) {
         if (OB_FAIL(new_dep_view.assign(*view_schema))) {
           LOG_WARN("fail to assign new dep view", KR(ret));
         } else if (OB_FAIL(ddl_operator.update_table_status(new_dep_view, new_schema_version, new_status,
                             update_object_status_ignore_version, get_trans_()))) {
           LOG_WARN("failed to update table status", KR(ret));
         }
         } // end heap var
       }
     }
   }
   return ret;
 }

 int ObCreateViewHelper::drop_trigger_schemas_() {
   int ret = OB_SUCCESS;
   ObSchemaService* schema_service = nullptr;
   if (OB_FAIL(check_inner_stat_())) {
     LOG_WARN("fail to check inner stat", KR(ret));
   } else if (OB_ISNULL(schema_service = schema_service_->get_schema_service())) {
     ret = OB_ERR_UNEXPECTED;
     LOG_WARN("schema_service is null", KR(ret));
   } else {
     for (int64_t i = 0; OB_SUCC(ret) && i < trigger_infos_.count(); ++i) {
       const ObTriggerInfo *trigger_info = trigger_infos_.at(i);
       int64_t new_schema_version = OB_INVALID_VERSION;
       if (OB_ISNULL(trigger_info)) {
         ret = OB_ERR_UNEXPECTED;
         LOG_WARN("trigger info is null", KR(ret));
       } else if (OB_FAIL(schema_service_->gen_new_schema_version(tenant_id_, new_schema_version))) {
         LOG_WARN("fail to gen schema version", KR(ret));
       } else if (OB_FAIL(schema_service->get_trigger_sql_service().drop_trigger(*trigger_info,
                                                                     false /* drop to recyclebin */,
                                                                     new_schema_version,
                                                                     get_trans_(),
                                                                     nullptr /* ddl stmt str */))) {
         LOG_WARN("fail to drop trigger", KR(ret), KPC(trigger_info));
       } else if (OB_FAIL(ObDependencyInfo::delete_schema_object_dependency(get_trans_(), tenant_id_,
                                                                            trigger_info->get_trigger_id(),
                                                                            new_schema_version /* not used */,
                                                                            trigger_info->get_object_type()))) {
         LOG_WARN("fail to delete schema object dependency", KR(ret), KPC(trigger_info));
       } else if (OB_FAIL(pl::ObRoutinePersistentInfo::delete_dll_from_disk(get_trans_(), tenant_id_,
                  share::schema::ObTriggerInfo::get_trigger_spec_package_id(trigger_info->get_trigger_id()),
                                                                            trigger_info->get_database_id()))) {
         LOG_WARN("fail to delete ddl from disk", KR(ret), K_(tenant_id), KPC(trigger_info));
       } else if (OB_FAIL(pl::ObRoutinePersistentInfo::delete_dll_from_disk(get_trans_(), tenant_id_,
                  share::schema::ObTriggerInfo::get_trigger_body_package_id(trigger_info->get_trigger_id()),
                                                                           trigger_info->get_database_id()))) {
         LOG_WARN("fail to delete ddl from disk", KR(ret), K_(tenant_id), KPC(trigger_info));
       }
       if (OB_SUCC(ret)) {
         ObErrorInfo error_info;
         if (OB_FAIL(error_info.handle_error_info(get_trans_(), trigger_info))) {
           LOG_WARN("delete trigger error info failed.", KR(ret), K(error_info));
         }
       }
     }
   }
   return ret;
 }

 int ObCreateViewHelper::drop_obj_privs_()
 {
   int ret = OB_SUCCESS;
   ObSchemaService* schema_service = nullptr;
   if (OB_FAIL(check_inner_stat_())) {
     LOG_WARN("fail to check inner stat", KR(ret));
   } else if (OB_ISNULL(schema_service = schema_service_->get_schema_service())) {
     ret = OB_ERR_UNEXPECTED;
     LOG_WARN("schema_service is null", KR(ret));
   } else {
     for (int64_t i = 0; OB_SUCC(ret) && i < obj_privs_.count(); ++i) {
       int64_t new_schema_version = OB_INVALID_VERSION;
       if (OB_FAIL(schema_service_->gen_new_schema_version(tenant_id_, new_schema_version))) {
         LOG_WARN("fail to gen new schema version", KR(ret), K_(tenant_id));
       } else if (OB_FAIL(schema_service->get_priv_sql_service().delete_obj_priv(obj_privs_.at(i),
                                                                                new_schema_version, get_trans_()))) {
         LOG_WARN("fail to drop obj privs", KR(ret), K(obj_privs_.at(i)));
       }
       // should not appear, just for compatible with old routine
       if (OB_UNLIKELY(OB_SEARCH_NOT_FOUND != ret)) {
         ret = OB_SUCCESS;
       }
     }
   }
   return ret;
 }

 int ObCreateViewHelper::drop_rls_schemas_()
 {
   int ret = OB_SUCCESS;
   ObSchemaService* schema_service = nullptr;
   if (OB_FAIL(check_inner_stat_())) {
     LOG_WARN("fail to check inner stat", KR(ret));
   } else if (OB_ISNULL(schema_service = schema_service_->get_schema_service())) {
     ret = OB_ERR_UNEXPECTED;
     LOG_WARN("schema_service is null", KR(ret));
   } else {
     for (int64_t i = 0; OB_SUCC(ret) && i < rls_policy_schemas_.count(); ++i) {
       int64_t new_schema_version = OB_INVALID_VERSION;
       if (OB_FAIL(schema_service_->gen_new_schema_version(tenant_id_, new_schema_version))) {
         LOG_WARN("fail to gen new schema version", KR(ret), K_(tenant_id));
       } else if (OB_FAIL(schema_service->get_rls_sql_service().apply_new_schema(
                                                                *rls_policy_schemas_.at(i),
                                                                new_schema_version,
                                                                get_trans_(),
                                                                OB_DDL_DROP_RLS_POLICY,
                                                                nullptr /*ddl_stmt_str*/))) {
         LOG_WARN("fail to drop rls policy", KR(ret), KPC(rls_policy_schemas_.at(i)));
       }
     }
     for (int64_t i = 0; OB_SUCC(ret) && i < rls_group_schemas_.count(); ++i) {
       int64_t new_schema_version = OB_INVALID_VERSION;
       if (OB_FAIL(schema_service_->gen_new_schema_version(tenant_id_, new_schema_version))) {
         LOG_WARN("fail to gen new schema version", KR(ret), K_(tenant_id));
       } else if (OB_FAIL(schema_service->get_rls_sql_service().apply_new_schema(
                                                                *rls_group_schemas_.at(i),
                                                                new_schema_version,
                                                                get_trans_(),
                                                                OB_DDL_DROP_RLS_GROUP,
                                                                nullptr /*ddl_stmt_str*/))) {
         LOG_WARN("fail to drop rls group", KR(ret), KPC(rls_group_schemas_.at(i)));
       }
     }
     for (int64_t i = 0; OB_SUCC(ret) && i < rls_context_schemas_.count(); ++i) {
       int64_t new_schema_version = OB_INVALID_VERSION;
       if (OB_FAIL(schema_service_->gen_new_schema_version(tenant_id_, new_schema_version))) {
         LOG_WARN("fail to gen new schema version", KR(ret), K_(tenant_id));
       } else if (OB_FAIL(schema_service->get_rls_sql_service().apply_new_schema(
                                                                *rls_context_schemas_.at(i),
                                                                new_schema_version,
                                                                get_trans_(),
                                                                OB_DDL_DROP_RLS_CONTEXT,
                                                                nullptr /*ddl_stmt_str*/))) {
         LOG_WARN("fail to drop rls context", KR(ret), KPC(rls_context_schemas_.at(i)));
       }
     }
   }
   return ret;
 }

 int ObCreateViewHelper::handle_error_info_()
 {
   int ret = OB_SUCCESS;
   bool is_oracle_mode = false;
   if (OB_FAIL(check_inner_stat_())) {
     LOG_WARN("fail to check inner stat", KR(ret));
   } else if (ERROR_STATUS_HAS_ERROR != arg_.error_info_.get_error_status()) {
     // do nothing
   } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(tenant_id_, is_oracle_mode))) {
     LOG_WARN("fail to check is oracle mode", KR(ret), K_(tenant_id));
   } else if (OB_UNLIKELY(!is_oracle_mode)) {
     ret = OB_ERR_UNEXPECTED;
     LOG_WARN("unexpected compat mode add create vew error info", KR(ret), K_(tenant_id), K(is_oracle_mode));
   } else if (OB_ISNULL(new_table_schema_)) {
     ret = OB_ERR_UNEXPECTED;
     LOG_WARN("new_table_schema_ is null", KR(ret));
   } else {
     ObErrorInfo error_info;
     if (OB_FAIL(error_info.assign(arg_.error_info_))) {
       LOG_WARN("fail to assign error info", KR(ret));
     } else {
       error_info.set_obj_id(new_table_schema_->get_table_id());
       error_info.set_obj_type(static_cast<uint64_t>(ObObjectType::VIEW));
       error_info.set_database_id(new_table_schema_->get_database_id());
       error_info.set_tenant_id(tenant_id_);
       error_info.set_schema_version(new_table_schema_->get_schema_version());
       if (OB_FAIL(error_info.handle_error_info(get_trans_(), nullptr /*info*/))) {
         LOG_WARN("insert create error info failed", KR(ret));
       }
     }
   }
   return ret;
 }

 int ObCreateViewHelper::drop_table_()
 {
   int ret = OB_SUCCESS;
   ObSchemaService* schema_service = nullptr;
   int64_t new_schema_version = OB_INVALID_VERSION;
   if (OB_FAIL(check_inner_stat_())) {
     LOG_WARN("fail to check inner stat", KR(ret));
   } else if (OB_ISNULL(schema_service = schema_service_->get_schema_service())) {
     ret = OB_ERR_UNEXPECTED;
     LOG_WARN("schema_service is null", KR(ret));
   } else if (OB_ISNULL(orig_table_schema_)) {
     ret = OB_ERR_UNEXPECTED;
     LOG_WARN("orig_table_schema_ is null", KR(ret));
   } else if (OB_FAIL(schema_service_->gen_new_schema_version(tenant_id_, new_schema_version))) {
     LOG_WARN("fail to gen new schema_version", KR(ret), K_(tenant_id));
   } else if (OB_FAIL(schema_service->get_table_sql_service().drop_table(*orig_table_schema_,
                                                                         new_schema_version,
                                                                         get_trans_(),
                                                                         nullptr /* ddl_stmt_str */,
                                                                         false /* is_truncate */,
                                                                         false /* is_drop_db */,
                                                                         false /* is_force_drop_lonely_lob_aux_table */,
                                                                         nullptr /* schema_guard */,
                                                                         nullptr /* drop_table_set */))) {
     LOG_WARN("fail to drop table", KR(ret), K_(tenant_id), K(*orig_table_schema_));
   } else if (OB_FAIL(ObDependencyInfo::delete_schema_object_dependency(get_trans_(), tenant_id_,
                                                                        orig_table_schema_->get_table_id(),
                                                                        orig_table_schema_->get_schema_version() /*not used*/,
                                                                        ObObjectType::VIEW))) {
     LOG_WARN("fail to delete schema object dependency", KR(ret), K_(tenant_id), K(orig_table_schema_->get_table_id()),
                                                         K(orig_table_schema_->get_schema_version()));
   }
   return ret;
 }

 int ObCreateViewHelper::restore_obj_privs_()
 {
   int ret = OB_SUCCESS;
   ObSchemaService* schema_service = nullptr;
   if (OB_FAIL(check_inner_stat_())) {
     LOG_WARN("fail to check inner stat", KR(ret));
   } else if (OB_ISNULL(schema_service = schema_service_->get_schema_service())) {
     ret = OB_ERR_UNEXPECTED;
     LOG_WARN("schema_service is null", KR(ret));
   } else if (OB_ISNULL(database_schema_)) {
     ret = OB_ERR_UNEXPECTED;
     LOG_WARN("database schema is null", KR(ret));
   } else if (OB_ISNULL(new_table_schema_)) {
     ret = OB_ERR_UNEXPECTED;
     LOG_WARN("new table schema is null", KR(ret));
   } else {
     for (int64_t i = 0; OB_SUCC(ret) && i < obj_privs_.count(); ++i) {
       ObObjPriv &obj_priv = obj_privs_.at(i);
       ObTablePrivSortKey table_key(obj_priv.get_tenant_id(),
                                    obj_priv.get_grantee_id(),
                                    database_schema_->get_database_name(),
                                    new_table_schema_->get_table_name());
       obj_priv.set_obj_id(new_table_schema_->get_table_id());
       ObObjPrivSortKey obj_priv_key = obj_priv.get_sort_key();
       const ObRawObjPrivArray &option_priv = raw_obj_privs_.at(i).first;
       const ObRawObjPrivArray &no_option_priv = raw_obj_privs_.at(i).second;
       if (option_priv.count() > 0) {
         int64_t new_schema_version = OB_INVALID_VERSION;
         if (OB_FAIL(schema_service_->gen_new_schema_version(tenant_id_, new_schema_version))) {
           LOG_WARN("fail to gen new schema version", KR(ret));
         } else if (OB_FAIL(schema_service->get_priv_sql_service().grant_table_ora_only(
           nullptr /* ddl_stmt_str */, get_trans_(), option_priv, true /* option */, obj_priv_key,
           new_schema_version, false /* is_delete */, false /* is_delete_all */))) {
           LOG_WARN("fail to grant table ora only", KR(ret), K(option_priv));
         }
       }
       if (OB_SUCC(ret) && no_option_priv.count() > 0) {
         int64_t new_schema_version = OB_INVALID_VERSION;
         if (OB_FAIL(schema_service_->gen_new_schema_version(tenant_id_, new_schema_version))) {
           LOG_WARN("fail to gen new schema version", KR(ret));
         } else if (OB_FAIL(schema_service->get_priv_sql_service().grant_table_ora_only(
           nullptr /* ddl_stmt_str */, get_trans_(), no_option_priv, false /* option */,
           obj_priv_key, new_schema_version, false /* is_delete */, false /* is_delete_all */))) {
           LOG_WARN("fail to grant table ora only", KR(ret), K(no_option_priv));
         }
       }
     }
   }
   return ret;
 }

 int ObCreateViewHelper::operate_schemas_()
 {
   int ret = OB_SUCCESS;
   if (OB_FAIL(check_inner_stat_())) {
     LOG_WARN("fail to check inner stat", KR(ret));
   } else if (OB_FAIL(drop_schemas_())) {
     LOG_WARN("fail to drop schemas", KR(ret));
   } else if (OB_FAIL(create_schemas_())) {
     LOG_WARN("fail create schemas", KR(ret));
   }
   return ret;
 }
 int ObCreateViewHelper::clean_on_fail_commit_()
 {
   // do nothing
   return OB_SUCCESS;
 }
 int ObCreateViewHelper::operation_before_commit_()
 {
   // do nothing
   return OB_SUCCESS;
 }

 int ObCreateViewHelper::construct_and_adjust_result_(int &return_ret)
 {
   int ret = return_ret;
   if (FAILEDx(check_inner_stat_())) {
     LOG_WARN("fail to check inner stat", KR(ret));
   } else {
     ObSchemaVersionGenerator *tsi_generator = GET_TSI(TSISchemaVersionGenerator);
     if (OB_ISNULL(tsi_generator)) {
       ret = OB_ERR_UNEXPECTED;
       LOG_WARN("tsi schema version generator is null", KR(ret));
     } else if (OB_ISNULL(new_table_schema_)) {
       ret = OB_NOT_INIT;
       LOG_WARN("new table schema is null", KR(ret));
     } else {
       tsi_generator->get_current_version(res_.schema_version_);
       res_.table_id_ = new_table_schema_->get_table_id();
     }
   }
   return ret;
 }


