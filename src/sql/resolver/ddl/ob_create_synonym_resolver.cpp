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

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/ddl/ob_create_synonym_resolver.h"
#include "lib/number/ob_number_v2.h"
#include "lib/string/ob_sql_string.h"
#include "share/schema/ob_table_schema.h"
#include "share/config/ob_server_config.h"
#include "sql/resolver/ddl/ob_create_synonym_stmt.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/expr/ob_raw_expr_resolver_impl.h"
#include "sql/resolver/expr/ob_raw_expr_part_func_checker.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/ob_sql_utils.h"
#include "sql/resolver/dml/ob_select_resolver.h"
#include "share/schema/ob_dependency_info.h"
#include "share/ob_tenant_id_schema_version.h"



/*
 *
 * CREATE [or replace ] [public] SYNONYM [schemaX.] synonym  for [schemaY.]OBJECT [@dlink];
 */
namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share::schema;
namespace sql
{
ObCreateSynonymResolver::ObCreateSynonymResolver(ObResolverParams &params)
    : ObDDLResolver(params)
{
}

ObCreateSynonymResolver::~ObCreateSynonymResolver()
{
}


int ObCreateSynonymResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *create_synonym_node = const_cast<ParseNode*>(&parse_tree);
  bool is_public_schema = false;
  bool schema_name_from_node = true;
  if (OB_ISNULL(create_synonym_node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN( "invalid argument.", K(ret));
  } else if (T_CREATE_SYNONYM != create_synonym_node->type_
      || CREATE_SYNONYM_NUM_CHILD != create_synonym_node->num_child_
      || OB_ISNULL(create_synonym_node->children_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), K(create_synonym_node->type_),
        K(create_synonym_node->num_child_), K(create_synonym_node->children_));
  } else {
    ObCreateSynonymStmt *create_synonym_stmt = NULL;
    if (OB_ISNULL(create_synonym_stmt = create_stmt<ObCreateSynonymStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to create stmt", K(ret));
    } else {
      uint64_t tenant_id = session_info_->get_effective_tenant_id();
      create_synonym_stmt->set_tenant_id(tenant_id);
    }
    //resolve or replace
    if (OB_SUCC(ret)) {
      if (NULL != create_synonym_node->children_[0]) {
        create_synonym_stmt->set_or_replace(true);
      }
    }
    //resolve is public
    if (OB_SUCC(ret)) {
      if (NULL != create_synonym_node->children_[1]) {
        if (T_PUBLIC != create_synonym_node->children_[1]->type_) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument.", K(ret), K(create_synonym_node->children_[1]->type_));
        } else {
          is_public_schema = true;
        }
      }
    }

    //resolve schema of synonym
    if (OB_SUCC(ret)) {
      if (NULL != create_synonym_node->children_[2]) {
        if (is_public_schema) {
          ret = OB_ERR_INVALID_SYNONYM_NAME;

        } else {
          ObString db_name;
          int32_t len = static_cast<int32_t>(create_synonym_node->children_[2]->str_len_);
          db_name.assign_ptr(const_cast<char*>(create_synonym_node->children_[2]->str_value_), len);
          create_synonym_stmt->set_database_name(db_name);
        }
      } else if (is_public_schema) {
        create_synonym_stmt->set_database_name(OB_PUBLIC_SCHEMA_NAME);
      } else {
        create_synonym_stmt->set_database_name(session_info_->get_database_name());
      }
    }
    if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
      CK(OB_NOT_NULL(session_info_), OB_NOT_NULL(schema_checker_));
      OZ (schema_checker_->check_ora_ddl_priv(
          session_info_->get_effective_tenant_id(),
          session_info_->get_priv_user_id(),
          create_synonym_stmt->get_database_name(),
          is_public_schema ? 
            stmt::T_CREATE_PUB_SYNONYM : stmt::T_CREATE_SYNONYM,
          session_info_->get_enable_role_array()),
          session_info_->get_effective_tenant_id(), session_info_->get_user_id());
    }
    
    //resolve name of synonym
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(create_synonym_node->children_[3])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error. invalid field. synonym is null pointer",
               K(ret), K(create_synonym_node->children_[3]));
      } else {
        ObString name;
        int32_t len = static_cast<int32_t>(create_synonym_node->children_[3]->str_len_);
        name.assign_ptr(const_cast<char*>(create_synonym_node->children_[3]->str_value_), len);
        if (OB_FAIL(create_synonym_stmt->set_synonym_name(name))) {
          LOG_WARN("Failed to set synonym name", K(ret));
        }
      }
    }

    //resolve database of object
    if (OB_SUCC(ret)) {
      if (NULL != create_synonym_node->children_[4]) {
        ObString db_name;
        int32_t len = static_cast<int32_t>(create_synonym_node->children_[4]->str_len_);
        db_name.assign_ptr(const_cast<char*>(create_synonym_node->children_[4]->str_value_), len);
        create_synonym_stmt->set_object_database_name(db_name);
        schema_name_from_node = true;
      } else {
        create_synonym_stmt->set_object_database_name(session_info_->get_database_name());
        schema_name_from_node = false;
      }
    }

    //resolve name of object
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(create_synonym_node->children_[5])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error. invalid field. synonym is null pointer",
               K(ret), K(create_synonym_node->children_[5]));
      } else {
        ObString string;
        int32_t len = static_cast<int32_t>(create_synonym_node->children_[5]->str_len_);
        string.assign_ptr(const_cast<char*>(create_synonym_node->children_[5]->str_value_), len);
        if (OB_FAIL(create_synonym_stmt->set_object_name(string))) {
          LOG_WARN("Failed to set object name", K(ret));
        }
      }
    }

    //resolve database link
    // the idea here is to store full dblink desc string in the object_name field,
    // the dblink desc string may like: 'remote_schema_name.tbl@dblink_name' or 'tbl@dblink_name'
    // we have to concat all parts together to get the full desc string.
    if (OB_SUCC(ret)) {
      // we don't have to check whether link_name is created or not,
      // because we may create the dblink with link_name after
      // the synonym is created. check it in runtime.
      ParseNode *dblink_name_node = create_synonym_node->children_[6];
      if (NULL != dblink_name_node && NULL != dblink_name_node->children_ && NULL != dblink_name_node->children_[0]) {
        dblink_name_node = dblink_name_node->children_[0];
        ObSqlString obj_with_dblink;
        const ObString &tmp_obj_name = create_synonym_stmt->get_object_name();
        // user write something like 'create synonym syn for remote_db.tbl_name@dblink'
        // in this case we add the remote_db in the object_name,
        // user may also write 'synonym syn for tbl_name@dblink', we don't have to add db name
        // because the actual db name have to extract from dblink schema.
        if (schema_name_from_node) {
          OZ (obj_with_dblink.append(create_synonym_stmt->get_object_database_name()));
          OZ (obj_with_dblink.append(ObString(".")));
        }
        OZ (obj_with_dblink.append(tmp_obj_name));
        OZ (obj_with_dblink.append_fmt("@%.*s", static_cast<int32_t>(dblink_name_node->str_len_),
                                       dblink_name_node->str_value_));
        if (OB_SUCC(ret)) {
          ObString dblink_name;
          CK (OB_NOT_NULL(allocator_));
          OZ (ob_write_string(*allocator_, obj_with_dblink.string(), dblink_name));
          OZ (create_synonym_stmt->set_object_name(dblink_name));
          
          // we have to reset database name if dblink mode, for the db name may be remote db name
          CK (OB_NOT_NULL(session_info_));
          if (schema_name_from_node) {
            OX (create_synonym_stmt->set_object_database_name(session_info_->get_database_name()));
          }
        }
      }
    }
    //well done.

    //add def obj info if exists
    bool ref_exists = false;
    ObObjectType ref_type = ObObjectType::INVALID;
    uint64_t ref_obj_id = OB_INVALID_ID;
    uint64_t ref_schema_version = share::OB_INVALID_SCHEMA_VERSION;
    uint64_t data_version = 0;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(check_valid(create_synonym_stmt))) {
      LOG_WARN("fail to check synonym stmt", K(ret));
    } else if (OB_FAIL(GET_MIN_DATA_VERSION(session_info_->get_effective_tenant_id(), data_version))) {
      LOG_WARN("failed to get data version", K(ret));
    } else if (data_version >= DATA_VERSION_4_1_0_0
               && OB_FAIL(ObSQLUtils::find_synonym_ref_obj(create_synonym_stmt->get_object_database_name(),
                                                        create_synonym_stmt->get_object_name(),
                                                        session_info_->get_effective_tenant_id(),
                                                        ref_exists,
                                                        ref_obj_id,
                                                        ref_type,
                                                        ref_schema_version))) {
      LOG_WARN("failed to find synonym ref obj", K(ret));
    } else {
      if (ref_exists) {
        ObDependencyInfo dep;
        dep.set_dep_obj_id(OB_INVALID_ID);
        dep.set_dep_obj_type(ObObjectType::SYNONYM);
        dep.set_ref_obj_id(ref_obj_id);
        dep.set_ref_obj_type(ref_type);
        dep.set_dep_timestamp(-1);
        dep.set_ref_timestamp(ref_schema_version);
        dep.set_tenant_id(session_info_->get_effective_tenant_id());
        create_synonym_stmt->set_dependency_info(dep);
      }
      stmt_ = create_synonym_stmt;
    }
  }
  return ret;
}

int ObCreateSynonymResolver::check_valid(const ObCreateSynonymStmt *synonym_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(synonym_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("synonym stmt is NULL", K(ret));
  } else {
    const ObString &db_name = synonym_stmt->get_database_name();
    const ObString &obj_db_name = synonym_stmt->get_object_database_name();
    const ObString &synonym_name = synonym_stmt->get_synonym_name();
    const ObString &object_name = synonym_stmt->get_object_name();
    if (0 == db_name.case_compare(obj_db_name)
        && 0 == synonym_name.case_compare(object_name)) {
      ret = OB_ERR_SYNONYM_SAME_AS_OBJECT;
      LOG_WARN("cannot create a synonym with same name as object");
    }
  }
  return ret;
}

}//end namespace sql
}//end namespace oceanbase
