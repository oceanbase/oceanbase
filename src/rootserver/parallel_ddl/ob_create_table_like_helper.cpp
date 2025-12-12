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
#include "rootserver/parallel_ddl/ob_table_helper.h"
#include "rootserver/parallel_ddl/ob_create_table_like_helper.h"
#include "share/ob_debug_sync_point.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/ob_domain_index_builder_util.h"
#include "rootserver/ob_lob_meta_builder.h"
#include "rootserver/ob_lob_piece_builder.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::rootserver;


ObCreateTableLikeHelper::ObCreateTableLikeHelper(
    share::schema::ObMultiVersionSchemaService *schema_service,
    const uint64_t tenant_id,
    const obrpc::ObCreateTableLikeArg &arg,
    obrpc::ObCreateTableRes &res,
    bool enable_ddl_parallel,
    ObDDLSQLTransaction *external_trans):
    ObDDLHelper(schema_service, tenant_id, "[create table like]", external_trans, enable_ddl_parallel),
    ObTableHelper(schema_service, tenant_id, "[create table like]", external_trans, enable_ddl_parallel),
    arg_(arg),
    res_(res),
    orig_table_id_(common::OB_INVALID_ID),
    replace_mock_fk_parent_table_id_(common::OB_INVALID_ID)
{}

ObCreateTableLikeHelper::~ObCreateTableLikeHelper()
{
}

int ObCreateTableLikeHelper::init_()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObCreateTableLikeHelper::lock_objects_()
{
  //TODO: enable create table like parallel
  return OB_SUCCESS;
}


int ObCreateTableLikeHelper::check_schema_valid_(const ObTableSchema *&orig_table_schema, uint64_t &new_database_id)
{
  int ret = OB_SUCCESS;
  orig_table_schema = nullptr;
  new_database_id = OB_INVALID_ID;
  orig_table_id_ = OB_INVALID_ID;
  ObTableType table_type = MAX_TABLE_TYPE;
  int64_t orig_schema_version = OB_INVALID_VERSION;
  int64_t new_schema_version = OB_INVALID_VERSION;
  uint64_t new_table_id = OB_INVALID_ID;
  uint64_t orig_database_id = OB_INVALID_ID;
  uint64_t synonym_id = OB_INVALID_ID;
  bool is_oracle_mode = false;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(
             tenant_id_, is_oracle_mode))) {
    LOG_WARN("fail to check is oracle mode", KR(ret));
  } else if (is_oracle_mode) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported in oracle mode", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(check_database_legitimacy_(arg_.origin_db_name_, orig_database_id))) {
    LOG_WARN("fail to check database legitimacy", KR(ret), K_(tenant_id), K(arg_.origin_db_name_));
  } else if (OB_FAIL(schema_guard_wrapper_.get_table_id(orig_database_id, arg_.session_id_, arg_.origin_table_name_, orig_table_id_, table_type, orig_schema_version))) {
    LOG_WARN("fail to get table id", KR(ret), K_(tenant_id), K(orig_database_id), K_(arg_.session_id), K_(arg_.origin_table_name));
  } else if (OB_INVALID_ID == orig_table_id_) {
    ret = OB_TABLE_NOT_EXIST;
    ObCStringHelper helper;
    LOG_USER_ERROR(OB_TABLE_NOT_EXIST, helper.convert(arg_.origin_db_name_),
                                       helper.convert(arg_.origin_table_name_));
  } else if (OB_FAIL(schema_guard_wrapper_.get_table_schema(orig_table_id_, orig_table_schema))) {
    LOG_WARN("fail to get orig table schema", KR(ret), K_(tenant_id), K_(orig_table_id));
  } else if (OB_ISNULL(orig_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig table schema is null", KR(ret));
  } else if (orig_table_schema->is_in_recyclebin()) {
    ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
    LOG_WARN("can create table like table in recyclebin", KR(ret),
              K(*orig_table_schema));
  } else if (!orig_table_schema->is_user_table() && !orig_table_schema->is_sys_table()) {
    ret = OB_ERR_WRONG_OBJECT;
    ObCStringHelper helper;
    LOG_USER_ERROR(OB_ERR_WRONG_OBJECT, helper.convert(arg_.origin_db_name_),
                                        helper.convert(arg_.origin_table_name_),
                   "BASE TABLE");
  } else if (orig_table_schema->has_mlog_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN(
        "create table like on table with materialized view log is not supported",
        KR(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED,
                   "create table like on table with materialized view log is");
  } else if (orig_table_schema->table_referenced_by_fast_lsm_mv()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN(
        "create table like on table required by materialized view is not supported", KR(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED,
                   "create table like on table required by materialized view is");
  } else if (is_inner_table(orig_table_schema->get_table_id())) {
    // tablegroup of system table is oceanbase,
    // Including the user table in it may cause some unexpected problems, please ban it here
    //
    ret = OB_ERR_WRONG_OBJECT;
    ObCStringHelper helper;
    LOG_USER_ERROR(OB_ERR_WRONG_OBJECT, helper.convert(arg_.origin_db_name_),
                                        helper.convert(arg_.origin_table_name_),
                   "BASE TABLE");
    LOG_WARN("create table like inner table not allowed", KR(ret), K_(arg));
  } else if (OB_FAIL(check_database_legitimacy_(arg_.new_db_name_, new_database_id))) {
    LOG_WARN("fail to check database legitimacy", KR(ret), K_(tenant_id), K(arg_.origin_db_name_));
  } else if (OB_FAIL(schema_guard_wrapper_.get_table_id(new_database_id, arg_.session_id_, arg_.new_table_name_, new_table_id, table_type, new_schema_version))) {
    LOG_WARN("fail to get table id", KR(ret), K_(tenant_id), K(new_database_id), K_(arg_.session_id), K_(arg_.origin_table_name));
  } else if (OB_INVALID_ID != new_table_id) {
    ret = OB_ERR_TABLE_EXIST;
    if (arg_.if_not_exist_) {
      res_.table_id_ = new_table_id;
      res_.schema_version_ = new_schema_version;
    }
    LOG_WARN("target table already exist", KR(ret), K_(arg), K_(tenant_id));
  } else if (OB_FAIL(schema_guard_wrapper_.get_synonym_id(new_database_id, arg_.new_table_name_, synonym_id))) {
    LOG_WARN("fail to get synonym id", KR(ret), K_(tenant_id), K(new_database_id), K_(arg_.new_table_name));
  } else if (OB_INVALID_ID != synonym_id) {
    ret = OB_ERR_EXIST_OBJECT;
    LOG_WARN("Name is already used by an existing object", KR(ret), K_(arg));
  }
  return ret;
}

int ObCreateTableLikeHelper::generate_table_schema_()
{
  int ret = OB_SUCCESS;
  const ObTableSchema *orig_table_schema = nullptr;
  uint64_t new_database_id = OB_INVALID_ID;
  uint64_t new_table_id = OB_INVALID_ID;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(check_schema_valid_(orig_table_schema, new_database_id))) {
    LOG_WARN("fail to check schema valid", KR(ret));
  } else if (OB_ISNULL(orig_table_schema) || OB_INVALID_ID == new_database_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("orig_table_schema is null or new_database_id invalid", KR(ret), KP(orig_table_schema), K(new_database_id));
  } else {
    HEAP_VAR(ObTableSchema, new_table_schema) {
    ObIDGenerator id_generator;
    int obj_cnt = orig_table_schema->get_constraint_count() + 1;
    if (OB_FAIL(new_table_schema.assign(*orig_table_schema))) {
      LOG_WARN("fail to assign schema", KR(ret));
    } else if (OB_FAIL(ddl_service_->delete_unused_columns_and_redistribute_schema(*orig_table_schema,
                       false/*need_redistribute_column_id, to avoid column_id mismatched between data table and index ones.*/,
                       new_table_schema))) {
      LOG_WARN("fail to delete unused columns and redistribute schema", KR(ret), K(*orig_table_schema));
    } else if (OB_FAIL(gen_object_ids_(obj_cnt, id_generator))) {
      LOG_WARN("fail to gen object ids", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(id_generator.next(new_table_id))) {
      LOG_WARN("fail to get next object_id", KR(ret));
    } else {
      new_table_schema.set_table_id(new_table_id);
      new_table_schema.set_table_name(arg_.new_table_name_);
      new_table_schema.set_database_id(new_database_id);
      new_table_schema.set_define_user_id(arg_.define_user_id_);
      new_table_schema.reset_foreign_key_infos();
      new_table_schema.reset_trigger_list();
      if (new_table_schema.has_constraint()) {
        ObTableSchema::const_constraint_iterator iter = new_table_schema.constraint_begin();
        ObTableSchema::const_constraint_iterator iter_last = iter;
        ObString new_constraint_name;
        bool is_constraint_name_exist = false;
        uint64_t object_id = OB_INVALID_ID;
        for (; OB_SUCC(ret) && iter != new_table_schema.constraint_end();++iter) {
          (*iter)->set_table_id(new_table_id);
          (*iter)->set_tenant_id(tenant_id_);
          do {
            if (OB_FAIL(ObTableSchema::create_cons_name_automatically(
                        new_constraint_name, arg_.new_table_name_, allocator_,
                        (*iter)->get_constraint_type(), false /*is_oracle_mode*/))) {
              SQL_RESV_LOG(WARN, "create cons name automatically failed", KR(ret));
            } else if (OB_UNLIKELY(0 == new_constraint_name.case_compare((*iter_last)->get_constraint_name_str()))) {
              is_constraint_name_exist = true;
            } else if (OB_FAIL(check_constraint_name_exist_(
                               new_table_schema, new_constraint_name, false /*is_foreign_key*/, is_constraint_name_exist))) {
              LOG_WARN("fail to check check constraint name is exist or not", KR(ret), K(new_constraint_name));
            }
          } while (OB_SUCC(ret) && is_constraint_name_exist);
          if (OB_SUCC(ret)) {
            if (OB_FAIL(id_generator.next(object_id))) {
              LOG_WARN("fail to get next object_id", KR(ret));
            } else {
              (*iter)->set_constraint_name(new_constraint_name);
              (*iter)->set_name_generated_type(GENERATED_TYPE_SYSTEM);
              (*iter)->set_constraint_id(object_id);
            }
          }
          iter_last = iter;
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (orig_table_schema->is_sys_table() || orig_table_schema->is_vir_table()) {
      new_table_schema.set_table_type(USER_TABLE);
    } else if (orig_table_schema->is_sys_view()) {
      new_table_schema.set_table_type(USER_VIEW);
    } else if (orig_table_schema->is_external_table()) {
      new_table_schema.set_table_type(EXTERNAL_TABLE);
    }
    if (new_table_schema.is_user_table()
        && (TMP_TABLE == arg_.table_type_ || TMP_TABLE_ORA_SESS == arg_.table_type_)) {
      new_table_schema.set_table_type(arg_.table_type_);
      new_table_schema.set_create_host(arg_.create_host_);
      new_table_schema.set_sess_active_time(ObTimeUtility::current_time());
      new_table_schema.set_session_id(arg_.session_id_);
    }
    if (orig_table_schema->is_primary_vp_table()) {
      new_table_schema.set_data_table_id(0); // VP not support
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(new_tables_.push_back(new_table_schema))) {
      LOG_WARN("failed to add table schema!", KR(ret));
    }
    } // end heap var
  }
  return ret;
}


int ObCreateTableLikeHelper::generate_aux_table_schemas_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(new_tables_.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table cnt not match", KR(ret), "table_cnt", new_tables_.count());
  } else {
    ObTableSchema *new_table_schema = &new_tables_.at(0);
    ObIDGenerator id_generator;
    ObSEArray<ObAuxTableMetaInfo,16> simple_index_infos;
    ObSArray<ObTableSchema> shared_schema_array;
    ObSArray<ObTableSchema> domain_schema_array;
    ObSArray<ObTableSchema> aux_schema_array;
    int64_t obj_cnt = 0;
    uint64_t new_table_id = OB_INVALID_ID;
    uint64_t new_database_id = OB_INVALID_ID;
    if (OB_ISNULL(new_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new table scheam is null", KR(ret));
    } else if (OB_FAIL(new_table_schema->get_simple_index_infos(simple_index_infos))) {
      LOG_WARN("get simple_index_infos failed", KR(ret));
    } else {
      obj_cnt= simple_index_infos.count();
      if (new_table_schema->has_lob_column(true/*ignore_unused_column*/)) {
        obj_cnt += 2;
      }
      new_table_id = new_table_schema->get_table_id();
      new_database_id = new_table_schema->get_database_id();
    }
    if (FAILEDx(gen_object_ids_(obj_cnt, id_generator))) {
      LOG_WARN("fail to gen object ids", KR(ret), K_(tenant_id), K(obj_cnt));
    }
    HEAP_VAR(ObTableSchema, new_index_schema) {
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
      const ObTableSchema *index_table_schema = nullptr;
      new_index_schema.reset();
      if (OB_FAIL(schema_guard_wrapper_.get_table_schema(simple_index_infos.at(i).table_id_, index_table_schema))) {
        LOG_WARN("fail to get index table schema", KR(ret), K_(tenant_id), K_(simple_index_infos.at(i).table_id));
      } else if (OB_ISNULL(index_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema should not be null", KR(ret));
      } else if (index_table_schema->is_in_recyclebin() ||
                 INDEX_STATUS_AVAILABLE != index_table_schema->get_index_status()) {
        continue;
      } else {
        ObString index_name;
        ObString new_index_table_name;
        uint64_t new_index_id = OB_INVALID_ID;
        if (OB_FAIL(new_index_schema.assign(*index_table_schema))) {
          LOG_WARN("fail to assign schema", KR(ret));
        } else if (OB_FAIL(ObTableSchema::get_index_name(allocator_, orig_table_id_,
                           index_table_schema->get_table_name_str(), index_name))) {
          LOG_WARN("fail to get index name", KR(ret), K(orig_table_id_), K(index_table_schema->get_table_name_str()));
        } else if (OB_FAIL(ObTableSchema::build_index_table_name(allocator_, new_table_id, index_name, new_index_table_name))) {
          LOG_WARN("fail to build new index table name", KR(ret), K(new_table_id), K(new_index_table_name));
        } else if (OB_FAIL(id_generator.next(new_index_id))) {
          LOG_WARN("fail to get next object_id", KR(ret));
        } else {
          new_index_schema.set_table_id(new_index_id);
          new_index_schema.set_data_table_id(new_table_id);
          new_index_schema.set_table_name(new_index_table_name);
          new_index_schema.set_database_id(new_database_id);
          //create table like, index always is valid
          new_index_schema.set_index_status(INDEX_STATUS_AVAILABLE);
          const ObIndexType index_type = new_index_schema.get_index_type();
          if (new_index_schema.is_rowkey_doc_id() ||
              new_index_schema.is_doc_id_rowkey() ||
              new_index_schema.is_vec_rowkey_vid_type() ||
              new_index_schema.is_vec_vid_rowkey_type()) {
            if (OB_FAIL(shared_schema_array.push_back(new_index_schema))) {
              LOG_WARN("fail to add shared schema array", KR(ret));
            }
          } else if (new_index_schema.is_fts_index_aux() ||
                     new_index_schema.is_multivalue_index_aux() ||
                     new_index_schema.is_vec_domain_index()) {
            if (OB_FAIL(domain_schema_array.push_back(new_index_schema))) {
              LOG_WARN("fail to add domain schema", KR(ret));
            }
          } else if (share::schema::is_fts_doc_word_aux(index_type) ||
                     share::schema::is_vec_index_id_type(index_type) ||
                     share::schema::is_vec_index_snapshot_data_type(index_type) ||
                     share::schema::is_built_in_vec_ivf_index(index_type)) {
            if (OB_FAIL(aux_schema_array.push_back(new_index_schema))) {
              LOG_WARN("fail to add aux schema", KR(ret));
            }
          } else if (OB_FAIL(new_tables_.push_back(new_index_schema))) {
            LOG_WARN("fail to add index schema", KR(ret));
          }
        }
      }
    }// end for
    }// end heap var
    if (OB_SUCC(ret) &&
        !domain_schema_array.empty() &&
        OB_FAIL(ObDomainIndexBuilderUtil::retrieve_complete_domain_index(shared_schema_array,
                                                                         domain_schema_array,
                                                                         aux_schema_array,
                                                                         allocator_,
                                                                         new_table_id,
                                                                         new_tables_))) {
      LOG_WARN("fail to retrieve complete index", KR(ret));
    }
    new_table_schema = &(new_tables_.at(0));
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(new_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new table schema is null", KR(ret), K(new_table_schema));
    } else if (new_table_schema->has_lob_column(true/*ignore_unused_column*/)) {
      HEAP_VARS_2((ObTableSchema, lob_meta_schema), (ObTableSchema, lob_piece_schema)) {
      ObLobMetaBuilder lob_meta_builder(*ddl_service_);
      ObLobPieceBuilder lob_piece_builder(*ddl_service_);
      bool need_object_id = false;
      uint64_t object_id = OB_INVALID_ID;
      if (OB_FAIL(id_generator.next(object_id))) {
        LOG_WARN("fail to get next object_id", KR(ret));
      } else if (OB_FAIL(lob_meta_builder.generate_aux_lob_meta_schema(
        schema_service_->get_schema_service(), *new_table_schema, object_id, lob_meta_schema, need_object_id))) {
        LOG_WARN("generate lob meta table failed", KR(ret), K(new_table_schema));
      } else if (OB_FAIL(id_generator.next(object_id))) {
        LOG_WARN("fail to get next object_id", KR(ret));
      } else if (OB_FAIL(lob_piece_builder.generate_aux_lob_piece_schema(
        schema_service_->get_schema_service(), *new_table_schema, object_id, lob_piece_schema, need_object_id))) {
        LOG_WARN("generate_schema for lob data table failed", KR(ret), K(new_table_schema));
      } else if (OB_FAIL(new_tables_.push_back(lob_meta_schema))) {
        LOG_WARN("push_back lob meta table failed", KR(ret));
      } else if (OB_FAIL(new_tables_.push_back(lob_piece_schema))) {
        LOG_WARN("push_back lob piece table failed", KR(ret));
      } else {
        new_table_schema = &new_tables_.at(0); // memory of data table may change after add table to new_tables_
        if (OB_ISNULL(new_table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("new table schema is null", KR(ret), K(new_table_schema));
        } else {
        new_table_schema->set_aux_lob_meta_tid(lob_meta_schema.get_table_id());
        new_table_schema->set_aux_lob_piece_tid(lob_piece_schema.get_table_id());
        }
      }
      }// end heap vars
    }
  }
  return ret;
}


int ObCreateTableLikeHelper::generate_foreign_keys_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(schema_guard_wrapper_.get_mock_fk_parent_table_id(
    new_tables_.at(0).get_database_id(), arg_.new_table_name_, replace_mock_fk_parent_table_id_))) {
    LOG_WARN("fail to ge mock fk parent table id",
              KR(ret), K_(tenant_id), K(new_tables_.at(0).get_database_id()), K_(arg_.new_table_name));
  } else if (OB_INVALID_ID != replace_mock_fk_parent_table_id_) {
    ObMockFKParentTableSchema *new_mock_fk_parent_table = nullptr;
    if (OB_FAIL(try_replace_mock_fk_parent_table_(replace_mock_fk_parent_table_id_,
                                                  new_mock_fk_parent_table))) {
      LOG_WARN("replace mock fk parent table failed", KR(ret));
    } else if (OB_NOT_NULL(new_mock_fk_parent_table)
               && OB_FAIL(new_mock_fk_parent_tables_.push_back(new_mock_fk_parent_table))) {
      LOG_WARN("fail to push back mock fk parent table", KR(ret), K(new_mock_fk_parent_table));
    }
  }
  return ret;
}


int ObCreateTableLikeHelper::operate_schemas_() {
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(inner_create_table_(&arg_.ddl_stmt_str_,
                                         replace_mock_fk_parent_table_id_))) {
    LOG_WARN("fail create table", KR(ret));
  }
  return ret;
}


int ObCreateTableLikeHelper::clean_on_fail_commit_()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObCreateTableLikeHelper::operation_before_commit_() {
  int ret = OB_SUCCESS;
  return ret;
}

int ObCreateTableLikeHelper::construct_and_adjust_result_(int &return_ret) {
  int ret = return_ret;
  if (FAILEDx(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_FAIL(get_current_version_(res_.schema_version_))) {
    LOG_WARN("fail to get current version", KR(ret));
  }
  if (OB_ERR_TABLE_EXIST == ret) {
    adjust_create_if_not_exist_(ret, arg_.if_not_exist_, res_.do_nothing_);
    if (arg_.if_not_exist_) {
      LOG_USER_NOTE(OB_ERR_TABLE_EXIST,
                    arg_.new_table_name_.length(), arg_.new_table_name_.ptr());
      LOG_INFO("table is exist, no need to create again", KR(ret), K_(arg));
    } else {
      LOG_USER_ERROR(OB_ERR_TABLE_EXIST, arg_.new_table_name_.length(), arg_.new_table_name_.ptr());
      LOG_WARN("table is exist, cannot create it twice", KR(ret), K_(arg));
    }
  }
  return ret;
}