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

#define USING_LOG_PREFIX SERVER
#include "observer/virtual_table/ob_show_grants.h"

#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_mgr.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/ob_priv_common.h"
using namespace oceanbase::common;
using namespace oceanbase::share::schema;
namespace oceanbase
{
namespace observer
{

ObShowGrants::ObShowGrants()
    : ObVirtualTableScannerIterator(),
      tenant_id_(OB_INVALID_ID),
      user_id_(OB_INVALID_ID)
{
}

ObShowGrants::~ObShowGrants()
{
}

void ObShowGrants::reset()
{
  tenant_id_ = OB_INVALID_ID;
  user_id_ = OB_INVALID_ID;
  ObVirtualTableScannerIterator::reset();
}

int ObShowGrants::add_priv_map(PRIV_MAP &priv_map, PrivKey &priv_key, ObPrivSet added_priv)
{
  int ret = OB_SUCCESS;
  ObPrivSet priv_set = OB_PRIV_SET_EMPTY;
  if (OB_FAIL(priv_map.get_refactored(priv_key, priv_set))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to get hash map", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  }
  if (OB_SUCC(ret)) {
    priv_set |= added_priv;
    OZ (priv_map.set_refactored(priv_key, priv_set, 1 /*overwrite*/));
  }
  return ret;
}

int ObShowGrants::add_priv_map_recursively(uint64_t user_id, PRIV_MAP &priv_map, bool expand_roles)
{
  int ret = OB_SUCCESS;
  const ObUserInfo *user_info = NULL;

  if (OB_ISNULL(schema_guard_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "schema guard is NULL", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "tenant_id is invalid", K(ret));
  } else if (OB_ISNULL(user_info = schema_guard_->get_user_info(tenant_id_, user_id))) {
    ret = OB_ERR_USER_NOT_EXIST;
    SERVER_LOG(WARN, "User not exist", K(ret), K_(tenant_id));
  } else {
    ObArray<const ObDBPriv *> db_priv_array;
    ObArray<const ObTablePriv *> table_priv_array;
    ObArray<const ObColumnPriv *> column_priv_array;
    ObArray<const ObRoutinePriv *> routine_priv_array;

    OZ (schema_guard_->get_db_priv_with_user_id(tenant_id_, user_id, db_priv_array));
    OZ (schema_guard_->get_table_priv_with_user_id(tenant_id_, user_id, table_priv_array));
    OZ (schema_guard_->get_column_priv_with_user_id(tenant_id_, user_id, column_priv_array));
    OZ (schema_guard_->get_routine_priv_with_user_id(tenant_id_, user_id, routine_priv_array));

    //user_level
    if (OB_SUCC(ret)) {
      PrivKey priv_key;
      OZ (add_priv_map(priv_map, priv_key, user_info->get_priv_set()));
    }

    //db_level
    for (int i = 0; OB_SUCC(ret) && i < db_priv_array.count(); i++) {
      PrivKey priv_key;
      priv_key.db_name_ = db_priv_array.at(i)->get_database_name_str();
      OZ (add_priv_map(priv_map, priv_key, db_priv_array.at(i)->get_priv_set()));
    }

    //table_level
    for (int i = 0; OB_SUCC(ret) && i < table_priv_array.count(); i++) {
      PrivKey priv_key;
      priv_key.db_name_ = table_priv_array.at(i)->get_database_name_str();
      priv_key.table_name_ = table_priv_array.at(i)->get_table_name_str();
      OZ (add_priv_map(priv_map, priv_key, table_priv_array.at(i)->get_priv_set()));
    }

    //column_level
    for (int i = 0; OB_SUCC(ret) && i < column_priv_array.count(); i++) {
      PrivKey priv_key;
      priv_key.db_name_ = column_priv_array.at(i)->get_database_name_str();
      priv_key.table_name_ = column_priv_array.at(i)->get_table_name_str();
      priv_key.column_name_ = column_priv_array.at(i)->get_column_name_str();
      CK (OB_NOT_NULL(column_priv_array.at(i)));
      OZ (add_priv_map(priv_map, priv_key, column_priv_array.at(i)->get_priv_set()));
    }

    //routine
    for (int i = 0; OB_SUCC(ret) && i < routine_priv_array.count(); i++) {
      PrivKey priv_key;
      priv_key.db_name_ = routine_priv_array.at(i)->get_database_name_str();
      priv_key.table_name_ = routine_priv_array.at(i)->get_routine_name_str();
      priv_key.obj_type_ =
        routine_priv_array.at(i)->get_routine_type() == ObRoutineType::ROUTINE_PROCEDURE_TYPE ?
                                              ObObjectType::PROCEDURE : ObObjectType::FUNCTION;
      OZ (add_priv_map(priv_map, priv_key, routine_priv_array.at(i)->get_priv_set()));
    }

    if (OB_SUCC(ret) && expand_roles) {
      for (int i = 0; OB_SUCC(ret) && i < user_info->get_role_id_array().count(); i++) {
        if (OB_FAIL(SMART_CALL(add_priv_map_recursively(user_info->get_role_id_array().at(i),
                                                        priv_map,
                                                        expand_roles)))) {
          LOG_WARN("fail to add_priv_map_recursively", K(ret));
        }
      }
    }

  }
  return ret;
}

int ObShowGrants::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator is NULL", K(ret));
  } else if (OB_ISNULL(schema_guard_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "schema guard is NULL", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "tenant_id is invalid", K(ret));
  } else {
    if (!start_to_read_) {
      ObObj *cells = NULL;
      uint64_t show_user_id = OB_INVALID_ID;
      ObArray<uint64_t> role_ids;
      if (OB_FAIL(calc_show_user_id(show_user_id, role_ids))) {
        SERVER_LOG(WARN, "fail to calc show user id", K(ret));
      } else if (OB_UNLIKELY(OB_INVALID_ID == show_user_id)) {
        ret = OB_ITER_END;//FIXME 暂不支持，返回空集
      } else if (OB_FAIL(has_show_grants_priv(show_user_id))) {
        SERVER_LOG(WARN, "There is no show grants priv", K(ret));
      } else if (OB_ISNULL(cells = cur_row_.cells_)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
      } else {
        ObArray<const ObTableSchema *> table_schema_array;
        cur_row_.cells_ = cells;
        cur_row_.count_ = reserved_column_cnt_;
        const ObUserInfo *user_info = NULL;
        ObArray<const ObDBPriv *> db_priv_array;
        ObArray<const ObTablePriv *> table_priv_array;
        ObArray<const ObRoutinePriv *> routine_priv_array;
        ObArray<const ObColumnPriv *> column_priv_array;
        ObArray<const ObObjPriv *>obj_priv_array;
        PRIV_MAP priv_map;
        const int64_t PRIV_BUF_LENGTH = 512;
        char buf[PRIV_BUF_LENGTH] = {};
        int64_t pos = 0;
        ObString user_name;
        ObString host_name;
        ObString result;
        ObNeedPriv have_priv;

        if (OB_ISNULL(user_info = schema_guard_->get_user_info(tenant_id_, show_user_id))) {
          ret = OB_ERR_USER_NOT_EXIST;
          SERVER_LOG(WARN, "User not exist", K(ret), K_(tenant_id));
        } else if (OB_FAIL(schema_guard_->get_db_priv_with_user_id(tenant_id_,
                                                                   show_user_id,
                                                                   db_priv_array))) {
          SERVER_LOG(WARN, "Get db priv with user id error", K(ret));
        } else if (OB_FAIL(schema_guard_->get_table_priv_with_user_id(tenant_id_,
                                                                      show_user_id,
                                                                      table_priv_array))) {
          SERVER_LOG(WARN, "Get table priv with user id error", K(ret));
        } else if (OB_FAIL(schema_guard_->get_routine_priv_with_user_id(tenant_id_,
                                                                       show_user_id,
                                                                       routine_priv_array))) {
          SERVER_LOG(WARN, "Get routine priv with user id error", K(ret));
        } else if (OB_FAIL(schema_guard_->get_column_priv_with_user_id(tenant_id_,
                                                                      show_user_id,
                                                                      column_priv_array))) {
          SERVER_LOG(WARN, "Get table priv with user id error", K(ret));
        } else if (OB_FAIL(schema_guard_->get_obj_priv_with_grantee_id(tenant_id_,
                                                                       show_user_id,
                                                                       obj_priv_array))) {
          SERVER_LOG(WARN, "Get table priv with user id error", K(ret));
        } else {
          user_name = user_info->get_user_name_str();
          host_name = user_info->get_host_name_str();
        }

        if (OB_SUCC(ret) && lib::is_mysql_mode()) {
          OZ (priv_map.create(32, "GrantsMap"));
          OZ (add_priv_map_recursively(show_user_id, priv_map, false /*expand_roles*/));
          for (int i = 0; OB_SUCC(ret) && i < role_ids.count(); i++) {
            OZ (add_priv_map_recursively(role_ids.at(i), priv_map, true /*expand_roles*/));
          }

          //user level
          for (PRIV_MAP::const_iterator iter = priv_map.begin(); OB_SUCC(ret) && iter != priv_map.end(); ++iter) {
            const PrivKey &priv_key = iter->first;
            const ObPrivSet &privs = iter->second;
            if (priv_key.db_name_.empty()) {
              pos = 0;
              have_priv.priv_level_ = OB_PRIV_USER_LEVEL;
              have_priv.priv_set_ = privs;

              OZ (get_grants_string(buf, PRIV_BUF_LENGTH, pos, have_priv, user_name, host_name));
              OX (result.assign_ptr(buf, static_cast<int32_t>(pos)));
              OZ (fill_row_cells(show_user_id, result));
              OZ (scanner_.add_row(cur_row_));
            }
          }

          //db level
          for (PRIV_MAP::const_iterator iter = priv_map.begin(); OB_SUCC(ret) && iter != priv_map.end(); ++iter) {
            const PrivKey &priv_key = iter->first;
            const ObPrivSet &privs = iter->second;
            if (!priv_key.db_name_.empty()
                && priv_key.table_name_.empty()) {
              pos = 0;
              have_priv.priv_level_ = OB_PRIV_DB_LEVEL;
              have_priv.priv_set_ = privs;
              have_priv.db_ = priv_key.db_name_;

              OZ (get_grants_string(buf, PRIV_BUF_LENGTH, pos, have_priv, user_name, host_name));
              OX (result.assign_ptr(buf, static_cast<int32_t>(pos)));
              OZ (fill_row_cells(show_user_id, result));
              OZ (scanner_.add_row(cur_row_));
            }
          }

          //table and column level
          ObArray<std::pair<PrivKey, ObPrivSet>> priv_key_array;
          for (PRIV_MAP::const_iterator iter = priv_map.begin(); OB_SUCC(ret) && iter != priv_map.end(); ++iter) {
            if (iter->first.table_name_.empty()
                || ObObjectType::INVALID != iter->first.obj_type_) {
              //do nothing
            } else if (OB_FAIL(priv_key_array.push_back(std::make_pair(iter->first, iter->second)))) {
              LOG_WARN("push back failed", K(ret));
            }
          }
          lib::ob_sort(priv_key_array.begin(), priv_key_array.end(), PrivKey::cmp);
          ObArray<std::pair<PrivKey, ObPrivSet>> priv_key_value_set;
          for (int64_t i = 0; OB_SUCC(ret) && i < priv_key_array.count(); i++) {
            bool need_print = false;
            if (OB_FAIL(priv_key_value_set.push_back(priv_key_array.at(i)))) {
              LOG_WARN("push back failed", K(ret));
            } else if (i + 1 < priv_key_array.count()) {
              if (priv_key_array.at(i).first.db_name_ != priv_key_array.at(i + 1).first.db_name_
                 || priv_key_array.at(i).first.table_name_ != priv_key_array.at(i + 1).first.table_name_) {
                need_print = true;
              }
            } else {
              need_print = true;
            }

            if (OB_SUCC(ret)) {
              if (need_print && !priv_key_value_set.empty()) {
                have_priv.priv_level_ = OB_PRIV_TABLE_LEVEL;
                have_priv.priv_set_ = 0;
                have_priv.db_ = priv_key_value_set.at(0).first.db_name_;
                have_priv.table_ = priv_key_value_set.at(0).first.table_name_;
                have_priv.obj_type_ = priv_key_value_set.at(0).first.obj_type_;
                if (priv_key_value_set.at(0).first.column_name_.empty()) {
                  have_priv.priv_set_ = priv_key_value_set.at(0).second;
                  OZ (priv_key_value_set.remove(0));
                } else {
                  have_priv.priv_set_ = 0;
                }
                pos = 0;
                OZ (get_grants_string(buf, PRIV_BUF_LENGTH, pos, have_priv, user_name, host_name, &priv_key_value_set));
                OX (result.assign_ptr(buf, static_cast<int32_t>(pos)));
                OZ (fill_row_cells(show_user_id, result));
                OZ (scanner_.add_row(cur_row_));

                priv_key_value_set.reuse();
              }
            }
          }

          // pl priv
          for (PRIV_MAP::const_iterator iter = priv_map.begin(); OB_SUCC(ret) && iter != priv_map.end(); ++iter) {
            const PrivKey &priv_key = iter->first;
            const ObPrivSet &privs = iter->second;
            if (!priv_key.db_name_.empty()
                && !priv_key.table_name_.empty()
                && priv_key.obj_type_ != ObObjectType::INVALID) {
              pos = 0;
              have_priv.priv_level_ = OB_PRIV_ROUTINE_LEVEL;
              have_priv.priv_set_ = privs;
              have_priv.db_ = priv_key.db_name_;
              have_priv.table_ = priv_key.table_name_;
              have_priv.obj_type_ = priv_key.obj_type_;

              OZ (get_grants_string(buf, PRIV_BUF_LENGTH, pos, have_priv, user_name, host_name));
              OX (result.assign_ptr(buf, static_cast<int32_t>(pos)));
              OZ (fill_row_cells(show_user_id, result));
              OZ (scanner_.add_row(cur_row_));
            }
          }

          //grant roles
          if (OB_SUCC(ret)) {
            for (int i = 0; OB_SUCC(ret) && i < 2; i++) {
              pos = 0;
              OZ (grant_role_to_buff(buf, PRIV_BUF_LENGTH, pos, *user_info, !!i));
              if (OB_SUCC(ret) && pos > 0) {
                result.reset();
                result.assign_ptr(buf, static_cast<int32_t>(pos));
                OZ (fill_row_cells(show_user_id, result));
                OZ (scanner_.add_row(cur_row_));
              }
            }
          }
        }

        if (OB_SUCC(ret) && lib::is_oracle_mode()) {
          //grants on user
          have_priv.priv_level_ = OB_PRIV_USER_LEVEL;
          have_priv.priv_set_ = user_info->get_priv_set();
          pos = 0;
          if (OB_SUCCESS != (ret = get_grants_string(buf, PRIV_BUF_LENGTH, pos, have_priv, user_name, host_name))) {
            SERVER_LOG(WARN, "Fill grants string failed", K(ret));
          } else {
            result.reset();
            result.assign_ptr(buf, static_cast<int32_t>(pos));
            if (OB_FAIL(fill_row_cells(show_user_id, result))) {
              SERVER_LOG(WARN, "fail to fill row cells", K(ret));
            } else if (OB_FAIL(scanner_.add_row(cur_row_))) {
              SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
            }
          }

          //grants on db
          for (int64_t i = 0; OB_SUCC(ret) && i < db_priv_array.count(); ++i) {
            pos = 0;
            const ObDBPriv *db_priv = db_priv_array.at(i);
            have_priv.priv_level_ = OB_PRIV_DB_LEVEL;
            have_priv.priv_set_ = db_priv->get_priv_set();
            have_priv.db_ = db_priv->get_database_name_str();
            if (OB_FAIL(get_grants_string(buf, PRIV_BUF_LENGTH, pos, have_priv, user_name, host_name))) {
              SERVER_LOG(WARN, "Fill grants string failed", K(ret));
            } else {
              result.reset();
              result.assign_ptr(buf, static_cast<int32_t>(pos));
              if (OB_FAIL(fill_row_cells(show_user_id, result))) {
                SERVER_LOG(WARN, "fail to fill row cells", K(ret));
              } else if (OB_FAIL(scanner_.add_row(cur_row_))) {
                SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
              }
            }
          }
          //grants on db.table
          for (int64_t i = 0; OB_SUCC(ret) && i < table_priv_array.count(); ++i) {
            pos = 0;
            const ObTablePriv *table_priv = table_priv_array.at(i);
            have_priv.priv_level_ = OB_PRIV_TABLE_LEVEL;
            have_priv.priv_set_ = table_priv->get_priv_set();
            have_priv.db_ = table_priv->get_database_name_str();
            have_priv.table_ = table_priv->get_table_name_str();
            if (OB_FAIL(get_grants_string(buf, PRIV_BUF_LENGTH, pos, have_priv, user_name, host_name))) {
              SERVER_LOG(WARN, "Fill grants string failed", K(ret));
            } else {
              result.reset();
              result.assign_ptr(buf, static_cast<int32_t>(pos));
              if (OB_FAIL(fill_row_cells(show_user_id, result))) {
                SERVER_LOG(WARN, "fail to fill row cells", K(ret));
              } else if (OB_FAIL(scanner_.add_row(cur_row_))) {
                SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
              }
            }
          }

          // grants for obj in oracle
          for (int64_t i = 0; OB_SUCC(ret) && i< obj_priv_array.count(); ++i) {
            pos = 0;
            const ObObjPriv *obj_priv = obj_priv_array.at(i);
            ObOraNeedPriv ora_have_priv;
            const ObSimpleTableSchemaV2 *table_schema = NULL;
            const ObSequenceSchema *seq_schema = NULL;
            const ObSimpleDatabaseSchema *db_schema = NULL;
            const ObColumnSchemaV2 *col_schema = NULL;
            bool correct_obj_type = true;
            
            ObString obj_name;
            ObString col_name;
            ObString db_name;
            if (obj_priv != NULL) {
              switch (obj_priv->get_objtype()) {
                case static_cast<uint64_t>(share::schema::ObObjectType::TABLE):
                  if (OB_FAIL(schema_guard_->get_simple_table_schema(tenant_id_,
                                                              obj_priv->get_obj_id(),
                                                              table_schema))) {
                    SERVER_LOG(WARN, "Get table schema with table id error", K(ret), K(tenant_id_));
                  } else if (table_schema == NULL) {
                    ret = OB_TABLE_NOT_EXIST;
                    SERVER_LOG(WARN, "Table not exist", K(ret));
                  } else {
                    obj_name = table_schema->get_table_name_str();
                    if (OB_FAIL(schema_guard_->get_database_schema(tenant_id_,
                        table_schema->get_database_id(), db_schema))) {
                      SERVER_LOG(WARN, "Get db schema with db id error", K(ret), K_(tenant_id));
                    } else if (db_schema == NULL) {
                      ret = OB_ERR_BAD_DATABASE;
                      SERVER_LOG(WARN, "db not exist", K(ret));
                    } else {
                      db_name = db_schema->get_database_name_str();
                      if (obj_priv->get_col_id() != OB_COMPACT_COLUMN_INVALID_ID) {
                        if (NULL == (col_schema = schema_guard_->get_column_schema(
                                                      tenant_id_,
                                                      obj_priv->get_obj_id(),
                                                      obj_priv->get_col_id()))) {
                          ret = OB_SCHEMA_ERROR;
                          SERVER_LOG(WARN, "Get col schema error", K(ret), K_(tenant_id), K(obj_priv));
                        } else {
                          col_name = col_schema->get_column_name_str();
                        }
                      }
                    }
                  }
                  break;
                case static_cast<uint64_t>(share::schema::ObObjectType::SEQUENCE):
                  if (OB_FAIL(schema_guard_->get_sequence_schema(tenant_id_,
                                                                 obj_priv->get_obj_id(), 
                                                                 seq_schema))) {
                    SERVER_LOG(WARN, "Get seq schema with seq id error", K(ret));
                  } else if (seq_schema == NULL) {
                    ret = OB_ERR_SEQ_NOT_EXIST;
                    SERVER_LOG(WARN, "Sequence not exist", K(ret));
                  } else {
                    obj_name = seq_schema->get_sequence_name();
                    if (OB_FAIL(schema_guard_->get_database_schema(tenant_id_,
                        seq_schema->get_database_id(), db_schema))) {
                      SERVER_LOG(WARN, "Get db schema with db id error", K(ret), K_(tenant_id));
                    } else if (db_schema == NULL) {
                      ret = OB_ERR_BAD_DATABASE;
                      SERVER_LOG(WARN, "db not exist", K(ret));
                    }
                  }
                  break;
                default:
                  correct_obj_type = false;
                  SERVER_LOG(INFO, "Get incorrect obj_type", K(ret), K(*obj_priv));
                  break;
              }
              if (OB_SUCC(ret) && correct_obj_type) {
                ora_have_priv.obj_privs_ = obj_priv->get_obj_privs();
              }
            }
            if (OB_SUCC(ret) && correct_obj_type) {
              if (OB_FAIL(get_grants_string_ora(buf, 
                                                PRIV_BUF_LENGTH, 
                                                pos, 
                                                ora_have_priv, 
                                                db_name,
                                                obj_name,
                                                col_name,
                                                user_name, 
                                                host_name))) {
                SERVER_LOG(WARN, "Fill grants string failed", K(ret));
              } else {
                result.reset();
                result.assign_ptr(buf, static_cast<int32_t>(pos));
                if (OB_FAIL(fill_row_cells(show_user_id, result))) {
                  SERVER_LOG(WARN, "fail to fill row cells", K(ret));
                } else if (OB_FAIL(scanner_.add_row(cur_row_))) {
                  SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
                }
              }
            }
          }

        }
      }
      if (OB_SUCC(ret)) {
        scanner_it_ = scanner_.begin();
        start_to_read_ = true;
      }
    }
    if (OB_SUCCESS == ret && start_to_read_) {
      if (OB_SUCCESS != (ret = scanner_it_.get_next_row(cur_row_))) {
        if (OB_ITER_END != ret) {
          SERVER_LOG(WARN, "fail to get next row", K(ret));
        }
      } else {
        row = &cur_row_;
      }
    }
  }
  return ret;
}

int ObShowGrants::get_grants_string_ora(
    char *buf,
    const int64_t buf_len,
    int64_t &pos,
    ObOraNeedPriv &have_priv,
    ObString &db_name,
    ObString &obj_name,
    ObString &col_name,
    ObString &user_name,
    ObString &host_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "Buf is NULL", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "GRANT"))) {
    SERVER_LOG(WARN, "Fill buffer failed", K(ret));
  } else if (OB_FAIL(print_privs_to_buff(buf, buf_len, pos,
                                         OB_PRIV_TABLE_LEVEL,
                                         have_priv.obj_privs_))) {
    SERVER_LOG(WARN, "Fill privs to buffer failed", K(ret));
  } else if (OB_FAIL(priv_obj_info_ora(buf, buf_len, pos, db_name, obj_name, col_name))) {
    SERVER_LOG(WARN, "Fill privs to buffer failed", K(ret));
  } else if (NULL == user_name.ptr() && 0 != user_name.length()) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "ObString ptr is NULL, but length is not 0", K(ret), K(user_name));
  } else if (NULL == host_name.ptr() && 0 != host_name.length()) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "ObString ptr is NULL, but length is not 0", K(ret), K(host_name));
  } else {
    if (0 == host_name.compare(OB_DEFAULT_HOST_NAME)) {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, " TO \"%.*s\"",
                                  user_name.length(), user_name.ptr()))) {
        SERVER_LOG(WARN, "Fill priv to buffer failed", K(ret));
      }
    } else {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, " TO \"%.*s\"@\"%.*s\"",
                                  user_name.length(), user_name.ptr(),
                                  host_name.length(), host_name.ptr()))) {
        SERVER_LOG(WARN, "Fill priv to buffer failed", K(ret));
      }
    }
  }

  // if (OB_SUCC(ret)) {
  //   if (OB_FAIL(grant_option_to_buff(buf, buf_len, pos, have_priv.obj_privs_))) {
  //     SERVER_LOG(WARN, "Fill priv to buffer failed", K(ret));
  //   } else {
  //     //do nothing
  //   }
  // }
  return ret;
}

int ObShowGrants::print_obj_privs_to_buff_ora(
    char *buf,
    const int64_t buf_len,
    int64_t &pos,
    const share::ObPackedObjPriv obj_privs)
{
  int ret = OB_SUCCESS;
  bool exists = false;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "Buf is NULL", K(ret));
  }
  
  if (OB_SUCC(ret)) {
#define OB_OBJ_PRIV_TYPE_DEF(priv_id, priv_name) \
    if (OB_SUCC(ret)) {\
      if (OB_FAIL(share::ObOraPrivCheck::raw_obj_priv_exists(priv_id, obj_privs, exists))) {\
        } else if (exists) {\
        ret = BUF_PRINTF(" " priv_name ",");\
      }\
    }
#include "share/schema/ob_obj_priv_type.h"
#undef OB_OBJ_PRIV_TYPE_DEF
 
    if (OB_SUCCESS == ret && pos > 0) {
      pos--; //Delete last ','
    }
  }

  if (OB_FAIL(ret)) {
    SERVER_LOG(WARN, "Fill buff failed", K(ret));
  }
  return ret;
}


int ObShowGrants::get_grants_string(
    char *buf,
    const int64_t buf_len,
    int64_t &pos,
    ObNeedPriv &have_priv,
    ObString &user_name,
    ObString &host_name,
    ObIArray<std::pair<PrivKey, ObPrivSet>> *priv_key_array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "Buf is NULL", K(ret));
  } else if (priv_key_array != NULL && have_priv.priv_level_ != OB_PRIV_TABLE_LEVEL) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid column privilege", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "GRANT"))) {
    SERVER_LOG(WARN, "Fill buffer failed", K(ret));
  } else if (OB_FAIL(print_privs_to_buff(buf, buf_len, pos,
                                         have_priv.priv_level_,
                                         have_priv.priv_set_,
                                         priv_key_array))) {
    SERVER_LOG(WARN, "Fill privs to buffer failed", K(ret));
  } else if (OB_FAIL(priv_level_printf(buf, buf_len, pos, have_priv))) {
    SERVER_LOG(WARN, "Fill privs to buffer failed", K(ret));
  } else if (NULL == user_name.ptr() && 0 != user_name.length()) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "ObString ptr is NULL, but length is not 0", K(ret), K(user_name));
  } else if (NULL == host_name.ptr() && 0 != host_name.length()) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "ObString ptr is NULL, but length is not 0", K(ret), K(host_name));
  } else {
    if (0 == host_name.compare(OB_DEFAULT_HOST_NAME)) {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, " TO '%.*s'",
                                  user_name.length(), user_name.ptr()))) {
        SERVER_LOG(WARN, "Fill priv to buffer failed", K(ret));
      }
    } else {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, " TO '%.*s'@'%.*s'",
                                  user_name.length(), user_name.ptr(),
                                  host_name.length(), host_name.ptr()))) {
        SERVER_LOG(WARN, "Fill priv to buffer failed", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(grant_priv_to_buff(buf, buf_len, pos, have_priv.priv_set_))) {
      SERVER_LOG(WARN, "Fill priv to buffer failed", K(ret));
    } else {
      //do nothing
    }
  }
  return ret;
}

int ObShowGrants::print_column_privs_to_buff(
    char *buf,
    const int64_t buf_len,
    int64_t &pos,
    ObIArray<std::pair<PrivKey, ObPrivSet>> &priv_key_array,
    const ObPrivType priv_type)
{
  int ret = OB_SUCCESS;
  ObPrivSet priv_all = 0;
  ObSEArray<ObString, 2> cols;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Buf is NULL", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < priv_key_array.count(); i++) {
    if ((priv_key_array.at(i).second & priv_type) != 0) {
      if (OB_FAIL(cols.push_back(priv_key_array.at(i).first.column_name_))) {
        LOG_WARN("push back failed", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && cols.count() > 0) {
    if (priv_type == OB_PRIV_SELECT) {
      if (OB_FAIL(BUF_PRINTF(" SELECT ("))) {
        LOG_WARN("buf print failed", K(ret));
      }
    } else if (priv_type == OB_PRIV_INSERT) {
      if (OB_FAIL(BUF_PRINTF(" INSERT ("))) {
        LOG_WARN("buf print failed", K(ret));
      }
    } else if (priv_type == OB_PRIV_UPDATE) {
      if (OB_FAIL(BUF_PRINTF(" UPDATE ("))) {
        LOG_WARN("buf print failed", K(ret));
      }
    } else if (priv_type == OB_PRIV_REFERENCES) {
      if (OB_FAIL(BUF_PRINTF(" REFERENCES ("))) {
        LOG_WARN("buf print failed", K(ret));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid column priv type", K(ret), K(priv_type));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < cols.count(); i++) {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, "`%.*s`, ",
                                  cols.at(i).length(), cols.at(i).ptr()))) {
        LOG_WARN("Fill priv to buffer failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && cols.count() > 0 && pos > 1) {
      pos -= 2; //Delete last ', '
    }
    if (OB_SUCC(ret) && OB_FAIL(BUF_PRINTF("),"))) {
      LOG_WARN("buf print failed", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("Fill buff failed", K(ret));
  }
  return ret;
}

int ObShowGrants::print_privs_to_buff(
    char *buf,
    const int64_t buf_len,
    int64_t &pos,
    const ObPrivLevel priv_level,
    const ObPrivSet priv_set,
    ObIArray<std::pair<PrivKey, ObPrivSet>> *priv_key_array)
{
  int ret = OB_SUCCESS;
  ObPrivSet priv_all = 0;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "Buf is NULL", K(ret));
  } else if (OB_PRIV_USER_LEVEL == priv_level) {
    priv_all = OB_PRIV_ALL;
  } else if (OB_PRIV_DB_LEVEL == priv_level) {
    priv_all = OB_PRIV_DB_ACC;
  } else if (OB_PRIV_TABLE_LEVEL == priv_level) {
    priv_all = OB_PRIV_TABLE_ACC;
  } else if (OB_PRIV_ROUTINE_LEVEL == priv_level) {
    priv_all = OB_PRIV_ALL;
  } else if (OB_PRIV_OBJ_ORACLE_LEVEL == priv_level) {
  } else {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "Invalid priv level", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (priv_level == OB_PRIV_OBJ_ORACLE_LEVEL) {
      if (OB_FAIL(print_obj_privs_to_buff_ora(buf, buf_len, pos, priv_set))) {
         SERVER_LOG(WARN, "print obj privs failed", K(ret));
      }
    } else {
      if (0 == (priv_set & priv_all) && (priv_key_array == NULL || priv_key_array->empty())) {
        ret = databuff_printf(buf, buf_len, pos, " USAGE");
      } else if (priv_all == (priv_set & priv_all)) {
        ret = databuff_printf(buf, buf_len, pos, " ALL PRIVILEGES");
      } else {
        if ((priv_set & OB_PRIV_ALTER) && OB_SUCCESS == ret) {
          ret = BUF_PRINTF(" ALTER,");
        }
        if ((priv_set & OB_PRIV_CREATE) && OB_SUCCESS == ret) {
          ret = BUF_PRINTF(" CREATE,");
        }
        if ((priv_set & OB_PRIV_CREATE_USER) && OB_SUCCESS == ret) {
          ret = BUF_PRINTF(" CREATE USER,");
        }
        if ((priv_set & OB_PRIV_DELETE) && OB_SUCCESS == ret) {
          ret = BUF_PRINTF(" DELETE,");
        }
        if ((priv_set & OB_PRIV_DROP) && OB_SUCCESS == ret) {
          ret = BUF_PRINTF(" DROP,");
        }
        if ((priv_set & OB_PRIV_INSERT) && OB_SUCCESS == ret) {
          ret = BUF_PRINTF(" INSERT,");
        }
        if (OB_SUCC(ret) && priv_key_array != NULL && OB_FAIL(print_column_privs_to_buff(buf, buf_len, pos,
                                                                                *priv_key_array, OB_PRIV_INSERT))) {
          LOG_WARN("print column privs to buff failed", K(ret));
        }
        if ((priv_set & OB_PRIV_UPDATE) && OB_SUCCESS == ret) {
          ret = BUF_PRINTF(" UPDATE,");
        }
        if (OB_SUCC(ret) && priv_key_array != NULL && OB_FAIL(print_column_privs_to_buff(buf, buf_len, pos,
                                                                              *priv_key_array, OB_PRIV_UPDATE))) {
          LOG_WARN("print column privs to buff failed", K(ret));
        }
        if ((priv_set & OB_PRIV_SELECT) && OB_SUCCESS == ret) {
          ret = BUF_PRINTF(" SELECT,");
        }
        if (OB_SUCC(ret) && priv_key_array != NULL && OB_FAIL(print_column_privs_to_buff(buf, buf_len, pos,
                                                                                *priv_key_array, OB_PRIV_SELECT))) {
          LOG_WARN("print column privs to buff failed", K(ret));
        }
        if ((priv_set & OB_PRIV_INDEX) && OB_SUCCESS == ret) {
          ret = BUF_PRINTF(" INDEX,");
        }
        if ((priv_set & OB_PRIV_CREATE_VIEW) && OB_SUCCESS == ret) {
          ret = BUF_PRINTF(" CREATE VIEW,");
        }
        if ((priv_set & OB_PRIV_SHOW_VIEW) && OB_SUCCESS == ret) {
          ret = BUF_PRINTF(" SHOW VIEW,");
        }
        if ((priv_set & OB_PRIV_SHOW_DB) && OB_SUCCESS == ret) {
          ret = BUF_PRINTF(" SHOW DATABASES,");
        }
        if ((priv_set & OB_PRIV_SUPER) && OB_SUCCESS == ret) {
          ret = BUF_PRINTF(" SUPER,");
        }
        if ((priv_set & OB_PRIV_PROCESS) && OB_SUCCESS == ret) {
          ret = BUF_PRINTF(" PROCESS,");
        }
        if ((priv_set & OB_PRIV_BOOTSTRAP) && OB_SUCCESS == ret) {
          ret = BUF_PRINTF(" BOOTSTRAP,");
        }
        if ((priv_set & OB_PRIV_CREATE_SYNONYM) && OB_SUCCESS == ret) {
          ret = BUF_PRINTF(" CREATE_SYNONYM,");
        }
        if ((priv_set & OB_PRIV_AUDIT) && OB_SUCCESS == ret) {
          ret = BUF_PRINTF(" AUDIT,");
        }
        if ((priv_set & OB_PRIV_COMMENT) && OB_SUCCESS == ret) {
          ret = BUF_PRINTF(" COMMENT,");
        }
        if ((priv_set & OB_PRIV_LOCK) && OB_SUCCESS == ret) {
          ret = BUF_PRINTF(" LOCK,");
        }
        if ((priv_set & OB_PRIV_RENAME) && OB_SUCCESS == ret) {
          ret = BUF_PRINTF(" RENAME,");
        }
        if ((priv_set & OB_PRIV_REFERENCES) && OB_SUCCESS == ret) {
          ret = BUF_PRINTF(" REFERENCES,");
        }
        if (OB_SUCC(ret) && priv_key_array != NULL && OB_FAIL(print_column_privs_to_buff(buf, buf_len, pos,
                                                                              *priv_key_array, OB_PRIV_REFERENCES))) {
          LOG_WARN("print column privs to buff failed", K(ret));
        }
        if ((priv_set & OB_PRIV_FLASHBACK) && OB_SUCCESS == ret) {
          ret = BUF_PRINTF(" FLASHBACK,");
        }
        if ((priv_set & OB_PRIV_READ) && OB_SUCCESS == ret) {
          ret = BUF_PRINTF(" READ,");
        }
        if ((priv_set & OB_PRIV_WRITE) && OB_SUCCESS == ret) {
          ret = BUF_PRINTF(" WRITE,");
        }
        if ((priv_set & OB_PRIV_FILE) && OB_SUCCESS == ret) {
          ret = BUF_PRINTF(" FILE,");
        }
        if ((priv_set & OB_PRIV_ALTER_TENANT) && OB_SUCCESS == ret) {
          ret = BUF_PRINTF(" ALTER TENANT,");
        }
        if ((priv_set & OB_PRIV_ALTER_SYSTEM) && OB_SUCCESS == ret) {
          ret = BUF_PRINTF(" ALTER SYSTEM,");
        }
        if ((priv_set & OB_PRIV_CREATE_RESOURCE_POOL) && OB_SUCCESS == ret) {
          ret = BUF_PRINTF(" CREATE RESOURCE POOL,");
        }
        if ((priv_set & OB_PRIV_CREATE_RESOURCE_UNIT) && OB_SUCCESS == ret) {
          ret = BUF_PRINTF(" CREATE RESOURCE UNIT,");
        }
        if ((priv_set & OB_PRIV_REPL_SLAVE) && OB_SUCCESS == ret) {
          ret = BUF_PRINTF(" REPLICATION SLAVE,");
        }
        if ((priv_set & OB_PRIV_REPL_CLIENT) && OB_SUCCESS == ret) {
          ret = BUF_PRINTF(" REPLICATION CLIENT,");
        }
        if ((priv_set & OB_PRIV_DROP_DATABASE_LINK) && OB_SUCCESS == ret) {
          ret = BUF_PRINTF(" DROP DATABASE LINK,");
        }
        if ((priv_set & OB_PRIV_CREATE_DATABASE_LINK) && OB_SUCCESS == ret) {
          ret = BUF_PRINTF(" CREATE DATABASE LINK,");
        }
        if ((priv_set & OB_PRIV_EXECUTE) && OB_SUCCESS == ret) {
          ret = BUF_PRINTF(" EXECUTE,");
        }
        if ((priv_set & OB_PRIV_ALTER_ROUTINE) && OB_SUCCESS == ret) {
          ret = BUF_PRINTF(" ALTER ROUTINE,");
        }
        if ((priv_set & OB_PRIV_CREATE_ROUTINE) && OB_SUCCESS == ret) {
          ret = BUF_PRINTF(" CREATE ROUTINE,");
        }
        if ((priv_set & OB_PRIV_CREATE_TABLESPACE) && OB_SUCCESS == ret) {
          ret = BUF_PRINTF(" CREATE TABLESPACE,");
        }
        if ((priv_set & OB_PRIV_SHUTDOWN) && OB_SUCCESS == ret) {
          ret = BUF_PRINTF(" SHUTDOWN,");
        }
        if ((priv_set & OB_PRIV_RELOAD) && OB_SUCCESS == ret) {
          ret = BUF_PRINTF(" RELOAD,");
        }
        if (OB_SUCCESS == ret && pos > 0) {
          pos--; //Delete last ','
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    SERVER_LOG(WARN, "Fill buff failed", K(ret));
  }
  return ret;
}

/* 打印objname */
int ObShowGrants::priv_obj_info_ora(
    char *buf,
    const int64_t buf_len,
    int64_t &pos,
    ObString &db_name,
    ObString &obj_name,
    ObString &col_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "Buf is NULL", K(ret));
  } else {
    if (!col_name.empty()) {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, 
                                  "(\"%.*s\")", 
                                  col_name.length(), col_name.ptr()))) {
        SERVER_LOG(WARN, "Fill col name to buffer failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                                  " ON \"%.*s\".\"%.*s\"",
                                  db_name.length(), db_name.ptr(),
                                  obj_name.length(), obj_name.ptr()))) {
        SERVER_LOG(WARN, "Fill privs obj to buffer failed", K(ret));
      }
    }
  }
  return ret;
}

int ObShowGrants::priv_level_printf(
    char *buf,
    const int64_t buf_len,
    int64_t &pos,
    ObNeedPriv &have_priv)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "Buf is NULL", K(ret));
  } else if (OB_PRIV_USER_LEVEL == have_priv.priv_level_) {
    if (OB_SUCCESS != (ret = databuff_printf(buf, buf_len, pos, " ON *.*"))) {
      SERVER_LOG(WARN, "Fill privs to buffer failed", K(ret));
    }
  } else if (OB_PRIV_DB_LEVEL == have_priv.priv_level_) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                                lib::is_oracle_mode() ? " ON \"%.*s\".*" : " ON `%.*s`.*",
                                have_priv.db_.length(), have_priv.db_.ptr()))) {
      SERVER_LOG(WARN, "Fill privs to buffer failed", K(ret));
    }
  } else if (OB_PRIV_TABLE_LEVEL == have_priv.priv_level_) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                                lib::is_oracle_mode() ? " ON \"%.*s\".\"%.*s\"" : 
                                                          " ON `%.*s`.`%.*s`",
                                have_priv.db_.length(), have_priv.db_.ptr(),
                                have_priv.table_.length(), have_priv.table_.ptr()))) {
      SERVER_LOG(WARN, "Fill privs to buffer failed", K(ret));
    }
  } else if (OB_PRIV_OBJ_ORACLE_LEVEL == have_priv.priv_level_) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                                lib::is_oracle_mode() ? " ON \"%.*s\".\"%.*s\"" : 
                                                          " ON `%.*s`.`%.*s`",
                                have_priv.db_.length(), have_priv.db_.ptr(),
                                have_priv.table_.length(), have_priv.table_.ptr()))) {
      SERVER_LOG(WARN, "Fill privs to buffer failed", K(ret));
    }
  } else if (OB_PRIV_ROUTINE_LEVEL == have_priv.priv_level_) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, have_priv.obj_type_ == ObObjectType::PROCEDURE ? " ON PROCEDURE `%.*s`.`%.*s`" :
                                                                        " ON FUNCTION `%.*s`.`%.*s`",
                                have_priv.db_.length(), have_priv.db_.ptr(),
                                have_priv.table_.length(), have_priv.table_.ptr()))) {
      SERVER_LOG(WARN, "Fill privs to buffer failed", K(ret));
    }
  }
  return ret;
}

// int ObShowGrants::grant_option_to_buff(
//     char *buf, 
//     const int64_t buf_len, i
//     nt64_t &pos, 
//     const ObPrivSet priv_set)
// {
//   int ret = OB_SUCCESS;
//   if (OB_ISNULL(buf)) {
//     ret = OB_INVALID_ARGUMENT;
//     SERVER_LOG(WARN, "Buf is NULL", K(ret));
//   } else if (0 != (OB_PRIV_GRANT & priv_set)) {
//     if (OB_SUCCESS != (ret = databuff_printf(buf, buf_len, pos, " WITH GRANT OPTION"))) {
//       SERVER_LOG(WARN, "Fill privs to buffer failed", K(ret));
//     }
//   } else {
//     //do nothing
//   }
//   return ret;
// }

int ObShowGrants::grant_priv_to_buff(char *buf, const int64_t buf_len, int64_t &pos, const ObPrivSet priv_set)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "Buf is NULL", K(ret));
  } else if (0 != (OB_PRIV_GRANT & priv_set)) {
    if (OB_SUCCESS != (ret = databuff_printf(buf, buf_len, pos, " WITH GRANT OPTION"))) {
      SERVER_LOG(WARN, "Fill privs to buffer failed", K(ret));
    }
  } else {
    //do nothing
  }
  return ret;
}

int ObShowGrants::calc_show_user_id(uint64_t &show_user_id, ObIArray<uint64_t> &role_ids)
{
  int ret = OB_SUCCESS;
  ObRowkey start_key;
  ObRowkey end_key;
  for (int64_t i = 0; OB_SUCC(ret) && i < key_ranges_.count(); ++i) {
    start_key = key_ranges_.at(i).start_key_;
    end_key = key_ranges_.at(i).end_key_;
    const ObObj *start_key_obj_ptr = start_key.get_obj_ptr();
    const ObObj *end_key_obj_ptr = end_key.get_obj_ptr();
    if (start_key.get_obj_cnt() > 0
        && start_key.get_obj_cnt() == end_key.get_obj_cnt()
        && NULL != start_key_obj_ptr
        && NULL != end_key_obj_ptr) {
      if ((!start_key_obj_ptr[0].is_min_value() || !end_key_obj_ptr[0].is_max_value())
          && start_key_obj_ptr[0] != end_key_obj_ptr[0]) {
        ret = OB_NOT_IMPLEMENT;
        SERVER_LOG(WARN, "table id must be exact value", K(ret));
      } else {
        if (start_key_obj_ptr[0] == end_key_obj_ptr[0]) {
          if (ObIntType == start_key_obj_ptr[0].get_type()
              && ObIntType == end_key_obj_ptr[0].get_type()) {
            if (OB_INVALID_ID != show_user_id) {
              OZ (role_ids.push_back(start_key_obj_ptr[0].get_int()));
            } else {
              show_user_id = start_key_obj_ptr[0].get_int();
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "Start key and end key obj type should be ObIntType",
                K(ret),
                "start type", start_key_obj_ptr[0].get_type(),
                "end type", end_key_obj_ptr[0].get_type());
          }
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "Key range not valid", K(ret),
                                              "start key cnt", start_key.get_obj_cnt(),
                                              "end key cnt", end_key.get_obj_cnt(),
                                              "start key obj ptr", start_key_obj_ptr,
                                              "end key obj ptr", end_key_obj_ptr);
    }
  }
  return ret;
}

int ObShowGrants::fill_row_cells(uint64_t show_user_id, const ObString &grants_str)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_row_.cells_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "cells should not be NULL", K(ret));
  } else {
    uint64_t cell_idx = 0;
    for (int64_t j = 0; OB_SUCC(ret) && j < output_column_ids_.count(); ++j) {
      uint64_t col_id = output_column_ids_.at(j);
      switch(col_id) {
        // user_id
        case OB_APP_MIN_COLUMN_ID: {
          cur_row_.cells_[cell_idx].set_int(static_cast<int64_t>(show_user_id));
          break;
        }
        // grants
        case OB_APP_MIN_COLUMN_ID + 1: {
          cur_row_.cells_[cell_idx].set_varchar(grants_str);
          cur_row_.cells_[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx),
                     K(j), K(output_column_ids_), K(col_id));
          break;
        }
      }
      if (OB_SUCC(ret)) {
        cell_idx++;
      }
    }
  }
  return ret;
}

int ObShowGrants::has_show_grants_priv(uint64_t show_user_id) const
{
  int ret = OB_SUCCESS;
  if (user_id_ == show_user_id) {
    //do nothing. User can show grants for current user
  } else if (sql::ObSchemaChecker::is_ora_priv_check()) {
  } else { //For other user, need SELECT privilege for mysql database
    //FIXME@xiyu: schema_cache: master aad alloc, which is no need as we use arena for priv
    ObArenaAllocator alloc;
    ObStmtNeedPrivs stmt_need_privs(alloc);
    ObNeedPriv need_priv("mysql", "user", OB_PRIV_TABLE_LEVEL, OB_PRIV_SELECT, false);
    if (OB_FAIL(stmt_need_privs.need_privs_.init(1))) {
      SERVER_LOG(WARN, "fail to init need_privs", K(ret));
    } else if (OB_FAIL(stmt_need_privs.need_privs_.push_back(need_priv))) {
      SERVER_LOG(WARN, "Add need priv to stmt_need_privs error", K(ret));
    } else if (OB_FAIL(schema_guard_->check_priv(session_priv_, stmt_need_privs))) {
      SERVER_LOG(WARN, "No privilege show grants", K(ret));
    } else {
      //do nothing
    }
  }
  return ret;
}

int ObShowGrants::grant_role_to_buff(
    char *buf,
    const int64_t buf_len,
    int64_t &pos,
    const ObUserInfo &user_info,
    bool with_admin_option)
{
  int ret = OB_SUCCESS;
  int64_t pos_bak = pos;
  bool is_empty = true;

  if (OB_ISNULL(schema_guard_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "schema guard is NULL", K(ret));
  }

  OZ (databuff_printf(buf, buf_len, pos, "GRANT "));

  for (int i = 0; OB_SUCC(ret) && i < user_info.get_role_id_array().count(); i++) {
    if (user_info.get_admin_option(user_info.get_role_id_option_array().at(i)) == with_admin_option) {
      const ObUserInfo *role = NULL;
      if (OB_ISNULL(role = schema_guard_->get_user_info(user_info.get_tenant_id(),
                                                        user_info.get_role_id_array().at(i)))) {
        //ignore error
        LOG_WARN("role not exist", K(ret));
      } else {
        is_empty = false;
        OZ (databuff_printf(buf, buf_len, pos, "`%.*s`@`%.*s`,",
                            role->get_user_name_str().length(),
                            role->get_user_name_str().ptr(),
                            role->get_host_name_str().length(),
                            role->get_host_name_str().ptr()));
      }
    }
  }

  if (OB_SUCC(ret) && is_empty) {
    pos = pos_bak;
  } else {
    OX (buf[pos - 1] = ' ');
    OZ (databuff_printf(buf, buf_len, pos, "TO `%.*s`@`%.*s`",
                        user_info.get_user_name_str().length(),
                        user_info.get_user_name_str().ptr(),
                        user_info.get_host_name_str().length(),
                        user_info.get_host_name_str().ptr()));
    if (with_admin_option) {
      OZ (databuff_printf(buf, buf_len, pos, " WITH ADMIN OPTION"));
    }
  }

  return ret;
}

}/* ns observer*/
}/* ns oceanbase */
