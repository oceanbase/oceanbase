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

#define USING_LOG_PREFIX SHARE_SCHEMA

#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_mgr.h"
#include "sql/resolver/ob_schema_checker.h"
#include "sql/privilege_check/ob_ora_priv_check.h"

namespace oceanbase
{
using namespace common;
using namespace observer;

namespace share
{
namespace schema
{
enum class CheckRolePrivMode {
  CHECK_PRIV_HAS_NONE,
  CHECK_PRIV_HAS_ALL,
  CHECK_PRIV_HAS_ANY,
  CHECK_PRIV_HAS_OTHER,
};

typedef int (*COLLECT_FUNC)(const ObPrivMgr &priv_mgr,
                            const ObUserInfo &role_info,
                            const ObNeedPriv &need_priv,
                            ObNeedPriv &collected_priv);

int collect_user_level_priv_in_roles(const ObPrivMgr &priv_mgr,
                                     const ObUserInfo &role_info,
                                     const ObNeedPriv &need_priv,
                                     ObNeedPriv &collected_priv)
{
  int ret = OB_SUCCESS;
  collected_priv.priv_set_ |= role_info.get_priv_set();
  return ret;
}

int collect_db_level_priv_in_roles(const ObPrivMgr &priv_mgr,
                                   const ObUserInfo &role_info,
                                   const ObNeedPriv &need_priv,
                                   ObNeedPriv &collected_priv)
{
  int ret = OB_SUCCESS;
  ObPrivSet role_priv_set = OB_PRIV_SET_EMPTY;
  ObOriginalDBKey db_priv_key_role(role_info.get_tenant_id(),
                                   role_info.get_user_id(),
                                   need_priv.db_);
  if (OB_FAIL(priv_mgr.get_db_priv_set(db_priv_key_role, role_priv_set))) {
    LOG_WARN("get db priv set failed", KR(ret), K(role_priv_set));
  } else {
    collected_priv.priv_set_ |= role_priv_set;
  }
  return ret;
}


int collect_table_level_priv_in_roles(const ObPrivMgr &priv_mgr,
                                      const ObUserInfo &role_info,
                                      const ObNeedPriv &need_priv,
                                      ObNeedPriv &collected_priv)
{
  int ret = OB_SUCCESS;
  ObPrivSet role_priv_set = OB_PRIV_SET_EMPTY;
  ObTablePrivSortKey role_table_priv_key(role_info.get_tenant_id(),
                                         role_info.get_user_id(),
                                         need_priv.db_,
                                         need_priv.table_);
  if (OB_FAIL(priv_mgr.get_table_priv_set(role_table_priv_key, role_priv_set))) {
    LOG_WARN("get table priv failed", KR(ret), K(role_priv_set) );
  } else {
    collected_priv.priv_set_ |= role_priv_set;
  }
  return ret;
}

int collect_column_level_priv_in_roles(const ObPrivMgr &priv_mgr,
                                       const ObUserInfo &role_info,
                                       const ObNeedPriv &need_priv,
                                       ObNeedPriv &collected_priv)
{
  int ret = OB_SUCCESS;
  ObPrivSet role_priv_set = OB_PRIV_SET_EMPTY;
  for (int64_t i = 0; OB_SUCC(ret) && i < need_priv.columns_.count(); i++) {
    const ObString &column_name = need_priv.columns_.at(i);
    ObColumnPrivSortKey column_priv_key(role_info.get_tenant_id(),
                                        role_info.get_user_id(),
                                        need_priv.db_,
                                        need_priv.table_,
                                        column_name);
    if (OB_FAIL(priv_mgr.get_column_priv_set(column_priv_key, role_priv_set))) {
      LOG_WARN("get table priv failed", KR(ret));
    } else if (OB_TEST_PRIVS(role_priv_set, need_priv.priv_set_)) {
      if (OB_FAIL(add_var_to_array_no_dup(collected_priv.columns_, column_name))) {
        LOG_WARN("fail to append array", K(ret));
      }
    }
  }
  return ret;
}

int collect_any_column_level_priv_in_roles(const ObPrivMgr &priv_mgr,
                                           const ObUserInfo &role_info,
                                           const ObNeedPriv &need_priv,
                                           ObNeedPriv &collected_priv)
{
  int ret = OB_SUCCESS;
  ObArray<const ObColumnPriv *> column_privs;
  if (OB_FAIL(priv_mgr.get_column_priv_in_table(role_info.get_tenant_id(),
                                                role_info.get_user_id(),
                                                need_priv.db_,
                                                need_priv.table_,
                                                column_privs))) {
    LOG_WARN("get table priv failed", KR(ret));
  } else if (column_privs.count() > 0) {
    if (OB_FAIL(collected_priv.columns_.push_back(""))) {
      LOG_WARN("fail to push back", K(ret));
    }
  }
  return ret;
}

int collect_column_level_all_priv_in_roles(const ObPrivMgr &priv_mgr,
                                           const ObUserInfo &role_info,
                                           const ObNeedPriv &need_priv,
                                           ObNeedPriv &collected_priv)
{
  int ret = OB_SUCCESS;
  ObPrivSet priv_set = OB_PRIV_SET_EMPTY;
  if (need_priv.columns_.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg", K(need_priv));
  } else {
    ObColumnPrivSortKey column_key(role_info.get_tenant_id(),
                                   role_info.get_user_id(),
                                   need_priv.db_,
                                   need_priv.table_,
                                   need_priv.columns_.at(0));

    if (OB_FAIL(priv_mgr.get_column_priv_set(column_key, priv_set))) {
      LOG_WARN("get table priv failed", KR(ret));
    } else if (priv_set != OB_PRIV_SET_EMPTY) {
      collected_priv.priv_set_ |= priv_set;
    }
  }
  return ret;
}

int collect_user_db_level_priv_in_roles(const ObPrivMgr &priv_mgr,
                                        const ObUserInfo &role_info,
                                        const ObNeedPriv &need_priv,
                                        ObNeedPriv &collected_priv)
{
  int ret = OB_SUCCESS;
  ObPrivSet role_priv_set = role_info.get_priv_set();
  if (OB_FAIL(collect_db_level_priv_in_roles(priv_mgr, role_info, need_priv, collected_priv))) {
    LOG_WARN("get db priv set failed", KR(ret), K(role_priv_set));
  } else {
    collected_priv.priv_set_ |= role_priv_set;
  }
  return ret;
}

int collect_user_db_tb_level_priv_in_roles(const ObPrivMgr &priv_mgr,
                                           const ObUserInfo &role_info,
                                           const ObNeedPriv &need_priv,
                                           ObNeedPriv &collected_priv)
{
  int ret = OB_SUCCESS;
  ObPrivSet role_priv_set = role_info.get_priv_set();
  if (OB_FAIL(collect_db_level_priv_in_roles(priv_mgr, role_info, need_priv, collected_priv))) {
    LOG_WARN("get db priv set failed", KR(ret), K(role_priv_set));
  } else if (OB_FAIL(collect_table_level_priv_in_roles(priv_mgr, role_info, need_priv, collected_priv))) {
    LOG_WARN("get db priv set failed", KR(ret), K(role_priv_set));
  } else {
    collected_priv.priv_set_ |= role_priv_set;
  }
  return ret;
}

int collect_priv_in_roles(const ObPrivMgr &priv_mgr,
                          const ObSessionPrivInfo &session_priv,
                          const common::ObIArray<uint64_t> &enable_role_id_array,
                          share::schema::ObSchemaGetterGuard &schema_guard,
                          const ObNeedPriv &need_priv,
                          COLLECT_FUNC collect_func,
                          ObNeedPriv &collected_priv,
                          bool &priv_succ,
                          CheckRolePrivMode mode = CheckRolePrivMode::CHECK_PRIV_HAS_ALL)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> role_ids_queue;
  const ObUserInfo *cur_user_info = NULL;
  priv_succ = false;

  if (OB_ISNULL(cur_user_info = schema_guard.get_user_info(session_priv.tenant_id_, session_priv.user_id_))) {
    ret = OB_USER_NOT_EXIST;
    LOG_WARN("fail to get user_info", K(ret));
  } else {
    LOG_DEBUG("check user info for roles",
              K(enable_role_id_array),
              K(cur_user_info->get_user_name_str()), K(cur_user_info->get_role_id_array()));
  }


  for (int i = 0; OB_SUCC(ret) && i < enable_role_id_array.count(); ++i) {
    uint64_t cur_role_id = enable_role_id_array.at(i);
    if (has_exist_in_array(cur_user_info->get_role_id_array(), cur_role_id)) {
      //enabled role can be revoked from the current user by other session
      //only check the granted roles
      if (OB_FAIL(role_ids_queue.push_back(cur_role_id))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
  }

  for (int i = 0; OB_SUCC(ret) && i < role_ids_queue.count() && !priv_succ; i++) {
    const uint64_t role_id = role_ids_queue.at(i);
    const ObUserInfo *role_info = NULL;
    if (OB_ISNULL(role_info = schema_guard.get_user_info(session_priv.tenant_id_, role_id))) {
      ret = OB_ERR_USER_NOT_EXIST;
      LOG_WARN("user not exist", K(ret));
    } else if (OB_FAIL(collect_func(priv_mgr, *role_info, need_priv, collected_priv))) {
      LOG_WARN("fail to collect priv", K(ret));
    } else {
      switch (mode) {
        case CheckRolePrivMode::CHECK_PRIV_HAS_ALL:
          priv_succ = OB_TEST_PRIVS(collected_priv.priv_set_, need_priv.priv_set_);
          break;
        case CheckRolePrivMode::CHECK_PRIV_HAS_ANY:
          priv_succ = OB_PRIV_HAS_ANY(collected_priv.priv_set_, need_priv.priv_set_);
          break;
        case CheckRolePrivMode::CHECK_PRIV_HAS_OTHER:
          priv_succ = OB_PRIV_HAS_OTHER(collected_priv.priv_set_, need_priv.priv_set_);
          break;
        case CheckRolePrivMode::CHECK_PRIV_HAS_NONE:
          break;
      }
      if (need_priv.columns_.count() > 0
          && (need_priv.columns_.count() == collected_priv.columns_.count()
              || need_priv.check_any_column_priv_)) {
        priv_succ = true;
      }
    }

    if (OB_SUCC(ret) && !priv_succ) {
      for (int j = 0; OB_SUCC(ret) && j < role_info->get_role_id_array().count(); j++) {
        OZ (role_ids_queue.push_back(role_info->get_role_id_array().at(j)));
      }
    }
  }

  return ret;
}

// TODO: check arguments
int ObSchemaGetterGuard::check_db_show(const ObSessionPrivInfo &session_priv,
                                       const common::ObIArray<uint64_t> &enable_role_id_array,
                                       const ObString &db,
                                       bool &allow_show)
{
  int ret = OB_SUCCESS;
  int can_show = OB_SUCCESS;
  uint64_t tenant_id = session_priv.tenant_id_;
  allow_show = true;
  ObPrivSet db_priv_set = 0;
  ObPrivSet need_priv = OB_PRIV_SHOW_DB;
  if (sql::ObSchemaChecker::is_ora_priv_check()) {
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (!session_priv.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", K(session_priv), KR(ret));
  } else if (OB_TEST_PRIVS(session_priv.user_priv_set_, need_priv)) {
    /* user priv level has show_db */
  } else if (0 == db.length() // only check user-level privilege if 0 == db.length()
             || OB_SUCCESS != (can_show = check_db_access(session_priv, enable_role_id_array, db, db_priv_set, false))) {
    allow_show = false;
  }
  return ret;
}

int ObSchemaGetterGuard::check_table_show(const ObSessionPrivInfo &session_priv,
                                          const common::ObIArray<uint64_t> &enable_role_id_array,
                                          const ObString &db,
                                          const ObString &table,
                                          bool &allow_show)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = session_priv.tenant_id_;
  const ObSchemaMgr *mgr = NULL;
  allow_show = true;
  if (sql::ObSchemaChecker::is_ora_priv_check()) {
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_PRIV_HAS_ANY(session_priv.user_priv_set_, OB_PRIV_TABLE_ACC)) {
    //allow
  } else if (0 == db.length() || 0 == table.length()) {
    allow_show = false;
  } else {
    const ObPrivMgr &priv_mgr = mgr->priv_mgr_;

    const ObTablePriv *table_priv = NULL;
    ObPrivSet db_priv_set = 0;
    //get db_priv_set
    if (session_priv.db_.length() != 0 && (session_priv.db_ == db || 0 == db.length())) {
      db_priv_set = session_priv.db_priv_set_;
    } else {
      ObOriginalDBKey db_priv_key(session_priv.tenant_id_, session_priv.user_id_, db);
      if (OB_FAIL(priv_mgr.get_db_priv_set(db_priv_key, db_priv_set))) {
        LOG_WARN("get db priv set failed", K(db_priv_key), KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      //check db_priv_set, then check table_priv_set
      if (OB_PRIV_HAS_ANY(db_priv_set, OB_PRIV_DB_ACC)) {
        //allow
      } else {
        ObTablePrivSortKey table_priv_key(session_priv.tenant_id_, session_priv.user_id_, db, table);
        if (OB_FAIL(priv_mgr.get_table_priv(table_priv_key, table_priv))) {
          LOG_WARN("get table priv failed", K(table_priv_key), KR(ret));
        } else if (NULL != table_priv
                   && OB_PRIV_HAS_ANY(table_priv->get_priv_set(), OB_PRIV_TABLE_ACC)) {
          // allow
        } else {
          allow_show = false;
        }
      }
    }
  }

  if (OB_SUCC(ret) && !allow_show) {
    ObNeedPriv need_priv(db, table, OB_PRIV_TABLE_LEVEL, OB_PRIV_TABLE_ACC, false);
    ObNeedPriv collected_priv;
    if (OB_FAIL(collect_priv_in_roles(mgr->priv_mgr_, session_priv, enable_role_id_array, *this,
                                      need_priv, collect_user_db_tb_level_priv_in_roles,
                                      collected_priv, allow_show,
                                      CheckRolePrivMode::CHECK_PRIV_HAS_ANY))) {
      LOG_WARN("fail to collect priv in roles", K(ret));
    }
  }

  return ret;
}

int ObSchemaGetterGuard::check_routine_show(const ObSessionPrivInfo &session_priv,
                                            const common::ObIArray<uint64_t> &enable_role_id_array,
                                                  const ObString &db,
                                                  const ObString &routine,
                                                  bool &allow_show,
                                                  int64_t routine_type)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = session_priv.tenant_id_;
  const ObSchemaMgr *mgr = NULL;
  allow_show = true;
  if (sql::ObSchemaChecker::is_ora_priv_check()) {
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_PRIV_HAS_ANY(session_priv.user_priv_set_, OB_PRIV_ROUTINE_ACC | OB_PRIV_CREATE_ROUTINE)) {
    //allow
  } else if (0 == db.length() || 0 == routine.length()) {
    allow_show = false;
  } else {
    const ObPrivMgr &priv_mgr = mgr->priv_mgr_;

    const ObRoutinePriv *routine_priv = NULL;
    ObPrivSet db_priv_set = 0;
    //get db_priv_set
    if (session_priv.db_.length() != 0 && (session_priv.db_ == db || 0 == db.length())) {
      db_priv_set = session_priv.db_priv_set_;
    } else {
      ObOriginalDBKey db_priv_key(session_priv.tenant_id_, session_priv.user_id_, db);
      if (OB_FAIL(priv_mgr.get_db_priv_set(db_priv_key, db_priv_set))) {
        LOG_WARN("get db priv set failed", K(db_priv_key), KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      //check db_priv_set, then check routine_priv_set
      if (OB_PRIV_HAS_ANY(db_priv_set, OB_PRIV_DB_ACC)) {
        //allow
      } else {
        ObRoutinePrivSortKey routine_priv_key(session_priv.tenant_id_, session_priv.user_id_, db, routine, routine_type);
        if (OB_FAIL(priv_mgr.get_routine_priv(routine_priv_key, routine_priv))) {
          LOG_WARN("get routine priv failed", K(routine_priv_key), KR(ret));
        } else if (NULL != routine_priv
                   && OB_PRIV_HAS_ANY(routine_priv->get_priv_set(), OB_PRIV_ROUTINE_ACC | OB_PRIV_CREATE_ROUTINE)) {
          // allow
        } else {
          allow_show = false;
        }
      }
    }
  }

  if (OB_SUCC(ret) && !allow_show) {
    ObNeedPriv need_priv(db, routine, OB_PRIV_ROUTINE_LEVEL, OB_PRIV_ROUTINE_ACC, false);
    ObNeedPriv collected_priv;
    if (OB_FAIL(collect_priv_in_roles(mgr->priv_mgr_, session_priv, enable_role_id_array, *this,
                                      need_priv, collect_user_db_tb_level_priv_in_roles,
                                      collected_priv, allow_show,
                                      CheckRolePrivMode::CHECK_PRIV_HAS_ANY))) {
      LOG_WARN("fail to collect priv in roles", K(ret));
    }
  }

  return ret;
}

int ObSchemaGetterGuard::check_user_priv(const ObSessionPrivInfo &session_priv,
                                         const common::ObIArray<uint64_t> &enable_role_id_array,
                                         const ObPrivSet priv_set,
                                         bool check_all)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = session_priv.tenant_id_;
  const ObSchemaMgr *mgr = NULL;
  ObPrivSet user_priv_set = session_priv.user_priv_set_;
  bool is_oracle_mode = false;

  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if ((!OB_TEST_PRIVS(user_priv_set, priv_set) && check_all)
             || (!OB_PRIV_HAS_ANY(user_priv_set, priv_set) && !check_all)) {
    if ((priv_set == OB_PRIV_ALTER_TENANT
        || priv_set == OB_PRIV_ALTER_SYSTEM
        || priv_set == OB_PRIV_CREATE_RESOURCE_POOL
        || priv_set == OB_PRIV_CREATE_RESOURCE_UNIT)
        && (OB_TEST_PRIVS(user_priv_set, OB_PRIV_SUPER))) {
    } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(tenant_id, is_oracle_mode))) {
      LOG_WARN("fail to get compat mode", K(ret));
    } else if (!is_oracle_mode) {
      ObNeedPriv need_priv("", "", OB_PRIV_USER_LEVEL, priv_set, false);
      ObNeedPriv collected_privs("", "", OB_PRIV_USER_LEVEL, OB_PRIV_SET_EMPTY, false);
      bool check_succ = false;
      if (OB_FAIL(collect_priv_in_roles(mgr->priv_mgr_, session_priv, enable_role_id_array, *this, need_priv,
                                        collect_user_level_priv_in_roles, collected_privs, check_succ))) {
        LOG_WARN("fail to collect privs in roles", K(ret));
      } else {
        user_priv_set |= collected_privs.priv_set_;
        if ((!check_succ && check_all) || (!OB_PRIV_HAS_ANY(user_priv_set, priv_set) && !check_all)) {
          ret = OB_ERR_NO_PRIVILEGE;
        }
      }
    }
  }
  if (OB_ERR_NO_PRIVILEGE == ret) {
    ObPrivSet lack_priv_set = priv_set &(~user_priv_set);
    const ObPrivMgr &priv_mgr = mgr->priv_mgr_;
    const char *priv_name = priv_mgr.get_first_priv_name(lack_priv_set);
    if (OB_ISNULL(priv_name)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid priv type", "priv_set", lack_priv_set);
    } else {
      if (priv_set == OB_PRIV_ALTER_TENANT
          || priv_set == OB_PRIV_ALTER_SYSTEM
          || priv_set == OB_PRIV_CREATE_RESOURCE_POOL
          || priv_set == OB_PRIV_CREATE_RESOURCE_UNIT) {
        ObSqlString priv_name_with_prefix;
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(priv_name_with_prefix.assign_fmt("SUPER or %s", priv_name))) {
          LOG_WARN("fail to assign fmt", KR(tmp_ret));
          LOG_USER_ERROR(OB_ERR_NO_PRIVILEGE, priv_name);
        } else {
          LOG_USER_ERROR(OB_ERR_NO_PRIVILEGE, priv_name_with_prefix.ptr());
        }
      } else if (priv_set == (OB_PRIV_CREATE_ROLE | OB_PRIV_CREATE_USER) && !check_all) {
        LOG_USER_ERROR(OB_ERR_NO_PRIVILEGE, "CREATE USER or CREATE ROLE");
      } else if (priv_set == (OB_PRIV_DROP_ROLE | OB_PRIV_CREATE_USER) && !check_all) {
        LOG_USER_ERROR(OB_ERR_NO_PRIVILEGE, "CREATE USER or DROP ROLE");
      } else {
        LOG_USER_ERROR(OB_ERR_NO_PRIVILEGE, priv_name);
      }
    }
  }

  return ret;
}

int ObSchemaGetterGuard::check_single_table_priv_or(const ObSessionPrivInfo &session_priv,
                                                    const common::ObIArray<uint64_t> &enable_role_id_array,
                                                    const ObNeedPriv &table_need_priv)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = session_priv.tenant_id_;
  const uint64_t user_id = session_priv.user_id_;
  const ObSchemaMgr *mgr = NULL;
  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == user_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", "tenant_id", tenant_id, "user_id", user_id, KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_PRIV_HAS_ANY(session_priv.user_priv_set_, table_need_priv.priv_set_)) {
    /* check success */
  } else {
    const ObPrivMgr &priv_mgr = mgr->priv_mgr_;
    bool pass = false;
    if (OB_FAIL(check_priv_db_or_(session_priv, enable_role_id_array, table_need_priv, priv_mgr, tenant_id, user_id, pass))) {
      LOG_WARN("failed to check priv db or", K(ret));
    } else if (pass) {
      /* check success */
    } else if (OB_FAIL(check_priv_table_or_(session_priv, enable_role_id_array, table_need_priv, priv_mgr, tenant_id, user_id, pass))) {
      LOG_WARN("fail to check priv table or", K(ret));
    } else if (pass) {
      /* check success */
    } else {
      ret = OB_ERR_NO_TABLE_PRIVILEGE;
      const char *priv_name = "ANY";
      LOG_USER_ERROR(OB_ERR_NO_TABLE_PRIVILEGE, (int)strlen(priv_name), priv_name,
                     session_priv.user_name_.length(), session_priv.user_name_.ptr(),
                     session_priv.host_name_.length(), session_priv.host_name_.ptr(),
                     table_need_priv.table_.length(), table_need_priv.table_.ptr());
    }
  }
  return ret;
}

int ObSchemaGetterGuard::check_single_table_priv(const ObSessionPrivInfo &session_priv,
                                                 const common::ObIArray<uint64_t> &enable_role_id_array,
                                                 const ObNeedPriv &table_need_priv)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = session_priv.tenant_id_;
  const ObSchemaMgr *mgr = NULL;
  bool is_oracle_mode = false;
  if (OB_INVALID_ID == session_priv.tenant_id_ || OB_INVALID_ID == session_priv.user_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", "tenant_id", session_priv.tenant_id_,
        "user_id", session_priv.user_id_,
        KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(tenant_id, is_oracle_mode))) {
    LOG_WARN("fail to get compat mode", K(ret));
  } else {
    //first:check user and db priv.
    //second:If user_db_priv_set has no enough privileges, check table priv.
    const ObPrivMgr &priv_mgr = mgr->priv_mgr_;
    if (!OB_TEST_PRIVS(session_priv.user_priv_set_, table_need_priv.priv_set_)) {
      ObPrivSet user_db_priv_set = 0;
      if (OB_SUCCESS != check_db_priv(session_priv, enable_role_id_array, table_need_priv.db_,
          table_need_priv.priv_set_, user_db_priv_set)) {
        //1. fetch table priv
        const ObTablePriv *table_priv = NULL;
        ObPrivSet table_priv_set = 0;
        bool is_table_priv_empty = true;
        ObTablePrivSortKey table_priv_key(session_priv.tenant_id_,
                                          session_priv.user_id_,
                                          table_need_priv.db_,
                                          table_need_priv.table_);
        if (OB_FAIL(priv_mgr.get_table_priv(table_priv_key, table_priv))) {
          LOG_WARN("get table priv failed", KR(ret), K(table_priv_key) );
        } else if (NULL != table_priv) {
          table_priv_set = table_priv->get_priv_set();
          is_table_priv_empty = false;
        }

        if (OB_SUCC(ret) && is_oracle_mode) {
          //2. fetch roles privs
          const ObUserInfo *user_info = NULL;
          if (OB_FAIL(get_user_info(tenant_id, session_priv.user_id_, user_info))) {
            LOG_WARN("failed to get user info", KR(ret), K(tenant_id), K(session_priv.user_id_));
          } else if (NULL == user_info) {
            ret = OB_USER_NOT_EXIST;
            LOG_WARN("user info is null", KR(ret), K(session_priv.user_id_));
          } else {
            const ObSEArray<uint64_t, 8> &role_id_array = user_info->get_role_id_array();
            for (int i = 0; OB_SUCC(ret) && i < role_id_array.count(); ++i) {
              const ObUserInfo *role_info = NULL;
              const ObTablePriv *role_table_priv = NULL;
              if (OB_FAIL(get_user_info(tenant_id, role_id_array.at(i), role_info))) {
                LOG_WARN("failed to get role ids", KR(ret), K(tenant_id), K(role_id_array.at(i)));
              } else if (NULL == role_info) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("role info is null", KR(ret), K(role_id_array.at(i)));
              } else {
                ObTablePrivSortKey role_table_priv_key(session_priv.tenant_id_,
                    role_info->get_user_id(),
                    table_need_priv.db_,
                    table_need_priv.table_);
                if (OB_FAIL(priv_mgr.get_table_priv(role_table_priv_key, role_table_priv))) {
                  LOG_WARN("get table priv failed", KR(ret), K(role_table_priv_key) );
                } else if (NULL != role_table_priv) {
                  is_table_priv_empty = false;
                  // append additional role
                  table_priv_set |= role_table_priv->get_priv_set();
                }
              }
            }
          }
        }

        if (OB_SUCC(ret) && !is_oracle_mode) {
          is_table_priv_empty = false;
          ObNeedPriv collected_tb_privs(table_need_priv.db_, table_need_priv.table_,
                                        OB_PRIV_TABLE_LEVEL, OB_PRIV_SET_EMPTY, false);
          bool check_succ = false;
          if (OB_FAIL(collect_priv_in_roles(mgr->priv_mgr_, session_priv, enable_role_id_array, *this, table_need_priv,
                                            collect_user_db_tb_level_priv_in_roles, collected_tb_privs, check_succ))) {
            LOG_WARN("fail to collect privs in roles", K(ret));
          } else {
            table_priv_set |= collected_tb_privs.priv_set_;
          }
        }

        //3. check privs
        if (OB_SUCC(ret)) {
          if (is_table_priv_empty) {
            ret = OB_ERR_NO_TABLE_PRIVILEGE;
            LOG_WARN("No privilege, cannot find table priv info",
                     "tenant_id", session_priv.tenant_id_,
                     "user_id", session_priv.user_id_, K(table_need_priv), K(lbt()));
          } else if (!OB_TEST_PRIVS(table_priv_set | user_db_priv_set, table_need_priv.priv_set_)) {
            ret = OB_ERR_NO_TABLE_PRIVILEGE;
            LOG_WARN("No privilege", "tenant_id", session_priv.tenant_id_,
                "user_id", session_priv.user_id_,
                K(table_need_priv),
                K(table_priv_set | user_db_priv_set));
          }
        }
        if (OB_ERR_NO_TABLE_PRIVILEGE == ret) {
          ObPrivSet lack_priv_set = table_need_priv.priv_set_ & (~(table_priv_set | user_db_priv_set));
          if (table_need_priv.columns_.empty()) {
            if (table_need_priv.check_any_column_priv_) {
              ret = OB_SUCCESS;
              ObArray<const ObColumnPriv *> column_privs;
              if (OB_FAIL(priv_mgr.get_column_priv_in_table(table_priv_key.tenant_id_, table_priv_key.user_id_,
                                                          table_priv_key.db_, table_priv_key.table_,
                                                          column_privs))) {
                LOG_WARN("get column priv in table failed", K(ret));
              } else {
                bool pass = false;
                for (int64_t i = 0; OB_SUCC(ret) && !pass && i < column_privs.count(); i++) {
                  if (OB_ISNULL(column_privs.at(i))) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("unexpected error", K(ret));
                  } else if ((column_privs.at(i)->get_priv_set() & table_need_priv.priv_set_)
                                                                                  == table_need_priv.priv_set_) {
                    pass = true;
                  }
                }
                if (OB_SUCC(ret) && !pass) {
                  ret = OB_ERR_NO_TABLE_PRIVILEGE;
                }
              }
            }

            if (OB_ERR_NO_COLUMN_PRIVILEGE == ret && !is_oracle_mode) {
              ret = OB_SUCCESS;
              ObNeedPriv collected_privs(table_need_priv.db_, table_need_priv.table_,
                                         OB_PRIV_TABLE_LEVEL, OB_PRIV_SET_EMPTY, false);
              bool check_succ = false;
              if (OB_FAIL(collect_priv_in_roles(mgr->priv_mgr_, session_priv, enable_role_id_array, *this, table_need_priv,
                                                collect_column_level_priv_in_roles, collected_privs, check_succ))) {
                LOG_WARN("fail to collect privs in roles", K(ret));
              } else {
                if (!check_succ) {
                  ret = OB_ERR_NO_COLUMN_PRIVILEGE;
                }
              }
            }

            if (OB_ERR_NO_TABLE_PRIVILEGE == ret) {
              const char *priv_name = priv_mgr.get_first_priv_name(lack_priv_set);
              if (OB_ISNULL(priv_name)) {
                ret = OB_INVALID_ARGUMENT;
                LOG_WARN("Invalid priv type", "priv_set", table_need_priv.priv_set_);
              } else {
                ret = OB_ERR_NO_TABLE_PRIVILEGE;
                LOG_USER_ERROR(OB_ERR_NO_TABLE_PRIVILEGE, (int)strlen(priv_name), priv_name,
                              session_priv.user_name_.length(), session_priv.user_name_.ptr(),
                              session_priv.host_name_.length(), session_priv.host_name_.ptr(),
                              table_need_priv.table_.length(), table_need_priv.table_.ptr());
              }
            }
          } else {
            ret = OB_SUCCESS;
            ObString col_name;
            ObPrivSet column_priv_set = 0;
            for (int64_t i = 0; OB_SUCC(ret) && i < table_need_priv.columns_.count(); i++) {
              const ObColumnPriv *column_priv = NULL;
              column_priv_set = 0;
              ObColumnPrivSortKey column_priv_key(session_priv.tenant_id_,
                                            session_priv.user_id_,
                                            table_need_priv.db_,
                                            table_need_priv.table_,
                                            table_need_priv.columns_.at(i));

              if (OB_FAIL(priv_mgr.get_column_priv(column_priv_key, column_priv))) {
                LOG_WARN("get table priv failed", KR(ret), K(table_priv_key) );
              } else if (NULL != column_priv) {
                column_priv_set = column_priv->get_priv_set();
                if (!OB_TEST_PRIVS(column_priv_set | table_priv_set | user_db_priv_set, table_need_priv.priv_set_)) {
                  ret = OB_ERR_NO_COLUMN_PRIVILEGE;
                  col_name = table_need_priv.columns_.at(i);
                }
              } else {
                ret = OB_ERR_NO_COLUMN_PRIVILEGE;
                col_name = table_need_priv.columns_.at(i);
              }
            }

            if (OB_ERR_NO_COLUMN_PRIVILEGE == ret && !is_oracle_mode) {
              ret = OB_SUCCESS;
              ObNeedPriv collected_privs(table_need_priv.db_, table_need_priv.table_,
                                         OB_PRIV_TABLE_LEVEL, OB_PRIV_SET_EMPTY, false);
              bool check_succ = false;
              if (OB_FAIL(collect_priv_in_roles(mgr->priv_mgr_, session_priv, enable_role_id_array, *this, table_need_priv,
                                                collect_column_level_priv_in_roles, collected_privs, check_succ))) {
                LOG_WARN("fail to collect privs in roles", K(ret));
              } else {
                if (!check_succ) {
                  ret = OB_ERR_NO_COLUMN_PRIVILEGE;
                }
              }
            }

            if (ret == OB_ERR_NO_COLUMN_PRIVILEGE) {
              ret = OB_SUCCESS;
              ObArray<const ObColumnPriv *> column_privs;
              bool found = false;
              if (OB_FAIL(priv_mgr.get_column_priv_in_table(table_priv_key.tenant_id_, table_priv_key.user_id_,
                                                          table_priv_key.db_, table_priv_key.table_,
                                                          column_privs))) {
                LOG_WARN("get column priv in table failed", K(ret));
              } else {
                for (int64_t i = 0; OB_SUCC(ret) && !found && i < column_privs.count(); i++) {
                  if (OB_ISNULL(column_privs.at(i))) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("unexpected error", K(ret));
                  } else if ((column_privs.at(i)->get_priv_set() & table_need_priv.priv_set_) != 0) {
                    found = true;
                  }
                }
              }
              ObPrivSet lack_priv_set = table_need_priv.priv_set_ & (~(column_priv_set | table_priv_set | user_db_priv_set));
              const char *priv_name = priv_mgr.get_first_priv_name(lack_priv_set);
              if (OB_FAIL(ret)) {
                LOG_WARN("other errors occur", K(ret));
              } else if (found) {
                ret = OB_ERR_NO_COLUMN_PRIVILEGE;
                LOG_USER_ERROR(OB_ERR_NO_COLUMN_PRIVILEGE, (int)strlen(priv_name), priv_name,
                                  session_priv.user_name_.length(), session_priv.user_name_.ptr(),
                                  session_priv.host_name_.length(), session_priv.host_name_.ptr(),
                                  col_name.length(), col_name.ptr(),
                                  table_need_priv.table_.length(), table_need_priv.table_.ptr());
              } else {
                ret = OB_ERR_NO_TABLE_PRIVILEGE;
                LOG_USER_ERROR(OB_ERR_NO_TABLE_PRIVILEGE, (int)strlen(priv_name), priv_name,
                              session_priv.user_name_.length(), session_priv.user_name_.ptr(),
                              session_priv.host_name_.length(), session_priv.host_name_.ptr(),
                              table_need_priv.table_.length(), table_need_priv.table_.ptr());
              }
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && table_need_priv.is_for_update_) {
      if (OB_FAIL(check_single_table_priv_for_update_(session_priv, enable_role_id_array, table_need_priv, priv_mgr))) {
        LOG_WARN("failed to check select table for update priv", K(ret));
      }
    }
  }
  return ret;
}

/* select ... from table for update, need select privilege and one of (delete, update lock tables).
 * ob donesn't have lock tables yet, then it checks select first and one of (delete、update on table level).
 */
int ObSchemaGetterGuard::check_single_table_priv_for_update_(const ObSessionPrivInfo &session_priv,
                                                             const common::ObIArray<uint64_t> &enable_role_id_array,
                                                             const ObNeedPriv &table_need_priv,
                                                             const ObPrivMgr &priv_mgr)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = session_priv.tenant_id_;
  const uint64_t user_id = session_priv.user_id_;
  bool pass = false;
  const ObNeedPriv need_priv(table_need_priv.db_, table_need_priv.table_, table_need_priv.priv_level_,
                             OB_PRIV_UPDATE | OB_PRIV_DELETE, table_need_priv.is_sys_table_,
                             table_need_priv.is_for_update_);
  if (OB_UNLIKELY(!table_need_priv.is_for_update_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not run this function without for update", K(ret), K(table_need_priv));
  } else if (OB_PRIV_HAS_ANY(session_priv.user_priv_set_, need_priv.priv_set_)) {
    /* check ok */
  } else if (OB_FAIL(check_priv_db_or_(session_priv, enable_role_id_array, need_priv, priv_mgr, tenant_id, user_id, pass))) {
    LOG_WARN("failed to check priv db or", K(ret));
  } else if (!pass && OB_FAIL(check_priv_table_or_(session_priv, enable_role_id_array, need_priv, priv_mgr, tenant_id, user_id, pass))) {
    LOG_WARN("fail to check priv table or", K(ret));
  } else if (!pass) {
    ret = OB_ERR_NO_TABLE_PRIVILEGE;
    const char *priv_name = "SELECT with locking clause";
    LOG_USER_ERROR(OB_ERR_NO_TABLE_PRIVILEGE, (int)strlen(priv_name), priv_name,
                                    session_priv.user_name_.length(), session_priv.user_name_.ptr(),
                                    session_priv.host_name_.length(), session_priv.host_name_.ptr(),
                                    table_need_priv.table_.length(), table_need_priv.table_.ptr());
  } else { /* check ok */ }
  return ret;
}

int ObSchemaGetterGuard::check_routine_priv(const ObSessionPrivInfo &session_priv,
                                            const common::ObIArray<uint64_t> &enable_role_id_array,
                                            const ObNeedPriv &routine_need_priv)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = session_priv.tenant_id_;
  const ObSchemaMgr *mgr = NULL;
  if (OB_INVALID_ID == session_priv.tenant_id_ || OB_INVALID_ID == session_priv.user_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", "tenant_id", session_priv.tenant_id_,
        "user_id", session_priv.user_id_,
        KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else {
    //first:check user and db priv.
    //second:If user_db_priv_set has no enough privileges, check routine priv.
    const ObPrivMgr &priv_mgr = mgr->priv_mgr_;
    if (!OB_TEST_PRIVS(session_priv.user_priv_set_, routine_need_priv.priv_set_)) {
      ObPrivSet user_db_priv_set = 0;
      if (OB_SUCCESS != check_db_priv(session_priv, enable_role_id_array, routine_need_priv.db_,
          routine_need_priv.priv_set_, user_db_priv_set)) {
        //1. fetch routine priv
        const ObRoutinePriv *routine_priv = NULL;
        ObPrivSet routine_priv_set = 0;
        bool is_routine_priv_empty = true;
        ObRoutinePrivSortKey routine_priv_key(session_priv.tenant_id_,
                                          session_priv.user_id_,
                                          routine_need_priv.db_,
                                          routine_need_priv.table_,
                                          routine_need_priv.obj_type_ == ObObjectType::PROCEDURE ? ObRoutineType::ROUTINE_PROCEDURE_TYPE :
                                          routine_need_priv.obj_type_ == ObObjectType::FUNCTION ? ObRoutineType::ROUTINE_FUNCTION_TYPE :
                                                                                                     ObRoutineType::INVALID_ROUTINE_TYPE);
        if (OB_FAIL(priv_mgr.get_routine_priv(routine_priv_key, routine_priv))) {
          LOG_WARN("get routine priv failed", KR(ret), K(routine_priv_key) );
        } else if (NULL != routine_priv) {
          routine_priv_set = routine_priv->get_priv_set();
          is_routine_priv_empty = false;
        }

        if (OB_SUCC(ret)) {
          //2. fetch roles privs
          const ObUserInfo *user_info = NULL;
          if (OB_FAIL(get_user_info(tenant_id, session_priv.user_id_, user_info))) {
            LOG_WARN("failed to get user info", KR(ret), K(tenant_id), K(session_priv.user_id_));
          } else if (NULL == user_info) {
            ret = OB_USER_NOT_EXIST;
            LOG_WARN("user info is null", KR(ret), K(session_priv.user_id_));
          } else {
            const ObSEArray<uint64_t, 8> &role_id_array = user_info->get_role_id_array();
            for (int i = 0; OB_SUCC(ret) && i < role_id_array.count(); ++i) {
              const ObUserInfo *role_info = NULL;
              const ObRoutinePriv *role_routine_priv = NULL;
              if (OB_FAIL(get_user_info(tenant_id, role_id_array.at(i), role_info))) {
                LOG_WARN("failed to get role ids", KR(ret), K(tenant_id), K(role_id_array.at(i)));
              } else if (NULL == role_info) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("role info is null", KR(ret), K(role_id_array.at(i)));
              } else {
                ObRoutinePrivSortKey role_routine_priv_key(session_priv.tenant_id_,
                    role_info->get_user_id(),
                    routine_need_priv.db_,
                    routine_need_priv.table_,
                    routine_need_priv.obj_type_ == ObObjectType::PROCEDURE ? ObRoutineType::ROUTINE_PROCEDURE_TYPE :
                    routine_need_priv.obj_type_ == ObObjectType::FUNCTION ? ObRoutineType::ROUTINE_FUNCTION_TYPE :
                                                                          ObRoutineType::INVALID_ROUTINE_TYPE);
                if (OB_FAIL(priv_mgr.get_routine_priv(role_routine_priv_key, role_routine_priv))) {
                  LOG_WARN("get routine priv failed", KR(ret), K(role_routine_priv_key) );
                } else if (NULL != role_routine_priv) {
                  is_routine_priv_empty = false;
                  // append additional role
                  routine_priv_set |= role_routine_priv->get_priv_set();
                }
              }
            }
          }
        }

        //3. check privs
        if (OB_SUCC(ret)) {
          if (is_routine_priv_empty) {
            ret = OB_ERR_NO_ROUTINE_PRIVILEGE;
            LOG_WARN("No privilege, cannot find routine priv info",
                     "tenant_id", session_priv.tenant_id_,
                     "user_id", session_priv.user_id_, K(routine_need_priv));
          } else if (!OB_TEST_PRIVS(routine_priv_set | user_db_priv_set, routine_need_priv.priv_set_)) {
            ret = OB_ERR_NO_ROUTINE_PRIVILEGE;
            LOG_WARN("No privilege", "tenant_id", session_priv.tenant_id_,
                "user_id", session_priv.user_id_,
                K(routine_need_priv),
                K(routine_priv_set | user_db_priv_set));
          }
        }
        if (OB_ERR_NO_ROUTINE_PRIVILEGE == ret) {
          ObPrivSet lack_priv_set = routine_need_priv.priv_set_ & (~(routine_priv_set | user_db_priv_set));
          const char *priv_name = priv_mgr.get_first_priv_name(lack_priv_set);
          if (OB_ISNULL(priv_name)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("Invalid priv type", "priv_set", routine_need_priv.priv_set_);
          } else {
            LOG_USER_ERROR(OB_ERR_NO_ROUTINE_PRIVILEGE, (int)strlen(priv_name), priv_name,
                           session_priv.user_name_.length(), session_priv.user_name_.ptr(),
                           session_priv.host_name_.length(), session_priv.host_name_.ptr(),
                           routine_need_priv.table_.length(), routine_need_priv.table_.ptr());
          }
        }
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::check_db_priv(const ObSessionPrivInfo &session_priv,
                                       const common::ObIArray<uint64_t> &enable_role_id_array,
                              const ObString &db,
                              const ObPrivSet need_priv_set,
                              ObPrivSet &user_db_priv_set)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = session_priv.tenant_id_;
  const ObSchemaMgr *mgr = NULL;
  ObPrivSet total_db_priv_set_role = OB_PRIV_SET_EMPTY;
  bool is_oracle_mode = false;
  if (OB_INVALID_ID == session_priv.tenant_id_ || OB_INVALID_ID == session_priv.user_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments", "tenant_id", session_priv.tenant_id_,
                                  "user_id", session_priv.user_id_,
                                  KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(tenant_id, is_oracle_mode))) {
    LOG_WARN("fail to get compat mode", K(ret));
  } else {
    ObPrivSet db_priv_set = 0;
    if (session_priv.db_.length() != 0 && (session_priv.db_ == db || 0 == db.length())) {
      db_priv_set = session_priv.db_priv_set_;
    } else {
      const ObPrivMgr &priv_mgr = mgr->priv_mgr_;
      ObOriginalDBKey db_priv_key(session_priv.tenant_id_, session_priv.user_id_, db);
      if (OB_FAIL(priv_mgr.get_db_priv_set(db_priv_key, db_priv_set))) {
        LOG_WARN("get db priv set failed", K(db_priv_key), KR(ret));
      }
    }
    /* load role db privs */
    if (OB_SUCC(ret) && is_oracle_mode) {
      const ObUserInfo *user_info = NULL;
      //bool is_grant_role = false;
      OZ (get_user_info(tenant_id, session_priv.user_id_, user_info), session_priv.user_id_);
      if (OB_SUCC(ret)) {
        if (NULL == user_info) {
          ret = OB_USER_NOT_EXIST;
          LOG_WARN("user info is null", KR(ret), K(session_priv.user_id_));
        }
      }
      if (OB_SUCC(ret)) {
        const ObSEArray<uint64_t, 8> &role_id_array = user_info->get_role_id_array();
        for (int i = 0; OB_SUCC(ret) && i < role_id_array.count(); ++i) {
          const ObUserInfo *role_info = NULL;
          if (OB_FAIL(get_user_info(tenant_id, role_id_array.at(i), role_info))) {
            LOG_WARN("failed to get role ids", KR(ret), K(tenant_id), K(role_id_array.at(i)));
          } else if (NULL == role_info) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("role info is null", KR(ret), K(role_id_array.at(i)));
          } else {
            ObPrivSet db_priv_set_role = OB_PRIV_SET_EMPTY;
            ObOriginalDBKey db_priv_key_role(session_priv.tenant_id_,
                                            role_info->get_user_id(),
                                            db);
            if (OB_FAIL(get_db_priv_set(db_priv_key_role, db_priv_set_role))) {
              LOG_WARN("get db priv set failed", KR(ret), K(db_priv_key_role));
            } else {
              // append db level privilege
              total_db_priv_set_role |= db_priv_set_role;
            }
          }
        }
      }
    }

    if (OB_SUCC(ret) && !is_oracle_mode) {
      ObNeedPriv need_priv(db, "", OB_PRIV_DB_LEVEL, OB_PRIV_SET_EMPTY, false);
      ObNeedPriv collected_privs(db, "", OB_PRIV_DB_LEVEL, OB_PRIV_SET_EMPTY, false);
      bool check_succ = false;
      if (OB_FAIL(collect_priv_in_roles(mgr->priv_mgr_, session_priv, enable_role_id_array, *this, need_priv,
                                        collect_user_db_level_priv_in_roles, collected_privs, check_succ))) {
        LOG_WARN("fail to collect privs in roles", K(ret));
      } else {
        total_db_priv_set_role |= collected_privs.priv_set_;
      }
    }

    if (OB_SUCC(ret)) {
      user_db_priv_set = session_priv.user_priv_set_ | db_priv_set | total_db_priv_set_role;
      if (!OB_TEST_PRIVS(user_db_priv_set, need_priv_set)) {
        ret = OB_ERR_NO_DB_PRIVILEGE;
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::check_db_priv(const ObSessionPrivInfo &session_priv,
                                       const common::ObIArray<uint64_t> &enable_role_id_array,
                              const common::ObString &db,
                              const ObPrivSet need_priv_set)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = session_priv.tenant_id_;
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (!OB_TEST_PRIVS(session_priv.user_priv_set_, need_priv_set)) {
    ObPrivSet user_db_priv_set = 0;
    if (OB_FAIL(check_db_priv(session_priv, enable_role_id_array, db, need_priv_set, user_db_priv_set))) {
      LOG_WARN("No db priv", "tenant_id", session_priv.tenant_id_,
                              "user_id", session_priv.user_id_,
                              K(db), KR(ret));
      if (OB_ERR_NO_DB_PRIVILEGE == ret) {
        LOG_USER_ERROR(OB_ERR_NO_DB_PRIVILEGE, session_priv.user_name_.length(), session_priv.user_name_.ptr(),
                       session_priv.host_name_.length(), session_priv.host_name_.ptr(),
                       db.length(), db.ptr());
      }
    }
  }
  return ret;
}

/* check all needed privileges of object*/
int ObSchemaGetterGuard::check_single_obj_priv(
    const uint64_t tenant_id,
    const uint64_t uid,
    const ObOraNeedPriv &need_priv,
    const ObIArray<uint64_t> &role_id_array)
{
  int ret = OB_SUCCESS;
  uint64_t uid_to_be_check;
  bool exists = false;
  for (int i = OBJ_PRIV_ID_NONE; OB_SUCC(ret) && i < OBJ_PRIV_ID_MAX; i++) {
    OZ (share::ObOraPrivCheck::raw_obj_priv_exists(i, need_priv.obj_privs_, exists));
    if (OB_SUCC(ret) && exists) {
      uid_to_be_check = need_priv.grantee_id_ == common::OB_INVALID_ID ? uid :
                        need_priv.grantee_id_;
      // If column privilege needs to be checked
      if (OBJ_LEVEL_FOR_COL_PRIV == need_priv.obj_level_) {
        // 1. Check sys and table privileges first
        OZX2 (sql::ObOraSysChecker::check_ora_obj_priv(*this,
                                                    tenant_id,
                                                    uid_to_be_check,
                                                    need_priv.db_name_,
                                                    need_priv.obj_id_,
                                                    OBJ_LEVEL_FOR_TAB_PRIV,
                                                    need_priv.obj_type_,
                                                    i,
                                                    need_priv.check_flag_,
                                                    need_priv.owner_id_,
                                                    role_id_array),
            OB_ERR_NO_SYS_PRIVILEGE, OB_TABLE_NOT_EXIST,
            tenant_id, uid_to_be_check, need_priv.db_name_, need_priv.obj_id_,
            OBJ_LEVEL_FOR_TAB_PRIV, need_priv.obj_type_, i, need_priv.check_flag_,
            need_priv.owner_id_, need_priv.grantee_id_);
        // 2. Check column privileges
        if (OB_ERR_NO_SYS_PRIVILEGE == ret || OB_TABLE_NOT_EXIST == ret) {
          bool table_accessible = (OB_ERR_NO_SYS_PRIVILEGE == ret); // check if table is visable to user
          bool column_priv_check_pass = true; // check all columns's privileges
          ret = OB_SUCCESS;
          for (int idx = 0; OB_SUCC(ret) && idx < need_priv.col_id_array_.count(); ++idx) {
            OZX2 (sql::ObOraSysChecker::check_ora_obj_priv(*this,
                                                        tenant_id,
                                                        uid_to_be_check,
                                                        need_priv.db_name_,
                                                        need_priv.obj_id_,
                                                        need_priv.col_id_array_.at(idx),
                                                        need_priv.obj_type_,
                                                        i,
                                                        need_priv.check_flag_,
                                                        need_priv.owner_id_,
                                                        role_id_array),
                OB_TABLE_NOT_EXIST, OB_ERR_NO_SYS_PRIVILEGE,
                tenant_id, uid_to_be_check, need_priv.db_name_, need_priv.obj_id_,
                need_priv.col_id_array_.at(idx), need_priv.obj_type_, i, need_priv.check_flag_,
                need_priv.owner_id_, need_priv.grantee_id_);
            if (OB_TABLE_NOT_EXIST == ret) { // overwrite ret
              column_priv_check_pass = false;
              ret = OB_SUCCESS;
            } else if (OB_ERR_NO_SYS_PRIVILEGE == ret) {
              column_priv_check_pass = false;
              table_accessible = true;
              ret = OB_SUCCESS;
            }
          }
          if (OB_SUCC(ret) && false == column_priv_check_pass) { // overwrite ret
            if (table_accessible) {
              ret = OB_ERR_NO_SYS_PRIVILEGE;
            } else {
              ret = OB_TABLE_NOT_EXIST;
            }
          }
        }
      } else {
        // Other cases
        OZ (sql::ObOraSysChecker::check_ora_obj_priv(*this,
                                                    tenant_id,
                                                    uid_to_be_check,
                                                    need_priv.db_name_,
                                                    need_priv.obj_id_,
                                                    need_priv.obj_level_,
                                                    need_priv.obj_type_,
                                                    i,
                                                    need_priv.check_flag_,
                                                    need_priv.owner_id_,
                                                    role_id_array),
            tenant_id, uid_to_be_check, need_priv.db_name_, need_priv.obj_id_,
            need_priv.obj_level_, need_priv.obj_type_, i, need_priv.check_flag_,
            need_priv.owner_id_, need_priv.grantee_id_);
      }
    }
  }
  return ret;
}

/* check all privileges of stmt */
int ObSchemaGetterGuard::check_ora_priv(
    const uint64_t tenant_id,
    const uint64_t uid,
    const ObStmtOraNeedPrivs &stmt_need_privs,
    const ObIArray<uint64_t> &role_id_array)
{
  int ret = OB_SUCCESS;
  const ObStmtOraNeedPrivs::OraNeedPrivs &need_privs = stmt_need_privs.need_privs_;
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < need_privs.count(); ++i) {
      const ObOraNeedPriv &need_priv = need_privs.at(i);
      if (OB_FAIL(check_single_obj_priv(tenant_id, uid, need_priv, role_id_array))) {
        LOG_WARN("No privilege", "tenant_id", tenant_id,
            "user_id", uid,
            "need_priv", need_priv.obj_privs_,
            "obj id", need_priv.obj_id_,
            KR(ret));//need print priv
      }
    }
  }
  return ret;
}

int ObSchemaGetterGuard::check_priv(const ObSessionPrivInfo &session_priv,
                                    const common::ObIArray<uint64_t> &enable_role_id_array,
                                    const ObStmtNeedPrivs &stmt_need_privs)
{
  int ret = OB_SUCCESS;
  const ObStmtNeedPrivs::NeedPrivs &need_privs = stmt_need_privs.need_privs_;
  uint64_t tenant_id = session_priv.tenant_id_;
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (session_priv.is_valid()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < need_privs.count(); ++i) {
      const ObNeedPriv &need_priv = need_privs.at(i);
      switch (need_priv.priv_level_) {
        case OB_PRIV_USER_LEVEL: {
          if (OB_FAIL(check_user_priv(session_priv,
                                      enable_role_id_array,
                                      need_priv.priv_set_,
                                      OB_PRIV_CHECK_ALL == need_priv.priv_check_type_))) {
            LOG_WARN("No privilege", "tenant_id", session_priv.tenant_id_,
                     "user_id", session_priv.user_id_,
                     "need_priv", need_priv.priv_set_,
                     "user_priv", session_priv.user_priv_set_,
                     KR(ret));//need print priv
          }
          break;
        }
        case OB_PRIV_DB_LEVEL: {
          if (OB_FAIL(check_db_priv(session_priv, enable_role_id_array, need_priv.db_, need_priv.priv_set_))) {
            LOG_WARN("No privilege", "tenant_id", session_priv.tenant_id_,
                "user_id", session_priv.user_id_,
                "need_priv", need_priv.priv_set_,
                "user_priv", session_priv.user_priv_set_,
                KR(ret));//need print priv
          }
          break;
        }
        case OB_PRIV_TABLE_LEVEL: {
          if (OB_PRIV_CHECK_ALL == need_priv.priv_check_type_) {
            if (OB_FAIL(check_single_table_priv(session_priv, enable_role_id_array, need_priv))) {
              LOG_WARN("No privilege", "tenant_id", session_priv.tenant_id_,
                  "user_id", session_priv.user_id_,
                  "need_priv", need_priv.priv_set_,
                  "table", need_priv.table_,
                  "db", need_priv.db_,
                  "user_priv", session_priv.user_priv_set_,
                  KR(ret));//need print priv
            }
          } else if (OB_PRIV_CHECK_ANY == need_priv.priv_check_type_) {
            if (OB_FAIL(check_single_table_priv_or(session_priv, enable_role_id_array, need_priv))) {
              LOG_WARN("No privilege", "tenant_id", session_priv.tenant_id_,
                       "user_id", session_priv.user_id_,
                       "need_priv", need_priv.priv_set_,
                       "table", need_priv.table_,
                       "db", need_priv.db_,
                       "user_priv", session_priv.user_priv_set_,
                       KR(ret));
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Privilege checking of other not use this function yet", KR(ret));
          }
          break;
        }
        case OB_PRIV_ROUTINE_LEVEL: {
          if (OB_ISNULL(this)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("schema guard is null", K(ret));
          } else if (!sql::ObSchemaChecker::enable_mysql_pl_priv_check(tenant_id, *this)) {
            //do nothing
          } else if (OB_FAIL(check_routine_priv(session_priv, enable_role_id_array, need_priv))) {
            LOG_WARN("No privilege", "tenant_id", session_priv.tenant_id_,
                "user_id", session_priv.user_id_,
                "need_priv", need_priv.priv_set_,
                "table", need_priv.table_,
                "db", need_priv.db_,
                "user_priv", session_priv.user_priv_set_,
                KR(ret));//need print priv
          }
          break;
        }
        case OB_PRIV_DB_ACCESS_LEVEL: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Privilege checking of database access should not use this function", KR(ret));
          break;
        }
        default: {
          break;
        }
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

int ObSchemaGetterGuard::check_priv_db_or_(const ObSessionPrivInfo &session_priv,
                                           const common::ObIArray<uint64_t> &enable_role_id_array,
                                           const ObNeedPriv &need_priv,
                                           const ObPrivMgr &priv_mgr,
                                           const uint64_t tenant_id,
                                           const uint64_t user_id,
                                           bool& pass) {
  int ret = OB_SUCCESS;
  int64_t total_db_priv_set_role = 0;
  ObString db = need_priv.db_;
  ObPrivSet db_priv_set = 0;
  if (session_priv.db_.length() != 0 && (session_priv.db_ == db || 0 == db.length())) {
    db_priv_set = session_priv.db_priv_set_;
  } else {
    ObOriginalDBKey db_priv_key(tenant_id, user_id, db);
    if (OB_FAIL(priv_mgr.get_db_priv_set(db_priv_key, db_priv_set))) {
      LOG_WARN("get db priv set failed", K(db_priv_key), KR(ret));
    }
  }

  if (OB_SUCC(ret)) {
    pass = OB_PRIV_HAS_ANY(db_priv_set, need_priv.priv_set_);
  }

  /* load role db privs */
  if (OB_SUCC(ret) && !pass) {
    //2. fetch roles privs
    bool check_succ = false;
    ObNeedPriv collected_privs(db, "", OB_PRIV_DB_LEVEL, OB_PRIV_SET_EMPTY, false);
    if (OB_FAIL(collect_priv_in_roles(priv_mgr, session_priv, enable_role_id_array, *this, need_priv,
                                      collect_user_db_level_priv_in_roles, collected_privs, check_succ))) {
      LOG_WARN("fail to collect priv in roles", K(ret));
    } else {
      db_priv_set |= collected_privs.priv_set_;
    }
  }

  if (OB_SUCC(ret)) {
    pass = OB_PRIV_HAS_ANY(db_priv_set, need_priv.priv_set_);
  }

  return ret;
}

int ObSchemaGetterGuard::check_priv_table_or_(const ObSessionPrivInfo &session_priv,
                                              const common::ObIArray<uint64_t> &enable_role_id_array,
                                              const ObNeedPriv &need_priv,
                                              const ObPrivMgr &priv_mgr,
                                              const uint64_t tenant_id,
                                              const uint64_t user_id,
                                              bool& pass) {
  int ret = OB_SUCCESS;
  //1. fetch table priv
  const ObTablePriv *table_priv = NULL;
  ObPrivSet table_priv_set = 0;
  ObTablePrivSortKey table_priv_key(tenant_id,
                                    user_id,
                                    need_priv.db_,
                                    need_priv.table_);
  if (OB_FAIL(priv_mgr.get_table_priv(table_priv_key, table_priv))) {
    LOG_WARN("get table priv failed", KR(ret), K(table_priv_key));
  } else if (NULL != table_priv) {
    table_priv_set = table_priv->get_priv_set();
  }

  if (OB_SUCC(ret)) {
    pass = OB_PRIV_HAS_ANY(table_priv_set, need_priv.priv_set_);
  }

  if (OB_SUCC(ret) && !pass) {
    //2. fetch roles privs
    bool check_succ = false;
    ObNeedPriv collected_privs(need_priv.db_, need_priv.table_, OB_PRIV_TABLE_LEVEL, OB_PRIV_SET_EMPTY, false);
    if (OB_FAIL(collect_priv_in_roles(priv_mgr, session_priv, enable_role_id_array, *this, need_priv,
                                      collect_user_db_tb_level_priv_in_roles, collected_privs, check_succ))) {
      LOG_WARN("fail to collect priv in roles", K(ret));
    } else {
      table_priv_set |= collected_privs.priv_set_;
    }
  }

  //3. check privs
  if (OB_SUCC(ret)) {
    pass = OB_PRIV_HAS_ANY(table_priv_set, need_priv.priv_set_);
  }

  return ret;
}

int ObSchemaGetterGuard::collect_all_priv_for_column(const ObSessionPrivInfo &session_priv,
                                                     const common::ObIArray<uint64_t> &enable_role_id_array,
                                                     const ObString &db_name,
                                                     const ObString &table_name,
                                                     const ObString &column_name,
                                                     ObPrivSet &col_priv_set)
{
  int ret = OB_SUCCESS;
  ObColumnPrivSortKey sort_key(session_priv.tenant_id_,
                               session_priv.user_id_,
                               db_name,
                               table_name,
                               column_name);
  col_priv_set = OB_PRIV_SET_EMPTY;
  if (OB_FAIL(get_column_priv_set(sort_key, col_priv_set))) {
    LOG_WARN("get column priv failed", K(ret), K(sort_key));
  } else {
    bool pass = false;
    uint64_t tenant_id = session_priv.tenant_id_;
    ObNeedPriv need_priv(db_name, table_name, OB_PRIV_TABLE_LEVEL, OB_PRIV_COLUMN_ACC, false);
    ObNeedPriv collected_privs(db_name, table_name, OB_PRIV_TABLE_LEVEL, OB_PRIV_SET_EMPTY, false);
    const ObSchemaMgr *mgr = NULL;
    if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
      LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
    } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
      LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
    } else if (OB_ISNULL(mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mgr is NULL", KR(ret), K(tenant_id));
    } else if (OB_FAIL(need_priv.columns_.push_back(column_name))) {
      LOG_WARN("fail to push back", K(ret));
    } else if (OB_FAIL(collect_priv_in_roles(mgr->priv_mgr_, session_priv, enable_role_id_array, *this, need_priv,
                                             collect_column_level_all_priv_in_roles, collected_privs, pass))) {
      LOG_WARN("fail to collect priv in roles", K(ret));
    } else {
      col_priv_set |= collected_privs.priv_set_;
    }
  }

  return ret;
}

int ObSchemaGetterGuard::check_priv_any_column_priv(const ObSessionPrivInfo &session_priv,
                                                    const common::ObIArray<uint64_t> &enable_role_id_array,
                                                    const ObString &db_name,
                                                    const ObString &table_name,
                                                    bool &pass)
{
  int ret = OB_SUCCESS;
  pass = false;
  ObArray<const ObColumnPriv*> column_privs;
  if (OB_FAIL(get_column_priv_in_table(
                ObTablePrivSortKey(session_priv.tenant_id_, session_priv.user_id_, db_name, table_name),
                column_privs))) {
    LOG_WARN("get column priv in table failed", K(ret));
  } else if (!column_privs.empty()) {
    pass = true;
  } else {
    uint64_t tenant_id = session_priv.tenant_id_;
    ObNeedPriv need_priv(db_name, table_name, OB_PRIV_TABLE_LEVEL, OB_PRIV_COLUMN_ACC, false);
    ObNeedPriv collected_privs(db_name, table_name, OB_PRIV_TABLE_LEVEL, OB_PRIV_SET_EMPTY, false);
    need_priv.check_any_column_priv_ = true;
    const ObSchemaMgr *mgr = NULL;
    if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
      LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
    } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
      LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
    } else if (OB_ISNULL(mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mgr is NULL", KR(ret), K(tenant_id));
    } else if (OB_FAIL(need_priv.columns_.push_back(""))) {
      LOG_WARN("fail to push back", K(ret));
    } else if (OB_FAIL(collect_priv_in_roles(mgr->priv_mgr_, session_priv, enable_role_id_array, *this, need_priv,
                                             collect_any_column_level_priv_in_roles, collected_privs, pass))) {
      LOG_WARN("fail to collect priv in roles", K(ret));
    }
  }
  return ret;
}

int ObSchemaGetterGuard::check_priv_or(const ObSessionPrivInfo &session_priv,
                                       const common::ObIArray<uint64_t> &enable_role_id_array,
                                       const ObStmtNeedPrivs &stmt_need_privs)
{
  int ret = OB_SUCCESS;

  const ObStmtNeedPrivs::NeedPrivs &need_privs = stmt_need_privs.need_privs_;
  bool pass = false;
  ObPrivLevel max_priv_level = OB_PRIV_INVALID_LEVEL;
  uint64_t tenant_id = session_priv.tenant_id_;
  uint64_t user_id = session_priv.user_id_;
  const ObSchemaMgr *mgr = NULL;
  if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mgr is NULL", KR(ret), K(tenant_id));
  } else if (session_priv.is_valid()) {
    const ObPrivMgr &priv_mgr = mgr->priv_mgr_;
    for (int64_t i = 0; !pass && OB_SUCC(ret) && i < need_privs.count(); ++i) {
      const ObNeedPriv &need_priv = need_privs.at(i);
      if (need_priv.priv_level_ > max_priv_level) {
        max_priv_level = need_priv.priv_level_;
      }
      switch (need_priv.priv_level_) {
        case OB_PRIV_USER_LEVEL: {
          pass = OB_PRIV_HAS_ANY(session_priv.user_priv_set_, need_priv.priv_set_);
          if (!pass) {
            bool check_succ = false;
            ObNeedPriv collected_privs("", "", OB_PRIV_USER_LEVEL, OB_PRIV_SET_EMPTY, false);
            if (OB_FAIL(collect_priv_in_roles(priv_mgr, session_priv, enable_role_id_array, *this, need_priv,
                                              collect_user_level_priv_in_roles, collected_privs, check_succ))) {
              LOG_WARN("fail to collect priv in roles", K(ret));
            } else {
              ObPrivSet total_set = (session_priv.user_priv_set_ | collected_privs.priv_set_);
              pass = OB_PRIV_HAS_ANY(total_set, need_priv.priv_set_);
            }
          }
          break;
        }
        case OB_PRIV_DB_LEVEL: {
          if (OB_FAIL(check_priv_db_or_(session_priv, enable_role_id_array, need_priv, priv_mgr, tenant_id, user_id, pass))) {
            LOG_WARN("fail to check priv db only", KR(ret), K(tenant_id), K(user_id), K(need_priv.db_));
          }
          break;
        }
        case OB_PRIV_TABLE_LEVEL: {
          if (OB_FAIL(check_priv_table_or_(session_priv, enable_role_id_array, need_priv, priv_mgr, tenant_id, user_id, pass))) {
            LOG_WARN("fail to check priv table only", KR(ret), K(tenant_id), K(user_id), K(need_priv.db_), K(need_priv.table_));
          }
          break;
        }
        case OB_PRIV_ROUTINE_LEVEL: {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("should not reach here", KR(ret));
          break;
        }
        case OB_PRIV_DB_ACCESS_LEVEL: {
          //this should not occur
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("should not reach here", KR(ret));
          break;
        }
        default: {
          break;
        }
      }
    }
    if (!pass) {
      //User log is printed outside
      switch (max_priv_level) {
      case OB_PRIV_USER_LEVEL: {
        ret = OB_ERR_NO_PRIVILEGE;
        break;
      }
      case OB_PRIV_DB_LEVEL: {
        ret = OB_ERR_NO_DB_PRIVILEGE;
        break;
      }
      case OB_PRIV_TABLE_LEVEL: {
        ret = OB_ERR_NO_TABLE_PRIVILEGE;
        break;
      }
      case OB_PRIV_ROUTINE_LEVEL: {
        ret = OB_ERR_NO_ROUTINE_PRIVILEGE;
        break;
      }
      default: {
        //this should not occur
        ret = OB_INVALID_ARGUMENT;
        break;
      }
      LOG_WARN("Or-ed privilege check not passed",
               "tenant id", session_priv.tenant_id_, "user id", session_priv.user_id_);
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

} //end of namespace schema
} //end of namespace share
} //end of namespace oceanbase
