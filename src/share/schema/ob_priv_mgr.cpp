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
#include "ob_priv_mgr.h"

#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <algorithm>
#include "lib/utility/utility.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/encrypt/ob_encrypted_helper.h"
#include "common/object/ob_obj_type.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/schema/ob_schema_utils.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
using namespace std;
using namespace common;
using namespace common::hash;

const char *ObPrivMgr::priv_names_[] = {
    "INVALID",
    "ALTER",
    "CREATE",
    "CREATE USER",
    "DELETE",
    "DROP",
    "GRANT",
    "INSERT",
    "UPDATE",
    "SELECT",
    "INDEX",
    "CREATE VIEW",
    "SHOW VIEW",
    "SHOW DB",
    "SUPER",
    "PROCESS",
    "BOOTSTRAP",
    "CREATE SYNONYM",
    "AUDIT",
    "COMMENT",
    "LOCK",
    "RENAME",
    "REFERENCES",
    "EXECUTE",
    "FLASHBACK",
    "READ",
    "WRITE",
    "FILE",
    "ALTER TENANT",
    "ALTER SYSTEM",
    "CREATE RESOURCE POOL",
    "CREATE RESOURCE UNIT",
    "DEBUG",
    "REPLICATION SLAVE",
    "REPLICATION CLIENT",
    "DROP DATABASE LINK",
    "CREATE DATABASE LINK",
};

ObPrivMgr::ObPrivMgr()
  : local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
    allocator_(local_allocator_),
    db_privs_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_PRIV_DB_PRIVS, ObCtxIds::SCHEMA_SERVICE)),
    table_privs_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_PRIV_TABLE_PRIVS, ObCtxIds::SCHEMA_SERVICE)),
    table_priv_map_(SET_USE_500(ObModIds::OB_SCHEMA_PRIV_TABLE_PRIV_MAP, ObCtxIds::SCHEMA_SERVICE)),
    obj_privs_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_PRIV_OBJ_PRIVS, ObCtxIds::SCHEMA_SERVICE)),
    obj_priv_map_(SET_USE_500(ObModIds::OB_SCHEMA_PRIV_OBJ_PRIV_MAP, ObCtxIds::SCHEMA_SERVICE)),
    sys_privs_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_PRIV_SYS_PRIVS, ObCtxIds::SCHEMA_SERVICE))
{
  static_assert(ARRAYSIZEOF(ObPrivMgr::priv_names_) == OB_PRIV_MAX_SHIFT_PLUS_ONE,
                "incomplete array priv_names_");
}

ObPrivMgr::ObPrivMgr(ObIAllocator &allocator)
  : local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
    allocator_(allocator),
    db_privs_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_PRIV_DB_PRIVS, ObCtxIds::SCHEMA_SERVICE)),
    table_privs_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_PRIV_TABLE_PRIVS, ObCtxIds::SCHEMA_SERVICE)),
    table_priv_map_(SET_USE_500(ObModIds::OB_SCHEMA_PRIV_TABLE_PRIV_MAP, ObCtxIds::SCHEMA_SERVICE)),
    obj_privs_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_PRIV_OBJ_PRIVS, ObCtxIds::SCHEMA_SERVICE)),
    obj_priv_map_(SET_USE_500(ObModIds::OB_SCHEMA_PRIV_OBJ_PRIV_MAP, ObCtxIds::SCHEMA_SERVICE)),
    sys_privs_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_PRIV_SYS_PRIVS, ObCtxIds::SCHEMA_SERVICE))
{}

ObPrivMgr::~ObPrivMgr()
{
}

int ObPrivMgr::init()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(table_priv_map_.init())) {
    LOG_WARN("init table priv map failed", K(ret));
  } else if (OB_FAIL(obj_priv_map_.init())) {
    LOG_WARN("init obj priv map failed", K(ret));
  }

  return ret;
}

void ObPrivMgr::reset()
{
  // reset will not release memory for vector, use clear()
  db_privs_.clear();
  table_privs_.clear();
  sys_privs_.clear();
  table_priv_map_.clear();
  obj_privs_.clear();
  obj_priv_map_.clear();
}

int ObPrivMgr::assign(const ObPrivMgr &other)
{
  int ret = OB_SUCCESS;

  if (this != &other) {
    reset();
    #define ASSIGN_FIELD(x)                        \
      if (OB_SUCC(ret)) {                          \
        if (OB_FAIL(x.assign(other.x))) {          \
          LOG_WARN("assign " #x "failed", K(ret)); \
        }                                          \
      }
    ASSIGN_FIELD(db_privs_);
    ASSIGN_FIELD(table_privs_);
    ASSIGN_FIELD(sys_privs_);
    ASSIGN_FIELD(table_priv_map_);
    ASSIGN_FIELD(obj_privs_);
    ASSIGN_FIELD(obj_priv_map_);
    #undef ASSIGN_FIELD
  }

  return ret;
}

int ObPrivMgr::deep_copy(const ObPrivMgr &other)
{
  int ret = OB_SUCCESS;

  if (this != &other) {
    reset();
    for (DBPrivIter iter = other.db_privs_.begin();
       OB_SUCC(ret) && iter != other.db_privs_.end(); iter++) {
      ObDBPriv *db_priv = *iter;
      if (OB_ISNULL(db_priv)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(db_priv), K(ret));
      } else if (OB_FAIL(add_db_priv(*db_priv))) {
        LOG_WARN("add db priv failed", K(*db_priv), K(ret));
      }
    }
    for (TablePrivIter iter = other.table_privs_.begin();
        OB_SUCC(ret) && iter != other.table_privs_.end(); iter++) {
      ObTablePriv *table_priv = *iter;
      if (OB_ISNULL(table_priv)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(table_priv), K(ret));
      } else if (OB_FAIL(add_table_priv(*table_priv))) {
        LOG_WARN("add table priv failed", K(*table_priv), K(ret));
      }
    }
    for (SysPrivIter iter = other.sys_privs_.begin();
       OB_SUCC(ret) && iter != other.sys_privs_.end(); iter++) {
      ObSysPriv *sys_priv = *iter;
      if (OB_ISNULL(sys_priv)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(sys_priv), K(ret));
      } else if (OB_FAIL(add_sys_priv(*sys_priv))) {
        LOG_WARN("add sys priv failed", K(*sys_priv), K(ret));
      }
    }
    for (ObjPrivIter iter = other.obj_privs_.begin();
        OB_SUCC(ret) && iter != other.obj_privs_.end(); iter++) {
      ObObjPriv *obj_priv = *iter;
      if (OB_ISNULL(obj_priv)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(obj_priv), K(ret));
      } else if (OB_FAIL(add_obj_priv(*obj_priv))) {
        LOG_WARN("add obj priv failed", K(*obj_priv), K(ret));
      }
    }
  }

  return ret;
}

const char *ObPrivMgr::get_priv_name(int64_t priv_shift) {
  const char *cret = NULL;
  if (priv_shift > OB_PRIV_INVALID_SHIFT && priv_shift < OB_PRIV_MAX_SHIFT_PLUS_ONE) {
    cret = priv_names_[priv_shift];
  }
  return cret;
}

const char *ObPrivMgr::get_first_priv_name(ObPrivSet priv_set) {
  const char *cret = NULL;
  for (int64_t shift = 0; NULL == cret && shift < OB_PRIV_MAX_SHIFT_PLUS_ONE; ++shift) {
    if (OB_PRIV_HAS_ANY(priv_set, OB_PRIV_GET_TYPE(shift))) {
      cret = get_priv_name(shift);
    }
  }
  return cret;
}

int ObPrivMgr::get_priv_schema_count(int64_t &priv_schema_count) const
{
  int ret = OB_SUCCESS;
  priv_schema_count = table_privs_.size() + db_privs_.size() 
                      + sys_privs_.size() + obj_privs_.size();
  return ret;
}

int ObPrivMgr::add_db_privs(const common::ObIArray<ObDBPriv> &db_privs)
{
  int ret = OB_SUCCESS;

  FOREACH_CNT_X(db_priv, db_privs, OB_SUCC(ret)) {
    if (OB_FAIL(add_db_priv(*db_priv))) {
      LOG_WARN("add db priv failed", K(ret), K(*db_priv));
    }
  }

  return ret;
}

int ObPrivMgr::del_db_privs(const common::ObIArray<ObOriginalDBKey> &db_priv_keys)
{
  int ret = OB_SUCCESS;

  FOREACH_CNT_X(db_priv_key, db_priv_keys, OB_SUCC(ret)) {
    if (OB_FAIL(del_db_priv(*db_priv_key))) {
      LOG_WARN("del db priv failed", K(ret), K(*db_priv_key));
    }
  }
  return ret;
}

int ObPrivMgr::add_db_priv(const ObDBPriv &db_priv)
{
  int ret = OB_SUCCESS;

  ObDBPriv *new_db_priv = NULL;
  if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator_,
                                          db_priv,
                                          new_db_priv))) {
    LOG_WARN("alloc scheam failed", K(ret));
  } else if (OB_ISNULL(new_db_priv)){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(new_db_priv));
  } else {
    //cmp sort value
    new_db_priv->set_sort(get_sort(new_db_priv->get_database_name_str()));
    DBPrivIter iter = NULL;
    if (OB_FAIL(get_db_priv_iter(new_db_priv->get_original_key(), iter))) {
      LOG_WARN("get db priv iter failed", K(ret), K(new_db_priv->get_original_key()));
    } else if (NULL == iter) {
      DBPrivIter insert_pos = NULL;
      if (OB_FAIL(db_privs_.insert(new_db_priv, insert_pos, ObDBPriv::cmp))) {
        LOG_WARN("Insert db_priv error", K(ret));
      }
    } else {
      ObDBPriv *old_db_priv = *iter;
      if (OB_ISNULL(old_db_priv)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(old_db_priv));
      } else {
        MEMCPY(iter, &new_db_priv, sizeof(new_db_priv));
      }
    }
  }

  return ret;
}

int ObPrivMgr::del_db_priv(const ObOriginalDBKey &db_priv_key)
{
  int ret = OB_SUCCESS;

  ObDBPriv *db_priv = NULL;
  DBPrivIter target_iter = NULL;
  if (OB_FAIL(get_db_priv_iter(db_priv_key, target_iter))) {
    LOG_WARN("get db priv iter failed", K(ret), K(db_priv_key));
  } else if (OB_ISNULL(target_iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(target_iter));
  } else if (OB_FAIL(db_privs_.remove(target_iter))) {
    LOG_WARN("remove failed", K(ret));
  } else if (OB_ISNULL(db_priv = *target_iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(db_priv));
  }

  return ret;
}

int ObPrivMgr::get_db_priv_iter(const ObOriginalDBKey &db_priv_key,
                                DBPrivIter &target_db_priv_iter) const
{
  int ret = OB_SUCCESS;
  target_db_priv_iter = NULL;

  ObTenantUserId tenant_user_id(db_priv_key.tenant_id_, db_priv_key.user_id_);
  DBPrivIter tenant_db_priv_begin =
      db_privs_.lower_bound(tenant_user_id, ObDBPriv::cmp_tenant_user_id);
  bool is_stop = false;
  for (DBPrivIter db_priv_iter = tenant_db_priv_begin;
      OB_SUCC(ret) && db_priv_iter != db_privs_.end() && !is_stop;
      ++db_priv_iter) {
    const ObDBPriv *db_priv = NULL;
    if (OB_ISNULL(db_priv = *db_priv_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(db_priv), K(ret));
    } else if (db_priv_key.tenant_id_ != db_priv->get_tenant_id()
               || db_priv_key.user_id_ != db_priv->get_user_id()) {
      is_stop = true;
    } else if (db_priv_key != db_priv->get_original_key()) {
      // do-nothing
    } else {
      target_db_priv_iter = db_priv_iter;
      is_stop = true;
    }
  }

  return ret;
}

int ObPrivMgr::get_db_priv(const ObOriginalDBKey &db_priv_key,
                           const ObDBPriv *&db_priv,
                           bool db_is_pattern/*default:false*/) const
{
  int ret = OB_SUCCESS;
  db_priv = NULL;

  const uint64_t tenant_id = db_priv_key.tenant_id_;
  const uint64_t user_id = db_priv_key.user_id_;
  const ObString &db = db_priv_key.db_;
  ObTenantUserId tenant_user_id(tenant_id, user_id);
  ConstDBPrivIter tenant_db_priv_begin =
      db_privs_.lower_bound(tenant_user_id, ObDBPriv::cmp_tenant_user_id);
  bool is_stop = false;
  for (ConstDBPrivIter db_priv_iter = tenant_db_priv_begin;
      OB_SUCC(ret) && db_priv_iter != db_privs_.end() && !is_stop;
      ++db_priv_iter) {
    const ObDBPriv *tmp_db_priv = NULL;
    if (OB_ISNULL(tmp_db_priv = *db_priv_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(tmp_db_priv), K(ret));
    } else if (tenant_id != tmp_db_priv->get_tenant_id()
               || user_id != tmp_db_priv->get_user_id()) {
      is_stop = true;
    } else if (0 != wild_compare(db, tmp_db_priv->get_database_name_str(),
                                 db_is_pattern)) {
      // do-nothing
    } else {
      db_priv = tmp_db_priv;
      is_stop = true;
    }
  }
  //anonymous user
  if (OB_SUCC(ret)) {
    if (!db_is_pattern && !is_empty_user(user_id) && NULL == db_priv) {
      ObOriginalDBKey new_db_priv_key(tenant_id, OB_EMPTY_USER_ID, db);
      if (OB_FAIL(get_db_priv(new_db_priv_key, db_priv, false))) {
        LOG_WARN("get db priv failed", K(ret), K(new_db_priv_key));
      }
    }
  }

  return ret;
}

int ObPrivMgr::get_db_priv_set(const ObOriginalDBKey &db_priv_key,
                               ObPrivSet &priv_set,
                               bool is_pattern) const
{
  int ret = OB_SUCCESS;
  priv_set = OB_PRIV_SET_EMPTY;

  const ObString &db = db_priv_key.db_;
  const ObDBPriv *db_priv = NULL;
  if (0 == db.case_compare(OB_INFORMATION_SCHEMA_NAME)) {
    //users have 'select' privilege for information_schema database
    priv_set = OB_PRIV_SHOW_VIEW | OB_PRIV_SELECT;
  } else if (OB_FAIL(get_db_priv(db_priv_key, db_priv, is_pattern))) {
    LOG_WARN("get db priv failed", K(ret), K(db_priv_key));
  } else if (NULL != db_priv) {
    priv_set = db_priv->get_priv_set();
  }

  return ret;
}

int ObPrivMgr::add_sys_privs(const common::ObIArray<ObSysPriv> &sys_privs)
{
  int ret = OB_SUCCESS;

  FOREACH_CNT_X(sys_priv, sys_privs, OB_SUCC(ret)) {
    if (OB_FAIL(add_sys_priv(*sys_priv))) {
      LOG_WARN("add sys priv failed", K(ret), K(*sys_priv));
    }
  }

  return ret;
}

int ObPrivMgr::del_sys_privs(const common::ObIArray<ObSysPrivKey> &sys_priv_keys)
{
  int ret = OB_SUCCESS;

  FOREACH_CNT_X(sys_priv_key, sys_priv_keys, OB_SUCC(ret)) {
    if (OB_FAIL(del_sys_priv(*sys_priv_key))) {
      LOG_WARN("del sys priv failed", K(ret), K(*sys_priv_key));
    }
  }

  return ret;
}

int ObPrivMgr::add_sys_priv(const ObSysPriv &sys_priv)
{
  int ret = OB_SUCCESS;

  ObSysPriv *new_sys_priv = NULL;
  if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator_,
                                          sys_priv,
                                          new_sys_priv))) {
    LOG_WARN("alloc scheam failed", K(ret));
  } else if (OB_ISNULL(new_sys_priv)){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(new_sys_priv));
  } else {
    //cmp sort value
    //new_sys_priv->set_sort(get_sort(new_sys_priv->get_database_name_str()));
    SysPrivIter iter = NULL;
    if (OB_FAIL(get_sys_priv_iter(new_sys_priv->get_key(), iter))) {
      LOG_WARN("get sys priv iter failed", K(ret), K(new_sys_priv->get_key()));
    } else if (NULL == iter) {
      SysPrivIter insert_pos = NULL;
      if (OB_FAIL(sys_privs_.insert(new_sys_priv, insert_pos, ObSysPriv::cmp))) {
        LOG_WARN("Insert sys_priv error", K(ret));
      }
    } else {
      ObSysPriv *old_sys_priv = *iter;
      if (OB_ISNULL(old_sys_priv)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(old_sys_priv));
      } else {
        MEMCPY(iter, &new_sys_priv, sizeof(new_sys_priv));
      }
    }
  }

  return ret;
}

int ObPrivMgr::del_sys_priv(const ObSysPrivKey &sys_priv_key)
{
  int ret = OB_SUCCESS;

  ObSysPriv *sys_priv = NULL;
  SysPrivIter target_iter = NULL;
  if (OB_FAIL(get_sys_priv_iter(sys_priv_key, target_iter))) {
    LOG_WARN("get sys priv iter failed", K(ret), K(sys_priv_key));
  } else if (OB_ISNULL(target_iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(target_iter));
  } else if (OB_FAIL(sys_privs_.remove(target_iter))) {
    LOG_WARN("remove failed", K(ret));
  } else if (OB_ISNULL(sys_priv = *target_iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(sys_priv));
  }

  return ret;
}

int ObPrivMgr::get_sys_priv_iter(const ObSysPrivKey &sys_priv_key,
                                 SysPrivIter &target_sys_priv_iter) const
{
  int ret = OB_SUCCESS;
  bool is_stop = false;
  SysPrivIter sys_priv_begin;

  ObTenantUserId tenant_user_id(sys_priv_key.tenant_id_, sys_priv_key.grantee_id_);
  //DBPrivIter tenant_db_priv_begin =
  //    db_privs_.lower_bound(tenant_user_id, ObDBPriv::cmp_tenant_user_id);

  sys_priv_begin = sys_privs_.lower_bound(tenant_user_id, ObSysPriv::cmp_tenant_grantee_id);
  for (SysPrivIter sys_priv_iter = sys_priv_begin;
      OB_SUCC(ret) && sys_priv_iter != NULL &&
      sys_priv_iter != sys_privs_.end() && !is_stop;
      ++sys_priv_iter) {
    const ObSysPriv *sys_priv = NULL;
    if (OB_ISNULL(sys_priv = *sys_priv_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(sys_priv), K(ret));
    } else if (sys_priv_key != sys_priv->get_key()) {
      is_stop = true;
    } else {
      target_sys_priv_iter = sys_priv_iter;
      is_stop = true;
    }
  }
  
  return ret;
}

int ObPrivMgr::get_sys_priv(const ObSysPrivKey &key,
                            const ObSysPriv *&sys_priv) const
{
  int ret = OB_SUCCESS;
  
  sys_priv = NULL;
  SysPrivIter target_iter = NULL;
  if (OB_FAIL(get_sys_priv_iter(key, target_iter))) {
    LOG_WARN("get sys priv iter failed", K(ret), K(key));
  } else if (OB_ISNULL(target_iter)) {
    LOG_INFO("get sys priv return NULL", K(key));
  } else {
    const ObSysPriv *tmp_priv = NULL;
    if (OB_ISNULL(tmp_priv = *target_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(tmp_priv), K(ret));
    } else if (key == (tmp_priv->get_key())) {
      sys_priv = tmp_priv;
    }
  }
  
  return ret;
}

int ObPrivMgr::add_table_privs(const common::ObIArray<ObTablePriv> &table_privs)
{
  int ret = OB_SUCCESS;

  FOREACH_CNT_X(table_priv, table_privs, OB_SUCC(ret)) {
    if (OB_FAIL(add_table_priv(*table_priv))) {
      LOG_WARN("add table priv failed", K(ret), K(*table_priv));
    }
  }

  return ret;
}

int ObPrivMgr::del_table_privs(const common::ObIArray<ObTablePrivSortKey> &table_priv_keys)
{
  int ret = OB_SUCCESS;

  FOREACH_CNT_X(table_priv_key, table_priv_keys, OB_SUCC(ret)) {
    if (OB_FAIL(del_table_priv(*table_priv_key))) {
      LOG_WARN("del table priv failed", K(ret), K(*table_priv_key));
    }
  }

  return ret;
}

int ObPrivMgr::add_table_priv(const ObTablePriv &table_priv)
{
  int ret = OB_SUCCESS;

  ObTablePriv *new_table_priv = NULL;
  TablePrivIter iter = NULL;
  ObTablePriv *replaced_table_priv = NULL;

  if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator_,
                                          table_priv,
                                          new_table_priv))) {
    LOG_WARN("alloc schema failed", K(ret));
  } else if (OB_ISNULL(new_table_priv)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(new_table_priv));
  } else if (OB_FAIL(table_privs_.replace(new_table_priv,
                                          iter,
                                          ObTablePriv::cmp,
                                          ObTablePriv::equal,
                                          replaced_table_priv))) {
      LOG_WARN("Failed to put table_priv into table_priv vector", K(ret));
  } else {
    int hash_ret = table_priv_map_.set_refactored(new_table_priv->get_sort_key(), new_table_priv, 1);
    if (OB_SUCCESS != hash_ret && OB_HASH_EXIST != hash_ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Failed to build table_priv hashmap",
               "table_priv_key", new_table_priv->get_sort_key(),
               K(ret), K(hash_ret));
    }
  }

  // ignore ret
  if (table_privs_.count() != table_priv_map_.item_count()) {
    LOG_WARN("table priv is non-consistent between map and vector",
             "table_privs vector count", table_privs_.count(),
             "table_privs map size", table_priv_map_.item_count());
  }

  return ret;
}

int ObPrivMgr::add_obj_privs(const common::ObIArray<ObObjPriv> &obj_privs)
{
  int ret = OB_SUCCESS;

  FOREACH_CNT_X(obj_priv, obj_privs, OB_SUCC(ret)) {
    if (OB_FAIL(add_obj_priv(*obj_priv))) {
      LOG_WARN("add obj priv failed", K(ret), K(*obj_priv));
    }
  }

  return ret;
}

int ObPrivMgr::add_obj_priv(const ObObjPriv &obj_priv)
{
  int ret = OB_SUCCESS;

  ObObjPriv *new_obj_priv = NULL;
  ObjPrivIter iter = NULL;
  ObObjPriv *replaced_obj_priv = NULL;

  if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator_,
      obj_priv, new_obj_priv))) {
    LOG_WARN("alloc schema failed", K(ret));
  } else if (OB_ISNULL(new_obj_priv)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(new_obj_priv));
  } else if (OB_FAIL(obj_privs_.replace(new_obj_priv,
                                        iter,
                                        ObObjPriv::cmp,
                                        ObObjPriv::equal,
                                        replaced_obj_priv))) {
      LOG_WARN("Failed to put obj_priv into obj_priv vector", K(ret));
  } else {
    int hash_ret = obj_priv_map_.set_refactored(new_obj_priv->get_sort_key(), new_obj_priv, 1);
    if (OB_SUCCESS != hash_ret && OB_HASH_EXIST != hash_ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Failed to build obj_priv hashmap",
               "obj_priv_key", new_obj_priv->get_sort_key(),
               K(ret), K(hash_ret));
    }
  }

  // ignore ret
  if (obj_privs_.count() != obj_priv_map_.item_count()) {
    LOG_WARN("obj priv is non-consistent between map and vector",
             "obj_privs vector count", obj_privs_.count(),
             "obj_privs map size", obj_priv_map_.item_count());
  }

  return ret;
}

int ObPrivMgr::del_table_priv(const ObTablePrivSortKey &table_priv_key)
{
  int ret = OB_SUCCESS;

  ObTablePriv *table_priv = NULL;
  if (OB_FAIL(table_privs_.remove_if(table_priv_key,
          ObTablePriv::cmp_sort_key,
          ObTablePriv::equal_sort_key,
          table_priv))) {
    LOG_WARN("Fail to remove table priv",K(table_priv_key), K(ret));
  } else if (OB_ISNULL(table_priv)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Removed table_priv return NULL", K(table_priv));
  } else {
    int hash_ret = table_priv_map_.erase_refactored(table_priv_key);
    if (OB_SUCCESS != hash_ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Failed to delete table priv from table priv map", K(ret), K(hash_ret));
    }
  }
  if (table_privs_.count() != table_priv_map_.item_count()) {
    LOG_WARN("table priv is non-consistent between map and vector",
             "table_privs vector count", table_privs_.count(),
             "table_privs map size", table_priv_map_.item_count());
  }
  return ret;
}

int ObPrivMgr::del_obj_priv(const ObObjPrivSortKey &obj_priv_key)
{
  int ret = OB_SUCCESS;

  ObObjPriv *obj_priv = NULL;
  if (OB_FAIL(obj_privs_.remove_if(obj_priv_key,
          ObObjPriv::cmp_sort_key,
          ObObjPriv::equal_sort_key,
          obj_priv))) {
    LOG_WARN("Fail to remove obj priv",K(obj_priv_key), K(ret));
  } else if (OB_ISNULL(obj_priv)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Removed obj_priv return NULL", K(obj_priv));
  } else {
    int hash_ret = obj_priv_map_.erase_refactored(obj_priv_key);
    if (OB_SUCCESS != hash_ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Failed to delete obj priv from obj priv map", K(ret), K(hash_ret));
    }
  }
  if (obj_privs_.count() != obj_priv_map_.item_count()) {
    LOG_WARN("obj priv is non-consistent between map and vector",
             "obj_privs vector count", obj_privs_.count(),
             "obj_privs map size", obj_priv_map_.item_count());
  }
  return ret;
}

int ObPrivMgr::get_obj_priv(const ObObjPrivSortKey &obj_priv_key,
                            const ObObjPriv *&obj_priv) const
{
  int ret = OB_SUCCESS;
  obj_priv = NULL;

  ObObjPriv *tmp_obj_priv = NULL;
  int hash_ret = obj_priv_map_.get_refactored(obj_priv_key, tmp_obj_priv);
  if (OB_SUCCESS == hash_ret) {
    if (OB_ISNULL(tmp_obj_priv)) {
      LOG_INFO("get obj priv return NULL", K(obj_priv_key));
    } else {
      obj_priv = tmp_obj_priv;
    }
  }

  return ret;
}

int ObPrivMgr::get_table_priv(const ObTablePrivSortKey &table_priv_key,
                              const ObTablePriv *&table_priv) const
{
  int ret = OB_SUCCESS;
  table_priv = NULL;

  ObTablePriv *tmp_table_priv = NULL;
  int hash_ret = table_priv_map_.get_refactored(table_priv_key, tmp_table_priv);
  if (OB_SUCCESS == hash_ret) {
    if (OB_ISNULL(tmp_table_priv)) {
      LOG_INFO("get table priv return NULL", K(table_priv_key));
    } else {
      table_priv = tmp_table_priv;
    }
  }
  return ret;
}

int ObPrivMgr::get_table_priv_set(const ObTablePrivSortKey &table_priv_key,
                                  ObPrivSet &priv_set) const
{
  int ret = OB_SUCCESS;
  priv_set = OB_PRIV_SET_EMPTY;

  const ObTablePriv *table_priv = NULL;
  if (OB_FAIL(get_table_priv(table_priv_key, table_priv))) {
    LOG_WARN("get table priv failed", K(ret), K(table_priv_key));
  } else if (NULL != table_priv) {
    priv_set = table_priv->get_priv_set();
  }

  return ret;
}

int ObPrivMgr::table_grant_in_db(const uint64_t tenant_id,
                                 const uint64_t user_id,
                                 const ObString &db,
                                 bool &is_grant) const
{
  int ret = OB_SUCCESS;
  is_grant = false;

  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == user_id || db.length() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument exist", K(tenant_id), K(user_id), K(db), K(ret));
  } else {
    ObTablePrivDBKey table_priv_db_key(tenant_id, user_id, db);
    ConstTablePrivIter iter =
        table_privs_.lower_bound(table_priv_db_key, ObTablePriv::cmp_db_key);
    if (iter != table_privs_.end()) {
      if (OB_ISNULL(*iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("Invalid user table priv pointer", K(ret));
      } else if (table_priv_db_key == (*iter)->get_db_key()) {
        is_grant = true;
      }
    }
  }
  return ret;
}

int ObPrivMgr::get_db_privs_in_tenant(const uint64_t tenant_id,
                                      ObIArray<const ObDBPriv *> &db_privs) const
{
  int ret = OB_SUCCESS;
  db_privs.reset();

  ConstDBPrivIter tenant_db_priv_begin =
      db_privs_.lower_bound(tenant_id, ObDBPriv::cmp_tenant_id);
  bool is_stop = false;
  for (ConstDBPrivIter iter = tenant_db_priv_begin;
      OB_SUCC(ret) && iter != db_privs_.end() && !is_stop; ++iter) {
    const ObDBPriv *db_priv = NULL;
    if (OB_ISNULL(db_priv = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(db_priv));
    } else if (tenant_id != db_priv->get_tenant_id()) {
      is_stop = true;
    } else if (OB_FAIL(db_privs.push_back(db_priv))) {
      LOG_WARN("push back db priv faield", K(ret));
    }
  }

  return ret;
}

int ObPrivMgr::get_db_privs_in_user(const uint64_t tenant_id,
                                    const uint64_t user_id,
                                    ObIArray<const ObDBPriv *> &db_privs) const
{
  int ret = OB_SUCCESS;
  db_privs.reset();

  ObTenantUserId tenant_user_id(tenant_id, user_id);
  ConstDBPrivIter tenant_db_priv_begin =
      db_privs_.lower_bound(tenant_user_id, ObDBPriv::cmp_tenant_user_id);
  bool is_stop = false;
  for (ConstDBPrivIter iter = tenant_db_priv_begin;
      OB_SUCC(ret) && iter != db_privs_.end() && !is_stop; ++iter) {
    const ObDBPriv *db_priv = NULL;
    if (OB_ISNULL(db_priv = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(db_priv));
    } else if (tenant_id != db_priv->get_tenant_id()
               || user_id != db_priv->get_user_id()) {
      is_stop = true;
    } else if (OB_FAIL(db_privs.push_back(db_priv))) {
      LOG_WARN("push back db priv failed", K(ret));
    }
  }

  return ret;
}

int ObPrivMgr::get_table_privs_in_tenant(const uint64_t tenant_id,
                                         ObIArray<const ObTablePriv *> &table_privs) const
{
  int ret = OB_SUCCESS;
  table_privs.reset();

  ConstTablePrivIter tenant_table_priv_begin =
      table_privs_.lower_bound(tenant_id, ObTablePriv::cmp_tenant_id);
  bool is_stop = false;
  for (ConstTablePrivIter iter = tenant_table_priv_begin;
      OB_SUCC(ret) && iter != table_privs_.end() && !is_stop; ++iter) {
    const ObTablePriv *table_priv = NULL;
    if (OB_ISNULL(table_priv = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Table priv pointer should not be null", K(ret));
    } else if (tenant_id != table_priv->get_tenant_id()) {
      is_stop = true;
    } else if (OB_FAIL(table_privs.push_back(table_priv))) {
      LOG_WARN("push back table priv failed", K(ret));
    }
  }

  return ret;
}

int ObPrivMgr::get_obj_privs_in_tenant(const uint64_t tenant_id,
                                       ObIArray<const ObObjPriv *> &obj_privs) const
{
  int ret = OB_SUCCESS;
  obj_privs.reset();

  ConstObjPrivIter tenant_obj_priv_begin =
      obj_privs_.lower_bound(tenant_id, ObObjPriv::cmp_tenant_id);
  bool is_stop = false;
  for (ConstObjPrivIter iter = tenant_obj_priv_begin;
      OB_SUCC(ret) && iter != obj_privs_.end() && !is_stop; ++iter) {
    const ObObjPriv *obj_priv = NULL;
    if (OB_ISNULL(obj_priv = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Obj priv pointer should not be null", K(ret));
    } else if (tenant_id != obj_priv->get_tenant_id()) {
      is_stop = true;
    } else if (OB_FAIL(obj_privs.push_back(obj_priv))) {
      LOG_WARN("push back obj priv failed", K(ret));
    }
  }

  return ret;
}

int ObPrivMgr::get_table_privs_in_user(const uint64_t tenant_id,
                                       const uint64_t user_id,
                                       ObIArray<const ObTablePriv *> &table_privs) const
{
  int ret = OB_SUCCESS;
  table_privs.reset();

  ObTenantUserId tenant_user_id(tenant_id, user_id);
  ConstTablePrivIter tenant_table_priv_begin =
      table_privs_.lower_bound(tenant_user_id, ObTablePriv::cmp_tenant_user_id);
  bool is_stop = false;
  for (ConstTablePrivIter iter = tenant_table_priv_begin;
      OB_SUCC(ret) && iter != table_privs_.end() && !is_stop; ++iter) {
    const ObTablePriv *table_priv = NULL;
    if (OB_ISNULL(table_priv = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(table_priv));
    } else if (tenant_id != table_priv->get_tenant_id()
               || user_id != table_priv->get_user_id()) {
      is_stop = true;
    } else if (OB_FAIL(table_privs.push_back(table_priv))) {
      LOG_WARN("push back table priv failed", K(ret));
    }
  }

  return ret;
}

int ObPrivMgr::get_obj_privs_in_grantee(const uint64_t tenant_id,
                                        const uint64_t grantee_id,
                                        ObIArray<const ObObjPriv *> &obj_privs) const
{
  int ret = OB_SUCCESS;
  obj_privs.reset();

  ObTenantUserId tenant_user_id(tenant_id, grantee_id);
  ConstObjPrivIter tenant_obj_priv_begin =
      obj_privs_.lower_bound(tenant_user_id, ObObjPriv::cmp_tenant_user_id);
  bool is_stop = false;
  for (ConstObjPrivIter iter = tenant_obj_priv_begin;
      OB_SUCC(ret) && iter != obj_privs_.end() && !is_stop; ++iter) {
    const ObObjPriv *obj_priv = NULL;
    if (OB_ISNULL(obj_priv = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(obj_priv));
    } else if (tenant_id != obj_priv->get_tenant_id()
               || grantee_id != obj_priv->get_grantee_id()) {
      is_stop = true;
    } else if (OB_FAIL(obj_privs.push_back(obj_priv))) {
      LOG_WARN("push back obj priv failed", K(ret));
    }
  }

  return ret;
}

/* we should iterate all the elments here.*/
int ObPrivMgr::get_obj_privs_in_grantor(const uint64_t tenant_id,
                                        const uint64_t grantor_id,
                                        ObIArray<const ObObjPriv *> &obj_privs,
                                        bool reset_flag) const
{
  int ret = OB_SUCCESS;
  if (reset_flag) {
    obj_privs.reset();
  }
  for (ConstObjPrivIter iter = obj_privs_.begin();
      OB_SUCC(ret) && iter != obj_privs_.end(); ++iter) {
    const ObObjPriv *obj_priv = NULL;
    if (OB_ISNULL(obj_priv = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(obj_priv));
    } else if (tenant_id == obj_priv->get_tenant_id()
               && grantor_id == obj_priv->get_grantor_id()) {
      if (OB_FAIL(obj_privs.push_back(obj_priv))) {
        LOG_WARN("push back obj priv failed", K(ret));
      }
    }
  }
  return ret;
}

/* obj_id/obj_type is not used for sorting, so we should iterate all the elments here.*/
int ObPrivMgr::get_obj_privs_in_obj(const uint64_t tenant_id,
                                    const uint64_t obj_id,
                                    const uint64_t obj_type,
                                    ObIArray<const ObObjPriv *> &obj_privs,
                                    bool reset_flag) const
{
  int ret = OB_SUCCESS;
  if (reset_flag) {
    obj_privs.reset();
  }
  for (ConstObjPrivIter iter = obj_privs_.begin();
      OB_SUCC(ret) && iter != obj_privs_.end(); ++iter) {
    const ObObjPriv *obj_priv = NULL;
    if (OB_ISNULL(obj_priv = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(obj_priv));
    } else if (tenant_id == obj_priv->get_tenant_id()
               && obj_id == obj_priv->get_obj_id()
               && obj_type == obj_priv->get_objtype()) {
      if (OB_FAIL(obj_privs.push_back(obj_priv))) {
        LOG_WARN("push back obj priv failed", K(ret));
      }
    }
  }
  return ret;
}

/* we should iterate all the elments here.
   in: grantor, objid(with colid), objtype */
int ObPrivMgr::get_obj_privs_in_grantor_obj_id(
    const uint64_t tenant_id,
    const ObObjPrivSortKey &obj_key,
    ObIArray<const ObObjPriv *> &obj_privs) const
{
  int ret = OB_SUCCESS;
  const uint64_t grantor_id = obj_key.grantor_id_;
  const uint64_t obj_id = obj_key.obj_id_;
  const uint64_t obj_type = obj_key.obj_type_;
  const uint64_t col_id = obj_key.col_id_;
  obj_privs.reset();
  for (ConstObjPrivIter iter = obj_privs_.begin();
      OB_SUCC(ret) && iter != obj_privs_.end(); ++iter) {
    const ObObjPriv *obj_priv = NULL;
    if (OB_ISNULL(obj_priv = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(obj_priv));
    } else if (tenant_id == obj_priv->get_tenant_id()
               && grantor_id == obj_priv->get_grantor_id()
               && obj_id == obj_priv->get_obj_id()
               && obj_type == obj_priv->get_objtype()
               && col_id == obj_priv->get_col_id()) {
      if (OB_FAIL(obj_privs.push_back(obj_priv))) {
        LOG_WARN("push back obj priv failed", K(ret));
      }
    }
  }
  return ret;
}

/* in obj_key: grantor_id is invalid, not used
   find all obj priv granted to ur,
   no matter who granted(without caring grantor) */
int ObPrivMgr::get_obj_privs_in_ur_and_obj(
    const uint64_t tenant_id,
    const ObObjPrivSortKey &obj_key,
    ObIArray<const ObObjPriv *> &obj_privs) const
{
  int ret = OB_SUCCESS;
  const uint64_t grantee_id = obj_key.grantee_id_;
  const uint64_t obj_id = obj_key.obj_id_;
  const uint64_t obj_type = obj_key.obj_type_;
  const uint64_t col_id = obj_key.col_id_;
  obj_privs.reset();

  ObTenantUrObjId tenant_ur_obj_id(tenant_id, grantee_id, obj_id, obj_type, col_id);
  ConstObjPrivIter tenant_obj_priv_begin =
      obj_privs_.lower_bound(tenant_ur_obj_id, ObObjPriv::cmp_tenant_ur_obj_id);
  bool is_stop = false;
  for (ConstObjPrivIter iter = tenant_obj_priv_begin;
      OB_SUCC(ret) && iter != obj_privs_.end() && !is_stop; ++iter) {
    const ObObjPriv *obj_priv = NULL;
    if (OB_ISNULL(obj_priv = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(obj_priv));
    } else if (tenant_id != obj_priv->get_tenant_id()
               || grantee_id != obj_priv->get_grantee_id()
               || obj_id != obj_priv->get_obj_id()
               || obj_type != obj_priv->get_objtype()
               || col_id != obj_priv->get_col_id()) {
      is_stop = true;
    } else if (OB_FAIL(obj_privs.push_back(obj_priv))) {
      LOG_WARN("push back obj priv failed", K(ret));
    }
  }

  return ret;
}

/* input: grantor, obj_id, grantee_id */
int ObPrivMgr::get_obj_privs_in_grantor_ur_obj_id(
    const uint64_t tenant_id,
    const ObObjPrivSortKey &obj_key,
    ObIArray<const ObObjPriv *> &obj_privs) const
{
  int ret = OB_SUCCESS;
  const uint64_t grantee_id = obj_key.grantee_id_;
  const uint64_t obj_id = obj_key.obj_id_;
  const uint64_t obj_type = obj_key.obj_type_;
  const uint64_t grantor_id = obj_key.grantor_id_;
  obj_privs.reset();

  ObTenantUrObjId tenant_ur_obj_id(tenant_id, grantee_id, obj_id, obj_type, 0);
  ConstObjPrivIter tenant_obj_priv_begin =
      obj_privs_.lower_bound(tenant_ur_obj_id, ObObjPriv::cmp_tenant_ur_obj_id);
  bool is_stop = false;
  for (ConstObjPrivIter iter = tenant_obj_priv_begin;
      OB_SUCC(ret) && iter != obj_privs_.end() && !is_stop; ++iter) {
    const ObObjPriv *obj_priv = NULL;
    if (OB_ISNULL(obj_priv = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(obj_priv));
    } else if (tenant_id != obj_priv->get_tenant_id()
               || grantee_id != obj_priv->get_grantee_id()
               || obj_id != obj_priv->get_obj_id()
               || obj_type != obj_priv->get_objtype()) {
      is_stop = true;
    } else if (grantor_id == obj_priv->get_grantor_id()) {
      if (OB_FAIL(obj_privs.push_back(obj_priv))) {
        LOG_WARN("push back obj priv failed", K(ret));
      }
    }
  }

  return ret;
}

int ObPrivMgr::get_obj_privs_in_ur_and_obj(
    const uint64_t tenant_id,
    const ObObjPrivSortKey &obj_key,
    ObPackedObjPriv &obj_privs) const
{
  int ret = OB_SUCCESS;
  const uint64_t grantee_id = obj_key.grantee_id_;
  const uint64_t obj_id = obj_key.obj_id_;
  const uint64_t obj_type = obj_key.obj_type_;
  const uint64_t col_id = obj_key.col_id_;
  obj_privs = 0;

  ObTenantUrObjId tenant_ur_obj_id(tenant_id, grantee_id, obj_id, obj_type, col_id);
  ConstObjPrivIter tenant_obj_priv_begin =
      obj_privs_.lower_bound(tenant_ur_obj_id, ObObjPriv::cmp_tenant_ur_obj_id);
  bool is_stop = false;
  for (ConstObjPrivIter iter = tenant_obj_priv_begin;
      OB_SUCC(ret) && iter != obj_privs_.end() && !is_stop; ++iter) {
    const ObObjPriv *obj_priv = *iter;
    if (OB_ISNULL(obj_priv)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(obj_priv));
    } else if (tenant_id != obj_priv->get_tenant_id()
               || grantee_id != obj_priv->get_grantee_id()
               || obj_id != obj_priv->get_obj_id()
               || obj_type != obj_priv->get_objtype()
               || col_id != obj_priv->get_col_id()) {
      is_stop = true;
    } else {
      obj_privs |= obj_priv->get_obj_privs();
    }
  }
  return ret;
}

int ObPrivMgr::get_sys_privs_in_tenant(const uint64_t tenant_id,
                                       ObIArray<const ObSysPriv *> &sys_privs) const
{
  int ret = OB_SUCCESS;
  sys_privs.reset();

  ConstSysPrivIter tenant_sys_priv_begin =
      sys_privs_.lower_bound(tenant_id, ObSysPriv::cmp_tenant_id);
  bool is_stop = false;
  for (ConstSysPrivIter iter = tenant_sys_priv_begin;
      OB_SUCC(ret) && iter != sys_privs_.end() && !is_stop; ++iter) {
    const ObSysPriv *sys_priv = NULL;
    if (OB_ISNULL(sys_priv = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(sys_priv));
    } else if (tenant_id != sys_priv->get_tenant_id()) {
      is_stop = true;
    } else if (OB_FAIL(sys_privs.push_back(sys_priv))) {
      LOG_WARN("push back sys priv failed", K(ret));
    }
  }

  return ret;
}

int ObPrivMgr::get_sys_priv_in_grantee(const uint64_t tenant_id,
                                       const uint64_t grantee_id,
                                       ObSysPriv *&sys_priv) const
{
  int ret = OB_SUCCESS;
  ObTenantUserId tenant_user_id(tenant_id, grantee_id);
  ConstSysPrivIter tenant_sys_priv_begin =
      sys_privs_.lower_bound(tenant_user_id, ObSysPriv::cmp_tenant_grantee_id);
  sys_priv = NULL;
  bool is_stop = false;
  for (ConstSysPrivIter iter = tenant_sys_priv_begin;
      OB_SUCC(ret) && iter != sys_privs_.end() && !is_stop; ++iter) {
    if (OB_ISNULL(*iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(sys_priv));
    } else if (tenant_id != (*iter)->get_tenant_id()
               || grantee_id != (*iter)->get_grantee_id()) {
      is_stop = true;
    } else {
      sys_priv = *iter;
    }
  }

  return ret;
}

void ObPrivMgr::dump() const
{
  for (DBPrivIter iter = db_privs_.begin();
      iter != db_privs_.end(); ++iter) {
    const ObDBPriv *db_priv = *iter;
    if (NULL == db_priv) {
      LOG_INFO("NULL ptr", K(db_priv));
    } else {
      LOG_INFO("DBPriv", K(*db_priv));
    }
  }

  for (TablePrivIter iter = table_privs_.begin();
      iter != table_privs_.end(); ++iter) {
    const ObTablePriv *table_priv = *iter;
    if (NULL == table_priv) {
      LOG_INFO("NULL ptr", K(table_priv));
    } else {
      LOG_INFO("TablePriv", K(*table_priv));
    }
  }

  for (ObjPrivIter iter = obj_privs_.begin();
      iter != obj_privs_.end(); ++iter) {
    const ObObjPriv *obj_priv = *iter;
    if (NULL == obj_priv) {
      LOG_INFO("NULL ptr", K(obj_priv));
    } else {
      LOG_INFO("ObjPriv", K(*obj_priv));
    }
  }

  for (SysPrivIter iter = sys_privs_.begin();
      iter != sys_privs_.end(); ++iter) {
    const ObSysPriv *sys_priv = *iter;
    if (NULL == sys_priv) {
      LOG_INFO("NULL ptr", K(sys_priv));
    } else {
      LOG_INFO("SysPriv", K(*sys_priv));
    }
  }
}

int ObPrivMgr::get_schema_statistics(const ObSchemaType schema_type, ObSchemaStatisticsInfo &schema_info) const
{
  int ret = OB_SUCCESS;
  schema_info.reset();
  if (TABLE_PRIV != schema_type
      && DATABASE_PRIV != schema_type
      && SYS_PRIV != schema_type
      && OBJ_PRIV != schema_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid schema type", K(ret), K(schema_type));
  } else {
    schema_info.schema_type_ = schema_type;
    if (TABLE_PRIV == schema_type) {
      schema_info.count_ = table_privs_.size();
      for (ConstTablePrivIter it = table_privs_.begin(); OB_SUCC(ret) && it != table_privs_.end(); it++) {
        if (OB_ISNULL(*it)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("schema is null", K(ret));
        } else {
          schema_info.size_ += (*it)->get_convert_size();
        }
      }
    } else if (DATABASE_PRIV == schema_type) {
      schema_info.count_ = db_privs_.size();
      for (ConstDBPrivIter it = db_privs_.begin(); OB_SUCC(ret) && it != db_privs_.end(); it++) {
        if (OB_ISNULL(*it)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("schema is null", K(ret));
        } else {
          schema_info.size_ += (*it)->get_convert_size();
        }
      }
    } else if (SYS_PRIV == schema_type) {
      schema_info.count_ = sys_privs_.size();
      for (ConstSysPrivIter it = sys_privs_.begin(); OB_SUCC(ret) && it != sys_privs_.end(); it++) {
        if (OB_ISNULL(*it)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("schema is null", K(ret));
        } else {
          schema_info.size_ += (*it)->get_convert_size();
        }
      }
    } else if (OBJ_PRIV == schema_type) {
      schema_info.count_ = obj_privs_.size();
      for (ConstObjPrivIter it = obj_privs_.begin(); OB_SUCC(ret) && it != obj_privs_.end(); it++) {
        if (OB_ISNULL(*it)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("schema is null", K(ret));
        } else {
          schema_info.size_ += (*it)->get_convert_size();
        }
      }
    }
  }
  return ret;
}

}
}
}
