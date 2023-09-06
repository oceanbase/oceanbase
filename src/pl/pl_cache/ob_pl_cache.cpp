/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX PL_CACHE
#include "ob_pl_cache.h"
#include "lib/oblog/ob_log_module.h"
#include "share/rc/ob_tenant_base.h"     //MTL
#include "pl/ob_pl_stmt.h"
namespace oceanbase
{
namespace pl
{

int PCVPlSchemaObj::init(const ObTableSchema *schema)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(schema) || OB_ISNULL(inner_alloc_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected null argument", K(ret), K(schema), K(inner_alloc_));
  } else {
    tenant_id_ = schema->get_tenant_id();
    database_id_ = schema->get_database_id();
    schema_id_ = schema->get_table_id();
    schema_version_ = schema->get_schema_version();
    schema_type_ = TABLE_SCHEMA;
    table_type_ = schema->get_table_type();
    is_tmp_table_ = schema->is_tmp_table();
    // copy table name
    char *buf = nullptr;
    const ObString &tname = schema->get_table_name_str();
    if (nullptr == (buf = static_cast<char *>(inner_alloc_->alloc(tname.length())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret), K(tname.length()));
    } else {
      MEMCPY(buf, tname.ptr(), tname.length());
      table_name_.assign_ptr(buf, tname.length());
    }
  }
  return ret;
}

bool PCVPlSchemaObj::operator==(const PCVPlSchemaObj &other) const
{
  bool ret = true;
  if (schema_type_ != other.schema_type_) {
    ret = false;
  } else if (TABLE_SCHEMA == other.schema_type_) {
    ret = tenant_id_ == other.tenant_id_ &&
          database_id_ == other.database_id_ &&
          schema_id_ == other.schema_id_ &&
          schema_version_ == other.schema_version_ &&
          table_type_ == other.table_type_;
  } else {
    ret = schema_id_ == other.schema_id_ &&
          schema_version_ == other.schema_version_;
  }
  return ret;
}

int PCVPlSchemaObj::init_without_copy_name(const ObSimpleTableSchemaV2 *schema)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected null argument", K(ret), K(schema));
  } else {
    tenant_id_ = schema->get_tenant_id();
    database_id_ = schema->get_database_id();
    schema_id_ = schema->get_table_id();
    schema_version_ = schema->get_schema_version();
    schema_type_ = TABLE_SCHEMA;
    table_type_ = schema->get_table_type();
    is_tmp_table_ = schema->is_tmp_table();

    table_name_ = schema->get_table_name_str();
  }
  return ret;
}

int PCVPlSchemaObj::init_with_version_obj(const ObSchemaObjVersion &schema_obj_version)
{
  int ret = OB_SUCCESS;
  schema_type_ = schema_obj_version.get_schema_type();
  schema_id_ = schema_obj_version.object_id_;
  schema_version_ = schema_obj_version.version_;
  return ret;
}

void PCVPlSchemaObj::reset()
{
  tenant_id_ = common::OB_INVALID_ID;
  database_id_ = common::OB_INVALID_ID;
  schema_id_ = common::OB_INVALID_ID;
  schema_type_ = OB_MAX_SCHEMA;
  schema_version_ = 0;
  table_type_ = MAX_TABLE_TYPE;
  is_tmp_table_ = false;
  if (inner_alloc_ != nullptr && table_name_.ptr() != nullptr) {
    inner_alloc_->free(table_name_.ptr());
    table_name_.reset();
    inner_alloc_ = nullptr;
  }
}

PCVPlSchemaObj::~PCVPlSchemaObj()
{
  reset();
}

void ObPLObjectKey::reset()
{
  db_id_ = common::OB_INVALID_ID;
  key_id_ = common::OB_INVALID_ID;
  sessid_ = 0;
  name_.reset();
  namespace_ = ObLibCacheNameSpace::NS_INVALID;
}

int ObPLObjectKey::deep_copy(ObIAllocator &allocator, const ObILibCacheKey &other)
{
  int ret = OB_SUCCESS;
  const ObPLObjectKey &key = static_cast<const ObPLObjectKey&>(other);
  if (OB_FAIL(common::ob_write_string(allocator, key.name_, name_))) {
    LOG_WARN("failed to deep copy name", K(ret), K(name_));
  } else {
    db_id_ = key.db_id_;
    key_id_ = key.key_id_;
    sessid_ = key.sessid_;
    namespace_ = key.namespace_;
  }
  return ret;
}

void ObPLObjectKey::destory(common::ObIAllocator &allocator)
{
  if (nullptr != name_.ptr()) {
    allocator.free(const_cast<char *>(name_.ptr()));
  }
}

uint64_t ObPLObjectKey::hash() const
{
  uint64_t hash_ret = murmurhash(&db_id_, sizeof(uint64_t), 0);
  hash_ret = murmurhash(&key_id_, sizeof(uint64_t), hash_ret);
  hash_ret = murmurhash(&sessid_, sizeof(uint32_t), hash_ret);
  hash_ret = name_.hash(hash_ret);
  hash_ret = murmurhash(&namespace_, sizeof(ObLibCacheNameSpace), hash_ret);
  return hash_ret;
}

bool ObPLObjectKey::is_equal(const ObILibCacheKey &other) const
{
  const ObPLObjectKey &key = static_cast<const ObPLObjectKey&>(other);
  bool cmp_ret = db_id_ == key.db_id_ &&
                 key_id_ == key.key_id_ &&
                 sessid_ == key.sessid_ &&
                 name_ == key.name_ &&
                 namespace_ == key.namespace_;
  return cmp_ret;
}

int ObPLObjectValue::init(const ObILibCacheObject &cache_obj, ObPLCacheCtx &pc_ctx)
{
  int ret = OB_SUCCESS;
  const pl::ObPLCacheObject &pl_object = static_cast<const pl::ObPLCacheObject &>(cache_obj);
  if (OB_FAIL(add_match_info(pc_ctx, &pc_ctx.key_, cache_obj))) {
    LOG_WARN("failed to add_match_info", K(ret));
  } else {
    params_info_.reset();
    if (OB_FAIL(params_info_.reserve(pl_object.get_params_info().count()))) {
      LOG_WARN("failed to reserve 2d array", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < pl_object.get_params_info().count(); ++i) {
      if (OB_FAIL(params_info_.push_back(pl_object.get_params_info().at(i)))) {
        LOG_WARN("fail to push back param info", K(ret));
      }
    }
  }
  return ret;
}

void ObPLObjectValue::reset()
{
  ObDLinkBase<ObPLObjectValue>::reset();
  for (int64_t i = 0; i < stored_schema_objs_.count(); i++) {
    if (OB_ISNULL(stored_schema_objs_.at(i)) || OB_ISNULL(pc_alloc_)) {
      // do nothing
    } else {
      stored_schema_objs_.at(i)->reset();
      pc_alloc_->free(stored_schema_objs_.at(i));
    }
  }
  stored_schema_objs_.reset();
  sys_schema_version_ = OB_INVALID_VERSION;
  tenant_schema_version_ = OB_INVALID_VERSION;
  sessid_ = 0;
  sess_create_time_ = 0;
  contain_sys_name_table_ = false;
  contain_sys_pl_object_ = false;
  contain_tmp_table_ = false;
  params_info_.reset();

  pl_routine_obj_ = nullptr;
}

int64_t ObPLObjectValue::get_mem_size()
{
  int64_t value_mem_size = 0;
  if (OB_ISNULL(pl_routine_obj_)) {
    BACKTRACE_RET(ERROR, OB_ERR_UNEXPECTED, true, "invalid routine obj");
  } else {
    value_mem_size = pl_routine_obj_->get_mem_size();
  }
  return value_mem_size;
}

int ObPLObjectValue::lift_tenant_schema_version(int64_t new_schema_version)
{
  int ret = OB_SUCCESS;
  if (new_schema_version <= tenant_schema_version_) {
    // do nothing
  } else {
    ATOMIC_STORE(&(tenant_schema_version_), new_schema_version);
  }
  return ret;
}

int ObPLObjectValue::check_value_version(share::schema::ObSchemaGetterGuard *schema_guard,
                                          bool need_check_schema,
                                          const ObIArray<PCVPlSchemaObj> &schema_array,
                                          bool &is_old_version)
{
  int ret = OB_SUCCESS;
  is_old_version = false;
  if (OB_ISNULL(schema_guard)) {
    int ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schema_guard));
  } else if (0 == schema_array.count()) {
    // do nothing
  } else {
    int64_t table_count = stored_schema_objs_.count();

    if (schema_array.count() != table_count) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table count do not match", K(ret), K(schema_array.count()), K(table_count));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && !is_old_version && i < table_count; ++i) {
        const PCVPlSchemaObj *schema_obj1 = stored_schema_objs_.at(i);
        const PCVPlSchemaObj &schema_obj2 = schema_array.at(i);
        if (nullptr == schema_obj1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("got an unexpected null table schema", K(ret), K(schema_obj1));
        } else if (*schema_obj1 == schema_obj2) { // schema do match
          LOG_DEBUG("matched schema objs", K(*schema_obj1), K(schema_obj2), K(i));
          // do nothing
        } else {
          LOG_WARN("mismatched schema objs", K(*schema_obj1), K(schema_obj2), K(i));
          is_old_version = true;
        }
      }
    }
  }
  return ret;
}


int ObPLObjectValue::need_check_schema_version(ObPLCacheCtx &pc_ctx,
                                                int64_t &new_schema_version,
                                                bool &need_check)
{
  int ret = OB_SUCCESS;
  need_check = false;
  if (OB_FAIL(pc_ctx.schema_guard_->get_schema_version(pc_ctx.session_info_->get_effective_tenant_id(),
                                                                new_schema_version))) {
    LOG_WARN("failed to get tenant schema version", K(ret));
  } else {
    int64_t cached_tenant_schema_version = ATOMIC_LOAD(&tenant_schema_version_);
    need_check = ((new_schema_version != cached_tenant_schema_version)
                  || contain_tmp_table_
                  || contain_sys_pl_object_
                  || contain_sys_name_table_);
    if (need_check && REACH_TIME_INTERVAL(10000000)) {
      LOG_INFO("need check schema", K(new_schema_version), K(cached_tenant_schema_version));
    }
  }
  return ret;
}

int ObPLObjectValue::get_all_dep_schema(ObSchemaGetterGuard &schema_guard,
                                         const DependenyTableStore &dep_schema_objs,
                                         ObIArray<PCVPlSchemaObj> &schema_array)
{
  int ret = OB_SUCCESS;
  schema_array.reset();
  const ObSimpleTableSchemaV2 *table_schema = nullptr;
  PCVPlSchemaObj tmp_schema_obj;

  for (int64_t i = 0; OB_SUCC(ret) && i < dep_schema_objs.count(); ++i) {
    if (TABLE_SCHEMA != dep_schema_objs.at(i).get_schema_type()) {
      if (OB_FAIL(tmp_schema_obj.init_with_version_obj(dep_schema_objs.at(i)))) {
        LOG_WARN("failed to init pcv schema obj", K(ret));
      } else if (OB_FAIL(schema_array.push_back(tmp_schema_obj))) {
        LOG_WARN("failed to push back pcv schema obj", K(ret));
      } else {
        tmp_schema_obj.reset();
      }
    } else if (OB_FAIL(schema_guard.get_simple_table_schema(
                                     MTL_ID(),
                                     dep_schema_objs.at(i).get_object_id(),
                                     table_schema))) {
      LOG_WARN("failed to get table schema",
               K(ret), K(dep_schema_objs.at(i)));
    } else if (nullptr == table_schema) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get an unexpected null table schema", K(ret));
    } else if (table_schema->is_index_table()) {
      // do nothing
    } else if (OB_FAIL(tmp_schema_obj.init_without_copy_name(table_schema))) {
      LOG_WARN("failed to init pcv schema obj", K(ret));
    } else if (OB_FAIL(schema_array.push_back(tmp_schema_obj))) {
      LOG_WARN("failed to push back pcv schema obj", K(ret));
    } else {
      table_schema = nullptr;
      tmp_schema_obj.reset();
    }
  }

  if (OB_FAIL(ret)) {
    schema_array.reset();
  } else {
    LOG_DEBUG("get all dep schema", K(schema_array));
  }
  return ret;
}

int ObPLObjectValue::get_all_dep_schema(ObPLCacheCtx &pc_ctx,
                                         const uint64_t database_id,
                                         int64_t &new_schema_version,
                                         bool &need_check_schema,
                                         ObIArray<PCVPlSchemaObj> &schema_array)
{
  int ret = OB_SUCCESS;
  need_check_schema = false;
  if (OB_FAIL(need_check_schema_version(pc_ctx,
                                        new_schema_version,
                                        need_check_schema))) {
    LOG_WARN("failed to get need_check_schema flag", K(ret));
  } else if (!need_check_schema) {
    // do nothing
  } else if (OB_ISNULL(pc_ctx.schema_guard_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    schema_array.reset();
    const ObSimpleTableSchemaV2 *table_schema = nullptr;
    PCVPlSchemaObj tmp_schema_obj;
    uint64_t tenant_id = OB_INVALID_ID;
    for (int64_t i = 0; OB_SUCC(ret) && i < stored_schema_objs_.count(); i++) {
      tenant_id = MTL_ID();
      ObSchemaGetterGuard &schema_guard = *pc_ctx.schema_guard_;
      PCVPlSchemaObj *pcv_schema = stored_schema_objs_.at(i);
      if (OB_ISNULL(pcv_schema)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("got unexpected null", K(ret));
      } else if (TABLE_SCHEMA != pcv_schema->schema_type_) {
        // if no table schema, get schema version is enough
        int64_t new_version = 0;
        if (PACKAGE_SCHEMA == stored_schema_objs_.at(i)->schema_type_
            || UDT_SCHEMA == stored_schema_objs_.at(i)->schema_type_
            || ROUTINE_SCHEMA == stored_schema_objs_.at(i)->schema_type_) {
          tenant_id = pl::get_tenant_id_by_object_id(stored_schema_objs_.at(i)->schema_id_);
        }
        if (OB_FAIL(schema_guard.get_schema_version(pcv_schema->schema_type_,
                                                    tenant_id,
                                                    pcv_schema->schema_id_,
                                                    new_version))) {
          LOG_WARN("failed to get schema version",
                   K(ret), K(tenant_id), K(pcv_schema->schema_type_), K(pcv_schema->schema_id_));
        } else {
          tmp_schema_obj.schema_id_ = pcv_schema->schema_id_;
          tmp_schema_obj.schema_type_ = pcv_schema->schema_type_;
          tmp_schema_obj.schema_version_ = new_version;
          if (OB_FAIL(schema_array.push_back(tmp_schema_obj))) {
            LOG_WARN("failed to push back array", K(ret));
          } else {
            tmp_schema_obj.reset();
          }
        }
      } else if (lib::is_oracle_mode()) {
        if (pcv_schema->is_explicit_db_name_) {
          //In oracle mode, if mark database name，use table id search schema directly.
          if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id,
                      pcv_schema->schema_id_, table_schema))) {
            LOG_WARN("failed to get table schema", K(pcv_schema->schema_id_), K(ret));
          } else { /* do nothing */ }
        } else if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id,
                                                                database_id,
                                                                pcv_schema->table_name_,
                                                                false,
                                                                table_schema))) {
          LOG_WARN("failed to get table schema", K(pcv_schema->schema_id_), K(ret));
        } else if (nullptr == table_schema && OB_FAIL(schema_guard.get_simple_table_schema(tenant_id,
                                                                pcv_schema->database_id_,
                                                                pcv_schema->table_name_,
                                                                false,
                                                                table_schema))) {
          LOG_WARN("failed to get table schema",
                  K(ret), K(pcv_schema->tenant_id_), K(pcv_schema->database_id_),
                  K(pcv_schema->table_name_));
        } else if (nullptr == table_schema && OB_FAIL(schema_guard.get_simple_table_schema(tenant_id,
                                                                                           common::OB_ORA_SYS_DATABASE_ID,
                                                                                           pcv_schema->table_name_,
                                                                                           false,
                                                                                           table_schema))) { // finaly,find sys tenand
          LOG_WARN("failed to get table schema", K(ret), K(tenant_id),
                   K(pcv_schema->table_name_));
        } else {
          // do nothing
        }
      } else if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id,
                                                              pcv_schema->database_id_,
                                                              pcv_schema->table_name_,
                                                              false,
                                                              table_schema))) { //In mysql mode, use db id cached by pcv schema to search
        LOG_WARN("failed to get table schema",
                 K(ret), K(pcv_schema->tenant_id_), K(pcv_schema->database_id_),
                 K(pcv_schema->table_name_));
      } else {
        // do nothing
      }

      if (OB_FAIL(ret)) {
        // do nothing
      } else if (TABLE_SCHEMA != pcv_schema->schema_type_) { // not table schema
        tmp_schema_obj.reset();
      } else if (nullptr == table_schema) {
        ret = OB_OLD_SCHEMA_VERSION;
        LOG_WARN("table not exist", K(ret), K(*pcv_schema), K(table_schema));
      } else if (OB_FAIL(tmp_schema_obj.init_without_copy_name(table_schema))) {
        LOG_WARN("failed to init pcv schema obj", K(ret));
      } else if (OB_FAIL(schema_array.push_back(tmp_schema_obj))) {
        LOG_WARN("failed to push back array", K(ret));
      } else {
        table_schema = nullptr;
        tmp_schema_obj.reset();
      }
    } // for end
  }
  return ret;
}


int ObPLObjectValue::match_dep_schema(const ObPLCacheCtx &pc_ctx,
                                       const ObIArray<PCVPlSchemaObj> &schema_array,
                                       bool &is_same)
{
  int ret = OB_SUCCESS;
  is_same = true;
  ObSQLSessionInfo *session_info = pc_ctx.session_info_;
  if (OB_ISNULL(session_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(session_info));
  } else if (schema_array.count() != stored_schema_objs_.count()) {
    is_same = false;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && is_same && i < schema_array.count(); i++) {
      if (OB_ISNULL(stored_schema_objs_.at(i))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid null table schema", K(ret), K(i));
      } else if (schema_array.at(i).is_tmp_table_) { // check for tmp table
        is_same = ((session_info->get_sessid_for_table() == sessid_) &&
                   (session_info->get_sess_create_time() == sess_create_time_));
        if (!is_same) {
          LOG_WARN("tmp table not match", K(ret), K(sessid_), K(session_info->get_sessid_for_table()),
                                                  K(sess_create_time_), K(session_info->get_sess_create_time()));
        }
      } else if (lib::is_oracle_mode()
                 && TABLE_SCHEMA == stored_schema_objs_.at(i)->schema_type_
                 && !stored_schema_objs_.at(i)->match_compare(schema_array.at(i))) {
        // check whether common table name is same as system table in oracle mode
        is_same = false;
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

int ObPLObjectValue::add_match_info(ObILibCacheCtx &ctx,
                                  ObILibCacheKey *key,
                                  const ObILibCacheObject &cache_obj)
{
  int ret = OB_SUCCESS;

  ObPLCacheCtx& pc_ctx = static_cast<ObPLCacheCtx&>(ctx);
  const pl::ObPLCacheObject &cache_object = static_cast<const pl::ObPLCacheObject &>(cache_obj);
  if (OB_UNLIKELY(!cache_object.is_prcr() &&
                  !cache_object.is_sfc() &&
                  !cache_object.is_pkg() &&
                  !cache_object.is_anon() &&
                  !cache_object.is_call_stmt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cache object is invalid", K(cache_object));
  } else if (OB_UNLIKELY(pl_routine_obj_ != nullptr)) {
    ret = OB_SQL_PC_PLAN_DUPLICATE;
  } else if (OB_FAIL(set_stored_schema_objs(cache_object.get_dependency_table(),
                                            pc_ctx.schema_guard_))) {
      LOG_WARN("failed to set stored schema objs",
               K(ret), K(cache_object.get_dependency_table()));
  } else {
    sys_schema_version_ = cache_object.get_sys_schema_version();
    tenant_schema_version_ = cache_object.get_tenant_schema_version();
    if (contain_tmp_table_) {
      sessid_ = pc_ctx.session_info_->get_sessid_for_table();
      sess_create_time_ = pc_ctx.session_info_->get_sess_create_time();
    }
  }
  return ret;
}


int ObPLObjectValue::set_stored_schema_objs(const DependenyTableStore &dep_table_store,
                                          share::schema::ObSchemaGetterGuard *schema_guard)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = nullptr;
  PCVPlSchemaObj *pcv_schema_obj = nullptr;
  void *obj_buf = nullptr;

  stored_schema_objs_.reset();
  stored_schema_objs_.set_allocator(pc_alloc_);

  if (OB_ISNULL(schema_guard)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null argument", K(ret), K(schema_guard));
  } else if (OB_FAIL(stored_schema_objs_.init(dep_table_store.count()))) {
    LOG_WARN("failed to init stored_schema_objs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < dep_table_store.count(); i++) {
      const ObSchemaObjVersion &table_version = dep_table_store.at(i);
      table_schema = nullptr;
      int hash_err = OB_SUCCESS;
      if (table_version.get_schema_type() != TABLE_SCHEMA) {
        // if not table schema, store schema id and version
        if (nullptr == (obj_buf = pc_alloc_->alloc(sizeof(PCVPlSchemaObj)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret));
        } else if (FALSE_IT(pcv_schema_obj = new(obj_buf)PCVPlSchemaObj(pc_alloc_))) {
          // do nothing
        } else if (OB_FAIL(pcv_schema_obj->init_with_version_obj(table_version))) {
          LOG_WARN("failed to init pcv schema obj", K(ret), K(table_version));
        } else if (OB_FAIL(stored_schema_objs_.push_back(pcv_schema_obj))) {
          LOG_WARN("failed to push back array", K(ret));
        } else {
          // do nothing
        }
      } else if (OB_FAIL(schema_guard->get_table_schema(
                  MTL_ID(),
                  table_version.get_object_id(),
                  table_schema))) { // now deal with table schema
        LOG_WARN("failed to get table schema", K(ret), K(table_version), K(table_schema));
      } else if (nullptr == table_schema) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get an unexpected null schema", K(ret), K(table_schema));
      } else if (table_schema->is_index_table()) {
        // do nothing
      } else if (nullptr == (obj_buf = pc_alloc_->alloc(sizeof(PCVPlSchemaObj)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else if (FALSE_IT(pcv_schema_obj = new(obj_buf)PCVPlSchemaObj(pc_alloc_))) {
        // do nothing
      } else if (OB_FAIL(pcv_schema_obj->init(table_schema))) {
        LOG_WARN("failed to init pcv schema obj with table schema", K(ret));
      } else if (FALSE_IT(pcv_schema_obj->is_explicit_db_name_ = table_version.is_db_explicit_)) {
        // do nothing
      } else if (OB_FAIL(stored_schema_objs_.push_back(pcv_schema_obj))) {
        LOG_WARN("failed to push back array", K(ret));
      } else if(!contain_sys_name_table_) {
        /* Ordinary tables in oracle mode can have the same name as tables in sys,
           and need to be distinguished when matching plans to match different plans
           The table under sys contains system tables and views, so call is_sys_table_name
           to check whether the table is under sys.
           In addition, if SQL contains internal tables, the schema version changes of the
           internal tables will not be reflected in the tenant schema version of ordinary tenants.
           In order to be able to update the plan in time, you need to check the schema version number
           of the corresponding internal table. The internal table of the oracle tenant is under sys,
           and the mysql tenant Under oceanbase. */
        if (lib::is_oracle_mode()) {
          if (OB_FAIL(share::schema::ObSysTableChecker::is_sys_table_name(MTL_ID(),
                                                                          OB_ORA_SYS_DATABASE_ID,
                                                                          table_schema->get_table_name(),
                                                                          contain_sys_name_table_))) {
            LOG_WARN("failed to check sys table", K(ret));
          }
        } else if (OB_FAIL(share::schema::ObSysTableChecker::is_sys_table_name(MTL_ID(),
                                                                               OB_SYS_DATABASE_ID,
                                                                               table_schema->get_table_name(),
                                                                               contain_sys_name_table_))) {
          LOG_WARN("failed to check sys table", K(ret));
        } else {
          // do nothing
        }
        LOG_DEBUG("check sys table", K(table_schema->get_table_name()), K(contain_sys_name_table_));
      } else {
        // do nothing
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(pcv_schema_obj)) {
        if (pcv_schema_obj->is_tmp_table_ && !contain_tmp_table_) {
          contain_tmp_table_ = true;
        }
        if (!contain_sys_pl_object_ &&
            (PACKAGE_SCHEMA == pcv_schema_obj->schema_type_ ||
             UDT_SCHEMA == pcv_schema_obj->schema_type_) &&
            OB_SYS_TENANT_ID == pl::get_tenant_id_by_object_id(pcv_schema_obj->schema_id_)) {
          contain_sys_pl_object_ = true;
        }
      }
      obj_buf = nullptr;
      pcv_schema_obj = nullptr;
      table_schema = nullptr;
    } // for end
  }
  if (OB_FAIL(ret)) {
    stored_schema_objs_.reset();
  } else {
    // do nothing
  }
  return ret;
}

bool ObPLObjectValue::match_params_info(const Ob2DArray<ObPlParamInfo,
                                        OB_MALLOC_BIG_BLOCK_SIZE,
                                        ObWrapperAllocator, false> &infos)
{
  bool is_same = true;
  if (infos.count() != params_info_.count()) {
    is_same = false;
  } else {
    int64_t N = infos.count();
    for (int64_t i = 0; is_same && i < N; ++i) {
      if (true == is_same && params_info_.at(i).flag_.need_to_check_type_) {
        if (infos.at(i).type_ != params_info_.at(i).type_
           || infos.at(i).scale_ != params_info_.at(i).scale_
           || infos.at(i).col_type_ != params_info_.at(i).col_type_
           || (params_info_.at(i).flag_.need_to_check_extend_type_
               && infos.at(i).ext_real_type_ != params_info_.at(i).ext_real_type_)
           || (params_info_.at(i).flag_.is_boolean_ != infos.at(i).flag_.is_boolean_)) {
          is_same = false;
        }
      }
      if (true == is_same && params_info_.at(i).flag_.need_to_check_bool_value_) {
        if (infos.at(i).flag_.expected_bool_value_
            != params_info_.at(i).flag_.expected_bool_value_) {
          is_same = false;
        }
      }
    }
  }
  return is_same;
}

int ObPLObjectValue::match_complex_type_info(const ObPlParamInfo &param_info,
                                              const ObObjParam &param,
                                              bool &is_same) const
{
  int ret = OB_SUCCESS;
  is_same = true;
  if (!param.is_pl_extend()) {
    is_same = false;
  } else if (param.get_meta().get_extend_type() != param_info.pl_type_) {
    is_same = false;
  } else if ((param_info.pl_type_ == pl::PL_NESTED_TABLE_TYPE ||
             param_info.pl_type_ == pl::PL_ASSOCIATIVE_ARRAY_TYPE ||
             param_info.pl_type_ == pl::PL_VARRAY_TYPE ||
             param_info.pl_type_ == pl::PL_RECORD_TYPE) &&
             OB_INVALID_ID != param_info.udt_id_) { // may be anonymous array
    const pl::ObPLComposite *composite =
            reinterpret_cast<const pl::ObPLComposite*>(param.get_ext());
    if (OB_ISNULL(composite)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("nested table is null", K(ret));
    } else if (param_info.udt_id_ != composite->get_id()) {
      is_same = false;
    }
  } else {
    ObDataType data_type;
    if (OB_FAIL(ObSQLUtils::get_ext_obj_data_type(param, data_type))) {
      LOG_WARN("fail to get obj data_type", K(ret), K(param));
    } else if (data_type.get_scale() == param_info.scale_ &&
                data_type.get_obj_type() == param_info.ext_real_type_) {
      is_same = true;
    } else {
      is_same = false;
    }
  }

  return ret;
}

int ObPLObjectValue::match_param_info(const ObPlParamInfo &param_info,
                                              const ObObjParam &param,
                                              bool &is_same) const
{
  int ret = OB_SUCCESS;
  is_same = true;

  if (param_info.flag_.need_to_check_type_) {
    if (lib::is_oracle_mode() &&
        param.get_param_meta().get_type() == ObCharType &&
        param.get_type() == ObNullType) {
    } else if (param.get_param_meta().get_type() != param.get_type()) {
      LOG_TRACE("differ in match param info",
                K(param.get_param_meta().get_type()),
                K(param.get_type()));
    }

    if (param.get_collation_type() != param_info.col_type_) {
      is_same = false;
    } else if (param.get_param_meta().get_type() != param_info.type_) {
      is_same = false;
    } else if (param.is_ext()) {
      ObDataType data_type;
      if (!param_info.flag_.need_to_check_extend_type_) {
        // do nothing
      } else if (OB_FAIL(match_complex_type_info(param_info, param, is_same))) {
        LOG_WARN("fail to match complex type info", K(ret), K(param), K(param_info));
      }
      LOG_DEBUG("ext match param info", K(data_type), K(param_info), K(is_same), K(ret));
    } else if (param_info.is_oracle_empty_string_ && !param.is_null()) { //Plain strings do not match the scheme of the empty string
      is_same = false;
    } else if (ObSQLUtils::is_oracle_empty_string(param)
               &&!param_info.is_oracle_empty_string_) { //Empty strings do not match the scheme of ordinary strings
      is_same = false;
    } else if (param_info.flag_.is_boolean_ != param.is_boolean()) { //bool type not match int type
      is_same = false;
    } else {
      is_same = (param.get_scale() == param_info.scale_);
    }
  }
  if (is_same && param_info.flag_.need_to_check_bool_value_) {
    bool is_value_true = false;
    if (OB_FAIL(ObObjEvaluator::is_true(param, is_value_true))) {
      SQL_PC_LOG(WARN, "fail to get param info", K(ret));
    } else if (is_value_true != param_info.flag_.expected_bool_value_) {
      is_same = false;
    }
  }
  return ret;
}

int ObPLObjectValue::match_params_info(const ParamStore *params,
                                       bool &is_same)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(params)) {
    is_same = true;
  } else if (params->count() > params_info_.count()) {
    is_same = false;
  } else {
    //match original param info
    int64_t N = params->count();
    LOG_DEBUG("params info", K(params_info_), K(*params), K(this));
    for (int64_t i = 0; OB_SUCC(ret) && is_same && i < N; ++i) {
      if (OB_FAIL(match_param_info(params_info_.at(i),
                                   params->at(i),
                                   is_same))) {
        LOG_WARN("fail to match param info", K(ret), K(params_info_), K(*params));
      }
    }

    if (OB_FAIL(ret)) {
      is_same = false;
    }
  }

  return ret;
}

int ObPLObjectSet::init(ObILibCacheCtx &ctx, const ObILibCacheObject *obj)
{
  int ret = OB_SUCCESS;
  ObPLCacheCtx& pc_ctx = static_cast<ObPLCacheCtx&>(ctx);

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(key_.deep_copy(allocator_, pc_ctx.key_))) {
    LOG_WARN("fail to init plan cache key in pcv set", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObPLObjectSet::create_new_pl_object_value(ObPLObjectValue *&pl_object_value)
{
  int ret = OB_SUCCESS;
  void *buff = nullptr;
  pl_object_value = nullptr;

  if (nullptr == (buff = allocator_.alloc(sizeof(ObPLObjectValue)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for ObPLObjectValue", K(ret));
  } else if (nullptr == (pl_object_value = new(buff)ObPLObjectValue(allocator_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to construct ObPLObjectValue", K(ret));
  } else {
    // do nothing
  }

  if (OB_SUCC(ret)) {
    // do nothing
  } else if (nullptr != pl_object_value) { // cleanup
    pl_object_value->~ObPLObjectValue();
    allocator_.free(pl_object_value);
    pl_object_value = nullptr;
  }

  return ret;
}

void ObPLObjectSet::free_pl_object_value(ObPLObjectValue *pl_object_value)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pl_object_value)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",K(ret));
  } else {
    pl_object_value->~ObPLObjectValue();
    allocator_.free(pl_object_value);
    pl_object_value = nullptr;
  }
}

void ObPLObjectSet::destroy()
{
  if (is_inited_) {
    while (!object_value_sets_.is_empty()) {
      ObPLObjectValue *pl_object_value= object_value_sets_.get_first();
      if (OB_ISNULL(pl_object_value)) {
        //do nothing;
      } else {
        object_value_sets_.remove(pl_object_value);
        free_pl_object_value(pl_object_value);
        pl_object_value = nullptr;
      }
    }

    key_.destory(allocator_);
    key_.reset();

    is_inited_ = false;
  }
}


int ObPLObjectSet::inner_get_cache_obj(ObILibCacheCtx &ctx,
                                        ObILibCacheKey *key,
                                        ObILibCacheObject *&cache_obj)
{
  int ret = OB_SUCCESS;

  ObPLCacheCtx& pc_ctx = static_cast<ObPLCacheCtx&>(ctx);
  pc_ctx.schema_guard_->set_session_id(pc_ctx.session_info_->get_sessid_for_table());
  ObSEArray<PCVPlSchemaObj, 4> schema_array;
  DLIST_FOREACH(pl_object_value, object_value_sets_) {
    schema_array.reset();
    int64_t new_tenant_schema_version = OB_INVALID_VERSION;
    bool need_check_schema = true;
    bool is_old_version = false;
    bool is_same = true;
    bool match_params = true;

    if (OB_FAIL(pl_object_value->get_all_dep_schema(pc_ctx,
                                        pc_ctx.session_info_->get_database_id(),
                                        new_tenant_schema_version,
                                        need_check_schema,
                                        schema_array))) {
      if (OB_OLD_SCHEMA_VERSION == ret) {
        LOG_WARN("old schema version, to be delete", K(ret), K(schema_array), KPC(pl_object_value));
      } else {
        LOG_WARN("failed to get all table schema", K(ret));
      }
    } else if (schema_array.count() != 0 && OB_FAIL(pl_object_value->match_dep_schema(pc_ctx, schema_array, is_same))) {
      LOG_WARN("failed to match_dep_schema", K(ret));
    } else if (!is_same) {
      ret = OB_OLD_SCHEMA_VERSION;
      LOG_WARN("old schema version, to be delete", K(ret), K(schema_array), KPC(pl_object_value));
    } else if (OB_FAIL(pl_object_value->check_value_version(pc_ctx.schema_guard_,
                                                            need_check_schema,
                                                            schema_array,
                                                            is_old_version))) {
      LOG_WARN("fail to check table version", K(ret));
    } else if (true == is_old_version) {
      ret = OB_OLD_SCHEMA_VERSION;
      LOG_WARN("old schema version, to be delete", K(ret), K(schema_array), KPC(pl_object_value));
    } else if (OB_FAIL(pl_object_value->match_params_info(pc_ctx.cache_params_, match_params))) {
      LOG_WARN("failed to match params info", K(ret));
    } else if (!match_params) {
      // do nothing
    } else {
      cache_obj = pl_object_value->pl_routine_obj_;
      cache_obj->set_dynamic_ref_handle(pc_ctx.handle_id_);
      if (OB_FAIL(pl_object_value->lift_tenant_schema_version(new_tenant_schema_version))) {
        LOG_WARN("failed to lift pcv's tenant schema version", K(ret));
      }
      break;
    }
  }
  if (OB_SUCC(ret) && nullptr == cache_obj) {
    ret = OB_SQL_PC_NOT_EXIST;
    LOG_WARN("failed to get cache obj in pl cache", K(ret));
  }
  return ret;
}

int ObPLObjectSet::inner_add_cache_obj(ObILibCacheCtx &ctx,
                                        ObILibCacheKey *key,
                                        ObILibCacheObject *cache_obj)
{
  int ret = OB_SUCCESS;

  ObPLCacheCtx& pc_ctx = static_cast<ObPLCacheCtx&>(ctx);
  pl::ObPLCacheObject *cache_object = static_cast<pl::ObPLCacheObject *>(cache_obj);
  ObSEArray<PCVPlSchemaObj, 4> schema_array;

  if (OB_ISNULL(cache_object)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null cache obj", K(ret));
  } else if (OB_UNLIKELY(!cache_object->is_prcr() &&
                         !cache_object->is_sfc() &&
                         !cache_object->is_pkg() &&
                         !cache_object->is_anon() &&
                         !cache_object->is_call_stmt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cache object is invalid", K(cache_object));
  } else if (OB_FAIL(ObPLObjectValue::get_all_dep_schema(*pc_ctx.schema_guard_,
                                                          cache_object->get_dependency_table(),
                                                          schema_array))) {
    LOG_WARN("failed to get all dep schema", K(ret));
  } else {
    DLIST_FOREACH(pl_object_value, object_value_sets_) {
      bool is_same = true;
      bool is_old_version = false;
      if (schema_array.count() != 0) {
        if (OB_FAIL(pl_object_value->match_dep_schema(pc_ctx, schema_array, is_same))) {
          LOG_WARN("failed to match_dep_schema", K(ret));
        } else if (!is_same) {
          ret = OB_OLD_SCHEMA_VERSION;
          LOG_WARN("old schema version, to be delete", K(ret), K(pl_object_value->pl_routine_obj_->get_object_id()));
        } else if (pl_object_value->check_value_version(pc_ctx.schema_guard_,
                                                true,
                                                schema_array,
                                                is_old_version)) {
          LOG_WARN("fail to check table version", K(ret));
        } else if (true == is_old_version) {
          ret = OB_OLD_SCHEMA_VERSION;
          LOG_WARN("old schema version, to be delete", K(ret), K(pl_object_value->pl_routine_obj_->get_object_id()));
        }
      }
      if (OB_SUCC(ret)) {
        if (true == pl_object_value->match_params_info(cache_object->get_params_info())) {
          ret = OB_SQL_PC_PLAN_DUPLICATE;
        }
      }
    }
  }

  /* if object_value_sets_ has a value which has different schema and schema version but same param info,
     it must report an error.
     so, if ret is 0, need to create new pl object value. */
  if (OB_SUCC(ret)) {
    ObPLObjectValue *pl_object_value = nullptr;
    if (OB_FAIL(create_new_pl_object_value(pl_object_value))) {
      LOG_WARN("fail to create new function value", K(ret));
    } else if (OB_UNLIKELY(nullptr == pl_object_value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (OB_FAIL(pl_object_value->init(*cache_obj, pc_ctx))) {
      LOG_WARN("failed to init pl function", K(ret));
    } else {
      bool is_old_version = false;
      if (pl_object_value->check_value_version(pc_ctx.schema_guard_,
                                                true,
                                                schema_array,
                                                is_old_version)) {
        LOG_WARN("fail to check table version", K(ret));
      } else if (true == is_old_version) {
        ret = OB_OLD_SCHEMA_VERSION;
        LOG_WARN("old schema version, to be delete", K(ret), K(pl_object_value->pl_routine_obj_->get_object_id()));
      } else {
        pl_object_value->pl_routine_obj_ = cache_object;
        pl_object_value->pl_routine_obj_->set_dynamic_ref_handle(PC_REF_PL_HANDLE);

        if (!object_value_sets_.add_last(pl_object_value)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to add pcv to object_value_sets_", K(ret));
          free_pl_object_value(pl_object_value);
          pl_object_value = nullptr;
        } else {
          // do nothing
        }
      }
    }
  }

  return ret;
}


int64_t ObPLObjectSet::get_mem_size()
{
  int64_t value_mem_size = 0;

  DLIST_FOREACH_NORET(pl_object_value, object_value_sets_) {
    if (OB_ISNULL(pl_object_value)) {
      BACKTRACE_RET(ERROR, OB_ERR_UNEXPECTED, true, "invalid pl_object_value");
    } else {
      value_mem_size += pl_object_value->get_mem_size();
    }
  } // end for
  return value_mem_size;
}

}
}
