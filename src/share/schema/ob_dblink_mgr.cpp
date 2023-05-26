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
#include "ob_dblink_mgr.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
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

ObDbLinkMgr::ObDbLinkMgr()
  : local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
    allocator_(local_allocator_),
    dblink_schemas_(0, NULL, SET_USE_500("DBLINK_MGR", ObCtxIds::SCHEMA_SERVICE)),
    dblink_id_map_(SET_USE_500("DBLINK_MGR", ObCtxIds::SCHEMA_SERVICE)),
    dblink_name_map_(SET_USE_500("DBLINK_MGR", ObCtxIds::SCHEMA_SERVICE)),
    is_inited_(false)
{}

ObDbLinkMgr::ObDbLinkMgr(ObIAllocator &allocator)
  : local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
    allocator_(allocator),
    dblink_schemas_(0, NULL, SET_USE_500("DBLINK_MGR", ObCtxIds::SCHEMA_SERVICE)),
    dblink_id_map_(SET_USE_500("DBLINK_MGR", ObCtxIds::SCHEMA_SERVICE)),
    dblink_name_map_(SET_USE_500("DBLINK_MGR", ObCtxIds::SCHEMA_SERVICE)),
    is_inited_(false)
{}

ObDbLinkMgr::~ObDbLinkMgr()
{}

int ObDbLinkMgr::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dblink_id_map_.init())) {
    LOG_WARN("init dblink id map failed", K(ret));
  } else if (OB_FAIL(dblink_name_map_.init())) {
    LOG_WARN("init dblink name map failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObDbLinkMgr::reset()
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    // reset will not release memory for vector, use clear()
    dblink_schemas_.clear();
    dblink_id_map_.clear();
    dblink_name_map_.clear();
  }
}

ObDbLinkMgr &ObDbLinkMgr::operator=(const ObDbLinkMgr &other)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(assign(other))) {
    LOG_WARN("assign failed", K(ret));
  }
  return *this;
}

int ObDbLinkMgr::assign(const ObDbLinkMgr &other)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (this != &other) {
    reset();
    #define ASSIGN_FIELD(x)                        \
      if (OB_SUCC(ret)) {                          \
        if (OB_FAIL(x.assign(other.x))) {          \
          LOG_WARN("assign " #x "failed", K(ret)); \
        }                                          \
      }
    ASSIGN_FIELD(dblink_schemas_);
    ASSIGN_FIELD(dblink_id_map_);
    ASSIGN_FIELD(dblink_name_map_);
    #undef ASSIGN_FIELD
  }
  return ret;
}

int ObDbLinkMgr::deep_copy(const ObDbLinkMgr &other)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (this != &other) {
    reset();
    for (DbLinkIter iter = other.dblink_schemas_.begin();
         OB_SUCC(ret) && iter != other.dblink_schemas_.end(); iter++) {
      ObDbLinkSchema *dblink = *iter;
      if (OB_ISNULL(dblink)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(dblink), K(ret));
      } else if (OB_FAIL(add_dblink(*dblink))) {
        LOG_WARN("add dblink failed", K(*dblink), K(ret));
      }
    }
  }
  return ret;
}

bool ObDbLinkMgr::check_inner_stat() const
{
  return is_inited_;
}

bool ObDbLinkMgr::compare_dblink(const ObDbLinkSchema *lhs, const ObDbLinkSchema *rhs)
{
  return lhs->get_tenant_dblink_id() < rhs->get_tenant_dblink_id();
}

bool ObDbLinkMgr::equal_dblink(const ObDbLinkSchema *lhs, const ObDbLinkSchema *rhs)
{
  return lhs->get_tenant_dblink_id() == rhs->get_tenant_dblink_id();
}

bool ObDbLinkMgr::compare_with_tenant_dblink_id(const ObDbLinkSchema *lhs,
                                                const ObTenantDbLinkId &tenant_dblink_id)
{
  return NULL != lhs ? (lhs->get_tenant_dblink_id() < tenant_dblink_id) : false;
}

bool ObDbLinkMgr::equal_with_tenant_dblink_id(const ObDbLinkSchema *lhs,
                                              const ObTenantDbLinkId &tenant_dblink_id)
{
  return NULL != lhs ? (lhs->get_tenant_dblink_id() == tenant_dblink_id) : false;
}

int ObDbLinkMgr::get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const
{
  int ret = OB_SUCCESS;
  schema_info.reset();
  schema_info.schema_type_ = SYNONYM_SCHEMA;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    schema_info.count_ = dblink_schemas_.size();
    for (ConstDbLinkIter it = dblink_schemas_.begin(); OB_SUCC(ret) && it != dblink_schemas_.end(); it++) {
      if (OB_ISNULL(*it)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema is null", K(ret));
      } else {
        schema_info.size_ += (*it)->get_convert_size();
      }
    }
  }
  return ret;
}

int ObDbLinkMgr::add_dblinks(const ObIArray<ObDbLinkSchema> &dblink_schemas)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    FOREACH_CNT_X(dblink_schema, dblink_schemas, OB_SUCC(ret)) {
      if (OB_FAIL(add_dblink(*dblink_schema))) {
        LOG_WARN("add dblink failed", K(ret), K(*dblink_schema));
      }
    }
  }
  return ret;
}

int ObDbLinkMgr::del_dblinks(const ObIArray<ObTenantDbLinkId> &tenant_dblink_ids)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    FOREACH_CNT_X(tenant_dblink_id, tenant_dblink_ids, OB_SUCC(ret)) {
      if (OB_FAIL(del_dblink(*tenant_dblink_id))) {
        LOG_WARN("del dblink failed", K(ret), K(*tenant_dblink_id));
      }
    }
  }
  return ret;
}

int ObDbLinkMgr::add_dblink(const ObDbLinkSchema &dblink_schema)
{
  int ret = OB_SUCCESS;
  ObDbLinkSchema *new_dblink_schema = NULL;
  ObDbLinkSchema *replaced_schema = NULL;
  DbLinkIter iter = NULL;
  int over_write = 1;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!dblink_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dblink_schema));
  } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator_, dblink_schema, new_dblink_schema))) {
    LOG_WARN("alloc schema failed", K(ret));
  } else if (OB_ISNULL(new_dblink_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(new_dblink_schema));
  } else if (OB_FAIL(dblink_schemas_.replace(new_dblink_schema,
                                             iter,
                                             compare_dblink,
                                             equal_dblink,
                                             replaced_schema))) {
    LOG_WARN("failed to add dblink schema", K(ret));
  }
  if (OB_SUCC(ret)) {
    uint64_t dblink_id = new_dblink_schema->get_dblink_id();
    if (OB_FAIL(dblink_id_map_.set_refactored(dblink_id,
                                              new_dblink_schema, over_write))) {
      if (OB_HASH_EXIST != ret) {
        LOG_WARN("build dblink id hashmap failed", K(ret),
                 "dblink_id", dblink_id);
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObDbLinkNameHashWrapper name_wrapper(new_dblink_schema->get_tenant_id(),
                                         new_dblink_schema->get_dblink_name());
    if (OB_FAIL(dblink_name_map_.set_refactored(name_wrapper, new_dblink_schema, over_write))) {
      if (OB_HASH_EXIST != ret) {
        LOG_WARN("build dblink name hashmap failed", K(ret),
                 "dblink_name", new_dblink_schema->get_dblink_name());
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  // ignore ret
  if (dblink_schemas_.count() != dblink_id_map_.item_count() ||
      dblink_schemas_.count() != dblink_name_map_.item_count()) {
    LOG_WARN("dblink info is non-consistent",
             K(dblink_schemas_.count()),
             K(dblink_id_map_.item_count()),
             K(dblink_name_map_.item_count()),
             K(dblink_schema));
    int rebuild_ret = rebuild_dblink_hashmap();
    if (OB_SUCCESS != rebuild_ret){
      LOG_WARN("rebuild dblink hashmap failed", K(rebuild_ret));
    }
  }
  return ret;
}

int ObDbLinkMgr::del_dblink(const ObTenantDbLinkId &tenant_dblink_id)
{
  int ret = OB_SUCCESS;
  ObDbLinkSchema *schema_to_del = NULL;
  ObDbLinkNameHashWrapper name_wrapper;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!tenant_dblink_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_dblink_id));
  } else if (OB_FAIL(dblink_schemas_.remove_if(tenant_dblink_id,
                                               compare_with_tenant_dblink_id,
                                               equal_with_tenant_dblink_id,
                                               schema_to_del))) {
    LOG_WARN("failed to remove dblink schema", K(tenant_dblink_id), K(ret));
  } else if (OB_ISNULL(schema_to_del)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("removed dblink schema return NULL, ", K(tenant_dblink_id), K(ret));
  } else if (OB_FAIL(dblink_id_map_.erase_refactored(schema_to_del->get_dblink_id()))) {
    LOG_WARN("failed delete dblink from dblink id hashmap", K(*schema_to_del), K(ret));
  } else if (FALSE_IT(name_wrapper.set_tenant_id(schema_to_del->get_tenant_id()))) {
  } else if (FALSE_IT(name_wrapper.set_dblink_name(schema_to_del->get_dblink_name()))) {
  } else if (OB_FAIL(dblink_name_map_.erase_refactored(name_wrapper))) {
    LOG_WARN("failed delete dblink from dblink name hashmap", K(*schema_to_del), K(ret));
  }
  // ignore ret
  if (dblink_schemas_.count() != dblink_id_map_.item_count() ||
      dblink_schemas_.count() != dblink_name_map_.item_count()) {
    LOG_WARN("dblink info is non-consistent",
             K(dblink_schemas_.count()),
             K(dblink_id_map_.item_count()),
             K(dblink_name_map_.item_count()));
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = rebuild_dblink_hashmap())){
      LOG_WARN("rebuild dblink hashmap failed", K(tmp_ret));
    }
  }
  return ret;
}

int ObDbLinkMgr::get_dblink_schema(
    const uint64_t dblink_id,
    const ObDbLinkSchema *&dblink_schema) const
{
  int ret = OB_SUCCESS;
  ObDbLinkSchema *tmp_schema = NULL;
  dblink_schema = NULL;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == dblink_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dblink_id));
  } else if (OB_FAIL(dblink_id_map_.get_refactored(dblink_id, tmp_schema))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("get dblink with id failed", K(ret));
    }
  } else if (OB_ISNULL(tmp_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(tmp_schema));
  } else {
    dblink_schema = tmp_schema;
  }
  return ret;
}

int ObDbLinkMgr::get_dblink_schema(const uint64_t tenant_id, const ObString &dblink_name,
                                   const ObDbLinkSchema *&dblink_schema) const
{
  int ret = OB_SUCCESS;
  ObDbLinkNameHashWrapper name_wrapper;
  ObDbLinkSchema *tmp_schema = NULL;
  dblink_schema = NULL;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || dblink_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(dblink_name));
  } else if (FALSE_IT(name_wrapper.set_tenant_id(tenant_id))) {
  } else if (FALSE_IT(name_wrapper.set_dblink_name(dblink_name))) {
  } else if (OB_FAIL(dblink_name_map_.get_refactored(name_wrapper, tmp_schema))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("get dblink with name failed", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_ISNULL(tmp_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(tmp_schema));
  } else {
    dblink_schema = tmp_schema;
  }
  return ret;
}

int ObDbLinkMgr::get_dblink_schemas_in_tenant(const uint64_t tenant_id,
                                              ObIArray<const ObDbLinkSchema *> &dblink_schemas) const
{
  int ret = OB_SUCCESS;
  dblink_schemas.reset();
  ObTenantDbLinkId tenant_dblink_id_lower(tenant_id, OB_MIN_ID);
  ConstDbLinkIter tenant_dblink_begin =
      dblink_schemas_.lower_bound(tenant_dblink_id_lower, compare_with_tenant_dblink_id);
  bool is_stop = false;
  for (ConstDbLinkIter iter = tenant_dblink_begin;
      OB_SUCC(ret) && iter != dblink_schemas_.end() && !is_stop; ++iter) {
    const ObDbLinkSchema *dblink = NULL;
    if (OB_ISNULL(dblink = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(dblink));
    } else if (tenant_id != dblink->get_tenant_id()) {
      is_stop = true;
    } else if (OB_FAIL(dblink_schemas.push_back(dblink))) {
      LOG_WARN("push back dblink failed", K(ret));
    }
  }
  return ret;
}

int ObDbLinkMgr::del_dblink_schemas_in_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObArray<const ObDbLinkSchema *> schemas;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(get_dblink_schemas_in_tenant(tenant_id, schemas))) {
    LOG_WARN("get dblink schemas failed", K(ret), K(tenant_id));
  } else {
    FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
      ObTenantDbLinkId tenant_dblink_id(tenant_id, (*schema)->get_dblink_id());
      if (OB_FAIL(del_dblink(tenant_dblink_id))) {
        LOG_WARN("del dblink failed", K(tenant_dblink_id), K(ret));
      }
    }
  }
  return ret;
}

int ObDbLinkMgr::get_dblink_schema_count(int64_t &schema_count) const
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    schema_count = dblink_schemas_.size();
  }
  return ret;
}

int ObDbLinkMgr::rebuild_dblink_hashmap()
{
  int ret = OB_SUCCESS;
  int over_write = 1;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    dblink_id_map_.clear();
    dblink_name_map_.clear();
  }
  for (ConstDbLinkIter iter = dblink_schemas_.begin(); iter != dblink_schemas_.end() && OB_SUCC(ret); ++iter) {
    ObDbLinkSchema *dblink_schema = *iter;
    ObDbLinkNameHashWrapper name_wrapper;
    if (OB_ISNULL(dblink_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dblink_schema is NULL", K(ret), K(dblink_schema));
    } else if (OB_FAIL(dblink_id_map_.set_refactored(dblink_schema->get_dblink_id(),
                                                     dblink_schema, over_write))) {
      LOG_WARN("build dblink id hashmap failed", K(ret),
               "dblink_id", dblink_schema->get_dblink_id());
    } else if (FALSE_IT(name_wrapper.set_tenant_id(dblink_schema->get_tenant_id()))) {
    } else if (FALSE_IT(name_wrapper.set_dblink_name(dblink_schema->get_dblink_name()))) {
    } else if (OB_FAIL(dblink_name_map_.set_refactored(name_wrapper, dblink_schema, over_write))) {
      LOG_WARN("build dblink name hashmap failed", K(ret),
               "dblink_name", dblink_schema->get_dblink_name());
    }
  }
  return ret;
}

} //end of namespace schema
} //end of namespace share
} //end of namespace oceanbase
