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
#include "ob_outline_mgr.h"
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

ObSimpleOutlineSchema::ObSimpleOutlineSchema()
  : ObSchema()
{
  reset();
}

ObSimpleOutlineSchema::ObSimpleOutlineSchema(ObIAllocator *allocator)
  : ObSchema(allocator)
{
  reset();
}

ObSimpleOutlineSchema::ObSimpleOutlineSchema(const ObSimpleOutlineSchema &other)
  : ObSchema()
{
  reset();
  *this = other;
}

ObSimpleOutlineSchema::~ObSimpleOutlineSchema()
{
}

ObSimpleOutlineSchema &ObSimpleOutlineSchema::operator =(const ObSimpleOutlineSchema &other)
{
  if (this != &other) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = other.error_ret_;
    tenant_id_ = other.tenant_id_;
    outline_id_ = other.outline_id_;
    schema_version_ = other.schema_version_;
    database_id_ = other.database_id_;
    if (OB_FAIL(deep_copy_str(other.name_, name_))) {
      LOG_WARN("Fail to deep copy outline name", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.signature_, signature_))) {
      LOG_WARN("Fail to deep copy signature", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.sql_id_, sql_id_))) {
      LOG_WARN("Fail to deep copy sql_id", K(ret));
    }

    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }

  return *this;
}

bool ObSimpleOutlineSchema::operator ==(const ObSimpleOutlineSchema &other) const
{
  bool ret = false;

  if (tenant_id_ == other.tenant_id_ &&
      outline_id_ == other.outline_id_ &&
      schema_version_ == other.schema_version_ &&
      database_id_ == other.database_id_ &&
      name_ == other.name_ &&
      signature_ == other.signature_ &&
      sql_id_ == other.sql_id_) {
    ret = true;
  }

  return ret;
}

void ObSimpleOutlineSchema::reset()
{
  ObSchema::reset();
  tenant_id_ = OB_INVALID_ID;
  outline_id_ = OB_INVALID_ID;
  schema_version_ = OB_INVALID_VERSION;
  database_id_ = OB_INVALID_ID;
  name_.reset();
  signature_.reset();
  sql_id_.reset();
}

bool ObSimpleOutlineSchema::is_valid() const
{
  bool ret = true;
  if (OB_INVALID_ID == tenant_id_ ||
      OB_INVALID_ID == outline_id_ ||
      schema_version_ < 0 ||
      OB_INVALID_ID == database_id_ ||
      name_.empty() ||
      (signature_.empty() && sql_id_.empty())) {
    ret = false;
  }
  return ret;
}

int64_t ObSimpleOutlineSchema::get_convert_size() const
{
  int64_t convert_size = 0;

  convert_size += sizeof(ObSimpleOutlineSchema);
  convert_size += name_.length() + 1;
  convert_size += signature_.length() + 1;
  convert_size += sql_id_.length() + 1;

  return convert_size;
}

ObOutlineMgr::ObOutlineMgr()
    : local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
      allocator_(local_allocator_),
      outline_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_OUTLINE_INFO_VECTOR, ObCtxIds::SCHEMA_SERVICE)),
      outline_id_map_(SET_USE_500(ObModIds::OB_SCHEMA_OUTLINE_ID_MAP, ObCtxIds::SCHEMA_SERVICE)),
      outline_name_map_(SET_USE_500(ObModIds::OB_SCHEMA_OUTLINE_NAME_MAP, ObCtxIds::SCHEMA_SERVICE)),
      signature_map_(SET_USE_500(ObModIds::OB_SCHEMA_OUTLINE_SQL_MAP, ObCtxIds::SCHEMA_SERVICE)),
      sql_id_map_(SET_USE_500(ObModIds::OB_SCHEMA_OUTLINE_SQL_MAP, ObCtxIds::SCHEMA_SERVICE))
{
}

ObOutlineMgr::ObOutlineMgr(ObIAllocator &allocator)
    : local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
      allocator_(allocator),
      outline_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_OUTLINE_INFO_VECTOR, ObCtxIds::SCHEMA_SERVICE)),
      outline_id_map_(SET_USE_500(ObModIds::OB_SCHEMA_OUTLINE_ID_MAP, ObCtxIds::SCHEMA_SERVICE)),
      outline_name_map_(SET_USE_500(ObModIds::OB_SCHEMA_OUTLINE_NAME_MAP, ObCtxIds::SCHEMA_SERVICE)),
      signature_map_(SET_USE_500(ObModIds::OB_SCHEMA_OUTLINE_SQL_MAP, ObCtxIds::SCHEMA_SERVICE)),
      sql_id_map_(SET_USE_500(ObModIds::OB_SCHEMA_OUTLINE_SQL_MAP, ObCtxIds::SCHEMA_SERVICE))
{
}

ObOutlineMgr::~ObOutlineMgr()
{
}

int ObOutlineMgr::init()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(outline_id_map_.init())) {
    LOG_WARN("init outline id map failed", K(ret));
  } else if (OB_FAIL(outline_name_map_.init())) {
    LOG_WARN("init outline name map failed", K(ret));
  } else if (OB_FAIL(signature_map_.init())) {
    LOG_WARN("init signature map failed", K(ret));
  } else if (OB_FAIL(sql_id_map_.init())) {
    LOG_WARN("init signature map failed", K(ret));
  }


  return ret;
}

void ObOutlineMgr::reset()
{
  int ret = OB_SUCCESS;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    // reset will not release memory for vector, use clear()
    outline_infos_.clear();

    outline_id_map_.clear();
    outline_name_map_.clear();
    signature_map_.clear();
    sql_id_map_.clear();
  }
}

ObOutlineMgr &ObOutlineMgr::operator =(const ObOutlineMgr &other)
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

int ObOutlineMgr::assign(const ObOutlineMgr &other)
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
    ASSIGN_FIELD(outline_infos_);
    ASSIGN_FIELD(outline_id_map_);
    ASSIGN_FIELD(outline_name_map_);
    ASSIGN_FIELD(signature_map_);
    ASSIGN_FIELD(sql_id_map_);
    #undef ASSIGN_FIELD
  }

  return ret;
}

int ObOutlineMgr::deep_copy(const ObOutlineMgr &other)
{
  int ret = OB_SUCCESS;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (this != &other) {
    reset();
    for (OutlineIter iter = other.outline_infos_.begin();
       OB_SUCC(ret) && iter != other.outline_infos_.end(); iter++) {
      ObSimpleOutlineSchema *outline = *iter;
      if (OB_ISNULL(outline)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(outline), K(ret));
      } else if (OB_FAIL(add_outline(*outline))) {
        LOG_WARN("add outline failed", K(*outline), K(ret));
      }
    }
  }

  return ret;
}

bool ObOutlineMgr::check_inner_stat() const
{
  bool ret = true;
  return ret;
}

bool ObOutlineMgr::compare_outline(const ObSimpleOutlineSchema *lhs, const ObSimpleOutlineSchema *rhs)
{
  return lhs->get_tenant_outline_id() < rhs->get_tenant_outline_id();
}

bool ObOutlineMgr::equal_outline(const ObSimpleOutlineSchema *lhs,
                                const ObSimpleOutlineSchema *rhs)
{
  return lhs->get_tenant_outline_id() == rhs->get_tenant_outline_id();
}

bool ObOutlineMgr::compare_with_tenant_outline_id(const ObSimpleOutlineSchema *lhs,
                                                 const ObTenantOutlineId &tenant_outline_id)
{
  return NULL != lhs ? (lhs->get_tenant_outline_id() < tenant_outline_id) : false;
}

bool ObOutlineMgr::equal_with_tenant_outline_id(const ObSimpleOutlineSchema *lhs,
                                               const ObTenantOutlineId &tenant_outline_id)
{
  return NULL != lhs ? (lhs->get_tenant_outline_id() == tenant_outline_id) : false;
}

int ObOutlineMgr::add_outlines(const ObIArray<ObSimpleOutlineSchema> &outline_schemas)
{
  int ret = OB_SUCCESS;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    FOREACH_CNT_X(outline_schema, outline_schemas, OB_SUCC(ret)) {
      if (OB_FAIL(add_outline(*outline_schema))) {
        LOG_WARN("add outline failed", K(ret),
                 "outline_schema", *outline_schema);
      }
    }
  }

  return ret;
}

int ObOutlineMgr::del_outlines(const ObIArray<ObTenantOutlineId> &outlines)
{
  int ret = OB_SUCCESS;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    FOREACH_CNT_X(outline, outlines, OB_SUCC(ret)) {
      if (OB_FAIL(del_outline(*outline))) {
        LOG_WARN("del outline failed", K(ret),
                 "tenant_id", outline->tenant_id_,
                 "outline_id", outline->outline_id_);
      }
    }
  }

  return ret;
}

int ObOutlineMgr::add_outline(const ObSimpleOutlineSchema &outline_schema)
{
  int ret = OB_SUCCESS;

  ObSimpleOutlineSchema *new_outline_schema = NULL;
  OutlineIter iter = NULL;
  ObSimpleOutlineSchema *replaced_outline = NULL;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!outline_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(outline_schema));
  } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator_, outline_schema, new_outline_schema))) {
    LOG_WARN("alloc schema failed", K(ret));
  } else if (OB_ISNULL(new_outline_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(new_outline_schema));
  } else if (OB_FAIL(outline_infos_.replace(new_outline_schema,
                                           iter,
                                           compare_outline,
                                           equal_outline,
                                           replaced_outline))) {
    LOG_WARN("failed to add outline schema", K(ret));
  } else {
    int over_write = 1;
    int hash_ret = outline_id_map_.set_refactored(new_outline_schema->get_outline_id(),
                                                  new_outline_schema,
                                                  over_write);
    if (OB_SUCCESS != hash_ret && OB_HASH_EXIST != hash_ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("build outline id hashmap failed", K(ret), K(hash_ret),
               "outline_id", new_outline_schema->get_outline_id());
    }
    if (OB_SUCC(ret)) {
      ObOutlineNameHashWrapper name_wrapper(new_outline_schema->get_tenant_id(),
                                                    new_outline_schema->get_database_id(),
                                                    new_outline_schema->get_name_str());
      hash_ret = outline_name_map_.set_refactored(name_wrapper, new_outline_schema,
                                                  over_write);
      if (OB_SUCCESS != hash_ret && OB_HASH_EXIST != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("build outline name hashmap failed", K(ret), K(hash_ret),
                 "outline_id", new_outline_schema->get_outline_id(),
                 "name", new_outline_schema->get_name());
      }
    }
    if (OB_SUCC(ret)) {
      if (0 != new_outline_schema->get_signature_str().length()) {
        ObOutlineSignatureHashWrapper outline_signature_wrapper(new_outline_schema->get_tenant_id(),
                                                                new_outline_schema->get_database_id(),
                                                                new_outline_schema->get_signature_str());
        hash_ret = signature_map_.set_refactored(outline_signature_wrapper,
                                                 new_outline_schema, over_write);
        if (OB_SUCCESS != hash_ret && OB_HASH_EXIST != hash_ret) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("build outline signature hashmap failed", K(ret), K(hash_ret),
                   "outline_id", new_outline_schema->get_outline_id(),
                   "outline_signature", new_outline_schema->get_signature());
        }
      } else {
        ObOutlineSqlIdHashWrapper outline_sql_id_wrapper(new_outline_schema->get_tenant_id(),
                                                                new_outline_schema->get_database_id(),
                                                                new_outline_schema->get_sql_id_str());
        hash_ret = sql_id_map_.set_refactored(outline_sql_id_wrapper,
                                                 new_outline_schema, over_write);
        if (OB_SUCCESS != hash_ret && OB_HASH_EXIST != hash_ret) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("build outline signature hashmap failed", K(ret), K(hash_ret),
                   "outline_id", new_outline_schema->get_outline_id(),
                   "outline_sql_id", new_outline_schema->get_sql_id_str());
        }
      }
    }
  }
  // ignore ret
  if (outline_infos_.count() != outline_id_map_.item_count() ||
      outline_infos_.count() != outline_name_map_.item_count() ||
      outline_infos_.count() != (signature_map_.item_count() + sql_id_map_.item_count())) {
    LOG_WARN("outline info is non-consistent",
             "outline_infos_count",
             outline_infos_.count(),
             "outline_id_map_item_count",
             outline_id_map_.item_count(),
             "outline_name_map_item_count",
             outline_name_map_.item_count(),
             "signature_map_item_count",
             signature_map_.item_count(),
             "sql_id_map_item_count",
             sql_id_map_.item_count(),
             "outline_id",
             outline_schema.get_outline_id(),
             "name",
             outline_schema.get_name());
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = rebuild_outline_hashmap())){
      LOG_WARN("rebuild outline hashmap failed", K(tmp_ret));
    }
  }

  return ret;
}

int ObOutlineMgr::del_outline(const ObTenantOutlineId &outline)
{
  int ret = OB_SUCCESS;

  ObSimpleOutlineSchema *schema_to_del = NULL;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!outline.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(outline));
  } else if (OB_FAIL(outline_infos_.remove_if(outline,
                                              compare_with_tenant_outline_id,
                                              equal_with_tenant_outline_id,
                                              schema_to_del))) {
    LOG_WARN("failed to remove outline schema, ",
             "tenant_id",
             outline.tenant_id_,
             "outline_id",
             outline.outline_id_,
             K(ret));
  } else if (OB_ISNULL(schema_to_del)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("removed outline schema return NULL, ",
             "tenant_id",
             outline.tenant_id_,
             "outline_id",
             outline.outline_id_,
             K(ret));
  } else {
    int hash_ret = outline_id_map_.erase_refactored(schema_to_del->get_outline_id());
    if (OB_SUCCESS != hash_ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed delete outline from outline id hashmap, ",
               "hash_ret", hash_ret,
               "outline_id", schema_to_del->get_outline_id());
    }
    if (OB_SUCC(ret)) {
      ObOutlineNameHashWrapper name_wrapper(schema_to_del->get_tenant_id(),
                                            schema_to_del->get_database_id(),
                                            schema_to_del->get_name_str());
      hash_ret = outline_name_map_.erase_refactored(name_wrapper);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed delete outline from outline name hashmap, ",
                 K(ret),
                 K(hash_ret),
                 "tenant_id", schema_to_del->get_tenant_id(),
                 "database_id", schema_to_del->get_database_id(),
                 "name", schema_to_del->get_name());
      }
    }
    if (OB_SUCC(ret)) {
      if (0 != schema_to_del->get_signature_str().length()) {
        ObOutlineSignatureHashWrapper outline_signature_wrapper(schema_to_del->get_tenant_id(),
                                                                schema_to_del->get_database_id(),
                                                                schema_to_del->get_signature_str());
        hash_ret = signature_map_.erase_refactored(outline_signature_wrapper);
        if (OB_SUCCESS != hash_ret) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed delete outline from signature hashmap, ",
                   K(ret),
                   K(hash_ret),
                   "tenant_id", schema_to_del->get_tenant_id(),
                   "database_id", schema_to_del->get_database_id(),
                   "signature", schema_to_del->get_signature());
        }
      } else {
        ObOutlineSqlIdHashWrapper outline_sql_id_wrapper(schema_to_del->get_tenant_id(),
                                                                schema_to_del->get_database_id(),
                                                                schema_to_del->get_sql_id_str());
        hash_ret = sql_id_map_.erase_refactored(outline_sql_id_wrapper);
        if (OB_SUCCESS != hash_ret) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed delete outline from signature hashmap, ",
                   K(ret),
                   K(hash_ret),
                   "tenant_id", schema_to_del->get_tenant_id(),
                   "database_id", schema_to_del->get_database_id(),
                   "sql_id", schema_to_del->get_sql_id_str());
        }
      }
    }
  }
  // ignore ret
  if (outline_infos_.count() != outline_id_map_.item_count() ||
      outline_infos_.count() != outline_name_map_.item_count() ||
      outline_infos_.count() != (signature_map_.item_count() + sql_id_map_.item_count())) {
    LOG_WARN("outline info is non-consistent",
             "outline_infos_count",
             outline_infos_.count(),
             "outline_id_map_item_count",
             outline_id_map_.item_count(),
             "outline_name_map_item_count",
             outline_name_map_.item_count(),
             "signature_map_item_count",
             signature_map_.item_count(),
             "sql_id_map_item_count",
             sql_id_map_.item_count(),
             "outline_id",
             outline.outline_id_);
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = rebuild_outline_hashmap())){
      LOG_WARN("rebuild outline hashmap failed", K(tmp_ret));
    }
  }

  return ret;
}

int ObOutlineMgr::get_outline_schema(const uint64_t outline_id,
                                     const ObSimpleOutlineSchema *&outline_schema) const
{
  int ret = OB_SUCCESS;
  outline_schema = NULL;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == outline_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(outline_id));
  } else {
    ObSimpleOutlineSchema *tmp_schema = NULL;
    int hash_ret = outline_id_map_.get_refactored(outline_id, tmp_schema);
    if (OB_SUCCESS == hash_ret) {
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(tmp_schema));
      } else {
        outline_schema = tmp_schema;
      }
    }
  }

  return ret;
}

int ObOutlineMgr::get_outline_schema_with_name(
  const uint64_t tenant_id,
  const uint64_t database_id,
  const ObString &name,
  const ObSimpleOutlineSchema *&outline_schema) const
{
  int ret = OB_SUCCESS;
  outline_schema = NULL;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id ||
             OB_INVALID_ID == database_id ||
             name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(database_id), K(name));
  } else {
    ObSimpleOutlineSchema *tmp_schema = NULL;
    const ObOutlineNameHashWrapper name_wrapper(tenant_id, database_id, name);
    int hash_ret = outline_name_map_.get_refactored(name_wrapper, tmp_schema);
    if (OB_SUCCESS == hash_ret) {
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(tmp_schema));
      } else {
        outline_schema = tmp_schema;
      }
    }
  }

  return ret;
}

int ObOutlineMgr::get_outline_schema_with_signature(
  const uint64_t tenant_id,
  const uint64_t database_id,
  const ObString &signature,
  const ObSimpleOutlineSchema *&outline_schema) const
{
  int ret = OB_SUCCESS;
  outline_schema = NULL;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id ||
             OB_INVALID_ID == database_id ||
             signature.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(database_id), K(signature));
  } else {
    ObSimpleOutlineSchema *tmp_schema = NULL;
    const ObOutlineSignatureHashWrapper outline_signature_wrapper(tenant_id, database_id,
                                                                  signature);
    int hash_ret = signature_map_.get_refactored(outline_signature_wrapper, tmp_schema);
    if (OB_SUCCESS == hash_ret) {
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(tmp_schema));
      } else {
        outline_schema = tmp_schema;
      }
    }
  }

  return ret;
}

int ObOutlineMgr::get_outline_schema_with_sql_id(
  const uint64_t tenant_id,
  const uint64_t database_id,
  const ObString &sql_id,
  const ObSimpleOutlineSchema *&outline_schema) const
{
  int ret = OB_SUCCESS;
  outline_schema = NULL;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id ||
             OB_INVALID_ID == database_id ||
             sql_id.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(database_id), K(sql_id));
  } else {
    ObSimpleOutlineSchema *tmp_schema = NULL;
    const ObOutlineSqlIdHashWrapper outline_sql_id_wrapper(tenant_id, database_id,
                                                                  sql_id);
    int hash_ret = sql_id_map_.get_refactored(outline_sql_id_wrapper, tmp_schema);
    if (OB_SUCCESS == hash_ret) {
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(tmp_schema));
      } else {
        outline_schema = tmp_schema;
      }
    }
  }

  return ret;
}

int ObOutlineMgr::get_outline_schemas_in_tenant(const uint64_t tenant_id,
    ObIArray<const ObSimpleOutlineSchema *> &outline_schemas) const
{
  int ret = OB_SUCCESS;
  outline_schemas.reset();

  ObTenantOutlineId tenant_outine_id_lower(tenant_id, OB_MIN_ID);
  ConstOutlineIter tenant_outline_begin =
      outline_infos_.lower_bound(tenant_outine_id_lower, compare_with_tenant_outline_id);
  bool is_stop = false;
  for (ConstOutlineIter iter = tenant_outline_begin;
      OB_SUCC(ret) && iter != outline_infos_.end() && !is_stop; ++iter) {
    const ObSimpleOutlineSchema *outline = NULL;
    if (OB_ISNULL(outline = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(outline));
    } else if (tenant_id != outline->get_tenant_id()) {
      is_stop = true;
    } else if (OB_FAIL(outline_schemas.push_back(outline))) {
      LOG_WARN("push back outline failed", K(ret));
    }
  }

  return ret;
}

int ObOutlineMgr::get_outline_schemas_in_database(const uint64_t tenant_id,
    const uint64_t database_id, ObIArray<const ObSimpleOutlineSchema *> &outline_schemas) const
{
  int ret = OB_SUCCESS;
  outline_schemas.reset();

  ObTenantOutlineId tenant_outine_id_lower(tenant_id, OB_MIN_ID);
  ConstOutlineIter tenant_outline_begin =
      outline_infos_.lower_bound(tenant_outine_id_lower, compare_with_tenant_outline_id);
  bool is_stop = false;
  for (ConstOutlineIter iter = tenant_outline_begin;
      OB_SUCC(ret) && iter != outline_infos_.end() && !is_stop; ++iter) {
    const ObSimpleOutlineSchema *outline = NULL;
    if (OB_ISNULL(outline = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(outline));
    } else if (tenant_id != outline->get_tenant_id()) {
      is_stop = true;
    } else if (outline->get_database_id() != database_id) {
      // do-nothing
    } else if (OB_FAIL(outline_schemas.push_back(outline))) {
      LOG_WARN("push back outline failed", K(ret));
    }
  }

  return ret;
}

int ObOutlineMgr::del_schemas_in_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    ObArray<const ObSimpleOutlineSchema *> schemas;
    if (OB_FAIL(get_outline_schemas_in_tenant(tenant_id, schemas))) {
      LOG_WARN("get outline schemas failed", K(ret), K(tenant_id));
    } else {
      FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
        ObTenantOutlineId tenant_outline_id(tenant_id,
          (*schema)->get_outline_id());
        if (OB_FAIL(del_outline(tenant_outline_id))) {
          LOG_WARN("del outlne failed",
                   "tenant_id", tenant_outline_id.tenant_id_,
                   "outline_id", tenant_outline_id.outline_id_,
                   K(ret));
        }
      }
    }
  }

  return ret;
}

int ObOutlineMgr::get_outline_schema_count(int64_t &outline_schema_count) const
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    outline_schema_count = outline_infos_.size();
  }
  return ret;
}

int ObOutlineMgr::get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const
{
  int ret = OB_SUCCESS;
  schema_info.reset();
  schema_info.schema_type_ = OUTLINE_SCHEMA;
  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    schema_info.count_ = outline_infos_.size();
    for (ConstOutlineIter it = outline_infos_.begin(); OB_SUCC(ret) && it != outline_infos_.end(); it++) {
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

// TODO@xiyu: which case will cause in-consistent for outline?
int ObOutlineMgr::rebuild_outline_hashmap()
{
  int ret = OB_SUCCESS;

  if (!check_inner_stat()) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    outline_id_map_.clear();
    outline_name_map_.clear();
    signature_map_.clear();
    sql_id_map_.clear();
    for (ConstOutlineIter iter = outline_infos_.begin();
        iter != outline_infos_.end() && OB_SUCC(ret); ++iter) {
      ObSimpleOutlineSchema *outline_schema = *iter;
      if (OB_ISNULL(outline_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("outline_schema is NULL", K(ret), K(outline_schema));
      } else {
        int over_write = 1;
        int hash_ret = outline_id_map_.set_refactored(outline_schema->get_outline_id(),
                                                      outline_schema,
                                                      over_write);
        if (OB_SUCCESS != hash_ret) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("build outline id hashmap failed", K(ret), K(hash_ret),
                   "outline_id", outline_schema->get_outline_id());
        }
        if (OB_SUCC(ret)) {
          ObOutlineNameHashWrapper name_wrapper(outline_schema->get_tenant_id(),
                                                        outline_schema->get_database_id(),
                                                        outline_schema->get_name_str());
          hash_ret = outline_name_map_.set_refactored(name_wrapper, outline_schema,
                                                      over_write);
          if (OB_SUCCESS != hash_ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("build outline name hashmap failed", K(ret), K(hash_ret),
                     "outline_id", outline_schema->get_outline_id(),
                     "name", outline_schema->get_name());
          }
        }
        if (OB_SUCC(ret)) {
          if (0 != outline_schema->get_signature_str().length()) {
            ObOutlineSignatureHashWrapper outline_signature_wrapper(outline_schema->get_tenant_id(),
                                                                    outline_schema->get_database_id(),
                                                                    outline_schema->get_signature_str());
            hash_ret = signature_map_.set_refactored(outline_signature_wrapper,
                                                     outline_schema, over_write);
            if (OB_SUCCESS != hash_ret) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("build outline signature hashmap failed", K(ret), K(hash_ret),
                       "outline_id", outline_schema->get_outline_id(),
                       "signature", outline_schema->get_signature());
            }
          } else {
            ObOutlineSqlIdHashWrapper outline_signature_wrapper(outline_schema->get_tenant_id(),
                                                                    outline_schema->get_database_id(),
                                                                    outline_schema->get_sql_id_str());
            hash_ret = sql_id_map_.set_refactored(outline_signature_wrapper,
                                                     outline_schema, over_write);
            if (OB_SUCCESS != hash_ret) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("build outline signature hashmap failed", K(ret), K(hash_ret),
                       "outline_id", outline_schema->get_outline_id(),
                       "sql_id", outline_schema->get_sql_id_str());
            }
          }
        }
      }
    }
  }

  return ret;
}

} //end of namespace schema
} //end of namespace share
} //end of namespace oceanbase
