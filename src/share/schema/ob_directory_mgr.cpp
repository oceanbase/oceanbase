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

#include "share/schema/ob_directory_mgr.h"
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
const char *ObDirectoryMgr::DIRECTORY_MGR = "DIRECTORY_MGR";

ObDirectoryMgr::ObDirectoryMgr()
  : is_inited_(false),
    local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
    allocator_(local_allocator_),
    directory_infos_(0, NULL, SET_USE_500(DIRECTORY_MGR, ObCtxIds::SCHEMA_SERVICE)),
    directory_name_map_(SET_USE_500(DIRECTORY_MGR, ObCtxIds::SCHEMA_SERVICE)),
    directory_id_map_(SET_USE_500(DIRECTORY_MGR, ObCtxIds::SCHEMA_SERVICE))
{
}

ObDirectoryMgr::ObDirectoryMgr(common::ObIAllocator &allocator)
  : is_inited_(false),
    local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
    allocator_(allocator),
    directory_infos_(0, NULL, SET_USE_500(DIRECTORY_MGR, ObCtxIds::SCHEMA_SERVICE)),
    directory_name_map_(SET_USE_500(DIRECTORY_MGR, ObCtxIds::SCHEMA_SERVICE)),
    directory_id_map_(SET_USE_500(DIRECTORY_MGR, ObCtxIds::SCHEMA_SERVICE))
{
}

ObDirectoryMgr::~ObDirectoryMgr()
{
}

ObDirectoryMgr &ObDirectoryMgr::operator=(const ObDirectoryMgr &other)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("directory manager not init", K(ret));
  } else if (OB_FAIL(assign(other))) {
    LOG_WARN("assign failed", K(ret));
  }
  return *this;
}

int ObDirectoryMgr::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init private directory schema manager twice", K(ret));
  } else if (OB_FAIL(directory_name_map_.init())) {
    LOG_WARN("init directory name map failed", K(ret));
  } else if (OB_FAIL(directory_id_map_.init())) {
    LOG_WARN("init directory id map failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObDirectoryMgr::reset()
{
  if (IS_NOT_INIT) {
    LOG_WARN_RET(OB_NOT_INIT, "directory manger not init");
  } else {
    directory_infos_.clear();
    directory_name_map_.clear();
    directory_id_map_.clear();
  }
}

int ObDirectoryMgr::assign(const ObDirectoryMgr &other)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("directory manager not init", K(ret));
  } else if (this != &other) {
    if (OB_FAIL(directory_name_map_.assign(other.directory_name_map_))) {
      LOG_WARN("assign directory name map failed", K(ret));
    } else if (OB_FAIL(directory_id_map_.assign(other.directory_id_map_))) {
      LOG_WARN("assign directory id map failed", K(ret));
    } else if (OB_FAIL(directory_infos_.assign(other.directory_infos_))) {
      LOG_WARN("assign directory schema vector failed", K(ret));
    }
  }
  return ret;
}

int ObDirectoryMgr::deep_copy(const ObDirectoryMgr &other)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("directory manager not init", K(ret));
  } else if (this != &other) {
    reset();
    ObDirectorySchema *schema = NULL;
    for (DirectoryIter iter = other.directory_infos_.begin();
         OB_SUCC(ret) && iter != other.directory_infos_.end(); iter++) {
      if (OB_ISNULL(schema = *iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", KP(schema), K(ret));
      } else if (OB_FAIL(add_directory(*schema))) {
        LOG_WARN("add directory failed", K(*schema), K(ret));
      }
    }
  }
  return ret;
}

int ObDirectoryMgr::add_directory(const ObDirectorySchema &schema)
{
  int ret = OB_SUCCESS;
  ObDirectorySchema *new_schema = NULL;
  ObDirectorySchema *old_schema = NULL;
  DirectoryIter iter = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("the schema mgr is not init", K(ret));
  } else if (OB_UNLIKELY(!schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schema));
  } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator_,
                                                 schema,
                                                 new_schema))) {
    LOG_WARN("alloc schema failed", K(ret));
  } else if (OB_ISNULL(new_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alloc schema is a NULL ptr", K(new_schema), K(ret));
  } else if (OB_FAIL(directory_infos_.replace(new_schema,
                                              iter,
                                              schema_compare,
                                              schema_equal,
                                              old_schema))) {
    LOG_WARN("failed to add directory schema", K(ret));
  } else {
    int over_write = 1;
    ObDirectoryNameHashKey hash_wrapper(new_schema->get_tenant_id(), new_schema->get_directory_name_str());
    if (OB_FAIL(directory_name_map_.set_refactored(hash_wrapper, new_schema, over_write))) {
      LOG_WARN("build directory hash map failed", K(ret));
    } else if (OB_FAIL(directory_id_map_.set_refactored(new_schema->get_directory_id(), new_schema, over_write))) {
      LOG_WARN("build directory id hashmap failed", K(ret),
               "directory_id", new_schema->get_directory_id());
    }
  }
  if ((directory_infos_.count() != directory_name_map_.item_count()
      || directory_infos_.count() != directory_id_map_.item_count())) {
    LOG_WARN("schema is inconsistent with its map", K(ret),
             K(directory_infos_.count()),
             K(directory_name_map_.item_count()),
             K(directory_id_map_.item_count()));
    int rebuild_ret = OB_SUCCESS;
    if (OB_SUCCESS != (rebuild_ret = rebuild_directory_hashmap())) {
      LOG_WARN("rebuild directory hashmap failed", K(rebuild_ret));
    }
    if (OB_SUCC(ret)) {
      ret = rebuild_ret;
    }
  }
  LOG_DEBUG("add directory", K(schema), K(directory_infos_.count()),
            K(directory_name_map_.item_count()), K(directory_id_map_.item_count()));
  return ret;
}

int ObDirectoryMgr::add_directorys(const common::ObIArray<ObDirectorySchema> &schemas)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < schemas.count() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(add_directory(schemas.at(i)))) {
      LOG_WARN("push directory failed", K(ret));
    }
  }
  return ret;
}

int ObDirectoryMgr::del_directory(const ObTenantDirectoryId &id)
{
  int ret = OB_SUCCESS;
  ObDirectorySchema *schema = NULL;
  if (OB_UNLIKELY(!id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(id));
  } else if (OB_FAIL(directory_infos_.remove_if(id,
                                                compare_with_tenant_directory_id,
                                                equal_to_tenant_directory_id,
                                                schema))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // if item does not exist, regard it as succeeded, schema will be refreshed later
      ret = OB_SUCCESS;
      LOG_INFO("failed to remove directory schema, item may not exist", K(ret));
    } else {
      LOG_WARN("failed to remove directory schema", K(ret));
    }
  } else if (OB_ISNULL(schema)) {
    // if item can be found, schema should not be null
    // defense code, should not happed
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("removed directory schema return NULL, ",
             "tenant_id", id.tenant_id_,
             "directory_id", id.schema_id_,
             K(ret));
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(schema)) {
    if (OB_FAIL(directory_id_map_.erase_refactored(schema->get_directory_id()))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("item does not exist, will igore it", K(ret),
            "directory id", schema->get_directory_id());
      } else {
        LOG_WARN("failed to delete directory from hashmap", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(schema)) {
    if (OB_FAIL(directory_name_map_.erase_refactored(
        ObDirectoryNameHashKey(schema->get_tenant_id(), schema->get_directory_name_str())))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("item does not exist, will igore it", K(ret),
            "directory name", schema->get_directory_name_str());
      } else {
        LOG_WARN("failed to delete directory from hashmap", K(ret));
      }
    }
  }

  if (directory_infos_.count() != directory_name_map_.item_count()
      || directory_infos_.count() != directory_id_map_.item_count()) {
    LOG_WARN("directory schema is non-consistent",K(id), K(directory_infos_.count()),
        K(directory_name_map_.item_count()), K(directory_id_map_.item_count()));
    int rebuild_ret = OB_SUCCESS;
    if (OB_SUCCESS != (rebuild_ret = rebuild_directory_hashmap())) {
      LOG_WARN("rebuild directory hashmap failed", K(rebuild_ret));
    }
    if (OB_SUCC(ret)) {
      ret = rebuild_ret;
    }
  }
  LOG_DEBUG("directory del", K(id), K(directory_infos_.count()),
      K(directory_name_map_.item_count()), K(directory_id_map_.item_count()));
  return ret;
}

int ObDirectoryMgr::get_directory_schema_by_id(const uint64_t directory_id,
                                               const ObDirectorySchema *&schema) const
{
  int ret = OB_SUCCESS;
  schema = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == directory_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(directory_id));
  } else {
    ObDirectorySchema *tmp_schema = NULL;
    int hash_ret = directory_id_map_.get_refactored(directory_id, tmp_schema);
    if (OB_LIKELY(OB_SUCCESS == hash_ret)) {
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(tmp_schema));
      } else {
        schema = tmp_schema;
      }
    }
  }
  return ret;
}

int ObDirectoryMgr::get_directory_schema_by_name(const uint64_t tenant_id,
                                                 const common::ObString &name,
                                                 const ObDirectorySchema *&schema) const
{
  int ret = OB_SUCCESS;
  schema = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id) || OB_UNLIKELY(name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(name));
  } else {
    ObDirectorySchema *tmp_schema = NULL;
    ObDirectoryNameHashKey hash_wrapper(tenant_id, name);
    if (OB_FAIL(directory_name_map_.get_refactored(hash_wrapper, tmp_schema))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("schema is not exist", K(tenant_id), K(name),
                 "map_cnt", directory_name_map_.item_count());
      }
    } else {
      schema = tmp_schema;
    }
  }
  return ret;
}

int ObDirectoryMgr::get_directory_schemas_in_tenant(const uint64_t tenant_id,
                                                    common::ObIArray<const ObDirectorySchema *> &schemas) const
{
  int ret = OB_SUCCESS;
  schemas.reset();
  ObTenantDirectoryId id(tenant_id, OB_MIN_ID);
  ConstDirectoryIter iter_begin =
      directory_infos_.lower_bound(id, compare_with_tenant_directory_id);
  bool is_stop = false;
  for (ConstDirectoryIter iter = iter_begin;
      OB_SUCC(ret) && iter != directory_infos_.end() && !is_stop; ++iter) {
    const ObDirectorySchema *schema = NULL;
    if (OB_ISNULL(schema = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(schema));
    } else if (tenant_id != schema->get_tenant_id()) {
      is_stop = true;
    } else if (OB_FAIL(schemas.push_back(schema))) {
      LOG_WARN("push back directory failed", K(ret));
    }
  }
  return ret;
}

int ObDirectoryMgr::del_directory_schemas_in_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    ObArray<const ObDirectorySchema *> schemas;
    if (OB_FAIL(get_directory_schemas_in_tenant(tenant_id, schemas))) {
      LOG_WARN("get directory schemas failed", K(ret), K(tenant_id));
    } else {
      FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
        ObTenantDirectoryId id(tenant_id, (*schema)->get_directory_id());
        if (OB_FAIL(del_directory(id))) {
          LOG_WARN("del directory failed",
                   "tenant_id",
                   id.tenant_id_,
                   "directory_id",
                   id.schema_id_,
                   K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDirectoryMgr::get_directory_schema_count(int64_t &schema_count) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    schema_count = directory_infos_.count();
  }
  return ret;
}

int ObDirectoryMgr::get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const
{
  int ret = OB_SUCCESS;
  schema_info.reset();
  schema_info.schema_type_ = DIRECTORY_SCHEMA;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    schema_info.count_ = directory_infos_.size();
    for (ConstDirectoryIter iter = directory_infos_.begin();
        OB_SUCC(ret) && iter != directory_infos_.end(); ++iter) {
      if (OB_ISNULL(*iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema is null", K(ret));
      } else {
        schema_info.size_ += (*iter)->get_convert_size();
      }
    }
  }
  return ret;
}

int ObDirectoryMgr::rebuild_directory_hashmap()
{
  int ret = OB_SUCCESS;
  int over_write = 1;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    directory_name_map_.clear();
    directory_id_map_.clear();
  }

  for (ConstDirectoryIter iter = directory_infos_.begin();
      OB_SUCC(ret) && iter != directory_infos_.end(); ++iter) {
    ObDirectorySchema *directory_schema = *iter;
    ObDirectoryNameHashKey hash_wrapper;
    if (OB_ISNULL(directory_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("directory_schema is NULL", K(ret), K(directory_schema));
    } else if (FALSE_IT(hash_wrapper.set_tenant_id(directory_schema->get_tenant_id()))) {
      // do nothing
    } else if (FALSE_IT(hash_wrapper.set_directory_name(directory_schema->get_directory_name()))) {
      // do nothing
    } else if (OB_FAIL(directory_name_map_.set_refactored(hash_wrapper,
        directory_schema, over_write))) {
      LOG_WARN("build directory name hashmap failed", K(ret),
          "directory_name", directory_schema->get_directory_name());
    } else if (OB_FAIL(directory_id_map_.set_refactored(directory_schema->get_directory_id(),
        directory_schema, over_write))) {
      LOG_WARN("build directory id hashmap failed", K(ret),
          "directory_id", directory_schema->get_directory_id());
    }
  }
  return ret;
}
} // namespace schema
} // namespace share
} // namespace oceanbase
