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
#include "ob_synonym_mgr.h"
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

ObSimpleSynonymSchema::ObSimpleSynonymSchema()
  : ObSchema()
{
  reset();
}

ObSimpleSynonymSchema::ObSimpleSynonymSchema(ObIAllocator *allocator)
  : ObSchema(allocator)
{
  reset();
}

ObSimpleSynonymSchema::ObSimpleSynonymSchema(const ObSimpleSynonymSchema &other)
  : ObSchema()
{
  reset();
  *this = other;
}

ObSimpleSynonymSchema::~ObSimpleSynonymSchema()
{
}

ObSimpleSynonymSchema &ObSimpleSynonymSchema::operator =(const ObSimpleSynonymSchema &other)
{
  if (this != &other) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = other.error_ret_;
    tenant_id_ = other.tenant_id_;
    synonym_id_ = other.synonym_id_;
    schema_version_ = other.schema_version_;
    database_id_ = other.database_id_;
    object_database_id_ = other.object_database_id_;
    status_ = other.status_;
    if (OB_FAIL(deep_copy_str(other.synonym_name_, synonym_name_))) {
      LOG_WARN("Fail to deep copy synonym name", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.object_name_, object_name_))) {
      LOG_WARN("Fail to deep copy signature", K(ret));
    }
    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }

  return *this;
}

void ObSimpleSynonymSchema::reset()
{
  ObSchema::reset();
  tenant_id_ = OB_INVALID_ID;
  synonym_id_ = OB_INVALID_ID;
  schema_version_ = OB_INVALID_VERSION;
  database_id_ = OB_INVALID_ID;
  database_id_ = OB_INVALID_ID;
  object_database_id_ = OB_INVALID_ID;
  synonym_name_.reset();
  object_name_.reset();
  status_ = ObObjectStatus::VALID;
}

bool ObSimpleSynonymSchema::is_valid() const
{
  bool ret = true;
  if (OB_INVALID_ID == tenant_id_ ||
      OB_INVALID_ID == synonym_id_ ||
      schema_version_ < 0 ||
      OB_INVALID_ID == database_id_ ||
      OB_INVALID_ID == object_database_id_||
      synonym_name_.empty() ||
      object_name_.empty() ||
      ObObjectStatus::INVALID == status_) {
    ret = false;
  }
  return ret;
}

int64_t ObSimpleSynonymSchema::get_convert_size() const
{
  int64_t convert_size = 0;

  convert_size += sizeof(ObSimpleSynonymSchema);
  convert_size += synonym_name_.length() + 1;
  convert_size += object_name_.length() + 1;

  return convert_size;
}

ObSynonymMgr::ObSynonymMgr()
    : is_inited_(false),
      local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
      allocator_(local_allocator_),
      synonym_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_SYNONYM, ObCtxIds::SCHEMA_SERVICE)),
      synonym_id_map_(SET_USE_500(ObModIds::OB_SCHEMA_SYNONYM, ObCtxIds::SCHEMA_SERVICE)),
      synonym_name_map_(SET_USE_500(ObModIds::OB_SCHEMA_SYNONYM, ObCtxIds::SCHEMA_SERVICE))
{
}

ObSynonymMgr::ObSynonymMgr(ObIAllocator &allocator)
    : is_inited_(false),
      local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
      allocator_(allocator),
      synonym_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_SYNONYM, ObCtxIds::SCHEMA_SERVICE)),
      synonym_id_map_(SET_USE_500(ObModIds::OB_SCHEMA_SYNONYM, ObCtxIds::SCHEMA_SERVICE)),
      synonym_name_map_(SET_USE_500(ObModIds::OB_SCHEMA_SYNONYM, ObCtxIds::SCHEMA_SERVICE))
{
}

ObSynonymMgr::~ObSynonymMgr()
{
}

int ObSynonymMgr::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init private synonym manager twice", K(ret));
  } else if (OB_FAIL(synonym_id_map_.init())) {
    LOG_WARN("init private synonym id map failed", K(ret));
  } else if (OB_FAIL(synonym_name_map_.init())) {
    LOG_WARN("init private synonym name map failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObSynonymMgr::reset()
{
  if (!is_inited_) {
    LOG_WARN_RET(OB_NOT_INIT, "synonym manger not init");
  } else {
    synonym_infos_.clear();
    synonym_id_map_.clear();
    synonym_name_map_.clear();
  }
}

ObSynonymMgr &ObSynonymMgr::operator =(const ObSynonymMgr &other)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("synonym manager not init", K(ret));
  } else  if (OB_FAIL(assign(other))) {
    LOG_WARN("assign failed", K(ret));
  }
  return *this;
}

int ObSynonymMgr::assign(const ObSynonymMgr &other)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("synonym manager not init", K(ret));
  } else if (this != &other) {
    if (OB_FAIL(synonym_id_map_.assign(other.synonym_id_map_))) {
      LOG_WARN("assign synonym id map failed", K(ret));
    } else if (OB_FAIL(synonym_name_map_.assign(other.synonym_name_map_))) {
      LOG_WARN("assign synonym name map failed", K(ret));
    } else if (OB_FAIL(synonym_infos_.assign(other.synonym_infos_))) {
      LOG_WARN("assign synonym infos vector failed", K(ret));
    }
  }
  return ret;
}

int ObSynonymMgr::deep_copy(const ObSynonymMgr &other)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("synonym manager not init", K(ret));
  } else if (this != &other) {
    reset();
    for (SynonymIter iter = other.synonym_infos_.begin();
         OB_SUCC(ret) && iter != other.synonym_infos_.end();
         iter++) {
      ObSimpleSynonymSchema *synonym_schema = *iter;
      if (OB_ISNULL(synonym_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(synonym_schema), K(ret));
      } else if (OB_FAIL(add_synonym(*synonym_schema))) {
        LOG_WARN("add synonym failed", K(*synonym_schema), K(ret));
      }
    }
  }
  return ret;
}


int ObSynonymMgr::add_synonym(const ObSimpleSynonymSchema &synonym_schema)
{
  int ret = OB_SUCCESS;
  int overwrite = 1;
  ObSimpleSynonymSchema *new_synonym_schema = NULL;
  SynonymIter iter = NULL;
  ObSimpleSynonymSchema *replaced_synonym = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("synonym manager not init", K(ret));
  } else if (OB_UNLIKELY(!synonym_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(synonym_schema));
  } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator_,
                                                 synonym_schema,
                                                 new_synonym_schema))) {
    LOG_WARN("alloca synonym schema failed", K(ret));
  } else if (OB_ISNULL(new_synonym_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(new_synonym_schema), K(ret));
  } else if (OB_FAIL(synonym_infos_.replace(new_synonym_schema,
                                      iter,
                                      compare_synonym,
                                      equal_synonym,
                                      replaced_synonym))) {
      LOG_WARN("failed to add synonym schema", K(ret));
  } else {
    ObSynonymHashWrapper hash_wrapper(new_synonym_schema->get_tenant_id(),
                                      new_synonym_schema->get_database_id(),
                                      new_synonym_schema->get_synonym_name());
    if (OB_FAIL(synonym_id_map_.set_refactored(new_synonym_schema->get_synonym_id(),
                                               new_synonym_schema, overwrite))) {
      LOG_WARN("build synonym id hash map failed", K(ret));
    } else if (OB_FAIL(synonym_name_map_.set_refactored(hash_wrapper, new_synonym_schema,
                                                        overwrite))) {
      LOG_WARN("build synonym name hash map failed", K(ret));
    }
  }
  if (synonym_infos_.count() != synonym_id_map_.item_count() ||
      synonym_infos_.count() != synonym_name_map_.item_count()) {
    LOG_WARN("synonym info is non-consistent",
             "synonym_infos_count",
             synonym_infos_.count(),
             "synonym_id_map_item_count",
             synonym_id_map_.item_count(),
             "synonym_name_map_item_count",
             synonym_name_map_.item_count(),
             "synonym_id",
             synonym_schema.get_synonym_id(),
             "synonym_name",
             synonym_schema.get_synonym_name());
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = rebuild_synonym_hashmap())) {
      LOG_WARN("rebuild synonym hashmap failed", K(tmp_ret));
    }
  }
  return ret;
}

int ObSynonymMgr::rebuild_synonym_hashmap()
{
  int ret = OB_SUCCESS;
  synonym_id_map_.clear();
  synonym_name_map_.clear();
  ConstSynonymIter iter = synonym_infos_.begin();
  for (; iter != synonym_infos_.end() && OB_SUCC(ret); ++iter) {
    ObSimpleSynonymSchema *synonym_schema = *iter;
    if (OB_ISNULL(synonym_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("synonym schema is NULL", K(synonym_schema), K(ret));
    } else {
      int overwrite = 1;
      ObSynonymHashWrapper hash_wrapper(synonym_schema->get_tenant_id(),
                                        synonym_schema->get_database_id(),
                                        synonym_schema->get_synonym_name());
      if (OB_FAIL(synonym_id_map_.set_refactored(synonym_schema->get_synonym_id(),
                                                 synonym_schema, overwrite))) {
        LOG_WARN("build synonym id hash map failed", K(ret));
      } else if (OB_FAIL(synonym_name_map_.set_refactored(hash_wrapper, synonym_schema,
                                                          overwrite))) {
        LOG_WARN("build synonym name hash map failed", K(ret));
      }
    }
  }
  return ret;
}

int ObSynonymMgr::add_synonyms(const common::ObIArray<ObSimpleSynonymSchema> &synonym_schemas)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < synonym_schemas.count() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(add_synonym(synonym_schemas.at(i)))) {
      LOG_WARN("push synonym failed", K(ret));
    }
  }
  return ret;
}

int ObSynonymMgr::get_synonym_schema_with_name(
  const uint64_t tenant_id,
  const uint64_t database_id,
  const ObString &name,
  const ObSimpleSynonymSchema *&synonym_schema) const
{
  int ret = OB_SUCCESS;
  synonym_schema = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == database_id
             || name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(database_id), K(name));
  } else {
    ObSimpleSynonymSchema *tmp_schema = NULL;
    ObSynonymHashWrapper hash_wrap(tenant_id, database_id, name);
    if (OB_FAIL(synonym_name_map_.get_refactored(hash_wrap, tmp_schema))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get synonym with name failed", K(tenant_id), K(database_id), K(name), K(ret));
      }
    } else {
      synonym_schema = tmp_schema;
    }
  }
  return ret;
}

//suppose there are no vital errors ,
//this function will always return ob_success no matter do_exist = true or not.
int ObSynonymMgr::get_object(const uint64_t tenant_id,
                             const uint64_t database_id,
                             const ObString &name,
                             uint64_t &obj_database_id,
                             uint64_t &synonym_id,
                             ObString &obj_table_name,
                             bool &do_exist,
                             bool search_public_schema,
                             bool *is_public) const
{
  int ret = OB_SUCCESS;
  do_exist = false;
  const ObSimpleSynonymSchema *synonym_schema = nullptr;
  int64_t n = (search_public_schema ? 2 : 1);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == database_id
             || name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(database_id), K(name));
  }

  for (int64_t i = 0; OB_SUCC(ret) && false == do_exist && i < n; ++i) {
    uint64_t cur_database_id = (i == 0 ? database_id : OB_PUBLIC_SCHEMA_ID);
    if (OB_FAIL(get_synonym_schema_with_name(tenant_id, cur_database_id, name, synonym_schema))) {
      LOG_WARN("fail to get synonym schema with name", K(tenant_id), K(cur_database_id), K(name), K(ret));
    } else if (OB_ISNULL(synonym_schema)) {
      LOG_DEBUG("synonym is not exist", K(tenant_id), K(cur_database_id), K(name), K(ret));
    } else {
      do_exist = true;
      if (NULL != is_public) {
        *is_public = is_public_database_id(cur_database_id);
      }
    }
  }

  if (OB_SUCC(ret) && synonym_schema != NULL) {
    do_exist = true;
    obj_database_id = synonym_schema->get_object_database_id();
    obj_table_name = synonym_schema->get_object_name_str();
    synonym_id = synonym_schema->get_synonym_id();
  }
  return ret;
}

int ObSynonymMgr::del_synonym(const ObTenantSynonymId &synonym)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  ObSimpleSynonymSchema *schema_to_del = NULL;
  if (!synonym.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(synonym));
  } else if (OB_FAIL(synonym_infos_.remove_if(synonym,
                                              compare_with_tenant_synonym_id,
                                              equal_to_tenant_synonym_id,
                                              schema_to_del))) {
    LOG_WARN("failed to remove synonym schema, ",
             "tenant_id",
             synonym.tenant_id_,
             "synonym_id",
             synonym.synonym_id_,
             K(ret));
  } else if (OB_ISNULL(schema_to_del)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("removed synonym schema return NULL, ",
             "tenant_id",
             synonym.tenant_id_,
             "synonym_id",
             synonym.synonym_id_,
             K(ret));
  } else {
    hash_ret = synonym_id_map_.erase_refactored(schema_to_del->get_synonym_id());
    if (OB_SUCCESS != hash_ret) {
      LOG_WARN("failed delete synonym from synonym name hashmap, ",
               K(ret),
               K(hash_ret),
               K(schema_to_del->get_synonym_id()));
      ret = OB_HASH_NOT_EXIST != hash_ret ? hash_ret : ret;
    }
    if (OB_SUCC(ret)) {
      ObSynonymHashWrapper synonym_wrapper(schema_to_del->get_tenant_id(),
                                          schema_to_del->get_database_id(),
                                          schema_to_del->get_synonym_name());
      hash_ret = synonym_name_map_.erase_refactored(synonym_wrapper);
      if (OB_SUCCESS != hash_ret) {
        LOG_WARN("failed delete synonym from synonym name hashmap, ",
                K(ret),
                K(hash_ret),
                "tenant_id", schema_to_del->get_tenant_id(),
                "database_id", schema_to_del->get_database_id(),
                "name", schema_to_del->get_synonym_name());
        ret = OB_HASH_NOT_EXIST != hash_ret ? hash_ret : ret;
      }
    }
  }

  if (synonym_infos_.count() != synonym_id_map_.item_count() ||
      synonym_infos_.count() != synonym_name_map_.item_count()) {
    LOG_WARN("synonym info is non-consistent",
             "synonym_infos_count",
             synonym_infos_.count(),
             "synonym_id_map_item_count",
             synonym_id_map_.item_count(),
             "synonym_name_map_item_count",
             synonym_name_map_.item_count(),
             "synonym_id",
             synonym.synonym_id_);
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = rebuild_synonym_hashmap())) {
      LOG_WARN("rebuild synonym hashmap failed", K(tmp_ret));
    }
  }
  return ret;
}

int ObSynonymMgr::get_synonym_schemas_in_database(const uint64_t tenant_id,
    const uint64_t database_id, ObIArray<const ObSimpleSynonymSchema *> &synonym_schemas) const
{
  
  int ret = OB_SUCCESS;
  synonym_schemas.reset();

  ObTenantSynonymId tenant_outine_id_lower(tenant_id, OB_MIN_ID);
  ConstSynonymIter tenant_synonym_begin =
      synonym_infos_.lower_bound(tenant_outine_id_lower, compare_with_tenant_synonym_id);
  bool is_stop = false;
  for (ConstSynonymIter iter = tenant_synonym_begin;
      OB_SUCC(ret) && iter != synonym_infos_.end() && !is_stop; ++iter) {
    const ObSimpleSynonymSchema *synonym = NULL;
    if (OB_ISNULL(synonym = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(synonym));
    } else if (tenant_id != synonym->get_tenant_id()) {
      is_stop = true;
    } else if (database_id != synonym->get_database_id()) {
      // do nothing
    } else if (OB_FAIL(synonym_schemas.push_back(synonym))) {
      LOG_WARN("push back synonym failed", K(ret));
    }
  }

  return ret;
}

int ObSynonymMgr::del_schemas_in_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    ObArray<const ObSimpleSynonymSchema *> schemas;
    if (OB_FAIL(get_synonym_schemas_in_tenant(tenant_id, schemas))) {
      LOG_WARN("get synonym schemas failed", K(ret), K(tenant_id));
    } else {
      FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
        ObTenantSynonymId tenant_synonym_id(tenant_id,
                                            (*schema)->get_synonym_id());
        if (OB_FAIL(del_synonym(tenant_synonym_id))) {
          LOG_WARN("del synonym failed",
                   "tenant_id", tenant_synonym_id.tenant_id_,
                   "synonym_id", tenant_synonym_id.synonym_id_,
                   K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSynonymMgr::get_synonym_schema(const uint64_t synonym_id,
                                     const ObSimpleSynonymSchema *&synonym_schema) const
{
  int ret = OB_SUCCESS;
  synonym_schema = NULL;

  if (OB_INVALID_ID == synonym_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(synonym_id));
  } else {
    ObSimpleSynonymSchema *tmp_schema = NULL;
    if (OB_FAIL(synonym_id_map_.get_refactored(synonym_id, tmp_schema))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get synonym with id failed", K(synonym_id), K(ret));
      }
    } else {
      synonym_schema = tmp_schema;
    }
  }
  return ret;
}

template<typename Filter, typename Acation, typename EarlyStopCondition>
int ObSynonymMgr::for_each(Filter &filter, Acation &action, EarlyStopCondition &condition)
{
  int ret = OB_SUCCESS;
  ConstSynonymIter iter = synonym_infos_.begin();
  for (; iter != synonym_infos_.end() && OB_SUCC(ret); iter++) {
    ObSimpleSynonymSchema *value = NULL;
    if (OB_ISNULL(value = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret));
    } else {
      if (filter(value)) {
        if (OB_FAIL(action(value, synonym_infos_, synonym_name_map_))) {
            LOG_WARN("action failed", K(ret));
        }
      }
    }
  }
  UNUSED(condition);
  return ret;
}

bool ObSynonymMgr::compare_with_tenant_synonym_id(const ObSimpleSynonymSchema *lhs,
                                                 const ObTenantSynonymId &tenant_synonym_id)
{
  return NULL != lhs ? (lhs->get_tenant_synonym_id() < tenant_synonym_id) : false;
}

bool ObSynonymMgr::equal_to_tenant_synonym_id(const ObSimpleSynonymSchema *lhs,
                                               const ObTenantSynonymId &tenant_synonym_id)
{
  return NULL != lhs ? (lhs->get_tenant_synonym_id() == tenant_synonym_id) : false;
}

int ObSynonymMgr::get_synonym_schemas_in_tenant(const uint64_t tenant_id,
    ObIArray<const ObSimpleSynonymSchema *> &synonym_schemas) const
{
  int ret = OB_SUCCESS;
  synonym_schemas.reset();

  ObTenantSynonymId tenant_outine_id_lower(tenant_id, OB_MIN_ID);
  ConstSynonymIter tenant_synonym_begin =
      synonym_infos_.lower_bound(tenant_outine_id_lower, compare_with_tenant_synonym_id);
  bool is_stop = false;
  for (ConstSynonymIter iter = tenant_synonym_begin;
      OB_SUCC(ret) && iter != synonym_infos_.end() && !is_stop; ++iter) {
    const ObSimpleSynonymSchema *synonym = NULL;
    if (OB_ISNULL(synonym = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(synonym));
    } else if (tenant_id != synonym->get_tenant_id()) {
      is_stop = true;
    } else if (OB_FAIL(synonym_schemas.push_back(synonym))) {
      LOG_WARN("push back synonym failed", K(ret));
    }
  }

  return ret;
}

int ObSynonymMgr::get_synonym_schema_count(int64_t &synonym_schema_count) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    LOG_WARN("synonym manger not init");
  } else {
    synonym_schema_count = synonym_infos_.size();
  }
  return ret;
}

int ObSynonymMgr::get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const
{
  int ret = OB_SUCCESS;
  schema_info.reset();
  schema_info.schema_type_ = SYNONYM_SCHEMA;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    schema_info.count_ = synonym_infos_.size();
    for (ConstSynonymIter it = synonym_infos_.begin(); OB_SUCC(ret) && it != synonym_infos_.end(); it++) {
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

} //end of namespace schema
} //end of namespace share
} //end of namespace oceanbase
