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
#include "share/schema/ob_trigger_mgr.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "share/schema/ob_schema_utils.h"
#include "share/ob_debug_sync.h"
#include "lib/utility/ob_tracepoint.h"

namespace oceanbase
{
using namespace std;
using namespace common;
using namespace common::hash;
namespace share
{
namespace schema
{

OB_SERIALIZE_MEMBER(ObSimpleTriggerSchema,
                    tenant_id_,
                    trigger_id_,
                    database_id_,
                    schema_version_,
                    trigger_name_);

ObSimpleTriggerSchema &ObSimpleTriggerSchema::operator =(const ObSimpleTriggerSchema &other)
{
  if (this != &other) {
    reset();
    int &ret = error_ret_;
    OZ (deep_copy(other));
  }

  return *this;
}

bool ObSimpleTriggerSchema::operator ==(const ObSimpleTriggerSchema &other) const
{
  bool ret = false;

  if (tenant_id_ == other.get_tenant_id() &&
      trigger_id_ == other.get_trigger_id() &&
      database_id_ == other.get_database_id() &&
      schema_version_ == other.get_schema_version() &&
      trigger_name_ == other.get_trigger_name()) {
    ret = true;
  }

  return ret;
}

int64_t ObSimpleTriggerSchema::get_convert_size() const
{
  int64_t convert_size = 0;
  convert_size += sizeof(ObSimpleTriggerSchema);
  convert_size += trigger_name_.length() + 1;
  return convert_size;
}

int ObSimpleTriggerSchema::deep_copy(const ObSimpleTriggerSchema &other)
{
  int ret = OB_SUCCESS;
  OX (set_tenant_id(other.get_tenant_id()));
  OX (set_trigger_id(other.get_trigger_id()));
  OX (set_database_id(other.get_database_id()));
  OX (set_schema_version(other.get_schema_version()));
  OZ (set_trigger_name(other.get_trigger_name()));
  return ret;
}

uint64_t ObSimpleTriggerSchema::get_exec_tenant_id() const
{
  return ObSchemaUtils::get_exec_tenant_id(tenant_id_);
}

ObTriggerMgr::ObTriggerMgr()
    : local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
      allocator_(local_allocator_),
      trigger_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_TRIGGER_INFO_VECTOR, ObCtxIds::SCHEMA_SERVICE)),
      trigger_id_map_(SET_USE_500(ObModIds::OB_SCHEMA_TRIGGER_ID_MAP, ObCtxIds::SCHEMA_SERVICE)),
      trigger_name_map_(SET_USE_500(ObModIds::OB_SCHEMA_TRIGGER_NAME_MAP, ObCtxIds::SCHEMA_SERVICE)),
      is_inited_(false)
{
}

ObTriggerMgr::ObTriggerMgr(ObIAllocator &allocator)
    : local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
      allocator_(allocator),
      trigger_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_TRIGGER_INFO_VECTOR, ObCtxIds::SCHEMA_SERVICE)),
      trigger_id_map_(SET_USE_500(ObModIds::OB_SCHEMA_TRIGGER_ID_MAP, ObCtxIds::SCHEMA_SERVICE)),
      trigger_name_map_(SET_USE_500(ObModIds::OB_SCHEMA_TRIGGER_NAME_MAP, ObCtxIds::SCHEMA_SERVICE)),
      is_inited_(false)
{
}


int ObTriggerMgr::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(trigger_id_map_.init())) {
    LOG_WARN("init trigger id map failed", K(ret));
  } else if (OB_FAIL(trigger_name_map_.init())) {
    LOG_WARN("init trigger name map failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObTriggerMgr::reset()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    // reset will not release memory for vector, use clear()
    trigger_infos_.clear();
    trigger_id_map_.clear();
    trigger_name_map_.clear();
  }
}

ObTriggerMgr &ObTriggerMgr::operator =(const ObTriggerMgr &other)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(assign(other))) {
    LOG_WARN("assign failed", K(ret));
  }
  return *this;
}

int ObTriggerMgr::assign(const ObTriggerMgr &other)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
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
    ASSIGN_FIELD(trigger_infos_);
    ASSIGN_FIELD(trigger_id_map_);
    ASSIGN_FIELD(trigger_name_map_);
    #undef ASSIGN_FIELD
  }
  return ret;
}

int ObTriggerMgr::deep_copy(const ObTriggerMgr &other)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (this != &other) {
    reset();
    for (TriggerIter iter = other.trigger_infos_.begin();
       OB_SUCC(ret) && iter != other.trigger_infos_.end(); iter++) {
      ObSimpleTriggerSchema *trigger = *iter;
      if (OB_ISNULL(trigger)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(trigger), K(ret));
      } else if (OB_FAIL(add_trigger(*trigger))) {
        LOG_WARN("add trigger failed", K(*trigger), K(ret));
      }
    }
  }
  return ret;
}

bool ObTriggerMgr::compare_trigger(const ObSimpleTriggerSchema *lhs, const ObSimpleTriggerSchema *rhs)
{
  return lhs->get_tenant_trigger_id() < rhs->get_tenant_trigger_id();
}

bool ObTriggerMgr::equal_trigger(const ObSimpleTriggerSchema *lhs, const ObSimpleTriggerSchema *rhs)
{
  return lhs->get_tenant_trigger_id() == rhs->get_tenant_trigger_id();
}

bool ObTriggerMgr::compare_with_tenant_trigger_id(const ObSimpleTriggerSchema *lhs,
                                                 const ObTenantTriggerId &tenant_trigger_id)
{
  return NULL != lhs ? (lhs->get_tenant_trigger_id() < tenant_trigger_id) : false;
}

bool ObTriggerMgr::equal_with_tenant_trigger_id(const ObSimpleTriggerSchema *lhs,
                                                    const ObTenantTriggerId &tenant_trigger_id)
{
  return NULL != lhs ? (lhs->get_tenant_trigger_id() == tenant_trigger_id) : false;
}

int ObTriggerMgr::add_triggers(const ObIArray<ObSimpleTriggerSchema> &trigger_schemas)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    FOREACH_CNT_X(trigger_schema, trigger_schemas, OB_SUCC(ret)) {
      if (OB_FAIL(add_trigger(*trigger_schema))) {
        LOG_WARN("add trigger failed", K(ret), "trigger_schema", *trigger_schema);
      }
    }
  }
  return ret;
}

int ObTriggerMgr::del_triggers(const ObIArray<ObTenantTriggerId> &triggers)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    FOREACH_CNT_X(trigger_id, triggers, OB_SUCC(ret)) {
      if (OB_FAIL(del_trigger(*trigger_id))) {
        LOG_WARN("del trigger failed", K(ret), K(*trigger_id));
      }
    }
  }
  return ret;
}

int ObTriggerMgr::add_trigger(const ObSimpleTriggerSchema &trigger_schema)
{
  int ret = OB_SUCCESS;
  ObSimpleTriggerSchema *new_trigger_schema = NULL;
  ObSimpleTriggerSchema *replaced_trigger = NULL;
  ObTriggerNameHashWrapper name_wrapper;
  uint64_t trigger_id = trigger_schema.get_trigger_id();
  TriggerIter iter = NULL;
  int overwrite = 1;
  OV (is_inited_, OB_NOT_INIT);
  OV (trigger_schema.is_valid(), OB_INVALID_ARGUMENT, trigger_schema);
  OZ (ObSchemaUtils::alloc_schema(allocator_, trigger_schema, new_trigger_schema), trigger_schema);
  OV (OB_NOT_NULL(new_trigger_schema), OB_ERR_UNEXPECTED, trigger_schema);
  OZ (trigger_infos_.replace(new_trigger_schema, iter, compare_trigger,
                             equal_trigger, replaced_trigger), trigger_schema);
  // 以下是错误注入
  DEBUG_SYNC(ADD_TRIGGER_BEFORE_MAP);
  int skip_map = OB_E(EventTable::EN_ADD_TRIGGER_SKIP_MAP) 0;
  if (OB_SUCC(ret) && skip_map == 0) {
    if (OB_NOT_NULL(replaced_trigger)) {
      OX (name_wrapper.set_tenant_id(replaced_trigger->get_tenant_id()));
      OX (name_wrapper.set_database_id(replaced_trigger->get_database_id()));
      OX (name_wrapper.set_trigger_name(replaced_trigger->get_trigger_name()));
      OZ (trigger_id_map_.erase_refactored(trigger_id), trigger_id);
      ret = (ret == OB_HASH_NOT_EXIST) ? OB_SUCCESS : ret;
      OZ (trigger_name_map_.erase_refactored(name_wrapper), name_wrapper);
      ret = (ret == OB_HASH_NOT_EXIST) ? OB_SUCCESS : ret;
    }
    OX (name_wrapper.set_tenant_id(new_trigger_schema->get_tenant_id()));
    OX (name_wrapper.set_database_id(new_trigger_schema->get_database_id()));
    OX (name_wrapper.set_trigger_name(new_trigger_schema->get_trigger_name()));
    OZ (trigger_id_map_.set_refactored(trigger_id, new_trigger_schema, overwrite), trigger_id);
    OZ (trigger_name_map_.set_refactored(name_wrapper, new_trigger_schema, overwrite), name_wrapper);
  }
  return ret;
}

int ObTriggerMgr::del_trigger(const ObTenantTriggerId &tenant_trigger_id)
{
  int ret = OB_SUCCESS;
  ObSimpleTriggerSchema *deleted_trigger = NULL;
  uint64_t trigger_id = tenant_trigger_id.get_trigger_id();
  OV (is_inited_, OB_NOT_INIT);
  OV (tenant_trigger_id.is_valid(), OB_INVALID_ARGUMENT, tenant_trigger_id);
  OZ (trigger_infos_.remove_if(tenant_trigger_id, compare_with_tenant_trigger_id,
                               equal_with_tenant_trigger_id, deleted_trigger));
  OV (OB_NOT_NULL(deleted_trigger), OB_ERR_UNEXPECTED, tenant_trigger_id);
  // 以下是错误注入
  DEBUG_SYNC(DEL_TRIGGER_BEFORE_MAP);
  int skip_map = OB_E(EventTable::EN_ADD_TRIGGER_SKIP_MAP) 0;
  if (OB_SUCC(ret) && skip_map == 0) {
    ObTriggerNameHashWrapper name_wrapper(deleted_trigger->get_tenant_id(),
                                          deleted_trigger->get_database_id(),
                                          deleted_trigger->get_trigger_name());
    OZ (trigger_id_map_.erase_refactored(trigger_id));
    ret = (ret == OB_HASH_NOT_EXIST) ? OB_SUCCESS : ret;
    OZ (trigger_name_map_.erase_refactored(name_wrapper));
    ret = (ret == OB_HASH_NOT_EXIST) ? OB_SUCCESS : ret;
  }
  return ret;
}

int ObTriggerMgr::try_rebuild_trigger_hashmap()
{
  int ret = OB_SUCCESS;
  if (trigger_infos_.count() != trigger_id_map_.item_count() ||
      trigger_infos_.count() != trigger_name_map_.item_count()) {
    LOG_WARN("trigger info is non-consistent",
             K(trigger_infos_.count()),
             K(trigger_id_map_.item_count()),
             K(trigger_name_map_.item_count()));
    int over_write = 0;
    ObTriggerNameHashWrapper name_wrapper;
    OV (is_inited_, OB_NOT_INIT);
    OX (trigger_id_map_.clear());
    OX (trigger_name_map_.clear());
    for (ConstTriggerIter iter = trigger_infos_.begin();
         OB_SUCC(ret) && iter != trigger_infos_.end(); ++iter) {
      ObSimpleTriggerSchema *trigger_info = *iter;
      OV (OB_NOT_NULL(trigger_info));
      OX (name_wrapper.set_tenant_id(trigger_info->get_tenant_id()));
      OX (name_wrapper.set_database_id(trigger_info->get_database_id()));
      OX (name_wrapper.set_trigger_name(trigger_info->get_trigger_name()));
      OZ (trigger_id_map_.set_refactored(trigger_info->get_trigger_id(), trigger_info, over_write),
          trigger_info);
      if (OB_SUCC(ret)) {
        int hash_ret = trigger_name_map_.set_refactored(name_wrapper, trigger_info, over_write);
        if (OB_SUCCESS != hash_ret) {
          ret = OB_HASH_EXIST == hash_ret ? OB_SUCCESS : hash_ret;
          LOG_ERROR("build trigger name map failed", KR(ret), KR(hash_ret),
                    "exist_tenant_id", trigger_info->get_tenant_id(),
                    "exist_database_id", trigger_info->get_database_id(),
                    "exist_trigger_name", trigger_info->get_trigger_name());
        }

      }
      OX (LOG_INFO("rebuild", K(*trigger_info)));
    }
    if (OB_SUCC(ret)
        && (trigger_infos_.count() != trigger_id_map_.item_count()
            || trigger_infos_.count() != trigger_name_map_.item_count())) {
      ret = OB_DUPLICATE_OBJECT_NAME_EXIST;
      LOG_ERROR("schema meta is still not consistent after rebuild, need fixing", KR(ret),
                "trigger_info_cnt", trigger_infos_.count(),
                "trigger_id_cnt", trigger_id_map_.item_count(),
                "trigger_name_cnt", trigger_name_map_.item_count());
      LOG_DBA_ERROR(OB_DUPLICATE_OBJECT_NAME_EXIST,
                    "msg", "duplicate trigger name exist",
                    "trigger_info_cnt", trigger_infos_.count(),
                    "trigger_id_cnt", trigger_id_map_.item_count(),
                    "trigger_name_cnt", trigger_name_map_.item_count());
      right_to_die_or_duty_to_live();
    }
  }
  return ret;
}

int ObTriggerMgr::get_trigger_schema(uint64_t trigger_id, const ObSimpleTriggerSchema *&trigger_schema) const
{
  int ret = OB_SUCCESS;
  trigger_schema = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == trigger_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(trigger_id));
  } else {
    ObSimpleTriggerSchema *tmp_schema = NULL;
    int hash_ret = trigger_id_map_.get_refactored(trigger_id, tmp_schema);
    if (OB_SUCCESS == hash_ret) {
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(tmp_schema));
      } else {
        trigger_schema = tmp_schema;
      }
    }
  }
  return ret;
}

int ObTriggerMgr::get_trigger_schema(uint64_t tenant_id, uint64_t database_id,
                                     const ObString &trigger_name,
                                     const ObSimpleTriggerSchema *&trigger_schema) const
{
  int ret = OB_SUCCESS;
  trigger_schema = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == database_id || trigger_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(database_id), K(trigger_name));
  } else {
    ObSimpleTriggerSchema *tmp_schema = NULL;
    ObTriggerNameHashWrapper name_wrapper(tenant_id, database_id, trigger_name);
    int hash_ret = trigger_name_map_.get_refactored(name_wrapper, tmp_schema);
    if (OB_SUCCESS == hash_ret) {
      if (OB_ISNULL(tmp_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(tmp_schema));
      } else {
        trigger_schema = tmp_schema;
      }
    }
  }
  return ret;
}

int ObTriggerMgr::get_trigger_schemas_in_tenant(uint64_t tenant_id, ObIArray<const ObSimpleTriggerSchema *> &trigger_schemas) const
{
  int ret = OB_SUCCESS;
  ObTenantTriggerId tenant_trigger_id_lower(tenant_id, OB_MIN_ID);
  ConstTriggerIter tenant_trigger_begin =
      trigger_infos_.lower_bound(tenant_trigger_id_lower, compare_with_tenant_trigger_id);
  bool is_stop = false;
  trigger_schemas.reset();
  for (ConstTriggerIter iter = tenant_trigger_begin; OB_SUCC(ret) && iter != trigger_infos_.end() && !is_stop; ++iter) {
    const ObSimpleTriggerSchema *trigger = NULL;
    if (OB_ISNULL(trigger = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(trigger));
    } else if (tenant_id != trigger->get_tenant_id()) {
      is_stop = true;
    } else if (OB_FAIL(trigger_schemas.push_back(trigger))) {
      LOG_WARN("push back trigger failed", K(ret));
    }
  }
  return ret;
}

int ObTriggerMgr::del_trigger_schemas_in_tenant(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    ObArray<const ObSimpleTriggerSchema *> schemas;
    if (OB_FAIL(get_trigger_schemas_in_tenant(tenant_id, schemas))) {
      LOG_WARN("get trigger schemas failed", K(ret), K(tenant_id));
    } else {
      FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
        ObTenantTriggerId tenant_trigger_id(tenant_id, (*schema)->get_trigger_id());
        if (OB_FAIL(del_trigger(tenant_trigger_id))) {
          LOG_WARN("del trigger failed", K(tenant_trigger_id), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTriggerMgr::get_trigger_schemas_in_database(uint64_t tenant_id, uint64_t database_id,
                                                  ObIArray<const ObSimpleTriggerSchema *> &trigger_schemas) const
{
  int ret = OB_SUCCESS;
  ObTenantTriggerId tenant_trigger_id_lower(tenant_id, OB_MIN_ID);
  ConstTriggerIter tenant_trigger_begin =
      trigger_infos_.lower_bound(tenant_trigger_id_lower, compare_with_tenant_trigger_id);
  bool is_stop = false;
  trigger_schemas.reset();
  for (ConstTriggerIter iter = tenant_trigger_begin; OB_SUCC(ret) && iter != trigger_infos_.end() && !is_stop; ++iter) {
    const ObSimpleTriggerSchema *trigger = NULL;
    if (OB_ISNULL(trigger = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(trigger));
    } else if (tenant_id != trigger->get_tenant_id()) {
      is_stop = true;
    } else if (trigger->get_database_id() != database_id) {
      // do-nothing
    } else if (OB_FAIL(trigger_schemas.push_back(trigger))) {
      LOG_WARN("push back trigger failed", K(ret));
    }
  }
  return ret;
}

int ObTriggerMgr::del_trigger_schemas_in_databae(uint64_t tenant_id, uint64_t database_id)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == database_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    ObArray<const ObSimpleTriggerSchema *> schemas;
    if (OB_FAIL(get_trigger_schemas_in_database(tenant_id, database_id, schemas))) {
      LOG_WARN("get trigger schemas failed", K(ret), K(tenant_id));
    } else {
      FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
        ObTenantTriggerId tenant_trigger_id(tenant_id, (*schema)->get_trigger_id());
        if (OB_FAIL(del_trigger(tenant_trigger_id))) {
          LOG_WARN("del trigger failed", K(tenant_trigger_id), K(ret));
        }
      }
    }
  }

  return ret;
}

int ObTriggerMgr::get_trigger_schema_count(int64_t &trigger_schema_count) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    trigger_schema_count = trigger_infos_.size();
  }
  return ret;
}

int ObTriggerMgr::get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const
{
  int ret = OB_SUCCESS;
  schema_info.reset();
  schema_info.schema_type_ = TRIGGER_SCHEMA;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    schema_info.count_ = trigger_infos_.size();
    for (ConstTriggerIter it = trigger_infos_.begin(); OB_SUCC(ret) && it != trigger_infos_.end(); it++) {
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

}  // namespace schema
}  // namespace share
}  // namespace oceanbase
