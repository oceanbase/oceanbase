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
#include "ob_security_audit_mgr.h"
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

namespace security_audit_mgr {

struct Deep_Copy_Filter
{
  Deep_Copy_Filter() {}
  bool operator() (ObSAuditSchema *value)
  {
    UNUSED(value);
    return true;
  }
};
struct Deep_Copy_Action
{
  Deep_Copy_Action(ObSAuditMgr &audit_mgr)
      : audit_mgr_(audit_mgr) {}
  int operator() (ObSAuditSchema *value,
                  ObSAuditMgr::AuditInfos &infos,
                  ObSAuditMgr::ObAuditMap &map)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("value is NULL", K(value), K(ret));
    } else if (OB_FAIL(audit_mgr_.add_audit(*value))) {
      LOG_WARN("push back failed", K(ret), K(*value));
    }
    UNUSED(infos);
    UNUSED(map);
    return ret;
  }
  ObSAuditMgr &audit_mgr_;
};
struct Deep_Copy_EarlyStopCondition
{
  bool operator() ()
  {
    return false;
  }
};

}

ObSAuditMgr::ObSAuditMgr()
    : is_inited_(false),
      local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
      allocator_(local_allocator_),
      audit_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_SECURITY_AUDIT, ObCtxIds::SCHEMA_SERVICE)),
      audit_map_(SET_USE_500(ObModIds::OB_SCHEMA_SECURITY_AUDIT, ObCtxIds::SCHEMA_SERVICE))
{
}

ObSAuditMgr::ObSAuditMgr(ObIAllocator &allocator)
    : is_inited_(false),
      local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
      allocator_(allocator),
      audit_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_SECURITY_AUDIT, ObCtxIds::SCHEMA_SERVICE)),
      audit_map_(SET_USE_500(ObModIds::OB_SCHEMA_SECURITY_AUDIT, ObCtxIds::SCHEMA_SERVICE))
{
}

ObSAuditMgr::~ObSAuditMgr()
{
}

int ObSAuditMgr::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init private audit manager twice", K(ret));
  } else if (OB_FAIL(audit_map_.init())) {
    LOG_WARN("init private audit map failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObSAuditMgr::reset()
{
  if (OB_UNLIKELY(!is_inited_)) {
    LOG_WARN_RET(OB_NOT_INIT, "audit manger not init");
  } else {
    audit_infos_.clear();
    audit_map_.clear();
  }
}

ObSAuditMgr &ObSAuditMgr::operator =(const ObSAuditMgr &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("audit manager not init", K(ret));
  } else  if (OB_FAIL(assign(other))) {
    LOG_WARN("assign failed", K(ret));
  }
  return *this;
}

int ObSAuditMgr::assign(const ObSAuditMgr &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("audit manager not init", K(ret));
  } else if (this != &other) {
    if (OB_FAIL(audit_map_.assign(other.audit_map_))) {
      LOG_WARN("assign audit map failed", K(ret));
    } else if (OB_FAIL(audit_infos_.assign(other.audit_infos_))) {
      LOG_WARN("assign audit infos vector failed", K(ret));
    }
  }
  return ret;
}

int ObSAuditMgr::deep_copy(const ObSAuditMgr &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("audit manager not init", K(ret));
  } else if (this != &other) {
    reset();
    security_audit_mgr::Deep_Copy_Filter filter;
    security_audit_mgr::Deep_Copy_Action action(*this);
    security_audit_mgr::Deep_Copy_EarlyStopCondition condition;
    if (OB_FAIL((const_cast<ObSAuditMgr&>(other)).for_each(filter, action, condition))) {
      LOG_WARN("deep copy failed", K(ret));
    }
  }
  return ret;
}

int ObSAuditMgr::get_audit_schema_count(int64_t &audit_schema_count) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    audit_schema_count = audit_infos_.count();
  }
  return ret;
}

int ObSAuditMgr::add_audit(const ObSAuditSchema &audit_schema)
{
  int ret = OB_SUCCESS;
  int overwrite = 1;
  ObSAuditSchema *new_audit_schema = NULL;
  AuditIter iter = NULL;
  ObSAuditSchema *replaced_audit = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("audit manager not init", K(ret));
  } else if (OB_UNLIKELY(!audit_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(audit_schema));
  } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator_, audit_schema, new_audit_schema))) {
    LOG_WARN("alloca audit schema failed", K(ret));
  } else if (OB_ISNULL(new_audit_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(new_audit_schema), K(ret));
  } else if (OB_FAIL(audit_infos_.replace(new_audit_schema,
                                          iter,
                                          compare_adudit,
                                          equal_audit,
                                          replaced_audit))) {
    LOG_WARN("failed to add audit schema", K(ret));
  } else {
    ObAuditHashWrapper hash_wrapper(new_audit_schema->get_tenant_id(),
                                    new_audit_schema->get_audit_type(),
                                    new_audit_schema->get_owner_id(),
                                    new_audit_schema->get_operation_type());
    if (OB_FAIL(audit_map_.set_refactored(hash_wrapper, new_audit_schema, overwrite))) {
      LOG_WARN("build audit hash map failed", K(ret));
    } else {
      LOG_INFO("add new audit to audit map", KPC(new_audit_schema));
    }
  }
  if (audit_infos_.count() != audit_map_.item_count()) {
    LOG_WARN("audit info is non-consistent",
             "audit_infos_count",
             audit_infos_.count(),
             "audit_map_item_count",
             audit_map_.item_count(),
             K(audit_schema));
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObSAuditMgr::rebuild_audit_hashmap(audit_infos_,
                                                                    audit_map_))) {
      LOG_WARN("rebuild audit hashmap failed", K(tmp_ret));
    }
  }
  LOG_DEBUG("add new audit schema", K(ret), K(audit_schema), K(audit_map_.item_count()));
  return ret;
}

int ObSAuditMgr::rebuild_audit_hashmap(const AuditInfos &audit_infos,
                                      ObAuditMap& audit_map)
{
  int ret = OB_SUCCESS;
  audit_map.clear();
  ConstAuditIter iter = audit_infos.begin();
  for (; iter != audit_infos.end() && OB_SUCC(ret); ++iter) {
    ObSAuditSchema *audit_schema = *iter;
    if (OB_ISNULL(audit_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("audit schema is NULL", K(audit_schema), K(ret));
    } else {
      bool overwrite = true;
      ObAuditHashWrapper hash_wrapper(audit_schema->get_tenant_id(),
                                       audit_schema->get_audit_type(),
                                       audit_schema->get_owner_id(),
                                       audit_schema->get_operation_type());
      if (OB_FAIL(audit_map.set_refactored(hash_wrapper, audit_schema, overwrite))) {
        LOG_WARN("build audit hash map failed", K(ret));
      }
    }
  }
  return ret;
}

int ObSAuditMgr::add_audits(const common::ObIArray<ObSAuditSchema> &audit_schemas)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < audit_schemas.count() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(add_audit(audit_schemas.at(i)))) {
      LOG_WARN("push audit failed", K(ret));
    }
  }
  return ret;
}

int ObSAuditMgr::del_audit(const ObTenantAuditKey &audit)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  ObSAuditSchema *schema_to_del = NULL;
  if (OB_UNLIKELY(!audit.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(audit));
  } else if (OB_FAIL(audit_infos_.remove_if(audit,
                                            compare_with_tenant_audit_id,
                                            equal_to_tenant_audit_id,
                                            schema_to_del))) {
    LOG_WARN("failed to remove audit schema", K(audit), K(ret));
  } else if (OB_ISNULL(schema_to_del)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("removed audit schema return NULL", K(audit), K(ret));
  } else {
    ObAuditHashWrapper audit_wrapper(schema_to_del->get_tenant_id(),
                                     schema_to_del->get_audit_type(),
                                     schema_to_del->get_owner_id(),
                                     schema_to_del->get_operation_type());
    hash_ret = audit_map_.erase_refactored(audit_wrapper);
    if (OB_SUCCESS != hash_ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed delete audit from audit hashmap, ",
               K(ret),
               K(hash_ret),
               KPC(schema_to_del));
    }
  }

  if (audit_infos_.count() != audit_map_.item_count()) {
    LOG_WARN("audit info is non-consistent",
             "audit_infos_count",
             audit_infos_.count(),
             "audit_map_item_count",
             audit_map_.item_count(),
             K(audit));
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObSAuditMgr::rebuild_audit_hashmap(audit_infos_, audit_map_))) {
      LOG_WARN("rebuild audit hashmap failed", K(tmp_ret));
    }
  }
  LOG_DEBUG("del new audit schema", K(ret), K(audit), K(audit_map_.item_count()));
  return ret;
}

int ObSAuditMgr::del_schemas_in_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    ObArray<const ObSAuditSchema *> schemas;
    if (OB_FAIL(get_audit_schemas_in_tenant(tenant_id, schemas))) {
      LOG_WARN("get audit schemas failed", K(ret), K(tenant_id));
    } else {
      FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
        ObTenantAuditKey tenant_audit_id(tenant_id,
                                        (*schema)->get_audit_id());
        if (OB_FAIL(del_audit(tenant_audit_id))) {
          LOG_WARN("del audit failed", K(tenant_audit_id), K(ret));
        }
      }
    }
  }
  return ret;
}

//////////////////////////////////////////////////////////////////////////////////
struct Get_Audit_Schema_Filter
{
  Get_Audit_Schema_Filter(const uint64_t audit_id)
  : audit_id_(audit_id) {}
  bool operator() (ObSAuditSchema *value)
  {
    return (value->get_audit_id() == audit_id_);
  }
  uint64_t audit_id_;
};
struct Get_Audit_Schema_Action
{
  Get_Audit_Schema_Action(const ObSAuditSchema *&audit_schema)
      : audit_schema_(audit_schema) {}
  int operator() (ObSAuditSchema *value,
                  ObSAuditMgr::AuditInfos &infos,
                  ObSAuditMgr::ObAuditMap &map)
  {
    UNUSED(infos);
    UNUSED(map);
    audit_schema_ = value;
    return OB_SUCCESS;
  }
  const ObSAuditSchema *&audit_schema_;
};
struct Get_Audit_Schema_EarlyStopCondition
{
  Get_Audit_Schema_EarlyStopCondition(const ObSAuditSchema *&audit_schema)
      : audit_schema_(audit_schema) {}
  bool operator() ()
  {
    return audit_schema_ != NULL;
  }
  const ObSAuditSchema *&audit_schema_;
};
///////////////////////////////////////////////////////////////////////////////////////////////////

int ObSAuditMgr::get_audit_schema(const uint64_t &audit_id,
                                  const ObSAuditSchema *&audit_schema) const
{
  int ret = OB_SUCCESS;
  audit_schema = NULL;
  Get_Audit_Schema_Filter filter(audit_id);
  Get_Audit_Schema_Action action(audit_schema);
  Get_Audit_Schema_EarlyStopCondition condition(audit_schema);
  if (OB_FAIL((const_cast<ObSAuditMgr&>(*this)).for_each(filter,action, condition))) {
    LOG_WARN("get audit schema failed", K(ret));
  }
  return ret;
}

int ObSAuditMgr::get_audit_schema(const uint64_t tenant_id,
                                  const ObSAuditType audit_type,
                                  const uint64_t owner_id,
                                  const ObSAuditOperationType operation_type,
                                  const ObSAuditSchema *&audit_schema) const
{
  int ret = OB_SUCCESS;
  audit_schema = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
             || OB_UNLIKELY(audit_type == AUDIT_INVALID)
             || OB_UNLIKELY(owner_id == common::OB_INVALID_ID)
             || OB_UNLIKELY(operation_type == AUDIT_OP_INVALID)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(audit_type), K(owner_id), K(operation_type));
  } else {
    ObSAuditSchema *tmp_schema = NULL;
    ObAuditHashWrapper hash_wrap(tenant_id, audit_type, owner_id, operation_type);
    if (OB_FAIL(audit_map_.get_refactored(hash_wrap, tmp_schema))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_DEBUG("audit is not exist", K(hash_wrap), "map_cnt", audit_map_.item_count());
      }
    } else {
      audit_schema = tmp_schema;
    }
  }
  return ret;
}


template<typename Filter, typename Acation, typename EarlyStopCondition>
int ObSAuditMgr::for_each(Filter &filter, Acation &action, EarlyStopCondition &condition)
{
  int ret = OB_SUCCESS;
  ConstAuditIter iter = audit_infos_.begin();
  for (; iter != audit_infos_.end() && OB_SUCC(ret); iter++) {
    ObSAuditSchema *value = NULL;
    if (OB_ISNULL(value = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret));
    } else {
      if (filter(value)) {
        if (OB_FAIL(action(value, audit_infos_, audit_map_))) {
            LOG_WARN("action failed", K(ret));
        }
      }
    }
  }
  UNUSED(condition);
  return ret;
}

bool ObSAuditMgr::compare_with_tenant_audit_id(const ObSAuditSchema *lhs,
                                               const ObTenantAuditKey &audit_key)
{
  return NULL != lhs ? (lhs->get_audit_key() < audit_key) : false;
}

bool ObSAuditMgr::equal_to_tenant_audit_id(const ObSAuditSchema *lhs,
                                             const ObTenantAuditKey &audit_key)
{
  return NULL != lhs ? (lhs->get_audit_key() == audit_key) : false;
}

int ObSAuditMgr::get_audit_schemas_in_tenant(const uint64_t tenant_id,
    ObIArray<const ObSAuditSchema *> &audit_schemas) const
{
  int ret = OB_SUCCESS;
  audit_schemas.reset();

  ObTenantAuditKey tenant_outine_id_lower(tenant_id, 0);
  ConstAuditIter tenant_audit_begin =
      audit_infos_.lower_bound(tenant_outine_id_lower, compare_with_tenant_audit_id);
  bool is_stop = false;
  for (ConstAuditIter iter = tenant_audit_begin;
      OB_SUCC(ret) && iter != audit_infos_.end() && !is_stop; ++iter) {
    const ObSAuditSchema *audit = NULL;
    if (OB_ISNULL(audit = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(audit));
    } else if (tenant_id != audit->get_tenant_id()) {
      is_stop = true;
    } else if (OB_FAIL(audit_schemas.push_back(audit))) {
      LOG_WARN("push back audit failed", K(ret));
    } else {
      LOG_DEBUG("new audit schema added", K(ret), K(tenant_id), K(*audit));
    }
  }

  return ret;
}

int ObSAuditMgr::get_audit_schemas_in_tenant(const uint64_t tenant_id,
    const ObSAuditType audit_type, const uint64_t owner_id,
    ObIArray<const ObSAuditSchema *> &audit_schemas) const
{
  int ret = OB_SUCCESS;
  audit_schemas.reset();

  ObTenantAuditKey tenant_outine_id_lower(tenant_id, 0);
  ConstAuditIter tenant_audit_begin =
      audit_infos_.lower_bound(tenant_outine_id_lower, compare_with_tenant_audit_id);
  bool is_stop = false;
  for (ConstAuditIter iter = tenant_audit_begin;
      OB_SUCC(ret) && iter != audit_infos_.end() && !is_stop; ++iter) {
    const ObSAuditSchema *audit = NULL;
    if (OB_ISNULL(audit = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(audit));
    } else if (tenant_id != audit->get_tenant_id()) {
      is_stop = true;
    } else if (owner_id != audit->get_owner_id() || audit_type != audit->get_audit_type()) {
      continue;
    } else if (OB_FAIL(audit_schemas.push_back(audit))) {
      LOG_WARN("push back audit failed", K(ret));
    } else {
      LOG_DEBUG("new audit schema added", K(ret), K(tenant_id), K(*audit));
    }
  }

  return ret;
}

int ObSAuditMgr::get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const
{
  int ret = OB_SUCCESS;
  schema_info.reset();
  schema_info.schema_type_ = AUDIT_SCHEMA;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    schema_info.count_ = audit_infos_.size();
    for (ConstAuditIter it = audit_infos_.begin(); OB_SUCC(ret) && it != audit_infos_.end(); it++) {
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
