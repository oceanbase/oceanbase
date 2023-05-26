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
#include "ob_context_mgr.h"
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

//////////////////////////////////////////////////////////////////////////////////
namespace context_mgr {

struct Deep_Copy_Filter
{
  Deep_Copy_Filter() {}
  bool operator() (ObContextSchema *value)
  {
    UNUSED(value);
    return true;
  }
};
struct Deep_Copy_Action
{
  Deep_Copy_Action(ObContextMgr &context_mgr)
      : context_mgr_(context_mgr) {}
  int operator() (ObContextSchema *value,
                  ObContextMgr::ContextInfos &infos,
                  ObContextMgr::ObContextMap &map)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("value is NULL", K(value), K(ret));
    } else if (OB_FAIL(context_mgr_.add_context(*value))) {
      LOG_WARN("push back failed", K(ret), K(value->get_namespace()));
    }
    UNUSED(infos);
    UNUSED(map);
    return ret;
  }
  ObContextMgr &context_mgr_;
};
struct Deep_Copy_EarlyStopCondition
{
  bool operator() ()
  {
    return false;
  }
};

}

ObContextHashWrapper ObGetContextKey<ObContextHashWrapper,ObContextSchema *>
::operator() (const ObContextSchema *context) const
{
  ObContextHashWrapper hash_wrap;
  if (!OB_ISNULL(context)) {
    hash_wrap.set_tenant_id(context->get_tenant_id());
    hash_wrap.set_context_namespace(context->get_namespace());
  }
  return hash_wrap;
}

ObContextMgr::ObContextMgr()
    : is_inited_(false),
      local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
      allocator_(local_allocator_),
      context_infos_(0, NULL, SET_USE_500("SchemaContext", ObCtxIds::SCHEMA_SERVICE)),
      context_map_(SET_USE_500("SchemaContext", ObCtxIds::SCHEMA_SERVICE))
{
}

ObContextMgr::ObContextMgr(ObIAllocator &allocator)
    : is_inited_(false),
      local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
      allocator_(allocator),
      context_infos_(0, NULL, SET_USE_500("SchemaContext", ObCtxIds::SCHEMA_SERVICE)),
      context_map_(SET_USE_500("SchemaContext", ObCtxIds::SCHEMA_SERVICE))
{
}

ObContextMgr::~ObContextMgr()
{
}

int ObContextMgr::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init private context manager twice", K(ret));
  } else if (OB_FAIL(context_map_.init())) {
    LOG_WARN("init private context map failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObContextMgr::reset()
{
  if (!is_inited_) {
    LOG_WARN_RET(OB_NOT_INIT, "context manger not init");
  } else {
    context_infos_.clear();
    context_map_.clear();
  }
}

ObContextMgr &ObContextMgr::operator =(const ObContextMgr &other)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("context manager not init", K(ret));
  } else  if (OB_FAIL(assign(other))) {
    LOG_WARN("assign failed", K(ret));
  }
  return *this;
}

int ObContextMgr::assign(const ObContextMgr &other)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("context manager not init", K(ret));
  } else if (this != &other) {
    if (OB_FAIL(context_map_.assign(other.context_map_))) {
      LOG_WARN("assign context map failed", K(ret));
    } else if (OB_FAIL(context_infos_.assign(other.context_infos_))) {
      LOG_WARN("assign context infos vector failed", K(ret));
    }
  }
  return ret;
}

int ObContextMgr::deep_copy(const ObContextMgr &other)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("context manager not init", K(ret));
  } else if (this != &other) {
    reset();
    context_mgr::Deep_Copy_Filter filter;
    context_mgr::Deep_Copy_Action action(*this);
    context_mgr::Deep_Copy_EarlyStopCondition condition;
    if (OB_FAIL((const_cast<ObContextMgr&>(other)).for_each(filter, action, condition))) {
      LOG_WARN("deep copy failed", K(ret));
    }
  }
  return ret;
}


int ObContextMgr::add_context(const ObContextSchema &context_schema)
{
  int ret = OB_SUCCESS;
  int overwrite = 1;
  ObContextSchema *new_context_schema = NULL;
  ContextIter iter = NULL;
  ObContextSchema *replaced_context = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("context manager not init", K(ret));
  } else if (OB_UNLIKELY(!context_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(context_schema));
  } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator_,
                                                 context_schema,
                                                 new_context_schema))) {
    LOG_WARN("alloc context schema failed", K(ret));
  } else if (OB_ISNULL(new_context_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(new_context_schema), K(ret));
  } else if (OB_FAIL(context_infos_.replace(new_context_schema,
                                             iter,
                                             compare_context,
                                             equal_context,
                                             replaced_context))) {
    LOG_WARN("failed to add context schema", K(ret));
  } else {
    ObContextHashWrapper hash_wrapper(new_context_schema->get_tenant_id(),
                                       new_context_schema->get_namespace());
    if (OB_FAIL(context_map_.set_refactored(hash_wrapper, new_context_schema, overwrite))) {
      LOG_WARN("build context hash map failed", K(ret));
    } else {
      LOG_INFO("add new context to context map", K(*new_context_schema));
    }
  }
  if (context_infos_.count() != context_map_.item_count()) {
    LOG_WARN("context info is non-consistent",
             "context_infos_count",
             context_infos_.count(),
             "context_map_item_count",
             context_map_.item_count(),
             "context_name",
             context_schema.get_namespace());
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObContextMgr::rebuild_context_hashmap(context_infos_,
                                                                       context_map_))) {
      LOG_WARN("rebuild context hashmap failed", K(tmp_ret));
    }
  }
  return ret;
}

int ObContextMgr::rebuild_context_hashmap(const ContextInfos &context_infos,
                                            ObContextMap& context_map)
{
  int ret = OB_SUCCESS;
  context_map.clear();
  ConstContextIter iter = context_infos.begin();
  for (; iter != context_infos.end() && OB_SUCC(ret); ++iter) {
    ObContextSchema *context_schema = *iter;
    if (OB_ISNULL(context_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("context schema is NULL", K(context_schema), K(ret));
    } else {
      bool overwrite = false;
      ObContextHashWrapper hash_wrapper(context_schema->get_tenant_id(),
                                        context_schema->get_namespace());
      int hash_ret = context_map.set_refactored(hash_wrapper, context_schema, overwrite);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_HASH_EXIST == hash_ret ? OB_SUCCESS : hash_ret;
        LOG_ERROR("build context hash map failed", KR(ret), KR(hash_ret),
                  "exist_tenant_id", context_schema->get_tenant_id(),
                  "exist_context_name", context_schema->get_namespace());
      }
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(context_infos.count() != context_map.count())) {
    ret = OB_DUPLICATE_OBJECT_NAME_EXIST;
    LOG_ERROR("Unexpected context map count", KR(ret), K(context_infos.count()), K(context_map.count()));
    LOG_DBA_ERROR(OB_DUPLICATE_OBJECT_NAME_EXIST,
                  "msg", "duplicate context name exist after rebuild",
                  K(context_infos.count()), K(context_map.count()));
    right_to_die_or_duty_to_live();
  }
  return ret;
}

int ObContextMgr::add_contexts(const common::ObIArray<ObContextSchema> &context_schemas)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < context_schemas.count() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(add_context(context_schemas.at(i)))) {
      LOG_WARN("push context failed", K(ret));
    }
  }
  return ret;
}

int ObContextMgr::del_context(const ObContextKey &context)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  ObContextSchema *schema_to_del = NULL;
  if (!context.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(context));
  } else if (OB_FAIL(context_infos_.remove_if(context,
                                              compare_with_context_key,
                                              equal_to_context_key,
                                              schema_to_del))) {
     if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("failed to remove context schema, item may not exist", K(ret));
    } else {
      LOG_WARN("failed to remove context schema, ",
              K(context.tenant_id_),
              K(context.context_id_),
              K(ret));
    }
  } else if (OB_ISNULL(schema_to_del)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("removed context schema return NULL, ",
             K(context.tenant_id_),
             K(context.context_id_),
             K(ret));
  } else {
    ObContextHashWrapper context_wrapper(schema_to_del->get_tenant_id(),
                                         schema_to_del->get_namespace());
    hash_ret = context_map_.erase_refactored(context_wrapper);
    if (OB_SUCCESS != hash_ret) {
      if (OB_HASH_NOT_EXIST == hash_ret) {
        LOG_INFO("failed to remove context schema, item may not exist", K(ret));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed delete context from context hashmap, ",
                K(ret),
                K(hash_ret),
                K(schema_to_del->get_tenant_id()),
                K(schema_to_del->get_namespace()));
      }
    }
  }

  if (context_infos_.count() != context_map_.item_count()) {
    LOG_WARN("context info is non-consistent",
             K(context_infos_.count()),
             K(context_map_.item_count()));
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret
                       = ObContextMgr::rebuild_context_hashmap(context_infos_, context_map_))) {
      LOG_WARN("rebuild context hashmap failed", K(tmp_ret));
    }
  }
  return ret;
}

int ObContextMgr::del_schemas_in_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    ObArray<const ObContextSchema *> schemas;
    if (OB_FAIL(get_context_schemas_in_tenant(tenant_id, schemas))) {
      LOG_WARN("get context schemas failed", K(ret), K(tenant_id));
    } else {
      FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
        ObContextKey context_key(tenant_id,
                                (*schema)->get_context_id());
        if (OB_FAIL(del_context(context_key))) {
          LOG_WARN("del context failed",
                   K(context_key.tenant_id_),
                   K(context_key.context_id_),
                   K(ret));
        }
      }
    }
  }
  return ret;
}

int ObContextMgr::get_context_schema(uint64_t tenant_id,
                                     uint64_t context_id,
                                     const ObContextSchema *&context_schema) const
{
  int ret = OB_SUCCESS;
  context_schema = NULL;
  ObContextKey context_key_lower(tenant_id, context_id);
  ConstContextIter tenant_context_begin =
      context_infos_.lower_bound(context_key_lower, compare_with_context_key);
  bool is_stop = false;
  for (ConstContextIter iter = tenant_context_begin;
      OB_SUCC(ret) && iter != context_infos_.end() && !is_stop; ++iter) {
    const ObContextSchema *context = NULL;
    if (OB_ISNULL(context = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(context));
    } else if (context->get_tenant_id() > tenant_id
              || (tenant_id == context->get_tenant_id()
                  && context->get_context_id() > context_id) ) {
      is_stop = true;
    } else if (tenant_id == context->get_tenant_id()
               && context_id == context->get_context_id()) {
      context_schema = context;
    }
  }
  return ret;
}

template<typename Filter, typename Acation, typename EarlyStopCondition>
int ObContextMgr::for_each(Filter &filter, Acation &action, EarlyStopCondition &condition)
{
  int ret = OB_SUCCESS;
  ConstContextIter iter = context_infos_.begin();
  for (; iter != context_infos_.end() && OB_SUCC(ret); iter++) {
    ObContextSchema *value = NULL;
    if (OB_ISNULL(value = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret));
    } else {
      if (filter(value)) {
        if (OB_FAIL(action(value, context_infos_, context_map_))) {
            LOG_WARN("action failed", K(ret));
        }
      }
    }
  }
  UNUSED(condition);
  return ret;
}

bool ObContextMgr::compare_context(const ObContextSchema *lhs,
                                   const ObContextSchema *rhs)
{
  return lhs->get_tenant_id() == rhs->get_tenant_id()
         ? (lhs->get_context_id() < rhs->get_context_id())
            : (lhs->get_tenant_id() < rhs->get_tenant_id());
}

bool ObContextMgr::equal_context(const ObContextSchema *lhs,
                                   const ObContextSchema *rhs)
{
  return lhs->get_tenant_id() == rhs->get_tenant_id()
         && lhs->get_context_id() == rhs->get_context_id();
}

bool ObContextMgr::compare_with_context_key(const ObContextSchema *lhs,
                                            const ObContextKey &context_key)
{
  return NULL != lhs ? (lhs->get_context_key() < context_key) : false;
}

bool ObContextMgr::equal_to_context_key(const ObContextSchema *lhs,
                                        const ObContextKey &context_key)
{
  return NULL != lhs ? (lhs->get_context_key() == context_key) : false;
}

int ObContextMgr::get_context_schemas_in_tenant(const uint64_t tenant_id,
    ObIArray<const ObContextSchema *> &context_schemas) const
{
  int ret = OB_SUCCESS;
  context_schemas.reset();

  ObContextKey context_key_lower(tenant_id, OB_MIN_ID);
  ConstContextIter tenant_context_begin =
      context_infos_.lower_bound(context_key_lower, compare_with_context_key);
  bool is_stop = false;
  for (ConstContextIter iter = tenant_context_begin;
      OB_SUCC(ret) && iter != context_infos_.end() && !is_stop; ++iter) {
    const ObContextSchema *context = NULL;
    if (OB_ISNULL(context = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(context));
    } else if (tenant_id != context->get_tenant_id()) {
      is_stop = true;
    } else if (OB_FAIL(context_schemas.push_back(context))) {
      LOG_WARN("push back context failed", K(ret));
    }
  }

  return ret;
}

int ObContextMgr::get_context_schema_count(int64_t &context_schema_count) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    context_schema_count = context_infos_.count();
  }
  return ret;
}

int ObContextMgr::get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const
{
  int ret = OB_SUCCESS;
  schema_info.reset();
  schema_info.schema_type_ = CONTEXT_SCHEMA;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    schema_info.count_ = context_infos_.size();
    for (ConstContextIter it = context_infos_.begin();
                     OB_SUCC(ret) && it != context_infos_.end(); it++) {
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

int ObContextMgr::get_context_schema_with_name(const uint64_t tenant_id,
                                               const ObString &ctx_namespace,
                                               const ObContextSchema *&context_schema) const
{
  int ret = OB_SUCCESS;
  context_schema = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id
             || 0 == ctx_namespace.length()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(ctx_namespace));
  } else {
    ObContextSchema *tmp_schema = NULL;
    ObContextHashWrapper hash_wrap(tenant_id, ctx_namespace);
    if (OB_FAIL(context_map_.get_refactored(hash_wrap, tmp_schema))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("context is not exist", K(tenant_id), K(ctx_namespace),
                 "map_cnt", context_map_.item_count());
      }
    } else {
      context_schema = tmp_schema;
    }
  }
  return ret;
}

} //end of namespace schema
} //end of namespace share
} //end of namespace oceanbase
