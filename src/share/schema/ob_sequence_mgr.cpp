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
#include "ob_sequence_mgr.h"
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
namespace sequence_mgr {
struct Del_Schema_In_Tenant_Filter
{
  Del_Schema_In_Tenant_Filter(uint64_t tenant_id): tenant_id_(tenant_id) {}
  bool operator() (ObSequenceSchema *value)
  {
    return value->get_tenant_id() == tenant_id_;
  }
  uint64_t tenant_id_;
};
struct Del_Schema_In_Tenant_Action
{
  Del_Schema_In_Tenant_Action() {}
  int operator() (ObSequenceSchema *value,
                  ObSequenceMgr::SequenceInfos &infos,
                  ObSequenceMgr::ObSequenceMap &map)
  {
    int ret = OB_SUCCESS;
    ObSequenceSchema *value_to_del = NULL;
    if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid argument, delete value is null", K(value), K(ret));
    } else if (OB_FAIL(infos.remove_if(value,
                                       ObSequenceMgr::compare_sequence,
                                       ObSequenceMgr::equal_sequence,
                                       value_to_del))) {
      LOG_WARN("fail to remove sequence schema", K(*value), K(ret));
    } else if (OB_ISNULL(value_to_del)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("removed sequence schema return NULL", K(*value), K(value_to_del), K(ret));
    } else {
      ObSequenceHashWrapper hash_wrapper(value->get_tenant_id(),
                                         value->get_database_id(),
                                         value->get_sequence_name());
      if (OB_FAIL(map.erase_refactored(hash_wrapper))) {
        LOG_WARN("fail to delete sequence schema from hashmap", K(*value), K(ret));
      }
    }

    if (infos.count() != map.item_count()) {
      LOG_WARN("sequence info is non-consistent",
               "sequence_infos_count",
               infos.count(),
               "sequence_map_item_count",
               map.item_count(),
               "sequence_id",
               value->get_sequence_id(),
               "sequence_name",
               value->get_sequence_name());
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = ObSequenceMgr::rebuild_sequence_hashmap(infos, map))) {
        LOG_WARN("rebuild sequence hashmap failed", K(tmp_ret));
      }
    }

    return ret;
  }
};
struct Del_Schema_In_Tenant_EarlyStopCondition
{
  Del_Schema_In_Tenant_EarlyStopCondition(){}
  bool operator() ()
  {
    return false;
  }
};
//////////////////////////////////////////////////////////////////////////////////
struct Del_Schema_Filter
{
  Del_Schema_Filter(const ObTenantSequenceId &sequence): sequence_(sequence) {}
  bool operator() (ObSequenceSchema *value)
  {
    return value->get_tenant_id() == sequence_.tenant_id_ &&
        value->get_sequence_id() == sequence_.sequence_id_;
  }
  const ObTenantSequenceId &sequence_;
};
//////////////////////////////////////////////////////////////////////////////////
struct Get_Sequence_Schema_Filter
{
  Get_Sequence_Schema_Filter(uint64_t sequence_id): sequence_id_(sequence_id) {}
  bool operator() (ObSequenceSchema *value)
  {
    return value->get_sequence_id() == sequence_id_;
  }
  uint64_t sequence_id_;
};
struct Get_Sequence_Schema_Action
{
  Get_Sequence_Schema_Action(const ObSequenceSchema *&sequence_schema)
      : sequence_schema_(sequence_schema) {}
  int operator() (ObSequenceSchema *value,
                  ObSequenceMgr::SequenceInfos &infos,
                  ObSequenceMgr::ObSequenceMap &map)
  {
    UNUSED(infos);
    UNUSED(map);
    sequence_schema_ = value;
    return OB_SUCCESS;
  }
  const ObSequenceSchema *&sequence_schema_;
};
struct Get_Sequence_Schema_EarlyStopCondition
{
  Get_Sequence_Schema_EarlyStopCondition(const ObSequenceSchema *&sequence_schema)
      : sequence_schema_(sequence_schema) {}
  bool operator() ()
  {
    return sequence_schema_ != NULL;
  }
  const ObSequenceSchema *&sequence_schema_;
};
///////////////////////////////////////////////////////////////////////////////////////////////////
struct Get_Sequence_Filter
{
  Get_Sequence_Filter(uint64_t db_id, uint64_t tenant_id)
      : db_id_(db_id), tenant_id_(tenant_id) {}
  bool operator() (ObSequenceSchema *value)
  {
    return value->get_database_id() == db_id_ &&
        value->get_tenant_id() == tenant_id_;
  }
  uint64_t db_id_;
  uint64_t tenant_id_;
};
struct Get_Sequence_Action
{
  Get_Sequence_Action(ObIArray<const ObSequenceSchema *> &sequence_schemas)
      : sequence_schemas_(sequence_schemas) {}
  int operator() (ObSequenceSchema *value,
                  ObSequenceMgr::SequenceInfos &infos,
                  ObSequenceMgr::ObSequenceMap &map)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(sequence_schemas_.push_back(value))) {
      LOG_WARN("push back failed", K(ret), K(value->get_sequence_name_str()));
    }
    UNUSED(infos);
    UNUSED(map);
    return ret;
  }
  ObIArray<const ObSequenceSchema *> &sequence_schemas_;
};
struct Get_Sequence_EarlyStopCondition
{
  bool operator() ()
  {
    return true;
  }
};
///////////////////////////////////////////////////////////////////////////////////////////////////
struct Deep_Copy_Filter
{
  Deep_Copy_Filter() {}
  bool operator() (ObSequenceSchema *value)
  {
    UNUSED(value);
    return true;
  }
};
struct Deep_Copy_Action
{
  Deep_Copy_Action(ObSequenceMgr &sequence_mgr)
      : sequence_mgr_(sequence_mgr) {}
  int operator() (ObSequenceSchema *value,
                  ObSequenceMgr::SequenceInfos &infos,
                  ObSequenceMgr::ObSequenceMap &map)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("value is NULL", K(value), K(ret));
    } else if (OB_FAIL(sequence_mgr_.add_sequence(*value))) {
      LOG_WARN("push back failed", K(ret), K(value->get_sequence_name_str()));
    }
    UNUSED(infos);
    UNUSED(map);
    return ret;
  }
  ObSequenceMgr &sequence_mgr_;
};
struct Deep_Copy_EarlyStopCondition
{
  bool operator() ()
  {
    return false;
  }
};

}

ObSequenceHashWrapper ObGetSequenceKey<ObSequenceHashWrapper, ObSequenceSchema *>::operator() (const ObSequenceSchema * sequence) const
{
  ObSequenceHashWrapper hash_wrap;
  if (!OB_ISNULL(sequence)) {
    hash_wrap.set_tenant_id(sequence->get_tenant_id());
    hash_wrap.set_database_id(sequence->get_database_id());
    hash_wrap.set_sequence_name(sequence->get_sequence_name());
  }
  return hash_wrap;
}

ObSequenceMgr::ObSequenceMgr()
    : is_inited_(false),
      local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
      allocator_(local_allocator_),
      sequence_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_SEQUENCE, ObCtxIds::SCHEMA_SERVICE)),
      sequence_map_(SET_USE_500(ObModIds::OB_SCHEMA_SEQUENCE, ObCtxIds::SCHEMA_SERVICE))
{
}

ObSequenceMgr::ObSequenceMgr(ObIAllocator &allocator)
    : is_inited_(false),
      local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
      allocator_(allocator),
      sequence_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_SEQUENCE, ObCtxIds::SCHEMA_SERVICE)),
      sequence_map_(SET_USE_500(ObModIds::OB_SCHEMA_SEQUENCE, ObCtxIds::SCHEMA_SERVICE))
{
}

ObSequenceMgr::~ObSequenceMgr()
{
}

int ObSequenceMgr::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init private sequence manager twice", K(ret));
  } else if (OB_FAIL(sequence_map_.init())) {
    LOG_WARN("init private sequence map failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObSequenceMgr::reset()
{
  if (!is_inited_) {
    LOG_WARN_RET(OB_NOT_INIT, "sequence manger not init");
  } else {
    sequence_infos_.clear();
    sequence_map_.clear();
  }
}

ObSequenceMgr &ObSequenceMgr::operator =(const ObSequenceMgr &other)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sequence manager not init", K(ret));
  } else  if (OB_FAIL(assign(other))) {
    LOG_WARN("assign failed", K(ret));
  }
  return *this;
}

int ObSequenceMgr::assign(const ObSequenceMgr &other)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sequence manager not init", K(ret));
  } else if (this != &other) {
    if (OB_FAIL(sequence_map_.assign(other.sequence_map_))) {
      LOG_WARN("assign sequence map failed", K(ret));
    } else if (OB_FAIL(sequence_infos_.assign(other.sequence_infos_))) {
      LOG_WARN("assign sequence infos vector failed", K(ret));
    }
  }
  return ret;
}

int ObSequenceMgr::deep_copy(const ObSequenceMgr &other)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sequence manager not init", K(ret));
  } else if (this != &other) {
    reset();
    sequence_mgr::Deep_Copy_Filter filter;
    sequence_mgr::Deep_Copy_Action action(*this);
    sequence_mgr::Deep_Copy_EarlyStopCondition condition;
    if (OB_FAIL((const_cast<ObSequenceMgr&>(other)).for_each(filter, action, condition))) {
      LOG_WARN("deep copy failed", K(ret));
    }
  }
  return ret;
}


int ObSequenceMgr::add_sequence(const ObSequenceSchema &sequence_schema)
{
  int ret = OB_SUCCESS;
  int overwrite = 1;
  ObSequenceSchema *new_sequence_schema = NULL;
  SequenceIter iter = NULL;
  ObSequenceSchema *replaced_sequence = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sequence manager not init", K(ret));
  } else if (OB_UNLIKELY(!sequence_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(sequence_schema));
  } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator_,
                                                 sequence_schema,
                                                 new_sequence_schema))) {
    LOG_WARN("alloca sequence schema failed", K(ret));
  } else if (OB_ISNULL(new_sequence_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(new_sequence_schema), K(ret));
  } else if (OB_FAIL(sequence_infos_.replace(new_sequence_schema,
                                             iter,
                                             compare_sequence,
                                             equal_sequence,
                                             replaced_sequence))) {
    LOG_WARN("failed to add sequence schema", K(ret));
  } else {
    ObSequenceHashWrapper hash_wrapper(new_sequence_schema->get_tenant_id(),
                                       new_sequence_schema->get_database_id(),
                                       new_sequence_schema->get_sequence_name());
    if (OB_FAIL(sequence_map_.set_refactored(hash_wrapper, new_sequence_schema, overwrite))) {
      LOG_WARN("build sequence hash map failed", K(ret));
    } else {
      LOG_INFO("add new sequence to sequence map", K(*new_sequence_schema));
    }
  }
  if (sequence_infos_.count() != sequence_map_.item_count()) {
    LOG_WARN("sequence info is non-consistent",
             "sequence_infos_count",
             sequence_infos_.count(),
             "sequence_map_item_count",
             sequence_map_.item_count(),
             "sequence_id",
             sequence_schema.get_sequence_id(),
             "sequence_name",
             sequence_schema.get_sequence_name());
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObSequenceMgr::rebuild_sequence_hashmap(sequence_infos_,
                                                                         sequence_map_))) {
      LOG_WARN("rebuild sequence hashmap failed", K(tmp_ret));
    }
  }
  return ret;
}

int ObSequenceMgr::rebuild_sequence_hashmap(const SequenceInfos &sequence_infos,
                                            ObSequenceMap& sequence_map)
{
  int ret = OB_SUCCESS;
  sequence_map.clear();
  ConstSequenceIter iter = sequence_infos.begin();
  for (; iter != sequence_infos.end() && OB_SUCC(ret); ++iter) {
    ObSequenceSchema *sequence_schema = *iter;
    if (OB_ISNULL(sequence_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sequence schema is NULL", K(sequence_schema), K(ret));
    } else {
      bool overwrite = true;
      ObSequenceHashWrapper hash_wrapper(sequence_schema->get_tenant_id(),
                                        sequence_schema->get_database_id(),
                                        sequence_schema->get_sequence_name());
      if (OB_FAIL(sequence_map.set_refactored(hash_wrapper, sequence_schema, overwrite))) {
        LOG_WARN("build sequence hash map failed", K(ret));
      }
    }
  }
  return ret;
}

int ObSequenceMgr::add_sequences(const common::ObIArray<ObSequenceSchema> &sequence_schemas)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < sequence_schemas.count() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(add_sequence(sequence_schemas.at(i)))) {
      LOG_WARN("push sequence failed", K(ret));
    }
  }
  return ret;
}

int ObSequenceMgr::get_sequence_schema_with_name(
  const uint64_t tenant_id,
  const uint64_t database_id,
  const ObString &name,
  const ObSequenceSchema *&sequence_schema) const
{
  int ret = OB_SUCCESS;
  sequence_schema = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == database_id
             || name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(database_id), K(name));
  } else {
    ObSequenceSchema *tmp_schema = NULL;
    ObSequenceHashWrapper hash_wrap(tenant_id, database_id, name);
    if (OB_FAIL(sequence_map_.get_refactored(hash_wrap, tmp_schema))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("sequence is not exist", K(tenant_id), K(database_id), K(name),
                 "map_cnt", sequence_map_.item_count());
      }
    } else {
      sequence_schema = tmp_schema;
    }
  }
  return ret;
}

int ObSequenceMgr::get_sequence_info_version(uint64_t sequence_id, int64_t &sequence_version) const
{
  int ret = OB_SUCCESS;
  sequence_version = OB_INVALID_ID;
  ConstSequenceIter iter = sequence_infos_.begin();
  for (; OB_SUCC(ret) && iter != sequence_infos_.end(); ++iter) {
    const ObSequenceSchema *sequence = NULL;
    if (OB_ISNULL(sequence = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret));
    } else if (sequence->get_sequence_id() == sequence_id) {
      sequence_version = sequence->get_schema_version();
      break;
    }
  }
  return ret;
}

//suppose there are no vital errors ,
//this function will always return ob_success no matter do_exist = true or not.
int ObSequenceMgr::get_object(const uint64_t tenant_id,
                             const uint64_t database_id,
                             const ObString &name,
                             uint64_t &obj_database_id,
                             uint64_t &sequence_id,
                             ObString &obj_table_name,
                             bool &do_exist) const
{
  int ret = OB_SUCCESS;
  do_exist = false;
  const ObSequenceSchema *sequence_schema = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == database_id
             || name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(database_id), K(name));
  }

  for (int64_t i = 0; OB_SUCC(ret) && false == do_exist && i < 2; ++i) {
    uint64_t cur_database_id = (i == 0 ? database_id : OB_PUBLIC_SCHEMA_ID);
    if (OB_FAIL(get_sequence_schema_with_name(tenant_id, cur_database_id, name, sequence_schema))) {
      LOG_WARN("fail to get sequence schema with name",
               K(tenant_id), K(cur_database_id), K(name), K(ret));
    } else if (OB_ISNULL(sequence_schema)) {
      LOG_INFO("sequence is not exist", K(tenant_id), K(cur_database_id), K(name), K(ret));
    } else {
      do_exist = true;
    }
  }

  if (OB_SUCC(ret) && sequence_schema != NULL) {
    do_exist = true;
    obj_database_id = sequence_schema->get_database_id();
    obj_table_name = sequence_schema->get_sequence_name();
    sequence_id = sequence_schema->get_sequence_id();
  }
  return ret;
}

int ObSequenceMgr::del_sequence(const ObTenantSequenceId &sequence)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  ObSequenceSchema *schema_to_del = NULL;
  if (!sequence.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(sequence));
  } else if (OB_FAIL(sequence_infos_.remove_if(sequence,
                                              compare_with_tenant_sequence_id,
                                              equal_to_tenant_sequence_id,
                                              schema_to_del))) {
    LOG_WARN("failed to remove sequence schema, ",
             "tenant_id",
             sequence.tenant_id_,
             "sequence_id",
             sequence.sequence_id_,
             K(ret));
  } else if (OB_ISNULL(schema_to_del)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("removed sequence schema return NULL, ",
             "tenant_id",
             sequence.tenant_id_,
             "sequence_id",
             sequence.sequence_id_,
             K(ret));
  } else {
    ObSequenceHashWrapper sequence_wrapper(schema_to_del->get_tenant_id(),
                                         schema_to_del->get_database_id(),
                                         schema_to_del->get_sequence_name());
    hash_ret = sequence_map_.erase_refactored(sequence_wrapper);
    if (OB_SUCCESS != hash_ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed delete sequence from sequence hashmap, ",
               K(ret),
               K(hash_ret),
               "tenant_id", schema_to_del->get_tenant_id(),
               "database_id", schema_to_del->get_database_id(),
               "name", schema_to_del->get_sequence_name());
    }
  }

  if (sequence_infos_.count() != sequence_map_.item_count()) {
    LOG_WARN("sequence info is non-consistent",
             "sequence_infos_count",
             sequence_infos_.count(),
             "sequence_map_item_count",
             sequence_map_.item_count(),
             "sequence_id",
             sequence.sequence_id_);
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObSequenceMgr::rebuild_sequence_hashmap(sequence_infos_, sequence_map_))) {
      LOG_WARN("rebuild sequence hashmap failed", K(tmp_ret));
    }
  }
  return ret;
}

int ObSequenceMgr::get_sequence_schemas_in_database(const uint64_t tenant_id,
    const uint64_t database_id, ObIArray<const ObSequenceSchema *> &sequence_schemas) const
{
  int ret = OB_SUCCESS;
  sequence_mgr::Get_Sequence_Filter filter(database_id, tenant_id);
  sequence_mgr::Get_Sequence_Action action(sequence_schemas);
  sequence_mgr::Get_Sequence_EarlyStopCondition condition;
  if (OB_FAIL((const_cast<ObSequenceMgr&>(*this)).for_each(filter, action, condition))) {
    LOG_WARN("get schema failed", K(ret));
  }
  return ret;
}

int ObSequenceMgr::del_schemas_in_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    ObArray<const ObSequenceSchema *> schemas;
    if (OB_FAIL(get_sequence_schemas_in_tenant(tenant_id, schemas))) {
      LOG_WARN("get sequence schemas failed", K(ret), K(tenant_id));
    } else {
      FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
        ObTenantSequenceId tenant_sequence_id(tenant_id,
                                            (*schema)->get_sequence_id());
        if (OB_FAIL(del_sequence(tenant_sequence_id))) {
          LOG_WARN("del sequence failed",
                   "tenant_id", tenant_sequence_id.tenant_id_,
                   "sequence_id", tenant_sequence_id.sequence_id_,
                   K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSequenceMgr::get_sequence_schema(const uint64_t sequence_id,
                                     const ObSequenceSchema *&sequence_schema) const
{
  int ret = OB_SUCCESS;
  sequence_schema = NULL;
  sequence_mgr::Get_Sequence_Schema_Filter filter(sequence_id);
  sequence_mgr::Get_Sequence_Schema_Action action(sequence_schema);
  sequence_mgr::Get_Sequence_Schema_EarlyStopCondition condition(sequence_schema);
  if (OB_FAIL((const_cast<ObSequenceMgr&>(*this)).for_each(filter,action, condition))) {
    LOG_WARN("get sequence schema failed", K(ret));
  }
  return ret;
}

template<typename Filter, typename Acation, typename EarlyStopCondition>
int ObSequenceMgr::for_each(Filter &filter, Acation &action, EarlyStopCondition &condition)
{
  int ret = OB_SUCCESS;
  ConstSequenceIter iter = sequence_infos_.begin();
  for (; iter != sequence_infos_.end() && OB_SUCC(ret); iter++) {
    ObSequenceSchema *value = NULL;
    if (OB_ISNULL(value = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret));
    } else {
      if (filter(value)) {
        if (OB_FAIL(action(value, sequence_infos_, sequence_map_))) {
            LOG_WARN("action failed", K(ret));
        }
      }
    }
  }
  UNUSED(condition);
  return ret;
}

bool ObSequenceMgr::compare_sequence(const ObSequenceSchema *lhs,
                                     const ObSequenceSchema *rhs)
{
  return lhs->get_sequence_id() < rhs->get_sequence_id();
}

bool ObSequenceMgr::equal_sequence(const ObSequenceSchema *lhs,
                                   const ObSequenceSchema *rhs)
{
  return lhs->get_sequence_id() == rhs->get_sequence_id();
}

bool ObSequenceMgr::compare_with_tenant_sequence_id(const ObSequenceSchema *lhs,
                                                    const ObTenantSequenceId &tenant_sequence_id)
{
  return NULL != lhs ? (lhs->get_tenant_sequence_id() < tenant_sequence_id) : false;
}

bool ObSequenceMgr::equal_to_tenant_sequence_id(const ObSequenceSchema *lhs,
                                               const ObTenantSequenceId &tenant_sequence_id)
{
  return NULL != lhs ? (lhs->get_tenant_sequence_id() == tenant_sequence_id) : false;
}

int ObSequenceMgr::get_sequence_schemas_in_tenant(const uint64_t tenant_id,
    ObIArray<const ObSequenceSchema *> &sequence_schemas) const
{
  int ret = OB_SUCCESS;
  sequence_schemas.reset();

  ObTenantSequenceId tenant_outine_id_lower(tenant_id, OB_MIN_ID);
  ConstSequenceIter tenant_sequence_begin =
      sequence_infos_.lower_bound(tenant_outine_id_lower, compare_with_tenant_sequence_id);
  bool is_stop = false;
  for (ConstSequenceIter iter = tenant_sequence_begin;
      OB_SUCC(ret) && iter != sequence_infos_.end() && !is_stop; ++iter) {
    const ObSequenceSchema *sequence = NULL;
    if (OB_ISNULL(sequence = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(sequence));
    } else if (tenant_id != sequence->get_tenant_id()) {
      is_stop = true;
    } else if (OB_FAIL(sequence_schemas.push_back(sequence))) {
      LOG_WARN("push back sequence failed", K(ret));
    }
  }

  return ret;
}

int ObSequenceMgr::get_sequence_schema_count(int64_t &sequence_schema_count) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    sequence_schema_count = sequence_infos_.count();
  }
  return ret;
}

int ObSequenceMgr::get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const
{
  int ret = OB_SUCCESS;
  schema_info.reset();
  schema_info.schema_type_ = SEQUENCE_SCHEMA;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    schema_info.count_ = sequence_infos_.size();
    for (ConstSequenceIter it = sequence_infos_.begin(); OB_SUCC(ret) && it != sequence_infos_.end(); it++) {
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
