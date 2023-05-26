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
#include "ob_tablespace_mgr.h"
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

namespace tablespace_mgr {
//////////////////////////////////////////////////////////////////////////////////
struct Del_Schema_In_Tenant_Filter
{
  Del_Schema_In_Tenant_Filter(uint64_t tenant_id): tenant_id_(tenant_id) {}
  bool operator() (ObTablespaceSchema *value)
  {
    return value->get_tenant_id() == tenant_id_;
  }
  uint64_t tenant_id_;
};

struct Del_Schema_In_Tenant_Action
{
  Del_Schema_In_Tenant_Action() {}
  int operator() (ObTablespaceSchema *value,
                  ObTablespaceMgr::TablespaceInfos &infos,
                  ObTablespaceMgr::ObTablespaceMap &map)
  {
    int ret = OB_SUCCESS;
    ObTablespaceSchema *value_to_del = NULL;
    if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid argument, delete value is null", K(value), K(ret));
    } else if (OB_FAIL(infos.remove_if(value,
                                       ObTablespaceMgr::compare_tablespace,
                                       ObTablespaceMgr::equal_tablespace,
                                       value_to_del))) {
      LOG_WARN("fail to remove tablespace schema", K(*value), K(ret));
    } else if (OB_ISNULL(value_to_del)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("removed tablespace schema return NULL", K(*value), K(value_to_del), K(ret));
    } else {
      ObTablespaceHashWrapper hash_wrapper(value->get_tenant_id(),
                                         value->get_tablespace_name());
      if (OB_FAIL(map.erase_refactored(hash_wrapper))) {
        LOG_WARN("fail to delete tablespace schema from hashmap", K(*value), K(ret));
      }
    }

    if (infos.count() != map.item_count()) {
      LOG_WARN("tablespace info is non-consistent",
               "tablespace_infos_count",
               infos.count(),
               "tablespace_map_item_count",
               map.item_count(),
               "tablespace_id",
               value->get_tablespace_id(),
               "tablespace_name",
               value->get_tablespace_name());
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = ObTablespaceMgr::rebuild_tablespace_hashmap(infos, map))) {
        LOG_WARN("rebuild tablespace hashmap failed", K(tmp_ret));
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
  Del_Schema_Filter(const ObTenantTablespaceId &tablespace): tablespace_(tablespace) {}
  bool operator() (ObTablespaceSchema *value)
  {
    return value->get_tenant_id() == tablespace_.tenant_id_ &&
        value->get_tablespace_id() == tablespace_.tablespace_id_;
  }
  const ObTenantTablespaceId &tablespace_;
};
//////////////////////////////////////////////////////////////////////////////////
struct Get_Tablespace_Schema_Filter
{
  Get_Tablespace_Schema_Filter(uint64_t tablespace_id): tablespace_id_(tablespace_id) {}
  bool operator() (ObTablespaceSchema *value)
  {
    return value->get_tablespace_id() == tablespace_id_;
  }
  uint64_t tablespace_id_;
};
struct Get_Tablespace_Schema_Action
{
  Get_Tablespace_Schema_Action(const ObTablespaceSchema *&tablespace_schema)
      : tablespace_schema_(tablespace_schema) {}
  int operator() (ObTablespaceSchema *value,
                  ObTablespaceMgr::TablespaceInfos &infos,
                  ObTablespaceMgr::ObTablespaceMap &map)
  {
    UNUSED(infos);
    UNUSED(map);
    tablespace_schema_ = value;
    return OB_SUCCESS;
  }
  const ObTablespaceSchema *&tablespace_schema_;
};
struct Get_Tablespace_Schema_EarlyStopCondition
{
  Get_Tablespace_Schema_EarlyStopCondition(const ObTablespaceSchema *&tablespace_schema)
      : tablespace_schema_(tablespace_schema) {}
  bool operator() ()
  {
    return tablespace_schema_ != NULL;
  }
  const ObTablespaceSchema *&tablespace_schema_;
};
///////////////////////////////////////////////////////////////////////////////////////////////////
struct Get_Tablespace_Filter
{
  Get_Tablespace_Filter(uint64_t db_id, uint64_t tenant_id)
      : db_id_(db_id), tenant_id_(tenant_id) {}
  bool operator() (ObTablespaceSchema *value)
  {
    return value->get_tenant_id() == tenant_id_;
  }
  uint64_t db_id_;
  uint64_t tenant_id_;
};
struct Get_Tablespace_Action
{
  Get_Tablespace_Action(ObIArray<const ObTablespaceSchema *> &tablespace_schemas)
      : tablespace_schemas_(tablespace_schemas) {}
  int operator() (ObTablespaceSchema *value,
                  ObTablespaceMgr::TablespaceInfos &infos,
                  ObTablespaceMgr::ObTablespaceMap &map)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(tablespace_schemas_.push_back(value))) {
      LOG_WARN("push back failed", K(ret), K(value->get_tablespace_name_str()));
    }
    UNUSED(infos);
    UNUSED(map);
    return ret;
  }
  ObIArray<const ObTablespaceSchema *> &tablespace_schemas_;
};
struct Get_Tablespace_EarlyStopCondition
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
  bool operator() (ObTablespaceSchema *value)
  {
    UNUSED(value);
    return true;
  }
};
struct Deep_Copy_Action
{
  Deep_Copy_Action(ObTablespaceMgr &tablespace_mgr)
      : tablespace_mgr_(tablespace_mgr) {}
  int operator() (ObTablespaceSchema *value,
                  ObTablespaceMgr::TablespaceInfos &infos,
                  ObTablespaceMgr::ObTablespaceMap &map)
  {
    int ret = OB_SUCCESS;
    UNUSED(infos);
    UNUSED(map);
    if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("value is NULL", K(value), K(ret));
    } else if (OB_FAIL(tablespace_mgr_.add_tablespace(*value))) {
      LOG_WARN("push back failed", K(ret), K(value->get_tablespace_name_str()));
    }
    return ret;
  }
  ObTablespaceMgr &tablespace_mgr_;
};
struct Deep_Copy_EarlyStopCondition
{
  bool operator() ()
  {
    return false;
  }
};
}

///////////////////////////////////////////////////////////////////
ObTablespaceMgr::ObTablespaceMgr()
    : is_inited_(false),
      local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
      allocator_(local_allocator_),
      tablespace_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_TABLESPACE, ObCtxIds::SCHEMA_SERVICE)),
      tablespace_map_(SET_USE_500(ObModIds::OB_SCHEMA_TABLESPACE, ObCtxIds::SCHEMA_SERVICE))
{
}

ObTablespaceMgr::ObTablespaceMgr(ObIAllocator &allocator)
    : is_inited_(false),
      local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
      allocator_(allocator),
      tablespace_infos_(0, NULL, SET_USE_500(ObModIds::OB_SCHEMA_TABLESPACE, ObCtxIds::SCHEMA_SERVICE)),
      tablespace_map_(SET_USE_500(ObModIds::OB_SCHEMA_TABLESPACE, ObCtxIds::SCHEMA_SERVICE))
{
}

ObTablespaceMgr::~ObTablespaceMgr()
{
}

int ObTablespaceMgr::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init private tablespace manager twice", K(ret));
  } else if (OB_FAIL(tablespace_map_.init())) {
    LOG_WARN("init private tablespace map failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObTablespaceMgr::reset()
{
  if (!is_inited_) {
    LOG_WARN_RET(OB_NOT_INIT, "tablespace manger not init");
  } else {
    tablespace_infos_.clear();
    tablespace_map_.clear();
  }
}

ObTablespaceMgr &ObTablespaceMgr::operator =(const ObTablespaceMgr &other)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablespace manager not init", K(ret));
  } else  if (OB_FAIL(assign(other))) {
    LOG_WARN("assign failed", K(ret));
  }
  return *this;
}

int ObTablespaceMgr::assign(const ObTablespaceMgr &other)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablespace manager not init", K(ret));
  } else if (this != &other) {
    if (OB_FAIL(tablespace_map_.assign(other.tablespace_map_))) {
      LOG_WARN("assign tablespace map failed", K(ret));
    } else if (OB_FAIL(tablespace_infos_.assign(other.tablespace_infos_))) {
      LOG_WARN("assign tablespace infos vector failed", K(ret));
    }
  }
  return ret;
}

int ObTablespaceMgr::deep_copy(const ObTablespaceMgr &other)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablespace manager not init", K(ret));
  } else if (this != &other) {
    reset();
    tablespace_mgr::Deep_Copy_Filter filter;
    tablespace_mgr::Deep_Copy_Action action(*this);
    tablespace_mgr::Deep_Copy_EarlyStopCondition condition;
    if (OB_FAIL((const_cast<ObTablespaceMgr&>(other)).for_each(filter, action, condition))) {
      LOG_WARN("deep copy failed", K(ret));
    }
  }
  return ret;
}


int ObTablespaceMgr::add_tablespace(const ObTablespaceSchema &tablespace_schema)
{
  int ret = OB_SUCCESS;
  int overwrite = 1;
  ObTablespaceSchema *new_tablespace_schema = NULL;
  TablespaceIter iter = NULL;
  ObTablespaceSchema *replaced_tablespace = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablespace manager not init", K(ret));
  } else if (OB_UNLIKELY(!tablespace_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablespace_schema));
  } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator_,
                                                 tablespace_schema,
                                                 new_tablespace_schema))) {
    LOG_WARN("alloca tablespace schema failed", K(ret));
  } else if (OB_ISNULL(new_tablespace_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(new_tablespace_schema), K(ret));
  } else if (OB_FAIL(tablespace_infos_.replace(new_tablespace_schema,
                                             iter,
                                             compare_tablespace,
                                             equal_tablespace,
                                             replaced_tablespace))) {
    LOG_WARN("failed to add tablespace schema", K(ret));
  } else {
    ObTablespaceHashWrapper hash_wrapper(new_tablespace_schema->get_tenant_id(),
                                       new_tablespace_schema->get_tablespace_name());
    if (OB_FAIL(tablespace_map_.set_refactored(hash_wrapper, new_tablespace_schema, overwrite))) {
      LOG_WARN("build tablespace hash map failed", K(ret));
    } else {
      LOG_INFO("add new tablespace to tablespace map", K(*new_tablespace_schema));
    }
  }
  if (tablespace_infos_.count() != tablespace_map_.item_count()) {
    LOG_WARN("tablespace info is non-consistent",
             "tablespace_infos_count",
             tablespace_infos_.count(),
             "tablespace_map_item_count",
             tablespace_map_.item_count(),
             "tablespace_id",
             tablespace_schema.get_tablespace_id(),
             "tablespace_name",
             tablespace_schema.get_tablespace_name());
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObTablespaceMgr::rebuild_tablespace_hashmap(tablespace_infos_,
                                                                         tablespace_map_))) {
      LOG_WARN("rebuild tablespace hashmap failed", K(tmp_ret));
    }
  }
  return ret;
}

//用tablespace_infos进行重建tablespace_map
int ObTablespaceMgr::rebuild_tablespace_hashmap(const TablespaceInfos &tablespace_infos,
                                            ObTablespaceMap& tablespace_map)
{
  int ret = OB_SUCCESS;
  tablespace_map.clear();
  ConstTablespaceIter iter = tablespace_infos.begin();
  for (; iter != tablespace_infos.end() && OB_SUCC(ret); ++iter) {
    ObTablespaceSchema *tablespace_schema = *iter;
    if (OB_ISNULL(tablespace_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablespace schema is NULL", K(tablespace_schema), K(ret));
    } else {
      bool overwrite = true;
      ObTablespaceHashWrapper hash_wrapper(tablespace_schema->get_tenant_id(),
                                        tablespace_schema->get_tablespace_name());
      if (OB_FAIL(tablespace_map.set_refactored(hash_wrapper, tablespace_schema, overwrite))) {
        LOG_WARN("build tablespace hash map failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTablespaceMgr::add_tablespaces(const common::ObIArray<ObTablespaceSchema> &tablespace_schemas)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < tablespace_schemas.count() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(add_tablespace(tablespace_schemas.at(i)))) {
      LOG_WARN("push tablespace failed", K(ret));
    }
  }
  return ret;
}

int ObTablespaceMgr::get_tablespace_schema_with_name(
  const uint64_t tenant_id,
  const ObString &name,
  const ObTablespaceSchema *&tablespace_schema) const
{
  int ret = OB_SUCCESS;
  tablespace_schema = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id
             || name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(name));
  } else {
    ObTablespaceSchema *tmp_schema = NULL;
    ObTablespaceHashWrapper hash_wrap(tenant_id, name);
    if (OB_FAIL(tablespace_map_.get_refactored(hash_wrap, tmp_schema))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("tablespace is not exist", K(tenant_id), K(name),
                 "map_cnt", tablespace_map_.item_count());
      }
    } else {
      tablespace_schema = tmp_schema;
    }
  }
  return ret;
}

int ObTablespaceMgr::get_tablespace_info_version(uint64_t tablespace_id, int64_t &tablespace_version) const
{
  int ret = OB_SUCCESS;
  tablespace_version = OB_INVALID_ID;
  ConstTablespaceIter iter = tablespace_infos_.begin();
  for (; OB_SUCC(ret) && iter != tablespace_infos_.end(); ++iter) {
    const ObTablespaceSchema *tablespace = NULL;
    if (OB_ISNULL(tablespace = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret));
    } else if (tablespace->get_tablespace_id() == tablespace_id) {
      tablespace_version = tablespace->get_schema_version();
      break;
    }
  }
  return ret;
}

//suppose there are no vital errors ,
//this function will always return ob_success no matter do_exist = true or not.
int ObTablespaceMgr::get_encryption_name(const uint64_t tenant_id,
                             const ObString &tablespace_name,
                             ObString &encryption_name,
                             bool &do_exist) const
{
  int ret = OB_SUCCESS;
  do_exist = false;
  const ObTablespaceSchema *tablespace_schema = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id
             || tablespace_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(tablespace_name));
  } else if (OB_FAIL(get_tablespace_schema_with_name(tenant_id, tablespace_name, tablespace_schema))) {
    LOG_WARN("fail to get tablespace schema with name",
        K(tenant_id), K(tablespace_name), K(ret));
  } else if (OB_ISNULL(tablespace_schema)) {
    LOG_INFO("tablespace is not exist", K(tenant_id), K(tablespace_name), K(ret));
  } else {
    do_exist = true;
    encryption_name = tablespace_schema->get_encryption_name();
  }

  return ret;
}

int ObTablespaceMgr::del_tablespace(const ObTenantTablespaceId &tablespace)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  ObTablespaceSchema *schema_to_del = NULL;
  if (!tablespace.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablespace));
  } else if (OB_FAIL(tablespace_infos_.remove_if(tablespace,
                                              compare_with_tenant_tablespace_id,
                                              equal_to_tenant_tablespace_id,
                                              schema_to_del))) {
    LOG_WARN("failed to remove tablespace schema, ",
             "tenant_id",
             tablespace.tenant_id_,
             "tablespace_id",
             tablespace.tablespace_id_,
             K(ret));
  } else if (OB_ISNULL(schema_to_del)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("removed tablespace schema return NULL, ",
             "tenant_id",
             tablespace.tenant_id_,
             "tablespace_id",
             tablespace.tablespace_id_,
             K(ret));
  } else {
    ObTablespaceHashWrapper tablespace_wrapper(schema_to_del->get_tenant_id(),
                                         schema_to_del->get_tablespace_name());
    hash_ret = tablespace_map_.erase_refactored(tablespace_wrapper);
    if (OB_SUCCESS != hash_ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed delete tablespace from tablespace hashmap, ",
               K(ret),
               K(hash_ret),
               "tenant_id", schema_to_del->get_tenant_id(),
               "name", schema_to_del->get_tablespace_name());
    }
  }

  if (tablespace_infos_.count() != tablespace_map_.item_count()) {
    LOG_WARN("tablespace info is non-consistent",
             "tablespace_infos_count",
             tablespace_infos_.count(),
             "tablespace_map_item_count",
             tablespace_map_.item_count(),
             "tablespace_id",
             tablespace.tablespace_id_);
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObTablespaceMgr::rebuild_tablespace_hashmap(tablespace_infos_, tablespace_map_))) {
      LOG_WARN("rebuild tablespace hashmap failed", K(tmp_ret));
    }
  }
  return ret;
}

int ObTablespaceMgr::del_schemas_in_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    ObArray<const ObTablespaceSchema *> schemas;
    if (OB_FAIL(get_tablespace_schemas_in_tenant(tenant_id, schemas))) {
      LOG_WARN("get tablespace schemas failed", K(ret), K(tenant_id));
    } else {
      FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
        ObTenantTablespaceId tenant_tablespace_id(tenant_id,
                                            (*schema)->get_tablespace_id());
        if (OB_FAIL(del_tablespace(tenant_tablespace_id))) {
          LOG_WARN("del tablespace failed",
                   "tenant_id", tenant_tablespace_id.tenant_id_,
                   "tablespace_id", tenant_tablespace_id.tablespace_id_,
                   K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTablespaceMgr::get_tablespace_schema(const uint64_t tablespace_id,
                                     const ObTablespaceSchema *&tablespace_schema) const
{
  int ret = OB_SUCCESS;
  tablespace_schema = NULL;
  tablespace_mgr::Get_Tablespace_Schema_Filter filter(tablespace_id);
  tablespace_mgr::Get_Tablespace_Schema_Action action(tablespace_schema);
  tablespace_mgr::Get_Tablespace_Schema_EarlyStopCondition condition(tablespace_schema);
  if (OB_FAIL((const_cast<ObTablespaceMgr&>(*this)).for_each(filter,action, condition))) {
    LOG_WARN("get tablespace schema failed", K(ret));
  }
  return ret;
}

template<typename Filter, typename Acation, typename EarlyStopCondition>
int ObTablespaceMgr::for_each(Filter &filter, Acation &action, EarlyStopCondition &condition)
{
  int ret = OB_SUCCESS;
  UNUSED(condition);
  ConstTablespaceIter iter = tablespace_infos_.begin();
  for (; iter != tablespace_infos_.end() && OB_SUCC(ret); iter++) {
    ObTablespaceSchema *value = NULL;
    if (OB_ISNULL(value = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret));
    } else {
      if (filter(value)) {
        if (OB_FAIL(action(value, tablespace_infos_, tablespace_map_))) {
            LOG_WARN("action failed", K(ret));
        }
      }
    }
  }
  return ret;
}

bool ObTablespaceMgr::compare_with_tenant_tablespace_id(const ObTablespaceSchema *lhs,
                                                    const ObTenantTablespaceId &tenant_tablespace_id)
{
  return NULL != lhs ? (lhs->get_tenant_tablespace_id() < tenant_tablespace_id) : false;
}

bool ObTablespaceMgr::equal_to_tenant_tablespace_id(const ObTablespaceSchema *lhs,
                                               const ObTenantTablespaceId &tenant_tablespace_id)
{
  return NULL != lhs ? (lhs->get_tenant_tablespace_id() == tenant_tablespace_id) : false;
}

int ObTablespaceMgr::get_tablespace_schemas_in_tenant(const uint64_t tenant_id,
    ObIArray<const ObTablespaceSchema *> &tablespace_schemas) const
{
  int ret = OB_SUCCESS;
  tablespace_schemas.reset();

  ObTenantTablespaceId tenant_outine_id_lower(tenant_id, OB_MIN_ID);
  ConstTablespaceIter tenant_tablespace_begin =
      tablespace_infos_.lower_bound(tenant_outine_id_lower, compare_with_tenant_tablespace_id);
  bool is_stop = false;
  for (ConstTablespaceIter iter = tenant_tablespace_begin;
      OB_SUCC(ret) && iter != tablespace_infos_.end() && !is_stop; ++iter) {
    const ObTablespaceSchema *tablespace = NULL;
    if (OB_ISNULL(tablespace = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(tablespace));
    } else if (tenant_id != tablespace->get_tenant_id()) {
      is_stop = true;
    } else if (OB_FAIL(tablespace_schemas.push_back(tablespace))) {
      LOG_WARN("push back tablespace failed", K(ret));
    }
  }

  return ret;
}

int ObTablespaceMgr::get_tablespace_schema_count(int64_t &tablespace_schema_count) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    tablespace_schema_count = tablespace_infos_.count();
  }
  return ret;
}

int ObTablespaceMgr::get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const
{
  int ret = OB_SUCCESS;
  schema_info.reset();
  schema_info.schema_type_ = TABLESPACE_SCHEMA;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    schema_info.count_ = tablespace_infos_.size();
    for (ConstTablespaceIter it = tablespace_infos_.begin(); OB_SUCC(ret) && it != tablespace_infos_.end(); it++) {
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
