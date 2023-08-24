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
#include "ob_mock_fk_parent_table_mgr.h"
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
namespace mock_fk_parent_table_mgr {

struct Deep_Copy_Filter
{
  Deep_Copy_Filter() {}
  bool operator() (ObSimpleMockFKParentTableSchema *value)
  {
    UNUSED(value);
    return true;
  }
};

struct Deep_Copy_Action
{
  Deep_Copy_Action(ObMockFKParentTableMgr &mock_fk_parent_table_mgr)
      : mock_fk_parent_table_mgr_(mock_fk_parent_table_mgr) {}
  int operator() (ObSimpleMockFKParentTableSchema *value,
      ObMockFKParentTableMgr::MockFKParentTableInfos &infos,
      ObMockFKParentTableMgr::MockFKParentTableMap &map)
  {
    UNUSED(infos);
    UNUSED(map);
    int ret = OB_SUCCESS;
    if (OB_ISNULL(value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("value is NULL", K(ret), K(value));
    } else if (OB_FAIL(mock_fk_parent_table_mgr_.add_mock_fk_parent_table(*value))) {
      LOG_WARN("push back failed", K(ret), K(value->get_mock_fk_parent_table_name()));
    }
    return ret;
  }
  ObMockFKParentTableMgr &mock_fk_parent_table_mgr_;
};

struct Deep_Copy_EarlyStopCondition
{
  bool operator() ()
  {
    return false;
  }
};

}

ObMockFKParentTableHashWrapper ObGetMockFKParentTableKey<
    ObMockFKParentTableHashWrapper, ObSimpleMockFKParentTableSchema *>::operator() (
        const ObSimpleMockFKParentTableSchema *schema) const
{
  ObMockFKParentTableHashWrapper hash_wrap;
  if (!OB_ISNULL(schema)) {
    hash_wrap.set_tenant_id(schema->get_tenant_id());
    hash_wrap.set_database_id(schema->get_database_id());
    hash_wrap.set_mock_fk_parent_table_name(schema->get_mock_fk_parent_table_name());
  }
  return hash_wrap;
}

ObMockFKParentTableMgr::ObMockFKParentTableMgr()
    : is_inited_(false),
      local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
      allocator_(local_allocator_),
      mock_fk_parent_table_infos_(0, NULL, SET_USE_500("MockFkParentTab", ObCtxIds::SCHEMA_SERVICE)),
      mock_fk_parent_table_map_(SET_USE_500("MockFkParentTab", ObCtxIds::SCHEMA_SERVICE))
{
}

ObMockFKParentTableMgr::ObMockFKParentTableMgr(ObIAllocator &allocator)
    : is_inited_(false),
      local_allocator_(SET_USE_500(ObModIds::OB_SCHEMA_GETTER_GUARD, ObCtxIds::SCHEMA_SERVICE)),
      allocator_(allocator),
      mock_fk_parent_table_infos_(0, NULL, SET_USE_500("MockFkParentTab", ObCtxIds::SCHEMA_SERVICE)),
      mock_fk_parent_table_map_(SET_USE_500("MockFkParentTab", ObCtxIds::SCHEMA_SERVICE))
{
}

ObMockFKParentTableMgr::~ObMockFKParentTableMgr()
{
}

int ObMockFKParentTableMgr::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init private mock_fk_parent_table manager twice", K(ret));
  } else if (OB_FAIL(mock_fk_parent_table_map_.init())) {
    LOG_WARN("init private mock_fk_parent_table map failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObMockFKParentTableMgr::reset()
{
  if (!is_inited_) {
    LOG_WARN_RET(OB_NOT_INIT, "mock_fk_parent_table manger not init");
  } else {
    mock_fk_parent_table_infos_.clear();
    mock_fk_parent_table_map_.clear();
  }
}

ObMockFKParentTableMgr &ObMockFKParentTableMgr::operator =(const ObMockFKParentTableMgr &other)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("mock_fk_parent_table manager not init", K(ret));
  } else  if (OB_FAIL(assign(other))) {
    LOG_WARN("assign failed", K(ret));
  }
  return *this;
}

int ObMockFKParentTableMgr::assign(const ObMockFKParentTableMgr &other)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("mock_fk_parent_table manager not init", K(ret));
  } else if (this != &other) {
    if (OB_FAIL(mock_fk_parent_table_map_.assign(other.mock_fk_parent_table_map_))) {
      LOG_WARN("assign mock_fk_parent_table map failed", K(ret));
    } else if (OB_FAIL(mock_fk_parent_table_infos_.assign(other.mock_fk_parent_table_infos_))) {
      LOG_WARN("assign mock_fk_parent_table infos vector failed", K(ret));
    }
  }
  return ret;
}

int ObMockFKParentTableMgr::deep_copy(const ObMockFKParentTableMgr &other)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("mock_fk_parent_table manager not init", K(ret));
  } else if (this != &other) {
    reset();
    mock_fk_parent_table_mgr::Deep_Copy_Filter filter;
    mock_fk_parent_table_mgr::Deep_Copy_Action action(*this);
    mock_fk_parent_table_mgr::Deep_Copy_EarlyStopCondition condition;
    if (OB_FAIL((const_cast<ObMockFKParentTableMgr&>(other)).for_each(filter, action, condition))) {
      LOG_WARN("deep copy failed", K(ret));
    }
  }
  return ret;
}

bool ObMockFKParentTableMgr::compare_mock_fk_parent_table(
    const ObSimpleMockFKParentTableSchema *lhs,
    const ObSimpleMockFKParentTableSchema *rhs)
{
  return lhs->get_tenant_id() == rhs->get_tenant_id()
         ? (lhs->get_mock_fk_parent_table_id() < rhs->get_mock_fk_parent_table_id())
            : (lhs->get_tenant_id() < rhs->get_tenant_id());
}

bool ObMockFKParentTableMgr::equal_mock_fk_parent_table(
    const ObSimpleMockFKParentTableSchema *lhs,
    const ObSimpleMockFKParentTableSchema *rhs)
{
  return lhs->get_tenant_id() == rhs->get_tenant_id()
         && lhs->get_mock_fk_parent_table_id() == rhs->get_mock_fk_parent_table_id();
}

bool ObMockFKParentTableMgr::compare_with_mock_fk_parent_table_key(
    const ObSimpleMockFKParentTableSchema *lhs,
    const ObMockFKParentTableKey &rhs)
{
  return NULL != lhs ? (lhs->get_mock_parent_table_key() < rhs) : false;
}

bool ObMockFKParentTableMgr::equal_to_mock_fk_parent_table_key(
    const ObSimpleMockFKParentTableSchema *lhs,
    const ObMockFKParentTableKey &rhs)
{
  return NULL != lhs ? (lhs->get_mock_parent_table_key() == rhs) : false;
}

int ObMockFKParentTableMgr::rebuild_mock_fk_parent_table_hashmap(
    const MockFKParentTableInfos &infos,
    MockFKParentTableMap& map)
{
  int ret = OB_SUCCESS;
  map.clear();
  ConstMockFKParentTableIter iter = infos.begin();
  for (; iter != infos.end() && OB_SUCC(ret); ++iter) {
    ObSimpleMockFKParentTableSchema *schema = *iter;
    if (OB_ISNULL(schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mock_fk_parent_table schema is NULL", K(ret), K(schema));
    } else {
      bool overwrite = false;
      ObMockFKParentTableHashWrapper hash_wrapper(schema->get_tenant_id(),
                                                  schema->get_database_id(),
                                                  schema->get_mock_fk_parent_table_name());
      int hash_ret = map.set_refactored(hash_wrapper, schema, overwrite);
      if (OB_SUCCESS != hash_ret) {
        ret = OB_HASH_EXIST == hash_ret ? OB_SUCCESS : hash_ret;
        LOG_ERROR("build mock_fk_parent_table_hashmap failed", KR(ret), KR(hash_ret),
                  "exist_tenant_id", schema->get_tenant_id(),
                  "exist_database_id", schema->get_database_id(),
                  "exist_mock_fk_parent_table_name", schema->get_mock_fk_parent_table_name());
      }
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(infos.count() != map.count())) {
    ret = OB_DUPLICATE_OBJECT_NAME_EXIST;
    LOG_ERROR("unexpected mock_fk_parent_table_hashmap map count",
              KR(ret), K(infos.count()), K(map.count()));
    LOG_DBA_ERROR(OB_DUPLICATE_OBJECT_NAME_EXIST,
                  "msg", "duplicate mock fk parent table name exist after rebuild",
                  K(infos.count()), K(map.count()));
    right_to_die_or_duty_to_live();
  }
  return ret;
}

int ObMockFKParentTableMgr::add_mock_fk_parent_table(const ObSimpleMockFKParentTableSchema &schema)
{
  int ret = OB_SUCCESS;
  int overwrite = 1;
  ObSimpleMockFKParentTableSchema *new_schema = NULL;
  MockFKParentTableIter iter = NULL;
  ObSimpleMockFKParentTableSchema *replaced_schema = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("mock_fk_parent_table manager not init", K(ret));
  } else if (OB_UNLIKELY(!schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schema));
  } else if (OB_FAIL(ObSchemaUtils::alloc_schema(allocator_, schema, new_schema))) {
    LOG_WARN("alloc schema failed", K(ret));
  } else if (OB_ISNULL(new_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(new_schema), K(ret));
  } else if (OB_FAIL(mock_fk_parent_table_infos_.replace(new_schema,
                                             iter,
                                             compare_mock_fk_parent_table,
                                             equal_mock_fk_parent_table,
                                             replaced_schema))) {
    LOG_WARN("failed to add mock_fk_parent_table schema", K(ret));
  } else {
    ObMockFKParentTableHashWrapper hash_wrapper(new_schema->get_tenant_id(),
                                                new_schema->get_database_id(),
                                                new_schema->get_mock_fk_parent_table_name());
    if (OB_FAIL(mock_fk_parent_table_map_.set_refactored(hash_wrapper, new_schema, overwrite))) {
      LOG_WARN("build mock_fk_parent_table hash map failed", K(ret));
    } else {
      LOG_INFO("add new mock_fk_parent_table to mock_fk_parent_table map", K(*new_schema));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (mock_fk_parent_table_infos_.count() != mock_fk_parent_table_map_.item_count()) {
    LOG_WARN("mock_fk_parent_table info is non-consistent",
             "mock_fk_parent_table_infos_count",
             mock_fk_parent_table_infos_.count(),
             "mock_fk_parent_table_map_item_count",
             mock_fk_parent_table_map_.item_count(),
             "mock_fk_parent_table_name",
             schema.get_mock_fk_parent_table_name());
    if (OB_FAIL(ObMockFKParentTableMgr::rebuild_mock_fk_parent_table_hashmap(
                                                         mock_fk_parent_table_infos_,
                                                         mock_fk_parent_table_map_))) {
      LOG_WARN("rebuild mock_fk_parent_table hashmap failed", K(ret));
    }
  }
  return ret;
}

int ObMockFKParentTableMgr::add_mock_fk_parent_tables(
    const common::ObIArray<ObSimpleMockFKParentTableSchema> &schemas)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < schemas.count() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(add_mock_fk_parent_table(schemas.at(i)))) {
      LOG_WARN("push mock_fk_parent_table failed", K(ret));
    }
  }
  return ret;
}

int ObMockFKParentTableMgr::del_mock_fk_parent_table(const ObMockFKParentTableKey &key)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  ObSimpleMockFKParentTableSchema *schema_to_del = NULL;
  if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key));
  } else if (OB_FAIL(mock_fk_parent_table_infos_.remove_if(key,
                     compare_with_mock_fk_parent_table_key,
                     equal_to_mock_fk_parent_table_key,
                     schema_to_del))) {
     if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("failed to remove mock_fk_parent_table schema, item may not exist", K(ret));
    } else {
      LOG_WARN("failed to remove mock_fk_parent_table schema", K(ret), K(key.tenant_id_), K(key.mock_fk_parent_table_id_));
    }
  } else if (OB_ISNULL(schema_to_del)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("removed schema return NULL", K(ret), K(key.tenant_id_), K(key.mock_fk_parent_table_id_));
  } else {
    ObMockFKParentTableHashWrapper wrapper(schema_to_del->get_tenant_id(),
                                           schema_to_del->get_database_id(),
                                           schema_to_del->get_mock_fk_parent_table_name());
    hash_ret = mock_fk_parent_table_map_.erase_refactored(wrapper);
    if (OB_SUCCESS != hash_ret) {
      if (OB_HASH_NOT_EXIST == hash_ret) {
        LOG_INFO("failed to remove mock_fk_parent_table schema, item may not exist", K(ret));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to delete mock_fk_parent_table from mock_fk_parent_table hashmap",
                  K(ret),
                  K(hash_ret),
                  K(schema_to_del->get_tenant_id()),
                  K(schema_to_del->get_database_id()),
                  K(schema_to_del->get_mock_fk_parent_table_name()));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (mock_fk_parent_table_infos_.count() != mock_fk_parent_table_map_.item_count()) {
    LOG_WARN("mock_fk_parent_table_infos_ is non-consistent",
             K(mock_fk_parent_table_infos_.count()),
             K(mock_fk_parent_table_map_.item_count()));
    if (OB_FAIL(ObMockFKParentTableMgr::rebuild_mock_fk_parent_table_hashmap(
            mock_fk_parent_table_infos_, mock_fk_parent_table_map_))) {
      LOG_WARN("rebuild mock_fk_parent_table hashmap failed", K(ret));
    }
  }
  return ret;
}

int ObMockFKParentTableMgr::del_schemas_in_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    ObArray<const ObSimpleMockFKParentTableSchema *> schemas;
    if (OB_FAIL(get_mock_fk_parent_table_schemas_in_tenant(tenant_id, schemas))) {
      LOG_WARN("get mock_fk_parent_table schemas failed", K(ret), K(tenant_id));
    } else {
      FOREACH_CNT_X(schema, schemas, OB_SUCC(ret)) {
        ObMockFKParentTableKey key(tenant_id, (*schema)->get_mock_fk_parent_table_id());
        if (OB_FAIL(del_mock_fk_parent_table(key))) {
          LOG_WARN("del del_mock_fk_parent_table failed", K(ret), K(key.tenant_id_), K(key.mock_fk_parent_table_id_));
        }
      }
    }
  }
  return ret;
}

template<typename Filter, typename Acation, typename EarlyStopCondition>
int ObMockFKParentTableMgr::for_each(Filter &filter, Acation &action, EarlyStopCondition &condition)
{
  int ret = OB_SUCCESS;
  ConstMockFKParentTableIter iter = mock_fk_parent_table_infos_.begin();
  for (; iter != mock_fk_parent_table_infos_.end() && OB_SUCC(ret); iter++) {
    ObSimpleMockFKParentTableSchema *value = NULL;
    if (OB_ISNULL(value = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret));
    } else {
      if (filter(value)) {
        if (OB_FAIL(action(value, mock_fk_parent_table_infos_, mock_fk_parent_table_map_))) {
            LOG_WARN("action failed", K(ret));
        }
      }
    }
  }
  UNUSED(condition);
  return ret;
}

int ObMockFKParentTableMgr::get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const
{
  int ret = OB_SUCCESS;
  schema_info.reset();
  schema_info.schema_type_ = MOCK_FK_PARENT_TABLE_SCHEMA;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    schema_info.count_ = mock_fk_parent_table_infos_.size();
    for (ConstMockFKParentTableIter it = mock_fk_parent_table_infos_.begin();
                     OB_SUCC(ret) && it != mock_fk_parent_table_infos_.end(); it++) {
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

int ObMockFKParentTableMgr::get_mock_fk_parent_table_schema_count(int64_t &schema_count) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    schema_count = mock_fk_parent_table_infos_.count();
  }
  return ret;
}

int ObMockFKParentTableMgr::get_mock_fk_parent_table_schema(
    const uint64_t tenant_id,
    const uint64_t mock_fk_parent_table_id,
    const ObSimpleMockFKParentTableSchema *&schema) const
{
  int ret = OB_SUCCESS;
  schema = NULL;
  ObMockFKParentTableKey key_lower(tenant_id, mock_fk_parent_table_id);
  ConstMockFKParentTableIter iter_begin = mock_fk_parent_table_infos_.lower_bound(
                                          key_lower, compare_with_mock_fk_parent_table_key);
  bool is_stop = false;
  for (ConstMockFKParentTableIter iter = iter_begin;
       OB_SUCC(ret) && iter != mock_fk_parent_table_infos_.end() && !is_stop;
       ++iter) {
    const ObSimpleMockFKParentTableSchema *mock_fk_parent_table = NULL;
    if (OB_ISNULL(mock_fk_parent_table = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(mock_fk_parent_table));
    } else if (mock_fk_parent_table->get_tenant_id() > tenant_id
              || (tenant_id == mock_fk_parent_table->get_tenant_id()
                  && mock_fk_parent_table->get_mock_fk_parent_table_id() > mock_fk_parent_table_id) ) {
      is_stop = true;
    } else if (tenant_id == mock_fk_parent_table->get_tenant_id()
               && mock_fk_parent_table_id == mock_fk_parent_table->get_mock_fk_parent_table_id()) {
      schema = mock_fk_parent_table;
    }
  }
  return ret;
}

int ObMockFKParentTableMgr::get_mock_fk_parent_table_schemas_in_tenant(
    const uint64_t tenant_id,
    ObIArray<const ObSimpleMockFKParentTableSchema *> &schemas) const
{
  int ret = OB_SUCCESS;
  schemas.reset();

  ObMockFKParentTableKey key_lower(tenant_id, OB_MIN_ID);
  ConstMockFKParentTableIter iter_begin =mock_fk_parent_table_infos_.lower_bound(
                                  key_lower, compare_with_mock_fk_parent_table_key);
  bool is_stop = false;
  for (ConstMockFKParentTableIter iter = iter_begin;
       OB_SUCC(ret) && iter != mock_fk_parent_table_infos_.end() && !is_stop;
       ++iter) {
    const ObSimpleMockFKParentTableSchema *mock_fk_parent_table = NULL;
    if (OB_ISNULL(mock_fk_parent_table = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret));
    } else if (tenant_id != mock_fk_parent_table->get_tenant_id()) {
      is_stop = true;
    } else if (OB_FAIL(schemas.push_back(mock_fk_parent_table))) {
      LOG_WARN("push back mock_fk_parent_table failed", K(ret));
    }
  }

  return ret;
}

int ObMockFKParentTableMgr::get_mock_fk_parent_table_schemas_in_database(
    const uint64_t tenant_id,
    const uint64_t database_id,
    ObIArray<const ObSimpleMockFKParentTableSchema *> &schemas) const
{
  int ret = OB_SUCCESS;
  schemas.reset();

  ObMockFKParentTableKey key_lower(tenant_id, OB_MIN_ID);
  ConstMockFKParentTableIter iter_begin = mock_fk_parent_table_infos_.lower_bound(
                                  key_lower, compare_with_mock_fk_parent_table_key);
  bool is_stop = false;
  for (ConstMockFKParentTableIter iter = iter_begin;
       OB_SUCC(ret) && iter != mock_fk_parent_table_infos_.end() && !is_stop;
       ++iter) {
    const ObSimpleMockFKParentTableSchema *mock_fk_parent_table = NULL;
    if (OB_ISNULL(mock_fk_parent_table = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret));
    } else if (tenant_id != mock_fk_parent_table->get_tenant_id()) {
      is_stop = true;
    } else if (database_id != mock_fk_parent_table->get_database_id()) {
      // do nothing
    } else if (OB_FAIL(schemas.push_back(mock_fk_parent_table))) {
      LOG_WARN("push back mock_fk_parent_table failed", K(ret));
    }
  }

  return ret;
}

int ObMockFKParentTableMgr::get_mock_fk_parent_table_schema_with_name(
    const uint64_t tenant_id,
    const uint64_t database_id,
    const common::ObString &mock_fk_parent_table_name,
    const ObSimpleMockFKParentTableSchema *&schema) const
{
  int ret = OB_SUCCESS;
  schema = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id
             || OB_INVALID_ID == database_id
             || 0 == mock_fk_parent_table_name.length()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(database_id), K(mock_fk_parent_table_name));
  } else {
    ObSimpleMockFKParentTableSchema *tmp_schema = NULL;
    ObMockFKParentTableHashWrapper hash_wrap(tenant_id, database_id, mock_fk_parent_table_name);
    if (OB_FAIL(mock_fk_parent_table_map_.get_refactored(hash_wrap, tmp_schema))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("mock_fk_parent_table is not exist", K(tenant_id), K(database_id), K(mock_fk_parent_table_name),
                 "map_cnt", mock_fk_parent_table_map_.item_count());
      }
    } else {
      schema = tmp_schema;
    }
  }
  return ret;
}

} //end of namespace schema
} //end of namespace share
} //end of namespace oceanbase
