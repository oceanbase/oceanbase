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

#define USING_LOG_PREFIX CLIENT
#include "ob_hkv_table.h"
#include "ob_table_service_client.h"
#include "share/table/ob_table.h"
using namespace oceanbase::common;
using namespace oceanbase::table;

const char* ObHKVTable::ROWKEY_CNAME = "K";
const char* ObHKVTable::CQ_CNAME = "Q";
const char* ObHKVTable::VERSION_CNAME = "T";
const char* ObHKVTable::VALUE_CNAME = "V";

const ObString ObHKVTable::ROWKEY_CNAME_STR = ObString::make_string(ROWKEY_CNAME);
const ObString ObHKVTable::CQ_CNAME_STR = ObString::make_string(CQ_CNAME);
const ObString ObHKVTable::VERSION_CNAME_STR = ObString::make_string(VERSION_CNAME);
const ObString ObHKVTable::VALUE_CNAME_STR = ObString::make_string(VALUE_CNAME);

ObHKVTable::Entity::Entity()
    :rowkey_obj_count_(0),
     has_property_set_(false)
{}

ObHKVTable::Entity::~Entity()
{}

void ObHKVTable::Entity::reset()
{
  rowkey_obj_count_ = 0;
  has_property_set_ = false;
}

int ObHKVTable::Entity::set_rowkey(const ObRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  rowkey_obj_count_ = 0;
  int64_t N = rowkey.get_obj_cnt();
  if (N != 3) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rowkey for kv table", K(ret), K(rowkey));
  } else {
    for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i) {
      if (OB_FAIL(set_rowkey_value(i, rowkey.ptr()[i]))) {
        LOG_WARN("failed to set rowkey value", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObHKVTable::Entity::set_rowkey(const ObITableEntity &other)
{
  int ret = OB_SUCCESS;
  const Entity *other_entity = dynamic_cast<const Entity*>(&other);
  if (NULL == other_entity) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid type of other entity");
  } else {
    key_ = other_entity->key_;
    rowkey_obj_count_ = other_entity->rowkey_obj_count_;
  }
  return ret;
}

int ObHKVTable::Entity::set_rowkey_value(int64_t idx, const ObObj &value)
{
  int ret = OB_SUCCESS;
  if (rowkey_obj_count_ >= 3) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("rowkey has only 3 obj", K(ret), K_(rowkey_obj_count));
  } else {
    switch(idx) {
      case 0:
        if (OB_FAIL(value.get_varbinary(key_.rowkey_))) {
          LOG_WARN("failed to get rowkey", K(value));
        }
        break;
      case 1:
        if (OB_FAIL(value.get_varchar(key_.column_qualifier_))) {
          LOG_WARN("failed to get rowkey", K(value));
        }
        break;
      case 2:
        if (OB_FAIL(value.get_int(key_.version_))) {
          LOG_WARN("failed to get rowkey", K(value));
        }
        break;
      default:
        ret = OB_INDEX_OUT_OF_RANGE;
        LOG_WARN("invalid rowkey index", K(ret), K(idx));
        break;
    }
    if (OB_SUCC(ret)) {
      ++rowkey_obj_count_;
    }
  }
  return ret;
}

int ObHKVTable::Entity::add_rowkey_value(const ObObj &value)
{
  int ret = OB_SUCCESS;
  if (rowkey_obj_count_ >= 3) {
    LOG_WARN("rowkey has only 3 obj", K(ret), K_(rowkey_obj_count));
  } else {
    return set_rowkey_value(rowkey_obj_count_, value);
  }
  return ret;
}

int ObHKVTable::Entity::get_rowkey_value(int64_t idx, ObObj &value) const
{
  int ret = OB_SUCCESS;
  switch(idx) {
    case 0:
      value.set_varbinary(key_.rowkey_);
      break;
    case 1:
      value.set_varchar(key_.column_qualifier_);
      value.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      break;
    case 2:
      value.set_int(key_.version_);
      break;
    default:
      ret = OB_INDEX_OUT_OF_RANGE;
      LOG_WARN("invalid rowkey index", K(ret), K(idx));
      break;
  }
  return ret;
}

ObRowkey ObHKVTable::Entity::get_rowkey() const
{
  ObRowkey rk(const_cast<ObObj*>(key_objs_), 3);
  (void)get_rowkey_value(0, const_cast<ObObj&>(key_objs_[0]));
  (void)get_rowkey_value(1, const_cast<ObObj&>(key_objs_[1]));
  (void)get_rowkey_value(2, const_cast<ObObj&>(key_objs_[2]));
  return rk;
}

int64_t ObHKVTable::Entity::hash_rowkey() const
{
  uint64_t hash_value = 0;
  ObObj value;
  for (int64_t i = 0; i < 3; ++i) {
    (void)get_rowkey_value(i, value);
    (void)value.hash(hash_value, hash_value);
  }
  return hash_value;
}

int ObHKVTable::Entity::get_property(const ObString &prop_name, ObObj &prop_value) const
{
  int ret = OB_SUCCESS;
  if (!has_property_set_) {
    ret = OB_HASH_NOT_EXIST;
  } else if (prop_name != VALUE_CNAME_STR) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid property name for kv table", K(ret), K(prop_name));
  } else {
    prop_value = value_;
  }
  return ret;
}

int ObHKVTable::Entity::set_property(const ObString &prop_name, const ObObj &prop_value)
{
  int ret = OB_SUCCESS;
  if (prop_name != VALUE_CNAME_STR) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid property name for kv table", K(ret), K(prop_name));
  } else {
    value_ = prop_value;
    has_property_set_ = true;
  }
  return ret;
}

int ObHKVTable::Entity::get_properties(ObIArray<std::pair<ObString, ObObj> > &properties) const
{
  int ret = OB_SUCCESS;
  properties.reset();
  if (has_property_set_) {
    ret = properties.push_back(std::make_pair(VALUE_CNAME_STR, value_));
  }
  return ret;
}

int ObHKVTable::Entity::get_properties_names(ObIArray<ObString> &properties) const
{
  int ret = OB_SUCCESS;
  properties.reset();
  if (has_property_set_) {
    ret = properties.push_back(VALUE_CNAME_STR);
  }
  return ret;
}

int ObHKVTable::Entity::get_properties_values(ObIArray<ObObj> &values) const
{
  int ret = OB_SUCCESS;
  if (has_property_set_) {
    ret = values.push_back(value_);
  }
  return ret;
}

int64_t ObHKVTable::Entity::get_properties_count() const
{
  return (has_property_set_ ? 1 : 0);
}

////////////////////////////////////////////////////////////////
ObHKVTable::ObHKVTable()
    :inited_(false),
     client_(NULL),
     tbl_(NULL),
     entity_factory_(ObModIds::MYSQL_CLIENT_CACHE)
{}

ObHKVTable::~ObHKVTable()
{
  destroy();
}

int ObHKVTable::init(ObTableServiceClient &client, ObTable *tbl)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
  } else {
    if (NULL == tbl) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("tbl is NULL", K(ret));
    } else {
      client_ = &client;
      tbl_ = tbl;
      tbl_->set_entity_factory(entity_factory_);
      inited_ = true;
    }
  }
  return ret;
}

void ObHKVTable::destroy()
{
  tbl_ = NULL;
  client_ = NULL;
  inited_ = false;
}

int ObHKVTable::get(const Key &key, Value &value)
{
  int ret = OB_SUCCESS;
  Entity entity;
  entity.set_key(key);
  ObObj null_obj;
  entity.set_value(null_obj);
  ObTableOperationResult result;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else {
    ObTableOperation table_op = ObTableOperation::retrieve(entity);
    const ObITableEntity *result_entity = NULL;
    if (OB_FAIL(tbl_->execute(table_op, result))) {
      LOG_WARN("failed to execute retrieve", K(ret), K(key));
    } else if (result.get_errno() == OB_ENTRY_NOT_EXIST) {
      value = ObObj::make_nop_obj();
    } else if (OB_FAIL(result.get_entity(result_entity))) {
      LOG_WARN("failed to get entity", K(ret));
    } else if (result_entity->get_property(VALUE_CNAME_STR, value)){
      LOG_WARN("failed to get V value", K(ret));
    }
  }
  return ret;
}

int ObHKVTable::multi_get(const IKeys &keys, IValues &values)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else {
    ObTableBatchOperation batch_operation;
    ObTableBatchOperationResult batch_result;
    ObTableEntityFactory<Entity> entity_factory;
    ObObj null_obj;
    int64_t N = keys.count();
    for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
    {
      const Key &key = keys.at(i);
      ObITableEntity *entity = entity_factory.alloc();
      if (NULL == entity) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc entity", K(ret));
        break;
      }
      Entity *real_entity = dynamic_cast<Entity*>(entity);
      real_entity->set_key(key);
      real_entity->set_value(null_obj);
      if (OB_FAIL(batch_operation.retrieve(*entity))) {
        LOG_WARN("failed to add table operation", K(ret));
      }
    } // end for
    if (OB_SUCC(ret)) {
      if (OB_FAIL(tbl_->batch_execute(batch_operation, batch_result))) {
        LOG_WARN("failed to execute put", K(ret), K(batch_operation));
      } else {
        values.reset();
        const ObITableEntity *result_entity = NULL;
        const int64_t N = batch_result.count();
        ObObj value;
        ObObj nop_obj = ObObj::make_nop_obj();
        for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
        {
          if (batch_result.at(i).get_errno() == OB_ENTRY_NOT_EXIST) {
            if (OB_FAIL(values.push_back(nop_obj))) {
              LOG_WARN("failed to push value", K(ret));
            }
          } else if (OB_FAIL(batch_result.at(i).get_entity(result_entity))) {
            LOG_WARN("failed to get entity", K(ret));
          } else if (result_entity->get_property(VALUE_CNAME_STR, value)){
            LOG_WARN("failed to get V value", K(ret));
          } else if (OB_FAIL(values.push_back(value))) {
            LOG_WARN("failed to push value", K(ret));
          }
        } // end for
      }
    }
  }
  return ret;
}

int ObHKVTable::put(const Key &key, const Value &value)
{
  int ret = OB_SUCCESS;
  Entity entity;
  entity.set_key(key);
  entity.set_value(value);
  ObTableOperationResult result;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
  } else {
    ObTableOperation table_op = ObTableOperation::insert_or_update(entity);
    if (OB_FAIL(tbl_->execute(table_op, result))) {
      LOG_WARN("failed to execute put", K(ret), K(key));
    } else {
      ret = result.get_errno();
    }
  }
  return ret;
}

int ObHKVTable::multi_put(const IKeys &keys, const IValues &values)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (keys.count() != values.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("keys and values have different size", K(ret));
  } else {
    ObTableBatchOperation batch_operation;
    ObTableBatchOperationResult batch_result;
    ObTableEntityFactory<Entity> entity_factory;
    int64_t N = keys.count();
    for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
    {
      const Key &key = keys.at(i);
      const Value &value = values.at(i);
      ObITableEntity *entity = entity_factory.alloc();
      if (NULL == entity) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc entity", K(ret));
        break;
      }
      Entity *real_entity = dynamic_cast<Entity*>(entity);
      real_entity->set_key(key);
      real_entity->set_value(value);
      if (OB_FAIL(batch_operation.insert(*entity))) {
        LOG_WARN("failed to add table operation", K(ret));
      } else {}
    } // end for
    if (OB_SUCC(ret)) {
      if (OB_FAIL(tbl_->batch_execute(batch_operation, batch_result))) {
        LOG_WARN("failed to execute multi_put", K(ret), K(batch_operation));
      } else {
        const int64_t N = batch_result.count();
        for (int64_t i = 0; i < N; ++i)
        {
          if (batch_result.at(i).get_errno() != OB_SUCCESS) {
            ret = batch_result.at(i).get_errno();
            break;
          }
        }
      }
    }
  }
  return ret;
}

int ObHKVTable::multi_put(const IEntities &entities)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else {
    ObTableBatchOperation batch_operation;
    ObTableBatchOperationResult batch_result;
    ObTableEntityFactory<Entity> entity_factory;
    int64_t N = entities.count();
    for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
    {
      const Key &key = entities.at(i).key();
      const Value &value = entities.at(i).value();
      ObITableEntity *entity = entity_factory.alloc();
      if (NULL == entity) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc entity", K(ret));
        break;
      }
      Entity *real_entity = dynamic_cast<Entity*>(entity);
      real_entity->set_key(key);
      real_entity->set_value(value);
      if (OB_FAIL(batch_operation.insert_or_update(*entity))) {
        LOG_WARN("failed to add table operation", K(ret));
      }
    } // end for
    if (OB_SUCC(ret)) {
      if (OB_FAIL(tbl_->batch_execute(batch_operation, batch_result))) {
        LOG_WARN("failed to execute multi-put", K(ret));
      } else {
        const int64_t N = batch_result.count();
        for (int64_t i = 0; i < N; ++i)
        {
          if (batch_result.at(i).get_errno() != OB_SUCCESS) {
            ret = batch_result.at(i).get_errno();
            break;
          }
        }
      }
    }
  }
  return ret;
}

int ObHKVTable::remove(const Key &key)
{
  int ret = OB_SUCCESS;
  Entity entity;
  entity.set_key(key);
  ObObj null_obj;
  entity.set_value(null_obj);
  ObTableOperationResult result;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
  } else {
    ObTableOperation table_op = ObTableOperation::del(entity);
    if (OB_FAIL(tbl_->execute(table_op, result))) {
      LOG_WARN("failed to execute remove", K(ret), K(key));
    } else {
      ret = result.get_errno();
    }
  }
  return ret;
}

int ObHKVTable::multi_remove(const IKeys &keys)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else {
    ObTableBatchOperation batch_operation;
    ObTableBatchOperationResult batch_result;
    ObTableEntityFactory<Entity> entity_factory;
    int64_t N = keys.count();
    for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
    {
      const Key &key = keys.at(i);
      ObITableEntity *entity = entity_factory.alloc();
      if (NULL == entity) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc entity", K(ret));
        break;
      }
      Entity *real_entity = dynamic_cast<Entity*>(entity);
      real_entity->set_key(key);
      if (OB_FAIL(batch_operation.del(*entity))) {
        LOG_WARN("failed to add table operation", K(ret));
      }
    } // end for
    if (OB_SUCC(ret)) {
      if (OB_FAIL(tbl_->batch_execute(batch_operation, batch_result))) {
        LOG_WARN("failed to execute multi-remove", K(ret));
      } else {
        const int64_t N = batch_result.count();
        for (int64_t i = 0; i < N; ++i)
        {
          if (batch_result.at(i).get_errno() != OB_SUCCESS) {
            ret = batch_result.at(i).get_errno();
            break;
          }
        }
      }
    }
  }
  return ret;
}
