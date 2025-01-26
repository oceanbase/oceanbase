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
#include "ob_table.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "ob_table_object.h"
#include "observer/table/ob_htable_utils.h"

using namespace oceanbase::table;
using namespace oceanbase::common;

int ObTableSingleOpEntity::construct_column_names(const ObTableBitMap &names_bit_map,
                                                  const ObIArray<ObString> &dictionary,
                                                  ObIArray<ObString> &column_names)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 8> names_pos;
  if (OB_FAIL(names_bit_map.get_true_bit_positions(names_pos))) {
    LOG_WARN("failed to get col names pos", K(ret), K(names_bit_map));
  } else {
    if (OB_FAIL(column_names.reserve(names_pos.count()))) {
      LOG_WARN("failed to reserve column_names", K(ret), K(names_pos));
    }
    int64_t all_keys_count = dictionary.count();
    ObString column_name;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < names_pos.count(); ++idx) {
      if (OB_UNLIKELY(names_pos[idx] >= all_keys_count)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("names_pos idx greater than all_keys_count", K(ret), K(all_keys_count), K(names_pos));
      } else if (OB_FAIL(dictionary.at(names_pos[idx], column_name))) {
        LOG_WARN("failed to get real_names", K(ret), K(idx), K(dictionary), K(names_pos));
      } else if (OB_FAIL(column_names.push_back(column_name))) {
        LOG_WARN("failed to push real_names", K(ret), K(idx));
      }
    }
  }

  return ret;
}

OB_DEF_SERIALIZE(ObITableEntity)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    const int64_t rowkey_size = get_rowkey_size();
    OB_UNIS_ENCODE(rowkey_size);
    ObObj obj;
    for (int64_t i = 0; i < rowkey_size && OB_SUCCESS == ret; ++i) {
      if (OB_FAIL(this->get_rowkey_value(i, obj))) {
        LOG_WARN("failed to get value", K(ret), K(i));
      }
      OB_UNIS_ENCODE(obj);
    }
  }
  if (OB_SUCC(ret)) {
    ObTableEntityType entity_type = const_cast<ObITableEntity *>(this)->get_entity_type();
    if (entity_type == ObTableEntityType::ET_KV) {
      const ObTableEntity *entity = static_cast<const ObTableEntity *>(this);
      const int64_t properties_count = get_properties_count();
      OB_UNIS_ENCODE(properties_count);
      const ObIArray<ObString>& prop_names = entity->get_properties_names();
      const ObIArray<ObObj>& prop_values = entity->get_properties_values();
      if (properties_count != prop_names.count() || properties_count != prop_values.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("propertites count is not matched", K(properties_count), K(prop_names.count()), K(prop_values.count()));
      }
      for (int64_t i = 0; i < properties_count && OB_SUCCESS == ret; ++i) {
        OB_UNIS_ENCODE(prop_names.at(i));
        OB_UNIS_ENCODE(prop_values.at(i));
      }
    } else {
       ObSEArray<std::pair<ObString, ObObj>, 8> properties;
      if (OB_FAIL(this->get_properties(properties))) {  // @todo optimize, use iterator
        LOG_WARN("failed to get properties", K(ret));
      } else {
        const int64_t properties_count = properties.count();
        OB_UNIS_ENCODE(properties_count);
        for (int64_t i = 0; i < properties_count && OB_SUCCESS == ret; ++i) {
          const std::pair<ObString, ObObj> &kv_pair = properties.at(i);
          OB_UNIS_ENCODE(kv_pair.first);
          OB_UNIS_ENCODE(kv_pair.second);
        }
      }
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObITableEntity)
{
  int ret = OB_SUCCESS;
  ObString key;
  ObObj value;
  if (NULL == alloc_) {
    // shallow copy
    if (OB_SUCC(ret)) {
      int64_t rowkey_size = -1;
      OB_UNIS_DECODE(rowkey_size);
      for (int64_t i = 0; OB_SUCCESS == ret && i < rowkey_size; ++i) {
        OB_UNIS_DECODE(value);
        if (OB_SUCC(ret)) {
          if (OB_FAIL(this->add_rowkey_value(value))) {
            LOG_WARN("failed to add rowkey value", K(ret), K(value));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      int64_t properties_count = -1;
      OB_UNIS_DECODE(properties_count);
      for (int64_t i = 0; i < properties_count && OB_SUCCESS == ret; ++i) {
        OB_UNIS_DECODE(key);
        OB_UNIS_DECODE(value);
        if (OB_SUCC(ret)) {
          if (OB_FAIL(this->set_property(key, value))) {
            LOG_WARN("failed to set property", K(ret), K(key), K(value));
          }
        }
      }
    }
  } else {
    // deep copy
    ObObj value_clone;
    if (OB_SUCC(ret)) {
      int64_t rowkey_size = -1;
      OB_UNIS_DECODE(rowkey_size);
      for (int64_t i = 0; OB_SUCCESS == ret && i < rowkey_size; ++i) {
        OB_UNIS_DECODE(value);
        if (OB_SUCC(ret)) {
          if (OB_FAIL(ob_write_obj(*alloc_, value, value_clone))) {
            LOG_WARN("failed to copy value", K(ret));
          } else if (OB_FAIL(this->add_rowkey_value(value_clone))) {
            LOG_WARN("failed to add rowkey value", K(ret), K(value_clone));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObString key_clone;
      int64_t properties_count = -1;
      OB_UNIS_DECODE(properties_count);
      for (int64_t i = 0; i < properties_count && OB_SUCCESS == ret; ++i) {
        OB_UNIS_DECODE(key);
        OB_UNIS_DECODE(value);
        if (OB_SUCC(ret)) {
          if (OB_FAIL(ob_write_string(*alloc_, key, key_clone))) {
            LOG_WARN("failed to clone string", K(ret));
          } else if (OB_FAIL(ob_write_obj(*alloc_, value, value_clone))) {
            LOG_WARN("failed to copy value", K(ret));
          } else if (OB_FAIL(this->set_property(key_clone, value_clone))) {
            LOG_WARN("failed to set property", K(ret), K(key), K(value_clone));
          }
        }
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObITableEntity)
{
  int64_t len = 0;
  int ret = OB_SUCCESS;
  ObString key;
  ObObj value;
  const int64_t rowkey_size = get_rowkey_size();
  OB_UNIS_ADD_LEN(rowkey_size);
  for (int64_t i = 0; i < rowkey_size && OB_SUCCESS == ret; ++i) {
    if (OB_FAIL(this->get_rowkey_value(i, value))) {
      LOG_WARN("failed to get value", K(ret), K(i));
    }
    OB_UNIS_ADD_LEN(value);
  }

  if (OB_SUCC(ret)) {
    ObTableEntityType entity_type = const_cast<ObITableEntity *>(this)->get_entity_type();
    if (entity_type == ObTableEntityType::ET_KV) {
      const ObTableEntity *entity = static_cast<const ObTableEntity *>(this);
      const int64_t properties_count = get_properties_count();
      OB_UNIS_ADD_LEN(properties_count);
      const ObIArray<ObString>& prop_names = entity->get_properties_names();
      const ObIArray<ObObj>& prop_values = entity->get_properties_values();
      if (properties_count != prop_names.count() || properties_count != prop_values.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("propertites count is not matched", K(properties_count), K(prop_names.count()), K(prop_values.count()));
      }
      for (int64_t i = 0; i < properties_count && OB_SUCCESS == ret; ++i) {
        OB_UNIS_ADD_LEN(prop_names.at(i));
        OB_UNIS_ADD_LEN(prop_values.at(i));
      }
    } else {
       ObSEArray<std::pair<ObString, ObObj>, 8> properties;
      if (OB_FAIL(this->get_properties(properties))) {  // @todo optimize, use iterator
        LOG_WARN("failed to get properties", K(ret));
      } else {
        const int64_t properties_count = properties.count();
        OB_UNIS_ADD_LEN(properties_count);
        for (int64_t i = 0; i < properties_count && OB_SUCCESS == ret; ++i) {
          const std::pair<ObString, ObObj> &kv_pair = properties.at(i);
          OB_UNIS_ADD_LEN(kv_pair.first);
          OB_UNIS_ADD_LEN(kv_pair.second);
        }
      }
    }
  }
  return len;
}

int ObITableEntity::deep_copy(common::ObIAllocator &allocator, const ObITableEntity &other)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(deep_copy_rowkey(allocator, other))) {
  } else if (OB_FAIL(deep_copy_properties(allocator, other))) {
  }
  return ret;
}

int ObITableEntity::deep_copy_rowkey(common::ObIAllocator &allocator, const ObITableEntity &other)
{
  int ret = OB_SUCCESS;
  ObObj value;
  ObObj cell_clone;
  const int64_t rowkey_size = other.get_rowkey_size();
  for (int64_t i = 0; OB_SUCCESS == ret && i < rowkey_size; ++i)
  {
    if (OB_FAIL(other.get_rowkey_value(i, value))) {
      LOG_WARN("failed to get rowkey value", K(ret), K(i), K(value));
    } else if (OB_FAIL(ob_write_obj(allocator, value, cell_clone))) {
      LOG_WARN("failed to copy cell", K(ret));
    } else if (OB_FAIL(this->add_rowkey_value(cell_clone))) {
      LOG_WARN("failed to add rowkey value", K(ret), K(value));
    }
  } // end for
  return ret;
}

int ObITableEntity::deep_copy_properties(common::ObIAllocator &allocator, const ObITableEntity &other)
{
  int ret = OB_SUCCESS;
  ObObj value;
  ObObj cell_clone;
  ObString name_clone;
  ObSEArray<std::pair<ObString, ObObj>, 8> properties;
  if (OB_FAIL(other.get_properties(properties))) {  // @todo optimize, use iterator
    LOG_WARN("failed to get properties", K(ret));
  } else {
    const int64_t properties_count = properties.count();
    for (int64_t i = 0; i < properties_count && OB_SUCCESS == ret; ++i) {
      const std::pair<ObString, ObObj> &kv_pair = properties.at(i);
      if (OB_FAIL(ob_write_string(allocator, kv_pair.first, name_clone))) {
        LOG_WARN("failed to clone string", K(ret));
      } else if (OB_FAIL(ob_write_obj(allocator, kv_pair.second, cell_clone))) {
        LOG_WARN("failed to copy cell", K(ret));
      } else if (OB_FAIL(this->set_property(name_clone, cell_clone))) {
        LOG_WARN("failed to set property", K(ret));
      }
    }  // end for
  }
  return ret;
}

int ObITableEntity::add_retrieve_property(const ObString &prop_name)
{
  ObObj null_obj;
  return set_property(prop_name, null_obj);
}

////////////////////////////////////////////////////////////////
ObTableEntity::ObTableEntity()
{}

ObTableEntity::~ObTableEntity()
{}

void ObTableEntity::reset()
{
    rowkey_.reset();
    properties_names_.reset();
    properties_values_.reset();
}

int ObTableEntity::construct_names_bitmap(const ObITableEntity &req_entity)
{
  int ret = OB_NOT_IMPLEMENT;
  LOG_WARN("not surpport to construct_names_bitmap", K(ret));
  return OB_NOT_IMPLEMENT;
}

const ObTableBitMap *ObTableEntity::get_rowkey_names_bitmap() const
{
  int ret = OB_NOT_IMPLEMENT;
  LOG_WARN("not surpport to get_rowkey_names_bitmap", K(ret));
  return nullptr;
}

const ObTableBitMap *ObTableEntity::get_properties_names_bitmap() const
{
  int ret = OB_NOT_IMPLEMENT;
  LOG_WARN("not surpport to get_properties_names_bitmap", K(ret));
  return nullptr;
}

const ObIArray<ObString> *ObTableEntity::get_all_rowkey_names() const
{
  int ret = OB_NOT_IMPLEMENT;
  LOG_WARN("not surpport", K(ret));
  return nullptr;
}

const ObIArray<ObString> *ObTableEntity::get_all_properties_names() const
{
  return &properties_names_;
}

void ObTableEntity::set_is_same_properties_names(bool is_same_properties_names)
{
  int ret = OB_NOT_IMPLEMENT;
  LOG_WARN("not surpport", K(ret));
}

int ObTableEntity::set_rowkey_value(int64_t idx, const ObObj &value)
{
  int ret = OB_SUCCESS;
  if (idx < 0) {
    ret = OB_INDEX_OUT_OF_RANGE;
  } else if (idx < rowkey_.count()) {
    rowkey_.at(idx) = value;
  } else {
    int64_t N = rowkey_.count();
    ObObj null_obj;
    for (int64_t i = N; OB_SUCC(ret) && i < idx; ++i) {
      if (OB_FAIL(rowkey_.push_back(null_obj))) {
        LOG_WARN("failed to pad null obj", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(rowkey_.push_back(value))) {
        LOG_WARN("failed to add value obj", K(ret), K(value));
      }
    }
  }
  return ret;
}

int ObTableEntity::add_rowkey_value(const ObObj &value)
{
  return rowkey_.push_back(value);
}

int ObTableEntity::get_rowkey_value(int64_t idx, ObObj &value) const
{
  return rowkey_.at(idx, value);
}

int ObTableEntity::set_rowkey(const ObRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  rowkey_.reset();
  const int64_t N = rowkey.get_obj_cnt();
  for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
  {
    if (OB_FAIL(rowkey_.push_back(rowkey.ptr()[i]))) {
      LOG_WARN("failed to push back rowkey", K(ret), K(i));
    }
  } // end for
  return ret;
}

int ObTableEntity::set_rowkey(const ObString &rowkey_name, const ObObj &rowkey_obj) {
  int ret = OB_SUCCESS;
  if (rowkey_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("rowkey name should not be empty string", K(ret), K(rowkey_name));
  } else {
    if (OB_FAIL(rowkey_names_.push_back(rowkey_name))) {
      LOG_WARN("failed to add prop name", K(ret), K(rowkey_name));
    } else if (OB_FAIL(rowkey_.push_back(rowkey_obj))) {
      LOG_WARN("failed to add prop value", K(ret), K(rowkey_obj));
    }
  }
  return ret;
}

int ObTableEntity::set_rowkey(const ObITableEntity &other)
{
  int ret = OB_SUCCESS;
  const ObTableEntity *other_entity = dynamic_cast<const ObTableEntity*>(&other);
  if (NULL == other_entity) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid type of other entity");
  } else {
    rowkey_.reset();
    int64_t N = other_entity->rowkey_.count();
    for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
    {
      if (OB_FAIL(rowkey_.push_back(other_entity->rowkey_.at(i)))) {
        LOG_WARN("failed to push back rowkey");
      }
    } // end for
  }
  return ret;
}

int64_t ObTableEntity::hash_rowkey() const
{
  uint64_t hash_value = 0;
  const int64_t N = rowkey_.count();
  for (int64_t i = 0; i < N; ++i)
  {
    rowkey_.at(i).hash(hash_value, hash_value);
  } // end for
  return hash_value;
}

bool ObTableEntity::has_exist_in_properties(const ObString &name, int64_t *idx /* =nullptr */) const
{
  bool exist = false;
  int64_t num = properties_names_.count();
  for (int64_t i = 0; i < num && !exist; i++) {
    if (0 == name.case_compare(properties_names_.at(i))) {
      exist = true;
      if (idx != NULL) {
        *idx = i;
      }
    }
  }
  return exist;
}

int ObTableEntity::get_property(const ObString &prop_name, ObObj &prop_value) const
{
  int ret = OB_SUCCESS;
  if (prop_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("property name should not be empty string", K(ret), K(prop_name));
  } else {
    int64_t idx = -1;
    if (has_exist_in_properties(prop_name, &idx)) {
      prop_value = properties_values_.at(idx);
    } else {
      ret = OB_SEARCH_NOT_FOUND;
      LOG_DEBUG("property name not exists in properties", K(ret), K(prop_name));
    }
  }
  return ret;
}

int ObTableEntity::set_property(const ObString &prop_name, const ObObj &prop_value)
{
  int ret = OB_SUCCESS;
  if (prop_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("property name should not be empty string", K(ret), K(prop_name));
  } else {
    // allow to push back repeated property, but only the first property will be get.
    if (OB_FAIL(properties_names_.push_back(prop_name))) {
      LOG_WARN("failed to add prop name", K(ret), K(prop_name));
    } else if (OB_FAIL(properties_values_.push_back(prop_value))) {
      LOG_WARN("failed to add prop value", K(ret), K(prop_value));
    }
  }
  return ret;
}

int ObTableEntity::push_value(const ObObj &prop_value)
{
  return properties_values_.push_back(prop_value);
}

int ObTableEntity::get_properties(ObIArray<std::pair<ObString, ObObj> > &properties) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < properties_names_.count(); i++) {
    if (OB_FAIL(properties.push_back(std::make_pair(
                                     properties_names_.at(i),
                                     properties_values_.at(i))))) {
      LOG_WARN("failed to add name-value pair", K(ret), K(i));
    }
  }
  return ret;
}

int ObTableEntity::get_properties_names(ObIArray<ObString> &properties_names) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(properties_names.assign(properties_names_))) {
    LOG_WARN("fail to assign properties name array", K(ret));
  }
  return ret;
}

int ObTableEntity::get_properties_values(ObIArray<ObObj> &properties_values) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(properties_values.assign(properties_values_))) {
    LOG_WARN("failed to assign properties values array", K(ret));
  }
  return ret;
}

const ObObj &ObTableEntity::get_properties_value(int64_t idx) const
{
  return properties_values_.at(idx);
}

int64_t ObTableEntity::get_properties_count() const
{
  return properties_names_.count();
}

void ObTableEntity::set_dictionary(
    const ObIArray<ObString> *all_rowkey_names, const ObIArray<ObString> *all_properties_names)
{
  int ret = OB_NOT_IMPLEMENT;
  LOG_WARN("not surpport set_dictionary", K(ret));
  return;
}

ObRowkey ObTableEntity::get_rowkey() const
{
  ObRowkey rowkey;
  int64_t obj_cnt = rowkey_.count();
  if (obj_cnt > 0) {
    rowkey.assign(const_cast<ObObj*>(&rowkey_.at(0)), obj_cnt);
  }
  return rowkey;
}

DEF_TO_STRING(ObTableEntity)
{
  int64_t pos = 0;
  J_OBJ_START();

  J_NAME("rowkey_names");
  J_COLON();
  BUF_PRINTO(rowkey_names_);

  J_NAME("rowkey");
  J_COLON();
  BUF_PRINTO(rowkey_);

  ObSEArray<std::pair<ObString, ObObj>, 8> properties;
  int ret = OB_SUCCESS;
  if (OB_FAIL(this->get_properties(properties))) {
    LOG_WARN("failed to get properties", K(ret));
  } else {
    J_COMMA();
    J_NAME("properties");
    J_COLON();
    J_OBJ_START();
    int64_t N = properties.count();
    for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
    {
      const std::pair<ObString, ObObj> &prop = properties.at(i);
      if (0 != i) {
        J_COMMA();
      }
      BUF_PRINTF("%.*s", prop.first.length(), prop.first.ptr());
      J_COLON();
      BUF_PRINTO(prop.second);
    }
    J_OBJ_END();
  }
  J_OBJ_END();
  return pos;
}

////////////////////////////////////////////////////////////////
ObTableOperation ObTableOperation::insert(const ObITableEntity &entity)
{
  ObTableOperation op;
  op.operation_type_ = ObTableOperationType::INSERT;
  op.entity_ = &entity;
  return op;
}

ObTableOperation ObTableOperation::put(const ObITableEntity &entity)
{
  ObTableOperation op;
  op.operation_type_ = ObTableOperationType::PUT;
  op.entity_ = &entity;
  return op;
}

ObTableOperation ObTableOperation::del(const ObITableEntity &entity)
{
  ObTableOperation op;
  op.operation_type_ = ObTableOperationType::DEL;
  op.entity_ = &entity;
  return op;
}

ObTableOperation ObTableOperation::update(const ObITableEntity &entity)
{
  ObTableOperation op;
  op.operation_type_ = ObTableOperationType::UPDATE;
  op.entity_ = &entity;
  return op;
}

ObTableOperation ObTableOperation::insert_or_update(const ObITableEntity &entity)
{
  ObTableOperation op;
  op.operation_type_ = ObTableOperationType::INSERT_OR_UPDATE;
  op.entity_ = &entity;
  return op;
}

ObTableOperation ObTableOperation::replace(const ObITableEntity &entity)
{
  ObTableOperation op;
  op.operation_type_ = ObTableOperationType::REPLACE;
  op.entity_ = &entity;
  return op;
}

ObTableOperation ObTableOperation::retrieve(const ObITableEntity &entity)
{
  ObTableOperation op;
  op.operation_type_ = ObTableOperationType::GET;
  op.entity_ = &entity;
  return op;
}

ObTableOperation ObTableOperation::increment(const ObITableEntity &entity)
{
  ObTableOperation op;
  op.operation_type_ = ObTableOperationType::INCREMENT;
  op.entity_ = &entity;
  return op;
}

ObTableOperation ObTableOperation::append(const ObITableEntity &entity)
{
  ObTableOperation op;
  op.operation_type_ = ObTableOperationType::APPEND;
  op.entity_ = &entity;
  return op;
}

int ObTableOperation::get_entity(ObITableEntity *&entity)
{
  int ret = OB_SUCCESS;
  if (NULL == entity_) {
    ret = OB_ERR_NULL_VALUE;
  } else {
    entity = const_cast<ObITableEntity*>(this->entity_);
  }
  return ret;
}

uint64_t ObTableOperation::get_checksum()
{
  uint64_t checksum = 0;
  ObITableEntity *entity = NULL;
  if (OB_SUCCESS != get_entity(entity) || OB_ISNULL(entity)) {
    // ignore
  } else {
    const int64_t rowkey_size = entity->get_rowkey_size();
    const int64_t property_count = entity->get_properties_count();
    checksum = ob_crc64(checksum, &rowkey_size, sizeof(rowkey_size));
    checksum = ob_crc64(checksum, &property_count, sizeof(property_count));
  }
  checksum = ob_crc64(checksum, &operation_type_, sizeof(operation_type_));
  return checksum;
}

const char *ObTableOperation::get_op_name(ObTableOperationType::Type op_type)
{
  const char *op_name = "unknown";
  switch (op_type) {
    case ObTableOperationType::Type::GET: {
      op_name = "get";
      break;
    }
    case ObTableOperationType::Type::INSERT: {
      op_name = "insert";
      break;
    }
    case ObTableOperationType::Type::DEL: {
      op_name = "delete";
      break;
    }
    case ObTableOperationType::Type::UPDATE: {
      op_name = "update";
      break;
    }
    case ObTableOperationType::Type::INSERT_OR_UPDATE: {
      op_name = "insertOrUpdate";
      break;
    }
    case ObTableOperationType::Type::REPLACE: {
      op_name = "replace";
      break;
    }
    case ObTableOperationType::Type::INCREMENT: {
      op_name = "increment";
      break;
    }
    case ObTableOperationType::Type::APPEND: {
      op_name = "append";
      break;
    }
    case ObTableOperationType::Type::CHECK_AND_INSERT_UP: {
      op_name = "checkAndInsertUp";
      break;
    }
    case ObTableOperationType::Type::PUT: {
      op_name = "put";
      break;
    }
    default: {
      op_name = "unknown";
    }
  }

  return op_name;
}

// statement is "insert test col1, col2, col3"
int64_t ObTableOperation::get_stmt_length(const ObString &table_name) const
{
  int64_t len = 0;
  ObString tmp_table_name = table_name.empty() ? ObString::make_string("(null)") : table_name;

  len += strlen(get_op_name(operation_type_)); // "insert"
  len += 1; // blank
  len += tmp_table_name.length(); // "table_name"
  len += 1; // blank
  const ObIArray<ObString> *propertiy_names = entity_->get_all_properties_names();
  if (OB_NOT_NULL(propertiy_names)) {
    int64_t N = propertiy_names->count();
    for (int64_t index = 0; index < N - 1; ++index) {
      len += propertiy_names->at(index).length(); // col1
      len += 2; // "\"\"" double quote
      len += 2; // ", "
    }
    if (0 < N) {
      len += propertiy_names->at(N - 1).length(); // col_n
      len += 2; // "\"\"" double quote
    }
  }

  return len;
}

// statement is "insert test col1, col2, col3"
int ObTableOperation::generate_stmt(const ObString &table_name,
                                    char *buf,
                                    int64_t buf_len,
                                    int64_t &pos) const
{
  int ret = OB_SUCCESS;
  ObString tmp_table_name = table_name.empty() ? ObString::make_string("(null)") : table_name;

  const char *op_name = get_op_name(operation_type_);
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is bull", KR(ret));
  } else if (buf_len < strlen(op_name)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buffer not enough", K(ret), K(buf_len), K(strlen(op_name)));
  } else {
    int64_t n = snprintf(buf + pos, buf_len - pos, "%s ", op_name); // "insert "
    if (n < 0 || n > buf_len - pos) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("snprintf error or buf not enough", KR(ret), K(n), K(pos), K(buf_len));
    } else {
      pos += n;
      strncat(buf + pos, tmp_table_name.ptr(), tmp_table_name.length()); // $table_name
      pos += tmp_table_name.length();
      int64_t n = snprintf(buf + pos, buf_len - pos, " ");
      if (n < 0 || n > buf_len - pos) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("snprintf error or buf not enough", KR(ret), K(n), K(pos), K(buf_len));
      } else {
        pos += n;
        const ObIArray<ObString> *propertiy_names = entity_->get_all_properties_names();
        if (OB_NOT_NULL(propertiy_names)) {
          int64_t N = propertiy_names->count();
          for (int64_t index = 0; index < N - 1; ++index) {
            BUF_PRINTO(propertiy_names->at(index)); // pos will change in BUF_PRINTO
            J_COMMA(); // ", "
          }
          if (0 < N) {
            BUF_PRINTO(propertiy_names->at(N - 1));
          }
        }
      }
    }
  }

  return ret;
}

OB_SERIALIZE_MEMBER(ObTableOperation, operation_type_, const_cast<ObITableEntity&>(*entity_));

////////////////////////////////////////////////////////////////
ObTableRequestOptions::ObTableRequestOptions()
    :consistency_level_(ObTableConsistencyLevel::STRONG),
     server_timeout_us_(10*1000*1000),
     max_execution_time_us_(10*1000*1000),
     retry_policy_(NULL),
     returning_affected_rows_(false),
     option_flag_(OB_TABLE_OPTION_DEFAULT),
     returning_affected_entity_(false),
     batch_operation_as_atomic_(false),
     binlog_row_image_type_(ObBinlogRowImageType::FULL)
{}

////////////////////////////////////////////////////////////////
int ObTableBatchOperation::add(const ObTableOperation &table_operation)
{
  int ret = table_operations_.push_back(table_operation);
  if (OB_SUCC(ret)) {
    if (is_readonly_ && ObTableOperationType::GET != table_operation.type()) {
      is_readonly_ = false;
    }
    if (is_same_type_) {
      const int64_t N = table_operations_.count();
      if (N >= 2 && table_operations_.at(N - 1).type() != table_operations_.at(N-2).type()) {
        is_same_type_ = false;
      }
    }
    if (is_same_properties_names_) {
      const int64_t num = table_operations_.count();
      if (num >= 2) {
        const ObTableOperation &prev = table_operations_.at(num-2);
        const ObTableOperation &curr = table_operations_.at(num-1);
        ObSEArray<ObString, 8> prev_columns;
        ObSEArray<ObString, 8> curr_columns;
        if (OB_FAIL(prev.entity().get_properties_names(prev_columns))) {
          LOG_WARN("failed to get retrieve columns", K(ret));
        } else if (OB_FAIL(curr.entity().get_properties_names(curr_columns))) {
          LOG_WARN("failed to get retrieve columns", K(ret));
        } else if (prev_columns.count() != curr_columns.count()) {
          is_same_properties_names_ = false;
        } else if (prev_columns.count() == 1) {
          is_same_properties_names_ = (prev_columns.at(0) == curr_columns.at(0));
        } else {
          int64_t N = prev_columns.count();
          ObObj value;
          for (int64_t i = 0; OB_SUCCESS == ret && i < N && is_same_properties_names_; ++i)
          {
            const ObString &name = prev_columns.at(i);
            if (OB_FAIL(curr.entity().get_property(name, value))) {
              if (OB_SEARCH_NOT_FOUND == ret) {
                is_same_properties_names_ = false;
              }
            }
          } // end for
        }
      }
    }
  }
  return ret;
}

int ObTableBatchOperation::insert(const ObITableEntity &entity)
{
  ObTableOperation op = ObTableOperation::insert(entity);
  return add(op);
}

int ObTableBatchOperation::put(const ObITableEntity &entity)
{
  ObTableOperation op = ObTableOperation::put(entity);
  return add(op);
}

int ObTableBatchOperation::del(const ObITableEntity &entity)
{
  ObTableOperation op = ObTableOperation::del(entity);
  return add(op);
}

int ObTableBatchOperation::update(const ObITableEntity &entity)
{
  ObTableOperation op = ObTableOperation::update(entity);
  return add(op);
}

int ObTableBatchOperation::insert_or_update(const ObITableEntity &entity)
{
  ObTableOperation op = ObTableOperation::insert_or_update(entity);
  return add(op);
}

int ObTableBatchOperation::replace(const ObITableEntity &entity)
{
  ObTableOperation op = ObTableOperation::replace(entity);
  return add(op);
}

int ObTableBatchOperation::retrieve(const ObITableEntity &entity)
{
  ObTableOperation op = ObTableOperation::retrieve(entity);
  return add(op);
}

int ObTableBatchOperation::increment(const ObITableEntity &entity)
{
  ObTableOperation op = ObTableOperation::increment(entity);
  return add(op);
}

int ObTableBatchOperation::append(const ObITableEntity &entity)
{
  ObTableOperation op = ObTableOperation::append(entity);
  return add(op);
}

uint64_t ObTableBatchOperation::get_checksum()
{
  uint64_t checksum = 0;
  const int64_t op_count = table_operations_.count();
  if (op_count > 0) {
    if (is_same_type()) {
      const uint64_t first_op_checksum = table_operations_.at(0).get_checksum();
      checksum = ob_crc64(checksum, &first_op_checksum, sizeof(first_op_checksum));
    } else {
      for (int64_t i = 0; i < op_count; ++i) {
        const uint64_t cur_op_checksum = table_operations_.at(i).get_checksum();
        checksum = ob_crc64(checksum, &cur_op_checksum, sizeof(cur_op_checksum));
      }
    }
  }
  checksum = ob_crc64(checksum, &is_readonly_, sizeof(is_readonly_));
  checksum = ob_crc64(checksum, &is_same_type_, sizeof(is_same_type_));
  checksum = ob_crc64(checksum, &is_same_properties_names_, sizeof(is_same_properties_names_));

  return checksum;
}

void ObTableBatchOperation::reset()
{
  table_operations_.reset();
  is_readonly_ = true;
  is_same_type_ = true;
  is_same_properties_names_ = true;
}

/*
OB_SERIALIZE_MEMBER(ObTableBatchOperation,
                    table_operations_,
                    is_readonly_,
                    is_same_type_,
                    is_same_properties_names_);
*/
OB_UNIS_DEF_SERIALIZE(ObTableBatchOperation,
                      table_operations_,
                      is_readonly_,
                      is_same_type_,
                      is_same_properties_names_);

OB_UNIS_DEF_SERIALIZE_SIZE(ObTableBatchOperation,
                           table_operations_,
                           is_readonly_,
                           is_same_type_,
                           is_same_properties_names_);

OB_DEF_DESERIALIZE(ObTableBatchOperation,)
{
  int ret = OB_SUCCESS;
  UNF_UNUSED_DES;
  reset();
  int64_t batch_size = 0;
  OB_UNIS_DECODE(batch_size);
  ObITableEntity *entity = NULL;
  ObTableOperation table_operation;
  for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
    if (OB_ISNULL(entity_factory_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("entity factory is null", KR(ret));
    } else if (NULL == (entity = entity_factory_->alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc", KR(ret));
    } else {
      table_operation.set_entity(*entity);
      OB_UNIS_DECODE(table_operation);
      if (OB_SUCC(ret)) {
        if (OB_FAIL(table_operations_.push_back(table_operation))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
    }
  } // end for
  LST_DO_CODE(OB_UNIS_DECODE,
              is_readonly_,
              is_same_type_,
              is_same_properties_names_);
  return ret;
}

////////////////////////////////////////////////////////////////
OB_SERIALIZE_MEMBER(ObTableResult, errno_, sqlstate_, msg_);

int ObTableResult::assign(const ObTableResult &other)
{
  errno_ = other.errno_;
  strncpy(sqlstate_, other.sqlstate_, sizeof(sqlstate_));
  strncpy(msg_, other.msg_, sizeof(msg_));
  return OB_SUCCESS;
}

void ObTableResult::set_errno(int err) {
  errno_ = err;
}

////////////////////////////////////////////////////////////////
ObTableOperationResult::ObTableOperationResult()
    :operation_type_(ObTableOperationType::GET),
     entity_(NULL),
     insertup_old_row_(nullptr),
     affected_rows_(0),
     flags_(0)
{}

void ObTableOperationResult::reset()
{
  ObTableResult::reset();
  operation_type_ = ObTableOperationType::GET;
  if (entity_ != nullptr) {
    entity_->reset();
  }
  affected_rows_ = 0;
  flags_ = 0;
  insertup_old_row_ = nullptr;
}

int ObTableOperationResult::get_entity(const ObITableEntity *&entity) const
{
  int ret = OB_SUCCESS;
  if (NULL == entity_) {
    ret = OB_ERR_NULL_VALUE;
  } else {
    entity = entity_;
  }
  return ret;
}

int ObTableOperationResult::get_entity(ObITableEntity *&entity)
{
  int ret = OB_SUCCESS;
  if (NULL == entity_) {
    ret = OB_ERR_NULL_VALUE;
  } else {
    entity = entity_;
  }
  return ret;
}

DEF_TO_STRING(ObTableOperationResult)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(errno),
       K_(operation_type),
       K_(affected_rows),
       K_(flags));
  J_COMMA();
  if (NULL == entity_) {
    J_NAME("entity");
    J_COLON();
    J_NULL();
  } else {
    J_KV("entity", *entity_);
  }
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER((ObTableOperationResult, ObTableResult),
                    operation_type_, *entity_, affected_rows_);

int ObTableOperationResult::deep_copy(common::ObIAllocator &allocator,
                                      ObITableEntityFactory &entity_factory, const ObTableOperationResult &other)
{
  int ret = OB_SUCCESS;
  const ObITableEntity *src_entity = NULL;
  ObITableEntity *dest_entity = NULL;
  if (OB_FAIL(other.get_entity(src_entity))) {
    LOG_WARN("failed to get entity", K(ret));
  } else if (NULL == (dest_entity = entity_factory.alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("no memory", K(ret));
  } else if (OB_FAIL(dest_entity->deep_copy(allocator, *src_entity))) {
    LOG_WARN("failed to copy entity", K(ret));
  } else if (OB_FAIL(ObTableResult::assign(other))) {
    LOG_WARN("failed to copy result", K(ret));
  } else {
    operation_type_ = other.operation_type_;
    entity_ = dest_entity;
    affected_rows_ = other.affected_rows_;
    flags_ = other.flags_;
  }
  return ret;
}

////////////////////////////////////////////////////////////////
OB_DEF_DESERIALIZE(ObTableBatchOperationResult,)
{
  int ret = OB_SUCCESS;
  UNF_UNUSED_DES;
  int64_t batch_size = 0;
  OB_UNIS_DECODE(batch_size);
  ObITableEntity *entity = NULL;
  ObTableOperationResult table_operation_result;
  reset();
  if (NULL == alloc_) {
    // shallow copy properties
    for (int64_t i = 0; OB_SUCCESS == ret && i < batch_size; ++i)
    {
      if (NULL == (entity = entity_factory_->alloc())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        table_operation_result.set_entity(*entity);
        if (OB_FAIL(serialization::decode(buf, data_len, pos, table_operation_result))) {
          LOG_WARN("fail to decode array item", K(ret), K(i), K(batch_size), K(data_len),
                   K(pos), K(table_operation_result));
        } else if (OB_FAIL(push_back(table_operation_result))) {
          LOG_WARN("fail to add item to array", K(ret), K(i), K(batch_size));
        }
      }
    } // end for
  } else {
    // deep copy properties
    for (int64_t i = 0; OB_SUCCESS == ret && i < batch_size; ++i)
    {
      if (NULL == (entity = entity_factory_->alloc())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        entity->set_allocator(alloc_);
        table_operation_result.set_entity(*entity);
        if (OB_FAIL(serialization::decode(buf, data_len, pos, table_operation_result))) {
          LOG_WARN("fail to decode array item", K(ret), K(i), K(batch_size), K(data_len),
                   K(pos), K(table_operation_result));
        } else if (OB_FAIL(push_back(table_operation_result))) {
          LOG_WARN("fail to add item to array", K(ret), K(i), K(batch_size));
        }
      }
    } // end for
  }
  return ret;
}

OB_UNIS_DEF_SERIALIZE((ObTableBatchOperationResult, ObTableBatchOperationResult::BaseType), );

OB_UNIS_DEF_SERIALIZE_SIZE((ObTableBatchOperationResult, ObTableBatchOperationResult::BaseType), );
////////////////////////////////////////////////////////////////
void ObTableQuery::reset()
{
  deserialize_allocator_ = NULL;
  key_ranges_.reset();
  select_columns_.reset();
  filter_string_.reset();
  scan_range_columns_.reset();
  aggregations_.reset();
  limit_ = -1;  // no limit
  offset_ = 0;
  scan_order_ = ObQueryFlag::Forward;
  index_name_.reset();
  batch_size_ = -1;
  max_result_size_ = -1;
  htable_filter_.reset();
}

bool ObTableQuery::is_valid() const
{
  bool valid = false;
  if (OB_NOT_NULL(ob_params_.ob_params_) && ob_params_.ob_params_->get_param_type() == ParamType::HBase) {
    valid = key_ranges_.count() > 0;
  } else {
    valid = (limit_ == -1 || limit_ > 0) && (offset_ >= 0) && key_ranges_.count() > 0;
  }
  return valid;
}

int ObTableQuery::add_scan_range(common::ObNewRange &scan_range)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(key_ranges_.push_back(scan_range))) {
    LOG_WARN("failed to add rowkey range", K(ret), K(scan_range));
  }
  return ret;
}

int ObTableQuery::set_scan_order(common::ObQueryFlag::ScanOrder scan_order)
{
  int ret = OB_SUCCESS;
  if (scan_order != ObQueryFlag::Forward
      && scan_order != ObQueryFlag::Reverse) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid scan order", K(ret), K(scan_order));
  } else {
    scan_order_ = scan_order;
  }
  return ret;
}

int ObTableQuery::add_select_column(const ObString &column)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(select_columns_.push_back(column))) {
    LOG_WARN("failed to add select column", K(ret), K(column));
  }
  return ret;
}

int ObTableQuery::set_scan_index(const ObString &index_name)
{
  index_name_ = index_name;
  return OB_SUCCESS;
}

int ObTableQuery::set_filter(const ObString &filter)
{
  filter_string_ = filter;
  return OB_SUCCESS;
}

int ObTableQuery::set_limit(int32_t limit)
{
  int ret = OB_SUCCESS;
  if (limit < -1 || 0 == limit) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("limit cannot be negative or zero", K(ret), K(limit));
  } else {
    limit_ = limit;
  }
  return ret;
}

int ObTableQuery::set_offset(int32_t offset)
{
  int ret = OB_SUCCESS;
  if (offset < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("offset cannot be negative", K(ret), K(offset));
  } else {
    offset_ = offset;
  }
  return ret;
}

int ObTableQuery::set_batch(int32_t batch_size)
{
  int ret = OB_SUCCESS;
  if (batch_size == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("batch_size cannot be zero", K(ret), K(batch_size));
  } else {
    batch_size_ = batch_size;
  }
  return ret;
}

int ObTableQuery::set_max_result_size(int64_t max_result_size)
{
  int ret = OB_SUCCESS;
  if (max_result_size == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("max_result_size cannot be zero", K(ret), K(max_result_size));
  } else {
    max_result_size_ = max_result_size;
  }
  return ret;
}

uint64_t ObTableQuery::get_checksum() const
{
  uint64_t checksum = 0;
  const int64_t range_count = get_range_count();
  checksum = ob_crc64(checksum, &range_count, sizeof(range_count));
  for (int64_t i = 0; i < select_columns_.count(); ++i) {
    const ObString &cur_column = select_columns_.at(i);
    checksum = ob_crc64(checksum, cur_column.ptr(), cur_column.length());
  }
  checksum = ob_crc64(checksum, filter_string_.ptr(), filter_string_.length());
  checksum = ob_crc64(checksum, &limit_, sizeof(limit_));
  checksum = ob_crc64(checksum, &offset_, sizeof(offset_));
  checksum = ob_crc64(checksum, &scan_order_, sizeof(scan_order_));
  checksum = ob_crc64(checksum, index_name_.ptr(), index_name_.length());
  checksum = ob_crc64(checksum, &batch_size_, sizeof(batch_size_));
  checksum = ob_crc64(checksum, &max_result_size_, sizeof(max_result_size_));
  if (htable_filter_.is_valid()) {
    const uint64_t htable_filter_checksum = htable_filter_.get_checksum();
    checksum = ob_crc64(checksum, &htable_filter_checksum, sizeof(htable_filter_checksum));
  }
  return checksum;
}

int ObTableQuery::deep_copy(ObIAllocator &allocator, ObTableQuery &dst) const
{
  int ret = OB_SUCCESS;
  if (this == &dst) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("attempted to deep copy the object to itself", K(ret));
  }
  // key_ranges_
  for (int64_t i = 0; OB_SUCC(ret) && i < key_ranges_.count(); i++) {
    const ObNewRange &src_range = key_ranges_.at(i);
    ObNewRange dst_range;
    if (OB_FAIL(deep_copy_range(allocator, src_range, dst_range))) {
      LOG_WARN("fail tp deep copy range", K(ret));
    } else if (OB_FAIL(dst.key_ranges_.push_back(dst_range))) {
      LOG_WARN("fail to push back new range", K(ret));
    }
  }

  // select_columns_
  for (int64_t i = 0; OB_SUCC(ret) && i < select_columns_.count(); i++) {
    ObString select_column;
    if (OB_FAIL(ob_write_string(allocator, select_columns_.at(i), select_column))) {
      LOG_WARN("fail to deep copy select column", K(ret), K(select_columns_.at(i)));
    } else if (OB_FAIL(dst.select_columns_.push_back(select_column))) {
      LOG_WARN("fail to push back select column", K(ret), K(select_column));
    }
  }

  // scan_range_columns_
  for (int64_t i = 0; OB_SUCC(ret) && i < scan_range_columns_.count(); i++) {
    ObString range_column_name;
    if (OB_FAIL(ob_write_string(allocator, scan_range_columns_.at(i), range_column_name))) {
      LOG_WARN("fail to deep copy range column name", K(ret), K(scan_range_columns_.at(i)));
    } else if (OB_FAIL(dst.scan_range_columns_.push_back(range_column_name))) {
      LOG_WARN("fail to push back range column name", K(ret), K(range_column_name));
    }
  }

  // aggregations_
  for (int64_t i = 0; OB_SUCC(ret) && i < aggregations_.count(); i++) {
    ObTableAggregation agg;
    if (OB_FAIL(aggregations_.at(i).deep_copy(allocator, agg))) {
      LOG_WARN("fail to deep copy aggregation", K(ret), K(aggregations_.at(i)));
    } else if (OB_FAIL(dst.aggregations_.push_back(agg))) {
      LOG_WARN("fail to push back aggregation", K(ret), K(agg));
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(ob_write_string(allocator, filter_string_, dst.filter_string_))) {
    LOG_WARN("fail to deep copy filter string", K(ret), K_(filter_string));
  } else if (OB_FAIL(ob_write_string(allocator, index_name_, dst.index_name_))) {
    LOG_WARN("fail to deep copy index name", K(ret), K_(index_name));
  } else if (OB_FAIL(htable_filter_.deep_copy(allocator, dst.htable_filter_))) {
    LOG_WARN("fail to deep copy htable filter", K(ret), K_(htable_filter));
  } else if (OB_FAIL(ob_params_.deep_copy(allocator, dst.ob_params_))){
    LOG_WARN("fail to deep copy htable filter", K(ret), K_(ob_params));
  } else {
    dst.deserialize_allocator_ = deserialize_allocator_;
    dst.limit_ = limit_;
    dst.offset_ = offset_;
    dst.scan_order_ = scan_order_;
    dst.batch_size_ = batch_size_;
    dst.max_result_size_ = max_result_size_;
  }
  return ret;
}

int ObTableQuery::add_aggregation(ObTableAggregation &aggregation)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(aggregations_.push_back(aggregation))) {
    LOG_WARN("failed to add rowkey range", K(ret), K(aggregation));
  }
  return ret;
}

// statement is: "query $table_name $column_0, $column_1, ..., $column_n range:$column_0, $column_1, ..., $column_n index:$index_name"
int64_t ObTableQuery::get_stmt_length(const ObString &table_name) const
{
  int64_t len = 0;
  ObString tmp_table_name = table_name.empty() ? ObString::make_string("(null)") : table_name;
  ObString tmp_index_name = index_name_.empty() ? ObString::make_string("(null)") : index_name_;

  len += strlen("query"); // "query"
  len += 1; // blank
  len += tmp_table_name.length(); // "table_name"
  len += 1; // blank

  // select column
  int64_t N = select_columns_.count();
  for (int64_t index = 0; index < N - 1; ++index) {
    len += select_columns_.at(index).length(); // column_0
    len += 2; // "\"\"" double quote
    len += 2; // ", "
  }
  if (0 < N) {
    len += select_columns_.at(N - 1).length(); // column_n
    len += 2; // "\"\"" double quote
  }
  len += 1; // blank

  len += strlen("range:"); // "range:"
  N = scan_range_columns_.count();
  for (int64_t index = 0; index < N - 1; ++index) {
    len += scan_range_columns_.at(index).length(); // column_0
    len += 2; // "\"\"" double quote
    len += 2; // ", "
  }
  if (0 < N) {
    len += scan_range_columns_.at(N - 1).length(); // column_n
    len += 2; // "\"\"" double quote
  }
  len += 1; // blank

  // index
  len += strlen("index:"); // "index:"
  len += tmp_index_name.length();

  return len;
}

// statement is: "query $table_name $column_0, $column_1, ..., $column_n range:$column_0, $column_1, ..., $column_n index:$index_name"
// filter will add later
int ObTableQuery::generate_stmt(const ObString &table_name,
                                char *buf,
                                int64_t buf_len,
                                int64_t &pos) const
{
  int ret = OB_SUCCESS;
  ObString tmp_table_name = table_name.empty() ? ObString::make_string("(null)") : table_name;
  ObString tmp_index_name = index_name_.empty() ? ObString::make_string("(null)") : index_name_;

  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is bull", KR(ret));
  } else {
    // table name
    int64_t n = snprintf(buf + pos, buf_len - pos, "query "); // "query "
    if (n < 0 || n > buf_len - pos) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("snprintf error or buf not enough", KR(ret), K(n), K(pos), K(buf_len));
    } else {
      pos += n;
      strncat(buf + pos, tmp_table_name.ptr(), tmp_table_name.length());
      pos += tmp_table_name.length();
      int64_t n = snprintf(buf + pos, buf_len - pos, " "); // " "
      if (n < 0 || n > buf_len - pos) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("snprintf error or buf not enough", KR(ret), K(n), K(pos), K(buf_len));
      } else {
        pos += n;
        // select column
        int64_t N = select_columns_.count();
        for (int64_t index = 0; index < N - 1; ++index) {
          BUF_PRINTO(select_columns_.at(index)); // pos will change in BUF_PRINTO
          J_COMMA(); // ", "
        }
        if (0 < N) {
          BUF_PRINTO(select_columns_.at(N - 1));
        }
        // range column
        n = snprintf(buf + pos, buf_len - pos, " range:");
        if (n < 0 || n > buf_len - pos) {
          ret = OB_BUF_NOT_ENOUGH;
          LOG_WARN("snprintf error or buf not enough", KR(ret), K(n), K(pos), K(buf_len));
        } else {
          pos += n;
          N = scan_range_columns_.count();
          for (int64_t index = 0; index < N - 1; ++index) {
            BUF_PRINTO(scan_range_columns_.at(index)); // pos will change in BUF_PRINTO
            J_COMMA(); // ", "
          }
          if (0 < N) {
            BUF_PRINTO(scan_range_columns_.at(N - 1));
          }
          // index
          n = snprintf(buf + pos, buf_len - pos, " index:");
          if (n < 0 || n > buf_len - pos) {
            ret = OB_BUF_NOT_ENOUGH;
            LOG_WARN("snprintf error or buf not enough", KR(ret), K(n), K(pos), K(buf_len));
          } else {
            pos += n;
            strncat(buf + pos, tmp_index_name.ptr(), tmp_index_name.length());
            pos += tmp_index_name.length();
          }
        }
      }
    }
  }

  return ret;
}

OB_UNIS_DEF_SERIALIZE(ObTableQuery,
                      key_ranges_,
                      select_columns_,
                      filter_string_,
                      limit_,
                      offset_,
                      scan_order_,
                      index_name_,
                      batch_size_,
                      max_result_size_,
                      htable_filter_,
                      scan_range_columns_,
                      aggregations_,
                      ob_params_);

OB_UNIS_DEF_SERIALIZE_SIZE(ObTableQuery,
                           key_ranges_,
                           select_columns_,
                           filter_string_,
                           limit_,
                           offset_,
                           scan_order_,
                           index_name_,
                           batch_size_,
                           max_result_size_,
                           htable_filter_,
                           scan_range_columns_,
                           aggregations_,
                           ob_params_);

OB_DEF_DESERIALIZE(ObTableQuery,)
{
  int ret = OB_SUCCESS;
  UNF_UNUSED_DES;
  if (OB_SUCC(ret) && pos < data_len) {
    int64_t count = 0;
    key_ranges_.reset();
    if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &count))) {
      LOG_WARN("fail to decode key ranges count", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < count; i ++) {
      if (OB_ISNULL(deserialize_allocator_)) {
        ret = OB_NOT_INIT;
        LOG_WARN("deserialize allocator is NULL", K(ret));
      } else {
        ObObj array[OB_MAX_ROWKEY_COLUMN_NUMBER * 2];
        ObNewRange copy_range;
        ObNewRange key_range;
        copy_range.start_key_.assign(array, OB_MAX_ROWKEY_COLUMN_NUMBER);
        copy_range.end_key_.assign(array + OB_MAX_ROWKEY_COLUMN_NUMBER, OB_MAX_ROWKEY_COLUMN_NUMBER);
        if (OB_FAIL(copy_range.deserialize(buf, data_len, pos))) {
          LOG_WARN("fail to deserialize range", K(ret));
        } else if (OB_FAIL(common::deep_copy_range(*deserialize_allocator_, copy_range, key_range))) {
          LOG_WARN("fail to deep copy range", K(ret));
        } else if (OB_FAIL(key_ranges_.push_back(key_range))) {
          LOG_WARN("fail to add key range to array", K(ret));
        } else {
          ob_params_.set_allocator(deserialize_allocator_);
        }
      }
    }
  }
  if (OB_SUCC(ret) && pos < data_len) {
    LST_DO_CODE(OB_UNIS_DECODE,
                select_columns_,
                filter_string_,
                limit_,
                offset_,
                scan_order_,
                index_name_,
                batch_size_,
                max_result_size_,
                htable_filter_,
                scan_range_columns_,
                aggregations_,
                ob_params_
                );
  }
  return ret;
}

void ObTableSingleOpQuery::reset()
{
  scan_range_cols_bp_.clear();
  ObTableQuery::reset();
}

OB_DEF_SERIALIZE(ObTableSingleOpQuery)
{
  int ret = OB_SUCCESS;
  // ensure all_rowkey_names_ is not null!
  if (!has_dictionary()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("has not valid all_names", K(ret));
  } else if (!scan_range_cols_bp_.has_init()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan_range_cols_bp is not init by rowkey_names", K(ret), K_(scan_range_cols_bp),
      K_(scan_range_columns), KPC(all_rowkey_names_));
  } else {
    OB_UNIS_ENCODE(index_name_);
    if (OB_SUCC(ret)) {
      // construct scan_range_cols_bp_
      // msg_format: index_name | scan_cols_count | scan_range_cols_bp | key_ranges | filter_string
      // properties_count | properties_values encode row_names_bp
      const int64_t scan_cols_count = scan_range_cols_bp_.get_valid_bits_num();
      OB_UNIS_ENCODE(scan_cols_count)
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(scan_range_cols_bp_.serialize(buf, buf_len, pos))) {
        LOG_WARN("failed to encode scan_range_cols_bp_", K(ret), K(scan_cols_count), K(pos));
      } else {
        int64_t key_range_size = key_ranges_.count();
        OB_UNIS_ENCODE(key_range_size);
        for (int64_t i = 0; OB_SUCC(ret) && i < key_range_size; i++) {
          if (OB_FAIL(ObTableSerialUtil::serialize(buf, buf_len, pos, key_ranges_[i]))) {
            LOG_WARN("fail to deserialize range", K(ret), K(i));
          }
        }
        OB_UNIS_ENCODE(filter_string_);
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableSingleOpQuery)
{
  int64_t len = 0;
  int ret = OB_SUCCESS;
  if (!has_dictionary() && get_scan_range_columns_count() > 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("has not valid all_names", K(ret));
  } else if (!scan_range_cols_bp_.has_init()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan_range_cols_bp_ is not init by rowkey_names", K(ret), K_(scan_range_cols_bp),
      K_(scan_range_columns), KPC(all_rowkey_names_));
  } else {
    LST_DO_CODE(OB_UNIS_ADD_LEN,
                index_name_);

    if (OB_SUCC(ret)) {
      // scan_range_cols_bp_ size
      int64_t cols_bit_count = get_scan_range_columns_count() == 0 ? 0 : all_rowkey_names_->count();
      OB_UNIS_ADD_LEN(cols_bit_count);
      if (OB_SUCC(ret)) {
        len += scan_range_cols_bp_.get_serialize_size();
        int64_t key_range_size = key_ranges_.count();
        OB_UNIS_ADD_LEN(key_range_size);
        for (int64_t i = 0; i < key_range_size; i++) {
          len += ObTableSerialUtil::get_serialize_size(key_ranges_[i]);
        }
        OB_UNIS_ADD_LEN(filter_string_);
      }
    }
  }

  return len;
}

OB_DEF_DESERIALIZE(ObTableSingleOpQuery, )
{
  int ret = OB_SUCCESS;
  if (!has_dictionary()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTableSingleOpQuery has not valid all_names", K(ret));
  } else {
    LST_DO_CODE(OB_UNIS_DECODE, index_name_);
    // decode scan_range_cols_bp_
    if (OB_SUCC(ret) && pos < data_len) {
      int64_t cols_bit_count = -1;
      OB_UNIS_DECODE(cols_bit_count);

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(scan_range_cols_bp_.init_bitmap_size(cols_bit_count))) {
        LOG_WARN("failed to init bitmap size", K(ret), K(cols_bit_count));
      } else if (OB_FAIL(scan_range_cols_bp_.deserialize(buf, data_len, pos))) {
        LOG_WARN("failed to decode scan_range_cols_bp", K(ret), K_(scan_range_cols_bp), K(pos));
      } else if (OB_FAIL(ObTableSingleOpEntity::construct_column_names(
                     scan_range_cols_bp_, *all_rowkey_names_, scan_range_columns_))) {
        LOG_WARN("failed to construct scan_range_columns", K(ret), K_(scan_range_cols_bp),
          KPC(all_rowkey_names_), K_(scan_range_columns));
      }
    }

    if (OB_SUCC(ret) && pos < data_len) {
      int64_t count = 0;
      if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &count))) {
        LOG_WARN("fail to decode key ranges count", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
        if (OB_ISNULL(deserialize_allocator_)) {
          ret = OB_NOT_INIT;
          LOG_WARN("deserialize allocator is NULL", K(ret));
        } else {
          ObObj array[OB_MAX_ROWKEY_COLUMN_NUMBER * 2];
          ObNewRange copy_range;
          ObNewRange key_range;
          copy_range.start_key_.assign(array, OB_MAX_ROWKEY_COLUMN_NUMBER);
          copy_range.end_key_.assign(array + OB_MAX_ROWKEY_COLUMN_NUMBER, OB_MAX_ROWKEY_COLUMN_NUMBER);
          if (OB_FAIL(ObTableSerialUtil::deserialize(buf, data_len, pos, copy_range))) {
            LOG_WARN("fail to deserialize range", K(ret));
          } else if (OB_FAIL(common::deep_copy_range(*deserialize_allocator_, copy_range, key_range))) {
            LOG_WARN("fail to deep copy range", K(ret));
          } else if (OB_FAIL(key_ranges_.push_back(key_range))) {
            LOG_WARN("fail to add key range to array", K(ret));
          } else {
            ob_params_.set_allocator(deserialize_allocator_);
          }
        }
      }
    }
    if (OB_SUCC(ret) && pos < data_len) {
      LST_DO_CODE(OB_UNIS_DECODE, filter_string_);
    }
    if (OB_SUCC(ret) && pos < data_len) {
      LST_DO_CODE(OB_UNIS_DECODE, select_columns_);
    }
    if (OB_SUCC(ret) && pos < data_len) {
      LST_DO_CODE(OB_UNIS_DECODE, scan_order_);
    }
    if (OB_SUCC(ret) && pos < data_len) {
      LST_DO_CODE(OB_UNIS_DECODE, htable_filter_);
    }
    if (OB_SUCC(ret) && pos < data_len) {
      LST_DO_CODE(OB_UNIS_DECODE, ob_params_);
    }
  }

  return ret;
}

////////////////////////////////////////////////////////////////
ObTableEntityIterator::~ObTableEntityIterator()
{}
////////////////////////////////////////////////////////////////
const char* const ObHTableConstants::ROWKEY_CNAME = "K";
const char* const ObHTableConstants::CQ_CNAME = "Q";
const char* const ObHTableConstants::VERSION_CNAME = "T";
const char* const ObHTableConstants::VALUE_CNAME = "V";
const char* const ObHTableConstants::TTL_CNAME = "TTL";

const ObString ObHTableConstants::ROWKEY_CNAME_STR = ObString::make_string(ROWKEY_CNAME);
const ObString ObHTableConstants::CQ_CNAME_STR = ObString::make_string(CQ_CNAME);
const ObString ObHTableConstants::VERSION_CNAME_STR = ObString::make_string(VERSION_CNAME);
const ObString ObHTableConstants::VALUE_CNAME_STR = ObString::make_string(VALUE_CNAME);
const ObString ObHTableConstants::TTL_CNAME_STR = ObString::make_string(TTL_CNAME);

ObHTableFilter::ObHTableFilter()
    :is_valid_(false),
     select_column_qualifier_(),
     min_stamp_(ObHTableConstants::INITIAL_MIN_STAMP),
     max_stamp_(ObHTableConstants::INITIAL_MAX_STAMP),
     max_versions_(1),
     limit_per_row_per_cf_(-1),
     offset_per_row_per_cf_(0),
     filter_string_()
{}

void ObHTableFilter::reset()
{
  is_valid_ = false;
  select_column_qualifier_.reset();
  min_stamp_ = ObHTableConstants::INITIAL_MIN_STAMP;
  max_stamp_ = ObHTableConstants::INITIAL_MAX_STAMP;
  max_versions_ = 1;
  limit_per_row_per_cf_ = -1;
  offset_per_row_per_cf_ = 0;
  filter_string_.reset();

}

int ObHTableFilter::add_column(const ObString &qualifier)
{
  int ret = OB_SUCCESS;
  const int64_t N = select_column_qualifier_.count();
  for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
  {
    if (0 == select_column_qualifier_.at(i).case_compare(qualifier)) {
      ret = OB_ERR_COLUMN_DUPLICATE;
      LOG_WARN("column already exists", K(ret), K(qualifier));
      break;
    }
  } // end for
  if (OB_SUCC(ret)) {
    if (OB_FAIL(select_column_qualifier_.push_back(qualifier))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  return ret;
}

int ObHTableFilter::set_time_range(int64_t min_stamp, int64_t max_stamp)
{
  int ret = OB_SUCCESS;
  if (min_stamp >= max_stamp || min_stamp_ < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid time range", K(ret), K(min_stamp), K(max_stamp));
  } else {
    min_stamp_ = min_stamp;
    max_stamp_ = max_stamp;
  }
  return ret;
}

int ObHTableFilter::set_max_versions(int32_t versions)
{
  int ret = OB_SUCCESS;
  if (versions <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid max versions", K(ret), K(versions));
  } else {
    max_versions_ = versions;
  }
  return ret;
}

int ObHTableFilter::set_max_results_per_column_family(int32_t limit)
{
  int ret = OB_SUCCESS;
  if (limit < -1 || 0 == limit) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("limit cannot be negative or zero", K(ret), K(limit));
  } else {
    limit_per_row_per_cf_ = limit;
  }
  return ret;
}

int ObHTableFilter::set_row_offset_per_column_family(int32_t offset)
{
  int ret = OB_SUCCESS;
  if (offset < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("offset cannot be negative", K(ret), K(offset));
  } else {
    offset_per_row_per_cf_ = offset;
  }
  return ret;
}

int ObHTableFilter::set_filter(const ObString &filter)
{
  filter_string_ = filter;
  return OB_SUCCESS;
}

uint64_t ObHTableFilter::get_checksum() const
{
  uint64_t checksum = 0;
  for (int64_t i = 0; i < select_column_qualifier_.count(); ++i) {
    const ObString &cur_qualifier = select_column_qualifier_.at(i);
    checksum = ob_crc64(checksum, cur_qualifier.ptr(), cur_qualifier.length());
  }
  checksum = ob_crc64(checksum, &min_stamp_, sizeof(min_stamp_));
  checksum = ob_crc64(checksum, &max_stamp_, sizeof(max_stamp_));
  checksum = ob_crc64(checksum, &max_versions_, sizeof(max_versions_));
  checksum = ob_crc64(checksum, &limit_per_row_per_cf_, sizeof(limit_per_row_per_cf_));
  checksum = ob_crc64(checksum, &offset_per_row_per_cf_, sizeof(offset_per_row_per_cf_));
  checksum = ob_crc64(checksum, filter_string_.ptr(), filter_string_.length());
  return checksum;
}

int ObHTableFilter::deep_copy(ObIAllocator &allocator, ObHTableFilter &dst) const
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < select_column_qualifier_.count(); i++) {
    ObString select_column;
    if (OB_FAIL(ob_write_string(allocator, select_column_qualifier_.at(i), select_column))) {
      LOG_WARN("fail to deep copy select column qualifier", K(ret), K(select_column_qualifier_.at(i)));
    } else if (OB_FAIL(dst.select_column_qualifier_.push_back(select_column))) {
      LOG_WARN("fail to push back select column qualifier", K(ret), K(select_column));
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(ob_write_string(allocator, filter_string_, dst.filter_string_))) {
    LOG_WARN("fail to write filter string", K(ret), K_(filter_string));
  } else {
    dst.is_valid_ = is_valid_;
    dst.min_stamp_ = min_stamp_;
    dst.max_stamp_ = max_stamp_;
    dst.max_versions_ = max_versions_;
    dst.limit_per_row_per_cf_ = limit_per_row_per_cf_;
    dst.offset_per_row_per_cf_ = offset_per_row_per_cf_;
  }

  return ret;
}

// If valid_ is true, serialize the members. Otherwise, nothing/dummy is serialized.
OB_SERIALIZE_MEMBER_IF(ObHTableFilter,
                       (true == is_valid_),
                       is_valid_,
                       select_column_qualifier_,
                       min_stamp_,
                       max_stamp_,
                       max_versions_,
                       limit_per_row_per_cf_,
                       offset_per_row_per_cf_,
                       filter_string_);

////////////////////////////////////////////////////////////////
int ObTableQueryIterableResultBase::append_family(ObNewRow &row, ObString family_name)
{
  int ret = OB_SUCCESS;
  // qualifier obj
  ObObj &qualifier = row.get_cell(ObHTableConstants::COL_IDX_Q);
  // get length and create buffer
  int64_t sep_pos = family_name.length() + 1;
  int64_t buf_size = sep_pos + qualifier.get_data_length();
  char *buf = nullptr;
  if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc family and qualifier buffer", K(ret));
  } else {
    MEMCPY(buf, family_name.ptr(), family_name.length());
    buf[family_name.length()] = '\0';
    MEMCPY(buf + sep_pos, qualifier.get_data_ptr(), qualifier.get_data_length());
    ObString family_qualifier(buf_size, buf);
    qualifier.set_varbinary(family_qualifier);
  }
  return ret;
}

int ObTableQueryIterableResultBase::transform_lob_cell(ObNewRow &row, const int64_t lob_storage_count)
{
  int ret = OB_SUCCESS;
  // check properties count and row count
  const int64_t N = row.get_count();
  ObObj *lob_cells = nullptr;
  if (OB_ISNULL(lob_cells = static_cast<ObObj*>(allocator_.alloc(sizeof(ObObj) * lob_storage_count)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc cells buffer", K(ret), K(lob_storage_count));
  }
  for (int i = 0, lob_cell_idx = 0; OB_SUCC(ret) && i < N; ++i) {
    const ObObj &cell = row.get_cell(i);
    ObObjType type = cell.get_type();
    if (is_lob_storage(type)) {
      ObString real_data;
      if (cell.has_lob_header()) {
        if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(&allocator_, cell, real_data))) {
          LOG_WARN("fail to read real string date", K(ret), K(cell));
        } else if (lob_cell_idx >= lob_storage_count) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected index count", K(ret), K(lob_cell_idx), K(lob_storage_count));
        } else {
          lob_cells[lob_cell_idx].set_lob_value(type, real_data.ptr(), real_data.length());
          lob_cells[lob_cell_idx].set_collation_type(cell.get_collation_type());
          row.get_cell(i) = lob_cells[lob_cell_idx]; // switch lob cell
          lob_cell_idx++;
        }
      }
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////
ObTableQueryResult::ObTableQueryResult()
    :row_count_(0),
     allocator_(ObModIds::TABLE_PROC, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
     prop_name_allocator_(ObModIds::TABLE_PROC, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
     fixed_result_size_(0),
     curr_idx_(0)
{
}

void ObTableQueryResult::reset_except_property()
{
  row_count_ = 0;
  buf_.reset();
  allocator_.reset();
  fixed_result_size_ = 0;
  curr_idx_ = 0;
  curr_entity_.reset();
}

void ObTableQueryResult::reset()
{
  properties_names_.reset();
  reset_except_property();
  prop_name_allocator_.reset();
}

void ObTableQueryResult::rewind()
{
  curr_idx_ = 0;
  buf_.get_position() = 0;
}

int ObTableQueryResult::get_htable_all_entity(ObIArray<ObITableEntity*> &entities)
{
  int ret = OB_SUCCESS;
  curr_idx_ = 0;
  buf_.free();
  while (OB_SUCC(ret) && curr_idx_ < row_count_) {
    ObITableEntity *entity = nullptr;
    if (OB_ISNULL(entity = OB_NEWx(ObTableEntity, &allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc entity", K(ret));
    } else {
      ObObj value;
      const int64_t N = properties_names_.count();
      for (int i = 0; OB_SUCC(ret) && i < N; ++i) {
        if (OB_FAIL(value.deserialize(buf_.get_data(), buf_.get_capacity(), buf_.get_position()))) {
          LOG_WARN("failed to deserialize obj", K(ret), K_(buf));
        } else if (i < ObHTableConstants::HTABLE_ROWKEY_SIZE) {
          if (OB_FAIL(entity->set_rowkey_value(i, value))) {
            LOG_WARN("failed to set entity rowkey value", K(ret), K(i), K(value));
          }
        } else if (OB_FAIL(entity->set_property(properties_names_.at(i), value))) {
          LOG_WARN("failed to set entity property", K(ret), K(i), K(value));
        }
      }
    }
    if (OB_SUCC(ret)) {
      curr_idx_++;
      entities.push_back(entity);
    }
  }
  return ret;
}

int ObTableQueryResult::get_next_entity(const ObITableEntity *&entity)
{
  int ret = OB_SUCCESS;
  if (0 >= properties_names_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid properties_names", K(ret));
  } else if (curr_idx_ >= row_count_) {
    ret = OB_ITER_END;
  } else {
    curr_entity_.reset();
    ObObj value;
    const int64_t N = properties_names_.count();
    for (int i = 0; OB_SUCCESS == ret && i < N; ++i)
    {
      if (OB_FAIL(value.deserialize(buf_.get_data(), buf_.get_capacity(), buf_.get_position()))) {
        LOG_WARN("failed to deserialize obj", K(ret), K_(buf));
      } else if (OB_FAIL(curr_entity_.set_property(properties_names_.at(i), value))) {
        LOG_WARN("failed to set entity property", K(ret), K(i), K(value));
      }
    } // end for
    if (OB_SUCC(ret)) {
      entity = &curr_entity_;
      ++curr_idx_;
    }
  }
  return ret;
}

int ObTableQueryResult::get_first_row(common::ObNewRow &row) const
{
  int ret = OB_SUCCESS;
  if (row.is_invalid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid row object", K(ret));
  } else if (0 >= row_count_) {
    ret = OB_ITER_END;
  } else {
    const char *databuf = buf_.get_data();
    const int64_t datalen = buf_.get_position();
    int64_t pos = 0;
    const int64_t N = row.count_;
    for (int i = 0; OB_SUCCESS == ret && i < N; ++i)
    {
      if (OB_FAIL(row.cells_[i].deserialize(databuf, datalen, pos))) {
        LOG_WARN("failed to deserialize obj", K(ret), K(datalen), K(pos));
      }
    } // end for
  }
  return ret;
}

int ObTableQueryResult::add_property_name(const ObString &name)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(properties_names_.push_back(name))) {
    LOG_WARN("failed to add name", K(ret), K(name));
  }
  return ret;
}

int ObTableQueryResult::assign_property_names(const ObIArray<ObString> &other)
{
  return properties_names_.assign(other);
}

int ObTableQueryResult::deep_copy_property_names(const ObIArray<ObString> &other)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(properties_names_.prepare_allocate(other.count()))) {
    LOG_WARN("failed to prepare allocate properties names", K(ret), K(other));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < other.count(); i++) {
    if (OB_FAIL(ob_write_string(prop_name_allocator_, other.at(i), properties_names_.at(i)))) {
      LOG_WARN("failed to write string", K(ret), K(other.at(i)));
    }
  }

  return ret;
}

int ObTableQueryResult::append_property_names(const ObIArray<ObString> &property_names)
{
  int ret = OB_SUCCESS;
  int curr_count = properties_names_.count();
  if (OB_FAIL(properties_names_.prepare_allocate(curr_count + property_names.count()))) {
    LOG_WARN("failed to prepare allocate properties names", K(ret), K(property_names));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < property_names.count(); i++) {
    if (OB_FAIL(ob_write_string(prop_name_allocator_, property_names.at(i), properties_names_.at(curr_count + i)))) {
      LOG_WARN("failed to write string", K(ret), K(property_names.at(i)));
    }
  }

  return ret;
}

int ObTableQueryResult::alloc_buf_if_need(const int64_t need_size)
{
  int ret = OB_SUCCESS;
  if (need_size <= 0 || need_size > get_max_buf_block_size()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(need_size), LITERAL_K(get_max_buf_block_size()));
  } else if (NULL == buf_.get_data()) { // first alloc
    int64_t actual_size = 0;
    if (need_size <= DEFAULT_BUF_BLOCK_SIZE) {
      actual_size = DEFAULT_BUF_BLOCK_SIZE;
    } else {
      actual_size = need_size;
    }
    char *tmp_buf = static_cast<char*>(allocator_.alloc(actual_size));
    if (NULL == tmp_buf) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("no memory", K(ret), K(actual_size));
    } else {
      buf_.set_data(tmp_buf, actual_size);
    }
  } else if (buf_.get_remain() < need_size) {
    if (need_size + buf_.get_position() > get_max_buf_block_size()) { // check max buf size when expand buf
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("will exceed max buf need_size", K(ret), K(need_size), K(buf_.get_position()), LITERAL_K(get_max_buf_block_size()));
    } else {
      int64_t actual_size = MAX(need_size + buf_.get_position(), 2 * buf_.get_capacity());
      actual_size = MIN(actual_size, get_max_buf_block_size());
      char *tmp_buf = static_cast<char*>(allocator_.alloc(actual_size));
      if (NULL == tmp_buf) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("no memory", K(ret), K(actual_size));
      } else {
        const int64_t old_buf_size = buf_.get_position();
        MEMCPY(tmp_buf, buf_.get_data(), old_buf_size);
        buf_.set_data(tmp_buf, actual_size);
        buf_.get_position() = old_buf_size;
      }
    }

  }
  return ret;
}

int ObTableQueryResult::process_lob_storage(ObNewRow &new_row) {
  int ret = OB_SUCCESS;
  ObObj *lob_cells = nullptr;
  const int64_t lob_storage_count = get_lob_storage_count(new_row);
  if (lob_storage_count != 0) {
    if (OB_ISNULL(lob_cells = static_cast<ObObj*>(allocator_.alloc(sizeof(ObObj) * lob_storage_count)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc cells buffer", K(ret), K(lob_storage_count));
    }
    for (int i = 0, lob_cell_idx = 0; OB_SUCC(ret) && i < new_row.get_count(); ++i) {
      const ObObj &cell = new_row.get_cell(i);
      ObObjType type = cell.get_type();
      if (is_lob_storage(type)) {
        ObString real_data;
        if (cell.has_lob_header()) {
          if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(&allocator_, cell, real_data))) {
            LOG_WARN("fail to read real string date", K(ret), K(cell));
          } else if (lob_cell_idx >= lob_storage_count) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected index count", K(ret), K(lob_cell_idx), K(lob_storage_count));
          } else {
            lob_cells[lob_cell_idx].set_lob_value(type, real_data.ptr(), real_data.length());
            lob_cells[lob_cell_idx].set_collation_type(cell.get_collation_type());
            new_row.get_cell(i) = lob_cells[lob_cell_idx]; // switch lob cell
            lob_cell_idx++;
          }
        }
      }
    }
  }
  return ret;
}

int ObTableQueryResult::add_row(const ObNewRow &row)
{
  int ret = OB_SUCCESS;
  // 1. check properties count and row count
  const int64_t N = row.get_count();
  if (0 != properties_names_.count()
      && N != properties_names_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cell count not match with property count", K(ret), K(N),
              "properties_count", properties_names_.count());
  } else {
    // 2. construct new row
    ObNewRow new_row = row;
    if (OB_FAIL(process_lob_storage(new_row))) {
      LOG_WARN("process lob storage fail", K(ret));
    }
    // 3. alloc serialize size
    int64_t size = new_row.get_serialize_size();
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(alloc_buf_if_need(size))) {
      LOG_WARN("failed to alloc buff", K(ret), K(size));
    }

    // 4. serialize
    for (int i = 0; OB_SUCC(ret) && i < N; ++i) {
      if (OB_FAIL(row.get_cell(i).serialize(buf_.get_data(), buf_.get_capacity(), buf_.get_position()))) {
        LOG_WARN("failed to serialize obj", K(ret), K_(buf));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ++row_count_;
  }

  return ret;
}

int ObTableQueryResult::add_all_property(const ObTableQueryResult &other)
{
  int ret = OB_SUCCESS;
  if (0 != properties_names_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid properties which has been initialized", K(ret));
  } else if (OB_FAIL(append(properties_names_, other.properties_names_))) {
    LOG_WARN("failed to append property", K(ret));
  }
  return ret;
}

int ObTableQueryResult::add_all_row(const ObTableQueryResult &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(alloc_buf_if_need(other.buf_.get_position()))) {
  } else if (buf_.get_remain() < other.buf_.get_position()) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    MEMCPY(buf_.get_cur_pos(), other.buf_.get_data(), other.buf_.get_position());
    buf_.get_position() += other.buf_.get_position();
    row_count_ += other.row_count_;
  }
  return ret;
}

int ObTableQueryResult::add_all_row(ObTableQueryIterableResultBase &other)
{
  int ret = OB_SUCCESS;
  ObNewRow row;
  while (OB_SUCC(ret) && OB_SUCC(other.get_row(row))) {
    if (OB_FAIL(add_row(row))) {
      LOG_WARN("fail to add row from ObTableQueryIterableResultBase", K(ret));
    }
  }
  if (ret == OB_ARRAY_OUT_OF_RANGE || ret == OB_ITER_END) {
    ret = OB_SUCCESS;
  } else {
    LOG_WARN("add_all_row get error", K(ret));
  }
  return ret;
}

int64_t ObTableQueryResult::get_result_size()
{
  if (0 >= fixed_result_size_) {
    fixed_result_size_ = properties_names_.get_serialize_size();
    fixed_result_size_ += obrpc::ObRpcPacketHeader::HEADER_SIZE
                          + 8/*appr. row_count*/
                          + 8/*appr. buf_position*/;
  }
  return fixed_result_size_ + buf_.get_position();
}

bool ObTableQueryResult::reach_batch_size_or_result_size(const int32_t batch_count,
                                                         const int64_t max_result_size)
{
  bool reach_size = false;
  if (batch_count > 0 && this->get_row_count() >= batch_count) {
    LOG_DEBUG("[yzfdebug] reach batch limit", K(batch_count));
    reach_size = true;
  } else if (max_result_size > 0 && this->get_result_size() >= max_result_size) {
    LOG_DEBUG("[yzfdebug] reach size limit", K(max_result_size));
    reach_size = true;
  }
  return reach_size;
}

int ObTableQueryResult::add_row(const ObIArray<ObObj> &row)
{
  int ret = OB_SUCCESS;
  int64_t serialize_size = 0;
  const int64_t cnt = row.count();
  for (int i = 0; i < cnt; i++) {
    serialize_size += row.at(i).get_serialize_size();
  }

  ret = alloc_buf_if_need(serialize_size);
  for (int i = 0; OB_SUCC(ret) && i < cnt; i++) {
    if (OB_FAIL(row.at(i).serialize(buf_.get_data(), buf_.get_capacity(), buf_.get_position()))) {
      LOG_WARN("failed to serialize obj", K(ret), K_(buf), K(row.at(i)));
    }
  } // end for
  if (OB_SUCC(ret)) {
    ++row_count_;
  }
  return ret;
}

OB_DEF_SERIALIZE(ObTableQueryResult)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              properties_names_,
              row_count_,
              buf_.get_position());
  if (OB_SUCC(ret)) {
    if (buf_len - pos < buf_.get_position()) {
      LOG_WARN("failed to serialize ObTableQueryResult", K(ret), K(buf_len), K(pos), "datalen", buf_.get_position());
    } else {
      MEMCPY(buf+pos, buf_.get_data(), buf_.get_position());
      pos += buf_.get_position();
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableQueryResult)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              properties_names_,
              row_count_,
              buf_.get_position());
  len += buf_.get_position();
  return len;
}

OB_DEF_DESERIALIZE(ObTableQueryResult)
{
  int ret = OB_SUCCESS;
  allocator_.reset();  // deep copy all
  properties_names_.reset();
  curr_idx_ = 0;
  int64_t databuff_len = 0;
  int64_t properties_count = 0;
  OB_UNIS_DECODE(properties_count);
  if (OB_SUCC(ret)) {
    ObString property_name;
    ObString name_clone;
    for (int64_t i = 0; OB_SUCCESS == ret && i < properties_count; ++i)
    {
      OB_UNIS_DECODE(property_name);
      if (OB_SUCC(ret)) {
        if (OB_FAIL(ob_write_string(allocator_, property_name, name_clone))) {
          LOG_WARN("failed to deep copy string", K(ret), K(property_name));
        } else if (OB_FAIL(properties_names_.push_back(name_clone))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
    } // end for
  }
  LST_DO_CODE(OB_UNIS_DECODE,
              row_count_,
              databuff_len);
  char *buff1 = NULL;
  if (OB_FAIL(ret)) {
  } else if (databuff_len > data_len - pos) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid data", K(ret), K(databuff_len), K(pos), K(data_len));
  } else if (databuff_len == 0) {
    buf_.set_data(NULL, 0);
  } else if (NULL == (buff1 = static_cast<char*>(allocator_.alloc(databuff_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("no memory", K(ret), K(databuff_len));
  } else {
    // deep copy
    MEMCPY(buff1, buf+pos, databuff_len);
    buf_.set_data(buff1, databuff_len);
    pos += databuff_len;
  }
  return ret;
}

////////////////////////////////////////////////////////////////
ObTableQueryIterableResult::ObTableQueryIterableResult()
  : ObTableQueryIterableResultBase(),
    current_(0)
  {
  }


void ObTableQueryIterableResult::reset_except_property()
{
  row_count_ = 0;
  allocator_.reset();
  current_ = 0;
  rows_.reset();
}
int ObTableQueryIterableResult::add_all_row(ObTableQueryDListResult &other)
{
  int ret = OB_SUCCESS;
  ObHTableCellEntity *row = nullptr;
  while (OB_SUCC(ret) && OB_SUCC(other.get_row(row))) {
    ObNewRow copy_row;
    if (OB_FAIL(ob_write_row(allocator_, *row->get_ob_row(), copy_row))) {
      LOG_WARN("fail to copy_row ", K(ret), K(row));
    } else {
      ObString family_name = row->get_family();
      if (OB_FAIL(append_family(copy_row, family_name))) {
        LOG_WARN("fail to append family to row", K(ret), K(copy_row));
      } else if (OB_FAIL(rows_.push_back(copy_row))) {
        LOG_WARN("fail to push_back to rows", K(ret));
      } else {
        row_count_++;
      }
    }
  }
  if (ret == OB_ITER_END) {
    ret = OB_SUCCESS;
  }

  return ret;
}

int ObTableQueryIterableResult::get_row(ObNewRow& row)
{
  int ret = 0;
  if (OB_FAIL(rows_.at(current_, row))) {
    if (ret == OB_ARRAY_OUT_OF_RANGE) {
      rows_.reset();
      allocator_.reset();
      current_ = 0;
    } else {
      LOG_WARN("fail to get row", K(ret), K(rows_));
    }
  } else {
    current_++;
  }
  return ret;
}

int ObTableQueryIterableResult::add_row(const common::ObNewRow &row, ObString family_name)
{
  int ret = OB_SUCCESS;

  // construct new row
  ObNewRow new_row = row;
  const int64_t lob_storage_count = get_lob_storage_count(new_row);
  if (lob_storage_count != 0) {
    if (OB_FAIL(transform_lob_cell(new_row, lob_storage_count))) {
      LOG_WARN("fail to switch lob cell", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ObNewRow copy_row;
    if (OB_FAIL(ob_write_row(allocator_, new_row, copy_row))) {
      LOG_WARN("fail to copy row", K(ret), K(new_row));
    } else if (OB_FAIL(append_family(copy_row, family_name))) {
      LOG_WARN("fail to append family to row", K(ret), K(copy_row));
    } else if (OB_FAIL(rows_.push_back(copy_row))) {
      LOG_WARN("fail to push back to array", K(ret));
    } else {
      row_count_++;
    }
  }

  return ret;
}

int ObTableQueryIterableResult::add_row(const common::ObNewRow &row)
{
  int ret = OB_SUCCESS;

  // construct new row
  ObNewRow new_row = row;
  const int64_t lob_storage_count = get_lob_storage_count(new_row);
  if (lob_storage_count != 0) {
    if (OB_FAIL(transform_lob_cell(new_row, lob_storage_count))) {
      LOG_WARN("fail to switch lob cell", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ObNewRow copy_row;
    if (OB_FAIL(ob_write_row(allocator_, new_row, copy_row))) {
      LOG_WARN("fail to copy row", K(ret), K(new_row));
    } else if (OB_FAIL(rows_.push_back(copy_row))) {
      LOG_WARN("fail to push back to array", K(ret));
    } else {
      row_count_++;
    }
  }

  return ret;
}

int ObTableQueryIterableResult::add_row(const common::ObIArray<ObObj> &row)
{
  int ret = OB_SUCCESS;
  const int64_t cnt = row.count();
  ObNewRow new_row(row.get_data(), cnt);
  if (OB_FAIL(rows_.push_back(std::ref(new_row)))) {
    LOG_WARN("fail to push back to rows", K(ret));
  } else {
    ++row_count_;
  }
  return ret;
}

bool ObTableQueryIterableResult::reach_batch_size_or_result_size(const int32_t batch_count, const int64_t max_result_size)
{
  bool reach_size = false;
  if (batch_count > 0 && this->get_row_count() >= batch_count) {
    LOG_DEBUG("reach batch limit", K(batch_count));
    reach_size = true;
  }
  return reach_size;
}

////////////////////////////////////////////////////////////////
ObTableQueryDListResult::ObTableQueryDListResult()
  : ObTableQueryIterableResultBase(),
    cell_list_()
{
}

ObTableQueryDListResult:: ~ObTableQueryDListResult()
{
}

int ObTableQueryDListResult::add_row(const common::ObNewRow &row, ObString family_name) {
  int ret = OB_SUCCESS;

  // construct new row
  ObNewRow new_row = row;
  const int64_t lob_storage_count = get_lob_storage_count(new_row);
  if (lob_storage_count != 0) {
    if (OB_FAIL(transform_lob_cell(new_row, lob_storage_count))) {
      LOG_WARN("fail to swtich lob cell", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ObNewRow *copy_row = nullptr;
    if (OB_ISNULL(copy_row = OB_NEWx(ObNewRow, (&allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc ObNewRow buffer", K(ret));
    } else if (OB_FAIL(ob_write_row(allocator_, new_row, *copy_row))) {
      LOG_WARN("fail to copy row", K(ret), K(new_row));
    } else {
      ObHTableCellEntity *cell_entity = nullptr;
      if (OB_ISNULL(cell_entity = OB_NEWx(ObHTableCellEntity, (&allocator_), copy_row))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc ObHTableCellEntity buffer", K(ret));
      } else {
        cell_entity->set_family(family_name);
        ObCellNode *new_node = nullptr;
        if (OB_ISNULL(new_node = OB_NEWx(ObCellNode, (&allocator_), cell_entity))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc ObHTableCellEntity buffer", K(ret));
        } else {
          if (cell_list_.add_first(new_node)) {
            ++row_count_;
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to add new_node to cell_list", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObTableQueryDListResult::add_row(const common::ObNewRow &row)
{
  int ret = OB_SUCCESS;
  // construct new row
  ObNewRow new_row = row;
  const int64_t lob_storage_count = get_lob_storage_count(new_row);
  if (lob_storage_count != 0) {
    if (OB_FAIL(transform_lob_cell(new_row, lob_storage_count))) {
      LOG_WARN("fail to swtich lob cell", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ObNewRow *copy_row = nullptr;
    if (OB_ISNULL(copy_row = OB_NEWx(ObNewRow, (&allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc ObNewRow buffer", K(ret));
    } else if (OB_FAIL(ob_write_row(allocator_, new_row, *copy_row))) {
      LOG_WARN("fail to copy row", K(ret), K(new_row));
    } else {
      ObHTableCellEntity *cell_entity = nullptr;
      if (OB_ISNULL(cell_entity = OB_NEWx(ObHTableCellEntity, (&allocator_), copy_row))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc ObHTableCellEntity buffer", K(ret));
      } else {
        ObCellNode *new_node = nullptr;
        if (OB_ISNULL(new_node = OB_NEWx(ObCellNode, (&allocator_), cell_entity))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc ObHTableCellEntity buffer", K(ret));
        } else {
          if (cell_list_.add_first(new_node)) {
            ++row_count_;
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to add new_node to cell_list", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObTableQueryDListResult::add_all_row(ObTableQueryIterableResultBase &other)
{
  int ret = OB_SUCCESS;
  ObNewRow row;
  while (OB_SUCC(ret) && OB_SUCC(other.get_row(row))) {
    ObNewRow *new_row = nullptr;
    // need to deep copy row, because other will free its row when it's empty
    if (OB_ISNULL(new_row = OB_NEWx(ObNewRow, (&allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for ObNewRow", K(ret));
    } else if (OB_FAIL(ob_write_row(allocator_, row, *new_row))) {
      LOG_WARN("fail to copy row", K(ret), K(row));
    } else {
      ObHTableCellEntity *cell_entity = nullptr;
      if (OB_ISNULL(cell_entity = OB_NEWx(ObHTableCellEntity, (&allocator_), new_row))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory for ObHTableCellEntity", K(ret));
      } else {
        ObCellNode *node = nullptr;
        if (OB_ISNULL(node = OB_NEWx(ObCellNode, (&allocator_), cell_entity))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory for ObCellNode", K(ret));
        } else if (OB_FAIL(cell_list_.add_first(node))) {
          LOG_WARN("fail to add to cell_list", K(ret));
        } else {
          ++row_count_;
        }
      }
    }
  }
  if (ret == OB_ITER_END || ret == OB_ARRAY_OUT_OF_RANGE) {
    ret = OB_SUCCESS;
  } else {
    LOG_WARN("add_all_row get error", K(ret));
  }
  return ret;
}

int ObTableQueryDListResult::get_row(ObNewRow &row)
{
  int ret = OB_SUCCESS;
  ObCellNode *node = cell_list_.remove_last();
  if (OB_ISNULL(node)) {
    if (0 != row_count_) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("fail to get row", K(ret), K(cell_list_));
    } else {
      ret = OB_ITER_END;
      cell_list_.reset();
      allocator_.reset();
    }
  } else {
    --row_count_;
    row = *node->get_data()->get_ob_row();
  }
  return ret;
}

int ObTableQueryDListResult::get_row(ObHTableCellEntity *&row) {
  int ret = OB_SUCCESS;
  ObCellNode *node = cell_list_.remove_last();
  if (OB_ISNULL(node)) {
    if (0 != row_count_) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("fail to get row from DListResult", K(ret), K(cell_list_));
    } else {
      ret = OB_ITER_END;
      cell_list_.reset();
      allocator_.reset();
    }
  } else {
    --row_count_;
    row = node->get_data();
  }
  return ret;
}

bool ObTableQueryDListResult::reach_batch_size_or_result_size(const int32_t batch_count, const int64_t max_result_size)
{
  bool reach_size = false;
  if (batch_count > 0 && this->get_row_count() >= batch_count) {
    LOG_DEBUG("reach batch limit", K(batch_count));
    reach_size = true;
  }
  return reach_size;
}

void ObTableQueryDListResult::reset()
{
  cell_list_.reset();
  allocator_.reset();
  row_count_ = 0;
}
////////////////////////////////////////////////////////////////
uint64_t ObTableQueryAndMutate::get_checksum()
{
  uint64_t checksum = 0;
  const uint64_t query_checksum = query_.get_checksum();
  const uint64_t mutation_checksum = mutations_.get_checksum();
  checksum = ob_crc64(checksum, &query_checksum, sizeof(query_checksum));
  checksum = ob_crc64(checksum, &mutation_checksum, sizeof(mutation_checksum));
  checksum = ob_crc64(checksum, &return_affected_entity_, sizeof(return_affected_entity_));
  checksum = ob_crc64(checksum, &flag_, sizeof(flag_));
  return checksum;
}

OB_SERIALIZE_MEMBER(ObTableQueryAndMutate,
                    query_,
                    mutations_,
                    return_affected_entity_,
                    flag_);

OB_SERIALIZE_MEMBER(ObTableQueryAndMutateResult,
                    affected_rows_,
                    affected_entity_);

OB_SERIALIZE_MEMBER((ObTableQueryAsyncResult, ObTableQueryResult),
  is_end_,
  query_session_id_
);

////////////////////////////////////////////////////////////////
OB_SERIALIZE_MEMBER(ObTableApiCredential,
                    cluster_id_,
                    tenant_id_,
                    user_id_,
                    database_id_,
                    expire_ts_,
                    hash_val_);

ObTableApiCredential::ObTableApiCredential()
  :cluster_id_(0),
   tenant_id_(0),
   user_id_(0),
   database_id_(0),
   expire_ts_(0),
   hash_val_(0)
{

}

ObTableApiCredential::~ObTableApiCredential()
{

}

int ObTableApiCredential::hash(uint64_t &hash_val, uint64_t seed /*= 0*/) const
{
  hash_val = murmurhash(&cluster_id_, sizeof(cluster_id_), seed);
  hash_val = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
  hash_val = murmurhash(&user_id_, sizeof(user_id_), hash_val);
  hash_val = murmurhash(&database_id_, sizeof(database_id_), hash_val);
  hash_val = murmurhash(&expire_ts_, sizeof(expire_ts_), hash_val);
  return OB_SUCCESS;
}

void ObTableApiCredential::reset()
{
  cluster_id_ = 0;
  tenant_id_ = 0;
  user_id_ = 0;
  database_id_ = 0;
  expire_ts_ = 0;
  hash_val_ = 0;
}

////////////////////////////////////////////////////////////////
int ObTableAggregation::deep_copy(ObIAllocator &allocator, ObTableAggregation &dst) const
{
  int ret = OB_SUCCESS;

  dst.type_ = type_;
  if (OB_FAIL(ob_write_string(allocator, column_, dst.column_))) {
    LOG_WARN("fail to deep copy aggregation column", K(ret), K_(column));
  }

  return ret;
}

OB_SERIALIZE_MEMBER(ObTableAggregation,
                    type_,
                    column_);

////////////////////////////////////////////////////////////////
OB_SERIALIZE_MEMBER(ObTableMoveReplicaInfo,
                    table_id_,
                    schema_version_,
                    tablet_id_,
                    server_,
                    role_,
                    replica_type_,
                    part_renew_time_,
                    reserved_);

OB_SERIALIZE_MEMBER(ObTableMoveResult,
                    replica_info_,
                    reserved_);

OB_UNIS_DEF_SERIALIZE(ObTableTabletOp,
                      tablet_id_,
                      option_flag_,
                      single_ops_);

OB_UNIS_DEF_SERIALIZE_SIZE(ObTableTabletOp,
                           tablet_id_,
                           option_flag_,
                           single_ops_);

OB_DEF_DESERIALIZE(ObTableTabletOp,)
{
  int ret = OB_SUCCESS;
  UNF_UNUSED_DES;
  LST_DO_CODE(OB_UNIS_DECODE,
              tablet_id_,
              option_flag_);

  int64_t single_op_size = 0;
  OB_UNIS_DECODE(single_op_size);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(single_ops_.prepare_allocate(single_op_size))) {
      LOG_WARN("fail to prepare allocatate single ops", K(ret), K(single_op_size));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < single_op_size; ++i) {
      ObTableSingleOp &single_op = single_ops_.at(i);
      single_op.set_deserialize_allocator(deserialize_alloc_);
      single_op.set_dictionary(all_rowkey_names_, all_properties_names_);
      single_op.set_is_same_properties_names(is_ls_same_properties_names_);
      OB_UNIS_DECODE(single_op);
    }  // end for
  }

  return ret;
}

ObTableTabletOp::ObTableTabletOp(const ObTableTabletOp &other)
{
  this->tablet_id_ = other.get_tablet_id();
  this->option_flag_ = other.get_option_flag();
  this->single_ops_ = other.single_ops_;
}

void ObTableLSOp::reset()
{
  tablet_ops_.reset();
  ls_id_ = share::ObLSID::INVALID_LS_ID;
  option_flag_ = 0;
}

OB_UNIS_DEF_SERIALIZE(ObTableLSOp,
                      ls_id_,
                      table_name_,
                      table_id_,
                      rowkey_names_,
                      properties_names_,
                      option_flag_,
                      tablet_ops_);

OB_UNIS_DEF_SERIALIZE_SIZE(ObTableLSOp,
                           ls_id_,
                           table_name_,
                           table_id_,
                           rowkey_names_,
                           properties_names_,
                           option_flag_,
                           tablet_ops_);

OB_DEF_DESERIALIZE(ObTableLSOp,)
{
  int ret = OB_SUCCESS;
  UNF_UNUSED_DES;
  LST_DO_CODE(OB_UNIS_DECODE,
              ls_id_,
              table_name_,
              table_id_,
              rowkey_names_,
              properties_names_,
              option_flag_);

  int64_t tablet_op_size = 0;
  OB_UNIS_DECODE(tablet_op_size);


  if (OB_SUCC(ret)) {
    if (OB_FAIL(tablet_ops_.prepare_allocate(tablet_op_size))) {
      LOG_WARN("fail to prepare allocatate tablet ops", K(ret), K(tablet_op_size));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_op_size; ++i) {
      ObTableTabletOp &tablet_op = tablet_ops_.at(i);
      tablet_op.set_deserialize_allocator(deserialize_alloc_);
      tablet_op.set_dictionary(&rowkey_names_, &properties_names_);
      tablet_op.set_is_ls_same_prop_name(is_same_properties_names_);
      OB_UNIS_DECODE(tablet_op);
    } // end for
  }

  return ret;
}

OB_DEF_SERIALIZE(ObTableSingleOp)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, op_type_, flag_);
  if (OB_SUCC(ret) && this->need_query()) {
    if (OB_ISNULL(op_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("query op is NULL", K(ret));
    } else {
      OB_UNIS_ENCODE(*op_query_);
    }
  }
  OB_UNIS_ENCODE(entities_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableSingleOp)
{
  int64_t len = 0;
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ADD_LEN, op_type_, flag_);
  if (OB_SUCC(ret) && this->need_query()) {
    if (OB_ISNULL(op_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("query op is NULL", K(ret));
    } else {
      OB_UNIS_ADD_LEN(*op_query_);
    }
  }
  OB_UNIS_ADD_LEN(entities_);
  return len;
}

OB_DEF_DESERIALIZE(ObTableSingleOp, )
{
  int ret = OB_SUCCESS;
  UNF_UNUSED_DES;
  LST_DO_CODE(OB_UNIS_DECODE, op_type_, flag_);

  if (OB_SUCC(ret) && this->need_query()) {
    // decode op_query
    if (OB_ISNULL(op_query_ = OB_NEWx(ObTableSingleOpQuery, deserialize_alloc_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret), K(sizeof(ObTableSingleOpQuery)));
    } else {
      op_query_->set_deserialize_allocator(deserialize_alloc_);
      op_query_->set_dictionary(all_rowkey_names_);
      OB_UNIS_DECODE(*op_query_);
    }
  }

  if (OB_SUCC(ret)) {
    // decode entities
    int64_t entities_size = 0;
    OB_UNIS_DECODE(entities_size);

    if (OB_SUCC(ret)) {
      if (OB_FAIL(entities_.prepare_allocate(entities_size))) {
        LOG_WARN("failed to prepare allocate single op entities", K(ret), K(entities_size));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < entities_size; ++i) {
        ObTableSingleOpEntity &op_entity = entities_.at(i);
        op_entity.set_dictionary(all_rowkey_names_, all_properties_names_);
        op_entity.set_is_same_properties_names(is_same_properties_names_);
        OB_UNIS_DECODE(op_entity);
      }
    }
  }
  return ret;
}

void ObTableSingleOp::reset()
{
  op_type_ = ObTableOperationType::INVALID;
  flag_ = 0;
  op_query_ = nullptr;
  entities_.reset();
}

int ObTableSingleOpQAM::set_mutations(const ObTableSingleOp &single_op)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObTableSingleOpEntity> &entities = single_op.get_entities();
  for (int64_t i = 0; OB_SUCC(ret) && i < entities.count(); i++) {
    if (single_op.get_op_type() != ObTableOperationType::CHECK_AND_INSERT_UP) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "single op type is not check and insert up");
      LOG_WARN("only check and insert up is supported", K(ret), K(single_op.get_op_type()));
    } else if (OB_FAIL(mutations_.insert_or_update(entities.at(i)))) {
      LOG_WARN("fail to construct mutations", K(ret), "entity", entities.at(i));
    }
  }
  return ret;
}

uint64_t ObTableSingleOp::get_checksum()
{
  uint64_t checksum = 0;
  checksum = ob_crc64(checksum, &op_type_, sizeof(op_type_));
  checksum = ob_crc64(checksum, &flag_, sizeof(flag_));
  if (OB_NOT_NULL(op_query_)) {
    const uint64_t query_checksum = op_query_->get_checksum();
    checksum = ob_crc64(checksum, &query_checksum, sizeof(query_checksum));
  }

  for (int64_t i = 0; i < entities_.count(); i++) {
    const int64_t rowkey_size = entities_.at(i).get_rowkey_size();
    const int64_t property_count = entities_.at(i).get_properties_count();
    checksum = ob_crc64(checksum, &rowkey_size, sizeof(rowkey_size));
    checksum = ob_crc64(checksum, &property_count, sizeof(property_count));
  }

  return checksum;
}

void ObTableSingleOpEntity::reset()
{
    rowkey_names_bp_.clear();
    properties_names_bp_.clear();
    ObTableEntity::reset();
}

int ObTableSingleOpEntity::deep_copy(common::ObIAllocator &allocator, const ObITableEntity &other)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(deep_copy_rowkey(allocator, other))) {
    LOG_WARN("failed to deep copy rowkey", K(ret), K(other));
  } else if (OB_FAIL(deep_copy_properties(allocator, other))) {
    LOG_WARN("failed to deep copy properties", K(ret), K(other));
  } else {
    this->all_rowkey_names_ = other.get_all_rowkey_names();
    this->all_properties_names_ = other.get_all_properties_names();

    const ObTableBitMap *other_rowkey_bp = other.get_rowkey_names_bitmap();
    if (OB_ISNULL(other_rowkey_bp)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get_rowkey_names_bitmap", K(ret), K(other));
    } else if (OB_FAIL(rowkey_names_bp_.init_bitmap_size(other_rowkey_bp->get_valid_bits_num()))) {
      LOG_WARN("failed to init_bitmap_size", K(ret), KPC(other_rowkey_bp));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < other_rowkey_bp->get_block_count(); ++i) {
        ObTableBitMap::size_type block_val;
        if (OB_FAIL(other_rowkey_bp->get_block_value(i, block_val))) {
          LOG_WARN("failed to get_block_value", K(ret), K(i), KPC(other_rowkey_bp));
        } else if (OB_FAIL(rowkey_names_bp_.push_block_data(block_val))) {
          LOG_WARN("failed to push_block_data", K(ret), K(i), K(block_val), K(rowkey_names_bp_));
        }
      }

      if (OB_SUCC(ret)) {
        const ObTableBitMap *other_prop_name_bp = other.get_properties_names_bitmap();
        if (OB_ISNULL(other_prop_name_bp)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get_properties_names_bitmap", K(ret), K(other));
        } else if (OB_FAIL(properties_names_bp_.init_bitmap_size(other_prop_name_bp->get_valid_bits_num()))) {
          LOG_WARN("failed to init_bitmap_size", K(ret), KPC(other_prop_name_bp));
        } else {
          for (int64_t i = 0; i < other_prop_name_bp->get_block_count(); ++i) {
            ObTableBitMap::size_type block_val;
            if (OB_FAIL(other_prop_name_bp->get_block_value(i, block_val))) {
              LOG_WARN("failed to get_block_value", K(ret), K(i), KPC(other_prop_name_bp));
            } else if (OB_FAIL(properties_names_bp_.push_block_data(block_val))) {
              LOG_WARN("failed to push_block_data", K(ret), K(i), K(block_val), K(properties_names_bp_));
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObTableSingleOpEntity::construct_names_bitmap(const ObITableEntity &req_entity)
{
  int ret = OB_SUCCESS;
  if (this->get_rowkey_size() != 0) {
    const ObTableBitMap *other_rowkey_bp = req_entity.get_rowkey_names_bitmap();
    if (OB_ISNULL(other_rowkey_bp)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get_rowkey_names_bitmap", K(ret), K(req_entity));
    } else {
      rowkey_names_bp_ = *other_rowkey_bp;
    }
  } else if (OB_FAIL(rowkey_names_bp_.init_bitmap_size(0))) {
    LOG_WARN("failed to init bitmap size", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (this->get_properties_count() != 0) {
      const ObTableBitMap *other_prop_name_bp = req_entity.get_properties_names_bitmap();
      if (OB_ISNULL(other_prop_name_bp)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get_properties_names_bitmap", K(ret), K(req_entity));
      } else {
        properties_names_bp_ = *other_prop_name_bp;
      }
    } else if (OB_FAIL(properties_names_bp_.init_bitmap_size(0))) {
      LOG_WARN("failed to init bitmap size", K(ret));
    }
  }
  return ret;
}

int ObTableSingleOpEntity::construct_names_bitmap_by_dict(const ObITableEntity &req_entity)
{
  int ret = OB_SUCCESS;
  if (this->get_rowkey_size() != 0) {
    const ObTableBitMap *other_rowkey_bp = req_entity.get_rowkey_names_bitmap();
    if (OB_ISNULL(other_rowkey_bp)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get_rowkey_names_bitmap", K(ret), K(req_entity));
    } else {
      rowkey_names_bp_ = *other_rowkey_bp;
    }
  } else if (OB_FAIL(rowkey_names_bp_.init_bitmap_size(0))) {
    LOG_WARN("failed to init bitmap size", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (this->get_properties_count() != 0) {
      if (OB_FAIL(construct_properties_bitmap_by_dict(req_entity))) {
        LOG_WARN("failed to construct properties bitmap by dict", K(ret));
      }
    } else if (OB_FAIL(properties_names_bp_.init_bitmap_size(0))) {
      LOG_WARN("failed to init bitmap size", K(ret));
    }
  }
  return ret;
}

int ObTableSingleOpEntity::construct_properties_bitmap_by_dict(const ObITableEntity &req_entity)
{
  int ret = OB_SUCCESS;
  int64_t all_prop_count = OB_INVALID_SIZE; // all_prop_count = -1
  if (OB_ISNULL(this->all_properties_names_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("all properties names is NULL", K(ret));
  } else if (FALSE_IT(all_prop_count = this->all_properties_names_->count())) {
  } else if (OB_FAIL(properties_names_bp_.init_bitmap(all_prop_count))) {
    LOG_WARN("failed to init bitmap size", K(ret));
  } else if (all_prop_count == this->get_properties_count()) {
    // fastpath: propterties in entity has all columns
    // and the result is from scan iterator, so we can ensure the property counts means all columns
    if (OB_FAIL(properties_names_bp_.set_all_bits_true())) {
      LOG_WARN("failed set all bits true", K(ret));
    }
  } else {
    // slow path: find each property position in dict and set bitmap
    const ObIArray<ObString> &prop_name = this->get_properties_names();
    for (int64_t i = 0; i < prop_name.count() && OB_SUCC(ret); i++) {
      int64_t idx = -1;
      if (!has_exist_in_array(*all_properties_names_, prop_name.at(i), &idx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("property name is not exist in properties name dict", K(ret),
                  K(prop_name.at(i)), KPC(all_properties_names_), K(i));
      } else if (OB_FAIL(properties_names_bp_.set(idx))) {
        LOG_WARN("failed to set bitmap", K(ret), K(idx), K(prop_name.at(i)), K(properties_names_bp_));
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObTableSingleOpEntity)
{
  int ret = OB_SUCCESS;
  // ensure all_rowkey_names_ and all_properties_names_ is not null!
  if (!has_dictionary()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTableSingleOpEntity has not valid all_names", K(ret));
  } else if (!rowkey_names_bp_.has_init()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowkey_names_bp is not init by rowkey_names", K(ret), K(rowkey_names_bp_),
      K(rowkey_names_), KPC(all_rowkey_names_));
  } else if (!properties_names_bp_.has_init()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("properties_names_bp is not init by prop_names", K(ret), K(properties_names_bp_),
      K(properties_names_), KPC(all_properties_names_));
  } else {
    // construct row_names_bp, properties_names_bp
    // msg_format: row_bp_count | row_names_bp | all_rowkey_names_count | rowkeys | all_properties_names_count |
    // properties_names_bp | properties_count | properties_values encode row_names_bp
    const int64_t row_names_bit_count = rowkey_names_bp_.get_valid_bits_num();
    OB_UNIS_ENCODE(row_names_bit_count)
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(rowkey_names_bp_.serialize(buf, buf_len, pos))) {
      LOG_WARN("failed to encode rowkey_names_bp_", K(ret), K(rowkey_names_bp_), K(pos));
    } else {
      // encode rowkeys
      const int64_t rowkey_size = this->get_rowkey_size();
      OB_UNIS_ENCODE(rowkey_size);

      for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_.count(); ++i) {
        const ObObj &obj = rowkey_.at(i);
        if (OB_FAIL(ObTableSerialUtil::serialize(buf, buf_len, pos, obj))) {
          LOG_WARN("fail to serialize table object", K(ret), K(buf), K(buf_len), K(pos));
        }
      }
    }
    if (OB_SUCC(ret)) {
      int64_t prop_names_bp_count = properties_names_bp_.get_valid_bits_num();
      OB_UNIS_ENCODE(prop_names_bp_count)

      if (OB_SUCC(ret)) {
        if (OB_FAIL(properties_names_bp_.serialize(buf, buf_len, pos))) {
          // encode properties_names_bp
          LOG_WARN("failed to encode properties_names_bp", K(ret), K(properties_names_bp_), K(pos));
        } else {
          // encode properties_values
          const int64_t properties_count = this->get_properties_count();
          OB_UNIS_ENCODE(properties_count);

          for (int64_t i = 0; OB_SUCC(ret) && i < properties_count; ++i) {
            const ObObj &obj = properties_values_.at(i);
            if (OB_FAIL(ObTableSerialUtil::serialize(buf, buf_len, pos, obj))) {
              LOG_WARN("fail to serialize table object", K(ret), K(buf), K(buf_len), K(pos));
            }
          }
        }
      }
    }
  }

  return ret;
}

OB_DEF_DESERIALIZE(ObTableSingleOpEntity)
{
  // msg_format: row_names_bp | rowkey_count | rowkeys | properties_names_bp | properties_count | properties_values
  int ret = OB_SUCCESS;
  reset();

  if (!has_dictionary()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTableSingleOpEntity has not valid all_names", K(ret));
  }else {
    // decode rowkey_names_bp_, rowkeys
    int64_t row_names_bit_count = -1;
    OB_UNIS_DECODE(row_names_bit_count);

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(rowkey_names_bp_.init_bitmap_size(row_names_bit_count))) {
      LOG_WARN("failed to init bitmap size", K(ret), K(row_names_bit_count));
    } else if (OB_FAIL(rowkey_names_bp_.deserialize(buf, data_len, pos))) {
      LOG_WARN("failed to decode rowkey_names_bp", K(ret), K(pos));
    } else {
      int64_t rowkey_size = -1;
      OB_UNIS_DECODE(rowkey_size);

      if (OB_FAIL(ret)) {
      } else {
        ObObj value;
        if (NULL == alloc_) {
          for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_size; ++i) {
            if (OB_FAIL(ObTableSerialUtil::deserialize(buf, data_len, pos, value))) {
              LOG_WARN("fail to deserialize table object", K(ret), K(buf), K(data_len), K(pos));
            } else if (OB_FAIL(rowkey_.push_back(value))) {
              LOG_WARN("fail to add obj", K(ret));
            }
          }
        } else {
          ObObj value_clone;
          for (int64_t i = 0; OB_SUCCESS == ret && i < rowkey_size; ++i) {
            if (OB_FAIL(ObTableSerialUtil::deserialize(buf, data_len, pos, value))) {
              LOG_WARN("fail to deserialize table object", K(ret), K(buf), K(data_len), K(pos));
            } else if (OB_FAIL(ob_write_obj(*alloc_, value, value_clone))) {
              LOG_WARN("failed to copy value", K(ret));
            } else if (OB_FAIL(this->add_rowkey_value(value_clone))) {
              LOG_WARN("failed to add rowkey value", K(ret), K(value_clone));
            }
          }
        }
      }
    }

    // decode properties_names_bp
    if (OB_SUCC(ret)) {
      int64_t properties_names_bit_count = -1;
      OB_UNIS_DECODE(properties_names_bit_count);

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(properties_names_bp_.init_bitmap_size(properties_names_bit_count))) {
        LOG_WARN("failed to init bitmap size", K(ret), K(properties_names_bit_count));
      } else if (OB_FAIL(properties_names_bp_.deserialize(buf, data_len, pos))) {
        LOG_WARN("failed to decode properties_names_bp", K(ret), K(pos));
      } else {
        // decode properties_values
        int64_t properties_count = -1;
        OB_UNIS_DECODE(properties_count);
        if (OB_FAIL(ret)) {
        } else {
          ObObj value;
          if (NULL == alloc_) {
            // shallow copy
            for (int64_t i = 0; OB_SUCC(ret) && i < properties_count; ++i) {
              if (OB_FAIL(ObTableSerialUtil::deserialize(buf, data_len, pos, value))) {
                LOG_WARN("fail to deserialize table object", K(ret), K(buf), K(data_len), K(pos));
              } else if (OB_FAIL(this->push_value(value))) {
                LOG_WARN("failed to push property values", K(ret), K(value));
              }
            }
          } else {
            // deep copy
            ObObj value_clone;
            for (int64_t i = 0; i < properties_count && OB_SUCCESS == ret; ++i) {
              if (OB_FAIL(ObTableSerialUtil::deserialize(buf, data_len, pos, value))) {
                LOG_WARN("fail to deserialize table object", K(ret), K(buf), K(data_len), K(pos));
              } else if (OB_FAIL(ob_write_obj(*alloc_, value, value_clone))) {
                LOG_WARN("failed to copy value", K(ret));
              } else if (OB_FAIL(this->push_value(value_clone))) {
                LOG_WARN("failed to set property", K(ret), K(value_clone));
              }
            }
          }
        }
      }
    }

    // bitmap key to real val
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObTableSingleOpEntity::construct_column_names(rowkey_names_bp_, *all_rowkey_names_, rowkey_names_))) {
        LOG_WARN("failed to construct rowkey names from bitmap", K(ret), K(rowkey_names_bp_),
          KPC(all_rowkey_names_), K(rowkey_names_));
      } else {
        if (is_same_properties_names_) {
          if (OB_FAIL(properties_names_.assign(*all_properties_names_))) {
            LOG_WARN("failed to assign properties_names", K(ret));
          }
        } else if (OB_FAIL(ObTableSingleOpEntity::construct_column_names(
                        properties_names_bp_, *all_properties_names_, properties_names_))) {
          LOG_WARN("failed to construct prop_names names from bitmap", K(ret),
            K(properties_names_bp_), KPC(all_properties_names_), K(properties_names_));
        }
      }
    }
  }

  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTableSingleOpEntity)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;

  if (!has_dictionary()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTableSingleOpEntity has not valid all_names", K(ret));
  } else if (!rowkey_names_bp_.has_init()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowkey_names_bp is not init by rowkey_names", K(ret), K(rowkey_names_bp_),
      K(rowkey_names_), KPC(all_rowkey_names_));
  } else if (!properties_names_bp_.has_init()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("properties_names_bp is not init by prop_names", K(ret),
      K(properties_names_bp_), K(properties_names_), KPC(all_properties_names_));
  } else {
    // row_key_names_bp size
    int64_t row_names_bit_count = get_rowkey_size() == 0 ? 0 : all_rowkey_names_->count();
    OB_UNIS_ADD_LEN(row_names_bit_count);
    len += rowkey_names_bp_.get_serialize_size();

    // row_keys size
    ObString key;
    ObObj value;
    const int64_t rowkey_size = get_rowkey_size();
    OB_UNIS_ADD_LEN(rowkey_size);
    for (int64_t i = 0; i < rowkey_size && OB_SUCCESS == ret; ++i) {
      if (OB_FAIL(this->get_rowkey_value(i, value))) {
        LOG_WARN("failed to get value", K(ret), K(i));
      }
      len += ObTableSerialUtil::get_serialize_size(value);
    }

    // properties_names_bp size
    int64_t prop_names_bp_count = get_properties_count() == 0 ? 0 : all_properties_names_->count();
    OB_UNIS_ADD_LEN(prop_names_bp_count);
    len += properties_names_bp_.get_serialize_size();

    // prop_vals size
    const int64_t properties_count = this->get_properties_count();
    OB_UNIS_ADD_LEN(properties_count);

    for (int64_t i = 0; i < properties_count && OB_SUCCESS == ret; ++i) {
      const ObObj &obj = this->get_properties_value(i);
      len += ObTableSerialUtil::get_serialize_size(obj);
    }
  }

  return len;
}

  ////////////////////////////////////////////////

int ObTableBitMap::init_bitmap_size(int64_t valid_bits_num)
{
  int ret = OB_SUCCESS;
  int64_t block_nums = get_need_blocks_num(valid_bits_num);
  if (block_nums < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("block_nums less than zero", K(ret), K(block_nums));
  } else if (block_count_ >= 0) {
    if (block_nums != block_count_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("bitmap had init before with diffrent val", K(ret), K(block_count_), K(block_nums));
    }
  } else if (OB_FAIL(datas_.reserve(block_nums))) {
    LOG_WARN("datas_ reserve failed", K(ret), K(block_nums));
  } else {
    block_count_ = block_nums;
    valid_bits_num_ = valid_bits_num;
  }

  return ret;
}

int ObTableBitMap::init_bitmap(int64_t valid_bits_num)
{
  int ret = OB_SUCCESS;
  int64_t block_nums = get_need_blocks_num(valid_bits_num);
  if (block_nums < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("block_nums less than zero", K(ret), K(block_nums));
  } else if (block_count_ >= 0) {
    if (block_nums != block_count_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("bitmap had init before with diffrent val", K(ret), K(block_count_), K(block_nums));
    }
  } else {
    for (int64_t i = 0; i < block_nums && OB_SUCC(ret); i++) {
      if (OB_FAIL(datas_.push_back(0))) {
        LOG_WARN("failed to init block value", K(ret), K(i));
      }
    }
    block_count_ = block_nums;
    valid_bits_num_ = valid_bits_num;
  }

  return ret;
}

int ObTableBitMap::reset()
{
  int ret = OB_SUCCESS;

  if (block_count_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bitmap is not init", K(ret), K(block_count_));
  } else {
    datas_.reset();
  }
  return ret;
}

int ObTableBitMap::push_block_data(size_type data)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(datas_.count() > block_count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN(
        "failed to push block data, block size less than datas size", K(block_count_), K(datas_.count()), K(data));
  } else if (OB_FAIL(datas_.push_back(data))) {
    LOG_WARN("failed to push block data", K(ret), K(datas_), K(data));
  }
  return ret;
}

int ObTableBitMap::get_true_bit_positions(ObIArray<int64_t> & true_pos) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(datas_.count() != block_count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("datas_.count() is not equal to block_count_", K(ret), K(block_count_), K(datas_.count()));
  }
  for (int64_t block_index = 0; OB_SUCC(ret) && block_index < block_count_; ++block_index) {
    size_type byte;
    if (OB_FAIL(datas_.at(block_index, byte))) {
      LOG_WARN("failed to get datas", K(ret), K(datas_), K(block_index));
    } else if (byte != 0){
      int64_t new_begin_pos = block_index << BLOCK_MOD_BITS;
      for (int64_t bit_index = 0; OB_SUCC(ret) && bit_index < BITS_PER_BLOCK; ++bit_index) {
        if (byte & (1 << bit_index)) {
          if (OB_FAIL(true_pos.push_back(new_begin_pos + bit_index))) {
            LOG_WARN("failed to add true pos", K(ret), K(new_begin_pos + bit_index));
          }
        }
      }
    }
  }
  return ret;
}

int ObTableBitMap::set(int64_t bit_pos)
{
  // datas_[bit_pos / 8] |= (1 << (bit_pos % 8));
  int ret = OB_SUCCESS;
  // bit_pos / 8
  int64_t bit_to_block_index = bit_pos >> BLOCK_MOD_BITS;
  if (bit_to_block_index >= block_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bit_pos is overflow", K(ret), K(bit_pos), K(block_count_));
  } else if (OB_UNLIKELY(block_count_ != datas_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("block_count_ no equal to datas_.count()", K(ret), K(block_count_), K(datas_.count()));
  } else {
    size_type byte;
    if (OB_FAIL(datas_.at(bit_to_block_index, byte))) {
      LOG_WARN("failed get block val", K(ret), K(datas_), K(bit_to_block_index));
    } else {
      //  byte | (1 << (bit_pos % 8))
      datas_.at(bit_to_block_index) = (byte | (1 << (bit_pos & (BITS_PER_BLOCK - 1))));
    }
  }

  return ret;
}

int ObTableBitMap::set_all_bits_true()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(datas_.count() != block_count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("datas_.count is not equal to block_count_", K(ret), K(block_count_), K(datas_.count()));
  }
  for (int64_t i = 0; i < block_count_ && OB_SUCC(ret); i++) {
    datas_.at(i) = SIZE_TYPE_MAX;
  }
  return ret;
}

int64_t ObTableBitMap::get_serialize_size() const
{
  int64_t len = 0;
  int ret = OB_SUCCESS;
  if (block_count_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bitmap is not init", K(ret), K(block_count_));
  } else if (block_count_ == 0) {
    len = 0;
  } else {
    len += (block_count_ * sizeof(size_type));
  }
  return len;
}

int ObTableBitMap::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (block_count_ != datas_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("block_count is not datas_.count() ", K(ret), K_(block_count), K_(datas));
  }
  for (int64_t i = 0; OB_SUCCESS == ret && i < block_count_; ++i) {
    size_type block_val;
    if (OB_FAIL(this->get_block_value(i, block_val))) {
      LOG_WARN("failed to get block value", K(ret), K(i));
    }
    OB_UNIS_ENCODE(block_val);
  }
  return ret;
}

int ObTableBitMap::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (block_count_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bitmap is not init", K(ret), K(block_count_));
  } else {
    size_type block_data;
    for (int64_t i = 0; OB_SUCCESS == ret && i < block_count_; ++i) {
      OB_UNIS_DECODE(block_data);

      if (OB_SUCC(ret)) {
        if (OB_FAIL(this->push_block_data(block_data))) {
          LOG_WARN("failed to add table bitmap block data", K(ret), K(block_data));
        }
      }
    }
  }
  return ret;
}

DEF_TO_STRING(ObTableBitMap)
{
  int64_t pos = 0;
  J_OBJ_START();

  J_NAME("valid_bits_num");
  J_COLON();
  BUF_PRINTO(valid_bits_num_);

  J_NAME("block_size");
  J_COLON();
  BUF_PRINTO(block_count_);

  J_NAME("block_data");
  J_COLON();
  BUF_PRINTO(datas_);

  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER_SIMPLE(ObHBaseParams,
                           caching_,
                           call_timeout_,
                           flag_,
                           hbase_version_);

int ObHBaseParams::deep_copy(ObIAllocator &allocator, ObKVParamsBase *hbase_params) const
{
  int ret = OB_SUCCESS;
  if (hbase_params == nullptr || hbase_params->get_param_type() != ParamType::HBase) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected hbase adress", K(ret), KPC(hbase_params));
  } else {
    ObHBaseParams *param = static_cast<ObHBaseParams*>(hbase_params);
    param->caching_ = caching_;
    param->call_timeout_ = call_timeout_;
    param->is_cache_block_ = is_cache_block_;
    param->allow_partial_results_ = allow_partial_results_;
    param->check_existence_only_ = check_existence_only_;
    if (OB_FAIL(ob_write_string(allocator, hbase_version_, param->hbase_version_))) {
      LOG_WARN("failed to write string", K(ret), K_(hbase_version));
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER_SIMPLE(ObFTSParam,
                           search_text_);

int ObFTSParam::deep_copy( ObIAllocator &allocator, ObKVParamsBase *fts_params) const
{
  int ret = OB_SUCCESS;
  if (fts_params == nullptr || fts_params->get_param_type() != ParamType::FTS) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected hbase adress", K(ret), KPC(fts_params));
  } else {
    ObFTSParam *param = static_cast<ObFTSParam*>(fts_params);
    ObString &search_text = param->get_search_text();
    if (OB_FAIL(ob_write_string(allocator, search_text_, search_text))) {
      LOG_WARN("fail to deep copy search text", K(ret), K(search_text_));
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObKVParams)
{
  int ret = OB_SUCCESS;
  if (pos < data_len) {
    int8_t param_type = -1;
    LST_DO_CODE(OB_UNIS_DECODE, param_type);
    if (OB_SUCC(ret)) {
      if (param_type == static_cast<int8_t>(ParamType::HBase)) {
        if (OB_FAIL(alloc_ob_params(ParamType::HBase, ob_params_))) {
          RPC_WARN("alloc ob_params_ memory failed", K(ret));
        }
      } else if (param_type == static_cast<int8_t>(ParamType::Redis)) {
        if (OB_FAIL(alloc_ob_params(ParamType::Redis, ob_params_))) {
          RPC_WARN("alloc ob_params_ memory failed", K(ret));
        }
      } else if (param_type == static_cast<int8_t>(ParamType::FTS)) {
        if (OB_FAIL(alloc_ob_params(ParamType::FTS, ob_params_))) {
          RPC_WARN("alloc ob_params_ memory failed", K(ret));
        }
      } else {
        ret = OB_NOT_SUPPORTED;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ob_params_->deserialize(buf, data_len, pos))) {
        RPC_WARN("ob_params deserialize fail");
      }
    }

  }
  return ret;
}

OB_DEF_SERIALIZE(ObKVParams)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(ob_params_)) {
    LST_DO_CODE(OB_UNIS_ENCODE, ob_params_->get_param_type(), *ob_params_);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObKVParams)
{
  int64_t len = 0;
  if (OB_NOT_NULL(ob_params_)) {
    LST_DO_CODE(OB_UNIS_ADD_LEN, ob_params_->get_param_type(), *ob_params_);
  }
  return len;
}

int ObKVParams::deep_copy(ObIAllocator &allocator, ObKVParams &ob_params) const
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(ob_params_)) {
    ob_params.set_allocator(&allocator);
    if (OB_FAIL(ob_params.alloc_ob_params(ob_params_->get_param_type(), ob_params.ob_params_))) {
      LOG_WARN("alloc ob params error", K(ob_params_->get_param_type()), K(ret));
    } else if (OB_FAIL(ob_params_->deep_copy(allocator, ob_params.ob_params_))) {
      LOG_WARN("ob_params_ deep_copy error", K(ret));
    }
  }
  return ret;
}

int ObKVParams::init_ob_params_for_hfilter(const ObHBaseParams*& params) const
{
  int ret = OB_SUCCESS;
  if (ob_params_->get_param_type() != ParamType::HBase) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ob_params_ adress", K(ob_params_));
  } else {
    params = static_cast<ObHBaseParams*>(ob_params_);
    if (params == nullptr) {
      ret = OB_BAD_NULL_ERROR;
      LOG_WARN("unexpected nullptr after static_cast");
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////
OB_SERIALIZE_MEMBER(ObRedisResult, ret_, msg_);

int ObRedisResult::assign(const ObRedisResult &other)
{
  int ret = OB_SUCCESS;
  ret_ = other.ret_;
  if (OB_FAIL(ob_write_string(*allocator_, other.msg_, msg_))) {
    LOG_WARN("fail to copy redis msg", K(other.msg_), K(msg_));
  }
  return ret;
}

int ObRedisResult::set_ret(int arg_ret, const ObString &redis_msg, bool need_deep_copy /* = false*/)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid null allocator", K(ret));
  } else if (need_deep_copy) {
    if (OB_FAIL(ob_write_string(*allocator_, redis_msg, msg_))) {
      LOG_WARN("fail to copy redis msg", K(ret), K(redis_msg));
    }
  }
  ret_ = (arg_ret == OB_SUCCESS) ? ret : arg_ret;

  if (OB_FAIL(ret_)) {
    set_err(ret_);
  }
  return ret;
}

int ObRedisResult::set_err(int err)
{
  int ret = OB_SUCCESS;
  ret_ = err;
  common::ObWarningBuffer *wb = common::ob_get_tsi_warning_buffer();
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null allocator", K(ret));
  } else if (OB_NOT_NULL(wb)) {
    char *err_msg = nullptr;
    if (OB_ISNULL(err_msg = reinterpret_cast<char*>(allocator_->alloc(common::OB_MAX_ERROR_MSG_LEN)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret), K(common::OB_MAX_ERROR_MSG_LEN));
    } else {
      int n = snprintf(err_msg, common::OB_MAX_ERROR_MSG_LEN, "%s", wb->get_err_msg());
      if (n < 0 || n > common::OB_MAX_ERROR_MSG_LEN) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("snprintf error or buf not enough", KR(ret), K(n));
      } else {
        msg_.assign(err_msg, n);
      }
    }
  }
  return ret;
}

int ObRedisResult::convert_to_table_op_result(ObTableOperationResult &result)
{
  int ret = OB_SUCCESS;
  if (ret_ == OB_SUCCESS) {
    ObTableEntity *res_entity = static_cast<ObTableEntity *>(result.get_entity());
    ObObj obj;
    if (OB_ISNULL(res_entity)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("res_entity is null", K(ret));
    } else if (OB_FALSE_IT(obj.set_varchar(msg_))) {
    } else if (OB_FAIL(res_entity->set_property(ObRedisUtil::REDIS_PROPERTY_NAME, obj))) {
      LOG_WARN("fail to set property", K(ret), K(msg_));
    }
  }

  result.set_err(ret == OB_SUCCESS ? ret_ : ret);
  result.set_type(ObTableOperationType::REDIS);

  return ret;
}
