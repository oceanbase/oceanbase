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

#ifndef INCLUDE_OB_IMPORT_SCHEMA_ITEM_HPP
#define INCLUDE_OB_IMPORT_SCHEMA_ITEM_HPP
#include "share/restore/ob_import_schema_item.h"
#include "lib/charset/ob_charset.h"
#endif


namespace oceanbase
{
namespace share
{
// ObImportSimpleSchemaItem
template<int64_t MAX_SIZE, ObIImportItem::ItemType ITEM_TYPE>
int64_t ObImportSimpleSchemaItem<MAX_SIZE, ITEM_TYPE>::get_serialize_size() const
{
  int64_t len = 0;
  len += serialization::encoded_length_vi32(mode_);
  len += name_.get_serialize_size();
  return len;
}

template<int64_t MAX_SIZE, ObIImportItem::ItemType ITEM_TYPE>
int ObImportSimpleSchemaItem<MAX_SIZE, ITEM_TYPE>::serialize(SERIAL_PARAMS) const
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if ((NULL == buf) || (buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid arguments.", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(serialization::encode_vi32(buf, buf_len, new_pos, mode_))) {
    SHARE_LOG(WARN, "fail to serialize name case mode", K(ret));
  } else if (OB_FAIL(name_.serialize(buf, buf_len, new_pos))) {
    SHARE_LOG(WARN, "serialize name failed", K(ret));
  } else {
    pos = new_pos;
  }
  return ret;
}

template<int64_t MAX_SIZE, ObIImportItem::ItemType ITEM_TYPE>
int ObImportSimpleSchemaItem<MAX_SIZE, ITEM_TYPE>::deserialize(DESERIAL_PARAMS)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if ((NULL == buf) || (data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid arguments.", K(ret), KP(buf), K(data_len));
  } else if (OB_FAIL(serialization::decode_vi32(buf, data_len, new_pos, ((int32_t *)(&mode_))))) {
    SHARE_LOG(WARN, "fail to deserialize name case mode, ", K(ret));
  } else if (OB_FAIL(name_.deserialize(buf, data_len, new_pos))) {
    SHARE_LOG(WARN, "deserialize name failed", K(ret));
  } else {
    pos = new_pos;
  }
  return ret;
}

template<int64_t MAX_SIZE, ObIImportItem::ItemType ITEM_TYPE>
void ObImportSimpleSchemaItem<MAX_SIZE, ITEM_TYPE>::reset()
{
  mode_ = common::OB_NAME_CASE_INVALID;
  name_.reset();
}

template<int64_t MAX_SIZE, ObIImportItem::ItemType ITEM_TYPE>
bool ObImportSimpleSchemaItem<MAX_SIZE, ITEM_TYPE>::is_valid() const
{
  return common::OB_NAME_CASE_INVALID != mode_
          && !name_.empty()
          && 0 < name_.length()
          && MAX_SIZE >= name_.length();
}

template<int64_t MAX_SIZE, ObIImportItem::ItemType ITEM_TYPE>
bool ObImportSimpleSchemaItem<MAX_SIZE, ITEM_TYPE>::case_mode_equal(const ObIImportItem &other) const
{
  bool is_equal = false;
  if (get_item_type() == other.get_item_type()) {
    const ObImportSimpleSchemaItem<MAX_SIZE, ITEM_TYPE> &the_other = static_cast<const ObImportSimpleSchemaItem<MAX_SIZE, ITEM_TYPE> &>(other);
    is_equal = ObCharset::case_mode_equal(mode_, name_, the_other.name_);
  }
  return is_equal;
}

template<int64_t MAX_SIZE, ObIImportItem::ItemType ITEM_TYPE>
int64_t ObImportSimpleSchemaItem<MAX_SIZE, ITEM_TYPE>::get_format_serialize_size() const
{
  // For example, db1 is formated as `db1`
  // Pre allocate twice the size to handle escape character.
  return name_.length() * 2 + 2;
}

template<int64_t MAX_SIZE, ObIImportItem::ItemType ITEM_TYPE>
int ObImportSimpleSchemaItem<MAX_SIZE, ITEM_TYPE>::format_serialize(
    char *buf,
    const int64_t buf_len,
    int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if ((NULL == buf) || (buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid arguments.", K(ret), KP(buf), K(buf_len));
  } else if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret));
  } else {
    const bool skip_escape = false;
    const bool do_oracle_mode_escape = false;
    int64_t string_size = 0;
    common::ObHexEscapeSqlStr hex_escape_name(name_, skip_escape, do_oracle_mode_escape);
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s", "`"))) {
      SHARE_LOG(WARN, "fail to format str", K(ret), K(pos), K(buf_len));
    } else if (OB_FALSE_IT(string_size = hex_escape_name.to_string(buf + pos, buf_len - pos))) {
    } else if (OB_FALSE_IT(pos += string_size)) {
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s", "`"))) {
      SHARE_LOG(WARN, "fail to format str", K(ret), K(pos), K(buf_len));
    }
  }

  return ret;
}

template<int64_t MAX_SIZE, ObIImportItem::ItemType ITEM_TYPE>
int ObImportSimpleSchemaItem<MAX_SIZE, ITEM_TYPE>::deep_copy(common::ObIAllocator &allocator, const ObIImportItem &src)
{
  int ret = OB_SUCCESS;
  if (!src.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(src));
  } else if (get_item_type() != src.get_item_type()) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "item type not match", K(ret), "src", src.get_item_type(), "dest", get_item_type());
  } else {
    const ObImportSimpleSchemaItem<MAX_SIZE, ITEM_TYPE> &other = static_cast<const ObImportSimpleSchemaItem<MAX_SIZE, ITEM_TYPE> &>(src);
    if (OB_FAIL(ob_write_string(allocator, other.name_, name_))) {
      SHARE_LOG(WARN, "failed to copy item", K(ret), K(other));
    } else {
      mode_ = other.mode_;
    }
  }

  return ret;
}

template<int64_t MAX_SIZE, ObIImportItem::ItemType ITEM_TYPE>
int ObImportSimpleSchemaItem<MAX_SIZE, ITEM_TYPE>::assign(const ObImportSimpleSchemaItem &other)
{
  int ret = OB_SUCCESS;
  if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(other));
  } else {
    mode_ = other.mode_;
    name_ = other.name_;
  }

  return ret;
}


// ObImportSchemaItemArray
OB_DEF_SERIALIZE_SIZE(ObImportSchemaItemArray<T>, template <typename T>)
{
  return items_.get_serialize_size();
}

OB_DEF_SERIALIZE(ObImportSchemaItemArray<T>, template <typename T>)
{
  int ret = OB_SUCCESS;
  if ((NULL == buf) || (buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(items_.serialize(buf, buf_len, pos))) {
    SHARE_LOG(WARN, "failed to serialize", K(ret), KP(buf), K(buf_len), K(pos));
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObImportSchemaItemArray<T>, template <typename T>)
{
  int ret = OB_SUCCESS;
  common::ObSArray<T> tmp_items;
  if ((NULL == buf) || (data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_FALSE_IT(reset())) {
  } else if (OB_FAIL(tmp_items.deserialize(buf, data_len, pos))) {
    SHARE_LOG(WARN, "failed to deserialize", K(ret), KP(buf), K(data_len), K(pos));
  }
  ARRAY_FOREACH(tmp_items, idx) {
    const T &item = tmp_items.at(idx);
    if (OB_FAIL(add_item(item))) {
      SHARE_LOG(WARN, "failed to add item", K(ret), K(idx), K(item));
    }
  }
  return ret;
}

template <typename T>
int64_t ObImportSchemaItemArray<T>::get_format_serialize_size() const
{
  int64_t size = 1; // include '\0'
  ARRAY_FOREACH_NORET(items_, idx) {
    const T &item = items_.at(idx);
    // item is concatenated with ','
    size += 0 == idx ? 0 : 1;
    size += item.get_format_serialize_size();
  }

  return size;
}

template <typename T>
int ObImportSchemaItemArray<T>::format_serialize(
    char *buf,
    const int64_t buf_len,
    int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if ((NULL == buf) || (buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid arguments.", K(ret), KP(buf), K(buf_len));
  } else if (items_.empty()) {
    buf[new_pos++] = 0;
  }

  ARRAY_FOREACH(items_, idx) {
    const T &item = items_.at(idx);
    // concatenated items with ','
    if (0 < idx && OB_FAIL(databuff_printf(buf, buf_len, new_pos, "%s", ","))) {
      SHARE_LOG(WARN, "fail to format str", K(ret), K(new_pos), K(buf_len));
    } else if (OB_FAIL(item.format_serialize(buf, buf_len, new_pos))) {
      SHARE_LOG(WARN, "fail to format str", K(ret), K(new_pos), K(buf_len));
    }
  }

  if (OB_SUCC(ret)) {
    pos = new_pos;
  }

  return ret;
}

template <typename T>
void ObImportSchemaItemArray<T>::reset()
{
  items_.reset();
  allocator_.reset();
}

template <typename T>
int ObImportSchemaItemArray<T>::assign(const ObImportSchemaItemArray<T> &other)
{
  int ret = OB_SUCCESS;
  const common::ObSArray<T> &items = other.get_items();

  reset();
  ARRAY_FOREACH(items, idx) {
    const T &item = items.at(idx);
    if (OB_FAIL(add_item(item))) {
      SHARE_LOG(WARN, "failed to add item", K(ret), K(idx), K(item));
    }
  }

  return ret;
}

template <typename T>
int ObImportSchemaItemArray<T>::add_item(const T& item)
{
  int ret = OB_SUCCESS;
  T tmp_item;
  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(item));
  } else if (OB_FAIL(tmp_item.deep_copy(allocator_, item))) {
    SHARE_LOG(WARN, "failed to copy item", K(ret), K(item));
  } else if (OB_FAIL(items_.push_back(tmp_item))) {
    SHARE_LOG(WARN, "failed to push back item", K(ret), K(item));
  } else {
    SHARE_LOG(INFO, "add one item", K(item));
  }
  return ret;
}


template <typename T>
bool ObImportSchemaItemArray<T>::is_exist(const T& item) const
{
  bool ret = false;
  const T *out = NULL;
  ret = is_exist(item, out);
  return ret;
}

template <typename T>
bool ObImportSchemaItemArray<T>::is_exist(const T& item, const T *&out) const
{
  bool ret = false;
  ARRAY_FOREACH_NORET(items_, idx) {
    const T &tmp = items_.at(idx);
    if (tmp == item) {
      ret = true;
      out = &tmp;
      break;
    }
  }
  return ret;
}

}
}