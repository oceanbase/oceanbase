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

#ifndef INCLUDE_OB_REMAP_SCHEMA_ITEM_HPP
#define INCLUDE_OB_REMAP_SCHEMA_ITEM_HPP
#include "share/restore/ob_remap_schema_item.h"
#endif


namespace oceanbase
{
namespace share
{

// ObRemapSchemaItem
template<typename S, typename T>
int64_t ObRemapSchemaItem<S, T>::get_serialize_size() const
{
  int64_t size = 0;
  size += src_.get_serialize_size();
  size += target_.get_serialize_size();
  return size;
}

template<typename S, typename T>
int ObRemapSchemaItem<S, T>::serialize(SERIAL_PARAMS) const
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if ((NULL == buf) || (buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid arguments.", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(src_.serialize(buf, buf_len, new_pos))) {
    SHARE_LOG(WARN, "serialize src item failed", K(ret));
  } else if (OB_FAIL(target_.serialize(buf, buf_len, new_pos))) {
    SHARE_LOG(WARN, "serialize target item failed", K(ret));
  } else {
    pos = new_pos;
  }
  return ret;
}

template<typename S, typename T>
int ObRemapSchemaItem<S, T>::deserialize(DESERIAL_PARAMS)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if ((NULL == buf) || (data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid arguments.", K(ret), KP(buf), K(data_len));
  } else if (OB_FAIL(src_.deserialize(buf, data_len, new_pos))) {
    SHARE_LOG(WARN, "deserialize src item failed", K(ret));
  } else if (OB_FAIL(target_.deserialize(buf, data_len, new_pos))) {
    SHARE_LOG(WARN, "deserialize target item failed", K(ret));
  } else {
    pos = new_pos;
  }
  return ret;
}


template<typename S, typename T>
void ObRemapSchemaItem<S, T>::reset()
{
  src_.reset();
  target_.reset();
}

template<typename S, typename T>
bool ObRemapSchemaItem<S, T>::is_valid() const
{
  return src_.is_valid() && target_.is_valid();
}

template<typename S, typename T>
int64_t ObRemapSchemaItem<S, T>::get_format_serialize_size() const
{
  // Concatenate src and target with ':'.
  return src_.get_format_serialize_size() + target_.get_format_serialize_size() + 1;
}

template<typename S, typename T>
int ObRemapSchemaItem<S, T>::format_serialize(
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
  } else if (OB_FAIL(src_.format_serialize(buf, buf_len, pos))) {
    SHARE_LOG(WARN, "fail to format str", K(ret), K(pos), K(buf_len), K_(src));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s", ":"))) {
    SHARE_LOG(WARN, "fail to format str", K(ret), K(pos), K(buf_len));
  } else if (OB_FAIL(target_.format_serialize(buf, buf_len, pos))) {
    SHARE_LOG(WARN, "fail to format str", K(ret), K(pos), K(buf_len), K_(target));
  }

  return ret;
}

template<typename S, typename T>
int ObRemapSchemaItem<S, T>::deep_copy(
    common::ObIAllocator &allocator,
    const ObIImportItem &src)
{
  int ret = OB_SUCCESS;
  if (!src.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(src));
  } else if (get_item_type() != src.get_item_type()) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "item type not match", K(ret), "src", src.get_item_type(), "dest", get_item_type());
  } else {
    const ObRemapSchemaItem<S, T> &other = static_cast<const ObRemapSchemaItem<S, T> &>(src);
    if (OB_FAIL(src_.deep_copy(allocator, other.src_))) {
      SHARE_LOG(WARN, "failed to copy src", K(ret), K(src));
    } else if (OB_FAIL(target_.deep_copy(allocator, other.target_))) {
      SHARE_LOG(WARN, "failed to copy target", K(ret), K(src));
    }
  }
  return ret;
}

template<typename S, typename T>
bool ObRemapSchemaItem<S, T>::operator==(const ObIImportItem &other) const
{
  bool is_equal = false;
  if (get_item_type() == other.get_item_type()) {
    const ObRemapSchemaItem<S, T> &the_other = static_cast<const ObRemapSchemaItem<S, T> &>(other);

    is_equal = src_ == the_other.src_ && target_ == the_other.target_;
  }
  return is_equal;
}

template<typename S, typename T>
int ObRemapSchemaItem<S, T>::assign(const ObRemapSchemaItem &other)
{
  int ret = OB_SUCCESS;
  if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(other));
  } else if (OB_FAIL(src_.assign(other.src_))) {
    SHARE_LOG(WARN, "failed to assign src", K(ret), K(other));
  } else if (OB_FAIL(target_.assign(other.target_))) {
    SHARE_LOG(WARN, "failed to assign target", K(ret), K(other));
  }

  return ret;
}


// ObRemapSchemaItemArray
template<typename S, typename T>
int64_t ObRemapSchemaItemArray<S, T>::get_serialize_size(void) const
{
  return remap_items_.get_serialize_size();
}

template<typename S, typename T>
int ObRemapSchemaItemArray<S, T>::serialize(SERIAL_PARAMS) const
{
  int ret = OB_SUCCESS;
  if ((NULL == buf) || (buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(remap_items_.serialize(buf, buf_len, pos))) {
    SHARE_LOG(WARN, "failed to serialize", K(ret), KP(buf), K(buf_len), K(pos));
  }
  return ret;
}

template<typename S, typename T>
int ObRemapSchemaItemArray<S, T>::deserialize(DESERIAL_PARAMS)
{
  int ret = OB_SUCCESS;
  common::ObSArray<ObRemapSchemaItem<S, T>> tmp_remap_items;
  if ((NULL == buf) || (data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_FALSE_IT(reset())) {
  } else if (OB_FAIL(tmp_remap_items.deserialize(buf, data_len, pos))) {
    SHARE_LOG(WARN, "failed to deserialize", K(ret), K(buf), K(data_len), K(pos));
  }
  ARRAY_FOREACH(tmp_remap_items, idx) {
    const ObRemapSchemaItem<S, T> &item = tmp_remap_items.at(idx);
    if (OB_FAIL(add_item(item))) {
      SHARE_LOG(WARN, "failed to add item", K(ret), K(idx), K(item));
    }
  }
  return ret;
}


template<typename S, typename T>
int64_t ObRemapSchemaItemArray<S, T>::get_format_serialize_size() const
{
  int64_t size = 1; // include '\0'
  ARRAY_FOREACH_NORET(remap_items_, idx) {
    const ObRemapSchemaItem<S, T> &item = remap_items_.at(idx);
    // item is concatenated with ','
    size += 0 == idx ? 0 : 1;
    size += item.get_format_serialize_size();
  }

  return size;
}

template<typename S, typename T>
int ObRemapSchemaItemArray<S, T>::format_serialize(
    char *buf,
    const int64_t buf_len,
    int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if ((NULL == buf) || (buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid arguments.", K(ret), KP(buf), K(buf_len));
  } else if (remap_items_.empty()) {
    buf[new_pos++] = 0;
  }

  ARRAY_FOREACH(remap_items_, idx) {
    const ObRemapSchemaItem<S, T> &remap_item = remap_items_.at(idx);
    // concatenated items with ','
    if (0 < idx && OB_FAIL(databuff_printf(buf, buf_len, new_pos, "%s", ","))) {
      SHARE_LOG(WARN, "fail to append str", K(ret), K(new_pos), K(buf_len));
    } else if (OB_FAIL(remap_item.format_serialize(buf, buf_len, new_pos))) {
      SHARE_LOG(WARN, "fail to format str", K(ret), K(new_pos), K(buf_len));
    }
  }

  if (OB_SUCC(ret)) {
    pos = new_pos;
  }

  return ret;
}

template<typename S, typename T>
void ObRemapSchemaItemArray<S, T>::reset()
{
  remap_items_.reset();
  allocator_.reset();
}

template<typename S, typename T>
int ObRemapSchemaItemArray<S, T>::assign(const ObRemapSchemaItemArray<S, T> &other)
{
  int ret = OB_SUCCESS;
  const common::ObSArray<ObRemapSchemaItem<S, T>> &remap_items = other.get_remap_items();

  reset();
  ARRAY_FOREACH(remap_items, idx) {
    const ObRemapSchemaItem<S, T> &item = remap_items.at(idx);
    if (OB_FAIL(add_item(item))) {
      SHARE_LOG(WARN, "failed to add item", K(ret), K(idx), K(item));
    }
  }

  return ret;
}

template<typename S, typename T>
bool ObRemapSchemaItemArray<S, T>::is_src_exist(const S &src) const
{
  bool is_exist = false;
  ARRAY_FOREACH_NORET(remap_items_, idx) {
    const ObRemapSchemaItem<S, T> &tmp = remap_items_.at(idx);
    if (tmp.src_ == src) {
      is_exist = true;
      break;
    }
  }
  return is_exist;
}

template<typename S, typename T>
bool ObRemapSchemaItemArray<S, T>::is_src_exist(
    const S &src,
    const S *&out) const
{
  bool is_exist = false;
  ARRAY_FOREACH_NORET(remap_items_, idx) {
    const ObRemapSchemaItem<S, T> &tmp = remap_items_.at(idx);
    if (tmp.src_ == src) {
      is_exist = true;
      out = &tmp.src_;
      break;
    }
  }
  return is_exist;
}

template<typename S, typename T>
bool ObRemapSchemaItemArray<S, T>::is_target_exist(const T &target) const
{
  bool is_exist = false;
  ARRAY_FOREACH_NORET(remap_items_, idx) {
    const ObRemapSchemaItem<S, T> &tmp = remap_items_.at(idx);
    if (tmp.target_ == target) {
      is_exist = true;
      break;
    }
  }
  return is_exist;
}

template<typename S, typename T>
bool ObRemapSchemaItemArray<S, T>::is_target_exist(
    const T &target,
    const T *&out) const
{
  bool is_exist = false;
  ARRAY_FOREACH_NORET(remap_items_, idx) {
    const ObRemapSchemaItem<S, T> &tmp = remap_items_.at(idx);
    if (tmp.target_ == target) {
      is_exist = true;
      out = &tmp.target_;
      break;
    }
  }
  return is_exist;
}

template<typename S, typename T>
bool ObRemapSchemaItemArray<S, T>::is_remap_target_exist(const S &src, const T *&out) const
{
  bool is_exist = false;
  ARRAY_FOREACH_NORET(remap_items_, idx) {
    const ObRemapSchemaItem<S, T> &tmp = remap_items_.at(idx);
    if (tmp.src_ == src) {
      out = &tmp.target_;
      is_exist = true;
      break;
    }
  }

  return is_exist;
}

template<typename S, typename T>
int ObRemapSchemaItemArray<S, T>::add_item(const ObRemapSchemaItem<S, T> &item)
{
  int ret = OB_SUCCESS;
  ObRemapSchemaItem<S, T> remap_item;
  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(item));
  } else if (OB_FAIL(remap_item.deep_copy(allocator_, item))) {
    SHARE_LOG(WARN, "failed to copy item", K(ret), K(item));
  } else if (OB_FAIL(remap_items_.push_back(remap_item))) {
    SHARE_LOG(WARN, "failed to push back item", K(ret), K(item));
  } else {
    SHARE_LOG(INFO, "add one remap item", K(item));
  }
  return ret;
}

}
}