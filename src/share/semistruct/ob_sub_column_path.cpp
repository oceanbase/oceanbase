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

#define USING_LOG_PREFIX SHARE
#include "ob_sub_column_path.h"
#include "lib/utility/ob_fast_convert.h"
#include "lib/json_type/ob_json_tree.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace share
{

using namespace common;

OB_DEF_SERIALIZE_SIZE(ObSubColumnPathItem)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(type_);
  if (OBJECT == type_) {
    OB_UNIS_ADD_LEN(key_);
  } else {
    OB_UNIS_ADD_LEN(id_);
  }
  return len;
}

OB_DEF_SERIALIZE(ObSubColumnPathItem)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(type_);
  if (OBJECT == type_) {
    OB_UNIS_ENCODE(key_);
  } else {
    OB_UNIS_ENCODE(id_);
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObSubColumnPathItem)
{
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(type_);
  if (OBJECT == type_) {
    OB_UNIS_DECODE(key_);
  } else {
    OB_UNIS_DECODE(id_);
  }
  return ret;
}

int ObSubColumnPathItem::encode(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(type_);
  if (OBJECT == type_) {
    OB_UNIS_ENCODE(key_);
  } else {
    OB_UNIS_ENCODE(id_);
  }
  return ret;
}

int ObSubColumnPathItem::decode(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(type_);
  if (OBJECT == type_) {
    OB_UNIS_DECODE(key_);
  } else {
    OB_UNIS_DECODE(id_);
  }
  return ret;
}

int64_t ObSubColumnPathItem::get_encode_size() const
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(type_);
  if (OBJECT == type_) {
    OB_UNIS_ADD_LEN(key_);
  } else {
    OB_UNIS_ADD_LEN(id_);
  }
  return len;
}

int64_t ObSubColumnPathItem::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  common::databuff_printf(buf, buf_len, pos, "{");
  common::databuff_print_kv(buf, buf_len, pos, "type", type_);
  common::databuff_printf(buf, buf_len, pos, ", ");
  if (type_ == ARRAY) {
    common::databuff_print_kv(buf, buf_len, pos, "array_idx", array_idx_);
  } else if (type_ == DICT_KEY) {
    common::databuff_print_kv(buf, buf_len, pos, "id", id_);
  } else {
    common::databuff_print_kv(buf, buf_len, pos, "key_len", key_.length());
    common::databuff_print_kv(buf, buf_len, pos, ", key", key_);
  }
  common::databuff_printf(buf, buf_len, pos, "}");
  return pos;
}

int ObSubColumnPathItem::compare(const ObSubColumnPathItem &other, const bool use_lexicographical_order) const
{
  int cmp_ret = type_ - other.type_;
  if (cmp_ret != 0) {
  } else if (type_ == ARRAY) {
    cmp_ret = array_idx_ - other.array_idx_;
  } else if (type_ == DICT_KEY) {
    cmp_ret = id_ - other.id_;
  } else {
    ObJsonKeyCompare key_cmp(use_lexicographical_order);
    cmp_ret = key_cmp.compare(key_, other.key_);
  }
  return cmp_ret;
}

ObSubColumnPath::ObSubColumnPath()
{
  items_.set_attr(lib::ObMemAttr(MTL_ID(), "SemiPath"));
}

OB_DEF_SERIALIZE_SIZE(ObSubColumnPath)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(items_);
  return len;
}

OB_DEF_SERIALIZE(ObSubColumnPath)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(items_);
  return ret;
}

OB_DEF_DESERIALIZE(ObSubColumnPath)
{
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(items_);
  return ret;
}

int64_t ObSubColumnPath::get_encode_size() const
{
  int64_t len = 0;
  int64_t count = items_.count();
  OB_UNIS_ADD_LEN(count);
  for (int64_t i = 0; i < count; ++i) {
    len += items_[i].get_encode_size();
  }
  return len;
}

int ObSubColumnPath::encode(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int64_t count = items_.count();
  OB_UNIS_ENCODE(count);
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    if (OB_FAIL(items_[i].encode(buf, buf_len, pos))) {
      LOG_WARN("encode failed", K(ret), K(i), K(count), K(pos), K(buf_len));
    }
  }
  return ret;
}

int ObSubColumnPath::decode(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  OB_UNIS_DECODE(count);
  if (OB_SUCC(ret) && OB_FAIL(items_.prepare_allocate(count))) {
    LOG_WARN("fail to allocate space", K(ret), K(count));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    if (OB_FAIL(items_.at(i).decode(buf, data_len, pos))) {
      LOG_WARN("decode failed", K(ret), K(i), K(count), K(pos), K(data_len));
    }
  }
  return ret;
}

int ObSubColumnPath::compare(const ObSubColumnPath &other, const bool use_lexicographical_order) const
{
  int cmp_ret = 0;
  int64_t cmp_cnt = OB_MIN(items_.count(), other.items_.count());
  for (int i = 0; 0 == cmp_ret && i < cmp_cnt; ++i) {
    cmp_ret = items_.at(i).compare(other.items_.at(i), use_lexicographical_order);
  }
  if (0 == cmp_ret) {
    cmp_ret = items_.count() - other.items_.count();
  }
  return cmp_ret;
}

bool ObSubColumnPath::is_prefix(const ObSubColumnPath &other, const bool use_lexicographical_order) const
{
  bool res = items_.count() < other.items_.count();
  if (res) {
    for (int i = 0; res && i < items_.count(); ++i) {
      if (items_.at(i).compare(other.items_.at(i), use_lexicographical_order) != 0) {
        res = false;
      }
    }
  }
  return res;
}

int ObSubColumnPath::deep_copy(ObIAllocator& allocator, const ObSubColumnPath &other)
{
  int ret = OB_SUCCESS;
  int64_t path_cnt = other.items_.count();
  if (OB_FAIL(items_.reserve(path_cnt))) {
    LOG_WARN("reserve fail", K(ret), K(path_cnt));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < path_cnt; ++i) {
      const ObSubColumnPathItem& item = other.items_.at(i);
      if (OB_FAIL(items_.push_back(item))) {
        LOG_WARN("push back fail", K(ret), K(i), K(item));
      } else if (item.type_ == ObSubColumnPathItem::OBJECT && OB_FAIL(ob_write_string(allocator, item.key_, items_.at(i).key_))) {
        LOG_WARN("ob_write_string fail", K(ret), K(i), K(item));
      }
    }
  }
  return ret;
}

int ObSubColumnPath::add_path_item(const ObSubColumnPathItem::Type type, const ObString& key)
{
  ObSubColumnPathItem item;
  item.type_ = type;
  item.key_ = key;
  return items_.push_back(item);
}

int ObSubColumnPath::add_path_item(const ObSubColumnPathItem::Type type, const int64_t array_idx)
{
  ObSubColumnPathItem item;
  item.type_ = type;
  item.array_idx_ = array_idx;
  return items_.push_back(item);
}

int ObSubColumnPath::parse_sub_column_path(const ObString& json_path, ObSubColumnPath &col_path)
{
  int ret = OB_SUCCESS;
  const char* ptr = json_path.ptr();
  int64_t len = json_path.length();
  int64_t i = 0;
  while (OB_SUCC(ret) && i < len) {
    if (ptr[i] == '$') {
      ++i;
    } else if (ptr[i] == '.') {
      ++i;
      if (i > len) {
        ret = OB_NOT_SUPPORTED;
        LOG_INFO("not support json path for semistruct", K(i), K(json_path));
      } else {
        int64_t start = i;
        while (i < len && (ptr[i] != '.' && ptr[i] != '[')) {
          ++i;
        }
        ObString key(i - start, ptr + start);
        if (OB_FAIL(col_path.add_path_item(ObSubColumnPathItem::OBJECT, key))) {
          LOG_WARN("add path item fail", K(ret), K(start), K(i), K(key), K(json_path));
        }
      }
    } else if (ptr[i] == '[') {
      ++i;
      if (i > len) {
        ret = OB_NOT_SUPPORTED;
        LOG_INFO("not support json path for semistruct", K(i), K(json_path));
      } else {
        int64_t start = i;
        while (i < len && ptr[i] != ']') {
          ++i;
        }
        if (i >= len) {
          ret = OB_NOT_SUPPORTED;
          LOG_INFO("not support json path for semistruct", K(i), K(json_path));
        } else if (ptr[i] != ']') {
          ret = OB_NOT_SUPPORTED;
          LOG_INFO("not support json path for semistruct", K(i), K(json_path));
        } else {
          bool is_valid = false;
          int64_t array_idx = ObFastAtoi<int64_t>::atoi(ptr + start, ptr + i, is_valid);
          if (! is_valid) {
            ret = OB_NOT_SUPPORTED;
            LOG_INFO("not support json path for semistruct", K(i), K(json_path));
          } else if (OB_FAIL(col_path.add_path_item(ObSubColumnPathItem::ARRAY, array_idx))) {
            LOG_WARN("add path item fail", K(ret), K(start), K(i), K(array_idx), K(json_path));
          } else {
            ++i; // ']'
          }
        }
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_INFO("not support json path for semistruct", K(i), K(json_path));
    }
  }
  return ret;
}

}  // end namespace share
}  // end namespace oceanbase