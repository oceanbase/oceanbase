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

#ifndef OCEANBASE_SHARE_OB_SUB_COLUMN_PATH_H_
#define OCEANBASE_SHARE_OB_SUB_COLUMN_PATH_H_
#include "lib/container/ob_se_array.h"
#include "lib/list/ob_list.h"

namespace oceanbase
{
namespace share
{

struct ObSubColumnPathItem
{
public:
  OB_UNIS_VERSION(1);

public:
  enum Type {
    INVALID = 0,
    OBJECT = 1,
    DICT_KEY = 2,
    ARRAY = 3,
    MAX
  };

  ObSubColumnPathItem():
    type_(INVALID), key_()
  {}
  ~ObSubColumnPathItem() {}

  Type type_;
  union {
    ObString key_;
    int64_t array_idx_;
    int64_t id_;
  };

  int64_t to_string(char *buf, const int64_t buf_len) const;
  int compare(const ObSubColumnPathItem &other, const bool use_lexicographical_order) const;

  bool is_array() const  { return type_ == ARRAY; }
  bool is_object() const  { return type_ == OBJECT; }
  bool is_dict_key() const  { return type_ == DICT_KEY; }

  // small legnth than serde
  int encode(char *buf, const int64_t buf_len, int64_t &pos) const;
  int decode(const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_encode_size() const;
};

class ObSubColumnPath
{
public:
  OB_UNIS_VERSION(1);

public:
  static int parse_sub_column_path(const ObString& json_path, ObSubColumnPath &col_path);

public:
  ObSubColumnPath();
  ~ObSubColumnPath() {}
  int assign(const ObSubColumnPath &path) { return items_.assign(path.items_); }
  void reset() { items_.reset(); }
  void reuse() { items_.reuse(); }

  int add_path_item(const ObSubColumnPathItem::Type type, const ObString& key);
  int add_path_item(const ObSubColumnPathItem::Type type, const int64_t array_idx);
  void pop_back() { items_.pop_back();}
  int deep_copy(ObIAllocator& allocator, const ObSubColumnPath &other);
  const ObSubColumnPathItem& get_path_item(const int i) const  { return items_.at(i); }
  ObSubColumnPathItem& get_path_item(const int i) { return items_.at(i); }
  int64_t get_path_item_count() const { return items_.count(); }
  int compare(const ObSubColumnPath &other, const bool use_lexicographical_order) const;
  bool is_prefix(const ObSubColumnPath &other, const bool use_lexicographical_order) const;

  // small legnth than serde
  int encode(char *buf, const int64_t buf_len, int64_t &pos) const;
  int decode(const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_encode_size() const;

  TO_STRING_KV(K_(items));

private:
  ObSEArray<ObSubColumnPathItem, 10> items_;
};

}  // end namespace share
}  // end namespace oceanbase

#endif  // OCEANBASE_SHARE_OB_SUB_COLUMN_PATH_H_