/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "lib/json/ob_json.h"
#include "lib/hash/ob_hashmap.h"
#include "plugin/sys/ob_plugin_utils.h"

namespace oceanbase {
namespace plugin {

class ObProperties final
{
public:
  ObProperties() = default;
  ~ObProperties();

  int init(ObIAllocator &allocator, bool encrypt_mode = true);
  void destroy();

  int set_property(const ObString &key, const ObString &value);
  int get_property(const ObString &key, ObString &value) const;

  /**
   * @brief mark the value associate to the key sensitive
   * @details the routine will encrypt the value automatically
   * and when we retrieve the value later, it will be
   * decrypted automatically
   */
  int mark_sensitive(const ObString &key);

  bool is_sensitive(const ObString &key) const;

  int64_t to_json_string(char buf[], int64_t len) const;
  int load_from_json_node(json::Value *node);

  int foreach(std::function<int (const ObString&, const ObString&)> callback) const;

  int to_kv_string(char buf[], int64_t buf_len, int64_t &pos,
                   const ObString &sensitive_string,
                   const ObString &kv_delimiter,
                   const ObString &item_delimiter,
                   const ObString &key_quote,
                   const ObString &value_quote) const;

  TO_STRING_KV(K(pairs_.size()));

  int equal_to(const ObProperties &other, bool &bret) const;

private:
  int encrypt(const ObString &src, ObString &encrypted);
  int decrypt(const ObString &src, ObString &decrypted);

private:
  struct PropertyValue
  {
    bool     sensitive = false;
    ObString decrypted_value;
    ObString encrypted_value;

    int64_t to_string(char buf[], int64_t len) const;
    int64_t to_json_string(char buf[], int64_t buf_len) const;
    int from_json(ObIAllocator &allocator, const json::Value *value);

    bool equal_to(const PropertyValue &other) const;

    void reuse();
    void destroy(ObIAllocator &allocator);
  };

public:
  using PropertyMap = common::hash::ObHashMap<ObString,
                                              PropertyValue,
                                              common::hash::LatchReadWriteDefendMode,
                                              ObPluginNameHash,
                                              ObPluginNameEqual>;

private:
  ObIAllocator *allocator_ = nullptr;

  PropertyMap pairs_;
  bool encrypt_mode_ = true;
};

} // namespace plugin
} // namespace oceanbase
