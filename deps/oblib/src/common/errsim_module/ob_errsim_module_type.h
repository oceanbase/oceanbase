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

#ifndef SRC_SHARE_ERRSIM_MODULE_OB_ERRSIM_MODULE_TYPE_H_
#define SRC_SHARE_ERRSIM_MODULE_OB_ERRSIM_MODULE_TYPE_H_

#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace common
{

struct ObErrsimModuleType final
{
  OB_UNIS_VERSION(1);
public:
  enum TYPE
  {
    ERRSIM_MODULE_NONE = 0,
    ERRSIM_MODULE_ALL = 1,
    ERRSIM_MODULE_MIGRATION = 2,
    ERRSIM_MODULE_TRANSFER = 3,
    ERRSIM_MODULE_MAX
  };
  ObErrsimModuleType() : type_(ERRSIM_MODULE_NONE) {}
  explicit ObErrsimModuleType(const ObErrsimModuleType::TYPE &type) : type_(type) {}
  ~ObErrsimModuleType() = default;
  void reset();
  bool is_valid() const;
  const char *get_str();
  bool operator == (const ObErrsimModuleType &other) const;
  int hash(uint64_t &hash_val) const;
  int64_t hash() const;
  TO_STRING_KV(K_(type));
  TYPE type_;
};

struct ObErrsimModuleTypeHelper final
{
  static const char *get_str(const ObErrsimModuleType::TYPE &type);
  static ObErrsimModuleType::TYPE get_type(const char *type_str);
  static bool is_valid(const ObErrsimModuleType::TYPE &type);
  static const int64_t MAX_TYPE_NAME_LENGTH = 16;
};


} // namespace common
} // namespace oceanbase
#endif
