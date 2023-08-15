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

#define USING_LOG_PREFIX COMMON
#include "ob_errsim_module_type.h"

namespace oceanbase
{
using namespace lib;

namespace common
{

static const char *OB_ERRSIM_MODULE_TYPES[] = {
    "NONE",
    "ALL",
    "MIGRATION",
    "TRANSFER",
};

void ObErrsimModuleType::reset()
{
  type_ = ObErrsimModuleType::ERRSIM_MODULE_MAX;
}

bool ObErrsimModuleType::is_valid() const
{
  return ObErrsimModuleTypeHelper::is_valid(type_);
}

const char *ObErrsimModuleType::get_str()
{
  return ObErrsimModuleTypeHelper::get_str(type_);
}

bool ObErrsimModuleType::operator == (const ObErrsimModuleType &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else {
    is_same = type_ == other.type_;
  }
  return is_same;
}

int64_t ObErrsimModuleType::hash() const
{
  int64_t hash_value = 0;
  hash_value = common::murmurhash(
      &type_, sizeof(type_), hash_value);
  return hash_value;
}

int ObErrsimModuleType::hash(uint64_t &hash_val) const
{
  hash_val = hash();
  return OB_SUCCESS;
}

OB_SERIALIZE_MEMBER(ObErrsimModuleType, type_);

const char *ObErrsimModuleTypeHelper::get_str(const ObErrsimModuleType::TYPE &type)
{
  const char *str = nullptr;
  if (!is_valid(type)) {
    str = "UNKNOWN";
  } else {
    str = OB_ERRSIM_MODULE_TYPES[type];
  }
  return str;
}

ObErrsimModuleType::TYPE ObErrsimModuleTypeHelper::get_type(const char *type_str)
{
  ObErrsimModuleType::TYPE type = ObErrsimModuleType::ERRSIM_MODULE_MAX;

  const int64_t count = ARRAYSIZEOF(OB_ERRSIM_MODULE_TYPES);
  STATIC_ASSERT(static_cast<int64_t>(ObErrsimModuleType::ERRSIM_MODULE_MAX) == count, "type count mismatch");
  for (int64_t i = 0; i < count; ++i) {
    if (0 == strcmp(type_str, OB_ERRSIM_MODULE_TYPES[i])) {
      type = static_cast<ObErrsimModuleType::TYPE>(i);
      break;
    }
  }
  return type;
}

bool ObErrsimModuleTypeHelper::is_valid(const ObErrsimModuleType::TYPE &type)
{
  return type >=  ObErrsimModuleType::ERRSIM_MODULE_NONE
      && type < ObErrsimModuleType::ERRSIM_MODULE_MAX;
}


} //common
} //oceanbase
