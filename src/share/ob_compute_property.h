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

#ifndef _OB_COMPUTE_PROPERTY_H
#define _OB_COMPUTE_PROPERTY_H

#include "lib/ob_define.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_unify_serialize.h"

namespace oceanbase
{
namespace share
{

enum class Monotonicity : uint8_t
{
  NONE_MONO,
  ASC,
  DESC,
  CONST
};

struct ObAggrParamProperty final
{
  OB_UNIS_VERSION_V(1);
public:
  ObAggrParamProperty() : mono_(Monotonicity::NONE_MONO), is_null_prop_(true) {}
  ObAggrParamProperty(const Monotonicity mono, const bool is_null_prop)
    : mono_(mono),
      is_null_prop_(is_null_prop)
  {}
  ~ObAggrParamProperty() = default;
  int assign(const ObAggrParamProperty &other)
  {
    int ret = common::OB_SUCCESS;
    if (this != &other) {
      mono_ = other.mono_;
      is_null_prop_ = other.is_null_prop_;
    }
    return ret;
  }
  TO_STRING_KV(K_(mono), K_(is_null_prop));
public:
  Monotonicity mono_;
  bool is_null_prop_;
};

}
}

#endif // _OB_COMPUTE_PROPERTY_H
