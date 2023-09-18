/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef _OB_CDC_MACRO_UTILS_H_
#define _OB_CDC_MACRO_UTILS_H_

#include "lib/atomic/ob_atomic.h"

namespace oceanbase
{
namespace libobcdc
{

// define private field and corresponding getter and setter method
#define DEFINE_PRIVATE_FIELD(TYPE, FIELD_NAME) \
  private: \
    TYPE    FIELD_NAME##_;

#define DEFINE_PUBLIC_GETTER(TYPE, FIELD_NAME) \
  public: \
    TYPE get_##FIELD_NAME() const { return ATOMIC_LOAD(&FIELD_NAME##_); }

#define DEFINE_PUBLIC_SETTER(TYPE, FIELD_NAME) \
  public: \
    void set_##FIELD_NAME(const TYPE FIELD_NAME) { ATOMIC_SET(&FIELD_NAME##_, FIELD_NAME); }


#define DEFINE_FIELD_WITH_GETTER(TYPE, FIELD_NAME) \
  DEFINE_PRIVATE_FIELD(TYPE, FIELD_NAME); \
  DEFINE_PUBLIC_GETTER(TYPE, FIELD_NAME); \
  DEFINE_PUBLIC_SETTER(TYPE, FIELD_NAME);

// other macros

} // namespace libobcdc
} // namespace oceanbase

#endif
