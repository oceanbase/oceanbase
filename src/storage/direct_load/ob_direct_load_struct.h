/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "lib/string/ob_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_unify_serialize.h"

namespace oceanbase
{
namespace storage
{

struct ObDirectLoadMode
{
#define OB_DIRECT_LOAD_MODE_DEF(DEF) \
  DEF(INVALID_MODE, = 0)             \
  DEF(LOAD_DATA, = 1)                \
  DEF(INSERT_INTO, = 2)              \
  DEF(TABLE_LOAD, = 3)               \
  DEF(INSERT_OVERWRITE,  = 4)        \
  DEF(MAX_MODE, )

  DECLARE_ENUM(Type, type, OB_DIRECT_LOAD_MODE_DEF, static);

  static bool is_type_valid(const Type type);
  static bool is_insert_overwrite(const Type type) { return INSERT_OVERWRITE == type; }
};

struct ObDirectLoadMethod
{
#define OB_DIRECT_LOAD_METHOD_DEF(DEF) \
  DEF(INVALID_METHOD, = 0)             \
  DEF(FULL, = 1)                       \
  DEF(INCREMENTAL, = 2)                \
  DEF(MAX_METHOD, )

  DECLARE_ENUM(Type, type, OB_DIRECT_LOAD_METHOD_DEF, static);

  static bool is_type_valid(const Type type);
  static bool is_full(const Type type) { return FULL == type; }
  static bool is_incremental(const Type type) { return INCREMENTAL == type; }
};

struct ObDirectLoadInsertMode
{
#define OB_DIRECT_LOAD_INSERT_MODE_DEF(DEF) \
  DEF(INVALID_INSERT_MODE, = 0)             \
  DEF(NORMAL, = 1)                          \
  DEF(INC_REPLACE, = 2)                     \
  DEF(OVERWRITE, = 3)                       \
  DEF(MAX_INSERT_MODE, )

  DECLARE_ENUM(Type, type, OB_DIRECT_LOAD_INSERT_MODE_DEF, static);

  static bool is_type_valid(const Type type);
  static bool is_valid_for_full_method(const Type type) { return NORMAL == type || OVERWRITE == type; }
  static bool is_valid_for_incremental_method(const Type type) { return NORMAL == type || INC_REPLACE == type; }
};

} // namespace storage
} // namespace oceanbase
