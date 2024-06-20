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
#define USING_LOG_PREFIX STORAGE

#include "storage/direct_load/ob_direct_load_struct.h"

namespace oceanbase
{
namespace storage
{

/**
 * ObDirectLoadMode
 */

DEFINE_ENUM_FUNC(ObDirectLoadMode::Type, type, OB_DIRECT_LOAD_MODE_DEF, ObDirectLoadMode::);

bool ObDirectLoadMode::is_type_valid(const Type type)
{
  return type > INVALID_MODE && type < MAX_MODE;
}

/**
 * ObDirectLoadMethod
 */

DEFINE_ENUM_FUNC(ObDirectLoadMethod::Type, type, OB_DIRECT_LOAD_METHOD_DEF, ObDirectLoadMethod::);

bool ObDirectLoadMethod::is_type_valid(const Type type)
{
  return type > INVALID_METHOD && type < MAX_METHOD;
}

/**
 * ObDirectLoadInsertMode
 */

DEFINE_ENUM_FUNC(ObDirectLoadInsertMode::Type, type, OB_DIRECT_LOAD_INSERT_MODE_DEF, ObDirectLoadInsertMode::);

bool ObDirectLoadInsertMode::is_type_valid(const Type type)
{
  return type > INVALID_INSERT_MODE && type < MAX_INSERT_MODE;
}

} // namespace storage
} // namespace oceanbase
