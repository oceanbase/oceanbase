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

#ifndef OCEANBASE_LIB_OB_HEX_UTILS_BASE_H_
#define OCEANBASE_LIB_OB_HEX_UTILS_BASE_H_

#include "common/object/ob_object.h"
#include "lib/string/ob_string.h"
#include "lib/allocator/ob_allocator.h"

namespace oceanbase
{
namespace common
{

/*
 * move unhex and hex function from ObHexUtils here, because we need them in
 * deps/oblib/src/lib/mysqlclient/ob_mysql_result_impl.cpp for raw type.
 */
class ObHexUtilsBase
{
public:
  static int unhex(const ObString &text, ObIAllocator &alloc, ObObj &result);
  static int hex(const ObString &text, ObIAllocator &alloc, ObObj &result);
  static int unhex(const ObString &text, ObIAllocator &alloc, char *&binary_buf, int64_t &binary_len);
  static int hex(ObString &text, ObIAllocator &alloc, const char *binary_buf, int64_t binary_len);
};
} // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_LIB_OB_HEX_UTILS_BASE_H_
