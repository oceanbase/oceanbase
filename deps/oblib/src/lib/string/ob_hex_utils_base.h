/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
