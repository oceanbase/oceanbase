/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEBASE_STORAGE_OB_DML_COMMON
#define OCEBASE_STORAGE_OB_DML_COMMON

namespace oceanbase
{
namespace storage
{
enum ObInsertFlag
{
  INSERT_RETURN_ALL_DUP = 0,
  INSERT_RETURN_ONE_DUP = 1,
};
} // namespace storage
} // namespace oceanbase

#endif // OCEBASE_STORAGE_OB_DML_COMMON
