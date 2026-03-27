/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_STORAGE_CHECKED_OBJECT_BASE_H_
#define OCEANBASE_STORAGE_OB_STORAGE_CHECKED_OBJECT_BASE_H_

#include "share/cache/ob_kvcache_struct.h"

namespace oceanbase
{
namespace storage
{

enum class ObStorageCheckID
{
  INVALID_ID,
  ALL_CACHE = MAX_CACHE_NUM,
  IO_HANDLE,
  STORAGE_ITER
};

class ObStorageCheckedObjectBase {
public:
  bool is_traced() const { return is_traced_; }
private:
  bool is_traced_{false};
  friend class ObStorageLeakChecker;
};

};
};

#endif
