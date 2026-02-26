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
