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

#ifndef OCEANBASE_STORAGE_OB_TABLET_OBJ_LOAD_HELPER
#define OCEANBASE_STORAGE_OB_TABLET_OBJ_LOAD_HELPER

#include <stdint.h>
#include "lib/ob_errno.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase
{
namespace common
{
class ObArenaAllocator;
}

namespace storage
{
class ObMetaDiskAddr;

class ObTabletObjLoadHelper
{
public:
  template <typename T>
  static int alloc_and_new(common::ObIAllocator &allocator, T *&ptr);

  static int read_from_addr(
    common::ObArenaAllocator &allocator,
    const ObMetaDiskAddr &meta_addr,
    char *&buf,
    int64_t &buf_len);
};

template <typename T>
int ObTabletObjLoadHelper::alloc_and_new(common::ObIAllocator &allocator, T *&ptr)
{
  int ret = common::OB_SUCCESS;
  void *buffer = allocator.alloc(sizeof(T));

  if (OB_ISNULL(buffer)) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to allocate memory", K(ret), "size", sizeof(T));
  } else {
    ptr = new (buffer) T();
  }

  return ret;
}
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_OBJ_LOAD_HELPER
