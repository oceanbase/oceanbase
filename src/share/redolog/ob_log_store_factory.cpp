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

#include "ob_log_store_factory.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/ob_mod_define.h"
#include "storage/blocksstable/ob_store_file_system.h"

using namespace oceanbase::clog;

namespace oceanbase {
namespace common {
/*static*/ ObILogFileStore* ObLogStoreFactory::create(
    const char* log_dir, const int64_t file_size, const ObLogWritePoolType type)
{
  int ret = OB_SUCCESS;
  ObILogFileStore* store = NULL;

  if (OB_ISNULL(log_dir) || file_size <= 0 || ObLogWritePoolType::INVALID_WRITE_POOL == type) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), KP(log_dir), K(type));
  } else if ('/' == log_dir[0] || '.' == log_dir[0]) {
    if (NULL == (store = OB_NEW(ObLogFileStore, ObModIds::OB_LOG_FILE_STORE))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "alloc file store failed.", K(ret), KP(store));
    } else if (OB_FAIL(store->init(log_dir, file_size, type))) {
      COMMON_LOG(WARN, "init file store failed.", K(ret));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid log file store", K(ret), K(log_dir));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(store)) {
    destroy(store);
  }

  return store;
}

/*static*/ void ObLogStoreFactory::destroy(ObILogFileStore*& store)
{
  if (OB_NOT_NULL(store)) {
    store->destroy();
    OB_DELETE(ObILogFileStore, ObModIds::OB_LOG_FILE_STORE, store);
    store = NULL;
  }
}

}  // namespace common
}  // namespace oceanbase
