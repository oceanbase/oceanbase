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

#ifndef OCEANBASE_STORAGE_OB_FILE_SYSTEM_UTIL_H_
#define OCEANBASE_STORAGE_OB_FILE_SYSTEM_UTIL_H_

#include "lib/ob_define.h"
#include "ob_tenant_file_struct.h"

namespace oceanbase {
namespace storage {
class ObFileSystemUtil final {
public:
  static int open_tenant_info_file(const common::ObAddr& svr_addr, const uint64_t tenant_id, const int64_t file_id,
      const int open_flag, blocksstable::ObStorageFile*& tenant_info_file);

  static int open_tenant_file(const common::ObAddr& svr_addr, const uint64_t tenant_id, const int64_t file_id,
      const int open_flag, blocksstable::ObStorageFile*& tenant_file);

  static int get_pg_file_with_guard(const common::ObPartitionKey& pkey, storage::ObIPartitionGroupGuard& pg_guard,
      blocksstable::ObStorageFile*& pg_file);
  static int get_pg_file_with_guard_by_pg_key(
      const common::ObPGKey& pg_key, storage::ObIPartitionGroupGuard& pg_guard, blocksstable::ObStorageFile*& pg_file);

  static int close_file(blocksstable::ObStorageFile* storage_file);

public:
  static const int CREATE_FLAGS = O_RDWR | O_APPEND | O_CREAT | O_EXCL;
  static const int WRITE_FLAGS = O_RDWR | O_APPEND;
  static const int READ_FLAGS = O_RDONLY;
};
}  // namespace storage
}  // namespace oceanbase

#endif /* OCEANBASE_STORAGE_OB_FILE_SYSTEM_UTIL_H_ */
