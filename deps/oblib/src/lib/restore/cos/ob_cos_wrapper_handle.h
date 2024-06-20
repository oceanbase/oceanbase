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

#ifndef SRC_LIBRARY_SRC_LIB_RESTORE_OB_COS_WRAPPER_HANDLE_H_
#define SRC_LIBRARY_SRC_LIB_RESTORE_OB_COS_WRAPPER_HANDLE_H_

#include <sys/types.h>
#include <sys/stat.h>
#include "lib/string/ob_string.h"
#include "lib/allocator/page_arena.h"
#include "lib/restore/ob_storage_info.h"
#include "ob_cos_wrapper.h"
#include "lib/restore/ob_i_storage.h"

namespace oceanbase
{
namespace common
{

// Allocator for creating cos handle
class ObCosMemAllocator
{
public:
  ObCosMemAllocator();
  virtual ~ObCosMemAllocator();
  void *alloc(size_t size);
  void free(void *addr);
  void reuse();
private:
  ObArenaAllocator allocator_;
};

// Create a temporary cos_handle object,
// utilizing the 'allocator' to allocate the necessary memory.
int create_cos_handle(
    ObCosMemAllocator &allocator,
    const qcloud_cos::ObCosAccount &cos_account,
    const bool check_md5,
    qcloud_cos::ObCosWrapper::Handle *&handle);

class ObCosWrapperHandle
{
public:
  ObCosWrapperHandle();
  virtual ~ObCosWrapperHandle() {};

  int init(const common::ObObjectStorageInfo *storage_info);
  void reset();
  int create_cos_handle(const bool check_md5);
  void destroy_cos_handle();
  qcloud_cos::ObCosWrapper::Handle *get_ptr() { return handle_; }

  int build_bucket_and_object_name(const ObString &uri);
  const ObString &get_bucket_name() const { return bucket_name_; }
  const ObString &get_object_name() const { return object_name_; }
  const qcloud_cos::ObCosAccount &get_cos_account() const { return cos_account_; }

  bool is_valid() const { return is_inited_ && handle_ != nullptr; }
  bool is_inited() const { return is_inited_; }

  int set_delete_mode(const char *parameter);
  int64_t get_delete_mode() const { return delete_mode_; }

  void *alloc_mem(size_t size);
  void free_mem(void *addr);

private:
  bool is_inited_;
  qcloud_cos::ObCosWrapper::Handle *handle_;
  qcloud_cos::ObCosAccount cos_account_;
  ObCosMemAllocator allocator_;
  ObString bucket_name_;
  ObString object_name_;
  int64_t delete_mode_;
};


} // common
} // oceanbase
#endif