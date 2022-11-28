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

#ifndef SRC_LIBRARY_SRC_LIB_RESTORE_OB_STORAGE_COS_OBJ_H_
#define SRC_LIBRARY_SRC_LIB_RESTORE_OB_STORAGE_COS_OBJ_H_

#include <sys/types.h>
#include <sys/stat.h>
#include "lib/string/ob_string.h"
#include "lib/allocator/page_arena.h"
#include "ob_cos_wrapper.h"

namespace oceanbase
{
namespace common
{

// Allocator for creating cos handle
class ObCosCtxAllocator
{
public:
  ObCosCtxAllocator();
  virtual ~ObCosCtxAllocator();
  void *alloc(size_t size);
  void free(void *addr);
  void reuse();
private:
  ObArenaAllocator allocator_;
};

// Object type, normal object or container.
enum ObCosObjectType
{
  COS_OBJECT_NORMAL = 0,
  COS_OBJECT_CONTAINER,
  COS_OBJECT_TYPE_MAX,
};


// ObCosBase
class ObCosBase
{
public:
  ObCosBase() : account_(NULL) {}
  virtual ~ObCosBase() {}
  int build_account(const ObString &storage_info);
  // user account, include id & key, etc.
  qcloud_cos::ObCosAccount *account_;
private:
  // allocator for creating account
  ObCosCtxAllocator cos_ctx_allocator_;
};

//obcosobject is the ctx of single file
class ObCosObject
{
public:
  ObCosObject();
  ObCosObject(ObCosBase* cos_base);
  virtual ~ObCosObject() {};
    // Returned the cos handle that user can use to access CosWrapper
  // directly.
  // Note you need build the account by calling build_account before
  // create the handle.
  qcloud_cos::ObCosWrapper::Handle *create_cos_handle();
  // release cos handle, the created handle cannot use anymore.
  void destroy_cos_handle(qcloud_cos::ObCosWrapper::Handle *handle);
    // convert an ObString structure to qcloud_cos::CosStringBuffer
  static inline qcloud_cos::CosStringBuffer obstring_to_string_buffer(const ObString &bucket)
  {
    return qcloud_cos::CosStringBuffer(bucket.ptr(), bucket.length());
  }

  const ObString& bucket_name_string() const { return bucket_name_string_; }

  const ObString& object_name_string() const { return object_name_string_; }

  const qcloud_cos::CosStringBuffer& bucket_name() const { return bucket_name_; }

  const qcloud_cos::CosStringBuffer& object_name() const { return object_name_; }

  int build_bucket_and_object_name(const ObString &uri);
  int head_meta(const ObString &uri, bool &is_exist, qcloud_cos::CosObjectMeta &meta);
  int del(const ObString &uri, int64_t &deleted_cnt);
  virtual int head_meta(bool &is_exist, qcloud_cos::CosObjectMeta &meta);
  virtual int del(int64_t &deleted_cnt);

  ObCosBase* cos_base_;
protected:
  // bucket and object name of object
  ObString bucket_name_string_;
  ObString object_name_string_;

  // bucket and object name used for CosWrapper
  qcloud_cos::CosStringBuffer bucket_name_;
  qcloud_cos::CosStringBuffer object_name_;
 private:
  // allocator for object/bucket name/handle
  ObCosCtxAllocator allocator_;
};


}
}

#endif