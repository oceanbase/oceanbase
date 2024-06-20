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

#include "lib/utility/ob_print_utils.h"
#include "ob_cos_wrapper_handle.h"

namespace oceanbase
{
namespace common
{
using namespace oceanbase::common;

/**
 * ------------------------------ObCosMemAllocator---------------------
 */
ObCosMemAllocator::ObCosMemAllocator()
  : allocator_(ObModIds::BACKUP, OB_MALLOC_NORMAL_BLOCK_SIZE)
{
}

ObCosMemAllocator::~ObCosMemAllocator()
{
}

void* ObCosMemAllocator::alloc(size_t size)
{
  return allocator_.alloc(size);
}

void ObCosMemAllocator::free(void *addr)
{
  allocator_.free(addr);
}

void ObCosMemAllocator::reuse()
{
  allocator_.reuse();
}


// Memory function used for cos context
static void *ob_cos_malloc(void *opaque, size_t size)
{
  void *buf = NULL;
  if (NULL != opaque) {
    ObCosMemAllocator *allocator = reinterpret_cast<ObCosMemAllocator*> (opaque);
    buf = allocator->alloc(size);
  }
  return buf;
}

static void ob_cos_free(void *opaque, void *address)
{
  if (NULL != opaque) {
    ObCosMemAllocator *allocator = reinterpret_cast<ObCosMemAllocator*> (opaque);
    allocator->free(address);
  }
}

/*--------------------------------ObCosWrapperHandle-----------------------------------*/
ObCosWrapperHandle::ObCosWrapperHandle()
  : is_inited_(false), handle_(nullptr), cos_account_(), allocator_(),
    delete_mode_(ObIStorageUtil::DELETE)
{}

int ObCosWrapperHandle::init(const ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  char storage_info_str[OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = { 0 };

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "init twice", K(ret));
  } else if (OB_ISNULL(storage_info)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "storage info is null", K(ret));
  } else if (OB_FAIL(storage_info->get_storage_info_str(storage_info_str, sizeof(storage_info_str)))) {
    OB_LOG(WARN, "fail to get cos storage info str", K(ret), K(storage_info));
  } else if (OB_FAIL(cos_account_.parse_from(storage_info_str, strlen(storage_info_str)))) {
    OB_LOG(WARN, "fail to parse cos account from storage info str", K(ret));
  } else if (strlen(cos_account_.delete_mode_) > 0 && OB_FAIL(set_delete_mode(cos_account_.delete_mode_))) {
    OB_LOG(WARN, "fail to set cos delete mode", K(cos_account_.delete_mode_), K(ret));
  } {
    is_inited_ = true;
  }
  return ret;
}

void ObCosWrapperHandle::reset()
{
  destroy_cos_handle();
  is_inited_ = false;
  delete_mode_ = ObIStorageUtil::DELETE;
}

int create_cos_handle(
    ObCosMemAllocator &allocator,
    const qcloud_cos::ObCosAccount &cos_account,
    const bool check_md5,
    qcloud_cos::ObCosWrapper::Handle *&handle)
{
  int ret = OB_SUCCESS;
  handle = nullptr;
  qcloud_cos::OB_COS_customMem cos_mem = {ob_cos_malloc, ob_cos_free, &allocator};
  if (OB_FAIL(qcloud_cos::ObCosWrapper::create_cos_handle(cos_mem, cos_account,
                                                          check_md5, &handle))) {
    OB_LOG(WARN, "failed to create tmp cos handle", K(ret));
  } else if (OB_ISNULL(handle)) {
    ret = OB_COS_ERROR;
    OB_LOG(WARN, "create tmp handle succeed, but returned handle is null", K(ret));
  }

  return ret;
}

int ObCosWrapperHandle::create_cos_handle(const bool check_md5)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(handle_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "handle is not null", K(ret));
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "handle is not inited", K(ret));
  } else if (OB_FAIL(common::create_cos_handle(allocator_, cos_account_, check_md5, handle_))) {
    OB_LOG(WARN, "failed to create cos handle", K(ret));
  }
  return ret;
}

void ObCosWrapperHandle::destroy_cos_handle()
{
  if (nullptr != handle_) {
    qcloud_cos::ObCosWrapper::destroy_cos_handle(handle_);
    handle_ = nullptr;
  }
}

int ObCosWrapperHandle::build_bucket_and_object_name(const ObString &uri)
{
  int ret = OB_SUCCESS;
  ObString::obstr_size_t bucket_start = 0;
  ObString::obstr_size_t bucket_end = 0;
  ObString::obstr_size_t object_start = 0;
  char* bucket_name_buff = NULL;
  char* object_name_buff = NULL;

  if (!uri.prefix_match(OB_COS_PREFIX)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "uri is invalid ", KCSTRING(OB_COS_PREFIX), K(uri), K(ret));
  } else {
    bucket_start = static_cast<ObString::obstr_size_t>(strlen(OB_COS_PREFIX));
    for (int64_t i = bucket_start; OB_SUCC(ret) && i < uri.length() - 2; i++) {
      if ('/' == *(uri.ptr() + i) && '/' == *(uri.ptr() + i + 1)) {
        ret = OB_INVALID_ARGUMENT;
        OB_LOG(WARN, "uri has two // ", K(uri), K(ret), K(i));
        break;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(bucket_name_buff = static_cast<char *>(allocator_.alloc(OB_MAX_URI_LENGTH)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "failed to alloc bucket name buff", K(ret), K(uri));
    } else if (OB_ISNULL(object_name_buff = static_cast<char *>(allocator_.alloc(OB_MAX_URI_LENGTH)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "failed to alloc object name buff", K(ret), K(uri));
    } else {
      //make sure this last always 0
      bucket_name_buff[0] = '\0';
      object_name_buff[0] = '\0';
    }
  }

  // parse bucket name
  if (OB_SUCC(ret)) {
    if (bucket_start >= uri.length()) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "bucket name is NULL", K(uri), K(ret));
    } else {
      for (bucket_end = bucket_start; OB_SUCC(ret) && bucket_end < uri.length(); ++bucket_end) {
        if ('/' == *(uri.ptr() + bucket_end)) {
          ObString::obstr_size_t bucket_length = bucket_end - bucket_start;
          //must end with '\0'
          if (OB_FAIL(databuff_printf(bucket_name_buff, OB_MAX_URI_LENGTH, "%.*s", bucket_length, uri.ptr() + bucket_start))) {
            OB_LOG(WARN, "fail to deep copy bucket", K(uri), K(bucket_start), K(bucket_length), K(ret));
          } else {
            bucket_name_.assign_ptr(bucket_name_buff, strlen(bucket_name_buff) + 1);// must include '\0'
          }
          break;
        }
      }

      if (OB_SUCC(ret) && bucket_end == uri.length()) {
        ret = OB_INVALID_ARGUMENT;
        OB_LOG(WARN, "bucket name is invalid", K(uri), K(ret));
      }
    }
  }

  // parse the object name
  if (OB_SUCC(ret)) {
    if (bucket_end + 1 >= uri.length()) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "object name is NULL", K(uri), K(ret));
    } else {
      object_start = bucket_end + 1;
      ObString::obstr_size_t object_length = uri.length() - object_start;
      //must end with '\0'
      if (OB_FAIL(databuff_printf(object_name_buff, OB_MAX_URI_LENGTH, "%.*s", object_length, uri.ptr() + object_start))) {
        OB_LOG(WARN, "fail to deep copy object", K(uri), K(object_start), K(object_length), K(ret));
      } else {
        object_name_.assign_ptr(object_name_buff, strlen(object_name_buff) + 1);//must include '\0'
      }
    }
  }

  if (OB_SUCC(ret)) {
    OB_LOG(DEBUG, "get bucket object name", K(uri), K_(bucket_name), K_(object_name));
  }

  return ret;
}

int ObCosWrapperHandle::set_delete_mode(const char *parameter)
{
  int ret = OB_SUCCESS;
  if (NULL == parameter) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid args", K(ret), KP(parameter));
  } else if (0 == strcmp(parameter, "delete")) {
    delete_mode_ = ObIStorageUtil::DELETE;
  } else if (0 == strcmp(parameter, "tagging")) {
    delete_mode_ = ObIStorageUtil::TAGGING;
  } else {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "delete mode is invalid", K(ret), K(parameter));
  }
  return ret;
}

void* ObCosWrapperHandle::alloc_mem(size_t size)
{
  return allocator_.alloc(size);
}

void ObCosWrapperHandle::free_mem(void *addr)
{
  allocator_.free(addr);
}

} // common
} // oceanbase
