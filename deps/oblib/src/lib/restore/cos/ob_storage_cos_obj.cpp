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
#include "ob_storage_cos_obj.h"

namespace oceanbase
{
namespace common
{

/**
 * ------------------------------ObCosCtxAllocator---------------------
 */
ObCosCtxAllocator::ObCosCtxAllocator()
  : allocator_(ObModIds::BACKUP, OB_MALLOC_NORMAL_BLOCK_SIZE)
{
}

ObCosCtxAllocator::~ObCosCtxAllocator()
{
}

void* ObCosCtxAllocator::alloc(size_t size)
{
  return allocator_.alloc(size);
}

void ObCosCtxAllocator::free(void *addr)
{
  allocator_.free(addr);
}

void ObCosCtxAllocator::reuse()
{
  allocator_.reuse();
}


// Memory function used for cos context
static void *ob_cos_malloc(void *opaque, size_t size)
{
  void *buf = NULL;
  if (NULL != opaque) {
    ObCosCtxAllocator *allocator = reinterpret_cast<ObCosCtxAllocator*> (opaque);
    buf = allocator->alloc(size);
  }
  return buf;
}

static void ob_cos_free(void *opaque, void *address)
{
  if (NULL != opaque) {
    ObCosCtxAllocator *allocator = reinterpret_cast<ObCosCtxAllocator*> (opaque);
    allocator->free(address);
  }
}

/**
 * ------------------------------ObCosBase---------------------
 */

int ObCosBase::build_account(const ObString &storage_info)
{
  int ret = OB_SUCCESS;
  if (NULL == account_) {
    account_ = static_cast<qcloud_cos::ObCosAccount *>(cos_ctx_allocator_.alloc(sizeof(qcloud_cos::ObCosAccount)));
    if (NULL == account_) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "failed to alloc account", K(ret), K(storage_info));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(account_->parse_from(storage_info.ptr(), storage_info.length()))) {
      OB_LOG(WARN, "failed to parse storage info", K(ret), K(storage_info));
    }
  }

  return ret;
}

int ObCosObject::build_bucket_and_object_name(const ObString &uri)
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
            bucket_name_string_.assign_ptr(bucket_name_buff, strlen(bucket_name_buff) + 1);// must include '\0'
            bucket_name_ = obstring_to_string_buffer(bucket_name_string_);
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
        object_name_string_.assign_ptr(object_name_buff, strlen(object_name_buff) + 1);//must include '\0'
        object_name_ = obstring_to_string_buffer(object_name_string_);
      }
    }
  }

  if (OB_SUCC(ret)) {
    OB_LOG(DEBUG, "get bucket object name ", K(uri), K(bucket_name_string_), K(object_name_string_));
  }

  return ret;
}
qcloud_cos::ObCosWrapper::Handle *ObCosObject::create_cos_handle()
{
  int ret = OB_SUCCESS;
  qcloud_cos::ObCosWrapper::Handle *h = NULL;
  qcloud_cos::OB_COS_customMem cos_mem = {ob_cos_malloc, ob_cos_free, &allocator_};
  if (OB_ISNULL(cos_base_)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "cos base is null!", K(ret));
  } else if (OB_FAIL(qcloud_cos::ObCosWrapper::create_cos_handle(cos_mem, *(cos_base_->account_), &h))) {
    OB_LOG(WARN, "failed to create cos handle", K(ret));
  } else if (OB_ISNULL(h)) {
    ret = OB_COS_ERROR;
    OB_LOG(WARN, "create handle succeed, but returned handle is null", K(ret));
  }

  return h;
}

void ObCosObject::destroy_cos_handle(qcloud_cos::ObCosWrapper::Handle *handle)
{
  if (NULL != handle) {
    qcloud_cos::ObCosWrapper::destroy_cos_handle(handle);
  }
}

/**
 * ------------------------------ObCosObject---------------------
 */
int ObCosObject::head_meta(const ObString &uri,
                           bool &is_exist,
						   qcloud_cos::CosObjectMeta &meta)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(build_bucket_and_object_name(uri))) {
    OB_LOG(WARN, "failed to build object", K(ret), K(uri));
  } else if (OB_FAIL(head_meta(is_exist, meta))) {
    OB_LOG(WARN, "failed to head meta", K(ret), K(uri));
  }

  return ret;
}

int ObCosObject::del(const ObString &uri, int64_t &deleted_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(build_bucket_and_object_name(uri))) {
    OB_LOG(WARN, "failed to build object", K(ret), K(uri));
  } else if (OB_FAIL(del(deleted_cnt))) {
    OB_LOG(WARN, "failed to del object", K(ret), K(uri), K(deleted_cnt));
  }

  return ret;
}

ObCosObject::ObCosObject(ObCosBase* cos_base) : cos_base_(cos_base)
{
}

ObCosObject::ObCosObject() : cos_base_(NULL)
{
}

int ObCosObject::head_meta(
  bool &is_exist,
  qcloud_cos::CosObjectMeta &meta)
{
  int ret = OB_SUCCESS;
  qcloud_cos::ObCosWrapper::Handle *h = NULL;
  if (OB_ISNULL(cos_base_)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "cos base is not init", K(ret));
  } else if (OB_ISNULL(h = create_cos_handle())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "create cos handle failed", K(ret), K(bucket_name_string_), K(object_name_string_));
  } else if (OB_FAIL(qcloud_cos::ObCosWrapper::head_object_meta(h, bucket_name_, object_name_, is_exist, meta))) {
    OB_LOG(WARN, "failed to head object meta from cos", K(ret), K(bucket_name_string_), K(object_name_string_));
  }

  meta.type = ObCosObjectType::COS_OBJECT_NORMAL;
  destroy_cos_handle(h);
  return ret;
}

int ObCosObject::del(int64_t &deleted_cnt)
{
  int ret = OB_SUCCESS;
  deleted_cnt = 0;
  qcloud_cos::ObCosWrapper::Handle *h = create_cos_handle();

  if (OB_ISNULL(cos_base_)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "cos base is not init", K(ret));
  }
  if (NULL == h) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "create cos handle failed", K(ret), K(bucket_name_string_), K(object_name_string_));
  } else {
    OB_LOG(DEBUG, "start to delete object", K(bucket_name_string_), K(object_name_string_));
    if (OB_FAIL(qcloud_cos::ObCosWrapper::del(h, bucket_name_, object_name_))) {
      OB_LOG(WARN, "failed to delete object from cos", K(ret), K(bucket_name_string_), K(object_name_string_));
    } else {
      deleted_cnt = 1;
      OB_LOG(DEBUG, "succeed to delete object from cos", K(ret), K(bucket_name_string_), K(object_name_string_));
    }
  }
  destroy_cos_handle(h);
  return ret;
}




}
}
