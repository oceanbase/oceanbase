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

#ifndef SRC_LIBRARY_SRC_LIB_OB_COS_WRAPPER_H_
#define SRC_LIBRARY_SRC_LIB_OB_COS_WRAPPER_H_

#include <string.h>
#include "ob_singleton.h"
#include <dirent.h>

namespace oceanbase
{
namespace common
{

// Naming qcloud_cos to avoid conflicts with the function called cos.
namespace qcloud_cos
{

#define OB_PUBLIC_API __attribute__ ((visibility ("default")))

class OB_PUBLIC_API ObCosEnv : public ObSingleton<ObCosEnv>
{
public:
  struct Conf
  {
    static const int64_t MIN_COS_SLICE_SIZE = 32000;
    static const int64_t MAX_COS_SLICE_SIZE = 10000000;
    int64_t slice_size = 524288; // default 512K
  };

  ObCosEnv() : ObSingleton<ObCosEnv>(), is_inited_(false) {}
  // global init cos env resource, must and only can be called once
  int init();

  void fin();

  int set_slice_size(const int64_t size);

  int64_t get_slice_size() const;

private:
  bool is_inited_;
  Conf conf_;
};


// COS domain name structure: bucket_name-appid.endpoint
// For example https://bucket-appid.cos.ap-guangzhou.myqcloud.com,
// cos.ap-guangzhou.myqcloud.com is the endpoint.
struct OB_PUBLIC_API ObCosAccount
{
  // max domain length
  static constexpr int MAX_COS_DOMAIN_LENGTH = 1536;
  // max endpoint length
  static constexpr int MAX_COS_ENDPOINT_LENGTH = 128;
  // max access id length
  static constexpr int MAX_COS_ACCESS_ID_LENGTH = 128;
  // max access key length
  static constexpr int MAX_COS_ACCESS_KEY_LENGTH = 128;
  // max appid length
  static constexpr int MAX_COS_APPID_LENGTH = 128;

  // cos endpoint
  char endpoint[MAX_COS_ENDPOINT_LENGTH];
  // your access id
  char access_id[MAX_COS_ACCESS_ID_LENGTH];
  // your access key
  char access_key[MAX_COS_ACCESS_KEY_LENGTH];
  // your appid
  char appid[MAX_COS_APPID_LENGTH];

  ObCosAccount()
  {
    memset(this, 0, sizeof(*this));
  }

  ~ObCosAccount() {}

  void clear()
  {
    memset(this, 0, sizeof(*this));
  }

  // parse endpoint, access id, access key and appid from storage_info
  // You must call parse_from first before using any field.
  int parse_from(const char *storage_info, uint32_t size);

private:
  // make sure 'value' end with '\0'
  int set_field(const char *value, char *field, uint32_t length);
};


// The string does not own the buffer.
struct OB_PUBLIC_API CosStringBuffer
{
  const char *data;
  int32_t size;

  CosStringBuffer() : data(NULL), size(0) {}

  CosStringBuffer(const char *ptr, int len) : data(ptr), size(len) {}

  bool empty() const
  {
    return NULL == data || 0 >= size;
  }

  ~CosStringBuffer() {}
};


struct OB_PUBLIC_API CosObjectMeta
{
  int64_t file_length;
  int64_t last_modified_ts;
  int type;

  CosObjectMeta()
  {
    reset();
  }

  ~CosObjectMeta() {}

  void reset()
  {
    memset(this, 0, sizeof(*this));
    file_length = -1;
  }
};


/*= Custom memory allocation functions */
typedef void* (*OB_COS_allocFunction) (void* opaque, size_t size);
typedef void  (*OB_COS_freeFunction) (void* opaque, void* address);
typedef struct { OB_COS_allocFunction customAlloc; OB_COS_freeFunction customFree; void* opaque; } OB_COS_customMem;

class OB_PUBLIC_API ObCosWrapper
{
public:
  struct Handle {};

  // alloc_f is used to allocate the handle's memory.
  // You need to call destroy_cos_handle when handle is not use
  // any more.
  static int create_cos_handle(
    OB_COS_customMem &custom_mem,
    const struct ObCosAccount &account,
    Handle **h);

  // You can not use handle any more after destroy_cos_handle is called.
  static void destroy_cos_handle(Handle *h);

  // Put an object to cos, the new object will overwrite the old one if object exist.
  static int put(
    Handle *h,
    const CosStringBuffer &bucket_name,
    const CosStringBuffer &object_name,
    const char *buffer,
    const int64_t size);

  // Get object meta
  static int head_object_meta(
    Handle *h,
    const CosStringBuffer &bucket_name,
    const CosStringBuffer &object_name,
    bool &is_exist,
    CosObjectMeta &meta);

  // Delete one object from cos.
  static int del(
    Handle *h,
    const CosStringBuffer &bucket_name,
    const CosStringBuffer &object_name);

  // Delete all objects that match the same dir_name prefix.
  static int del_objects_in_dir(
    Handle *h,
    const CosStringBuffer &bucket_name,
    const CosStringBuffer &dir_name,
    int64_t &deleted_cnt);

  // Update object last modofied time.
  static int update_object_modified_ts(
    Handle *h,
    const CosStringBuffer &bucket_name,
    const CosStringBuffer &object_name);

  // Random read data of specific range from object.
  // Buffer's memory is provided by user.
  static int pread(
    Handle *h,
    const CosStringBuffer &bucket_name,
    const CosStringBuffer &object_name,
    int64_t offset,
    char *buf,
    int64_t buf_size,
    int64_t &read_size);

  // Get whole object
  // Buffer's memory is provided by user.
  static int get_object(
    Handle *h,
    const CosStringBuffer &bucket_name,
    const CosStringBuffer &object_name,
    char *buf,
    int64_t buf_size,
    int64_t &read_size);

  struct CosListObjPara {
    CosListObjPara() : arg(NULL), cur_full_path_slice_name(NULL),
          full_path_size(0), cur_object_size(0), next_flag(false)
    {
      last_container_name.d_name[0] = '\0';
      last_container_name.d_type = DT_REG;
      last_container_name.d_reclen = sizeof(struct dirent);
    }
    void* arg;
    char* cur_full_path_slice_name;
    struct dirent last_container_name;
    int64_t full_path_size;
    int64_t cur_object_size;
    bool next_flag;
  };

  typedef int (*handleObjectNameFunc)(CosListObjPara&);
  typedef int (*handleDirectoryFunc)(void*, const char*, int64_t);
  // List objects in the same directory, include all objects
  // in the inner sub directories.
  // dir_name must be end with "/\0".
  static int list_objects(
    Handle *h,
    const CosStringBuffer &bucket_name,
    const CosStringBuffer &dir_name,
    const CosStringBuffer &next_marker,
    handleObjectNameFunc handle_object_name_f,
    void *arg);

  static int list_directories(
    Handle *h,
    const CosStringBuffer &bucket_name,
    const CosStringBuffer &dir_name,
    const CosStringBuffer &next_marker,
    const CosStringBuffer &delimiter,
    handleDirectoryFunc handle_directory_name_f,
    void *arg);
};


#undef OB_PUBLIC_API
}
}
}
#endif
