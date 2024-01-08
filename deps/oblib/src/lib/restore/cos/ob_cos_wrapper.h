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

  void destroy();

private:
  bool is_inited_;
  Conf conf_;
};

static constexpr int MAX_TAGGING_STR_LEN = 16;

// COS domain name structure: bucket_name-appid.endpoint
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
  char endpoint_[MAX_COS_ENDPOINT_LENGTH];
  // your access id
  char access_id_[MAX_COS_ACCESS_ID_LENGTH];
  // your access key
  char access_key_[MAX_COS_ACCESS_KEY_LENGTH];
  // your appid
  char appid_[MAX_COS_APPID_LENGTH];
  // cos object delete mode
  char delete_mode_[MAX_TAGGING_STR_LEN];

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
  const char *data_;
  int32_t size_;

  CosStringBuffer() : data_(NULL), size_(0) {}

  CosStringBuffer(const char *ptr, int len) : data_(ptr), size_(len) {}

  ~CosStringBuffer() {}

  bool empty() const
  {
    return NULL == data_ || 0 >= size_;
  }

  bool prefix_match(const char *str) const
  {
    bool match = false;
    int32_t str_len = 0;
    if (NULL != str) {
      str_len = static_cast<int32_t>(strlen(str));
    }

    if (data_ == str) {
      match = (size_ >= str_len ? true : false);
    } else if (size_ < str_len) {
      // skip
    } else if (0 == strncasecmp(data_, str, str_len)) {
      match = true;
    }
    return match;
  }

  int32_t get_data_size() const
  {
    return (!empty() && '\0' == data_[size_ - 1]) ? size_ - 1 : size_;
  }

  bool is_prefix_of(const char *str, const int32_t str_len) const
  {
    bool match = false;
    const int32_t data_size = get_data_size();
    if (str == data_) {
      match = (data_size <= str_len) ? true : false;
    } else if (data_size > str_len) {
      match = false;
    } else if (0 == memcmp(data_, str, data_size)) {
      match = true;
    }
    return match;
  }

  bool is_end_with_slash_and_null() const
  {
    return (NULL != data_ && size_ >= 2 && data_[size_ - 1] == '\0' && data_[size_ - 2] == '/');
  }
};


struct OB_PUBLIC_API CosObjectMeta
{
  enum CosObjectType
  {
    COS_OBJ_INVALID,
    COS_OBJ_NORMAL,
    COS_OBJ_APPENDABLE,
  };

  int64_t file_length_;
  int64_t last_modified_ts_;
  int type_;

  CosObjectMeta()
  {
    reset();
  }

  ~CosObjectMeta() {}

  void reset()
  {
    memset(this, 0, sizeof(*this));
    file_length_ = -1;
    type_ = CosObjectType::COS_OBJ_INVALID;
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
    const bool check_md5,
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

  // Append content to the specific object from the @offset position in cos.
  static int append(
    Handle *h,
    const CosStringBuffer &bucket_name,
    const CosStringBuffer &object_name,
    const char *buf,
    const int64_t buf_size,
    const int64_t offset);

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

  // Tag one object from cos
  static int tag(
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
    const int64_t offset,
    char *buf,
    const int64_t buf_size,
    const bool is_range_read,
    int64_t &read_size);

  // Get whole object
  // Buffer's memory is provided by user.
  static int get_object(
    Handle *h,
    const CosStringBuffer &bucket_name,
    const CosStringBuffer &object_name,
    char *buf,
    const int64_t buf_size,
    int64_t &read_size);

  static int is_object_tagging(
    Handle *h,
    const CosStringBuffer &bucket_name,
    const CosStringBuffer &object_name,
    bool &is_tagging);

  struct CosListObjPara
  {
    enum class CosListType
    {
      COS_LIST_INVALID,
      COS_LIST_CB_ARG,
      COS_PART_LIST_CTX
    };

    CosListObjPara()
      : arg_(NULL), cur_obj_full_path_(NULL),
        full_path_size_(0), cur_object_size_str_(NULL),
        next_flag_(false), type_(CosListType::COS_LIST_INVALID),
        next_token_(NULL), next_token_size_(0), finish_part_list_(false)
    {
      last_container_name_.d_name[0] = '\0';
      last_container_name_.d_type = DT_REG;
    }

    int set_cur_obj_meta(
        char *obj_full_path,
        const int64_t full_path_size,
        char *object_size_str);

    void *arg_;
    char *cur_obj_full_path_;
    struct dirent last_container_name_;
    int64_t full_path_size_;
    char *cur_object_size_str_;
    bool next_flag_;
    CosListType type_;
    char *next_token_;
    int64_t next_token_size_;
    bool finish_part_list_;
  };

  typedef int (*handleObjectNameFunc)(CosListObjPara&);
  typedef int (*handleDirectoryFunc)(void*, const CosListObjPara::CosListType, const char*, int64_t);
  // List objects in the same directory, include all objects
  // in the inner sub directories.
  // dir_name must be end with "/\0".
  static int list_objects(
    Handle *h,
    const CosStringBuffer &bucket_name,
    const CosStringBuffer &dir_name,
    handleObjectNameFunc handle_object_name_f,
    void *arg);

  // Only list up-to 1000 objects
  static int list_part_objects(
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

  static int is_empty_directory(
    Handle *h,
    const CosStringBuffer &bucket_name,
    const CosStringBuffer &dir_name,
    bool &is_empty_dir);

  static int init_multipart_upload(
    Handle *h,
    const CosStringBuffer &bucket_name,
    const CosStringBuffer &object_name,
    char *&upload_id_str);

  static int upload_part_from_buffer(
    Handle *h,
    const CosStringBuffer &bucket_name,
    const CosStringBuffer &object_name,
    const CosStringBuffer &upload_id_str,
    const int part_num, /*the sequence number of this part, [1, 10000]*/
    const char *buf,
    const int64_t buf_size);

  static int complete_multipart_upload(
    Handle *h,
    const CosStringBuffer &bucket_name,
    const CosStringBuffer &object_name,
    const CosStringBuffer &upload_id_str);

  static int abort_multipart_upload(
    Handle *h,
    const CosStringBuffer &bucket_name,
    const CosStringBuffer &object_name,
    const CosStringBuffer &upload_id_str);

  static int del_unmerged_parts(
    Handle *h,
    const CosStringBuffer &bucket_name,
    const CosStringBuffer &object_name);
};

#undef OB_PUBLIC_API
}
}
}
#endif
