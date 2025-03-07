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

#ifndef SRC_LIBRARY_SRC_LIB_RESTORE_OB_I_STORAGE_H_
#define SRC_LIBRARY_SRC_LIB_RESTORE_OB_I_STORAGE_H_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_se_array.h"
#include "common/storage/ob_device_common.h"
#include "ob_storage_info.h"
#include "ob_object_storage_base.h"

namespace oceanbase
{
namespace common
{

static constexpr int64_t MAX_APPENDABLE_FRAGMENT_SUFFIX_LENGTH = 64;
static constexpr int64_t MAX_APPENDABLE_FRAGMENT_LENGTH = 128;
static constexpr char APPENDABLE_OBJECT_ALLOCATOR[] = "AppendableAlloc";
static constexpr int64_t IO_HANDLED_SIZE_ZERO = 0;

enum StorageOpenMode
{
  CREATE_OPEN_LOCK = 0, // default, create and open
  EXCLUSIVE_CREATE = 1, // exclusive create and open
  ONLY_OPEN_UNLOCK = 2, // only open
  CREATE_OPEN_NOLOCK = 3, // create and open nolock
};

enum ObStorageObjectMetaType
{
  OB_OBJ_INVALID = 0,
  OB_OBJ_NORMAL = 1,
  OB_OBJ_SIMULATE_APPEND = 2,
  OB_FS_DIR = 3,
  OB_FS_FILE = 4,
};

// check the str is end with '/' or not
bool is_end_with_slash(const char *str);
bool is_null_or_end_with_slash(const char *str);
int get_safe_str_len(const char* str);
int c_str_to_int(const char *str, const int64_t length, int64_t &num);
int c_str_to_int(const char *str, int64_t &num);
int get_storage_prefix_from_path(const common::ObString &uri, const char *&prefix);
int handle_listed_object(ObBaseDirEntryOperator &op,
    const char *obj_name, const int64_t obj_name_len, const int64_t obj_size);
int handle_listed_directory(ObBaseDirEntryOperator &op,
    const char *dir_name, const int64_t dir_name_len);
int build_bucket_and_object_name(ObIAllocator &allocator,
    const ObString &uri, ObString &bucket, ObString &object);
int construct_fragment_full_name(const ObString &logical_appendable_object_name,
    const char *fragment_name, char *name_buf, const int64_t name_buf_len);
int construct_fragment_full_name(const ObString &logical_appendable_object_name,
    const int64_t start, const int64_t end, char *name_buf, const int64_t name_buf_len);
// Used for batch_del_files
// check files are not null and files's idx are valid
int check_files_map_validity(const hash::ObHashMap<ObString, int64_t> &files_to_delete);
// Used for batch_del_files
// record all files's idx remained in files_to_delete
int record_failed_files_idx(const hash::ObHashMap<ObString, int64_t> &files_to_delete,
                            ObIArray<int64_t> &failed_files_idx);
int ob_set_field(const char *value, char *field, const uint32_t field_length);
int ob_apr_abort_fn(int retcode);

struct ObStorageObjectMetaBase
{
  OB_UNIS_VERSION_V(1);
public:
  ObStorageObjectMetaBase() : type_(ObStorageObjectMetaType::OB_OBJ_INVALID) { reset(); }
  ~ObStorageObjectMetaBase() { reset(); }

  void reset() { is_exist_ = false; length_ = -1; }

  TO_STRING_KV(K_(is_exist), K_(length));

  bool is_exist_;
  int64_t length_;
  ObStorageObjectMetaType type_;
};

// Each fragment meta corresponds to a normal object in a 'dir'.
// The 'dir' name is the S3 appendable object name.
// Fragment name format: /xxx/xxx/appendable_obj_name/prefix-start-end[-suffix]
// 'prefix' is a special string which represents this object is a S3 appendable object fragment.
// 'start-end' means the data range covered by this file. [start, end), include start、not include end.
// 'suffix' may exist, mainly used by deleting file situation.
struct ObAppendableFragmentMeta
{
  OB_UNIS_VERSION_V(1);
public:
  enum ObAppendableFragmentType
  {
    APPENDABLE_FRAGMENT_DATA = 0,
    APPENDABLE_FRAGMENT_FORMAT_META = 1,
    APPENDABLE_FRAGMENT_SEAL_META = 2,
  };

  ObAppendableFragmentMeta()
    : start_(-1), end_(-1), type_(ObAppendableFragmentType::APPENDABLE_FRAGMENT_DATA) {
    suffix_[0] = '\0';
  }
  virtual ~ObAppendableFragmentMeta() {}

  bool is_format_meta() const { return type_ == ObAppendableFragmentType::APPENDABLE_FRAGMENT_FORMAT_META; }
  bool is_seal_meta() const { return type_ == ObAppendableFragmentType::APPENDABLE_FRAGMENT_SEAL_META; }
  bool is_data() const { return type_ == ObAppendableFragmentType::APPENDABLE_FRAGMENT_DATA; }
  bool is_valid() const
  {
    return (is_format_meta()) || (is_seal_meta()) || (is_data() && start_ >= 0 && end_ > start_);
  }

  int assign(const ObAppendableFragmentMeta &other);
  int parse_from(ObString &fragment_name);
  int64_t to_string(char *buf, const int64_t len) const;

  int64_t get_length() const { return end_ - start_; }

  int64_t start_;
  int64_t end_;
  char suffix_[MAX_APPENDABLE_FRAGMENT_SUFFIX_LENGTH];
  ObAppendableFragmentType type_;
};

struct ObStorageObjectMeta : public ObStorageObjectMetaBase
{
  OB_UNIS_VERSION_V(1);
public:
  ObStorageObjectMeta()
    : ObStorageObjectMetaBase(),
      fragment_metas_()
  {}

  ~ObStorageObjectMeta() { reset(); }
  void reset();

  bool is_valid() const;
  // Based on the range[start, end), to choose the needed files and save these meta into @fragments.
  int get_needed_fragments(const int64_t start, const int64_t end,
      ObArray<ObAppendableFragmentMeta> &fragments);

  bool is_object_file_type() const
  {
    return (type_ == ObStorageObjectMetaType::OB_OBJ_NORMAL) ||
           (type_ == ObStorageObjectMetaType::OB_FS_FILE);
  }
  bool is_dir_type() const
  {
    return type_ == ObStorageObjectMetaType::OB_FS_DIR;
  }
  bool is_simulate_append_type() const { return type_ == ObStorageObjectMetaType::OB_OBJ_SIMULATE_APPEND; }

  static bool fragment_meta_cmp_func(const ObAppendableFragmentMeta &left, const ObAppendableFragmentMeta &right);

  TO_STRING_KV(K_(is_exist), K_(length), K_(type), K_(fragment_metas));

  ObSEArray<ObAppendableFragmentMeta, 10> fragment_metas_;
};

struct ObStorageListCtxBase
{
public:
  int64_t max_list_num_; // each round list, can only get up-to @max_list_num_ items.
  char **name_arr_; // for object storage, save full path; for file system, save file name.
  int64_t max_name_len_; // no matter full path, or just object/file name, can not be longer than this value.
  int64_t rsp_num_; // real listed-item number which is obtained from the listed result
  bool has_next_; // list result can only return up-to 1000 objects once, thus may need to multi operation.
  bool need_size_; // If true, that means when we list items, we also need to get each item's size
  int64_t *size_arr_; // save all the length of each object/file (the order is the same with name_arr)
  int64_t cur_listed_count_;
  int64_t total_list_limit_;  // The maximum number of objects required to be listed. <= 0 means there is no limit

  ObStorageListCtxBase()
    : max_list_num_(0), name_arr_(NULL), max_name_len_(0), rsp_num_(0),
      has_next_(false), need_size_(false), size_arr_(NULL),
      cur_listed_count_(0), total_list_limit_(-1)
  {}

  virtual ~ObStorageListCtxBase() { reset(); }

  int init(ObArenaAllocator &allocator, const int64_t max_list_num, const bool need_size);

  void reset();

  bool is_valid() const;
  void set_total_list_limit(const int64_t limit);
  void inc_cur_listed_count();
  bool has_reached_list_limit() const;

  TO_STRING_KV(K_(max_list_num), K_(max_name_len), K_(rsp_num), K_(has_next), K_(need_size),
    KP_(name_arr), KP_(size_arr));
};

// Used for object storage
struct ObStorageListObjectsCtx : public ObStorageListCtxBase
{
public:
  char *next_token_; // save marker/continuation_token
  int64_t next_token_buf_len_; // length of marker/continuation_token should not be longer than this value
  char *cur_appendable_full_obj_path_;
  const char *marker_;

  ObStorageListObjectsCtx()
    : next_token_(NULL), next_token_buf_len_(0), cur_appendable_full_obj_path_(NULL),
      marker_(nullptr)
  {}

  virtual ~ObStorageListObjectsCtx() { reset(); }

  void reset();

  int init(ObArenaAllocator &allocator, const int64_t max_list_num, const bool need_size);

  bool is_valid() const { return ObStorageListCtxBase::is_valid() && (next_token_ != NULL)
                                 && (next_token_buf_len_ > 0); }
  int set_next_token(const bool has_next, const char *next_token, const int64_t next_token_len);
  int set_marker(const char *marker);
  int handle_object(const char *obj_path, const int obj_path_len, const int64_t obj_size);

  INHERIT_TO_STRING_KV("ObStorageListCtxBase", ObStorageListCtxBase,
      K_(next_token), K_(next_token_buf_len), K_(cur_appendable_full_obj_path));
};

// Used for file system
struct ObStorageListFilesCtx : public ObStorageListCtxBase
{
public:
  DIR *open_dir_;
  struct dirent next_entry_; // If has_next=true, it will get the next entry based on this value.
  bool already_open_dir_; // only during the first round, need to open dir

  ObStorageListFilesCtx()
    : open_dir_(NULL), next_entry_(), already_open_dir_(false)
  {}

  virtual ~ObStorageListFilesCtx() { reset(); }

  void reset();

  bool is_valid() const;

  INHERIT_TO_STRING_KV("ObStorageListCtxBase", ObStorageListCtxBase, K_(already_open_dir));
};

enum ObStorageDeleteMode: uint8_t
{
  NONE = 0,
  STORAGE_DELETE_MODE = 1,
  STORAGE_TAGGING_MODE = 2,
  MAX
};

class ObIStorageUtil
{
public:
  virtual int open(common::ObObjectStorageInfo *storage_info) = 0;
  virtual void close() = 0;
  virtual int is_exist(const common::ObString &uri, bool &exist) = 0;
  virtual int get_file_length(const common::ObString &uri, int64_t &file_length) = 0;
  virtual int head_object_meta(const common::ObString &uri, ObStorageObjectMetaBase &obj_meta) = 0;
  virtual int del_file(const common::ObString &uri) = 0;
  virtual int batch_del_files(
      const ObString &uri,
      hash::ObHashMap<ObString, int64_t> &files_to_delete,
      ObIArray<int64_t> &failed_files_idx) = 0;
  virtual int write_single_file(const common::ObString &uri, const char *buf, const int64_t size) = 0;
  virtual int mkdir(const common::ObString &uri) = 0;
  // list all objects which are 'prefix-matched'
  virtual int list_files(const common::ObString &dir_path, common::ObBaseDirEntryOperator &op) = 0;
  // If we want to get the listed result and handle it in other logic, we can use this interface.
  // @list_ctx will save the listed result(cuz obj_storage can only return up-to 1000 items at a time).
  virtual int list_files(const common::ObString &dir_path, ObStorageListCtxBase &list_ctx) = 0;
  virtual int del_dir(const common::ObString &uri) = 0;
  virtual int list_directories(const common::ObString &dir_path, common::ObBaseDirEntryOperator &op) = 0;
  virtual int is_tagging(const common::ObString &uri, bool &is_tagging) = 0;
  virtual int del_unmerged_parts(const common::ObString &uri) = 0;
};

class ObIStorageReader
{
public:
  virtual int open(const common::ObString &uri,
                   common::ObObjectStorageInfo *storage_info, const bool head_meta = true) = 0;
  virtual int pread(char *buf,const int64_t buf_size, const int64_t offset, int64_t &read_size) = 0;
  virtual int close() = 0;
  virtual int64_t get_length() const = 0;
  virtual bool is_opened() const = 0;
};

class ObIStorageWriter
{
public:
  virtual int open(const common::ObString &uri, common::ObObjectStorageInfo *storage_info) = 0;
  virtual int write(const char *buf,const int64_t size) = 0;
  virtual int pwrite(const char *buf, const int64_t size, const int64_t offset) = 0;
  virtual int close() = 0;
  virtual int64_t get_length() const = 0;
  virtual bool is_opened() const = 0;
};

// TODO @fangdan: delete this interface
class ObIStorageMultiPartWriter
{
public:
  virtual int open(const common::ObString &uri, common::ObObjectStorageInfo *storage_info) = 0;
  virtual int write(const char *buf, const int64_t size) = 0;
  virtual int pwrite(const char *buf, const int64_t size, const int64_t offset) = 0;
  virtual int complete() = 0;
  virtual int abort() = 0;
  virtual int close() = 0;
  virtual int64_t get_length() const = 0;
  virtual bool is_opened() const = 0;
};

class ObIStorageParallelMultipartWriter
{
public:
  virtual int open(const common::ObString &uri, common::ObObjectStorageInfo *storage_info) = 0;
  virtual int upload_part(const char *buf, const int64_t size, const int64_t part_id) = 0;
  virtual int complete() = 0;
  virtual int abort() = 0;
  virtual int close() = 0;
  virtual bool is_opened() const = 0;
};

class ObStoragePartInfoHandler
{
public:
  // [etag, checksum]
  using PartInfo = std::pair<const char *, const char *>;
  // part_id -> [etag, checksum]
  using PartInfoMap = hash::ObHashMap<int64_t, PartInfo>;

  ObStoragePartInfoHandler();
  virtual ~ObStoragePartInfoHandler();
  void reset_part_info();

  int init();
  bool is_init() const { return is_inited_; }
  int add_part_info(const int64_t part_id, const char *etag, const char *checksum);
  int64_t size() const { return part_info_map_.size(); }


protected:
  bool is_inited_;
  ObArenaAllocator part_info_allocator_;  // ObArenaAllocator is not thread safe
  PartInfoMap part_info_map_;
  SpinRWLock lock_;

private:
  static constexpr const char *PART_INFO_ALLOCATOR_TAG = "PART_INFO_ALLOC";
  static constexpr const char *PART_INFO_MAP_TAG = "PART_INFO_MAP";
};

class ObObjectStorageGuard : public lib::ObMallocHookAttrGuard
{
public:
  ObObjectStorageGuard(
      const char *file, const int64_t line, const char *func,
      const int &ob_errcode,
      const ObObjectStorageInfo *storage_info,
      const ObString &uri,
      const int64_t &handled_size);
  ~ObObjectStorageGuard();

private:
  void print_access_storage_log_() const;
  bool is_slow_io_(const int64_t cost_time_us) const;

private:
  static constexpr int64_t SMALL_IO_SIZE = 128LL * 1024LL;             // 128KB
  static constexpr int64_t MEDIUM_IO_SIZE = 2LL * 1024LL * 1024LL;     // 2MB

  static constexpr int64_t UTIL_IO_WARN_THRESHOLD_TIME_US = 50LL * 1000;       // 50ms
  static constexpr int64_t SMALL_IO_WARN_THRESHOLD_TIME_US = 100LL * 1000;     // 100ms
  static constexpr int64_t MEDIUM_IO_WARN_THRESHOLD_TIME_US = 200LL * 1000;    // 200ms
  static constexpr int64_t LARGE_IO_WARN_THRESHOLD_TIME_US = 300LL * 1000;     // 300ms

  const char *file_name_;
  const int64_t line_;
  const char *func_name_;
  const int &ob_errcode_;
  const ObObjectStorageInfo *storage_info_;
  const int64_t start_time_us_;
  // Note: We don't use a reference here because if passing a `const char *` to the constructor
  // creates a temporary `ObString` object. This temporary object is destroyed when the constructor
  // exits, leaving the reference dangling.
  const ObString uri_;
  const int64_t &handled_size_;
};

#define OBJECT_STORAGE_GUARD(storage_info, uri, handled_size) \
    common::ObObjectStorageGuard object_storage_guard_(       \
        __FILE__, __LINE__, __FUNCTION__,                     \
        ret, storage_info, uri, handled_size)

}//common
}//oceanbase
#endif /* SRC_LIBRARY_SRC_LIB_RESTORE_OB_I_STORAGE_H_ */
