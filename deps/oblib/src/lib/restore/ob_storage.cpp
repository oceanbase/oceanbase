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

#include "ob_storage.h"
#include "lib/restore/ob_object_device.h"
#include "common/storage/ob_device_common.h"
#include "lib/utility/ob_sort.h"
#include "lib/stat/ob_diagnostic_info_guard.h"

namespace oceanbase
{
namespace common
{

const char *OB_STORAGE_TYPES_STR[] = {"OSS", "FILE", "COS", "LOCAL", "S3", "LOCAL_CACHE", "HDFS"};

void print_access_storage_log(
    const char *msg,
    const common::ObString &uri,
    const int64_t start_ts,
    const int64_t size,
    bool *is_slow)
{
  if (NULL != is_slow) {
    *is_slow = false;
  }

  if (NULL != msg) {
    int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
    const int64_t warn_cost_time = 500 * 1000;  // 500ms
    double speed = 1.0 * (double)size / 1024 / 1024 * 1000 * 1000 / (double)cost_ts;  // MB/s
    if (cost_ts > warn_cost_time || speed < 1) {
      if (NULL != is_slow) {
        *is_slow = true;
      }
      _STORAGE_LOG_RET(INFO, OB_SUCCESS, "access storage op=%s uri=%.*s size=%ld Byte cost_ts=%ld us speed=%.2f MB/s",
        msg, uri.length(), uri.ptr(), size, cost_ts, speed);
    }
  }
}

int validate_uri_type(const common::ObString &uri)
{
  int ret = OB_SUCCESS;
  if (!uri.prefix_match(OB_OSS_PREFIX) &&
      !uri.prefix_match(OB_COS_PREFIX) &&
      !uri.prefix_match(OB_S3_PREFIX) &&
      !uri.prefix_match(OB_FILE_PREFIX) &&
      !uri.prefix_match(OB_HDFS_PREFIX)) {
    ret = OB_INVALID_BACKUP_DEST;
    STORAGE_LOG(ERROR, "invalid backup uri", K(ret), K(uri));
  }
  return ret;  
}

int get_storage_type_from_path(const common::ObString &uri, ObStorageType &type)
{
  int ret = OB_SUCCESS;
  type = OB_STORAGE_MAX_TYPE;

  if (uri.prefix_match(OB_OSS_PREFIX)) {
    type = OB_STORAGE_OSS;
  } else if (uri.prefix_match(OB_COS_PREFIX)) {
    type = OB_STORAGE_COS;
  } else if (uri.prefix_match(OB_S3_PREFIX)) {
    type = OB_STORAGE_S3;
  } else if (uri.prefix_match(OB_FILE_PREFIX)) {
    type = OB_STORAGE_FILE;
  } else if (uri.prefix_match(OB_HDFS_PREFIX)) {
    type = OB_STORAGE_HDFS;
  }else {
    ret = OB_INVALID_BACKUP_DEST;
    STORAGE_LOG(ERROR, "invalid backup uri", K(ret), K(uri));
  }
  return ret;
}

const char *get_storage_type_str(const ObStorageType &type)
{
  const char *str = "UNKNOWN";
  STATIC_ASSERT(static_cast<int64_t>(OB_STORAGE_MAX_TYPE) == ARRAYSIZEOF(OB_STORAGE_TYPES_STR), "ObStorageType count mismatch");
  if (type >= 0 && type < OB_STORAGE_MAX_TYPE) {
    str = OB_STORAGE_TYPES_STR[type];
  }
  return str;
}

bool is_storage_type_match(const common::ObString &uri, const ObStorageType &type)
{
  return (OB_STORAGE_OSS == type && uri.prefix_match(OB_OSS_PREFIX))
      || (OB_STORAGE_COS == type && uri.prefix_match(OB_COS_PREFIX))
      || (OB_STORAGE_S3 == type && uri.prefix_match(OB_S3_PREFIX))
      || (OB_STORAGE_FILE == type && uri.prefix_match(OB_FILE_PREFIX))
      || (OB_STORAGE_HDFS == type && uri.prefix_match(OB_HDFS_PREFIX));
}

bool is_object_storage_type(const ObStorageType &type)
{ return ObStorageType::OB_STORAGE_FILE != type
      && ObStorageType::OB_STORAGE_MAX_TYPE != type;
}
bool is_io_error(const int result)
{
  return OB_IO_ERROR == result || OB_OBJECT_STORAGE_IO_ERROR == result;
}

int get_storage_type_from_name(const char *type_str, ObStorageType &type)
{
  int ret = OB_SUCCESS;
  type = ObStorageType::OB_STORAGE_MAX_TYPE;
  const int64_t count = ARRAYSIZEOF(OB_STORAGE_TYPES_STR);
  STATIC_ASSERT(static_cast<int64_t>(ObStorageType::OB_STORAGE_MAX_TYPE) == count, "status count mismatch");
  for (int64_t i = 0; i < count; ++i) {
    if (0 == strcmp(type_str, OB_STORAGE_TYPES_STR[i])) {
      type = static_cast<ObStorageType>(i);
      break;
    }
  }
  return ret;
}

// let path end with '/'
int build_full_dir_path(const char *dir_path, char *dir_path_buf, const int64_t dir_path_buf_len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_ISNULL(dir_path) || OB_ISNULL(dir_path_buf) ||
      OB_UNLIKELY(strlen(dir_path) <= 0 || dir_path_buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), K(dir_path), KP(dir_path_buf), K(dir_path_buf_len));
  } else if (OB_FAIL(databuff_printf(dir_path_buf, dir_path_buf_len, pos, "%s", dir_path))) {
    OB_LOG(WARN, "failed to deep copy dir path", K(ret), K(dir_path), K(dir_path_buf_len));
  } else if (!is_end_with_slash(dir_path)
      && OB_FAIL(databuff_printf(dir_path_buf, dir_path_buf_len, pos, "/"))) {
    OB_LOG(WARN, "failed to make path end with '/'", K(ret), K(dir_path), K(dir_path_buf_len));
  }
  return ret;
}

/**
 * ------------------------------ListAppendableObjectFragmentOp------------------------------
 */
int ListAppendableObjectFragmentOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  ObString fragment_name(entry->d_name);
  ObAppendableFragmentMeta fragment_meta;
  if (OB_FAIL(fragment_meta.parse_from(fragment_name))) {
    OB_LOG(WARN, "fail to parse fragment name", K(ret), K(entry->d_name));
  } else if (OB_LIKELY(fragment_meta.is_valid())) {
    if (fragment_meta.is_format_meta()) {
      exist_format_meta_ = true;
    } else if (fragment_meta.is_seal_meta()) {
      exist_seal_meta_ = true;
    } else if (fragment_meta.is_data()) {
      if (need_get_file_size()) {
        if (get_size() != fragment_meta.get_length()) {
          ret = OB_ERR_UNEXPECTED;
          OB_LOG(WARN, "fragment file size is not equal to the fragment name range", K(ret), "file_size",
            get_size(), K(fragment_meta));
        }
      }

      if (FAILEDx(meta_arr_.push_back(fragment_meta))) {
        OB_LOG(WARN, "fail to push back fragment meta", K(ret), K(fragment_meta));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "unknown fragment meta type", K(ret), K(fragment_meta));
    }
  } else {
    // just skip this invalid fragment, continue next one.
    OB_LOG(WARN, "invalid fragment name", K(ret), K(entry->d_name));
  }
  return ret;
}

int ListAppendableObjectFragmentOp::gen_object_meta(ObStorageObjectMeta &obj_meta)
{
  int ret = OB_SUCCESS;
  int64_t i = 0;
  int64_t last_end_offset = 0;
  obj_meta.reset();
  obj_meta.type_ = ObStorageObjectMetaType::OB_OBJ_SIMULATE_APPEND;
  lib::ob_sort(meta_arr_.begin(), meta_arr_.end(), ObStorageObjectMeta::fragment_meta_cmp_func);
  while (OB_SUCC(ret) && i < meta_arr_.size()) {
    ObAppendableFragmentMeta &tmp_meta = meta_arr_[i];
    last_end_offset = tmp_meta.end_;
    if (OB_FAIL(obj_meta.fragment_metas_.push_back(tmp_meta))) {
      OB_LOG(WARN, "failed to push back fragment meta", K(ret), K(obj_meta.fragment_metas_));
    }

    // find a fragment that intersects with the current fragment(and not covered by the current fragment)
    // and can maximize the coverage of the file contents
    int64_t j = i + 1;
    int64_t selected_idx = i;
    int64_t max_end_offset = tmp_meta.end_;
    while (OB_SUCC(ret) && j < meta_arr_.size() && meta_arr_[j].start_ <= tmp_meta.end_) {
      if (meta_arr_[j].end_ > max_end_offset) {
        selected_idx = j;
        max_end_offset = meta_arr_[j].end_;
      }
      j++;
    }

    if (OB_FAIL(ret)) {
    } else if (selected_idx == i) {
      if (i <  meta_arr_.size() - 1) {
        // do not return error
        OB_LOG(WARN, "cannot find a fragment that intersects with the current fragment",
            K(i), K(j), K(tmp_meta));
      }
      i = j;
    } else {
      i = selected_idx;
    }
  }

  if (OB_SUCC(ret)) {
    obj_meta.is_exist_ = true;
    obj_meta.length_ = last_end_offset;
  }
  return ret;
}

/**
 * ------------------------------DelAppendableObjectFragmentOp------------------------------
 */
DelAppendableObjectFragmentOp::DelAppendableObjectFragmentOp(
  const common::ObString &uri, ObStorageUtil &util)
  : uri_(uri), util_(util)
{}

int DelAppendableObjectFragmentOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  char obj_fragment_path[OB_MAX_URI_LENGTH] = {0};
  const char *delemiter_str = is_end_with_slash(uri_.ptr()) ? "" : "/";

  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_FAIL(databuff_printf(obj_fragment_path, sizeof(obj_fragment_path),
             "%s%s%s", uri_.ptr(), delemiter_str, entry->d_name))) {
    OB_LOG(WARN, "fail to construct appendable obj fragment path", K(ret), K_(uri), K(delemiter_str), K(entry->d_name));
  } else if (OB_FAIL(util_.del_file(obj_fragment_path))) {
    EVENT_INC(ObStatEventIds::BACKUP_IO_DEL_FAIL_COUNT);
    OB_LOG(WARN, "fail to del appendable object fragment", K(ret), K(obj_fragment_path));
  } else {
    OB_LOG(DEBUG, "succ to del appendable object fragment", K(obj_fragment_path));
  }
  EVENT_INC(ObStatEventIds::BACKUP_DELETE_COUNT);
  return ret;
}

/**
 * ------------------------------ObTopNMinimumDirEntryWithMarkerOperator------------------------------
 */

// return true when lsh is smaller than or equal to rhs
bool less_than(const char *lhs, const char *rhs)
{
  bool bool_ret = false;
  if (OB_NOT_NULL(lhs) && OB_NOT_NULL(rhs)) {
    int strcmp_ret = strcmp(lhs, rhs);
    bool_ret = (0 >= strcmp_ret ? true : false);
  }
  return bool_ret;
}

ObTopNMinimumDirEntryWithMarkerOperator::ObTopNMinimumDirEntryWithMarkerOperator(
    const int64_t num,
    const char *marker,
    const bool need_size)
    : ObBaseDirEntryOperator(), n_(num), marker_(marker), need_size_(need_size),
      less_than_(), heap_(less_than_), allocator_("TopNDir")
{
}

ObTopNMinimumDirEntryWithMarkerOperator::~ObTopNMinimumDirEntryWithMarkerOperator()
{
  n_ = -1;
  heap_.reset();
  allocator_.reset();
}

bool ObTopNMinimumDirEntryWithMarkerOperator::TopNCompElement::operator()(const Entry &lhs,
                                                                          const Entry &rhs)
{
  return less_than(lhs.obj_name_, rhs.obj_name_);
}

int ObTopNMinimumDirEntryWithMarkerOperator::TopNCompElement::get_error_code()
{
  return OB_SUCCESS;
}

bool ObTopNMinimumDirEntryWithMarkerOperator::need_get_file_size() const
{
  return need_size_;
}

int ObTopNMinimumDirEntryWithMarkerOperator::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  const char *d_name = nullptr;
  Entry tmp_entry;
  if (OB_UNLIKELY(n_ <= 0) || OB_ISNULL(entry) || OB_ISNULL(entry->d_name)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K_(n), KP(entry));
  } else if (FALSE_IT(d_name = entry->d_name)) {
    // filter each dirent whose name is smaller than or equal to marker_
  } else if (less_than(d_name, marker_)) {
  } else if (heap_.count() < n_) {
    if (OB_FAIL(alloc_and_init_(d_name, tmp_entry))) {
      STORAGE_LOG(WARN, "alloc_and_init_ failed", K(ret), K(d_name));
    } else if (OB_FAIL(heap_.push(tmp_entry))) {
      STORAGE_LOG(WARN, "push failed", K(ret), K(d_name), K(tmp_entry));
    } else {}

    if (OB_FAIL(ret)) {
      free_memory_(tmp_entry);
    }
  } else if (OB_FAIL(try_replace_top_(d_name))) {
    STORAGE_LOG(WARN, "try_replace_top_ failed", K(ret), K(d_name), K(heap_.top()), K(tmp_entry));
  }

  return ret;
}

int ObTopNMinimumDirEntryWithMarkerOperator::handle_each_dir_entry(
    common::ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  int64_t index = 0;
  Entry top_data;
  ObArray<Entry> entry_list;
  while (!heap_.empty() && OB_SUCC(ret)) {
    if (OB_FAIL(entry_list.push_back(heap_.top()))) {
      STORAGE_LOG(WARN, "fail to push entry to entry_list", K(ret), K(entry_list.size()));
    } else if (OB_FAIL(heap_.pop())) {
      STORAGE_LOG(WARN, "pop failed", K(ret));
    }
  }
  // Data obtained using a max heap is in descending order.
  // To maintain consistency with the results returned by object storage,
  // the obtained data is processed in reverse order.
  for (int64_t i = entry_list.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
    if (OB_ISNULL(entry_list[i].obj_name_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "obj_name_ is NULL", K(ret), K(entry_list[i]), K(i));
    } else if (OB_FAIL(handle_listed_object(op, entry_list[i].obj_name_,
                                            strlen(entry_list[i].obj_name_),
                                            entry_list[i].obj_size_))) {
      OB_LOG(WARN, "fail to handle listed object", K(ret), K(entry_list[i]), K(i));
    } else {
      free_memory_(entry_list[i]);
    }
  }
  return ret;
}

int ObTopNMinimumDirEntryWithMarkerOperator::alloc_and_init_(
    const char *d_name,
    Entry &out_entry)
{
  int ret = OB_SUCCESS;
  char *out_ptr = nullptr;
  int64_t buf_size = -1;
  if (OB_ISNULL(d_name)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(d_name));
  } else if (FALSE_IT(buf_size = strlen(d_name) + 1)) {
  } else if (OB_ISNULL(out_ptr = static_cast<char*>(allocator_.alloc(buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc mem for out ptr", K(ret), K(buf_size), K(d_name));
  } else {
    strncpy(out_ptr, d_name, buf_size - 1);
    out_ptr[buf_size - 1] = '\0';
    out_entry.obj_name_ = out_ptr;
    if (need_size_) {
      out_entry.obj_size_ = get_size();
    }
  }
  return ret;
}

void ObTopNMinimumDirEntryWithMarkerOperator::free_memory_(Entry &out_entry)
{
  if (OB_NOT_NULL(out_entry.obj_name_)) {
    allocator_.free(out_entry.obj_name_);
    out_entry.obj_name_ = nullptr;
  }
  out_entry.obj_size_ = -1;
}

int ObTopNMinimumDirEntryWithMarkerOperator::try_replace_top_(const char *d_name)
{
  int ret = OB_SUCCESS;
  // inner func, do not check heap_ whether is empty
  Entry top_data = heap_.top();
  Entry new_entry;
  if (OB_ISNULL(d_name)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(d_name));
  } else if (less_than(top_data.obj_name_, d_name)) {
  } else if (OB_FAIL(alloc_and_init_(d_name, new_entry))) {
    STORAGE_LOG(WARN, "alloc_and_cp_ failed", K(ret), K(d_name), K(top_data));
  } else {
    free_memory_(top_data);
    if (OB_FAIL(heap_.replace_top(new_entry))) {
      free_memory_(new_entry);
      STORAGE_LOG(WARN, "TopNDir replace_top failed", K(ret), K(new_entry), K(top_data));
    }
  }

  return ret;
}

/**
 * ------------------------------ObStorageGlobalIns------------------------------
 */
ObStorageGlobalIns::ObStorageGlobalIns()
  :io_prohibited_(false)
{}

ObStorageGlobalIns& ObStorageGlobalIns::get_instance()
{
  static ObStorageGlobalIns instance;
  return instance;
}

int ObStorageGlobalIns::init() 
{
  int ret = OB_SUCCESS;

  io_prohibited_ = false;

  return ret;
}
  
void ObStorageGlobalIns::fin()
{

}

void ObStorageGlobalIns::set_io_prohibited(bool prohibited)
{
  STORAGE_LOG_RET(WARN, OB_SUCCESS, "set_io_prohibited", K_(io_prohibited), K(prohibited));
  io_prohibited_ = prohibited;
}

bool ObStorageGlobalIns::is_io_prohibited() const
{
  return io_prohibited_;
}

/**
 * ------------------------------ObExternalIOCounter------------------------------
 */
int64_t ObExternalIOCounter::flying_io_cnt_ = 0;

void ObExternalIOCounter::inc_flying_io()
{
  ATOMIC_INC(&ObExternalIOCounter::flying_io_cnt_);
}

void ObExternalIOCounter::dec_flying_io()
{
  ATOMIC_DEC(&ObExternalIOCounter::flying_io_cnt_);
}

int64_t ObExternalIOCounter::get_flying_io_cnt()
{
  return ATOMIC_LOAD(&ObExternalIOCounter::flying_io_cnt_);
}

/**
 * ------------------------------ObExternalIOCounterGuard------------------------------
 */
ObExternalIOCounterGuard::ObExternalIOCounterGuard()
{
  io_counter_.inc_flying_io();
}

ObExternalIOCounterGuard::~ObExternalIOCounterGuard()
{
  io_counter_.dec_flying_io();
}

/**
 * ------------------------------ObStorageUtil------------------------------
 */
ObStorageUtil::ObStorageUtil()
  : file_util_(),
    oss_util_(),
    cos_util_(),
    s3_util_(),
    hdfs_util_(),
    util_(NULL),
    storage_info_(NULL),
    init_state(false),
    device_type_(ObStorageType::OB_STORAGE_MAX_TYPE)
{
}

/*this fun just like the get_util*/
int ObStorageUtil::open(common::ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  OBJECT_STORAGE_GUARD(storage_info, "", IO_HANDLED_SIZE_ZERO);

  if (is_init()) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "double init the storage util", K(ret));
  } else if (OB_ISNULL(storage_info) || OB_UNLIKELY(!storage_info->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KPC(storage_info));
  } else if (OB_FALSE_IT(device_type_ = storage_info->get_type())) {
  } else if (OB_STORAGE_OSS == device_type_) {
    util_ = &oss_util_;
  } else if (OB_STORAGE_COS == device_type_) {
    util_ = &cos_util_;
  } else if (OB_STORAGE_S3 == device_type_) {
    util_ = &s3_util_;
  } else if (OB_STORAGE_FILE == device_type_) {
    util_ = &file_util_;
  } else if (OB_STORAGE_HDFS == device_type_) {
    util_ = &hdfs_util_;
  } else {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid device type", K(ret), K_(device_type));
  }

  if (OB_SUCC(ret) && NULL != util_) {
    if (OB_FAIL(util_->open(storage_info))) {
      STORAGE_LOG(WARN, "failed to open util", K(ret), K_(device_type));
      util_ = NULL;
    } else {
      storage_info_ = storage_info;
      init_state = true;
    }
  }
  return ret;
}

void ObStorageUtil::close()
{
  int ret = OB_SUCCESS;
  OBJECT_STORAGE_GUARD(storage_info_, "", IO_HANDLED_SIZE_ZERO);
  if (NULL != util_) {
    util_->close();
    util_ = NULL;
    storage_info_ = NULL;
    init_state = false;
    device_type_ = OB_STORAGE_MAX_TYPE;
  }
}

// inner func
int ObStorageUtil::head_object_meta_(const ObString &uri, ObStorageObjectMetaBase &obj_meta)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_init())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "util is not inited", K(ret), K(uri));
  } else if (OB_UNLIKELY(!is_storage_type_match(uri, device_type_))) {
    ret = OB_INVALID_BACKUP_DEST;
    STORAGE_LOG(WARN, "uri prefix does not match the expected device type",
        K(ret), K(uri), K_(device_type));
  } else if (OB_FAIL(util_->head_object_meta(uri, obj_meta))) {
    EVENT_INC(ObStatEventIds::OBJECT_STORAGE_IO_HEAD_FAIL_COUNT);
    STORAGE_LOG(WARN, "fail to head object meta", K(ret), K(uri));
  }
  EVENT_INC(ObStatEventIds::OBJECT_STORAGE_IO_HEAD_COUNT);
  return ret;
}
/*---------------------------------- NEW ADAPTIVE INTERFACE ----------------------------------*/
int ObStorageUtil::detect_storage_obj_meta(
    const common::ObString &uri,
    const bool is_adaptive,
    const bool need_fragment_meta,
    ObStorageObjectMeta &obj_meta)
{
  int ret = OB_SUCCESS;
  OBJECT_STORAGE_GUARD(storage_info_, uri, IO_HANDLED_SIZE_ZERO);
  obj_meta.reset();
  bool need_detect_appendable = is_adaptive;
  if (OB_UNLIKELY(!is_init())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "util is not inited", K(ret), K(uri));
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer backup io is prohibited", K(ret), K(uri));
  } else if (OB_UNLIKELY(!is_storage_type_match(uri, device_type_))) {
    ret = OB_INVALID_BACKUP_DEST;
    STORAGE_LOG(WARN, "uri prefix does not match the expected device type",
        K(ret), K(uri), K_(device_type));
  } else if (OB_FAIL(head_object_meta_(uri, obj_meta))) {
    STORAGE_LOG(WARN, "fail to head object meta", K(ret), K(uri));
  } else if (obj_meta.is_exist_) {
    if (ObStorageObjectMetaType::OB_FS_DIR != obj_meta.type_) {
      // just return directly
      need_detect_appendable = false;
      obj_meta.type_ = ObStorageObjectMetaType::OB_OBJ_NORMAL;
    }
  }

  if (OB_SUCC(ret) && need_detect_appendable) {
    obj_meta.reset();
    int64_t pos = 0;
    // if need_fragment_meta is TRUE, we need to obtain fragment meta from seal_meta or list_info
    if (need_fragment_meta) {
      if (OB_FAIL(read_seal_meta_if_needed(uri, obj_meta))) {
        OB_LOG(WARN, "fail to read seal meta if needed", K(ret), K(uri));
      }

      // If not exist seal meta, we need to list all files and check format meta exist or not.
      // format meta must exist, otherwise need to return error.
      if (OB_SUCC(ret) && !obj_meta.is_exist_) {
        if (OB_FAIL(list_appendable_file_fragments(uri, obj_meta))) {
          OB_LOG(WARN, "fail to list appendable file fragments", K(ret), K(uri));
        }
      }
    } else {
      // if need_fragment_meta is FALSE, just check format_meta exist or not
      char format_meta_uri[OB_MAX_URI_LENGTH] = { 0 };
      if (OB_FAIL(construct_fragment_full_name(uri, OB_S3_APPENDABLE_FORMAT_META,
                                               format_meta_uri, sizeof(format_meta_uri)))) {
        OB_LOG(WARN, "fail to construct s3 format_meta name", K(ret), K(uri));
      } else if (OB_FAIL(head_object_meta_(format_meta_uri, obj_meta))) {
        STORAGE_LOG(WARN, "fail to head object meta", K(ret), K(format_meta_uri));
      } else if (obj_meta.is_exist_) {
        obj_meta.type_ = ObStorageObjectMetaType::OB_OBJ_SIMULATE_APPEND;
      } else {
        if (ObStorageObjectMetaType::OB_FS_DIR == obj_meta.type_) {
          ret = OB_INVALID_ARGUMENT;
          OB_LOG(WARN, "please set is_adaptive as false when want to check a common dir exist or not",
            K(ret), K(uri), K(is_adaptive));
        }
      }
    }
  }
  return ret;
}

int ObStorageUtil::read_seal_meta_if_needed(
    const common::ObString &uri,
    ObStorageObjectMeta &obj_meta)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObArenaAllocator allocator(APPENDABLE_OBJECT_ALLOCATOR);
  int64_t pos = 0;
  char *seal_meta_uri = NULL;

  if (OB_ISNULL(seal_meta_uri = static_cast<char *>(allocator.alloc(OB_MAX_URI_LENGTH)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "fail to alloc buf for reading seal meta file", K(ret));
  } else if (FALSE_IT(memset(seal_meta_uri, 0, OB_MAX_URI_LENGTH))) {
  } else if (OB_FAIL(construct_fragment_full_name(uri, OB_S3_APPENDABLE_SEAL_META,
                                                  seal_meta_uri, OB_MAX_URI_LENGTH))) {
    OB_LOG(WARN, "fail to construct s3 seal_meta name", K(ret), K(uri));
  } else {
    ObStorageReader *reader = nullptr;
    if (OB_ISNULL(reader = static_cast<ObStorageReader *>(allocator.alloc(sizeof(ObStorageReader))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "fail to alloc buf for reader", K(ret));
    } else if (FALSE_IT(new (reader) ObStorageReader())) {
    } else {
      int64_t seal_meta_len = 0;
      if (OB_FAIL(reader->open(seal_meta_uri, storage_info_))) {
        if (OB_OBJECT_NOT_EXIST == ret) {
          obj_meta.is_exist_ = false;
          ret = OB_SUCCESS;
        }
      } else {
        // If exist seal meta, directly read it content.
        seal_meta_len = reader->get_length();
        if (seal_meta_len > 0) {
          int64_t read_size = 0;
          char *buf = NULL;
          pos = 0;
          if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(seal_meta_len + 1)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            OB_LOG(WARN, "fail to alloc buf for reading seal meta file", K(ret), K(seal_meta_uri), K(seal_meta_len));
          } else if (OB_FAIL(reader->pread(buf, seal_meta_len, 0/*offset*/, read_size))) {
            OB_LOG(WARN, "failed to read seal meta file content", K(ret), K(seal_meta_uri), K(seal_meta_len));
          } else if (OB_UNLIKELY(seal_meta_len != read_size)) {
            ret = OB_ERR_UNEXPECTED;
            OB_LOG(WARN, "fail to read seal meta file entire content",
                K(ret), K(seal_meta_uri), K(seal_meta_len), K(read_size));
          } else if (OB_FAIL(obj_meta.deserialize(buf, read_size, pos))) {
            OB_LOG(WARN, "fail to deserialize storage object meta", K(ret), K(seal_meta_uri), K(read_size), KP(buf));
          } else {
            obj_meta.is_exist_ = true;
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          OB_LOG(WARN, "the seal meta file is empty", K(ret), K(seal_meta_uri));
        }

        if (OB_TMP_FAIL(reader->close())) {
          OB_LOG(WARN, "fail to close reader", K(ret), K(tmp_ret), K(seal_meta_uri));
        }
      }
    }
    if (OB_NOT_NULL(reader)) {
      reader->~ObStorageReader();
      reader = nullptr;
    }
  }
  return ret;
}

int ObStorageUtil::list_appendable_file_fragments(
    const ObString &uri,
    ObStorageObjectMeta &obj_meta)
{
  int ret = OB_SUCCESS;
  int64_t appendable_file_length = -1;
  ListAppendableObjectFragmentOp op;
  obj_meta.reset();
  if (OB_FAIL(list_files(uri, false, op))) {
    OB_LOG(WARN, "failed to list appendable object fragments", K(ret), K(uri));
  } else if (OB_UNLIKELY(!op.exist_format_meta())) {
    ret = OB_OBJECT_NOT_EXIST;
    OB_LOG(WARN, "not exist valid appendable file, cuz lack of format_meta", K(ret), K(uri));
  } else if (FALSE_IT(obj_meta.is_exist_ = true)) {
  } else if (FALSE_IT(obj_meta.type_ = ObStorageObjectMetaType::OB_OBJ_SIMULATE_APPEND)) {
  } else if (OB_FAIL(op.gen_object_meta(obj_meta))) {
    OB_LOG(WARN, "fail to gen object meta based on the listed meta info", K(ret), K(uri));
  }
  return ret;
}

int ObStorageUtil::is_exist(const common::ObString &uri, const bool is_adaptive, bool &exist)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  exist = false;
  ObStorageObjectMeta obj_meta;
  OBJECT_STORAGE_GUARD(storage_info_, uri, IO_HANDLED_SIZE_ZERO);

#ifdef ERRSIM
  ret = OB_E(EventTable::EN_BACKUP_IO_IS_EXIST) OB_SUCCESS;
#endif
  if (OB_FAIL(ret)) {
    //do nothing
  } else if (OB_UNLIKELY(!is_storage_type_match(uri, device_type_))) {
    ret = OB_INVALID_BACKUP_DEST;
    STORAGE_LOG(WARN, "uri prefix does not match the expected device type",
        K(ret), K(uri), K_(device_type));
  } else if (OB_FAIL(detect_storage_obj_meta(uri, is_adaptive, false/*need_fragment_meta*/, obj_meta))) {
    OB_LOG(WARN, "fail to detect storage obj type", K(ret), K(uri), K(is_adaptive));
  } else {
    exist = obj_meta.is_exist_;
  }

  return ret;
}

int ObStorageUtil::is_directory(const common::ObString &uri,
                                const bool is_adaptive, bool &is_directory)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  is_directory = false;
  ObStorageObjectMeta obj_meta;
  OBJECT_STORAGE_GUARD(storage_info_, uri, IO_HANDLED_SIZE_ZERO);

  if (OB_FAIL(ret)) {
    //do nothing
  } else if (OB_UNLIKELY(!is_storage_type_match(uri, device_type_))) {
    ret = OB_INVALID_BACKUP_DEST;
    STORAGE_LOG(WARN, "uri prefix does not match the expected device type",
        K(ret), K(uri), K_(device_type));
  } else if (OB_FAIL(detect_storage_obj_meta(
                 uri, is_adaptive, false /*need_fragment_meta*/, obj_meta))) {
    OB_LOG(WARN, "fail to detect storage obj type", K(ret), K(uri),
           K(is_adaptive));
  } else {
    is_directory = obj_meta.is_dir_type();
  }

  return ret;
}

int ObStorageUtil::get_file_length(const common::ObString &uri, const bool is_adaptive, int64_t &file_length)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  file_length = -1;
  ObStorageObjectMeta obj_meta;
  OBJECT_STORAGE_GUARD(storage_info_, uri, IO_HANDLED_SIZE_ZERO);

#ifdef ERRSIM
  ret = OB_E(EventTable::EN_BACKUP_IO_GET_FILE_LENGTH) OB_SUCCESS;
#endif
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!is_storage_type_match(uri, device_type_))) {
    ret = OB_INVALID_BACKUP_DEST;
    STORAGE_LOG(WARN, "uri prefix does not match the expected device type",
        K(ret), K(uri), K_(device_type));
  } else if (OB_FAIL(detect_storage_obj_meta(uri, is_adaptive, true/*need_fragment_meta*/, obj_meta))) {
    OB_LOG(WARN, "fail to detect storage obj type", K(ret), K(uri), K(is_adaptive));
  } else if (!obj_meta.is_exist_) {
    ret = OB_OBJECT_NOT_EXIST;
    OB_LOG(INFO, "cannot get file length for not exist file", K(ret), K(uri));
  } else {
    file_length = obj_meta.length_;
  }

  if (OB_SUCC(ret)) {
    if (file_length == 0) {
      OB_LOG(INFO, "this file is empty", K(ret), K(uri), K(file_length));
    } else if (file_length < 0) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "this file length is invalid", K(ret), K(uri), K(file_length));
    }
  }

  return ret;
}

int ObStorageUtil::del_file(const common::ObString &uri, const bool is_adaptive)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  ObStorageObjectMeta obj_meta;
  OBJECT_STORAGE_GUARD(storage_info_, uri, IO_HANDLED_SIZE_ZERO);

#ifdef ERRSIM
  ret = OB_E(EventTable::EN_BACKUP_IO_BEFORE_DEL_FILE) OB_SUCCESS;
#endif
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!is_storage_type_match(uri, device_type_))) {
    ret = OB_INVALID_BACKUP_DEST;
    STORAGE_LOG(WARN, "uri prefix does not match the expected device type",
        K(ret), K(uri), K_(device_type));
  } else if (OB_FAIL(detect_storage_obj_meta(uri, is_adaptive, false/*need_fragment_meta*/, obj_meta))) {
    OB_LOG(WARN, "fail to detect storage obj type", K(ret), K(uri), K(is_adaptive));
  } else if (obj_meta.is_simulate_append_type()) {
    if (OB_FAIL(del_appendable_file(uri))) {
      OB_LOG(WARN, "fail to delete appendable file", K(ret), K(uri));
    }
  } else {
    if (OB_FAIL(util_->del_file(uri))) {
      EVENT_INC(ObStatEventIds::BACKUP_IO_DEL_FAIL_COUNT);
      OB_LOG(WARN, "fail to delete file", K(ret), K(uri));
    }
    EVENT_INC(ObStatEventIds::BACKUP_DELETE_COUNT);
    EVENT_ADD(ObStatEventIds::BACKUP_DELETE_DELAY, ObTimeUtility::current_time() - start_ts);
  }

  if (OB_SUCC(ret)) {
    OB_LOG(DEBUG, "succ to delete file", K(ret), K(uri));
  } else {
    OB_LOG(WARN, "fail to delete file", K(ret), K(uri));
  }

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_BACKUP_IO_AFTER_DEL_FILE) OB_SUCCESS;
  }
#endif
  return ret;
}

int ObStorageUtil::del_appendable_file(const ObString &uri)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  if (OB_UNLIKELY(!is_init())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "util is not inited", K(ret), K(uri));
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer backup io is prohibited", K(ret), K(uri));
  } else if (OB_UNLIKELY(!is_storage_type_match(uri, device_type_))) {
    ret = OB_INVALID_BACKUP_DEST;
    STORAGE_LOG(WARN, "uri prefix does not match the expected device type",
        K(ret), K(uri), K_(device_type));
  } else {
    DelAppendableObjectFragmentOp op(uri, *this);
    if (OB_FAIL(list_files(uri, false/*is_adaptive*/, op))) {
      OB_LOG(WARN, "fail to list appendable object fragments", K(ret), K(uri));
    }
  }
  return ret;
}

int ObStorageUtil::list_files(
    const common::ObString &uri,
    const bool is_adaptive,
    common::ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  char uri_buf[OB_MAX_URI_LENGTH] = {0};
  OBJECT_STORAGE_GUARD(storage_info_, uri, IO_HANDLED_SIZE_ZERO);

  if (OB_UNLIKELY(!is_init())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "util is not inited", K(ret), K(uri));
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer backup io is prohibited", K(ret), K(uri));
  } else if (OB_UNLIKELY(!is_storage_type_match(uri, device_type_))) {
    ret = OB_INVALID_BACKUP_DEST;
    STORAGE_LOG(WARN, "uri prefix does not match the expected device type",
        K(ret), K(uri), K_(device_type));
  } else if (OB_FAIL(build_full_dir_path(uri.ptr(), uri_buf, sizeof(uri_buf)))) {
    OB_LOG(WARN, "fail to make uri end with '/'", K(ret), K(uri));
  } else if (is_adaptive && OB_FAIL(list_adaptive_files(uri_buf, op))) {
    STORAGE_LOG(WARN, "fail to list adaptive files", K(ret), K(uri));
  } else if (!is_adaptive && OB_FAIL(util_->list_files(uri_buf, op))) {
    STORAGE_LOG(WARN, "fail to list files", K(ret), K(uri));
  }

  if (OB_FAIL(ret)) {
    EVENT_INC(ObStatEventIds::BACKUP_IO_LS_FAIL_COUNT);
  }

  EVENT_INC(ObStatEventIds::BACKUP_IO_LS_COUNT);

  return ret;
}

int ObStorageUtil::list_adaptive_files(
    const common::ObString &uri,
    common::ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  OBJECT_STORAGE_GUARD(storage_info_, uri, IO_HANDLED_SIZE_ZERO);

  ObArenaAllocator allocator(APPENDABLE_OBJECT_ALLOCATOR);
  ObStorageListObjectsCtx list_obj_ctx;
  ObStorageListFilesCtx list_file_ctx;
  ObStorageListCtxBase *list_ctx = NULL;
  bool is_obj_storage = (OB_STORAGE_FILE != device_type_);
  const bool need_get_size = op.need_get_file_size();

  ObString bucket;
  ObString dir_path;

  if (OB_UNLIKELY(!is_init())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "util is not inited", K(ret), K(uri));
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer backup io is prohibited", K(ret), K(uri));
  } else if (OB_UNLIKELY(!is_storage_type_match(uri, device_type_))) {
    ret = OB_INVALID_BACKUP_DEST;
    STORAGE_LOG(WARN, "uri prefix does not match the expected device type",
        K(ret), K(uri), K_(device_type));
  } else if (OB_FAIL(build_bucket_and_object_name(allocator, uri, bucket, dir_path))) {
    OB_LOG(WARN, "fail to build bucket and object name", K(ret), K(uri));
  }
  const char *marker = op.get_marker();
  if (OB_FAIL(ret)) {
  } else if (op.is_marker_scan() && OB_ISNULL(marker)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "marker should not be null for marker scan", K(ret), KP(marker));
  } else if (is_obj_storage) {
    if (OB_FAIL(list_obj_ctx.init(allocator, OB_STORAGE_LIST_MAX_NUM, need_get_size))) {
      OB_LOG(WARN, "fail to init list_obj_ctx", K(ret), K(need_get_size));
    } else {
      list_ctx = &list_obj_ctx;
    }

    if (op.is_marker_scan()) {
      if (!is_null_or_end_with_slash(dir_path.ptr())) {
        ret = OB_INVALID_ARGUMENT;
        OB_LOG(WARN, "dir_path not end with '/'", K(ret), K(uri), K(marker), K(dir_path));
      } else if (marker[0] != '\0') {
        int64_t pos = 0;
        char marker_buf[OB_MAX_URI_LENGTH];
        // if we want to list bucket, the maker should equal to the original name, else we need to concat dir_path and original marker
        // e.g. marker = "a/b/c", dir_path = "d/e/", then the next marker should be "d/e/a/b/c"
        // e.g. maker = "a/b/c", dir_path = "", then the next marker should be "a/b/c"
        if (!dir_path.empty() && OB_FAIL(databuff_printf(marker_buf, sizeof(marker_buf), pos, "%s%s",
                                    dir_path.ptr(), marker))) {
          OB_LOG(WARN, "fail to construct next token", K(ret), K(uri), K(dir_path), K(marker));
        } else if (dir_path.empty() && OB_FAIL(databuff_printf(marker_buf, sizeof(marker_buf), pos, "%s", marker))) {
          OB_LOG(WARN, "fail to construct next token", K(ret), K(uri), K(dir_path), K(marker));
        } else if (OB_FAIL(list_obj_ctx.set_next_token(true/*has_next*/, marker_buf, pos))) {
          OB_LOG(WARN, "fail to set next token", K(ret), K(uri), K(marker), K(marker_buf));
        } else if (OB_FAIL(list_obj_ctx.set_marker(marker))) {
          OB_LOG(WARN, "fail to set list ctx marker", K(ret), K(uri), K(marker));
        }
      }

      if (OB_SUCC(ret)) {
        list_obj_ctx.set_total_list_limit(op.get_scan_count());
      }
    }
  } else {
    if (OB_FAIL(list_file_ctx.init(allocator, OB_STORAGE_LIST_MAX_NUM, need_get_size))) {
      OB_LOG(WARN, "fail to init list_file_ctx", K(ret), K(need_get_size));
    } else {
      list_ctx = &list_file_ctx;
    }
  }
  if (OB_SUCC(ret)) {
    do {
      list_ctx->rsp_num_ = 0;
      list_ctx->has_next_ = false;
      if (OB_FAIL(util_->list_files(uri, *list_ctx))) {
        OB_LOG(WARN, "fail to list files", K(ret), K(uri), K(list_ctx));
      } else if (list_ctx->rsp_num_ > 0) {
        if (is_obj_storage) {
          if (OB_FAIL(handle_listed_objs(list_ctx, uri, dir_path, op))) {
            OB_LOG(WARN, "fail to handle listed objs", K(ret), K(uri), K(dir_path), K(list_ctx));
          }
        } else {
          if (OB_FAIL(handle_listed_fs(list_ctx, uri, dir_path, op))) {
            OB_LOG(WARN, "fail to handle listed fs", K(ret), K(uri), K(dir_path), K(list_ctx));
          }
        }
      }
    } while (OB_SUCC(ret) && list_ctx->has_next_ && !list_ctx->has_reached_list_limit());
  }

  if (OB_FAIL(ret)) {
    EVENT_INC(ObStatEventIds::BACKUP_IO_LS_FAIL_COUNT);
  }

  EVENT_INC(ObStatEventIds::BACKUP_IO_LS_COUNT);

  return ret;
}

int ObStorageUtil::handle_listed_objs(
    ObStorageListCtxBase *ctx_base,
    const common::ObString &uri,
    const common::ObString &dir_path,
    common::ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_base)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret));
  } else {
    ObStorageListObjectsCtx *list_ctx = static_cast<ObStorageListObjectsCtx *>(ctx_base);
    int64_t full_dir_path_len = dir_path.length();
    if (!is_null_or_end_with_slash(dir_path.ptr())) {
      full_dir_path_len++;
    }
    char cur_appendable_full_path[OB_MAX_URI_LENGTH] = {0};
    bool already_exist_fragment = (strlen(list_ctx->cur_appendable_full_obj_path_) > 0);
    for (int64_t i = 0; OB_SUCC(ret) && (i < list_ctx->rsp_num_) && !list_ctx->has_reached_list_limit(); ++i) {
      ObString cur_obj_path(strlen(list_ctx->name_arr_[i]), list_ctx->name_arr_[i]);
      char *contain_idx_ptr = strstr(cur_obj_path.ptr(), OB_S3_APPENDABLE_FRAGMENT_PREFIX);
      bool is_fragment_obj = (nullptr != contain_idx_ptr);
      if (is_fragment_obj) {
        const int64_t appendable_full_path_len = (contain_idx_ptr - cur_obj_path.ptr());
        if (OB_UNLIKELY(appendable_full_path_len <= full_dir_path_len ||
                        appendable_full_path_len >= OB_MAX_URI_LENGTH)) {
          ret = OB_ERR_UNEXPECTED;
          OB_LOG(WARN, "appendable obj full path len is invalid", K(ret), K(appendable_full_path_len),
              K(full_dir_path_len), K(OB_MAX_URI_LENGTH), K(dir_path), K(cur_obj_path));
        } else {
          if (!already_exist_fragment) {
            MEMCPY(list_ctx->cur_appendable_full_obj_path_, cur_obj_path.ptr(), appendable_full_path_len);
            list_ctx->cur_appendable_full_obj_path_[appendable_full_path_len] = '\0';
            already_exist_fragment = true;
          } else {
            // check current fragment's parent dir is same with the previous one
            // if not, we need to handle previous one, and then save current one into list_ctx
            MEMCPY(cur_appendable_full_path, cur_obj_path.ptr(), appendable_full_path_len);
            cur_appendable_full_path[appendable_full_path_len] = '\0';
            if (0 != STRCMP(list_ctx->cur_appendable_full_obj_path_, cur_appendable_full_path)) {
              if (OB_FAIL(handle_listed_appendable_obj(list_ctx, uri, dir_path, op))) {
                OB_LOG(WARN, "fail to handle listed appendable obj", K(ret), K(uri), K(dir_path),
                    K(cur_appendable_full_path), K(list_ctx->cur_appendable_full_obj_path_));
              } else {
                MEMCPY(list_ctx->cur_appendable_full_obj_path_, cur_obj_path.ptr(), appendable_full_path_len);
                list_ctx->cur_appendable_full_obj_path_[appendable_full_path_len] = '\0';
              }
            }
          }
        }
      } else {
        // use @op to handle previous appendable object name
        if (already_exist_fragment) {
          if (OB_FAIL(handle_listed_appendable_obj(list_ctx, uri, dir_path, op))) {
            OB_LOG(WARN, "fail to handle listed appendable obj", K(ret), K(uri), K(dir_path));
          } else {
            already_exist_fragment = false;
          }
        }

        // use @op to handle current normal object name
        if (OB_SUCC(ret) && !list_ctx->has_reached_list_limit()) {
          const int64_t size = op.need_get_file_size() ? list_ctx->size_arr_[i] : -1;
          if (OB_FAIL(handle_listed_object(op, cur_obj_path.ptr() + full_dir_path_len,
                                           cur_obj_path.length() - full_dir_path_len,
                                           size))) {
            OB_LOG(WARN, "fail to handle listed object", K(ret), K(cur_obj_path),
                K(cur_obj_path.length()), K(full_dir_path_len), K(dir_path), K(size));
          } else {
            list_ctx->inc_cur_listed_count();
          }
        }
      }
    }

    if (OB_SUCC(ret) && !list_ctx->has_reached_list_limit()) {
      if (already_exist_fragment && strlen(list_ctx->cur_appendable_full_obj_path_) > 0 && !list_ctx->has_next_) {
        if (OB_FAIL(handle_listed_appendable_obj(list_ctx, uri, dir_path, op))) {
          OB_LOG(WARN, "fail to handle listed appendable obj", K(ret), K(uri), K(dir_path));
        } else {
          already_exist_fragment = false;
        }
      }
    }
  }
  return ret;
}

int ObStorageUtil::handle_listed_appendable_obj(
    ObStorageListObjectsCtx *list_ctx,
    const common::ObString &uri,
    const common::ObString &dir_path,
    common::ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  bool need_handle_file = true;
  int64_t dir_path_len = dir_path.length();
  int64_t full_dir_path_len = dir_path.length();
  if (!is_null_or_end_with_slash(dir_path.ptr())) {
    full_dir_path_len++;
  }
  const int64_t appendable_full_path_len = strlen(list_ctx->cur_appendable_full_obj_path_);
  int64_t appendable_file_len = 0;

  // Assuming the file hierarchy is:
  // base_dir/
  // ├── a
  // ├── b_appendable
  // │    ├── 0-100
  // │    └── FORMAT_META
  // └── c
  // 'b_appendable' is 'SIMULATE_APPEND' type.
  // If the marker is 'b_appendable', the expected result is ["c"].
  // However, fragments of the 'b_appendable' have a greater lexicographical order than the marker.
  // Therefore, fragments of 'b_appendable' are included in the listed results
  // and are ultimately aggregated under the logical object name 'b_appendable',
  // which is necessary to be filtered out.
  if (op.is_marker_scan() && OB_NOT_NULL(list_ctx->marker_)) {
    // list_ctx->cur_appendable_full_obj_path_ is equal to full_dir_path + logic_apendable_obj_name + '/'
    list_ctx->cur_appendable_full_obj_path_[appendable_full_path_len - 1] = '\0';
    if (less_than(list_ctx->cur_appendable_full_obj_path_ + full_dir_path_len, list_ctx->marker_)) {
      need_handle_file = false;
    }
    list_ctx->cur_appendable_full_obj_path_[appendable_full_path_len - 1] = '/';
  }

  if (need_handle_file && op.need_get_file_size()) {
    char append_obj_uri[OB_MAX_URI_LENGTH] = {0};
    // uri is equal to "prefix + bucket + '/' + dir_path"
    const int64_t uri_prefix_len = uri.length() - dir_path_len;
    MEMCPY(append_obj_uri, uri.ptr(), uri_prefix_len);  // now append_obj_uri == prefix + bucket + '/'
    // cur_appendable_full_obj_path_ == full_dir_path + logic_apendable_obj_name + '/'
    // not copy the last character '/'
    MEMCPY(append_obj_uri + uri_prefix_len, list_ctx->cur_appendable_full_obj_path_, appendable_full_path_len - 1);
    append_obj_uri[uri_prefix_len + appendable_full_path_len - 1] = '\0';

    // after above operation, append_obj_uri is equal to "prefix + bucket + '/' + full_dir_path + apendable_obj_name"

    if (OB_FAIL(get_adaptive_file_length(append_obj_uri, appendable_file_len))) {
      OB_LOG(WARN, "fail to get adaptive file length", K(ret), KP(append_obj_uri));
    } else if (appendable_file_len == 0) {
      // For appendable file, if its length is 0, we think that this file should not exist in the listed result,
      // thus we will not handle this file.
      need_handle_file = false;
    } else if (appendable_file_len < 0) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "appendable file len should not be less than 0",
          K(ret), K(appendable_file_len), K(append_obj_uri));
    }
  }

  if (OB_SUCC(ret) && need_handle_file) {
    if (OB_FAIL(handle_listed_object(op, list_ctx->cur_appendable_full_obj_path_ + full_dir_path_len,
                                     appendable_full_path_len - full_dir_path_len - 1,
                                     appendable_file_len))) {
      OB_LOG(WARN, "fail to handle listed object",
          K(ret), K(full_dir_path_len), K(appendable_full_path_len));
    } else {
      list_ctx->cur_appendable_full_obj_path_[0] = '\0';
      list_ctx->inc_cur_listed_count();
    }
  }
  return ret;
}

int ObStorageUtil::handle_listed_fs(
    ObStorageListCtxBase *ctx_base,
    const common::ObString &uri,
    const common::ObString &dir_path,
    common::ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_base)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret));
  } else {
    ObStorageListFilesCtx *list_ctx = static_cast<ObStorageListFilesCtx *>(ctx_base);
    const bool is_slash_end = is_end_with_slash(uri.ptr());
    const char *slash_delimiter = is_slash_end ? "" : "/";
    char tmp_uri[OB_MAX_URI_LENGTH] = { 0 };

    int64_t size = 0;
    // When handling file system list_files, the result will only save file's name(not include dir path)
    for (int64_t i = 0; OB_SUCC(ret) && (i < list_ctx->rsp_num_); ++i) {
      const int64_t name_len = strlen(list_ctx->name_arr_[i]);
      // if the file's suffix is '/', that means this is a 'appendable' file. otherwise it is a normal file
      if (list_ctx->name_arr_[i][name_len - 1] == '/') {
        list_ctx->name_arr_[i][name_len - 1] = '\0';
        if (op.need_get_file_size()) {
          if (OB_FAIL(databuff_printf(tmp_uri, OB_MAX_URI_LENGTH, "%s%s%s", uri.ptr(), slash_delimiter,
              list_ctx->name_arr_[i]))) {
            STORAGE_LOG(WARN, "fail to build appendable file uri", K(ret), K(i), K(uri), K(is_slash_end),
              KP(list_ctx->name_arr_[i]));
          } else if (OB_FAIL(get_adaptive_file_length(tmp_uri, size))) {
            OB_LOG(WARN, "fail to get adaptive file length", K(ret), KP(tmp_uri));
          }
        }
      } else {
        if (op.need_get_file_size()) {
          size = list_ctx->size_arr_[i];
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(handle_listed_object(op, list_ctx->name_arr_[i], name_len, size))) {
          OB_LOG(WARN, "fail to handle listed object",
              K(ret), K(list_ctx->name_arr_[i]), K(dir_path), K(size));
        }
      }
    }
  }
  return ret;
}

int ObStorageUtil::list_directories(
    const common::ObString &uri,
    const bool is_adaptive,
    common::ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  char uri_buf[OB_MAX_URI_LENGTH] = {0};
  const int64_t start_ts = ObTimeUtility::current_time();
  OBJECT_STORAGE_GUARD(storage_info_, uri, IO_HANDLED_SIZE_ZERO);

#ifdef ERRSIM
  ret = OB_E(EventTable::EN_BACKUP_IO_LIST_FILE) OB_SUCCESS;
#endif

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(is_adaptive)) {
    ret = OB_NOT_SUPPORTED;
    OB_LOG(WARN, "not support list directories for adaptive mode", K(ret), K(uri), K(is_adaptive));
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer backup io is prohibited", K(ret), K(uri));
  } else if (OB_UNLIKELY(!is_init())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "util is not inited", K(ret), K(uri));
  } else if (OB_UNLIKELY(!is_storage_type_match(uri, device_type_))) {
    ret = OB_INVALID_BACKUP_DEST;
    STORAGE_LOG(WARN, "uri prefix does not match the expected device type",
        K(ret), K(uri), K_(device_type));
  } else if (OB_FAIL(build_full_dir_path(uri.ptr(), uri_buf, sizeof(uri_buf)))) {
    OB_LOG(WARN, "fail to make uri end with '/'", K(ret), K(uri));
  } else if (OB_FAIL(util_->list_directories(uri_buf, op))) {
    STORAGE_LOG(WARN, "failed to list_files", K(ret), K(uri), K(uri_buf));
  }

  if (OB_FAIL(ret)) {
    EVENT_INC(ObStatEventIds::BACKUP_IO_LS_FAIL_COUNT);
  }
  EVENT_INC(ObStatEventIds::BACKUP_IO_LS_COUNT);
  return ret;
}

/*---------------------------------- NEW ADAPTIVE INTERFACE ----------------------------------*/

int ObStorageUtil::is_exist(const common::ObString &uri, bool &exist)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  exist = false;
  OBJECT_STORAGE_GUARD(storage_info_, uri, IO_HANDLED_SIZE_ZERO);

#ifdef ERRSIM
  ret = OB_E(EventTable::EN_BACKUP_IO_IS_EXIST) OB_SUCCESS;
#endif
  if (OB_FAIL(ret)) {
    //do nothing
  } else if (OB_UNLIKELY(!is_init())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "util is not inited", K(ret), K(uri));
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer backup io is prohibited", K(ret), K(uri));
  } else if (OB_UNLIKELY(!is_storage_type_match(uri, device_type_))) {
    ret = OB_INVALID_BACKUP_DEST;
    STORAGE_LOG(WARN, "uri prefix does not match the expected device type",
        K(ret), K(uri), K_(device_type));
  } else if (OB_FAIL(util_->is_exist(uri, exist))) {
    STORAGE_LOG(WARN, "failed to check is exist", K(ret), K(uri));
  }

  if (OB_FAIL(ret)) {
    EVENT_INC(ObStatEventIds::OBJECT_STORAGE_IO_HEAD_FAIL_COUNT);
  }
  EVENT_INC(ObStatEventIds::OBJECT_STORAGE_IO_HEAD_COUNT);
  return ret;
}

int ObStorageUtil::get_file_length(const common::ObString &uri, int64_t &file_length)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  file_length = -1;
  OBJECT_STORAGE_GUARD(storage_info_, uri, IO_HANDLED_SIZE_ZERO);

#ifdef ERRSIM
  ret = OB_E(EventTable::EN_BACKUP_IO_GET_FILE_LENGTH) OB_SUCCESS;
#endif
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!is_init())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "util is not inited", K(ret), K(uri));
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer backup io is prohibited", K(ret), K(uri));
  } else if (OB_UNLIKELY(!is_storage_type_match(uri, device_type_))) {
    ret = OB_INVALID_BACKUP_DEST;
    STORAGE_LOG(WARN, "uri prefix does not match the expected device type",
        K(ret), K(uri), K_(device_type));
  } else if (OB_FAIL(util_->get_file_length(uri, file_length))) {
    if (OB_OBJECT_NOT_EXIST == ret) {
      STORAGE_LOG(INFO, "cannot get file length for not exist file", K(ret), K(uri));
    } else {
      STORAGE_LOG(WARN, "failed to get_file_length", K(ret), K(uri));
    }
  }

  if (OB_SUCC(ret)) {
    if (file_length == 0) {
      STORAGE_LOG(INFO, "this file is empty", K(ret), K(uri), K(file_length));
    } else if (file_length < 0) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "this file length is invalid", K(ret), K(uri), K(file_length));
    }
  }

  if (OB_FAIL(ret)) {
    EVENT_INC(ObStatEventIds::OBJECT_STORAGE_IO_HEAD_FAIL_COUNT);
  }
  EVENT_INC(ObStatEventIds::OBJECT_STORAGE_IO_HEAD_COUNT);
  return ret;
}

int ObStorageUtil::get_adaptive_file_length(const common::ObString &uri, int64_t &file_length)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  file_length = 0;
  ObStorageObjectMeta obj_meta;
  OBJECT_STORAGE_GUARD(storage_info_, uri, IO_HANDLED_SIZE_ZERO);
#ifdef ERRSIM
  ret = OB_E(EventTable::EN_BACKUP_IO_GET_FILE_LENGTH) OB_SUCCESS;
#endif
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!is_init())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "util is not inited", K(ret), K(uri));
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer backup io is prohibited", K(ret), K(uri));
  } else if (OB_UNLIKELY(!is_storage_type_match(uri, device_type_))) {
    ret = OB_INVALID_BACKUP_DEST;
    STORAGE_LOG(WARN, "uri prefix does not match the expected device type",
        K(ret), K(uri), K_(device_type));
  } else {
    if (OB_FAIL(read_seal_meta_if_needed(uri, obj_meta))) {
      OB_LOG(WARN, "fail to read seal meta if needed", K(ret), K(uri));
    }
    
    // If not exist seal meta, we need to list all files and check format meta exist or not.
    // format meta must exist, otherwise need to return error.
    if (OB_SUCC(ret) && !obj_meta.is_exist_) {
      if (OB_FAIL(list_appendable_file_fragments(uri, obj_meta))) {
        OB_LOG(WARN, "fail to list appendable file fragments", K(ret), K(uri));
      }
    }

    if (OB_SUCC(ret) && obj_meta.is_exist_) {
      file_length = obj_meta.length_;
    }
  }

  if (OB_SUCC(ret)) {
    if (file_length == 0) {
      STORAGE_LOG(INFO, "this file is empty", K(ret), K(uri), K(file_length));
    } else if (file_length < 0) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "this file length is invalid", K(ret), K(uri), K(file_length));
    }
  }

  return ret;
}

int ObStorageUtil::del_file(const common::ObString &uri)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  OBJECT_STORAGE_GUARD(storage_info_, uri, IO_HANDLED_SIZE_ZERO);

#ifdef ERRSIM
  ret = OB_E(EventTable::EN_BACKUP_IO_BEFORE_DEL_FILE) OB_SUCCESS;
#endif
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!is_init())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "util is not inited", K(ret), K(uri));
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer backup io is prohibited", K(ret), K(uri));
  } else if (OB_UNLIKELY(!is_storage_type_match(uri, device_type_))) {
    ret = OB_INVALID_BACKUP_DEST;
    STORAGE_LOG(WARN, "uri prefix does not match the expected device type",
        K(ret), K(uri), K_(device_type));
  } else if (OB_FAIL(util_->del_file(uri))) {
    OB_LOG(WARN, "failed to delete file", K(ret), K(uri));
  } else {
    OB_LOG(DEBUG, "succ to delete file", K(ret), K(uri));
  }

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_BACKUP_IO_AFTER_DEL_FILE) OB_SUCCESS;
  }
#endif

  if (OB_FAIL(ret)) {
    EVENT_INC(ObStatEventIds::BACKUP_IO_DEL_FAIL_COUNT);
  }
  EVENT_INC(ObStatEventIds::BACKUP_DELETE_COUNT);
  EVENT_ADD(ObStatEventIds::BACKUP_DELETE_DELAY, ObTimeUtility::current_time() - start_ts);
  return ret;
}

int ObStorageUtil::batch_del_files(
    const ObIArray<ObString> &files_to_delete, ObIArray<int64_t> &failed_files_idx)
{
  int ret = OB_SUCCESS;
  const char *files_uri = nullptr;
  if (files_to_delete.count() <= 0) {
    files_uri = "";
  } else {
    files_uri = files_to_delete.at(0).ptr();
  }
  OBJECT_STORAGE_GUARD(storage_info_, files_uri, IO_HANDLED_SIZE_ZERO);
  const char *mem_tag = "BatchDelFiles";
  const int64_t start_ts = ObTimeUtility::current_time();
  const int64_t n_files_to_delete = files_to_delete.count();
  int64_t cur_deleted_pos = 0;
  failed_files_idx.reset();
  const bool use_batch_del_flag =
      (OB_STORAGE_FILE != device_type_);

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!is_init())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "util is not inited", K(ret));
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer backup io is prohibited", K(ret));
  } else if (OB_UNLIKELY(0 == n_files_to_delete)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(n_files_to_delete));
  } else if (use_batch_del_flag) {
    hash::ObHashMap<ObString, int64_t> files_to_delete_map;
    const ObString &uri = files_to_delete.at(0);
    const char *prefix = nullptr;
    int64_t object_start = 0;
    char bucket_with_prefix[OB_MAX_URI_LENGTH] = { 0 };
    MEMSET(bucket_with_prefix, 0, sizeof(bucket_with_prefix));

    if (OB_FAIL(files_to_delete_map.create(177, mem_tag))) {
      STORAGE_LOG(WARN, "fail to create map", K(ret), K(uri), K_(device_type));
    } else if (OB_UNLIKELY(!is_storage_type_match(uri, device_type_))) {
      ret = OB_INVALID_BACKUP_DEST;
      STORAGE_LOG(WARN, "uri prefix does not match the expected device type",
          K(ret), K(uri), K_(device_type));
    } else if (OB_FAIL(get_storage_prefix_from_path(uri, prefix))) {
      STORAGE_LOG(WARN, "fail to get storage type prefix", K(ret), K(uri));
    } else {
      object_start = strlen(prefix);
      while (OB_SUCC(ret) && object_start < uri.length() && uri[object_start] != '/') {
        object_start++;
      }
      object_start++; // skip '/'
      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY(object_start >= uri.length()
          || object_start >= sizeof(bucket_with_prefix))) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "uri has no object or bucket too long", K(ret), K(uri), K(object_start));
      } else {
        MEMCPY(bucket_with_prefix, uri.ptr(), object_start);
        bucket_with_prefix[object_start] = '\0';
      }
    }

    // Due to the batch delete interface's limitation of handling only 1000 files at a time,
    // we need to iteratively build the file list for each batch delete call.
    cur_deleted_pos = 0;
    while (OB_SUCC(ret) && cur_deleted_pos < n_files_to_delete) {
      while (OB_SUCC(ret)
          && cur_deleted_pos < n_files_to_delete
          && files_to_delete_map.size() < OB_STORAGE_DEL_MAX_NUM) {
        const ObString &cur_uri = files_to_delete.at(cur_deleted_pos);
        if (OB_UNLIKELY(!cur_uri.prefix_match(bucket_with_prefix))) {
          ret = OB_INVALID_ARGUMENT;
          STORAGE_LOG(WARN, "all obejcts's bucket and prefix must be same",
              K(ret), K(uri), K(cur_uri), K(cur_deleted_pos), K(bucket_with_prefix));
        } else if (OB_FAIL(files_to_delete_map.set_refactored(cur_uri.ptr() + object_start,
                                                              cur_deleted_pos))) {
          STORAGE_LOG(WARN, "fail to add cur uri to map", K(ret), K(cur_uri),
              K(object_start), K(cur_deleted_pos), K(files_to_delete_map.size()));
        } else {
          cur_deleted_pos++;
        }
      }

      if (FAILEDx(util_->batch_del_files(uri, files_to_delete_map, failed_files_idx))) {
        STORAGE_LOG(WARN, "fail to batch del files",
            K(ret), K(uri), K(object_start), K(cur_deleted_pos));
      }
      files_to_delete_map.reuse();
    }
  }

  if (OB_NOT_SUPPORTED == ret
      || (OB_SUCC(ret) && !use_batch_del_flag)) {
    ret = OB_SUCCESS;
    STORAGE_LOG(WARN, "batch_del interface is not supported, "
        "falling back to a looped single-delete calls instead",
        K(n_files_to_delete), KPC_(storage_info), K_(device_type));
    // Initiating a loop to call the delete interface individually for each file
    cur_deleted_pos = 0;
    while (OB_SUCC(ret) && cur_deleted_pos < n_files_to_delete) {
      if (OB_FAIL(del_file(files_to_delete.at(cur_deleted_pos)))) {
        STORAGE_LOG(WARN, "fail to batch del files",
            K(ret), K(cur_deleted_pos), K(files_to_delete.at(cur_deleted_pos)));
      } else {
        cur_deleted_pos++;
      }
    }

    if (OB_FAIL(ret)) {
      // Attempting to record all objects that failed to be deleted
      int tmp_ret = OB_SUCCESS;
      for (int64_t i = cur_deleted_pos; OB_SUCCESS == tmp_ret && i < n_files_to_delete; i++) {
        if (OB_TMP_FAIL(failed_files_idx.push_back(i))) {
          STORAGE_LOG(WARN, "fail to record failed files idx",
              K(ret), K(tmp_ret), K(i), K(cur_deleted_pos));
        }
      }
      if (OB_SUCCESS == tmp_ret) {
        // Successfully recorded all objects that failed to delete,
        // resetting the error code accordingly.
        ret = OB_SUCCESS;
      }
    }
  }

  if (OB_FAIL(ret)) {
    EVENT_ADD(ObStatEventIds::BACKUP_IO_DEL_FAIL_COUNT, n_files_to_delete - failed_files_idx.count());
  }
  EVENT_ADD(ObStatEventIds::BACKUP_DELETE_COUNT, n_files_to_delete);
  EVENT_ADD(ObStatEventIds::BACKUP_DELETE_DELAY, ObTimeUtility::current_time() - start_ts);
  return ret;
}

int ObStorageUtil::mkdir(const common::ObString &uri)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  OBJECT_STORAGE_GUARD(storage_info_, uri, IO_HANDLED_SIZE_ZERO);

  STORAGE_LOG(DEBUG, "mkdir", K(uri));
#ifdef ERRSIM
  ret = OB_E(EventTable::EN_BACKUP_IO_BEFORE_MKDIR) OB_SUCCESS;
#endif
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!is_init())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "util is not inited", K(ret), K(uri));
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer backup io is prohibited", K(ret), K(uri));
  } else if (OB_UNLIKELY(!is_storage_type_match(uri, device_type_))) {
    ret = OB_INVALID_BACKUP_DEST;
    STORAGE_LOG(WARN, "uri prefix does not match the expected device type",
        K(ret), K(uri), K_(device_type));
  } else if (OB_FAIL(util_->mkdir(uri))) {
    STORAGE_LOG(WARN, "failed to mkdir", K(ret), K(uri));
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_BACKUP_IO_AFTER_MKDIR) OB_SUCCESS;
  }
#endif
  return ret;
}

int ObStorageUtil::list_files(const common::ObString &uri, common::ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  char uri_buf[OB_MAX_URI_LENGTH] = {0};
  const int64_t start_ts = ObTimeUtility::current_time();
  OBJECT_STORAGE_GUARD(storage_info_, uri, IO_HANDLED_SIZE_ZERO);

#ifdef ERRSIM
  ret = OB_E(EventTable::EN_BACKUP_IO_LIST_FILE) OB_SUCCESS;
#endif

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!is_init())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "util is not inited", K(ret), K(uri));
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer backup io is prohibited", K(ret), K(uri));
  } else if (OB_UNLIKELY(!is_storage_type_match(uri, device_type_))) {
    ret = OB_INVALID_BACKUP_DEST;
    STORAGE_LOG(WARN, "uri prefix does not match the expected device type",
        K(ret), K(uri), K_(device_type));
  } else if (OB_FAIL(build_full_dir_path(uri.ptr(), uri_buf, sizeof(uri_buf)))) {
    OB_LOG(WARN, "fail to make dir path end with '/'", K(ret), K(uri));
  } else if (OB_FAIL(util_->list_files(uri_buf, op))) {
    STORAGE_LOG(WARN, "failed to list_files", K(ret), K(uri), K(uri_buf));
  }

  if (OB_FAIL(ret)) {
    EVENT_INC(ObStatEventIds::BACKUP_IO_LS_FAIL_COUNT);
  }

  EVENT_INC(ObStatEventIds::BACKUP_IO_LS_COUNT);

  return ret;
}

int ObStorageUtil::write_single_file(const common::ObString &uri, const char *buf, const int64_t size)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  OBJECT_STORAGE_GUARD(storage_info_, uri, size);

#ifdef ERRSIM
  ret = OB_E(EventTable::EN_BACKUP_IO_BEFORE_WRITE_SINGLE_FILE) OB_SUCCESS;
#endif
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!is_init())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "util is not inited", K(ret), K(uri));
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer backup io is prohibited", K(ret), K(uri));
  } else if (OB_UNLIKELY(!is_storage_type_match(uri, device_type_))) {
    ret = OB_INVALID_BACKUP_DEST;
    STORAGE_LOG(WARN, "uri prefix does not match the expected device type",
        K(ret), K(uri), K_(device_type));
  } else if (OB_FAIL(util_->write_single_file(uri, buf, size))) {
    STORAGE_LOG(WARN, "failed to write single file", K(ret), K(uri));
  } else {
    EVENT_ADD(ObStatEventIds::BACKUP_IO_WRITE_BYTES, size);
  }

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_BACKUP_IO_AFTER_WRITE_SINGLE_FILE) OB_SUCCESS;
  }
#endif

  if (OB_FAIL(ret)) {
    EVENT_INC(ObStatEventIds::BACKUP_IO_WRITE_FAIL_COUNT);
  }

  EVENT_INC(ObStatEventIds::BACKUP_IO_WRITE_COUNT);
  EVENT_ADD(ObStatEventIds::BACKUP_IO_WRITE_DELAY, ObTimeUtility::current_time() - start_ts);

  return ret;
}

int ObStorageUtil::del_dir(const common::ObString &uri)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  OBJECT_STORAGE_GUARD(storage_info_, uri, IO_HANDLED_SIZE_ZERO);

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!is_init())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "util is not inited", K(ret), K(uri));
  } else if (OB_UNLIKELY(!is_storage_type_match(uri, device_type_))) {
    ret = OB_INVALID_BACKUP_DEST;
    STORAGE_LOG(WARN, "uri prefix does not match the expected device type",
        K(ret), K(uri), K_(device_type));
  } else if (OB_FAIL(util_->del_dir(uri))) {
    STORAGE_LOG(WARN, "failed to del_file", K(ret), K(uri));
  }
  return ret;
}

int ObStorageUtil::list_directories(const common::ObString &uri, common::ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  char uri_buf[OB_MAX_URI_LENGTH] = {0};
  const int64_t start_ts = ObTimeUtility::current_time();
  OBJECT_STORAGE_GUARD(storage_info_, uri, IO_HANDLED_SIZE_ZERO);

#ifdef ERRSIM
  ret = OB_E(EventTable::EN_BACKUP_IO_LIST_FILE) OB_SUCCESS;
#endif

  if (OB_FAIL(ret)) {
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer backup io is prohibited", K(ret), K(uri));
  } else if (OB_UNLIKELY(!is_init())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "util is not inited", K(ret), K(uri));
  } else if (OB_UNLIKELY(!is_storage_type_match(uri, device_type_))) {
    ret = OB_INVALID_BACKUP_DEST;
    STORAGE_LOG(WARN, "uri prefix does not match the expected device type",
        K(ret), K(uri), K_(device_type));
  } else if (OB_FAIL(build_full_dir_path(uri.ptr(), uri_buf, sizeof(uri_buf)))) {
    OB_LOG(WARN, "fail to make dir path end with '/'", K(ret), K(uri));
  } else if (OB_FAIL(util_->list_directories(uri_buf, op))) {
    STORAGE_LOG(WARN, "failed to list_directories", K(ret), K(uri), K(uri_buf));
  } 

  if (OB_FAIL(ret)) {
    EVENT_INC(ObStatEventIds::BACKUP_IO_LS_FAIL_COUNT);
  }
  EVENT_INC(ObStatEventIds::BACKUP_IO_LS_COUNT);
  return ret;
}


int ObStorageUtil::is_tagging(const common::ObString &uri, bool &is_tagging)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  OBJECT_STORAGE_GUARD(storage_info_, uri, IO_HANDLED_SIZE_ZERO);
  is_tagging = false;
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!is_init())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "util is not inited", K(ret), K(uri));
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer backup io is prohibited", K(ret), K(uri));
  } else if (OB_UNLIKELY(!is_storage_type_match(uri, device_type_))) {
    ret = OB_INVALID_BACKUP_DEST;
    STORAGE_LOG(WARN, "uri prefix does not match the expected device type",
        K(ret), K(uri), K_(device_type));
  } else if (OB_FAIL(util_->is_tagging(uri, is_tagging))) {
    STORAGE_LOG(WARN, "failed to check is tagging", K(ret), K(uri));
  }

  if (OB_FAIL(ret)) {
    EVENT_INC(ObStatEventIds::OBJECT_STORAGE_IO_HEAD_FAIL_COUNT);
  }
  EVENT_INC(ObStatEventIds::OBJECT_STORAGE_IO_HEAD_COUNT);
  return ret;
}


int ObStorageUtil::list_files_with_marker(const common::ObString &uri, common::ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  OBJECT_STORAGE_GUARD(storage_info_, uri, IO_HANDLED_SIZE_ZERO);
  const char *marker = op.get_marker();
  char uri_buf[OB_MAX_URI_LENGTH] = {0};
  const int64_t start_ts = ObTimeUtility::current_time();

  if (OB_UNLIKELY(!is_init())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "util is not inited", K(ret), K(uri));
  } else if (OB_UNLIKELY(uri.empty() || !op.is_marker_scan()) || OB_ISNULL(marker)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "marker ptr should not be nullptr or op type invalid", K(ret), K(uri), K(marker));
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer backup io is prohibited", K(ret), K(uri));
  } else if (OB_FAIL(validate_uri_type(uri))) {
    STORAGE_LOG(WARN, "fail to validate uri!", K(uri));
  } else if (OB_FAIL(build_full_dir_path(uri.ptr(), uri_buf, sizeof(uri_buf)))) {
    OB_LOG(WARN, "fail to make uri end with '/'", K(ret), K(uri));
  } else {}

  if (OB_FAIL(ret)) {
  } else if (OB_STORAGE_FILE != device_type_) {
    if (OB_FAIL(list_adaptive_files(uri_buf, op))) {
      STORAGE_LOG(WARN, "failed to list adaptive files with marker",
          K(ret), K(uri), K(uri_buf), K(marker));
    }
  } else {
    const int64_t scan_count = (op.get_scan_count() <= 0 ? INT64_MAX : op.get_scan_count());
    ObTopNMinimumDirEntryWithMarkerOperator top_n_op(scan_count, marker, op.need_get_file_size());

    if (OB_FAIL(list_adaptive_files(uri_buf, top_n_op))) {
      STORAGE_LOG(WARN, "failed to list adaptive files with marker",
          K(ret), K(uri), K(uri_buf), K(marker), K(scan_count));
    } else if (OB_FAIL(top_n_op.handle_each_dir_entry(op))) {
      STORAGE_LOG(WARN, "failed to handle_each_dir_entry", K(ret), K(uri), K(scan_count));
    }
  }


  return ret;
}

int ObStorageUtil::del_unmerged_parts(const common::ObString &uri)
{
  int ret = OB_NOT_SUPPORTED;
  OB_LOG(WARN, "del_unmerged_parts is not supported yet", K(ret), K(uri));
  // OBJECT_STORAGE_GUARD(storage_info_, uri, IO_HANDLED_SIZE_ZERO);
  // const int64_t start_ts = ObTimeUtility::current_time();
  // if (OB_FAIL(ret)) {
  // } else if (OB_UNLIKELY(!is_init())) {
  //   ret = OB_NOT_INIT;
  //   STORAGE_LOG(WARN, "util is not inited", K(ret), K(uri));
  // } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
  //   ret = OB_BACKUP_IO_PROHIBITED;
  //   STORAGE_LOG(WARN, "current observer backup io is prohibited", K(ret), K(uri));
  // else if (OB_UNLIKELY(!is_storage_type_match(uri, device_type_))) {
  //   ret = OB_INVALID_BACKUP_DEST;
  //   STORAGE_LOG(WARN, "uri prefix does not match the expected device type",
  //       K(ret), K(uri), K_(device_type));
  // } else if (OB_FAIL(util_->del_unmerged_parts(uri))) {
  //   STORAGE_LOG(WARN, "fail to del unmerged parts!", K(ret), K(uri));
  // }
  return ret;
}


/**
 * ------------------------------ObStorageReader------------------------------
 */
ObStorageAccesser::ObStorageAccesser() : is_inited_(false), ref_cnt_(0), device_holder_(), fd_()
{
}

ObStorageAccesser::~ObStorageAccesser()
{
  fd_.reset();
  device_holder_.reset();
  ref_cnt_ = 0;
  is_inited_ = false;
}

int ObStorageAccesser::init(const ObIOFd &fd, ObObjectDevice *device)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "init twice", K(ret));
  } else {
    fd_ = fd;
    device_holder_.hold(device);
    is_inited_ = true;
  }
  return ret;
}

void ObStorageAccesser::inc_ref()
{
  IGNORE_RETURN ATOMIC_FAA(&ref_cnt_, 1);
}

void ObStorageAccesser::dec_ref()
{
  int ret = OB_SUCCESS;
  int64_t tmp_ref = ATOMIC_SAF(&ref_cnt_, 1);
  if (tmp_ref < 0) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(ERROR, "ref_cnt < 0", K(ret), K(tmp_ref), KCSTRING(lbt()));
  } else if (0 == tmp_ref) {
    ObObjectDevice *object_device = device_holder_.get_ptr();
    // device may be null, when directly using ObStorageAccesser and not using ObObjectDevice::open
    if (OB_NOT_NULL(object_device)) {
      // relaese_fd will destruct ObStorageAccesser, which will reset fd_
      const ObIOFd fd = fd_;
      if (OB_FAIL(object_device->release_fd(fd))) {
        OB_LOG(WARN, "fail to release fd", K(ret), K(fd));
      }
    }
  }
}


/**
 * ------------------------------ObStorageReader------------------------------
 */
ObStorageReader::ObStorageReader()
  : file_length_(-1),
    reader_(NULL),
    file_reader_(),
    oss_reader_(),
    cos_reader_(),
    s3_reader_(),
    start_ts_(0),
    has_meta_(false),
    storage_info_(nullptr)
{
  uri_[0] = '\0';
}

ObStorageReader::~ObStorageReader()
{
  if (NULL != reader_) {
    STORAGE_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "reader not closed", KCSTRING(uri_));
  }
}

int ObStorageReader::open(const common::ObString &uri,
    common::ObObjectStorageInfo *storage_info, const bool head_meta)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObStorageType type = OB_STORAGE_MAX_TYPE;
  start_ts_ = ObTimeUtility::current_time();
  OBJECT_STORAGE_GUARD(storage_info, uri, IO_HANDLED_SIZE_ZERO);

#ifdef ERRSIM
  ret = OB_E(EventTable::EN_BACKUP_IO_READER_OPEN) OB_SUCCESS;
#endif
  if (OB_FAIL(ret)) {
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer backup io is prohibited", K(ret), K(uri));
  } else if (NULL != reader_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot open twice", K(ret), K(uri));
  } else if (OB_ISNULL(storage_info) || OB_UNLIKELY(uri.empty() || !storage_info->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(uri), KPC(storage_info));
  } else if (FALSE_IT(type = storage_info->get_type())) {
  } else if (OB_UNLIKELY(!is_storage_type_match(uri, type))) {
    ret = OB_INVALID_BACKUP_DEST;
    STORAGE_LOG(WARN, "uri prefix does not match the expected device type",
        K(ret), K(uri), KPC(storage_info), K(type));
  } else if (OB_FAIL(databuff_printf(uri_, sizeof(uri_), "%.*s", uri.length(), uri.ptr()))) {
    STORAGE_LOG(WARN, "failed to fill uri", K(ret), K(uri));
  } else if (FALSE_IT(storage_info_ = storage_info)) {
  } else if (OB_STORAGE_OSS == type) {
    reader_ = &oss_reader_;
  } else if (OB_STORAGE_COS == type) {
    reader_ = &cos_reader_;
  } else if (OB_STORAGE_S3 == type) {
    reader_ = &s3_reader_;
  } else if (OB_STORAGE_FILE == type) {
    reader_ = &file_reader_;
  } else if (OB_STORAGE_HDFS == type) {
    reader_ = &hdfs_reader_;
  } else {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "unkown storage type", K(ret), K(uri));
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(reader_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "reader_ is null", K(ret), K(uri));
    } else if (OB_FAIL(reader_->open(uri, storage_info, head_meta))) {
      STORAGE_LOG(WARN, "failed to open reader", K(ret), K(uri), KPC(storage_info), K(head_meta));
    } else {
      has_meta_ = head_meta;
      if (head_meta) {
        file_length_ = reader_->get_length();
      }
    }
  }

  if(OB_FAIL(ret)) {
    if (OB_SUCCESS != (tmp_ret = close())) {
      STORAGE_LOG(WARN, "failed to close read file", K(ret), K(tmp_ret), K(uri));
    }
  } else {
    storage_info_ = storage_info;
  }


  if (!head_meta) {
  } else {
    if (OB_FAIL(ret)) {
      EVENT_INC(ObStatEventIds::OBJECT_STORAGE_IO_HEAD_FAIL_COUNT);
    }
    EVENT_INC(ObStatEventIds::OBJECT_STORAGE_IO_HEAD_COUNT);
  }
  return ret;
}

int ObStorageReader::pread(char *buf, const int64_t buf_size, int64_t offset, int64_t &read_size)
{
  int ret = OB_SUCCESS;
  read_size = 0;
  OBJECT_STORAGE_GUARD(storage_info_, uri_, read_size);

#ifdef ERRSIM
  ret = OB_E(EventTable::EN_BACKUP_IO_READER_PREAD) OB_SUCCESS;
#endif

  const int64_t start_ts = ObTimeUtility::current_time();
  if (OB_FAIL(ret)) {
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer backup io is prohibited", K(ret), K(offset), K(buf_size));
  } else if (OB_ISNULL(reader_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not opened", K(ret));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(offset < 0 || (has_meta_ && offset > file_length_))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), KP(buf), K(offset), K_(has_meta), K_(file_length));
  } else if (OB_FAIL(reader_->pread(buf, buf_size, offset, read_size))) {
    EVENT_INC(ObStatEventIds::BACKUP_IO_READ_FAIL_COUNT);
    STORAGE_LOG(WARN, "failed to read file", K(ret));
  } else {
    EVENT_ADD(ObStatEventIds::BACKUP_IO_READ_BYTES, read_size);
  }

  EVENT_INC(ObStatEventIds::BACKUP_IO_READ_COUNT);
  EVENT_ADD(ObStatEventIds::BACKUP_IO_READ_DELAY, ObTimeUtility::current_time() - start_ts);

  return ret;
}

int ObStorageReader::close()
{
  int ret = OB_SUCCESS;
  OBJECT_STORAGE_GUARD(storage_info_, uri_, IO_HANDLED_SIZE_ZERO);

  if (OB_ISNULL(reader_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not opened", K(ret));
  } else if (OB_FAIL(reader_->close())) {
    STORAGE_LOG(WARN, "failed to close reader", K(ret));
  }
  reader_  = NULL;
  file_length_ = -1;
  start_ts_ = 0;
  uri_[0] = '\0';
  has_meta_ = false;
  storage_info_ = nullptr;
  return ret;
}

ObStorageAdaptiveReader::ObStorageAdaptiveReader()
    : allocator_(APPENDABLE_OBJECT_ALLOCATOR),
      meta_(),
      bucket_(),
      object_(),
      reader_(NULL),
      file_reader_(),
      oss_reader_(),
      cos_reader_(),
      s3_reader_(),
      hdfs_reader_(),
      start_ts_(0),
      storage_info_(NULL)
{
  uri_[0] = '\0';
}

ObStorageAdaptiveReader::~ObStorageAdaptiveReader()
{
  if (NULL != reader_) {
    STORAGE_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "adaptive reader not closed", KCSTRING(uri_));
  }
}

template<typename ReaderType>
static int alloc_reader_type(ObIAllocator &allocator, ObIStorageReader *&reader)
{
  int ret = OB_SUCCESS;
  ReaderType *r = nullptr;
  r = static_cast<ReaderType *>(allocator.alloc(sizeof(ReaderType)));
  if (OB_NOT_NULL(r)) {
    new (r) ReaderType();
    reader = static_cast<ObIStorageReader *>(r);
  } else {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "failed to allocate storage reader", K(ret), K(sizeof(ReaderType)));
  }
  return ret;
}

static int alloc_reader(ObIAllocator &allocator, const ObStorageType &type, ObIStorageReader *&reader)
{
  int ret = OB_SUCCESS;
  reader = nullptr;
  if (OB_STORAGE_OSS == type) {
    ret = alloc_reader_type<ObStorageOssReader>(allocator, reader);
  } else if (OB_STORAGE_COS == type) {
    ret = alloc_reader_type<ObStorageCosReader>(allocator, reader);
  } else if (OB_STORAGE_S3 == type) {
    ret = alloc_reader_type<ObStorageS3Reader>(allocator, reader);
  } else if (OB_STORAGE_FILE == type) {
    ret = alloc_reader_type<ObStorageFileReader>(allocator, reader);
  } else if (OB_STORAGE_HDFS == type) {
    ret = alloc_reader_type<ObStorageHdfsReader>(allocator, reader);
  } else {
    ret = OB_INVALID_ARGUMENT;
  }

  if (OB_FAIL(ret)) {
    OB_LOG(WARN, "failed to allocate storage reader", K(ret), K(type));
  } else if (OB_ISNULL(reader)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "failed to allocate storage reader", K(ret), K(type));
  }
  return ret;
}

int ObStorageAdaptiveReader::open(const common::ObString &uri,
                                  common::ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObStorageType type = OB_STORAGE_MAX_TYPE;
  start_ts_ = ObTimeUtility::current_time();
  ObStorageUtil util;
  OBJECT_STORAGE_GUARD(storage_info, uri, IO_HANDLED_SIZE_ZERO);

  if (OB_FAIL(ret)) {
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer backup io is prohibited", K(ret), K(uri));
  } else if (NULL != reader_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot open twice", K(ret), K(uri));
  } else if (OB_ISNULL(storage_info) || OB_UNLIKELY(uri.empty() || !storage_info->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(uri), KPC(storage_info));
  } else if (FALSE_IT(type = storage_info->get_type())) {
  } else if (OB_UNLIKELY(!is_storage_type_match(uri, type))) {
    ret = OB_INVALID_BACKUP_DEST;
    STORAGE_LOG(WARN, "uri prefix does not match the expected device type",
        K(ret), K(uri), KPC(storage_info), K(type));
  } else if (OB_FAIL(databuff_printf(uri_, sizeof(uri_), "%.*s", uri.length(), uri.ptr()))) {
    STORAGE_LOG(WARN, "failed to fill uri", K(ret), K(uri));
  } else if (FALSE_IT(storage_info_ = storage_info)) {
  } else if (OB_STORAGE_OSS == type) {
    reader_ = &oss_reader_;
  } else if (OB_STORAGE_COS == type) {
    reader_ = &cos_reader_;
  } else if (OB_STORAGE_S3 == type) {
    reader_ = &s3_reader_;
  } else if (OB_STORAGE_FILE == type) {
    reader_ = &file_reader_;
  } else if (OB_STORAGE_HDFS == type) {
    reader_ = &hdfs_reader_;
  } else {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "unkown storage type", K(ret), K(uri));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(reader_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "reader_ is null", K(ret), K(uri));
  } else if (OB_FAIL(util.open(storage_info))) {
    OB_LOG(WARN, "fail to open util", K(ret), K(uri), KPC(storage_info));
  } else if (OB_FAIL(util.detect_storage_obj_meta(uri, true/*is_adaptive*/,
                                                  true/*need_fragment_meta*/, meta_))) {
    OB_LOG(WARN, "fail to detect obejct meta", K(ret), K(uri), KPC(storage_info));
  } else if (meta_.is_simulate_append_type()) {
    // no need to open reader
  } else if (meta_.is_object_file_type()) {
    if (OB_FAIL(reader_->open(uri, storage_info))) {
      EVENT_INC(ObStatEventIds::OBJECT_STORAGE_IO_HEAD_FAIL_COUNT);
      OB_LOG(WARN, "fail to open reader", K(ret), K(uri), KPC(storage_info));
    }
    EVENT_INC(ObStatEventIds::OBJECT_STORAGE_IO_HEAD_COUNT);
  } else {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "invalid storage object type", K(ret), K(uri), KPC(storage_info), K_(meta));
  }

  if(OB_FAIL(ret)) {
    if (OB_SUCCESS != (tmp_ret = close())) {
      STORAGE_LOG(WARN, "failed to close read file", K(ret), K(tmp_ret), K(uri));
    }
  }

  return ret;
}

int ObStorageAdaptiveReader::pread(char *buf,
    const int64_t buf_size, const int64_t offset, int64_t &read_size)
{
  int ret = OB_SUCCESS;
  read_size = 0;
  const int64_t start_ts = ObTimeUtility::current_time();
  OBJECT_STORAGE_GUARD(storage_info_, uri_, read_size);

  if (OB_FAIL(ret)) {
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    OB_LOG(WARN, "current observer backup io is prohibited", K(ret), K(offset), K(buf_size));
  } else if (OB_ISNULL(reader_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "adaptive reader not opened", K(ret));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(buf_size <= 0 || offset < 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid args", K(ret), KP(buf), K(buf_size), K(offset), K_(meta));
  } else if (OB_UNLIKELY(offset > meta_.length_)) {
    ret = OB_FILE_LENGTH_INVALID;
    OB_LOG(WARN, "offset is larger than file length", K(ret), K(offset), K_(meta));
  } else if (offset == meta_.length_) {
    ret = OB_SUCCESS;
    read_size = 0;
  } else if (meta_.is_object_file_type()) {
    if (OB_FAIL(reader_->pread(buf, buf_size, offset, read_size))) {
      OB_LOG(WARN, "fail to read object", K(ret), K_(meta));
    }
    EVENT_INC(ObStatEventIds::BACKUP_IO_READ_COUNT);
  } else if (meta_.is_simulate_append_type()) {
    // To enable parallel pread,
    // use a temporary allocator/reader for each pread call instead of using allocator_/reader_
    ObArenaAllocator tmp_allocator(APPENDABLE_OBJECT_ALLOCATOR);
    ObIStorageReader *tmp_reader = nullptr;
    char uri[OB_MAX_URI_LENGTH] = { 0 };
    ObArray<ObAppendableFragmentMeta> fragments_need_to_read;

    if (OB_FAIL(alloc_reader(tmp_allocator, storage_info_->get_type(), tmp_reader))) {
      OB_LOG(WARN, "fail to alloc reader", K(ret),
          K_(bucket), K_(object), K(offset), K(buf_size), K_(meta), KPC_(storage_info));
    } else if (OB_FAIL(meta_.get_needed_fragments(offset, offset + buf_size,
                                                  fragments_need_to_read))) {
      OB_LOG(WARN, "failed to get fragments need to read",
          K(ret), K_(bucket), K_(object), K(offset), K(buf_size), K_(meta));
    } else if (fragments_need_to_read.empty()) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "the subsequent data that is anticipated to be read may not be present",
          K(ret), K(offset), K(buf_size));
    } else {
      int64_t cur_read_size = 0;
      int64_t actual_read_size = -1;
      int64_t expected_read_size = -1;
      for (int64_t i = 0; OB_SUCC(ret) && i < fragments_need_to_read.size(); i++) {
        actual_read_size = -1;
        const ObAppendableFragmentMeta &fragment_meta = fragments_need_to_read[i];
        expected_read_size =
            std::min({meta_.length_, fragment_meta.end_, offset + buf_size})
            - (offset + cur_read_size);
        if (OB_UNLIKELY(!fragment_meta.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          OB_LOG(WARN, "invalid fragment meta",
              K(ret), K(fragment_meta), K(cur_read_size));
        } else if (OB_FAIL(construct_fragment_full_name(uri_,
                                                        fragment_meta.start_, fragment_meta.end_,
                                                        uri, sizeof(uri)))) {
          OB_LOG(WARN, "failed to construct fragment name", K(ret),
              K(fragment_meta), K(cur_read_size));
        } else if (OB_FAIL(tmp_reader->open(uri, storage_info_, false/*head_meta*/))) {
          OB_LOG(WARN, "fail to open reader for fragment",
             K(ret), K(uri), K(fragment_meta), K(cur_read_size), K(offset));
        } else if (OB_FAIL(tmp_reader->pread(buf + cur_read_size,
                                             expected_read_size,
                                             (offset + cur_read_size) - fragment_meta.start_,
                                             actual_read_size))) {
          OB_LOG(WARN, "failed to read fragment content", K(ret),
              K(uri), K(fragment_meta), K(cur_read_size), K(offset), K(expected_read_size));
        } else if (OB_FAIL(tmp_reader->close())) {
          OB_LOG(WARN, "fail to close reader", K(ret), K(uri), K(fragment_meta), K(cur_read_size));
        } else if (actual_read_size != expected_read_size) {
          ret = OB_IO_ERROR;
          OB_LOG(WARN, "failed to read expected size from fragment", K(ret),
              K(uri), K(fragment_meta),
              K(cur_read_size), K(offset), K(actual_read_size), K(expected_read_size));
        } else {
          cur_read_size += actual_read_size;
        }
        EVENT_INC(ObStatEventIds::BACKUP_IO_READ_COUNT);
      }

      if (OB_SUCC(ret)) {
        read_size = cur_read_size;
      }
    }
  } else {
    ret = OB_ERR_SYS;
    OB_LOG(ERROR, "unkown object type", K(ret), K_(meta));
  }

  EVENT_ADD(ObStatEventIds::BACKUP_IO_READ_BYTES, read_size);
  if (OB_FAIL(ret)) {
    EVENT_INC(ObStatEventIds::BACKUP_IO_READ_FAIL_COUNT);
  }
  EVENT_ADD(ObStatEventIds::BACKUP_IO_READ_DELAY, ObTimeUtility::current_time() - start_ts);
  return ret;
}

int ObStorageAdaptiveReader::close()
{
  int ret = OB_SUCCESS;
  OBJECT_STORAGE_GUARD(storage_info_, uri_, IO_HANDLED_SIZE_ZERO);

  if (OB_ISNULL(reader_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not opened", K(ret));
  } else if (OB_FAIL(reader_->close())) {
    STORAGE_LOG(WARN, "failed to close reader", K(ret));
  }
  reader_  = NULL;
  start_ts_ = 0;
  uri_[0] = '\0';
  allocator_.clear();
  meta_.reset();
  storage_info_ = NULL;
  return ret;
}

ObStorageWriter::ObStorageWriter()
  : writer_(NULL),
    file_writer_(),
    oss_writer_(),
    cos_writer_(),
    s3_writer_(),
    start_ts_(0),
    storage_info_(nullptr)
{
    uri_[0] = '\0';
}

ObStorageWriter::~ObStorageWriter()
{
  if (NULL != writer_) {
    STORAGE_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "writer not close");
  }
}

int ObStorageWriter::open(const common::ObString &uri, common::ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObStorageType type = OB_STORAGE_MAX_TYPE;
  start_ts_ = ObTimeUtility::current_time();
  OBJECT_STORAGE_GUARD(storage_info, uri, IO_HANDLED_SIZE_ZERO);

#ifdef ERRSIM
  ret = OB_E(EventTable::EN_BACKUP_IO_WRITE_OPEN) OB_SUCCESS;
#endif
  if (OB_FAIL(ret)) {
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer backup io is prohibited", K(ret), K(uri));
  } else if (NULL != writer_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot open twice", K(ret), K(uri));
  } else if (OB_ISNULL(storage_info) || OB_UNLIKELY(uri.empty() || !storage_info->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(uri), KPC(storage_info));
  } else if (FALSE_IT(type = storage_info->get_type())) {
  } else if (OB_UNLIKELY(!is_storage_type_match(uri, type))) {
    ret = OB_INVALID_BACKUP_DEST;
    STORAGE_LOG(WARN, "uri prefix does not match the expected device type",
        K(ret), K(uri), KPC(storage_info), K(type));
  } else if (OB_FAIL(databuff_printf(uri_, sizeof(uri_), "%.*s", uri.length(), uri.ptr()))) {
    STORAGE_LOG(WARN, "failed to fill uri", K(ret), K(uri));
  } else if (FALSE_IT(storage_info_ = storage_info)) {
  } else if (OB_STORAGE_OSS == type) {
    writer_ = &oss_writer_;
  } else if (OB_STORAGE_COS == type) {
    writer_ = &cos_writer_;
  } else if (OB_STORAGE_S3 == type) {
    writer_ = &s3_writer_;
  } else if (OB_STORAGE_FILE == type) {
    writer_ = &file_writer_;
  } else {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "unkown storage type", K(ret), K(uri));
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(writer_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "writer_ is null", K(ret), K(uri));
    } else if (OB_FAIL(writer_->open(uri, storage_info))) {
      STORAGE_LOG(WARN, "failed to open writer", K(ret), K(uri));
    } else {
      storage_info_ = storage_info;
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_SUCCESS != (tmp_ret = close())) {
      STORAGE_LOG(WARN, "failed close write file", K(ret), K(tmp_ret), K(uri));
    }
  }

  return ret;
}

int ObStorageWriter::write(const char *buf,const int64_t size)
{
  int ret = OB_SUCCESS;
  OBJECT_STORAGE_GUARD(storage_info_, uri_, size);

#ifdef ERRSIM
  ret = OB_E(EventTable::EN_BACKUP_IO_WRITE_WRITE) OB_SUCCESS;
#endif

  const int64_t start_ts = ObTimeUtility::current_time();
  if (OB_FAIL(ret)) {
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer backup io is prohibited", K(ret), K(size));
  } else if (OB_ISNULL(writer_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not opened", K(ret));
  } else if(OB_FAIL(writer_->write(buf, size))) {
    EVENT_INC(ObStatEventIds::BACKUP_IO_WRITE_FAIL_COUNT);
    STORAGE_LOG(WARN, "failed to write", K(ret));
  } else {
    EVENT_ADD(ObStatEventIds::BACKUP_IO_WRITE_BYTES, size);
  }

  EVENT_INC(ObStatEventIds::BACKUP_IO_WRITE_COUNT);
  EVENT_ADD(ObStatEventIds::BACKUP_IO_WRITE_DELAY, ObTimeUtility::current_time() - start_ts);

  return ret;
}

int ObStorageWriter::close()
{
  int ret = OB_SUCCESS;
  OBJECT_STORAGE_GUARD(storage_info_, uri_, IO_HANDLED_SIZE_ZERO);
  if (OB_ISNULL(writer_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not opened", K(ret));
  } else if (OB_FAIL(writer_->close())) {
    STORAGE_LOG(WARN, "failed to close writer", K(ret));
  }
  writer_  = NULL;
  start_ts_ = 0;
  uri_[0] = '\0';
  storage_info_ = nullptr;
  return ret;
}

ObStorageAppender::ObStorageAppender()
  : appender_(NULL),
    file_appender_(),
    oss_appender_(),
    cos_appender_(),
    s3_appender_(),
    start_ts_(0),
    is_opened_(false),
    storage_info_(),
    allocator_(APPENDABLE_OBJECT_ALLOCATOR),
    type_(OB_STORAGE_MAX_TYPE)
{
    uri_[0] = '\0';
}

ObStorageAppender::ObStorageAppender(StorageOpenMode mode)
  : appender_(NULL),
    file_appender_(mode),
    oss_appender_(),
    cos_appender_(),
    s3_appender_(),
    start_ts_(0),
    is_opened_(false),
    storage_info_(nullptr),
    allocator_(APPENDABLE_OBJECT_ALLOCATOR),
    type_(OB_STORAGE_MAX_TYPE)
{
    uri_[0] = '\0';
}

ObStorageAppender::~ObStorageAppender()
{
  if (is_opened_ && NULL != appender_) {
    STORAGE_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "appender not close");
  }
}

int ObStorageAppender::open(
    const common::ObString &uri, 
    common::ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  start_ts_ = ObTimeUtility::current_time();
  OBJECT_STORAGE_GUARD(storage_info, uri, IO_HANDLED_SIZE_ZERO);

#ifdef ERRSIM
  ret = OB_E(EventTable::EN_BACKUP_IO_APPENDER_OPEN) OB_SUCCESS;
#endif
  if (OB_FAIL(ret)) {
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer backup io is prohibited", K(ret), K(uri));
  } else if (NULL != appender_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot open twice", K(ret), K(uri));
  } else if (OB_ISNULL(storage_info) || OB_UNLIKELY(uri.empty() || !storage_info->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(uri), KPC(storage_info));
  } else if (FALSE_IT(type_ = storage_info->get_type())) {
  } else if (OB_UNLIKELY(!is_storage_type_match(uri, type_))) {
    ret = OB_INVALID_BACKUP_DEST;
    STORAGE_LOG(WARN, "uri prefix does not match the expected device type",
        K(ret), K(uri), KPC(storage_info), K_(type));
  } else if (OB_FAIL(databuff_printf(uri_, sizeof(uri_), "%.*s", uri.length(), uri.ptr()))) {
    STORAGE_LOG(WARN, "failed to fill uri", K(ret), K(uri));
  } else if (FALSE_IT(storage_info_ = storage_info)) {
  } else if (OB_STORAGE_OSS == type_ || OB_STORAGE_COS == type_ || OB_STORAGE_S3 == type_) {
    if (OB_STORAGE_OSS == type_) {
      appender_ = &oss_appender_;
    } else if (OB_STORAGE_COS == type_) {
      appender_ = &cos_appender_;
    } else if (OB_STORAGE_S3 == type_) {
      appender_ = &s3_appender_;
    }
  } else if (OB_STORAGE_FILE == type_) {
    appender_ = &file_appender_;
  } else {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "unkown storage type", K(ret), K(uri));
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(appender_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "appender_ is null", K(ret), K(uri));
    } else if (OB_FAIL(appender_->open(uri, storage_info))) {
      STORAGE_LOG(WARN, "failed to open writer", K(ret), K(uri));
    } else {
      is_opened_ = true;
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_SUCCESS != (tmp_ret = close())) {
      STORAGE_LOG(WARN, "failed close write file", K(ret), K(tmp_ret), K(uri));
    }
  }

  return ret;
}

int ObStorageAppender::repeatable_pwrite_(const char *buf, const int64_t size, const int64_t offset)
{
  int ret = OB_SUCCESS;
  OBJECT_STORAGE_GUARD(storage_info_, uri_, size);
  int tmp_ret = OB_SUCCESS;
  int64_t read_size = 0;
  int64_t read_buf_size = 0;
  int64_t actual_write_offset = 0;
  char *read_buffer = nullptr;
  ObStorageReader *reader = nullptr;
  ObArenaAllocator allocator;

  if (OB_ISNULL(appender_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not opened", K(ret));
  } else if (OB_ISNULL(reader = static_cast<ObStorageReader *>(allocator.alloc(sizeof(ObStorageReader))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "fail to alloc buf for reader", K(ret));
  } else if(FALSE_IT(new (reader) ObStorageReader())) {
  } else if (OB_FAIL(reader->open(uri_, storage_info_))) {
    STORAGE_LOG(WARN, "failed to open reader", K(ret));
  } else if (reader->get_length() <= offset) {
    // This situation also has concurrency issues.
    // The length read by the reader may be old, so offset not match needs to be returned for retry.
    ret = OB_OBJECT_STORAGE_PWRITE_OFFSET_NOT_MATCH;
    STORAGE_LOG(WARN, "offset is invalid", K(offset), "length", reader->get_length(), K(ret));
  } else if (OB_FALSE_IT(actual_write_offset = reader->get_length() - offset)) {
  } else if (OB_FALSE_IT(read_buf_size = std::min(actual_write_offset, size))) {
  } else if (OB_ISNULL(read_buffer = static_cast<char *>(allocator.alloc(read_buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "failed to allocate memory", K(ret), K(size));
  } else if (OB_FAIL(reader->pread(read_buffer, read_buf_size, offset, read_size))) {
    STORAGE_LOG(WARN, "failed to pread", K(ret));
  } else if (0 != MEMCMP(buf, read_buffer, read_buf_size)) {
    ret = OB_OBJECT_STORAGE_PWRITE_CONTENT_NOT_MATCH;
    STORAGE_LOG(WARN, "data inconsistent", K(ret));
  } else if (offset + size > reader->get_length()) {
    if (OB_FAIL(appender_->pwrite(buf + actual_write_offset, size - actual_write_offset, reader->get_length()))) {
      if (OB_OBJECT_STORAGE_PWRITE_OFFSET_NOT_MATCH == ret) {
        ret = OB_IO_ERROR;
        STORAGE_LOG(WARN, "There may be concurrency problems that require the caller to retry", K(ret));
      }
    }
  }

  if (OB_SUCCESS != (tmp_ret = reader->close())) {
    STORAGE_LOG(WARN, "failed to close reader", K(tmp_ret));
  }
  if (OB_NOT_NULL(reader)) {
    reader->~ObStorageReader();
    reader = nullptr;
  }
  return ret;
}

int ObStorageAppender::pwrite(const char *buf, const int64_t size, const int64_t offset)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  OBJECT_STORAGE_GUARD(storage_info_, uri_, size);

#ifdef ERRSIM
  ret = OB_E(EventTable::EN_BACKUP_IO_APPENDER_WRITE) OB_SUCCESS;
#endif
  if (OB_FAIL(ret)) {
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer backup io is prohibited", K(ret), K(offset), K(size));
  } else if (OB_ISNULL(appender_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not opened", K(ret));
  } else if (OB_FAIL(appender_->pwrite(buf, size, offset))) {
    STORAGE_LOG(WARN, "failed to write", K(ret));
  }

  // no need to adjust the function repeatable_pwrite_
  // because S3 will not return OB_OBJECT_STORAGE_PWRITE_OFFSET_NOT_MATCH
  if (OB_OBJECT_STORAGE_PWRITE_OFFSET_NOT_MATCH == ret && appender_ != &s3_appender_) {
    if (OB_FAIL(repeatable_pwrite_(buf, size, offset))) {
      STORAGE_LOG(WARN, "failed to repeatable_pwrite", K(ret));
    } else {
      STORAGE_LOG(DEBUG, "repeatable pwrite success", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    EVENT_INC(ObStatEventIds::BACKUP_IO_WRITE_FAIL_COUNT);
  } else {
    EVENT_ADD(ObStatEventIds::BACKUP_IO_WRITE_BYTES, size);
  }
  EVENT_INC(ObStatEventIds::BACKUP_IO_WRITE_COUNT);
  EVENT_ADD(ObStatEventIds::BACKUP_IO_WRITE_DELAY, ObTimeUtility::current_time() - start_ts);

  return ret;
}

int64_t ObStorageAppender::get_length()
{
  int64_t ret_int = -1;
  const int64_t start_ts = ObTimeUtility::current_time();
  int ret = OB_SUCCESS;
  OBJECT_STORAGE_GUARD(storage_info_, uri_, IO_HANDLED_SIZE_ZERO);

  if (OB_ISNULL(appender_)) {
    STORAGE_LOG_RET(WARN, common::OB_ERR_UNEXPECTED, "appender not opened");
  } else if (OB_STORAGE_S3 != type_) {
    ret_int = appender_->get_length();
  } else {
    int ret = OB_SUCCESS;
    ObStorageUtil util;
    ObStorageObjectMeta meta;
    ObExternalIOCounterGuard io_guard;
    char uri[OB_MAX_URI_LENGTH] = { 0 };
    if (OB_FAIL(util.open(storage_info_))) {
      OB_LOG(WARN, "failed to open util", K(ret));
    } else if (OB_FAIL(util.list_appendable_file_fragments(uri_, meta))) {
      OB_LOG(WARN, "failed to list appendable object fragments", K(ret), K_(uri));
    } else {
      ret_int = meta.length_;
    }
    util.close();
  }

  return ret_int;
}

int ObStorageAppender::close()
{
  int ret = OB_SUCCESS;
  OBJECT_STORAGE_GUARD(storage_info_, uri_, IO_HANDLED_SIZE_ZERO);

  if (OB_ISNULL(appender_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not opened", K(ret));
  } else if (OB_FAIL(appender_->close())) {
    STORAGE_LOG(WARN, "failed to close writer", K(ret));
  }
  appender_  = NULL;
  start_ts_ = 0;
  uri_[0] = '\0';
  is_opened_ = false;
  storage_info_ = nullptr;
  return ret;
}

int ObStorageAppender::seal_for_adaptive()
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  int64_t handled_size = 0;
  OBJECT_STORAGE_GUARD(storage_info_, uri_, IO_HANDLED_SIZE_ZERO);

  if (OB_ISNULL(appender_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not opened", K(ret));
  } else if (OB_STORAGE_S3 != type_) {
  } else {
    char *buf = NULL;
    char seal_meta_uri[OB_MAX_URI_LENGTH] = { 0 };
    ObStorageUtil util;
    ListAppendableObjectFragmentOp op;
    ObStorageObjectMeta appendable_obj_meta;
    int64_t pos = 0;              // for serializing appendable_obj_meta
    int64_t serialize_size = -1;  // for serializing appendable_obj_meta
    ObExternalIOCounterGuard io_guard;

    if (OB_FAIL(util.open(storage_info_))) {
      OB_LOG(WARN, "failed to open util", K(ret), K_(uri), K_(storage_info));
    } else if (OB_FAIL(util.list_appendable_file_fragments(uri_, appendable_obj_meta))) {
      OB_LOG(WARN, "failed to list appendable object fragments", K(ret), K_(uri));
    } else if (FALSE_IT(serialize_size = appendable_obj_meta.get_serialize_size() + 1)) {
    } else if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(serialize_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "failed to alloc memory for appendable object seal meta buf",
          K(ret), K(serialize_size), K_(uri), K(appendable_obj_meta));
    } else if (OB_FAIL(appendable_obj_meta.serialize(buf, serialize_size, pos))) {
      OB_LOG(WARN, "failed to serialize s3 appendable object meta",
          K(ret), K(serialize_size), K_(uri), K(appendable_obj_meta));
    } else if (OB_FAIL(construct_fragment_full_name(uri_, OB_S3_APPENDABLE_SEAL_META,
                                                    seal_meta_uri, sizeof(seal_meta_uri)))) {
      OB_LOG(WARN, "failed to construct s3 appendable object name for writing seal meta file",
          K(ret), K_(uri), K(appendable_obj_meta));
    } else if (OB_FAIL(util.write_single_file(seal_meta_uri, buf, pos))) {
      OB_LOG(WARN, "fail to write seal meta file", K(ret), K(seal_meta_uri), K(appendable_obj_meta));
    } else {
      // The seal operation includes listing fragments and writing this data to a meta file.
      // Here, 'serialize_size' is used as an approximation of the data throughput of the list operation.
      handled_size = serialize_size + pos;
      OB_LOG(DEBUG, "succeed to write seal meta file",
          K_(uri),  K(seal_meta_uri), KP(buf), K(pos), K(appendable_obj_meta));
    }
    util.close();
  }

  return ret;
}

ObStorageMultiPartWriter::ObStorageMultiPartWriter()
    : multipart_writer_(NULL),
      file_multipart_writer_(),
      cos_multipart_writer_(),
      oss_multipart_writer_(),
      s3_multipart_writer_(),
      start_ts_(0),
      is_opened_(false),
      storage_info_(nullptr),
      cur_max_offset_(0)
{
  uri_[0] = '\0';
}

ObStorageMultiPartWriter::~ObStorageMultiPartWriter()
{
  if (is_opened_ && NULL != multipart_writer_) {
    STORAGE_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "multipart_writer not close");
  }
}

int ObStorageMultiPartWriter::open(
    const common::ObString &uri,
    common::ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObStorageType type = OB_STORAGE_MAX_TYPE;
  start_ts_ = ObTimeUtility::current_time();
  OBJECT_STORAGE_GUARD(storage_info, uri, IO_HANDLED_SIZE_ZERO);

  if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer backup io is prohibited", K(ret), K(uri));
  } else if (NULL != multipart_writer_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "multipart writer cannot open twice", K(ret), K(uri));
  } else if (OB_ISNULL(storage_info) || OB_UNLIKELY(uri.empty() || !storage_info->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(uri), KPC(storage_info));
  } else if (FALSE_IT(type = storage_info->get_type())) {
  } else if (OB_UNLIKELY(!is_storage_type_match(uri, type))) {
    ret = OB_INVALID_BACKUP_DEST;
    STORAGE_LOG(WARN, "uri prefix does not match the expected device type",
        K(ret), K(uri), KPC(storage_info), K(type));
  } else if (OB_FAIL(databuff_printf(uri_, sizeof(uri_), "%.*s", uri.length(), uri.ptr()))) {
    STORAGE_LOG(WARN, "failed to fill uri", K(ret), K(uri));
  } else if (FALSE_IT(storage_info_ = storage_info)) {
  } else if (OB_STORAGE_OSS == type || OB_STORAGE_COS == type || OB_STORAGE_S3 == type) {
    if (OB_STORAGE_OSS == type) {
      multipart_writer_ = &oss_multipart_writer_;
    } else if (OB_STORAGE_COS == type) {
      multipart_writer_ = &cos_multipart_writer_;
    } else if (OB_STORAGE_S3 == type) {
      multipart_writer_ = &s3_multipart_writer_;
    }
  } else if (OB_STORAGE_FILE == type) {
    multipart_writer_ = &file_multipart_writer_;
  } else {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "unkown storage type", K(ret), K(uri));
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(multipart_writer_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "multipart_writer is null", K(ret), K(uri));
    } else if (OB_FAIL(multipart_writer_->open(uri, storage_info))) {
      STORAGE_LOG(WARN, "failed to open multipart writer", K(ret), K(uri), K(storage_info));
    } else {
      is_opened_ = true;
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_SUCCESS != (tmp_ret = close())) {
      STORAGE_LOG(WARN, "failed close multipart writer", K(ret), K(tmp_ret), K(uri));
    }
  }

  return ret;
}

int ObStorageMultiPartWriter::write(const char *buf, const int64_t size)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  OBJECT_STORAGE_GUARD(storage_info_, uri_, size);

  if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer backup io is prohibited", K(ret), K(size));
  } else if (OB_ISNULL(multipart_writer_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "multipart writer not opened", K(ret));
  } else if(OB_FAIL(multipart_writer_->write(buf, size))) {
    EVENT_INC(ObStatEventIds::BACKUP_IO_WRITE_FAIL_COUNT);
    STORAGE_LOG(WARN, "failed to write", K(ret));
  } else {
    EVENT_ADD(ObStatEventIds::BACKUP_IO_WRITE_BYTES, size);
  }
  if (OB_FAIL(ret)) {
    EVENT_INC(ObStatEventIds::BACKUP_IO_WRITE_FAIL_COUNT);
  }
  EVENT_INC(ObStatEventIds::BACKUP_IO_WRITE_COUNT);
  EVENT_ADD(ObStatEventIds::BACKUP_IO_WRITE_DELAY, ObTimeUtility::current_time() - start_ts);

  return ret;
}

int ObStorageMultiPartWriter::pwrite(const char *buf, const int64_t size, const int64_t offset)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  OBJECT_STORAGE_GUARD(storage_info_, uri_, size);
  if (OB_FAIL(ret)) {
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer backup io is prohibited", K(ret), K(offset), K(size));
  } else if (OB_ISNULL(multipart_writer_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "multipart writer not opened", K(ret));
  } else if (OB_UNLIKELY(offset != cur_max_offset_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "call pwrite in an unexpected order",
        K(ret), K(offset), K(size), K_(cur_max_offset));
  } else if (OB_FAIL(multipart_writer_->pwrite(buf, size, offset))) {
    EVENT_INC(ObStatEventIds::BACKUP_IO_WRITE_FAIL_COUNT);
    STORAGE_LOG(WARN, "failed to write", K(ret));
  } else {
    EVENT_ADD(ObStatEventIds::BACKUP_IO_WRITE_BYTES, size);
    cur_max_offset_ = offset + size;
  }

  EVENT_INC(ObStatEventIds::BACKUP_IO_WRITE_COUNT);
  EVENT_ADD(ObStatEventIds::BACKUP_IO_WRITE_DELAY, ObTimeUtility::current_time() - start_ts);
  return ret;
}

int64_t ObStorageMultiPartWriter::get_length()
{
  int64_t ret_int = -1;
  const int64_t start_ts = ObTimeUtility::current_time();
  int ret = OB_SUCCESS;
  OBJECT_STORAGE_GUARD(storage_info_, uri_, IO_HANDLED_SIZE_ZERO);
  if (OB_ISNULL(multipart_writer_)) {
    STORAGE_LOG_RET(WARN, common::OB_ERR_UNEXPECTED, "multipart_writer_ not opened");
  } else {
    ret_int = multipart_writer_->get_length();
  }

  return ret_int;
}

int ObStorageMultiPartWriter::complete()
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  OBJECT_STORAGE_GUARD(storage_info_, uri_, IO_HANDLED_SIZE_ZERO);
  if (OB_FAIL(ret)) {
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer backup io is prohibited", K(ret));
  } else if (OB_ISNULL(multipart_writer_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "multipart writer not opened", K(ret));
  } else if (OB_FAIL(multipart_writer_->complete())) {
    STORAGE_LOG(WARN, "failed to complete", K(ret));
  }

  // for complete
  if (OB_FAIL(ret)) {
    EVENT_INC(ObStatEventIds::BACKUP_IO_WRITE_FAIL_COUNT);
  }
  EVENT_INC(ObStatEventIds::BACKUP_IO_WRITE_COUNT);
  EVENT_ADD(ObStatEventIds::BACKUP_IO_WRITE_DELAY, ObTimeUtility::current_time() - start_ts);
  return ret;
}

int ObStorageMultiPartWriter::abort()
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  OBJECT_STORAGE_GUARD(storage_info_, uri_, IO_HANDLED_SIZE_ZERO);
  if (OB_FAIL(ret)) {
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer backup io is prohibited");
  } else if (OB_ISNULL(multipart_writer_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "multipart writer not opened", K(ret));
  } else if (OB_FAIL(multipart_writer_->abort())) {
    STORAGE_LOG(WARN, "failed to abort", K(ret));
  }

  // for abort
  if (OB_FAIL(ret)) {
    EVENT_INC(ObStatEventIds::BACKUP_IO_WRITE_FAIL_COUNT);
  }
  EVENT_INC(ObStatEventIds::BACKUP_IO_WRITE_COUNT);
  EVENT_ADD(ObStatEventIds::BACKUP_IO_WRITE_DELAY, ObTimeUtility::current_time() - start_ts);
  return ret;
}

int ObStorageMultiPartWriter::close()
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  OBJECT_STORAGE_GUARD(storage_info_, uri_, IO_HANDLED_SIZE_ZERO);
  if (OB_ISNULL(multipart_writer_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not opened", K(ret));
  } else if (OB_FAIL(multipart_writer_->close())) {
    STORAGE_LOG(WARN, "failed to close multipart writer", K(ret));
  }
  multipart_writer_  = NULL;
  start_ts_ = 0;
  uri_[0] = '\0';
  is_opened_ = false;
  cur_max_offset_ = 0;
  storage_info_ = nullptr;
  return ret;
}

/**
 * ------------------------------Parallel MultipartWriters------------------------------
 */
ObStorageParallelMultiPartWriterBase::ObStorageParallelMultiPartWriterBase()
    : multipart_writer_(nullptr),
      file_multipart_writer_(),
      cos_multipart_writer_(),
      oss_multipart_writer_(),
      s3_multipart_writer_(),
      start_ts_(0),
      is_opened_(false),
      storage_info_(nullptr)
{
  uri_[0] = '\0';
}

ObStorageParallelMultiPartWriterBase::~ObStorageParallelMultiPartWriterBase()
{
}

void ObStorageParallelMultiPartWriterBase::reset()
{
  is_opened_ = false;
  multipart_writer_ = nullptr;
  start_ts_ = 0;
  uri_[0] = '\0';
  storage_info_ = nullptr;
}

int ObStorageParallelMultiPartWriterBase::open(
    const ObString &uri,
    ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  OBJECT_STORAGE_GUARD(storage_info, uri, IO_HANDLED_SIZE_ZERO);
  ObStorageType type = OB_STORAGE_MAX_TYPE;
  start_ts_ = ObTimeUtility::current_time();
  if (OB_NOT_NULL(multipart_writer_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "multipart writer cannot open twice", K(ret), K(uri));
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer object storag io is prohibited", K(ret), K(uri));
  } else if (OB_ISNULL(storage_info) || OB_UNLIKELY(uri.empty() || !storage_info->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(uri), KPC(storage_info));
  } else if (FALSE_IT(type = storage_info->get_type())) {
  } else if (OB_UNLIKELY(!is_storage_type_match(uri, type))) {
    ret = OB_INVALID_BACKUP_DEST;
    STORAGE_LOG(WARN, "uri prefix does not match the expected device type",
        K(ret), K(uri), KPC(storage_info), K(type));
  } else if (OB_FAIL(databuff_printf(uri_, sizeof(uri_), "%.*s", uri.length(), uri.ptr()))) {
    STORAGE_LOG(WARN, "failed to fill uri", K(ret), K(uri));
  } else if (OB_STORAGE_OSS == type) {
    multipart_writer_ = &oss_multipart_writer_;
  } else if (OB_STORAGE_COS == type) {
    multipart_writer_ = &cos_multipart_writer_;
  } else if (OB_STORAGE_S3 == type) {
    multipart_writer_ = &s3_multipart_writer_;
  } else if (OB_STORAGE_FILE == type) {
    multipart_writer_ = &file_multipart_writer_;
  } else {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "unkown storage type", K(ret), K(uri), K(type));
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(multipart_writer_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "multipart_writer is null", K(ret), K(uri), K(type));
    } else if (OB_FAIL(multipart_writer_->open(uri, storage_info))) {
      STORAGE_LOG(WARN, "failed to open multipart writer", K(ret), K(uri), KPC(storage_info));
    } else {
      storage_info_ = storage_info;
      is_opened_ = true;
    }
  }

  return ret;
}

ObStorageDirectMultiPartWriter::ObStorageDirectMultiPartWriter()
    : ObStorageParallelMultiPartWriterBase(),
      lock_(ObLatchIds::OBJECT_DEVICE_LOCK),
      uploaded_file_length_(-1),
      cur_part_id_(0)
{
}

ObStorageDirectMultiPartWriter::~ObStorageDirectMultiPartWriter()
{
  if (is_opened_ && OB_NOT_NULL(multipart_writer_)) {
    STORAGE_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "direct multipart writer not close");
  }
  reset();
}

void ObStorageDirectMultiPartWriter::reset()
{
  uploaded_file_length_ = -1;
  cur_part_id_ = 0;
  ObStorageParallelMultiPartWriterBase::reset();
}

int ObStorageDirectMultiPartWriter::open(
    const common::ObString &uri,
    common::ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObStorageParallelMultiPartWriterBase::open(uri, storage_info))) {
    STORAGE_LOG(WARN, "fail to open in base writer", K(ret), K(uri), KPC(storage_info));
  } else {
    uploaded_file_length_ = 0;
  }

  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObStorageDirectMultiPartWriter::upload_part(
    const char *buf, const int64_t size, const int64_t part_id)
{
  int ret = OB_SUCCESS;
  OBJECT_STORAGE_GUARD(storage_info_, uri_, size);
  const int64_t start_ts = ObTimeUtility::current_time();
  if (OB_ISNULL(multipart_writer_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "multipart writer not opened", K(ret));
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer object storage io is prohibited",
        K(ret), K_(uri), K(part_id));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(size <= 0)) {
    // The validity of part_id is to be verified by each object storage individually
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(buf), K(size));
  } else if (OB_FAIL(multipart_writer_->upload_part(buf, size, part_id))) {
    STORAGE_LOG(WARN, "fail to upload part", K(ret), K_(uri), K(size), K(part_id));
  }

  if (OB_SUCC(ret)) {
    SpinWLockGuard guard(lock_);
    uploaded_file_length_ += size;
  }
  if (OB_FAIL(ret)) {
    EVENT_INC(ObStatEventIds::BACKUP_IO_WRITE_FAIL_COUNT);
  } else {
    EVENT_ADD(ObStatEventIds::BACKUP_IO_WRITE_BYTES, size);
  }
  EVENT_INC(ObStatEventIds::BACKUP_IO_WRITE_COUNT);
  EVENT_ADD(ObStatEventIds::BACKUP_IO_WRITE_DELAY, ObTimeUtility::current_time() - start_ts);
  return ret;
}

int ObStorageDirectMultiPartWriter::complete()
{
  int ret = OB_SUCCESS;
  OBJECT_STORAGE_GUARD(storage_info_, uri_, IO_HANDLED_SIZE_ZERO);
  const int64_t start_ts = ObTimeUtility::current_time();
  if (OB_ISNULL(multipart_writer_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "multipart writer not opened", K(ret));
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer object storage io is prohibited", K(ret), K_(uri));
  } else if (OB_FAIL(multipart_writer_->complete())) {
    STORAGE_LOG(WARN, "fail to complete multipart upload", K(ret), K_(uri));
  }

  return ret;
}

int ObStorageDirectMultiPartWriter::abort()
{
  int ret = OB_SUCCESS;
  OBJECT_STORAGE_GUARD(storage_info_, uri_, IO_HANDLED_SIZE_ZERO);
  const int64_t start_ts = ObTimeUtility::current_time();
  if (OB_ISNULL(multipart_writer_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "multipart writer not opened", K(ret));
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer object storage io is prohibited", K(ret), K_(uri));
  } else if (OB_FAIL(multipart_writer_->abort())) {
    STORAGE_LOG(WARN, "fail to abort multipart upload", K(ret), K_(uri));
  }

  // for abort
  if (OB_FAIL(ret)) {
    EVENT_INC(ObStatEventIds::BACKUP_IO_WRITE_FAIL_COUNT);
  }
  EVENT_INC(ObStatEventIds::BACKUP_IO_WRITE_COUNT);
  EVENT_ADD(ObStatEventIds::BACKUP_IO_WRITE_DELAY, ObTimeUtility::current_time() - start_ts);
  return ret;
}

int ObStorageDirectMultiPartWriter::close()
{
  int ret = OB_SUCCESS;
  OBJECT_STORAGE_GUARD(storage_info_, uri_, IO_HANDLED_SIZE_ZERO);
  SpinWLockGuard guard(lock_);
  const int64_t start_ts = ObTimeUtility::current_time();
  if (OB_ISNULL(multipart_writer_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "multipart writer not opened", K(ret));
  } else {
    if (OB_FAIL(multipart_writer_->close())) {
      STORAGE_LOG(WARN, "failed to close multipart writer", K(ret));
    }
  }

  reset();
  return ret;
}

int64_t ObStorageDirectMultiPartWriter::get_length() const
{
  SpinRLockGuard guard(lock_);
  return uploaded_file_length_;
}

int ObStorageDirectMultiPartWriter::buf_append_part(
    const char *buf, const int64_t size, const uint64_t tenant_id, bool &is_full)
{
  UNUSED(buf);
  UNUSED(size);
  UNUSED(tenant_id);

  int ret = OB_SUCCESS;
  is_full = false;
  if (OB_ISNULL(multipart_writer_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "multipart writer not opened", K(ret));
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer object storage io is prohibited", K(ret), K_(uri));
  } else {
    is_full = true;
  }
  return ret;
}

int ObStorageDirectMultiPartWriter::get_part_id(bool &is_exist, int64_t &part_id)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  part_id = -1;
  if (OB_ISNULL(multipart_writer_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "multipart writer not opened", K(ret));
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer object storage io is prohibited", K(ret), K_(uri));
  } else {
    SpinWLockGuard guard(lock_);
    is_exist = true;
    cur_part_id_++;
    part_id = cur_part_id_;
  }
  return ret;
}

int ObStorageDirectMultiPartWriter::get_part_size(const int64_t part_id, int64_t &part_size) const
{
  int ret = OB_NOT_SUPPORTED;
  STORAGE_LOG(WARN, "direct multipart writer do not support get part size", K(ret));
  return ret;
}

ObStorageBufferedMultiPartWriter::ObStorageBufferedMultiPartWriter()
    : ObStorageDirectMultiPartWriter(),
      cur_buf_(nullptr),
      cur_buf_pos_(0),
      part_id_to_data_map_()
{
}

ObStorageBufferedMultiPartWriter::~ObStorageBufferedMultiPartWriter()
{
  if (is_opened_ && OB_NOT_NULL(multipart_writer_)) {
    STORAGE_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "buffered multipart_writer not close");
  }
  reset();
}

void ObStorageBufferedMultiPartWriter::reset()
{
  if (OB_NOT_NULL(cur_buf_)) {
    ob_free(cur_buf_);
    cur_buf_ = nullptr;
  }
  cur_buf_pos_ = 0;
  hash::ObHashMap<int64_t, PartData>::iterator iter = part_id_to_data_map_.begin();
  while (iter != part_id_to_data_map_.end()) {
    free_part_data_(iter->second);
    iter++;
  }
  part_id_to_data_map_.destroy();
  ObStorageDirectMultiPartWriter::reset();
}

int ObStorageBufferedMultiPartWriter::open(const ObString &uri, ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObStorageDirectMultiPartWriter::open(uri, storage_info))) {
    STORAGE_LOG(WARN, "fail to open base multipart writer", K(ret), K(uri), KPC(storage_info));
  } else if (OB_FAIL(part_id_to_data_map_.create(7, ALLOC_TAG))) {
    STORAGE_LOG(WARN, "fail to create part_id_to_data_map_", K(ret), K(uri), KPC(storage_info));
  }

  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObStorageBufferedMultiPartWriter::upload_part(
    const char *buf, const int64_t size, const int64_t part_id)
{
  UNUSED(buf);
  UNUSED(size);

  int ret = OB_SUCCESS;
  if (OB_ISNULL(multipart_writer_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "multipart writer not opened", K(ret));
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer object storage io is prohibited", K(ret), K_(uri));
  } else {
    PartData para_data;
    {
      SpinWLockGuard guard(lock_);
      if (OB_FAIL(part_id_to_data_map_.erase_refactored(part_id, &para_data))) {
        STORAGE_LOG(WARN, "fail to get part data from map", K(ret), K(part_id));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(!para_data.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected error, part data is null or invalid",
          K(ret), K(part_id), K(para_data));
    } else if (OB_FAIL(ObStorageDirectMultiPartWriter::upload_part(para_data.data_,
                                                                     para_data.size_,
                                                                     part_id))) {
      STORAGE_LOG(WARN, "fail to upload specified part", K(ret), K(part_id), K(para_data));
    }

    free_part_data_(para_data);
  }
  return ret;
}

int ObStorageBufferedMultiPartWriter::buf_append_part(
    const char *buf, const int64_t size, const uint64_t tenant_id, bool &is_full)
{
  int ret = OB_SUCCESS;
  is_full = false;
  if (OB_ISNULL(multipart_writer_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "multipart writer not opened", K(ret));
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer object storage io is prohibited", K(ret), K_(uri));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(size <= 0 || !is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(buf), K(size), K(tenant_id));
  } else {
    SpinWLockGuard guard(lock_);
    if (OB_FAIL(append_buf_(buf, size, tenant_id))) {
      STORAGE_LOG(WARN, "fail to append data into cur buf",
          K(ret), K_(cur_buf_pos), K(size), K(tenant_id));
    } else {
      is_full = (cur_buf_pos_ >= PART_SIZE_THRESHOLD);
    }
  }
  return ret;
}

int ObStorageBufferedMultiPartWriter::append_buf_(
    const char *buf, const int64_t size, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  char *tmp_buf = nullptr;
  int64_t final_size = cur_buf_pos_ + size;
  ObMemAttr attr(tenant_id, ALLOC_TAG);

  if (OB_ISNULL(buf) || OB_UNLIKELY(size <= 0 || !is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(buf), K(size), K(tenant_id));
  } else if (OB_ISNULL(tmp_buf = static_cast<char *>(ob_malloc(final_size, attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to realloc buf",
        K(ret), K_(cur_buf_pos), K(size), K(final_size), K(tenant_id));
  } else {
    MEMCPY(tmp_buf + cur_buf_pos_, buf, size);
    if (OB_NOT_NULL(cur_buf_)) {
      MEMCPY(tmp_buf, cur_buf_, cur_buf_pos_);
      ob_free(cur_buf_);
      cur_buf_ = nullptr;
    }
    cur_buf_ = tmp_buf;
    cur_buf_pos_ = final_size;
  }
  return ret;
}

int ObStorageBufferedMultiPartWriter::save_buf_to_map_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_buf_) || OB_UNLIKELY(cur_buf_pos_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "cur buf is empty, cannot insert into map",
        K(ret), KP_(cur_buf), K_(cur_buf_pos));
  } else {
    cur_part_id_++;
    PartData part_data;
    part_data.data_ = cur_buf_;
    part_data.size_ = cur_buf_pos_;
    if (OB_FAIL(part_id_to_data_map_.set_refactored(cur_part_id_, part_data))) {
      STORAGE_LOG(WARN, "fail to insert part data into map",
          K(ret), K_(cur_part_id), K_(cur_buf_pos));
    } else {
      cur_buf_pos_ = 0;
      cur_buf_ = nullptr;
    }
  }
  return ret;
}

void ObStorageBufferedMultiPartWriter::free_part_data_(PartData &part_data)
{
  if (OB_NOT_NULL(part_data.data_)) {
    ob_free(part_data.data_);
    part_data.data_ = nullptr;
  }
}

int ObStorageBufferedMultiPartWriter::get_part_id(bool &is_exist, int64_t &part_id)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  part_id = -1;
  SpinWLockGuard guard(lock_);
  if (OB_ISNULL(multipart_writer_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "multipart writer not opened", K(ret));
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer object storage io is prohibited", K(ret), K_(uri));
  } else if (OB_NOT_NULL(cur_buf_)) {
    if (OB_FAIL(save_buf_to_map_())) {
      STORAGE_LOG(WARN, "fail to save cur buf to map", K(ret), K_(cur_buf_pos));
    } else {
      is_exist = true;
      part_id = cur_part_id_;
    }
  }
  return ret;
}

int ObStorageBufferedMultiPartWriter::get_part_size(const int64_t part_id, int64_t &part_size) const
{
  int ret = OB_SUCCESS;
  part_size = -1;
  if (OB_ISNULL(multipart_writer_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "multipart writer not opened", K(ret));
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer object storage io is prohibited", K(ret), K_(uri));
  } else if (OB_UNLIKELY(part_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "part id is invalid", K(ret), K(part_id));
  } else {
    SpinRLockGuard guard(lock_);
    PartData part_data;
    if (OB_FAIL(part_id_to_data_map_.get_refactored(part_id, part_data))) {
      if (ret == OB_HASH_NOT_EXIST) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "the specified part does not exist", K(ret), K(part_id));
      }
    } else {
      part_size = part_data.size_;
    }
  }
  return ret;
}

}
}//oceanbase
