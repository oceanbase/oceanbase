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

#include "ob_i_storage.h"
#include "lib/container/ob_se_array_iterator.h"

namespace oceanbase
{
namespace common
{

static const char SLASH = '/';
bool is_end_with_slash(const char *str)
{
  bool bret = false;
  int64_t str_len = -1;
  if (OB_NOT_NULL(str) && (str_len = strlen(str)) > 0) {
    bret = (SLASH == str[str_len - 1]);
  }
  return bret;
}

bool is_null_or_end_with_slash(const char *str)
{
  bool bret = false;
  if (OB_ISNULL(str)) {
    bret = true;
  } else {
    bret = is_end_with_slash(str);
  }
  return bret;
}

// get the length of a string safely even the it is NULL
int get_safe_str_len(const char *str)
{
  return OB_ISNULL(str) ? 0 : strlen(str);
}

int c_str_to_int(const char *str, const int64_t length, int64_t &num)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("C_STR_TO_INT");
  char *tmp_str = nullptr;
  if (OB_ISNULL(str) || OB_UNLIKELY(length <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KCSTRING(str), K(length));
  } else if (OB_ISNULL(tmp_str = static_cast<char *>(allocator.alloc(length + 1)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "fail to alloc memory", K(ret), KCSTRING(str), K(length));
  } else {
    MEMCPY(tmp_str, str, length);
    tmp_str[length] = '\0';
    if(OB_FAIL(c_str_to_int(tmp_str, num))) {
      OB_LOG(WARN, "fail to c_str_to_int", K(ret), KCSTRING(str), K(length));
    }
  }
  return ret;
}

int c_str_to_int(const char *str, int64_t &num)
{
  int ret = OB_SUCCESS;
  errno = 0;
  char *end_str = NULL;
  if (OB_ISNULL(str) || OB_UNLIKELY(0 == strlen(str))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "c_str_to_int str should not be null/empty", KP(str));
  } else {
    num = strtoll(str, &end_str, 10);
    if (errno != 0 || (NULL != end_str && *end_str != '\0')) {
      ret = OB_INVALID_DATA;
      OB_LOG(WARN, "strtoll convert string to int value fail", K(str), K(num),
          "error", strerror(errno), K(end_str));
    }
  }
  return ret;
}

int handle_listed_object(ObBaseDirEntryOperator &op,
    const char *obj_name, const int64_t obj_name_len, const int64_t obj_size)
{
  int ret = OB_SUCCESS;
  dirent entry;
  entry.d_type = DT_REG;
  if (OB_ISNULL(obj_name)
      || OB_UNLIKELY(sizeof(entry.d_name) <= obj_name_len || obj_name_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments",
        K(ret), K(obj_name), K(obj_name_len), K(sizeof(entry.d_name)));
  } else {
    if (op.need_get_file_size()) {
      if (OB_UNLIKELY(obj_size < 0)) {
        ret = OB_INVALID_ARGUMENT;
        OB_LOG(WARN, "invalid object size", K(obj_size));
      } else {
        op.set_size(obj_size);
      }
    }

    if (OB_SUCC(ret)) {
      MEMCPY(entry.d_name, obj_name, obj_name_len);
      entry.d_name[obj_name_len] = '\0';
      if (OB_FAIL(op.func(&entry))) {
        OB_LOG(WARN, "fail to exe application callback for listed object",
            K(ret), K(obj_name), K(obj_name_len), K(obj_size));
      }
    }
  }
  return ret;
}

int handle_listed_directory(ObBaseDirEntryOperator &op,
    const char *dir_name, const int64_t dir_name_len)
{
  int ret = OB_SUCCESS;
  dirent entry;
  entry.d_type = DT_DIR;
  if (OB_ISNULL(dir_name)
      || OB_UNLIKELY(sizeof(entry.d_name) <= dir_name_len || dir_name_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments",
        K(ret), K(dir_name), K(dir_name_len), K(sizeof(entry.d_name)));
  } else {
    MEMCPY(entry.d_name, dir_name, dir_name_len);
    entry.d_name[dir_name_len] = '\0';
    if (OB_FAIL(op.func(&entry))) {
      OB_LOG(WARN, "fail to exe application callback for listed directory",
          K(ret), K(dir_name), K(dir_name_len));
    }
  }
  return ret;
}

int get_storage_prefix_from_path(const common::ObString &uri, const char *&prefix)
{
  int ret = OB_SUCCESS;
  if (uri.prefix_match(OB_OSS_PREFIX)) {
    prefix = OB_OSS_PREFIX;
  } else if (uri.prefix_match(OB_COS_PREFIX)) {
    prefix = OB_COS_PREFIX;
  } else if (uri.prefix_match(OB_S3_PREFIX)) {
    prefix = OB_S3_PREFIX;
  } else if (uri.prefix_match(OB_FILE_PREFIX)) {
    prefix = OB_FILE_PREFIX;
  } else if (uri.prefix_match(OB_HDFS_PREFIX)) {
    prefix = OB_HDFS_PREFIX;
  } else {
    ret = OB_INVALID_BACKUP_DEST;
    STORAGE_LOG(ERROR, "invalid backup uri", K(ret), K(uri));
  }
  return ret;
}

int build_bucket_and_object_name(ObIAllocator &allocator,
    const ObString &uri, ObString &bucket, ObString &object)
{
  int ret = OB_SUCCESS;
  ObString::obstr_size_t bucket_start = 0;
  ObString::obstr_size_t bucket_end = 0;
  ObString::obstr_size_t object_start = 0;
  char *bucket_name_buff = nullptr;
  char *object_name_buff = nullptr;

  const char *prefix = "UNKNOWN";
  if (OB_FAIL(get_storage_prefix_from_path(uri, prefix))) {
    OB_LOG(WARN, "fail to get storage type", K(ret), K(uri));
  } else {
    bucket_start = static_cast<ObString::obstr_size_t>(strlen(prefix));
    if (OB_UNLIKELY(bucket_start >= uri.length())) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "bucket and object are empty", K(uri), K(ret), K(bucket_start));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (prefix == OB_FILE_PREFIX) {
    // for nfs, bucket is empty
    if (OB_FAIL(ob_write_string(allocator, uri.ptr() + bucket_start, object, true/*c_style*/))) {
      OB_LOG(WARN, "fail to deep copy object", K(uri), K(bucket_start), K(ret));
    }
  } else {
    for (int64_t i = bucket_start; OB_SUCC(ret) && i < uri.length() - 1; i++) {
      if ('/' == *(uri.ptr() + i) && '/' == *(uri.ptr() + i + 1)) {
        ret = OB_INVALID_ARGUMENT;
        OB_LOG(WARN, "uri has two // ", K(uri), K(ret), K(i));
        break;
      }
    }

    for (bucket_end = bucket_start; OB_SUCC(ret) && bucket_end < uri.length(); ++bucket_end) {
      if ('/' == *(uri.ptr() + bucket_end)) {
        ObString::obstr_size_t bucket_length = bucket_end - bucket_start;
        //must end with '\0'
        if (OB_UNLIKELY(bucket_length <= 0)) {
          ret = OB_INVALID_ARGUMENT;
          OB_LOG(WARN, "bucket name is empty", K(ret), K(uri), K(bucket_start), K(bucket_length));
        } else if (OB_ISNULL(bucket_name_buff =
            static_cast<char *>(allocator.alloc(bucket_length + 1)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          OB_LOG(WARN, "failed to alloc bucket name buff", K(ret), K(uri), K(bucket_length));
        } else if (OB_FAIL(databuff_printf(bucket_name_buff, bucket_length + 1,
                                           "%.*s", bucket_length, uri.ptr() + bucket_start))) {
          OB_LOG(WARN, "fail to deep copy bucket", K(uri), K(bucket_start), K(bucket_length), K(ret));
        } else {
          bucket.assign_ptr(bucket_name_buff, bucket_length + 1);// must include '\0'
        }
        break;
      }
    }

    // parse the object name
    if (OB_SUCC(ret)) {
      object_start = bucket_end + 1;
      ObString::obstr_size_t object_length = uri.length() - object_start;
      // must end with '\0'
      // It is impossible to find object_length < 0 here.
      if (OB_UNLIKELY(object_length < 0)) {
        ret = OB_INVALID_ARGUMENT;
        OB_LOG(WARN, "uri is invalid", K(ret), K(uri), K(object_start), K(object_length));
      } else if (object_length == 0) {
        // When object_length == 0, it means no object is given in the uri.
      } else if (object_length > 0) {
        // And we only allocate memory to object when object_length > 0
        if (OB_FAIL(ob_write_string(allocator, uri.ptr() + object_start, object, true/*c_style*/))) {
          OB_LOG(WARN, "fail to deep copy object", K(uri), K(object_start), K(object_length), K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    OB_LOG(DEBUG, "get bucket object name", K(uri), K(bucket), K(object));
  }
  return ret;
}

int construct_fragment_full_name(const ObString &logical_appendable_object_name,
    const char *fragment_name, char *name_buf, const int64_t name_buf_len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const char *suffix = NULL;
  if (OB_ISNULL(fragment_name) || OB_ISNULL(name_buf) || OB_UNLIKELY(strlen(fragment_name) <= 0)
      || OB_UNLIKELY(logical_appendable_object_name.empty() || name_buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret),
        K(logical_appendable_object_name), KP(fragment_name), KP(name_buf), K(name_buf_len));
  } else if (OB_FAIL(databuff_printf(name_buf, name_buf_len, pos, "%s/%s%s",
                                     logical_appendable_object_name.ptr(),
                                     OB_S3_APPENDABLE_FRAGMENT_PREFIX, fragment_name))) {
    OB_LOG(WARN, "failed to construct formatted mock append object fragment name",
        K(ret), K(logical_appendable_object_name), K(fragment_name));
  } else {
    // Fixed the logic to correctly identify the object name's suffix.
    // Now it only considers the string after the last '.' following the final '/' as the suffix,
    // ignoring any '.' in the path.
    // For example: if the object name is "a/b.c/d", the original logic treats "c/d" as the suffix.
    const char *object_name = logical_appendable_object_name.reverse_find(SLASH);
    if (OB_NOT_NULL(object_name)) {
      suffix = ObString(object_name).reverse_find('.');
    } else {
      suffix = logical_appendable_object_name.reverse_find('.');
    }
    if (OB_NOT_NULL(suffix)) {
      if (OB_UNLIKELY(strlen(suffix) <= 1 || strlen(suffix) >= MAX_APPENDABLE_FRAGMENT_SUFFIX_LENGTH)) {
        ret = OB_INVALID_ARGUMENT;
        OB_LOG(WARN, "object name has invalid suffix",
            K(ret), K(logical_appendable_object_name), K(suffix));
      } else if (OB_FAIL(databuff_printf(name_buf, name_buf_len, pos, "%s", suffix))) {
        OB_LOG(WARN, "failed to set formatted mock append object fragment suffix",
            K(ret), K(logical_appendable_object_name), K(fragment_name), K(suffix));
      }
    }
  }
  return ret;
}

int construct_fragment_full_name(const ObString &logical_appendable_object_name,
    const int64_t start, const int64_t end, char *name_buf, const int64_t name_buf_len)
{
  int ret = OB_SUCCESS;
  char fragment_name[MAX_APPENDABLE_FRAGMENT_LENGTH] = { 0 };
  if (OB_UNLIKELY(start < 0 || end <= start)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), K(start), K(end));
  } else if (OB_FAIL(databuff_printf(fragment_name, sizeof(fragment_name), "%ld-%ld", start, end))) {
    OB_LOG(WARN, "failed to construct mock append object fragment name", K(ret), K(start), K(end));
  } else if (OB_FAIL(construct_fragment_full_name(logical_appendable_object_name,
                                                  fragment_name, name_buf, name_buf_len))) {
    OB_LOG(WARN, "failed to construct mock append object fragment name",
        K(ret), K(start), K(end), K(fragment_name), K(logical_appendable_object_name));
  }
  return ret;
}

int check_files_map_validity(const hash::ObHashMap<ObString, int64_t> &files_to_delete)
{
  int ret = OB_SUCCESS;
  const int64_t n_files_to_delete = files_to_delete.size();
  if (OB_UNLIKELY(0 >= n_files_to_delete)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "files to delete is empty", K(ret), K(n_files_to_delete));
  } else if (OB_UNLIKELY(OB_STORAGE_DEL_MAX_NUM < n_files_to_delete)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "too many files to delete", K(ret), K(n_files_to_delete));
  } else {
    hash::ObHashMap<ObString, int64_t>::const_iterator iter = files_to_delete.begin();
    while (OB_SUCC(ret) && iter != files_to_delete.end()) {
      if (OB_UNLIKELY(iter->first.empty() || iter->second < 0)) {
        ret = OB_INVALID_ARGUMENT;
        OB_LOG(WARN, "object name is null or object name idx invalid",
            K(ret), K(iter->first), K(iter->second));
      }
      iter++;
    }
  }
  return ret;
}

int record_failed_files_idx(const hash::ObHashMap<ObString, int64_t> &files_to_delete,
                            ObIArray<int64_t> &failed_files_idx)
{
  int ret = OB_SUCCESS;
  hash::ObHashMap<ObString, int64_t>::const_iterator iter = files_to_delete.begin();
  while (OB_SUCC(ret) && iter != files_to_delete.end()) {
    if (OB_FAIL(failed_files_idx.push_back(iter->second))) {
      OB_LOG(WARN, "fail to record failed del", K(ret), K(iter->first), K(iter->second));
    }
    iter++;
  }
  return ret;
}

int ob_set_field(const char *value, char *field, const uint32_t field_length)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(value) || OB_ISNULL(field)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), KP(value), KP(field));
  } else {
    const int64_t value_len = strlen(value);
    if (value_len >= field_length) {
      ret = OB_SIZE_OVERFLOW;
      OB_LOG(WARN, "value is too long", K(ret), KP(value), K(value_len), K(field_length));
    } else {
      MEMCPY(field, value, value_len);
      field[value_len] = '\0';
    }
  }
  return ret;
}

int ob_apr_abort_fn(int retcode)
{
  int ret = OB_ALLOCATE_MEMORY_FAILED;
  OB_LOG(ERROR, "fail to alloc mem for OSS/COS", K(ret), K(retcode));
  return ret;
}

/*--------------------------------ObAppendableFragmentMeta--------------------------------*/
OB_SERIALIZE_MEMBER(ObAppendableFragmentMeta, start_, end_);

int ObAppendableFragmentMeta::assign(const ObAppendableFragmentMeta &other)
{
  int ret = OB_SUCCESS;
  start_ = other.start_;
  end_ = other.end_;
  type_ = other.type_;
  MEMCPY(suffix_, other.suffix_, sizeof(suffix_));
  return ret;
}

int ObAppendableFragmentMeta::parse_from(ObString &fragment_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!fragment_name.prefix_match(OB_S3_APPENDABLE_FRAGMENT_PREFIX))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid fragment prefix", K(ret), K(fragment_name));
  } else {
    fragment_name += strlen(OB_S3_APPENDABLE_FRAGMENT_PREFIX);
    const char *fragment_suffix = fragment_name.reverse_find('.');
    fragment_name.clip(fragment_suffix);
    if (OB_NOT_NULL(fragment_suffix)) {
      if (strlen(fragment_suffix) <= 1 || strlen(fragment_suffix) >= MAX_APPENDABLE_FRAGMENT_SUFFIX_LENGTH) {
        ret = OB_INVALID_ARGUMENT;
        OB_LOG(WARN, "invalid fragment suffix", K(ret), K(fragment_suffix));
      } else {
        MEMCPY(suffix_, fragment_suffix, strlen(fragment_suffix));
        suffix_[strlen(fragment_suffix)] = '\0';
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (0 == fragment_name.compare(OB_S3_APPENDABLE_FORMAT_META)) {
    type_ = ObAppendableFragmentType::APPENDABLE_FRAGMENT_FORMAT_META;
  } else if (0 == fragment_name.compare(OB_S3_APPENDABLE_SEAL_META)) {
    type_ = ObAppendableFragmentType::APPENDABLE_FRAGMENT_SEAL_META;
  } else {
    ObArenaAllocator allocator(ObModIds::BACKUP);
    ObString start_part = fragment_name.split_on('-');
    ObString start_string;
    ObString end_string;
    if (OB_UNLIKELY(!start_part.is_numeric() || !fragment_name.is_numeric())) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "unexpected fragment name", K(start_part), K(fragment_name));
    } else if (OB_FAIL(ob_write_string(allocator, start_part, start_string, true))) {
      OB_LOG(WARN, "fail to deep copy start part of fragment name",
          K(ret), K(start_string), K(fragment_name), K_(suffix));
    } else if (OB_FAIL(ob_write_string(allocator, fragment_name, end_string, true))) {
      OB_LOG(WARN, "fail to deep copy end part of fragment name",
          K(ret), K(start_string), K(fragment_name), K_(suffix));
    } else if (OB_FAIL(c_str_to_int(start_string.ptr(), start_))) {
      OB_LOG(WARN, "fail to parse 'start'", K(ret), K(start_string), K(fragment_name));
    } else if (OB_FAIL(c_str_to_int(end_string.ptr(), end_))) {
      OB_LOG(WARN, "fail to parse 'end'", K(ret), K(end_string), K(fragment_name));
    } else {
      type_ = ObAppendableFragmentType::APPENDABLE_FRAGMENT_DATA;
      if (OB_UNLIKELY(!is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        OB_LOG(WARN, "invalid fragment name", K(ret), K_(type), K(start_string), K(end_string),
          K(fragment_name), K_(start), K_(end), K_(suffix));
      }
    }
  }
  return ret;
}

int64_t ObAppendableFragmentMeta::to_string(char *buf, const int64_t len) const
{
  int64_t pos = 0;
  if (OB_NOT_NULL(buf) && OB_LIKELY(len > 0)) {
    if (type_ == ObAppendableFragmentType::APPENDABLE_FRAGMENT_DATA) {
      pos = snprintf(buf, len, "%ld-%ld%s", start_, end_, suffix_);
    } else {
      const char *meta_name = (type_ == ObAppendableFragmentType::APPENDABLE_FRAGMENT_FORMAT_META) ?
                               OB_S3_APPENDABLE_FORMAT_META :
                               OB_S3_APPENDABLE_SEAL_META;
      pos = snprintf(buf, len, "%s%s%s", OB_S3_APPENDABLE_FRAGMENT_PREFIX, meta_name, suffix_);
    }

    if (pos < 0) {
      pos = 0;
    } else if (pos >= len) {
      pos = len - 1;
    }
  }
  return pos;
}

/*--------------------------------ObStorageObjectMeta--------------------------------*/
OB_SERIALIZE_MEMBER(ObStorageObjectMetaBase, length_);
OB_SERIALIZE_MEMBER((ObStorageObjectMeta, ObStorageObjectMetaBase), type_, fragment_metas_);

void ObStorageObjectMeta::reset()
{
  // reset do not change obj type
  ObStorageObjectMetaBase::reset();
  fragment_metas_.reset();
}

bool ObStorageObjectMeta::is_valid() const
{
  bool is_valid_flag = (length_ >= 0);
  if (is_simulate_append_type()) {
    for (int64_t i = 0; is_valid_flag && i < fragment_metas_.count(); i++) {
      is_valid_flag = fragment_metas_[i].is_valid();
    }
    if (is_valid_flag && fragment_metas_.count() > 1) {
      for (int64_t i = 1; is_valid_flag && i < fragment_metas_.count(); i++) {
        is_valid_flag = (fragment_metas_[i - 1].start_ < fragment_metas_[i].start_
                && fragment_metas_[i - 1].end_ < fragment_metas_[i].end_);
      }
    }
  } else {
    // for normal objs, fragment_metas_ must be empty;
    is_valid_flag &= fragment_metas_.empty();
  }

  return is_valid_flag;
}

bool ObStorageObjectMeta::fragment_meta_cmp_func(
    const ObAppendableFragmentMeta &left,
    const ObAppendableFragmentMeta &right)
{
  // for fragments with the same start offset, prioritize placing the largest fragment at the beginning,
  // to facilitate subsequent cleaning of overlapping fragments
  return left.start_ < right.start_ || (left.start_ == right.start_ && left.end_ > right.end_);
}

int ObStorageObjectMeta::get_needed_fragments(
    const int64_t start,
    const int64_t end,
    ObArray<ObAppendableFragmentMeta> &fragments)
{
  int ret = OB_SUCCESS;
  fragments.reset();
  if (OB_UNLIKELY(start < 0 || end <= start)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(start), K(end));
  } else if (OB_UNLIKELY(!is_simulate_append_type() || !is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "invalid storage object meta", K(ret), K_(type), K_(fragment_metas));
  } else if (OB_UNLIKELY(fragment_metas_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "empty storage appendable object", K(ret));
  } else if (fragment_metas_[fragment_metas_.count() - 1].end_ <= start) {
    // the data to be read does not exist
  } else {
    int64_t cur_fragment_idx = -1;
    ObAppendableFragmentMeta start_meta;
    start_meta.start_ = start;
    ObSEArray<ObAppendableFragmentMeta, 10>::iterator it =
        std::upper_bound(fragment_metas_.begin(), fragment_metas_.end(), start_meta, fragment_meta_cmp_func);
    if (it == fragment_metas_.begin()) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "the object data may contain holes, can't read correct data", K(ret), K(start), K(end),
        K(fragment_metas_[0].start_));
    } else if (FALSE_IT(cur_fragment_idx = it - fragment_metas_.begin() - 1)) {
    } else {
      int64_t last_fragment_end = fragment_metas_[cur_fragment_idx].start_;
      while (OB_SUCC(ret) && cur_fragment_idx < fragment_metas_.count()
             && fragment_metas_[cur_fragment_idx].start_ < end
             && last_fragment_end < end) {
        if (fragment_metas_[cur_fragment_idx].start_ > last_fragment_end) {
          ret = OB_ERR_UNEXPECTED;
          OB_LOG(WARN, "the object data may contain holes, can't read correct data", K(ret), K(start), K(end),
            K(fragment_metas_[cur_fragment_idx]), K(last_fragment_end));
        } else if (OB_FAIL(fragments.push_back(fragment_metas_[cur_fragment_idx]))) {
          OB_LOG(WARN, "fail to push back fragement", K(ret), K(fragments));
        } else {
          last_fragment_end = fragment_metas_[cur_fragment_idx].end_;
          cur_fragment_idx++;
        }
      }
    }
  }
  return ret;
}

/*--------------------------------ObStorageListCtxBase--------------------------------*/
int ObStorageListCtxBase::init(
    ObArenaAllocator &allocator,
    const int64_t max_list_num,
    const bool need_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(max_list_num < 1)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), K(max_list_num));
  } else {
    max_list_num_ = max_list_num;
    max_name_len_ = OB_MAX_URI_LENGTH;
    need_size_ = need_size;
    if (OB_ISNULL(name_arr_ = static_cast<char **>(allocator.alloc(sizeof(void *) * max_list_num_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "fail to alloc name_arr buff", K(ret), K(*this));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && (i < max_list_num_); ++i) {
        if (OB_ISNULL(name_arr_[i] = static_cast<char *>(allocator.alloc(max_name_len_)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          OB_LOG(WARN, "fail to alloc name buff", K(ret), K(i), K(*this));
        } else {
          name_arr_[i][0] = '\0';
        }
      }
    }

    if (OB_SUCC(ret) && need_size) {
      if (OB_ISNULL(size_arr_ = static_cast<int64_t *>(allocator.alloc(sizeof(int64_t) * max_list_num_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        OB_LOG(WARN, "fail to alloc size_arr buff", K(ret), K(*this));
      }
    }
  }
  return ret;
}

void ObStorageListCtxBase::reset()
{
  max_list_num_ = 0;
  name_arr_ = NULL;
  max_name_len_ = 0;
  rsp_num_ = 0;
  has_next_ = false;
  need_size_ = false;
  size_arr_ = NULL;
  cur_listed_count_ = 0;
  total_list_limit_ = -1;
}

bool ObStorageListCtxBase::is_valid() const
{
  bool bret = (max_list_num_ > 0) && (name_arr_ != NULL) && (max_name_len_ > 0);
  if (need_size_) {
    bret &= (size_arr_ != NULL);
  }
  return bret;
}

void ObStorageListCtxBase::set_total_list_limit(const int64_t limit)
{
  total_list_limit_ = limit;
}

void ObStorageListCtxBase::inc_cur_listed_count()
{
  cur_listed_count_++;
}

bool ObStorageListCtxBase::has_reached_list_limit() const
{
  bool bret = false;
  if (total_list_limit_ > 0) {
    bret = (cur_listed_count_ >= total_list_limit_);
  }
  return bret;
}

/*--------------------------------ObStorageListObjectsCtx--------------------------------*/
void ObStorageListObjectsCtx::reset()
{
  next_token_ = NULL;
  next_token_buf_len_ = 0;
  cur_appendable_full_obj_path_ = NULL;
  marker_ = nullptr;
  ObStorageListCtxBase::reset();
}

int ObStorageListObjectsCtx::init(
    ObArenaAllocator &allocator,
    const int64_t max_list_num,
    const bool need_size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObStorageListCtxBase::init(allocator, max_list_num, need_size))) {
    OB_LOG(WARN, "fail to init storage_list_ctx_base", K(ret), K(max_list_num), K(need_size));
  } else {
    next_token_buf_len_ = OB_MAX_URI_LENGTH;
    if (OB_ISNULL(next_token_ = static_cast<char *>(allocator.alloc(next_token_buf_len_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "fail to alloc next_token buff", K(ret), K(*this));
    } else {
      next_token_[0] = '\0';

      if (OB_ISNULL(cur_appendable_full_obj_path_ = static_cast<char *>(allocator.alloc(OB_MAX_URI_LENGTH)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        OB_LOG(WARN, "fail to alloc cur appendable full obj path buff", K(ret), K(*this));
      } else {
        cur_appendable_full_obj_path_[0] = '\0';
      }
    }
  }
  return ret;
}

int ObStorageListObjectsCtx::set_next_token(
    const bool has_next,
    const char *next_token,
    const int64_t next_token_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "ObStorageListObjectsCtx not init", K(ret));
  } else {
    has_next_ = has_next;
    if (has_next) {
      if (OB_ISNULL(next_token) || OB_UNLIKELY(next_token_len <= 0)) {
        ret = OB_INVALID_ARGUMENT;
        OB_LOG(WARN, "invalid arguments", K(ret), K(has_next), K(next_token), K(next_token_len));
      } else if (OB_UNLIKELY(next_token_len >= next_token_buf_len_)) {
        ret = OB_SIZE_OVERFLOW;
        OB_LOG(WARN, "fail to set next token, size overflow",
            K(ret), K(has_next), K(next_token_len));
      } else {
        MEMCPY(next_token_, next_token, next_token_len);
        next_token_[next_token_len] = '\0';
      }
    } else {
      next_token_[0] = '\0';
    }
  }
  return ret;
}

int ObStorageListObjectsCtx::set_marker(const char *marker)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(marker)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "marker is null", K(ret), K(marker));
  } else {
    marker_ = marker;
  }
  return ret;
}

int ObStorageListObjectsCtx::handle_object(
    const char *obj_path,
    const int obj_path_len,
    const int64_t obj_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "ObStorageListObjectsCtx not init", K(ret));
  } else if (OB_UNLIKELY(obj_size < 0 || obj_path_len >= max_name_len_
                        || obj_path_len <= 0 || rsp_num_ >= max_list_num_)
            || OB_ISNULL(obj_path) || OB_ISNULL(name_arr_[rsp_num_])) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), K(obj_path), K(obj_path_len), K(obj_size), K(*this));
  } else {
    if (need_size_) {
      size_arr_[rsp_num_] = obj_size;
    }
    MEMCPY(name_arr_[rsp_num_], obj_path, obj_path_len);
    name_arr_[rsp_num_][obj_path_len] = '\0';
    ++rsp_num_;
  }
  return ret;
}

/*--------------------------------ObStorageListFilesCtx--------------------------------*/
bool ObStorageListFilesCtx::is_valid() const
{
  bool bret = ObStorageListCtxBase::is_valid();
  if (already_open_dir_) {
    bret &= (open_dir_ != NULL);
  }
  return bret;
}

void ObStorageListFilesCtx::reset()
{
  open_dir_ = NULL;
  already_open_dir_ = false;
  ObStorageListCtxBase::reset();
}

/*--------------------------------ObStoragePartInfoHandler--------------------------------*/
ObStoragePartInfoHandler::ObStoragePartInfoHandler()
    : is_inited_(false),
      part_info_allocator_(PART_INFO_ALLOCATOR_TAG),
      part_info_map_(),
      lock_(ObLatchIds::OBJECT_DEVICE_LOCK)
{
}

ObStoragePartInfoHandler::~ObStoragePartInfoHandler()
{
  reset_part_info();
}

void ObStoragePartInfoHandler::reset_part_info()
{
  SpinWLockGuard guard(lock_);
  is_inited_ = false;
  part_info_map_.destroy();
  part_info_allocator_.clear();
}

int ObStoragePartInfoHandler::init()
{
  SpinWLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  const int64_t PART_INFO_MAP_BUCKET_NUM = 509;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "ObStoragePartInfoHandler has been inited", K(ret));
  } else if (OB_FAIL(part_info_map_.create(PART_INFO_MAP_BUCKET_NUM, PART_INFO_MAP_TAG))) {
    OB_LOG(WARN, "fail to create part info map", K(ret), K(PART_INFO_MAP_BUCKET_NUM));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObStoragePartInfoHandler::add_part_info(
    const int64_t part_id, const char *etag, const char *checksum)
{
  SpinWLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  char *copied_etag_str = nullptr;
  char *copied_checksum_str = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "ObStoragePartInfoHandler not inited", K(ret));
  // checksum is allowed to be null
  // e.g. S3 use md5 | OSS | COS | OBS
  } else if (OB_UNLIKELY(part_id < 1) || OB_ISNULL(etag)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid args", K(ret), K(part_id), KP(etag));
  } else if (OB_FAIL(ob_dup_cstring(part_info_allocator_, etag, copied_etag_str))) {
    OB_LOG(WARN, "fail to deep copy etag", K(ret), K(etag), K(checksum));
  } else if (OB_NOT_NULL(checksum)
      && OB_FAIL(ob_dup_cstring(part_info_allocator_, checksum, copied_checksum_str))) {
    OB_LOG(WARN, "fail to deep copy checksum", K(ret), K(etag), K(checksum));
  // 'set_refactored' is thread safe
  } else if (OB_FAIL(part_info_map_.set_refactored(
      part_id, PartInfo(copied_etag_str, copied_checksum_str)))) {
    OB_LOG(WARN, "fail to store part info", K(ret), K(etag), K(checksum));
  }
  return ret;
}

/*--------------------------------ObObjectStorageGuard--------------------------------*/

static lib::ObMemAttr get_mem_attr_from_storage_info(const ObObjectStorageInfo *storage_info)
{
  static lib::ObMemAttr oss_attr;
  static lib::ObMemAttr cos_attr;
  static lib::ObMemAttr s3_attr;
  static lib::ObMemAttr nfs_attr;
  static lib::ObMemAttr hdfs_attr;
  static lib::ObMemAttr default_attr;
  oss_attr.label_ = "OSS_SDK";
  cos_attr.label_ = "COS_SDK";
  s3_attr.label_ = "S3_SDK";
  nfs_attr.label_ = "NFS_SDK";
  hdfs_attr.label_ = "HDFS_SDK";
  default_attr.label_ = "OBJECT_STORAGE";

  lib::ObMemAttr ret_attr = default_attr;
  if (OB_NOT_NULL(storage_info) && storage_info->is_valid()) {
    const ObStorageType type = storage_info->get_type();
    if (OB_STORAGE_OSS == type) {
      ret_attr = oss_attr;
    } else if (OB_STORAGE_COS == type) {
      ret_attr = cos_attr;
    } else if (OB_STORAGE_S3 == type) {
      ret_attr = s3_attr;
    } else if (OB_STORAGE_FILE == type) {
      ret_attr = nfs_attr;
    } else if (OB_STORAGE_HDFS == type) {
      ret_attr = hdfs_attr;
    }
  }
  return ret_attr;
}

ObObjectStorageGuard::ObObjectStorageGuard(
    const char *file, const int64_t line, const char *func,
    const int &ob_errcode,
    const ObObjectStorageInfo *storage_info,
    const ObString &uri,
    const int64_t &handled_size)
    : lib::ObMallocHookAttrGuard(get_mem_attr_from_storage_info(storage_info)),
      file_name_(file), line_(line), func_name_(func),
      ob_errcode_(ob_errcode),
      storage_info_(storage_info),
      start_time_us_(ObTimeUtility::current_time()),
      uri_(uri),
      handled_size_(handled_size)
{
  if (OB_ISNULL(file_name_)) {
    file_name_ = "";
  } else if (OB_NOT_NULL(strrchr(file_name_, '/'))) {
    file_name_ = strrchr(file_name_, '/') + 1;
  }
}


// when accessing the object storage, if the error code returned is OB_BACKUP_PERMISSION_DENIED,
// it may be due to expired temporary ak/sk
// attempt to refresh the temporary ak/sk, and if the refresh fails,
// only log the error message to avoid overriding the original error code.
static void try_refresh_device_credential(
    const int ob_errcode, const ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  if (ob_errcode != OB_OBJECT_STORAGE_PERMISSION_DENIED) {
    // do nothing
  } else if (OB_ISNULL(storage_info) || OB_UNLIKELY(!storage_info->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), K(ob_errcode), KPC(storage_info));
  } else if (storage_info->is_assume_role_mode()
      && OB_FAIL(ObDeviceCredentialMgr::get_instance().curl_credential(
          *storage_info, true /*update_access_time*/))) {
    OB_LOG(WARN, "failed to refresh credential", K(ret), K(ob_errcode), KPC(storage_info));
  }
}

void ObObjectStorageGuard::print_access_storage_log_() const
{
  const int64_t cost_time_us = ObTimeUtility::current_time() - start_time_us_;
  // MB/s
  const double speed = ((double)handled_size_ / 1024 / 1024)
                     / ((double)cost_time_us / 1000 / 1000);
  const bool is_slow = is_slow_io_(cost_time_us);
  if (is_slow) {
    _STORAGE_LOG_RET(WARN, ob_errcode_,
        "access object storage cost too much time: %s (%s:%ld), "
        "uri=%.*s, size=%ld byte, start_time=%ld, cost_ts=%ld us, speed=%.2f MB/s, is_slow=%d",
        func_name_, file_name_, line_,
        uri_.length(), uri_.ptr(), handled_size_, start_time_us_, cost_time_us, speed, is_slow);
  }
}

bool ObObjectStorageGuard::is_slow_io_(const int64_t cost_time_us) const
{
  bool is_slow = false;
  if (handled_size_ == 0 && cost_time_us >= UTIL_IO_WARN_THRESHOLD_TIME_US) {
    is_slow = true;
  } else if (handled_size_ <= SMALL_IO_SIZE && cost_time_us >= SMALL_IO_WARN_THRESHOLD_TIME_US) {
    is_slow = true;
  } else if (handled_size_ <= MEDIUM_IO_SIZE && cost_time_us >= MEDIUM_IO_WARN_THRESHOLD_TIME_US) {
    is_slow = true;
  } else if (cost_time_us >= LARGE_IO_WARN_THRESHOLD_TIME_US) {
    is_slow = true;
  }
  return is_slow;
}

ObObjectStorageGuard::~ObObjectStorageGuard()
{
  print_access_storage_log_();
  try_refresh_device_credential(ob_errcode_, storage_info_);
  lib::ObMallocHookAttrGuard::~ObMallocHookAttrGuard();
}

}//common
}//oceanbase
