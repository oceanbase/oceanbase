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

#include "ob_storage_cos.h"
#include "lib/hash/ob_hashset.h"
#include "lib/stat/ob_diagnose_info.h"
#include "ob_storage.h"

namespace oceanbase
{
namespace common
{

int init_cos_env()
{
  return qcloud_cos::ObCosEnv::get_instance().init();
}

void fin_cos_env()
{
  // wait doing io finish before destroy cos env.
  const int64_t start_time = ObTimeUtility::current_time();
  const int64_t timeout = ObExternalIOCounter::FLYING_IO_WAIT_TIMEOUT;
  int64_t flying_io_cnt = ObExternalIOCounter::get_flying_io_cnt();
  while(0 < flying_io_cnt) {
    const int64_t end_time = ObTimeUtility::current_time();
    if (end_time - start_time > timeout) {
      OB_LOG(INFO, "force fin_cos_env", K(flying_io_cnt));
      break;
    }
    usleep(100 * 1000L); // 100ms
    flying_io_cnt = ObExternalIOCounter::get_flying_io_cnt();
  }

  qcloud_cos::ObCosEnv::get_instance().fin();
}


/**
 * ------------------------------ObCosUtil---------------------
 */

int ObCosUtil::open(void* base_info)
{
  int ret = OB_SUCCESS;
  cos_base_ = static_cast<ObCosBase*>(base_info);
  if (OB_ISNULL(cos_base_)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "base info is null", K(ret));
  }
  return ret;
}

void ObCosUtil::close()
{
  cos_base_ = NULL;
  //do nothing, does not hold the resource
}

int ObCosUtil::get_file_meta( const ObString &uri, bool &exist, qcloud_cos::CosObjectMeta &meta)
{
  int ret = OB_SUCCESS;
  ObCosObject obj(cos_base_);
  // may be it is a container
  ObCosContainer container(cos_base_);
  exist = false;
  ObExternalIOCounterGuard io_guard;

  const int64_t start_time = ObTimeUtility::current_time();
  if (OB_ISNULL(cos_base_)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "cos handle is null", K(ret), K(uri));
  } else if (OB_FAIL(obj.head_meta(uri, exist, meta))) {
    OB_LOG(WARN, "failed to head meta of object", K(ret), K(uri));
  } else if (!exist && OB_FAIL(container.head_meta(uri, exist, meta))) {
    OB_LOG(WARN, "failed to head meta of container", K(ret), K(uri));
  } else if (exist) {
    OB_LOG(DEBUG, "get file meta succeed", K(uri), K(meta.type), K(meta.file_length));
  }

  //print slow info
  bool is_slow = false;
  print_access_storage_log("cos slow request:get_file_meta ", uri, start_time, 1, &is_slow);

  return ret;
}

int ObCosUtil::is_exist(const ObString &uri, bool &exist)
{
  int ret = OB_SUCCESS;
  qcloud_cos::CosObjectMeta meta;
  ObExternalIOCounterGuard io_guard;

  const int64_t start_time = ObTimeUtility::current_time();
  if (OB_FAIL(get_file_meta(uri, exist, meta))) {
    OB_LOG(WARN, "failed to get file meta", K(ret));
  }

  //print slow info
  bool is_slow = false;
  print_access_storage_log("cos slow request:is_exist ", uri, start_time, 1, &is_slow);

  return ret;
}

int ObCosUtil::get_file_length(const ObString &uri, int64_t &file_length)
{
  int ret = OB_SUCCESS;
  bool exist = false;
  qcloud_cos::CosObjectMeta meta;
  ObExternalIOCounterGuard io_guard;

  const int64_t start_time = ObTimeUtility::current_time();
  if (OB_FAIL(get_file_meta(uri, exist, meta))) {
    OB_LOG(WARN, "failed to get file meta", K(ret));
  } else if(!exist) {
    ret = OB_BACKUP_FILE_NOT_EXIST;
    file_length = -1;
    STORAGE_LOG(INFO, "file not exist", K(ret), K(uri));
  } else {
    file_length = meta.file_length;
  }

  //print slow info
  bool is_slow = false;
  print_access_storage_log("cos slow request:get_file_length ", uri, start_time, 1, &is_slow);

  return ret;
}


int ObCosUtil::write_single_file(const ObString &uri, const char *buf, const int64_t size)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  ObCosObject obj(cos_base_);

  const int64_t start_time = ObTimeUtility::current_time();
  if (OB_ISNULL(cos_base_)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "cos base is null!", K(ret), K(uri));
  } else if (OB_FAIL(obj.build_bucket_and_object_name(uri))) {
    OB_LOG(WARN, "failed to build object", K(ret), K(uri));
  } else {
    const qcloud_cos::CosStringBuffer &bucket_name = obj.bucket_name();
    const qcloud_cos::CosStringBuffer &object_name = obj.object_name();
    qcloud_cos::ObCosWrapper::Handle *h = obj.create_cos_handle();

    const int64_t write_start_time = ObTimeUtility::current_time();
    if (NULL == cos_base_) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "the cos handle is null", K(uri));
    } else if (OB_FAIL(qcloud_cos::ObCosWrapper::put(h, bucket_name, object_name, buf, size))) {
      OB_LOG(WARN, "failed to put object", K(ret), K(uri), K(size));
    } else {
      EVENT_ADD(ObStatEventIds::COS_IO_WRITE_BYTES, size);
      OB_LOG(DEBUG, "succeed to put object", K(uri));
    }

    EVENT_INC(ObStatEventIds::COS_IO_WRITE_COUNT);
    EVENT_ADD(ObStatEventIds::COS_IO_WRITE_DELAY, ObTimeUtility::current_time() - write_start_time);
    obj.destroy_cos_handle(h);
  }

  //print slow info
  bool is_slow = false;
  print_access_storage_log("cos slow request:write_single_file ", uri, start_time, size, &is_slow);

  return ret;
}

// cos no dir
int ObCosUtil::mkdir(const ObString &uri)
{
  int ret = OB_SUCCESS;
  OB_LOG(DEBUG, "no need to create dir in cos", K(uri));

  UNUSED(uri);
  return ret;
}

int ObCosUtil::del_file(const ObString &uri)
{
  int ret = OB_SUCCESS;
  ObCosObject obj(cos_base_);
  // may be it is a container
  ObCosContainer container(cos_base_);
  ObCosObject *base = NULL;

  bool exist = false;
  qcloud_cos::CosObjectMeta meta;
  ObExternalIOCounterGuard io_guard;

  int64_t deleted_cnt;
  const int64_t start_time = ObTimeUtility::current_time();
  if (OB_ISNULL(cos_base_)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "cos handle is null", K(ret));
  } else if (OB_FAIL(get_file_meta(uri, exist, meta))) {
    OB_LOG(WARN, "failed to get file meta", K(ret));
  } else if (!exist) {
    ret = OB_SUCCESS;
    OB_LOG(INFO, "to delete file not exist", K(uri));
  } else {
    if (ObCosObjectType::COS_OBJECT_NORMAL == meta.type) {
      // this is a normal object
      base = &obj;
    } else if (ObCosObjectType::COS_OBJECT_CONTAINER == meta.type) {
      base = &container;
    }

    const int64_t delete_start_time = ObTimeUtility::current_time();
    if (OB_FAIL(base->del(uri, deleted_cnt))) {
      OB_LOG(WARN, "failed to delete file", K(ret), K(uri), K(meta.type), K(meta.file_length));
    } else {
      OB_LOG(DEBUG, "delete file from cos succeeded", K(uri), K(meta.type), K(meta.file_length));
    }

    EVENT_ADD(ObStatEventIds::COS_DELETE_COUNT, deleted_cnt);
    EVENT_ADD(ObStatEventIds::COS_DELETE_DELAY, ObTimeUtility::current_time() - delete_start_time);
  }

  //print slow info
  bool is_slow = false;
  print_access_storage_log("cos slow request:del_file ", uri, start_time, 1, &is_slow);
  EVENT_INC(ObStatEventIds::COS_DELETE_COUNT);

  return ret;
}

// Define structure for callback of cos list objects.
struct CosListFilesCbArg
{
  common::ObIAllocator &allocator;
  ObString &dir_path;
  ObBaseDirEntryOperator &list_op;

  CosListFilesCbArg(
    common::ObIAllocator &alloc,
    ObString &dir,
    ObBaseDirEntryOperator &op)
    : allocator(alloc),
      dir_path(dir),
      list_op(op) {}

  ~CosListFilesCbArg() {}
};

static int handle_upper_callback(ObBaseDirEntryOperator &op,
                                qcloud_cos::ObCosWrapper::CosListObjPara &para,
                                ObString &file_name)
{
  int ret = OB_SUCCESS;
  int64_t dname_size = sizeof(para.last_container_name.d_name);
  if (dname_size >= (file_name.length() + 1)) {
    if (OB_FAIL(databuff_printf(para.last_container_name.d_name, dname_size, "%s", file_name.ptr()))) {
      OB_LOG(WARN, "fail to copy file name to entry buf!", K(ret), K(dname_size));
    } else if (OB_FAIL(op.func(&(para.last_container_name)))) {
      OB_LOG(WARN, "fail to execute upper callback!", K(ret), KCSTRING(para.last_container_name.d_name),
            K(para.last_container_name.d_type), K(DT_REG), K(dname_size));
    }
  } else {
    ret = OB_BUF_NOT_ENOUGH;
    OB_LOG(WARN, "file name len is too long!", K(ret), K(file_name.length()), K(sizeof(dirent)));
  }
  return ret;
}

//Handle each object returned by list cos object
static int handle_object_name_cb(qcloud_cos::ObCosWrapper::CosListObjPara &para)
{
  int ret = OB_SUCCESS;
  para.next_flag = true;
  if (OB_ISNULL(para.arg)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "arg is empty", K(ret));
  } else if (OB_ISNULL(para.cur_full_path_slice_name) || 0 >= para.full_path_size) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "current object_name is empty", K(ret));
  } else {
    CosListFilesCbArg *ctx = (CosListFilesCbArg *)para.arg;

    // Returned object name is the whole object path, but we donot need the prefix dir_path.
    // So, we trim the dir_name from object_name path.
    const int dir_name_str_len = strlen(ctx->dir_path.ptr());
    const int object_name_len = para.full_path_size - dir_name_str_len;
    const int object_name_start_pos = dir_name_str_len;
    //tmp_file_name like slice name, file_name like container name
    ObString tmp_file_name(object_name_len, object_name_len, para.cur_full_path_slice_name + object_name_start_pos);
    ObString file_name;
    bool only;
    // check if is a container
    if (ObCosSlice::is_a_valid_slice_name(tmp_file_name, only)) {
      // get container name
      if (OB_FAIL(ObCosSlice::get_container_name(tmp_file_name, file_name, ctx->allocator))) {
        OB_LOG(WARN, "fail to allocate memory to save file name", K(ret), K(tmp_file_name));
      } else {
        ObString last_file_name(para.last_container_name.d_name);
        if (0 == last_file_name.compare(file_name)) {
          OB_LOG(DEBUG, "ignore pushed slice", K(ret), K(file_name), K(tmp_file_name));
        } else if (OB_FAIL(handle_upper_callback(ctx->list_op, para, file_name))) {
          OB_LOG(WARN, "fail to handle upper callback!", K(ret));
        }
      }
    } else if (OB_FAIL(handle_upper_callback(ctx->list_op, para, tmp_file_name))) {
      OB_LOG(WARN, "fail to handle upper callback!", K(ret));
    }
  }

  return ret;
}


static int handle_directory_name_cb(
  void *arg,
  const char *object_name,
  int64_t object_size)
{
  int ret = OB_SUCCESS;
  const char delimiter = '/';
  if (OB_ISNULL(arg)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "arg is empty", K(ret), KP(arg));
  } else if (OB_ISNULL(object_name) || 0 >= object_size) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "object_name is empty", K(ret), KP(object_name), K(object_size));
  } else if (delimiter != object_name[object_size - 1]) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "object name is unexpected", K(ret), KCSTRING(object_name), K(object_size));
  } else {
    const int64_t name_length = object_size - 1;
    if (name_length <= 0) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "object is not exist", K(ret), KCSTRING(object_name), K(object_size));
    } else {
      ObString directory_name;
      dirent dir_name_entry;
      dir_name_entry.d_type = DT_DIR;
      ObString tmp_directory_name(name_length, name_length, object_name);
      if (OB_FAIL(databuff_printf(dir_name_entry.d_name, sizeof(dir_name_entry.d_name), "%.*s",
                                  tmp_directory_name.length(), tmp_directory_name.ptr()))) {
        OB_LOG(WARN, "fail to set dir name entry", K(ret), K(tmp_directory_name));
      }
    }
  }
  return ret;
}


int ObCosUtil::list_files(
  const ObString &uri,
  common::ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  ObArenaAllocator allocator;
  ObCosObject obj(cos_base_);

  const int64_t start_time = ObTimeUtility::current_time();
  if (OB_ISNULL(cos_base_)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "cos handle is null", K(ret), K(uri));
  } else if (OB_FAIL(obj.build_bucket_and_object_name(uri))) {
    OB_LOG(WARN, "failed to build object", K(ret), K(uri));
  } else {
    char dir_path_array[OB_MAX_URI_LENGTH] = {0};
    // point to actual dir path used
    const char *ptr = NULL;
    // dir_path end with '\0', its length must be >= 2
    const int min_dir_path_len = 2;
    const char *seperator = "/";
    const char end_marker = '\0';

    const ObString &dir_path_string = obj.object_name_string();
    const ObString::obstr_size_t dir_path_len = dir_path_string.length();
    int32_t actual_dir_path_len = dir_path_len;
    if (dir_path_len < min_dir_path_len) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "dir_path_string length is unepxected", K(ret), K(dir_path_string), K(dir_path_len));
    } else {
      // Let dir path end with '/'
      if (dir_path_string.ptr()[dir_path_len - 1] != end_marker) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "dir_path_string not end with '\0'");
      } else if (dir_path_string.ptr()[dir_path_len - 2] == *seperator) {
        ptr = dir_path_string.ptr();
      } else if (OB_FAIL(databuff_printf(dir_path_array, OB_MAX_URI_LENGTH, "%s/", dir_path_string.ptr()))) {
        OB_LOG(WARN, "fail to deep copy dir path", K(ret), K(dir_path_len), K(OB_MAX_URI_LENGTH));
      } else {
        ptr = dir_path_array;
        actual_dir_path_len += 1;
      }
    }

    if (OB_SUCC(ret)) {
      ObString tmp_dir;
      // List objects
      tmp_dir.assign(const_cast<char *>(ptr), actual_dir_path_len);

      // Construct list object callback arg
      CosListFilesCbArg arg(allocator, tmp_dir, op);

      const qcloud_cos::CosStringBuffer &bucket_name = obj.bucket_name();
      qcloud_cos::CosStringBuffer dir_name = ObCosObject::obstring_to_string_buffer(tmp_dir);
      qcloud_cos::CosStringBuffer next_marker(seperator, strlen(seperator) + 1);
      const int64_t ls_start_time = ObTimeUtility::current_time();
      qcloud_cos::ObCosWrapper::Handle *h = obj.create_cos_handle();
      if (NULL == h) {
        ret = OB_INVALID_ARGUMENT;
        OB_LOG(WARN, "cos handle is null", K(ret), K(uri));
      } else if (OB_FAIL(qcloud_cos::ObCosWrapper::list_objects(h, bucket_name, dir_name,
                 next_marker, handle_object_name_cb, (void *)(&arg)))) {
        if (ret == OB_IO_LIMIT) {
          EVENT_INC(ObStatEventIds::COS_IO_LS_LIMIT_COUNT);
        }
        const ObString &bucket_name_string = obj.bucket_name_string();
        OB_LOG(WARN, "failed to list objects", K(ret), K(bucket_name_string), K(tmp_dir));
      } else {
        OB_LOG(DEBUG, "list objects succeed");
      }

      EVENT_INC(ObStatEventIds::COS_IO_LS_COUNT);
      EVENT_ADD(ObStatEventIds::COS_IO_LS_DELAY, ObTimeUtility::current_time() - ls_start_time);
      obj.destroy_cos_handle(h);
    }
  }

  //print slow info
  bool is_slow = false;
  print_access_storage_log("cos slow request:list_files ", uri, start_time, 0, &is_slow);

  return ret;
}

int ObCosUtil::del_dir(const ObString &uri)
{
  int ret = OB_SUCCESS;
  UNUSED(uri);
  return ret;
}

int ObCosUtil::check_backup_dest_lifecycle(const common::ObString &path, bool &is_set_lifecycle)
{
  int ret = OB_SUCCESS;
  UNUSED(path);
  is_set_lifecycle = false;
  return ret;
}

int ObCosUtil::list_directories(const ObString &uri, common::ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  ObArenaAllocator allocator;
  ObCosObject obj(cos_base_);

  const int64_t start_time = ObTimeUtility::current_time();
  if (OB_ISNULL(cos_base_)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "cos handle is null", K(ret), K(uri));
  } else if (OB_FAIL(obj.build_bucket_and_object_name(uri))) {
    OB_LOG(WARN, "failed to build object", K(ret), K(uri));
  } else {
    char dir_path_array[OB_MAX_URI_LENGTH] = {0};
    // point to actual dir path used
    const char *ptr = NULL;
    // dir_path end with '\0', its length must be >= 2
    const int min_dir_path_len = 2;
    const char *delimiter_string = "/";
    const char *next_maker_string = "";
    const char end_marker = '\0';

    const ObString &dir_path_string = obj.object_name_string();
    const ObString::obstr_size_t dir_path_len = dir_path_string.length();
    int32_t actual_dir_path_len = dir_path_len;
    if (dir_path_len < min_dir_path_len) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "dir_path_string length is unepxected", K(ret), K(dir_path_string), K(dir_path_len));
    } else {
      // Let dir path end with '/'
      if (dir_path_string.ptr()[dir_path_len - 1] != end_marker) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "dir_path_string not end with '\0'");
      } else if (dir_path_string.ptr()[dir_path_len - 2] == *delimiter_string) {
        ptr = dir_path_string.ptr();
      } else if (OB_FAIL(databuff_printf(dir_path_array, OB_MAX_URI_LENGTH, "%s/", dir_path_string.ptr()))) {
        OB_LOG(WARN, "fail to deep copy dir path", K(ret), K(dir_path_len), K(OB_MAX_URI_LENGTH));
      } else {
        ptr = dir_path_array;
        actual_dir_path_len += 1;
      }
    }

    if (OB_SUCC(ret)) {
      ObString tmp_dir;
      // List objects
      tmp_dir.assign(const_cast<char *>(ptr), actual_dir_path_len);
      // Construct list object callback arg
      CosListFilesCbArg arg(allocator, tmp_dir, op);

      const qcloud_cos::CosStringBuffer &bucket_name = obj.bucket_name();
      qcloud_cos::CosStringBuffer dir_name = ObCosObject::obstring_to_string_buffer(tmp_dir);
      qcloud_cos::CosStringBuffer next_marker(next_maker_string, strlen(next_maker_string) + 1);
      qcloud_cos::CosStringBuffer delimiter(delimiter_string, strlen(delimiter_string) + 1);
      const int64_t ls_start_time = ObTimeUtility::current_time();
      qcloud_cos::ObCosWrapper::Handle *h = obj.create_cos_handle();
      if (NULL == h) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        OB_LOG(WARN, "failed to create cos handle", K(ret), K(uri));
      } else if (OB_FAIL(qcloud_cos::ObCosWrapper::list_directories(h, bucket_name, dir_name, next_marker,
                                    delimiter, handle_directory_name_cb, (void *)(&arg)))) {
        if (ret == OB_IO_LIMIT) {
          EVENT_INC(ObStatEventIds::COS_IO_LS_LIMIT_COUNT);
        }
        const ObString &bucket_name_string = obj.bucket_name_string();
        OB_LOG(WARN, "failed to list objects", K(ret), K(bucket_name_string), K(tmp_dir));
      } else {
        OB_LOG(DEBUG, "list objects succeed");
      }

      EVENT_INC(ObStatEventIds::COS_IO_LS_COUNT);
      EVENT_ADD(ObStatEventIds::COS_IO_LS_DELAY, ObTimeUtility::current_time() - ls_start_time);
      obj.destroy_cos_handle(h);
    }
  }
  //print slow info
  bool is_slow = false;
  print_access_storage_log("cos slow request:list_directories ", uri, start_time, 0, &is_slow);
  OB_LOG(DEBUG, "list directories", K(ret), K(uri));
  return ret;
}

int ObCosUtil::is_tagging(const common::ObString &uri, bool &is_tagging)
{
  // cos don't support tagging
  int ret = OB_SUCCESS;
  ObCosObject obj;
  if (OB_FAIL(obj.build_bucket_and_object_name(uri))) {
    OB_LOG(WARN, "failed to build object", K(ret), K(uri));
  } else {
    is_tagging = false;
  }
  return ret;
}
/**
 * ------------------------------ObCosRandomAccessObject---------------------
 */
ObCosRandomAccessObject::ObCosRandomAccessObject()
  : obj_(), meta_(), is_opened_(false)
{

}

int ObCosRandomAccessObject::open( const ObString &uri, void *cos_base)
{
  int ret = OB_SUCCESS;
  bool is_file_exist = false;

  const int64_t start_time = ObTimeUtility::current_time();

  if (OB_ISNULL(obj_.cos_base_ = static_cast<ObCosBase*>(cos_base))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "the cos handle is null", K(ret));
  } else if (is_opened()) {
    // cannot open an object twice
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "already open, cannot open again", K(ret));
  } else if (OB_FAIL(obj_.head_meta(uri, is_file_exist, meta_))) {
    OB_LOG(WARN, "failed to head meta", K(ret), K(uri));
  } else if (!is_file_exist) {
    // file not exist, open failed.
    ret = OB_BACKUP_FILE_NOT_EXIST;
    const ObString &bucket_name = obj_.bucket_name_string();
    const ObString &object_name = obj_.object_name_string();
    OB_LOG(WARN, "file not exist", K(bucket_name), K(object_name));
  } else {
    is_opened_ = true;
    // open successfully, and file exist
    OB_LOG(DEBUG, "open file successfully", K(uri));
  }

  //print slow info
  bool is_slow = false;
  print_access_storage_log("cos slow request:ObCosRandomAccessObject::open ", uri, start_time, 1L, &is_slow);

  return ret;
}

int ObCosRandomAccessObject::pread(
  char *buf,
  const int64_t buf_size,
  int64_t offset,
  int64_t &read_size)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;

  const int64_t start_time = ObTimeUtility::current_time();
  if (!is_opened()) {
    ret = OB_COS_ERROR;
    OB_LOG(WARN, "cos cannot do read before open the object");
  } else if (NULL == buf || buf_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "buf is NULL or buf_size is invalid", K(buf_size), K(ret));
  } else if (meta_.file_length <= offset) {
    ret = OB_FILE_LENGTH_INVALID;
    OB_LOG(WARN, "read offset is beyond the object size", K(offset), K(meta_.file_length), K(ret));
  } else {
    const qcloud_cos::CosStringBuffer &bucket_name = obj_.bucket_name();
    const qcloud_cos::CosStringBuffer &object_name = obj_.object_name();
    const ObString &bucket_name_string = obj_.bucket_name_string();
    const ObString &object_name_string = obj_.object_name_string();
    qcloud_cos::ObCosWrapper::Handle *h = obj_.create_cos_handle();

    if (NULL == h) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "failed to create cos handle", K(ret), K(bucket_name_string), K(object_name_string), K(offset), K(buf_size));
    } else if (OB_FAIL(qcloud_cos::ObCosWrapper::pread(h, bucket_name, object_name, offset, buf, buf_size, read_size))) {
      OB_LOG(WARN, "faied to read from cos", K(ret), K(bucket_name_string), K(object_name_string), K(offset), K(buf_size));
    } else {
      EVENT_ADD(ObStatEventIds::COS_IO_READ_BYTES, read_size);
    }

    EVENT_INC(ObStatEventIds::COS_IO_READ_COUNT);
    EVENT_ADD(ObStatEventIds::COS_IO_READ_DELAY, ObTimeUtility::current_time() - start_time);
    obj_.destroy_cos_handle(h);
    //print slow info
    bool is_slow = false;
    print_access_storage_log("cos slow request:ObCosRandomAccessObject::pread ", object_name_string, start_time, read_size, &is_slow);
  }

  return ret;
}

int ObCosRandomAccessObject::close()
{
  int ret = OB_SUCCESS;
  if (!is_opened()) {
    ret = OB_COS_ERROR;
    OB_LOG(WARN, "cannot close before it is opened", K(ret));
  } else {
    is_opened_ = false;
    meta_.reset();
  }

  return ret;
}


/**
 * ------------------------------ObCosOverWriteObject---------------------
 */
ObCosOverWriteObject::ObCosOverWriteObject(bool append_flag)
  : obj_(), file_length_(-1), object_size_(-1), is_opened_(false), append_flag_(append_flag)
{

}

int ObCosOverWriteObject::open(const ObString &uri, void *cos_base)
{
  int ret = OB_SUCCESS;
  bool is_exist;
  qcloud_cos::CosObjectMeta meta;
  ObExternalIOCounterGuard io_guard;

  const int64_t start_time = ObTimeUtility::current_time();

  if (OB_ISNULL(obj_.cos_base_ = static_cast<ObCosBase*>(cos_base))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "the cos device handle is null", K(ret));
  } else if (is_opened()) {
    // cannot open an object twice
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "already open, cannot open again", K(ret));
  } else if (OB_FAIL(obj_.head_meta(uri, is_exist, meta))) {
    OB_LOG(WARN, "failed to head meta", K(ret), K(uri));
  } else {
    // open successfully no matter the object exist or not.
    if (!is_exist || !append_flag_) {
      object_size_ = 0;
    } else {
      object_size_ = meta.file_length;
    }
    is_opened_ = true;
    file_length_ = 0;
    OB_LOG(DEBUG, "open file successfully", K(uri));
  }

  //print slow info
  bool is_slow = false;
  print_access_storage_log("cos slow request:ObCosOverWriteObject::open ", uri, start_time, 1L, &is_slow);

  return ret;
}

int ObCosOverWriteObject::write(const char *buf, const int64_t size)
{
  // As cos does not support append semantics. Each write will read
  // the whole object from cos first, append the writing data
  // to the tail, then put the new data to cos as a new object.
  int ret = OB_SUCCESS;
  const char *buf_ptr = buf;
  int64_t new_length = size;
  common::ObArenaAllocator allocator;
  ObExternalIOCounterGuard io_guard;

  const int64_t start_time = ObTimeUtility::current_time();
  if (!is_opened()) {
    ret = OB_COS_ERROR;
    OB_LOG(WARN, "cos cannot do write before open the object");
  } else if (0 < object_size_) {
    // Object exist, do read first.
    new_length = object_size_ + size;
    char *new_buf = NULL;
    // Allocate memory for object and the new writing data.
    if (NULL == (new_buf = (char *)allocator.alloc(new_length))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "failed to allocate memory.", K(ret), K(new_length));
    } else {
      buf_ptr = new_buf;
      // get the object from cos.
      if (OB_FAIL(get_(new_buf, object_size_))) {
        OB_LOG(WARN, "failed to get object.", K(ret), K(object_size_));
      } else {
        // append the writing data
        MEMCPY(new_buf + object_size_, buf, size);
      }
    }
  }

  if (is_opened()) {
    //print slow info
    const ObString &object_name_string = obj_.object_name_string();
    bool is_slow = false;
    print_access_storage_log("cos slow request:ObCosOverWriteObject::write-get ", object_name_string, start_time, new_length - size, &is_slow);
  }

  // put the new buffer
  if (OB_SUCC(ret)) {
    const qcloud_cos::CosStringBuffer &bucket_name = obj_.bucket_name();
    const qcloud_cos::CosStringBuffer &object_name = obj_.object_name();
    qcloud_cos::ObCosWrapper::Handle *h = obj_.create_cos_handle();

    const int64_t write_start_time = ObTimeUtility::current_time();
    if (NULL == h) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "failed to create cos handle", K(ret), K(new_length));
    } else if (OB_FAIL(qcloud_cos::ObCosWrapper::put(h, bucket_name, object_name, buf_ptr, new_length))) {
      OB_LOG(WARN, "failed to put object.", K(ret), K(new_length));
    } else {
      file_length_ += size;
      object_size_ += size;
      EVENT_ADD(ObStatEventIds::COS_IO_WRITE_BYTES, new_length);
    }

    EVENT_INC(ObStatEventIds::COS_IO_WRITE_COUNT);
    EVENT_ADD(ObStatEventIds::COS_IO_WRITE_DELAY, ObTimeUtility::current_time() - write_start_time);
    obj_.destroy_cos_handle(h);
    //print slow info
    bool is_slow = false;
    print_access_storage_log("cos slow request:ObCosOverWriteObject::write-put ", object_name_string_(), start_time, new_length, &is_slow);
  }

  if (OB_FAIL(ret)) {
    OB_LOG(WARN, "failed to write", K(ret), K(bucket_name_string_()), K(object_name_string_()), K(object_size_), K(size));
  }

  return ret;
}

int ObCosOverWriteObject::pwrite(const char *buf, const int64_t size, const int64_t offset)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  if (!is_opened()) {
    ret = OB_COS_ERROR;
    OB_LOG(WARN, "cos cannot do write before open the object");
  } else if (object_size_ != offset) {
    ret = OB_EAGAIN;
    OB_LOG(WARN, "object_size and offset do not match", K(ret), K_(object_size), K(offset), K(size));
  } else if (OB_FAIL(write(buf, size))) {
    OB_LOG(WARN, "failed to do pwrite", K(ret), K_(object_size), K(offset), K(size));
  }
  return ret;
}

int ObCosOverWriteObject::close()
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  if (!is_opened()) {
    ret = OB_COS_ERROR;
    OB_LOG(WARN, "cannot close before it is opened", K(ret));
  } else {
    is_opened_ = false;
    file_length_ = -1;
    object_size_ = -1;
  }

  return ret;
}

int ObCosOverWriteObject::get_ (
  char *buf,
  const int64_t buf_size)
{
  int ret = OB_SUCCESS;
  int64_t read_size = 0;
  const qcloud_cos::CosStringBuffer &bucket_name = obj_.bucket_name();
  const qcloud_cos::CosStringBuffer &object_name = obj_.object_name();
  qcloud_cos::ObCosWrapper::Handle *h = NULL;

  const int64_t start_time = ObTimeUtility::current_time();
  if (OB_ISNULL(obj_.cos_base_)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "cos base is null!", K(ret));
  } else if (OB_ISNULL(h = obj_.create_cos_handle())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "failed to create cos handle", K(ret), K(bucket_name_string_()), K(object_name_string_()), K(buf_size));
  } else if (OB_FAIL(qcloud_cos::ObCosWrapper::get_object(h, bucket_name, object_name, buf, buf_size, read_size))) {
    OB_LOG(WARN, "faied to get object from cos", K(ret), K(bucket_name_string_()), K(object_name_string_()), K(buf_size));
  } else if (read_size != buf_size) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "object size is not match, maybe there is another writer writing the same object", K(bucket_name_string_()), K(object_name_string_()), K(buf_size), K(read_size));
  } else {
    EVENT_ADD(ObStatEventIds::COS_IO_READ_BYTES, read_size);
  }

  EVENT_INC(ObStatEventIds::COS_IO_READ_COUNT);
  EVENT_ADD(ObStatEventIds::COS_IO_READ_DELAY, ObTimeUtility::current_time() - start_time);
  obj_.destroy_cos_handle(h);

  //print slow info
  bool is_slow = false;
  print_access_storage_log("cos slow request:ObCosOverWriteObject::get ", object_name_string_(), start_time, read_size, &is_slow);

  return ret;
}

/**
 * ------------------------------ObCosSlice---------------------
 */
ObCosSlice::ObCosSlice(const ObCosSlice::Option& option) : option_(option)
{

}

bool ObCosSlice::is_a_valid_slice_name(const ObString& path, bool &only)
{
  // A valid slice name must start with @part@_, for example, name of slice id 1
  // is either @part@_1 which is not a multi-version slice or @part@_1-@version@_500
  // which is a multi-version slice with a version of 500.
  bool res = false;
  // find the first '/' from the back to the front
  const char *start = path.reverse_find('/');
  ObString slice_name;
  if (NULL == start) {
    // only slice name
    start = path.ptr();
    slice_name.assign(const_cast<char *>(start), path.length());
    only = true;
  } else {
    const int64_t n = path.length() - (start - path.ptr() + 1);
    slice_name.assign(const_cast<char *>(start) + 1, n);
    only = false;
  }

  // slice name must begin with prefix of COS_SLICE_PREFIX
  if (slice_name.prefix_match(COS_SLICE_MARKER)) {
    res = true;
  }

  // TODO:validate more strictly, use regular expression

  return res;
}

int ObCosSlice::parse_slice_name(
  const ObString &path,
  ObCosSlice::Option &option)
{
  int ret = OB_SUCCESS;
  char buffer[OB_MAX_URI_LENGTH];

  // find the last '/'
  const char *start = path.reverse_find('/');
  int64_t slice_name_length = 0;
  const char *slice_name_start = NULL;
  if (NULL == start) {
    // only slice name.
    slice_name_start = path.ptr();
    slice_name_length = path.length();
  } else {
    slice_name_start = start + 1;
    slice_name_length = path.length() - (start - path.ptr() + 1);
  }

  // copy slice name to buffer
  if (OB_MAX_URI_LENGTH > slice_name_length) {
    MEMCPY(buffer, slice_name_start, slice_name_length);
    buffer[slice_name_length] = '\0';

    char *token = buffer;
    char *saved_ptr = NULL;
    char *end_ptr = NULL;

    option.mask = Mask::OB_COS_SLICE_INVALID_MASK;
    option.sliceid = 0;
    option.version = 0;
    for (char *str = token; ret == OB_SUCCESS; str = NULL) {
      token = ::strtok_r(str, COS_SLICE_CONCATENATION, &saved_ptr);
      if (NULL == token) {
        break;
      } else if (0 == strncmp(token, COS_SLICE_MARKER, strlen(COS_SLICE_MARKER))) {
        if (OB_FAIL(ob_strtoll(token + strlen(COS_SLICE_MARKER), end_ptr, option.sliceid))) {
          OB_LOG(WARN, "failed to parse slice id", KCSTRING(token), K(path), K(ret));
        } else if (*end_ptr != 0) {
          ret = OB_COS_ERROR;
          OB_LOG(WARN, "not a valid slice name", KCSTRING(token), K(path), K(option.sliceid), K(ret));
        } else {
          option.mask |= Mask::OB_COS_SLICE_ID_MASK;
          OB_LOG(DEBUG, "sliceid", K(option.sliceid), KCSTRING(token));
        }
      } else if (0 == strncmp(token, COS_VERSION_MAKER, strlen(COS_VERSION_MAKER))) {
        if (OB_FAIL(ob_strtoll(token + strlen(COS_VERSION_MAKER), end_ptr, option.version))) {
          OB_LOG(WARN, "failed to parse version", KCSTRING(token), K(path), K(ret));
        } else if (*end_ptr != 0) {
          ret = OB_COS_ERROR;
          OB_LOG(WARN, "not a valid slice name", KCSTRING(token), K(path), K(option.version), K(ret));
        } else {
          option.mask |= Mask::OB_COS_MULTI_VERSION_MASK;
          OB_LOG(DEBUG, "version", K(option.version), KCSTRING(token));
        }
      } else {
        ret = OB_COS_ERROR;
        OB_LOG(WARN, "unkown token", KCSTRING(token), K(path), K(ret));
      }
    }
  } else {
    ret = OB_SIZE_OVERFLOW;
    OB_LOG(WARN, "slice name is too long", K(path), K(slice_name_length), K(ret));
  }


  if (OB_SUCC(ret)) {
    // check if slice is valid
    ObCosSlice slice(option);
    if (!slice.is_valid()) {
      ret = OB_COS_ERROR;
      OB_LOG(WARN, "unexpected error, not a valid slice path", K(ret), K(path), K(slice));
    }
  }

  return ret;
}

int ObCosSlice::get_container_name(
  const ObString& slice_name,
  ObString &container_name,
  common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObString tmp;
  const char *p = NULL;
  bool only;
  if (!ObCosSlice::is_a_valid_slice_name(slice_name, only)) {
    ret = OB_COS_ERROR;
    OB_LOG(WARN, "not a valid slice name", K(ret), K(slice_name));
  } else if (NULL == (p = slice_name.reverse_find('/'))) {
    ret = OB_COS_ERROR;
    OB_LOG(WARN, "container name not exist", K(ret), K(slice_name));
  } else {
    const int64_t n = p - slice_name.ptr();
    if (0 >= n) {
      ret = OB_COS_ERROR;
      OB_LOG(WARN, "container name not exist", K(ret), K(slice_name));
    } else {
      tmp.assign(const_cast<char *>(slice_name.ptr()), n);

      if (OB_FAIL(ob_write_string(allocator, tmp, container_name, true /*c_style*/))) {
        OB_LOG(WARN, "failed to allocate memory to save container name", K(ret), K(slice_name), K(tmp));
      }
    }
  }

  return ret;
}

int ObCosSlice::build_slice_name(char *buffer, int64_t buff_size)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (!is_valid()) {
    ret = OB_COS_ERROR;
    OB_LOG(WARN, "slice is not valid", K(ret), K(option_));
  } else if (OB_FAIL(databuff_printf(buffer, buff_size, pos, "%s%ld", COS_SLICE_MARKER, option_.sliceid))) {
    OB_LOG(WARN, "failed to fill slice id", K(ret), K(option_));
  } else if (is_multi_version_slice()) {
    if (OB_FAIL(databuff_printf(buffer, buff_size, pos, "%s%s%ld", COS_SLICE_CONCATENATION, COS_VERSION_MAKER, option_.version))) {
      OB_LOG(WARN, "failed to fill version", K(ret), K(option_));
    }
  }

  return ret;
}


/**
 * ------------------------------ObCosContainer---------------------
 */
struct CosContainerListArgs
{
  CosContainerListArgs(
    ObString &container,
    common::ObIAllocator &alloc,
    common::ObIArray <ObString> &slice_names_array,
    common::ObIArray <int64_t> &slice_lengths_array) :
    container_name(container),
    allocator(alloc),
    slice_names(slice_names_array),
    slice_lengths(slice_lengths_array)
  {

  }

  ~CosContainerListArgs() {}

  ObString &container_name;
  common::ObIAllocator &allocator;
  common::ObIArray <ObString> &slice_names;
  common::ObIArray <int64_t> &slice_lengths;
};

// handle each slice returned by list object
static int handle_each_slice_cb(qcloud_cos::ObCosWrapper::CosListObjPara &para)
{
  int ret = OB_SUCCESS;
  CosContainerListArgs *ctx = reinterpret_cast<CosContainerListArgs *>(para.arg);
  if (OB_ISNULL(para.arg)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "args is null", K(ret));
  } else if (OB_ISNULL(para.cur_full_path_slice_name) || 0 >= para.full_path_size) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "uri is null", K(ret));
  } else {
    const ObString &container_name = ctx->container_name;
    const int container_name_len = container_name.length();
    const int slice_name_len = para.full_path_size - container_name_len;
    const int slice_name_start_pos = container_name_len;

    para.next_flag = true;
    // get rid of the container name from the uri
    ObString tmp_slice_name(slice_name_len, slice_name_len, para.cur_full_path_slice_name + slice_name_start_pos);
    bool only;
    if (ObCosSlice::is_a_valid_slice_name(tmp_slice_name, only) && only) {
      ObString slice_name;
      if (OB_FAIL(ob_write_string(ctx->allocator, tmp_slice_name, slice_name, true /*c_style*/))) {
        OB_LOG(WARN, "failed to allocate memory to save slice name", K(ret), K(tmp_slice_name));
      } else if (OB_FAIL(ctx->slice_names.push_back(slice_name))) {
        OB_LOG(WARN, "failed to push back slice name", K(ret), K(slice_name), K(para.cur_object_size));
      } else if (OB_FAIL(ctx->slice_lengths.push_back(para.cur_object_size))) {
        OB_LOG(WARN, "failed to push back slice size", K(ret), K(slice_name), K(para.cur_object_size));
      } else {
        OB_LOG(DEBUG, "get one slice", K(slice_name), K(para.cur_object_size));
      }
    }
  }

  return ret;
}

int ObCosContainer::head_meta(
  bool &is_exist,
  qcloud_cos::CosObjectMeta &meta,
  common::ObIAllocator &allocator,
  common::ObIArray <ObString> &slice_names,
  common::ObIArray <int64_t> &slice_lengths)
{
  int ret = OB_SUCCESS;

  // provide memory for all returned slice name
  ObArenaAllocator tmp_allocator;
  // hold all returned slices
  ObArray<ObString> tmp_slice_names_array;
  // hold all returned lengths of slices
  ObArray<int64_t> tmp_slice_lengths_array;
  int64_t max_sliceid = 0;
  int64_t max_version = 0;

  const ObString &bucket_name_string = this->bucket_name_string();
  const ObString &container_name_string = this->object_name_string();

  if (OB_FAIL(list_slices(is_exist, tmp_allocator, tmp_slice_names_array, tmp_slice_lengths_array))) {
    OB_LOG(WARN, "failed to list slices", K(ret), K(bucket_name_string), K(container_name_string));
  }

  // validate the returned slices mask and names.
  if (OB_SUCC(ret) && is_exist) {
    // get first slice
    ObCosSlice::Option first_slice_option;
    ObString &first_slice_name = tmp_slice_names_array.at(0);
    if (OB_FAIL(ObCosSlice::parse_slice_name(first_slice_name, first_slice_option))) {
      OB_LOG(WARN, "parse first slice failed", K(ret), K(first_slice_name));
    } else {
      max_sliceid = first_slice_option.sliceid;
      max_version = first_slice_option.version;
      ObCosSlice::Option slice_option;
      for (int64_t i = 1; i < tmp_slice_names_array.count(); i++) {
        ObString &slice_name = tmp_slice_names_array.at(i);
        if (OB_FAIL(ObCosSlice::parse_slice_name(slice_name, slice_option))) {
          OB_LOG(WARN, "parse slice failed", K(ret), K(slice_name), K(i));
          break;
        } else if (slice_option.mask != first_slice_option.mask) {
          // mask does not match
          ret = OB_COS_ERROR;
          OB_LOG(WARN, "slice mask not match", K(ret), K(first_slice_name), K(slice_name));
          break;
        } else {
          // update max sliceid in container
          max_sliceid = max(max_sliceid, slice_option.sliceid);
          max_version = max(max_version, slice_option.version);
        }
      }

      if (max_sliceid > ObCosContainer::MAX_SLICE_ID) {
        ret = OB_COS_ERROR;
        OB_LOG(WARN, "max slice id is not valid", K(max_sliceid), K(ret));
      }
    }
  }

  bool *slice_id_flag = NULL;
  int64_t *versions = NULL;
  int64_t *lengths = NULL;
  int64_t *slice_name_indexes = NULL;
  if (OB_SUCC(ret) && is_exist) {
    slice_id_flag = (bool *)tmp_allocator.alloc(max_sliceid * sizeof(bool));
    versions = (int64_t *)tmp_allocator.alloc(max_sliceid * sizeof(int64_t));
    lengths = (int64_t *)tmp_allocator.alloc(max_sliceid * sizeof(int64_t));
    slice_name_indexes = (int64_t *)tmp_allocator.alloc(max_sliceid * sizeof(int64_t));
    if (NULL == slice_id_flag || NULL == versions || NULL == lengths || NULL == slice_name_indexes) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "failed to allocate memory", K(ret));
    } else {
      MEMSET(slice_id_flag, 0, max_sliceid * sizeof(bool));
      MEMSET(versions, 0, max_sliceid * sizeof(int64_t));
      MEMSET(lengths, 0, max_sliceid * sizeof(int64_t));
      MEMSET(slice_name_indexes, 0, max_sliceid * sizeof(int64_t));
    }
  }


  if (OB_SUCC(ret) && is_exist) {
    // sort slice by id and version
    ObCosSlice::Option slice_option;
    for (int64_t idx = 0; idx < tmp_slice_names_array.count(); idx++) {
      ObString &slice_name = tmp_slice_names_array.at(idx);
      // have validated the slice name before
      ObCosSlice::parse_slice_name(slice_name, slice_option);
      // mark this slice id exist
      slice_id_flag[slice_option.sliceid - 1] = true;
      if (!slice_option.is_multi_version_slice()) {
        // not multi version slice
        lengths[slice_option.sliceid - 1] = tmp_slice_lengths_array[idx];
        slice_name_indexes[slice_option.sliceid - 1] = idx;
      } else if (slice_option.version >= versions[slice_option.sliceid - 1]) {
        // multi version slice, and current is newer.
        versions[slice_option.sliceid - 1] = slice_option.version;
        lengths[slice_option.sliceid - 1] = tmp_slice_lengths_array[idx];
        slice_name_indexes[slice_option.sliceid - 1] = idx;
      } else {
        // open version, and current is older
        // do nothing
        OB_LOG(DEBUG, "ignore smaller version", K(bucket_name_string), K(container_name_string), K(slice_name));
      }
    }

    // validate again
    // condition 1: first slice must be exist
    // condition 2: slice id starts from 1, and must be continous.
    // condition 3: the next version must be <= previous if open version.
    meta.file_length = 0;
    int64_t valid_slice_number = 0;
    for (int64_t idx = 0; idx < max_sliceid; idx++) {
      if (!slice_option.is_multi_version_slice()) {
        // not multi version container
        if (slice_id_flag[idx]) {
          ++valid_slice_number;
          meta.file_length += lengths[idx];
        } else {
          ret = OB_COS_ERROR;
          const int64_t sliceid = idx + 1;
          OB_LOG(WARN, "Corruption, slice id is not continous.", K(ret), K(bucket_name_string), K(container_name_string), K(sliceid));
          break;
        }
      } else {
        if (slice_id_flag[idx]) {
          if (versions[idx] == max_version) {
            if (valid_slice_number == idx) {
              ++valid_slice_number;
              meta.file_length += lengths[idx];
            } else {
              ret = OB_COS_ERROR;
              const int64_t sliceid = idx + 1;
              OB_LOG(WARN, "Corruption, slice id is not continous.", K(ret), K(bucket_name_string), K(container_name_string), K(sliceid), K(max_sliceid), K(valid_slice_number), K(max_version));
              break;
            }
          }
        }
      }
    }

    // output the result
    if (OB_SUCC(ret)) {
      for (int64_t idx = 0; idx < valid_slice_number; idx++) {
        ObString slice_name;
        ObString &tmp_slice_name = tmp_slice_names_array[slice_name_indexes[idx]];
        int64_t slice_length = lengths[idx];
        if (OB_FAIL(ob_write_string(allocator, tmp_slice_name, slice_name, true /*c_style*/))) {
          OB_LOG(WARN, "failed to allocate memory to save slice name", K(ret), K(bucket_name_string), K(container_name_string), K(tmp_slice_name), K(slice_length));
          break;
        } else if (OB_FAIL(slice_names.push_back(slice_name))) {
          OB_LOG(WARN, "failed to push back slice name", K(ret), K(bucket_name_string), K(container_name_string), K(tmp_slice_name), K(slice_length));
          break;
        } else if (OB_FAIL(slice_lengths.push_back(slice_length))) {
          OB_LOG(WARN, "failed to push back slice size", K(ret), K(bucket_name_string), K(container_name_string), K(tmp_slice_name), K(slice_length));
          break;
        } else {
          OB_LOG(DEBUG, "return one slice", K(bucket_name_string), K(container_name_string), K(tmp_slice_name), K(slice_length));
        }
      }
    }
  }

  meta.type = ObCosObjectType::COS_OBJECT_CONTAINER;

  return ret;
}

int ObCosContainer::head_meta(
  bool &is_exist,
  qcloud_cos::CosObjectMeta &meta)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObArray<ObString> slice_names_array;
  ObArray<int64_t> slice_lengths_array;
  if (OB_FAIL(head_meta(is_exist, meta, allocator, slice_names_array, slice_lengths_array))) {
    const ObString &bucket_name_string = this->bucket_name_string();
    const ObString &container_name_string = this->object_name_string();
    OB_LOG(WARN, "failed to head meta", K(ret), K(bucket_name_string), K(container_name_string));
  }

  return ret;
}

int ObCosContainer::del(int64_t &deleted_cnt)
{
  int ret = OB_SUCCESS;
  // buffer to hold the dir name
  char dir_name_buff[OB_MAX_URI_LENGTH];
  qcloud_cos::ObCosWrapper::Handle *h = NULL;

  if (OB_ISNULL(cos_base_)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "cos base is null", K(ret));
  } else if (OB_ISNULL(h = create_cos_handle())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "create cos handle failed", K(ret), K(bucket_name_string_), K(object_name_string_));
  } else if (OB_FAIL(databuff_printf(dir_name_buff, OB_MAX_URI_LENGTH, "%s/", object_name_string_.ptr()))) {
    OB_LOG(WARN, "failed to build current container dir name", K(ret), K(bucket_name_string_), K(object_name_string_));
  } else {
    qcloud_cos::CosStringBuffer dir_name(dir_name_buff, strlen(dir_name_buff) + 1); // include '\0'

    OB_LOG(DEBUG, "start to delete container", K(bucket_name_string_), K(object_name_string_));
    if (OB_FAIL(qcloud_cos::ObCosWrapper::del_objects_in_dir(h, bucket_name_, dir_name, deleted_cnt))) {
      OB_LOG(WARN, "failed to delete container from cos", K(ret), K(bucket_name_string_), K(object_name_string_), K(deleted_cnt));
    } else {
      OB_LOG(DEBUG, "succeed to delete container from cos", K(ret), K(bucket_name_string_), K(object_name_string_), K(deleted_cnt));
    }
  }

  destroy_cos_handle(h);
  return ret;
}


int ObCosContainer::list_slices(
  bool &is_exist,
  common::ObIAllocator &allocator,
  common::ObIArray <ObString> &slice_names_array,
  common::ObIArray <int64_t> &slice_lengths_array)
{
  int ret = OB_SUCCESS;

  const qcloud_cos::CosStringBuffer &bucket_name = this->bucket_name();
  const ObString &bucket_name_string = this->bucket_name_string();
  const ObString &container_name_string = this->object_name_string();

  const char seperator[] = "/";
  char container_param_buff[OB_MAX_URI_LENGTH];
  is_exist = true;
  qcloud_cos::ObCosWrapper::Handle *h = NULL;
  const int64_t ls_start_time = ObTimeUtility::current_time();
  if (OB_ISNULL(cos_base_)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "cos base is null!", K(ret), K(bucket_name_string), K(container_name_string));
  } else if (OB_ISNULL(h = create_cos_handle())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "fail to create cos handle! ", K(ret), K(bucket_name_string), K(container_name_string));
  } else if (OB_FAIL(databuff_printf(container_param_buff, OB_MAX_URI_LENGTH, "%s/", container_name_string.ptr()))) {
    OB_LOG(WARN, "failed to format container list param", K(ret), K(bucket_name_string), K(container_name_string), K(container_name_string.length()));
  } else {
    qcloud_cos::CosStringBuffer next_marker(seperator, sizeof(seperator));
    // list slices in container
    ObString container_param_buff_string(strlen(container_param_buff), strlen(container_param_buff), container_param_buff);
    qcloud_cos::CosStringBuffer container_param_required(container_param_buff, strlen(container_param_buff) + 1); // include '\0'
    // construct list callback arg
    CosContainerListArgs args(container_param_buff_string, allocator, slice_names_array, slice_lengths_array);

    // list slices
    if (OB_FAIL(qcloud_cos::ObCosWrapper::list_objects(h, bucket_name, container_param_required,
                                            next_marker, handle_each_slice_cb, (void *)(&args)))) {
      if (ret == OB_IO_LIMIT) {
        EVENT_INC(ObStatEventIds::COS_IO_LS_LIMIT_COUNT);
      }
      OB_LOG(WARN, "failed to list slices", K(ret), K(bucket_name_string), K(container_param_buff_string));
    } else if (slice_names_array.empty() || slice_lengths_array.empty()) {
      is_exist = false;
      OB_LOG(WARN, "container is empty", K(bucket_name_string), K(container_param_buff_string));
    } else {
      OB_LOG(DEBUG, "list objects succeed", K(bucket_name_string), K(container_param_buff_string));
    }
  }

  EVENT_INC(ObStatEventIds::COS_IO_LS_COUNT);
  EVENT_ADD(ObStatEventIds::COS_IO_LS_DELAY, ObTimeUtility::current_time() - ls_start_time);
  destroy_cos_handle(h);

  return ret;
}


int ObCosContainer::find_max_slice_option(
  const int64_t version,
  bool &is_exist,
  int64_t &container_size,
  ObCosSlice::Option &max_slice_option,
  int64_t &last_slice_size)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObArray<ObString> slice_names_array;
  ObArray<int64_t> slice_lengths_array;

  const ObString &bucket_name_string = this->bucket_name_string();
  const ObString &container_name_string = this->object_name_string();
  if (OB_FAIL(list_slices(is_exist, allocator, slice_names_array, slice_lengths_array))) {
    OB_LOG(WARN, "failed to list slices", K(ret), K(bucket_name_string), K(container_name_string));
  } else if (!is_exist){
    OB_LOG(DEBUG, "no slice in container", K(bucket_name_string), K(container_name_string));
  } else if (slice_names_array.empty() || slice_lengths_array.empty()) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "slice names or lengths is empty", K(ret), K(slice_names_array), K(slice_lengths_array));
  } else if (slice_names_array.count() != slice_lengths_array.count()) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "slice names/lengths array size not match", K(ret), K(slice_names_array), K(slice_lengths_array));
  } else {
    ObString &first_slice_name = slice_names_array.at(0);
    last_slice_size = slice_lengths_array.at(0);
    container_size = last_slice_size;
    if (OB_FAIL(ObCosSlice::parse_slice_name(first_slice_name, max_slice_option))) {
      OB_LOG(WARN, "parse first slice failed", K(ret), K(first_slice_name));
    }

    ObCosSlice::Option slice_option;
    for (int64_t i = 1; i < slice_names_array.count() && OB_SUCC(ret); i++) {
      ObString &slice_name = slice_names_array.at(i);
      int64_t &slice_size = slice_lengths_array.at(i);
      if (OB_FAIL(ObCosSlice::parse_slice_name(slice_name, slice_option))) {
        OB_LOG(WARN, "parse slice failed", K(ret), K(slice_name), K(i));
      } else if (slice_option.mask != max_slice_option.mask) {
        // mask does not match
        ret = OB_COS_ERROR;
        OB_LOG(WARN, "slice mask not match", K(ret), K(first_slice_name), K(slice_name));
      } else {
        if (slice_option.is_multi_version_slice()) {
          // version match, return max slice id with that version
          if (slice_option.version == version) {
            if (max_slice_option.version != version) {
              max_slice_option = slice_option;
              last_slice_size = slice_size;
              container_size = slice_size;
            } else if (slice_option.sliceid > max_slice_option.sliceid) {
              max_slice_option = slice_option;
              last_slice_size = slice_size;
              container_size += slice_size;
            } else {
              container_size += slice_size;
            }
          } else if (slice_option.version > max_slice_option.version) {
            if (max_slice_option.version != version) {
              max_slice_option = slice_option;
              last_slice_size = slice_size;
              container_size = slice_size;
            }
          } else if (slice_option.version == max_slice_option.version) {
            if (slice_option.sliceid > max_slice_option.sliceid) {
              max_slice_option = slice_option;
              last_slice_size = slice_size;
            }
            container_size += slice_size;
          }
        } else {
          if (slice_option.sliceid > max_slice_option.sliceid) {
            max_slice_option = slice_option;
            last_slice_size = slice_size;
          }
          container_size += slice_size;
        }
      }
    }
  }

  return ret;
}


/**
 * ------------------------------ObCosWritableContainer---------------------
 */
ObCosWritableContainer::ObCosWritableContainer()
  : is_opened_(false), length_(0), container_size_(0), last_slice_size_(0), container_(), overwrite_obj_(true)
{
  current_slice_id_ = ObCosContainer::get_start_slice_id();
}

ObCosWritableContainer::ObCosWritableContainer(const ObCosContainer::Option &option)
  : option_(option), is_opened_(false), length_(0), container_size_(0), last_slice_size_(0), container_(), overwrite_obj_(true)
{
  current_slice_id_ = ObCosContainer::get_start_slice_id();
}

int ObCosWritableContainer::open(const ObString &uri, void *cos_base)
{
  int ret = OB_SUCCESS;
  bool is_exist;
  ObCosSlice::Option max_slice_option;
  const int64_t start_time = ObTimeUtility::current_time();
  container_.cos_base_ = static_cast<ObCosBase*>(cos_base);
  if (is_opened()) {
    // cannot open an object twice
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "already open, cannot open again", K(ret), K(uri));
  } else if (OB_ISNULL(cos_base)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "the cos base is null", K(ret));
  } else if (OB_FAIL(container_.build_bucket_and_object_name(uri))) {
    OB_LOG(WARN, "failed to build object", K(ret), K(uri));
  } else if (OB_FAIL(ob_write_string(allocator_, uri, uri_, true /*c_style*/))) {
    OB_LOG(WARN, "failed to record uri", K(ret), K(uri));
  } else if (OB_FAIL(container_.find_max_slice_option(option_.version, is_exist, container_size_, max_slice_option, last_slice_size_))) {
    OB_LOG(WARN, "failed to find max slice option", K(ret), K(uri));
  } else {
    // open successfully no matter the object exist or not.
    if (!is_exist) {
      // this is a new container
      current_slice_id_ = ObCosContainer::get_start_slice_id();
      container_size_ = 0;
      last_slice_size_ = 0;
    } else {
      // the container has been created before
      // we need to ensure the available slice id
      if (max_slice_option.is_multi_version_slice()) {
        if (!option_.open_version) {
          ret = OB_COS_ERROR;
          OB_LOG(WARN, "unexpected error, version switch not match", K(ret), K(uri), K(option_), K(max_slice_option));
        } else if (max_slice_option.version != option_.version) {
          ret = OB_IO_ERROR;
          OB_LOG(WARN, "version not match", K(ret), K(uri), K(option_), K(max_slice_option));
        } else {
          current_slice_id_ = max_slice_option.sliceid;
          OB_LOG(DEBUG, "open container find a matching slice", K(ret), K(uri), K(max_slice_option));
        }
      } else {
        current_slice_id_ = max_slice_option.sliceid;
        OB_LOG(DEBUG, "open container from slice", K(ret), K(uri), K(max_slice_option));
      }
    }

    // open current slice
    if (OB_SUCC(ret) && OB_SUCC(open_current_slice())) {
      is_opened_ = true;
      length_ = 0;
      OB_LOG(DEBUG, "succeed to open container", K(ret), K(uri), K(current_slice_id_), K(option_));
    } else {
      OB_LOG(WARN, "failed to open container", K(ret), K(uri), K(current_slice_id_), K(option_));
    }
  }

  //print slow info
  bool is_slow = false;
  print_access_storage_log("cos slow request:ObCosWritableContainer::open ", uri, start_time, 1L, &is_slow);

  return ret;
}

int ObCosWritableContainer::write(const char *buf, const int64_t size)
{
  int ret = OB_SUCCESS;
  const int64_t start_time = ObTimeUtility::current_time();
  if (!is_opened()) {
    ret = OB_COS_ERROR;
    OB_LOG(WARN, "cos cannot do write before open the object");
  } else if (OB_FAIL(switch_to_next_slice(size))) {
    OB_LOG(WARN, "failed to switch to next slice", K(ret), K(uri_), K(current_slice_id_));
  } else if (OB_FAIL(overwrite_obj_.write(buf, size))) {
    OB_LOG(WARN, "failed to write data", K(ret),K(uri_), K(size));
  } else {
    length_ += size;
    container_size_ = container_size_ - last_slice_size_ + overwrite_obj_.get_length();
    last_slice_size_ = overwrite_obj_.get_object_size();
  }

  if (is_opened()) {
    //print slow info
    bool is_slow = false;
    print_access_storage_log("cos slow request:ObCosWritableContainer::write ", object_name_string(), start_time, size, &is_slow);
  }


  return ret;
}

int ObCosWritableContainer::pwrite(const char *buf, const int64_t size, const int64_t offset)
{
  int ret = OB_SUCCESS;

  const int64_t start_time = ObTimeUtility::current_time();
  // offset written on slice.
  int64_t written_offset;
  if (!is_opened()) {
    ret = OB_COS_ERROR;
    OB_LOG(WARN, "cos cannot do write before open the object");
  } else if (offset != container_size_) {
    ret = OB_IO_ERROR;
    OB_LOG(WARN, "container_size and offset do not match", K(ret), K_(container_size), K(offset), K(size));
  } else if (OB_FAIL(switch_to_next_slice(size))) {
    OB_LOG(WARN, "failed to switch to next slice", K(ret), K(uri_), K(current_slice_id_));
  } else if (FALSE_IT(written_offset = (offset - (container_size_ - last_slice_size_)))) {
  } else if (0 > written_offset) {
    ret = OB_IO_ERROR;
    OB_LOG(WARN, "container_size and offset do not match", K(ret), K_(uri), K_(container_size), K_(last_slice_size), K(offset), K(size));
  } else if (OB_FAIL(overwrite_obj_.pwrite(buf, size, written_offset))) {
    OB_LOG(WARN, "failed to pwrite data", K(ret), K_(uri), K_(container_size), K_(last_slice_size), K(offset), K(size), K(written_offset));
  } else {
    length_ += size;
    container_size_ += size;
    last_slice_size_ = overwrite_obj_.get_object_size();
  }

  if (is_opened()) {
    //print slow info
    bool is_slow = false;
    print_access_storage_log("cos slow request:ObCosWritableContainer::pwrite ", object_name_string(), start_time, size, &is_slow);
  }

  return ret;
}

int ObCosWritableContainer::close()
{
  int ret = OB_SUCCESS;
  if (!is_opened()) {
    ret = OB_COS_ERROR;
    OB_LOG(WARN, "cannot close before it is opened", K(ret));
  } else {
    // ignore error
    int tmp_ret = overwrite_obj_.close();
    if(OB_FAIL(tmp_ret)) {
      OB_LOG(WARN, "failed to close slice", K(tmp_ret), K(uri_), K(current_slice_id_));
    }
    allocator_.clear();
    uri_.reset();
    is_opened_ = false;
    length_ = -1;
  }

  return ret;
}

int ObCosWritableContainer::open_current_slice()
{
  int ret = OB_SUCCESS;

  // make slice uri
  char slice_name_buff[OB_MAX_URI_LENGTH];
  if (OB_FAIL(build_current_slice_name(slice_name_buff, OB_MAX_URI_LENGTH))) {
    OB_LOG(WARN, "failed to build current slice uri", K(ret), K(bucket_name_string()), K(object_name_string()));
  } else {
    ObString slice_name_string(slice_name_buff);
    if (OB_FAIL(overwrite_obj_.open(slice_name_string, container_.cos_base_))) {
      OB_LOG(WARN, "failed to open slice", K(ret), K(slice_name_string));
    } else {
      OB_LOG(DEBUG, "succeed to open slice", K(slice_name_string));
    }
  }

  return ret;
}

int ObCosWritableContainer::build_current_slice_name(char *slice_name_buff, int32_t buff_size)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(databuff_printf(slice_name_buff, buff_size, "%s/", uri_.ptr()))) {
    OB_LOG(WARN, "failed to build current slice name", K(ret), K(uri_), K(buff_size));
  } else {
    int64_t pos = strlen(slice_name_buff);
    ObCosSlice::Option option = {get_slice_mask(), current_slice_id_, option_.version};
    ObCosSlice slice(option);
    if (OB_FAIL(slice.build_slice_name(slice_name_buff + pos, buff_size - pos))) {
      OB_LOG(WARN, "failed to build slice name", K(ret), K(uri_), K(buff_size));
    }
  }

  return ret;
}

int ObCosWritableContainer::switch_to_next_slice(const int64_t size)
{
  // To switch to next slice, 3 following steps will be done:
  // 1. close prev writer;
  // 2. increase current slice id;
  // 3. open next slice;
  int ret = OB_SUCCESS;
  const int64_t tmp_slice_id = current_slice_id_;
  while (OB_SUCC(ret) && need_switch_next_slice(size)) {
    int64_t next_slice_id = ObCosContainer::generate_next_slice_id(current_slice_id_);
    if (next_slice_id > ObCosContainer::MAX_SLICE_ID) {
      ret = OB_IO_ERROR;
      OB_LOG(WARN, "slice id overflow", K(ret), K(next_slice_id), K(uri_));
    } else if (OB_FAIL(overwrite_obj_.close())) {
      // close current slice
      OB_LOG(WARN, "failed to close current slice", K(ret), K(current_slice_id_), K(uri_));
    } else {
      // open next slice
      current_slice_id_ = next_slice_id;
      last_slice_size_ = 0;
      if (OB_FAIL(open_current_slice())) {
        OB_LOG(WARN, "failed to open next slice", K(ret), K(current_slice_id_), K(uri_));
      } else {
        OB_LOG(DEBUG, "succeed to open next slice", K(ret), K(current_slice_id_), K(uri_));
      }
    }
  }

  if (current_slice_id_ - tmp_slice_id > 1) {
    OB_LOG(WARN, "slice id skipped", K(ret), K(current_slice_id_), K(tmp_slice_id), K(uri_));
  }

  return ret;
}

bool ObCosWritableContainer::need_switch_next_slice(const int64_t size) const
{
  UNUSED(size);
  return overwrite_obj_.get_object_size() > option_.threshold;
}

/**
 * ------------------------------ObCosRandomAccessContainer---------------------
 */
ObCosRandomAccessContainer::ObCosRandomAccessContainer()
  :container_(), is_opened_(false)
{
  meta_.reset();
}

int ObCosRandomAccessContainer::open(const ObString &uri, void *cos_base)
{
  int ret = OB_SUCCESS;
  bool is_exist;
  container_.cos_base_ = static_cast<ObCosBase*>(cos_base);
  const int64_t start_time = ObTimeUtility::current_time();

  if(uri.empty()) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "uri is empty", K(ret)                             );
  } else if (OB_ISNULL(cos_base)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "the cos base is null", K(ret));
  } else if (is_opened()) {
    // cannot open an object twice
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "already open, cannot open again", K(uri), K(ret));
  } else if (OB_FAIL(container_.build_bucket_and_object_name(uri))) {
    OB_LOG(WARN, "failed to build bucket and container name", K(ret), K(uri));
  } else if (OB_FAIL(ob_write_string(allocator_, uri, uri_, true /*c_style*/))) {
    OB_LOG(WARN, "failed to record uri", K(ret), K(uri));
  } else if (OB_FAIL(container_.head_meta(is_exist, meta_, allocator_, slice_names_array_, slice_lengths_array_))) {
    OB_LOG(WARN, "failed to head container meta", K(ret), K(uri));
  } else if (!is_exist) {
    ret = OB_BACKUP_FILE_NOT_EXIST;
    OB_LOG(WARN, "container not exist", K(ret), K(uri));
  } else {
    is_opened_ = true;
  }

  //print slow info
  bool is_slow = false;
  print_access_storage_log("cos slow request:ObCosRandomAccessContainer::open ", uri, start_time, 1L, &is_slow);

  return ret;
}

int ObCosRandomAccessContainer::pread(
  char *buf,
  const int64_t buf_size,
  int64_t offset,
  int64_t &read_size)
{
  int ret = OB_SUCCESS;

  const int64_t start_time = ObTimeUtility::current_time();
  if (!is_opened()) {
    ret = OB_COS_ERROR;
    OB_LOG(WARN, "cos cannot do read before open the object", K(ret));
  } else if (NULL == buf || buf_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "buf is NULL or buf_size is invalid", K(buf_size), K(ret));
  } else {
    int64_t size = 0;
    int64_t done_read_size = 0;
    int64_t to_read_size = 0;
    int64_t to_read_offset = 0;
    int64_t actual_read_size = 0;
    int64_t container_size = 0;

    for (int64_t slice_idx = 0; slice_idx < slice_names_array_.count(); slice_idx++) {
      const int64_t slice_size = slice_lengths_array_.at(slice_idx);
      container_size += slice_size;
      // skip current slice
      if (size + slice_size <= offset) {
        size += slice_size;
        continue;
      }

      // have read enough data
      if (done_read_size >= buf_size) {
        break;
      }

      // this is the slice which does first read from.
      if (offset >= size) {
        // adjust the read offset in the first slice
        to_read_offset = offset - size;
      } else {
        // subsequent slice need to be read at offset 0.
        to_read_offset = 0;
      }

      // read all from the offset
      to_read_size = slice_size - to_read_offset;
      if (buf_size - done_read_size < to_read_size) {
        // Just read we needed if this is the last slice we do read.
        to_read_size = buf_size - done_read_size;
      }

      if (OB_FAIL(pread_from_slice(slice_idx, buf + done_read_size, to_read_size, to_read_offset, actual_read_size))) {
        OB_LOG(WARN, "failed to read slice", K(ret), K(to_read_offset), K(to_read_size), K(done_read_size));
        break;
      }

      if (to_read_size != actual_read_size) {
        ret = OB_COS_ERROR;
        OB_LOG(WARN, "unexpected error", K(ret), K(to_read_offset), K(to_read_size), K(actual_read_size));
        break;
      }

      size += slice_size;
      done_read_size += to_read_size;
    }

    read_size = done_read_size;
    const ObString &container_name_string = container_.object_name_string();
    // offset overflow the container size
    if(offset >= container_size) {
      ret = OB_FILE_LENGTH_INVALID;
      OB_LOG(WARN, "read offset overflow", K(ret), K(container_name_string), K(offset), K(container_size), K(read_size));
    }

    //print slow info
    bool is_slow = false;
    print_access_storage_log("cos slow request:ObCosRandomAccessContainer::pread ", container_name_string, start_time, read_size, &is_slow);

  }

  return ret;
}

int ObCosRandomAccessContainer::close()
{
  int ret = OB_SUCCESS;
  if (!is_opened()) {
    ret = OB_COS_ERROR;
    OB_LOG(WARN, "cannot close before it is opened", K(ret));
  } else {
    allocator_.clear();
    uri_.reset();

    is_opened_ = false;
    meta_.reset();
  }
  return ret;
}

int ObCosRandomAccessContainer::build_slice_name(
  int64_t slice_idx,
  char *slice_name_buff,
  int32_t buff_size)
{
  int ret = OB_SUCCESS;

  const ObString &slice = slice_names_array_.at(slice_idx);
  if (OB_FAIL(databuff_printf(slice_name_buff, buff_size, "%s/%s", uri_.ptr(), slice.ptr()))) {
    OB_LOG(WARN, "build slice name failed", K(ret), K(uri_), K(slice));
  }

  return ret;
}

int ObCosRandomAccessContainer::pread_from_slice(
    int64_t slice_idx,
    char *buf,
    const int64_t buf_size,
    int64_t offset,
    int64_t &read_size)
{
  int ret = OB_SUCCESS;

  char slice_name_buff[OB_MAX_URI_LENGTH];
  if (OB_FAIL(build_slice_name(slice_idx, slice_name_buff, OB_MAX_URI_LENGTH))) {
    OB_LOG(WARN, "failed to build slice name,", K(ret), K(slice_idx));
  } else {
    ObCosRandomAccessObject random_access_obj;
    ObString slice_name_string(strlen(slice_name_buff), strlen(slice_name_buff), slice_name_buff);

    OB_LOG(DEBUG, "start read at slice", K(slice_name_string), K(offset), K(buf_size));
    if (OB_FAIL(random_access_obj.open(slice_name_string, container_.cos_base_))) {
      OB_LOG(WARN, "failed to open slice", K(ret), K(slice_name_string), K(slice_idx), K(offset));
    } else if (OB_FAIL(random_access_obj.pread(buf, buf_size, offset, read_size))){
      OB_LOG(WARN, "failed to read from slice", K(ret), K(slice_name_string), K(slice_idx), K(offset));
    } else if (OB_FAIL(random_access_obj.close())) {
      OB_LOG(WARN, "failed to close slice", K(ret), K(slice_name_string), K(slice_idx), K(offset));
    }

    OB_LOG(DEBUG, "end read at slice", K(ret), K(slice_name_string), K(offset), K(buf_size), K(read_size));
  }

  return ret;
}


/**
 * ------------------------------ObCosRandomAccessReader---------------------
 */
ObCosRandomAccessReader::ObCosRandomAccessReader()
  : random_access_container_(), random_access_object_(), reader_(NULL)
{

}

int ObCosRandomAccessReader::open(const ObString &uri, void *cos_base)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  bool is_file_exist = false;
  ObCosUtil util;
  qcloud_cos::CosObjectMeta meta;
  util.open(cos_base);

  // We need to decide which type the uri is, an object or container.
  if (is_opened()) {
    // cannot open an object twice
    ret = OB_COS_ERROR;
    OB_LOG(WARN, "already open, cannot open again", K(ret));
  } else if (OB_FAIL(util.get_file_meta(uri, is_file_exist, meta))) {
    OB_LOG(WARN, "failed to get file meta", K(ret), K(uri));
  } else if (!is_file_exist) {
    ret = OB_BACKUP_FILE_NOT_EXIST;
    OB_LOG(WARN, "file not exist", K(ret), K(uri));
  } else if (ObCosObjectType::COS_OBJECT_TYPE_MAX <= meta.type) {
    ret = OB_COS_ERROR;
    OB_LOG(WARN, "unknow object type", K(ret), K(uri), K(meta.type));
  } else {
    if (ObCosObjectType::COS_OBJECT_NORMAL == meta.type) {
      // uri is an object
      reader_ = &random_access_object_;
    } else {
      // uri is an container
      reader_ = &random_access_container_;
    }

    // do open now
    if (OB_FAIL(reader_->open(uri, cos_base))) {
      reader_ = NULL;
      OB_LOG(WARN, "failed to open file", K(ret), K(uri), K(meta.type));
    }
  }
  util.close();

  return ret;
}

int ObCosRandomAccessReader::pread(
  char *buf,
  const int64_t buf_size,
  int64_t offset,
  int64_t &read_size)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  if (!is_opened()) {
    ret = OB_COS_ERROR;
    OB_LOG(WARN, "cos cannot do read before open the object");
  } else if(OB_FAIL(reader_->pread(buf, buf_size, offset, read_size))) {
    OB_LOG(WARN, "failed to read from cos.", K(ret), K(buf_size), K(offset));
  }

  return ret;
}

int ObCosRandomAccessReader::close()
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  if (!is_opened()) {
    ret = OB_COS_ERROR;
    OB_LOG(WARN, "cannot close before it is opened", K(ret));
  } else if (OB_FAIL(reader_->close())) {
    OB_LOG(WARN, "failed to close cos reader.", K(ret));
  } else {
    reader_ = NULL;
  }

  return ret;
}


/**
 * ------------------------------ObCosAppender---------------------
 */
ObCosAppender::ObCosAppender()
  : writable_container_(), overwrite_obj_(true), writer_(NULL), obj_type_(ObCosObjectType::COS_OBJECT_TYPE_MAX)
{

}

int ObCosAppender::open(const ObString &uri, void *cos_base)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;

  if (OB_ISNULL(cos_base)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "cos handle is null", K(ret));
  } else if (is_opened()) {
    // cannot open an object twice
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "already open, cannot open again", K(ret));
  } else {
    if (ObCosObjectType::COS_OBJECT_TYPE_MAX > obj_type_) {
      if (ObCosObjectType::COS_OBJECT_NORMAL == obj_type_) {
        writer_ = &overwrite_obj_;
      } else {
        writer_ = &writable_container_;
      }

      // do open now
      if (OB_FAIL(writer_->open(uri, cos_base))) {
        writer_ = NULL;
        OB_LOG(WARN, "failed to open file", K(ret), K(uri), K(obj_type_));
      }
    } else {
      ret = OB_COS_ERROR;
      OB_LOG(WARN, "have not set object type", K(ret), K(uri), K(obj_type_));
    }
  }

  return ret;
}

int ObCosAppender::write(const char *buf, const int64_t size)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  if (!is_opened()) {
    ret = OB_COS_ERROR;
    OB_LOG(WARN, "cos cannot do write before open the object");
  } else if(OB_FAIL(writer_->write(buf, size))) {
    OB_LOG(WARN, "failed to write buffer.", K(ret), K(size));
  }

  return ret;
}

int ObCosAppender::pwrite(const char *buf, const int64_t size, const int64_t offset)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  if (!is_opened()) {
    ret = OB_COS_ERROR;
    OB_LOG(WARN, "cos cannot do write before open the object");
  } else if(OB_FAIL(writer_->pwrite(buf, size, offset))) {
    OB_LOG(WARN, "failed to write buffer.", K(ret), K(size), K(offset));
  }

  return ret;
}

int ObCosAppender::close()
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  if (!is_opened()) {
    ret = OB_COS_ERROR;
    OB_LOG(WARN, "cannot close before it is opened", K(ret));
  } else if (OB_FAIL(writer_->close())) {
    OB_LOG(WARN, "failed to close cos writer.", K(ret));
  } else {
    writer_ = NULL;
  }

  return ret;
}

}
}
