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

#include "ob_admin_test_io_device_executor.h"

using namespace oceanbase::share;
using namespace oceanbase::common;

namespace oceanbase
{
namespace tools
{

TestExecGuard::TestExecGuard(const char *title, int64_t &test_id, int &ret)
  : title_(title),
    test_id_(test_id),
    ret_(ret),
    start_time_us_(ObTimeUtility::current_time())
{
  if (OB_ISNULL(title_)) {
    title_ = "";
  }
  std::cout << LIGHT_GREEN << "[RUN TEST" << test_id << "] " << NONE_COLOR << title_ << std::endl;
}

TestExecGuard::~TestExecGuard()
{
  const int64_t cost_time_ms = (ObTimeUtility::current_time() - start_time_us_) / 1000;
  if (ret_ == OB_SUCCESS) {
    std::cout << LIGHT_GREEN << "[       OK] " << NONE_COLOR << title_ << " (" << cost_time_ms << " ms)" << std::endl;
  } else {
    std::cout << LIGHT_RED << "ERROR CODE: " << ret_ << ' ' << common::ob_error_name(ret_) << std::endl;
    std::cout << LIGHT_RED << "[ FAILED! ] " << NONE_COLOR << title_ << " (" << cost_time_ms << " ms)" << std::endl;
  }
  test_id_++;
}

TestObjectStorageInterfaceContext::TestObjectStorageInterfaceContext()
    : single_file_content_(nullptr),
      appendable_file_content_(nullptr),
      single_file_is_exist_(false),
      appendable_file_is_exist_(false)
{
  test_dir_path_[0] = '\0';
  single_file_path_[0] = '\0';
  appendable_file_path_[0] = '\0';
}

int ObAdminTestIODeviceExecutor::test_object_storage_interface_()
{
  int ret = OB_SUCCESS;
  TestExecGuard guard("test_object_storage_interface", test_id_, ret);
  TestObjectStorageInterfaceContext ctx;

  if (OB_FAIL(test_object_storage_interface_prepare_(ctx))) {
    STORAGE_LOG_FILTER(ERROR, "failed to prepare envionment for test object storage interface", K(ret));
  } else if (OB_FAIL(test_object_storage_interface_is_exist_(ctx))) {
    STORAGE_LOG_FILTER(ERROR, "failed to test object storage interface is exist", K(ret));
  } else if (OB_FAIL(test_object_storage_interface_list_file_(ctx))) {
    STORAGE_LOG_FILTER(ERROR, "failed to test object storage interface list file", K(ret));
  } else if (OB_FAIL(test_object_storage_interface_list_directories_(ctx))) {
    STORAGE_LOG_FILTER(ERROR, "failed to test object storage interface list directories", K(ret));
  } else if (OB_FAIL(test_object_storage_interface_get_file_length_(ctx))) {
    STORAGE_LOG_FILTER(ERROR, "failed to test object storage interface get file length", K(ret));
  } else if (OB_FAIL(test_object_storage_interface_read_(ctx))) {
    STORAGE_LOG_FILTER(ERROR, "failed to test object storage interface read", K(ret));
  } else if (OB_FAIL(test_object_storage_interface_del_(ctx))) {
    STORAGE_LOG_FILTER(ERROR, "failed to test object storage interface del", K(ret));
  }
  return ret;
}

int ObAdminTestIODeviceExecutor::test_object_storage_interface_prepare_(TestObjectStorageInterfaceContext &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ctx.storage_info_.set(backup_path_, storage_info_))) {
    STORAGE_LOG_FILTER(ERROR, "failed to set storage info", K(ret), K_(backup_path), K_(storage_info));
  } else if (OB_FAIL(databuff_printf(ctx.test_dir_path_, OB_MAX_URI_LENGTH, "%s/%s",
                     backup_path_, ctx.test_dir_name_))) {
    STORAGE_LOG_FILTER(ERROR, "failed to set test dir path", K(ret), K_(backup_path), K(ctx.test_dir_name_));
  } else if (OB_FAIL(databuff_printf(ctx.single_file_path_, OB_MAX_URI_LENGTH, "%s/%s",
                     ctx.test_dir_path_, ctx.single_file_name_))) {
    STORAGE_LOG_FILTER(ERROR, "failed to set single file path", K(ret), K(ctx.test_dir_path_), K(ctx.single_file_name_));
  } else if (OB_FAIL(generate_random_str_(ctx.single_file_content_, ctx.SINGLE_FILE_LENGTH))) {
    STORAGE_LOG_FILTER(ERROR, "failed to generate random str", K(ret), K(ctx.SINGLE_FILE_LENGTH));
  } else if (OB_FAIL(ctx.util_.mk_parent_dir(ctx.single_file_path_, &ctx.storage_info_))) {
    STORAGE_LOG_FILTER(ERROR, "failed to make parent dir for full clog file", K(ret), K(ctx.single_file_path_), K(ctx.storage_info_));
  } else if (OB_FAIL(databuff_printf(ctx.appendable_file_path_, OB_MAX_URI_LENGTH, "%s/%s",
                     ctx.test_dir_path_, ctx.appendable_file_name_))) {
    STORAGE_LOG_FILTER(ERROR, "failed to set appendable file path", K(ret), K(ctx.test_dir_path_), K(ctx.appendable_file_name_));
  } else if (OB_FAIL(generate_random_str_(ctx.appendable_file_content_, ctx.APPENDABLE_FILE_LENGTH))) {
    STORAGE_LOG_FILTER(ERROR, "failed to generate random str", K(ret), K(ctx.APPENDABLE_FILE_LENGTH));
  } else if (OB_FAIL(ctx.util_.write_single_file(ctx.single_file_path_, &ctx.storage_info_,
                     ctx.single_file_content_, ctx.SINGLE_FILE_LENGTH, ObStorageIdMod::get_default_id_mod()))) {
    STORAGE_LOG_FILTER(ERROR, "failed to write single file", K(ret), K(ctx.SINGLE_FILE_LENGTH), K(ctx.storage_info_));
  } else if (OB_FAIL(test_multi_step_write_appendable_file_(ctx))) {
    STORAGE_LOG_FILTER(ERROR, "failed to write appendable file",
        K(ret), K(ctx.appendable_file_path_), K(ctx.APPENDABLE_FILE_LENGTH), K(ctx.storage_info_));
  } else if (OB_FAIL(databuff_printf(ctx.upload_file_path_, OB_MAX_URI_LENGTH, "%s/%s",
                                     ctx.test_dir_path_, ctx.upload_file_name_))) {
    STORAGE_LOG_FILTER(ERROR, "failed to set upload file path", K(ret), K(ctx.test_dir_path_), K(ctx.upload_file_name_));
  } else if (OB_FAIL(generate_random_str_(ctx.upload_file_content_, ctx.UPLOAD_FILE_LENGTH))) {
    STORAGE_LOG_FILTER(ERROR, "failed to generate random str", K(ret), K(ctx.UPLOAD_FILE_LENGTH));
  } else if (OB_FAIL(test_object_storage_interface_async_upload_(ctx))) {
    STORAGE_LOG_FILTER(ERROR, "failed to exec async upload", K(ret), K(ctx.upload_file_path_), K(ctx.UPLOAD_FILE_LENGTH));
  }
  return ret;
}

int ObAdminTestIODeviceExecutor::test_object_storage_interface_async_upload_(TestObjectStorageInterfaceContext &ctx)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObStorageAccessType access_type = OB_STORAGE_ACCESS_DIRECT_MULTIPART_WRITER;
  ObIODevice *device_handle = nullptr;
  ObIOFd fd;
  ObIOHandle io_handle;

  if (OB_FAIL(ctx.util_.open_with_access_type(device_handle, fd, &ctx.storage_info_,
                                             ctx.upload_file_path_, access_type,
                                             ObStorageIdMod::get_default_id_mod()))) {
    STORAGE_LOG_FILTER(ERROR, "failed to open device with access type", K(ret), K(ctx.upload_file_path_), K(access_type), K(ctx.storage_info_));
  } else if (OB_FAIL(ctx.util_.async_upload_data(*device_handle, fd, ctx.upload_file_content_,
                                                 0, ctx.UPLOAD_FILE_LENGTH, io_handle))) {
    STORAGE_LOG_FILTER(ERROR, "failed to start async upload task!", K(ret), K(ctx.storage_info_),
        K(ctx.upload_file_path_), K(ctx.UPLOAD_FILE_LENGTH));
  } else if (OB_FAIL(io_handle.wait())) {
    STORAGE_LOG_FILTER(ERROR, "failed to wait async upload data finish", K(ret), K(ctx.storage_info_),
        K(ctx.upload_file_path_), K(ctx.UPLOAD_FILE_LENGTH));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ctx.util_.complete(*device_handle, fd))) {
      STORAGE_LOG_FILTER(ERROR, "failed to complete", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    if (OB_TMP_FAIL(ctx.util_.abort(*device_handle, fd))) {
      ret = COVER_SUCC(tmp_ret);
      STORAGE_LOG_FILTER(ERROR, "failed to abort", K(ret), K(tmp_ret));
    }
  }
  if (OB_TMP_FAIL(ctx.util_.close_device_and_fd(device_handle, fd))) {
    ret = COVER_SUCC(tmp_ret);
    STORAGE_LOG_FILTER(ERROR, "failed to close file and release device!", K(ret), K(ctx.appendable_file_path_), K(ctx.storage_info_));
  }

  return ret;
}

int ObAdminTestIODeviceExecutor::test_object_storage_interface_is_exist_(TestObjectStorageInterfaceContext &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ctx.util_.is_exist(ctx.single_file_path_, &ctx.storage_info_, ctx.single_file_is_exist_))) {
    STORAGE_LOG_FILTER(ERROR, "failed to check if single file is exist", K(ret), K(ctx.single_file_path_), K(ctx.storage_info_));
  } else if (OB_UNLIKELY(!ctx.single_file_is_exist_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG_FILTER(ERROR, "single file does not exist!", K(ret), K(ctx.single_file_path_), K(ctx.storage_info_));
  } else if (OB_FAIL(ctx.util_.is_exist(ctx.appendable_file_path_, &ctx.storage_info_, ctx.appendable_file_is_exist_))) {
    STORAGE_LOG_FILTER(ERROR, "failed to check if appendable file is exist",
        K(ret), K(ctx.appendable_file_path_), K(ctx.storage_info_));
  } else if (ctx.storage_info_.get_type() == ObStorageType::OB_STORAGE_S3
             && OB_UNLIKELY(ctx.appendable_file_is_exist_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG_FILTER(ERROR, "appendable file should not be detected when storage type is S3",
        K(ret), K(ctx.appendable_file_path_), K(ctx.storage_info_));
  } else if (ctx.storage_info_.get_type() != ObStorageType::OB_STORAGE_S3
             && OB_UNLIKELY(!ctx.appendable_file_is_exist_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG_FILTER(ERROR, "appendable file should be detected when storage type is not S3",
        K(ret), K(ctx.appendable_file_path_), K(ctx.storage_info_));
  } else if (FALSE_IT(ctx.single_file_is_exist_ = false)) {
  } else if (FALSE_IT(ctx.appendable_file_is_exist_ = false)) {
  } else if (OB_FAIL(ctx.util_.adaptively_is_exist(ctx.single_file_path_, &ctx.storage_info_, ctx.single_file_is_exist_))) {
    STORAGE_LOG_FILTER(ERROR, "failed to check if single file is exist",
        K(ret), K(ctx.single_file_path_), K(ctx.storage_info_));
  } else if (OB_UNLIKELY(!ctx.single_file_is_exist_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG_FILTER(ERROR, "single file does not exist!",
        K(ret), K(ctx.single_file_path_), K(ctx.storage_info_));
  } else if (OB_FAIL(ctx.util_.adaptively_is_exist(ctx.appendable_file_path_, &ctx.storage_info_,
                     ctx.appendable_file_is_exist_))) {
    STORAGE_LOG_FILTER(ERROR, "failed to check if appendable file is exist",
        K(ret), K(ctx.appendable_file_path_), K(ctx.storage_info_));
  } else if (OB_UNLIKELY(!ctx.appendable_file_is_exist_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG_FILTER(ERROR, "appendable file does not exist!", K(ret), K(ctx.appendable_file_path_), K(ctx.storage_info_));
  }

  return ret;
}

int ObAdminTestIODeviceExecutor::test_object_storage_interface_list_file_(TestObjectStorageInterfaceContext &ctx)
{
  int ret = OB_SUCCESS;
  ObArray<ObString> file_names;
  ObFileListArrayOp file_list_array_op(file_names, allocator_);

  if (OB_FAIL(ctx.util_.list_files(ctx.test_dir_path_, &ctx.storage_info_, file_list_array_op))) {
    STORAGE_LOG_FILTER(ERROR, "failed to list test dir path", K(ret), K(ctx.test_dir_path_), K(ctx.storage_info_));
  } else if (ctx.storage_info_.get_type() == ObStorageType::OB_STORAGE_S3
             && OB_UNLIKELY(file_names.size() != ctx.S3_FILE_COUNT)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG_FILTER(ERROR, "when the storage type is set to OB_STORAGE_S3, there should be 10 files present here",
        K(ret), K(ctx.test_dir_path_), K(file_names.size()), K(ctx.storage_info_));
  } else if (ctx.storage_info_.get_type() != ObStorageType::OB_STORAGE_S3
             && OB_UNLIKELY(file_names.size() != ctx.OTHER_FILE_COUNT)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG_FILTER(ERROR, "when the storage type is not set to OB_STORAGE_S3, there should be 3 files present here",
        K(ret), K(ctx.test_dir_path_), K(file_names.size()), K(ctx.storage_info_));
  } else if (FALSE_IT(file_names.reuse())) {
  } else if (OB_FAIL(ctx.util_.adaptively_list_files(ctx.test_dir_path_, &ctx.storage_info_, file_list_array_op))) {
    STORAGE_LOG_FILTER(ERROR, "failed to list test dir path", K(ret), K(ctx.test_dir_path_), K(ctx.storage_info_));
  } else if (OB_UNLIKELY(file_names.count() != ctx.OTHER_FILE_COUNT)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG_FILTER(ERROR, "the length of file_names does not equal to 2", K(ret), K(file_names.count()), K(ctx.storage_info_));
  }

  return ret;
}

int ObAdminTestIODeviceExecutor::test_object_storage_interface_list_directories_(TestObjectStorageInterfaceContext &ctx)
{
  int ret = OB_SUCCESS;
  ObArray<ObString> dir_names;
  ObFileListArrayOp dir_list_array_op(dir_names, allocator_);

  if (OB_FAIL(ctx.util_.list_directories(ctx.test_dir_path_, &ctx.storage_info_, dir_list_array_op))) {
    STORAGE_LOG_FILTER(ERROR, "failed to list directories", K(ret), K(ctx.test_dir_path_), K(ctx.storage_info_));
  } else if (ctx.storage_info_.get_type() == ObStorageType::OB_STORAGE_S3
             && (OB_UNLIKELY(dir_names.size() != 1) || OB_UNLIKELY(dir_names[0] != ObString(ctx.appendable_file_name_)))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG_FILTER(ERROR, "appendable dir not find when storage type is s3",
        K(ret), K(ctx.test_dir_path_), K(ctx.appendable_file_name_), K(dir_names.size()), K(ctx.storage_info_));
  } else if (ctx.storage_info_.get_type() != ObStorageType::OB_STORAGE_S3
             && OB_UNLIKELY(dir_names.size() != 0)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG_FILTER(ERROR, "length of the list directories result should be 0 when storage type is not s3",
        K(ret), K(ctx.test_dir_path_), K(ctx.appendable_file_name_), K(dir_names.size()), K(ctx.storage_info_));
  }

  return ret;
}

int ObAdminTestIODeviceExecutor::test_object_storage_interface_get_file_length_(TestObjectStorageInterfaceContext &ctx)
{
  int ret = OB_SUCCESS;
  int64_t single_file_length_query = 0;
  int64_t appendable_file_length_query = 0;

  // test get_file_length
  if (OB_FAIL(ctx.util_.get_file_length(ctx.single_file_path_, &ctx.storage_info_, single_file_length_query))) {
    STORAGE_LOG_FILTER(ERROR, "failed to get single file length", K(ret), K(ctx.single_file_path_), K(ctx.storage_info_));
  } else if (OB_UNLIKELY(single_file_length_query != ctx.SINGLE_FILE_LENGTH)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG_FILTER(ERROR, "get single file length not equal to real length",
        K(ret), K(ctx.single_file_path_), K(ctx.SINGLE_FILE_LENGTH), K(single_file_length_query), K(ctx.storage_info_));
  } else if (FALSE_IT(single_file_length_query = 0)) {
  } else if (ctx.storage_info_.get_type() == ObStorageType::OB_STORAGE_S3) {
    if (OB_SUCC(ctx.util_.get_file_length(ctx.appendable_file_path_, &ctx.storage_info_, appendable_file_length_query))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG_FILTER(ERROR, "when the storage type is set to OB_STORAGE_S3, the get_file_length interface should fail to be invoked.",
          K(ret), K(ctx.appendable_file_path_), K(ctx.storage_info_));
    } else if (OB_UNLIKELY(ret != OB_OBJECT_NOT_EXIST)) {
      STORAGE_LOG_FILTER(ERROR, "the get_file_length interface should return OB_OBJECT_NOT_EXIST when storage type is set to OB_STORAGE_S3",
          K(ret), K(ctx.appendable_file_path_), K(ctx.storage_info_));
    } else {
      ret = OB_SUCCESS;
    }
  } else {
    if (OB_FAIL(ctx.util_.get_file_length(ctx.appendable_file_path_, &ctx.storage_info_, appendable_file_length_query))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG_FILTER(ERROR, "failed to get file length when the storage type is not S3",
          K(ret), K(ctx.appendable_file_path_), K(ctx.storage_info_));
    } else if (OB_UNLIKELY(appendable_file_length_query != ctx.APPENDABLE_FILE_LENGTH)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG_FILTER(ERROR, "get appendable file length not equal to real length",
          K(ret), K(ctx.APPENDABLE_FILE_LENGTH), K(appendable_file_length_query));
    }
  }

  // test adaptively_get_file_length
  if (FAILEDx(ctx.util_.adaptively_get_file_length(ctx.single_file_path_, &ctx.storage_info_,
                     single_file_length_query))) {
    STORAGE_LOG_FILTER(ERROR, "failed to get single file length", K(ret), K(ctx.single_file_path_), K(ctx.storage_info_));
  } else if (OB_UNLIKELY(single_file_length_query != ctx.SINGLE_FILE_LENGTH)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG_FILTER(ERROR, "get single file length not equal to real length",
        K(ret), K(ctx.single_file_path_), K(ctx.SINGLE_FILE_LENGTH), K(single_file_length_query), K(ctx.storage_info_));
  } else if (OB_FAIL(ctx.util_.adaptively_get_file_length(ctx.appendable_file_path_, &ctx.storage_info_,
                     appendable_file_length_query))) {
    STORAGE_LOG_FILTER(ERROR, "failed to get appendable file length", K(ret), K(ctx.appendable_file_path_), K(ctx.storage_info_));
  } else if (OB_UNLIKELY(appendable_file_length_query != ctx.APPENDABLE_FILE_LENGTH)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG_FILTER(ERROR, "get appendable file length not equal to real length",
        K(ret), K(ctx.appendable_file_path_), K(ctx.APPENDABLE_FILE_LENGTH), K(appendable_file_length_query), K(ctx.storage_info_));
  }
  return ret;
}

int ObAdminTestIODeviceExecutor::test_object_storage_interface_read_(TestObjectStorageInterfaceContext &ctx)
{
  int ret = OB_SUCCESS;
  char *single_file_buf = nullptr;
  char *appendable_file_buf = nullptr;
  char *single_file_read_part_buf = nullptr;
  char *appendable_file_read_part_buf = nullptr;
  char *pread_buf = nullptr;
  char *async_pread_buf = nullptr;
  int64_t read_part_read_size = 0;
  int64_t pread_read_size = 0;
  // read_part_size randomly chosen from the range [MIN_READ_PART_SIZE, MAX_READ_PART_SIZE]
  // specifically from [1M, 2M]
  const int64_t MIN_READ_PART_SIZE = 1024 * 1024;
  const int64_t MAX_READ_PART_SIZE = 2 * 1024 * 1024;
  const int64_t read_part_size = ObRandom::rand(MIN_READ_PART_SIZE, MAX_READ_PART_SIZE);
  const int64_t async_pread_read_size = ObRandom::rand(MIN_READ_PART_SIZE, ctx.APPENDABLE_FILE_LENGTH);
  const int64_t single_file_read_part_offset = ObRandom::rand(0, ctx.SINGLE_FILE_LENGTH - read_part_size);
  const int64_t appendable_file_read_part_offset = ObRandom::rand(0, ctx.APPENDABLE_FILE_LENGTH - read_part_size);
  const int64_t async_pread_read_offset = ObRandom::rand(0, ctx.APPENDABLE_FILE_LENGTH - async_pread_read_size);
  const int64_t pread_offset = ObRandom::rand(0, ctx.SINGLE_FILE_LENGTH - read_part_size);

  ObIOFd fd;
  ObIODevice *device_handle = nullptr;
  ObStorageAccessType access_type = OB_STORAGE_ACCESS_ADAPTIVE_READER;
  ObIOHandle io_handle;

  // test read_single_text_file
  if (OB_ISNULL(single_file_buf = static_cast<char *>(allocator_.alloc(ctx.SINGLE_FILE_LENGTH + 1)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG_FILTER(ERROR, "failed to allocate buf", K(ret), K(ctx.single_file_path_), K(ctx.SINGLE_FILE_LENGTH), K(ctx.storage_info_));
  } else if (OB_FAIL(ctx.util_.read_single_text_file(ctx.single_file_path_, &ctx.storage_info_,
                     single_file_buf, ctx.SINGLE_FILE_LENGTH + 1, ObStorageIdMod::get_default_id_mod()))) {
    STORAGE_LOG_FILTER(ERROR, "failed to read single file text", K(ret), K(ctx.single_file_path_), K(ctx.SINGLE_FILE_LENGTH), K(ctx.storage_info_));
  } else if (OB_UNLIKELY(strlen(single_file_buf) != ctx.SINGLE_FILE_LENGTH)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG_FILTER(ERROR, "read single file length not equal to real length", K(ret), K(ctx.single_file_path_),
        K(ctx.SINGLE_FILE_LENGTH), K(strlen(appendable_file_buf)), K(ctx.storage_info_));
  }

  // test adaptively_read_single_text_file
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(appendable_file_buf = static_cast<char *>(allocator_.alloc(ctx.APPENDABLE_FILE_LENGTH + 1)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG_FILTER(ERROR, "failed to allocate buf", K(ret), K(ctx.appendable_file_path_), K(ctx.APPENDABLE_FILE_LENGTH), K(ctx.storage_info_));
  } else if (OB_FAIL(ctx.util_.adaptively_read_single_text_file(ctx.appendable_file_path_,
                     &ctx.storage_info_, appendable_file_buf, ctx.APPENDABLE_FILE_LENGTH + 1, ObStorageIdMod::get_default_id_mod()))) {
    STORAGE_LOG_FILTER(ERROR, "failed to read appendable text file",
        K(ret), K(ctx.appendable_file_path_), K(ctx.APPENDABLE_FILE_LENGTH + 1), K(ctx.storage_info_));
  } else if (OB_UNLIKELY(strlen(appendable_file_buf) != ctx.APPENDABLE_FILE_LENGTH)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG_FILTER(ERROR, "read appendable file length not equal to real length",
        K(ret), K(ctx.appendable_file_path_), K(ctx.APPENDABLE_FILE_LENGTH), K(strlen(appendable_file_buf)), K(ctx.storage_info_));
  }

  // test read_part_file
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(single_file_read_part_buf = static_cast<char *>(allocator_.alloc(read_part_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG_FILTER(ERROR, "failed to allocate buf", K(ret), K(ctx.single_file_path_), K(ctx.SINGLE_FILE_LENGTH), K(ctx.storage_info_));
  } else if (OB_FAIL(ctx.util_.read_part_file(ctx.single_file_path_, &ctx.storage_info_, single_file_read_part_buf,
                     read_part_size, single_file_read_part_offset, read_part_read_size, ObStorageIdMod::get_default_id_mod()))) {
    STORAGE_LOG_FILTER(ERROR, "failed to read part file",
        K(ret), K(ctx.single_file_path_), K(single_file_read_part_offset), K(read_part_size), K(ctx.storage_info_));
  } else if (OB_UNLIKELY(read_part_size != read_part_read_size)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG_FILTER(ERROR, "read size is inaccurate",
        K(ret), K(ctx.single_file_path_), K(single_file_read_part_offset), K(read_part_size), K(read_part_read_size), K(ctx.storage_info_));
  }

  // test adaptively_read_part_file
  read_part_read_size = 0;
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(appendable_file_read_part_buf = static_cast<char *>(allocator_.alloc(read_part_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG_FILTER(ERROR, "failed to allocate buf", K(ret), K(ctx.appendable_file_path_), K(read_part_size), K(ctx.storage_info_));
  } else if (OB_FAIL(ctx.util_.adaptively_read_part_file(ctx.appendable_file_path_, &ctx.storage_info_,
                     appendable_file_read_part_buf, read_part_size, appendable_file_read_part_offset, read_part_read_size, ObStorageIdMod::get_default_id_mod()))) {
    STORAGE_LOG_FILTER(ERROR, "failed to adaptively read part file",
        K(ret), K(ctx.appendable_file_path_), K(appendable_file_read_part_offset), K(read_part_size), K(ctx.storage_info_));
  } else if (OB_UNLIKELY(read_part_size != read_part_read_size)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG_FILTER(ERROR, "read size is inaccurate", K(ret), K(ctx.appendable_file_path_),
        K(appendable_file_read_part_offset), K(read_part_size), K(read_part_read_size), K(ctx.storage_info_));
  }

  // test pread
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(pread_buf = static_cast<char *>(allocator_.alloc(read_part_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG_FILTER(ERROR, "failed to allocate buf", K(ret), K(read_part_size), K(ctx.storage_info_));
  } else if (OB_FAIL(ctx.util_.pread(ctx.single_file_path_, &ctx.storage_info_,
                                     pread_buf, read_part_size, pread_offset,
                                     pread_read_size, ObStorageIdMod::get_default_id_mod()))) {
    STORAGE_LOG_FILTER(ERROR, "failed to pread single file", K(ret), K(ctx.storage_info_), K(ctx.single_file_path_), K(pread_offset), K(read_part_size));
  } else if (OB_UNLIKELY(pread_read_size != read_part_size)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG_FILTER(ERROR, "pread size is inaccurate", K(ret), K(ctx.storage_info_), K(ctx.single_file_path_), K(pread_offset), K(read_part_size));
  }

  // test async_pread
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(async_pread_buf = static_cast<char *>(allocator_.alloc(async_pread_read_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG_FILTER(ERROR, "failed to allocate buf", K(ret), K(async_pread_read_size));
    } else if (OB_FAIL(ctx.util_.open_with_access_type(device_handle, fd, &ctx.storage_info_,
            ctx.appendable_file_path_, access_type,
            ObStorageIdMod::get_default_id_mod()))) {
      STORAGE_LOG_FILTER(ERROR, "failed to open device with access type", K(ret), K(ctx.storage_info_), K(ctx.appendable_file_path_), K(access_type));
    } else if (OB_FAIL(ctx.util_.async_pread(*device_handle, fd, async_pread_buf,
            async_pread_read_offset, async_pread_read_size,
            io_handle))) {
      STORAGE_LOG_FILTER(ERROR, "failed to start async pread task!", K(ret), K(ctx.storage_info_),
          K(async_pread_read_offset),
          K(async_pread_read_size));
    } else if (OB_FAIL(io_handle.wait())) {
      STORAGE_LOG_FILTER(ERROR, "failed to wait async pread finish", K(ret), K(ctx.storage_info_),
          K(async_pread_read_offset),
          K(async_pread_read_size));
    } else if (OB_UNLIKELY(memcmp(ctx.appendable_file_content_ + async_pread_read_offset,
            async_pread_buf,
            async_pread_read_size) != 0)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG_FILTER(ERROR, "incorrect read from async pread", K(ret), K(ctx.storage_info_),
          K(async_pread_read_offset), K(async_pread_read_size));
    }

    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ctx.util_.close_device_and_fd(device_handle, fd))) {
      ret = COVER_SUCC(tmp_ret);
      STORAGE_LOG_FILTER(ERROR, "failed to close the file and release device!", K(ret));
    }
  }
  return ret;
}

int ObAdminTestIODeviceExecutor::test_object_storage_interface_del_(TestObjectStorageInterfaceContext &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ctx.util_.del_file(ctx.single_file_path_, &ctx.storage_info_))) {
    STORAGE_LOG_FILTER(ERROR, "failed to del single file", K(ret), K(ctx.single_file_path_), K(ctx.storage_info_));
  } else if (FALSE_IT(ctx.single_file_is_exist_ = true)) {
  } else if (OB_FAIL(ctx.util_.is_exist(ctx.single_file_path_, &ctx.storage_info_, ctx.single_file_is_exist_))) {
    STORAGE_LOG_FILTER(ERROR, "failed to check if single file is exist", K(ret), K(ctx.single_file_path_), K(ctx.storage_info_));
  } else if (OB_UNLIKELY(ctx.single_file_is_exist_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG_FILTER(ERROR, "failed to del single file", K(ret), K(ctx.single_file_path_), K(ctx.single_file_is_exist_), K(ctx.storage_info_));
  } else if (OB_FAIL(ctx.util_.adaptively_del_file(ctx.appendable_file_path_, &ctx.storage_info_))) {
    STORAGE_LOG_FILTER(ERROR, "failed to del appendable file path", K(ret), K(ctx.appendable_file_path_), K(ctx.storage_info_));
  } else if (FALSE_IT(ctx.appendable_file_is_exist_ = true)) {
  } else if (OB_FAIL(ctx.util_.adaptively_is_exist(ctx.appendable_file_path_, &ctx.storage_info_, ctx.appendable_file_is_exist_))) {
    STORAGE_LOG_FILTER(ERROR, "failed to check if appendable file is exist", K(ret), K(ctx.appendable_file_path_), K(ctx.storage_info_));
  } else if (OB_UNLIKELY(ctx.appendable_file_is_exist_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG_FILTER(ERROR, "failed to del appendable file", K(ret), K(ctx.appendable_file_path_), K(ctx.appendable_file_is_exist_), K(ctx.storage_info_));
  } else if (OB_FAIL(ctx.util_.del_dir(ctx.test_dir_path_, &ctx.storage_info_, true))) {
    STORAGE_LOG_FILTER(ERROR, "failed to del dir", K(ret), K(ctx.test_dir_path_), K(ctx.storage_info_));
  } else if (FALSE_IT(ctx.upload_file_is_exist_ = true)) {
  } else if (OB_FAIL(ctx.util_.adaptively_is_exist(ctx.upload_file_path_,
                                                  &ctx.storage_info_,
                                                  ctx.upload_file_is_exist_))) {
    STORAGE_LOG_FILTER(ERROR, "failed to check if upload file is exist", K(ret),
        K(ctx.upload_file_path_), K(ctx.storage_info_));
  } else if (OB_UNLIKELY(ctx.upload_file_is_exist_)) {
    ret = OB_ERR_UNDEFINED;
    STORAGE_LOG_FILTER(ERROR, "failed to del upload file", K(ret), K(ctx.upload_file_path_),
        K(ctx.storage_info_), K(ctx.upload_file_is_exist_));
  } else if (OB_FAIL(ctx.util_.del_dir(ctx.test_dir_path_, &ctx.storage_info_))) {
    STORAGE_LOG_FILTER(ERROR, "failed to del dir", K(ret), K(ctx.test_dir_path_), K(ctx.storage_info_));
  }
  return ret;
}

int ObAdminTestIODeviceExecutor::test_multi_step_write_appendable_file_(TestObjectStorageInterfaceContext &ctx)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObIOFd fd;
  ObIODevice *device_handle = nullptr;
  ObStorageAccessType access_type = OB_STORAGE_ACCESS_APPENDER;
  const int64_t STEP_COUNT = 5;
  const int64_t STEP_LEN = 2L * 1024 * 1024;
  int64_t offset = 0;
  int64_t writed_len = 0;
  int64_t remain_len = 0;
  int64_t single_write_size = 0;
  int64_t write_size = 0;

  ObArray<ObString> file_names;
  ObFileListArrayOp file_list_array_op(file_names, allocator_);
  ObArray<ObString> dir_names;
  ObFileListArrayOp dir_list_array_op(dir_names, allocator_);

  if (OB_ISNULL(ctx.appendable_file_path_)
      || OB_ISNULL(ctx.appendable_file_content_)
      || OB_UNLIKELY(ctx.APPENDABLE_FILE_LENGTH <= STEP_COUNT * STEP_LEN)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG_FILTER(ERROR, "arguments is invalid",
        K(ret), K(ctx.appendable_file_path_), K(ctx.appendable_file_content_), K(ctx.APPENDABLE_FILE_LENGTH), K(ctx.storage_info_));
  } else if (OB_FAIL(ctx.util_.open_with_access_type(device_handle, fd, &ctx.storage_info_, ctx.appendable_file_path_, access_type, ObStorageIdMod::get_default_id_mod()))) {
    STORAGE_LOG_FILTER(ERROR, "failed to open device with access type",
        K(ret), K(ctx.appendable_file_path_), K(access_type), K(ctx.storage_info_));
  } else if (OB_ISNULL(device_handle)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG_FILTER(ERROR, "device handle is null", K(ret), K(device_handle), K_(backup_path), K(ctx.storage_info_));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < STEP_COUNT; i++) {
      if (OB_FAIL(ctx.util_.pwrite(*device_handle, fd, ctx.appendable_file_content_ + offset, offset,
                                   STEP_LEN, single_write_size, false/*is_can_seal*/))) {
        STORAGE_LOG_FILTER(ERROR, "failed to pwrite", K(ret), K(offset), K(ctx.storage_info_));
      } else if (OB_UNLIKELY(single_write_size != STEP_LEN)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG_FILTER(ERROR, "write_size not equal to STEP_LEN", K(ret), K(single_write_size), K(STEP_LEN), K(ctx.storage_info_));
      } else {
        offset += single_write_size;
        writed_len += single_write_size;
      }
    }

    if (OB_SUCC(ret)) {
      // test list_files, adaptively_list_files and list_directories before seal
      if (OB_FAIL(ctx.util_.list_files(ctx.test_dir_path_, &ctx.storage_info_, file_list_array_op))) {
        STORAGE_LOG_FILTER(ERROR, "failed to list test dir path", K(ret), K(ctx.test_dir_path_), K(ctx.storage_info_));
      } else if (ctx.storage_info_.get_type() == ObStorageType::OB_STORAGE_S3
                 && OB_UNLIKELY(file_names.size() != 7)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG_FILTER(ERROR, "when the storage type is set to OB_STORAGE_S3, there should be 7 files present here",
            K(ret), K(ctx.test_dir_path_), K(file_names.size()), K(ctx.storage_info_));
      } else if (ctx.storage_info_.get_type() != ObStorageType::OB_STORAGE_S3
                 && OB_UNLIKELY(file_names.size() != 2)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG_FILTER(ERROR, "when the storage type is not set to OB_STORAGE_S3, there should be 2 files present here",
            K(ret), K(ctx.test_dir_path_), K(file_names.size()), K(ctx.storage_info_));
      } else if (FALSE_IT(file_names.reuse())){
      } else if (OB_FAIL(ctx.util_.adaptively_list_files(ctx.test_dir_path_, &ctx.storage_info_, file_list_array_op))) {
        STORAGE_LOG_FILTER(ERROR, "failed to list test dir path", K(ret), K(ctx.test_dir_path_), K(ctx.storage_info_));
      } else if (OB_UNLIKELY(file_names.size() != 2)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG_FILTER(ERROR, "the length of file_names does not equal to 2", K(ret), K(file_names.size()), K(ctx.storage_info_));
      } else if (OB_FAIL(ctx.util_.list_directories(ctx.test_dir_path_, &ctx.storage_info_, dir_list_array_op))) {
        STORAGE_LOG_FILTER(ERROR, "failed to list directories", K(ret), K(ctx.test_dir_path_), K(ctx.storage_info_));
      } else if (ctx.storage_info_.get_type() == ObStorageType::OB_STORAGE_S3
                 && OB_UNLIKELY(dir_names.size() != 1 || dir_names[0] != ObString(ctx.appendable_file_name_))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG_FILTER(ERROR, "appendable dir not find when storage type is s3",
            K(ret), K(ctx.test_dir_path_), K(ctx.appendable_file_path_), K(ctx.storage_info_));
      } else if (ctx.storage_info_.get_type() != ObStorageType::OB_STORAGE_S3
                 && OB_UNLIKELY(dir_names.size() != 0)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG_FILTER(ERROR, "length of the list directories result should be 0 when storage type is not s3",
            K(ret), K(ctx.test_dir_path_), K(ctx.appendable_file_path_), K(ctx.storage_info_));
      } else if (FALSE_IT(remain_len = ctx.APPENDABLE_FILE_LENGTH - writed_len)) {
      } else if (OB_UNLIKELY(remain_len <= 0)) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG_FILTER(ERROR, "remain_len must be greater than 0",
            K(ret), K(remain_len), K(ctx.APPENDABLE_FILE_LENGTH), K(writed_len), K(ctx.storage_info_));
      } else if (OB_FAIL(ctx.util_.pwrite(ctx.appendable_file_path_, &ctx.storage_info_,
                                          ctx.appendable_file_content_ + offset, offset, remain_len,
                                          access_type, write_size, true, ObStorageIdMod::get_default_id_mod()))) {
        STORAGE_LOG_FILTER(ERROR, "failed to pwrite", K(ret), K(offset), K(remain_len), K(ctx.storage_info_));
      } else if (OB_FAIL(device_handle->seal_file(fd))) {
        STORAGE_LOG_FILTER(ERROR, "failed to seal file", K(ret), K(ctx.appendable_file_path_), K(ctx.storage_info_));
      }
    }

    if (OB_TMP_FAIL(ctx.util_.close_device_and_fd(device_handle, fd))) {
      ret = COVER_SUCC(tmp_ret);
      STORAGE_LOG_FILTER(ERROR, "failed to close file and release device!", K(ret), K(ctx.appendable_file_path_), K(ctx.storage_info_));
    }
  }
  return ret;
}


int ObAdminTestIODeviceExecutor::test_list_before_complete_multipart_write_()
{
  int ret = OB_SUCCESS;
  TestExecGuard guard("test_list_before_complete_multipart_write", test_id_, ret);
  int tmp_ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  share::ObBackupStorageInfo storage_info;
  ObIOFd fd;
  ObIODevice *device_handle = nullptr;
  ObStorageAccessType access_type = OB_STORAGE_ACCESS_MULTIPART_WRITER;
  const char *file_name = "multipart_file_name.txt";
  char file_path[OB_MAX_URI_LENGTH];
  char *file_content = nullptr;
  const int64_t file_length = 32L * 1024 * 1024;
  int64_t write_size;

  ObArray<ObString> file_names;
  ObFileListArrayOp file_list_array_op(file_names, allocator_);

  if (OB_FAIL(storage_info.set(backup_path_, storage_info_))) {
    STORAGE_LOG_FILTER(ERROR, "failed to set storage info", K(ret), K_(backup_path), K_(storage_info));
  } else if (OB_FAIL(databuff_printf(file_path, OB_MAX_URI_LENGTH, "%s/%s", backup_path_, file_name))) {
    STORAGE_LOG_FILTER(ERROR, "failed to set file path", K(ret), K_(backup_path), K(file_name), K(storage_info));
  } else if (OB_FAIL(generate_random_str_(file_content, file_length))) {
    STORAGE_LOG_FILTER(ERROR, "failed to generate content for file", K(ret), K(file_length), K(storage_info));
  } else if (OB_FAIL(util.open_with_access_type(device_handle, fd, &storage_info,
          file_path, access_type, ObStorageIdMod::get_default_id_mod()))) {
    STORAGE_LOG_FILTER(ERROR, "failed to open device with access type", K(ret), K(file_path), K(access_type), K(storage_info));
  } else if (OB_ISNULL(device_handle)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG_FILTER(ERROR, "device handle is null", K(ret), K(device_handle), K_(backup_path), K(storage_info));
  } else if (OB_FAIL(device_handle->write(fd, file_content, file_length, write_size))) {
    STORAGE_LOG_FILTER(ERROR, "failed to write file", K(ret), K(file_length), K(storage_info));
  } else if (OB_FAIL(util.adaptively_list_files(backup_path_, &storage_info, file_list_array_op))) {
    STORAGE_LOG_FILTER(ERROR, "failed to list files in dir", K(ret), K_(backup_path), K(storage_info));
  } else if (OB_UNLIKELY(std::find(file_names.begin(), file_names.end(), file_name) != file_names.end())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG_FILTER(ERROR, "uncompleted multipart file are invisible!", K(ret), K(file_path), K(storage_info));
  } else if (OB_FAIL(device_handle->complete(fd))) {
    STORAGE_LOG_FILTER(ERROR, "failed to complete multipart upload", K(ret), K(device_handle), K(fd), K(storage_info));
  } else if (OB_FAIL(util.adaptively_list_files(backup_path_, &storage_info, file_list_array_op))) {
    STORAGE_LOG_FILTER(ERROR, "failed to list files in dir", K(ret), K_(backup_path), K(storage_info));
  } else if (OB_UNLIKELY(std::find(file_names.begin(), file_names.end(), file_name) == file_names.end())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG_FILTER(ERROR, "completed multipart file are visible!", K(ret), K(file_path), K(storage_info));
  } else if (OB_FAIL(util.del_file(file_path, &storage_info))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG_FILTER(ERROR, "failed to del file", K(ret), K(file_path), K(storage_info));
  }

  if (OB_FAIL(ret)) {
    if (OB_TMP_FAIL(device_handle->abort(fd))) {
      ret = COVER_SUCC(tmp_ret);
      STORAGE_LOG_FILTER(ERROR, "failed to abort multipart upload", K(ret), K(tmp_ret), K(device_handle), K(fd), K(storage_info));
    }
  }
  if (OB_TMP_FAIL(util.close_device_and_fd(device_handle, fd))) {
    ret = COVER_SUCC(tmp_ret);
    STORAGE_LOG_FILTER(ERROR, "failed to close device and fd", K(ret), K(tmp_ret), K(device_handle), K(fd), K(storage_info));
  }

  return ret;
}

} //namespace tools
} //namespace oceanbase
