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
#include "test_common_storage.h"
#include "test_common_storage_util.h"

namespace oceanbase {

using namespace oceanbase::common;

/*-------------------------------------ObTestStorageMeta--------------------------------------*/
int ObTestStorageMeta::build_config(const ObTestStorageType type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_type(type))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), K(type));
  } else {
    switch (type) {
      case ObTestStorageType::TEST_STORAGE_S3:
        build_s3_cfg();
        if (config_.is_valid() && OB_FAIL(init_s3_env())) {
          OB_LOG(WARN, "fail to init s3 env", K(ret));
        }
        break;
      case ObTestStorageType::TEST_STORAGE_OSS:
        build_oss_cfg();
        if (config_.is_valid() && OB_FAIL(init_oss_env())) {
          OB_LOG(WARN, "fail to init oss env", K(ret));
        }
        break;
      case ObTestStorageType::TEST_STORAGE_COS:
        build_cos_cfg();
        if (config_.is_valid() && OB_FAIL(init_cos_env())) {
          OB_LOG(WARN, "fail to init cos env", K(ret));
        }
        break;
      case ObTestStorageType::TEST_STORAGE_FS:
        build_fs_cfg();
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        break;
    }

    if (config_.is_valid()) {
      type_ = type;
    }
  }
  return ret;
}

bool ObTestStorageMeta::is_valid() const
{
  return is_valid_type(type_) && config_.is_valid();
}

bool ObTestStorageMeta::is_valid_type(const ObTestStorageType type) const
{
  return !(type == ObTestStorageType::TEST_STORAGE_INVALID ||
           type == ObTestStorageType::TEST_STORAGE_MAX);
}

void ObTestStorageMeta::build_s3_cfg()
{
  if (!config_.is_valid()) {
    if (0 != STRCMP(S3_BUCKET, INVALID_STR) &&
        0 != STRCMP(S3_REGION, INVALID_STR) &&
        0 != STRCMP(S3_ENDPOINT, INVALID_STR) &&
        0 != STRCMP(S3_AK, INVALID_STR) &&
        0 != STRCMP(S3_SK, INVALID_STR)) {
      config_.set_bucket(S3_BUCKET);
      config_.set_region(S3_REGION);
      config_.set_endpoint(S3_ENDPOINT);
      config_.set_ak(S3_AK);
      config_.set_sk(S3_SK);
      config_.is_valid_ = true;

      if (0 != STRCMP(S3_CHECKSUM_TYPE, INVALID_STR)) {
        config_.set_checksum_type(S3_CHECKSUM_TYPE);
      }
    } else {
      config_.is_valid_ = false;
    }
  }
}

void ObTestStorageMeta::build_oss_cfg()
{
  if (!config_.is_valid()) {
    if (0 != STRCMP(OSS_BUCKET, INVALID_STR) &&
        0 != STRCMP(OSS_ENDPOINT, INVALID_STR) &&
        0 != STRCMP(OSS_AK, INVALID_STR) &&
        0 != STRCMP(OSS_SK, INVALID_STR)) {
      config_.set_bucket(OSS_BUCKET);
      config_.set_endpoint(OSS_ENDPOINT);
      config_.set_ak(OSS_AK);
      config_.set_sk(OSS_SK);
      config_.is_valid_ = true;

      if (0 != STRCMP(OSS_CHECKSUM_TYPE, INVALID_STR)) {
        config_.set_checksum_type(OSS_CHECKSUM_TYPE);
      }
    } else {
      config_.is_valid_ = false;
    }
  }
}

void ObTestStorageMeta::build_cos_cfg()
{
  if (!config_.is_valid()) {
    if (0 != STRCMP(COS_BUCKET, INVALID_STR) &&
        0 != STRCMP(COS_ENDPOINT, INVALID_STR) &&
        0 != STRCMP(COS_AK, INVALID_STR) &&
        0 != STRCMP(COS_SK, INVALID_STR) &&
        0 != STRCMP(COS_APPID, INVALID_STR)) {
      config_.set_bucket(COS_BUCKET);
      config_.set_endpoint(COS_ENDPOINT);
      config_.set_ak(COS_AK);
      config_.set_sk(COS_SK);
      config_.set_appid(COS_APPID);
      config_.is_valid_ = true;

      if (0 != STRCMP(COS_CHECKSUM_TYPE, INVALID_STR)) {
        config_.set_checksum_type(COS_CHECKSUM_TYPE);
      }
    } else {
      config_.is_valid_ = false;
    }
  }
}

void ObTestStorageMeta::build_fs_cfg()
{
  if (0 == STRCMP(FS_PATH, INVALID_STR)) {
    config_.is_valid_ = false;
  } else {
    if (getcwd(config_.fs_path_, sizeof(config_.fs_path_)) != nullptr) {
      config_.is_valid_ = true;
    } else {
      config_.is_valid_ = false;
    }
  }
}

/*-------------------------------------TestStorageListOp--------------------------------------*/
class TestStorageListOp : public ObBaseDirEntryOperator
{
public:
  TestStorageListOp() {}
  virtual ~TestStorageListOp() { reset(); }
  int func(const dirent *entry) override;

  void reset() { item_names_.reset(); }

  ObArray<char *> item_names_;
private :
  ObArenaAllocator allocator_;
};

int TestStorageListOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  char *name_buf = (char *)allocator_.alloc(strlen(entry->d_name) + 1);
  if (OB_ISNULL(name_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN,"fail to alloc buf for item name", K(ret), "name", entry->d_name);
  } else {
    STRCPY(name_buf, entry->d_name);
    item_names_.push_back(name_buf);
  }
  return ret;
}

/*-------------------------------------TestCommonStorage--------------------------------------*/
class TestCommonStorage : public testing::Test
{
public:
  typedef hash::ObHashMap<uint8_t, ObTestStorageMeta> MetaMap;
  // for object_storage, it is the common prefix
  // for file system, it is the parent dir
  const char *default_dir = "TestCStorageDir";
  const char *default_put_obj = "cs_put";
  const char *default_append_obj = "cs_append.bak";
  const static int64_t default_round = 5;
  const char *default_multi_obj = "cs_multipart";
  const static int64_t default_size = 512 * 1024;
  const static int64_t part_size = 2 * 1024 * 1024;

public:
  TestCommonStorage();
  virtual ~TestCommonStorage();

  virtual void SetUp() {}
  virtual void TearDown() {}
  virtual void TestBody() {}

  void reset();
  int init();

  int mainly_check(const bool is_basic_situation, const bool clear_prev_data, const bool clear_cur_data); // Entrance of all

private:

  int check_basic_situation();
  int check_abnormal_situation();

  int common_prepare();

  /*
   * basic situation check
   */
  int basic_prepare();
  int check_basic_writer();
  int check_basic_appender();
  int check_basic_multipartwriter();
  int check_basic_reader(); // include reader and adaptive_reader
  int check_basic_util();

  /*
   * abnormal situation check
   */
  int abnormal_prepare();
  int check_abnormal_writer();
  int check_abnormal_appender();
  int check_abnormal_multipartwriter();
  int check_abnormal_reader(); // include reader and adaptive_reader
  int check_abnormal_util();

  //
  // some common func
  //
  int init_storage_info();
  int clear_all_data();
  void fin_obj_env(const ObTestStorageType type);
  int build_uri(char *uri_buf, const int64_t buf_len, const char *file_name);
  int delete_listed_objects(const common::ObString &uri);
  int create_fs_dir(const common::ObString &uri, const bool drop_if_exist = true);
  int is_exist(const common::ObString &uri, const bool is_adaptive, bool &is_exist);
  int remove_empty_dir(const common::ObString &uri);
  int force_delete_dir(const common::ObString &uri);
  int write_single_file(const common::ObString &uri, const int64_t length, const int64_t buf_len = -1);
  int write_single_file(const common::ObString &uri, const char *buf, const int64_t length);
  int write_multi_files(const common::ObString &uri, const int64_t length, const int64_t file_num,
                        const char *file_prefix, const int64_t start_idx = 0);
  int append_write(const common::ObString &uri, const int64_t offset, const int64_t length);
  int read_single_file(const common::ObString &uri, const int64_t offset, const int64_t length);
  int delete_single_file(const common::ObString &uri);
  int adaptive_read_file(const common::ObString &uri, const int64_t offset, const int64_t length);
  int get_file_length(const common::ObString &uri, const bool is_adaptive, int64_t &file_length);
  int list_directories(const common::ObString &uri, const bool is_adaptive, common::ObBaseDirEntryOperator &op);
  int list_files(const common::ObString &uri, const bool is_adaptive, common::ObBaseDirEntryOperator &op);
  int detect_storage_obj_meta(const common::ObString &uri, const bool is_adaptive, const bool need_fragment_meta,
                              ObStorageObjectMeta &obj_meta);

private:
  bool is_inited_;
  MetaMap meta_map_;
  ObTestStorageMeta *cur_meta_;
  ObTestStorageType cur_type_;
  ObObjectStorageInfo cur_storage_info_;

  bool need_clear_prev_data_;
  bool need_clear_cur_data_;
};

TestCommonStorage::TestCommonStorage()
  : meta_map_(), cur_meta_(nullptr), cur_type_(ObTestStorageType::TEST_STORAGE_INVALID),
    cur_storage_info_(), need_clear_prev_data_(false), need_clear_cur_data_(false)
{}

TestCommonStorage::~TestCommonStorage()
{}

void TestCommonStorage::reset()
{
  meta_map_.clear();
  cur_type_ = ObTestStorageType::TEST_STORAGE_INVALID;
  need_clear_prev_data_ = false;
  need_clear_cur_data_ = false;
}

int TestCommonStorage::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_storage_info())) {
    OB_LOG(WARN, "fail to init storage info", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int TestCommonStorage::init_storage_info()
{
  int ret = OB_SUCCESS;
  uint8_t type_start = static_cast<uint8_t>(ObTestStorageType::TEST_STORAGE_INVALID) + 1;
  uint8_t type_end = static_cast<uint8_t>(ObTestStorageType::TEST_STORAGE_MAX);
  if (OB_FAIL(meta_map_.create(type_end - 1, "TestObjMetaMap"))) {
    OB_LOG(WARN, "fail to create hashmap", K(ret), K(type_end));
  }

  for (uint8_t i = type_start; OB_SUCC(ret) && i < type_end; ++i) {
    ObTestStorageType type = static_cast<ObTestStorageType>(i);
    ObTestStorageMeta meta;
    if (OB_FAIL(meta.build_config(type))) {
      OB_LOG(WARN, "fail to build config", K(ret), K(type));
    } else if (meta.is_valid()) {
      if (OB_FAIL(meta_map_.set_refactored(i, meta))) {
        OB_LOG(WARN, "fail to set refactored", K(ret), K(type));
      }
    }
  }
  return ret;
}

void TestCommonStorage::fin_obj_env(const ObTestStorageType type)
{
  switch (type) {
    case ObTestStorageType::TEST_STORAGE_S3:
      fin_s3_env();
      break;
    case ObTestStorageType::TEST_STORAGE_OSS:
      fin_oss_env();
      break;
    case ObTestStorageType::TEST_STORAGE_COS:
      fin_cos_env();
      break;
    default:
      break;
  }
}

int TestCommonStorage::mainly_check(const bool is_basic_situation, const bool clear_prev_data, const bool clear_cur_data)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "not init", K(ret), K(is_basic_situation));
  } else {
    need_clear_prev_data_ = clear_prev_data;
    need_clear_cur_data_ = clear_cur_data;
  }

  for (MetaMap::iterator it = meta_map_.begin(); OB_SUCC(ret) && it != meta_map_.end(); ++it) {
    cur_storage_info_.reset();
    ObTestStorageMeta &meta = it->second;
    if (OB_UNLIKELY(!meta.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "meta should not be invalid", K(ret), K(meta));
    } else if (meta.is_obj_type() && OB_FAIL(TestCommonStorageUtil::build_object_storage_info(meta.config_.bucket_,
               meta.config_.endpoint_, meta.config_.ak_, meta.config_.sk_, meta.config_.region_, meta.config_.appid_,
               meta.config_.checksum_type_, cur_storage_info_))) {
      OB_LOG(WARN, "fail to build object storage info", K(ret), K(meta));
    } else if (meta.is_file_type() && OB_FAIL(TestCommonStorageUtil::build_fs_storage_info(cur_storage_info_))) {
      OB_LOG(WARN, "fail to build fs storage info", K(ret), K(meta));
    } else if (FALSE_IT(cur_meta_ = &meta)) {
    } else if (FALSE_IT(cur_type_ = meta.type_)) {
    } else {
      if (need_clear_prev_data_ && clear_all_data()) {
        OB_LOG(WARN, "fail to clear all data", K(ret), K_(cur_type));
      }

      if (OB_FAIL(ret)) {
      } else if (is_basic_situation) {
        if (OB_FAIL(check_basic_situation())) {
          OB_LOG(WARN, "fail to check basic situation", K(ret), K(meta));
        }
      } else {
        if (OB_FAIL(check_abnormal_situation())) {
          OB_LOG(WARN, "fail to check abnormal situation", K(ret), K(meta));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (need_clear_cur_data_ && clear_all_data()) {
        OB_LOG(WARN, "fail to clear all data", K(ret), K_(cur_type));
      }

      fin_obj_env(meta.type_);
    }

    uint8_t type = MIN(static_cast<uint8_t>(cur_type_), static_cast<uint8_t>(ObTestStorageType::TEST_STORAGE_MAX));
    const char *ret_info = (OB_SUCCESS == ret) ? "PASSED!" : "FAILED!";
    printf("[%s] finish %s test, %s\n", test_storage_type_str_arr[type], is_basic_situation ? "basic" : "abnormal", ret_info);
  }
  return ret;
}

int TestCommonStorage::check_basic_situation()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(basic_prepare())) {
    OB_LOG(WARN, "fail to basic_prepare", K(ret));
  } else if (OB_FAIL(check_basic_writer())) {
    OB_LOG(WARN, "fail to check basic writer", K(ret));
  } else if (OB_FAIL(check_basic_appender())) {
    OB_LOG(WARN, "fail to check basic appender", K(ret));
  } else if (OB_FAIL(check_basic_multipartwriter())) {
    OB_LOG(WARN, "fail to check basic multipartwriter", K(ret));
  } else if (OB_FAIL(check_basic_reader())) {
    OB_LOG(WARN, "fail to check basic reader", K(ret));
  } else if (OB_FAIL(check_basic_util())) {
    OB_LOG(WARN, "fail to check basic util", K(ret));
  }
  return ret;
}

int TestCommonStorage::check_abnormal_situation()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(abnormal_prepare())) {
    OB_LOG(WARN, "fail to abnormal_prepare", K(ret));
  } else if (OB_FAIL(check_abnormal_writer())) {
    OB_LOG(WARN, "fail to check abnormal writer", K(ret));
  } else if (OB_FAIL(check_abnormal_appender())) {
    OB_LOG(WARN, "fail to check abnormal appender", K(ret));
  } else if (OB_FAIL(check_abnormal_multipartwriter())) {
    OB_LOG(WARN, "fail to check abnormal multipartwriter", K(ret));
  } else if (OB_FAIL(check_abnormal_reader())) {
    OB_LOG(WARN, "fail to check abnormal reader", K(ret));
  } else if (OB_FAIL(check_abnormal_util())) {
    OB_LOG(WARN, "fail to check abnormal util", K(ret));
  }
  return ret;
}

int TestCommonStorage::common_prepare()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_meta_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "cur meta should not be null", K(ret));
  } else if (cur_meta_->is_file_type()) {
    char uri_buf[OB_MAX_URI_LENGTH] = { 0 };
    if (OB_FAIL(build_uri(uri_buf, OB_MAX_URI_LENGTH, nullptr))) {
      OB_LOG(WARN, "fail to build uri", K(ret));
    } else if (OB_FAIL(create_fs_dir(uri_buf, false/*drop_if_exist*/))) {
      OB_LOG(WARN, "fail to create fs dir", K(ret), K(uri_buf));
    }
  }
  return ret;
}

/*----------------------------------------- START --- BUILD --- DIFFERENT --- CASE -----------------------------------------*/
int TestCommonStorage::basic_prepare()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(common_prepare())) {
    OB_LOG(WARN, "fail to common prepare", K(ret));
  }
  return ret;
}

int TestCommonStorage::check_basic_writer()
{
  int ret = OB_SUCCESS;
  char uri_buf[OB_MAX_URI_LENGTH] = { 0 };
  // write "/parent_path/cs_put"
  if (OB_FAIL(build_uri(uri_buf, OB_MAX_URI_LENGTH, default_put_obj))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(write_single_file(uri_buf, default_size))) {
    OB_LOG(WARN, "fail to write single file", K(ret), K(uri_buf));
  }

  // mkdir "/parent_path/append_file1.bak/" and write "/parent_path/append_file1.bak/@APD_PART@FORMAT_META"
  if (FAILEDx(build_uri(uri_buf, OB_MAX_URI_LENGTH, "append_file1.bak/"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(create_fs_dir(uri_buf, true))) {
    OB_LOG(WARN, "fail to create fs dir", K(ret), K(uri_buf));
  } else if (OB_FAIL(build_uri(uri_buf, OB_MAX_URI_LENGTH, "append_file1.bak/@APD_PART@FORMAT_META"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(write_single_file(uri_buf, default_size))) {
    OB_LOG(WARN, "fail to write single file", K(ret), K(uri_buf));
  }
  // write "/parent_path/append_file1.bak/@APD_PART@0-100"
  if (FAILEDx(build_uri(uri_buf, OB_MAX_URI_LENGTH, "append_file1.bak/@APD_PART@0-100.bak"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(write_single_file(uri_buf, 100))) {
    OB_LOG(WARN, "fail to write single file", K(ret), K(uri_buf));
  }
  // write "/parent_path/append_file1.bak/@APD_PART@100-300"
  if (FAILEDx(build_uri(uri_buf, OB_MAX_URI_LENGTH, "append_file1.bak/@APD_PART@100-300.bak"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(write_single_file(uri_buf, 200))) {
    OB_LOG(WARN, "fail to write single file", K(ret), K(uri_buf));
  }
  // write "/parent_path/append_file1.bak/@APD_PART@200-500"
  if (FAILEDx(build_uri(uri_buf, OB_MAX_URI_LENGTH, "append_file1.bak/@APD_PART@200-500.bak"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(write_single_file(uri_buf, 300))) {
    OB_LOG(WARN, "fail to write single file", K(ret), K(uri_buf));
  }
  return ret;
}

int TestCommonStorage::check_basic_appender()
{
  int ret = OB_SUCCESS;
  char uri_buf[OB_MAX_URI_LENGTH] = { 0 };
  if (OB_FAIL(build_uri(uri_buf, OB_MAX_URI_LENGTH, default_append_obj))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else {
    const int64_t round = default_round;
    const int64_t each_append_size = 100;
    for (int64_t i = 0; OB_SUCC(ret) && i < round; ++i) {
      if (OB_FAIL(append_write(uri_buf, each_append_size * i, each_append_size))) {
        OB_LOG(WARN, "fail to append_write", K(ret), K(i));
      }
    }
  }

  return ret;
}

int TestCommonStorage::check_basic_multipartwriter()
{
  int ret = OB_SUCCESS;
  char uri_buf[OB_MAX_URI_LENGTH] = { 0 };
  if (OB_FAIL(build_uri(uri_buf, OB_MAX_URI_LENGTH, default_multi_obj))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else {
    ObStorageMultiPartWriter multiwriter;
    ObArenaAllocator allocator;
    char *buf = NULL;
    const int64_t buf_len = part_size;
    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "fail to alloc", K(ret), K(buf_len));
    } else {
      MEMSET(buf, 'a', buf_len);
      if (OB_FAIL(multiwriter.open(uri_buf, &cur_storage_info_))) {
        OB_LOG(WARN, "fail to open multipartwriter", K(ret), K(uri_buf));
      } else {
        const int64_t round = default_round;
        for (int64_t i = 0; OB_SUCC(ret) && i < round; ++i) {
          if (OB_FAIL(multiwriter.pwrite(buf, buf_len, buf_len * i))) {
            OB_LOG(WARN, "fail to pwrite", K(ret), K(i));
          }
        }

        if (OB_SUCC(ret) && OB_FAIL(multiwriter.complete())) {
          OB_LOG(WARN, "fail to complete", K(ret));
        }

        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(multiwriter.close())) {
          ret = (OB_SUCCESS != ret) ? ret : tmp_ret;
          OB_LOG(WARN, "fail to close multipartwriter", K(ret), K(tmp_ret), K(uri_buf));
        }
      }
    }
  }
  return ret;
}

int TestCommonStorage::check_basic_reader()
{
  int ret = OB_SUCCESS;
  char uri_buf[OB_MAX_URI_LENGTH] = { 0 };
  // read "/parent_path/cs_put"
  if (OB_FAIL(build_uri(uri_buf, OB_MAX_URI_LENGTH, default_put_obj))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(read_single_file(uri_buf, 0, default_size))) {
    OB_LOG(WARN, "fail to read single file", K(ret), K(uri_buf));
  }

  // read "/parent_path/append_file1.bak/@APD_PART@FORMAT_META"
  if (FAILEDx(build_uri(uri_buf, OB_MAX_URI_LENGTH, "append_file1.bak/@APD_PART@FORMAT_META"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(read_single_file(uri_buf, 0, default_size))) {
    OB_LOG(WARN, "fail to read single file", K(ret), K(uri_buf));
  }

  // read "/parent_path/cs_append.bak"
  if (FAILEDx(build_uri(uri_buf, OB_MAX_URI_LENGTH, default_append_obj))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(adaptive_read_file(uri_buf, 200/*offset*/, 100/*length*/))) {
    OB_LOG(WARN, "fail to adaptive read file", K(ret), K(uri_buf));
  }

  // read "/parent_path/cs_multipart"
  if (FAILEDx(build_uri(uri_buf, OB_MAX_URI_LENGTH, default_multi_obj))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(read_single_file(uri_buf, part_size * 2, part_size))) {
    OB_LOG(WARN, "fail to read single file", K(ret), K(uri_buf));
  }

  // adaptive read "/parent_path/append_file1.bak/"
  if (FAILEDx(build_uri(uri_buf, OB_MAX_URI_LENGTH, "append_file1.bak"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(adaptive_read_file(uri_buf, 100/*offset*/, 400))) {
    OB_LOG(WARN, "fail to adaptive read file", K(ret), K(uri_buf));
  }

  return ret;
}

int TestCommonStorage::check_basic_util()
{
  int ret = OB_SUCCESS;
  /*
   * Mainly to check these interfaces:
   *
   * mkdir、write_single_file、del_dir、is_tagging、is_exist、get_file_length、
   * list_appendable_file_fragments、del_file、detect_storage_obj_meta、
   * list_files、list_directories
   */
  char uri_buf[OB_MAX_URI_LENGTH] = { 0 };
  const int64_t buf_len = OB_MAX_URI_LENGTH;

  // create dir && delete dir && is_exist
  if (OB_FAIL(build_uri(uri_buf, buf_len, "tmp_dir/"))) {
    OB_LOG(WARN, "fail to build uri", K(ret), K(default_dir));
  } else if (OB_FAIL(create_fs_dir(uri_buf, true/*drop_if_exist*/))) {
    OB_LOG(WARN, "fail to create fs dir", K(ret), K(uri_buf));
  } else if (OB_FAIL(remove_empty_dir(uri_buf))) { // it will delete dir and check its exist
    OB_LOG(WARN, "fail to remove empty dir", K(ret), K(uri_buf));
  }

  // write_single_file && get_file_length && del_file
  int64_t file_length = -1;
  if (FAILEDx(build_uri(uri_buf, OB_MAX_URI_LENGTH, "tmp_write"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(write_single_file(uri_buf, default_size))) {
    OB_LOG(WARN, "fail to write single file", K(ret), K(uri_buf));
  } else if (OB_FAIL(get_file_length(uri_buf, false/*is_adaptive*/, file_length))) {
    OB_LOG(WARN, "fail to get file length", K(ret), K(uri_buf));
  } else if (OB_UNLIKELY(file_length != default_size)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "get file length is not same with original length", K(ret), K(file_length));
  } else if (OB_FAIL(delete_single_file(uri_buf))) {
    OB_LOG(WARN, "fail to delete single file", K(ret), K(uri_buf));
  }

  // list_directories:     ---tmp_dir_1
  //                             ---sub_dir1
  //                             ---sub_dir2
  TestStorageListOp list_op;
  if (FAILEDx(build_uri(uri_buf, buf_len, "tmp_dir_1/"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(create_fs_dir(uri_buf, true/*drop_if_exist*/))) {
    OB_LOG(WARN, "fail to create fs dir", K(ret), K(uri_buf));
  } else if (OB_FAIL(build_uri(uri_buf, buf_len, "tmp_dir_1/sub_dir1"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(create_fs_dir(uri_buf, true/*drop_if_exist*/))) {
    OB_LOG(WARN, "fail to create fs dir", K(ret), K(uri_buf));
  } else if (OB_FAIL(build_uri(uri_buf, buf_len, "tmp_dir_1/sub_dir2"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(create_fs_dir(uri_buf, true/*drop_if_exist*/))) {
    OB_LOG(WARN, "fail to create fs dir", K(ret), K(uri_buf));
  } else if (OB_FAIL(build_uri(uri_buf, buf_len, "tmp_dir_1/"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(list_directories(uri_buf, false/*is_adaptive*/, list_op))) {
    OB_LOG(WARN, "fail to list_directories", K(ret), K(uri_buf));
  } else if (OB_UNLIKELY(list_op.item_names_.size() != 2)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "list directories result is unexpected", "actual_item_size", list_op.item_names_.size());
  }

  // list_files:     ---tmp_dir_2
  //                       ---file1
  //                       ---file2
  //                       ---file3
  list_op.reset();
  if (FAILEDx(build_uri(uri_buf, buf_len, "tmp_dir_2/"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(create_fs_dir(uri_buf, true/*drop_if_exist*/))) {
    OB_LOG(WARN, "fail to create fs dir", K(ret), K(uri_buf));
  } else if (OB_FAIL(build_uri(uri_buf, buf_len, "tmp_dir_2/file1"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(write_single_file(uri_buf, default_size))) {
    OB_LOG(WARN, "fail to write single file", K(ret), K(uri_buf));
  } else if (OB_FAIL(build_uri(uri_buf, buf_len, "tmp_dir_2/file2"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(write_single_file(uri_buf, default_size))) {
    OB_LOG(WARN, "fail to write single file", K(ret), K(uri_buf));
  } else if (OB_FAIL(build_uri(uri_buf, buf_len, "tmp_dir_2/file3"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(write_single_file(uri_buf, default_size))) {
    OB_LOG(WARN, "fail to write single file", K(ret), K(uri_buf));
  } else if (OB_FAIL(build_uri(uri_buf, buf_len, "tmp_dir_2/"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(list_files(uri_buf, false/*is_adaptive*/, list_op))) {
    OB_LOG(WARN, "fail to list_files", K(ret), K(uri_buf));
  } else if (OB_UNLIKELY(list_op.item_names_.size() != 3)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "list files result is unexpected", "actual_item_size", list_op.item_names_.size());
  }

  // list 999 common items in one dir
  list_op.reset();
  if (OB_FAIL(build_uri(uri_buf, buf_len, "basic_tmp_list/"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(create_fs_dir(uri_buf, true/*drop_if_exist*/))) {
    OB_LOG(WARN, "fail to create fs dir", K(ret), K(uri_buf));
  } else if (OB_FAIL(write_multi_files(uri_buf, 100/*file_length*/, 999/*file_num*/, "basic_tmp_file", 0/*start_idx*/))) {
    OB_LOG(WARN, "fail to write multi files", K(ret), K(uri_buf));
  } else if (OB_FAIL(list_files(uri_buf, false/*is_adaptive*/, list_op))) {
    OB_LOG(WARN, "fail to list files", K(ret), K(uri_buf));
  } else if (OB_UNLIKELY(list_op.item_names_.size() != 999)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "listed result not match with original", K(ret), "listed_size", list_op.item_names_.size());
  }

  // list 1001 common items in one dir
  list_op.reset();
  if (FAILEDx(write_multi_files(uri_buf, 100/*file_length*/, 2/*file_num*/, "basic_tmp_file", 999/*start_idx*/))) {
    OB_LOG(WARN, "fail to write multi files", K(ret), K(uri_buf));
  } else if (OB_FAIL(list_files(uri_buf, false/*is_adaptive*/, list_op))) {
    OB_LOG(WARN, "fail to list files", K(ret), K(uri_buf));
  } else if (OB_UNLIKELY(list_op.item_names_.size() != 1001)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "listed result not match with original", K(ret), "listed_size", list_op.item_names_.size());
  }

  // detect_storage_obj_meta
  // list_files:     ---tmp_dir_3
  //                       ---file1
  //                       ---append_file2.bak
  //                             ---@APD_PART@FORMAT_META
  //                             ---@APD_PART@0-100
  //                             ---@APD_PART@100-400
  list_op.reset();
  ObStorageObjectMeta storage_meta;
  if (FAILEDx(build_uri(uri_buf, buf_len, "tmp_dir_3/"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(create_fs_dir(uri_buf, true/*drop_if_exist*/))) {
    OB_LOG(WARN, "fail to create fs dir", K(ret), K(uri_buf));
  } else if (OB_FAIL(build_uri(uri_buf, buf_len, "tmp_dir_3/file1"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(write_single_file(uri_buf, default_size))) {
    OB_LOG(WARN, "fail to write single file", K(ret), K(uri_buf));
  } else if (OB_FAIL(build_uri(uri_buf, buf_len, "tmp_dir_3/append_file2.bak"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(create_fs_dir(uri_buf, true/*drop_if_exist*/))) {
    OB_LOG(WARN, "fail to create fs dir", K(ret), K(uri_buf));
  } else if (OB_FAIL(build_uri(uri_buf, buf_len, "tmp_dir_3/append_file2.bak/@APD_PART@FORMAT_META"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(write_single_file(uri_buf, default_size))) {
    OB_LOG(WARN, "fail to write single file", K(ret), K(uri_buf));
  } else if (OB_FAIL(build_uri(uri_buf, buf_len, "tmp_dir_3/append_file2.bak/@APD_PART@0-100.bak"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(write_single_file(uri_buf, 100/*file_length*/))) {
    OB_LOG(WARN, "fail to write single file", K(ret), K(uri_buf));
  } else if (OB_FAIL(build_uri(uri_buf, buf_len, "tmp_dir_3/append_file2.bak/@APD_PART@100-400.bak"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(write_single_file(uri_buf, 300/*file_length*/))) {
    OB_LOG(WARN, "fail to write single file", K(ret), K(uri_buf));
  } else if (OB_FAIL(build_uri(uri_buf, buf_len, "tmp_dir_3/"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(list_files(uri_buf, true/*is_adaptive*/, list_op))) {
    OB_LOG(WARN, "fail to list_files", K(ret), K(uri_buf));
  } else if (OB_UNLIKELY(list_op.item_names_.size() != 2)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "adaptive list files result is unexpected", "actual_item_size", list_op.item_names_.size());
  } else if (OB_FAIL(build_uri(uri_buf, buf_len, "tmp_dir_3/append_file2.bak"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(detect_storage_obj_meta(uri_buf, true/*is_adaptive*/, true/*need_fragment*/, storage_meta))) {
    OB_LOG(WARN, "fail to detect_storage_obj_meta", K(ret), K(uri_buf));
  } else if (OB_UNLIKELY(!storage_meta.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "appendable storage meta should be valid", K(ret), K(uri_buf), K(storage_meta));
  } else if (OB_UNLIKELY(storage_meta.fragment_metas_.count() != 2 || storage_meta.length_ != 400)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "appendable storage meta should be valid", K(ret), K(uri_buf), K(storage_meta));
  }

  return ret;
}

int TestCommonStorage::abnormal_prepare()
{
  int ret = OB_SUCCESS;
  char uri_buf[OB_MAX_URI_LENGTH] = { 0 };
  const int64_t buf_len = OB_MAX_URI_LENGTH;
  if (OB_FAIL(common_prepare())) {
    OB_LOG(WARN, "fail to common prepare", K(ret));
  }

  // 1. build a appendable file which not have FORMAT_META
  if (FAILEDx(build_uri(uri_buf, buf_len, "ab_tmp_append_file.bak"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(create_fs_dir(uri_buf, true/*drop_if_exist*/))) {
    OB_LOG(WARN, "fail to create fs dir", K(ret), K(uri_buf));
  } else if (OB_FAIL(build_uri(uri_buf, buf_len, "ab_tmp_append_file.bak/@APD_PART@0-100.bak"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(write_single_file(uri_buf, 100))) {
    OB_LOG(WARN, "fail to write single file", K(ret), K(uri_buf));
  } else if (OB_FAIL(build_uri(uri_buf, buf_len, "ab_tmp_append_file.bak/@APD_PART@100-200.bak"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(write_single_file(uri_buf, 100))) {
    OB_LOG(WARN, "fail to write single file", K(ret), K(uri_buf));
  }

  // 2. build a appendable file which content not continuous
  if (FAILEDx(build_uri(uri_buf, buf_len, "ab_tmp_append_file_nc.bak"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(create_fs_dir(uri_buf, true/*drop_if_exist*/))) {
    OB_LOG(WARN, "fail to create fs dir", K(ret), K(uri_buf));
  } else if (OB_FAIL(build_uri(uri_buf, buf_len, "ab_tmp_append_file_nc.bak/@APD_PART@FORMAT_META"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(write_single_file(uri_buf, 100))) {
    OB_LOG(WARN, "fail to write single file", K(ret), K(uri_buf));
  } else if (OB_FAIL(build_uri(uri_buf, buf_len, "ab_tmp_append_file_nc.bak/@APD_PART@0-100.bak"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(write_single_file(uri_buf, 100))) {
    OB_LOG(WARN, "fail to write single file", K(ret), K(uri_buf));
  } else if (OB_FAIL(build_uri(uri_buf, buf_len, "ab_tmp_append_file_nc.bak/@APD_PART@200-300.bak"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(write_single_file(uri_buf, 100))) {
    OB_LOG(WARN, "fail to write single file", K(ret), K(uri_buf));
  }
  return ret;
}

int TestCommonStorage::check_abnormal_writer()
{
  int ret = OB_SUCCESS;
  char uri_buf[OB_MAX_URI_LENGTH] = { 0 };
  // 1. write_content buf length < param_length, the final length is param_length
  // [Expect]: succ to write, and the file length will be param_length
  // for the file content, there will exist some unknown content(param_length-buf_length).
  int64_t actual_len = 0;
  if (OB_FAIL(build_uri(uri_buf, OB_MAX_URI_LENGTH, "ab_tmp_w_file1"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(write_single_file(uri_buf, 120/*param_length*/, 100/*buf_length*/))) {
    OB_LOG(WARN, "fail to write single file", K(ret), K(uri_buf));
  } else if (OB_FAIL(get_file_length(uri_buf, false/*is_adaptive*/, actual_len))) {
    OB_LOG(WARN, "fail to get file length", K(ret), K(uri_buf));
  } else if (OB_UNLIKELY(actual_len != 120)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "actual write length is unexpected", K(ret), K(actual_len), K(uri_buf));
  }

  // 2. write_content buf length > param_length
  // [Expect]: succ to write, and file length will be param_length
  actual_len = 0;
  if (FAILEDx(build_uri(uri_buf, OB_MAX_URI_LENGTH, "ab_tmp_w_file2"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(write_single_file(uri_buf, 40/*param_length*/, 60/*buf_length*/))) {
    OB_LOG(WARN, "fail to write single file", K(ret), K(uri_buf));
  } else if (OB_FAIL(get_file_length(uri_buf, false/*is_adaptive*/, actual_len))) {
    OB_LOG(WARN, "fail to get file length", K(ret), K(uri_buf));
  } else if (OB_UNLIKELY(actual_len != 40)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "actual write length is unexpected", K(ret), K(actual_len), K(uri_buf));
  }

  return ret;
}

int TestCommonStorage::check_abnormal_appender()
{
  int ret = OB_SUCCESS;
  char uri_buf[OB_MAX_URI_LENGTH] = { 0 };
  // 1. when first append, the offset not start with 0
  // [Expect]: for nfs and s3, succ; for oss and cos, fail
  if (OB_FAIL(build_uri(uri_buf, OB_MAX_URI_LENGTH, "ab_tmp_append_file_1.bak"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else {
    int tmp_ret = append_write(uri_buf, 100/*offset*/, 200/*length*/);
    if (cur_type_ == ObTestStorageType::TEST_STORAGE_S3 ||
        cur_type_ == ObTestStorageType::TEST_STORAGE_FS) {
      // nfs can succ, but the file length is 200, cuz nfs open file with O_APPEND
      // s3 can succ, but there won't exist FORMAT_META
      if (tmp_ret != OB_SUCCESS) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "s3 and fs can append succ if offset not start with 0", K(ret), K_(cur_type));
      }
    } else {
      // cos and oss must append with start_offset=0
      if (tmp_ret == OB_SUCCESS) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "cos and oss should append fail if offset not start with 0", K(ret), K_(cur_type));
      }
    }
  }

  // 2. append offset not continuous
  if (FAILEDx(build_uri(uri_buf, OB_MAX_URI_LENGTH, "ab_tmp_append_file_2.bak"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(append_write(uri_buf, 0/*offset*/, 100/*length*/))) {
    OB_LOG(WARN, "fail to append write", K(ret), K(uri_buf));
  } else {
    int tmp_ret = append_write(uri_buf, 200/*offset*/, 100/*length*/);
    if (cur_type_ == ObTestStorageType::TEST_STORAGE_S3 ||
        cur_type_ == ObTestStorageType::TEST_STORAGE_FS) {
      // nfs can succ, but the file length is 200, cuz nfs open file with O_APPEND
      // s3 can succ, but there won't exist FORMAT_META
      if (tmp_ret != OB_SUCCESS) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "s3 and fs can append succ if offset not continuous", K(ret), K_(cur_type));
      }
    } else {
      // cos and oss must append with continuous offset
      if (tmp_ret == OB_SUCCESS) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "cos and oss should append fail if offset not continuous", K(ret), K_(cur_type));
      }
    }
  }
  return ret;
}

int TestCommonStorage::check_abnormal_multipartwriter()
{
  int ret = OB_SUCCESS;
  return ret;
}

int TestCommonStorage::check_abnormal_reader()
{
  int ret = OB_SUCCESS;
  char uri_buf[OB_MAX_URI_LENGTH] = { 0 };
  // 1. use adaptive_reader to read a normal file
  // [Expect]: succ
  if (OB_FAIL(build_uri(uri_buf, OB_MAX_URI_LENGTH, "ab_tmp_w_file1"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(adaptive_read_file(uri_buf, 0/*offset*/, 100/*length*/))) {
    OB_LOG(WARN, "fail to adaptive read normal file", K(ret), K(uri_buf));
  }

  // 2. use adaptive_reader to read a appendable file which doesn't have FORMAT_META
  // [Expect]: return OB_BACKUP_FILE_NOT_EXIST
  if (FAILEDx(build_uri(uri_buf, OB_MAX_URI_LENGTH, "ab_tmp_append_file.bak"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(adaptive_read_file(uri_buf, 0/*offset*/, 100/*length*/))) {
    if (OB_BACKUP_FILE_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      OB_LOG(WARN, "fail to adaptive read file", K(ret), K(uri_buf));
    }
  }

  // 3. use adaptive_reader to read a appendable file which doesn't have continuous
  // [Expect]: return error
  if (FAILEDx(build_uri(uri_buf, OB_MAX_URI_LENGTH, "ab_tmp_append_file_nc.bak"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(adaptive_read_file(uri_buf, 100/*offset*/, 200/*length*/))) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int TestCommonStorage::check_abnormal_util()
{
  int ret = OB_SUCCESS;

  char uri_buf[OB_MAX_URI_LENGTH] = { 0 };
  const int64_t buf_len = OB_MAX_URI_LENGTH;

  // 1. create a file and then create a same name dir
  // [Expect]: return OB_FILE_ALREADY_EXIST cuz already exist a same name file
  bool exist = false;
  if (FAILEDx(build_uri(uri_buf, buf_len, "ab_tmp_f1"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(write_single_file(uri_buf, default_size))) {
    OB_LOG(WARN, "fail to write single file", K(ret), K(uri_buf));
  } else if (OB_FAIL(build_uri(uri_buf, buf_len, "ab_tmp_f1/"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(is_exist(uri_buf, false/*is_adaptive*/, exist))) {
    OB_LOG(WARN, "fail to check is_exist", K(ret), K(uri_buf));
  } else if (OB_UNLIKELY(exist)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "this dir should not be existed, cuz it exists as a file", K(ret), K(uri_buf));
  } else if (OB_FAIL(create_fs_dir(uri_buf, true/*drop_if_exist*/))) {
    if (OB_FILE_ALREADY_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      OB_LOG(WARN, "fail to create fs dir", K(ret), K(uri_buf));
    }
  }

  // 2. create a dir whose parent dir not exist
  // [Expect]: succ to create 'ab_tmp_dir1/' and 'ab_tmp_dir1/sub_dir1/'
  if (FAILEDx(build_uri(uri_buf, buf_len, "ab_tmp_dir1/sub_dir1/"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(create_fs_dir(uri_buf, true/*drop_if_exist*/))) {
    OB_LOG(WARN, "fail to create fs dir", K(ret), K(uri_buf));
  }

  // 3. get file length of appendable file which doesn't have FORMAT_META
  // [Expect]: return OB_BACKUP_FILE_NOT_EXIST
  int64_t file_len = 0;
  if (FAILEDx(build_uri(uri_buf, buf_len, "ab_tmp_append_file.bak"))) {
    OB_LOG(WARN, "fail to build uri", K(ret));
  } else if (OB_FAIL(get_file_length(uri_buf, true/*is_adaptive*/, file_len))) {
    if (OB_BACKUP_FILE_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      OB_LOG(WARN, "fail to get file length", K(ret));
    }
  }

  return ret;
}

/*----------------------------------------- COMMON --- FUNC -----------------------------------------*/
int TestCommonStorage::clear_all_data()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_meta_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "cur meta should not be null", K(ret));
  } else {
    char uri_buf[OB_MAX_URI_LENGTH] = { 0 };
    if (OB_FAIL(build_uri(uri_buf, OB_MAX_URI_LENGTH, nullptr))) {
      OB_LOG(WARN, "fail to build uri", K(ret), K_(cur_storage_info));
    } else if (cur_meta_->is_file_type() && OB_FAIL(force_delete_dir(uri_buf))) {
      OB_LOG(WARN, "fail to force delete fs dir", K(ret), K(uri_buf));
    } else if (cur_meta_->is_obj_type() && OB_FAIL(delete_listed_objects(uri_buf))) {
      OB_LOG(WARN, "fail to delete listed objects", K(ret), K(uri_buf));
    }
  }
  return ret;
}

int TestCommonStorage::build_uri(char *uri_buf, const int64_t buf_len, const char *file_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_meta_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "cur meta should not be null", K(ret));
  } else {
    if (cur_meta_->is_file_type()) {
      if (OB_FAIL(TestCommonStorageUtil::gen_fs_uri(uri_buf, buf_len, cur_meta_->config_.fs_path_,
          default_dir, file_name))) {
        OB_LOG(WARN, "fail to gen fs uri", K(ret), K(default_dir));
      }
    } else if (cur_meta_->is_obj_type()) {
      if (OB_FAIL(TestCommonStorageUtil::gen_object_uri(uri_buf, buf_len, cur_meta_->config_.bucket_,
          default_dir, file_name))) {
        OB_LOG(WARN, "fail to gen object uri", K(ret), "bucket", cur_meta_->config_.bucket_, K(default_dir));
      }
    }
  }
  return ret;
}

int TestCommonStorage::delete_listed_objects(const common::ObString &uri)
{
  int ret = OB_SUCCESS;
  ObStorageUtil util;
  if (OB_FAIL(util.open(&cur_storage_info_))) {
    OB_LOG(WARN, "fail to open storage_util", K(ret));
  } else {
    DelAppendableObjectFragmentOp del_op(uri, util);
    if (OB_FAIL(util.list_files(uri, false/*is_adaptive*/, del_op))) {
      OB_LOG(WARN, "fail to list files", K(ret), K(uri));
    }
  }

  util.close();
  return ret;
}

int TestCommonStorage::create_fs_dir(
    const common::ObString &uri,
    const bool drop_if_exist)
{
  int ret = OB_SUCCESS;
  ObStorageUtil util;
  bool is_dir_exist = false;
  if (OB_FAIL(util.open(&cur_storage_info_))) {
    OB_LOG(WARN, "fail to open storage_util", K(ret));
  } else if (OB_FAIL(util.is_exist(uri, false, is_dir_exist))) {
    OB_LOG(WARN, "fail to check is_exist", K(ret), K(uri));
  } else if (is_dir_exist) {
    if (drop_if_exist) {
      if (OB_FAIL(force_delete_dir(uri))) {
        OB_LOG(WARN, "fail to force delete dir", K(ret), K(uri));
      } else if (OB_FAIL(util.mkdir(uri))) {
        OB_LOG(WARN, "fail to mkdir", K(ret), K(uri));
      }
    } else {
      OB_LOG(INFO, "dir already exist", K(uri));
    }
  } else if (OB_FAIL(util.mkdir(uri))) {
    OB_LOG(WARN, "fail to mkdir", K(ret), K(uri));
  }

  util.close();
  return ret;
}

int TestCommonStorage::is_exist(const common::ObString &uri, const bool is_adaptive, bool &is_exist)
{
  int ret = OB_SUCCESS;
  ObStorageUtil util;
  if (OB_FAIL(util.open(&cur_storage_info_))) {
    OB_LOG(WARN, "fail to open storage_util", K(ret));
  } else if (OB_FAIL(util.is_exist(uri, is_adaptive, is_exist))) {
    OB_LOG(WARN, "fail to check is_exist", K(ret), K(uri));
  }

  util.close();
  return ret;
}

int TestCommonStorage::force_delete_dir(const common::ObString &uri)
{
  int ret = OB_SUCCESS;
  char cmd[OB_MAX_URI_LENGTH] = { 0 };
  const int64_t offset = strlen(OB_FILE_PREFIX);

  if (OB_FAIL(databuff_printf(cmd, OB_MAX_URI_LENGTH, "%s%.*s", "rm -rf ",
      static_cast<int>(uri.length() - offset), uri.ptr() + offset))) {
    STORAGE_LOG(WARN, "fail to fill path", K(ret), K(uri));
  } else if (0 != std::system(cmd)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "fail to delete dir", K(ret), K(uri), K(cmd));
  }
  return ret;
}

int TestCommonStorage::remove_empty_dir(const common::ObString &uri)
{
  int ret = OB_SUCCESS;
  ObStorageUtil util;
  if (OB_FAIL(util.open(&cur_storage_info_))) {
    OB_LOG(WARN, "fail to open storage_util", K(ret));
  } else if (OB_FAIL(util.del_dir(uri))) {
    OB_LOG(WARN, "fail to delete dir", K(ret), K(uri));
  }

  bool is_dir_exist = false;
  if (OB_FAIL(util.is_exist(uri, false/*is_adaptive*/, is_dir_exist))) {
    OB_LOG(WARN, "fail to check is_exist", K(ret), K(uri));
  } else if (is_dir_exist) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "fail to delete dir cuz dir still exist", K(ret), K(uri));
  }

  util.close();
  return ret;
}

int TestCommonStorage::write_single_file(const common::ObString &uri, const int64_t length, const int64_t param_buf_len)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  int64_t buf_len = param_buf_len;
  if (buf_len == -1) {
    buf_len = length;
  }
  char *buf = NULL;
  if (OB_UNLIKELY(length < 1)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), K(length));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "fail to alloc", K(ret), K(buf_len));
  } else {
    MEMSET(buf, 'a', buf_len);
    if (OB_FAIL(write_single_file(uri, buf, length))) {
      OB_LOG(WARN, "fail to write single file", K(ret), K(uri), K(length));
    }
  }
  return ret;
}

int TestCommonStorage::write_single_file(const common::ObString &uri, const char *buf, const int64_t length)
{
  int ret = OB_SUCCESS;
  ObStorageWriter writer;
  if (OB_ISNULL(buf) || OB_UNLIKELY(length < 1)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), K(length), KP(buf));
  } else if (OB_FAIL(writer.open(uri, &cur_storage_info_))) {
    OB_LOG(WARN, "fail to open writer", K(ret), K(uri));
  } else {
    if (OB_FAIL(writer.write(buf, length))) {
      OB_LOG(WARN, "fail to write", K(ret), K(length), K(uri));
    }

    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(writer.close())) {
      ret = (OB_SUCCESS != ret) ? ret : tmp_ret;
      OB_LOG(WARN, "fail to close writer", K(ret), K(tmp_ret), K(length), K(uri));
    }
  }
  return ret;
}

int TestCommonStorage::write_multi_files(
    const common::ObString &uri,
    const int64_t length,
    const int64_t file_num,
    const char *file_prefix,
    const int64_t start_idx)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  const int64_t buf_len = length;
  char *buf = NULL;
  if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "fail to alloc", K(ret), K(length));
  } else {
    MEMSET(buf, 'a', buf_len);
    const int64_t end_idx = start_idx + file_num;
    const bool use_multi_thread = true;
    constexpr int64_t thread_cnt = 5;
    if (file_num <= thread_cnt || !use_multi_thread) {
      char *full_uri = NULL;
      if (OB_ISNULL(full_uri = static_cast<char *>(allocator.alloc(OB_MAX_URI_LENGTH)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        OB_LOG(WARN, "fail to alloc", K(ret), K(length));
      }

      for (int64_t i = start_idx; OB_SUCC(ret) && (i < end_idx); ++i) {
        if (OB_FAIL(databuff_printf(full_uri, OB_MAX_URI_LENGTH, "%.*s%s_%ld",
            static_cast<int>(uri.length()), uri.ptr(), file_prefix, (i + 1)))) {
          STORAGE_LOG(WARN, "fail to fill path", K(ret), K(uri));
        } else if (OB_FAIL(write_single_file(full_uri, buf, buf_len))) {
          OB_LOG(WARN, "fail to write single file", K(ret), K(uri), K(length));
        }
      }
    } else {
      std::thread insert_threads[thread_cnt];
      char **full_uri_arr = NULL;
      if (OB_ISNULL(full_uri_arr = static_cast<char **>(allocator.alloc(sizeof(void*) * thread_cnt)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        OB_LOG(WARN, "fail to alloc", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && (i < thread_cnt); ++i) {
          if (OB_ISNULL(full_uri_arr[i] = static_cast<char *>(allocator.alloc(OB_MAX_URI_LENGTH)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            OB_LOG(WARN, "fail to alloc", K(ret));
          }
        }
      }

      for (auto i = 0; OB_SUCC(ret) && (i < thread_cnt); ++i) {
        insert_threads[i] = std::thread([&, i]() {
          for (auto j = start_idx + i; OB_SUCC(ret) && j < end_idx; j += thread_cnt) {
            if (OB_FAIL(databuff_printf(full_uri_arr[i], OB_MAX_URI_LENGTH, "%.*s%s_%ld",
                static_cast<int>(uri.length()), uri.ptr(), file_prefix, (j + 1)))) {
              STORAGE_LOG(WARN, "fail to fill path", K(ret), K(uri));
            } else if (OB_FAIL(write_single_file(full_uri_arr[i], buf, buf_len))) {
              OB_LOG(WARN, "fail to write single file", K(ret), K(uri), K(length));
            }
          }
        });
      }

      for (auto i = 0; i < thread_cnt; ++i) {
        insert_threads[i].join();
      }
    }
  }
  return ret;
}

int TestCommonStorage::append_write(const common::ObString &uri, const int64_t offset, const int64_t length)
{
  int ret = OB_SUCCESS;
  ObStorageAppender appender;
  ObArenaAllocator allocator;
  char *buf = NULL;
  if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "fail to alloc", K(ret), K(length));
  } else {
    MEMSET(buf, 'a', length);
    if (OB_FAIL(appender.open(uri, &cur_storage_info_))) {
      OB_LOG(WARN, "fail to open appender", K(ret), K(uri));
    } else {
      if (OB_FAIL(appender.pwrite(buf, length, offset))) {
        OB_LOG(WARN, "fail to pwrite", K(ret), K(length), K(offset));
      }

      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(appender.close())) {
        ret = (OB_SUCCESS != ret) ? ret : tmp_ret;
        OB_LOG(WARN, "fail to close appender", K(ret), K(tmp_ret), K(uri), K(length), K(offset));
      }
    }
  }
  return ret;
}

int TestCommonStorage::read_single_file(const common::ObString &uri, const int64_t offset, const int64_t length)
{
  int ret = OB_SUCCESS;
  ObStorageReader reader;
  if (OB_FAIL(reader.open(uri, &cur_storage_info_))) {
    OB_LOG(WARN, "fail to open reader", K(ret), K(uri));
  } else {
    int64_t read_size = 0;
    char *buf = NULL;
    ObArenaAllocator allocator;
    const int64_t buf_len = length;
    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "fail to alloc", K(ret), K(length));
    } else if (OB_FAIL(reader.pread(buf, buf_len, offset, read_size))) {
      OB_LOG(WARN, "fail to pread", K(ret), K(buf_len), K(uri));
    } else if (OB_UNLIKELY(read_size != length)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "the read size not equal to the write size", K(read_size), "write_size", length, K(uri));
    }

    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(reader.close())) {
      ret = (OB_SUCCESS != ret) ? ret : tmp_ret;
      OB_LOG(WARN, "fail to close reader", K(ret), K(tmp_ret));
    }
  }
  return ret;
}

int TestCommonStorage::delete_single_file(const common::ObString &uri)
{
  int ret = OB_SUCCESS;
  ObStorageUtil util;
  if (OB_FAIL(util.open(&cur_storage_info_))) {
    OB_LOG(WARN, "fail to open storage_util", K(ret));
  } else if (OB_FAIL(util.del_file(uri))) {
    OB_LOG(WARN, "fail to delete file", K(ret), K(uri));
  }

  bool is_file_exist = false;
  if (FAILEDx(util.is_exist(uri, false/*is_adaptive*/, is_file_exist))) {
    OB_LOG(WARN, "fail to check is_exist", K(ret), K(uri));
  } else if (is_file_exist) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "fail to delete file cuz file still exist", K(ret), K(uri));
  }

  util.close();
  return ret;
}

int TestCommonStorage::adaptive_read_file(const common::ObString &uri, const int64_t offset, const int64_t length)
{
  int ret = OB_SUCCESS;
  ObStorageAdaptiveReader adaptive_reader;
  if (OB_FAIL(adaptive_reader.open(uri, &cur_storage_info_))) {
    OB_LOG(WARN, "fail to open adaptive reader", K(ret), K(uri));
  } else {
    int64_t read_size = 0;
    char *buf = NULL;
    ObArenaAllocator allocator;
    const int64_t buf_len = length;
    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "fail to alloc", K(ret), K(length));
    } else if (OB_FAIL(adaptive_reader.pread(buf, buf_len, offset, read_size))) {
      OB_LOG(WARN, "fail to pread", K(ret), K(buf_len), K(uri));
    } else if (OB_UNLIKELY(read_size != length)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "the adaptive read size not equal to the write size", K(read_size), "write_size", length, K(uri));
    }

    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(adaptive_reader.close())) {
      ret = (OB_SUCCESS != ret) ? ret : tmp_ret;
      OB_LOG(WARN, "fail to close adaptive reader", K(ret), K(tmp_ret));
    }
  }
  return ret;
}

int TestCommonStorage::get_file_length(const common::ObString &uri, const bool is_adaptive, int64_t &file_length)
{
  int ret = OB_SUCCESS;
  ObStorageUtil util;
  file_length = 0;
  if (OB_FAIL(util.open(&cur_storage_info_))) {
    OB_LOG(WARN, "fail to open storage_util", K(ret));
  } else if (OB_FAIL(util.get_file_length(uri, is_adaptive, file_length))) {
    OB_LOG(WARN, "fail to get file length", K(ret), K(uri));
  }

  util.close();
  return ret;
}

int TestCommonStorage::list_directories(const common::ObString &uri, const bool is_adaptive, common::ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  ObStorageUtil util;
  if (OB_FAIL(util.open(&cur_storage_info_))) {
    OB_LOG(WARN, "fail to open storage_util", K(ret));
  } else if (OB_FAIL(util.list_directories(uri, is_adaptive, op))) {
    OB_LOG(WARN, "fail to list directories", K(ret), K(uri));
  }
  util.close();
  return ret;
}

int TestCommonStorage::list_files(const common::ObString &uri, const bool is_adaptive, common::ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  ObStorageUtil util;
  if (OB_FAIL(util.open(&cur_storage_info_))) {
    OB_LOG(WARN, "fail to open storage_util", K(ret));
  } else if (OB_FAIL(util.list_files(uri, is_adaptive, op))) {
    OB_LOG(WARN, "fail to list files", K(ret), K(uri));
  }
  util.close();
  return ret;
}

int TestCommonStorage::detect_storage_obj_meta(
    const common::ObString &uri, const bool is_adaptive,
    const bool need_fragment_meta, ObStorageObjectMeta &obj_meta)
{
  int ret = OB_SUCCESS;
  ObStorageUtil util;
  if (OB_FAIL(util.open(&cur_storage_info_))) {
    OB_LOG(WARN, "fail to open storage_util", K(ret));
  } else if (OB_FAIL(util.detect_storage_obj_meta(uri, is_adaptive, need_fragment_meta, obj_meta))) {
    OB_LOG(WARN, "fail to list files", K(ret), K(uri));
  }
  util.close();
  return ret;
}

/*-------------------------------------Test Cases--------------------------------------*/
TEST_F(TestCommonStorage, base_situation_test)
{
  TestCommonStorage storage;
  ASSERT_EQ(OB_SUCCESS, storage.init());
  ASSERT_EQ(OB_SUCCESS, storage.mainly_check(true/*is_basic*/, true/*clear_prev_data*/, false/*clear_cur_data*/));
}

TEST_F(TestCommonStorage, abnormal_situation_test)
{
  TestCommonStorage storage;
  ASSERT_EQ(OB_SUCCESS, storage.init());
  ASSERT_EQ(OB_SUCCESS, storage.mainly_check(false/*is_basic*/, false/*clear_pre_data*/, true/*clear_cur_data*/));
}

} // end of oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_common_storage.log*");
  OB_LOGGER.set_file_name("test_common_storage.log", true, true);
  OB_LOGGER.set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}