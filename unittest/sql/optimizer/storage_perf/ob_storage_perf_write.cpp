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

#include "ob_storage_perf_write.h"
#include "ob_storage_perf_data.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace storage;
using namespace share;
using namespace share::schema;

namespace storageperf
{

ObStoragePerfWrite::ObStoragePerfWrite()
 : schema_service_(NULL)
 , config_(NULL)
 , row_count_(0)
 , thread_no_(-1)
 , is_inited_(false)
{

}

ObStoragePerfWrite::~ObStoragePerfWrite()
{
}

int ObStoragePerfWrite::init(ObStoragePerfConfig *config,
                             const int64_t thread_no,
                             ObRestoreSchema *restore_schema,
                             MockSchemaService *schema_service)
{
  int ret = OB_SUCCESS;

  if(is_inited_){
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice");
  } else if(NULL == config || NULL == schema_service){
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "config is NULL", K(config));
  } else {
    config_ = config;
    thread_no_ = thread_no;
    row_count_ = config_->total_scan_row_count_;
    int n1 = snprintf(data_file_path_, MAX_PATH_SIZE, "%s/%ld", config_->get_sstable_data_dir(), thread_no + 1);
    int n2 = snprintf(log_dir_path_, MAX_PATH_SIZE, "%s/%ld", config_->get_slog_dir(), thread_no + 1);
    int n3 = snprintf(sstable_meta_path_, MAX_PATH_SIZE, "%s/%ld", config_->get_sstable_meta_dir(), thread_no + 1);
    if(n1 > MAX_PATH_SIZE || n1 < 0
        || n2 > MAX_PATH_SIZE || n2 < 0
        || n3 > MAX_PATH_SIZE || n3 < 0){
      ret = OB_BUF_NOT_ENOUGH;
      STORAGE_LOG(WARN, "fail to create data file path");
    } else {
      schema_service_ = schema_service;
      restore_schema_ = restore_schema;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObStoragePerfWrite::store_sstable_meta(const ObSSTable &sstable)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t size = sstable.get_serialize_size();
  ObArenaAllocator allocator(ObModIds::TEST);
  int fd = -1;

  char *buf = static_cast<char *>(allocator.alloc(size));
  if(NULL == buf){
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc memory", K(size), K(ret));
  } else if(OB_SUCCESS != (ret = sstable.serialize(buf, size, pos))){
    STORAGE_LOG(WARN, "fait to serialize sstable", K(ret));
  } else if(-1 == ( fd = open(sstable_meta_path_, O_CREAT|O_EXCL|O_RDWR, 0777))){
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to open file", K(sstable_meta_path_), K(fd));
  } else if (size != write(fd, buf, size)){
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "faile to write meta", K(size));
  } else {
    close(fd);
  }
  return ret;
}

int ObStoragePerfWrite::init_data_file()
{
  int ret = OB_SUCCESS;
  ObBlockFile::FileLocation  location;
  ObBlockFile::FileSpec spec;
  location.disk_no_ = 1;
  location.install_sequence_ = 1;

  if (static_cast<int64_t>(strlen(data_file_path_)) > common::OB_MAX_FILE_NAME_LENGTH){
    ret = OB_SIZE_OVERFLOW;
    STORAGE_LOG(WARN, "data file is too long", K(strlen(data_file_path_)));
  } else {
    strcpy(location.path_, data_file_path_);
    spec.macro_block_size_ = OB_DEFAULT_MACRO_BLOCK_SIZE;
    if (config_->unittest_mode_) {
      spec.data_file_size_ = 512 * 1024L * 1024L;
    } else {
      spec.data_file_size_ = config_->get_partition_size() * 1024L * 1024L * 1024L;//GB
    }
    if (OB_SUCCESS != (ret = data_file_.format(location, spec))) {
      STORAGE_LOG(WARN, "format data file error.", K(ret));
    }
  }

  if(OB_SUCC(ret)){
    if (OB_SUCCESS != (ret = data_file_.open(location, &image_))) {
      STORAGE_LOG(WARN, "open data file error.", K(ret));
    } else if (OB_SUCCESS != (ret = image_.initialize(&data_file_, &logger_))) {
        STORAGE_LOG(WARN, "initialize macro block meta error.", K(ret));
    } else if (OB_SUCCESS != (ret = logger_.init(data_file_, log_dir_path_, 1024L*1024L*1024L))) {
      STORAGE_LOG(WARN, "initialize commit logger", K(ret));
    } else if (OB_SUCCESS != (ret = logger_.replay())) {
      STORAGE_LOG(WARN, "replay commit log error.", K(ret));
    } else if (OB_SUCCESS != (ret = marker_.initialize(&data_file_, &image_))) {
      STORAGE_LOG(WARN, "initialize macro block marker error.", K(ret));
    } else if (OB_SUCCESS != (ret = marker_.register_storage_meta(&image_))) {
      STORAGE_LOG(WARN, "register macro block meta error.", K(ret));
    } else if (OB_SUCCESS != (ret = marker_.mark_init())) {
      STORAGE_LOG(WARN, "build first free list error.", K(ret));
    }
  }
  return ret;
}

void ObStoragePerfWrite::cleanup_sstable() {
  char cmd[MAX_PATH_SIZE];
  int n = snprintf(cmd, MAX_PATH_SIZE, "rm -rf %s", data_file_path_);
  system(cmd);
  n = snprintf(cmd, MAX_PATH_SIZE, "rm -rf %s", log_dir_path_);
  system(cmd);
  n = snprintf(cmd, MAX_PATH_SIZE, "rm -rf %s", sstable_meta_path_);
  UNUSED(n);
  system(cmd);
}

int ObStoragePerfWrite::create_sstable(ObStorageCacheSuite &cache_suite, int64_t &used_macro_num)
{
  int ret = OB_SUCCESS;
  ObRowGenerate row_generate;
  ObSSTable sstable;
  ObMergeMacroBlockWriter writer;
  if(!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "should init first");
  } else {
    cleanup_sstable();
    ObSchemaGetterGuard schema_guard;
    schema_service_->get_schema_guard(schema_guard, INT64_MAX);
    const ObTableSchema *schema = NULL;
    if (OB_SUCCESS != (ret = schema_guard.get_table_schema(combine_id(tenant_id, pure_id), schema))) {
      STORAGE_LOG(WARN, "failed to get table schema", K(ret));
    } else if (NULL == schema) {
      STORAGE_LOG(WARN, "schema is null");
    } else if(OB_SUCCESS != (ret = row_generate.init(*schema))){
      STORAGE_LOG(WARN, "fail to init table schema");
    } else if(OB_SUCCESS != (ret = init_data_file())){
      STORAGE_LOG(WARN, "fail init data file", K(ret));
    } else if(OB_SUCCESS != (ret = sstable.init(&data_file_, cache_suite, 1, NULL))){
      STORAGE_LOG(WARN, "fail to init sstable", K(ret));
    } else if(OB_SUCCESS != (ret = sstable.open(*schema, data_version))){
      STORAGE_LOG(WARN, "fail to open sstable", K(ret));
    } else if (OB_SUCCESS != (ret = writer.init(&data_file_, NULL, 0))) {
      STORAGE_LOG(WARN, "failed to init sstable", K(ret));
    } else if(OB_SUCCESS != (ret = writer.open(*schema, data_version,
        ObDataStoreDesc::default_row_store_type(),
        ObDataStoreDesc::default_encoder_opt(),
        1))){
      STORAGE_LOG(WARN, "open sstable failed", K(ret));
    } else if(OB_SUCCESS != (ret = image_.enable_write_log())){
      STORAGE_LOG(WARN, "fail to set log", K(ret));
    } else if(OB_SUCCESS != (ret = logger_.begin(OB_LOG_CS_DAILY_MERGE))){
      STORAGE_LOG(WARN, "fail to start logger", K(ret));
    } else {
      const ObStoreRow *row = NULL;
      for(int64_t i = 0; i < row_count_ && OB_SUCCESS == ret; ++i){
        if(OB_SUCCESS != (ret = row_generate.get_next_row(row))){
          STORAGE_LOG(WARN, "fail to generate row", K(*row), K(ret));
        } else if(OB_SUCCESS != (ret = writer.append(*row))){
          STORAGE_LOG(WARN, "fail to append row", K(*row), K(ret));
        }
      }
    }
  }

  if(OB_SUCC(ret)){
    int64_t log_seq_num = 0;
    if (OB_FAIL(writer.close())) {
      STORAGE_LOG(WARN, "Fail to close writer, ", K(ret));
    } else if (OB_FAIL(sstable.append(writer.get_macro_blocks()))) {
      STORAGE_LOG(WARN, "Fail to append macro blocks, ", K(ret));
    } else if (OB_FAIL(sstable.close())) {  
      STORAGE_LOG(WARN, "fail to close sstable", K(ret));
    } else if(OB_SUCCESS != (ret = logger_.commit(log_seq_num))){
      STORAGE_LOG(WARN, "fail to commit log", K(ret));
    } else if(OB_SUCCESS != (ret = store_sstable_meta(sstable))){
      STORAGE_LOG(WARN, "fail to store sstable meta", K(ret));
    } else {
      close_sstable();
      used_macro_num = data_file_.get_total_macro_block_cnt() - data_file_.get_reserved_block_count()
        - data_file_.get_free_macro_block_count();
    }
  }
  return ret;
}

int ObStoragePerfWrite::close_sstable()
{
  int ret = OB_SUCCESS;
  logger_.write_check_point();
  data_file_.close();
  image_.destroy();
  marker_.destroy();
  logger_.destroy();
  return ret;
}

}//end namespace storageperf
}//end namespace oceanbase
