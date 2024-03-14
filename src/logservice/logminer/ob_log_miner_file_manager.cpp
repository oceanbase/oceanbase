/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX LOGMNR

#include "share/backup/ob_backup_io_adapter.h"
#include "ob_log_miner_file_manager.h"
#include "ob_log_miner_batch_record.h"
#include "ob_log_miner_args.h"
#include "ob_log_miner_logger.h"

namespace oceanbase
{
namespace oblogminer
{

////////////////////////////// ObLogMinerFileManager //////////////////////////////

const char * ObLogMinerFileManager::META_PREFIX = "META/";
const char * ObLogMinerFileManager::META_EXTENSION = "meta";
const char * ObLogMinerFileManager::CSV_SUFFIX = "csv";
const char * ObLogMinerFileManager::SQL_SUFFIX = "sql";
const char * ObLogMinerFileManager::CONFIG_FNAME = "CONFIG";
const char * ObLogMinerFileManager::CHECKPOINT_FNAME = "CHECKPOINT";
const char * ObLogMinerFileManager::INDEX_FNAME = "COMMIT_INDEX";
const char * ObLogMinerFileManager::EXITCODE_FNAME = "EXITCODE";

ObLogMinerFileManager::ObLogMinerFileManager():
    is_inited_(false),
    output_dest_(),
    mode_(FileMgrMode::INVALID),
    format_(RecordFileFormat::INVALID),
    last_write_ckpt_(),
    meta_map_() { }

ObLogMinerFileManager::~ObLogMinerFileManager()
{
  destroy();
}

int ObLogMinerFileManager::init(const char *path,
    const RecordFileFormat format,
    const FileMgrMode mode)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter adapter;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("ObLogMinerFileManager has been initialized", K(is_inited_), K(path), K(format), K(mode));
  } else if (FileMgrMode::RESTART == mode) {
    // TODO: restart when interrupted
    LOG_ERROR("unsupported mode", K(mode));
  } else if (RecordFileFormat::INVALID == format || FileMgrMode::INVALID == mode) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("get invalid record file format or filemgr mode", K(format), K(mode));
  } else if (OB_FAIL(output_dest_.set(path))) {
    LOG_ERROR("output dest failed to set path", K(path));
  } else if (OB_FAIL(meta_map_.init("LogMinerFileIdx"))) {
    LOG_ERROR("file index init failed");
  } else if (FileMgrMode::ANALYZE == mode && OB_FAIL(init_path_for_analyzer_())) {
    LOG_ERROR("init path for analyzer failed", K(path), K(format), K(mode));
    // TODO: init for flashbacker and restart mode
  } else {
    last_write_ckpt_.reset();
    mode_ = mode;
    format_ = format;
    is_inited_ = true;
    LOG_INFO("ObLogMinerFileManager finished to init", K(path), K(format), K(mode));
    LOGMINER_STDOUT_V("ObLogMinerFileManager finished to init\n");
  }
  return ret;
}

void ObLogMinerFileManager::destroy()
{
  if (IS_INIT) {
    is_inited_ = false;
    output_dest_.reset();
    mode_ = FileMgrMode::INVALID;
    format_ = RecordFileFormat::INVALID;
    last_write_ckpt_.reset();
    meta_map_.destroy();
    LOG_INFO("ObLogMinerFileManager destroyed");
    LOGMINER_STDOUT_V("ObLogMinerFileManager destroyed\n");
  }
}

int ObLogMinerFileManager::append_records(const ObLogMinerBatchRecord &batch_record)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("file manager hasn't been initialized yet", K(is_inited_));
  } else {
    ObLogMinerFileMeta meta;
    const int64_t file_id = batch_record.get_file_id();
    const char *data = nullptr;
    int64_t data_len = 0;
    batch_record.get_data(data, data_len);

    // make sure to get meta here
    if (OB_FAIL(meta_map_.get(FileIdWrapper(file_id), meta))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        if (OB_FAIL(create_data_file_(file_id))) {
          LOG_ERROR("file manager failed to create file", K(file_id));
        } else if (OB_FAIL(meta_map_.get(FileIdWrapper(file_id), meta))) {
          LOG_ERROR("failed to get file meta after create file", K(file_id));
        }
      } else {
        LOG_ERROR("get meta from meta index failed", K(meta));
      }
    }

    // write file in append mode
    if (OB_SUCC(ret)) {
      if (0 < data_len && nullptr != data && OB_FAIL(append_data_file_(file_id, data, data_len))) {
        LOG_ERROR("file manager write file failed", K(batch_record));
      } else {
        const ObLogMinerProgressRange &range = batch_record.get_progress_range();
        meta.data_length_ += data_len;
        if (OB_INVALID_TIMESTAMP == meta.range_.min_commit_ts_) {
          meta.range_.min_commit_ts_ = range.min_commit_ts_;
        }

        if (OB_INVALID_TIMESTAMP == meta.range_.min_commit_ts_ ||
            range.max_commit_ts_ >= meta.range_.max_commit_ts_) {
          meta.range_.max_commit_ts_ = range.max_commit_ts_;
        } else if (range.max_commit_ts_ < meta.range_.max_commit_ts_) {
          // TODO: just print error, to findout commit version reverse
          LOG_ERROR("range max_commit_ts rollbacked", K(meta), K(batch_record));
        }

        if (OB_FAIL(write_meta_(file_id, meta))) {
          LOG_ERROR("write meta failed when appending batch_record", K(file_id), K(meta));
        } else if (OB_FAIL(meta_map_.insert_or_update(FileIdWrapper(file_id), meta))) {
          LOG_ERROR("failed to update meta_map", K(file_id), K(meta));
        }
      }
    }
  }
  return ret;
}

int ObLogMinerFileManager::write_config(const ObLogMinerArgs &args)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("file manager hasn't been initialized yet", K(is_inited_));
  } else {
    char config_file_uri[OB_MAX_URI_LENGTH];
    const int64_t arg_size = args.get_serialize_size();
    ObArenaAllocator alloc;
    char *config_file_data_buf = static_cast<char*>(alloc.alloc(arg_size + 1));
    int64_t pos = 0;
    int64_t config_file_uri_len = 0;
    if (OB_ISNULL(config_file_data_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("allocate memory for config_file_data failed", K(arg_size), K(args));
    } else if (OB_FAIL(config_file_uri_(config_file_uri, sizeof(config_file_uri), config_file_uri_len))) {
      LOG_ERROR("failed to get config file uri");
    } else if (OB_FAIL(args.serialize(config_file_data_buf, arg_size + 1, pos))) {
      LOG_ERROR("failed to serialize arg into buffer", K(arg_size), K(pos));
    } else {
      ObBackupIoAdapter utils;
      ObString config_file_uri_str(sizeof(config_file_uri), config_file_uri_len, config_file_uri);
      if OB_FAIL(utils.write_single_file(config_file_uri_str, output_dest_.get_storage_info(),
          config_file_data_buf, arg_size)) {
        LOG_ERROR("failed to write config file to dest", K(config_file_uri_str), K(output_dest_), K(arg_size));
      }
    }

    if (OB_NOT_NULL(config_file_data_buf)) {
      alloc.free(config_file_data_buf);
    }
  }
  return ret;
}

int ObLogMinerFileManager::write_checkpoint(const ObLogMinerCheckpoint &ckpt)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("file manager hasn't been initialized yet", K(is_inited_));
  } else {
    char checkpoint_file_uri[OB_MAX_URI_LENGTH];
    const int64_t ckpt_size = ckpt.get_serialize_size();
    ObArenaAllocator alloc;
    char *ckpt_buf = static_cast<char*>(alloc.alloc(ckpt_size + 1));
    int64_t pos = 0;
    int64_t checkpoint_file_uri_len = 0;
    if (OB_ISNULL(ckpt_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("allocte memory for checkpoint failed", K(ckpt_size), K(ckpt));
    } else if (OB_FAIL(checkpoint_file_uri_(checkpoint_file_uri,
        sizeof(checkpoint_file_uri), checkpoint_file_uri_len))) {
      LOG_ERROR("get checkpoint file uri failed", K(checkpoint_file_uri_len));
    } else if (OB_FAIL(ckpt.serialize(ckpt_buf, ckpt_size+1, pos))) {
      LOG_ERROR("ckpt failed to serialize to ckpt_buf", K(ckpt), K(ckpt_size), K(pos));
    } else {
      // TODO: remove meta from meta_index according to max_file_id when writing checkpoint
      ObBackupIoAdapter utils;
      ObString ckpt_file_uri_str(sizeof(checkpoint_file_uri), checkpoint_file_uri_len, checkpoint_file_uri);
      if (OB_FAIL(utils.write_single_file(ckpt_file_uri_str, output_dest_.get_storage_info(),
          ckpt_buf, ckpt_size))) {
        LOG_ERROR("failed to write config file to dest", K(checkpoint_file_uri), K(output_dest_), K(ckpt_size));
      } else if (OB_FAIL(update_last_write_ckpt_(ckpt))) {
        LOG_ERROR("update last write ckpt failed", K(ckpt), K(last_write_ckpt_));
      }
    }

    if (OB_NOT_NULL(ckpt_buf)) {
      alloc.free(ckpt_buf);
    }
  }

  return ret;
}

int ObLogMinerFileManager::read_file(const int64_t file_id, char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  // TODO : read single file
  return ret;
}

int ObLogMinerFileManager::get_file_range(const int64_t min_timestamp_us,
    const int64_t max_timestamp_us,
    int64_t &min_file_id,
    int64_t &max_file_id)
{
  int ret = OB_SUCCESS;
  // TODO: flashback mode, get file range according to min_timestamp_us and max_timestamp_us
  return ret;
}

int ObLogMinerFileManager::read_config(ObLogMinerArgs &args)
{
  int ret = OB_SUCCESS;
  // TODO: restart mode, read config to recover from exception
  return ret;
}

int ObLogMinerFileManager::read_checkpoint(ObLogMinerCheckpoint &ckpt)
{
  int ret = OB_SUCCESS;
  // TODO: flashback/restart mode, get checkpoint
  return ret;
}

int ObLogMinerFileManager::config_file_uri_(char *buf, const int64_t buf_len, int64_t &length) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, length, "%s/%s",
      output_dest_.get_root_path().ptr(), CONFIG_FNAME))) {
    LOG_ERROR("fill config file uri into buffer failed", K(output_dest_), K(buf_len));
  }
  return ret;
}

int ObLogMinerFileManager::checkpoint_file_uri_(char *buf, const int64_t buf_len, int64_t &length) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, length, "%s/%s",
      output_dest_.get_root_path().ptr(), CHECKPOINT_FNAME))) {
    LOG_ERROR("fill checkpoint file uri into buffer failed", K(output_dest_), K(buf_len));
  }
  return ret;
}

int ObLogMinerFileManager::data_file_uri_(
    const int64_t file_id,
    char *buf,
    const int64_t buf_len,
    int64_t &length) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, length, "%s/%ld.%s",
      output_dest_.get_root_path().ptr(), file_id, data_file_extension_()))) {
    LOG_ERROR("failed to get data file uri", K(length), K(file_id), K(buf_len));
  }
  return ret;
}

int ObLogMinerFileManager::meta_file_uri_(
    const int64_t file_id,
    char *buf,
    const int64_t buf_len,
    int64_t &length) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, length, "%s/%s%ld.%s",
      output_dest_.get_root_path().ptr(), META_PREFIX, file_id, META_EXTENSION))) {
    LOG_ERROR("failed to get meta file uri", K(length), K(file_id), K(buf_len));
  }
  return ret;
}

int ObLogMinerFileManager::index_file_uri_(char *buf, const int64_t buf_len, int64_t &length) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, length, "%s/%s",
      output_dest_.get_root_path().ptr(), INDEX_FNAME))) {
    LOG_ERROR("fill index file uri into buffer failed", K(output_dest_), K(buf_len));
  }
  return ret;
}

const char *ObLogMinerFileManager::data_file_extension_() const
{
  const char *suffix = "";
  switch(format_) {
    case RecordFileFormat::CSV: {
      suffix = "csv";
      break;
    }

    case RecordFileFormat::REDO_ONLY:
    case RecordFileFormat::UNDO_ONLY: {
      suffix = "sql";
      break;
    }

    case RecordFileFormat::JSON: {
      suffix = "json";
      break;
    }

    case RecordFileFormat::AVRO: {
      suffix = "avro";
      break;
    }

    case RecordFileFormat::PARQUET: {
      suffix = "parquet";
      break;
    }

    default: {
      suffix = "";
      break;
    }
  }
  return suffix;
}

int ObLogMinerFileManager::create_data_file_(const int64_t file_id)
{
  int ret = OB_SUCCESS;
  // create file, generate file header first.
  int64_t pos = 0;
  ObLogMinerFileMeta meta;
  ObArenaAllocator alloc;
  char *header_data = nullptr;
  int64_t header_len = 0;
  char data_file_uri[OB_MAX_URI_LENGTH];
  int64_t uri_len = 0;

  // create file meta, no need to write metainfo into storage
  // but it's okay to write meta here
  if (OB_FAIL(generate_data_file_header_(alloc, header_data, header_len))) {
    LOG_ERROR("failed to generate file header", K(format_), K(header_len));
  } else if (OB_FAIL(data_file_uri_(file_id, data_file_uri, sizeof(data_file_uri), uri_len))) {
    LOG_ERROR("failed to get data_file uri", K(file_id), K(uri_len));
  } else if (OB_FAIL(append_file_(data_file_uri, 0, header_data, header_len))) {
    LOG_ERROR("failed to append file when creating file", K(data_file_uri), K(file_id), K(header_len));
  } else {
    meta.range_.min_commit_ts_ = OB_INVALID_TIMESTAMP;
    meta.range_.max_commit_ts_ = OB_INVALID_TIMESTAMP;
    meta.data_length_ = header_len;
    if (OB_FAIL(write_meta_(file_id, meta))) {
      LOG_ERROR("failed to write meta when writing file", K(file_id), K(meta));
    } else if (OB_FAIL(meta_map_.insert(file_id, meta))) {
      LOG_ERROR("failed to insert meta into meta_index", K(file_id), K(meta));
    }
  }

  if (OB_NOT_NULL(header_data)) {
    alloc.free(header_data);
  }

  return ret;
}

int ObLogMinerFileManager::generate_data_file_header_(
    ObIAllocator &alloc,
    char *&data,
    int64_t &data_len)
{
  int ret = OB_SUCCESS;
  switch (format_) {
    case RecordFileFormat::CSV: {
      #define MINER_SCHEMA_DEF(field, id, args...) \
        #field,
      const char *header[] = {
        #include "ob_log_miner_analyze_schema.h"
      };
      #undef MINER_SCHEMA_DEF
      int64_t header_len = 0, element_cnt = sizeof(header)/sizeof(const char*);
      char *buf = nullptr;
      int64_t pos = 0;
      for (int i = 0; i < element_cnt; i++) {
        header_len += strlen(header[i]) + 1;
      }
      if (OB_ISNULL(buf = static_cast<char*>(alloc.alloc(header_len + 1)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("allocate memory for csv header failed", K(header_len));
      } else {
        for (int i = 0; OB_SUCC(ret) && i < element_cnt; i++) {
          char delimiter = i == element_cnt-1 ? '\n': ',';
          if (OB_FAIL(databuff_printf(buf, header_len + 1, pos, "%s%c", header[i], delimiter))) {
            LOG_ERROR("failed to fill header into buffer", K(i), K(header[i]), K(pos));
          }
        }

        if (OB_SUCC(ret)) {
          data = buf;
          data_len = header_len;
        }
      }
      break;
    }

    case RecordFileFormat::REDO_ONLY:
    case RecordFileFormat::UNDO_ONLY:
    case RecordFileFormat::JSON: {
      // do nothing;
      break;
    }

    // TODO: support other type
    default: {
      ret = OB_NOT_SUPPORTED;
      break;
    }
  }
  return ret;
}

int ObLogMinerFileManager::append_data_file_(const int64_t file_id, const char *data, const int64_t data_len)
{
  int ret = OB_SUCCESS;

  ObBackupIoAdapter utils;
  char data_file_uri[OB_MAX_URI_LENGTH];
  int64_t uri_len = 0;
  ObIOFd fd;
  ObIODevice *device_handle = nullptr;
  ObLogMinerFileMeta meta;
  int64_t write_size = 0;

  if (OB_FAIL(meta_map_.get(FileIdWrapper(file_id), meta))) {
    LOG_ERROR("failed to get meta in meta_index when trying to append data to file", K(file_id),
        K(data_len), K(meta));
  } else if (OB_FAIL(data_file_uri_(file_id, data_file_uri, sizeof(data_file_uri), uri_len))) {
    LOG_ERROR("failed to get data_file uri", K(file_id), K(uri_len), K(output_dest_));
  } else if (OB_FAIL(append_file_(data_file_uri, meta.data_length_, data, data_len))) {
    LOG_ERROR("failed to open device", K(file_id), K(output_dest_), K(data_file_uri));
  }

  return ret;
}

int ObLogMinerFileManager::append_file_(const ObString &uri,
    const int64_t offset,
    const char *data,
    const int64_t data_len)
{
  int ret = OB_SUCCESS;

  ObBackupIoAdapter utils;
  ObIOFd fd;
  ObIODevice *device_handle = nullptr;
  int64_t write_size = 0;
  if (nullptr == data || 0 == data_len) {
    // do nothing
  } else if (OB_FAIL(utils.open_with_access_type(device_handle, fd, output_dest_.get_storage_info(),
      uri, common::OB_STORAGE_ACCESS_RANDOMWRITER))) {
    LOG_ERROR("failed to open device", K(uri), K(output_dest_), K(uri));
  } else if (OB_FAIL(device_handle->pwrite(fd, offset, data_len, data, write_size))) {
    LOG_ERROR("failed to write data into file", K(uri), K(output_dest_),
        K(data_len), K(write_size));
  } else if (write_size != data_len) {
    ret = OB_IO_ERROR;
    LOG_WARN("write length not equal to data length", K(write_size), K(data_len));
  }

  if (nullptr != device_handle && fd.is_valid()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(utils.close_device_and_fd(device_handle, fd))) {
      LOG_ERROR("failed to close device and fd", K(uri), K(fd));
    }
  }
  return ret;
}

int ObLogMinerFileManager::read_data_file_(const int64_t file_id, char *data, const int64_t data_len)
{
  int ret = OB_SUCCESS;
  // TODO: read the whole file into the buf(data)
  return ret;
}

int ObLogMinerFileManager::write_data_file_(const int64_t file_id, const char *data, const int64_t data_len)
{
  int ret = OB_SUCCESS;
  // TODO: write the whole file with data, used in restart mode.
  return ret;
}

int ObLogMinerFileManager::read_meta_(const int64_t file_id, ObLogMinerFileMeta &meta)
{
  int ret = OB_SUCCESS;
  // TODO: read meta to read file.
  return ret;
}

int ObLogMinerFileManager::write_meta_(const int64_t file_id, const ObLogMinerFileMeta &meta)
{
  int ret = OB_SUCCESS;
  char meta_uri[OB_MAX_URI_LENGTH];
  ObArenaAllocator alloc;
  int64_t uri_len = 0;
  ObBackupIoAdapter utils;
  const int64_t buf_len = meta.get_serialize_size();
  int64_t pos = 0;
  char *buf = static_cast<char*>(alloc.alloc(buf_len + 1));

  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate memory to file meta data buffer", K(buf_len), K(buf));
  } else if (OB_FAIL(meta.serialize(buf, buf_len + 1, pos))) {
    LOG_ERROR("failed to serialize meta into buffer", K(buf_len), K(meta));
  } else if (OB_FAIL(meta_file_uri_(file_id, meta_uri, sizeof(meta_uri), uri_len))) {
    LOG_ERROR("failed to get meta_file uri", K(file_id), K(uri_len));
  } else if (OB_FAIL(utils.write_single_file(meta_uri,
      output_dest_.get_storage_info(), buf, buf_len))) {
    LOG_ERROR("failed to write meta file", K(meta), K(file_id), K(meta_uri), K(buf_len));
  }

  if (OB_NOT_NULL(buf)) {
    alloc.free(buf);
  }

  return ret;
}

int ObLogMinerFileManager::write_index_(const int64_t file_id)
{
  int ret = OB_SUCCESS;

  ObLogMinerFileMeta meta;
  char index_uri_cstr[OB_MAX_URI_LENGTH];
  int64_t index_uri_len = 0;

  if (OB_FAIL(meta_map_.get(FileIdWrapper(file_id), meta))) {
    LOG_ERROR("get meta from meta_index failed", K(file_id), K(meta));
  } else if (OB_FAIL(index_file_uri_(index_uri_cstr, sizeof(index_uri_cstr), index_uri_len))) {
    LOG_ERROR("failed to get index file uri", K(output_dest_), K(file_id), K(index_uri_len));
  } else {
    const int64_t index_file_offset = file_index_.get_index_file_len();
    FileIndexItem item(file_id, meta.range_.min_commit_ts_, meta.range_.max_commit_ts_);
    const int64_t index_data_len = item.get_serialize_size();
    ObArenaAllocator alloc;
    int64_t pos = 0;
    char *item_buf = static_cast<char*>(alloc.alloc(index_data_len + 1));
    if (OB_ISNULL(item_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("allocate memory for fileIndexItem failed", K(item_buf), K(index_data_len), K(item));
    } else if (OB_FAIL(item.serialize(item_buf, index_data_len + 1, pos))) {
      LOG_ERROR("failed to serialize index item into item_buf", K(index_data_len), K(pos));
    } else if (OB_FAIL(append_file_(index_uri_cstr, index_file_offset, item_buf, index_data_len))) {
      LOG_ERROR("failed to append index item data into storage system", K(index_uri_cstr), K(index_file_offset),
          K(index_data_len));
    } else if (OB_FAIL(file_index_.insert_index_item(item))) {
      LOG_ERROR("failed to insert index itme into file_index", K(file_index_), K(item));
    }

  }

  return ret;
}

int ObLogMinerFileManager::update_last_write_ckpt_(const ObLogMinerCheckpoint &ckpt)
{
  int ret = OB_SUCCESS;

  const int64_t last_max_file_id = last_write_ckpt_.max_file_id_;
  const int64_t cur_max_file_id = ckpt.max_file_id_;

  // traverse from last_max_file_id+1 to cur_max_file_id
  for (int64_t i = last_max_file_id + 1; OB_SUCC(ret) && i <= cur_max_file_id; i++) {
    if (OB_FAIL(write_index_(i))) {
      LOG_ERROR("failed to write index for data file", "file_id", i, K(last_max_file_id), K(cur_max_file_id));
    } else if (OB_FAIL(meta_map_.erase(i))) {
      LOG_ERROR("failed to erase meta from meta_index", "file_id", i, K(last_max_file_id), K(cur_max_file_id));
    } else {
    }
  }

  if (OB_SUCC(ret)) {
    last_write_ckpt_ = ckpt;
  }

  return ret;
}

int ObLogMinerFileManager::init_path_for_analyzer_()
{
  int ret = OB_SUCCESS;

  ObBackupIoAdapter adapter;
  char path_uri[OB_MAX_URI_LENGTH];
  bool is_exist = false;

  if (OB_FAIL(adapter.is_exist(output_dest_.get_root_path(),
      output_dest_.get_storage_info(), is_exist))) {
    LOG_ERROR("failed to check output_dest existence", K(output_dest_));
  } else if (! is_exist) {
    if (OB_FAIL(adapter.mkdir(output_dest_.get_root_path(), output_dest_.get_storage_info()))) {
      LOG_ERROR("failed to make output_dest", K(output_dest_));
    }
  }
  // oss don't create directory actually, so is_empty_directory() must be checked.
  if(OB_SUCC(ret)) {
    bool is_empty = false;
    if (OB_FAIL(adapter.is_empty_directory(output_dest_.get_root_path(),
        output_dest_.get_storage_info(), is_empty))) {
      LOG_ERROR("failed to check is empty directory", K(output_dest_));
    } else if (!is_empty) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("init for analyzer but not empty directory", K(output_dest_), K(is_empty));
      LOGMINER_STDOUT("not empty output directory: %s\n", output_dest_.get_root_path().ptr());
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(databuff_printf(path_uri, sizeof(path_uri), "%s/%s",
        output_dest_.get_root_path().ptr(), META_PREFIX))) {
      LOG_ERROR("failed to fill path for meta entry", K(output_dest_), K(META_PREFIX));
    } else if (OB_FAIL(adapter.mkdir(path_uri, output_dest_.get_storage_info()))) {
      LOG_ERROR("failed to mkdir for meta", K(path_uri), K(output_dest_));
    }
  }

  return ret;
}

}
}