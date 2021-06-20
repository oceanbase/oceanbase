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

#include "ob_ilog_storage.h"
#include "storage/ob_partition_service.h"
#include "share/redolog/ob_log_store_factory.h"
#include "share/redolog/ob_log_file_reader.h"
#include "share/ob_thread_mgr.h"
#include "ob_clog_config.h"
#include "ob_log_engine.h"
#include "ob_log_file_trailer.h"

namespace oceanbase {
using namespace common;
using namespace storage;
using namespace share;
namespace clog {

ObIlogAccessor::ObIlogAccessor()
    : file_store_(NULL),
      file_id_cache_(),
      buffer_(),
      log_tail_(),
      direct_reader_(),
      old_version_max_file_id_(OB_INVALID_FILE_ID),
      inited_(false)
{}

ObIlogAccessor::~ObIlogAccessor()
{
  destroy();
}

void ObIlogAccessor::destroy()
{
  buffer_.destroy();
  file_id_cache_.destroy();
  direct_reader_.destroy();
  ObLogStoreFactory::destroy(file_store_);
  file_store_ = NULL;
  old_version_max_file_id_ = OB_INVALID_FILE_ID;
  inited_ = false;
}

int ObIlogAccessor::init(const char* dir_name, const char* shm_path, const int64_t server_seq,
    const common::ObAddr& addr, ObLogCache* log_cache)
{
  int ret = OB_SUCCESS;
  const bool use_log_cache = true;
  if (inited_) {
    ret = OB_INIT_TWICE;
    CSR_LOG(ERROR, "ObIlogStorage init twice", K(ret));
  } else if (OB_ISNULL(dir_name) || OB_ISNULL(log_cache) || OB_UNLIKELY(server_seq < 0 || !addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid argument", KP(dir_name), KP(log_cache), K(ret));
  } else if (NULL == (file_store_ = ObLogStoreFactory::create(dir_name,
                          CLOG_FILE_SIZE,  // file_size is only used in CLOG
                          ObLogWritePoolType::ILOG_WRITE_POOL))) {
    ret = OB_INIT_FAIL;
    CSR_LOG(ERROR, "file_store_ init failed", K(ret));
  } else if (OB_FAIL(file_id_cache_.init(server_seq, addr, this))) {
    CSR_LOG(ERROR, "file_id_cache_ init failed", K(ret));
  } else if (OB_FAIL(direct_reader_.init(
                 dir_name, shm_path, use_log_cache, log_cache, &log_tail_, ObLogWritePoolType::ILOG_WRITE_POOL))) {
    CSR_LOG(ERROR, "direct_reader_ init failed", K(ret));
  } else if (OB_FAIL(buffer_.init(OB_MAX_LOG_BUFFER_SIZE, CLOG_DIO_ALIGN_SIZE, ObModIds::OB_CLOG_INFO_BLK_HNDLR))) {
    CSR_LOG(ERROR, "buffer init failed", K(ret));
  } else {
    old_version_max_file_id_ = OB_INVALID_FILE_ID;
    inited_ = true;
    CSR_LOG(INFO, "ObIlogStorage init failed", K(ret));
  }
  return ret;
}

int ObIlogAccessor::add_partition_needed_to_file_id_cache(const ObPartitionKey& pkey, const uint64_t last_replay_log_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogAccessor is not inited", K(pkey), KR(ret));
  } else if (OB_FAIL(file_id_cache_.add_partition_needed(pkey, last_replay_log_id))) {
    CSR_LOG(ERROR, "failed to add_partition_need_to_filter", K(pkey), K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObIlogAccessor::fill_file_id_cache()
{
  int ret = OB_SUCCESS;
  file_id_t min_file_id = OB_INVALID_FILE_ID;
  file_id_t max_file_id = OB_INVALID_FILE_ID;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogAccessor is not inited", K(ret));
  } else if (OB_FAIL(file_store_->get_file_id_range(min_file_id, max_file_id)) && OB_ENTRY_NOT_EXIST != ret) {
    CSR_LOG(ERROR, "ilog_dir_ get_file_id_range failed", K(ret));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
  } else if (OB_FAIL(handle_last_ilog_file_(max_file_id))) {
    CSR_LOG(ERROR, "handle_last_ilog_file_ failed", K(ret));
  } else {
    for (file_id_t file_id = min_file_id; OB_SUCC(ret) && file_id <= max_file_id; file_id++) {
      if (OB_FAIL(fill_file_id_cache_(file_id))) {
        CSR_LOG(ERROR, "fill_file_id_cache_ failed", K(ret), K(file_id));
      } else {
        // do nothing
      }
    }
    CSR_LOG(INFO, "finish fill_file_id_cache_", K(ret), K(min_file_id), K(max_file_id));
  }
  return ret;
}

int ObIlogAccessor::get_cursor_from_ilog_file(const common::ObAddr& addr, const int64_t seq,
    const common::ObPartitionKey& partition_key, const uint64_t query_log_id, const Log2File& item,
    ObLogCursorExt& log_cursor_ext)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogStorage is not inited", K(ret), K(partition_key));
  } else if (OB_UNLIKELY(!partition_key.is_valid() || !addr.is_valid() || !is_valid_log_id(query_log_id) || seq < 0 ||
                         !item.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid arguments", KR(ret), K(addr), K(seq), K(partition_key), K(query_log_id), K(item));
  } else {
    const int64_t CSR_BATCH_SIZE = 1;
    char cursor_batch_buf[CSR_BATCH_SIZE * sizeof(ObLogCursorExt)];
    ObLogCursorExt* cursor_array = (ObLogCursorExt*)cursor_batch_buf;
    ObGetCursorResult result(cursor_array, CSR_BATCH_SIZE);
    if (OB_FAIL(direct_reader_.get_batch_log_cursor(partition_key, addr, seq, query_log_id, item, result))) {
      CSR_LOG(
          WARN, "failed to get_batch_log_cursor", K(ret), K(addr), K(seq), K(partition_key), K(query_log_id), K(item));
    } else {
      log_cursor_ext = result.csr_arr_[0];
    }
  }
  return ret;
}

int ObIlogAccessor::get_cursor_batch_for_fast_recovery(const ObAddr& addr, const int64_t seq,
    const ObPartitionKey& partition_key, const uint64_t query_log_id, ObGetCursorResult& result)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogStorage is not inited", K(ret), K(partition_key));
  } else if (!partition_key.is_valid() || !addr.is_valid() || !is_valid_log_id(query_log_id)) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid arguments", KR(ret), K(addr), K(partition_key), K(query_log_id));
  } else {
    Log2File prev_item;
    Log2File next_item;
    const bool locate_by_log_id = true;
    const int64_t query_value = static_cast<int64_t>(query_log_id);

    ret = file_id_cache_.locate(partition_key, query_value, locate_by_log_id, prev_item, next_item);
    int file_id_cache_locate_err = ret;
    if (OB_SUCCESS == ret || OB_ERR_OUT_OF_UPPER_BOUND == ret) {
      ret = OB_SUCCESS;
      bool prev_item_contain_target_log = prev_item.contain_log_id(query_log_id);
      if (!prev_item_contain_target_log) {
        ret = OB_CURSOR_NOT_EXIST;
      } else if (OB_FAIL(
                     direct_reader_.get_batch_log_cursor(partition_key, addr, seq, query_log_id, prev_item, result))) {
        CSR_LOG(WARN,
            "failed to get_batch_log_cursor",
            K(ret),
            K(addr),
            K(seq),
            K(partition_key),
            K(query_log_id),
            K(prev_item));
      } else {
        // do nothing
      }

      if (OB_CURSOR_NOT_EXIST == ret) {
        if (OB_ERR_OUT_OF_UPPER_BOUND == file_id_cache_locate_err) {
          ret = OB_ERR_OUT_OF_UPPER_BOUND;
        } else {
          ret = OB_CURSOR_NOT_EXIST;
        }
      }
    } else if (OB_PARTITION_NOT_EXIST == ret || OB_ERR_OUT_OF_LOWER_BOUND == ret) {
      ret = OB_CURSOR_NOT_EXIST;
    }
  }
  return ret;
}

int ObIlogAccessor::handle_last_ilog_file_(const file_id_t file_id)
{
  int ret = OB_SUCCESS;
  int64_t st_size = 0;

  if (OB_FAIL(file_store_->get_file_st_size(file_id, st_size))) {
    CSR_LOG(ERROR, "fstat failed, io_error", K(ret), K(errno));
  } else {
    ObReadParam param;
    param.file_id_ = file_id;
    param.offset_ = static_cast<offset_t>(st_size - CLOG_TRAILER_SIZE);
    param.read_len_ = CLOG_TRAILER_SIZE;
    param.timeout_ = DEFAULT_READ_TIMEOUT;

    ObReadRes res;
    ObReadCost dummy_cost;

    int16_t magic_number = 0;
    int64_t pos = 0;
    ObReadBufGuard guard(ObModIds::OB_LOG_DIRECT_READER_ILOG_ID);
    ObReadBuf& rbuf = guard.get_read_buf();
    if (OB_UNLIKELY(!rbuf.is_valid())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      CSR_LOG(ERROR, "direct_reader_ alloc_buf failed", K(param), K(ret));
    } else if (OB_FAIL(direct_reader_.read_data_direct(param, rbuf, res, dummy_cost))) {
      CSR_LOG(ERROR, "read_data_direct failed", K(ret), K(param));
    } else if (res.data_len_ != param.read_len_) {
      ret = OB_ERR_UNEXPECTED;
      CSR_LOG(ERROR, "data_len not match", K(ret), K(res.data_len_), K(param.read_len_));
    } else if (OB_FAIL(serialization::decode_i16(res.buf_, res.data_len_, pos, &magic_number))) {
      CSR_LOG(ERROR, "decode_i16 failed", K(ret));
    } else if (magic_number == ObIlogFileTrailerV2::MAGIC_NUMBER) {
      // do nothing
    } else {
      // handle last ilog file which version is smaller than 2.1.0 and doesn't
      // contain info_block and trailer.
      ObRawIndexIterator iter;
      ObIndexInfoBlockHandler info_block_handler;
      if (OB_FAIL(info_block_handler.init())) {
        CSR_LOG(ERROR, "info_block_handler init failed", K(ret));
      } else if (OB_FAIL(iter.init(&direct_reader_, file_id, 0, file_id, DEFAULT_READ_TIMEOUT))) {
        CSR_LOG(ERROR, "iter init failed", K(ret));
      } else {
        ObIndexEntry entry;
        int64_t persist_len = 0;  // not used here
        while (OB_SUCC(ret) && OB_SUCC(iter.next_entry(entry, param, persist_len))) {
          if (OB_FAIL(info_block_handler.update_info(
                  entry.get_partition_key(), entry.get_log_id(), entry.get_submit_timestamp()))) {
            CSR_LOG(ERROR, "info_block_handler update_info failed", K(ret));
          } else {
            // do nothing
          }
        }

        if (OB_ITER_END == ret && param.file_id_ == file_id) {
          if (OB_FAIL(write_old_version_info_block_and_trailer_(param.file_id_, param.offset_, info_block_handler))) {
            CSR_LOG(ERROR, "write_old_version_trailer_ failed", K(ret));
          }
        } else {
          CSR_LOG(ERROR, "iter next_entry failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObIlogAccessor::write_old_version_info_block_and_trailer_(
    const file_id_t file_id, const offset_t offset, ObIndexInfoBlockHandler& info_block_handler)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(file_store_->open(file_id))) {
    CSR_LOG(ERROR, "open failed", K(ret), K(file_id));
  } else if (OB_FAIL(write_old_version_info_block_(file_id, offset, info_block_handler))) {
    CSR_LOG(ERROR, "write_old_version_info_block_ failed", K(ret), K(file_id), K(offset));
  } else if (OB_FAIL(write_old_version_trailer_(file_id, offset))) {
    CSR_LOG(ERROR, "write_old_version_trailer_ failed", K(ret), K(file_id), K(offset));
  } else if (OB_FAIL(file_store_->close())) {
    CSR_LOG(ERROR, "close_fd failed", K(ret));
  }
  return ret;
}

int ObIlogAccessor::write_old_version_info_block_(
    const file_id_t file_id, const offset_t offset, ObIndexInfoBlockHandler& info_block_handler)
{
  int ret = OB_SUCCESS;

  ObLogBlockMetaV2 meta;
  char* buf = buffer_.get_align_buf() + meta.get_serialize_size();
  int64_t buf_len = buffer_.get_size() - meta.get_serialize_size();
  int64_t data_len = 0;
  int64_t pos = 0;

  buffer_.reuse();
  if (OB_FAIL(info_block_handler.build_info_block(buf, buf_len, data_len))) {
    CSR_LOG(ERROR, "build_info_block failed", K(ret));
  } else if (OB_FAIL(meta.build_serialized_block(
                 buffer_.get_align_buf(), meta.get_serialize_size(), buf, data_len, OB_INFO_BLOCK, pos))) {
    CSR_LOG(ERROR, "build_serialized_block failed", K(ret));
  } else if (OB_FAIL(file_store_->write(buffer_.get_align_buf(), meta.get_total_len(), offset))) {
    CSR_LOG(ERROR, "write block failed. ", K(ret), K(file_id), K(offset), K(errno));
  } else {
    // success
  }

  return ret;
}

int ObIlogAccessor::write_old_version_trailer_(const file_id_t file_id, const offset_t offset)
{
  int ret = OB_SUCCESS;

  ObLogFileTrailer trailer;
  int64_t pos = 0;
  buffer_.reuse();

  if (OB_FAIL(trailer.build_serialized_trailer(buffer_.get_align_buf(), buffer_.get_size(), offset, file_id, pos))) {
    CSR_LOG(ERROR, "build_serialized_trailer failed", K(ret), K(file_id), K(offset));
  } else if (OB_FAIL(file_store_->write(buffer_.get_align_buf(), CLOG_TRAILER_SIZE, CLOG_TRAILER_OFFSET))) {
    CSR_LOG(ERROR, "write block failed. ", K(ret), K(errno));
  } else {
    // success
  }

  return ret;
}

int ObIlogAccessor::fill_file_id_cache_(const file_id_t file_id)
{
  int ret = OB_SUCCESS;
  IndexInfoBlockMap index_info_block_map;
  const bool update_old_version_max_file_id = true;
  if (!is_valid_file_id(file_id)) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid arguments", K(ret), K(file_id));
  } else if (OB_FAIL(get_index_info_block_map_(file_id, index_info_block_map, update_old_version_max_file_id))) {
    CSR_LOG(ERROR, "get_index_info_block_map_ failed", K(ret), K(file_id));
  } else if (OB_FAIL(file_id_cache_.append(file_id, index_info_block_map))) {
    CSR_LOG(ERROR, "file_id_cache_ append failed", K(ret), K(file_id));
  }
  return ret;
}

int ObIlogAccessor::get_index_info_block_map_(
    const file_id_t file_id, IndexInfoBlockMap& index_info_block_map, const bool update_old_version_max_file_id)
{
  int ret = OB_SUCCESS;
  ObLogReadFdHandle fd_handle;
  int64_t st_size = 0;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogStorage is not inited", K(ret));
  } else if (!is_valid_file_id(file_id)) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid arguments", K(ret), K(file_id));
  } else if (OB_FAIL(OB_LOG_FILE_READER.get_fd(file_store_->get_dir_name(), file_id, fd_handle))) {
    CSR_LOG(ERROR, "get_fd failed", K(ret), K(file_id));
  } else if (OB_FAIL(file_store_->get_file_st_size(file_id, st_size))) {
    CSR_LOG(ERROR, "fstat failed, io_error", K(ret), K(errno));
  } else {
    ObReadParam param;
    param.file_id_ = file_id;
    param.offset_ = static_cast<offset_t>(st_size - CLOG_TRAILER_SIZE);
    param.read_len_ = CLOG_TRAILER_SIZE;
    param.timeout_ = DEFAULT_READ_TIMEOUT;

    ObReadRes res;
    ObReadCost dummy_cost;

    int16_t magic_number = 0;
    int64_t pos = 0;
    ObReadBufGuard guard(ObModIds::OB_LOG_DIRECT_READER_ILOG_ID);
    ObReadBuf& rbuf = guard.get_read_buf();
    if (OB_UNLIKELY(!rbuf.is_valid())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      CSR_LOG(ERROR, "direct_reader_ alloc_buf failed", K(param), K(ret));
    } else if (OB_FAIL(direct_reader_.read_data_direct(param, rbuf, res, dummy_cost))) {
      CSR_LOG(ERROR, "read_data_direct failed", K(ret), K(param));
    } else if (res.data_len_ != param.read_len_) {
      ret = OB_ERR_UNEXPECTED;
      CSR_LOG(ERROR, "data_len not match", K(ret), K(res.data_len_), K(param.read_len_));
    } else if (OB_FAIL(serialization::decode_i16(res.buf_, res.data_len_, pos, &magic_number))) {
      CSR_LOG(ERROR, "decode_i16 failed", K(ret));
    } else if (magic_number == ObIlogFileTrailerV2::MAGIC_NUMBER) {
      // new version
      pos = 0;
      ObIlogFileTrailerV2 trailer;
      if (OB_FAIL(trailer.deserialize(res.buf_, res.data_len_, pos))) {
        CSR_LOG(ERROR, "new version ilog trailer deserialize failed", K(ret));
      } else {
        // direct_reader can't be used here, because the length of data which need to read
        // can be greater than OB_MAX_LOG_BUFFER_SIZE.
        const int64_t info_block_size = upper_align(trailer.get_info_block_size(), CLOG_DIO_ALIGN_SIZE);
        const int64_t MAX_ENTRY_NUM = info_block_size / index_info_block_map.item_size() + 1000;
        char* buf = NULL;
        pos = 0;
        int64_t read_size = -1;
        if (NULL == (buf = static_cast<char*>(
                         ob_malloc_align(CLOG_DIO_ALIGN_SIZE, info_block_size, ObModIds::OB_CLOG_INFO_BLK_HNDLR)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          CSR_LOG(ERROR, "ob_malloc_align failed", K(ret));

        } else if (OB_FAIL(OB_LOG_FILE_READER.pread(
                       fd_handle, buf, info_block_size, trailer.get_info_block_start_offset(), read_size))) {
          CSR_LOG(ERROR, "read not enough", K(ret), K(file_id), K(trailer), K(errno));
        } else if (info_block_size != read_size) {
          ret = OB_IO_ERROR;
          CSR_LOG(ERROR, "read not enough", K(ret), K(file_id), K(trailer), K(info_block_size), K(read_size));
        } else if (OB_FAIL(index_info_block_map.init(ObModIds::OB_CLOG_INFO_BLK_HNDLR, MAX_ENTRY_NUM))) {
          CSR_LOG(ERROR, "index_info_block_map init failed", K(ret));
        } else if (OB_FAIL(index_info_block_map.deserialize(buf, info_block_size, pos))) {
          CSR_LOG(ERROR, "index_info_block_map deserialize failed", K(ret));
        } else {
          // do nothing
        }

        if (NULL != buf) {
          ob_free_align(buf);
          buf = NULL;
        }
      }
    } else {
      // old version
      pos = 0;
      ObLogFileTrailer trailer;
      // Only thread which execute fill_file_id_cache can update old_version_max_file_id_.
      if (update_old_version_max_file_id) {
        ATOMIC_STORE(&old_version_max_file_id_, file_id);
      }
      if (OB_FAIL(trailer.deserialize(res.buf_, res.data_len_, pos))) {
        CSR_LOG(ERROR, "old version ilog trailer deserialize failed", K(ret));
      } else {
        ObLogBlockMetaV2 block;
        pos = 0;
        int64_t pos2 = 0;
        ObReadParam new_param;
        new_param.file_id_ = file_id;
        new_param.offset_ = trailer.get_start_pos();
        new_param.read_len_ = OB_MAX_LOG_BUFFER_SIZE;
        new_param.timeout_ = DEFAULT_READ_TIMEOUT;
        ObIndexInfoBlockHandler ilog_handler;

        if (OB_FAIL(ilog_handler.init())) {
          CSR_LOG(ERROR, "ilog_handler failed", K(ret));
        } else if (OB_FAIL(direct_reader_.read_data_direct(new_param, rbuf, res, dummy_cost))) {
          CSR_LOG(ERROR, "read_data_direct failed", K(ret), K(new_param));
        } else if (OB_FAIL(block.deserialize(res.buf_, res.data_len_, pos))) {
          CSR_LOG(ERROR, "block deserialize failed", K(ret));
        } else if (!block.check_meta_checksum()) {
          ret = OB_INVALID_DATA;
          CSR_LOG(ERROR, "check_meta_checksum failed", K(ret));
        } else if (!block.check_integrity(res.buf_ + block.get_serialize_size(), block.get_data_len())) {
          ret = OB_INVALID_DATA;
          CSR_LOG(ERROR, "check_integrity failed", K(ret));
        } else if (OB_FAIL(ilog_handler.resolve_info_block(res.buf_ + pos, block.get_data_len(), pos2))) {
          CSR_LOG(ERROR, "ilog_handler resolve_info_block failed", K(ret));
        } else if (OB_FAIL(ilog_handler.get_all_entry(index_info_block_map))) {
          CSR_LOG(ERROR, "ilog_handler get_all_entry failed", K(ret));
        }
        ilog_handler.destroy();
      }
    }
  }
  return ret;
}

bool ObIlogAccessor::is_new_version_ilog_file_(const file_id_t file_id) const
{
  const file_id_t old_version_max_file_id = ATOMIC_LOAD(&old_version_max_file_id_);
  return (OB_INVALID_FILE_ID == old_version_max_file_id) || (file_id > old_version_max_file_id);
}

int ObIlogAccessor::query_max_ilog_from_file_id_cache(
    const common::ObPartitionKey& partition_key, uint64_t& ret_max_ilog_id)
{
  int ret = OB_SUCCESS;
  const uint64_t LARGE_ENOUGH_LOG_ID = UINT64_MAX - 1;  // != OB_INVALID_ID
  Log2File prev_item;
  Log2File next_item;
  const bool locate_by_log_id = true;
  const int64_t query_value = static_cast<int64_t>(LARGE_ENOUGH_LOG_ID);

  ret = file_id_cache_.locate(partition_key, query_value, locate_by_log_id, prev_item, next_item);
  if (OB_ERR_OUT_OF_UPPER_BOUND == ret) {
    if (OB_INVALID_ID == prev_item.get_max_log_id()) {
      ret = OB_ERR_UNEXPECTED;
      CSR_LOG(ERROR, "prev_item max_log_id_ invalid", K(ret), K(partition_key), K(prev_item));
    } else {
      ret = OB_SUCCESS;
      ret_max_ilog_id = prev_item.get_max_log_id();
    }
  } else if (OB_PARTITION_NOT_EXIST == ret) {
    ret_max_ilog_id = 0;
    CSR_LOG(TRACE,
        "file id cache locate log fail, partition not exist",
        K(ret),
        K(partition_key),
        K(prev_item),
        K(next_item));
  } else {
    CSR_LOG(ERROR, "file id cache locate log fail", K(ret), K(partition_key), K(prev_item), K(next_item));
    ret = OB_ERR_UNEXPECTED;
  }
  CSR_LOG(TRACE, "query_max_ilog_from_file_id_cache finished", K(ret), K(partition_key), K(ret_max_ilog_id));
  return ret;
}

int ObIlogAccessor::ensure_log_continuous_in_file_id_cache(const ObPartitionKey& pkey, const uint64_t log_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogAccessor is not inited", K(ret), K(pkey));
  } else if (!pkey.is_valid() || OB_INVALID_ID == log_id) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(WARN, "invalid arguments", K(ret), K(pkey), K(log_id));
  } else if (OB_FAIL(file_id_cache_.ensure_log_continuous(pkey, log_id))) {
    CSR_LOG(ERROR, "file_id_cache_ ensure_log_continuous failed", K(ret), K(pkey), K(log_id));
  } else { /*do nothing*/
  }
  return ret;
}

int ObIlogAccessor::check_is_clog_obsoleted(
    const ObPartitionKey& pkey, const file_id_t file_id, const offset_t offset, bool& is_obsoleted) const
{
  int ret = OB_SUCCESS;
  file_id_t base_file_id = OB_INVALID_FILE_ID;
  offset_t base_offset = OB_INVALID_OFFSET;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogAccessor is not inited", K(ret), K(pkey));
  } else if (OB_UNLIKELY(OB_INVALID_FILE_ID == file_id || offset == OB_INVALID_OFFSET)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", KR(ret), K(pkey), K(file_id), K(offset));
  } else if (OB_FAIL(file_id_cache_.get_clog_base_pos(pkey, base_file_id, base_offset))) {
    CLOG_LOG(WARN, "failed to get_clog_base_pos", KR(ret), K(pkey), K(file_id), K(offset));
  } else if (OB_INVALID_FILE_ID == base_file_id) {
    is_obsoleted = false;
  } else if ((file_id < base_file_id) || (file_id == base_file_id && offset <= base_offset)) {
    is_obsoleted = true;
  } else {
    is_obsoleted = false;
  }
  return ret;
}

// Only return error when:
// 1) ObIlogAccessor is not initted;
// 2) arguments are invalid;
// 3) get_partition_saved_last_log_info failed and error
//    code is not OB_PARTITION_NOT_EXIST;
int ObIlogAccessor::check_partition_ilog_can_be_purged(const common::ObPartitionKey& partition_key,
    const int64_t max_decided_trans_version, const uint64_t item_max_log_id, const int64_t item_max_log_ts,
    bool& can_purge)
{
  int ret = OB_SUCCESS;
  can_purge = false;
  uint64_t last_replay_log_id = OB_INVALID_ID;
  int64_t unused_ts = OB_INVALID_TIMESTAMP;
  storage::ObIPartitionGroupGuard guard;
  if (false == inited_) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogAccessor is not init", K(ret));
  } else if (OB_UNLIKELY(!partition_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "check_partition_ilog_can_be_purged invalid arguments", K(ret), K(partition_key));
  } else if (OB_FAIL(storage::ObPartitionService::get_instance().get_partition_saved_last_log_info(
                 partition_key, last_replay_log_id, unused_ts)) &&
             OB_PARTITION_NOT_EXIST != ret) {
    CSR_LOG(ERROR, "get_saved_last_log_info failed", K(ret), K(partition_key));
  } else if (OB_PARTITION_NOT_EXIST == ret || item_max_log_id <= last_replay_log_id) {
    can_purge = true;
    // When partition not exist, return OB_SUCCESS
    ret = OB_SUCCESS;
    if (GCONF.enable_one_phase_commit) {
      can_purge = item_max_log_ts <= max_decided_trans_version;
    }
  }
  if (false == can_purge && REACH_TIME_INTERVAL(1000 * 1000)) {
    CSR_LOG(INFO,
        "cannot purge",
        K(partition_key),
        K(item_max_log_id),
        K(item_max_log_ts),
        K(last_replay_log_id),
        K(max_decided_trans_version));
  }
  return ret;
}

class ObIlogStorage::PurgeCheckFunctor {
public:
  PurgeCheckFunctor(ObIlogStorage* host, int64_t max_decided_trans_version, file_id_t file_id)
      : host_(host), max_decided_trans_version_(max_decided_trans_version), file_id_(file_id), can_purge_(true)
  {}

  ~PurgeCheckFunctor()
  {}

public:
  bool operator()(const common::ObPartitionKey& partition_key, const IndexInfoBlockEntry& index_info_block_entry)
  {
    int ret = OB_SUCCESS;

    if (can_purge_) {
      if (OB_UNLIKELY(OB_ISNULL(host_))) {
        can_purge_ = false;
        ret = OB_ERR_UNEXPECTED;
        CSR_LOG(ERROR, "PurgeCheckFunctor unexpected error, host_ mustn't be NULL", KP(host_));
      } else if (OB_FAIL(host_->check_partition_ilog_can_be_purged(partition_key,
                     max_decided_trans_version_,
                     index_info_block_entry.max_log_id_,
                     index_info_block_entry.max_log_timestamp_,
                     can_purge_))) {
        CLOG_LOG(ERROR,
            "check_partition_ilog_can_be_purged failed",
            K(ret),
            K(partition_key),
            K(max_decided_trans_version_),
            K(index_info_block_entry));
      }
    }
    return OB_SUCCESS == ret;
  }
  bool can_purge() const
  {
    return can_purge_;
  }

private:
  ObIlogStorage* host_;
  int64_t max_decided_trans_version_;
  file_id_t file_id_;
  bool can_purge_;

private:
  DISALLOW_COPY_AND_ASSIGN(PurgeCheckFunctor);
};

int ObIlogStorage::ObIlogStorageTimerTask::init(ObIlogStorage* ilog_storage)
{
  int ret = OB_SUCCESS;
  if (NULL == ilog_storage) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid argument", K(ret), KP(ilog_storage));
  } else {
    ilog_storage_ = ilog_storage;
  }
  return ret;
}

void ObIlogStorage::ObIlogStorageTimerTask::runTimerTask()
{
  wash_ilog_cache_();
  purge_stale_file_();
  purge_stale_ilog_index_();
}

void ObIlogStorage::ObIlogStorageTimerTask::wash_ilog_cache_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ilog_storage_)) {
    ret = OB_ERR_UNEXPECTED;
    CSR_LOG(ERROR, "null ilog_storage", K(ret));
  } else if (OB_FAIL(ilog_storage_->wash_ilog_cache())) {
    CSR_LOG(WARN, "ilog_storage_timer wash_ilog_cache failed", K(ret));
  } else {
    CSR_LOG(TRACE, "ilog_storage_timer wash_ilog_cache success");
  }
}

void ObIlogStorage::ObIlogStorageTimerTask::purge_stale_file_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ilog_storage_)) {
    ret = OB_ERR_UNEXPECTED;
    CSR_LOG(WARN, "null ilog_storage", K(ret));
  } else if (OB_FAIL(ilog_storage_->purge_stale_file())) {
    CSR_LOG(WARN, "ilog_storage_timer purge_stale_file failed", K(ret));
  } else {
    CSR_LOG(TRACE, "ilog_storage_timer pruge_stale_file success");
  }
}

void ObIlogStorage::ObIlogStorageTimerTask::purge_stale_ilog_index_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ilog_storage_)) {
    ret = OB_ERR_UNEXPECTED;
    CSR_LOG(WARN, "null ilog_storage", K(ret));
  } else if (OB_FAIL(ilog_storage_->purge_stale_ilog_index()) && OB_NEED_WAIT != ret) {
    CSR_LOG(WARN, "ilog_storage_timer purge_stale_ilog_index failed", K(ret));
  } else {
    CSR_LOG(INFO, "ilog_storage_timer pruge_stale_ilog_index_ success", K(ret));
  }
}

ObIlogStorage::ObIlogStorage()
    : is_inited_(false),
      partition_service_(NULL),
      commit_log_env_(NULL),
      ilog_store_(),
      pf_cache_builder_(),
      ilog_cache_(),
      task_()
{}

ObIlogStorage::~ObIlogStorage()
{
  destroy();
}

int ObIlogStorage::init(const char* dir_name, const char* shm_path, const int64_t server_seq,
    const common::ObAddr& addr, ObLogCache* log_cache, ObPartitionService* partition_service,
    ObCommitLogEnv* commit_log_env)
{
  int ret = OB_SUCCESS;

  ObIlogCacheConfig ilog_cache_config;
  ilog_cache_config.hold_file_count_limit_ = CURSOR_CACHE_HOLD_FILE_COUNT_LIMIT;
  file_id_t next_ilog_file_id = OB_INVALID_FILE_ID;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    CSR_LOG(ERROR, "ObIlogStorage init twice", K(ret));
  } else if (OB_ISNULL(dir_name) || OB_ISNULL(log_cache) || OB_ISNULL(partition_service) || OB_ISNULL(commit_log_env) ||
             OB_UNLIKELY(server_seq < 0 || !addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR,
        "invalid argument",
        KR(ret),
        KP(dir_name),
        KP(log_cache),
        KP(partition_service),
        KP(commit_log_env),
        K(server_seq),
        K(addr));
  } else if (OB_FAIL(ObIlogAccessor::init(dir_name, shm_path, server_seq, addr, log_cache))) {
    CSR_LOG(ERROR, "failed to init ObIlogAccessor", K(ret));
  } else if (OB_FAIL(init_next_ilog_file_id_(next_ilog_file_id))) {
    CSR_LOG(ERROR, "get_next_ilog_file_id failed", K(ret));
  } else if (OB_FAIL(ilog_store_.init(
                 next_ilog_file_id, file_store_, &file_id_cache_, &direct_reader_, partition_service))) {
    CSR_LOG(ERROR, "ilog_store_ init failed", K(ret));
  } else if (OB_FAIL(pf_cache_builder_.init(this, &file_id_cache_))) {
    CSR_LOG(ERROR, "pf_cache_builder_ init failed", K(ret));
  } else if (OB_FAIL(ilog_cache_.init(ilog_cache_config, &pf_cache_builder_))) {
    CSR_LOG(ERROR, "ilog_cache_ init failed", K(ret));
  } else if (OB_FAIL(task_.init(this))) {
    CSR_LOG(ERROR, "task_ init failed", K(ret));
  } else if (OB_FAIL(TG_START(lib::TGDefIDs::ILOGPurge))) {
    CSR_LOG(ERROR, "timer_ init failed", K(ret));
  } else {
    partition_service_ = partition_service;
    commit_log_env_ = commit_log_env;
    is_inited_ = true;
  }

  if (OB_SUCC(ret)) {
    CSR_LOG(INFO, "ObIlogStorage init success");
  } else {
    is_inited_ = false;
    CSR_LOG(ERROR, "ObIlogStorage init failed", K(ret));
  }
  return ret;
}

void ObIlogStorage::destroy()
{
  is_inited_ = false;

  partition_service_ = NULL;
  commit_log_env_ = NULL;
  ilog_store_.destroy();
  pf_cache_builder_.destroy();
  ilog_cache_.destroy();
  // tasks don't need to destory
  TG_DESTROY(lib::TGDefIDs::ILOGPurge);
  ObIlogAccessor::destroy();
}

int ObIlogStorage::start()
{
  int ret = OB_SUCCESS;
  const bool repeat = true;
  if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::ILOGPurge, task_, TIMER_TASK_INTERVAL, repeat))) {
    CSR_LOG(WARN, "fail to schedule task", K(ret));
  } else if (OB_FAIL(ilog_store_.start())) {
    CSR_LOG(WARN, "ilog_store_ start failed", K(ret));
  }
  CSR_LOG(INFO, "ObIlogStorage start finished", K(ret));
  return ret;
}

void ObIlogStorage::stop()
{
  TG_STOP(lib::TGDefIDs::ILOGPurge);
  ilog_store_.stop();
  CSR_LOG(INFO, "ObIlogStorage stop");
}

void ObIlogStorage::wait()
{
  TG_WAIT(lib::TGDefIDs::ILOGPurge);
  ilog_store_.wait();
  CSR_LOG(INFO, "ObIlogStorage wait");
}

int ObIlogStorage::get_cursor_batch(
    const common::ObPartitionKey& partition_key, const uint64_t query_log_id, ObGetCursorResult& result)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogStorage is not inited", K(ret));
  } else if (!partition_key.is_valid() || !is_valid_log_id(query_log_id)) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid arguments", K(ret), K(partition_key), K(query_log_id));
  } else if (OB_FAIL(get_cursor_from_memstore_(partition_key, query_log_id, result)) && OB_ENTRY_NOT_EXIST != ret &&
             OB_ERR_OUT_OF_UPPER_BOUND != ret) {
    CSR_LOG(ERROR, "get_cursor_from_memstore_ failed", K(ret), K(partition_key), K(query_log_id));
  } else if (OB_ERR_OUT_OF_UPPER_BOUND == ret || OB_SUCCESS == ret) {
    // do nothing
  } else {
    // Below: OB_ENTRY_NOT_EXIST == ret
    uint64_t min_log_id = OB_INVALID_ID;
    int64_t min_log_ts = OB_INVALID_TIMESTAMP;
    if (OB_SUCCESS != (tmp_ret = get_ilog_memstore_min_log_id_and_ts(partition_key, min_log_id, min_log_ts)) &&
        OB_PARTITION_NOT_EXIST != tmp_ret) {
      CSR_LOG(ERROR, "get_ilog_memstore_min_log_id_and_ts failed", K(ret), K(partition_key));
    }

    if (OB_FAIL(get_cursor_from_file_(partition_key, query_log_id, result))) {
      // subsequent code will judge the error number, determines if need tot return error.
      CSR_LOG(TRACE, "get_cursor_from_file_ failed", K(ret), K(partition_key), K(query_log_id));
      // handle logs betweent ilog_store and memstore.
      if (OB_ERR_OUT_OF_UPPER_BOUND == ret) {
        if (OB_SUCCESS == tmp_ret && query_log_id < min_log_id) {
          CSR_LOG(INFO,
              "get_cursor_from_file_ return OB_ERR_OUT_OF_UPPER_BOUND, but smaller than memstore min_log_id, rewrite "
              "return value",
              K(tmp_ret),
              K(query_log_id),
              K(min_log_id));
          ret = OB_CURSOR_NOT_EXIST;
        }
      }
    }
  }
  if (OB_SUCCESS != ret && OB_ERR_OUT_OF_UPPER_BOUND != ret && OB_CURSOR_NOT_EXIST != ret && OB_NEED_RETRY != ret &&
      OB_FILE_RECYCLED != ret) {
    CSR_LOG(ERROR, "get_cursor_batch return value unexpected", K(ret), K(partition_key), K(query_log_id));
  }
  CSR_LOG(TRACE, "get_cursor_batch finished", K(ret), K(partition_key), K(query_log_id), K(result));
  return ret;
}

int ObIlogStorage::get_cursor_batch_from_file(
    const common::ObPartitionKey& partition_key, const uint64_t query_log_id, ObGetCursorResult& result)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogStorage is not inited", K(ret));
  } else if (!partition_key.is_valid() || !is_valid_log_id(query_log_id)) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid arguments", K(ret), K(partition_key), K(query_log_id));
  } else if (OB_FAIL(get_cursor_from_file_(partition_key, query_log_id, result))) {
    CSR_LOG(TRACE, "get_cursor_from_file_ failed", K(ret), K(partition_key), K(query_log_id));
  }
  if (OB_SUCCESS != ret && OB_ERR_OUT_OF_UPPER_BOUND != ret && OB_CURSOR_NOT_EXIST != ret && OB_NEED_RETRY != ret &&
      OB_FILE_RECYCLED != ret) {
    CSR_LOG(ERROR, "get_cursor_batch_from_file return value unexpected", K(ret), K(partition_key), K(query_log_id));
  }
  CSR_LOG(TRACE, "get_cursor_batch_from_file finished", K(ret), K(partition_key), K(query_log_id), K(result));
  return ret;
}

int ObIlogStorage::get_cursor(
    const common::ObPartitionKey& partition_key, const uint64_t query_log_id, ObLogCursorExt& log_cursor_ext)
{
  int ret = OB_SUCCESS;
  const int64_t CSR_BATCH_SIZE = 1;
  char cursor_batch_buf[CSR_BATCH_SIZE * sizeof(ObLogCursorExt)];
  ObLogCursorExt* cursor_array = (ObLogCursorExt*)cursor_batch_buf;
  ObGetCursorResult result(cursor_array, CSR_BATCH_SIZE);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogStorage is not inited", K(ret));
  } else if (!partition_key.is_valid() || !is_valid_log_id(query_log_id)) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid arguments", K(ret), K(partition_key), K(query_log_id));
  } else if (OB_FAIL(get_cursor_batch(partition_key, query_log_id, result)) && OB_ERR_OUT_OF_UPPER_BOUND != ret &&
             OB_CURSOR_NOT_EXIST != ret && OB_NEED_RETRY != ret) {
    CSR_LOG(ERROR, "get_cursor_batch failed", K(ret), K(partition_key), K(query_log_id));
  } else if (OB_SUCCESS == ret) {
    log_cursor_ext = result.csr_arr_[0];
  } else {
    // do nothing
  }
  if (OB_SUCCESS != ret && OB_ERR_OUT_OF_UPPER_BOUND != ret && OB_CURSOR_NOT_EXIST != ret && OB_NEED_RETRY != ret) {
    CSR_LOG(ERROR, "get_cursor return value unexpected", K(ret), K(partition_key), K(query_log_id));
  }
  CLOG_LOG(TRACE, "get_cursor", K(ret), K(partition_key), K(query_log_id), K(log_cursor_ext));
  return ret;
}

int ObIlogStorage::submit_cursor(
    const common::ObPartitionKey& partition_key, const uint64_t log_id, const ObLogCursorExt& log_cursor_ext)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogStorage is not inited", K(ret));
  } else if (!partition_key.is_valid() || !is_valid_log_id(log_id) || !log_cursor_ext.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid arguments", K(ret), K(partition_key), K(log_id), K(log_cursor_ext));
  } else if (OB_FAIL(ilog_store_.submit_cursor(partition_key, log_id, log_cursor_ext))) {
    CSR_LOG(ERROR, "ilog_store_ submit_cursor failed", K(ret), K(partition_key), K(log_id), K(log_cursor_ext));
  } else {
    // do nothing
  }
  return ret;
}

int ObIlogStorage::submit_cursor(const common::ObPartitionKey& partition_key, const uint64_t log_id,
    const ObLogCursorExt& log_cursor_ext, const common::ObMemberList& memberlist, const int64_t replica_num,
    const int64_t memberlist_version)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogStorage is not inited", K(ret));
  } else if (!partition_key.is_valid() || !is_valid_log_id(log_id) || !log_cursor_ext.is_valid() ||
             !memberlist.is_valid() || replica_num <= 0 || memberlist_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR,
        "invalid arguments",
        K(ret),
        K(partition_key),
        K(log_id),
        K(log_cursor_ext),
        K(memberlist),
        K(replica_num),
        K(memberlist_version));
  } else if (OB_FAIL(ilog_store_.submit_cursor(
                 partition_key, log_id, log_cursor_ext, memberlist, replica_num, memberlist_version))) {
    CSR_LOG(ERROR,
        "ilog_store_ submit_cursor failed",
        K(ret),
        K(partition_key),
        K(log_id),
        K(log_cursor_ext),
        K(memberlist),
        K(replica_num),
        K(memberlist_version));
  } else {
    // do nothing
  }
  return ret;
}

int ObIlogStorage::query_max_ilog_id(const common::ObPartitionKey& partition_key, uint64_t& ret_max_ilog_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogStorage is not inited", K(ret));
  } else if (!partition_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid arguments", K(ret), K(partition_key));
  } else if (OB_FAIL(query_max_ilog_from_memstore_(partition_key, ret_max_ilog_id)) && OB_PARTITION_NOT_EXIST != ret) {
    CSR_LOG(ERROR, "query_max_ilog_from_memstore_ failed", K(ret), K(partition_key));
  } else if (OB_PARTITION_NOT_EXIST == ret) {
    if (OB_FAIL(query_max_ilog_from_file_id_cache(partition_key, ret_max_ilog_id)) && OB_PARTITION_NOT_EXIST != ret) {
      CSR_LOG(ERROR, "query_max_ilog_from_file_id_cache failed", K(ret), K(partition_key));
    }
  }
  if (OB_PARTITION_NOT_EXIST == ret) {
    CSR_LOG(INFO, "query_max_ilog_id partition not exist", K(ret), K(partition_key));
  }
  return ret;
}

int ObIlogStorage::query_max_flushed_ilog_id(const common::ObPartitionKey& partition_key, uint64_t& ret_max_ilog_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogStorage is not inited", K(ret));
  } else if (!partition_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid arguments", K(ret), K(partition_key));
  } else if (OB_FAIL(query_max_ilog_from_file_id_cache(partition_key, ret_max_ilog_id)) &&
             OB_PARTITION_NOT_EXIST != ret) {
    CSR_LOG(ERROR, "query_max_ilog_from_file_id_cache failed", K(ret), K(partition_key));
  } else {
    CSR_LOG(TRACE, "query_max_flushed_ilog_id finished", K(ret), K(partition_key), K(ret_max_ilog_id));
  }
  if (OB_SUCCESS != ret && OB_PARTITION_NOT_EXIST != ret) {
    CSR_LOG(ERROR, "query_max_flushed_ilog_id return value unexpected", K(ret), K(partition_key), K(ret_max_ilog_id));
  }
  return ret;
}

int ObIlogStorage::get_ilog_memstore_min_log_id_and_ts(
    const common::ObPartitionKey& partition_key, uint64_t& min_log_id, int64_t& min_log_ts) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogStorage is not inited", K(ret));
  } else if (!partition_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid arguments", K(ret));
  } else if (OB_FAIL(ilog_store_.get_memstore_min_log_id_and_ts(partition_key, min_log_id, min_log_ts)) &&
             OB_PARTITION_NOT_EXIST != ret) {
    CSR_LOG(ERROR, "get_memstore_min_log_id_and_ts failed", K(ret), K(partition_key));
  } else {
    // do nothing
  }
  return ret;
}

int ObIlogStorage::get_file_id_range(file_id_t& min_file_id, file_id_t& max_file_id) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogStorage is not inited", K(ret));
  } else if (OB_FAIL(file_store_->get_file_id_range(min_file_id, max_file_id))) {
    CSR_LOG(WARN, "get_file_id_range failed", K(ret));
  }
  return ret;
}

int ObIlogStorage::locate_by_timestamp(const common::ObPartitionKey& partition_key, const int64_t start_ts,
    uint64_t& target_log_id, int64_t& target_log_timestamp)
{
  int ret = OB_SUCCESS;
  Log2File prev_item;
  Log2File next_item;
  const bool locate_by_log_id = false;
  const int64_t query_value = start_ts;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogStorage is not inited", K(ret));
  } else if (!partition_key.is_valid() || start_ts <= 0) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid arguments", K(ret), K(partition_key), K(start_ts));
  } else {
    ret = file_id_cache_.locate(partition_key, query_value, locate_by_log_id, prev_item, next_item);
    if (OB_SUCCESS == ret || OB_ERR_OUT_OF_UPPER_BOUND == ret) {
      if (!prev_item.contain_timestamp(start_ts)) {
        ret = handle_locate_between_files_(prev_item, next_item, target_log_id, target_log_timestamp);
      } else {
        const uint64_t min_log_id = prev_item.get_min_log_id();
        const uint64_t max_log_id = prev_item.get_max_log_id();
        if (!is_valid_log_id(min_log_id) || !is_valid_log_id(max_log_id)) {
          ret = OB_ERR_UNEXPECTED;
          CSR_LOG(ERROR, "log_id is invalid", K(ret), K(min_log_id), K(max_log_id), K(partition_key), K(prev_item));
        } else {
          const int64_t CSR_BATCH_SIZE = max_log_id - min_log_id + 1;
          ObLogCursorExt* cursor_array =
              (ObLogCursorExt*)ob_malloc(CSR_BATCH_SIZE * sizeof(ObLogCursorExt), "ObIlogLocate");
          const ObLogCursorExt* target_cursor = NULL;
          if (OB_ISNULL(cursor_array)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            CSR_LOG(ERROR, "alloc cursor_array failed", K(ret));
          } else {
            ObGetCursorResult result(cursor_array, CSR_BATCH_SIZE);
            if (OB_FAIL(prepare_cursor_result_for_locate_(partition_key, prev_item, result))) {
              CSR_LOG(WARN, "prepare_cursor_result_for_locate_ failed", K(ret), K(partition_key), K(prev_item));
            } else if (OB_FAIL(search_cursor_result_for_locate_(result, start_ts, target_cursor)) &&
                       OB_ERR_OUT_OF_UPPER_BOUND != ret) {
              CSR_LOG(WARN, "search_cursor_result_for_locate_ failed", K(ret));
            }

            if (OB_SUCCESS == ret) {
              if (NULL != target_cursor) {
                target_log_id = min_log_id + (target_cursor - cursor_array);
                target_log_timestamp = target_cursor->get_submit_timestamp();
              } else {
                ret = OB_ERR_UNEXPECTED;
                CSR_LOG(ERROR, "target_cursor is NULL, unexpected", K(ret));
              }
            } else if (OB_ERR_OUT_OF_UPPER_BOUND == ret) {
              ret = handle_locate_between_files_(prev_item, next_item, target_log_id, target_log_timestamp);
            } else {
              // do nothing
            }
          }
          if (NULL != cursor_array) {
            ob_free(cursor_array);
            cursor_array = NULL;
          }
        }
      }
    } else if (OB_ERR_OUT_OF_LOWER_BOUND == ret) {
      target_log_id = next_item.get_min_log_id();
      target_log_timestamp = next_item.get_min_log_timestamp();
    } else if (OB_PARTITION_NOT_EXIST == ret) {
      // do nothing
    } else {
      // do nothing
    }
  }
  if (OB_SUCCESS != ret && OB_ERR_OUT_OF_UPPER_BOUND != ret && OB_ERR_OUT_OF_LOWER_BOUND != ret &&
      OB_PARTITION_NOT_EXIST != ret && OB_NEED_RETRY != ret) {
    CSR_LOG(ERROR, "locate_by_timestamp return value unexpected", K(ret), K(partition_key), K(start_ts));
  }
  CSR_LOG(
      TRACE, "locate_by_timestamp", K(ret), K(partition_key), K(start_ts), K(target_log_id), K(target_log_timestamp));
  return ret;
}

int ObIlogStorage::locate_ilog_file_by_log_id(
    const common::ObPartitionKey& partition_key, const uint64_t start_log_id, uint64_t& end_log_id, file_id_t& file_id)
{
  int ret = OB_SUCCESS;
  Log2File prev_item;
  Log2File next_item;
  const bool locate_by_log_id = true;
  const int64_t query_value = start_log_id;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogStorage is not inited", K(ret));
  } else if (!partition_key.is_valid() || !is_valid_log_id(start_log_id)) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid arguments", K(ret), K(partition_key), K(start_log_id));
  } else {
    ret = file_id_cache_.locate(partition_key, query_value, locate_by_log_id, prev_item, next_item);
  }

  if (OB_SUCCESS == ret) {
    if (OB_LIKELY(prev_item.contain_log_id(start_log_id))) {
      end_log_id = prev_item.get_max_log_id();
      file_id = prev_item.get_file_id();
    }
  }

  return ret;
}

int ObIlogStorage::wash_ilog_cache()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogStorage is not inited", K(ret));
  } else if (OB_FAIL(ilog_cache_.timer_wash())) {
    CSR_LOG(ERROR, "ilog_cache_ timer_wash", K(ret));
  }
  return ret;
}

int ObIlogStorage::purge_stale_file()
{
  int ret = OB_SUCCESS;
  file_id_t min_ilog_file_id = OB_INVALID_FILE_ID;
  file_id_t max_ilog_file_id = OB_INVALID_FILE_ID;
  int64_t max_decided_trans_version = OB_INVALID_TIMESTAMP;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogStorage is not inited", K(ret));
  } else if (OB_FAIL(file_store_->get_file_id_range(min_ilog_file_id, max_ilog_file_id)) && OB_ENTRY_NOT_EXIST != ret) {
    CSR_LOG(WARN, "ilog_dir_ get_file_id_range failed", K(ret));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    // do nothing;
    CSR_LOG(INFO, "get_file_id_range return OB_ENTRY_NOT_EXIST, do nothing", K(ret));
  } else if (OB_FAIL(partition_service_->get_global_max_decided_trans_version(max_decided_trans_version))) {
    CSR_LOG(ERROR, "get_global_max_decided_trans_version failed", K(ret));
  } else {
    bool can_purge = true;
    file_id_t file_id = OB_INVALID_FILE_ID;
    for (file_id = min_ilog_file_id; can_purge && OB_SUCC(ret) && file_id < max_ilog_file_id; file_id++) {
      if (OB_FAIL(purge_stale_file_(file_id, max_decided_trans_version, can_purge))) {
        CSR_LOG(ERROR, "purge_stale_file_ failed", K(ret));
      } else if (can_purge) {
        file_store_->update_min_file_id(file_id + 1);
      }
    }
  }
  return ret;
}

int ObIlogStorage::purge_stale_ilog_index()
{
  int ret = OB_SUCCESS;
  const int64_t begin_ts = ObClockGenerator::getClock();
  const int64_t ilog_index_expire_time = ObServerConfig::get_instance().ilog_index_expire_time;
  const int64_t can_purge_ilog_index_min_timestamp = ObClockGenerator::getClock() - ilog_index_expire_time;
  int64_t max_decided_trans_version = OB_INVALID_TIMESTAMP;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogStorage is not inited", K(ret));
  } else if (OB_FAIL(partition_service_->get_global_max_decided_trans_version(max_decided_trans_version))) {
    CSR_LOG(ERROR, "get_global_max_decided_trans_version failed", K(ret));
  } else if (OB_FAIL(do_purge_stale_ilog_index_(max_decided_trans_version, can_purge_ilog_index_min_timestamp)) &&
             OB_NEED_WAIT != ret) {
    CSR_LOG(
        ERROR, "do_purge_stale_ilog_index_ failed", K(ret), K(max_decided_trans_version), K(ilog_index_expire_time));
  } else {
    CSR_LOG(TRACE, "purge_stale_ilog_index success", K(ret), K(ilog_index_expire_time), K(max_decided_trans_version));
  }
  const int64_t cost_ts = ObClockGenerator::getClock() - begin_ts;
  CSR_LOG(INFO,
      "purge_stale_ilog_index_ finish",
      K(ret),
      K(cost_ts),
      K(ilog_index_expire_time),
      K(can_purge_ilog_index_min_timestamp));
  return ret;
}

ObIRawIndexIterator* ObIlogStorage::alloc_raw_index_iterator(
    const file_id_t start_file_id, const file_id_t end_file_id, const offset_t offset)
{
  int ret = OB_SUCCESS;
  ObRawIndexIterator* iter = NULL;
  const int64_t DEFAULT_TIMEOUT = 10 * 1000 * 1000;  // 10s
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogStorage is not inited", K(ret));
  } else if (OB_INVALID_FILE_ID == start_file_id || OB_INVALID_FILE_ID == end_file_id || OB_INVALID_OFFSET == offset) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid arguments", K(ret), K(start_file_id), K(end_file_id), K(offset));
  } else if (NULL == (iter = OB_NEW(ObRawIndexIterator, ObModIds::OB_ILOG_STORAGE_ITERATOR))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CSR_LOG(ERROR, "allow ObRawIndexIterator failed", K(ret), K(end_file_id), K(start_file_id), K(offset));
  } else if (OB_FAIL(iter->init(&direct_reader_, start_file_id, offset, end_file_id, DEFAULT_TIMEOUT))) {
    CSR_LOG(ERROR, "iter init failed", K(ret), K(start_file_id), K(end_file_id), K(offset));
    revert_raw_index_iterator(iter);
    iter = NULL;
  } else {
    // do nothing;
  }
  return iter;
}

void ObIlogStorage::revert_raw_index_iterator(ObIRawIndexIterator* iter)
{
  if (NULL != iter) {
    OB_DELETE(ObIRawIndexIterator, ObModIds::OB_ILOG_STORAGE_ITERATOR, iter);
    iter = NULL;
  }
}

int ObIlogStorage::get_used_disk_space(int64_t& used_space) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "get_used_disk_space is not inited", K(ret));
  } else if (OB_FAIL(file_store_->get_total_used_size(used_space))) {
    CSR_LOG(WARN, "ilog_dir_ get_total_size failed", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObIlogStorage::init_next_ilog_file_id_(file_id_t& next_ilog_file_id) const
{
  int ret = OB_SUCCESS;
  // do not check IS_NOT_INIT
  file_id_t min_ilog_file_id = OB_INVALID_FILE_ID;
  file_id_t max_ilog_file_id = OB_INVALID_FILE_ID;
  if (OB_FAIL(file_store_->get_file_id_range(min_ilog_file_id, max_ilog_file_id)) && OB_ENTRY_NOT_EXIST != ret &&
      OB_NO_SUCH_FILE_OR_DIRECTORY != ret) {
    CSR_LOG(ERROR, "ilog_dir_ get_max_file_id failed", K(ret));
  } else if (OB_ENTRY_NOT_EXIST == ret || OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
    ret = OB_SUCCESS;
    next_ilog_file_id = 1;
  } else {
    next_ilog_file_id = max_ilog_file_id + 1;
    file_store_->update_min_file_id(min_ilog_file_id);
    file_store_->update_max_file_id(max_ilog_file_id);
  }
  return ret;
}

int ObIlogStorage::get_next_ilog_file_id_from_memory(file_id_t& next_ilog_file_id) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogStorage is not inited", K(ret));
  } else {
    next_ilog_file_id = ilog_store_.get_next_ilog_file_id();
  }
  return ret;
}

int ObIlogStorage::get_cursor_from_memstore_(
    const common::ObPartitionKey& partition_key, const uint64_t query_log_id, ObGetCursorResult& result)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogStorage is not inited", K(ret));
  } else if (!partition_key.is_valid() || query_log_id <= 0 || query_log_id == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid arguments", K(ret), K(partition_key), K(query_log_id));
  } else if (OB_FAIL(ilog_store_.get_cursor_from_memstore(partition_key, query_log_id, result)) &&
             OB_ENTRY_NOT_EXIST != ret && OB_ERR_OUT_OF_UPPER_BOUND != ret) {
    CSR_LOG(ERROR, "get_cursor_from_memstore_ failed", K(ret), K(partition_key), K(query_log_id));
  }
  return ret;
}

int ObIlogStorage::get_cursor_from_file_(
    const common::ObPartitionKey& partition_key, const uint64_t query_log_id, ObGetCursorResult& result)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogStorage is not inited", K(ret));
  } else if (!partition_key.is_valid() || !is_valid_log_id(query_log_id)) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid arguments", K(ret), K(partition_key), K(query_log_id));
  } else {
    Log2File prev_item;
    Log2File next_item;
    const bool locate_by_log_id = true;
    const int64_t query_value = static_cast<int64_t>(query_log_id);

    ret = file_id_cache_.locate(partition_key, query_value, locate_by_log_id, prev_item, next_item);
    int file_id_cache_locate_err = ret;
    if (OB_SUCCESS == ret || OB_ERR_OUT_OF_UPPER_BOUND == ret) {
      ret = OB_SUCCESS;
      bool prev_item_contain_target_log = prev_item.contain_log_id(query_log_id);
      if (!prev_item_contain_target_log) {
        ret = OB_CURSOR_NOT_EXIST;
      } else if (!is_new_version_ilog_file_(prev_item.get_file_id())) {
        if (OB_FAIL(get_cursor_from_ilog_cache_(partition_key, query_log_id, prev_item, result))) {
          CSR_LOG(WARN, "get_cursor_from_ilog_cache_ failed", K(ret), K(partition_key), K(query_log_id), K(prev_item));
        }
      } else {
        if (OB_FAIL(get_cursor_from_ilog_file_(partition_key, query_log_id, prev_item, result))) {
          CSR_LOG(WARN, "get_cursor_from_ilog_file_ failed", K(ret), K(partition_key), K(query_log_id), K(prev_item));
        }
      }

      if (OB_CURSOR_NOT_EXIST == ret) {
        if (OB_ERR_OUT_OF_UPPER_BOUND == file_id_cache_locate_err) {
          ret = OB_ERR_OUT_OF_UPPER_BOUND;
        } else {
          ret = OB_CURSOR_NOT_EXIST;
        }
      }
    } else if (OB_PARTITION_NOT_EXIST == ret || OB_ERR_OUT_OF_LOWER_BOUND == ret) {
      ret = OB_CURSOR_NOT_EXIST;
    }
  }
  return ret;
}

int ObIlogStorage::get_cursor_from_ilog_file_(const common::ObPartitionKey& partition_key, const uint64_t query_log_id,
    const Log2File& log2file_item, ObGetCursorResult& result)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogStorage is not inited", K(ret));
  } else if (!partition_key.is_valid() || query_log_id <= 0 || query_log_id == OB_INVALID_ID ||
             !log2file_item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid arguments", K(ret), K(partition_key), K(query_log_id), K(log2file_item));
  } else if (OB_FAIL(ilog_store_.get_cursor_from_ilog_file(partition_key, query_log_id, log2file_item, result))) {
    CSR_LOG(WARN,
        "ilog_store_ get_cursor_from_ilog_file failed",
        K(ret),
        K(partition_key),
        K(query_log_id),
        K(log2file_item));
  }
  return ret;
}

int ObIlogStorage::get_cursor_from_ilog_cache_(const common::ObPartitionKey& partition_key, const uint64_t query_log_id,
    const Log2File& log2file_item, ObGetCursorResult& result)
{
  int ret = OB_SUCCESS;
  const bool is_backfilled = (log2file_item.get_start_offset() != OB_INVALID_OFFSET);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogStorage is not inited", K(ret));
  } else if (!partition_key.is_valid() || query_log_id <= 0 || query_log_id == OB_INVALID_ID ||
             !log2file_item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid arguments", K(ret), K(partition_key), K(query_log_id), K(log2file_item));
  } else if (is_backfilled) {
    const uint64_t min_log_id = log2file_item.get_min_log_id();
    const uint64_t max_log_id = log2file_item.get_max_log_id();
    const offset_t start_offset_index = log2file_item.get_start_offset();
    ObIlogStorageQueryCost dummy_cost;

    if (OB_FAIL(ilog_cache_.get_cursor(log2file_item.get_file_id(),
            partition_key,
            query_log_id,
            min_log_id,
            max_log_id,
            start_offset_index,
            result,
            dummy_cost))) {
      CSR_LOG(WARN, "ObIlogCache get_cursor error", K(ret), K(partition_key), K(query_log_id), K(log2file_item));
    } else {
      CSR_LOG(TRACE, "ObIlogCache get_cursor success", K(partition_key), K(query_log_id), K(log2file_item), K(result));
    }
  } else {  // not backfilled, this file never loaded
    if (OB_FAIL(ilog_cache_.prepare_cache_node(log2file_item.get_file_id()))) {
      CSR_LOG(
          WARN, "ObIlogCache prepare_cache_node failed", K(ret), K(partition_key), K(query_log_id), K(log2file_item));
    } else {
      ret = OB_NEED_RETRY;
      CSR_LOG(INFO,
          "ObIlogCache prepare_cache_node success, need_retry",
          K(ret),
          K(partition_key),
          K(query_log_id),
          K(log2file_item));
    }
  }
  return ret;
}

int ObIlogStorage::query_max_ilog_from_memstore_(const common::ObPartitionKey& partition_key, uint64_t& ret_max_ilog_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogStorage is not inited", K(ret));
  } else if (!partition_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid arguments", K(ret), K(partition_key));
  } else if (OB_FAIL(ilog_store_.get_max_ilog_from_memstore(partition_key, ret_max_ilog_id)) &&
             OB_PARTITION_NOT_EXIST != ret) {
    CSR_LOG(ERROR, "get_max_ilog_from_memstore failed", K(ret), K(partition_key));
  }
  return ret;
}

int ObIlogStorage::handle_locate_between_files_(
    const Log2File& prev_item, const Log2File& next_item, uint64_t& target_log_id, int64_t& target_log_timestamp)
{
  int ret = OB_SUCCESS;

  if (!next_item.is_valid()) {
    ret = OB_ERR_OUT_OF_UPPER_BOUND;
    prev_item.get_max_log_info(target_log_id, target_log_timestamp);
  } else if (!prev_item.is_preceding_to(next_item)) {
    ret = OB_ERR_OUT_OF_LOWER_BOUND;
    next_item.get_min_log_info(target_log_id, target_log_timestamp);
  } else {
    ret = OB_SUCCESS;
    next_item.get_min_log_info(target_log_id, target_log_timestamp);
  }
  return ret;
}

int ObIlogStorage::get_index_info_block_map(const file_id_t file_id, IndexInfoBlockMap& index_info_block_map)
{
  const bool update_old_version_max_file_id = false;
  return get_index_info_block_map_(file_id, index_info_block_map, update_old_version_max_file_id);
}

int ObIlogStorage::purge_stale_file_(const file_id_t file_id, const int64_t max_decided_trans_version, bool& can_purge)
{
  int ret = OB_SUCCESS;

  IndexInfoBlockMap index_info_block_map;
  PurgeCheckFunctor functor(this, max_decided_trans_version, file_id);
  const bool update_old_version_max_file_id = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CSR_LOG(ERROR, "ObIlogStorage is not inited", K(ret));
  } else if (!is_valid_file_id(file_id) || OB_INVALID_ARGUMENT == max_decided_trans_version) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid arguments", K(ret), K(file_id), K(max_decided_trans_version));
  } else if (OB_FAIL(check_modify_time_for_purge_(file_id, can_purge))) {
    CSR_LOG(ERROR, "check_modify_time_for_purge_ failed", K(ret));
  } else if (true == can_purge) {
    if (OB_FAIL(get_index_info_block_map_(file_id, index_info_block_map, update_old_version_max_file_id))) {
      CSR_LOG(ERROR, "get_index_info_block_map_ failed", K(ret), K(file_id));
    } else if (OB_FAIL(index_info_block_map.for_each(functor))) {
      CSR_LOG(ERROR, "index_info_block_map for_each failed", K(ret));
    } else {
      can_purge = functor.can_purge();

#ifdef ERRSIM
      can_purge = can_purge && ObServerConfig::get_instance().enable_ilog_recycle;
#endif

      if (can_purge && OB_SUCC(ret)) {
        if (OB_FAIL(do_purge_stale_ilog_file_(file_id))) {
          CSR_LOG(ERROR, "do_purge_stale_ilog_fil_ failed", K(ret), K(file_id));
        } else {
          CSR_LOG(INFO, "finish purge ilog file ", K(ret), K(file_id));
        }
      }
    }
  }

  return ret;
}

int ObIlogStorage::check_modify_time_for_purge_(const file_id_t file_id, bool& can_purge)
{
  int ret = OB_SUCCESS;

  time_t ilog_mtime = 0;
  time_t min_clog_mtime = 0;
  if (OB_FAIL(get_ilog_file_mtime_(file_id, ilog_mtime))) {
    CSR_LOG(ERROR, "get_ilog_file_mtime_ failed", K(ret), K(file_id));
  } else if (OB_FAIL(get_min_clog_file_mtime_(min_clog_mtime))) {
    CSR_LOG(ERROR, "get_min_clog_file_mtime_ failed", K(ret));
  } else {
    can_purge = (ilog_mtime < min_clog_mtime);
    if (!can_purge) {
      CSR_LOG(INFO, "cannot purge", K(ret), K(file_id), K(ilog_mtime), K(min_clog_mtime));
    }
  }

  return ret;
}

int ObIlogStorage::get_ilog_file_mtime_(const file_id_t file_id, time_t& ilog_time)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(file_store_->get_file_st_time(file_id, ilog_time))) {
    CSR_LOG(WARN, "get_file_st_time failed", K(ret), K(errno));
  }
  return ret;
}

int ObIlogStorage::get_min_clog_file_mtime_(time_t& min_clog_mtime)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(commit_log_env_->get_min_file_mtime(min_clog_mtime))) {
    CSR_LOG(ERROR, "clog_env get_min_file_mtime failed", K(ret));
  }

  return ret;
}

int ObIlogStorage::do_purge_stale_ilog_file_(const file_id_t file_id)
{
  int ret = OB_SUCCESS;
  ObFileIdCachePurgeByFileId purge_strategy(file_id, file_id_cache_);
  if (OB_FAIL(file_id_cache_.purge(purge_strategy))) {
    CSR_LOG(ERROR, "file_id_cache_ purge failed", K(ret), K(purge_strategy));
  } else if (OB_FAIL(file_store_->delete_file(file_id))) {
    CSR_LOG(WARN, "purge failed", K(ret), K(file_id), K(errno));
  }
  return ret;
}

int ObIlogStorage::do_purge_stale_ilog_index_(
    const int64_t max_decided_trans_version, const int64_t can_purge_ilog_index_min_timestamp)
{
  int ret = OB_SUCCESS;
  ObFileIdCachePurgeByTimestamp purge_strategy(
      max_decided_trans_version, can_purge_ilog_index_min_timestamp, *this, file_id_cache_);
  if (OB_FAIL(file_id_cache_.purge(purge_strategy)) && OB_NEED_WAIT != ret) {
    CSR_LOG(ERROR, "file_id_cache_ purge failed", K(ret), K(purge_strategy));
  } else if (OB_NEED_WAIT == ret && REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
    CSR_LOG(INFO, "no need purge ilog index");
  }
  return ret;
}

int ObIlogStorage::prepare_cursor_result_for_locate_(
    const common::ObPartitionKey& partition_key, const Log2File& item, ObGetCursorResult& result)
{
  int ret = OB_SUCCESS;
  while (result.ret_len_ < result.arr_len_ && OB_SUCC(ret)) {
    ObGetCursorResult tmp_result(result.csr_arr_ + result.ret_len_, result.arr_len_ - result.ret_len_);
    const uint64_t min_log_id = item.get_min_log_id() + result.ret_len_;
    if (!is_new_version_ilog_file_(item.get_file_id())) {
      if (OB_FAIL(get_cursor_from_ilog_cache_(partition_key, min_log_id, item, tmp_result))) {
        CSR_LOG(WARN, "get_cursor_from_ilog_cache_ failed", K(ret), K(partition_key), K(min_log_id), K(item));
      }
    } else {
      if (OB_FAIL(get_cursor_from_ilog_file_(partition_key, min_log_id, item, tmp_result))) {
        CSR_LOG(WARN, "get_cursor_from_ilog_file_ failed", K(ret), K(partition_key), K(min_log_id), K(item));
      }
    }

    if (OB_SUCC(ret)) {
      result.ret_len_ = result.ret_len_ + tmp_result.ret_len_;
    }
  }
  return ret;
}

int ObIlogStorage::search_cursor_result_for_locate_(
    const ObGetCursorResult& result, const int64_t start_ts, const ObLogCursorExt*& target_cursor) const
{
  int ret = OB_SUCCESS;
  const ObLogCursorExt* csr_arr = result.csr_arr_;
  const int64_t array_len = result.ret_len_;
  if (OB_ISNULL(csr_arr) || OB_UNLIKELY(array_len <= 0) || OB_UNLIKELY(OB_INVALID_TIMESTAMP == start_ts)) {
    CSR_LOG(WARN, "invalid argument", K(csr_arr), K(array_len));
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t begin = 0;
    int64_t end = array_len - 1;

    while (begin <= end) {
      int64_t middle = ((begin + end) / 2);
      if (csr_arr[middle].get_submit_timestamp() >= start_ts) {
        end = middle - 1;
      } else {
        begin = middle + 1;
      }
    }

    if (end + 1 < array_len) {
      target_cursor = csr_arr + end + 1;
    } else {
      ret = OB_ERR_OUT_OF_UPPER_BOUND;
    }
  }
  return ret;
}
}  // namespace clog
}  // namespace oceanbase
