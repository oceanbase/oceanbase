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

#define USING_LOG_PREFIX STORAGE

#include "storage/ddl/ob_cg_block_tmp_file.h"
#include "storage/tmp_file/ob_tmp_file_manager.h"
#include "storage/blocksstable/index_block/ob_macro_meta_temp_store.h"
#include "storage/tmp_file/ob_tmp_file_manager.h"
#include "share/io/ob_io_manager.h"

namespace oceanbase
{
using namespace tmp_file;
namespace storage
{
static int64_t get_tmp_file_io_timeout_ms()
{
  return GCTX.is_shared_storage_mode() ?
         OB_IO_MANAGER.get_object_storage_io_timeout_ms(MTL_ID()) :
         GCONF._data_storage_io_timeout / 1000L;
}

/**
* -----------------------------------ObCGBlock::StoreHeader-----------------------------------
*/
void ObCGBlock::StoreHeader::reset()
{
  store_version_ = static_cast<uint64_t>(UNKNOWN_VERSION);
  is_complete_macro_block_ = static_cast<uint64_t>(false);
  reserved_ = 0;
  macro_buffer_size_ = 0;
}

int ObCGBlock::StoreHeader::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || buf_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("there are invalid arguments", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode_i64(buf,
                                               buf_len,
                                               pos,
                                               header_))) {
    LOG_WARN("fail to serialize store header", K(ret));
  }
  return ret;
}

int ObCGBlock::StoreHeader::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || data_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("there are invalid arguments", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_i64(buf,
                                               data_len,
                                               pos,
                                               &header_))) {
    LOG_WARN("fail to deserialize store header", K(ret));
  }
  return ret;
}

int64_t ObCGBlock::StoreHeader::get_serialize_size() const
{
  return serialization::encoded_length_i64(header_);
}

/**
* -----------------------------------ObCGBlock-----------------------------------
*/
int ObCGBlock::init(const char *macro_block_buffer,
                    const StoredVersion store_version,
                    const bool is_complete_macro_block,
                    const int64_t macro_buffer_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cg block has been initialized", K(ret));
  } else if (OB_UNLIKELY(nullptr == macro_block_buffer ||
                         UNKNOWN_VERSION == store_version ||
                         macro_buffer_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("there are invalid arguments",
        K(ret), KP(macro_block_buffer), K(store_version), K(macro_buffer_size));
  } else {
    store_header_.store_version_ = static_cast<uint64_t>(store_version);
    store_header_.is_complete_macro_block_ = static_cast<uint64_t>(is_complete_macro_block);
    store_header_.macro_buffer_size_ = macro_buffer_size;
    macro_block_buffer_ = macro_block_buffer;
    cg_block_offset_ = 0;
    micro_block_idx_ = 0;
    is_inited_ = true;
  }
  return ret;
}

int ObCGBlock::init(const StoreHeader &store_header)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cg block has been initialized", K(ret));
  } else if (OB_UNLIKELY(!store_header.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("there are invalid arguments", K(ret), K(store_header));
  } else {
    store_header_ = store_header;
    cg_block_offset_ = 0;
    micro_block_idx_ = 0;
    void *buf = nullptr;
    int64_t buf_size = store_header.macro_buffer_size_ + store_header.get_serialize_size();
    if (OB_ISNULL(buf = allocator_.alloc(buf_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret), K(store_header.macro_buffer_size_));
    } else {
      macro_block_buffer_ = static_cast<char *>(buf);
      is_inited_ = true;
    }
  }
  return ret;
}

void ObCGBlock::reset()
{
  is_inited_ = false;
  store_header_.reset();
  cg_block_offset_ = 0;
  micro_block_idx_ = 0;
  allocator_.reset();
  macro_block_buffer_ = nullptr;
}

 /**
 * -----------------------------------CgBlockFile::BlockStore-----------------------------------
 */
int ObCGBlockFile::BlockStore::open()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("block store file initialized twice", K(ret));
  } else if (OB_FAIL(MTL(ObTenantTmpFileManager*)->alloc_dir(file_dir_))) {
    LOG_WARN("failed to alloc file dir", K(ret));
  } else if (OB_FAIL(MTL(ObTenantTmpFileManager*)->open(fd_, file_dir_, nullptr/*label*/))) {
    LOG_WARN("failed to open tmp file", K(ret), K(file_dir_));
  } else {
    file_size_ = 0;
    file_offset_ = 0;
    remain_data_size_ = 0;
    curr_cg_block_offset_ = 0;
    curr_cg_block_micro_idx_ = 0;
    is_inited_ = true;
  }
  return ret;
}

int ObCGBlockFile::BlockStore::close()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("cg block tmp file do not initialized", K(ret));
  } else if (OB_FAIL(MTL(ObTenantTmpFileManager*)->remove(fd_))) {
    LOG_WARN("fail to remove tmp file fd", K(ret), K(fd_));
  } else {
    is_inited_ = false;
    LOG_INFO("success to close cg block tmp file", K(ret), K(file_dir_), K(fd_));
  }
  return ret;
}

int ObCGBlockFile::BlockStore::append_cg_block(const ObCGBlock &cg_block)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("block store is not initialized", K(ret));
  } else if (OB_UNLIKELY(!cg_block.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cg block is not initialized", K(ret), K(cg_block));
  } else if (OB_FAIL(write_cg_block_header(cg_block.get_store_header()))) {
    LOG_WARN("fail to write cg block header", K(ret), K(cg_block));
  } else {
    const char *buf = cg_block.get_macro_block_buffer();
    const int64_t macro_buffer_size = cg_block.get_macro_buffer_size();
    const int64_t timeout_ms = get_tmp_file_io_timeout_ms();
    tmp_file::ObTmpFileIOInfo io_info;
    if (OB_FAIL(get_io_info(buf, macro_buffer_size, timeout_ms, io_info))) {
      LOG_WARN("fail to get io info", K(ret));
    } else if (OB_FAIL(MTL(ObTenantTmpFileManager*)->write(MTL_ID(), io_info))) {
      LOG_WARN("fail to write cg block data", K(ret), K(io_info));
    } else {
      file_size_ += macro_buffer_size;
      remain_data_size_ += macro_buffer_size;
    }
  }
  return ret;
}

int ObCGBlockFile::BlockStore::get_next_cg_block(ObCGBlock &cg_block)
{
  int ret = OB_SUCCESS;
  cg_block.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("block store is not initialized", K(ret));
  } else if (file_offset_ >= file_size_) {
    ret = OB_ITER_END;
  } else {
    ObCGBlock::StoreHeader store_header;
    if (!store_header_.is_valid() && OB_FAIL(read_cg_block_header(store_header_))) {
      LOG_WARN("fail to read cg block header", K(ret));
    } else if (OB_FAIL(cg_block.init(store_header_))) {
      LOG_WARN("fail to init cg block", K(ret), K(store_header));
    } else {
      const int64_t timeout_ms = get_tmp_file_io_timeout_ms();
      tmp_file::ObTmpFileIOInfo io_info;
      tmp_file::ObTmpFileIOHandle handle;
      const char *buf = cg_block.get_macro_block_buffer();
      const int64_t macro_buffer_size = cg_block.get_macro_buffer_size();
      const bool is_tail_cg_block = (cg_block.get_total_size() + file_offset_) > file_size_;
      int64_t read_buffer_size = is_tail_cg_block ? macro_buffer_size : cg_block.get_total_size();

      if (OB_FAIL(get_io_info(buf, read_buffer_size, timeout_ms, io_info))) {
        LOG_WARN("fail to get io info", K(ret));
      } else if (OB_FAIL(MTL(ObTenantTmpFileManager*)->pread(MTL_ID(), io_info, file_offset_, handle))) { // TODO @youchuan.yc using aio
        LOG_WARN("fail to read tmp file", K(ret), K(io_info), K(file_offset_));
      } else {
        file_offset_ += read_buffer_size;
        remain_data_size_ -= (macro_buffer_size - curr_cg_block_offset_);
        cg_block.set_cg_block_offset(curr_cg_block_offset_); // TODO @youchuan.yc record to iterator
        curr_cg_block_offset_ = 0;
        cg_block.set_micro_block_idx(curr_cg_block_micro_idx_);
        curr_cg_block_micro_idx_ = 0;
        if (remain_data_size_ < 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("cg block file remain data size is invalid", K(ret), K(remain_data_size_));
        } else if (is_tail_cg_block) {
          store_header_.reset();
        } else {
          // false == is_tail_cg_block
          if (OB_FAIL(take_cg_block_header(cg_block))) {
            LOG_WARN("fail to take cg block header", K(ret), K(cg_block));
          } else if (!store_header_.is_valid()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("cg block store header is invalid", K(ret), K(store_header_));
          }
        }
      }
    }
  }
  return ret;
}

// TODO @youchuan.yc using two-layer iterators
int ObCGBlockFile::BlockStore::put_cg_block_back(const ObCGBlock &cg_block)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("block store is not initialized", K(ret));
  } else if (OB_UNLIKELY(!cg_block.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cg block is not initialized", K(ret), K(cg_block));
  } else {
    const bool is_iter_end = (file_offset_ >= file_size_);
    file_offset_ -= (is_iter_end ? cg_block.get_macro_buffer_size() : cg_block.get_total_size());
    remain_data_size_ += (cg_block.get_macro_buffer_size() - cg_block.get_cg_block_offset());
    curr_cg_block_offset_ = cg_block.get_cg_block_offset();
    curr_cg_block_micro_idx_ = cg_block.get_micro_block_idx();
    if (OB_UNLIKELY(file_offset_ < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cg block file offset is invalid", K(ret), K(file_offset_));
    } else {
      store_header_ = cg_block.get_store_header();
    }
  }
  return ret;
}

/* start private function */
int ObCGBlockFile::BlockStore::get_io_info(const char *buf,
                                           const int64_t size,
                                           const int64_t timeout_ms,
                                           tmp_file::ObTmpFileIOInfo &io_info)
{
  int ret = OB_SUCCESS;
  io_info.reset();
  if (OB_UNLIKELY(nullptr == buf || size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("there are invalid argument", K(ret), KP(buf), K(size));
  } else {
    io_info.fd_ = fd_;
    io_info.io_desc_.set_wait_event(common::ObWaitEventIds::CG_BLOCK_TMP_FILE_WAIT);
    io_info.buf_ = const_cast<char *>(buf);
    io_info.size_ = size;
    io_info.io_timeout_ms_ = timeout_ms;
  }
  return ret;
}

int ObCGBlockFile::BlockStore::write_cg_block_header(const ObCGBlock::StoreHeader &cg_block_store_header)
{
  int ret = OB_SUCCESS;
  const int64_t timeout_ms = get_tmp_file_io_timeout_ms();
  tmp_file::ObTmpFileIOInfo io_info;
  int64_t store_header_size = cg_block_store_header.get_serialize_size();
  int64_t pos = 0;
  char header_buf[store_header_size];
  MEMSET(header_buf, '\0', store_header_size);
  if (OB_UNLIKELY(!cg_block_store_header.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cg block store header is invalid", K(ret), K(cg_block_store_header));
  } else if (OB_FAIL(cg_block_store_header.serialize(header_buf,
                                                     store_header_size,
                                                     pos))) {
    LOG_WARN("fail to serialize cg block store header",
        K(ret), K(cg_block_store_header), K(store_header_size), K(pos));
  } else if (OB_FAIL(get_io_info(header_buf, store_header_size, timeout_ms, io_info))) {
    LOG_WARN("fail to get io info", K(ret));
  } else if (OB_FAIL(MTL(ObTenantTmpFileManager*)->write(MTL_ID(), io_info))) {
    LOG_WARN("fail to write cg block data", K(ret), K(io_info));
  } else {
    file_size_ += store_header_size;
  }
  return ret;
}

int ObCGBlockFile::BlockStore::read_cg_block_header(ObCGBlock::StoreHeader &cg_block_store_header)
{
  int ret = OB_SUCCESS;
  const int64_t timeout_ms = get_tmp_file_io_timeout_ms();
  tmp_file::ObTmpFileIOInfo io_info;
  tmp_file::ObTmpFileIOHandle handle;
  int64_t store_header_size = cg_block_store_header.get_serialize_size();
  char header_buf[store_header_size];
  MEMSET(header_buf, '\0', store_header_size);
  cg_block_store_header.reset();
  if (file_offset_ > 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cg block file offset is not as expected", K(ret), K(file_offset_));
  } else if (OB_FAIL(get_io_info(header_buf, store_header_size, timeout_ms, io_info))) {
    LOG_WARN("fail to get io info", K(ret));
  } else if (OB_FAIL(MTL(ObTenantTmpFileManager*)->pread(MTL_ID(), io_info, file_offset_, handle))) {
    LOG_WARN("fail to read tmp file", K(ret), K(io_info), K(file_offset_));
  } else {
    int64_t pos = 0;
    if (OB_FAIL(cg_block_store_header.deserialize(header_buf,
                                                  store_header_size,
                                                  pos))) {
      LOG_WARN("fail to deserialize cg block store header",
          KP(header_buf), K(store_header_size), K(pos));
    } else {
      file_offset_ += store_header_size;
    }
  }
  return ret;
}

int ObCGBlockFile::BlockStore::take_cg_block_header(const ObCGBlock &cg_block)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const char *header_buf = cg_block.get_macro_block_buffer() + cg_block.get_macro_buffer_size();
  const int64_t store_header_size = store_header_.get_serialize_size();
  store_header_.reset();
  if (OB_FAIL(store_header_.deserialize(header_buf,
                                        store_header_.get_serialize_size(),
                                        pos))) {
    LOG_WARN("fail to deserialize cg block store header",
        KP(header_buf), K(store_header_size), K(pos));
  }
  return ret;
}

/**
* -----------------------------------ObCGBlockFile-----------------------------------
*/
int ObCGBlockFile::open(const ObTabletID tablet_id,
                        const int64_t slice_idx,
                        const int64_t scan_idx,
                        const int64_t cg_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cg block file is initialized twice", K(ret));
  } else if (OB_UNLIKELY(!tablet_id.is_valid() ||
                         slice_idx < 0 ||
                         scan_idx < 0 ||
                         cg_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("there are invalid arguments",
        K(ret), K(tablet_id), K(slice_idx), K(scan_idx), K(cg_idx));
  } else {
    tablet_id_ = tablet_id;
    slice_idx_ = slice_idx;
    scan_idx_ = scan_idx;
    cg_idx_ = cg_idx;
    if (OB_FAIL(block_store_.open())) {
      LOG_WARN("fail to open block store", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObCGBlockFile::close()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("cg block tmp file do not initialized", K(ret));
  } else if (OB_FAIL(block_store_.close())) {
    LOG_WARN("fail to remove tmp file fd", K(ret), K(block_store_));
  } else {
    is_inited_ = false;
  }
  return ret;
}

int ObCGBlockFile::append_cg_block(const ObCGBlock &cg_block)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("cg block file is not initialized", K(ret));
  } else if (OB_UNLIKELY(!cg_block.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cg block is not initialized", K(ret), K(cg_block));
  } else if (OB_FAIL(block_store_.append_cg_block(cg_block))) {
    LOG_WARN("fail to append cg block", K(ret), K(cg_block), K(*this));
  }
  return ret;
}

int ObCGBlockFile::get_next_cg_block(ObCGBlock &cg_block)
{
  int ret = OB_SUCCESS;
  cg_block.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("cg block file is not inited", K(ret));
  } else if (OB_FAIL(block_store_.get_next_cg_block(cg_block))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next cg block", K(ret), K(*this));
    }
  }
  return ret;
}

// TODO @youchuan.yc using two-layer iterators
int ObCGBlockFile::put_cg_block_back(const ObCGBlock &cg_block)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("cg block file is not inited", K(ret));
  } else if (OB_UNLIKELY(!cg_block.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cg block is not initialized", K(ret), K(cg_block));
  } else if (OB_FAIL(block_store_.put_cg_block_back(cg_block))) {
    LOG_WARN("fail to put cg block back", K(ret), K(cg_block));
  }
  return ret;
}

/**
* -----------------------------------ObCGBlockFileWriter-----------------------------------
*/
int ObCGBlockFileWriter::init(ObCGBlockFile *cg_block_file)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("the cg block file writer has been initialized", K(ret));
  } else if (nullptr == cg_block_file) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("there are invalid arguments", K(ret), KP(cg_block_file));
  } else {
    cg_block_file_ = cg_block_file;
    is_inited_ = true;
  }
  return ret;
}

void ObCGBlockFileWriter::reset()
{
  is_inited_ = false;
  cg_block_file_ = nullptr;
}

int ObCGBlockFileWriter::write(const char *buf, const bool is_complete_macro_block, const int64_t buf_size)
{
  int ret = OB_SUCCESS;
  ObCGBlock cg_block;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("the cg block writer is not initialized", K(ret));
  } else if (OB_UNLIKELY(nullptr == buf || buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("there are invalid arguments", K(ret), KP(buf), K(buf_size));
  } else if (OB_FAIL(cg_block.init(buf,
                                   ObCGBlock::DATA_VERSION_4_4_0_0_AFTER,
                                   is_complete_macro_block,
                                   buf_size))) {
    LOG_WARN("fail to initialized cg block",
        K(ret), KP(buf), K(is_complete_macro_block), K(buf_size));
  } else if (OB_FAIL(cg_block_file_->append_cg_block(cg_block))) {
    LOG_WARN("fail to append cg block", K(ret), K(cg_block), K(*this));
  }
  return ret;
}

} //end storage
} // end oceanbase
