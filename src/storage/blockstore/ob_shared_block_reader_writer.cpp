/**
 * Copyright (c) 2022 OceanBase
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
#include "storage/blockstore/ob_shared_block_reader_writer.h"
#include "storage/blocksstable/ob_block_manager.h"

namespace oceanbase
{
using namespace blocksstable;
using namespace common;
namespace storage
{

ObSharedBlockWriteInfo::ObSharedBlockWriteInfo()
  : buffer_(nullptr),
    offset_(0),
    size_(0),
    io_desc_(),
    io_callback_(nullptr)
{
}

bool ObSharedBlockWriteInfo::is_valid() const
{
  return nullptr != buffer_ && size_ > 0 && io_desc_.is_valid();
}

void ObSharedBlockWriteInfo::reset()
{
  buffer_ = nullptr;
  offset_ = 0;
  size_ = 0;
  io_desc_.reset();
  io_callback_ = nullptr;
}

ObSharedBlockWriteInfo& ObSharedBlockWriteInfo::operator=(const ObSharedBlockWriteInfo &other)
{
  if (this != &other) {
    buffer_ = other.buffer_;
    offset_ = other.offset_;
    size_ = other.size_;
    io_desc_ = other.io_desc_;
    io_callback_ = other.io_callback_;
  }
  return *this;
}

bool ObSharedBlockReadInfo::is_valid() const
{
  return addr_.is_valid() && io_desc_.is_valid();
}

//=================================== ObSharedBlocksWriteCtx =============================
bool ObSharedBlocksWriteCtx::is_valid() const
{
  return addr_.is_valid() && addr_.is_block() && block_ids_.count() > 0;
}
ObSharedBlocksWriteCtx::~ObSharedBlocksWriteCtx()
{
  clear();
}

void ObSharedBlocksWriteCtx::clear()
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
   for (int64_t i = 0; i < block_ids_.count(); ++i) {
     if (OB_FAIL(OB_SERVER_BLOCK_MGR.dec_ref(block_ids_.at(i)))) {
       LOG_ERROR("Fail to dec macro block ref cnt", K(ret), K(block_ids_.count()), K(i),
                                                  "macro id", block_ids_.at(i));
     }
     abort_unless(OB_SUCCESS == ret);
   }
  }
  block_ids_.reset();
  addr_.reset();
}
int ObSharedBlocksWriteCtx::set_addr(const ObMetaDiskAddr &addr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!addr.is_valid() || !addr.is_block())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid addr", K(ret), K(addr));
  } else {
    addr_ = addr;
  }
  return ret;
}

int ObSharedBlocksWriteCtx::add_block_id(const blocksstable::MacroBlockId &block_id)
{
  int ret = OB_SUCCESS;
  const int64_t cnt = block_ids_.count();
  if (OB_UNLIKELY(!block_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid block id", K(ret), K(block_id));
  } else if (cnt > 0 && block_id == block_ids_.at(cnt - 1)) {
    // skip, link handle uses one write_ctx to record all blocks' id sequentially
  } else if (OB_FAIL(block_ids_.push_back(block_id))) {
    LOG_WARN("Fail to push back block id", K(ret), K(block_id));
  } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.inc_ref(block_id))) {
    block_ids_.pop_back();
    LOG_ERROR("Fail to inc macro block ref cnt", K(ret));
  }
  return ret;
}

int ObSharedBlocksWriteCtx::assign(const ObSharedBlocksWriteCtx &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    clear();
    if (OB_FAIL(set_addr(other.addr_))) {
      LOG_WARN("Fail to set add", K(ret), K(other));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < other.block_ids_.count(); ++i) {
        if (OB_FAIL(add_block_id(other.block_ids_.at(i)))) {
          LOG_WARN("Fail to add block id", K(ret), K(other));
        }
      }
    }
  }
  return ret;
}

//=================================== ObSharedBlockHeader =============================
DEFINE_GET_SERIALIZE_SIZE(ObSharedBlockHeader)
{
  return sizeof(ObSharedBlockHeader);
}

DEFINE_SERIALIZE(ObSharedBlockHeader)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0 || pos < 0 || pos + get_serialize_size() < buf_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data block meta value is invalid", K(ret), KPC(this));
  } else {
    ObSharedBlockHeader *header = reinterpret_cast<ObSharedBlockHeader *>(buf + pos);
    *header = *this;
    pos += get_serialize_size();
  }
  return ret;
}

DEFINE_DESERIALIZE(ObSharedBlockHeader)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0 || pos < 0 || pos + sizeof(ObSharedBlockHeader) < data_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(buf), K(data_len), K(pos));
  } else {
    const ObSharedBlockHeader *header = reinterpret_cast<const ObSharedBlockHeader *>(buf + pos);
    *this = *header;
    if (OB_UNLIKELY(!is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data block meta value is invalid", K(ret), KPC(this));
    } else {
      pos += get_serialize_size();
    }
  }
  return ret;
}

bool ObSharedBlockHeader::is_valid() const
{
  return OB_LINKED_BLOCK_HEADER_MAGIC == magic_
      && OB_LINKED_BLOCK_HEADER_VERSION == version_
      && cur_block_idx_ <= total_block_cnt_
      && total_block_cnt_ == 1
      && get_serialize_size() == header_size_
      && DEFAULT_MACRO_ID == next_macro_id_;
}


//=================================== ObSharedBlockWriteHandle =============================

void ObSharedBlockBaseHandle::reset()
{
  macro_handles_.reset();
  addrs_.reset();
}

int ObSharedBlockBaseHandle::wait()
{
  int ret = OB_SUCCESS;
  const int64_t io_timeout_ms = std::max(GCONF._data_storage_io_timeout / 1000, DEFAULT_IO_WAIT_TIME_MS);
  for (int64_t i = 0; OB_SUCC(ret) && i < macro_handles_.count(); ++i) {
    ObMacroBlockHandle macro_handle = macro_handles_.at(i);
    if (OB_UNLIKELY(!macro_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected invalid macro handle", K(ret), K(i), K(macro_handle), KPC(this));
    } else if (OB_FAIL(macro_handle.wait(io_timeout_ms))) {
      LOG_WARN("Failt to wait macro handle finish", K(ret), K(macro_handle), K(io_timeout_ms));
    }
  }
  return ret;
}

int ObSharedBlockBaseHandle::add_macro_handle(const ObMacroBlockHandle &macro_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(macro_handle));
  } else if (OB_FAIL(macro_handles_.push_back(macro_handle))) {
    LOG_WARN("Fail to push back macro handle", K(ret));
  }
  return ret;
}

int ObSharedBlockBaseHandle::add_meta_addr(const ObMetaDiskAddr &addr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!addr.is_valid() || !addr.is_block())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(addr));
  } else if (OB_FAIL(addrs_.push_back(addr))) {
    LOG_WARN("Fail to push back macro handle", K(ret));
  }
  return ret;
}


bool ObSharedBlockWriteHandle::is_valid() const
{
  return macro_handles_.count() > 0 && addrs_.count() == 1;
}

bool ObSharedBlockReadHandle::is_valid() const
{
  return macro_handle_.is_valid();
}

bool ObSharedBlockReadHandle::is_empty() const
{
  return macro_handle_.is_empty();
}
ObSharedBlockReadHandle::ObSharedBlockReadHandle(const ObSharedBlockReadHandle &other)
{
  *this = other;
}

ObSharedBlockReadHandle &ObSharedBlockReadHandle::operator=(const ObSharedBlockReadHandle &other)
{
  if (&other != this) {
    macro_handle_ = other.macro_handle_;
  }
  return *this;
}

int ObSharedBlockReadHandle::wait(const int64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  const int64_t io_timeout_ms = timeout_ms > 0
      ? timeout_ms : MAX(GCONF._data_storage_io_timeout / 1000, DEFAULT_IO_WAIT_TIME_MS);
  if (OB_UNLIKELY(!macro_handle_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected read handle", K(ret), K_(macro_handle));
  } else if (OB_FAIL(macro_handle_.wait(io_timeout_ms))) {
    LOG_WARN("Failt to wait macro handle finish", K(ret), K(macro_handle_), K(io_timeout_ms));
  }
  return ret;
}

int ObSharedBlockReadHandle::get_data(ObIAllocator &allocator, char *&buf, int64_t &buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(wait())) {
    LOG_WARN("Fail to wait io finish", K(ret));
  } else {
    const char *data_buf = macro_handle_.get_buffer();
    const int64_t data_size = macro_handle_.get_data_size();
    int64_t header_size = 0;
    if (OB_FAIL(verify_checksum(data_buf, data_size, header_size, buf_len))) {
      LOG_WARN("fail to verify checksum", K(ret), KP(data_buf), K(data_size), K(header_size), K(buf_len));
    } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc buf", K(ret), K(buf_len));
    } else {
      MEMCPY(buf, data_buf + header_size, buf_len);
    }
  }
  return ret;
}

int ObSharedBlockReadHandle::parse_data(
    const char *data_buf,
    const int64_t data_size,
    char *&buf,
    int64_t &buf_len)
{
  int ret = OB_SUCCESS;
  int64_t header_size = 0;
  if (OB_FAIL(verify_checksum(data_buf, data_size, header_size, buf_len))) {
    LOG_WARN("fail to verify checksum", K(ret), KP(data_buf), K(data_size), K(header_size), K(buf_len));
  } else {
    buf = const_cast<char *>(data_buf) + header_size;
  }
  return ret;
}

int ObSharedBlockReadHandle::verify_checksum(
    const char *data_buf,
    const int64_t data_size,
    int64_t &header_size,
    int64_t &buf_len)
{
  int ret = OB_SUCCESS;
  if (data_size < sizeof(ObSharedBlockHeader)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected data size", K(ret), K(data_size));
  } else {
    const ObSharedBlockHeader *header = reinterpret_cast<const ObSharedBlockHeader *>(data_buf);
    int64_t checksum = 0;
    if (OB_UNLIKELY(!header->is_valid()
        || data_size < header->header_size_ + header->data_size_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected header", K(ret), KPC(header), K(data_size));
    } else if (OB_UNLIKELY(header->checksum_
        != (checksum = ob_crc64_sse42(data_buf + header->header_size_, header->data_size_)))) {
      ret = OB_CHECKSUM_ERROR;
      LOG_WARN("Checksum error", K(ret), K(checksum), KPC(header));
    } else {
      header_size = header->header_size_;
      buf_len = header->data_size_;
    }
    LOG_DEBUG("zhuixin debug read shared block", K(ret), KPC(header));
  }
  return ret;
}

int ObSharedBlockReadHandle::set_macro_handle(const ObMacroBlockHandle &macro_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(macro_handle));
  } else {
    macro_handle_ = macro_handle;
  }
  return ret;
}

int ObSharedBlockWriteHandle::get_write_ctx(ObSharedBlocksWriteCtx &write_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected invalid shared handle", K(ret), KPC(this));
  } else if (OB_FAIL(wait())) {
    LOG_WARN("Fail to wait io finish", K(ret), KPC(this));
  } else if (OB_FAIL(write_ctx.set_addr(addrs_.at(0)))) {
    LOG_WARN("Fail to set addr", K(ret), K(addrs_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < macro_handles_.count(); ++i) {
      if (OB_FAIL(write_ctx.add_block_id(macro_handles_.at(i).get_macro_id()))) {
        LOG_WARN("Fail to add block id", K(ret), K(i), K(macro_handles_.at(i)));
      }
    }
  }
  return ret;
}

void ObSharedBlockBatchHandle::reset()
{
  write_ctxs_.reset();
  ObSharedBlockBaseHandle::reset();
}
bool ObSharedBlockBatchHandle::is_valid() const
{
  return macro_handles_.count() > 0 && addrs_.count() > 0
      && write_ctxs_.count() == addrs_.count();
}

int ObSharedBlockBatchHandle::batch_get_write_ctx(ObIArray<ObSharedBlocksWriteCtx> &write_ctxs)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected invalid batch handle", K(ret), KPC(this));
  } else if (OB_FAIL(wait())) {
    LOG_WARN("Fail to wait io finish", K(ret), KPC(this));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < write_ctxs_.count(); ++i) {
      if (OB_UNLIKELY(!write_ctxs_.at(i).is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected write ctx", K(ret), K(i), K(write_ctxs_.at(i)));
      } else if (OB_FAIL(write_ctxs.push_back(write_ctxs_.at(i)))) {
        LOG_WARN("Fail to add meta disk addr", K(ret), K(write_ctxs_.at(i)));
      }
    }
  }
  return ret;
}

void ObSharedBlockLinkHandle::reset()
{
  write_ctx_.clear();
  ObSharedBlockBaseHandle::reset();
}

bool ObSharedBlockLinkHandle::is_valid() const
{
  return macro_handles_.count() > 0 && addrs_.count() == 1; // only record last addr
}

int ObSharedBlockLinkHandle::get_write_ctx(ObSharedBlocksWriteCtx &write_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected invalid batch handle", K(ret), KPC(this));
  } else if (OB_FAIL(wait())) {
    LOG_WARN("Fail to wait io finish", K(ret), KPC(this));
  } else if (OB_FAIL(write_ctx.assign(write_ctx_))) {
    LOG_WARN("Fail to get write ctx", K(ret), KPC(this));
  }
  return ret;
}

int ObSharedBlockLinkIter::init(const ObMetaDiskAddr &head)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Init twice", K(ret));
  } else if (OB_UNLIKELY(!head.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid meta disk addr", K(ret), K(head));
  } else {
    head_ = head;
    cur_ = head;
    is_inited_ = true;
  }
  return ret;
}

int ObSharedBlockLinkIter::reuse()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!head_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("head_ is invalid", K(ret), K_(head));
  } else {
    cur_ = head_;
  }
  return ret;
}

int ObSharedBlockLinkIter::get_next_block(ObIAllocator &allocator, char *&buf, int64_t &buf_len)
{
  int ret = OB_SUCCESS;
  ObSharedBlockReadHandle block_handle;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret), KPC(this));
  } else if (cur_.is_none()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(read_next_block(block_handle))) {
    LOG_WARN("fail to read next block", K(ret), K(head_), K(cur_));
  } else if (OB_FAIL(block_handle.get_data(allocator, buf, buf_len))) {
    LOG_WARN("Fail to get data", K(ret), K(block_handle));
  }
  return ret;
}

int ObSharedBlockLinkIter::get_next_macro_id(MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  ObSharedBlockReadHandle block_handle;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret), KPC(this));
  } else if (cur_.is_none()) {
    ret = OB_ITER_END;
  } else if (!cur_.is_block()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur addr is not block addr", K(ret), K(cur_));
  } else if (FALSE_IT(macro_id = cur_.block_id())) {
  } else if (OB_FAIL(read_next_block(block_handle))) {
    LOG_WARN("fail to read next block", K(ret), K(head_), K(cur_));
  }
  return ret;
}

int ObSharedBlockLinkIter::read_next_block(ObSharedBlockReadHandle &block_handle)
{
  int ret = OB_SUCCESS;
  ObSharedBlockReadInfo read_info;
  read_info.addr_ = cur_;
  read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
  if (OB_FAIL(ObSharedBlockReaderWriter::async_read(read_info, block_handle))) {
    LOG_WARN("Fail to read block", K(ret), K(read_info));
  } else if (OB_FAIL(block_handle.wait())) {
    LOG_WARN("Fail to wait read io finish", K(ret), K(block_handle));
  } else {
    ObMacroBlockHandle &macro_handle = block_handle.macro_handle_;
    const ObSharedBlockHeader *header =
        reinterpret_cast<const ObSharedBlockHeader *>(macro_handle.get_buffer());
    cur_ = header->prev_addr_;
    LOG_DEBUG("get next link block macro id", K(ret), K(head_), K(cur_), KPC(header));
  }
  return ret;
}


//=================================== ObSharedBlockReaderWriter =============================
const MacroBlockId ObSharedBlockHeader::DEFAULT_MACRO_ID(0, MacroBlockId::AUTONOMIC_BLOCK_INDEX, 0);
ObSharedBlockReaderWriter::ObSharedBlockReaderWriter()
    : mutex_(), data_("SharedBlockRW"), macro_handle_(), offset_(0), align_offset_(0), write_align_size_(0),
      hanging_(false), need_align_(false), need_cross_(false),
      is_inited_(false)
{}

ObSharedBlockReaderWriter::~ObSharedBlockReaderWriter()
{
  reset();
}

int ObSharedBlockReaderWriter::init(
    const bool need_align,
    const bool need_cross)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Init twice", K(ret));
  } else if (!need_align || need_cross) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Not supported", K(ret), K(need_align), K(need_cross));
  } else if (OB_FAIL(data_.ensure_space(DEFAULT_MACRO_BLOCK_SIZE))) {
    LOG_WARN("Fail to ensure space", K(ret));
  } else {
    offset_ = 0;
    align_offset_ = 0;
    write_align_size_ = DIO_READ_ALIGN_SIZE; // 4K
    hanging_ = false;
    need_align_ = need_align;
    need_cross_ = need_cross;
    if (OB_FAIL(reserve_header())) {
      LOG_WARN("fail to reserve header when init", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}
void ObSharedBlockReaderWriter::reset()
{
  data_.reset();
  macro_handle_.reset();
  offset_ = 0;
  align_offset_ = 0;
  write_align_size_ = 0;
  hanging_ = false;
  need_align_ = false;
  need_cross_ = false;
  is_inited_ = false;
}
int ObSharedBlockReaderWriter::async_write(
    const ObSharedBlockWriteInfo &write_info,
    ObSharedBlockWriteHandle &block_handle)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  ObSharedBlocksWriteCtx write_ctx;
  ObSharedBlockWriteArgs write_args;
  write_args.need_align_ = need_align_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else if (OB_FAIL(inner_async_write(write_info, write_args, block_handle, write_ctx))) {
    LOG_WARN("Fail to inner async write block", K(ret), K(write_info), K(write_args));
  }
  return ret;
}

int ObSharedBlockReaderWriter::async_batch_write(
    const ObIArray<ObSharedBlockWriteInfo> &write_infos,
    ObSharedBlockBatchHandle &block_handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else {
    lib::ObMutexGuard guard(mutex_);
    ObSharedBlockWriteArgs write_args;
    ObSharedBlocksWriteCtx write_ctx;
    for (int64_t i = 0; OB_SUCC(ret) && i < write_infos.count(); ++i) {
      // only the last need flush and align
      write_args.need_flush_ = (i == write_infos.count() - 1);
      write_args.need_align_ = (i == write_infos.count() - 1) ? need_align_ : false;
      write_ctx.clear();
      if (OB_FAIL(inner_async_write(write_infos.at(i), write_args, block_handle, write_ctx))) {
        LOG_WARN("Fail to async write block", K(ret), K(i), K(write_infos.at(i)), K(write_args));
      } else if (OB_FAIL(block_handle.write_ctxs_.push_back(write_ctx))) {
        LOG_WARN("Fail to add write ctx", K(ret), K(write_ctx));
      }
    }
  }
  return ret;
}

int ObSharedBlockReaderWriter::async_link_write(
    const ObSharedBlockWriteInfo &write_info,
    ObSharedBlockLinkHandle &block_handle)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  ObSharedBlockWriteArgs write_args;
  ObSharedBlocksWriteCtx write_ctx;
  write_args.need_flush_ = true;
  write_args.need_align_ = need_align_;
  write_args.is_linked_ = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else if (OB_FAIL(block_handle.wait())) {
    LOG_WARN("Fail to wait other blocks finish", K(ret), K(block_handle));
  } else if (OB_FAIL(inner_async_write(write_info, write_args, block_handle, write_ctx))) {
    LOG_WARN("Fail to inner async write block", K(ret), K(write_info), K(write_args));
  } else if (OB_FAIL(block_handle.write_ctx_.set_addr(write_ctx.addr_))) {
    LOG_WARN("Fail to set addr to write ctx", K(ret), K(write_ctx));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < write_ctx.block_ids_.count(); ++i) {
      if (OB_FAIL(block_handle.write_ctx_.add_block_id(write_ctx.block_ids_.at(i)))) {
        LOG_WARN("Fail to add block id", K(ret), K(write_ctx));
      }
    }
  }
  return ret;
}

int ObSharedBlockReaderWriter::inner_async_write(
    const ObSharedBlockWriteInfo &write_info,
    const ObSharedBlockWriteArgs &write_args,
    ObSharedBlockBaseHandle &block_handle,
    ObSharedBlocksWriteCtx &write_ctx)
{
  int ret = OB_SUCCESS;
  if (need_cross_ && OB_FAIL(write_cross_block(write_info, write_args, block_handle))) {
    LOG_WARN("Fail to write cross block", K(ret), K(write_info));
  } else if (OB_FAIL(write_block(write_info, write_args, block_handle, write_ctx))) {
    LOG_WARN("Fail to write block", K(ret), K(write_info));
  }
  return ret;
}

int ObSharedBlockReaderWriter::reserve_header()
{
  int ret = OB_SUCCESS;
  ObMacroBlockCommonHeader common_header;
  common_header.reset();
  common_header.set_attr(ObMacroBlockCommonHeader::MacroBlockType::SharedMetaData);
  if (OB_UNLIKELY(offset_ > 0 || align_offset_ > 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to reserve header", K(ret), K_(offset), K_(align_offset));
  } else if (OB_FAIL(common_header.build_serialized_header(data_.current(), common_header.get_serialize_size()))) {
    LOG_WARN("fail to write common header", K(ret), K(common_header));
  } else if (OB_FAIL(data_.advance(common_header.get_serialize_size()))) {
    LOG_WARN("Fail to advance size", K(ret), K(common_header));
  } else {
    offset_ = common_header.get_serialize_size();
    hanging_ = true;
  }

  if (OB_FAIL(ret)) { // recover from failure
    offset_ = 0;
    hanging_ = false;
    data_.reuse();
  }
  return ret;
}

int ObSharedBlockReaderWriter::switch_block(ObMacroBlockHandle &macro_handle)
{
  int ret = OB_SUCCESS;
  macro_handle.reset();
  if (hanging_) {
    ObMacroBlockWriteInfo macro_info;
    macro_info.buffer_ = data_.data() + align_offset_;
    macro_info.offset_ = align_offset_;
    macro_info.size_ = upper_align(offset_ - align_offset_, write_align_size_);
    macro_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
    // io_callback
    // do not use macro_handle_ to write, since it will be reset if failed
    macro_handle = macro_handle_;
    if (OB_FAIL(macro_handle.async_write(macro_info))) {
      LOG_WARN("Fail to async write block", K(ret), K(macro_info));
    }
  }
  if (OB_SUCC(ret)) {
    hanging_ = false;
    data_.reuse();
    macro_handle_.reset();
    offset_ = 0;
    align_offset_ = 0;
    if (OB_FAIL(OB_SERVER_BLOCK_MGR.alloc_block(macro_handle_))) {
      LOG_WARN("fail to alloc block for new macro block", K(ret));
    } else if (OB_FAIL(reserve_header())) {
      LOG_WARN("fail to reserve header after switch block", K(ret));
    }
  }
  return ret;
}

int ObSharedBlockReaderWriter::calc_store_size(
    const ObSharedBlockHeader &header,
    const bool need_align,
    int64_t &store_size,
    int64_t &align_store_size)
{
  int ret = OB_SUCCESS;
  store_size = 0;
  align_store_size = 0;
  store_size = header.header_size_ + header.data_size_;
  const int64_t next_align_offset = upper_align(offset_ + store_size, write_align_size_);
  align_store_size = next_align_offset - align_offset_;
  if (need_align) {
    store_size = next_align_offset - offset_;
  }
  if (OB_UNLIKELY(store_size > DEFAULT_MACRO_BLOCK_SIZE)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Not supported block size", K(ret), K(header), K_(offset), K_(align_offset), K(store_size));
  }
  return ret;
}

int ObSharedBlockReaderWriter::inner_write_block(
    const ObSharedBlockHeader &header,
    const char *buf,
    const int64_t &size,
    ObSharedBlockBaseHandle &block_handle,
    const bool need_flush,
    const bool need_align)
{
  int ret = OB_SUCCESS;
  ObMacroBlockHandle macro_handle;
  ObMetaDiskAddr addr;
  const int64_t blk_size = header.header_size_ + header.data_size_;
  int64_t store_size = 0, align_store_size = 0;
  if (OB_FAIL(calc_store_size(header, need_align, store_size, align_store_size))) {
    LOG_WARN("fail to calc store size", K(ret));
  } else if (!macro_handle_.get_macro_id().is_valid()
      && OB_FAIL(OB_SERVER_BLOCK_MGR.alloc_block(macro_handle_))) {
    LOG_WARN("fail to alloc block for new macro block", K(ret));
  } else if (store_size + offset_ > DEFAULT_MACRO_BLOCK_SIZE) {
    if (OB_FAIL(switch_block(macro_handle))) {
      LOG_WARN("Fail to switch new block", K(ret));
    } else if (macro_handle.is_valid() && OB_FAIL(block_handle.add_macro_handle(macro_handle))) {
      LOG_WARN("Fail to flush last macro block", K(ret), K(macro_handle));
    } else if (OB_FAIL(calc_store_size(header, need_align, store_size, align_store_size))) {
      LOG_WARN("fail to calc store size", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    macro_handle.reset();
    int64_t pos = 0;
    const int64_t prev_pos = data_.pos();
    const int64_t prev_offset = offset_;
    const int64_t prev_align_offset = align_offset_;
    const bool prev_hanging = hanging_;
    if (OB_FAIL(header.serialize(data_.current(), header.header_size_, pos))) {
      LOG_WARN("Fail to serialize header", K(ret), K(header));
    } else {
      MEMCPY(data_.current() + pos, buf, size);
      ObMacroBlockWriteInfo macro_info;
      macro_info.buffer_ = data_.data() + align_offset_;
      macro_info.offset_ = align_offset_;
      macro_info.size_ = align_store_size;
      macro_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
      macro_info.io_desc_.set_group_id(ObIOModule::SHARED_BLOCK_RW_IO);
      // io_callback
      if (OB_FAIL(addr.set_block_addr(macro_handle_.get_macro_id(),
                                      offset_,
                                      blk_size))) {
        LOG_WARN("Fail to set block addr", K(ret));
      } else if (OB_FAIL(block_handle.add_meta_addr(addr))) {
        LOG_WARN("Fail to add meta addr", K(ret), K(addr));
      } else if (OB_FAIL(data_.advance(store_size))) {
        LOG_WARN("Fail to advance size", K(ret), K(store_size));
      } else {
        offset_ += store_size;
      }
      if (OB_SUCC(ret) && need_flush) {
        macro_handle = macro_handle_;
        if (OB_FAIL(macro_handle.async_write(macro_info))) {
          LOG_WARN("Fail to async write block", K(ret), K(macro_info));
        } else if (OB_FAIL(block_handle.add_macro_handle(macro_handle))) {
          LOG_WARN("Fail to add macro handle", K(ret), K(macro_handle), K(addr));
        } else {
          hanging_ = false;
          align_offset_ = lower_align(offset_, write_align_size_);
        }
      } else if (!need_flush) {
        hanging_ = true;
      }
      LOG_DEBUG("zhuixin debug inner write block", K(ret), K(header), K(size), K(need_flush),
          K(need_align), K(store_size), K(align_store_size), K(offset_), K(align_offset_),
          K(hanging_), K(addr), K(macro_handle));
    }
    // roll back status
    if (OB_FAIL(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(data_.set_pos(prev_pos))) {
        LOG_ERROR("fail to roll back data buffer", K(ret), K(tmp_ret), K(prev_pos), K(header));
        ob_usleep(1000 * 1000);
        ob_abort();
      } else {
        offset_ = prev_offset;
        align_offset_ = prev_align_offset;
        hanging_ = prev_hanging;
      }
    }
  }
  return ret;
}

void ObSharedBlockReaderWriter::get_cur_shared_block(blocksstable::MacroBlockId &macro_id)
{
  lib::ObMutexGuard guard(mutex_);
  macro_id = macro_handle_.get_macro_id();
}

int ObSharedBlockReaderWriter::write_block(
    const ObSharedBlockWriteInfo &write_info,
    const ObSharedBlockWriteArgs &write_args,
    ObSharedBlockBaseHandle &block_handle,
    ObSharedBlocksWriteCtx &write_ctx)
{
  int ret = OB_SUCCESS;
  ObMetaDiskAddr prev_addr;
  prev_addr.set_none_addr();
  if (OB_UNLIKELY(!write_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid shared block write info", K(ret), K(write_info));
  } else if (write_args.is_linked_ && block_handle.addrs_.count() > 0) {
    prev_addr = block_handle.addrs_.at(0);
    block_handle.reset(); // clear prev blocks info
  }
  if (OB_SUCC(ret)) {
    ObSharedBlockHeader header;
    header.cur_block_idx_ = 1;
    header.total_block_cnt_ = 1;
    header.header_size_ = header.get_serialize_size();
    header.data_size_ = write_info.size_;
    header.checksum_ = ob_crc64_sse42(write_info.buffer_, write_info.size_);
    header.next_macro_id_ = ObSharedBlockHeader::DEFAULT_MACRO_ID;
    header.prev_addr_ = prev_addr;
    if (OB_FAIL(inner_write_block(header, write_info.buffer_, write_info.size_,
        block_handle, write_args.need_flush_, write_args.need_align_))) {
      LOG_WARN("Fail to write block", K(ret), K(write_info), K(write_args));
    } else {
      const int64_t cnt = block_handle.addrs_.count();
      const ObMetaDiskAddr &addr = block_handle.addrs_.at(cnt - 1);
      if (OB_FAIL(write_ctx.set_addr(addr))) {
        LOG_WARN("Fail to add addr to write ctx", K(ret), K(addr));
      } else if (OB_FAIL(write_ctx.add_block_id(addr.block_id()))) {
        LOG_WARN("Fail to add block id to write ctx", K(ret), K(addr));
      }
    }
  }
  return ret;
}

int ObSharedBlockReaderWriter::write_cross_block(
    const ObSharedBlockWriteInfo &write_info,
    const ObSharedBlockWriteArgs &write_args,
    ObSharedBlockBaseHandle &block_handle)
{
  UNUSED(write_info);
  UNUSED(write_args);
  UNUSED(block_handle);
  return OB_NOT_SUPPORTED;
}

int ObSharedBlockReaderWriter::async_read(
    const ObSharedBlockReadInfo &read_info,
    ObSharedBlockReadHandle &block_handle)
{
  int ret = OB_SUCCESS;
  ObMacroBlockReadInfo macro_read_info;
  ObMacroBlockHandle macro_handle;
  macro_read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
  macro_read_info.io_desc_.set_group_id(ObIOModule::SHARED_BLOCK_RW_IO);
  macro_read_info.io_callback_ = read_info.io_callback_;
  if (OB_UNLIKELY(!read_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid shared block read info", K(ret), K(read_info));
  } else if (OB_FAIL(read_info.addr_.get_block_addr(
      macro_read_info.macro_block_id_,
      macro_read_info.offset_,
      macro_read_info.size_))) {
    LOG_WARN("Fail to get block addr", K(ret), K(read_info));
  } else if (OB_FAIL(macro_handle.async_read(macro_read_info))) {
    LOG_WARN("Fail to async read block", K(ret), K(macro_read_info));
  } else if (OB_FAIL(block_handle.set_macro_handle(macro_handle))) {
    LOG_WARN("Fail to add macro handle", K(ret), K(macro_read_info));
  }
  return ret;
}


}  // end namespace storage
}  // end namespace oceanbase
