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
#include "ob_shared_object_reader_writer.h"
#include "src/storage/ob_storage_struct.h"

namespace oceanbase
{
using namespace blocksstable;
using namespace common;
namespace storage
{

ObSharedObjectWriteInfo::ObSharedObjectWriteInfo()
  : buffer_(nullptr),
    offset_(0),
    size_(0),
    ls_epoch_(0),
    io_desc_(),
    io_callback_(nullptr),
    write_callback_(nullptr)
{
}

bool ObSharedObjectWriteInfo::is_valid() const
{
  return nullptr != buffer_ && size_ > 0 && io_desc_.is_valid();
}

void ObSharedObjectWriteInfo::reset()
{
  buffer_ = nullptr;
  offset_ = 0;
  size_ = 0;
  ls_epoch_ = 0;
  io_desc_.reset();
  io_callback_ = nullptr;
  write_callback_ = nullptr;
}

ObSharedObjectWriteInfo& ObSharedObjectWriteInfo::operator=(const ObSharedObjectWriteInfo &other)
{
  if (this != &other) {
    buffer_ = other.buffer_;
    offset_ = other.offset_;
    size_ = other.size_;
    ls_epoch_ = other.ls_epoch_;
    io_desc_ = other.io_desc_;
    io_callback_ = other.io_callback_;
    write_callback_ = other.write_callback_;
  }
  return *this;
}

bool ObSharedObjectReadInfo::is_valid() const
{
  return addr_.is_valid() && io_desc_.is_valid();
}

//=================================== ObSharedObjectsWriteCtx =============================
bool ObSharedObjectsWriteCtx::is_valid() const
{
  return addr_.is_valid() && addr_.is_block() && block_ids_.count() > 0;
}
ObSharedObjectsWriteCtx::~ObSharedObjectsWriteCtx()
{
  clear();
}

void ObSharedObjectsWriteCtx::clear()
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
   for (int64_t i = 0; i < block_ids_.count(); ++i) {
     if (OB_FAIL(OB_STORAGE_OBJECT_MGR.dec_ref(block_ids_.at(i)))) {
       LOG_ERROR("Fail to dec macro block ref cnt", K(ret), K(block_ids_.count()), K(i),
                                                  "macro id", block_ids_.at(i));
     }
     abort_unless(OB_SUCCESS == ret);
   }
  }
  block_ids_.reset();
  addr_.reset();
  next_opt_.set_private_object_opt();
}
int ObSharedObjectsWriteCtx::set_addr(const ObMetaDiskAddr &addr)
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

int ObSharedObjectsWriteCtx::add_object_id(const blocksstable::MacroBlockId &object_id)
{
  int ret = OB_SUCCESS;
  const int64_t cnt = block_ids_.count();
  if (OB_UNLIKELY(!object_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid block id", K(ret), K(object_id));
  } else if (cnt > 0 && object_id == block_ids_.at(cnt - 1)) {
    // skip, link handle uses one write_ctx to record all blocks' id sequentially
  } else if (OB_FAIL(block_ids_.push_back(object_id))) {
    LOG_WARN("Fail to push back block id", K(ret), K(object_id));
  } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.inc_ref(object_id))) {
    block_ids_.pop_back();
    LOG_ERROR("Fail to inc macro block ref cnt", K(ret));
  }
  return ret;
}

int ObSharedObjectsWriteCtx::assign(const ObSharedObjectsWriteCtx &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    clear();
    if (OB_FAIL(set_addr(other.addr_))) {
      LOG_WARN("Fail to set add", K(ret), K(other));
    } else {
      next_opt_ = other.next_opt_;
      for (int64_t i = 0; OB_SUCC(ret) && i < other.block_ids_.count(); ++i) {
        if (OB_FAIL(add_object_id(other.block_ids_.at(i)))) {
          LOG_WARN("Fail to add block id", K(ret), K(other));
        }
      }
    }
  }
  return ret;
}
int ObSharedObjectsWriteCtx::advance_data_seq()
{
  int ret = OB_SUCCESS;
  if (ObStorageObjectType::SHARED_MAJOR_META_MACRO == next_opt_.object_type_) {
    next_opt_.ss_share_opt_.data_seq_++;
  }
  return ret;
}
//=================================== ObSharedObjectHeader =============================
DEFINE_GET_SERIALIZE_SIZE(ObSharedObjectHeader)
{
  return sizeof(magic_) + sizeof(version_) + sizeof(cur_block_idx_) + sizeof(total_block_cnt_)
      + sizeof(header_size_) + sizeof(data_size_) + sizeof(checksum_)
      + next_macro_id_.get_serialize_size() + prev_addr_.get_serialize_size();
}

DEFINE_SERIALIZE(ObSharedObjectHeader)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data block meta value is invalid", K(ret), K(*this));
  }
  SERIALIZE_MEMBER_WITH_MEMCPY(magic_);
  SERIALIZE_MEMBER_WITH_MEMCPY(version_);
  SERIALIZE_MEMBER_WITH_MEMCPY(cur_block_idx_);
  SERIALIZE_MEMBER_WITH_MEMCPY(total_block_cnt_);
  SERIALIZE_MEMBER_WITH_MEMCPY(header_size_);
  SERIALIZE_MEMBER_WITH_MEMCPY(data_size_);
  SERIALIZE_MEMBER_WITH_MEMCPY(checksum_);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(next_macro_id_.serialize(buf, buf_len, pos))) {
      LOG_WARN("fail to serialize next_macro_id", K(ret), K(*this));
    } else if(OB_FAIL(prev_addr_.serialize(buf, buf_len, pos))) {
      LOG_WARN("fail to serialize prev_addr_", K(ret), K(*this));
    }
  }
  return ret;
}

DEFINE_DESERIALIZE(ObSharedObjectHeader)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(buf), K(data_len), K(pos));
  }
  const int64_t start_pos = pos;
  DESERIALIZE_MEMBER_WITH_MEMCPY(magic_);
  DESERIALIZE_MEMBER_WITH_MEMCPY(version_);
  DESERIALIZE_MEMBER_WITH_MEMCPY(cur_block_idx_);
  DESERIALIZE_MEMBER_WITH_MEMCPY(total_block_cnt_);
  DESERIALIZE_MEMBER_WITH_MEMCPY(header_size_);
  DESERIALIZE_MEMBER_WITH_MEMCPY(data_size_);
  DESERIALIZE_MEMBER_WITH_MEMCPY(checksum_);

  if (OB_SUCC(ret)) {
    if (OB_SHARED_BLOCK_HEADER_VERSION_V1 == version_) {
      if (OB_FAIL(next_macro_id_.memcpy_deserialize(buf, data_len, pos))) {
        LOG_WARN("fail to deserialize previous_macro_block_id", K(ret), K(*this));
      } else if (OB_FAIL(prev_addr_.memcpy_deserialize(buf, data_len, pos))) {
        LOG_WARN("fail to deserialzie pre addr", K(ret));
      } else if (OB_UNLIKELY(pos - start_pos != header_size_)) {
        ret = OB_DESERIALIZE_ERROR;
        LOG_WARN("fail to deserialize ObSharedObjectHeader v1", K(ret), K(*this));
      } else {
        next_macro_id_ = DEFAULT_MACRO_ID;
        version_ = OB_SHARED_BLOCK_HEADER_VERSION_V2;
        header_size_ = get_serialize_size();
      }
    } else if (OB_SHARED_BLOCK_HEADER_VERSION_V2 == version_) {
      if (OB_FAIL(next_macro_id_.deserialize(buf, data_len, pos))) {
        LOG_WARN("fail to deserialize previous_macro_block_id", K(ret), K(*this));
      } else if (OB_FAIL(prev_addr_.deserialize(buf, data_len, pos))) {
        LOG_WARN("fail to deserialzie pre addr", K(ret));
      }
    } else {
      ret = OB_DESERIALIZE_ERROR;
      LOG_WARN("unexpected ObSharedObjectHeader version", K(ret), K(*this));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(!is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ObSharedObjectHeader is invalid", K(ret), K(*this));
    }
  }
  return ret;
}

bool ObSharedObjectHeader::is_valid() const
{
  return OB_SHARED_BLOCK_HEADER_MAGIC == magic_
      && OB_SHARED_BLOCK_HEADER_VERSION_V2 == version_
      && cur_block_idx_ <= total_block_cnt_
      && total_block_cnt_ == 1
      && get_serialize_size() == header_size_
      && DEFAULT_MACRO_ID == next_macro_id_;
}


//=================================== ObSharedObjectWriteHandle =============================

void ObSharedObjectBaseHandle::reset()
{
  object_handles_.reset();
  addrs_.reset();
}

int ObSharedObjectBaseHandle::wait()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < object_handles_.count(); ++i) {
    ObStorageObjectHandle object_handle = object_handles_.at(i);
    if (OB_UNLIKELY(!object_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected invalid object handle", K(ret), K(i), K(object_handle), KPC(this));
    } else if (OB_FAIL(object_handle.wait())) {
      LOG_WARN("Failt to wait object handle finish", K(ret), K(object_handle));
    }
  }
  return ret;
}

int ObSharedObjectBaseHandle::add_object_handle(const ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!object_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(object_handle));
  } else if (OB_FAIL(object_handles_.push_back(object_handle))) {
    LOG_WARN("Fail to push back object handle", K(ret));
  }
  return ret;
}

int ObSharedObjectBaseHandle::add_meta_addr(const ObMetaDiskAddr &addr)
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


bool ObSharedObjectWriteHandle::is_valid() const
{
  return object_handles_.count() > 0 && addrs_.count() == 1;
}


ObSharedObjectReadHandle::ObSharedObjectReadHandle()
  : allocator_(nullptr),
    object_handle_()
{
}

ObSharedObjectReadHandle::ObSharedObjectReadHandle(ObIAllocator &allocator)
  : allocator_(&allocator),
    object_handle_()
{
}

ObSharedObjectReadHandle::~ObSharedObjectReadHandle()
{
  reset();
}

void ObSharedObjectReadHandle::reset()
{
  allocator_ = nullptr;
  object_handle_.reset();
}

bool ObSharedObjectReadHandle::is_valid() const
{
  return object_handle_.is_valid();
}

bool ObSharedObjectReadHandle::is_empty() const
{
  return object_handle_.is_empty();
}
ObSharedObjectReadHandle::ObSharedObjectReadHandle(const ObSharedObjectReadHandle &other)
{
  *this = other;
}

ObSharedObjectReadHandle &ObSharedObjectReadHandle::operator=(const ObSharedObjectReadHandle &other)
{
  if (&other != this) {
    object_handle_ = other.object_handle_;
    allocator_ = other.allocator_;
  }
  return *this;
}

int ObSharedObjectReadHandle::wait()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!object_handle_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected read handle", K(ret), K_(object_handle));
  } else if (OB_FAIL(object_handle_.wait())) {
    LOG_WARN("Failt to wait macro handle finish", K(ret), K(object_handle_));
  }
  return ret;
}

int ObSharedObjectReadHandle::alloc_io_buf(char *&buf, const int64_t &buf_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, allocator is nullptr");
  } else if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator_->alloc(buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc macro read info buffer", K(ret), K(buf_size));
  }
  return ret;
}

int ObSharedObjectReadHandle::get_data(ObIAllocator &allocator, char *&buf, int64_t &buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(wait())) {
    LOG_WARN("Fail to wait io finish", K(ret));
  } else if (OB_UNLIKELY(!addr_.is_block())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected addr type", K(ret), K(addr_));
  } else {
    const char *data_buf = object_handle_.get_buffer();
    const int64_t data_size = object_handle_.get_data_size();
    int64_t header_size = 0;
    if (!addr_.is_raw_block()) {
      if (OB_FAIL(verify_checksum(data_buf, data_size, header_size, buf_len))) {
        LOG_WARN("fail to verify checksum", K(ret), KP(data_buf), K(data_size), K(header_size), K(buf_len));
      }
    } else { // is raw block
      buf_len = data_size;
    }

    if (OB_FAIL(ret)) {
    } else if (allocator_ == &allocator) { // allocator is same, use shallow copy
      buf = const_cast<char *>(data_buf) + header_size;
    } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc buf", K(ret),K(buf_len));
    } else {
      MEMCPY(buf, data_buf + header_size, buf_len);
    }
  }

  return ret;
}


int ObSharedObjectReadHandle::get_data(const ObMetaDiskAddr &addr, const char *data_buf, const int64_t data_size, char *&buf, int64_t &buf_len)
{
  int ret = OB_SUCCESS;
  buf = nullptr;
  buf_len = 0;
  if (OB_ISNULL(data_buf) || 0 == data_size) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_buf), K(data_size));
  } else if (OB_UNLIKELY(!addr.is_block())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected addr type", K(ret), K(addr));
  } else {
    int64_t header_size = 0;
    if (!addr.is_raw_block()) {
      if (OB_FAIL(verify_checksum(data_buf, data_size, header_size, buf_len))) {
        LOG_WARN("fail to verify checksum", K(ret), KP(data_buf), K(data_size), K(header_size), K(buf_len));
      }
    } else { // is raw block
      buf_len = data_size;
    }
    if (OB_FAIL(ret)) {
    } else {
      buf = const_cast<char*>(data_buf) + header_size;
    }
  }
  return ret;
}

int ObSharedObjectReadHandle::verify_checksum(
    const char *data_buf,
    const int64_t data_size,
    int64_t &header_size,
    int64_t &buf_len)
{
  int ret = OB_SUCCESS;
  ObSharedObjectHeader header;
  int64_t pos = 0;
  int64_t checksum = 0;
  if (OB_UNLIKELY(nullptr == data_buf || data_size < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), K(data_size));
  } else if (OB_FAIL(header.deserialize(data_buf, data_size, pos))) {
    LOG_WARN("fail to deserialize header", K(ret), KP(data_buf), K(data_size));
  } else if (OB_UNLIKELY(data_size - pos < header.data_size_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected header", K(ret), K(header), K(pos), K(data_size));
  } else if (OB_UNLIKELY(header.checksum_
      != (checksum = ob_crc64_sse42(data_buf + pos, header.data_size_)))) {
    ret = OB_CHECKSUM_ERROR;
    LOG_WARN("Checksum error", K(ret), K(checksum), K(header));
  } else {
    header_size = pos;
    buf_len = header.data_size_;
  }
  return ret;
}

int ObSharedObjectReadHandle::set_addr_and_object_handle(
    const ObMetaDiskAddr &addr, const ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!object_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(object_handle));
  } else {
    addr_ = addr;
    object_handle_ = object_handle;
  }
  return ret;
}

int ObSharedObjectWriteHandle::get_write_ctx(ObSharedObjectsWriteCtx &write_ctx)
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
    for (int64_t i = 0; OB_SUCC(ret) && i < object_handles_.count(); ++i) {
      if (OB_FAIL(write_ctx.add_object_id(object_handles_.at(i).get_macro_id()))) {
        LOG_WARN("Fail to add block id", K(ret), K(i), K(object_handles_.at(i)));
      }
    }
  }
  return ret;
}

void ObSharedObjectBatchHandle::reset()
{
  write_ctxs_.reset();
  ObSharedObjectBaseHandle::reset();
}
bool ObSharedObjectBatchHandle::is_valid() const
{
  return object_handles_.count() > 0 && addrs_.count() > 0
      && write_ctxs_.count() == addrs_.count();
}

int ObSharedObjectBatchHandle::batch_get_write_ctx(ObIArray<ObSharedObjectsWriteCtx> &write_ctxs)
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

void ObSharedObjectLinkHandle::reset()
{
  write_ctx_.clear();
  ObSharedObjectBaseHandle::reset();
}

bool ObSharedObjectLinkHandle::is_valid() const
{
  return object_handles_.count() > 0 && addrs_.count() == 1; // only record last addr
}

int ObSharedObjectLinkHandle::get_write_ctx(ObSharedObjectsWriteCtx &write_ctx)
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

int ObSharedObjectLinkIter::init(const ObMetaDiskAddr &head)
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

int ObSharedObjectLinkIter::reuse()
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

int ObSharedObjectLinkIter::get_next_block(ObIAllocator &allocator, char *&buf, int64_t &buf_len)
{
  int ret = OB_SUCCESS;
  ObSharedObjectReadHandle block_handle(allocator);
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

int ObSharedObjectLinkIter::get_next_macro_id(MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("share_block");
  ObSharedObjectReadHandle block_handle(allocator);
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

int ObSharedObjectLinkIter::read_next_block(ObSharedObjectReadHandle &shared_obj_handle)
{
  int ret = OB_SUCCESS;
  ObSharedObjectReadInfo read_info;
  ObSharedObjectHeader header;
  read_info.addr_ = cur_;
  read_info.ls_epoch_ = 0;/* ls_epoch for share storage */
  read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
  if (OB_FAIL(ObSharedObjectReaderWriter::async_read(read_info, shared_obj_handle))) {
    LOG_WARN("Fail to read block", K(ret), K(read_info));
  } else if (OB_FAIL(shared_obj_handle.wait())) {
    LOG_WARN("Fail to wait read io finish", K(ret), K(shared_obj_handle));
  } else {
    int64_t pos = 0;
    const char *buf = shared_obj_handle.object_handle_.get_buffer();
    const int64_t data_size = shared_obj_handle.object_handle_.get_data_size();
    if (OB_FAIL(header.deserialize(buf, data_size, pos))) {
      LOG_WARN("failt to deserialize ObSharedObjectHeader", K(ret));
    } else {
      cur_ = header.prev_addr_;
    }
    LOG_DEBUG("get next link block", K(ret), K(head_), K(cur_), K(header));
  }
  return ret;
}

//=================================== ObSharedObjectIOCallback =============================
ObSharedObjectIOCallback::~ObSharedObjectIOCallback()
{
  if (nullptr != io_allocator_ && NULL != data_buf_) {
    io_allocator_->free(data_buf_);
  }
  io_allocator_ = nullptr;
  data_buf_ = nullptr;
}

int ObSharedObjectIOCallback::alloc_data_buf(const char *io_data_buffer, const int64_t data_size)
{
  int ret = alloc_and_copy_data(io_data_buffer, data_size, io_allocator_, data_buf_);
  return ret;
}

int ObSharedObjectIOCallback::inner_process(const char *data_buffer, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(size <= 0 || data_buffer == nullptr)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid data buffer size", K(ret), K(size), KP(data_buffer));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected addr type", K(ret), K(addr_));
  } else if (OB_FAIL(alloc_and_copy_data(data_buffer, size, io_allocator_, data_buf_))) {
    LOG_WARN("Fail to allocate memory, ", K(ret), K(size));
  } else {
    const char *raw_buf = nullptr; // buf without shared block header
    int64_t raw_buf_len = 0;
    int64_t header_size = 0;
    if (!addr_.is_raw_block()) {
      if (OB_FAIL(ObSharedObjectReadHandle::verify_checksum(data_buf_, size, header_size, raw_buf_len))) {
        LOG_WARN("fail to verify checksum", K(ret), KP(data_buffer), K(size), K(header_size));
      } else {
        raw_buf = data_buf_ + header_size;
      }
    } else { // is raw block
      raw_buf = data_buf_;
      raw_buf_len = size;
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(do_process(raw_buf, raw_buf_len))) {
        LOG_WARN("fail to do process", K(ret), KP(raw_buf), K(raw_buf_len));
      }
    }
  }

  if (OB_FAIL(ret) && NULL != io_allocator_ && NULL != data_buf_) {
    io_allocator_->free(data_buf_);
    data_buf_ = NULL;
  }
  return ret;
}

const char *ObSharedObjectIOCallback::get_data()
{
  return data_buf_;
}

//=================================== ObSharedObjectReaderWriter =============================
const MacroBlockId ObSharedObjectHeader::DEFAULT_MACRO_ID(0, MacroBlockId::AUTONOMIC_BLOCK_INDEX, 0);
ObSharedObjectReaderWriter::ObSharedObjectReaderWriter()
    : mutex_(), data_("SHARE_BLOCK", 0, false, false/*use_fixed_blk*/),
      object_handle_(), offset_(0), align_offset_(0), write_align_size_(0),
      hanging_(false), need_align_(false), need_cross_(false),
      is_inited_(false)
{}

ObSharedObjectReaderWriter::~ObSharedObjectReaderWriter()
{
  reset();
}

int ObSharedObjectReaderWriter::init(
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
  } else if (OB_FAIL(data_.clean())) {
    LOG_WARN("fail to memset 0 data_", KR(ret));
  } else {
    offset_ = 0;
    align_offset_ = 0;
    write_align_size_ = DIO_READ_ALIGN_SIZE; // 4K
    hanging_ = false;
    need_align_ = need_align;
    need_cross_ = need_cross;
    if (GCTX.is_shared_storage_mode()) {
      hanging_ = false;
      is_inited_ = true;
    } else {
      if (OB_FAIL(reserve_header())) {
        LOG_WARN("fail to reserve header when init", K(ret));
      } else {
        is_inited_ = true;
      }
    }
  }
  return ret;
}
void ObSharedObjectReaderWriter::reset()
{
  data_.reset();
  object_handle_.reset();
  offset_ = 0;
  align_offset_ = 0;
  write_align_size_ = 0;
  hanging_ = false;
  need_align_ = false;
  need_cross_ = false;
  is_inited_ = false;
}

int callback_do_write_io(const ObSharedObjectWriteInfo &write_info)
{
  int ret = OB_SUCCESS;
  if (!write_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid write info", K(ret), K(write_info));
  } else if (nullptr != write_info.write_callback_ && OB_FAIL(write_info.write_callback_->do_write_io())) {
    LOG_WARN("fail to start  write callback", K(ret));
  }
  return ret;
}

int callback_do_write_io(const ObIArray<ObSharedObjectWriteInfo> &write_infos)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < write_infos.count(); ++i) {
    if (OB_FAIL(callback_do_write_io(write_infos.at(i)))) {
      LOG_WARN("failed to start write callback", K(ret));
    }
  }
  return ret;
}

int ObSharedObjectReaderWriter::async_write(
    const ObSharedObjectWriteInfo &write_info,
    const blocksstable::ObStorageObjectOpt &curr_opt,
    ObSharedObjectWriteHandle &shared_obj_handle)
{
  int ret = OB_SUCCESS;
  ObSharedObjectWriteArgs write_args;
  ObMetaDiskAddr prev_addr;
  prev_addr.set_none_addr();
  write_args.with_header_ = false;
  write_args.object_opt_ = curr_opt;
  ObSharedObjectHeader header;
  ObSharedObjectsWriteCtx tmp_write_ctx;
  {
    lib::ObMutexGuard guard(mutex_);
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      LOG_WARN("Not init", K(ret));
    } else if (OB_UNLIKELY(!write_info.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arg", K(ret), K(write_info));
    } else if (GCTX.is_shared_storage_mode() && OB_FAIL(do_switch(write_args.object_opt_))) {
      LOG_WARN("fail to switch object for shared storage", K(ret), K(write_args));
    }
    if (FAILEDx(inner_write_block(
        header,
        write_info,
        write_args,
        shared_obj_handle,
        tmp_write_ctx))) {
      LOG_WARN("fail to write block", K(ret), K(write_info), K(write_args));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(callback_do_write_io(write_info))) {
    LOG_WARN("failed to start write callback", K(ret));
  }
  return ret;
}

int ObSharedObjectReaderWriter::async_batch_write(
    const ObIArray<ObSharedObjectWriteInfo> &write_infos,
    ObSharedObjectBatchHandle &shared_obj_handle,
    blocksstable::ObStorageObjectOpt &curr_opt)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else {
    lib::ObMutexGuard guard(mutex_);
    ObSharedObjectWriteArgs write_args;
    ObSharedObjectsWriteCtx write_ctx;
    if (GCTX.is_shared_storage_mode()
        && write_infos.count() >= 1
        && OB_FAIL(do_switch(curr_opt))) {
      LOG_WARN("fail to switch object for shared storage", K(ret), K(write_infos.at(0)));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < write_infos.count(); ++i) {
      // only the last need flush and align
      write_args.need_flush_ = (i == write_infos.count() - 1);
      write_args.need_align_ = (i == write_infos.count() - 1) ? need_align_ : false;
      write_args.object_opt_ = curr_opt;
      write_ctx.clear();
      if (OB_FAIL(inner_async_write(write_infos.at(i), write_args, shared_obj_handle, write_ctx))) {
        LOG_WARN("Fail to async write block", K(ret), K(i), K(write_infos.at(i)), K(write_args));
      } else if (OB_FAIL(shared_obj_handle.write_ctxs_.push_back(write_ctx))) {
        LOG_WARN("Fail to add write ctx", K(ret), K(write_ctx));
      } else {
        curr_opt = write_ctx.next_opt_;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(callback_do_write_io(write_infos))) {
    LOG_WARN("failed to start write callback", K(ret));
  }
  return ret;
}

int ObSharedObjectReaderWriter::async_link_write(
    const ObSharedObjectWriteInfo &write_info,
    const blocksstable::ObStorageObjectOpt &curr_opt,
    ObSharedObjectLinkHandle &shared_obj_handle)
{
  int ret = OB_SUCCESS;
  {
    lib::ObMutexGuard guard(mutex_);
    ObSharedObjectWriteArgs write_args;
    ObSharedObjectsWriteCtx write_ctx;
    write_args.object_opt_ = curr_opt;
    write_args.need_flush_ = true;
    write_args.need_align_ = need_align_;
    write_args.is_linked_ = true;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      LOG_WARN("Not init", K(ret));
    } else if (OB_FAIL(shared_obj_handle.wait())) {
      LOG_WARN("Fail to wait other blocks finish", K(ret), K(shared_obj_handle));
    } else if (GCTX.is_shared_storage_mode() && OB_FAIL(do_switch(write_args.object_opt_))) {
      LOG_WARN("fail to switch object for shared storage", K(ret), K(write_args));
    }

    if (FAILEDx(inner_async_write(write_info, write_args, shared_obj_handle, write_ctx))) {
      LOG_WARN("Fail to inner async write block", K(ret), K(write_info), K(write_args));
    } else if (OB_FAIL(shared_obj_handle.write_ctx_.set_addr(write_ctx.addr_))) {
      LOG_WARN("Fail to set addr to write ctx", K(ret), K(write_ctx));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < write_ctx.block_ids_.count(); ++i) {
        if (OB_FAIL(shared_obj_handle.write_ctx_.add_object_id(write_ctx.block_ids_.at(i)))) {
          LOG_WARN("Fail to add block id", K(ret), K(write_ctx));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(callback_do_write_io(write_info))) {
    LOG_WARN("failed to satrt write callback", K(ret));
  }
  return ret;
}

int ObSharedObjectReaderWriter::inner_async_write(
    const ObSharedObjectWriteInfo &write_info,
    const ObSharedObjectWriteArgs &write_args,
    ObSharedObjectBaseHandle &shared_obj_handle,
    ObSharedObjectsWriteCtx &write_ctx)
{
  int ret = OB_SUCCESS;
  if (need_cross_ && OB_FAIL(write_cross_block(write_info, write_args, shared_obj_handle))) {
    LOG_WARN("Fail to write cross block", K(ret), K(write_info));
  } else if (OB_FAIL(write_block(write_info, write_args, shared_obj_handle, write_ctx))) {
    LOG_WARN("Fail to write block", K(ret), K(write_info));
  }
  return ret;
}

int ObSharedObjectReaderWriter::reserve_header()
{
  int ret = OB_SUCCESS;
  ObMacroBlockCommonHeader common_header;
  common_header.reset();
  if (OB_FAIL(common_header.set_attr(ObMacroBlockCommonHeader::MacroBlockType::SharedMetaData))) {
    LOG_WARN("fail to set type for common header", K(ret), K(common_header));
  } else if (OB_UNLIKELY(offset_ > 0 || align_offset_ > 0)) {
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
    data_.clean();
  }
  return ret;
}

int ObSharedObjectReaderWriter::switch_object(ObStorageObjectHandle &object_handle,
                                              const blocksstable::ObStorageObjectOpt &next_opt)
{
  int ret = OB_SUCCESS;
  object_handle.reset();
  if (hanging_) {
    ObStorageObjectWriteInfo write_info;
    write_info.buffer_ = data_.data() + align_offset_;
    write_info.offset_ = align_offset_;
    write_info.size_ = upper_align(offset_ - align_offset_, write_align_size_);
    write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
    write_info.io_desc_.set_sealed();
    write_info.mtl_tenant_id_ = MTL_ID();

    // do not use object_handle_ to write, since it will be reset if failed
    object_handle = object_handle_;
    if (OB_FAIL(object_handle.async_write(write_info))) {
      LOG_WARN("Fail to async write block", K(ret), K(write_info));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(do_switch(next_opt))) {
      LOG_WARN("fail to switch block", K(ret), K(next_opt));
    }
  }
  return ret;
}

int ObSharedObjectReaderWriter::do_switch(const ObStorageObjectOpt &opt)
{
  int ret = OB_SUCCESS;
  hanging_ = false;
  data_.reuse();
  object_handle_.reset();
  offset_ = 0;
  align_offset_ = 0;
  if (OB_FAIL(OB_STORAGE_OBJECT_MGR.alloc_object(opt, object_handle_))) {
    LOG_WARN("fail to alloc object", K(ret));
  } else if (OB_FAIL(data_.clean())) {
    LOG_WARN("fail to memset 0 data_", KR(ret));
  } else if (GCTX.is_shared_storage_mode()) {
    hanging_ = false;
  } else {
    if (OB_FAIL(reserve_header())) {
      LOG_WARN("fail to reserve header when init", K(ret));
    }
  }
  return ret;
}

int ObSharedObjectReaderWriter::calc_store_size(
    const int64_t total_size,
    const bool need_align,
    int64_t &store_size,
    int64_t &align_store_size)
{
  int ret = OB_SUCCESS;
  store_size = 0;
  align_store_size = 0;
  store_size = total_size;
  const int64_t next_align_offset = upper_align(offset_ + store_size, write_align_size_);
  align_store_size = next_align_offset - align_offset_;
  if (need_align) {
    store_size = next_align_offset - offset_;
  }
  if (!GCTX.is_shared_storage_mode() && OB_UNLIKELY(store_size > DEFAULT_MACRO_BLOCK_SIZE)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Not supported object size", K(ret), K_(offset), K_(align_offset), K(store_size));
  }
  return ret;
}

int ObSharedObjectReaderWriter::inner_write_block(
    const ObSharedObjectHeader &header,
    const ObSharedObjectWriteInfo &write_info,
    const ObSharedObjectWriteArgs &write_args,
    ObSharedObjectBaseHandle &shared_obj_handle,
    ObSharedObjectsWriteCtx &write_ctx)
{
  int ret = OB_SUCCESS;
  ObStorageObjectHandle object_handle;
  ObMetaDiskAddr addr;
  const int64_t blk_size = write_args.with_header_ ? header.header_size_ + header.data_size_ : write_info.size_;
  int64_t store_size = 0, align_store_size = 0;
  bool need_align = write_args.need_align_;
  bool need_flush = write_args.need_flush_;
  write_ctx.next_opt_ = write_args.object_opt_;
  if (OB_FAIL(calc_store_size(blk_size, need_align, store_size, align_store_size))) {
    LOG_WARN("fail to calc store size", K(ret));
  } else if (!object_handle_.get_macro_id().is_valid()
      && OB_FAIL(OB_STORAGE_OBJECT_MGR.alloc_object(write_args.object_opt_, object_handle_))) {
    LOG_WARN("fail to alloc new object", K(ret));
  } else if (store_size + offset_ > DEFAULT_MACRO_BLOCK_SIZE) {
    if (OB_FAIL(write_ctx.advance_data_seq())) {
      LOG_WARN("Fail to get next opt", K(ret), K(write_args), K(write_ctx.next_opt_));
    } else if (OB_FAIL(switch_object(object_handle, write_ctx.next_opt_))) {
      LOG_WARN("Fail to switch new block", K(ret));
    } else if (object_handle.is_valid() && OB_FAIL(shared_obj_handle.add_object_handle(object_handle))) {
      LOG_WARN("Fail to flush last macro block", K(ret), K(object_handle));
    } else if (OB_FAIL(calc_store_size(blk_size, need_align, store_size, align_store_size))) {
      LOG_WARN("fail to calc store size", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    object_handle.reset();
    int64_t pos = 0;
    const int64_t prev_pos = data_.pos();
    const int64_t prev_offset = offset_;
    const int64_t prev_align_offset = align_offset_;
    const bool prev_hanging = hanging_;
    if (write_args.with_header_ && OB_FAIL(header.serialize(data_.current(), header.header_size_, pos))) {
      LOG_WARN("Fail to serialize header", K(ret), K(header));
    } else {
      MEMCPY(data_.current() + pos, write_info.buffer_, write_info.size_);
      ObStorageObjectWriteInfo object_info;
      object_info.buffer_ = data_.data() + align_offset_;
      object_info.offset_ = align_offset_;
      object_info.size_ = align_store_size;
      object_info.mtl_tenant_id_ = MTL_ID();
      object_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
      object_info.io_desc_.set_unsealed();
      object_info.io_desc_.set_sys_module_id(ObIOModule::SHARED_BLOCK_RW_IO);
      object_info.ls_epoch_id_ = write_info.ls_epoch_;

      if (OB_FAIL(ret)) {
      } else if (OB_NOT_NULL(write_info.write_callback_) && need_flush) {
        ObLogicMacroBlockId unused_logic_id;
        if (OB_FAIL(write_info.write_callback_->write(object_handle_, unused_logic_id, const_cast<char*>(object_info.buffer_), object_info.size_, 0/* row count,unused*/))) {
          LOG_WARN("failed to write call back", K(ret));
        }
      }

      if (OB_FAIL(ret)) {
      } else {
      // io_callback
        if (OB_FAIL(addr.set_block_addr(object_handle_.get_macro_id(),
                                        offset_,
                                        blk_size,
                                        write_args.with_header_ ? ObMetaDiskAddr::DiskType::BLOCK : ObMetaDiskAddr::DiskType::RAW_BLOCK))) {
          LOG_WARN("Fail to set block addr", K(ret));
        } else if (OB_FAIL(shared_obj_handle.add_meta_addr(addr))) {
          LOG_WARN("Fail to add meta addr", K(ret), K(addr));
        } else if (OB_FAIL(data_.advance(store_size))) {
          LOG_WARN("Fail to advance size", K(ret), K(store_size));
        } else if (OB_FAIL(write_ctx.set_addr(addr))) {
          LOG_WARN("Fail to add addr to write ctx", K(ret), K(addr));
        } else if (OB_FAIL(write_ctx.add_object_id(addr.block_id()))) {
          LOG_WARN("Fail to add block id to write ctx", K(ret), K(addr));
        } else {
          offset_ += store_size;
        }
        if (OB_SUCC(ret) && need_flush) {
          object_handle = object_handle_;
          if (OB_FAIL(object_handle.async_write(object_info))) {
            LOG_WARN("Fail to async write object", K(ret), K(object_info));
          } else if (OB_FAIL(shared_obj_handle.add_object_handle(object_handle))) {
            LOG_WARN("Fail to add object handle", K(ret), K(object_handle), K(addr));
          } else if (OB_FAIL(write_ctx.advance_data_seq())) {
            // update next_opt of write_ctx, which will be return to the caller to keep the sequential writing macro_seq.
            LOG_WARN("Fail to get next opt", K(ret), K(write_args), K(write_ctx.next_opt_));
          } else {
            hanging_ = false;
            align_offset_ = lower_align(offset_, write_align_size_);
          }
        } else if (!need_flush) {
          hanging_ = true;
        }
        LOG_DEBUG("inner write block", K(ret), K(header), K(write_info), K(need_flush),
            K(need_align), K(store_size), K(align_store_size), K(offset_), K(align_offset_),
            K(hanging_), K(addr), K(object_handle));
      }
    }
    // roll back status
    if (OB_FAIL(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(data_.set_pos(prev_pos))) {
        LOG_ERROR("fail to roll back data buffer", K(ret), K(tmp_ret), K(prev_pos), K(header), K(write_args));
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

void ObSharedObjectReaderWriter::get_cur_shared_block(blocksstable::MacroBlockId &macro_id)
{
  lib::ObMutexGuard guard(mutex_);
  macro_id = object_handle_.get_macro_id();
}

int ObSharedObjectReaderWriter::write_block(
    const ObSharedObjectWriteInfo &write_info,
    const ObSharedObjectWriteArgs &write_args,
    ObSharedObjectBaseHandle &shared_obj_handle,
    ObSharedObjectsWriteCtx &write_ctx)
{
  int ret = OB_SUCCESS;
  ObMetaDiskAddr prev_addr;
  prev_addr.set_none_addr();
  if (OB_UNLIKELY(!write_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid shared block write info", K(ret), K(write_info));
  } else if (write_args.is_linked_ && shared_obj_handle.addrs_.count() > 0) {
    prev_addr = shared_obj_handle.addrs_.at(0);
    shared_obj_handle.reset(); // clear prev blocks info
  }
  if (OB_SUCC(ret)) {
    ObSharedObjectHeader header;
    header.cur_block_idx_ = 1;
    header.total_block_cnt_ = 1;
    header.data_size_ = write_info.size_;
    header.checksum_ = ob_crc64_sse42(write_info.buffer_, write_info.size_);
    header.next_macro_id_ = ObSharedObjectHeader::DEFAULT_MACRO_ID;
    header.prev_addr_ = prev_addr;
    header.header_size_ = header.get_serialize_size();
    if (OB_FAIL(inner_write_block(header, write_info, write_args, shared_obj_handle, write_ctx))) {
      LOG_WARN("Fail to write block", K(ret), K(write_info), K(write_args));
    }
  }
  return ret;
}

int ObSharedObjectReaderWriter::write_cross_block(
    const ObSharedObjectWriteInfo &write_info,
    const ObSharedObjectWriteArgs &write_args,
    ObSharedObjectBaseHandle &shared_obj_handle)
{
  UNUSED(write_info);
  UNUSED(write_args);
  UNUSED(shared_obj_handle);
  return OB_NOT_SUPPORTED;
}

int ObSharedObjectReaderWriter::async_read(
    const ObSharedObjectReadInfo &read_info,
    ObSharedObjectReadHandle &shared_obj_handle)
{
  int ret = OB_SUCCESS;
  ObStorageObjectHandle object_handle;
  ObStorageObjectReadInfo object_read_info;
  object_read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
  object_read_info.io_timeout_ms_ = read_info.io_timeout_ms_;
#ifdef OB_BUILD_SHARED_STORAGE
  object_read_info.ls_epoch_id_ = read_info.ls_epoch_;
#endif
  object_read_info.io_desc_.set_sys_module_id(ObIOModule::SHARED_BLOCK_RW_IO);
  object_read_info.io_callback_ = read_info.io_callback_;
  object_read_info.mtl_tenant_id_ = MTL_ID();
  if (OB_UNLIKELY(!read_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid shared block read info", K(ret), K(read_info));
  } else if (OB_FAIL(read_info.addr_.get_block_addr(
      object_read_info.macro_block_id_,
      object_read_info.offset_,
      object_read_info.size_))) {
    LOG_WARN("Fail to get block addr", K(ret), K(read_info));
  } else if (nullptr == read_info.io_callback_
      && OB_FAIL(shared_obj_handle.alloc_io_buf(object_read_info.buf_, object_read_info.size_))) {
    LOG_WARN("Fail to alloc io buf", K(ret), K(object_read_info));
  } else if (OB_FAIL(object_handle.async_read(object_read_info))) {
    LOG_WARN("Fail to async read block", K(ret), K(object_read_info));
  } else if (OB_FAIL(shared_obj_handle.set_addr_and_object_handle(read_info.addr_, object_handle))) {
    LOG_WARN("Fail to add macro handle", K(ret), K(object_read_info));
  }
  return ret;
}

int ObSharedObjectReaderWriter::parse_data_from_object(
    ObStorageObjectHandle &object_handle,
    const ObMetaDiskAddr addr,
    char *&buf,
    int64_t &buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!object_handle.is_valid() || !addr.is_valid() || !addr.is_block())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(object_handle), K(addr));
  } else {
    const char *block_buf = object_handle.get_buffer();
    const int64_t block_buf_len = object_handle.get_data_size();
    if (OB_UNLIKELY(addr.offset() + addr.size() > block_buf_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("block_buf is not enough", K(ret), K(addr), K(block_buf_len));
    } else if (!addr.is_raw_block()) {
      int64_t header_size = 0;
      if (OB_FAIL(ObSharedObjectReadHandle::verify_checksum(block_buf + addr.offset(), addr.size(), header_size, buf_len))) {
        LOG_WARN("fail to verify checksum", K(ret), K(addr));
      } else {
        buf = const_cast<char*>(block_buf) +  addr.offset() + header_size;
      }
    } else { // is raw block
      buf = const_cast<char*>(block_buf) + addr.offset();
      buf_len = addr.size();
    }
  }
  return ret;
}



}  // end namespace storage
}  // end namespace oceanbase
