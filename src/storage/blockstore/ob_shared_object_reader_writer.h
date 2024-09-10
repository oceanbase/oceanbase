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

#ifndef OB_STORAGE_SLOG_SHARED_OBJECT_READER_WRITER_H
#define OB_STORAGE_SLOG_SHARED_OBJECT_READER_WRITER_H

#include "storage/meta_mem/ob_meta_obj_struct.h"
#include "storage/blocksstable/ob_macro_block_handle.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "storage/blocksstable/ob_storage_object_handle.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/blocksstable/ob_imacro_block_flush_callback.h"

namespace oceanbase
{
namespace storage
{
class ObSharedObjectIOCallback;
struct ObSharedObjectWriteInfo final
{
public:
  ObSharedObjectWriteInfo();
  ~ObSharedObjectWriteInfo() = default;
  ObSharedObjectWriteInfo &operator=(const ObSharedObjectWriteInfo &other);
  bool is_valid() const;
  void reset();
  TO_STRING_KV(KP_(buffer), K_(offset), K_(size), K_(ls_epoch), K_(io_desc), K_(io_callback), KP_(write_callback));
public:
  const char *buffer_;
  int64_t offset_;
  int64_t size_;
  int64_t ls_epoch_;
  common::ObIOFlag io_desc_;
  ObSharedObjectIOCallback *io_callback_;
  blocksstable::ObIMacroBlockFlushCallback *write_callback_;
};

struct ObSharedObjectReadInfo final
{
public:
  ObSharedObjectReadInfo()
    : addr_(), io_desc_(), io_callback_(nullptr), io_timeout_ms_(DEFAULT_IO_WAIT_TIME_MS), ls_epoch_(0)
  {}
  ~ObSharedObjectReadInfo() = default;
  bool is_valid() const;
  TO_STRING_KV(K_(addr), K_(io_desc), K_(io_callback), K_(io_timeout_ms), K_(ls_epoch));
public:
  ObMetaDiskAddr addr_;
  common::ObIOFlag io_desc_;
  ObSharedObjectIOCallback *io_callback_;
  int64_t io_timeout_ms_;
  int64_t ls_epoch_;
  DISALLOW_COPY_AND_ASSIGN(ObSharedObjectReadInfo);
};

struct ObSharedObjectsWriteCtx final
{
public:
  ObSharedObjectsWriteCtx()
    : addr_(), block_ids_(), next_opt_()
  {
    block_ids_.set_attr(ObMemAttr(MTL_ID(), "SharedBlkWCtx"));
  }
  ~ObSharedObjectsWriteCtx();
  bool is_valid() const;
  int set_addr(const ObMetaDiskAddr &addr); // overwrite
  int add_object_id(const blocksstable::MacroBlockId &object_id); // distinct
  void clear();
  int assign(const ObSharedObjectsWriteCtx &other);
  int advance_data_seq(); //update next_opt_
  TO_STRING_KV(K_(addr), K_(block_ids), K_(next_opt));
public:
  ObMetaDiskAddr addr_;
  ObArray<blocksstable::MacroBlockId> block_ids_;
  blocksstable::ObStorageObjectOpt next_opt_;
  DISALLOW_COPY_AND_ASSIGN(ObSharedObjectsWriteCtx);
};

struct ObSharedObjectHeader final
{
public:
  static const uint16_t OB_SHARED_BLOCK_HEADER_MAGIC = 1386;
  static const uint16_t OB_SHARED_BLOCK_HEADER_VERSION_V1 = 1;
  static const uint16_t OB_SHARED_BLOCK_HEADER_VERSION_V2 = 2;
  static const blocksstable::MacroBlockId DEFAULT_MACRO_ID; // -1
  ObSharedObjectHeader()
    : magic_(OB_SHARED_BLOCK_HEADER_MAGIC), version_(OB_SHARED_BLOCK_HEADER_VERSION_V2),
      cur_block_idx_(0), total_block_cnt_(0), header_size_(0), data_size_(0),
      checksum_(0), next_macro_id_(DEFAULT_MACRO_ID)
  {
    prev_addr_.set_none_addr();
  }
  ~ObSharedObjectHeader() = default;
  bool is_valid() const;

  NEED_SERIALIZE_AND_DESERIALIZE;
  TO_STRING_KV(K_(magic), K_(version), K_(header_size), K_(data_size),
               K_(cur_block_idx), K_(total_block_cnt), K_(checksum),
               K_(next_macro_id), K_(prev_addr));
public:
  uint16_t magic_;
  uint16_t version_;
  uint16_t cur_block_idx_;
  uint16_t total_block_cnt_;
  int32_t header_size_;
  int32_t data_size_;
  int64_t checksum_;
  blocksstable::MacroBlockId next_macro_id_; // -1 indicates end
  ObMetaDiskAddr prev_addr_;
};

class ObSharedObjectBaseHandle
{
  friend class ObSharedObjectReaderWriter;
  friend class ObSharedObjectLinkIter;
public:
  ObSharedObjectBaseHandle()
    : object_handles_(), addrs_()
  {}
  virtual ~ObSharedObjectBaseHandle() = default;
  void reset();
  TO_STRING_KV(K(addrs_.count()), K(object_handles_.count()), K_(addrs), K_(object_handles));
protected:
  int wait();
  int add_object_handle(const blocksstable::ObStorageObjectHandle &object_handle);
  int add_meta_addr(const ObMetaDiskAddr &addr);
protected:
  ObSEArray<blocksstable::ObStorageObjectHandle, 1> object_handles_;
  ObSEArray<ObMetaDiskAddr, 1> addrs_;
  DISALLOW_COPY_AND_ASSIGN(ObSharedObjectBaseHandle);
};

class ObSharedObjectIOCallback : public common::ObIOCallback
{
public:
  ObSharedObjectIOCallback(common::ObIAllocator *io_allocator, const ObMetaDiskAddr addr,
                           const common::ObIOCallbackType type)
    : common::ObIOCallback(type), io_allocator_(io_allocator), addr_(addr), data_buf_(nullptr) {}
  virtual ~ObSharedObjectIOCallback();
  virtual int alloc_data_buf(const char *io_data_buffer, const int64_t data_size) override;
  int inner_process(const char *data_buffer, const int64_t size) override;
  virtual int do_process(const char *buf, const int64_t buf_len) = 0;
  virtual int64_t size() const = 0;
  virtual const char *get_data() override;
  virtual ObIAllocator *get_allocator() override { return io_allocator_; }

  VIRTUAL_TO_STRING_KV(K_(addr), KP_(io_allocator), KP_(data_buf));
  bool is_valid() const
  {
    return addr_.is_block() && nullptr != io_allocator_;
  }

private:
  ObIAllocator *io_allocator_;
  ObMetaDiskAddr addr_;
  char *data_buf_;   // actual data buffer
};


class ObSharedObjectReadHandle final
{
  friend class ObSharedObjectReaderWriter;
  friend class ObSharedObjectLinkIter;
  friend class ObSharedObjectIOCallback;
public:
  ObSharedObjectReadHandle();
  ObSharedObjectReadHandle(ObIAllocator &allocator);
  ~ObSharedObjectReadHandle();
  ObSharedObjectReadHandle(const ObSharedObjectReadHandle &other);
  ObSharedObjectReadHandle &operator=(const ObSharedObjectReadHandle &other);
  bool is_valid() const;
  bool is_empty() const;
  int wait();
  int get_data(ObIAllocator &allocator, char *&buf, int64_t &buf_len);

  static int get_data(const ObMetaDiskAddr &addr, const char *src_buf, const int64_t src_buf_len,
                      char *&buf, int64_t &buf_len);
  void reset();
  TO_STRING_KV(K_(addr), K_(object_handle));

private:
  static int verify_checksum(
      const char *data_buf,
      const int64_t data_size,
      int64_t &header_size,
      int64_t &buf_len);
  int alloc_io_buf(char *&buf, const int64_t &buf_size);
  int set_addr_and_object_handle(const ObMetaDiskAddr &addr, const blocksstable::ObStorageObjectHandle &object_handle);

private:
  ObIAllocator *allocator_;
  blocksstable::ObStorageObjectHandle object_handle_;
  ObMetaDiskAddr addr_;
};

class ObSharedObjectWriteHandle final : public ObSharedObjectBaseHandle
{
  friend class ObSharedObjectReaderWriter;
public:
  ObSharedObjectWriteHandle() = default;
  ~ObSharedObjectWriteHandle() = default;
  bool is_valid() const;
  int get_write_ctx(ObSharedObjectsWriteCtx &write_ctx);
  DISALLOW_COPY_AND_ASSIGN(ObSharedObjectWriteHandle);
};

class ObSharedObjectBatchHandle final : public ObSharedObjectBaseHandle
{
  friend class ObSharedObjectReaderWriter;
public:
  ObSharedObjectBatchHandle() = default;
  ~ObSharedObjectBatchHandle() = default;
  void reset();
  bool is_valid() const;
  int batch_get_write_ctx(ObIArray<ObSharedObjectsWriteCtx> &write_ctxs);
  INHERIT_TO_STRING_KV("ObSharedObjectBaseHandle", ObSharedObjectBaseHandle, K_(write_ctxs));
protected:
  ObArray<ObSharedObjectsWriteCtx> write_ctxs_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSharedObjectBatchHandle);
};
class ObSharedObjectLinkHandle final : public ObSharedObjectBaseHandle
{
  friend class ObSharedObjectReaderWriter;
public:
  ObSharedObjectLinkHandle() = default;
  ~ObSharedObjectLinkHandle() = default;
  void reset();
  bool is_valid() const;
  int get_write_ctx(ObSharedObjectsWriteCtx &write_ctx); // always get prev block
  INHERIT_TO_STRING_KV("ObSharedObjectBaseHandle", ObSharedObjectBaseHandle, K_(write_ctx));
protected:
  ObSharedObjectsWriteCtx write_ctx_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSharedObjectLinkHandle);
};

class ObSharedObjectLinkIter final
{
public:
  ObSharedObjectLinkIter()
    : head_(), cur_(), is_inited_(false)
  {}
  ~ObSharedObjectLinkIter() = default;
  int init(const ObMetaDiskAddr &head);
  int reuse();
  int get_next_block(ObIAllocator &allocator, char *&buf, int64_t &buf_len);
  int get_next_macro_id(blocksstable::MacroBlockId &macro_id);
  TO_STRING_KV(K_(head), K_(cur), K_(is_inited));
private:
  int read_next_block(ObSharedObjectReadHandle &shared_obj_handle);
private:
  ObMetaDiskAddr head_;
  ObMetaDiskAddr cur_;
  bool is_inited_;
};

class ObSharedObjectReaderWriter final
{
private:
  struct ObSharedObjectWriteArgs;
public:
  ObSharedObjectReaderWriter();
  ~ObSharedObjectReaderWriter();
  int init(
      const bool need_align = true,
      const bool need_cross = false);
  void reset();
  void get_cur_shared_block(blocksstable::MacroBlockId &macro_id);
  static int async_read(const ObSharedObjectReadInfo &read_info, ObSharedObjectReadHandle &shared_obj_handle);
  static int parse_data_from_object(
      blocksstable::ObStorageObjectHandle &object_handle,
      const ObMetaDiskAddr addr,
      char *&buf,
      int64_t &buf_len);
  int async_write(
      const ObSharedObjectWriteInfo &write_info,
      const blocksstable::ObStorageObjectOpt &curr_opt,
      ObSharedObjectWriteHandle &shared_obj_handle);
  int async_batch_write(
      const common::ObIArray<ObSharedObjectWriteInfo> &write_infos,
      ObSharedObjectBatchHandle &shared_obj_handle,
      blocksstable::ObStorageObjectOpt &curr_opt);
  int async_link_write(
      const ObSharedObjectWriteInfo &write_infos,
      const blocksstable::ObStorageObjectOpt &curr_opt,
      ObSharedObjectLinkHandle &shared_obj_handle);
private:
  int inner_async_write(
      const ObSharedObjectWriteInfo &write_info,
      const ObSharedObjectWriteArgs &write_args,
      ObSharedObjectBaseHandle &shared_obj_handle,
      ObSharedObjectsWriteCtx &write_ctx);
  int write_block(
      const ObSharedObjectWriteInfo &write_info,
      const ObSharedObjectWriteArgs &write_args,
      ObSharedObjectBaseHandle &shared_obj_handle,
      ObSharedObjectsWriteCtx &write_ctx); // not cross
  int write_cross_block(
      const ObSharedObjectWriteInfo &write_info,
      const ObSharedObjectWriteArgs &write_args,
      ObSharedObjectBaseHandle &shared_obj_handle); // cross
  int calc_store_size(
      const int64_t total_size,
      const bool need_align,
      int64_t &store_size,
      int64_t &align_store_size);
  int inner_write_block(
      const ObSharedObjectHeader &header,
      const ObSharedObjectWriteInfo &write_info,
      const ObSharedObjectWriteArgs &write_args,
      ObSharedObjectBaseHandle &shared_obj_handle,
      ObSharedObjectsWriteCtx &write_ctx);
  int switch_object(blocksstable::ObStorageObjectHandle &object_handle,
                    const blocksstable::ObStorageObjectOpt &next_opt);
  int do_switch(const blocksstable::ObStorageObjectOpt &opt);
  int reserve_header();
private:
struct ObSharedObjectWriteArgs final
{
public:
  ObSharedObjectWriteArgs()
    : need_flush_(true), need_align_(true), is_linked_(false), with_header_(true), object_opt_()
  {}
  ~ObSharedObjectWriteArgs() = default;
  TO_STRING_KV(K_(need_flush), K_(need_align), K_(is_linked), K_(with_header), K_(object_opt));
  bool need_flush_;
  bool need_align_;
  bool is_linked_;
  bool with_header_;
  blocksstable::ObStorageObjectOpt object_opt_;
};
private:
  lib::ObMutex mutex_;
  blocksstable::ObSelfBufferWriter data_;
  blocksstable::ObStorageObjectHandle object_handle_;
  int64_t offset_;
  int64_t align_offset_;
  int64_t write_align_size_;
  bool hanging_;
  bool need_align_;
  bool need_cross_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObSharedObjectReaderWriter);
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_STORAGE_SLOG_SHARED_BLOCK_READER_WRITER_H
