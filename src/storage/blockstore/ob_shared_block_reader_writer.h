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

#ifndef OB_STORAGE_SLOG_SHARED_BLOCK_READER_WRITER_H
#define OB_STORAGE_SLOG_SHARED_BLOCK_READER_WRITER_H

#include "storage/meta_mem/ob_meta_obj_struct.h"
#include "storage/blocksstable/ob_macro_block_handle.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/blocksstable/ob_data_buffer.h"

namespace oceanbase
{
namespace storage
{
class ObSharedBlockIOCallback;
struct ObSharedBlockWriteInfo final
{
public:
  ObSharedBlockWriteInfo();
  ~ObSharedBlockWriteInfo() = default;
  ObSharedBlockWriteInfo &operator=(const ObSharedBlockWriteInfo &other);
  bool is_valid() const;
  void reset();
  TO_STRING_KV(KP_(buffer), K_(offset), K_(size), K_(io_desc), K_(io_callback));
public:
  const char *buffer_;
  int64_t offset_;
  int64_t size_;
  common::ObIOFlag io_desc_;
  ObSharedBlockIOCallback *io_callback_;
};

struct ObSharedBlockReadInfo final
{
public:
  ObSharedBlockReadInfo()
    : addr_(), io_desc_(), io_callback_(nullptr), io_timeout_ms_(DEFAULT_IO_WAIT_TIME_MS)
  {}
  ~ObSharedBlockReadInfo() = default;
  bool is_valid() const;
  TO_STRING_KV(K_(addr), K_(io_desc), K_(io_callback), K_(io_timeout_ms));
public:
  ObMetaDiskAddr addr_;
  common::ObIOFlag io_desc_;
  ObSharedBlockIOCallback *io_callback_;
  int64_t io_timeout_ms_;
  DISALLOW_COPY_AND_ASSIGN(ObSharedBlockReadInfo);
};

struct ObSharedBlocksWriteCtx final
{
public:
  ObSharedBlocksWriteCtx()
    : addr_(), block_ids_()
  {
    block_ids_.set_attr(ObMemAttr(MTL_ID(), "SharedBlkWCtx"));
  }
  ~ObSharedBlocksWriteCtx();
  bool is_valid() const;
  int set_addr(const ObMetaDiskAddr &addr); // overwrite
  int add_block_id(const blocksstable::MacroBlockId &block_id); // distinct
  void clear();
  int assign(const ObSharedBlocksWriteCtx &other);
  TO_STRING_KV(K_(addr), K_(block_ids));
public:
  ObMetaDiskAddr addr_;
  ObArray<blocksstable::MacroBlockId> block_ids_;
  DISALLOW_COPY_AND_ASSIGN(ObSharedBlocksWriteCtx);
};

struct ObSharedBlockHeader final
{
  static const uint16_t OB_LINKED_BLOCK_HEADER_MAGIC = 1386;
  static const uint16_t OB_LINKED_BLOCK_HEADER_VERSION = 1;
  static const blocksstable::MacroBlockId DEFAULT_MACRO_ID; // -1
  OB_UNIS_VERSION(1);
public:
  ObSharedBlockHeader()
    : magic_(OB_LINKED_BLOCK_HEADER_MAGIC), version_(OB_LINKED_BLOCK_HEADER_VERSION),
      cur_block_idx_(0), total_block_cnt_(0), header_size_(0), data_size_(0),
      checksum_(0), next_macro_id_(DEFAULT_MACRO_ID)
  {
    prev_addr_.set_none_addr();
  }
  ~ObSharedBlockHeader() = default;
  bool is_valid() const;
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

class ObSharedBlockBaseHandle
{
  friend class ObSharedBlockReaderWriter;
  friend class ObSharedBlockLinkIter;
public:
  ObSharedBlockBaseHandle()
    : macro_handles_(), addrs_()
  {}
  virtual ~ObSharedBlockBaseHandle() = default;
  void reset();
  TO_STRING_KV(K(addrs_.count()), K(macro_handles_.count()), K_(addrs), K_(macro_handles));
protected:
  int wait();
  int add_macro_handle(const blocksstable::ObMacroBlockHandle &macro_handle);
  int add_meta_addr(const ObMetaDiskAddr &addr);
protected:
  ObSEArray<blocksstable::ObMacroBlockHandle, 1> macro_handles_;
  ObSEArray<ObMetaDiskAddr, 1> addrs_;
  DISALLOW_COPY_AND_ASSIGN(ObSharedBlockBaseHandle);
};

class ObSharedBlockIOCallback : public common::ObIOCallback
{
public:
  ObSharedBlockIOCallback(common::ObIAllocator *io_allocator, const ObMetaDiskAddr addr)
    : io_allocator_(io_allocator), addr_(addr), data_buf_(nullptr) {}
  virtual ~ObSharedBlockIOCallback();
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


class ObSharedBlockReadHandle final
{
  friend class ObSharedBlockReaderWriter;
  friend class ObSharedBlockLinkIter;
  friend class ObSharedBlockIOCallback;
public:
  ObSharedBlockReadHandle();
  ObSharedBlockReadHandle(ObIAllocator &allocator);
  ~ObSharedBlockReadHandle();
  ObSharedBlockReadHandle(const ObSharedBlockReadHandle &other);
  ObSharedBlockReadHandle &operator=(const ObSharedBlockReadHandle &other);
  bool is_valid() const;
  bool is_empty() const;
  int wait();
  int get_data(ObIAllocator &allocator, char *&buf, int64_t &buf_len);
  void reset();
  TO_STRING_KV(K_(addr), K_(macro_handle));

private:
  static int verify_checksum(
      const char *data_buf,
      const int64_t data_size,
      int64_t &header_size,
      int64_t &buf_len);
  int alloc_io_buf(char *&buf, const int64_t &buf_size);
  int set_addr_and_macro_handle(const ObMetaDiskAddr &addr, const blocksstable::ObMacroBlockHandle &macro_handle);

private:
  ObIAllocator *allocator_;
  blocksstable::ObMacroBlockHandle macro_handle_;
  ObMetaDiskAddr addr_;
};

class ObSharedBlockWriteHandle final : public ObSharedBlockBaseHandle
{
  friend class ObSharedBlockReaderWriter;
public:
  ObSharedBlockWriteHandle() = default;
  ~ObSharedBlockWriteHandle() = default;
  bool is_valid() const;
  int get_write_ctx(ObSharedBlocksWriteCtx &write_ctx);
  DISALLOW_COPY_AND_ASSIGN(ObSharedBlockWriteHandle);
};

class ObSharedBlockBatchHandle final : public ObSharedBlockBaseHandle
{
  friend class ObSharedBlockReaderWriter;
public:
  ObSharedBlockBatchHandle() = default;
  ~ObSharedBlockBatchHandle() = default;
  void reset();
  bool is_valid() const;
  int batch_get_write_ctx(ObIArray<ObSharedBlocksWriteCtx> &write_ctxs);
  INHERIT_TO_STRING_KV("ObSharedBlockBaseHandle", ObSharedBlockBaseHandle, K_(write_ctxs));
protected:
  ObArray<ObSharedBlocksWriteCtx> write_ctxs_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSharedBlockBatchHandle);
};
class ObSharedBlockLinkHandle final : public ObSharedBlockBaseHandle
{
  friend class ObSharedBlockReaderWriter;
public:
  ObSharedBlockLinkHandle() = default;
  ~ObSharedBlockLinkHandle() = default;
  void reset();
  bool is_valid() const;
  int get_write_ctx(ObSharedBlocksWriteCtx &write_ctx); // always get prev block
  INHERIT_TO_STRING_KV("ObSharedBlockBaseHandle", ObSharedBlockBaseHandle, K_(write_ctx));
protected:
  ObSharedBlocksWriteCtx write_ctx_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSharedBlockLinkHandle);
};

class ObSharedBlockLinkIter final
{
public:
  ObSharedBlockLinkIter()
    : head_(), cur_(), is_inited_(false)
  {}
  ~ObSharedBlockLinkIter() = default;
  int init(const ObMetaDiskAddr &head);
  int reuse();
  int get_next_block(ObIAllocator &allocator, char *&buf, int64_t &buf_len);
  int get_next_macro_id(blocksstable::MacroBlockId &macro_id);
  TO_STRING_KV(K_(head), K_(cur), K_(is_inited));
private:
  int read_next_block(ObSharedBlockReadHandle &block_handle);
private:
  ObMetaDiskAddr head_;
  ObMetaDiskAddr cur_;
  bool is_inited_;
};

class ObSharedBlockReaderWriter final
{
private:
  struct ObSharedBlockWriteArgs;
public:
  ObSharedBlockReaderWriter();
  ~ObSharedBlockReaderWriter();
  int init(
      const bool need_align = true,
      const bool need_cross = false);
  void reset();
  void get_cur_shared_block(blocksstable::MacroBlockId &macro_id);
  static int async_read(const ObSharedBlockReadInfo &read_info, ObSharedBlockReadHandle &block_handle);
  static int parse_data_from_macro_block(
      blocksstable::ObMacroBlockHandle &macro_handle,
      const ObMetaDiskAddr addr,
      char *&buf, int64_t &buf_len);
  int async_write(
      const ObSharedBlockWriteInfo &write_info,
      ObSharedBlockWriteHandle &block_handle);
  int async_batch_write(
      const common::ObIArray<ObSharedBlockWriteInfo> &write_infos,
      ObSharedBlockBatchHandle &block_handle);
  int async_link_write(
      const ObSharedBlockWriteInfo &write_infos,
      ObSharedBlockLinkHandle &block_handle);
private:
  int inner_async_write(
      const ObSharedBlockWriteInfo &write_info,
      const ObSharedBlockWriteArgs &write_args,
      ObSharedBlockBaseHandle &block_handle,
      ObSharedBlocksWriteCtx &write_ctx);
  int write_block(
      const ObSharedBlockWriteInfo &write_info,
      const ObSharedBlockWriteArgs &write_args,
      ObSharedBlockBaseHandle &block_handle,
      ObSharedBlocksWriteCtx &write_ctx); // not cross
  int write_cross_block(
      const ObSharedBlockWriteInfo &write_info,
      const ObSharedBlockWriteArgs &write_args,
      ObSharedBlockBaseHandle &block_handle); // cross
  int calc_store_size(
      const int64_t total_size,
      const bool need_align,
      int64_t &store_size,
      int64_t &align_store_size);
  int inner_write_block(
      const ObSharedBlockHeader &header,
      const char *buf,
      const int64_t size,
      const ObSharedBlockWriteArgs &write_args,
      ObSharedBlockBaseHandle &block_handle);
  int switch_block(blocksstable::ObMacroBlockHandle &macro_handle);
  int reserve_header();
private:
struct ObSharedBlockWriteArgs final
{
public:
  ObSharedBlockWriteArgs()
    : need_flush_(true), need_align_(true), is_linked_(false), with_header_(true)
  {}
  ~ObSharedBlockWriteArgs() = default;
  TO_STRING_KV(K_(need_flush), K_(need_align), K_(is_linked), K_(with_header));
  bool need_flush_;
  bool need_align_;
  bool is_linked_;
  bool with_header_;
};
private:
  lib::ObMutex mutex_;
  blocksstable::ObSelfBufferWriter data_;
  blocksstable::ObMacroBlockHandle macro_handle_;
  int64_t offset_;
  int64_t align_offset_;
  int64_t write_align_size_;
  bool hanging_;
  bool need_align_;
  bool need_cross_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObSharedBlockReaderWriter);
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_STORAGE_SLOG_SHARED_BLOCK_READER_WRITER_H
