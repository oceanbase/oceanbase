/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 * This file is for define of plugin vector index util
 */
#ifndef OB_STORAGE_COLUMN_STORE_OB_CO_MERGE_LOG_H_
#define OB_STORAGE_COLUMN_STORE_OB_CO_MERGE_LOG_H_

#include "lib/utility/ob_print_utils.h"
#include "lib/oblog/ob_log.h"
#include "lib/ob_define.h"
#include "lib/utility/ob_unify_serialize.h"
#include "storage/tmp_file/ob_tmp_file_manager.h"
#include "storage/compaction/ob_basic_tablet_merge_ctx.h"

namespace oceanbase
{
namespace compaction
{
struct ObCOMergeLogBuffer;
struct ObMergeLog final {
public:
  OB_UNIS_VERSION(1);
public:
  enum OpType : uint8_t {
    INSERT = 1,
    UPDATE,
    DELETE,
    REPLAY,
    DELETE_RANGE, // filtered by compaction_filter
    INVALID
  };
  ObMergeLog()
    : op_(INVALID),
      major_idx_(-1),
      row_id_(-1)
  {}
  ObMergeLog(const OpType op, const int32_t major_idx, const int64_t row_id)
    : op_(op),
      major_idx_(major_idx),
      row_id_(row_id)
  {}
  void reset()
  {
    op_ = INVALID;
    major_idx_ = -1;
    row_id_ = -1;
  }
  OB_INLINE void set_value(const OpType op, const int32_t major_idx, const int64_t row_id)
  {
    op_ = op;
    major_idx_ = major_idx;
    row_id_ = row_id;
  }
  bool is_valid() const { return OpType::INVALID > op_ && 0 < op_; }
  bool is_continuous(const ObMergeLog &pre)
  {
    return REPLAY == op_ &&
           (!pre.is_valid() ||
           (op_ == pre.op_ && major_idx_ == pre.major_idx_ && row_id_ >= pre.row_id_));
  }
  bool operator==(const ObMergeLog &other) const
  {
    return op_ == other.op_ && major_idx_ == other.major_idx_ && row_id_ == other.row_id_;
  }
  void operator=(const ObMergeLog &other)
  {
    op_ = other.op_;
    major_idx_ = other.major_idx_;
    row_id_ = other.row_id_;
  }
  TO_STRING_KV(K_(op), K_(major_idx), K_(row_id))
  OpType op_;
  int32_t major_idx_;
  int64_t row_id_;
};

class ObCOMergeProjector final
{
public:
  ObCOMergeProjector()
    : is_inited_(false),
      allocator_(nullptr),
      projector_(nullptr),
      projector_count_(0),
      project_row_()
    {}
  ~ObCOMergeProjector() { reset(); };
  int init(const ObStorageColumnGroupSchema &cg_schema, ObIAllocator &allocator);
  void reset();
  const blocksstable::ObDatumRow &get_project_row() const { return project_row_; }
  int project(const blocksstable::ObDatumRow &row);
  int project(const blocksstable::ObDatumRow &row, blocksstable::ObDatumRow &result_row, bool &is_all_nop) const;
  TO_STRING_KV(K_(is_inited), K_(projector), K_(project_row))
private:
  void clean_project_row();
private:
  bool is_inited_;
  ObIAllocator *allocator_;
  uint16_t *projector_;
  uint16_t projector_count_;
  blocksstable::ObDatumRow project_row_;
};

class ObCOMergeLogIterator
{
public:
  ObCOMergeLogIterator()
    : is_inited_(false)
  {}
  virtual ~ObCOMergeLogIterator() = default;
  virtual int init(ObBasicTabletMergeCtx &ctx, const int64_t idx, const int64_t cg_idx) = 0;
  virtual void reset() = 0;
  // when mergelog.op_ = REPLAY, row = nullptr
  virtual int get_next_log(ObMergeLog &mergelog, const blocksstable::ObDatumRow *&row) = 0;
  virtual int close() = 0;
  OB_INLINE bool is_inited() const { return is_inited_; }
  VIRTUAL_TO_STRING_KV(K_(is_inited));
protected:
  bool is_inited_;
};

template<typename CallbackImpl>
class ObCOMergeLogConsumer
{
public:
  static int consume_all_merge_log(ObCOMergeLogIterator &iter, CallbackImpl &callback);
};

// 24B
struct ObCOMergeLogPieceHeader
{
  ObCOMergeLogPieceHeader()
    : log_id_(0),
      piece_id_(0),
      total_pieces_(0),
      length_(0),
      log_length_(0)
  {}
  void operator=(const ObCOMergeLogPieceHeader &other)
  {
    log_id_ = other.log_id_;
    piece_id_ = other.piece_id_;
    total_pieces_ = other.total_pieces_;
    length_ = other.length_;
    log_length_ = other.log_length_;
  }
  uint64_t log_id_;
  uint32_t piece_id_; // start by 1
  uint32_t total_pieces_; // piece_id_ == total_pieces_ means last piece
  uint32_t length_;
  uint32_t log_length_;
  char data_[0];

  TO_STRING_KV(K_(log_id), K_(piece_id), K_(total_pieces), K_(length), K_(log_length));
};

// 12B
struct ObCOMergeLogBlockHeader
{
  ObCOMergeLogBlockHeader()
    : magic_num_(0),
      piece_count_(0),
      length_(0)
  {}
  uint32_t magic_num_;
  uint32_t piece_count_;
  uint32_t length_;
  char data_[0];

  TO_STRING_KV(K_(magic_num), K_(piece_count), K_(length));
  static const uint32_t MAGIC_NUM = 0x54329876; // magic number for block header
};

struct ObCOMergeLogBlock
{
public:
  ObCOMergeLogBlock()
    : block_size_(0),
      header_(nullptr)
  {}
  OB_INLINE int64_t block_max_writable_size() const
  {
    return block_size_ - sizeof(ObCOMergeLogBlockHeader); // exclude the header
  }
  OB_INLINE int64_t block_max_readable_size() const
  {
    return nullptr == header_ ? 0 : header_->length_;
  }
  TO_STRING_KV(K_(block_size), K_(header));
public:
  int64_t block_size_;
  ObCOMergeLogBlockHeader *header_;
};

class ObCOMergeLogBuffer
{
public:
  ObCOMergeLogBuffer(ObIAllocator &allocator)
    : is_inited_(false),
      data_(nullptr),
      capacity_(0),
      pos_(0),
      current_block_(),
      overflow_buffer_(nullptr),
      overflow_buffer_size_(0),
      allocator_(allocator)
  {}
  virtual ~ObCOMergeLogBuffer() { reset(); }
  int init(const int64_t capacity);
  int reserve(const int64_t size);
  int set_current_block(const int64_t block_size);
  void reset();

  OB_INLINE char *data()
  {
    return data_;
  }
  TO_STRING_KV(K_(is_inited), KP_(data), K_(capacity), K_(current_block), K_(overflow_buffer), K_(overflow_buffer_size));
protected:
  OB_INLINE void reuse_buffer()
  {
    MEMSET(data_, 0, capacity_);
    pos_ = 0;
  }
  OB_INLINE char *current()
  {
    return current_block_.header_->data_ + pos_;
  }
  void free_overflow_buffer();
  int alloc_overflow_buffer(const int64_t size);
protected:
  bool is_inited_;
  char *data_;
  int64_t capacity_;
  int64_t pos_; // start by sizeof(ObCOMergeLogBlockHeade)
  ObCOMergeLogBlock current_block_;
  char *overflow_buffer_;
  int64_t overflow_buffer_size_;
  ObIAllocator &allocator_;
};

class ObCOMergeLogFile
{
public:
  ObCOMergeLogFile()
    : dir_id_(tmp_file::ObTmpFileGlobal::INVALID_TMP_FILE_DIR_ID),
      file_fd_(tmp_file::ObTmpFileGlobal::INVALID_TMP_FILE_FD),
      block_count_(0),
      block_size_()
  {}
  virtual ~ObCOMergeLogFile() { destroy(); }
  void destroy();
  OB_INLINE bool is_valid() const
  {
    return dir_id_ != tmp_file::ObTmpFileGlobal::INVALID_TMP_FILE_DIR_ID &&
           file_fd_ != tmp_file::ObTmpFileGlobal::INVALID_TMP_FILE_FD;
  }
  int open(const int64_t dir_id, const int64_t block_size = BLOCK_SIZE);
  int append_block(ObCOMergeLogBlock &block);
  int read_block(
      ObCOMergeLogBuffer &buffer,
      const int64_t start_block_idx);
  int64_t get_block_count() const { return block_count_; }
  int64_t get_block_size() const { return block_size_; }
  TO_STRING_KV(K_(dir_id), K_(file_fd), K_(block_count), K_(block_size));
private:
  int get_io_info(
      char *buf,
      tmp_file::ObTmpFileIOInfo &io_info);
public:
  static const int64_t TIMEOUT_MS = 5000;
  static const int64_t BLOCK_SIZE = 8L * 1024L; // 8KB
private:
  int64_t dir_id_;
  int64_t file_fd_;
  int64_t block_count_;
  int64_t block_size_;
};

// only support append
class ObCOMergeLogBufferWriter : public ObCOMergeLogBuffer
{
public:
  ObCOMergeLogBufferWriter(
      ObCOMergeProjector *projector,
      ObCOMergeLogFile &file,
      ObIAllocator &allocator)
    : ObCOMergeLogBuffer(allocator),
      projector_(projector),
      file_(file)
  {}
  virtual ~ObCOMergeLogBufferWriter() { reset(); }
  void reset();
  int init();
  int write(const int64_t log_id, const blocksstable::ObDatumRow &row);
  int write(const int64_t log_id, const ObMergeLog &log);
  int close();

  INHERIT_TO_STRING_KV("ObCOMergeLogBuffer", ObCOMergeLogBuffer, KP_(projector));
private:
  OB_INLINE int64_t write_capacity() const
  {
    return current_block_.block_max_writable_size();
  }
  OB_INLINE int64_t write_remain() const
  {
    return write_capacity() - pos_;
  }
  void reuse_current_block();
  int flush_current_block();
  int64_t calc_piece_count(const int64_t serialize_size, const int64_t header_size);

  template <typename T>
  int write_piece(const int64_t log_id, const T &value)
  {
    int ret = OB_SUCCESS;
    const int64_t header_size = sizeof(ObCOMergeLogPieceHeader);
    const int64_t serialize_size = value.get_serialize_size();
    int64_t total_size = serialize_size + header_size;
    if (write_remain() >= total_size) {
      // write single piece
      set_current_piece_header(log_id, serialize_size, serialize_size, 1/*piece_id*/, 1/*total_pieces*/);
      current_block_.header_->length_ += header_size;
      pos_ += header_size;
      if (OB_FAIL(value.serialize(current_block_.header_->data_, write_capacity(), pos_))) {
        STORAGE_LOG(WARN, "failed to serialize value", K(ret), K(value), K(*this));
      } else {
        current_block_.header_->piece_count_++;
        current_block_.header_->length_ += serialize_size;
      }
    } else if (write_capacity() >= total_size && can_reserved()) {
      if (OB_FAIL(flush_current_block())) {
        STORAGE_LOG(WARN, "failed to flush current block", K(ret));
      } else if (OB_FAIL(write_piece(log_id, value))) {
        STORAGE_LOG(WARN, "failed to write log", K(ret), K(log_id), K(value));
      }
    } else {
      int64_t pos = 0;
      if (OB_FAIL(alloc_overflow_buffer(serialize_size))) {
        STORAGE_LOG(WARN, "failed to alloc overflow buffer", K(ret), K(serialize_size));
      } else if (OB_FAIL(value.serialize(overflow_buffer_, overflow_buffer_size_, pos))) {
        STORAGE_LOG(WARN, "failed to serialize value", K(ret), K(value));
      } else if (OB_FAIL(write_pieces(log_id, overflow_buffer_, serialize_size))) {
        STORAGE_LOG(WARN, "failed to write piece", K(ret), K(log_id), K(overflow_buffer_), K(serialize_size));
      }
    }
    return ret;
  }
  int write_pieces(const int64_t log_id, const char *buf, const int64_t buf_len);
  static constexpr double MAX_RESERVED_RATIO = 0.2;
  static const int64_t MAX_RESERVED_SIZE = 1024;
  bool can_reserved();
  void set_current_piece_header(
    const int64_t log_id,
    const int64_t piece_length,
    const int64_t log_length,
    const int64_t piece_id,
    const int64_t total_pieces);
private:
  ObCOMergeProjector *projector_;
  ObCOMergeLogFile &file_;
};

// only support sequential read
class ObCOMergeLogBufferReader : public ObCOMergeLogBuffer
{
public:
  ObCOMergeLogBufferReader(
      ObCOMergeLogFile &file,
      ObIAllocator &allocator)
    : ObCOMergeLogBuffer(allocator),
      block_read_piece_count_(0),
      total_read_block_count_(0),
      file_(file)
  {}
  virtual ~ObCOMergeLogBufferReader() { reset(); }
  void reset();
  int init();
  template <typename T>
  int read_next_log(int64_t &log_id, T &value)
  {
#define COPY_PIECE_DATA(buf, offset, header) \
do { \
  MEMCPY(buf + offset, header->data_, header->length_);\
  offset += header->length_; \
} while (0)
    int ret = OB_SUCCESS;
    int64_t pos = 0;
    ObCOMergeLogPieceHeader *header = nullptr;
    if (OB_FAIL(read_next_piece(header))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "failed to read next piece", K(ret));
      }
    } else if (FALSE_IT(log_id = header->log_id_)) {
    } else if (header->total_pieces_ == 1) {
      if (header->piece_id_ != header->total_pieces_ || header->length_ != header->log_length_) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "piece id is not equal to total pieces", K(ret), K(*header));
      } else if (OB_FAIL(value.deserialize(header->data_, header->length_, pos))) {
        STORAGE_LOG(WARN, "failed to deserialize value", K(ret), K(*header));
      }
    } else {
      const int64_t serialize_size = header->log_length_;
      int64_t last_piece_id = header->piece_id_;
      int64_t read_size = 0;
      if (OB_FAIL(alloc_overflow_buffer(serialize_size))) {
        STORAGE_LOG(WARN, "failed to alloc overflow buffer", K(ret), K(serialize_size));
      } else {
        COPY_PIECE_DATA(overflow_buffer_, read_size, header);
        while (OB_SUCC(ret) && read_size < serialize_size) {
          if (OB_FAIL(read_next_piece(header))) {
            STORAGE_LOG(WARN, "failed to read next piece", K(ret));
            if (OB_ITER_END == ret) {
              ret = OB_ERR_UNEXPECTED;
            }
          } else if (header->log_id_ != log_id || header->piece_id_ != last_piece_id + 1) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "unexpect header", K(ret), K(log_id), K(last_piece_id), K(*header));
          } else {
            COPY_PIECE_DATA(overflow_buffer_, read_size, header);
            last_piece_id = header->piece_id_;
          }
        }
        if (OB_FAIL(ret)) {
        } else if (read_size != serialize_size || header->piece_id_ != header->total_pieces_) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "read size not match", K(ret), K(read_size), K(serialize_size), K(*header));
        } else if (OB_FAIL(value.deserialize(overflow_buffer_, overflow_buffer_size_, pos))) {
          STORAGE_LOG(WARN, "deserialize failed", K(ret), K(*this));
        }
      }
    }
    return ret;
  }
  INHERIT_TO_STRING_KV("ObCOMergeLogBuffer", ObCOMergeLogBuffer, K_(block_read_piece_count), K_(total_read_block_count));
private:
  int read_next_block();
  int read_next_piece(ObCOMergeLogPieceHeader *&header);
private:
  int64_t block_read_piece_count_;
  int64_t total_read_block_count_;
  ObCOMergeLogFile &file_;
};

// for merge log build, single write
// for merge log replay, multi read
class ObCOMergeLogFileMgr
{
public:
  ObCOMergeLogFileMgr()
    : is_inited_(false),
      readable_(false),
      dir_id_(tmp_file::ObTmpFileGlobal::INVALID_TMP_FILE_DIR_ID),
      row_file_count_(0),
      log_file_(),
      row_files_(nullptr),
      allocator_("LogFileMgr")
  {}
  virtual ~ObCOMergeLogFileMgr() { reset(); }
  int init(
      const common::ObIArray<ObStorageColumnGroupSchema> &cg_array,
      const int64_t file_block_size,
      const bool skip_base_cg = false);
  void reset();
  int close_part(const int64_t start_cg_idx, const int64_t end_cg_idx);
  int check_could_release(bool &could_release);

  int get_row_file(const int64_t idx, ObCOMergeLogFile *&file);
  int get_log_file(ObCOMergeLogFile *&file);
  void set_readable() { readable_ = true; }

  OB_INLINE bool is_inited() const { return is_inited_; }
  OB_INLINE int64_t get_block_count() const { return log_file_.get_block_count(); } // for unittest
  OB_INLINE int64_t get_row_file_count() const { return row_file_count_; }
  OB_INLINE bool is_readable() const { return readable_; } // use this flag to skip build retry
  TO_STRING_KV(K_(is_inited), K_(readable), K_(dir_id),
      K_(log_file), K_(row_files), K_(row_file_count));
private:
  bool is_inited_;
  bool readable_;
  int64_t dir_id_;
  int64_t row_file_count_; // if row_store, row_file_count_  = 1, if column_store, row_file_count_ = column_group_count
  ObCOMergeLogFile log_file_;
  ObCOMergeLogFile **row_files_;
  ObLocalArena allocator_;
};
}
}
#endif