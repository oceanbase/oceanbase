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

#ifndef OCEANBASE_BASIC_OB_TEMP_BLOCK_STORE_H_
#define OCEANBASE_BASIC_OB_TEMP_BLOCK_STORE_H_

#include "share/ob_define.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/page_arena.h"
#include "lib/list/ob_dlist.h"
#include "sql/engine/basic/ob_sql_mem_callback.h"
#include "lib/checksum/ob_crc64.h"
#include "sql/engine/basic/chunk_store/ob_chunk_block_compressor.h"
#include "storage/blocksstable/ob_tmp_file.h"

namespace oceanbase
{
namespace sql
{

class ObIOEventObserver;

/*
 * Implementation of block store supporting random access, structure simplified as follows:
 *                      +------------+
 *                      | BlockIndex | (in-memory list)
 *                      +------------+
 *                            |
 *                      +------------+
 *                      | IndexBlock |
 *                      +------------+
 *                            |
 *                      +------------+
 *                      | BlockIndex |
 *                      +------------+
 *                            |
 *                        +-------+
 *                        | Block |  ...
 *                        +-------+
 * Block: data block used to store data
 * BlockIndex: index of block, which can be either data block or index block,
 *             storing memory pointer (for in-memory block) or file offset (for disk block)
 * IndexBlock: index block made up of multiple indexes.
 *
 * Supports random access between blocks, and the BlockReader can randomly access the block
 * by specifying block_id externally.
 */
class ObTempBlockStore
{
  OB_UNIS_VERSION_V(1);
  static const int64_t TRUNCATE_THRESHOLD = 2L << 20;
public:
  /*
   * ShrinkBuffer, a buffer wrapper class supporting bidirectional writing,
   * which writes from the front using the head and from the back using the tail.
   *       data(block payload)                 cap
   *        |                                   |
   *        +-----------------------------------+
   *        |          (ShrinkBuffer)           |
   *        +-----------------------------------+
   *            | ->                         <- |
   *          head                             tail
   */
  class ShrinkBuffer
  {
  public:
    ShrinkBuffer() : data_(NULL), head_(0), tail_(0), cap_(0) {}

    int init(char *buf, const int64_t buf_size);
    inline int64_t remain() const { return tail_ - head_; }
    inline char *data() { return data_; }
    inline const char *data() const { return data_; }
    inline char *head() const { return data_ + head_; }
    inline int64_t head_size() const { return head_; }
    inline char *tail() const { return data_ + tail_; }
    inline int64_t tail_size() const { return cap_ - tail_; }
    inline int64_t capacity() const { return cap_; }
    inline bool is_inited() const { return NULL != data_; }
    inline void reset() { *this = ShrinkBuffer(); }
    inline void reuse() { head_ = 0; tail_ = cap_; }
    inline void fast_advance(int64_t size) { head_ += size; }
    inline int advance(int64_t size) { return fill_head(size); }
    inline int fill_head(int64_t size);
    inline int fill_tail(int64_t size);
    inline int compact();
    inline int64_t head_pos() const { return head_; }
    inline int64_t tail_pos() const { return tail_;}
    inline void fast_update_head(const int64_t pos) { head_ = pos; }
    TO_STRING_KV(KP_(data), K_(head), K_(tail), K_(cap));
  private:
    char *data_;
    int64_t head_;
    int64_t tail_;
    int64_t cap_;
  };

  /*
   * Block, a stucture storing the data uses block id for indexing,
   * the real data starts from the payload.
   * If the block is in the process of data appending in dtl, the tail will occupy one
   * `ShrinkBuffer` size to record the current writing position information.
   * The memory layout is as follows:
   * +------------------------------------------------------------------+
   * | Block Header | Payload                   | ShrinkBuffer(optional)|
   * +------------------------------------------------------------------+
   */
  struct Block
  {
    static const int64_t MAGIC = 0x35f4451b9b56eb12;

    Block() : magic_(MAGIC), block_id_(0), cnt_(0), raw_size_(0) {}

    template <bool WITH_BLK_BUF>
    inline static int64_t min_blk_size(const int64_t size)
    {
      return sizeof(Block) + size + (WITH_BLK_BUF ? sizeof(ShrinkBuffer) : 0);
    }
    inline bool contain(const int64_t block_id) const
    {
      return begin() <= block_id && block_id < end();
    }
    inline int64_t begin() const { return block_id_; }
    inline int64_t end() const { return block_id_ + cnt_; }
    inline int64_t payload_size() const { return raw_size_ - sizeof(Block); }
    // We put the buffer at the end of the block. This is only used in dtl scenarios
    // to support the self-explanatory ability of the block.
    inline static char *buffer_position(void *mem, const int64_t size)
    {
      return static_cast<char*>(mem) + size - sizeof(ShrinkBuffer);
    }
    inline uint64_t checksum() const
    {
      ObBatchChecksum bc;
      bc.fill(payload_, raw_size_ - sizeof(Block));
      return bc.calc();
    }
    TO_STRING_KV(K_(magic), K_(block_id), K_(cnt), K_(raw_size));

    int64_t magic_;
    // increment identity of the block, it also can be row_id or any identity that
    // satisfies the incremental unique identity.
    int64_t block_id_;
    // count of items in the block, ensure that the next block's `begin()` should
    // be equal to the `end()` of this block.
    uint32_t cnt_;
    union {
      // raw size of the block before compression.
      uint32_t raw_size_;
      // buffer offset before block switching.
      uint32_t buf_off_;
    };
    char payload_[0];
  } __attribute__((packed));

  struct IndexBlock;
  // The index of the block (index block) records the block's id, size, capacity, memory pointer
  // or file offset.
  struct BlockIndex
  {
    static bool compare(const BlockIndex &bi, const int64_t block_id)
    {
      return bi.block_id_ < block_id;
    }

    TO_STRING_KV(K_(is_idx_block), K_(on_disk), K_(block_id), K_(offset), K_(length), K_(capacity));

    uint64_t is_idx_block_:1;
    uint64_t on_disk_:1;
    uint64_t block_id_ : 62;
    union {
      IndexBlock *idx_blk_;
      Block *blk_;
      int64_t offset_;
    };
    int32_t length_;
    int32_t capacity_;
  } __attribute__((packed));

  // Used for block linking in memory.
  class LinkNode : public common::ObDLinkBase<LinkNode>
  {
  };
  // An index block composed of indexes.
  struct IndexBlock
  {
    const static int64_t MAGIC = 0x4847bcb053c3703f;
    const static int64_t INDEX_BLOCK_SIZE = (32 << 10) - sizeof(LinkNode);
    constexpr static inline int64_t capacity()
    {
      return (INDEX_BLOCK_SIZE - sizeof(IndexBlock)) / sizeof(BlockIndex);
    }

    IndexBlock() : magic_(MAGIC), cnt_(0) {}

    inline int64_t buffer_size() const { return sizeof(*this) + sizeof(BlockIndex) * cnt_; }
    inline bool is_full() const { return cnt_ == capacity(); }
    inline bool is_empty() const { return 0 == cnt_; }

    // may return false when row in position (false negative),
    // since block index only contain start id, we can not detect right boundary.
    inline bool blk_in_pos(const int64_t block_id, const int64_t pos);

    void reset() { cnt_ = 0; }

    inline uint64_t block_id() const { return block_indexes_[0].block_id_; }

    TO_STRING_KV(K_(magic), K_(cnt));

    int64_t magic_;
    int32_t cnt_;
    BlockIndex block_indexes_[0];
  } __attribute__((packed));

  // Iteration age used for iterated rows life cycle control, iterated rows' memory are available
  // until age increased. E.g.:
  //
  //   IterationAge iter_age;
  //   Reader it(ra_row_store);
  //   it.set_iteration_age(iter_age);
  //
  //   while (...) {
  //     iter_age.inc();
  //
  //     it.get_row(idx1, row1);
  //     it.get_row(iex2, row2);
  //
  //     // row1 and row2's memory are still available here, until the get_row() is called
  //     // after iteration age increased.
  //   }
  class IterationAge
  {
  public:
    IterationAge() : age_(0) {}
    int64_t get(void) const { return age_; }
    void inc(void) { age_ += 1; }
  private:
    int64_t age_;
  };

  class BlockReader;
  struct TryFreeMemBlk
  {
    TryFreeMemBlk *next_;
    union {
      int64_t age_;
      BlockReader *reader_;
    };
    int64_t size_;

    TryFreeMemBlk() = delete;
  };

  static_assert(std::is_pod<TryFreeMemBlk>::value == true, "TryFreeMemBlk should be pod");

  struct BlockHolder
  {
    BlockHolder() : blocks_(NULL) {}
    ~BlockHolder()
    {
      release();
    }
    void release();
    TryFreeMemBlk *blocks_;
  };

  // A reader that supports random access between blocks. must be deleted before TempBlockStore
  class BlockReader
  {
    friend class ObTempBlockStore;
    friend class BlockHolder;
    static const int AIO_BUF_CNT = 2;
  public:
    BlockReader() : store_(NULL), idx_blk_(NULL), ib_pos_(0), file_size_(0), age_(NULL),
                    try_free_list_(NULL), blk_holder_ptr_(NULL), read_io_handle_(),
                    is_async_(true), aio_buf_idx_(0), aio_blk_(nullptr) {}
    virtual ~BlockReader() { reset(); }

    int init(ObTempBlockStore *store, const bool async = true);
    int get_block(const int64_t block_id, const Block *&blk);
    inline int64_t get_block_cnt() const { return store_->get_block_cnt(); }
    void set_iteration_age(IterationAge *age) { age_ = age; }
    void set_blk_holder(BlockHolder *holder) { blk_holder_ptr_ = holder; }
    blocksstable::ObTmpFileIOHandle& get_read_io_handler() { return read_io_handle_; }
    inline bool is_async() const { return is_async_; }
    void reset();
    void reuse();
    void begin_new_batch()
    {
      if (NULL == age_) {
        age_ = &inner_age_;
      }
      inner_age_.inc();
    }
    TO_STRING_KV(KP_(store), K_(buf), K_(idx_buf), KP_(idx_blk), K_(ib_pos), K_(file_size),
                 KP_(age), KP_(try_free_list), KP_(blk_holder_ptr), K_(cur_file_offset),
                 K_(is_async), K(read_io_handle_), K(aio_buf_), K(decompr_buf_));

  private:
    void reset_cursor(const int64_t file_size, const bool need_release = true);
    void free_all_blks();
    void free_blk_mem(void *mem, const int64_t size) { store_->free_blk_mem(mem, size); }
    int aio_wait();

  private:
    ObTempBlockStore *store_;
    ShrinkBuffer buf_;
    ShrinkBuffer aio_buf_[AIO_BUF_CNT];
    ShrinkBuffer decompr_buf_;
    ShrinkBuffer idx_buf_;
    IndexBlock *idx_blk_;
     // current block index position in index block
    int64_t ib_pos_;
    // idx_blk_, blk_ may point to the writing block,
    // we need to invalid the pointers if file_size_ change.
    int64_t file_size_;
    IterationAge *age_;
    TryFreeMemBlk *try_free_list_;
    BlockHolder *blk_holder_ptr_;
    // inner iteration age is used for batch iteration with no outside age control.
    IterationAge inner_age_;
    // to optimize performance, record the last_extent_id to avoid do binary search every time
    // calling read.
    blocksstable::ObTmpFileIOHandle read_io_handle_;
    int64_t cur_file_offset_;
    bool is_async_;
    int aio_buf_idx_;
    const Block *aio_blk_;
    DISALLOW_COPY_AND_ASSIGN(BlockReader);
  };

public:
  const static int64_t BLOCK_SIZE = (64L << 10) - sizeof(LinkNode);
  const static int64_t BIG_BLOCK_SIZE = (256L << 10) - sizeof(LinkNode);
  const static int64_t DEFAULT_BLOCK_CNT = (1L << 20) / BLOCK_SIZE;

  explicit ObTempBlockStore(common::ObIAllocator *alloc = NULL);
  virtual ~ObTempBlockStore() { reset(); }
  int init(int64_t mem_limit,
           bool enable_dump,
           uint64_t tenant_id,
           int64_t mem_ctx_id,
           const char *label,
           common::ObCompressorType compressor_type,
           const bool enable_trunc = false);
  void reset();
  void reuse();
  void reset_block_cnt();
  bool is_inited() const { return inited_; }
  bool is_file_open() const { return io_.fd_ >= 0; }
  void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  void set_mem_ctx_id(const int64_t ctx_id) { ctx_id_ = ctx_id; }
  void set_mem_limit(const int64_t limit) { mem_limit_ = limit; }
  void set_mem_stat(ObSqlMemoryCallback *mem_stat) { mem_stat_ = mem_stat; }
  void set_callback(ObSqlMemoryCallback *callback) { mem_stat_ = callback; }
  void reset_callback()
  {
    mem_stat_ = nullptr;
    io_observer_ = nullptr;
  }
  void set_io_event_observer(ObIOEventObserver *io_observer) { io_observer_ = io_observer; }
  // set iteration age for inner reader.
  void set_allocator(common::ObIAllocator &alloc) { allocator_ = &alloc; }
  void set_inner_allocator_attr(const lib::ObMemAttr &attr) { inner_allocator_.set_attr(attr); }
  void set_dir_id(int64_t dir_id) { io_.dir_id_ = dir_id; }
  void set_iteration_age(IterationAge *age) { inner_reader_.set_iteration_age(age); }
  inline void set_mem_used(const int64_t mem_used) { mem_used_ = mem_used; }
  inline void inc_mem_used(const int64_t mem_used) { mem_used_ += mem_used; }
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline const char* get_label() { return label_; }
  inline int64_t get_mem_ctx_id() const { return ctx_id_; }
  inline int64_t get_block_id_cnt() const { return block_id_cnt_; }
  inline void inc_block_id_cnt(int64_t cnt) { block_id_cnt_ += cnt; }
  inline int64_t get_dumped_block_id_cnt() const { return dumped_block_id_cnt_; }
  inline int64_t get_block_cnt() const { return block_cnt_; }
  inline int64_t get_index_block_cnt() const { return index_block_cnt_; }
  inline int64_t get_block_cnt_on_disk() const { return block_cnt_on_disk_; }
  inline int64_t get_block_cnt_in_mem() const { return block_cnt_ - block_cnt_on_disk_; }
  inline int64_t get_block_list_cnt() { return blk_mem_list_.get_size(); }
  inline int64_t get_mem_hold() const { return mem_hold_; }
  inline int64_t get_mem_used() const { return mem_used_; }
  inline int64_t get_alloced_mem_size() const { return alloced_mem_size_; }
  inline int64_t get_alloced_mem_cnt() const { return alloced_mem_list_.get_size(); }
  inline int64_t get_file_fd() const { return io_.fd_; }
  inline int64_t get_file_dir_id() const { return io_.dir_id_; }
  inline int64_t get_file_size() const { return file_size_; }
  inline int64_t get_max_blk_size() const { return max_block_size_; }
  inline int64_t get_max_hold_mem() const { return max_hold_mem_; }
  inline common::DefaultPageAllocator& get_inner_allocator() { return inner_allocator_; }
  inline int64_t has_dumped() const { return block_cnt_on_disk_ > 0; }
  inline int64_t get_last_buffer_mem_size() const { return blk_buf_.capacity(); }
  static int init_dtl_block_buffer(void* mem, const int64_t size, Block *&block);
  int append_block(const char *buf, const int64_t size);
  int append_block_payload(const char *buf, const int64_t size, const int64_t cnt);
  int alloc_dir_id();
  int dump(const bool all_dump, const int64_t target_dump_size=INT64_MAX);
  int finish_add_row(bool need_dump = true);
  void set_enable_truncate(bool enable_trunc)
  {
    enable_trunc_ = enable_trunc;
  }
  inline ShrinkBuffer &get_blk_buf() { return blk_buf_; }
  bool is_truncate() { return enable_trunc_; }
  inline int64_t get_cur_file_offset() const { return cur_file_offset_; }
  inline void set_cur_file_offset(int64_t file_offset) { cur_file_offset_ = file_offset; }

  inline bool is_empty_save_block_cnt() const { return saved_block_id_cnt_ == 0; }
  // include index blocks and data blocks

  TO_STRING_KV(K_(inited), K_(enable_dump), K_(tenant_id), K_(label), K_(ctx_id),  K_(mem_limit),
    K_(mem_hold), K_(mem_used), K_(io_.fd), K_(io_.dir_id), K_(file_size), K_(block_cnt),
    K_(index_block_cnt), K_(block_cnt_on_disk), K_(block_id_cnt), K_(dumped_block_id_cnt),
    K_(alloced_mem_size), K_(enable_trunc), K_(last_trunc_offset), K_(cur_file_offset));

  void *alloc(const int64_t size)
  {
    alloced_mem_size_ += size;
    return alloc_blk_mem(size, &alloced_mem_list_);
  }
  void free(void *mem, const int64_t size)
  {
    alloced_mem_size_ -= size;
    free_blk_mem(mem, size);
  }

  int new_block(const int64_t mem_size, Block *&blk, const bool strict_mem_size);
  int truncate_file(int64_t offset);

protected:
  /*
   * Allocate a new block as the currently written block, which can get block pointer through
   * `blk_` and plz ensure that the `head_size` of ` blk_->get_buffer()` is the actual size
   * after used. During the allocation process, the block will be indexed and memory managed.
   *
   * @param `mem_size`: memory size required by external callers
   * @param `strict_mem_size`: If true, allocate memory strictly according to the size passed in.
                               The actual memory size used is the sum of mem_size,
                               block header(sizeof(Block)) and link size (sizeof(LinkNode)).
                               If false, The size of the actual allocation may limit the minimum
                               block size and do memory alignment.
   */
  inline int new_block(const int64_t mem_size, const bool strict_mem_size = false)
  {
    return new_block(mem_size, blk_, strict_mem_size);
  }

protected:
  int get_block(BlockReader &reader, const int64_t block_id, const Block *&blk);
  /*
   * A hook interface reserved for the subclasses prepare `finish_add_row`. It can override
   * this interface to implement customized finish writing code. For example, release the memory
   * requested or close file.
   */
  virtual int finish_write()
  {
    return OB_SUCCESS;
  }

  inline int ensure_write_blk(const int64_t mem_size, const bool strict_mem_size = false)
  {
    return new_block(mem_size, blk_, strict_mem_size);
  }

private:
  int inner_get_block(BlockReader &reader, const int64_t block_id,
                      const Block *&blk, bool &blk_on_disk);
  int decompr_block(BlockReader &reader, const Block *&blk);
  inline static int64_t block_magic(const void *mem)
  {
    return *(static_cast<const int64_t *>(mem));
  }
  inline static bool is_block(const void *mem) { return Block::MAGIC == block_magic(mem); };
  inline static bool is_index_block(const void *mem)
  {
    return IndexBlock::MAGIC == block_magic(mem);
  }
  inline bool is_last_block(const void *mem) const
  {
    return mem == blk_ || mem == idx_blk_;
  }
  static int get_timeout(int64_t &timeout_ms);
  int alloc_block(Block *&blk, const int64_t min_size, const bool strict_mem_size);
  void *alloc_blk_mem(const int64_t size, common::ObDList<LinkNode> *list);
  int setup_block(ShrinkBuffer &buf, Block *&blk);
  // new block is not needed if %min_size is zero. (finish add row)
  int switch_block(const int64_t min_size, const bool strict_mem_size);
  int add_block_idx(const BlockIndex &bi);
  int alloc_idx_block(IndexBlock *&ib);
  int build_idx_block();
  int switch_idx_block(bool finish_add = false);
  int link_idx_block(IndexBlock *idx_blk);
  void set_mem_hold(int64_t hold);
  void inc_mem_hold(int64_t hold);
  void free_blk_mem(void *mem, const int64_t size = 0);

  int load_block(BlockReader &reader, const int64_t block_id, const Block *&blk, bool &on_disk);
  int find_block_idx(BlockReader &reader, const int64_t block_id, BlockIndex *&bi);
  int load_idx_block(BlockReader &reader, IndexBlock *&ib, const BlockIndex &bi);
  int ensure_reader_buffer(BlockReader &reader, ShrinkBuffer &buf, const int64_t size);
  int write_file(BlockIndex &bi, void *buf, int64_t size);
  int read_file(void *buf, const int64_t size, const int64_t offset,
                blocksstable::ObTmpFileIOHandle &handle, const bool is_async);
  int dump_block_if_need(const int64_t extra_size);
  bool need_dump(const int64_t extra_size);
  int write_compressed_block(Block *blk, BlockIndex *bi);
  int dump_block(Block *blk, int64_t &dumped_size);
  int dump_index_block(IndexBlock *idx_blk, int64_t &dumped_size);
  void free_mem_list(common::ObDList<LinkNode> &list);
  inline bool has_index_block() const { return index_block_cnt_ > 0; }
  inline int64_t get_block_raw_size(const Block *blk) const
  { return is_last_block(blk) ? blk_buf_.head_size() : blk->raw_size_; }
  inline bool need_compress() const { return compressor_.get_compressor_type() != NONE_COMPRESSOR; }
  inline ObCompressorType get_compressor_type() const { return compressor_.get_compressor_type(); }

protected:
  /**
   * These functions are inserted into different stages of the block, and their main function
   * is to customize some special operations to meet the needs of different data formats or
   * scenarios. The overall calling timing of there functions is as follows:
   * new_blk -> prepare_setup_blk -> blk_add_batch -> blk_is_full -> prepare_blk_for_switch ->
   * switch_blk -> prepare_blk_for_write -> dump_blk -> prepare_blk_for_read -> read_blk
   *
   * `prepare_setup_blk`: called when the block has just initialized the initial meta information.
   * The block does not yet have any content. You can add some new meta information you need
   * by overwriting it.
   */
  virtual int prepare_setup_blk(Block *blk) { return OB_SUCCESS; }
  /**
   * `prepare_blk_for_switch`: The function call occurs when the current block is full and blk
   * needs to be switched. Before switching, some customized actions will be performed on `blk`,
   * such as data compaction, etc.
   */
  virtual int prepare_blk_for_switch(Block *blk) { return OB_SUCCESS; }
  /**
   * `prepare_blk_for_write/read`: These two functions are used in conjunction.
   * `prepare_blk_for_write` is called before the block is dumped on the disk, and
   * `prepare_blk_for_read` occurs when the block has just been read from the disk.
   */
  virtual int prepare_blk_for_write(Block *blk) { return OB_SUCCESS; }
  virtual int prepare_blk_for_read(Block *blk) { return OB_SUCCESS; }

protected:
  bool inited_;
  common::ObIAllocator *allocator_;
  Block *blk_; // currently operating block
  // variables related to `block_id`, the total number of `block_id` is the sum of
  // all block's `cnt_`, and it can also be used to count rows.
  ShrinkBuffer blk_buf_;
  int64_t block_id_cnt_;
  int64_t saved_block_id_cnt_;
  int64_t dumped_block_id_cnt_;
  bool enable_dump_;
  bool enable_trunc_; // if true, the read contents of tmp file we be removed from disk.
  int64_t last_trunc_offset_;

private:
  uint64_t tenant_id_;
  char label_[lib::AOBJECT_LABEL_SIZE + 1];
  int64_t ctx_id_;

  // variables used to record memory usage
  int64_t mem_limit_;
  int64_t mem_hold_;
  int64_t mem_used_;
  int64_t file_size_;

  // block related variables used to count various blocks
  int64_t block_cnt_;
  int64_t index_block_cnt_;
  int64_t block_cnt_on_disk_;
  int64_t alloced_mem_size_;
  int64_t max_block_size_;
  int64_t max_hold_mem_;

  IndexBlock *idx_blk_;
  BlockReader inner_reader_;

  common::ObDList<LinkNode> blk_mem_list_;
  common::ObDList<LinkNode> alloced_mem_list_;
  common::ObSEArray<BlockIndex, DEFAULT_BLOCK_CNT> blocks_;
  common::DefaultPageAllocator inner_allocator_;
  ObSqlMemoryCallback *mem_stat_;
  ObChunkBlockCompressor compressor_;
  ObIOEventObserver *io_observer_;
  blocksstable::ObTmpFileIOHandle write_io_handle_;
  blocksstable::ObTmpFileIOInfo io_;
  bool last_block_on_disk_;
  int64_t cur_file_offset_;

  DISALLOW_COPY_AND_ASSIGN(ObTempBlockStore);
};

inline int ObTempBlockStore::ShrinkBuffer::fill_head(int64_t size)
{
  int ret = common::OB_SUCCESS;
  if (size > remain()) {
    ret = common::OB_BUF_NOT_ENOUGH;
    SQL_ENG_LOG(WARN, "buffer not enough", K(size), "remain", remain());
  } else {
    head_ += size;
  }
  return ret;
}

inline int ObTempBlockStore::ShrinkBuffer::fill_tail(int64_t size)
{
  int ret = common::OB_SUCCESS;
  if (size > remain()) {
    ret = common::OB_BUF_NOT_ENOUGH;
    SQL_ENG_LOG(WARN, "buffer not enough", K(size), "remain", remain());
  } else {
    tail_ -= size;
  }
  return ret;
}

inline int ObTempBlockStore::ShrinkBuffer::compact()
{
  int ret = common::OB_SUCCESS;
  if (!is_inited()) {
    ret = common::OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "not inited", K(ret));
  } else {
    const int64_t tail_data_size = tail_size();
    MEMMOVE(head(), tail(), tail_data_size);
    head_ += tail_data_size;
    tail_ += tail_data_size;
  }
  return ret;
}

inline bool ObTempBlockStore::IndexBlock::blk_in_pos(const int64_t block_id, const int64_t pos)
{
  bool in_pos = false;
  if (cnt_ > 0 && pos >= 0 && pos < cnt_) {
    if (pos + 1 == cnt_) {
      in_pos = block_indexes_[pos].block_id_ == block_id;
    } else {
      in_pos = block_indexes_[pos].block_id_ <= block_id &&
        block_id < block_indexes_[pos + 1].block_id_;
    }
  }
  return in_pos;
}

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_BASIC_OB_TEMP_BLOCK_STORE_H_
