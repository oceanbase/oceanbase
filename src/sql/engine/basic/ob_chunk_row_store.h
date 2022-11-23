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

#ifndef OCEANBASE_BASIC_OB_ROW_STORE2_H_
#define OCEANBASE_BASIC_OB_ROW_STORE2_H_

#include "share/ob_define.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/page_arena.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/list/ob_dlist.h"
#include "common/row/ob_row.h"
#include "common/row/ob_row_iterator.h"
#include "storage/blocksstable/ob_tmp_file.h"
#include "sql/engine/basic/ob_sql_mem_callback.h"

namespace oceanbase
{
namespace sql
{

// Random access row store, support disk store.
// All row must have same cell count and  projector.
class ObChunkRowStore
{
  OB_UNIS_VERSION_V(1);
public:
  enum STORE_MODE {           /* e.g. [0, 1, 2] with projector (0, 2) */
     WITHOUT_PROJECTOR = 0,   /* store [0, 2]    get [0, 2] */
     FULL                     /* store [0, 1, 2] get [0, 1, 2] with(0, 2) */
  };
  struct StoredRow
  {
    StoredRow() : cnt_(0), row_size_(0) {}

    static inline int64_t row_copy_size(const common::ObNewRow &row)
    {
      int64_t size = sizeof(common::ObObj) * row.get_count();
      for (int64_t i = 0; i < row.get_count(); ++i) {
        size += row.get_cell(i).get_deep_copy_size();
      }
      return size;
    }
    inline int copy_row(const common::ObNewRow &row, char *buf, const int64_t size,
        const int64_t row_size, const uint32_t row_extend_size);
    inline common::ObObj *cells() { return reinterpret_cast<common::ObObj *>(payload_); }
    inline const common::ObObj *cells() const
        { return reinterpret_cast<const common::ObObj *>(payload_); }
    inline void *get_extra_payload() const { return static_cast<void*>(const_cast<char*>(payload_ + sizeof(ObObj) * cnt_)); }
    int assign(const StoredRow *sr);
    inline void unswizzling(char *base = NULL);
    inline void swizzling();
    
    TO_STRING_KV(K_(cnt), K_(row_size), K(cells()[0]));

    uint32_t cnt_;
    uint32_t row_size_;
    char payload_[0];
  } __attribute__((packed));

  typedef int (*copy_row_fun_t_)(const StoredRow *stored_row);

  class BlockBuffer;
  struct Block
  {
    static const int64_t MAGIC = 0xbc054e02d8536315;
    static const int32_t ROW_HEAD_SIZE = sizeof(StoredRow);
    Block() : magic_(0), blk_size_(0), rows_(0){}

    static int64_t inline min_buf_size(const common::ObNewRow &row)
    {
      return BlockBuffer::HEAD_SIZE + sizeof(BlockBuffer) + row_store_size(row);
    }
    static int64_t inline min_buf_size(const int64_t row_store_size)
    {
      return BlockBuffer::HEAD_SIZE + sizeof(BlockBuffer) + row_store_size;
    }

    static int64_t inline row_store_size(const common::ObNewRow &row, uint32_t row_extend_size = 0)
    {
      return ROW_HEAD_SIZE + row_extend_size + StoredRow::row_copy_size(row);
    }
    int add_row(const common::ObNewRow &row, const int64_t row_size, uint32_t row_extend_size,
                StoredRow **stored_row = NULL);
    int copy_row(const StoredRow *stored_row);
    int gen_unswizzling_payload(char *unswizzling_payload, uint32 size);
    int unswizzling();
    int swizzling(int64_t *col_cnt);
    inline bool magic_check() { return MAGIC == magic_; }
    int get_store_row(int64_t &cur_pos, const StoredRow *&sr);
    inline Block* get_next() const { return next_; }
    inline bool is_empty() { return get_buffer()->is_empty(); }
    inline void set_block_size(uint32 blk_size) { blk_size_ = blk_size; }
    inline BlockBuffer* get_buffer() { return static_cast<BlockBuffer*>(static_cast<void*>(payload_ + blk_size_ - BlockBuffer::HEAD_SIZE)); }
    inline int64_t data_size() { return get_buffer()->data_size(); }
    inline uint32_t rows() { return rows_; }
    inline int64_t remain() { return get_buffer()->remain(); }
    friend class BlockBuffer;
    TO_STRING_KV(K_(magic), K_(blk_size), K_(rows));
    union{
      int64_t magic_;   //for dump
      Block* next_;      //for block list in mem
    };
    uint32 blk_size_;  /* current blk's size, for dump/read */
    uint32 rows_;
    char payload_[0];
  } __attribute__((packed));

  struct BlockList
  {
  public:
    BlockList() : head_(NULL), last_(NULL), size_(0) {}
    inline int64_t get_size() const { return size_; }
    inline bool is_empty() { return size_ == 0; }
    inline Block* get_first() const { return head_; }
    inline void reset() { size_ = 0; head_ = NULL; last_ = NULL; }

    inline void add_last(Block* blk)
    {
      if (NULL == head_) {
        head_ = blk;
        last_ = blk;
        blk->next_ = NULL;
      } else {
        last_->next_ = blk;
        blk->next_ = NULL;
        last_ = blk;
      }
      size_++;
    }

    inline Block* remove_first() {
      Block* cur = head_;
      if (NULL != head_) {
        head_ = head_->next_;
        cur->next_ = NULL;
        size_--;
        if (0 == size_) {
          last_ = NULL;
        }
      }
      return cur;
    }
    TO_STRING_KV(K_(size), K_(head), K_(last), K_(*head), K_(last));
  private:
    Block* head_;
    Block* last_;
    int64_t size_;
  };

  /* contiguous memory:
   * |----------------|---Block
   * |next_           |-|-------------|
   * |cnt_            | |--HEAD_SIZE  |
   * |block_size_     |-|             |--block_size
   * |payload[]       |               |
   * |                |---------------|
   * |----------------|--BlockBuffer
   * |data->BlockHead |-|
   * |cur_pos         | |--TAIL_SIZE
   * |cap_=block_size |-|
   * |----------------|
   * */
  class BlockBuffer
  {
  public:
    static const int64_t HEAD_SIZE = sizeof(Block); /* n_rows, check_sum */
    BlockBuffer() : data_(NULL), cur_pos_(0), cap_(0){}

    int init(char *buf, const int64_t buf_size);
    inline int64_t remain() const { return cap_ - cur_pos_; }
    inline char *data() { return data_; }
    inline Block *get_block() { return block; }
    inline char *head() const { return data_ + cur_pos_; }
    inline int64_t capacity() const { return cap_; }
    inline int64_t mem_size() const { return cap_ + sizeof(BlockBuffer); }
    inline int64_t data_size() const { return cur_pos_; }
    inline bool is_inited() const { return NULL != data_; }
    inline bool is_empty() const { return HEAD_SIZE >= cur_pos_; }

    inline void reset() { cur_pos_ = 0; cap_ = 0; data_ = NULL; }
    inline void reuse() { cur_pos_ = 0; advance(HEAD_SIZE); block->rows_ = 0; }
    inline int advance(int64_t size);

    TO_STRING_KV(KP_(data), K_(cur_pos), K_(cap));

    friend ObChunkRowStore;
    friend Block;
  private:
    union {
      char *data_;
      Block *block;
    };
    int64_t cur_pos_;
    int64_t cap_;
  };

  class ChunkIterator;
  class RowIterator
  {
  public:
    friend class ObChunkRowStore;
    RowIterator();
    virtual ~RowIterator() { reset(); }
    int init(ChunkIterator *chunk_it);
    /* from StoredRow to NewRow */
    int get_next_row(const StoredRow *&sr);
    static int store_row2new_row(common::ObNewRow &row, const StoredRow &sr);
    int convert_to_row(common::ObNewRow &row, const StoredRow *sr);
    int convert_to_row(common::ObNewRow *&row, const StoredRow *sr);
    int convert_to_row_full(common::ObNewRow &row, const StoredRow *sr);
    int convert_to_row_full(common::ObNewRow *&row, const StoredRow *sr);

    void reset() { reset_cursor(); }
    bool is_valid() const { return store_ != NULL && cur_iter_blk_ != NULL; }
    inline bool cur_blk_has_next() const
    {
      return (cur_iter_blk_ != NULL && cur_row_in_blk_ < cur_iter_blk_->rows_);
    }
    inline bool has_next() const
    {
      return cur_iter_blk_ != NULL && (cur_iter_blk_->get_next() != NULL || cur_row_in_blk_ < cur_iter_blk_->rows_);
    }

    TO_STRING_KV(KP_(store), K_(*store), K_(cur_iter_blk), K_(cur_row_in_blk), K_(cur_pos_in_blk),
        K_(n_blocks), K_(cur_nth_block));
  private:
    explicit RowIterator(ObChunkRowStore *row_store);
    void reset_cursor()
    {
      cur_iter_blk_ = NULL;
      cur_row_in_blk_ = 0;
      cur_pos_in_blk_ = 0;
      n_blocks_ = 0;
      cur_nth_block_ = 0;
    }

  protected:
    ObChunkRowStore* store_;
    Block* cur_iter_blk_;
    common::ObNewRow row_;
    int64_t cur_row_in_blk_;  //cur nth row in cur block for in-mem debug
    int64_t cur_pos_in_blk_;  //cur nth row in cur block
    int64_t n_blocks_;
    int64_t cur_nth_block_;
  };


  class ChunkIterator
  {
  public:
    enum IterEndState
    {
      PROCESSING = 0x00,
      MEM_ITER_END = 0x01,
      DISK_ITER_END = 0x02
    };
  public:
     friend class ObChunkRowStore;
     ChunkIterator();
     virtual ~ChunkIterator();
     int init(ObChunkRowStore *row_store, int64_t chunk_read_size);
     int load_next_chunk(RowIterator& it);
     inline bool has_next_chunk() { return store_->n_blocks_ > 0 && (cur_nth_blk_ < store_->n_blocks_ - 1); }
     void set_chunk_read_size(int64_t chunk_read_size) { chunk_read_size_ = chunk_read_size; }
     inline int64_t get_chunk_read_size() { return chunk_read_size_; }
     inline int64_t get_row_cnt() const { return store_->get_row_cnt(); }
     inline int64_t get_cur_chunk_row_cnt() const { return chunk_n_rows_; }
     inline int64_t get_chunk_read_size() const { return chunk_read_size_; }
     void reset();
     inline bool is_valid() { return store_ != NULL; }
     inline bool read_file_iter_end() { return iter_end_flag_ & DISK_ITER_END; }
     inline void set_read_file_iter_end() { iter_end_flag_ |= DISK_ITER_END; }
     inline bool read_mem_iter_end() { return iter_end_flag_ & MEM_ITER_END; }
     inline void set_read_mem_iter_end() { iter_end_flag_ |= MEM_ITER_END; }

     TO_STRING_KV(KP_(store), KP_(cur_iter_blk), KP_(cur_iter_blk_buf),
         K_(cur_chunk_n_blocks), K_(cur_iter_pos), K_(file_size), K_(chunk_read_size), KP_(chunk_mem));
  private:
     void reset_cursor(const int64_t file_size);
  protected:
     ObChunkRowStore* store_;
     Block* cur_iter_blk_;
     BlockBuffer* cur_iter_blk_buf_; /*for reuse of cur_iter_blk_;
                                       cause Block::get_buffer() depends on blk_size_
                                       but blk_size_ will change with block reusing
                                      */
     int64_t cur_nth_blk_;     //reading nth blk start from 1
     int64_t cur_chunk_n_blocks_; //the number of blocks of current chunk
     int64_t cur_iter_pos_;    //pos in file
     int64_t file_size_;
     int64_t chunk_read_size_;
     char* chunk_mem_;
     int64_t chunk_n_rows_;
     int32_t iter_end_flag_;
  };

#define CONVERT_FUN(C, W, T) \
  inline int C##W(RowIterator &it, T row, const StoredRow *sr) \
  { \
    return it.C(row, sr);\
  }

  class Iterator : public common::ObOuterRowIterator
  {
  public:
     friend class ObChunkRowStore;
     Iterator() : start_iter_(false), convert_row_with_obj_fun_(NULL), convert_row_fun_(NULL) {}
     virtual ~Iterator() {}
     int init(ObChunkRowStore *row_store, int64_t chunk_read_size);
     void set_chunk_read_size(int64_t chunk_read_size) { chunk_it_.set_chunk_read_size(chunk_read_size); }
     /// @param row [in/out] row.size_ is used to verify the data
     int get_next_row(common::ObNewRow &row);
     int get_next_row(common::ObNewRow *&row);
     int get_next_row(const StoredRow *&sr);
     void reset() { row_it_.reset(); chunk_it_.reset(); start_iter_ = false; }
     inline bool has_next() { return chunk_it_.has_next_chunk() || (row_it_.is_valid() && row_it_.has_next()); }
     bool is_valid() { return chunk_it_.is_valid(); }
     inline int64_t get_chunk_read_size() { return chunk_it_.get_chunk_read_size(); }
     int convert_to_row_with_obj(const StoredRow *sr, common::ObNewRow &row)
     { return (this->*convert_row_with_obj_fun_)(row_it_, row, sr); }
     int convert_to_row(const StoredRow *sr, common::ObNewRow *&row)
     { return (this->*convert_row_fun_)(row_it_, row, sr); }
  private:
     explicit Iterator(ObChunkRowStore *row_store);

     CONVERT_FUN(convert_to_row, , common::ObNewRow *&)
     CONVERT_FUN(convert_to_row, _with_obj, common::ObNewRow &)
     CONVERT_FUN(convert_to_row_full, , common::ObNewRow *&)
     CONVERT_FUN(convert_to_row_full, _with_obj, common::ObNewRow &)
  protected:
     bool start_iter_;
     ChunkIterator chunk_it_;
     RowIterator row_it_;
     int (Iterator::*convert_row_with_obj_fun_)(RowIterator &it, common::ObNewRow &row, const StoredRow *sr);
     int (Iterator::*convert_row_fun_)(RowIterator &it, ObNewRow *&row, const StoredRow *sr);
  };


public:
  const static int64_t BLOCK_SIZE = (64L << 10); //+ BlockBuffer::HEAD_SIZE;

  explicit ObChunkRowStore(common::ObIAllocator *alloc = NULL);

  virtual ~ObChunkRowStore() { reset(); }

  int init(int64_t mem_limit,
      uint64_t tenant_id = common::OB_SERVER_TENANT_ID,
      int64_t mem_ctx_id = common::ObCtxIds::DEFAULT_CTX_ID,
      const char *label = common::ObModIds::OB_SQL_CHUNK_ROW_STORE,
      bool enable_dump = true,
      STORE_MODE row_store_mode = WITHOUT_PROJECTOR,
      uint32_t row_extra_size = 0);

  void set_allocator(common::ObIAllocator &alloc) { allocator_ = &alloc; }

  void reset();
  // Keeping one memory block, reader must call reuse() too.
  void reuse();

  /// begin iterator
  int begin(ChunkIterator &it, int64_t chunk_read_size = 0)
  {
    return it.init(this, chunk_read_size);
  }

  int begin(Iterator &it, int64_t chunk_read_size = 0)
  {
    return it.init(this, chunk_read_size);
  }

  //template<typename T = ObChunkStoredRowType::StoredRow>
  int add_row(const common::ObNewRow &row, StoredRow **stored_row = NULL);
  int copy_row(const StoredRow *stored_row, ObChunkRowStore* crs = NULL);
  int finish_add_row(bool need_dump = true);
  OB_INLINE bool is_inited() const { return inited_; }
  bool is_file_open() const { return io_.fd_ >= 0; }

  //void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  //void set_mem_ctx_id(const int64_t ctx_id) { ctx_id_ = ctx_id; }
  void set_mem_limit(const int64_t limit) { mem_limit_ = limit; }
  inline int64_t get_mem_limit() { return mem_limit_; }
  void set_block_size(const int64_t size) { default_block_size_ = size; }
  inline int64_t get_block_cnt() const { return n_blocks_; }
  inline int64_t get_block_list_cnt() { return blocks_.get_size(); }
  inline int64_t get_row_cnt() const { return row_cnt_; }
  inline int64_t get_col_cnt() const { return col_count_; }
  inline int64_t get_mem_hold() const { return mem_hold_; }
  inline int64_t get_mem_used() const { return mem_used_; }
  inline int64_t get_max_hold_mem() const { return max_hold_mem_; }
  inline int64_t get_file_fd() const { return io_.fd_; }
  inline int64_t get_file_dir_id() const { return io_.dir_id_; }
  inline int64_t get_file_size() const { return file_size_; }
  inline int64_t min_blk_size(const int64_t row_store_size)
  {
    int64_t size = std::max(std::max(static_cast<int64_t>(BLOCK_SIZE), default_block_size_), row_store_size);
    size = common::next_pow2(size);
    return size;
  }
  int init_block_buffer(void* mem, const int64_t size, Block *&block);
  int add_block(Block* block, bool need_swizzling, bool *added = nullptr);
  int append_block(char *buf, int size,  bool need_swizzling);
  void remove_added_blocks();
  bool has_dumped() { return has_dumped_; }
  inline int64_t get_row_cnt_in_memory() const { return row_cnt_ - dumped_row_cnt_; }
  inline int64_t get_row_cnt_on_disk() const { return dumped_row_cnt_; }
  void set_callback(ObSqlMemoryCallback *callback) { callback_ = callback; }
  void reset_callback() { callback_ = nullptr; }
  int dump(bool reuse, bool all_dump);
  // 目前dir id 的策略是上层逻辑（一般是算子）统一申请，然后再set过来
  void set_dir_id(int64_t dir_id) { io_.dir_id_ = dir_id; }
  int alloc_dir_id();
  uint64_t get_tenant_id() { return tenant_id_; }
  const char *get_label() { return label_; }
  TO_STRING_KV(K_(tenant_id), K_(label), K_(ctx_id),  K_(mem_limit),
      K_(row_cnt), K_(file_size));

private:
  static int get_timeout(int64_t &timeout_ms);
  void *alloc_blk_mem(const int64_t size, const bool for_iterator);
  void free_blk_mem(void *mem, const int64_t size = 0);
  void free_block(Block *item);
  void free_blk_list();
  bool shrink_block(int64_t size);
  int alloc_block_buffer(Block *&block, const int64_t data_size, const bool for_iterator);
  int alloc_block_buffer(Block *&block, const int64_t data_size,
        const int64_t min_size, const bool for_iterator);
  // new block is not needed if %min_size is zero. (finish add row)
  int switch_block(const int64_t min_size);
  int clean_memory_data(bool reuse);

  inline void use_block(Block *item)
    {
      cur_blk_ = item;
      int64_t used = item->get_buffer()->capacity() + sizeof(BlockBuffer);
      mem_used_ += used;
    }
  inline int dump_one_block(BlockBuffer *item);

  int write_file(void *buf, int64_t size);
  int read_file(void *buf, const int64_t size, const int64_t offset,
                const int64_t file_size, const int64_t cur_pos,
                int64_t &tmp_file_size);

  bool need_dump(int64_t extra_size);
  BlockBuffer* new_block();
  void set_io(int64_t size, char *buf) { io_.size_ = size; io_.buf_ = buf; }
  bool find_block_can_hold(const int64_t size, bool &need_shrink);
  int get_store_row(RowIterator &it, const StoredRow *&sr);
  int load_next_block(ChunkIterator &it);
  int load_next_chunk_blocks(ChunkIterator &it);
  inline void callback_alloc(int64_t size)
  {
    alloc_size_ += size;
    if (callback_ != nullptr) {
      callback_->alloc(size);
    }
  }
  inline void callback_free(int64_t size)
  {
    free_size_ += size;
    if (callback_ != nullptr) {
      callback_->free(size);
    }
  }
private:
  bool inited_;
  uint64_t tenant_id_;
  const char *label_;
  int64_t ctx_id_;
  int64_t mem_limit_;

  Block* cur_blk_;
  BlockList blocks_;  // ASSERT: all linked blocks has at least one row stored
  BlockList free_list_;  // empty blocks
  int64_t max_blk_size_; //max block ever allocated
  int64_t min_blk_size_; //min block ever allocated
  int64_t default_block_size_; //default(min) block size; blocks larger then this will not be reused
  int64_t n_blocks_;
  int64_t col_count_;
  int64_t row_cnt_;

  bool enable_dump_;
  bool has_dumped_;
  int64_t dumped_row_cnt_;

  //int fd_;
  blocksstable::ObTmpFileIOInfo io_;
  int64_t file_size_;
  int64_t n_block_in_file_;

  //BlockList blocks_;  // ASSERT: all linked blocks has at least one row stored
  int64_t mem_hold_;
  int64_t mem_used_;
  int64_t max_hold_mem_;
  common::DefaultPageAllocator inner_allocator_;
  common::ObIAllocator *allocator_;
  STORE_MODE row_store_mode_;
  int32_t *projector_;
  int64_t projector_size_;

  uint32_t row_extend_size_;
  int64_t alloc_size_;
  int64_t free_size_;
  ObSqlMemoryCallback *callback_;

  DISALLOW_COPY_AND_ASSIGN(ObChunkRowStore);
};

inline int ObChunkRowStore::BlockBuffer::advance(int64_t size)
{
  int ret = common::OB_SUCCESS;
  if (size < -cur_pos_) {
    //overflow
    ret = common::OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(size), K_(cur_pos));
  } else if (size > remain()) {
    ret = common::OB_BUF_NOT_ENOUGH;
    SQL_ENG_LOG(WARN, "buffer not enough", K(size), "remain", remain());
  } else {
    cur_pos_ += size;
  }
  return ret;
}

class ObChunkStoreUtil
{
public:
  static int alloc_dir_id(int64_t &dir_id);
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_BASIC_OB_RA_ROW_STORE_H_
