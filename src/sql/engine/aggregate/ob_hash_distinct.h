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

#ifndef SQL_ENGINE_AGGREGATE_OB_HASH_DISTINCT
#define SQL_ENGINE_AGGREGATE_OB_HASH_DISTINCT
#include "lib/string/ob_string.h"
#include "share/ob_define.h"
#include "common/row/ob_row.h"
#include "sql/engine/aggregate/ob_distinct.h"
#include "lib/container/ob_fixed_array.h"
#include "sql/engine/basic/ob_chunk_row_store.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_sql_mem_mgr_processor.h"
#include "lib/container/ob_2d_array.h"

namespace oceanbase {
namespace sql {

class ObHashDistinct : public ObDistinct {
  OB_UNIS_VERSION_V(1);

public:
  class ObHashDistinctCtx : public ObPhyOperatorCtx {
  public:
    static const int64_t MIN_PART_COUNT = 8;
    static const int64_t MAX_PART_COUNT = 256;
    enum HDState { SCAN_CHILD = 1, OUTPUT_HA = 2, PROCESS_IN_MEM = 3, GET_PARTITION = 4, SCAN_PARTITION = 5 };
    friend class ObHashDistinct;
    class PartitionLinkNode : public common::ObDLinkBase<PartitionLinkNode> {
    public:
      PartitionLinkNode(ObHashDistinctCtx* data, ObHashDistinctCtx* parent)
          : ObDLinkBase<PartitionLinkNode>(), data_(data), parent_(parent)
      {}
      ObHashDistinctCtx* data_;
      ObHashDistinctCtx* parent_;
    };
    struct HashRow {
      HashRow() : stored_row_(NULL), next_tuple_(NULL)
      {}
      const ObChunkRowStore::StoredRow* stored_row_;
      HashRow* next_tuple_;
      TO_STRING_KV(K_(stored_row), K(static_cast<void*>(next_tuple_)));
    };
    struct HashTable {
      const static int16_t BUCKET_BUF_SIZE = 1024;
      const static int16_t BUCKET_SHIFT = 10;
      const static int16_t BUCKET_MASK = 1023;
      HashTable() : mem_context_(NULL), buckets_(), nbuckets_(0), buckets_buf_(), buf_cnt_(0), cur_(0)
      {}
      void reuse()
      {
        buckets_.set_all(NULL);
        cur_ = 0;
      }
      void reset()
      {
        buckets_.reset();
        buckets_buf_.reset();
        nbuckets_ = 0;
        cur_ = 0;
      }
      TO_STRING_KV(K_(nbuckets), K_(buf_cnt), K_(cur));
      lib::MemoryContext* mem_context_;
      using RowArray = common::ObSegmentArray<HashRow*, OB_MALLOC_BIG_BLOCK_SIZE, common::ModulePageAllocator>;
      RowArray buckets_;
      int64_t nbuckets_;
      RowArray buckets_buf_;
      int64_t buf_cnt_;
      int64_t cur_;
    };
    OB_INLINE int64_t get_part_idx(const uint64_t hash_value)
    {
      int64_t part_idx = ((hash_value & part_mask_) >> bucket_bits_) % part_count_;
      return part_idx < part_count_ ? part_idx : part_count_ - 1;
    }

    OB_INLINE uint64_t get_bucket_idx(const uint64_t hash_value)
    {
      return hash_value & bucket_mask_;
    }
    explicit ObHashDistinctCtx(ObExecContext& exec_ctx);
    virtual ~ObHashDistinctCtx()
    {}
    virtual void destroy()
    {
      reset();
      ObPhyOperatorCtx::destroy_base();
    }
    void reset();
    void reuse();
    int estimate_memory_usage(int64_t input, int64_t mem_limit, int64_t& ha_mem_limit);
    int estimate_memory_usage_for_partition(int64_t input, int64_t mem_limit, int64_t& ha_mem_limit);
    int init_hash_table(const ObSQLSessionInfo* session, int64_t bucket_size, int64_t mem_limit);
    int init_partitions_info(ObSQLSessionInfo* session);
    int set_hash_info(int64_t part_count, int64_t n_bucket, int64_t level = 0);
    inline bool equal_with_hashrow(
        const ObHashDistinct* dist, const common::ObNewRow* row, uint64_t hash_val, const void* other) const;
    bool in_hash_tab(const ObHashDistinct* dist, const common::ObNewRow* row, const uint64_t hash_val,
        const uint64_t bucket_idx) const;
    int insert_hash_tab(const ObChunkRowStore::StoredRow* sr, const uint64_t bucket_idx);
    int process_rows_from_child(const ObHashDistinct* dist, const ObNewRow*& row, bool& got_distinct_row);
    int process_one_partition(const ObHashDistinct* dist, ObChunkRowStore* row_store, ObSQLSessionInfo* my_session,
        const ObNewRow*& row, bool& got_distinct_row);
    // get cur sub partition from list
    inline bool get_cur_sub_partition(ObHashDistinctCtx* top_ctx, PartitionLinkNode*& link_node)
    {
      bool got = false;
      while (!got && top_ctx->sub_list_.get_size() > 0) {
        link_node = top_ctx->sub_list_.get_first();
        got = true;
      }
      return got;
    }
    int init_sql_mem_profile(ObIAllocator* allocator, int64_t max_mem_size, int64_t cache_size);
    void destroy_sql_mem_profile();
    int update_used_mem_bound(int64_t mem_used);
    int update_used_mem_size();
    int update_mem_status_periodically();
    int add_row(const ObNewRow* row, ObChunkRowStore::StoredRow** sr, bool& ha_is_full_);
    inline int64_t get_mem_used()
    {
      return nullptr == mem_context_ ? 0 : mem_context_->used();
    }
    inline int64_t get_data_size()
    {
      int64_t data_size = 0;
      if (OB_NOT_NULL(sql_mem_processor_)) {
        data_size = sql_mem_processor_->get_data_size();
      }
      return data_size;
    }
    void calc_data_ratio();
    int64_t get_total_based_mem_used();
    int get_next_partition(ObHashDistinctCtx* top_ctx, PartitionLinkNode*& link_node);
    int out_put_ha_rows(const ObNewRow*& row);
    int get_dist_row_from_in_mem_partitions(const ObHashDistinct* dist, const ObNewRow*& row, bool& got_distinct_row);
    int clean_up_partitions();
    inline int64_t get_partitions_mem_usage();
    int get_next_mem_partition();
    int open_cur_partition(ObChunkRowStore* row_store);
    int assign_sub_ctx(ObHashDistinctCtx* parent, ObChunkRowStore* parent_row_stores, ObHashDistinctCtx* top_ctx);
    DISALLOW_COPY_AND_ASSIGN(ObHashDistinctCtx);

  private:
    int64_t mem_limit_;
    // n_bucket must be pow2, so the last N bits of hash_value can used as bucket_idx directly
    // N = bucket_bits_ = log2(n_buckets_)
    // and last M to N + 1 bits can used to calculate part_idx
    //(M - N + 1) = next_pow2(part_count_ - 1) - 1
    //           64..         M   N      1
    // hash_value: |----------|---|-------|
    // bucket_mask:          0xF..F000..000
    // part_mask:                0xFFF..FFF
    int64_t part_count_;
    int64_t part_mem_limit_;
    int64_t estimated_mem_usage_;  // estimate according to card and the avg_width of tuples
    // int64_t n_bucket_;
    int64_t bucket_bits_;
    int64_t part_mask_;
    int64_t bucket_mask_;
    // part_it_ will go through part_row_stores_ twice, one for rs in mem, one for rs has dumped
    ObChunkRowStore* part_row_stores_;
    ObChunkRowStore::Iterator part_it_;
    int64_t cur_it_part_;
    // bool it_in_mem_; //iterate rows in mem
    HashTable* hash_tab_;
    ObChunkRowStore* ha_row_store_;
    lib::MemoryContext* mem_context_ = nullptr;
    bool ha_is_full_;
    bool bkt_created_;
    bool child_finished_;

    ObDList<PartitionLinkNode> sub_list_;

    int64_t state_;
    int64_t part_level_;

    int64_t cur_rows_;
    int64_t pre_mem_used_;
    ObSqlWorkAreaProfile* profile_;
    ObSqlMemMgrProcessor* sql_mem_processor_;
    ObHashDistinctCtx* top_ctx_;
    bool is_top_ctx_;
    ObPhyOperatorType op_type_;
    uint64_t op_id_;
    bool enable_sql_dumped_;
    bool first_read_part_;

    friend class ObHashDistinct;
  };

public:
  static const int64_t MIN_BUCKET_COUNT = 1L << 14;  // 16384;
  static const int64_t MAX_BUCKET_COUNT = 1L << 19;  // 524288;
  static const int64_t HASH_DISTINCT_BUCKET_RATIO = 2;
  explicit ObHashDistinct(common::ObIAllocator& alloc);
  // ObHashDistinct();
  virtual ~ObHashDistinct();
  virtual void set_mem_limit(int64_t mem_limit)
  {
    mem_limit_ = mem_limit;
  }
  virtual void reset();
  virtual void reuse();
  virtual int rescan(ObExecContext& ctx) const;

private:
  int estimate_memory_usage(
      ObHashDistinctCtx* hash_ctx, int64_t memory_limit, int64_t& ha_mem, int64_t& mem_need) const;
  int get_hash_value(const common::ObNewRow* row, uint64_t& hash_value) const;

  int init_ha(const ObSQLSessionInfo* session, ObHashDistinctCtx* hash_ctx) const;

  // bool hashrows_are_equal(const ObHashDistinctCtx::HashRow *one) const;

  inline bool equal_with_hashrow(const common::ObNewRow* row, uint64_t hash_val, const void* other) const;

  /* bool in_hash_tab(const ObHashDistinctCtx* hash_ctx,
                 const common::ObNewRow *row,
                 const uint64_t hash_val,
                 const uint64_t bucket_idx) const;
  */
  // Get bucket size to create
  static int get_bucket_size(int64_t est_bucket, int64_t max_bound_size, double extra_ratio, int64_t& bucket_size);

  static int64_t estimate_mem_size_by_rows(int64_t rows)
  {
    return next_pow2(rows * HASH_DISTINCT_BUCKET_RATIO);
  }
  /**
   * @brief init operator context, will create a physical operator context (and a current row space)
   * @param ctx[in], execute context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  virtual int init_op_ctx(ObExecContext& ctx) const;
  /**
   * @brief: called by get_next_row(), get a row from the child operator or row_store
   * @param: ctx[in], execute context
   * @param: row[out], ObSqlRow an obj array and row_size
   */
  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  /**
   * @brief open operator, not including children operators.
   * called by open.
   * Every op should implement this method.
   */
  virtual int inner_open(ObExecContext& ctx) const;
  /**
   * @brief close operator, not including children operators.
   * Every op should implement this method.
   */
  virtual int inner_close(ObExecContext& ctx) const;
  /**
   * @brief for specified phy operator to print it's member variable with json key-value format
   * @param buf[in] to string buffer
   * @param buf_len[in] buffer length
   * @return if success, return the length used by print string, otherwise return 0
   */
  virtual int64_t to_string_kv(char* buf, const int64_t buf_len) const;

  int64_t mem_limit_;

  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObHashDistinct);
};
}  // namespace sql
}  // namespace oceanbase

#endif
