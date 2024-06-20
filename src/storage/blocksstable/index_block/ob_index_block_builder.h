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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_INDEX_MICRO_BLOCK_BUILDER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_INDEX_MICRO_BLOCK_BUILDER_H_

#include "lib/hash/ob_cuckoo_hashmap.h"
#include "storage/blocksstable/index_block/ob_index_block_row_struct.h"
#include "storage/blocksstable/ob_macro_block_writer.h"
#include "storage/blocksstable/ob_sstable_meta.h"
#include "storage/blocksstable/ob_micro_block_reader.h"
#include "storage/blocksstable/encoding/ob_micro_block_decoder.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"
#include "share/cache/ob_kvcache_pre_warmer.h"
#include "ob_index_block_aggregator.h"
#include "storage/blocksstable/ob_data_store_desc.h"

namespace oceanbase
{
namespace blocksstable
{
struct ObBlockInfo;
struct ObIndexTreeRootCtx;
class ObIMicroBlockReader;
class ObIMacroBlockFlushCallback;

static const uint32_t META_BLOCK_MAGIC_NUM = 0x7498;
static const uint32_t META_BLOCK_VERSION = 1;
static const int64_t DEFAULT_MICRO_BLOCK_WRITER_COUNT = 64 + 1;
static const int64_t DEFAULT_MACRO_LEVEL_ROWS_COUNT = 8;
static const int64_t DEFAULT_MACRO_BLOCK_CNT = 64;
static const int64_t SMALL_SSTABLE_THRESHOLD = 1 << 20; // 1M
typedef common::ObSEArray<ObIndexTreeRootCtx *, DEFAULT_MICRO_BLOCK_WRITER_COUNT>
    IndexTreeRootCtxList;
typedef common::ObSEArray<ObDataMacroBlockMeta *, DEFAULT_MACRO_LEVEL_ROWS_COUNT>
    ObMacroMetasArray;
typedef common::ObSEArray<int64_t, DEFAULT_MACRO_LEVEL_ROWS_COUNT>
    ObAbsoluteOffsetArray;
struct ObIndexTreeRootCtx final
{
public:
  ObIndexTreeRootCtx()
    :allocator_(nullptr),
     last_key_(),
     task_idx_(-1),
     data_column_cnt_(0),
     data_blocks_cnt_(0),
     macro_metas_(nullptr),
     absolute_offsets_(nullptr),
     use_old_macro_block_count_(0),
     meta_block_offset_(0),
     meta_block_size_(0),
     last_macro_size_(0),
     use_absolute_offset_(false),
     is_inited_(false) {}
  ~ObIndexTreeRootCtx();
  int init(common::ObIAllocator &allocator);
  void reset();
  bool is_absolute_vaild() const;
  int add_absolute_row_offset(const int64_t absolute_row_offset);
  int add_macro_block_meta(const ObDataMacroBlockMeta &macro_meta);
  int get_macro_id_array(common::ObIArray<blocksstable::MacroBlockId> &block_ids); //inc ref
  // used for small sstable
  int change_macro_id(const MacroBlockId &macro_block_id);
  TO_STRING_KV(KP(allocator_), K_(last_key), K_(task_idx), K_(data_column_cnt), K_(data_blocks_cnt),
      K_(use_old_macro_block_count), K_(meta_block_offset), K_(meta_block_size),
      K_(last_macro_size), KP_(macro_metas),
      KP_(absolute_offsets), K_(use_absolute_offset), K_(is_inited));
  common::ObIAllocator *allocator_;
  //TODO :replace by task id
  ObDatumRowkey last_key_;
  int64_t task_idx_;
  int64_t data_column_cnt_;
  int64_t data_blocks_cnt_;
  ObMacroMetasArray *macro_metas_;
  ObAbsoluteOffsetArray *absolute_offsets_;
  int64_t use_old_macro_block_count_;
  int64_t meta_block_offset_;
  int64_t meta_block_size_;
  int64_t last_macro_size_;
  bool use_absolute_offset_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObIndexTreeRootCtx);
};

struct ObIndexTreeRootCtxCompare final
{
public:
  ObIndexTreeRootCtxCompare(int &ret, const ObStorageDatumUtils &datum_utils) : ret_(ret), datum_utils_(datum_utils) {}
  ~ObIndexTreeRootCtxCompare() = default;
  bool operator() (const ObIndexTreeRootCtx *left, const ObIndexTreeRootCtx *right)
  {
    int &ret = ret_;
    int32_t cmp_ret = 0;
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(left) || OB_ISNULL(right)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected null root block desc", K(ret));
    } else if (OB_FAIL(left->last_key_.compare(right->last_key_, datum_utils_, cmp_ret))) {
      STORAGE_LOG(WARN, "Failed to compare last key", K(ret), KPC(left), KPC(right));
      cmp_ret = 0;
    }
    return cmp_ret < 0;
  }
  int &ret_;
  const ObStorageDatumUtils &datum_utils_;
};

struct ObIndexTreeRootCtxCGCompare final
{
public:
  ObIndexTreeRootCtxCGCompare(int &ret) : ret_(ret) {}
  ~ObIndexTreeRootCtxCGCompare() = default;
  bool operator() (const ObIndexTreeRootCtx *left, const ObIndexTreeRootCtx *right)
  {
    int &ret = ret_;
    bool cmp_ret = false;
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(left) || OB_ISNULL(right) || left->task_idx_ < 0 || right->task_idx_ < 0) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected null root block desc", K(ret), KPC(left), KPC(right));
    } else {
      cmp_ret = left->task_idx_ < right->task_idx_;
    }
    return cmp_ret;
  }
  int &ret_;
};

struct ObDataMacroMetaCompare final
{
public:
  ObDataMacroMetaCompare(int &ret, const ObStorageDatumUtils &datum_utils) : ret_(ret), datum_utils_(datum_utils) {}
  ~ObDataMacroMetaCompare() = default;
  bool operator() (const ObDataMacroBlockMeta *left, const ObDataMacroBlockMeta *right)
  {
    int &ret = ret_;
    int32_t cmp_ret = 0;
    ObDatumRowkey left_key;
    ObDatumRowkey right_key;
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(left) || OB_ISNULL(right)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected null meta", K(ret));
    } else if (OB_FAIL(left->get_rowkey(left_key))) {
      STORAGE_LOG(WARN, "Failed to get last key", K(ret), KPC(left));
    } else if (OB_FAIL(right->get_rowkey(right_key))) {
      STORAGE_LOG(WARN, "Failed to get last key", K(ret), KPC(right));
    } else if (OB_FAIL(left_key.compare(right_key, datum_utils_, cmp_ret))) {
      STORAGE_LOG(WARN, "Failed to compare last key", K(ret), KPC(left), KPC(right));
      cmp_ret = 0;
    }
    return cmp_ret < 0;
  }
  int &ret_;
  const ObStorageDatumUtils &datum_utils_;
};


struct ObIndexTreeRootBlockDesc final
{
public:
  ObIndexTreeRootBlockDesc()
    :addr_(),
     buf_(nullptr),
     height_(0),
     is_meta_root_(false) {}
  ~ObIndexTreeRootBlockDesc() = default;
  bool is_valid() const;
  bool is_empty() const { return addr_.is_none(); }
  void set_empty();
  bool is_mem_type() const { return addr_.is_memory(); }
  bool is_file_type() const { return addr_.is_file(); }
  TO_STRING_KV(K_(addr), KP_(buf), K_(height), K_(is_meta_root));
public:
  storage::ObMetaDiskAddr addr_;
  char *buf_;
  int64_t height_;
  bool is_meta_root_;
};

// when we close index tree, return tree info to record necessary inner info
struct ObIndexTreeInfo final
{
  ObIndexTreeInfo()
    :root_desc_(),
     row_count_(0),
     max_merged_trans_version_(0),
     contain_uncommitted_row_(false) {}
  ~ObIndexTreeInfo() = default;
  void set_empty();
  TO_STRING_KV(K_(root_desc), K_(row_count),
      K_(max_merged_trans_version), K_(contain_uncommitted_row));
  ObIndexTreeRootBlockDesc root_desc_;
  int64_t row_count_;
  int64_t max_merged_trans_version_;
  bool contain_uncommitted_row_;
};

struct ObSSTableMergeRes final
{
public:
  ObSSTableMergeRes();
  ~ObSSTableMergeRes();
  bool is_valid() const;
  void reset();
  int assign(const ObSSTableMergeRes &src);
  int prepare_column_checksum_array(const int64_t data_column_cnt);
  static int fill_column_checksum_for_empty_major(
      const int64_t column_count,
      common::ObIArray<int64_t> &column_checksums);
  OB_INLINE static void fill_addr_and_data(const ObIndexTreeRootBlockDesc &src_block_desc,
                                           storage::ObMetaDiskAddr &dst_addr,
                                           ObMicroBlockData &dst_data)
  {
    dst_addr = src_block_desc.addr_;
    dst_data.buf_ = src_block_desc.buf_;
    dst_data.size_ = src_block_desc.addr_.size();
  }
  TO_STRING_KV(K_(root_desc), K_(data_root_desc), K(data_block_ids_.count()), K(other_block_ids_.count()),
      K_(index_blocks_cnt), K_(data_blocks_cnt), K_(micro_block_cnt),
      K_(data_column_cnt), K_(data_column_checksums),
      K_(row_count), K_(max_merged_trans_version), K_(contain_uncommitted_row),
      K_(occupy_size), K_(original_size), K_(data_checksum), K_(use_old_macro_block_count),
      K_(compressor_type), K_(root_row_store_type), K_(nested_offset), K_(nested_size),
      K_(encrypt_id), K_(master_key_id), KPHEX_(encrypt_key, sizeof(encrypt_key_)));
public:
  ObIndexTreeRootBlockDesc root_desc_;
  ObIndexTreeRootBlockDesc data_root_desc_;
  common::ObSEArray<MacroBlockId, DEFAULT_MACRO_BLOCK_CNT> data_block_ids_;
  common::ObSEArray<MacroBlockId, DEFAULT_MACRO_BLOCK_CNT> other_block_ids_;
  int64_t index_blocks_cnt_;
  int64_t data_blocks_cnt_;
  int64_t micro_block_cnt_;
  int64_t data_column_cnt_;
  int64_t row_count_;
  int64_t max_merged_trans_version_;
  bool contain_uncommitted_row_;
  int64_t occupy_size_;
  int64_t original_size_;
  int64_t data_checksum_;
  int64_t use_old_macro_block_count_;
  common::ObSEArray<int64_t, 1> data_column_checksums_;
  common::ObCompressorType compressor_type_;
  int64_t encrypt_id_;
  int64_t master_key_id_;
  int64_t nested_offset_;
  int64_t nested_size_;
  ObRowStoreType root_row_store_type_;
  char encrypt_key_[share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH];
  DISALLOW_COPY_AND_ASSIGN(ObSSTableMergeRes);
};

class ObBaseIndexBlockBuilder
{
public:
  ObBaseIndexBlockBuilder();
  virtual ~ObBaseIndexBlockBuilder();
  int init(const ObDataStoreDesc &data_store_desc,
           ObDataStoreDesc &index_store_desc,
           ObIAllocator &allocator,
           ObMacroBlockWriter *macro_writer,
           const int64_t level);
  int append_row(const ObIndexBlockRowDesc &row_desc);
  int append_row(const ObDataMacroBlockMeta &macro_meta, ObIndexBlockRowDesc &row_desc);
  int close(ObIAllocator &allocator, ObIndexTreeInfo &tree_info);
  void reset();
protected:
/* protected interfaces will be accessed by derived class*/
  int check_order(const ObIndexBlockRowDesc &row_desc);
  virtual int append_index_micro_block();
  int build_index_micro_block(ObMicroBlockDesc &micro_block_desc);
  void clean_status();
  int get_aggregated_row(ObIndexBlockRowDesc &next_row_desc);
  virtual int insert_and_update_index_tree(const ObDatumRow *index_row);
  int close_index_tree(ObBaseIndexBlockBuilder *&root_builder);
  void block_to_row_desc(
      const ObMicroBlockDesc &micro_block_desc,
      ObIndexBlockRowDesc &row_desc);
  int meta_to_row_desc(
      const ObDataMacroBlockMeta &macro_meta,
      ObIndexBlockRowDesc &row_desc);
  int row_desc_to_meta(
      const ObIndexBlockRowDesc &macro_row_desc,
      ObDataMacroBlockMeta &macro_meta,
      ObIAllocator &allocator);
  int64_t get_row_count() { return micro_writer_->get_row_count(); }
  virtual OB_INLINE bool need_pre_warm() const { return false; }
private:
  int new_next_builder(ObBaseIndexBlockBuilder *&next_builder);
  virtual int append_next_row(const ObMicroBlockDesc &micro_block_desc);
  int64_t calc_basic_micro_block_data_offset(const uint64_t column_cnt);
protected:
  static const int64_t ROOT_BLOCK_SIZE_LIMIT = 16 << 10; // 16KB
  static const int64_t MIN_INDEX_MICRO_BLOCK_ROW_CNT = 10;
  static const int64_t MAX_LEVEL_LIMIT = 20;

  bool is_inited_;
  bool is_closed_;
  bool use_absolute_offset_;
  ObDataStoreDesc *index_store_desc_;
  // full_store_col_desc in data_store_desc is only used to generate skip index
  const ObDataStoreDesc *data_store_desc_;
  ObIndexBlockRowBuilder row_builder_;
  ObDatumRowkey last_rowkey_;
  compaction::ObLocalArena row_allocator_; // Used to apply for memory whose lifetime is row
  common::ObIAllocator *allocator_;        // Used to apply for memory whose lifetime is task
  ObIMicroBlockWriter *micro_writer_;
  ObMacroBlockWriter *macro_writer_;
  ObIndexBlockCachePreWarmer index_block_pre_warmer_;
  ObMicroBlockAdaptiveSplitter micro_block_adaptive_splitter_;
  int64_t row_offset_;
  ObIndexBlockAggregator index_block_aggregator_;
private:
  ObBaseIndexBlockBuilder *next_level_builder_;
  int64_t level_; // default 0
  ObAggRowWriter agg_row_writer_;
  DISALLOW_COPY_AND_ASSIGN(ObBaseIndexBlockBuilder);
};

class ObDataIndexBlockBuilder : public ObBaseIndexBlockBuilder
{
public:
  ObDataIndexBlockBuilder();
  ~ObDataIndexBlockBuilder();
  int init(const ObDataStoreDesc &data_store_desc,
           ObSSTableIndexBuilder &sstable_builder);
  int append_row(const ObMicroBlockDesc &micro_block_desc,
                 const ObMacroBlock &macro_block);
  int generate_macro_row(ObMacroBlock &macro_block, const MacroBlockId &id, const int64_t ddl_start_row_offset);
  int append_macro_block(const ObDataMacroBlockMeta &macro_meta);
  int cal_macro_meta_block_size(const ObDatumRowkey &rowkey, int64_t &estimate_block_size);
  int set_parallel_task_idx(const int64_t task_idx);
  inline int64_t get_estimate_index_block_size() const { return estimate_leaf_block_size_; }
  inline int64_t get_estimate_meta_block_size() const { return estimate_meta_block_size_; }
  int close(const ObDatumRowkey &last_key,
            ObMacroBlocksWriteCtx *data_write_ctx);
  void reset();
private:
  int append_index_micro_block(
      ObMacroBlock &macro_block,
      const MacroBlockId &block_id,
      const int64_t ddl_start_row_offset);
  int write_meta_block(
      ObMacroBlock &macro_block,
      const MacroBlockId &block_id,
      const ObIndexBlockRowDesc &macro_row_desc,
      const int64_t ddl_start_row_offset);

  void update_macro_meta_with_offset(const int64_t macro_block_row_count, const int64_t ddl_start_row_offset);
  virtual int insert_and_update_index_tree(const ObDatumRow *index_row) override;
  int append_next_row(const ObMicroBlockDesc &micro_block_desc, ObIndexBlockRowDesc &macro_row_desc);
  int add_row_offset(ObIndexBlockRowDesc &row_desc);
  virtual OB_INLINE bool need_pre_warm() const override { return true; }
private:
  ObSSTableIndexBuilder *sstable_builder_;
  compaction::ObLocalArena task_allocator_;  // Used to apply for memory whose lifetime is task
  compaction::ObLocalArena meta_row_allocator_; // Used to apply for memory whose lifetime is row
  ObMicroBlockBufferHelper micro_helper_;
  ObIndexBlockRowDesc macro_row_desc_;
  ObIndexTreeRootCtx *index_tree_root_ctx_;
  ObMacroMetasArray *macro_meta_list_;
  ObIMicroBlockWriter *meta_block_writer_;
  ObDatumRow meta_row_;
  ObDataMacroBlockMeta macro_meta_;
  ObStorageDatum cg_rowkey_;
  ObDataStoreDesc *leaf_store_desc_;
  ObDataStoreDesc *local_leaf_store_desc_;
  int64_t data_blocks_cnt_;
  int64_t meta_block_offset_;
  int64_t meta_block_size_;
  int64_t estimate_leaf_block_size_;
  int64_t estimate_meta_block_size_;
  DISALLOW_COPY_AND_ASSIGN(ObDataIndexBlockBuilder);
};

class ObMetaIndexBlockBuilder : public ObBaseIndexBlockBuilder
{
public:
  ObMetaIndexBlockBuilder();
  ~ObMetaIndexBlockBuilder();
  int init(ObDataStoreDesc &data_store_desc,
           ObIAllocator &allocator,
           ObMacroBlockWriter &macro_writer);
  int append_leaf_row(const ObDatumRow &leaf_row);
  int close(
      ObIAllocator &allocator,
      const IndexTreeRootCtxList &roots,
      const int64_t nested_size,
      const int64_t nested_offset,
      ObIndexTreeRootBlockDesc &block_desc);
  void reset();
private:
  int build_single_macro_row_desc(
      const IndexTreeRootCtxList &roots,
      const int64_t nested_size,
      const int64_t nested_offset,
      ObIAllocator &allocator);
  int build_micro_block(ObMicroBlockDesc &micro_block_desc);
  int append_micro_block(ObMicroBlockDesc &micro_block_desc);
  int build_single_node_tree(
      ObIAllocator &allocator,
      const ObMicroBlockDesc &micro_block_desc,
      ObIndexTreeRootBlockDesc &block_desc);
private:
  ObIMicroBlockWriter *micro_writer_;
  ObMacroBlockWriter *macro_writer_;
  ObDatumRowkey last_leaf_rowkey_;
  compaction::ObLocalArena leaf_rowkey_allocator_;
  int64_t meta_row_offset_;
  DISALLOW_COPY_AND_ASSIGN(ObMetaIndexBlockBuilder);
};

class ObIndexBlockRebuilder final
{
public:
  ObIndexBlockRebuilder();
  ~ObIndexBlockRebuilder();
  int init(ObSSTableIndexBuilder &sstable_builder, bool need_sort = true, const int64_t *task_idx = nullptr, const bool is_ddl_merge_sstable = false);
  int append_macro_row(
      const char *buf,
      const int64_t size,
      const MacroBlockId &macro_id);
  int append_macro_row(const ObDataMacroBlockMeta &macro_meta);
  int close();
  void reset();
  static int get_macro_meta(
      const char *buf,
      const int64_t size,
      const MacroBlockId &macro_id,
      common::ObIAllocator &allocator,
      ObDataMacroBlockMeta *&macro_meta);
private:
  static int inner_get_macro_meta(
      const char *buf,
      const int64_t size,
      const MacroBlockId &macro_id,
      common::ObIAllocator &allocator,
      ObDataMacroBlockMeta *&macro_meta,
      ObSSTableMacroBlockHeader &macro_header);
  static int get_meta_block(
      const char *buf,
      const int64_t buf_size,
      common::ObIAllocator &allocator,
      ObSSTableMacroBlockHeader &header,
      ObMicroBlockData &meta_block);
  int fill_abs_offset_for_ddl();
private:
  bool is_inited_;
  bool need_sort_;
  lib::ObMutex mutex_;
  ObDataStoreDesc *index_store_desc_;
  ObIndexTreeRootCtx *index_tree_root_ctx_;
  ObMacroMetasArray *macro_meta_list_;
  ObSSTableIndexBuilder *sstable_builder_;
  DISALLOW_COPY_AND_ASSIGN(ObIndexBlockRebuilder);
};


class ObSSTableIndexBuilder final
{
public:
  class ObMacroMetaIter final
  {
  public:
    ObMacroMetaIter()
      : macro_metas_(),
        block_cnt_(0),
        cur_block_idx_(0),
        is_inited_(false)
    {}
    ~ObMacroMetaIter() = default;
    int init(IndexTreeRootCtxList &roots, const bool is_cg);
    int get_next_macro_block(const ObDataMacroBlockMeta *&macro_meta);
    int64_t get_macro_block_count() const { return block_cnt_; }
    void reuse() { cur_block_idx_ = 0; }
    TO_STRING_KV(K_(macro_metas), K_(block_cnt), K_(cur_block_idx), K_(is_inited));
  private:
    ObMacroMetasArray macro_metas_;
    int64_t block_cnt_;
    int64_t cur_block_idx_;
    bool is_inited_;
  };
  enum ObSpaceOptimizationMode
  {
    ENABLE = 0,   // enable the optimization for small sstable
    DISABLE = 1,  // disable the optimization
  };
public:
  ObSSTableIndexBuilder();
  ~ObSSTableIndexBuilder();
  int init(
      const ObDataStoreDesc &data_desc,
      ObIMacroBlockFlushCallback *callback = nullptr,
      ObSpaceOptimizationMode mode = ENABLE);
  void reset();
  int new_index_builder(ObDataIndexBlockBuilder *&builder,
                        const ObDataStoreDesc &data_store_desc,
                        ObIAllocator &data_allocator);
  int init_builder_ptrs(
      ObSSTableIndexBuilder *&sstable_builder,
      ObDataStoreDesc *&index_store_desc,
      ObDataStoreDesc *&leaf_store_desc,
      ObIndexTreeRootCtx *&index_tree_root_ctx,
      ObMacroMetasArray *&macro_meta_list);
  int append_root(ObIndexTreeRootCtx &index_tree_root_ctx);
  int close(
      ObSSTableMergeRes &res,
      const int64_t nested_size = OB_DEFAULT_MACRO_BLOCK_SIZE,
      const int64_t nested_offset = 0);
  int init_meta_iter(ObMacroMetaIter &iter);
  bool is_inited() const { return is_inited_; }
  TO_STRING_KV(K(roots_.count()));
public:
  static bool check_version_for_small_sstable(const ObDataStoreDesc &index_desc);
  static int load_single_macro_block(
      const ObDataMacroBlockMeta &macro_meta,
      const int64_t nested_size,
      const int64_t nested_offset,
      ObIAllocator &allocator,
      char *&data_buf);
  static int parse_macro_header(
      const char *buf,
      const int64_t buf_size,
      ObSSTableMacroBlockHeader &macro_header);
private:
  int check_and_rewrite_sstable(ObSSTableMergeRes &res);
  int check_and_rewrite_sstable_without_size(ObSSTableMergeRes &res);
  int do_check_and_rewrite_sstable(ObBlockInfo &block_info);
  int rewrite_small_sstable(ObSSTableMergeRes &res);
  bool check_single_block();
  int set_row_store_type(ObDataStoreDesc &index_desc);
  bool check_index_desc(const ObDataStoreDesc &index_desc) const;
  int trim_empty_roots();
  int sort_roots();
  int merge_index_tree(ObSSTableMergeRes &res);
  int build_meta_tree(ObSSTableMergeRes &res);
  int generate_macro_blocks_info(ObSSTableMergeRes &res);
  int accumulate_macro_column_checksum(
      const ObDataMacroBlockMeta &meta, ObSSTableMergeRes &res);
  void clean_status();
private:
  compaction::ObLocalSafeArena sstable_allocator_; // to keep multi-thread safe. Used to apply for memory whose lifetime is sstable
  compaction::ObLocalArena self_allocator_; // used to apply for ObSSTableMergeRes
  compaction::ObLocalArena row_allocator_; // Used to apply for memory whose lifetime is row
  lib::ObMutex mutex_;
  ObWholeDataStoreDesc data_store_desc_;
  ObWholeDataStoreDesc index_store_desc_;
  ObDataStoreDesc leaf_store_desc_;
  ObDataStoreDesc container_store_desc_; // used to open all index macro writers
  ObDatumRow index_row_;
  ObBaseIndexBlockBuilder index_builder_;
  ObMetaIndexBlockBuilder data_builder_;
  ObMacroBlockWriter macro_writer_;
  ObIMacroBlockFlushCallback *callback_;
  IndexTreeRootCtxList roots_;
  ObSSTableMergeRes res_;
  ObSpaceOptimizationMode optimization_mode_;
  bool is_closed_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObSSTableIndexBuilder);
};

}//end namespace blocksstable
}//end namespace oceanbase
#endif
