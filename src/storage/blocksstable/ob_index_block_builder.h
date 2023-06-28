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
#include "storage/blocksstable/ob_index_block_row_struct.h"
#include "storage/blocksstable/ob_macro_block_writer.h"
#include "storage/blocksstable/ob_sstable_meta.h"
#include "storage/blocksstable/ob_micro_block_reader.h"
#include "storage/blocksstable/encoding/ob_micro_block_decoder.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"
#include "share/cache/ob_kvcache_pre_warmer.h"

namespace oceanbase
{
namespace blocksstable
{
struct ObBlockInfo;
struct ObIndexMicroBlockDesc;
class ObIMicroBlockReader;
class ObIMacroBlockFlushCallback;

static const uint32_t META_BLOCK_MAGIC_NUM = 0x7498;
static const uint32_t META_BLOCK_VERSION = 1;
static const int64_t DEFAULT_MICRO_BLOCK_WRITER_COUNT = 64 + 1;
static const int64_t DEFAULT_MACRO_LEVEL_ROWS_COUNT = 8;
static const int64_t DEFAULT_MACRO_BLOCK_CNT = 64;
static const int64_t SMALL_SSTABLE_THRESHOLD = 1 << 20; // 1M
typedef common::ObSEArray<ObIndexMicroBlockDesc *, DEFAULT_MICRO_BLOCK_WRITER_COUNT>
    IndexMicroBlockDescList;

typedef common::ObSEArray<ObMacroBlocksWriteCtx *, DEFAULT_MICRO_BLOCK_WRITER_COUNT>
    MacroBlocksWriteCtxList;

typedef common::ObSEArray<ObDataMacroBlockMeta *, DEFAULT_MACRO_LEVEL_ROWS_COUNT>
    ObMacroMetasArray;
struct ObIndexMicroBlockDesc final
{
public:
  ObIndexMicroBlockDesc()
    :last_key_(),
     data_column_cnt_(0),
     data_blocks_cnt_(0),
     macro_metas_(nullptr),
     data_write_ctx_(nullptr),
     meta_block_offset_(0),
     meta_block_size_(0),
     last_macro_size_(0) {}
  ~ObIndexMicroBlockDesc() = default;
  TO_STRING_KV(K_(last_key), K_(data_column_cnt), K_(data_blocks_cnt),
       K_(meta_block_offset), K_(meta_block_size), K_(last_macro_size),
       KPC_(data_write_ctx), KP(macro_metas_));
  ObDatumRowkey last_key_;
  int64_t data_column_cnt_;
  int64_t data_blocks_cnt_;
  ObMacroMetasArray *macro_metas_;
  ObMacroBlocksWriteCtx *data_write_ctx_; // contains: block_ids array [hold ref]
  int64_t meta_block_offset_;
  int64_t meta_block_size_;
  int64_t last_macro_size_;
};

struct ObIndexMicroBlockDescCompare final
{
public:
  ObIndexMicroBlockDescCompare(int &ret, const ObStorageDatumUtils &datum_utils) : ret_(ret), datum_utils_(datum_utils) {}
  ~ObIndexMicroBlockDescCompare() = default;
  bool operator() (const ObIndexMicroBlockDesc *left, const ObIndexMicroBlockDesc *right)
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
  int init(ObDataStoreDesc &index_store_desc,
           ObIAllocator &allocator,
           ObMacroBlockWriter *macro_writer,
           const int64_t level = 0);
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
  void update_accumulative_info(ObIndexBlockRowDesc &next_row_desc);
  virtual int insert_and_update_index_tree(const ObDatumRow *index_row);
  int close_index_tree(ObBaseIndexBlockBuilder *&root_builder);
  void block_to_row_desc(
      const ObMicroBlockDesc &micro_block_desc,
      ObIndexBlockRowDesc &row_desc);
  int meta_to_row_desc(
      const ObDataMacroBlockMeta &macro_meta,
      ObIndexBlockRowDesc &row_desc);
  void row_desc_to_meta(
      const ObIndexBlockRowDesc &macro_row_desc,
      ObDataMacroBlockMeta &macro_meta);
  int64_t get_row_count() { return micro_writer_->get_row_count(); }
private:
  void reset_accumulative_info();
  int new_next_builder(ObBaseIndexBlockBuilder *&next_builder);
  virtual int append_next_row(const ObMicroBlockDesc &micro_block_desc);
  int64_t calc_basic_micro_block_data_offset(const uint64_t column_cnt);

protected:
  static const int64_t ROOT_BLOCK_SIZE_LIMIT = 16 << 10; // 16KB

  bool is_inited_;
  bool is_closed_;
  ObDataStoreDesc *index_store_desc_;
  ObRowkeyReadInfo idx_read_info_;
  ObIndexBlockRowBuilder row_builder_;
  ObDatumRowkey last_rowkey_;
  common::ObArenaAllocator rowkey_allocator_;
  common::ObIAllocator *allocator_;
  ObIMicroBlockWriter *micro_writer_;
  ObMacroBlockWriter *macro_writer_;
  // accumulative info
  ObIndexBlockCachePreWarmer index_block_pre_warmer_;
  int64_t row_count_;
  int64_t row_count_delta_;
  int64_t max_merged_trans_version_;
  int64_t macro_block_count_;
  int64_t micro_block_count_;
  bool can_mark_deletion_;
  bool contain_uncommitted_row_;
  bool has_string_out_row_;
  bool has_lob_out_row_;
  bool is_last_row_last_flag_;
private:
  ObBaseIndexBlockBuilder *next_level_builder_;
  int64_t level_; // default 0
};

class ObDataIndexBlockBuilder : public ObBaseIndexBlockBuilder
{
public:
  ObDataIndexBlockBuilder();
  ~ObDataIndexBlockBuilder();
  int init(ObDataStoreDesc &data_store_desc,
           ObSSTableIndexBuilder &sstable_builder);
  int append_row(const ObMicroBlockDesc &micro_block_desc,
                 const ObMacroBlock &macro_block);
  int generate_macro_row(ObMacroBlock &macro_block, const MacroBlockId &id);
  int append_macro_block(const ObMacroBlockDesc &macro_desc);
  int close(const ObDatumRowkey &last_key,
            ObMacroBlocksWriteCtx *data_write_ctx);
  void reset();
  static int add_macro_block_meta(
      const ObDataMacroBlockMeta &macro_meta,
      ObIArray<ObDataMacroBlockMeta *> &macro_meta_list,
      ObIAllocator &allocator);
private:
  int append_index_micro_block(ObMacroBlock &macro_block, const MacroBlockId &block_id);
  int write_meta_block(
      ObMacroBlock &macro_block,
      const MacroBlockId &block_id,
      const ObIndexBlockRowDesc &macro_row_desc);
  int insert_and_update_index_tree(const ObDatumRow *index_row) override;
  int cal_macro_meta_block_size(const ObDatumRowkey &rowkey, int64_t &estimate_block_size);
  int append_next_row(const ObMicroBlockDesc &micro_block_desc, ObIndexBlockRowDesc &macro_row_desc);
private:
  ObDataStoreDesc *data_store_desc_;
  ObSSTableIndexBuilder *sstable_builder_;
  ObIAllocator *sstable_allocator_;
  ObDataStoreDesc leaf_store_desc_;
  ObMicroBlockBufferHelper micro_helper_;
  ObIndexBlockRowDesc macro_row_desc_;
  ObIndexMicroBlockDesc *root_micro_block_desc_;
  ObMacroMetasArray *macro_meta_list_;
  ObIMicroBlockWriter *meta_block_writer_;
  ObDatumRow meta_row_;
  ObDataMacroBlockMeta macro_meta_;
  ObArenaAllocator row_allocator_;
  int64_t data_blocks_cnt_;
  int64_t meta_block_offset_;
  int64_t meta_block_size_;
  int64_t estimate_leaf_block_size_;
  int64_t estimate_meta_block_size_;
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
  int close(const IndexMicroBlockDescList &roots, ObIndexTreeRootBlockDesc &block_desc);
  void reset();
  int build_single_macro_row_desc(const IndexMicroBlockDescList &roots);
private:
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
  ObArenaAllocator leaf_rowkey_allocator_;
};

class ObIndexBlockRebuilder final
{
public:
  ObIndexBlockRebuilder();
  ~ObIndexBlockRebuilder();
  int init(ObSSTableIndexBuilder &sstable_builder);
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
      int64_t &meta_block_offset,
      int64_t &meta_block_size);
  static int get_meta_block(
      const char *buf,
      const int64_t buf_size,
      common::ObIAllocator &allocator,
      ObSSTableMacroBlockHeader &header,
      ObMicroBlockData &meta_block);
private:
  bool is_inited_;
  lib::ObMutex mutex_;
  ObDataStoreDesc *index_store_desc_;
  ObMacroBlocksWriteCtx block_write_ctx_;
  ObIndexMicroBlockDesc *root_micro_block_desc_;
  ObMacroMetasArray *macro_meta_list_;
  ObIAllocator *sstable_allocator_;
  ObSSTableIndexBuilder *sstable_builder_;
  ObArenaAllocator allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObIndexBlockRebuilder);
};

class ObSSTableIndexBuilder final
{
public:
  enum ObSpaceOptimizationMode
  {
    // TODO zhouxinlan.zxl : delete the mode DISABLE
    ENABLE = 0,   // enable the optimization for small sstable with given occupy_size
    DISABLE = 1,  // disable the optimization
    AUTO = 2      // enable the optimization without giving occupy_size (users don't know/assign occupy_size)
  };
public:
  ObSSTableIndexBuilder();
  ~ObSSTableIndexBuilder();
  int init(
      const ObDataStoreDesc &index_desc,
      ObIMacroBlockFlushCallback *callback = nullptr,
      ObSpaceOptimizationMode mode = ENABLE);
  void reset();
  int new_index_builder(ObDataIndexBlockBuilder *&builder,
                        ObDataStoreDesc &data_store_desc,
                        ObIAllocator &data_allocator);
  int init_builder_ptrs(
      ObSSTableIndexBuilder *&sstable_builder,
      ObDataStoreDesc *&index_store_desc,
      ObIAllocator *&sstable_allocator,
      ObIndexMicroBlockDesc *&root_micro_block_desc,
      ObMacroMetasArray *&macro_meta_list);
  int append_root(ObIndexMicroBlockDesc &root_micro_block_desc);
  int close(ObSSTableMergeRes &res);
  const ObDataStoreDesc &get_index_store_desc() const { return index_store_desc_; }
  TO_STRING_KV(K(roots_.count()));
public:
  static bool check_version_for_small_sstable(const ObDataStoreDesc &index_desc);
  static int load_single_macro_block(
      const ObDataMacroBlockMeta &macro_meta,
      ObMacroBlockHandle &read_handle,
      ObSSTableMacroBlockHeader &macro_header);
private:
  int check_and_rewrite_sstable(ObSSTableMergeRes &res);
  int check_and_rewrite_sstable_without_size(ObSSTableMergeRes &res);
  int do_check_and_rewrite_sstable(ObBlockInfo &block_info);
  static int parse_macro_header(
      const char *buf,
      const int64_t buf_size,
      ObSSTableMacroBlockHeader &macro_header);
  int rewrite_small_sstable(ObSSTableMergeRes &res);
  bool check_single_block();
  int set_row_store_type(ObDataStoreDesc &index_desc);
  bool check_index_desc(const ObDataStoreDesc &index_desc) const;
  int trim_empty_roots();
  int sort_roots();
  int merge_index_tree(ObSSTableMergeRes &res);
  int build_meta_tree(ObSSTableMergeRes &res);
  int generate_macro_blocks_info(ObSSTableMergeRes &res);
  void release_index_block_desc(ObIndexMicroBlockDesc *&root);

  int accumulate_macro_column_checksum(
      const ObDataMacroBlockMeta &meta, ObSSTableMergeRes &res);
  void clean_status();
private:
  common::ObArenaAllocator orig_allocator_;
  common::ObSafeArenaAllocator allocator_; // to keep multi-thread safe
  common::ObArenaAllocator self_allocator_; // used to merge
  lib::ObMutex mutex_;
  ObDataStoreDesc index_store_desc_;
  ObDataStoreDesc container_store_desc_; // used to open all index macro writers
  MacroBlocksWriteCtxList index_write_ctxs_;
  ObDatumRow index_row_;
  ObIMicroBlockReader *micro_reader_;
  ObMicroBlockReaderHelper reader_helper_;
  ObBaseIndexBlockBuilder index_builder_;
  ObMetaIndexBlockBuilder data_builder_;
  ObMacroBlockWriter macro_writer_;
  ObIMacroBlockFlushCallback *callback_;
  IndexMicroBlockDescList roots_;
  ObSSTableMergeRes res_;
  ObSpaceOptimizationMode optimization_mode_;
  bool is_closed_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObSSTableIndexBuilder);
};

}//end namespace blocksstable
}//end namespace oceanbase
#endif
