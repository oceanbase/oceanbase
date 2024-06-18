/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_DDL_INDEX_BLOCK_ROW_ITERATOR_H
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_DDL_INDEX_BLOCK_ROW_ITERATOR_H

#include "storage/blocksstable/index_block/ob_index_block_row_scanner.h"
#include "storage/blocksstable/index_block/ob_index_block_macro_iterator.h"

namespace oceanbase
{

namespace storage
{
class ObDDLMemtable;
}
namespace blocksstable
{
typedef keybtree::BtreeIterator<blocksstable::ObDatumRowkeyWrapper, storage::ObBlockMetaTreeValue *> DDLBtreeIterator;
class ObDDLIndexBlockRowIterator : public ObIndexBlockRowIterator
{
public:
  ObDDLIndexBlockRowIterator();
  virtual ~ObDDLIndexBlockRowIterator();
  virtual int init(const ObMicroBlockData &idx_block_data,
                   const ObStorageDatumUtils *datum_utils,
                   ObIAllocator *allocator,
                   const bool is_reverse_scan,
                   const ObIndexBlockIterParam &iter_param) override;
  virtual int get_current(const ObIndexBlockRowHeader *&idx_row_header,
                          ObCommonDatumRowkey &endkey) override;
  virtual int get_next(const ObIndexBlockRowHeader *&idx_row_header,
                       ObCommonDatumRowkey &endkey,
                       bool &is_scan_left_border,
                       bool &is_scan_right_border,
                       const ObIndexBlockRowMinorMetaInfo *&idx_minor_info,
                       const char *&agg_row_buf,
                       int64_t &agg_buf_size,
                       int64_t &row_offset) override;
  virtual int locate_key(const ObDatumRowkey &rowkey) override;
  virtual int locate_range(const ObDatumRange &range,
                           const bool is_left_border,
                           const bool is_right_border,
                           const bool is_normal_cg) override;
  virtual int locate_range() override;
  virtual int skip_to_next_valid_position(const ObDatumRowkey &rowkey) override;
  virtual int find_rowkeys_belong_to_same_idx_row(ObMicroIndexInfo &idx_block_row, int64_t &rowkey_begin_idx, int64_t &rowkey_end_idx, const ObRowsInfo *&rows_info) override;
  virtual int check_blockscan(const ObDatumRowkey &rowkey, bool &can_blockscan) override;
  virtual bool end_of_block() const override;
  virtual int get_index_row_count(const ObDatumRange &range,
                                  const bool is_left_border,
                                  const bool is_right_border,
                                  int64_t &index_row_count,
                                  int64_t &data_row_count) override;
  virtual void reset() override;
  virtual void reuse() override;
  virtual void set_iter_end() override { is_iter_finish_ = true; }
  INHERIT_TO_STRING_KV("base iterator:", ObIndexBlockRowIterator, "format:", "ObDDLIndexBlockRowIterator",
                       K_(is_iter_start), K_(is_iter_finish), KP(cur_tree_value_), KP(block_meta_tree_), K(is_co_sstable_));
public:
  int set_iter_param(const ObStorageDatumUtils *datum_utils,
                     bool is_reverse_scan,
                     const storage::ObBlockMetaTree *block_meta_tree,
                     const bool is_co_sstable,
                     const int64_t iter_step = INT64_MAX);
  bool is_valid() { return OB_NOT_NULL(block_meta_tree_); }
  int get_next_meta(const ObDataMacroBlockMeta *&meta);
private:
  int inner_get_current(const ObIndexBlockRowHeader *&idx_row_header,
                        ObCommonDatumRowkey &endkey,
                        int64_t &row_offset/*for co sstable*/);
private:
  bool is_iter_start_;
  bool is_iter_finish_;
  bool is_co_sstable_;
  DDLBtreeIterator btree_iter_;
  const storage::ObBlockMetaTree *block_meta_tree_;
  storage::ObBlockMetaTreeValue *cur_tree_value_;
};

// for ddl_merge_sstable with index_tree_height > 2
class ObDDLSStableAllRangeIterator : public ObIndexBlockRowIterator
{
public:
  ObDDLSStableAllRangeIterator();
  virtual ~ObDDLSStableAllRangeIterator();
  virtual int init(const ObMicroBlockData &idx_block_data,
                   const ObStorageDatumUtils *datum_utils,
                   ObIAllocator *allocator,
                   const bool is_reverse_scan,
                   const ObIndexBlockIterParam &iter_param) override;
  virtual int get_current(const ObIndexBlockRowHeader *&idx_row_header,
                          ObCommonDatumRowkey &endkey) override;
  virtual int get_next(const ObIndexBlockRowHeader *&idx_row_header,
                       ObCommonDatumRowkey &endkey,
                       bool &is_scan_left_border,
                       bool &is_scan_right_border,
                       const ObIndexBlockRowMinorMetaInfo *&idx_minor_info,
                       const char *&agg_row_buf,
                       int64_t &agg_buf_size,
                       int64_t &row_offset) override;
  virtual int locate_key(const ObDatumRowkey &rowkey) override;
  virtual int locate_range(const ObDatumRange &range,
                           const bool is_left_border,
                           const bool is_right_border,
                           const bool is_normal_cg) override;
  virtual int check_blockscan(const ObDatumRowkey &rowkey, bool &can_blockscan) override;
  virtual bool end_of_block() const override;
  virtual int get_index_row_count(const ObDatumRange &range,
                                  const bool is_left_border,
                                  const bool is_right_border,
                                  int64_t &index_row_count,
                                  int64_t &data_row_count) override;
  virtual void reuse() override;
  virtual void reset() override;
  INHERIT_TO_STRING_KV("base iterator:", ObIndexBlockRowIterator, "format:", "ObDDLSStableAllRangeIterator", K(is_iter_start_), K(is_iter_finish_),
      KPC(rowkey_read_info_), K(index_macro_iter_), K(iter_param_), K(cur_index_info_));

private:
  bool is_iter_start_;
  bool is_iter_finish_;
  const ObITableReadInfo *rowkey_read_info_;
  ObIndexBlockMacroIterator index_macro_iter_;
  ObIndexBlockIterParam iter_param_;
  ObMicroIndexRowItem cur_index_info_;
  ObArenaAllocator macro_iter_allocator_;
  ObArenaAllocator idx_row_allocator_;
};

// for empty ddl_merge_sstable
class ObDDLMergeEmptyIterator : public ObIndexBlockRowIterator
{
public:
  ObDDLMergeEmptyIterator();
  virtual ~ObDDLMergeEmptyIterator();
  virtual int init(const ObMicroBlockData &idx_block_data,
                   const ObStorageDatumUtils *datum_utils,
                   ObIAllocator *allocator,
                   const bool is_reverse_scan,
                   const ObIndexBlockIterParam &iter_param) override;
  virtual int get_current(const ObIndexBlockRowHeader *&idx_row_header,
                          ObCommonDatumRowkey &endkey) override;
  virtual int get_next(const ObIndexBlockRowHeader *&idx_row_header,
                       ObCommonDatumRowkey &endkey,
                       bool &is_scan_left_border,
                       bool &is_scan_right_border,
                       const ObIndexBlockRowMinorMetaInfo *&idx_minor_info,
                       const char *&agg_row_buf,
                       int64_t &agg_buf_size,
                       int64_t &row_offset) override;
  virtual int locate_key(const ObDatumRowkey &rowkey) override;
  virtual int locate_range(const ObDatumRange &range,
                           const bool is_left_border,
                           const bool is_right_border,
                           const bool is_normal_cg) override;
  virtual int check_blockscan(const ObDatumRowkey &rowkey, bool &can_blockscan) override;
  virtual bool end_of_block() const override;
  virtual int get_index_row_count(const ObDatumRange &range,
                                  const bool is_left_border,
                                  const bool is_right_border,
                                  int64_t &index_row_count,
                                  int64_t &data_row_count) override;
  virtual void reuse() override;
  INHERIT_TO_STRING_KV("base iterator:", ObIndexBlockRowIterator, "format:", "ObDDLMergeEmptyIterator");
};

class ObDDLMergeBlockRowIterator : public ObIndexBlockRowIterator
{
public:
  static const int64_t MAX_SSTABLE_COUNT = 4096;
  typedef ObSimpleRowsMerger<ObDDLSSTableMergeLoserTreeItem, ObDDLSSTableMergeLoserTreeCompare> SimpleMerger;
  typedef common::ObLoserTree<ObDDLSSTableMergeLoserTreeItem, ObDDLSSTableMergeLoserTreeCompare, MAX_SSTABLE_COUNT> MergeLoserTree;
  ObDDLMergeBlockRowIterator();
  virtual ~ObDDLMergeBlockRowIterator();
  virtual int init(const ObMicroBlockData &idx_block_data,
                   const ObStorageDatumUtils *datum_utils,
                   ObIAllocator *allocator,
                   const bool is_reverse_scan,
                   const ObIndexBlockIterParam &iter_param) override;
  virtual int get_current(const ObIndexBlockRowHeader *&idx_row_header,
                          ObCommonDatumRowkey &endkey) override;
  virtual int get_next(const ObIndexBlockRowHeader *&idx_row_header,
                       ObCommonDatumRowkey &endkey,
                       bool &is_scan_left_border,
                       bool &is_scan_right_border,
                       const ObIndexBlockRowMinorMetaInfo *&idx_minor_info,
                       const char *&agg_row_buf,
                       int64_t &agg_buf_size,
                       int64_t &row_offset) override;
  virtual int locate_key(const ObDatumRowkey &rowkey) override;
  virtual int locate_range(const ObDatumRange &range,
                           const bool is_left_border,
                           const bool is_right_border,
                           const bool is_normal_cg) override;
  virtual int check_blockscan(const ObDatumRowkey &rowkey, bool &can_blockscan) override;
  virtual bool end_of_block() const override;
  virtual int get_index_row_count(const ObDatumRange &range,
                                  const bool is_left_border,
                                  const bool is_right_border,
                                  int64_t &index_row_count,
                                  int64_t &data_row_count) override;
  virtual void reset() override;
  virtual void reuse() override;
  virtual int switch_context(ObStorageDatumUtils *datum_utils) override;
  INHERIT_TO_STRING_KV("base iterator:", ObIndexBlockRowIterator, "format:", "ObDDLMergeBlockRowIterator",
                       KP(raw_iter_), KP(transformed_iter_), KP(empty_merge_iter_), KP(all_range_iter_), K(iters_), KP(allocator_), KP(consumers_),
                       K(consumer_cnt_), K(compare_), KPC(simple_merge_), KPC(loser_tree_), KPC(endkey_merger_), K(is_single_sstable_),
                       K(is_iter_start_), K(is_iter_finish_), K(query_range_), KP(idx_block_data_), K(first_index_item_), K(iter_param_));
  struct MergeIndexItem final
  {
  public:
    MergeIndexItem() : is_scan_left_border_(false), is_scan_right_border_(false),
                       idx_row_header_(nullptr), rowkey_(nullptr), idx_minor_info_(nullptr), agg_row_buf_(nullptr),
                       item_allocator_(nullptr), agg_buf_size_(0), row_offset_(0), iter_index_(INT64_MAX) {}
    ~MergeIndexItem()
    {
      reset();
    }
    int init(ObIAllocator *allocator,
             const ObIndexBlockRowHeader *idx_row_header,
             const ObCommonDatumRowkey &endkey,
             const bool is_scan_left_border,
             const bool is_scan_right_border,
             const ObIndexBlockRowMinorMetaInfo *idx_minor_info,
             const char *agg_row_buf,
             const int64_t agg_buf_size,
             const int64_t row_offset,
             const int64_t iter_idx);
    void reset();
    bool is_valid();
    TO_STRING_KV(K(is_scan_left_border_), K(is_scan_right_border_), K(agg_buf_size_), K(row_offset_), K(iter_index_),
                 KP(idx_minor_info_), KP(agg_row_buf_), KPC_(idx_row_header), KPC_(rowkey), KP(item_allocator_));

  public:
    bool is_scan_left_border_;
    bool is_scan_right_border_;
    ObIndexBlockRowHeader *idx_row_header_;
    blocksstable::ObDatumRowkey *rowkey_;
    ObIndexBlockRowMinorMetaInfo *idx_minor_info_;
    char *agg_row_buf_;
    ObIAllocator *item_allocator_;
    int64_t agg_buf_size_;
    int64_t row_offset_;
    int64_t iter_index_;
  };

private:
  int locate_first_endkey(); //for reverse scan
  int get_readable_ddl_kvs(const ObIndexBlockIterParam &iter_param,
                           ObArray<storage::ObDDLMemtable *> &ddl_memtables);
  int init_sstable_index_iter(const ObMicroBlockData &idx_block_data,
                              const ObStorageDatumUtils *datum_utils,
                              ObIAllocator *allocator,
                              const bool is_reverse_scan,
                              const ObIndexBlockIterParam &iter_param,
                              ObIndexBlockRowIterator *&sst_index_iter);
  int init_ddl_kv_index_iters(const ObMicroBlockData &idx_block_data,
                              const ObStorageDatumUtils *datum_utils,
                              ObIAllocator *allocator,
                              const bool is_reverse_scan,
                              const ObIndexBlockIterParam &iter_param);
  int init_merger();
  int inner_get_next(const ObIndexBlockRowHeader *&idx_row_header,
                     ObCommonDatumRowkey &endkey,
                     bool &is_scan_left_border,
                     bool &is_scan_right_border,
                     const ObIndexBlockRowMinorMetaInfo *&idx_minor_info,
                     const char *&agg_row_buf,
                     int64_t &agg_buf_size,
                     int64_t &row_offset);
  int supply_consume();
private:
  bool is_single_sstable_;
  bool is_iter_start_;
  bool is_iter_finish_;
  ObIAllocator *allocator_;
  const ObMicroBlockData *idx_block_data_;
  ObRAWIndexBlockRowIterator *raw_iter_;
  ObTFMIndexBlockRowIterator *transformed_iter_;
  ObDDLMergeEmptyIterator *empty_merge_iter_;
  ObDDLSStableAllRangeIterator *all_range_iter_;
  ObArray<ObIndexBlockRowIterator *> iters_;
  int64_t *consumers_;
  int64_t consumer_cnt_;
  ObDDLSSTableMergeLoserTreeCompare compare_;
  SimpleMerger *simple_merge_;
  MergeLoserTree *loser_tree_;
  common::ObRowsMerger<ObDDLSSTableMergeLoserTreeItem, ObDDLSSTableMergeLoserTreeCompare> *endkey_merger_; //point to one of above two iters
  ObDatumRange query_range_;
  MergeIndexItem first_index_item_;
  ObIndexBlockIterParam iter_param_;
};

} // end namespace blocksstable
} // end namespace oceanbase
#endif
