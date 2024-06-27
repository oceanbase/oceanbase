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

#ifndef OB_INDEX_BLOCK_ROW_SCANNER_H_
#define OB_INDEX_BLOCK_ROW_SCANNER_H_

#include "storage/ob_i_store.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_micro_block_reader_helper.h"
#include "storage/blocksstable/ob_datum_range.h"
#include "storage/blocksstable/index_block/ob_ddl_sstable_scan_merge.h"
#include "storage/blocksstable/ob_datum_rowkey_vector.h"
#include "storage/column_store/ob_column_store_util.h"
#include "ob_index_block_row_struct.h"
#include "storage/memtable/mvcc/ob_keybtree.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/access/ob_simple_rows_merger.h"
#include "share/cache/ob_kvcache_pointer_swizzle.h"
namespace oceanbase
{
namespace storage
{
struct ObTableIterParam;
struct ObTableAccessContext;
class ObBlockMetaTree;
class ObBlockMetaTreeValue;
class ObRowsInfo;
class ObRowKeysInfo;
}
namespace blocksstable
{
class ObSSTable;
class ObDDLIndexBlockRowIterator;
class ObDDLMergeBlockRowIterator;
// Memory structure of Index micro block.
// This struct won't hold extra memory, lifetime security need to be ensured by caller
struct ObIndexBlockDataHeader
{
  OB_INLINE bool is_valid() const
  {
    return row_cnt_ >= 0
        && col_cnt_ > 0
        && nullptr != rowkey_vector_
        && nullptr != index_datum_array_
        && nullptr != ps_node_array_;
  }
  int get_index_data(const int64_t row_idx, const char *&index_ptr, int64_t &index_len) const;

  int deep_copy_transformed_index_block(
      const ObIndexBlockDataHeader &header,
      const int64_t buf_size,
      char *buf,
      int64_t &pos);

  int64_t row_cnt_;
  int64_t col_cnt_;
  // Vectors of rowkeys
  const ObRowkeyVector *rowkey_vector_;
  // Array of index Object
  ObStorageDatum *index_datum_array_;
  ObPointerSwizzleNode *ps_node_array_;
  const char *data_buf_;
  int64_t data_buf_size_;

  TO_STRING_KV(
      K_(row_cnt), K_(col_cnt),
      KPC_(rowkey_vector),
      KP_(index_datum_array),
      KP_(ps_node_array), KP_(data_buf), K_(data_buf_size)
      );
};

class ObIndexBlockDataTransformer
{
public:
  ObIndexBlockDataTransformer();
  virtual ~ObIndexBlockDataTransformer();
  int transform(
      const ObMicroBlockData &raw_data,
      ObMicroBlockData &transformed_data,
      ObIAllocator &allocator,
      char *&allocated_buf,
      const ObIArray<share::schema::ObColDesc> *col_descs = nullptr);

  // For micro header bug in version before 4.3, when root block serialized in sstable meta,
  // data length related fileds was lefted to be filled
int fix_micro_header_and_transform(
    const ObMicroBlockData &raw_data,
    ObMicroBlockData &transformed_data,
    ObIAllocator &allocator,
    char *&allocated_buf);
  static int get_transformed_upper_mem_size(const ObIArray<share::schema::ObColDesc> *rowkey_col_descs, const char *raw_block_data, int64_t &mem_limit);
private:
  int get_reader(const ObRowStoreType store_type, ObIMicroBlockReader *&micro_reader);
private:
  ObArenaAllocator allocator_;
  ObMicroBlockReaderHelper micro_reader_helper_;
};

enum class ObIndexFormat {
  INVALID = 0,
  RAW_DATA,
  TRANSFORMED,
  BLOCK_TREE,
  DDL_MERGE
};

class ObIndexBlockIterParam final
{
public:
  ObIndexBlockIterParam();
  ObIndexBlockIterParam(const ObSSTable *sstable, const ObTablet *tablet);
  ~ObIndexBlockIterParam();
  ObIndexBlockIterParam &operator=(const ObIndexBlockIterParam &other);
  int assign(const ObIndexBlockIterParam &other);
  void reset();
  bool is_valid() const;
  TO_STRING_KV(KP(sstable_), KP(tablet_));

public:
  const ObSSTable *sstable_;
  const ObTablet *tablet_;
};

class ObIndexBlockRowIterator
{
public:
  ObIndexBlockRowIterator();
  virtual ~ObIndexBlockRowIterator();
  virtual void reset();
  virtual void reuse() = 0;
  virtual int init(const ObMicroBlockData &idx_block_data,
                   const ObStorageDatumUtils *datum_utils,
                   ObIAllocator *allocator,
                   const bool is_reverse_scan,
                   const ObIndexBlockIterParam &iter_param) = 0;
  virtual int get_current(const ObIndexBlockRowHeader *&idx_row_header,
                          ObCommonDatumRowkey &endkey) = 0;
  virtual int get_next(const ObIndexBlockRowHeader *&idx_row_header,
                       ObCommonDatumRowkey &endkey,
                       bool &is_scan_left_border,
                       bool &is_scan_right_border,
                       const ObIndexBlockRowMinorMetaInfo *&idx_minor_info,
                       const char *&agg_row_buf,
                       int64_t &agg_buf_size,
                       int64_t &row_offset) = 0;
  virtual int locate_key(const ObDatumRowkey &rowkey) = 0;
  virtual int locate_range(const ObDatumRange &range,
                           const bool is_left_border,
                           const bool is_right_border,
                           const bool is_normal_cg) = 0;
  virtual int locate_range() { return OB_NOT_SUPPORTED; }
  virtual int check_blockscan(const ObDatumRowkey &rowkey, bool &can_blockscan) = 0;
  virtual bool end_of_block() const = 0;
  virtual int get_index_row_count(const ObDatumRange &range,
                                  const bool is_left_border,
                                  const bool is_right_border,
                                  int64_t &index_row_count,
                                  int64_t &data_row_count) = 0;
  //todo @hanling :refactor these OB_NOT_SUPPORTED interface
  virtual int get_idx_row_header_in_target_idx(const int64_t idx,
                                               const ObIndexBlockRowHeader *&idx_row_header) { return OB_NOT_SUPPORTED; }
  virtual int find_out_rows(const int32_t range_idx,
                            const int64_t scanner_range_idx,
                            int64_t &found_idx) { return OB_NOT_SUPPORTED; }
  virtual int find_out_rows_from_start_to_end(const int32_t range_idx,
                                              const int64_t scanner_range_idx,
                                              const ObCSRowId start_row_id,
                                              const ObCSRange &parent_row_range,
                                              bool &is_certain,
                                              int64_t &found_idx) { return OB_NOT_SUPPORTED; }
  virtual int skip_to_next_valid_position(const ObDatumRowkey &rowkey) { return OB_NOT_SUPPORTED; }
  virtual int find_rowkeys_belong_to_same_idx_row(ObMicroIndexInfo &idx_block_row, int64_t &rowkey_begin_idx, int64_t &rowkey_end_idx, const ObRowsInfo *&rows_info) { return OB_NOT_SUPPORTED; }
  virtual int find_rowkeys_belong_to_curr_idx_row(ObMicroIndexInfo &idx_block_row, const int64_t rowkey_end_idx, const ObRowKeysInfo *rowkeys_info) { return OB_NOT_SUPPORTED; }
  virtual int advance_to_border(const ObDatumRowkey &rowkey,
                                const bool is_left_border,
                                const bool is_right_border,
                                const ObCSRange &parent_row_range,
                                ObCSRange &cs_range) { return OB_NOT_SUPPORTED; }
  virtual int get_end_key(ObCommonDatumRowkey &endkey) { return OB_NOT_SUPPORTED; }
  virtual void set_iter_end() {}
  virtual ObPointerSwizzleNode* get_cur_ps_node() { return nullptr; }
  virtual int64_t get_cur_ps_node_index() { return 0; }
public:
  virtual int switch_context(ObStorageDatumUtils *datum_utils)
  {
    datum_utils_ = datum_utils;
    return OB_SUCCESS;
  }
  bool is_inited() { return is_inited_; }
  VIRTUAL_TO_STRING_KV(K(is_inited_), K(is_reverse_scan_), K(iter_step_), KPC(datum_utils_));

protected:
  bool is_inited_;
  bool is_reverse_scan_;
  int64_t iter_step_;
  ObIndexBlockRowParser idx_row_parser_;
  const ObStorageDatumUtils *datum_utils_;
};


class ObRAWIndexBlockRowIterator : public ObIndexBlockRowIterator
{
public:
  ObRAWIndexBlockRowIterator();
  virtual ~ObRAWIndexBlockRowIterator();
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
  virtual void set_iter_end() override { current_ = ObIMicroBlockReader::INVALID_ROW_INDEX; }
  virtual int check_blockscan(const ObDatumRowkey &rowkey, bool &can_blockscan) override;
  virtual bool end_of_block() const override;
  virtual int get_index_row_count(const ObDatumRange &range,
                                  const bool is_left_border,
                                  const bool is_right_border,
                                  int64_t &index_row_count,
                                  int64_t &data_row_count) override;
  virtual void reset() override;
  virtual void reuse() override;
  INHERIT_TO_STRING_KV("base iterator:", ObIndexBlockRowIterator, "format:", "ObRAWIndexBlockRowIterator",
                       K(current_), K(start_), K(end_), KP(micro_reader_), K(endkey_), KP_(datum_row), KP_(allocator));
private:
  int init_datum_row(const ObStorageDatumUtils &datum_utils, ObIAllocator *allocator);
  bool is_in_border(bool is_reverse_scan, bool is_left_border, bool is_right_border);
  int compare_rowkey(const ObDatumRowkey &rowkey, int32_t &cmp_ret);
protected:
  int64_t current_;
  int64_t start_;               // inclusive
  int64_t end_;                 // inclusive
  ObIMicroBlockReader *micro_reader_;
  ObIAllocator *allocator_;
  ObDatumRow *datum_row_;
  ObMicroBlockReaderHelper micro_reader_helper_;
  ObDatumRowkey endkey_;
};

class ObTFMIndexBlockRowIterator : public ObRAWIndexBlockRowIterator
{
public:
  ObTFMIndexBlockRowIterator();
  virtual ~ObTFMIndexBlockRowIterator();
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
  virtual int check_blockscan(const ObDatumRowkey &rowkey, bool &can_blockscan) override;
  virtual void reset() override;
  virtual void reuse() override;
  virtual int find_out_rows(const int32_t range_idx,
                            const int64_t scanner_range_idx,
                            int64_t &found_idx) override;
  virtual int find_out_rows_from_start_to_end(const int32_t range_idx,
                                              const int64_t scanner_range_idx,
                                              const ObCSRowId start_row_id,
                                              const ObCSRange &parent_row_range,
                                              bool &is_certain,
                                              int64_t &found_idx) override;
  virtual int skip_to_next_valid_position(const ObDatumRowkey &rowkey) override;
  virtual int find_rowkeys_belong_to_same_idx_row(ObMicroIndexInfo &idx_block_row, int64_t &rowkey_begin_idx, int64_t &rowkey_end_idx, const ObRowsInfo *&rows_info) override;
  virtual int find_rowkeys_belong_to_curr_idx_row(ObMicroIndexInfo &idx_block_row, const int64_t rowkey_end_idx, const ObRowKeysInfo *rowkeys_info) override;
  virtual int get_idx_row_header_in_target_idx(const int64_t idx,
                                               const ObIndexBlockRowHeader *&idx_row_header) override;
  virtual int advance_to_border(const ObDatumRowkey &rowkey,
                                const bool is_left_border,
                                const bool is_right_border,
                                const ObCSRange &parent_row_range,
                                ObCSRange &cs_range) override;
  virtual int get_end_key(ObCommonDatumRowkey &endkey) override;
  virtual int64_t get_cur_ps_node_index() { return cur_node_index_; }
  virtual ObPointerSwizzleNode* get_cur_ps_node() {
    return idx_data_header_->ps_node_array_ + cur_node_index_;
  }
  INHERIT_TO_STRING_KV("base iterator:", ObRAWIndexBlockRowIterator, "format:", "ObTFMIndexBlockRowIterator", KPC(idx_data_header_));

private:
  int get_cur_row_id_range(const ObCSRange &parent_row_range,
                           ObCSRange &cs_range);
  int locate_range_by_rowkey_vector(
      const ObDatumRange &range,
      const bool is_left_border,
      const bool is_right_border,
      const bool is_normal_cg,
      int64_t &begin_idx,
      int64_t &end_idx);
  int advance_to_border_by_rowkey_vector(const ObDatumRowkey &rowkey,
                                         const bool is_left_border,
                                         const bool is_right_border,
                                         const ObCSRange &parent_row_range,
                                         ObCSRange &cs_range);

private:
  const ObIndexBlockDataHeader *idx_data_header_;
  int64_t cur_node_index_;
};

class ObIndexBlockRowScanner
{
public:
  ObIndexBlockRowScanner();
  virtual ~ObIndexBlockRowScanner();
  void reuse();
  void reset();

  int init(
      const ObStorageDatumUtils &datum_utils,
      ObIAllocator &allocator,
      const common::ObQueryFlag &query_flag,
      const int64_t nested_offset,
      const bool is_normal_cg = false,
      const ObIArray<share::schema::ObColDesc> *rowkey_col_descs = nullptr);
  // todo :qilu get ls_id from MTL() after ddl_kv_mgr split to tenant
  int open(
      const MacroBlockId &macro_id,
      const ObMicroBlockData &idx_block_data,
      const ObDatumRowkey &rowkey,
      const int64_t range_idx = 0,
      const ObMicroIndexInfo *idx_info = nullptr);
  int open(
      const MacroBlockId &macro_id,
      const ObMicroBlockData &idx_block_data,
      const ObDatumRange &range,
      const int64_t range_idx,
      const bool is_left_border,
      const bool is_right_border,
      const ObMicroIndexInfo *idx_info = nullptr);
  int open(
      const MacroBlockId &macro_id,
      const ObMicroBlockData &idx_block_data,
      const ObRowsInfo *rows_info,
      const int64_t rowkey_begin_idx,
      const int64_t rowkey_end_idx);
   int open(
      const MacroBlockId &macro_id,
      const ObMicroBlockData &idx_block_data,
      const ObRowKeysInfo *row_keys_info,
      const int64_t rowkey_begin_idx,
      const int64_t rowkey_end_idx);
  int get_next(
      ObMicroIndexInfo &idx_block_row,
      const bool is_multi_check = false,
      const bool is_sorted_multi_get = false);
  void set_iter_param(const ObSSTable *sstable,
                      const ObTablet *tablet);
  bool end_of_block() const;
  bool is_ddl_merge_type() const;
  int get_index_row_count(int64_t &index_row_count) const;
  int check_blockscan(const ObDatumRowkey &rowkey, bool &can_blockscan);
  int locate_range(const ObDatumRange &range, const bool is_left_border, const bool is_right_border);
  int advance_to_border(
      const ObDatumRowkey &rowkey,
      const int32_t range_idx,
      ObCSRange &cs_range);
  int find_out_rows(
      const int32_t range_idx,
      int64_t &found_idx);
  int find_out_rows_from_start_to_end(
      const int32_t range_idx,
      const ObCSRowId start_row_id,
      bool &is_certain,
      int64_t &found_idx);
  bool is_in_border();
  int get_end_key(ObCommonDatumRowkey &endkey) const;
  OB_INLINE bool is_valid() const { return is_inited_; }
  OB_INLINE bool is_ddl_merge_scan() const { return index_format_ == ObIndexFormat::DDL_MERGE; }
  void switch_context(const ObSSTable &sstable,
                      const ObTablet *tablet,
                      const ObStorageDatumUtils &datum_utils,
                      ObTableAccessContext &access_ctx,
                      const ObIArray<share::schema::ObColDesc> *rowkey_col_descs = nullptr);
  TO_STRING_KV(K_(index_format), KP_(raw_iter), KP_(transformed_iter), KP_(ddl_iter), KP_(ddl_merge_iter),
               KPC_(iter), K_(range_idx), K_(is_get), K_(is_reverse_scan), K_(is_left_border), K_(is_right_border),
               K_(rowkey_begin_idx), K_(rowkey_end_idx), K_(is_inited), K_(macro_id), KPC_(datum_utils),
               K_(is_normal_cg), K_(parent_row_range), K_(filter_constant_type), K_(is_normal_query),
               K_(iter_param), KP_(rowkey_col_descs));
private:
  int init_by_micro_data(const ObMicroBlockData &idx_block_data);
  int locate_key(const ObDatumRowkey &rowkey);
  int init_datum_row();
  int get_cur_row_id_range(ObCSRange &cs_range);
  int get_idx_row_header_in_target_idx(
      const ObIndexBlockRowHeader *&idx_row_header,
      const int64_t idx);
  int advance_to_border(
      const ObDatumRowkey &rowkey,
      const int64_t limit_idx,
      ObCSRange &cs_range);
  int get_next_idx_row(ObMicroIndexInfo &idx_block_row);
  void skip_index_rows();
  int skip_to_next_valid_position(ObMicroIndexInfo &idx_block_row);
private:
  union {
    const ObDatumRowkey *rowkey_;
    const ObDatumRange *range_;
    const ObRowsInfo *rows_info_;
    const ObRowKeysInfo *rowkeys_info_;
    const void *query_range_;
  };
  MacroBlockId macro_id_;
  ObIAllocator *allocator_;
  ObRAWIndexBlockRowIterator *raw_iter_;
  ObTFMIndexBlockRowIterator *transformed_iter_;
  ObDDLIndexBlockRowIterator *ddl_iter_;
  ObDDLMergeBlockRowIterator *ddl_merge_iter_;
  ObIndexBlockRowIterator *iter_; //point to one of above four iter
  const ObStorageDatumUtils *datum_utils_;
  int64_t range_idx_;
  int64_t nested_offset_;
  int64_t rowkey_begin_idx_;
  int64_t rowkey_end_idx_;
  ObIndexFormat index_format_;
  ObCSRange parent_row_range_;
  bool is_get_;
  bool is_reverse_scan_;
  bool is_left_border_;
  bool is_right_border_;
  bool is_inited_;
  bool is_normal_cg_;
  bool is_normal_query_;
  sql::ObBoolMaskType filter_constant_type_;
  ObIndexBlockIterParam iter_param_; // todo qilu: refactor this after refactor ddl_kv_mgr
  const ObIArray<share::schema::ObColDesc> *rowkey_col_descs_;
};

} // namespace blocksstable
} // namespace oceanbase

#endif
