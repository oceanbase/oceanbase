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

#ifndef OCEANBASE_STORAGE_OB_BLOCK_SAMPLE_ITERATOR_H
#define OCEANBASE_STORAGE_OB_BLOCK_SAMPLE_ITERATOR_H

#include "storage/ob_i_store.h"
#include "ob_i_sample_iterator.h"
#include "ob_multiple_scan_merge.h"
#include "storage/blocksstable/ob_index_block_tree_cursor.h"

namespace oceanbase
{
namespace storage
{

class ObBlockSampleSSTableEndkeyIterator final
{
public:
  ObBlockSampleSSTableEndkeyIterator();
  ~ObBlockSampleSSTableEndkeyIterator();
  void reset();
  int open(
      const blocksstable::ObSSTable &sstable,
      const blocksstable::ObDatumRange &range,
      const ObITableReadInfo &rowkey_read_info,
      ObIAllocator &allocator,
      const bool is_reverse_scan);
  int upgrade_to_macro(const blocksstable::ObDatumRange &range);
  int move_forward();
  OB_INLINE const blocksstable::ObDatumRowkey &get_endkey() const { return curr_key_;}
  OB_INLINE int64_t get_macro_count() const { return macro_count_; }
  OB_INLINE int64_t get_micro_count() const { return micro_count_; }
  OB_INLINE bool is_reach_end() const { return is_iter_end_; }
  TO_STRING_KV(K_(is_inited), K_(is_iter_end), K_(is_reverse_scan), K_(curr_block_id), K_(macro_count),
               K_(start_bound_micro_block), K_(end_bound_micro_block), K_(sample_level), K_(tree_cursor));

private:
  int locate_bound(const blocksstable::ObDatumRange &range);
  int locate_bound_micro_block(
      const blocksstable::ObDatumRowkey &rowkey,
      blocksstable::ObMicroBlockId &bound_block,
      bool &is_beyond_range);
  int get_current_block_id(blocksstable::ObMicroBlockId &micro_block_id);

private:
  int64_t macro_count_;
  int64_t micro_count_;
  blocksstable::ObDatumRowkey curr_key_;
  blocksstable::ObIndexBlockTreeCursor tree_cursor_;
  blocksstable::ObMicroBlockId start_bound_micro_block_;
  blocksstable::ObMicroBlockId end_bound_micro_block_;
  blocksstable::ObMicroBlockId curr_block_id_;
  blocksstable::ObIndexBlockTreeCursor::MoveDepth sample_level_;
  bool is_reverse_scan_;
  bool is_iter_end_;
  bool is_inited_;
};

class ObBlockSampleRangeIterator final
{
public:
  ObBlockSampleRangeIterator();
  ~ObBlockSampleRangeIterator();
  void reset();
  int open(
    ObGetTableParam &get_table_param,
    const blocksstable::ObDatumRange &range,
    ObIAllocator &allocator,
    const double percent,
    const bool is_reverse_scan);
  int get_next_range(const blocksstable::ObDatumRange *&range);
  TO_STRING_KV(K_(is_inited), K_(is_range_iter_end), K_(is_reverse_scan), K_(batch_size), K_(schema_rowkey_column_count),
      K_(curr_key), K_(prev_key), K_(curr_range), KP_(allocator), KP_(sample_range), KP_(datum_utils),
      K_(endkey_comparor), K_(endkey_heap), K_(endkey_iters));

private:
  struct ObBlockSampleSSTableEndkeyComparor
  {
    ObBlockSampleSSTableEndkeyComparor ()
      : datum_utils_(nullptr),
        ret_(OB_SUCCESS),
        is_reverse_scan_(false)
    {}
    ~ObBlockSampleSSTableEndkeyComparor () = default;
    OB_INLINE void reset() { ret_ = OB_SUCCESS; }
    OB_INLINE int get_error_code() const { return ret_; }
    void init(const blocksstable::ObStorageDatumUtils &utils, const bool is_reverse_scan);
    bool operator()(const ObBlockSampleSSTableEndkeyIterator * left, const ObBlockSampleSSTableEndkeyIterator * right);
    TO_STRING_KV(K_(datum_utils), K_(ret), K_(is_reverse_scan));
    const blocksstable::ObStorageDatumUtils *datum_utils_;
    int ret_;
    bool is_reverse_scan_;
  };
  struct ObBlockSampleEndkey
  {
    ObBlockSampleEndkey ()
      : key_buf_(nullptr),
        buf_size_(0),
        key_()
    {}
    ~ObBlockSampleEndkey () = default;
    void reset();
    TO_STRING_KV(K_(key), K_(buf_size));
    char *key_buf_;
    int64_t buf_size_;
    blocksstable::ObDatumRowkey key_;
  };

private:
  int init_and_push_endkey_iterator(ObGetTableParam &get_table_param);
  int calculate_level_and_batch_size(const double percent);
  int get_next_batch(ObBlockSampleSSTableEndkeyIterator *&iter);
  void generate_cur_range(const blocksstable::ObDatumRowkey &curr_bound_key);
  int deep_copy_rowkey(const blocksstable::ObDatumRowkey &src_key, ObBlockSampleEndkey &dest);
  int move_endkey_iter(ObBlockSampleSSTableEndkeyIterator &iter);

private:
  static const int64_t DEFAULT_SSTABLE_CNT = 9;
  static const int64_t EXPECTED_MIN_MACRO_SAMPLE_BLOCK_COUNT = 2;
  static const int64_t EXPECTED_OPEN_RANGE_NUM = 8;
  const blocksstable::ObDatumRange *sample_range_;
  ObIAllocator *allocator_;
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  int64_t schema_rowkey_column_count_;
  int64_t batch_size_;
  ObBlockSampleEndkey curr_key_;
  ObBlockSampleEndkey prev_key_;
  blocksstable::ObDatumRange curr_range_;
  ObSEArray<ObBlockSampleSSTableEndkeyIterator *, DEFAULT_SSTABLE_CNT> endkey_iters_;
  ObBlockSampleSSTableEndkeyComparor endkey_comparor_;
  common::ObBinaryHeap<ObBlockSampleSSTableEndkeyIterator *, ObBlockSampleSSTableEndkeyComparor> endkey_heap_;
  bool is_range_iter_end_;
  bool is_reverse_scan_;
  bool is_inited_;
};

class ObBlockSampleIterator : public ObISampleIterator
{
public:
  explicit ObBlockSampleIterator(const common::SampleInfo &sample_info);
  virtual ~ObBlockSampleIterator();
  virtual void reuse();
  virtual void reset() override;
  int open(ObMultipleScanMerge &scan_merge,
           ObTableAccessContext &access_ctx,
           const blocksstable::ObDatumRange &range,
           ObGetTableParam &get_table_param,
           const bool is_reverse_scan);
  virtual int get_next_row(blocksstable::ObDatumRow *&row) override;
private:
  int open_range(blocksstable::ObDatumRange &range);
private:
  ObTableAccessContext *access_ctx_;
  const ObITableReadInfo *read_info_;
  ObMultipleScanMerge *scan_merge_;
  int64_t block_num_;
  common::ObArenaAllocator range_allocator_;
  ObBlockSampleRangeIterator range_iterator_;
  blocksstable::ObDatumRange micro_range_;
  bool has_opened_range_;
};


}
}

#endif /* OCEANBASE_STORAGE_OB_BLOCK_SAMPLE_ITERATOR_H */
