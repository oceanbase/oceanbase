// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#ifndef OCEANBASE_STORAGE_COLUMN_STORE_OB_CG_PREFETCHER_H_
#define OCEANBASE_STORAGE_COLUMN_STORE_OB_CG_PREFETCHER_H_
#include "storage/access/ob_index_tree_prefetcher.h"
#include "storage/access/ob_sstable_index_filter.h"
#include "storage/column_store/ob_column_store_util.h"
#include "storage/column_store/ob_cg_bitmap.h"

namespace oceanbase {
namespace storage {
class ObAggGroupBase;
class ObCGPrefetcher : public ObIndexTreeMultiPassPrefetcher<>
{
public:
  ObCGPrefetcher() :
      is_reverse_scan_(false),
      is_project_without_filter_(false),
      need_prewarm_(false),
      cg_iter_type_(-1),
      query_index_range_(),
      query_range_(),
      leaf_query_range_(),
      filter_bitmap_(nullptr),
      micro_data_prewarm_idx_(0),
      filter_constant_type_(),
      max_filter_constant_id_(OB_INVALID_CS_ROW_ID),
      cur_micro_data_read_idx_(-1),
      agg_group_(nullptr),
      sstable_index_filter_(nullptr)
  {}
  virtual ~ObCGPrefetcher()
  {}
  virtual void reset() override;
  virtual void reuse() override;
  int init(
      const int cg_iter_type,
      ObSSTable &sstable,
      const ObTableIterParam &iter_param,
      ObTableAccessContext &access_ctx);
  int switch_context(
      const int cg_iter_type,
      ObSSTable &sstable,
      const ObTableIterParam &iter_param,
      ObTableAccessContext &access_ctx);
  int locate(const ObCSRange &range, const ObCGBitmap *bitmap);
  virtual int prefetch() override;
  OB_INLINE bool is_empty_range() const
  { return 0 == micro_data_prefetch_idx_ && is_prefetch_end_; }
  virtual bool read_wait() override final
  {
    return !is_prefetch_end_ &&
        (0 == micro_data_prefetch_idx_ ||
         cur_micro_data_fetch_idx_ >= micro_data_prefetch_idx_);
  }
  OB_INLINE bool read_finish() const
  {
    return is_prefetch_end_ &&
        (cur_micro_data_fetch_idx_ == micro_data_prefetch_idx_ - 1);
  }
  OB_INLINE bool is_prefetched_full() const
  {
    return micro_data_prefetch_idx_  - cur_micro_data_read_idx_ == max_micro_handle_cnt_
        || access_ctx_->micro_block_handle_mgr_.reach_hold_limit();
  }
  OB_INLINE bool can_index_filter_skip(blocksstable::ObMicroIndexInfo &index_info)
  {
    return index_info.has_agg_data() && index_info.is_filter_uncertain();
  }
  OB_INLINE bool is_cg_scanner() const
  { return ObICGIterator::OB_CG_SCANNER == cg_iter_type_; }
  void recycle_block_data();
  void set_agg_group(ObAggGroupBase *agg_group) { agg_group_ = agg_group; }
  void set_project_type(const bool project_without_filter) { is_project_without_filter_ = project_without_filter; }
  OB_INLINE sql::ObBoolMask get_filter_constant_type() const
  {
    return filter_constant_type_;
  }
  OB_INLINE ObCSRowId get_max_filter_constant_id() const
  {
    return max_filter_constant_id_;
  }
  INHERIT_TO_STRING_KV("ObCGPrefetcher", ObIndexTreeMultiPassPrefetcher,
                       K_(is_reverse_scan), K_(is_project_without_filter), K_(need_prewarm),
                       K_(query_index_range), K_(query_range), K_(cg_iter_type),
                       K_(micro_data_prewarm_idx), K_(cur_micro_data_read_idx), KP_(filter_bitmap),
                       KP_(agg_group), KP_(sstable_index_filter), K_(filter_constant_type), K_(max_filter_constant_id));
protected:
  int get_prefetch_depth(int64_t &depth, const int64_t prefetching_idx);
private:
  int prewarm();
  int refresh_constant_filter_info();
  bool is_constant_filter_continuous(const ObMicroIndexInfo &index_info)
  {
    return index_info.is_filter_constant() &&
        ((is_reverse_scan_ && 0 == index_info.get_row_range().compare(max_filter_constant_id_ - 1)) ||
         (!is_reverse_scan_ && 0 == index_info.get_row_range().compare(max_filter_constant_id_ + 1)));
  }
  bool check_and_update_constant_filter(const ObMicroIndexInfo &index_info)
  {
    bool can_continuous = false;
    if (is_constant_filter_continuous(index_info) &&
        (filter_constant_type_.is_uncertain() ||
         index_info.get_filter_constant_type() == filter_constant_type_.bmt_)) {
      filter_constant_type_.set(index_info.get_filter_constant_type());
      max_filter_constant_id_ = is_reverse_scan_ ? index_info.get_row_range().start_row_id_ : index_info.get_row_range().end_row_id_;
      can_continuous = true;
    }
    return can_continuous;
  }
  struct ObCSIndexTreeLevelHandle : public ObIndexTreeLevelHandle {
  public:
    int prefetch(const int64_t level, ObCGPrefetcher &prefetcher);
    virtual int forward(
        ObIndexTreeMultiPassPrefetcher &prefetcher,
        const bool has_lob_out) override final;
    int locate_row_index(ObCGPrefetcher &prefetcher, const bool is_root, const ObCSRowId row_idx, bool &found);

  };

private:
  int open_index_root();
  int locate_in_prefetched_data(bool &found);
  int refresh_index_tree();
  virtual int init_tree_handles(const int64_t count) override final;
  virtual int prefetch_index_tree() override final;
  virtual int prefetch_micro_data() override final;
  void update_query_range(const ObCSRange &range);
  void update_leaf_query_range(const ObCSRowId leaf_start_row_id);
  int compare_range(const ObCSRange &index_range);
  bool contain_rows(const ObCSRange &index_range);
  bool locate_back(const ObCSRange &locate_range);
  int can_agg_micro_index(const blocksstable::ObMicroIndexInfo &index_info, bool &can_agg);

private:
  bool is_reverse_scan_;
  bool is_project_without_filter_;
  bool need_prewarm_;
  int16_t cg_iter_type_;
  ObStorageDatum datums_[2];
  ObCSRange query_index_range_;
  ObDatumRange query_range_;
  ObDatumRange leaf_query_range_;
  const ObCGBitmap *filter_bitmap_;
  int64_t micro_data_prewarm_idx_;
  // filter_constant_type_ and max_filter_constant_id_ only avaliable in filter scanner
  // when filter_constant_type_ is always_true,  max_filter_constant_id_ means the max continuous rowid that selected by the filter;
  // when filter_constant_type_ is always_false, max_filter_constant_id_ means the max continuose rowid that filtered;
  // when filter_constant_type_ is probablistic, means nothing
  sql::ObBoolMask filter_constant_type_;
  ObCSRowId max_filter_constant_id_;
public:
  int64_t cur_micro_data_read_idx_;
  ObAggGroupBase *agg_group_;
  ObSSTableIndexFilter *sstable_index_filter_;
};

// ObCGIndexPrefetcher only prefetch micro index info, skip data block
// 1. disable submit disk io for data block
// 2. disable multi block prefetch
// 3. disable prewarm data block
class ObCGIndexPrefetcher : public ObCGPrefetcher
{
public:
  virtual void reset() override final;
  virtual void reuse() override final;
  int init(
      const int cg_iter_type,
      ObSSTable &sstable,
      const ObTableIterParam &iter_param,
      ObTableAccessContext &access_ctx);
  int switch_context(
      const int cg_iter_type,
      ObSSTable &sstable,
      const ObTableIterParam &iter_param,
      ObTableAccessContext &access_ctx);
  virtual int prefetch() override final;
};

}
}
#endif // OCEANBASE_STORAGE_COLUMN_STORE_OB_CG_PREFETCHER_H_
