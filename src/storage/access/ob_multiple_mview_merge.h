// Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#ifndef OCEANBASE_STORAGE_OB_MULTIPLE_MVIEW_MERGE_
#define OCEANBASE_STORAGE_OB_MULTIPLE_MVIEW_MERGE_
#include "ob_multiple_merge.h"
#include "ob_single_merge.h"
#include "ob_multiple_scan_merge.h"
#include "ob_multiple_get_merge.h"
#include "ob_multiple_multi_scan_merge.h"

namespace oceanbase
{
namespace storage
{

struct ObBaseTableAccessInfo
{
  int construct_access_ctx(
      common::ObIAllocator *range_allocator,
      const ObTableAccessContext &ctx);
  ObStoreCtx store_ctx_;
  ObTableAccessContext access_ctx_;
};

// base iterator to get base row before version range
// (0, base_version]
class ObMviewBaseMerge final : public ObSingleMerge
{
public:
  ObMviewBaseMerge();
  virtual ~ObMviewBaseMerge();
  OB_INLINE int get_storage_row(blocksstable::ObDatumRow &row)
  {
    return ObSingleMerge::inner_get_next_row(row);
  }
  OB_INLINE bool is_empty() const
  {
    return tables_.empty();
  }
  virtual int check_table_need_read(const ObITable *table, bool &need_table) const override;
  virtual int alloc_row_store(ObTableAccessContext &context, const ObTableAccessParam &param) override;
};

// incremental iterator to get incremental row in version range
// (base_version, read snapshot version]
// use ObMviewBaseMerge to check old row's exists and fuse nop values
class ObMviewIncrMerge
{
public:
  ObMviewIncrMerge();
  virtual ~ObMviewIncrMerge();
  virtual int get_storage_row(blocksstable::ObDatumRow &row) = 0;
  void reset();
  void reuse();
protected:
  int get_incr_row(
      ObTableAccessParam &param,
      ObTableAccessContext &context,
      ObGetTableParam &get_table_param,
      ObMultipleMerge &merge,
      blocksstable::ObDatumRow &row);
  ObMviewBaseMerge *base_data_merge_;
  ObBaseTableAccessInfo *base_access_info_;
  bool is_table_store_refreshed_;
private:
  int check_version_fit(
      const ObTableAccessParam &param,
      const ObTableAccessContext &context,
      const blocksstable::ObDatumRow &row,
      bool &version_fit);
  int generate_output_row(
      const ObTableAccessParam &param,
      ObNopPos &nop_pos,
      const blocksstable::ObDatumRow &base_row,
      blocksstable::ObDatumRow &row);
  int open_base_data_merge(
      ObTableAccessParam &param,
      ObTableAccessContext &context,
      ObGetTableParam &get_table_param,
      const ObDatumRowkey &rowkey);
  int set_old_new_row_flag(
      const ObTableAccessParam &param,
      const ObTableAccessContext &context,
      blocksstable::ObDatumRow &row) const;
  OB_INLINE bool has_no_nop_values(const ObTableAccessParam &param, const ObNopPos &nop_pos) const
  {
    const int64_t old_new_idx = param.iter_param_.get_mview_old_new_col_index();
    const int64_t group_idx = param.iter_param_.get_group_idx_col_index();
    return 0 == nop_pos.count() ||
           (1 == nop_pos.count() && nop_pos.nops_[0] == old_new_idx) ||
           (2 == nop_pos.count() && ((nop_pos.nops_[0] == old_new_idx && nop_pos.nops_[1] == group_idx) ||
                                     (nop_pos.nops_[0] == group_idx && nop_pos.nops_[1] == old_new_idx)));
  }
  bool scan_old_row_;
  const ObColDescIArray *col_descs_;
  ObDatumRowkey base_rowkey_;
  common::ObArenaAllocator rowkey_allocator_;
};

#define DEFINE_MERGE_WRAPPER(Classname, BaseClass)                                                     \
class Classname final : public BaseClass, public ObMviewIncrMerge {                                    \
public:                                                                                                \
  virtual int switch_param(ObTableAccessParam &param, ObTableAccessContext &context,                   \
                           ObGetTableParam &get_table_param) override                                  \
  {                                                                                                    \
    int ret = OB_SUCCESS;                                                                              \
    if (OB_FAIL(BaseClass::switch_param(param, context, get_table_param))) {                           \
      STORAGE_LOG(WARN, "Failed to switch param", K(ret));                                             \
    } else if (nullptr != base_data_merge_) {                                                          \
      is_table_store_refreshed_ = true;                                                                \
    }                                                                                                  \
    return ret;                                                                                        \
  }                                                                                                    \
  virtual int open(ObTableScanRange &table_scan_range) override;                                       \
  virtual int get_storage_row(blocksstable::ObDatumRow &row) override                                  \
  {                                                                                                    \
    return BaseClass::inner_get_next_row(row);                                                         \
  }                                                                                                    \
  virtual void reset() override                                                                        \
  {                                                                                                    \
    BaseClass::reset();                                                                                \
    ObMviewIncrMerge::reset();                                                                         \
  }                                                                                                    \
  virtual void reuse() override                                                                        \
  {                                                                                                    \
    BaseClass::reuse();                                                                                \
    ObMviewIncrMerge::reuse();                                                                         \
  }                                                                                                    \
protected:                                                                                             \
  virtual int inner_get_next_row(blocksstable::ObDatumRow &row) override                               \
  {                                                                                                    \
    return ObMviewIncrMerge::get_incr_row(*access_param_, *access_ctx_, *get_table_param_, *this, row);\
  }                                                                                                    \
  virtual int calc_scan_range() override                                                               \
  {                                                                                                    \
    is_table_store_refreshed_ = true;                                                                  \
    return BaseClass::calc_scan_range();                                                               \
  }                                                                                                    \
private:                                                                                               \
  virtual int64_t generate_read_tables_version() const override                                        \
  {                                                                                                    \
    return access_ctx_->trans_version_range_.base_version_ > 0 ?                                       \
            access_ctx_->trans_version_range_.base_version_ :                                          \
            access_ctx_->store_ctx_->mvcc_acc_ctx_.get_snapshot_version().get_val_for_tx();            \
  }                                                                                                    \
  virtual int check_table_need_read(const ObITable *table, bool &need_table) const override            \
  {                                                                                                    \
    int ret = OB_SUCCESS;                                                                              \
    need_table = true;                                                                                 \
    if (OB_FAIL(ObMultipleMerge::check_table_need_read(table, need_table))) {                          \
      STORAGE_LOG(WARN, "fail to check table need read", K(ret), KPC(table));                          \
    } else {                                                                                           \
      need_table = need_table && !table->is_major_sstable();                                           \
    }                                                                                                  \
    return ret;                                                                                        \
  }                                                                                                    \
};

DEFINE_MERGE_WRAPPER(ObMviewSingleMerge, ObSingleMerge);
DEFINE_MERGE_WRAPPER(ObMviewScanMerge, ObMultipleScanMerge);
DEFINE_MERGE_WRAPPER(ObMviewGetMerge, ObMultipleGetMerge);
DEFINE_MERGE_WRAPPER(ObMviewMultiScanMerge, ObMultipleMultiScanMerge);
#undef DEFINE_MERGE_WRAPPER

typedef ObMultipleMerge ObMviewMerge;
class ObMviewMergeWrapper
{
public:
  ObMviewMergeWrapper();
  ~ObMviewMergeWrapper();
  void reuse();
  int switch_param(
      ObTableAccessParam &param,
      ObTableAccessContext &context,
      ObGetTableParam &get_table_param);
  static int alloc_mview_merge(
      ObTableAccessParam &param,
      ObTableAccessContext &context,
      ObGetTableParam &get_table_param,
      ObTableScanRange &table_scan_range,
      ObMviewMergeWrapper *&merge_wrapper,
      ObMviewMerge *&mview_merge);
private:
  int get_mview_merge(
      ObTableAccessParam &param,
      ObTableAccessContext &context,
      ObGetTableParam &get_table_param,
      ObTableScanRange &table_scan_range,
      ObMviewMerge *&version_merge);
  ObMviewMerge *merges_[T_MAX_ITER_TYPE];
};

}
}

#endif
