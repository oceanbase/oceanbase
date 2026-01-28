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

#ifndef OB_STORAGE_COMPACTION_I_COMPACTION_FILTER_H_
#define OB_STORAGE_COMPACTION_I_COMPACTION_FILTER_H_

#include "lib/utility/ob_print_utils.h"
#include "share/schema/ob_table_param.h"
#include "storage/compaction/ob_block_op.h"

namespace oceanbase
{
namespace blocksstable
{
struct ObDatumRow;
struct ObMacroBlockDesc;
struct ObMicroBlock;
}
namespace compaction
{
struct ObMinorRowkeyOutputState;
class ObICompactionFilter
{
public:
  ObICompactionFilter()
  {
  }

  virtual ~ObICompactionFilter() {}
  // for statistics
  enum ObFilterRet : uint8_t
  {
    FILTER_RET_KEEP = 0,
    FILTER_RET_REMOVE = 1,
    FILTER_RET_MAX = 2,
  };
  const static char *ObFilterRetStr[];
  const static char *get_filter_ret_str(const int64_t idx);
  static bool is_valid_filter_ret(const ObFilterRet filter_ret);

  struct ObFilterStatistics
  {
    ObFilterStatistics()
    {
      reset();
    }
    ~ObFilterStatistics() {}
    void add(const ObFilterStatistics &other);
    void row_inc(ObFilterRet filter_ret);
    void micro_inc(ObBlockOp::BlockOp block_op, const int64_t filter_row_cnt);
    void macro_inc(ObBlockOp::BlockOp block_op, const int64_t filter_row_cnt);
    void reset();
    int64_t to_string(char *buf, const int64_t buf_len) const;
    void gene_info(char* buf, const int64_t buf_len, int64_t &pos) const;
    int64_t get_filter_row_cnt() const { return row_cnt_[FILTER_RET_REMOVE] + filter_block_row_cnt_; }
    int64_t filter_sstable_cnt_;
    int64_t filter_block_row_cnt_;
    int64_t row_cnt_[FILTER_RET_MAX];
    int64_t micro_cnt_[ObBlockOp::OP_MAX];
    int64_t macro_cnt_[ObBlockOp::OP_MAX];
  };

  enum CompactionFilterType : uint8_t
  {
    TX_DATA_MINOR,
    MDS_MINOR_FILTER_DATA,
    MDS_MINOR_CROSS_LS,
    MDS_IN_MEDIUM_INFO,
    REORG_INFO_MINOR,
    ROWSCN_FILTER,
    MLOG_PURGE_FILTER,
    FILTER_TYPE_MAX
  };
  const static char *ObFilterTypeStr[];
  const static char *get_filter_type_str(const int64_t idx);
  static bool need_gene_filter_statistics(const CompactionFilterType filter_type)
  {
    return filter_type == MDS_IN_MEDIUM_INFO || filter_type == ROWSCN_FILTER || filter_type == MLOG_PURGE_FILTER;
  }

  // need be thread safe
  virtual int filter(
      const blocksstable::ObDatumRow &row,
      ObFilterRet &filter_ret) const = 0;
  virtual CompactionFilterType get_filter_type() const = 0;
  virtual int get_filter_op(
    const int64_t min_merged_snapshot,
    const int64_t max_merged_snapshot,
    ObBlockOp &op) const
  {
    op.set_open(); // open all blocks by default
    return OB_SUCCESS;
  }
  // use trans_version column & min-max skip index to speed up filter
  // return -1 if not supported
  virtual int64_t get_trans_version_col_idx() const
  {
    return -1;
  }
  VIRTUAL_TO_STRING_KV("filter_type", get_filter_type_str(get_filter_type()));
};

// compaction_filter is shared by multiple compaction tasks,
// so we need to collect filter statistics for each compaction task
struct ObCompactionFilterHandle final
{
public:
  ObCompactionFilterHandle()
  : compaction_filter_(nullptr),
    filter_statistics_()
  {}
  ~ObCompactionFilterHandle() {}
  bool is_valid() const { return nullptr != compaction_filter_; }
  int init(ObICompactionFilter *compaction_filter);
  int filter(
      const blocksstable::ObDatumRow &row,
      ObICompactionFilter::ObFilterRet &filter_ret);
  int get_block_op_from_filter(const blocksstable::ObMacroBlockDesc &macro_desc, ObBlockOp &block_op);
  int get_block_op_from_filter_for_minor(
    const blocksstable::ObMacroBlockDesc &macro_desc,
    const ObMinorRowkeyOutputState &rowkey_state,
    ObBlockOp &block_op);
  int get_block_op_from_filter(const blocksstable::ObMicroBlock &micro_block, ObBlockOp &block_op);
  void inc_filter_row_cnt() { filter_statistics_.row_inc(ObICompactionFilter::FILTER_RET_REMOVE); }
  void inc_macro_open_cnt() { filter_statistics_.macro_inc(ObBlockOp::OP_OPEN, 0/*useless*/); }
  TO_STRING_KV(KPC_(compaction_filter), K_(filter_statistics));
private:
  int inner_get_block_op_from_filter(
    const blocksstable::ObMacroBlockDesc &macro_desc,
    ObBlockOp &block_op);
public:
  ObICompactionFilter *compaction_filter_;
  ObICompactionFilter::ObFilterStatistics filter_statistics_;
};

struct ObCompactionFilterFactory final
{
public:
  template <typename T, typename... Args>
  static int alloc_compaction_filter(
    common::ObIAllocator &allocator,
    ObICompactionFilter *&compaction_filter,
    Args&... args)
  {
    compaction_filter = nullptr;
    int ret = OB_SUCCESS;
    void *buf = nullptr;
    T *new_filter = nullptr;
    if (OB_ISNULL(buf = allocator.alloc(sizeof(T)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc memory", K(ret));
    } else {
      new_filter = new (buf) T();
      if (OB_FAIL(new_filter->init(args...))) {
        STORAGE_LOG(WARN, "failed to init filter", K(ret));
        allocator.free(new_filter);
        new_filter = nullptr;
      } else {
        compaction_filter = new_filter;
      }
    }
    return ret;
  }
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_I_COMPACTION_FILTER_H_
