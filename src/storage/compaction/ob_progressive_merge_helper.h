//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_STORAGE_COMPACTION_PROGRESSIVE_MERGE_HELPER_H_
#define OB_STORAGE_COMPACTION_PROGRESSIVE_MERGE_HELPER_H_
#include "/usr/include/stdint.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/container/ob_iarray.h"
namespace oceanbase
{
namespace blocksstable
{
struct ObMacroBlockDesc;
class ObSSTable;
class ObSSTableSecMetaIterator;
struct ObSSTableBasicMeta;
}
namespace storage
{
class ObITable;
class ObStorageSchema;
}
namespace common
{
class ObTabletID;
}
namespace compaction
{
struct ObMergeParameter;
struct ObMacroBlockOp;

class ObProgressiveMergeMgr final
{
public:
  ObProgressiveMergeMgr();
  ~ObProgressiveMergeMgr() { reset(); }
  void reset();
  bool is_inited() const { return is_inited_; }
  int init(
    const common::ObTabletID &tablet_id,
    const bool is_full_merge,
    const blocksstable::ObSSTableBasicMeta &base_meta,
    const storage::ObStorageSchema &schema,
    const uint64_t data_version);
  #define MGR_DEFINE_FUNC(var_name) \
    OB_INLINE int64_t get_##var_name() const { return is_inited_ ? var_name##_ : 0; }
  MGR_DEFINE_FUNC(progressive_merge_round);
  MGR_DEFINE_FUNC(progressive_merge_num);
  MGR_DEFINE_FUNC(progressive_merge_step);
  int64_t get_result_progressive_merge_step(const common::ObTabletID &tablet_id, const int64_t column_group_idx) const;
  bool need_calc_progressive_merge() const { return progressive_merge_round_ > 1 && progressive_merge_step_ < progressive_merge_num_; }
  void mark_progressive_round_unfinish();
  static const int64_t INIT_PROGRESSIVE_MERGE_ROUND = 1;

  TO_STRING_KV(K_(progressive_merge_round), K_(progressive_merge_num), K_(progressive_merge_step), K_(data_version),
    K_(finish_cur_round), K_(is_inited));
private:
  int64_t progressive_merge_round_;
  int64_t progressive_merge_num_;
  int64_t progressive_merge_step_;
  uint64_t data_version_;
  bool finish_cur_round_;
  bool is_inited_;
};

class ObProgressiveMergeHelper final
{
public:
  ObProgressiveMergeHelper(
    const int64_t table_idx = 0)
    : table_idx_(table_idx),
      mgr_(NULL),
      rewrite_block_cnt_(0),
      need_rewrite_block_cnt_(0),
      data_version_(0),
      full_merge_(false),
      check_macro_need_merge_(false),
      is_inited_(false)
    {}
  ~ObProgressiveMergeHelper() = default;
  int init(ObIArray<storage::ObITable*> &tables, const ObMergeParameter &merge_param, ObProgressiveMergeMgr *mgr);

  void reset();
  inline bool is_valid() const { return is_inited_; }
  int check_macro_block_op(const blocksstable::ObMacroBlockDesc &macro_desc, ObMacroBlockOp &block_op);
  int64_t get_compare_progressive_round() const
  {
    return OB_NOT_NULL(mgr_) ? mgr_->get_progressive_merge_round() : -1;
  }
  bool need_calc_progressive_merge() const { return OB_NOT_NULL(mgr_) && mgr_->need_calc_progressive_merge(); }
  inline bool is_progressive_merge_finish_in_cur_step() { return need_rewrite_block_cnt_ == 0 || rewrite_block_cnt_ >= need_rewrite_block_cnt_; }
  void end() const;
  TO_STRING_KV(K_(table_idx), K_(mgr), K_(rewrite_block_cnt), K_(need_rewrite_block_cnt), K_(data_version), K_(full_merge), K_(check_macro_need_merge), K_(is_inited));
private:
  int collect_macro_info(
    const blocksstable::ObSSTable &sstable,
    const ObMergeParameter &merge_param,
    int64_t &rewrite_macro_cnt,
    int64_t &reduce_macro_cnt,
    int64_t &rewrite_block_cnt_for_progressive);
  int open_macro_iter(
    const blocksstable::ObSSTable &sstable,
    const ObMergeParameter &merge_param,
    ObIAllocator &allocator,
    blocksstable::ObSSTableSecMetaIterator *&sec_meta_iter);
  const static int64_t CG_TABLE_CHECK_REWRITE_CNT_ = 4;
  const static int64_t DEFAULT_MACRO_BLOCK_REWRTIE_THRESHOLD = 30;
  constexpr static float REWRITE_MACRO_SIZE_THRESHOLD = OB_DEFAULT_MACRO_BLOCK_SIZE * DEFAULT_MACRO_BLOCK_REWRTIE_THRESHOLD / 100;
private:
  const int64_t table_idx_; // for print log
  ObProgressiveMergeMgr *mgr_;
  int64_t rewrite_block_cnt_; // only record progressive rewrite cnt
  int64_t need_rewrite_block_cnt_;
  uint64_t data_version_;
  bool full_merge_;
  bool check_macro_need_merge_;
  bool is_inited_;
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_PROGRESSIVE_MERGE_HELPER_H_
