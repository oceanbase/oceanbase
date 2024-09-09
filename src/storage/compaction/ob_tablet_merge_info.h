//Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_STORAGE_COMPACTION_TABLET_MERGE_INFO_H_
#define OB_STORAGE_COMPACTION_TABLET_MERGE_INFO_H_
#include "storage/compaction/ob_sstable_merge_history.h"
#include "storage/compaction/ob_sstable_builder.h"
namespace oceanbase
{
namespace blocksstable
{
struct ObSSTableMergeRes;
}
namespace compaction
{
struct ObBasicTabletMergeCtx;
// used for record output macro blocks
class ObTabletMergeInfo
{
public:
  ObTabletMergeInfo();
  virtual ~ObTabletMergeInfo();

  int init(const ObMergeStaticInfo &static_history);
  int prepare_sstable_builder(const ObITableReadInfo *index_read_info = nullptr); // the default parameter is just for unit test.
  int prepare_index_builder();
  int create_sstable(
      ObBasicTabletMergeCtx &ctx,
      ObTableHandleV2 &merge_table_handle,
      bool &skip_to_create_empty_cg,
      const ObStorageColumnGroupSchema *cg_schema = nullptr,
      const int64_t column_group_idx = 0);
  ObSSTableMergeHistory &get_merge_history() { return merge_history_; }
  blocksstable::ObWholeDataStoreDesc &get_sstable_build_desc() { return sstable_builder_.get_data_desc(); }
  blocksstable::ObSSTableIndexBuilder *get_index_builder() { return sstable_builder_.get_index_builder(); }
  void destroy();
  int build_sstable_merge_res(
    const ObStaticMergeParam &merge_param,
    const share::ObPreWarmerParam &pre_warm_param,
    int64_t &macro_start_seq,
    blocksstable::ObSSTableMergeRes &res);
  TO_STRING_KV(K_(is_inited), K_(merge_history), K_(sstable_builder));

private:
  static int build_create_sstable_param(const ObBasicTabletMergeCtx &ctx,
                                        const blocksstable::ObSSTableMergeRes &res,
                                        ObTabletCreateSSTableParam &param,
                                        const ObStorageColumnGroupSchema *cg_schema = nullptr,
                                        const int64_t column_group_idx = 0);

  static int record_start_tx_scn_for_tx_data(const ObBasicTabletMergeCtx &ctx, ObTabletCreateSSTableParam &param);
private:
  bool is_inited_;
  ObSSTableMergeHistory merge_history_;
  ObSSTableBuilder sstable_builder_;
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_TABLET_MERGE_INFO_H_
