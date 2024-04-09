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
#include "storage/ob_sstable_struct.h"
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

  int init(const ObBasicTabletMergeCtx &ctx, bool need_check = true, bool merge_start = true);
  int add_macro_blocks(const ObSSTableMergeInfo &sstable_merge_info);
  int prepare_sstable_builder(const ObITableReadInfo *index_read_info = nullptr); // the default parameter is just for unit test.
  int prepare_index_builder();
  int create_sstable(
      ObBasicTabletMergeCtx &ctx,
      ObTableHandleV2 &merge_table_handle,
      bool &skip_to_create_empty_cg,
      const ObStorageColumnGroupSchema *cg_schema = nullptr,
      const int64_t column_group_idx = 0);
  ObSSTableMergeInfo &get_sstable_merge_info() { return sstable_merge_info_; }
  blocksstable::ObWholeDataStoreDesc &get_sstable_build_desc() { return sstable_builder_.get_data_desc(); }
  blocksstable::ObSSTableIndexBuilder *get_index_builder() { return sstable_builder_.get_index_builder(); }
  void destroy();
  TO_STRING_KV(K_(is_inited), K_(sstable_merge_info), K_(sstable_builder));

private:
  void build_sstable_merge_info(const ObBasicTabletMergeCtx &ctx);
  static int build_create_sstable_param(const ObBasicTabletMergeCtx &ctx,
                                        const blocksstable::ObSSTableMergeRes &res,
                                        const blocksstable::MacroBlockId &bf_macro_id,
                                        ObTabletCreateSSTableParam &param,
                                        const ObStorageColumnGroupSchema *cg_schema = nullptr,
                                        const int64_t column_group_idx = 0);

  static int record_start_tx_scn_for_tx_data(const ObBasicTabletMergeCtx &ctx, ObTabletCreateSSTableParam &param);
private:
  bool is_inited_;
  common::ObSpinLock lock_;
  blocksstable::MacroBlockId bloomfilter_block_id_;
  ObSSTableMergeInfo sstable_merge_info_;
  ObSSTableBuilder sstable_builder_;
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_TABLET_MERGE_INFO_H_
