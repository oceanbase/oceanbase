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

#ifndef OB_STORAGE_CKPT_TENANT_STORAGE_CHECKPOINT_WRITER_H_
#define OB_STORAGE_CKPT_TENANT_STORAGE_CHECKPOINT_WRITER_H_

#include "storage/slog_ckpt/ob_linked_macro_block_writer.h"
#include "storage/ob_super_block_struct.h"
#include "storage/meta_mem/ob_tablet_map_key.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"

namespace oceanbase
{
namespace storage
{
class ObSharedBlockReaderWriter;
class ObTenantStorageCheckpointWriter final
{
public:
  ObTenantStorageCheckpointWriter();
  ~ObTenantStorageCheckpointWriter() = default;
  ObTenantStorageCheckpointWriter(const ObTenantStorageCheckpointWriter &) = delete;
  ObTenantStorageCheckpointWriter &operator=(const ObTenantStorageCheckpointWriter &) = delete;

  int init();
  void reset();
  int write_checkpoint(ObTenantSuperBlock &super_block);
  int get_ls_block_list(common::ObIArray<blocksstable::MacroBlockId> *&block_list);
  int get_tablet_block_list(common::ObIArray<blocksstable::MacroBlockId> *&block_list);
  int batch_compare_and_swap_tablet(const bool is_replay_old);
  int rollback();

private:
  struct TabletItemAddrInfo
  {
    ObTabletMapKey tablet_key_;
    ObMetaDiskAddr old_addr_;
    ObMetaDiskAddr new_addr_;
    ObTabletPoolType tablet_pool_type_;
    bool need_rollback_;

    TabletItemAddrInfo()
      : tablet_key_(), old_addr_(), new_addr_(),
        tablet_pool_type_(ObTabletPoolType::TP_MAX),
        need_rollback_(true)
    {
    }

    TO_STRING_KV(K_(tablet_key), K_(old_addr), K_(new_addr), K_(tablet_pool_type), K_(need_rollback));
  };

  static bool ignore_ret(int ret);
  int get_tablet_with_addr(
      const TabletItemAddrInfo &addr_info,
      ObTabletHandle &tablet_handle);
  int do_rollback(
      common::ObArenaAllocator &allocator,
      const ObMetaDiskAddr &load_addr);
  int write_ls_checkpoint(blocksstable::MacroBlockId &ls_entry_block);
  int write_tablet_checkpoint(ObLS &ls, blocksstable::MacroBlockId &tablet_meta_entry);
  int copy_one_tablet_item(
      const ObTabletMapKey &tablet_key,
      const ObMetaDiskAddr &old_addr,
      char *slog_buf);

private:
  bool is_inited_;
  common::ObArray<TabletItemAddrInfo> tablet_item_addr_info_arr_;
  ObLinkedMacroBlockItemWriter ls_item_writer_;
  ObLinkedMacroBlockItemWriter tablet_item_writer_;
};

}  // end namespace storage
}  // namespace oceanbase

#endif  // OB_STORAGE_CKPT_TENANT_STORAGE_CHECKPOINT_WRITER_H_
