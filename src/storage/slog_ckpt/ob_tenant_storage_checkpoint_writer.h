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

namespace oceanbase
{
namespace storage
{

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

  int update_tablet_meta_addr();


private:
  struct TabletItemAddrInfo
  {
    ObTabletMapKey tablet_key_;
    int64_t item_idx_;
    ObMetaDiskAddr old_addr_;
    ObMetaDiskAddr new_addr_;

    TO_STRING_KV(K_(tablet_key), K_(item_idx), K_(old_addr), K_(new_addr));
  };

  int write_ls_checkpoint(blocksstable::MacroBlockId &entry_block);
  int write_tablet_checkpoint(const common::ObLogCursor &cursor, blocksstable::MacroBlockId &entry_block);
  int copy_one_tablet_item(ObLinkedMacroBlockItemWriter &tablet_item_writer,
    const ObMetaDiskAddr &addr, int64_t *item_idx);
  int read_tablet_from_slog(const ObMetaDiskAddr &addr, char *buf, int64_t &pos);

private:
  bool is_inited_;
  common::ObConcurrentFIFOAllocator allocator_;

  common::ObArray<TabletItemAddrInfo> tablet_item_addr_info_arr_;

  // record ls ids when make ls checkpoint to filter out unwanted tablets when making tablet checkpoint,
  // this ensures that the ls of the tablet is replayed before than the tablet
  common::hash::ObHashSet<share::ObLSID> ls_id_set_;
  ObLinkedMacroBlockItemWriter ls_item_writer_;
  ObLinkedMacroBlockItemWriter tablet_item_writer_;

};

}  // end namespace storage
}  // namespace oceanbase

#endif  // OB_STORAGE_CKPT_TENANT_STORAGE_CHECKPOINT_WRITER_H_
