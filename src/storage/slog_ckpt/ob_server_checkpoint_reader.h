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

#ifndef OB_STORAGE_CKPT_SERVER_CHECKPOINT_LOG_READER_H
#define OB_STORAGE_CKPT_SERVER_CHECKPOINT_LOG_READER_H

#include "common/log/ob_log_cursor.h"
#include "storage/slog_ckpt/ob_linked_macro_block_reader.h"
#include "observer/omt/ob_tenant_meta.h"
namespace oceanbase
{
namespace storage
{
class ObServerCheckpointReader final
{
public:
  ObServerCheckpointReader() = default;
  ~ObServerCheckpointReader() = default;
  ObServerCheckpointReader(const ObServerCheckpointReader &) = delete;
  ObServerCheckpointReader &operator=(const ObServerCheckpointReader &) = delete;

  int read_checkpoint(const ObServerSuperBlock &super_block);
  common::ObIArray<blocksstable::MacroBlockId> &get_meta_block_list();
  int get_tenant_metas(common::hash::ObHashMap<uint64_t, omt::ObTenantMeta> &tenant_meta_map);
  const common::ObArray<omt::ObTenantMeta> &get_tenant_meta_list() const { return tenant_meta_list_; }

private:
  int read_tenant_meta_checkpoint(const blocksstable::MacroBlockId &entry_block);
  int deserialize_tenant_meta(const char *buf, const int64_t buf_len);

private:
  ObLinkedMacroBlockItemReader tenant_meta_item_reader_;
  common::ObArray<omt::ObTenantMeta> tenant_meta_list_;

};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_STORAGE_CKPT_SERVER_CHECKPOINT_LOG_READER_H
