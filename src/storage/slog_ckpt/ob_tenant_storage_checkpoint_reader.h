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

#ifndef OB_STORAGE_CKPT_TENANT_STORAGE_CHECKPOINT_READER_H_
#define OB_STORAGE_CKPT_TENANT_STORAGE_CHECKPOINT_READER_H_

#include "storage/slog_ckpt/ob_linked_macro_block_reader.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "lib/function/ob_function.h"

namespace oceanbase
{
namespace storage
{

struct ObMetaDiskAddr;

class ObTenantStorageCheckpointReader final
{
public:
  using ObStorageMetaOp = common::ObFunction<int(const ObMetaDiskAddr &addr, const char *buf, const int64_t buf_len)>;
  ObTenantStorageCheckpointReader();
  ~ObTenantStorageCheckpointReader() = default;
  ObTenantStorageCheckpointReader(const ObTenantStorageCheckpointReader &) = delete;
  ObTenantStorageCheckpointReader &operator=(const ObTenantStorageCheckpointReader &) = delete;
  int iter_read_meta_item(const blocksstable::MacroBlockId &entry_block,
                                const ObStorageMetaOp &op,
                                ObIArray<blocksstable::MacroBlockId> &block_list);

  static int read_tablet_checkpoint_by_addr(const ObIArray<blocksstable::MacroBlockId> &block_list,
    const ObMetaDiskAddr &addr, char *item_buf, int64_t &item_buf_len);

};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_STORAGE_CKPT_TENANT_STORAGE_META_READER_H_
