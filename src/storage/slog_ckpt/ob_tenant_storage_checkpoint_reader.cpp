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

#define USING_LOG_PREFIX STORAGE

#include "storage/slog_ckpt/ob_tenant_storage_checkpoint_reader.h"
#include "storage/tx/ob_timestamp_service.h"
#include "storage/tx/ob_trans_id_service.h"
#include "storage/ls/ob_ls_meta.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
using namespace share;
using namespace common;
using namespace blocksstable;
namespace storage
{

ObTenantStorageCheckpointReader::ObTenantStorageCheckpointReader() {}

int ObTenantStorageCheckpointReader::iter_read_meta_item(
    const MacroBlockId &entry_block,
    const ObStorageMetaOp &op,
    ObIArray<MacroBlockId> &block_list)
{
  int ret = OB_SUCCESS;
  ObLinkedMacroBlockItemReader item_reader;
  block_list.reset();
  ObMemAttr mem_attr(MTL_ID(), ObModIds::OB_CHECKPOINT);

  if (OB_UNLIKELY(IS_EMPTY_BLOCK_LIST(entry_block))) {
    LOG_INFO("has no snapshot of log stream", K(ret));
  } else if (OB_FAIL(item_reader.init(entry_block, mem_attr))) {
    LOG_WARN("failed to init log stream item reader");
  } else {
    char *item_buf = nullptr;
    int64_t item_buf_len = 0;
    ObMetaDiskAddr addr;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(item_reader.get_next_item(item_buf, item_buf_len, addr))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next log stream item", K(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_FAIL(op(addr, item_buf, item_buf_len))) {
        LOG_WARN("failed to call op", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      const ObIArray<MacroBlockId> &macro_block_list = item_reader.get_meta_block_list();
      // the list in the item_reader is in reverse order, adjust to normal order
      for (int64_t i = macro_block_list.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
        if (OB_FAIL(block_list.push_back(macro_block_list.at(i)))) {
          LOG_WARN("fail to push back macro block", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObTenantStorageCheckpointReader::read_tablet_checkpoint_by_addr(
  const ObIArray<MacroBlockId> &block_list,
  const ObMetaDiskAddr &addr,
  char *item_buf,
  int64_t &item_buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!addr.is_valid() || !addr.is_block() || nullptr == item_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(addr));
  } else if (OB_FAIL(
               ObLinkedMacroBlockItemReader::read_item(block_list, addr, item_buf, item_buf_len))) {
    LOG_WARN("failed to read tablet item", K(ret));
  }
  return ret;
}

}  // end namespace storage
}  // end namespace oceanbase
