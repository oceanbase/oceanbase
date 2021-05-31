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

#include "ob_tenant_file_super_block_checkpoint_reader.h"
#include "storage/ob_tenant_file_mgr.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;

ObTenantFileSuperBlockCheckpointReader::ObTenantFileSuperBlockCheckpointReader() : reader_()
{}

int ObTenantFileSuperBlockCheckpointReader::read_checkpoint(
    const ObSuperBlockMetaEntry& entry, ObBaseFileMgr& file_mgr, blocksstable::ObStorageFileHandle& file_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!entry.macro_block_id_.is_valid()) || OB_UNLIKELY(!file_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(entry), K(file_handle));
  } else if (OB_FAIL(reader_.init(entry.macro_block_id_, file_handle))) {
    LOG_WARN("fail to init reader", K(ret));
  } else {
    ObPGMetaItemBuffer item;
    while (OB_SUCC(ret)) {
      ObTenantFileInfo file_info;
      ObTenantFileSuperBlockCheckpointEntry entry(file_info);
      int64_t pos = 0;
      if (OB_FAIL(reader_.get_next_item(item))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next item", K(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_FAIL(entry.deserialize(item.buf_, item.buf_len_, pos))) {
        LOG_WARN("fail to deserialize entry", K(ret));
      } else if (OB_FAIL(file_mgr.replay_alloc_file(file_info))) {
        LOG_WARN("fail to replay add file", K(ret));
      }
    }
  }
  return ret;
}

ObIArray<MacroBlockId>& ObTenantFileSuperBlockCheckpointReader::get_meta_block_list()
{
  return reader_.get_meta_block_list();
}
