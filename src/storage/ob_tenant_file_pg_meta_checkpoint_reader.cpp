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

#include "ob_tenant_file_pg_meta_checkpoint_reader.h"
#include "storage/ob_tenant_file_struct.h"
#include "storage/ob_tenant_file_mgr.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;

ObTenantFilePGMetaCheckpointReader::ObTenantFilePGMetaCheckpointReader()
{}

int ObTenantFilePGMetaCheckpointReader::read_checkpoint(const ObTenantFileKey& file_key,
    const ObTenantFileSuperBlock& tenant_file_super_block, ObBaseFileMgr& file_mgr, ObPartitionMetaRedoModule& pg_mgr)
{
  int ret = OB_SUCCESS;
  const MacroBlockId& macro_entry_block = tenant_file_super_block.macro_meta_entry_.macro_block_id_;
  const MacroBlockId& pg_entry_block = tenant_file_super_block.pg_meta_entry_.macro_block_id_;
  ObMacroMetaReplayMap* replay_map = nullptr;

  LOG_INFO("read pg meta checkpoint", K(file_key), K(tenant_file_super_block));
  if (macro_entry_block.is_valid() || pg_entry_block.is_valid()) {
    ObStorageFileHandle file_handle;
    ObPGAllMetaCheckpointReader reader;
    STORAGE_LOG(INFO, "read pg meta checkpoint", K(file_key), K(macro_entry_block), K(pg_entry_block));
    if (OB_FAIL(file_mgr.get_tenant_file(file_key, file_handle))) {
      LOG_WARN("fail to get tenant file", K(ret), K(file_key));
    } else if (OB_FAIL(file_mgr.get_macro_meta_replay_map(file_key, replay_map)) || OB_ISNULL(replay_map)) {
      LOG_WARN("fail to get tenant file replay map", K(ret), K(file_key), KP(replay_map));
    } else if (OB_FAIL(reader.init(macro_entry_block, pg_entry_block, file_handle, pg_mgr, replay_map))) {
      LOG_WARN("fail to init ObPGAllMetaCheckpointReader", K(ret));
    } else if (OB_FAIL(reader.read_checkpoint())) {
      LOG_WARN("fail to read checkpoint", K(ret));
    }

    if (OB_SUCC(ret)) {
      ObArray<MacroBlockId> meta_block_list;
      if (OB_FAIL(reader.get_meta_block_list(meta_block_list))) {
        LOG_WARN("fail to get meta block list", K(ret));
      } else if (OB_FAIL(file_mgr.update_tenant_file_meta_blocks(file_key, meta_block_list))) {
        LOG_WARN("fail to update tenant file meta blocks", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("read tenant file pg meta checkpoint", K(ret), K(file_key));
  }
  return ret;
}
