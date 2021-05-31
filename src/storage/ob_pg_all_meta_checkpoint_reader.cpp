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

#include "ob_pg_all_meta_checkpoint_reader.h"
#include "storage/ob_partition_service.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;

ObPGAllMetaCheckpointReader::ObPGAllMetaCheckpointReader() : is_inited_(false), macro_meta_reader_(), pg_meta_reader_()
{}

int ObPGAllMetaCheckpointReader::init(const blocksstable::MacroBlockId& macro_entry_block,
    const blocksstable::MacroBlockId& pg_entry_block, ObStorageFileHandle& file_handle,
    ObPartitionMetaRedoModule& pg_mgr, ObMacroMetaReplayMap* replay_map)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObPGAllMetaCheckpointReader has already been inited", K(ret));
  } else if (OB_UNLIKELY(!macro_entry_block.is_valid() || !pg_entry_block.is_valid() || !file_handle.is_valid() ||
                         nullptr == replay_map)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(macro_entry_block), K(pg_entry_block), K(file_handle), KP(replay_map));
  } else if (OB_FAIL(macro_meta_reader_.init(macro_entry_block, file_handle, replay_map))) {
    LOG_WARN("fail to init macro meta reader", K(ret));
  } else if (OB_FAIL(pg_meta_reader_.init(pg_entry_block, file_handle, pg_mgr))) {
    LOG_WARN("fail to init pg meta reader", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObPGAllMetaCheckpointReader::read_checkpoint()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGAllMetaCheckpointReader has not been inited", K(ret));
  } else if (OB_FAIL(macro_meta_reader_.read_checkpoint())) {
    LOG_WARN("fail to read checkpoint", K(ret));
  } else if (OB_FAIL(pg_meta_reader_.read_checkpoint())) {
    LOG_WARN("fail to read checkpoint", K(ret));
  }
  return ret;
}

void ObPGAllMetaCheckpointReader::reset()
{
  is_inited_ = false;
  macro_meta_reader_.reset();
  pg_meta_reader_.reset();
}

int ObPGAllMetaCheckpointReader::get_meta_block_list(ObIArray<MacroBlockId>& meta_block_list)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGAllMetaCheckpointReader has not been inited", K(ret));
  } else {
    ObIArray<MacroBlockId>& macro_meta_block_list = macro_meta_reader_.get_meta_block_list();
    ObIArray<MacroBlockId>& pg_meta_block_list = pg_meta_reader_.get_meta_block_list();
    for (int64_t i = 0; OB_SUCC(ret) && i < macro_meta_block_list.count(); ++i) {
      if (OB_FAIL(meta_block_list.push_back(macro_meta_block_list.at(i)))) {
        LOG_WARN("fail to push back meta block list", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < pg_meta_block_list.count(); ++i) {
      if (OB_FAIL(meta_block_list.push_back(pg_meta_block_list.at(i)))) {
        LOG_WARN("fail to push back meta block list", K(ret));
      }
    }
  }
  return ret;
}
