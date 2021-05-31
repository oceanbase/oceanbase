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

#include "storage/ob_pg_macro_meta_checkpoint_reader.h"
#include "storage/ob_macro_meta_replay_map.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;

ObPGMacroMetaCheckpointReader::ObPGMacroMetaCheckpointReader() : reader_(), replay_map_(nullptr), is_inited_(false)
{}

int ObPGMacroMetaCheckpointReader::init(
    const blocksstable::MacroBlockId& entry_block, ObStorageFileHandle& file_handle, ObMacroMetaReplayMap* replay_map)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObPGMacroMetaCheckpointReader has been inited twice", K(ret));
  } else if (OB_UNLIKELY(!entry_block.is_valid() || !file_handle.is_valid() || nullptr == replay_map)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(entry_block), K(file_handle), KP(replay_map));
  } else if (OB_FAIL(reader_.init(entry_block, file_handle))) {
    LOG_WARN("fail to init ObPGMacroMetaCheckpointReader", K(ret));
  } else {
    replay_map_ = replay_map;
    is_inited_ = true;
  }
  return ret;
}

int ObPGMacroMetaCheckpointReader::read_checkpoint()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGMacroMetaCheckpointReader has not been inited", K(ret));
  } else {
    ObMacroBlockKey macro_key;
    ObObj endkey[OB_MAX_ROWKEY_COLUMN_NUMBER];

    while (OB_SUCC(ret)) {
      ObMacroBlockMetaV2 meta;
      MEMSET(&meta, 0, sizeof(meta));
      meta.endkey_ = endkey;
      ObPGMacroBlockMetaCheckpointEntry entry(meta);
      if (OB_FAIL(read_next_entry(entry))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next entry", K(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else {
        macro_key.table_key_ = entry.table_key_;
        macro_key.macro_block_id_ = entry.macro_block_id_;
        if (OB_FAIL(replay_map_->set(macro_key, entry.meta_, false /*overwrite*/))) {
          LOG_WARN("fail to set to map", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObPGMacroMetaCheckpointReader::read_next_entry(ObPGMacroBlockMetaCheckpointEntry& entry)
{
  int ret = OB_SUCCESS;
  ObPGMetaItemBuffer item;
  int64_t pos = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGMacroMetaCheckpointReader has not been inited", K(ret));
  } else if (OB_FAIL(reader_.get_next_item(item))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next item", K(ret));
    }
  } else if (OB_FAIL(entry.deserialize(item.buf_, item.buf_len_, pos))) {
    LOG_WARN("fail to deserialize macro block meta checkpoint entry", K(ret));
  }
  return ret;
}

void ObPGMacroMetaCheckpointReader::reset()
{
  is_inited_ = false;
  reader_.reset();
  replay_map_ = nullptr;
}

ObIArray<MacroBlockId>& ObPGMacroMetaCheckpointReader::get_meta_block_list()
{
  return reader_.get_meta_block_list();
}
