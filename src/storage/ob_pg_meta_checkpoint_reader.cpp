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

#include "ob_pg_meta_checkpoint_reader.h"
#include "storage/ob_partition_service.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;

ObPGMetaCheckpointReader::ObPGMetaCheckpointReader() : is_inited_(false), reader_(), file_handle_(), pg_mgr_(nullptr)
{}

int ObPGMetaCheckpointReader::init(
    const blocksstable::MacroBlockId& entry_block, ObStorageFileHandle& file_handle, ObPartitionMetaRedoModule& pg_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObPGMetaCheckpointReader has not been inited", K(ret));
  } else if (OB_UNLIKELY(!entry_block.is_valid() || !file_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(entry_block), K(file_handle));
  } else if (OB_FAIL(reader_.init(entry_block, file_handle))) {
    LOG_WARN("fail to init ObPGMetaItemReader", K(ret));
  } else if (OB_FAIL(file_handle_.assign(file_handle))) {
    LOG_WARN("fail to assign file handle", K(ret));
  } else {
    pg_mgr_ = &pg_mgr;
    is_inited_ = true;
  }
  return ret;
}

int ObPGMetaCheckpointReader::read_checkpoint()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGMetaCheckpointReader has not been inited", K(ret));
  } else {
    ObPGMetaItemBuffer item;
    int64_t pos = 0;
    while (OB_SUCC(ret)) {
      pos = 0;
      if (OB_FAIL(read_item(item))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next item", K(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_FAIL(pg_mgr_->load_partition(item.buf_, item.buf_len_, pos, file_handle_))) {
        LOG_WARN("fail to load partition", K(ret));
      }
    }
  }
  return ret;
}

int ObPGMetaCheckpointReader::read_item(ObPGMetaItemBuffer& item)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPGMetaCheckpointReader has not been inited", K(ret));
  } else if (OB_FAIL(reader_.get_next_item(item))) {
    LOG_WARN("fail to get next item", K(ret));
  }
  return ret;
}

void ObPGMetaCheckpointReader::reset()
{
  is_inited_ = false;
  reader_.reset();
  file_handle_.reset();
}

ObIArray<MacroBlockId>& ObPGMetaCheckpointReader::get_meta_block_list()
{
  return reader_.get_meta_block_list();
}
