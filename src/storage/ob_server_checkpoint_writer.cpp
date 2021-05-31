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

#include "ob_server_checkpoint_writer.h"
#include "storage/ob_tenant_file_checkpoint_writer.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;

ObServerCheckpointWriter::ObServerCheckpointWriter()
    : meta_writer_(),
      tenant_file_super_block_writer_(),
      tenant_config_meta_writer_(),
      checkpoint_lock_(),
      meta_lock_(),
      is_inited_(false)
{}

void ObServerCheckpointWriter::reset()
{
  meta_writer_.reset();
  tenant_file_super_block_writer_.reset();
  tenant_config_meta_writer_.reset();
  meta_block_handle_.reset();
}

int ObServerCheckpointWriter::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(checkpoint_lock_.init(FILE_CNT_PER_SERVER))) {
    LOG_WARN("fail to init checkpoint lock", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObServerCheckpointWriter::write_checkpoint(const common::ObLogCursor& checkpoint)
{
  int ret = OB_SUCCESS;
  LOG_INFO("start to write checkpoint", K(checkpoint));
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerCheckpointWriter has not been inited", K(ret));
  } else {
    ObBucketWLockAllGuard guard(checkpoint_lock_);
    ObSuperBlockMetaEntry super_block_entry;
    ObSuperBlockMetaEntry tenant_config_meta_entry;
    ObTenantFileKey invalid_tenant_file_key;
    ObStorageFile* file = nullptr;
    if (OB_FAIL(meta_block_handle_.reserve(MAX_SERVER_META_MACRO_CNT))) {
      LOG_WARN("fail to reserve meta block handle", K(ret));
    } else if (OB_FAIL(meta_writer_.init(
                   ObPartitionService::get_instance(), OB_SERVER_FILE_MGR, invalid_tenant_file_key))) {
      LOG_WARN("fail to init partition meta checkpoint writer", K(ret));
    } else if (OB_FAIL(meta_writer_.write_checkpoint())) {
      LOG_WARN("fail to write pg meta checkpoint", K(ret));
    } else if (OB_FAIL(tenant_file_super_block_writer_.write_checkpoint(OB_FILE_SYSTEM.get_server_root_handle(),
                   OB_SERVER_FILE_MGR,
                   meta_writer_.get_file_checkpoint_map(),
                   super_block_entry))) {
      LOG_WARN("fail to write checkpoint", K(ret));
    } else if (OB_FAIL(tenant_config_meta_writer_.write_checkpoint(tenant_config_meta_entry))) {
      LOG_WARN("fail to write tenant config meta checkpoint", K(ret));
    } else if (OB_ISNULL(file = OB_FILE_SYSTEM.get_server_root_handle().get_storage_file())) {
      ret = OB_ERR_SYS;
      LOG_WARN("error sys, file must not be null", K(ret));
    } else if (OB_FAIL(file->fsync())) {
      LOG_WARN("failed to fsync store file", K(ret));
    } else {
      ObServerSuperBlock super_block = OB_FILE_SYSTEM.get_server_super_block();  // copy assign
      super_block.content_.replay_start_point_ = checkpoint;
      super_block.content_.modify_timestamp_ = ObTimeUtility::current_time();
      super_block.content_.super_block_meta_ = super_block_entry;
      super_block.content_.tenant_config_meta_ = tenant_config_meta_entry;
      TCWLockGuard guard(meta_lock_);
      meta_block_handle_.set_storage_file(file);
      if (OB_FAIL(file->write_super_block(super_block))) {
        LOG_WARN("fail to write server super block", K(ret));
      } else if (OB_FAIL(update_tenant_file_super_block(meta_writer_.get_file_checkpoint_map()))) {
        LOG_WARN("fail to update tenant file super block", K(ret));
        ob_abort();
      } else if (OB_FAIL(meta_block_handle_.add_macro_blocks(
                     tenant_file_super_block_writer_.get_meta_block_list(), false /*switch handle*/))) {
        LOG_WARN("fail to add macro blocks", K(ret));
        ob_abort();
      } else if (OB_FAIL(meta_block_handle_.add_macro_blocks(
                     tenant_config_meta_writer_.get_meta_block_list(), true /*switch handle*/))) {
        LOG_WARN("fail to add macro blocks", K(ret));
        ob_abort();
      }
    }

    if (OB_SUCC(ret)) {
      SLOGGER.remove_useless_log_file(checkpoint.file_id_);
      STORAGE_LOG(INFO, "succeed to write checkpoint", K(checkpoint));
    } else {
      // ignore ret
      (void)SLOGGER.abort();
      STORAGE_LOG(WARN, "failed to write checkpoint", K(ret), K(checkpoint));
    }
    meta_writer_.reset();
    tenant_file_super_block_writer_.reset();
    tenant_config_meta_writer_.reset();
  }
  return ret;
}

int ObServerCheckpointWriter::write_file_checkpoint(const ObTenantFileKey& file_key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObServerCheckpointWriter has not been inited", K(ret));
  } else if (!file_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(file_key));
  } else {
    ObBucketHashWLockGuard bucket_guard(checkpoint_lock_, file_key.hash());
    ObTenantFileCheckpointWriter checkpoint_writer;
    if (OB_FAIL(checkpoint_writer.init(ObPartitionService::get_instance(), OB_SERVER_FILE_MGR, file_key))) {
      LOG_WARN("fail to init checkpoint writer", K(ret));
    } else if (OB_FAIL(checkpoint_writer.write_checkpoint())) {
      LOG_WARN("fail to write file checkpoint", K(ret), K(file_key));
    }
  }
  return ret;
}

ObServerCheckpointWriter& ObServerCheckpointWriter::get_instance()
{
  static ObServerCheckpointWriter instance;
  return instance;
}

int ObServerCheckpointWriter::set_meta_block_list(const ObIArray<MacroBlockId>& meta_block_list)
{
  int ret = OB_SUCCESS;
  TCWLockGuard guard(meta_lock_);
  if (FALSE_IT(meta_block_handle_.set_storage_file(OB_FILE_SYSTEM.get_server_root_handle().get_storage_file()))) {
    LOG_WARN("fail to set file handle", K(ret));
  } else if (OB_FAIL(meta_block_handle_.add_macro_blocks(meta_block_list, true /*switch handle*/))) {
    LOG_WARN("fail to add macro blocks", K(ret));
  }
  return ret;
}

int ObServerCheckpointWriter::get_meta_block_list(ObIArray<MacroBlockId>& meta_block_list)
{
  int ret = OB_SUCCESS;
  TCRLockGuard guard(meta_lock_);
  if (OB_FAIL(meta_block_list.assign(meta_block_handle_.get_meta_block_list()))) {
    LOG_WARN("fail to assign meta block list", K(ret));
  }
  return ret;
}

int ObServerCheckpointWriter::update_tenant_file_super_block(
    common::hash::ObHashMap<ObTenantFileKey, ObTenantFileCheckpointEntry>& file_checkpoint_map)
{
  int ret = OB_SUCCESS;
  for (common::hash::ObHashMap<ObTenantFileKey, ObTenantFileCheckpointEntry>::iterator iter =
           file_checkpoint_map.begin();
       OB_SUCC(ret) && iter != file_checkpoint_map.end();
       ++iter) {
    if (OB_FAIL(OB_SERVER_FILE_MGR.update_tenant_file_meta_info(iter->second, false /*do not write slog*/))) {
      LOG_WARN("fail to update tenant file super block", K(ret));
    }
  }
  return ret;
}
