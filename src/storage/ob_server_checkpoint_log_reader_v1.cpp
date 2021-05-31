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

#include "ob_server_checkpoint_log_reader_v1.h"
#include "blocksstable/ob_macro_meta_block_reader.h"
#include "storage/ob_table_mgr_meta_block_reader.h"
#include "storage/ob_partition_meta_block_reader.h"
#include "storage/ob_tenant_config_meta_block_reader.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_table_mgr.h"
#include "storage/ob_tenant_config_mgr.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;

int ObServerCheckpointLogReaderV1::read_checkpoint_and_replay_log(
    ObSuperBlockV2& super_block, ObIArray<blocksstable::MacroBlockId>& meta_block_list)
{
  int ret = OB_SUCCESS;
  ObLogCursor replay_start_cursor;
  if (OB_FAIL(read_checkpoint(super_block, replay_start_cursor, meta_block_list))) {
    LOG_WARN("fail to read checkpoint", K(ret));
  } else if (OB_FAIL(replay_slog(replay_start_cursor))) {
    LOG_WARN("fail to replay slog", K(ret));
  } else if (OB_FAIL(ObTableMgr::get_instance().free_all_sstables())) {
    LOG_WARN("fail to free all sstables", K(ret));
  } else {
    ObMacroBlockMetaMgr::get_instance().destroy();
  }
  return ret;
}

int ObServerCheckpointLogReaderV1::read_checkpoint(
    ObSuperBlockV2& super_block, ObLogCursor& cursor, ObIArray<blocksstable::MacroBlockId>& meta_block_list)
{
  int ret = OB_SUCCESS;
  cursor.file_id_ = 1;
  cursor.log_id_ = 0;
  cursor.offset_ = 0;

  STORAGE_LOG(INFO, "Start to load checkpoint v1!");
  if (OB_UNLIKELY(!super_block.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "super block is not valid", K(ret), K(super_block));
  } else {
    ObLogCursor ck_cursor;

    if (super_block.content_.replay_start_point_.newer_than(ck_cursor)) {
      ck_cursor = super_block.content_.replay_start_point_;
    }

    if (ck_cursor.is_valid()) {
      ObMacroMetaBlockReader macro_meta_reader;
      ObTableMgrMetaBlockReader table_mgr_meta_reader;
      ObPartitionMetaBlockReader partition_meta_reader(ObPartitionService::get_instance());
      ObTenantConfigMetaBlockReader tenant_config_meta_reader;

      if (OB_FAIL(tenant_config_meta_reader.read(super_block.content_.tenant_config_meta_))) {
        STORAGE_LOG(WARN, "Fail to read tenant config meta block", K(ret));
      } else if (OB_FAIL(macro_meta_reader.read(super_block.content_.macro_block_meta_))) {
        STORAGE_LOG(WARN, "Fail to read macro meta block, ", K(ret));
      } else if (OB_FAIL(table_mgr_meta_reader.read(super_block.content_.table_mgr_meta_))) {
        STORAGE_LOG(WARN, "failed to read table mgr meta block", K(ret));
      } else if (OB_FAIL(partition_meta_reader.read(super_block.content_.partition_meta_))) {
        STORAGE_LOG(WARN, "Fail to read partition meta block, ", K(ret));
      } else {
        meta_block_list.reset();
        const common::ObIArray<MacroBlockId>* macro_blocks = NULL;
        macro_blocks = &tenant_config_meta_reader.get_macro_blocks();
        for (int64_t i = 0; OB_SUCC(ret) && i < macro_blocks->count(); ++i) {
          if (OB_FAIL(meta_block_list.push_back(macro_blocks->at(i)))) {
            STORAGE_LOG(WARN, "Fail to push macro block id to array, ", K(ret), K(i));
          }
        }

        macro_blocks = &macro_meta_reader.get_macro_blocks();
        for (int64_t i = 0; OB_SUCC(ret) && i < macro_blocks->count(); ++i) {
          if (OB_FAIL(meta_block_list.push_back(macro_blocks->at(i)))) {
            STORAGE_LOG(WARN, "Fail to push macro block id to array, ", K(ret), K(i));
          }
        }

        macro_blocks = &partition_meta_reader.get_macro_blocks();
        for (int64_t i = 0; OB_SUCC(ret) && i < macro_blocks->count(); ++i) {
          if (OB_FAIL(meta_block_list.push_back(macro_blocks->at(i)))) {
            STORAGE_LOG(WARN, "Fail to push macro block id to array, ", K(ret), K(i));
          }
        }

        macro_blocks = &table_mgr_meta_reader.get_macro_blocks();
        for (int64_t i = 0; OB_SUCC(ret) && i < macro_blocks->count(); ++i) {
          if (OB_FAIL(meta_block_list.push_back(macro_blocks->at(i)))) {
            STORAGE_LOG(WARN, "Fail to push macro block id to array, ", K(ret), K(i));
          }
        }

        cursor = ck_cursor;
      }
    } else {
      cursor.file_id_ = 1;
      cursor.log_id_ = 0;
      cursor.offset_ = 0;
    }

    if (OB_SUCC(ret)) {
      STORAGE_LOG(INFO, "Success to load checkpoint v1!", K(super_block));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(set_replay_log_seq_num(super_block))) {
        STORAGE_LOG(ERROR, "failed to set_replay_log_seq_num", K(ret));
      }
    }
  }
  return ret;
}

int ObServerCheckpointLogReaderV1::set_replay_log_seq_num(ObSuperBlockV2& super_block)
{
  int ret = OB_SUCCESS;
  if (!super_block.is_valid()) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "super_block_ must valid", K(ret), K(super_block));
  } else {
    ObPartitionService::get_instance().set_replay_old(true);
  }
  return ret;
}

int ObServerCheckpointLogReaderV1::replay_slog(ObLogCursor& replay_start_cursor)
{
  int ret = OB_SUCCESS;
  ObStorageLogCommittedTransGetter committed_trans_getter;
  if (OB_FAIL(committed_trans_getter.init(SLOGGER.get_log_dir(), replay_start_cursor))) {
    LOG_WARN("fail to init committed trans getter", K(ret));
  } else if (OB_FAIL(ObBaseStorageLogger::get_instance().replay(replay_start_cursor, committed_trans_getter))) {
    STORAGE_LOG(WARN, "Fail to replay base storage log, ", K(ret));
  }
  return ret;
}
