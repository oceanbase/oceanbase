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

#include "ob_server_checkpoint_log_reader.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_macro_meta_replay_map.h"
#include "storage/blocksstable/ob_store_file_system.h"
#include "storage/ob_tenant_file_mgr.h"
#include "storage/ob_server_checkpoint_writer.h"
#include "storage/ob_tenant_config_mgr.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;

ObServerCheckpointLogReader::ObServerCheckpointLogReader()
    : pg_meta_reader_(), tenant_file_reader_(), tenant_config_meta_reader_()
{}

int ObServerCheckpointLogReader::read_checkpoint_and_replay_log()
{
  int ret = OB_SUCCESS;
  ObLogCursor replay_start_cursor;
  const ObServerSuperBlock& super_block = OB_FILE_SYSTEM.get_server_super_block();
  ObStorageLogCommittedTransGetter committed_trans_getter;
  if (OB_UNLIKELY(!super_block.is_valid())) {
    ret = OB_ERR_SYS;
    LOG_WARN("super block is invalid", K(ret), K(super_block));
  } else if (OB_FAIL(get_replay_start_point(super_block.content_.replay_start_point_, replay_start_cursor))) {
    LOG_WARN("fail to get replay start point", K(ret));
  } else if (OB_FAIL(committed_trans_getter.init(SLOGGER.get_log_dir(), replay_start_cursor))) {
    LOG_WARN("fail to init committed trans getter", K(ret));
  } else if (OB_FAIL(read_tenant_file_super_block_checkpoint(super_block.content_.super_block_meta_))) {
    LOG_WARN("fail to read tenant file super block checkpoint", K(ret));
  } else if (OB_FAIL(read_tenant_meta_checkpoint(super_block.content_.tenant_config_meta_))) {
    LOG_WARN("fail to read tenant meta checkpoint", K(ret));
  } else if (OB_FAIL(replay_server_slog(replay_start_cursor, committed_trans_getter))) {
    LOG_WARN("fail to replay server slog", K(ret));
  } else if (OB_FAIL(read_pg_meta_checkpoint())) {
    LOG_WARN("fail to read pg meta checkpoint", K(ret));
  } else if (OB_FAIL(replay_other_slog(replay_start_cursor, committed_trans_getter))) {
    LOG_WARN("fail to replay other slog", K(ret));
  } else if (OB_FAIL(set_meta_block_list())) {
    LOG_WARN("fail to set meta block list", K(ret));
  } else if (OB_FAIL(OB_SERVER_FILE_MGR.replay_over())) {
    LOG_WARN("fail to replay over file mgr", K(ret));
  } else {
    LOG_INFO("succeed to load checkpoint");
  }
  return ret;
}

int ObServerCheckpointLogReader::get_replay_start_point(const ObLogCursor& org_log_cursor, ObLogCursor& replay_cursor)
{
  int ret = OB_SUCCESS;
  if (org_log_cursor.newer_than(replay_cursor) && org_log_cursor.is_valid()) {
    replay_cursor = org_log_cursor;
  } else {
    replay_cursor.file_id_ = 1;
    replay_cursor.log_id_ = 0;
    replay_cursor.offset_ = 0;
  }

#ifdef ERRSIM
  if (1 == org_log_cursor.file_id_) {
    replay_cursor.file_id_ = 1;
    replay_cursor.log_id_ = 0;
    replay_cursor.offset_ = 0;
    LOG_INFO("ERRSIM force reset replay start cursor", K(replay_cursor));
  }
#endif

  return ret;
}

int ObServerCheckpointLogReader::read_tenant_file_super_block_checkpoint(const ObSuperBlockMetaEntry& entry)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(
          tenant_file_reader_.read_checkpoint(entry, OB_SERVER_FILE_MGR, OB_FILE_SYSTEM.get_server_root_handle()))) {
    LOG_WARN("fail to read checkpoint", K(ret));
  }
  return ret;
}

int ObServerCheckpointLogReader::replay_server_slog(
    const ObLogCursor& replay_start_cursor, ObStorageLogCommittedTransGetter& committed_trans_getter)
{
  int ret = OB_SUCCESS;
  ObStorageLogReplayer log_replayer;
  ObReplayServerSLogFilter filter_before_parse;
  const char* slog_dir = SLOGGER.get_log_dir();
  if (OB_ISNULL(slog_dir)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, slog dir must not be null", K(ret));
  } else if (OB_FAIL(log_replayer.init(slog_dir, &filter_before_parse))) {
    LOG_WARN("fail to init log replayer", K(ret));
  } else if (OB_FAIL(log_replayer.register_redo_module(OB_REDO_LOG_TENANT_FILE, &OB_SERVER_FILE_MGR))) {
    LOG_WARN("fail to register redo module", K(ret));
  } else if (OB_FAIL(
                 log_replayer.register_redo_module(OB_REDO_LOG_TENANT_CONFIG, &ObTenantConfigMgr::get_instance()))) {
    LOG_WARN("fail to register redo module", K(ret));
  } else if (OB_FAIL(log_replayer.replay(replay_start_cursor, committed_trans_getter))) {
    LOG_WARN("fail to replay log replayer", K(ret));
  } else if (OB_FAIL(OB_SERVER_FILE_MGR.replay_open_files(GCTX.self_addr_))) {
    LOG_WARN("fail to replay open files", K(ret), K(GCTX.self_addr_));
  }
  return ret;
}

int ObServerCheckpointLogReader::read_pg_meta_checkpoint()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(pg_meta_reader_.read_checkpoint(OB_SERVER_FILE_MGR, ObPartitionService::get_instance()))) {
    LOG_WARN("fail to read checkpoint", K(ret));
  }
  return ret;
}

int ObServerCheckpointLogReader::replay_other_slog(
    const ObLogCursor& replay_start_cursor, ObStorageLogCommittedTransGetter& committed_trans_getter)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObBaseStorageLogger::get_instance().replay(replay_start_cursor, committed_trans_getter))) {
    LOG_WARN("fail to replay storage log", K(ret));
  }
  return ret;
}

int ObServerCheckpointLogReader::read_tenant_meta_checkpoint(const blocksstable::ObSuperBlockMetaEntry& meta_entry)
{
  int ret = OB_SUCCESS;
  ObStorageFileHandle& server_root_handle = OB_FILE_SYSTEM.get_server_root_handle();
  if (OB_UNLIKELY(!server_root_handle.is_valid())) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, server root must not be null", K(ret));
  } else if (OB_FAIL(tenant_config_meta_reader_.init(meta_entry.macro_block_id_, server_root_handle))) {
    LOG_WARN("fail to init tenant config meta reader", K(ret));
  } else if (OB_FAIL(tenant_config_meta_reader_.read_checkpoint())) {
    LOG_WARN("fail to read checkpoint", K(ret));
  }
  return ret;
}

int ObServerCheckpointLogReader::set_meta_block_list()
{
  int ret = OB_SUCCESS;
  ObArray<MacroBlockId> meta_block_list;
  const ObIArray<MacroBlockId>& tenant_file_super_block_meta_list = tenant_file_reader_.get_meta_block_list();
  const ObIArray<MacroBlockId>& tenant_config_meta_list = tenant_config_meta_reader_.get_meta_block_list();
  for (int64_t i = 0; OB_SUCC(ret) && i < tenant_file_super_block_meta_list.count(); ++i) {
    if (OB_FAIL(meta_block_list.push_back(tenant_file_super_block_meta_list.at(i)))) {
      LOG_WARN("fail to push back meta block", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tenant_config_meta_list.count(); ++i) {
    if (OB_FAIL(meta_block_list.push_back(tenant_config_meta_list.at(i)))) {
      LOG_WARN("fail to push back meta block", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObServerCheckpointWriter::get_instance().set_meta_block_list(meta_block_list))) {
      LOG_WARN("fail to set meta block list", K(ret));
    }
  }
  return ret;
}

int ObServerPGMetaSLogFilter::filter(const ObISLogFilter::Param& param, bool& is_filtered) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cmd_filter_.filter(param, is_filtered))) {
    LOG_WARN("fail to check is filtered", K(ret));
  } else if (!is_filtered) {
    if (OB_FAIL(file_filter_.filter(param, is_filtered))) {
      LOG_WARN("fail to check is filtered", K(ret));
    }
  }
  return ret;
}
