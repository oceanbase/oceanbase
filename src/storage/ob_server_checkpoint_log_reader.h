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

#ifndef _OB_SERVER_CHECKPOINT_LOG_READER_H
#define _OB_SERVER_CHECKPOINT_LOG_READER_H

#include "storage/ob_server_pg_meta_checkpoint_reader.h"
#include "storage/ob_tenant_config_meta_checkpoint_reader.h"
#include "storage/ob_tenant_file_super_block_checkpoint_reader.h"

namespace oceanbase {
namespace storage {

class ObServerCheckpointLogReader final {
public:
  ObServerCheckpointLogReader();
  ~ObServerCheckpointLogReader() = default;
  int read_checkpoint_and_replay_log();

private:
  int get_replay_start_point(const common::ObLogCursor& org_log_cursor, common::ObLogCursor& replay_start_cursor);
  int read_tenant_file_super_block_checkpoint(const blocksstable::ObSuperBlockMetaEntry& meta_entry);
  int replay_server_slog(const common::ObLogCursor& replay_start_cursor,
      blocksstable::ObStorageLogCommittedTransGetter& committed_trans_getter);
  int read_pg_meta_checkpoint();
  int read_tenant_meta_checkpoint(const blocksstable::ObSuperBlockMetaEntry& meta_entry);
  int replay_other_slog(const common::ObLogCursor& replay_start_cursor,
      blocksstable::ObStorageLogCommittedTransGetter& committed_trans_getter);
  int set_meta_block_list();

private:
  ObServerPGMetaCheckpointReader pg_meta_reader_;
  ObTenantFileSuperBlockCheckpointReader tenant_file_reader_;
  ObTenantConfigMetaCheckpointReader tenant_config_meta_reader_;
};

class ObServerPGMetaSLogFilter : public blocksstable::ObISLogFilter {
public:
  ObServerPGMetaSLogFilter() : cmd_filter_(), file_filter_()
  {}
  virtual ~ObServerPGMetaSLogFilter() = default;
  virtual int filter(const ObISLogFilter::Param& param, bool& is_filtered) const override;

private:
  blocksstable::ObNotReplaySuperBlockAndConfigMetaSLogFilter cmd_filter_;
  ObTenantFileSLogFilter file_filter_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  //_OB_SERVER_CHECKPOINT_LOG_READER_H
