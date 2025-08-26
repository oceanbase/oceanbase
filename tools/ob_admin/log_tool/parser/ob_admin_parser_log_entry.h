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

#ifndef OB_ADMIN_PARSER_LOG_ENTRY_H_
#define OB_ADMIN_PARSER_LOG_ENTRY_H_
#include <stdint.h>
#include "storage/tx/ob_tx_log.h"
#include "logservice/ob_log_base_type.h"
#include "../ob_admin_log_tool_executor.h"

namespace oceanbase
{
namespace transaction
{
class ObTxLogHeader;
class ObTxRedoLog;
}
namespace logservice
{
class ObLogBaseHeader;
}
namespace palf
{
class LogEntry;
}
namespace tools
{

class ObAdminParserLogEntry
{
public:
  ObAdminParserLogEntry(const palf::LogEntry *log_entry,
                        const char *block_name,
                        const palf::LSN lsn,
                        const share::ObAdminMutatorStringArg &str_arg);
#ifdef OB_BUILD_SHARED_LOG_SERVICE
  ObAdminParserLogEntry(const libpalf::LibPalfLogEntry *log_entry,
                        const char *block_name,
                        const palf::LSN lsn,
                        const share::ObAdminMutatorStringArg &str_arg);
#endif
  ~ObAdminParserLogEntry();
  int parse();

private:
  int parse_different_entry_type_(const logservice::ObLogBaseHeader &header);
  int get_entry_header_(logservice::ObLogBaseHeader &header);
  int parse_trans_service_log_(transaction::ObTxLogBlock &tx_log_block, const logservice::ObLogBaseHeader &base_header);
  int parse_schema_log_();
  int parse_tablet_seq_sync_log_();
  int parse_ddl_log_();
  int parse_keep_alive_log_();
  int parse_timestamp_log_();
  int parse_trans_id_log_();
  int parse_gc_ls_log_(const logservice::ObLogBaseHeader &header);
  int parse_major_freeze_log_();
  int parse_primary_ls_service_log_();
  int parse_recovery_ls_service_log_();
  int parse_standby_timestamp_log_();
  int parse_gais_log_();
  int parse_data_dict_log_();
  int parse_reserved_snapshot_log_();
  int parse_medium_log_();
  int parse_dup_table_log_();
  int parse_vector_index_log_();

  //log type belong to trans_service
  int parse_trans_redo_log_(transaction::ObTxLogBlock &tx_log_block,
                            transaction::TxID tx_id,
                            bool &has_dumped_tx_id);
int prepare_log_buf_(logservice::ObLogBaseHeader &header);
private:
  int alloc_mutator_string_buf_();
  int dump_tx_id_ts_(share::ObAdminLogDumperInterface *writer_ptr,
                     int64_t tx_id,
                     bool &has_dumped_tx_id);

private:

  const char *buf_;
  int64_t buf_len_;
  int64_t pos_;

  int64_t scn_val_;
  const palf::LogEntry *entry_;
#ifdef OB_BUILD_SHARED_LOG_SERVICE
  const libpalf::LibPalfLogEntry *libpalf_entry_;
#endif
  char block_name_[OB_MAX_FILE_NAME_LENGTH];
  palf::LSN lsn_;
  share::ObAdminMutatorStringArg str_arg_;
};
}
}
#endif
