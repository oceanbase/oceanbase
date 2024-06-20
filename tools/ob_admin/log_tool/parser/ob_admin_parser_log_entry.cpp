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

#define USING_LOG_PREFIX CLOG
#include "ob_admin_parser_log_entry.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_print_utils.h"
#include "share/ob_rpc_struct.h"
#include "storage/ob_sync_tablet_seq_clog.h"
#include "storage/ddl/ob_ddl_clog.h"
#include "storage/tx/ob_tx_log.h"
#include "share/ob_admin_dump_helper.h"
#include "storage/tx/ob_id_service.h"
#include "storage/memtable/ob_memtable_mutator.h"
#include "storage/tx/ob_keep_alive_ls_handler.h"
#include "storage/tx/ob_dup_table_dump.h"
#include "logservice/ob_log_base_header.h"
#include "logservice/ob_garbage_collector.h"
#ifdef OB_BUILD_LOG_STORAGE_COMPRESS
#include "logservice/ob_log_compression.h"
#endif
#include "logservice/data_dictionary/ob_data_dict_iterator.h"     // ObDataDictIterator
#include "share/scn.h"


#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

namespace oceanbase
{
using namespace share;
using namespace transaction;
using namespace logservice;
using namespace palf;

namespace tools
{
ObAdminParserLogEntry::ObAdminParserLogEntry(const LogEntry &entry,
                                             const char *block_name,
                                             const LSN lsn,
                                             const ObAdminMutatorStringArg &str_arg)
    : scn_val_(entry.get_scn().get_val_for_logservice()), entry_(entry), lsn_(lsn), str_arg_()
{
  buf_ = entry.get_data_buf();
  buf_len_ = entry.get_data_len();
  pos_ = 0;
  memset(block_name_, '\0', OB_MAX_FILE_NAME_LENGTH);
  memcpy(block_name_, block_name, OB_MAX_FILE_NAME_LENGTH);
  str_arg_ = str_arg;
}

ObAdminParserLogEntry::~ObAdminParserLogEntry()
{}

int ObAdminParserLogEntry::parse()
{
  int ret = OB_SUCCESS;
  ObLogBaseHeader header;
  if (OB_FAIL(get_entry_header_(header))) {
    LOG_WARN("get_entry_header failed", K(ret));
  } else if (OB_FAIL(prepare_log_buf_(header))) {
    LOG_WARN("failed to prepare_log_buf", K(ret), K(header));
  } else if (OB_FAIL(parse_different_entry_type_(header))){
    LOG_WARN("parse_different_entry_type_ failed", K(ret), K(header));
  } else {
    LOG_TRACE("ObAdminParserLogEntry parse success", K(header), K(str_arg_));
  }
  return ret;
}

int ObAdminParserLogEntry::get_entry_header_(ObLogBaseHeader &header)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos_;
  if (OB_FAIL(header.deserialize(buf_, buf_len_, pos_))) {
    LOG_WARN("deserialize ObLogBaseHeader failed", K(ret), K(pos_), K(buf_len_));
  } else {
    str_arg_.log_stat_->log_base_header_size_ +=  (pos_ - tmp_pos);
    if (str_arg_.flag_ == LogFormatFlag::NO_FORMAT
        && str_arg_.flag_ != LogFormatFlag::STAT_FORMAT ) {
      fprintf(stdout, ", BASE_HEADER:%s", to_cstring(header));
    }
    LOG_INFO("get_entry_header success", K(header), K(pos_), K(entry_));
  }
  return ret;
}

int ObAdminParserLogEntry::parse_trans_service_log_(ObTxLogBlock &tx_log_block, const logservice::ObLogBaseHeader &base_header)
{
  int ret = OB_SUCCESS;

  str_arg_.log_stat_->total_tx_log_count_++;
  TxID tx_id;
  ObTxLogBlockHeader &tx_block_header = tx_log_block.get_header();
  if (OB_FAIL(tx_log_block.init_for_replay(buf_, buf_len_))) {
    LOG_WARN("ObTxLogBlock init failed", K(ret));
  } else if (FALSE_IT(tx_id = tx_block_header.get_tx_id().get_id())) {
  } else if (str_arg_.filter_.is_tx_id_valid() && tx_id != str_arg_.filter_.get_tx_id()) {
    //just skip this
    LOG_TRACE("skip with tx_id", K(str_arg_), K(tx_id), K(block_name_), K(lsn_));
  } else {

    str_arg_.log_stat_->tx_block_header_size_ += tx_block_header.get_serialize_size();
    rapidjson::StringBuffer json_str;
    ObAdminLogNormalDumper normal_writer;
    ObAdminLogJsonDumper json_writer(json_str);
    ObAdminLogStatDumper stat_writer;
    if (LogFormatFlag::TX_FORMAT == str_arg_.flag_) {
      str_arg_.writer_ptr_ = &json_writer;
    } else if (LogFormatFlag::STAT_FORMAT == str_arg_.flag_) {
      str_arg_.writer_ptr_ = &stat_writer;
    } else {
      str_arg_.writer_ptr_ = &normal_writer;
    }
    //reset_buf
    str_arg_.reset_buf();
    bool has_dumped_tx_id = false;
    if (!str_arg_.filter_.is_tablet_id_valid()) {
      str_arg_.writer_ptr_->start_object();
      if (LogFormatFlag::NO_FORMAT != str_arg_.flag_) {
        //print block_id and lsn for tx_format and filter_format
        str_arg_.writer_ptr_->dump_key("BlockId");
        str_arg_.writer_ptr_->dump_key(block_name_);
        str_arg_.writer_ptr_->dump_key("LSN");
        str_arg_.writer_ptr_->dump_int64((int64_t)(lsn_.val_));
        str_arg_.writer_ptr_->dump_key("ReplayHint");
        str_arg_.writer_ptr_->dump_int64(base_header.get_replay_hint());
        str_arg_.writer_ptr_->dump_key("ReplayBarrier");
        bool pre_b = base_header.need_pre_replay_barrier();
        bool post_b = base_header.need_post_replay_barrier();
        if (pre_b && post_b) {
          str_arg_.writer_ptr_->dump_string("STRICT");
        } else if (pre_b) {
          str_arg_.writer_ptr_->dump_string("PRE");
        } else if (post_b) {
          str_arg_.writer_ptr_->dump_string("POST");
        } else {
          str_arg_.writer_ptr_->dump_string("NONE");
        }
      }
      str_arg_.writer_ptr_->dump_key("TxID");
      str_arg_.writer_ptr_->dump_int64(tx_id);
      str_arg_.writer_ptr_->dump_key("scn");
      str_arg_.writer_ptr_->dump_int64(scn_val_);
      str_arg_.writer_ptr_->dump_key("TxBlockHeader");
      str_arg_.writer_ptr_->dump_string(to_cstring(tx_block_header));
      has_dumped_tx_id = true;
    }
    ObTxLogHeader log_header;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(tx_log_block.get_next_log(log_header))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("ObTxLogBlock get_next_log failed", K(ret), K(log_header));
        }
      } else {
        str_arg_.log_stat_->tx_log_header_size_ += log_header.get_serialize_size();
        LOG_TRACE("ObTxLogBlock get_next_log's log_header succ", K(ret), K(log_header));
        ObTxLogType tx_log_type = log_header.get_tx_log_type();
        if (LogFormatFlag::FILTER_FORMAT == str_arg_.flag_ && str_arg_.filter_.is_tablet_id_valid()) {
          //filter_format with valid tablet_id only cares redo log
          if (tx_log_type == transaction::ObTxLogType::TX_REDO_LOG) {
            if (OB_FAIL(parse_trans_redo_log_(tx_log_block, tx_id, has_dumped_tx_id))) {
              LOG_WARN("failed to parse_trans_redo_log_", K(ret), K(str_arg_), K(tx_id), K(scn_val_));
            }
          } else { /*do nothing*/}
        } else {
          switch (tx_log_type) {
            case transaction::ObTxLogType::TX_REDO_LOG: {
              if (OB_FAIL(parse_trans_redo_log_(tx_log_block, tx_id, has_dumped_tx_id))) {
                LOG_WARN("failed to parse_trans_redo_log_", K(ret), K(str_arg_), K(tx_id), K(scn_val_));
              }
              break;
            }
            case transaction::ObTxLogType::TX_ROLLBACK_TO_LOG: {
              ObTxRollbackToLog rollback_log;
              if (OB_FAIL(dump_tx_id_ts_(str_arg_.writer_ptr_, tx_id, has_dumped_tx_id))) {
                LOG_WARN("failed to dump_tx_id_ts_", K(ret), K(rollback_log));
              } else if (OB_FAIL(tx_log_block.deserialize_log_body(rollback_log))) {
                LOG_WARN("tx_log_block.deserialize_log_body failed", K(ret), K(rollback_log));
              } else if (OB_FAIL(rollback_log.ob_admin_dump(str_arg_))) {
                LOG_WARN("failed to dump ObTxRollbackToLog", K(ret), K(rollback_log), K(str_arg_));
              } else { /*do nothing*/}
              break;
            }
            case transaction::ObTxLogType::TX_MULTI_DATA_SOURCE_LOG: {
              // ObTxMultiSourceDataLogTempRef temp_ref;
              ObTxMultiDataSourceLog ms_log;
              if (OB_FAIL(dump_tx_id_ts_(str_arg_.writer_ptr_, tx_id, has_dumped_tx_id))) {
                LOG_WARN("failed to dump_tx_id_ts_", K(ret));
              } else if (OB_FAIL(tx_log_block.deserialize_log_body(ms_log))) {
                LOG_WARN("tx_log_block.deserialize_log_body failed", K(ret), K(ms_log));
              } else if (OB_FAIL(ms_log.ob_admin_dump(str_arg_))) {
                LOG_WARN("failed to dump ObTxMultiDataSourceLog", K(ret), K(ms_log), K(str_arg_));
              } else {/*do nothing*/}
              break;
            }
            case transaction::ObTxLogType::TX_ACTIVE_INFO_LOG: {
              ObTxActiveInfoLogTempRef temp_ref;
              ObTxActiveInfoLog infolog(temp_ref);
              if (OB_FAIL(dump_tx_id_ts_(str_arg_.writer_ptr_, tx_id, has_dumped_tx_id))) {
                LOG_WARN("failed to dump_tx_id_ts_", K(ret));
              } else if (OB_FAIL(tx_log_block.deserialize_log_body(infolog))) {
                LOG_WARN("tx_log_block.deserialize_log_body failed", K(ret), K(infolog));
              } else if (OB_FAIL(infolog.ob_admin_dump(str_arg_))) {
                LOG_WARN("failed to dump ObTxActiveInfoLog", K(ret), K(infolog), K(str_arg_));
              } else {/*do nothing*/}
              break;
            }
            case transaction::ObTxLogType::TX_RECORD_LOG: {
              ObTxRecordLogTempRef temp_ref;
              ObTxRecordLog record_log(temp_ref);
              if (OB_FAIL(dump_tx_id_ts_(str_arg_.writer_ptr_, tx_id, has_dumped_tx_id))) {
                LOG_WARN("failed to dump_tx_id_ts_", K(ret));
              } else if (OB_FAIL(tx_log_block.deserialize_log_body(record_log))) {
                LOG_WARN("tx_log_block.deserialize_log_body failed", K(ret), K(record_log));
              } else if (OB_FAIL(record_log.ob_admin_dump(str_arg_))) {
                LOG_WARN("failed to dump ObTxRecordLog", K(ret), K(record_log), K(str_arg_));
              } else {/*do nothing*/}
              break;
            }
            case transaction::ObTxLogType::TX_COMMIT_INFO_LOG: {
              ObTxCommitInfoLogTempRef temp_ref;
              ObTxCommitInfoLog infolog(temp_ref);
              if (OB_FAIL(dump_tx_id_ts_(str_arg_.writer_ptr_, tx_id, has_dumped_tx_id))) {
                LOG_WARN("failed to dump_tx_id_ts_", K(ret));
              } else if (OB_FAIL(tx_log_block.deserialize_log_body(infolog))) {
                LOG_WARN("tx_log_block.deserialize_log_body failed", K(ret), K(infolog));
              } else if (OB_FAIL(infolog.ob_admin_dump(str_arg_))) {
                LOG_WARN("failed to dump ObTxCommitInfoLog", K(ret), K(infolog), K(str_arg_));
              } else {/*do nothing*/}
              break;
            }
            case transaction::ObTxLogType::TX_PREPARE_LOG: {
              ObTxPrepareLogTempRef temp_ref;
              ObTxPrepareLog prepare_log(temp_ref);
              if (OB_FAIL(dump_tx_id_ts_(str_arg_.writer_ptr_, tx_id, has_dumped_tx_id))) {
                LOG_WARN("failed to dump_tx_id_ts_", K(ret));
              } else if (OB_FAIL(tx_log_block.deserialize_log_body(prepare_log))) {
                LOG_WARN("tx_log_block.deserialize_log_body failed", K(ret), K(prepare_log));
              } else if (OB_FAIL(prepare_log.ob_admin_dump(str_arg_))) {
                LOG_WARN("failed to dump ObTxPrepareLog", K(ret), K(prepare_log), K(str_arg_));
              } else {/*do nothing*/}
              break;
            }
            case transaction::ObTxLogType::TX_COMMIT_LOG: {
              ObTxCommitLogTempRef temp_ref;
              ObTxCommitLog commit_log(temp_ref);
              if (OB_FAIL(dump_tx_id_ts_(str_arg_.writer_ptr_, tx_id, has_dumped_tx_id))) {
                LOG_WARN("failed to dump_tx_id_ts_", K(ret));
              } else if (OB_FAIL(tx_log_block.deserialize_log_body(commit_log))) {
                LOG_WARN("tx_log_block.deserialize_log_body failed", K(ret), K(commit_log));
              } else if (OB_FAIL(commit_log.ob_admin_dump(str_arg_))) {
                LOG_WARN("failed to dump ObTxCommitLog", K(ret), K(commit_log), K(str_arg_));
              } else {/*do nothing*/}
              break;
            }
            case transaction::ObTxLogType::TX_ABORT_LOG: {
              ObTxAbortLogTempRef temp_ref;
              ObTxAbortLog abort_log(temp_ref);
              if (OB_FAIL(dump_tx_id_ts_(str_arg_.writer_ptr_, tx_id, has_dumped_tx_id))) {
                LOG_WARN("failed to dump_tx_id_ts_", K(ret));
              } else if (OB_FAIL(tx_log_block.deserialize_log_body(abort_log))) {
                LOG_WARN("tx_log_block.deserialize_log_body failed", K(ret), K(abort_log));
              } else if (OB_FAIL(abort_log.ob_admin_dump(str_arg_))) {
                LOG_WARN("failed to dump ObTxAbortLog", K(ret), K(abort_log), K(str_arg_));
              } else {/*do nothing*/}
              break;
            }
            case transaction::ObTxLogType::TX_CLEAR_LOG: {
              ObTxClearLogTempRef temp_ref;
              ObTxClearLog clear_log(temp_ref);
              if (OB_FAIL(dump_tx_id_ts_(str_arg_.writer_ptr_, tx_id, has_dumped_tx_id))) {
                LOG_WARN("failed to dump_tx_id_ts_", K(ret));
              } else if (OB_FAIL(tx_log_block.deserialize_log_body(clear_log))) {
                LOG_WARN("tx_log_block.deserialize_log_body failed", K(ret), K(clear_log));
              } else if (OB_FAIL(clear_log.ob_admin_dump(str_arg_))) {
                LOG_WARN("failed to dump ObTxClearLog", K(ret), K(clear_log), K(str_arg_));
              } else {/*do nothing*/}
              break;
            }
            case transaction::ObTxLogType::TX_START_WORKING_LOG: {
              ObTxStartWorkingLogTempRef temp_ref;
              ObTxStartWorkingLog sw_log(temp_ref);
              if (OB_FAIL(dump_tx_id_ts_(str_arg_.writer_ptr_, tx_id, has_dumped_tx_id))) {
                LOG_WARN("failed to dump_tx_id_ts_", K(ret));
              } else if (OB_FAIL(tx_log_block.deserialize_log_body(sw_log))) {
                LOG_WARN("tx_log_block.deserialize_log_body failed", K(ret), K(sw_log));
              } else if (OB_FAIL(sw_log.ob_admin_dump(str_arg_))) {
                LOG_WARN("failed to dump ObTxStartWorkingLog", K(ret), K(sw_log), K(str_arg_));
              } else {/*do nothing*/}
              break;
            }
            case transaction::ObTxLogType::TX_LOG_TYPE_LIMIT: {
              LOG_WARN("UNKNOWN tx log type", K(tx_log_block));
              break;
            }
            case transaction::ObTxLogType::UNKNOWN: {
              LOG_WARN("UNKNOWN tx log type", K(tx_log_block));
              break;
            }
            default: {
              fprintf(stdout, "not supported TX Log Type: %ld", log_header.get_tx_log_type());
              LOG_WARN("don't support this log type", K(log_header.get_tx_log_type()));
              ret = OB_NOT_SUPPORTED;
            }
          }
        }
        LOG_TRACE("finish parse one trans log", K(ret), K(log_header));
      }
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (has_dumped_tx_id) {
      str_arg_.writer_ptr_->end_object();
    }

    if (LogFormatFlag::STAT_FORMAT == str_arg_.flag_) {
      //do nothing
    } else if (LogFormatFlag::TX_FORMAT == str_arg_.flag_) {
      fprintf(stdout, "\n%s\n", json_str.GetString());
    } else if (has_dumped_tx_id) {
      fprintf(stdout, "\n");
    }
  }
  return ret;
}

int ObAdminParserLogEntry::parse_schema_log_()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  int64_t table_version = OB_INVALID_VERSION;
  ObStorageSchema storage_schema;
  if (OB_FAIL(serialization::decode_i64(buf_, buf_len_, pos_, &table_version))) {
    LOG_WARN("fail to deserialize table_version", K(ret));
  } else if (OB_FAIL(storage_schema.deserialize(allocator, buf_, buf_len_, pos_))) {
    LOG_WARN("fail to deserialize table schema", K(ret));
  } else {
    fprintf(stdout, " ###<StorageSchemaLog>: table_version:%ld, schema: %s\n", table_version, to_cstring(storage_schema));
  }

  return ret;
}

int ObAdminParserLogEntry::parse_tablet_seq_sync_log_()
{
  int ret = OB_SUCCESS;
  ObSyncTabletSeqLog log;
  if (OB_FAIL(log.deserialize(buf_, buf_len_, pos_))) {
    LOG_WARN("deserialize sync tablet seq log failed", K(ret), KP(buf_), K(buf_len_), K(pos_));
  } else {
    fprintf(stdout, " ###<SyncTabletSeqLog>:%s\n", to_cstring(log));
  }
  return ret;
}

int ObAdminParserLogEntry::parse_ddl_log_()
{
  int ret = OB_SUCCESS;
  ObDDLClogHeader header;
  if (OB_FAIL(header.deserialize(buf_, buf_len_, pos_))) {
    LOG_WARN("fail to deserialize header", K(ret));
  } else {
    const ObDDLClogType &type = header.get_ddl_clog_type();
    switch (type) {
      case ObDDLClogType::DDL_REDO_LOG: {
        ObDDLRedoLog log;
        if (OB_FAIL(log.deserialize(buf_, buf_len_, pos_))) {
          LOG_WARN("deserialize ddl redo log failed", K(ret), KP(buf_), K(buf_len_), K(pos_));
        } else {
          fprintf(stdout, " ###<ObDDLRedoLog>: %s\n", to_cstring(log));
        }
        break;
      }
      case ObDDLClogType::DDL_COMMIT_LOG: {
        ObDDLCommitLog log;
        if (OB_FAIL(log.deserialize(buf_, buf_len_, pos_))) {
          LOG_WARN("deserialize ddl commit log failed", K(ret), KP(buf_), K(buf_len_), K(pos_));
        } else {
          fprintf(stdout, " ###<ObDDLCommitLog>: %s\n", to_cstring(log));
        }
        break;
      }
      case ObDDLClogType::DDL_TABLET_SCHEMA_VERSION_CHANGE_LOG: {
        ObTabletSchemaVersionChangeLog log;
        if (OB_FAIL(log.deserialize(buf_, buf_len_, pos_))) {
          LOG_WARN("deserialize tablet schema version change log failed", K(ret), KP(buf_),
                   K(buf_len_), K(pos_));
        } else {
          fprintf(stdout, " ###<TabletSchemaVersionChangeLog>: %s\n", to_cstring(log));
        }
        break;
      }
      case ObDDLClogType::DDL_START_LOG: {
        ObDDLStartLog log;
        if (OB_FAIL(log.deserialize(buf_, buf_len_, pos_))) {
          LOG_WARN("deserialize ddl start log failed", K(ret), KP(buf_), K(buf_len_), K(pos_));
        } else {
          fprintf(stdout, " ###<ObDDLStartLog>: %s\n", to_cstring(log));
        }
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("Unknown log type", K(ret), K(type));
      } // default
    } // end of switch
  }
  return ret;
}

int ObAdminParserLogEntry::parse_keep_alive_log_()
{
  int ret = OB_SUCCESS;
  ObKeepAliveLogBody log_body;
  if (OB_FAIL(log_body.deserialize(buf_, buf_len_, pos_))) {
    LOG_WARN("deserialize keepalive log failed", K(ret), KP(buf_), K(buf_len_), K(pos_));
  } else {
    fprintf(stdout, " ###<ObKeepAliveLog>: %s\n", to_cstring(log_body));
  }
  return ret;
}

int ObAdminParserLogEntry::parse_timestamp_log_()
{
  int ret = OB_SUCCESS;
  ObPresistIDLog log;
  if (OB_FAIL(log.deserialize(buf_, buf_len_, pos_))) {
    LOG_WARN("deserialize ObPresistIDLog failed", K(ret), KP(buf_), K(buf_len_), K(pos_));
  } else {
    fprintf(stdout, " ###PersistID<TIMESTAMP>: %s\n", to_cstring(log));
  }
  return ret;
}

int ObAdminParserLogEntry::parse_trans_id_log_()
{
  int ret = OB_SUCCESS;
  ObPresistIDLog log;
  if (OB_FAIL(log.deserialize(buf_, buf_len_, pos_))) {
    LOG_WARN("deserialize ObPresistIDLog failed", K(ret), KP(buf_), K(buf_len_), K(pos_));
  } else {
    fprintf(stdout, " ###PersistID<TRANS_ID>: %s\n", to_cstring(log));
  }
  return ret;
}
int ObAdminParserLogEntry::parse_gc_ls_log_(const logservice::ObLogBaseHeader &header)
{
  int ret = OB_SUCCESS;
  pos_ -= header.get_serialize_size();
  ObGCLSLog gc_log;
  if (OB_FAIL(gc_log.deserialize(buf_, buf_len_, pos_))) {
    LOG_WARN("deserialize ObPresistIDLog failed", K(ret), KP(buf_), K(buf_len_), K(pos_));
  } else {
    fprintf(stdout, " ###ObGCLSLog: %s\n", to_cstring(gc_log));
  }

  return ret;
}

int ObAdminParserLogEntry::parse_major_freeze_log_()
{
  //not supported so far, just reserved
  int ret = OB_NOT_SUPPORTED;
  return ret;
}

int ObAdminParserLogEntry::parse_primary_ls_service_log_()
{
  //not supported so far, just reserved
  int ret = OB_NOT_SUPPORTED;
  return ret;
}

int ObAdminParserLogEntry::parse_recovery_ls_service_log_()
{
  //not supported so far, just reserved
  int ret = OB_NOT_SUPPORTED;
  return ret;
}

int ObAdminParserLogEntry::parse_standby_timestamp_log_()
{
  //not supported so far, just reserved
  int ret = OB_NOT_SUPPORTED;
  return ret;
}

int ObAdminParserLogEntry::parse_gais_log_()
{
  //not supported so far, just reserved
  int ret = OB_NOT_SUPPORTED;
  return ret;
}

int ObAdminParserLogEntry::parse_data_dict_log_()
{
  int ret = OB_SUCCESS;
  static datadict::ObDataDictIterator dict_iterator;
  ObArenaAllocator allocator("ObAdmDictDump");

  if (OB_FAIL(dict_iterator.init(OB_SERVER_TENANT_ID))) {
    LOG_WARN("dict_iterator init failed", KR(ret), KP_(buf), K_(buf_len), K_(pos));
  } else if (OB_FAIL(dict_iterator.append_log_buf(buf_, buf_len_, pos_))) {
    LOG_WARN("append palf_log to data_dict_iterator failed", KR(ret), KP_(buf), K_(buf_len), K_(pos));
  } else {
    while (OB_SUCC(ret)) {
      datadict::ObDictMetaHeader header;

      if (OB_FAIL(dict_iterator.next_dict_header(header))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("next_dict_header failed", KR(ret), K(header));
        }
      } else if (! header.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("expect valid dict_header", KR(ret), K(header));
      } else {
        ObAdminLogNormalDumper normal_writer;
        str_arg_.writer_ptr_ = &normal_writer;
        str_arg_.writer_ptr_->start_object();
        str_arg_.writer_ptr_->dump_key("DictHeader");
        str_arg_.writer_ptr_->dump_string(to_cstring(header));
        switch (header.get_dict_meta_type()) {
          case datadict::ObDictMetaType::TENANT_META: {
            datadict::ObDictTenantMeta tenant_meta(&allocator);
            if (OB_FAIL(dict_iterator.next_dict_entry(header, tenant_meta))) {
              LOG_ERROR("get next_dict_entry failed", KR(ret), K(header), K(tenant_meta));
            } else {
              str_arg_.writer_ptr_->dump_key("TenantMeta");
              str_arg_.writer_ptr_->dump_string(to_cstring(tenant_meta));
            }
            break;
          }
          case datadict::ObDictMetaType::DATABASE_META: {
            datadict::ObDictDatabaseMeta db_meta(&allocator);
            if (OB_FAIL(dict_iterator.next_dict_entry(header, db_meta))) {
              LOG_ERROR("get next_dict_entry failed", KR(ret), K(header), K(db_meta));
            } else {
              str_arg_.writer_ptr_->dump_key("DatabaseMeta");
              str_arg_.writer_ptr_->dump_string(to_cstring(db_meta));
            }
            break;
          }
          case datadict::ObDictMetaType::TABLE_META: {
            datadict::ObDictTableMeta table_meta(&allocator);
            if (OB_FAIL(dict_iterator.next_dict_entry(header, table_meta))) {
              LOG_ERROR("get next_dict_entry failed", KR(ret), K(header), K(table_meta));
            } else {
              str_arg_.writer_ptr_->dump_key("TableMeta");
              str_arg_.writer_ptr_->dump_string(to_cstring(table_meta));
            }
            break;
          }
          default: {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("invalid meta_type", KR(ret), K(header));
          }
        }
        str_arg_.writer_ptr_->end_object();
      }
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObAdminParserLogEntry::parse_reserved_snapshot_log_()
{
  int ret = OB_SUCCESS;
  int64_t update_version = 0;
  if (OB_FAIL(serialization::decode_i64(buf_, buf_len_, pos_, &update_version))) {
    LOG_WARN("fail to deserialize update_version", K(ret));
  } else {
    fprintf(stdout, " ###<LSReservedSnapshotLog>: snapshot: %ld\n", update_version);
  }
  return ret;
}

int ObAdminParserLogEntry::parse_medium_log_()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObTabletID tablet_id;
  int64_t medium_snapshot = 0;
  compaction::ObMediumCompactionInfo medium_info;
  ObStorageSchema storage_schema;
  if (OB_FAIL(tablet_id.deserialize(buf_, buf_len_, pos_))) {
    LOG_WARN("fail to deserialize tablet id", K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf_, buf_len_, pos_, &medium_snapshot))) {
    LOG_WARN("fail to deserialize medium_snapshot", K(ret));
  } else if (OB_FAIL(medium_info.deserialize(allocator, buf_, buf_len_, pos_))) {
    LOG_WARN("fail to deserialize medium info", K(ret));
  } else {
    fprintf(stdout, " ###<MediumCompactionLog>: tablet_id:%ld, medium_info: %s\n", tablet_id.id(), to_cstring(medium_info));
  }
  return ret;
}

int ObAdminParserLogEntry::parse_dup_table_log_()
{
  int ret = OB_SUCCESS;

  oceanbase::transaction::ObDupTableLogDumpIterator dup_table_log_dump_iter;

  if (OB_FAIL(dup_table_log_dump_iter.init_with_log_buf(buf_ + pos_, buf_len_, &str_arg_))) {
    LOG_WARN("fail to init  dup table log dump_iter", K(ret));
  } else if (OB_FAIL(dup_table_log_dump_iter.dump_dup_table_log())) {
    LOG_WARN("fail to dump dup table log", K(ret));
  } else {
    pos_ += dup_table_log_dump_iter.get_iter_buf_pos();
  }

  return ret;
}

int ObAdminParserLogEntry::parse_different_entry_type_(const logservice::ObLogBaseHeader &header)
{
  int ret = OB_SUCCESS;
  if (LogFormatFlag::TX_FORMAT == str_arg_.flag_
      || LogFormatFlag::FILTER_FORMAT == str_arg_.flag_
      || LogFormatFlag::STAT_FORMAT == str_arg_.flag_) {
    if (oceanbase::logservice::ObLogBaseType::TRANS_SERVICE_LOG_BASE_TYPE == header.get_log_type()) {
      //TX_FORMAT only cares trans_log
      ObTxLogBlock log_block;
      ret = parse_trans_service_log_(log_block, header);
    }
  } else {
    switch (header.get_log_type()) {
      case oceanbase::logservice::ObLogBaseType::TRANS_SERVICE_LOG_BASE_TYPE: {
        ObTxLogBlock log_block;
        ret = parse_trans_service_log_(log_block, header);
        break;
      }
      case oceanbase::logservice::ObLogBaseType::STORAGE_SCHEMA_LOG_BASE_TYPE: {
        ret = parse_schema_log_();
        break;
      }
      case oceanbase::logservice::ObLogBaseType::TABLET_SEQ_SYNC_LOG_BASE_TYPE: {
        ret = parse_tablet_seq_sync_log_();
        break;
      }
      case oceanbase::logservice::ObLogBaseType::DDL_LOG_BASE_TYPE: {
        ret = parse_ddl_log_();
        break;
      }
      case oceanbase::logservice::ObLogBaseType::KEEP_ALIVE_LOG_BASE_TYPE: {
        ret = parse_keep_alive_log_();
        break;
      }
      case oceanbase::logservice::ObLogBaseType::TIMESTAMP_LOG_BASE_TYPE: {
        ret = parse_timestamp_log_();
        break;
      }
      case oceanbase::logservice::ObLogBaseType::TRANS_ID_LOG_BASE_TYPE: {
        ret = parse_trans_id_log_();
        break;
      }
      case oceanbase::logservice::ObLogBaseType::GC_LS_LOG_BASE_TYPE: {
        ret = parse_gc_ls_log_(header);
        break;
      }
      case oceanbase::logservice::ObLogBaseType::MAJOR_FREEZE_LOG_BASE_TYPE: {
        ret = parse_major_freeze_log_();
        break;
      }
      case oceanbase::logservice::ObLogBaseType::PRIMARY_LS_SERVICE_LOG_BASE_TYPE: {
        ret = parse_primary_ls_service_log_();
        break;
      }
      case oceanbase::logservice::ObLogBaseType::RECOVERY_LS_SERVICE_LOG_BASE_TYPE: {
        ret = parse_recovery_ls_service_log_();
        break;
      }
      case oceanbase::logservice::ObLogBaseType::STANDBY_TIMESTAMP_LOG_BASE_TYPE: {
        ret = parse_standby_timestamp_log_();
        break;
      }
      case oceanbase::logservice::ObLogBaseType::GAIS_LOG_BASE_TYPE: {
        ret = parse_gais_log_();
        break;
      }
      case oceanbase::logservice::ObLogBaseType::DATA_DICT_LOG_BASE_TYPE: {
        ret = parse_data_dict_log_();
        break;
      }
      case oceanbase::logservice::ObLogBaseType::RESERVED_SNAPSHOT_LOG_BASE_TYPE: {
        ret = parse_reserved_snapshot_log_();
        break;
      }
      case oceanbase::logservice::ObLogBaseType::MEDIUM_COMPACTION_LOG_BASE_TYPE: {
        ret = parse_medium_log_();
        break;
      }
      case oceanbase::logservice::ObLogBaseType::DUP_TABLE_LOG_BASE_TYPE: {
        ret = parse_dup_table_log_();
        break;
      }

      default: {
        fprintf(stdout, "  Unknown Base Log Type : %d\n", header.get_log_type());
        LOG_WARN("don't support this log type", K(header.get_log_type()));
        ret = OB_NOT_SUPPORTED;
      }
    }
  }
  return ret;
}


int ObAdminParserLogEntry::dump_tx_id_ts_(ObAdminLogDumperInterface *writer_ptr,
                                          int64_t tx_id,
                                          bool &has_dumped_tx_id)
{
  int ret = OB_SUCCESS;
  if (!has_dumped_tx_id) {
    writer_ptr->start_object();
    writer_ptr->dump_key("TxID");
    writer_ptr->dump_int64(tx_id);
    writer_ptr->dump_key("log_ts");
    writer_ptr->dump_int64(scn_val_);
    has_dumped_tx_id = true;
  }
  return ret;
}

int ObAdminParserLogEntry::parse_trans_redo_log_(ObTxLogBlock &tx_log_block,
                                                 TxID tx_id,
                                                 bool &has_dumped_tx_id)
{
  int ret = OB_SUCCESS;
  ObTxRedoLogTempRef temp_ref;
  ObTxRedoLog redolog(temp_ref);
  memtable::ObMemtableMutatorIterator mmi;
  share::SCN scn;
  str_arg_.log_stat_->total_tx_redo_log_count_++;
  if (OB_FAIL(tx_log_block.deserialize_log_body(redolog))) {
    LOG_WARN("tx_log_block.deserialize_log_body failed", K(ret), K(redolog));
  } else if (OB_FAIL(scn.convert_for_logservice(scn_val_))) {
    LOG_WARN("failed to convert", K(ret), K(scn_val_));
  } else if (OB_FAIL(redolog.ob_admin_dump(&mmi, str_arg_, block_name_, lsn_, tx_id, scn, has_dumped_tx_id))) {
    LOG_WARN("get mutator json string failed", K(block_name_), K(lsn_), K(tx_id), K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObAdminParserLogEntry::prepare_log_buf_(ObLogBaseHeader &header)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_LOG_STORAGE_COMPRESS
  if (header.is_compressed()) {
    LogCompressedPayloadHeader com_header;
    int64_t decompressed_len = 0;
    const int64_t header_len = pos_;
    int64_t local_pos = 0;
    if (OB_FAIL(logservice::decompress(buf_ + pos_, buf_len_ - pos_, str_arg_.decompress_buf_ + header_len,
                                       str_arg_.decompress_buf_len_- header_len, decompressed_len))) {
      LOG_ERROR("failed to decompress", K(header), K(entry_));
    } else if (OB_FAIL(header.serialize(str_arg_.decompress_buf_, header_len, local_pos))){
      LOG_ERROR("failed to serialize", K(header));
    } else if (OB_UNLIKELY(header_len != local_pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("succ to decompress", K(header), K(local_pos), K(header_len));
    } else {
      LOG_INFO("succ to decompress", K(header), K(entry_));
      buf_ = str_arg_.decompress_buf_;
      buf_len_ = decompressed_len + local_pos;
    }
  }
#endif
  return ret;
}
}//end of namespace tools
}//end of namespace oceanbase
