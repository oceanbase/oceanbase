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

#define USING_LOG_PREFIX STORAGE_REDO
#include "ob_storage_log_replayer.h"
#include "share/ob_force_print_log.h"
#include "share/redolog/ob_log_store_factory.h"
#include "storage/blocksstable/slog/ob_storage_log_reader.h"
#include "storage/ob_file_system_router.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace storage;
namespace blocksstable {

ObRedoModuleReplayParam::ObRedoModuleReplayParam()
    : log_seq_num_(0), subcmd_(0), file_id_(0), buf_(nullptr), buf_len_(0)
{}

int ObReplayServerSLogFilter::filter(const ObISLogFilter::Param& param, bool& is_filtered) const
{
  int ret = OB_SUCCESS;
  enum ObRedoLogMainType main_type = OB_REDO_LOG_MAX;
  int32_t sub_type = 0;
  is_filtered = false;
  ObIRedoModule::parse_subcmd(param.subcmd_, main_type, sub_type);
  is_filtered = OB_REDO_LOG_TENANT_FILE != main_type && OB_REDO_LOG_TENANT_CONFIG != main_type;
  return ret;
}

int ObNotReplaySuperBlockAndConfigMetaSLogFilter::filter(const ObISLogFilter::Param& param, bool& is_filtered) const
{
  int ret = OB_SUCCESS;
  enum ObRedoLogMainType main_type = OB_REDO_LOG_MAX;
  int32_t sub_type = 0;
  is_filtered = false;
  ObIRedoModule::parse_subcmd(param.subcmd_, main_type, sub_type);
  is_filtered = OB_REDO_LOG_TENANT_FILE == main_type || OB_REDO_LOG_TENANT_CONFIG == main_type;
  return ret;
}

int ObIRedoModule::parse_valid_record(const char* log_data, const int64_t data_len, common::ObIAllocator& allocator,
    ObStorageLogValidRecordEntry*& precord)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char* copy_data = NULL;
  ObStorageLogValidRecordEntry record;
  precord = NULL;

  if (NULL == log_data || data_len < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", K(ret), KP(log_data), K(data_len));
  } else if (OB_FAIL(record.deserialize(log_data, data_len, pos))) {
    LOG_WARN("fail to deserialize valid record, ", K(ret), K(data_len), K(pos));
  } else if (record.rollback_count_ > 0) {
    if (nullptr == (copy_data = static_cast<char*>(allocator.alloc(sizeof(ObStorageLogValidRecordEntry))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("allocate memory for valid record failed.", K(ret), K(sizeof(ObStorageLogValidRecordEntry)), K(pos));
    } else {
      precord = new (copy_data) ObStorageLogValidRecordEntry();
      *precord = record;
    }
  }

  return ret;
}

ObStorageLogCommittedTransGetter::ObStorageLogCommittedTransGetter()
    : allocator_(common::ObModIds::OB_CS_COMMIT_LOG),
      committed_trans_map_(),
      min_log_file_id_(0),
      is_empty_dir_(false),
      is_inited_(false)
{}

ObStorageLogCommittedTransGetter::~ObStorageLogCommittedTransGetter()
{
  destroy();
}

void ObStorageLogCommittedTransGetter::destroy()
{
  committed_trans_map_.destroy();
  is_empty_dir_ = false;
  is_inited_ = false;
}

int ObStorageLogCommittedTransGetter::init(const char* log_dir, const ObLogCursor& replay_start_cursor)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObStorageLogCommittedTransGetter has already been inited", K(ret));
  } else if (OB_UNLIKELY(nullptr == log_dir || !replay_start_cursor.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(log_dir), K(replay_start_cursor));
  } else if (OB_FAIL(load_committed_trans(log_dir, replay_start_cursor))) {
    LOG_WARN("fail to load committed trans", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObStorageLogCommittedTransGetter::revise_log(const char* log_dir)
{
  int ret = OB_SUCCESS;
  ObILogFileStore* store = NULL;
  uint32_t min_log_id = 0;
  uint32_t max_log_id = 0;
  ObStorageLogReader slog_reader;

  if (OB_ISNULL(log_dir)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", K(ret), KP(log_dir));
  } else if (nullptr == (store = ObLogStoreFactory::create(
                             log_dir, 256 << 20 /*file_size*/, clog::ObLogWritePoolType::SLOG_WRITE_POOL))) {
    ret = OB_INIT_FAIL;
    LOG_WARN("Fail to init store, ", K(ret));
  } else if (OB_FAIL(store->get_file_id_range(min_log_id, max_log_id)) && OB_ENTRY_NOT_EXIST != ret) {
    LOG_WARN("Fail to get log id range.", K(ret));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    // empty dir, do nothing
    ret = OB_SUCCESS;
    min_log_id = 1;
    is_empty_dir_ = true;
  } else if (OB_FAIL(slog_reader.init(log_dir, max_log_id, 0))) {
    LOG_WARN("Fail to init log_reader, ", K(ret));
  } else if (OB_FAIL(slog_reader.revise_log(false))) {
    LOG_WARN("Fail to revise log, ", K(ret));
  }

  if (OB_SUCC(ret)) {
    min_log_file_id_ = min_log_id;
  }
  return ret;
}

int ObStorageLogCommittedTransGetter::load_committed_trans(const char* log_dir, const ObLogCursor& replay_start_cursor)
{
  int ret = OB_SUCCESS;
  ObStorageLogReader slog_reader;
  hash::ObHashSet<int64_t> started_trans_ids;

  FLOG_INFO("start construct commit trans map");
  if (nullptr == log_dir || !replay_start_cursor.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", K(ret), KP(log_dir), K(replay_start_cursor));
  } else if (OB_FAIL(revise_log(log_dir))) {
    LOG_WARN("Fail to revise log ", K(ret), KP(log_dir));
  } else if (is_empty_dir_) {
    // empty dir, do nothing
    FLOG_INFO("empty directory, skip get committed trans", K(log_dir));
  } else if (OB_FAIL(started_trans_ids.create(RECOVERY_TRANS_CNT))) {
    LOG_WARN("Fail to create started_trans_ids map, ", K(ret));
  } else if (OB_FAIL(committed_trans_map_.create(RECOVERY_TRANS_CNT, ObModIds::OB_CS_COMMIT_LOG))) {
    LOG_WARN("fail to create committed trans map", K(ret));
  } else if (OB_FAIL(slog_reader.init(log_dir,
                 replay_start_cursor.file_id_,
                 replay_start_cursor.log_id_ > 0 ? replay_start_cursor.log_id_ - 1 : 0))) {
    LOG_WARN("Fail to init log_reader, ", K(log_dir), K(ret));
  } else {
    LogCommand cmd;
    uint64_t seq = 0;
    ObBaseStorageLogBuffer log_buffer;
    int64_t data_len = 0;
    ObBaseStorageLogHeader log_header;
    char* log_data = NULL;
    enum ObRedoLogMainType main_type = OB_REDO_LOG_SYS;
    int32_t sub_type = 0;
    ObStorageLogCommittedTrans committed_trans;

    while (OB_SUCC(ret) && OB_SUCC(slog_reader.read_log(cmd, seq, log_data, data_len))) {
      if (OB_LOG_SWITCH_LOG != cmd && OB_LOG_NOP != cmd && OB_LOG_CHECKPOINT != cmd) {
        log_buffer.reset();
        if (OB_FAIL(log_buffer.assign(log_data, data_len))) {
          LOG_WARN("Fail to assign log_buffer, ", K(ret));
        } else {
          while (OB_SUCC(ret) && OB_SUCC(log_buffer.read_log(log_header, log_data))) {
            ObIRedoModule::parse_subcmd(log_header.subcmd_, main_type, sub_type);
            if (OB_REDO_LOG_SYS == main_type) {
              if (OB_REDO_LOG_BEGIN == sub_type) {
                if (OB_FAIL(started_trans_ids.set_refactored(log_header.trans_id_))) {
                  LOG_WARN("Fail to insert to started_trans_ids, ", K(ret));
                }
              } else if (OB_REDO_LOG_COMMIT == sub_type) {
                if (OB_HASH_EXIST == started_trans_ids.exist_refactored(log_header.trans_id_)) {
                  committed_trans.commit_lsn_ = static_cast<int64_t>(seq);
                  if (OB_FAIL(ObIRedoModule::parse_valid_record(
                          log_data, log_header.log_len_, allocator_, committed_trans.valid_record_))) {
                    LOG_WARN("parse valid record from commit entry failed.", K(ret), K(log_header));
                  } else if (OB_FAIL(committed_trans_map_.set_refactored(log_header.trans_id_, committed_trans))) {
                    LOG_WARN("Fail to insert to commited_trans_ids, ", K(ret));
                  }
                } else {
                  // not exist in started trans, ignore it
                }
              } else {
                // ignore
              }
            }
          }

          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          } else if (OB_FAIL(ret)) {
            LOG_WARN("Fail to parse sub log, ", K(ret));
          }
        }
      }
    }

    if (OB_READ_NOTHING == ret) {
      ret = OB_SUCCESS;
    }
  }

  if (started_trans_ids.created()) {
    started_trans_ids.destroy();
  }
  FLOG_INFO("finish construct commit trans map", K(ret), K(committed_trans_map_.size()));

  return ret;
}

int ObStorageLogCommittedTransGetter::get_trans(const int64_t trans_id, ObStorageLogCommittedTrans& trans) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObStorageLogCommittedTransGetter has not been inited", K(ret));
  } else if (OB_FAIL(committed_trans_map_.get_refactored(trans_id, trans))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    }
    LOG_WARN("fail to get from trans map", K(trans_id));
  }
  return ret;
}

ObStorageLogReplayer::ObStorageLogReplayer()
    : is_inited_(false),
      trans_id_(0),
      min_log_file_id_(1),
      finish_cursor_(),
      allocator_(ObModIds::OB_CS_COMMIT_LOG, OB_MALLOC_BIG_BLOCK_SIZE),
      filter_before_parse_(nullptr),
      filter_after_parse_(nullptr)
{
  MEMSET(log_dir_, 0, OB_MAX_FILE_NAME_LENGTH);
  MEMSET(redo_modules_, 0, sizeof(redo_modules_));
}

ObStorageLogReplayer::~ObStorageLogReplayer()
{
  destroy();
}

int ObStorageLogReplayer::init(
    const char* log_dir, ObISLogFilter* filter_before_parse, ObISLogFilter* filter_after_parse)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("The ObStorageLogReplayer has been inited.", K(ret));
  } else if (OB_ISNULL(log_dir)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments, ", K(ret), KP(log_dir));
  } else {
    if (OB_SUCC(ret)) {
      snprintf(log_dir_, OB_MAX_FILE_NAME_LENGTH, "%s", log_dir);
      filter_before_parse_ = filter_before_parse;
      filter_after_parse_ = filter_after_parse;
      is_inited_ = true;
    }
  }

  if (IS_NOT_INIT) {
    destroy();
  }

  return ret;
}

void ObStorageLogReplayer::destroy()
{
  if (IS_INIT) {
    trans_id_ = 0;
    min_log_file_id_ = 1;
    finish_cursor_.reset();
    allocator_.reset();
    MEMSET(log_dir_, 0, sizeof(log_dir_));
    MEMSET(redo_modules_, 0, sizeof(redo_modules_));
    filter_before_parse_ = nullptr;
    filter_after_parse_ = nullptr;
    is_inited_ = false;
  }
}

int ObStorageLogReplayer::register_redo_module(enum ObRedoLogMainType cmd_prefix, ObIRedoModule* module)
{
  int ret = OB_SUCCESS;

  if (OB_REDO_LOG_SYS == cmd_prefix || cmd_prefix >= OB_REDO_LOG_MAX || NULL == module) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments, ", K(ret), K(cmd_prefix), KP(module));
  } else if (NULL != redo_modules_[cmd_prefix]) {
    ret = OB_ENTRY_EXIST;
    LOG_WARN("The cmd_prefix has been registered, ", K(ret), K(cmd_prefix));
  } else {
    redo_modules_[cmd_prefix] = module;
  }

  return ret;
}

int ObStorageLogReplayer::unregister_redo_module(enum ObRedoLogMainType cmd_prefix)
{
  int ret = OB_SUCCESS;

  if (OB_REDO_LOG_SYS == cmd_prefix || cmd_prefix >= OB_REDO_LOG_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments, ", K(ret), K(cmd_prefix));
  } else if (NULL == redo_modules_[cmd_prefix]) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("The cmd_prefix has not been registered, ", K(ret), K(cmd_prefix));
  } else {
    redo_modules_[cmd_prefix] = NULL;
  }

  return ret;
}

int ObStorageLogReplayer::replay(
    const ObLogCursor& replay_start_cursor, const ObStorageLogCommittedTransGetter& committed_trans_getter)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObStorageLogReplayer has not been inited.", K(ret));
  } else if (OB_FAIL(replay_(replay_start_cursor, committed_trans_getter))) {
    LOG_WARN("Failed to replay", K(ret));
  } else if (OB_FAIL(replay_over())) {
    LOG_WARN("set all module replay over status failed.", K(ret));
  } else {
    LOG_INFO("Replay done!", K(min_log_file_id_), K(replay_start_cursor), K(finish_cursor_), K(trans_id_));
  }

  return ret;
}

int ObStorageLogReplayer::replay_in_tools(
    const common::ObLogCursor& replay_start_cursor, const ObStorageLogCommittedTransGetter& committed_trans_getter)
{
  int ret = OB_SUCCESS;
  ret = replay_(replay_start_cursor, committed_trans_getter);
  return ret;
}

int ObStorageLogReplayer::replay_(
    const common::ObLogCursor& replay_start_cursor, const ObStorageLogCommittedTransGetter& committed_trans_getter)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObStorageLogReplayer has not been inited.", K(ret));
  } else if (committed_trans_getter.is_empty_dir()) {
    // don't has log
    ret = OB_SUCCESS;
    finish_cursor_.file_id_ = 1;
    finish_cursor_.log_id_ = 1;
    finish_cursor_.offset_ = 0;
    trans_id_ = 0;
    LOG_INFO("There aren't redo logs.", K(finish_cursor_), K(trans_id_));
  } else {
    // has log, replay them
    LOG_INFO("Start replay static data redo log, ", K(ret));
    min_log_file_id_ = committed_trans_getter.get_min_log_file_id();
    if (OB_FAIL(replay_after_ckpt(replay_start_cursor, committed_trans_getter))) {
      LOG_WARN("Fail to replay after checkpoint, ", K(ret));
    }
  }

  return ret;
}

int ObStorageLogReplayer::get_active_cursor(ObLogCursor& log_cursor)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObStorageLogReplayer has not been inited.", K(ret));
  } else {
    log_cursor = finish_cursor_;
  }
  return ret;
}

int ObStorageLogReplayer::filter_single_log(const ObBaseStorageLogHeader& log_header, bool& is_filtered)
{
  int ret = OB_SUCCESS;
  if (nullptr == filter_before_parse_) {
    is_filtered = false;
  } else {
    ObISLogFilter::Param param;
    param.subcmd_ = log_header.subcmd_;
    param.attr_.tenant_id_ = log_header.tenant_id_;
    param.attr_.data_file_id_ = log_header.data_file_id_;
    if (OB_FAIL(filter_before_parse_->filter(param, is_filtered))) {
      LOG_WARN("fail to filter slog", K(ret));
    }
  }
  return ret;
}

int ObStorageLogReplayer::replay_after_ckpt(
    const ObLogCursor& replay_start_cursor, const ObStorageLogCommittedTransGetter& committed_trans_getter)
{
  int ret = OB_SUCCESS;
  ObStorageLogReader slog_reader;
  uint64_t last_entry_seq = -1;

  LOG_INFO("Start replay static redo log after checkpoint, ", K(replay_start_cursor));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObStorageLogReplayer has not been inited.", K(ret));
  } else if (OB_UNLIKELY(!replay_start_cursor.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", K(replay_start_cursor), K(ret));
  } else if (OB_FAIL(slog_reader.init(log_dir_,
                 replay_start_cursor.file_id_,
                 replay_start_cursor.log_id_ > 0 ? replay_start_cursor.log_id_ - 1 : 0))) {
    LOG_WARN("Fail to init log_reader, ", K(ret));
  } else {
    LogCommand cmd = common::OB_LOG_UNKNOWN;
    uint64_t entry_seq = 0;
    ObBaseStorageLogHeader log_header;
    char* log_data = NULL;
    int64_t data_len = 0;
    enum ObRedoLogMainType main_type = OB_REDO_LOG_SYS;
    int32_t sub_type = 0;
    ObBaseStorageLogBuffer log_buffer;
    ObStorageLogCommittedTrans committed_trans;
    int64_t index = 0;
    ObLogCursor curr_cursor;
    int64_t replay_cost[OB_REDO_LOG_MAX] = {0};
    int64_t replay_cnt[OB_REDO_LOG_MAX] = {0};
    int64_t replay_start_time = 0;

    while (OB_SUCC(ret) && OB_SUCC(slog_reader.read_log(cmd, entry_seq, log_data, data_len))) {
      log_buffer.reset();
      if (last_entry_seq == entry_seq) {
        FLOG_INFO("meet duplicate slog", K(entry_seq), K(cmd));
      }

      if (OB_LOG_SWITCH_LOG == cmd || OB_LOG_NOP == cmd || OB_LOG_CHECKPOINT == cmd) {
        // ignore sys logs
      } else if (OB_FAIL(log_buffer.assign(log_data, data_len))) {
        LOG_WARN("Fail to assign log_buffer, ", K(ret));
      } else {
        while (OB_SUCC(ret) && OB_SUCC(log_buffer.read_log(log_header, log_data))) {
          ObIRedoModule::parse_subcmd(log_header.subcmd_, main_type, sub_type);
          trans_id_ = MAX(log_header.trans_id_, trans_id_);
          bool is_filtered = false;
          index = 0;

          if (OB_REDO_LOG_SYS == main_type) {
            continue;  // skip begin/commit log
          } else if (OB_FAIL(filter_single_log(log_header, is_filtered))) {
            LOG_WARN("check log filtered fail", K(ret), K(log_header));
          } else if (is_filtered) {
            continue;
          } else if (OB_ISNULL(redo_modules_[main_type])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("The redo module has not been registered, ", K(ret), K(main_type));
          } else if (OB_FAIL(committed_trans_getter.get_trans(log_header.trans_id_, committed_trans)) &&
                     OB_ENTRY_NOT_EXIST != ret) {
            LOG_ERROR("Fail to get committed trans id, ", K(ret), K(log_header));
          } else if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS;  // the log hasn't committed, ignore
            continue;
          } else if (nullptr != committed_trans.valid_record_ &&
                     OB_ENTRY_NOT_EXIST == committed_trans.valid_record_->find_extent(log_header.log_seq_, index)) {
            LOG_INFO("current log entry is invalid, cannot replay. ", K(log_header), K(entry_seq));
            continue;
          }

          if (OB_SUCC(ret)) {
            replay_start_time = ObTimeUtility::current_time();
            ObRedoModuleReplayParam replay_param;
            replay_param.log_seq_num_ = committed_trans.commit_lsn_;
            replay_param.subcmd_ = log_header.subcmd_;
            replay_param.buf_ = log_data;
            replay_param.buf_len_ = log_header.log_len_;
            replay_param.file_id_ = log_header.data_file_id_;
            if (OB_FAIL(redo_modules_[main_type]->replay(replay_param))) {
              slog_reader.get_cursor(curr_cursor);
              LOG_ERROR("Fail to replay sub cmd, ",
                  K(ret),
                  K(entry_seq),
                  K(main_type),
                  K(sub_type),
                  K(log_header),
                  K(curr_cursor),
                  K(log_buffer.pos()),
                  K(log_buffer.length()),
                  K(log_buffer.capacity()),
                  K(data_len));
            }
            replay_cost[main_type] += ObTimeUtility::current_time() - replay_start_time;
            replay_cnt[main_type]++;
          }
        }  // while log_buffer.read_log

        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }

      if (OB_SUCC(ret)) {
        last_entry_seq = entry_seq;
      }
    }  // while slog_reader.read_log

    if (OB_READ_NOTHING == ret) {
      ret = OB_SUCCESS;
    }
    FLOG_INFO("slog replay stat: ",
        "partition log cnt",
        replay_cnt[OB_REDO_LOG_PARTITION],
        "partition log cost",
        replay_cost[OB_REDO_LOG_PARTITION],
        "macro block log cnt",
        replay_cnt[OB_REDO_LOG_MACROBLOCK],
        "macro block log cost",
        replay_cost[OB_REDO_LOG_MACROBLOCK],
        "table mgr log cnt",
        replay_cnt[OB_REDO_LOG_TABLE_MGR],
        "table mgr log cost",
        replay_cost[OB_REDO_LOG_TABLE_MGR],
        "tenant config log cnt",
        replay_cnt[OB_REDO_LOG_TENANT_CONFIG],
        "tenant config log cost",
        replay_cost[OB_REDO_LOG_TENANT_CONFIG],
        "tenant file log cnt",
        replay_cnt[OB_REDO_LOG_TENANT_FILE],
        "tenant file log cost",
        replay_cost[OB_REDO_LOG_TENANT_FILE]);
  }

  if (OB_SUCC(ret)) {
    // get write start cursor
    if (-1 != last_entry_seq) {
      if (OB_FAIL(slog_reader.get_next_cursor(finish_cursor_))) {
        LOG_WARN("Fail to get write_start_cursor, ", K(ret));
      }
    } else {
      finish_cursor_.file_id_ = replay_start_cursor.file_id_;
      finish_cursor_.log_id_ = replay_start_cursor.log_id_;
      finish_cursor_.offset_ = replay_start_cursor.offset_;
    }
  }

  return ret;
}

}  // namespace blocksstable
}  // namespace oceanbase
