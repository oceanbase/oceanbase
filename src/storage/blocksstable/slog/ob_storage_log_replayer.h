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

#ifndef OB_STORAGE_LOG_REPLAYER_H_
#define OB_STORAGE_LOG_REPLAYER_H_

#include "ob_storage_log_struct.h"
#include "common/log/ob_log_cursor.h"

#define PrintSLogEntry(entry_class)                                                \
  entry_class entry;                                                               \
  if (OB_FAIL(entry.deserialize(buf, len, pos))) {                                 \
    LOG_WARN("Fail to deserialize " #entry_class, K(ret));                         \
  } else if (0 > fprintf(stream, "\n" #entry_class "\n%s\n", to_cstring(entry))) { \
    ret = OB_IO_ERROR;                                                             \
    LOG_WARN("failed to print " #entry_class, K(ret), K(entry));                   \
  }

namespace oceanbase {
namespace blocksstable {

class ObISLogFilter {
public:
  struct Param final {
  public:
    Param() : entry_(nullptr), subcmd_(0), attr_()
    {}
    ~Param() = default;
    ObIBaseStorageLogEntry* entry_;
    int64_t subcmd_;
    ObStorageLogAttribute attr_;
  };
  ObISLogFilter() = default;
  virtual ~ObISLogFilter() = default;
  virtual int filter(const Param& param, bool& is_filtered) const = 0;
};

class ObReplayServerSLogFilter : public ObISLogFilter {
public:
  ObReplayServerSLogFilter() = default;
  virtual ~ObReplayServerSLogFilter() = default;
  virtual int filter(const ObISLogFilter::Param& param, bool& is_filtered) const override;
};

class ObNotReplaySuperBlockAndConfigMetaSLogFilter : public ObISLogFilter {
public:
  ObNotReplaySuperBlockAndConfigMetaSLogFilter() = default;
  virtual ~ObNotReplaySuperBlockAndConfigMetaSLogFilter() = default;
  virtual int filter(const ObISLogFilter::Param& param, bool& is_filtered) const override;
};

struct ObStorageLogCommittedTrans final {
public:
  ObStorageLogCommittedTrans() : commit_lsn_(0), valid_record_(NULL)
  {}
  int64_t commit_lsn_;
  struct ObStorageLogValidRecordEntry* valid_record_;
};

struct ObRedoModuleReplayParam final {
public:
  ObRedoModuleReplayParam();
  ~ObRedoModuleReplayParam() = default;
  bool is_valid() const
  {
    return log_seq_num_ >= 0 && nullptr != buf_ && buf_len_ > 0 && subcmd_ >= 0;
  }
  TO_STRING_KV(K_(log_seq_num), K_(subcmd), K_(file_id), KP_(buf), K_(buf_len));
  int64_t log_seq_num_;
  int64_t subcmd_;
  int64_t file_id_;
  const char* buf_;
  int64_t buf_len_;
};

// The interface of some modules need to write redo log.
class ObIRedoModule {
public:
  ObIRedoModule() : enable_write_log_(false), filter_(nullptr){};
  virtual ~ObIRedoModule() = default;

  // replay the redo log.
  virtual int replay(const ObRedoModuleReplayParam& param) = 0;
  // parse the redo log to stream
  virtual int parse(const int64_t subcmd, const char* buf, const int64_t len, FILE* stream) = 0;
  virtual int enable_write_log()
  {
    enable_write_log_ = true;
    return common::OB_SUCCESS;
  }

  int set_filter(const ObISLogFilter* filter)
  {
    filter_ = filter;
    return common::OB_SUCCESS;
  }
  const ObISLogFilter* get_filter() const
  {
    return filter_;
  }
  static int parse_valid_record(const char* log_data, const int64_t data_len, common::ObIAllocator& allocator,
      ObStorageLogValidRecordEntry*& precord);
  inline static int64_t gen_subcmd(const enum ObRedoLogMainType subcmd_prefix, const int32_t subcmd_suffix);
  inline static void parse_subcmd(const int64_t subcmd, enum ObRedoLogMainType& subcmd_prefix, int32_t& subcmd_suffix);

protected:
  bool enable_write_log_;
  const ObISLogFilter* filter_;
};

class ObStorageLogCommittedTransGetter final {
public:
  ObStorageLogCommittedTransGetter();
  ~ObStorageLogCommittedTransGetter();
  int init(const char* log_dir, const common::ObLogCursor& replay_start_cursor);
  int get_trans(const int64_t trans_id, ObStorageLogCommittedTrans& trans) const;
  OB_INLINE int64_t get_min_log_file_id() const
  {
    return min_log_file_id_;
  }
  OB_INLINE bool is_empty_dir() const
  {
    return is_empty_dir_;
  }
  void destroy();

private:
  int load_committed_trans(const char* log_dir, const common::ObLogCursor& replay_start_cursor);
  int revise_log(const char* log_dir);

private:
  static const int64_t RECOVERY_TRANS_CNT = 10000;
  common::ObArenaAllocator allocator_;
  common::hash::ObHashMap<int64_t, ObStorageLogCommittedTrans> committed_trans_map_;
  int64_t min_log_file_id_;
  bool is_empty_dir_;
  bool is_inited_;
};

class ObStorageLogReplayer {
public:
  ObStorageLogReplayer();
  virtual ~ObStorageLogReplayer();

  virtual int init(
      const char* log_dir, ObISLogFilter* filter_before_parse = nullptr, ObISLogFilter* filter_after_parse = nullptr);
  virtual void destroy();

  // NOT thread safe.
  // redo module register.
  virtual int register_redo_module(enum ObRedoLogMainType cmd_prefix, ObIRedoModule* module);
  virtual int unregister_redo_module(enum ObRedoLogMainType cmd_prefix);

  // NOT thread safe.
  // read checkpoints and replay redo logs
  virtual int replay(
      const common::ObLogCursor& replay_start_cursor, const ObStorageLogCommittedTransGetter& committed_trans_getter);
  // NOTICE only use in ob_admin
  virtual int replay_in_tools(
      const common::ObLogCursor& replay_start_cursor, const ObStorageLogCommittedTransGetter& committed_trans_getter);

  // NOT thread safe
  virtual int get_active_cursor(common::ObLogCursor& log_cursor);

  const char* get_log_dir() const
  {
    return log_dir_;
  }

protected:
  virtual int replay_over()
  {
    return common::OB_SUCCESS;
  };

private:
  int replay_(
      const common::ObLogCursor& replay_start_cursor, const ObStorageLogCommittedTransGetter& committed_trans_getter);
  int replay_after_ckpt(
      const common::ObLogCursor& replay_start_cursor, const ObStorageLogCommittedTransGetter& committed_trans_getter);
  int filter_single_log(const ObBaseStorageLogHeader& log_header, bool& is_filtered);

protected:
  bool is_inited_;
  int64_t trans_id_;
  uint64_t min_log_file_id_;
  common::ObLogCursor finish_cursor_;
  common::ObArenaAllocator allocator_;
  char log_dir_[common::OB_MAX_FILE_NAME_LENGTH];
  ObIRedoModule* redo_modules_[OB_REDO_LOG_MAX];
  ObISLogFilter* filter_before_parse_;
  ObISLogFilter* filter_after_parse_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObStorageLogReplayer);
};

inline int64_t ObIRedoModule::gen_subcmd(const enum ObRedoLogMainType subcmd_prefix, const int32_t subcmd_suffix)
{
  int64_t subcmd = static_cast<int64_t>(subcmd_prefix);
  subcmd = (subcmd << 32) + subcmd_suffix;
  return subcmd;
}

inline void ObIRedoModule::parse_subcmd(
    const int64_t subcmd, enum ObRedoLogMainType& subcmd_prefix, int32_t& subcmd_suffix)
{
  subcmd_prefix = static_cast<enum ObRedoLogMainType>(subcmd >> 32);
  subcmd_suffix = static_cast<int32_t>(subcmd & 0x00000000FFFFFFFF);
}
}  // namespace blocksstable
}  // namespace oceanbase

#endif /* OB_STORAGE_LOG_REPLAYER_H_ */
