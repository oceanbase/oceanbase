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

#ifndef OCEANBASE_TRANSACTION_OB_TRANS_LOG_
#define OCEANBASE_TRANSACTION_OB_TRANS_LOG_

#include <stdint.h>
#include "share/ob_define.h"
#include "share/config/ob_server_config.h"
#include "storage/ob_storage_log_type.h"
#include "ob_trans_define.h"
#include "share/ob_cluster_version.h"
#include "ob_clog_encrypt_info.h"
#include "storage/tx/ob_trans_factory.h"

namespace oceanbase
{
namespace common
{
class ObDataBuffer;
}
namespace transaction
{

static const int64_t LOG_VERSION_0 = 0;
// for replication group
static const int64_t LOG_VERSION_1 = 1;

class ObTransLog
{
  OB_UNIS_VERSION(1);
public:
  ObTransLog()
    : log_type_(storage::OB_LOG_UNKNOWN),
      trans_id_(),
      cluster_id_(0),
      cluster_version_(0) {}
  ObTransLog(const int64_t log_type,
             const ObTransID &trans_id,
             const uint64_t cluster_id,
             const uint64_t cluster_version)
    : log_type_(log_type),
      trans_id_(trans_id),
      cluster_id_(cluster_id),
      cluster_version_(cluster_version) {}
  virtual ~ObTransLog() {}
  int init(const int64_t log_type,
      const ObTransID &trans_id, const uint64_t cluster_id, const uint64_t cluster_version);
public:
  int64_t get_log_type() const { return log_type_; }
  const ObTransID &get_trans_id() const { return trans_id_; }
  uint64_t get_cluster_id() const { return cluster_id_; }
  uint64_t get_cluster_version() const { return cluster_version_; }
  // Update tenant_id for physical backup and restore
  VIRTUAL_TO_STRING_KV(K_(log_type),  K_(trans_id), K_(cluster_id), K_(cluster_version));
private:
  DISALLOW_COPY_AND_ASSIGN(ObTransLog);
protected:
  int64_t log_type_;
  ObTransID trans_id_;
  uint64_t cluster_id_;
  uint64_t cluster_version_;
};

class ObTransMutator : public common::ObDataBuffer
{
  OB_UNIS_VERSION(1);
public:
  ObTransMutator() : mutator_buf_(NULL), use_mutator_buf_(true) { reset(); }
  ~ObTransMutator() { destroy(); }
  int init(const bool use_mutator_buf = true) {return 0;}
  void reset() { }
  void destroy() { reset(); }
public:
  MutatorBuf *get_mutator_buf() { return mutator_buf_; }
  bool get_use_mutator_buf() const { return use_mutator_buf_; }
  int assign(const ObTransMutator &src) { return 0;}
  int assign(const char *data, const int64_t size) { return 0; }

  TO_STRING_KV(KP_(mutator_buf), K_(use_mutator_buf), K_(data), K_(position), K_(capacity));
private:
  DISALLOW_COPY_AND_ASSIGN(ObTransMutator);
private:
  MutatorBuf *mutator_buf_;
  // If MutatorBuf is not used to store data, data will not be copied during deserialization,
  // and the data pointer directly points to the deserialized buffer.
  // Whether to use MutatorBuf to store data
  bool use_mutator_buf_;
};

class ObTransMutatorIterator
{
public:
  ObTransMutatorIterator() : ptr_(NULL), size_(0), pos_(0) {}
  ~ObTransMutatorIterator() {}
  int init(const char *ptr, const int64_t size);
  void reset();
private:
  const char *ptr_;
  int64_t size_;
  int64_t pos_;
};

class ObTransRedoLogHelper
{
public:
  ObTransRedoLogHelper() {}
  ~ObTransRedoLogHelper() {}
public:
};

class ObTransRedoLog : public ObTransLog
{
  OB_UNIS_VERSION_V(1);
public:
  ObTransRedoLog(ObTransRedoLogHelper &helper)
    : ObTransLog() {}

  ~ObTransRedoLog() {}
  int init();
public:
  bool is_last() const { return false; }
  const ObTransMutator &get_mutator() const { return mutator_; }

  VIRTUAL_TO_STRING_KV(K_(log_type));
private:
  DISALLOW_COPY_AND_ASSIGN(ObTransRedoLog);
private:
  ObTransMutator mutator_;
};

class ObTransRecordLogHelper
{
public:
  ObTransRecordLogHelper()
    : prev_record_log_id_(0) {}
  ~ObTransRecordLogHelper() {}
public:
  uint64_t prev_record_log_id_;
};

class ObTransRecordLog : public ObTransLog 
{
  OB_UNIS_VERSION_V(1);
public:
  ObTransRecordLog(ObTransRecordLogHelper &helper)
    : ObTransLog(),
      prev_record_log_id_(helper.prev_record_log_id_),
      prev_log_ids_(ObModIds::OB_TRANS_REDO_LOG_ID_ARRAY, OB_MALLOC_NORMAL_BLOCK_SIZE) {}

  ObTransRecordLog(const int64_t log_type,
      ObTransID &trans_id, const uint64_t cluster_id, 
      const uint64_t cluster_version, const uint64_t prev_record_log_id)
    : ObTransLog(log_type, trans_id, cluster_id, cluster_version),
      prev_record_log_id_(prev_record_log_id), 
      prev_log_ids_(ObModIds::OB_TRANS_REDO_LOG_ID_ARRAY, OB_MALLOC_NORMAL_BLOCK_SIZE) {}
  ~ObTransRecordLog() {}
public:
  uint64_t get_prev_record_log_id() const { return prev_record_log_id_; };
  uint64_t get_log_ids_count() const { return prev_log_ids_.count(); };
  uint64_t get_log_id(int i) const { return prev_log_ids_.at(i); };
  void add_log_id(const uint64_t log_id) { }

  VIRTUAL_TO_STRING_KV(K_(log_type));
private:
  DISALLOW_COPY_AND_ASSIGN(ObTransRecordLog);
private:
  // the log_id of the previous record log
  uint64_t prev_record_log_id_;
  // the array storing all redo log_ids since the last checkpoint  
  ObRedoLogIdArray prev_log_ids_;
};

class ObPartitionLogInfo final
{
  OB_UNIS_VERSION(1);
public:
  ObPartitionLogInfo() : log_id_(0), log_timestamp_(0) {}
  ObPartitionLogInfo(const int64_t log_id, const int64_t log_timestamp) :
      log_id_(log_id), log_timestamp_(log_timestamp) { }
  int64_t get_log_id() const { return log_id_; }
  int64_t get_log_timestamp() const { return log_timestamp_; }
  bool is_valid() const { return log_id_ > 0 && log_timestamp_ > 0; }
  TO_STRING_KV(K_(log_id), K_(log_timestamp));
private:
  int64_t log_id_;
  int64_t log_timestamp_;
};
typedef common::ObSEArray<ObPartitionLogInfo, 10, TransModulePageAllocator> PartitionLogInfoArray;
class ObTransPrepareLogHelper
{
public:
  ObTransPrepareLogHelper() {}
  ~ObTransPrepareLogHelper() {}
public:
};

class ObTransPrepareLog : public ObTransLog
{
  OB_UNIS_VERSION(1);
public:

  ObTransPrepareLog(ObTransPrepareLogHelper &helper)
    : ObTransLog() {}

  ~ObTransPrepareLog() {}
public:

  TO_STRING_KV(K_(log_type));
private:
  DISALLOW_COPY_AND_ASSIGN(ObTransPrepareLog);
private:
};

class ObTransCommitLog : public ObTransLog
{
  OB_UNIS_VERSION(1);
public:
  ~ObTransCommitLog() {}
public:
  TO_STRING_KV(K_(log_type));
private:
  DISALLOW_COPY_AND_ASSIGN(ObTransCommitLog);
private:
};

class ObTransPreCommitLog : public ObTransLog
{
  OB_UNIS_VERSION(1);
public:
  ObTransPreCommitLog()
    : ObTransLog() {}
  ~ObTransPreCommitLog() {}
  share::SCN get_publish_version() const { return publish_version_; }
  bool is_valid() const;
public:
  TO_STRING_KV(K_(log_type));
private:
  share::SCN publish_version_;
};

class ObTransAbortLog : public ObTransLog
{
  OB_UNIS_VERSION(1);
public:
  ObTransAbortLog()
    : ObTransLog() {}
  ~ObTransAbortLog() {}
  bool is_valid() const;
  TO_STRING_KV(K_(log_type));
private:
  DISALLOW_COPY_AND_ASSIGN(ObTransAbortLog);
private:
};

class ObTransClearLog : public ObTransLog
{
  OB_UNIS_VERSION(1);
public:
  ObTransClearLog() : ObTransLog() {}
  ~ObTransClearLog() {}
private:
  DISALLOW_COPY_AND_ASSIGN(ObTransClearLog);
public:
  bool is_valid() const;
};

class ObSpTransRedoLogHelper
{
public:
  ObSpTransRedoLogHelper() : clog_encrypt_info_() {}
  ~ObSpTransRedoLogHelper() {}
public:
  ObCLogEncryptInfo clog_encrypt_info_;
};

class ObSpTransRedoLog : public ObTransLog
{
  OB_UNIS_VERSION(1);
public:
  ObSpTransRedoLog()
    : ObTransLog() {}
  ObSpTransRedoLog(ObSpTransRedoLogHelper &helper)
    : ObTransLog() {}
  ~ObSpTransRedoLog() {}
public:
  VIRTUAL_TO_STRING_KV(K_(log_type));
private:
  DISALLOW_COPY_AND_ASSIGN(ObSpTransRedoLog);
protected:
};

class ObSpTransCommitLogHelper : public ObSpTransRedoLogHelper
{
public:
  ObSpTransCommitLogHelper() : ObSpTransRedoLogHelper() {}
  ~ObSpTransCommitLogHelper() {}
};

class ObSpTransCommitLog : public ObSpTransRedoLog
{
  OB_UNIS_VERSION(1);

public:
  ObSpTransCommitLog()
    : ObSpTransRedoLog() {}
  ObSpTransCommitLog(ObSpTransCommitLogHelper &helper)
    : ObSpTransRedoLog(helper) {}
  ~ObSpTransCommitLog() {}
public:
  TO_STRING_KV(K_(log_type));
private:
  DISALLOW_COPY_AND_ASSIGN(ObSpTransCommitLog);
private:
};

class ObSpTransAbortLog : public ObTransLog
{
  OB_UNIS_VERSION(1);
public:
  ObSpTransAbortLog() : ObTransLog() {}
  ~ObSpTransAbortLog() {}
public:
  bool is_valid() const;
  TO_STRING_KV(K_(log_type));
private:
  DISALLOW_COPY_AND_ASSIGN(ObSpTransAbortLog);
};

class ObCheckpointLog
{
  OB_UNIS_VERSION(1);
public:
  ObCheckpointLog() : checkpoint_(0) {}
  ~ObCheckpointLog() {}
  int init(const int64_t checkpoint);
public:
  int64_t get_checkpoint() const { return checkpoint_; }
  bool is_valid() const;
  TO_STRING_KV(K_(checkpoint));
private:
  int64_t checkpoint_;
};

class ObTransMutatorLogHelper
{
public:
  ObTransMutatorLogHelper() : clog_encrypt_info_() {}
  ~ObTransMutatorLogHelper() {}
public:
  ObCLogEncryptInfo clog_encrypt_info_;
};

class ObTransMutatorLog : public ObTransLog
{
  OB_UNIS_VERSION(1);
public:
  ObTransMutatorLog()
    : ObTransLog(),
      tenant_id_(common::OB_INVALID_TENANT_ID),
      trans_expired_time_(0),
      trans_param_(),
      log_no_(0),
      mutator_(),
      prev_trans_arr_(),
      can_elr_(false),
      cluster_version_(0),
      clog_encrypt_info_(nullptr) {}
  ObTransMutatorLog(ObTransMutatorLogHelper &helper)
    : ObTransLog(),
      tenant_id_(common::OB_INVALID_TENANT_ID),
      trans_expired_time_(0),
      trans_param_(),
      log_no_(0),
      mutator_(),
      prev_trans_arr_(),
      can_elr_(false),
      cluster_version_(0),
      clog_encrypt_info_(&(helper.clog_encrypt_info_)) {}
  ~ObTransMutatorLog() { destroy(); }
  void destroy() {}
public:
  uint64_t get_tenant_id() const { return tenant_id_; }
  int64_t get_trans_expired_time() const { return trans_expired_time_; }
  const ObStartTransParam &get_trans_param() const { return trans_param_; }
  int64_t get_log_no() const { return log_no_; }
  const ObTransMutator &get_mutator() const { return mutator_; }
  ObTransMutator &get_mutator() { return mutator_; }
  const ObElrTransInfoArray &get_prev_trans_arr() const { return prev_trans_arr_; }
  bool is_can_elr() const { return can_elr_; }
  uint64_t get_cluster_version() const { return cluster_version_; }
  const ObCLogEncryptInfo* get_clog_encrypt_info() const { return clog_encrypt_info_; }
  ObCLogEncryptInfo* get_clog_encrypt_info() { return clog_encrypt_info_; }
  int init_for_deserialize(const bool use_mutator_buf = true) { return 0;}
  int replace_encrypt_info_tenant_id(const uint64_t real_tenant_id) { return 0;}
  int decrypt_table_key() { return 0;}
  int decrypt(char *buf, const int64_t buf_size, int64_t &decrypt_len,
              const bool need_extract_encrypt_meta,
              share::ObEncryptMeta &final_encrypt_meta,
              share::ObCLogEncryptStatMap &encrypt_stat_map) {return 0;};
  int archive_encrypt(const uint64_t origin_tenant_id,
                      char *buf,
                      const int64_t buf_size,
                      int64_t &encrypt_len) { return 0;}
public:
  TO_STRING_KV(K_(log_type));
private:
  int64_t tenant_id_;
  int64_t trans_expired_time_;
  ObStartTransParam trans_param_;
  int64_t log_no_;
  ObTransMutator mutator_;
  ObElrTransInfoArray prev_trans_arr_;
  bool can_elr_;
  uint64_t cluster_version_;
  ObCLogEncryptInfo *clog_encrypt_info_;
};

class ObTransMutatorAbortLog : public ObTransLog
{
  OB_UNIS_VERSION(1);
public:
  ObTransMutatorAbortLog() {}
  ~ObTransMutatorAbortLog() {}
  TO_STRING_KV(K_(log_type));
};

// Provide related methods for log analysis
class ObTransLogParseUtils
{
public:
  static int parse_redo_prepare_log(uint64_t &log_id,
      int64_t &log_timestamp, ObTransID &trans_id) { return 0;}
};

} // transaction
} // oceanbase
#endif //OCEANBASE_TRANSACTION_OB_TRANS_LOG_
