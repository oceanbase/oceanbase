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

#ifndef LOGSERVICE_LOG_IO_TASK_CB_UTILS_
#define LOGSERVICE_LOG_IO_TASK_CB_UTILS_
#include "lib/ob_define.h"
#include "lib/oblog/ob_log_print_kv.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"               // TO_STRING_KV
#include "log_group_entry_header.h"
#include "share/scn.h"
#include "lsn.h"
#include "palf_base_info.h"
#include "log_meta_info.h"

namespace oceanbase
{
namespace palf
{
struct FlushLogCbCtx
{
  FlushLogCbCtx();
  FlushLogCbCtx(const int64_t log_id, const share::SCN &scn, const LSN &lsn,
                const int64_t &log_proposal_id, const int64_t total_len,
                const int64_t &curr_log_proposal_id, const int64_t begine_ts);
  ~FlushLogCbCtx();
  bool is_valid() const { return true == lsn_.is_valid() && true == scn_.is_valid(); }
  void reset();
  FlushLogCbCtx &operator=(const FlushLogCbCtx &flush_log_cb_ctx);
  TO_STRING_KV(K_(log_id), K_(scn), K_(lsn), K_(log_proposal_id), K_(total_len), K_(curr_proposal_id), K_(begin_ts));
  int64_t log_id_;
  share::SCN scn_;
  LSN lsn_;
  int64_t log_proposal_id_;
  int64_t total_len_;
  int64_t curr_proposal_id_;
  int64_t begin_ts_;
};

struct TruncateLogCbCtx {
  TruncateLogCbCtx(const LSN &lsn);
  TruncateLogCbCtx();
  ~TruncateLogCbCtx();
  bool is_valid() const { return true == lsn_.is_valid();}
  void reset();
  TruncateLogCbCtx &operator=(const TruncateLogCbCtx &truncate_log_cb_ctx);
  TO_STRING_KV(K_(lsn));
  LSN lsn_;
};

struct TruncatePrefixBlocksCbCtx {
  TruncatePrefixBlocksCbCtx(const LSN &lsn);
  TruncatePrefixBlocksCbCtx();
  ~TruncatePrefixBlocksCbCtx();
  bool is_valid() const { return true == lsn_.is_valid();}
  void reset();
  TruncatePrefixBlocksCbCtx& operator=(const TruncatePrefixBlocksCbCtx& truncate_prefix_blocks_ctx);
  TO_STRING_KV(K_(lsn));
  LSN lsn_;
};

struct FlashbackCbCtx {
  FlashbackCbCtx(const share::SCN &flashback_scn);
  FlashbackCbCtx();
  ~FlashbackCbCtx();
  bool is_valid() const { return flashback_scn_.is_valid(); }
  void reset();
  FlashbackCbCtx& operator=(const FlashbackCbCtx& flashback_ctx);
  TO_STRING_KV(K_(flashback_scn));
  share::SCN flashback_scn_;
};

enum MetaType {
  PREPARE_META = 0,
  CHANGE_CONFIG_META = 1,
  MODE_META = 2,
  SNAPSHOT_META = 3,
  REPLICA_PROPERTY_META = 4,
  INVALID_META_TYPE
};

inline const char *meta_type_2_str(const MetaType type)
{
#define EXTRACT_META_TYPE(type_var) ({ case(type_var): return #type_var; })
  switch(type)
  {
    EXTRACT_META_TYPE(PREPARE_META);
    EXTRACT_META_TYPE(CHANGE_CONFIG_META);
    EXTRACT_META_TYPE(MODE_META);
    EXTRACT_META_TYPE(SNAPSHOT_META);
    EXTRACT_META_TYPE(REPLICA_PROPERTY_META);

    default:
      return "Invalid Type";
  }
#undef EXTRACT_META_TYPE
}

struct FlushMetaCbCtx {
  FlushMetaCbCtx();
  ~FlushMetaCbCtx();
  bool is_valid() const { return INVALID_META_TYPE != type_; }
  void reset();
  FlushMetaCbCtx &operator=(const FlushMetaCbCtx &flush_meta_cb_ctx);
  TO_STRING_KV("type", meta_type_2_str(type_), K_(proposal_id), K_(config_version), K_(base_lsn), K_(allow_vote),
      K_(log_mode_meta));
  MetaType type_;
  int64_t proposal_id_;
  LogConfigVersion config_version_;
  LSN base_lsn_;
  bool allow_vote_;
  // log_mode_meta_ is apply-effective, so need record log_mode_meta in FlushCtx
  bool is_applied_mode_meta_;
  LogModeMeta log_mode_meta_;
};


struct PurgeThrottlingCbCtx
{
public:
  PurgeThrottlingCbCtx() : purge_type_(PurgeThrottlingType::INVALID_PURGE_TYPE) {}
  explicit PurgeThrottlingCbCtx(PurgeThrottlingType type) : purge_type_(type) {}
  ~PurgeThrottlingCbCtx() {reset();}
  bool is_valid() const;
  void reset();
  TO_STRING_KV("purge_type", purge_throttling_type_2_str(purge_type_));
public:
  PurgeThrottlingType purge_type_;
};
}
}

#endif
