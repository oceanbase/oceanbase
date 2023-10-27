/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include <gtest/gtest.h>

#define USING_LOG_PREFIX OBLOG
#define private public
#include "logservice/palf/log_entry.h"
#include "storage/tx/ob_tx_log.h"
#undef private
#include "storage/memtable/ob_memtable_mutator.h"
#include "storage/memtable/ob_memtable_context.h"
#include "storage/tx/ob_clog_encrypt_info.h"
#include "logservice/libobcdc/src/ob_cdc_define.h"
#include "logservice/libobcdc/src/ob_log_utils.h"
#include "logservice/libobcdc/src/ob_small_arena.h"
#include "logservice/ob_log_base_header.h"
#include "logservice/palf/log_define.h"
#include "logservice/palf/log_entry_header.h"
#include "storage/memtable/utils_rowkey_builder.h"

namespace oceanbase
{
using namespace palf;
using namespace memtable;
using namespace transaction;
using namespace libobcdc;

namespace unittest
{

class ObTxLogBlockBuilder
{
public:
  ObTxLogBlockBuilder(const int64_t tx_id, const uint64_t cluster_id)
    : tx_id_(tx_id), cluster_id_(cluster_id), log_entry_no_(0), tx_log_block_() {}
  ~ObTxLogBlockBuilder() {}
  public:
  int next_log_block();
  int fill_redo(ObTxRedoLog &redo_log);
  template <typename T> int fill_tx_log_except_redo(T &tx_log_body)
  {
    return tx_log_block_.add_new_log(tx_log_body);
  }
  ObTxLogBlock &build() { return tx_log_block_; }
  int64_t get_log_entry_no() { return log_entry_no_; }
private:
  TxID tx_id_;
  uint64 cluster_id_;
  int64_t log_entry_no_;
  ObTxLogBlock tx_log_block_;
};

// usage example: generate a log_entry with redo+commit_info
// LogEntry log_entry;
// palf::LSN lsn;
// ObTxLogGenerator log_generator(xxx);
// log_generator.gen_redo_log()
// log_generator.gen_commit_info_log()
// log_generator.gen_log_entry(log_entry, lsn)
class ObTxLogGenerator
{
public:
  ObTxLogGenerator(const uint64_t tenant_id, const int64_t ls_id, const int64_t tx_id, const uint64_t cluster_id)
    : tls_id_(tenant_id, share::ObLSID(ls_id)),
      block_builder_(tx_id, cluster_id),
      lsn_arr_(),
      last_record_lsn_(),
      last_normal_lsn_(),
      trans_type_(transaction::TransType::SP_TRANS),
      addr_(),
      cluster_version_(1),
      trace_id_str_("obcdc_unittest_trace"),
      can_elr_(false),
      is_dup_(false),
      is_sub2pc_(false),
      epoch_(1024),
      last_op_scn_(2048),
      checksum_(29890209),
      has_record_log_(false),
      allocator_()
  {
    addr_.set_ip_addr("127.0.0.1", 2881);
    block_builder_.next_log_block();
  }

  ~ObTxLogGenerator() { reset(); }
  void reset() { allocator_.reset(); }

public:
  static const int64_t ENTRY_BUF_SIZE = 1 << 21;
public:
  int gen_log_entry(LogEntry &log_entry, LSN &lsn)
  {
    int ret = OB_SUCCESS;
    log_entry.reset();
    lsn = last_lsn_();
    ObTxLogBlock &tx_log_block = block_builder_.build();
    LogEntryHeader entry_header;
    char *tx_buf = tx_log_block.get_buf();
    int64_t buf_len = tx_log_block.get_size();
    char *buf = static_cast<char*>(allocator_.alloc(buf_len));
    MEMCPY(buf, tx_buf, buf_len);
    LogType log_type = palf::LOG_PADDING;
    const bool is_padding_log = (palf::LOG_PADDING == log_type);
    int64_t log_entry_header_size = entry_header.get_serialize_size();
    int64_t ts = get_timestamp();
    share::SCN scn;

    if (OB_FAIL(scn.convert_for_logservice(ts))) {
      LOG_ERROR("fail to convert ts", KR(ret), K(ts));
    } else if (OB_FAIL(entry_header.generate_header(buf, buf_len, scn))) {
      LOG_ERROR("generate_header failed", KR(ret), K(buf), K(buf_len), K(scn));
    } else {
      log_entry.header_ = entry_header;
      log_entry.buf_ = buf;
      lsn.val_ += (log_entry.get_serialize_size());
      if (OB_UNLIKELY(! log_entry.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("log_entry is not valid", KR(ret));
      } else if (OB_UNLIKELY(! lsn.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("lsn is not valid", KR(ret));
      } else if (OB_FAIL(block_builder_.next_log_block())) {
        LOG_ERROR("next_log_block failed", "log_entry_no", block_builder_.get_log_entry_no());
      } else if (has_record_log_) {
        last_record_lsn_ = lsn;
        last_normal_lsn_ = lsn;
        has_record_log_ = false;
      } else if (OB_FAIL(lsn_arr_.push_back(lsn))) {
        LOG_ERROR("push_back lsn to lsn_arr failed", KR(ret), K(lsn));
      } else {
        last_normal_lsn_ = lsn;
      }
    }

    return ret;
  }

  int gen_ls_offline_log_entry(LogEntry &log_entry, LSN &lsn)
  {
    int ret = OB_SUCCESS;
    int64_t pos = 0;
    lsn = last_lsn_();
    LogEntryHeader entry_header;
    const int64_t ts = get_timestamp();
    logservice::ObLogBaseHeader log_base_header(logservice::ObLogBaseType::GC_LS_LOG_BASE_TYPE, logservice::ObReplayBarrierType::NO_NEED_BARRIER, 1);
    const int64_t serizlize_size = log_base_header.get_serialize_size();
    char *buf = static_cast<char*>(allocator_.alloc(serizlize_size));
    share::SCN scn;

    if (OB_FAIL(scn.convert_for_logservice(ts))) {
      LOG_ERROR("fail to convert ts", KR(ret), K(ts));
    } else if (OB_FAIL(log_base_header.serialize(buf, serizlize_size, pos))) {
      LOG_ERROR("serialize log_base_header failed", KR(ret), K(buf), K(serizlize_size), K(pos));
    } else if (OB_FAIL(entry_header.generate_header(buf, serizlize_size, scn))) {
      LOG_ERROR("generate_header for offline log_entry failed", KR(ret));
    } else {
      log_entry.buf_ = buf;
      log_entry.header_ = entry_header;
      lsn.val_ += log_entry.get_serialize_size();

      if (OB_UNLIKELY(! log_entry.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("offline log_entry is not valid", KR(ret));
      }
    }

    return ret;
  }

  void gen_redo_log();
  void gen_rollback_to_log();
  void gen_mds_log();
  void gen_record_log();
  void gen_commit_info_log();
  void gen_prepare_log();
  void gen_commit_log();
private:
  // lsn of last log_entry, may be invalid if haven't generate any log_entry.
  palf::LSN last_lsn_();
private:
  logservice::TenantLSID tls_id_;
  ObTxLogBlockBuilder block_builder_;
  ObLogLSNArray lsn_arr_;
  LSN last_record_lsn_;
  LSN last_normal_lsn_;
  int32_t trans_type_;
  ObAddr addr_;
  int64_t cluster_version_;
  common::ObString trace_id_str_;
  bool can_elr_;
  bool is_dup_;
  bool is_sub2pc_;
  int64_t epoch_;
  int64_t last_op_scn_;
  int64_t checksum_;
  bool has_record_log_;
  common::ObArenaAllocator allocator_;
};

int ObTxLogBlockBuilder::next_log_block()
{
  int ret = OB_SUCCESS;
  ObTxLogBlockHeader block_header(cluster_id_, log_entry_no_, tx_id_, common::ObAddr());
  tx_log_block_.reset();

  if (OB_FAIL(tx_log_block_.init(tx_id_, block_header))) {
    LOG_ERROR("init tx_log_block_ failed", KR(ret), K_(tx_id), K(block_header));
  } else {
    log_entry_no_ ++;
  }

  return ret;
}

int ObTxLogBlockBuilder::fill_redo(ObTxRedoLog &redo_log)
{
  int ret = OB_SUCCESS;
  uint8_t tmp_flag = memtable::ObTransRowFlag::NORMAL_ROW;
  int64_t mutator_pos = 0;
  common::ObString row_str("obcdc_test");

  if (OB_FAIL(tx_log_block_.prepare_mutator_buf(redo_log))) {
    LOG_ERROR("gen_mutator_buf for tx_log_block_ failed", KR(ret), K_(tx_log_block), K(redo_log));
  } else {
    ObMutatorWriter mmw;
    // ObMemtableMutatorRow row;
    ObCLogEncryptInfo encrypt_info;
    mmw.set_buffer(redo_log.get_mutator_buf(), redo_log.get_mutator_size());

    if (OB_FAIL(mmw.append_row_buf(row_str.ptr(), row_str.length()))) {
      LOG_ERROR("append_row failed", KR(ret));
    } else if (OB_FAIL(mmw.serialize(tmp_flag, mutator_pos, encrypt_info))) {
      LOG_ERROR("serialize memtable_mutator failed", KR(ret));
    } else if (OB_FAIL(tx_log_block_.finish_mutator_buf(redo_log, mutator_pos))) {
      LOG_ERROR("finish_mutator_buf failed", KR(ret), K_(tx_log_block), K(redo_log));
    }
  }

  return ret;
}


void ObTxLogGenerator::gen_redo_log()
{
  ObTxRedoLogTempRef redo_log_ref;
  ObTxRedoLog redo_log(redo_log_ref);
  EXPECT_EQ(OB_SUCCESS, block_builder_.fill_redo(redo_log));
}

void ObTxLogGenerator::gen_rollback_to_log()
{
  ObTxRollbackToLog rollback_to_log(ObTxSEQ(2, 0), ObTxSEQ(1, 0));
  EXPECT_EQ(OB_SUCCESS, block_builder_.fill_tx_log_except_redo(rollback_to_log));
}

void ObTxLogGenerator::gen_mds_log()
{
}

void ObTxLogGenerator::gen_record_log()
{
  ObTxRecordLogTempRef record_log_ref;
  if (last_record_lsn_.is_valid()) {
    record_log_ref.prev_record_lsn_ = last_record_lsn_;
  }
  for (int i = 0; i < lsn_arr_.count(); i++) {
    EXPECT_EQ(OB_SUCCESS, record_log_ref.redo_lsns_.push_back(lsn_arr_.at(i)));
  }
  ObTxRecordLog record_log(record_log_ref);
  LOG_DEBUG("gen record", K(record_log));
  has_record_log_ = true;
  EXPECT_EQ(OB_SUCCESS, block_builder_.fill_tx_log_except_redo(record_log));
  lsn_arr_.reset();
}

void ObTxLogGenerator::gen_commit_info_log()
{
  ObTxCommitInfoLogTempRef commit_info_log_ref;
  for (int i = 0; i < lsn_arr_.count(); i++) {
    EXPECT_EQ(OB_SUCCESS, commit_info_log_ref.redo_lsns_.push_back(lsn_arr_.at(i)));
  }
  ObTxCommitInfoLog commit_info_log(commit_info_log_ref);
  LOG_DEBUG("gen commit_info_log", K(commit_info_log));
  EXPECT_EQ(OB_SUCCESS, block_builder_.fill_tx_log_except_redo(commit_info_log));
}

void ObTxLogGenerator::gen_prepare_log()
{
  share::ObLSArray inc_ls_arr;
  transaction::LogOffSet prev_lsn = last_lsn_();
  ObTxPrepareLog prepare_log(inc_ls_arr, prev_lsn);
  LOG_DEBUG("gen prepare_log", K(prepare_log));
  EXPECT_EQ(OB_SUCCESS, block_builder_.fill_tx_log_except_redo(prepare_log));
  trans_type_ = transaction::TransType::DIST_TRANS; // dist trans.
}

void ObTxLogGenerator::gen_commit_log()
{
  int64_t commit_ts = get_timestamp();
  share::SCN commit_version;
  commit_version.convert_for_logservice(commit_ts);
  uint64_t checksum = 0;
  share::ObLSArray inc_ls_arr;
  ObTxBufferNodeArray mds_arr;
  transaction::ObLSLogInfoArray ls_info_arr;

  if (transaction::TransType::DIST_TRANS == trans_type_) {
    ObLSLogInfo ls_info1(share::ObLSID(1), LSN(190));
    ObLSLogInfo ls_info2(share::ObLSID(1001), last_lsn_());
    EXPECT_EQ(OB_SUCCESS, ls_info_arr.push_back(ls_info1));
    EXPECT_EQ(OB_SUCCESS, ls_info_arr.push_back(ls_info2));
  }

  ObTxCommitLog commit_log(
      commit_version,
      checksum,
      inc_ls_arr,
      mds_arr,
      trans_type_,
      last_lsn_(),
      ls_info_arr);
  LOG_DEBUG("gen commit_log", K(commit_log));
  EXPECT_EQ(OB_SUCCESS, block_builder_.fill_tx_log_except_redo(commit_log));
}

palf::LSN ObTxLogGenerator::last_lsn_()
{
  palf::LSN lsn;
  if (last_record_lsn_.is_valid()) {
    if (last_normal_lsn_.is_valid()) {
      lsn = std::max(last_record_lsn_, last_normal_lsn_);
    } else {
      lsn = last_record_lsn_;
    }
  } else {
    lsn = last_normal_lsn_;
  }
  return lsn;
}

} // namespace unittest
} // namespace oceanbase
