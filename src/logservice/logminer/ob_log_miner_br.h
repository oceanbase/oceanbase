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

#ifndef OCEANBASE_LOG_MINER_BR_H_
#define OCEANBASE_LOG_MINER_BR_H_

#include "libobcdc.h"
#include "lib/worker.h"
#include "ob_log_miner_recyclable_task.h"
#include "storage/tx/ob_trans_define.h"

#ifndef OB_USE_DRCMSG
#include "ob_cdc_msg_convert.h"
#else
#include <drcmsg/BR.h>               // ICDCRecord
#include <drcmsg/MD.h>               // ITableMeta
#include <drcmsg/MsgWrapper.h>       // IStrArray
#include <drcmsg/binlogBuf.h>        // binlogBuf
#include <drcmsg/DRCMessageFactory.h>
#endif

namespace oceanbase
{
namespace oblogminer
{
class ObLogMinerBR: public ObLogMinerRecyclableTask
{
public:
  ObLogMinerBR():
      ObLogMinerRecyclableTask(TaskType::BINLOG_RECORD),
      is_filtered_(false),
      compat_mode_(lib::Worker::CompatMode::INVALID),
      seq_no_(0),
      tenant_id_(OB_INVALID_TENANT_ID),
      major_version_(0),
      commit_scn_(),
      record_type_(EUNKNOWN),
      trans_id_(),
      br_host_(nullptr),
      br_(nullptr)
  {
  }

  ~ObLogMinerBR() {
    destroy();
  }

  int init(libobcdc::IObCDCInstance *host,
      ICDCRecord *br,
      const lib::Worker::CompatMode mode,
      const transaction::ObTransID &trans_id,
      const uint64_t tenant_id,
      const int32_t major_version);

  // reentrant
  void reset();

  void destroy() {
    reset();
  }

  ICDCRecord *get_br() {
    return br_;
  }

  void mark_filtered() {
    is_filtered_ = true;
  }

  bool is_filtered() const {
    return is_filtered_;
  }

  lib::Worker::CompatMode get_compat_mode() const {
    return compat_mode_;
  }

  uint64_t get_tenant_id() const {
    return tenant_id_;
  }

  share::SCN get_commit_scn() const {
    return commit_scn_;
  }

  void set_seq_no(int64_t seq_no) {
    seq_no_ = seq_no;
  }

  int64_t get_seq_no() const {
    return seq_no_;
  }

  RecordType get_record_type() const {
    return record_type_;
  }

  const transaction::ObTransID& get_trans_id() const {
    return trans_id_;
  }

  TO_STRING_KV(
    K(is_filtered_),
    K(compat_mode_),
    K(seq_no_),
    K(tenant_id_),
    K(commit_scn_),
    K(record_type_),
    K(trans_id_)
  );

private:
  bool is_filtered_;
  lib::Worker::CompatMode compat_mode_;
  int64_t seq_no_;
  uint64_t tenant_id_;
  int32_t major_version_;
  share::SCN commit_scn_;
  RecordType record_type_;
  transaction::ObTransID trans_id_;
  libobcdc::IObCDCInstance *br_host_;
  ICDCRecord *br_;
};
}
}

#endif