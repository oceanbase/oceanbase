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

#define USING_LOG_PREFIX LOGMNR

#include "ob_log_binlog_record.h"
#include "ob_log_miner_br.h"
#include "lib/ob_define.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase
{

namespace oblogminer
{

int ObLogMinerBR::init(libobcdc::IObCDCInstance *host,
    ICDCRecord *br,
    const lib::Worker::CompatMode mode,
    const transaction::ObTransID &trans_id,
    const uint64_t tenant_id,
    const int32_t major_version)
{
  int ret = OB_SUCCESS;
  libobcdc::ObLogBR *oblog_br = nullptr;
  share::SCN commit_scn;
  if (nullptr == br) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("get null binlog record", K(br), K(tenant_id), K(major_version));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("get invalid tenant_id", K(br), K(tenant_id), K(major_version));
  } else if (OB_ISNULL(oblog_br = static_cast<libobcdc::ObLogBR*>(br->getUserData()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get ICDCRecord without user data, unexpected", KP(oblog_br));
  } else if (OB_FAIL(commit_scn.convert_for_inner_table_field(oblog_br->get_commit_version()))) {
    LOG_ERROR("failed to convert commit_version to scn", "commit_version", oblog_br->get_commit_version());
  } else {
    br_host_ = host;
    br_ = br;
    compat_mode_ = mode;
    tenant_id_ = tenant_id;
    major_version_ = major_version;
    commit_scn_ = commit_scn;
    record_type_ = static_cast<RecordType>(br->recordType());
    trans_id_ = trans_id;
  }
  return ret;
}

void ObLogMinerBR::reset()
{
  is_filtered_ = false;
  compat_mode_ = lib::Worker::CompatMode::INVALID;
  seq_no_ = 0;
  tenant_id_ = OB_INVALID_TENANT_ID;
  major_version_ = 0;
  commit_scn_.reset();
  record_type_ = RecordType::EUNKNOWN;
  trans_id_ = transaction::ObTransID();
  if (nullptr != br_host_ && nullptr != br_) {
    br_host_->release_record(br_);
  }
  br_host_ = nullptr;
  br_ = nullptr;
}

}

}
