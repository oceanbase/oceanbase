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

#define USING_LOG_PREFIX STORAGE

#include "storage/ddl/ob_ddl_inc_clog.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace transaction;

ObDDLIncLogBasic::ObDDLIncLogBasic()
  : tablet_id_(),
    lob_meta_tablet_id_(),
    direct_load_type_(ObDirectLoadType::DIRECT_LOAD_INVALID),
    trans_id_(),
    seq_no_()
{
}

int ObDDLIncLogBasic::init(
    const ObTabletID &tablet_id,
    const ObTabletID &lob_meta_tablet_id,
    const ObDirectLoadType direct_load_type,
    const ObTransID &trans_id,
    const ObTxSEQ &seq_no)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id));
  } else if (OB_UNLIKELY(!is_incremental_direct_load(direct_load_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("only support incremental direct load type", KR(ret), K(direct_load_type));
  } else {
    tablet_id_ = tablet_id;
    lob_meta_tablet_id_ = lob_meta_tablet_id;
    direct_load_type_ = direct_load_type;
    trans_id_ = trans_id;
    seq_no_ = seq_no;
  }

  return ret;
}

uint64_t ObDDLIncLogBasic::hash() const
{
  uint64_t hash_val = 0;
  hash_val = common::murmurhash(&tablet_id_, sizeof(tablet_id_), hash_val);
  hash_val = common::murmurhash(&lob_meta_tablet_id_, sizeof(lob_meta_tablet_id_), hash_val);
  return hash_val;
}

int ObDDLIncLogBasic::hash(uint64_t &hash_val) const
{
  hash_val = hash();
  return OB_SUCCESS;
}

OB_SERIALIZE_MEMBER(ObDDLIncLogBasic, tablet_id_, lob_meta_tablet_id_, direct_load_type_, trans_id_, seq_no_);

ObDDLIncStartLog::ObDDLIncStartLog()
  : log_basic_(),
    has_cs_replica_(false)
{
}

int ObDDLIncStartLog::init(const ObDDLIncLogBasic &log_basic)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!log_basic.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(log_basic));
  } else {
    log_basic_ = log_basic;
    has_cs_replica_ = false;
  }

  return ret;
}

OB_SERIALIZE_MEMBER(ObDDLIncStartLog, log_basic_, has_cs_replica_);

ObDDLIncCommitLog::ObDDLIncCommitLog()
  : log_basic_()
{
}

int ObDDLIncCommitLog::init(const ObDDLIncLogBasic &log_basic)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!log_basic.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(log_basic));
  } else {
    log_basic_ = log_basic;
  }

  return ret;
}

OB_SERIALIZE_MEMBER(ObDDLIncCommitLog, log_basic_);

} // namespace storage
} // namespace oceanbase
