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
#include "storage/ob_storage_schema.h"

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
    seq_no_(),
    snapshot_version_(0),
    data_format_version_(0)
{
}

int ObDDLIncLogBasic::init(
    const ObTabletID &tablet_id,
    const ObTabletID &lob_meta_tablet_id,
    const ObDirectLoadType direct_load_type,
    const ObTransID &trans_id,
    const ObTxSEQ &seq_no,
    const int64_t snapshot_version,
    const uint64_t data_format_version)
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
    snapshot_version_ = snapshot_version;
    data_format_version_ = data_format_version;
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

OB_SERIALIZE_MEMBER(ObDDLIncLogBasic, tablet_id_, lob_meta_tablet_id_, direct_load_type_,
                    trans_id_, seq_no_, snapshot_version_, data_format_version_);

ObDDLIncStartLog::ObDDLIncStartLog()
  : log_basic_(),
    has_cs_replica_(false),
    storage_schema_(nullptr),
    allocator_("IncStartLog")
{
  allocator_.set_tenant_id(MTL_ID());
}

ObDDLIncStartLog::~ObDDLIncStartLog()
{
  if (OB_NOT_NULL(storage_schema_)) {
    storage_schema_->~ObStorageSchema();
    storage_schema_ = nullptr;
  }
  allocator_.reset();
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
    storage_schema_ = nullptr;
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDDLIncStartLog)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, log_basic_, has_cs_replica_);
  if (is_incremental_major_direct_load(log_basic_.get_direct_load_type())) {
    if (OB_NOT_NULL(storage_schema_)) {
      len += storage_schema_->get_serialize_size();
    }
  }
  return len;
}

OB_DEF_SERIALIZE(ObDDLIncStartLog)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, log_basic_, has_cs_replica_);
  if (OB_FAIL(ret)) {
  } else if (is_incremental_major_direct_load(log_basic_.get_direct_load_type())) {
    if (OB_ISNULL(storage_schema_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("storage schema should not be null", KR(ret), K_(log_basic));
    } else if (OB_FAIL(storage_schema_->serialize(buf, buf_len, pos))) {
      LOG_WARN("failed to serialize storage schema", KR(ret), K_(log_basic));
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObDDLIncStartLog)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, log_basic_, has_cs_replica_);
  if (OB_FAIL(ret)) {
  } else if (is_incremental_major_direct_load(log_basic_.get_direct_load_type())) {
    if (OB_ISNULL(storage_schema_)) {
      char *buf = nullptr;
      if (OB_ISNULL(buf = static_cast<char*>(allocator_.alloc(sizeof(ObStorageSchema))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", KR(ret), K_(log_basic));
      } else {
        storage_schema_ = new (buf) ObStorageSchema();
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(storage_schema_->deserialize(allocator_, buf, data_len, pos))) {
      LOG_WARN("failed to deserialize storage schema", K(ret), KPC(this));
    }
  }
  return ret;
}

ObDDLIncCommitLog::ObDDLIncCommitLog()
  : log_basic_(), is_rollback_(false)
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

OB_SERIALIZE_MEMBER(ObDDLIncCommitLog, log_basic_, is_rollback_);

} // namespace storage
} // namespace oceanbase
