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
  } else if (is_incremental_major_direct_load(direct_load_type)
          && OB_UNLIKELY(!trans_id.is_valid()
                      || !seq_no.is_valid()
                      || (snapshot_version <= 0)
                      || !is_data_version_support_inc_major_direct_load(data_format_version))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments for incremental major direct load", KR(ret),
        K(direct_load_type), K(trans_id), K(seq_no), K(snapshot_version), K(data_format_version));
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
  hash_val = common::murmurhash(&direct_load_type_, sizeof(direct_load_type_), hash_val);
  hash_val = common::murmurhash(&trans_id_, sizeof(trans_id_), hash_val);
  hash_val = common::murmurhash(&seq_no_, sizeof(seq_no_), hash_val);
  return hash_val;
}

int ObDDLIncLogBasic::hash(uint64_t &hash_val) const
{
  hash_val = hash();
  return OB_SUCCESS;
}

OB_SERIALIZE_MEMBER(ObDDLIncLogBasic, tablet_id_, lob_meta_tablet_id_, direct_load_type_,
                    trans_id_, seq_no_, snapshot_version_, data_format_version_);

/**
 * ObDDLIncStartLog
 */

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
  reset();
}

int ObDDLIncStartLog::init(
    const ObDDLIncLogBasic &log_basic,
    const bool has_cs_replica,
    const ObStorageSchema *storage_schema)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!log_basic.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(log_basic));
  } else if (is_incremental_major_direct_load(log_basic.get_direct_load_type())
          && (OB_ISNULL(storage_schema) || !storage_schema->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments for incremental major direct load", KR(ret),
        K(log_basic), KPC(storage_schema));
  } else if (OB_FAIL(set_storage_schema(*storage_schema))) {
    LOG_WARN("failed to set storage schema", KR(ret), KPC(storage_schema));
  } else {
    log_basic_ = log_basic;
    has_cs_replica_ = has_cs_replica;
  }
  return ret;
}

bool ObDDLIncStartLog::is_valid() const
{
  return log_basic_.is_valid()
      && (is_incremental_minor_direct_load(log_basic_.get_direct_load_type())
          || (is_incremental_major_direct_load(log_basic_.get_direct_load_type())
              && OB_NOT_NULL(storage_schema_)
              && storage_schema_->is_valid()));
}

void ObDDLIncStartLog::reset()
{
  log_basic_.reset();
  has_cs_replica_ = false;
  if (OB_NOT_NULL(storage_schema_)) {
    storage_schema_->~ObStorageSchema();
    storage_schema_ = nullptr;
  }
  allocator_.reset();
}

int ObDDLIncStartLog::set_storage_schema(const ObStorageSchema &other)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid storage schema", KR(ret), K(other));
  } else if (OB_NOT_NULL(storage_schema_)) {
  } else if (OB_ISNULL(buf = static_cast<char*>(allocator_.alloc(sizeof(ObStorageSchema))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate buf", KR(ret), KP(buf));
  } else {
    storage_schema_ = new (buf) ObStorageSchema();
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(storage_schema_->assign(allocator_, other))) {
    LOG_WARN("failed to assign storage schema", KR(ret), K(other));
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
  reset();
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

/**
 * ObDDLIncCommitLog
 */

ObDDLIncCommitLog::ObDDLIncCommitLog()
  : log_basic_(), is_rollback_(false)
{
}

int ObDDLIncCommitLog::init(const ObDDLIncLogBasic &log_basic, const bool is_rollback)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!log_basic.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(log_basic));
  } else {
    log_basic_ = log_basic;
    is_rollback_ = is_rollback;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObDDLIncCommitLog, log_basic_, is_rollback_);

} // namespace storage
} // namespace oceanbase
