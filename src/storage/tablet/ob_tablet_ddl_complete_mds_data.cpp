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
#include "storage/tablet/ob_tablet_ddl_complete_mds_data.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/ob_errno.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/ddl/ob_direct_load_struct.h"
#include "storage/multi_data_source/mds_key_serialize_util.h"

#define USING_LOG_PREFIX MDS

using namespace oceanbase::common;
using namespace oceanbase::transaction;

namespace oceanbase
{
using namespace mds;
namespace storage
{
/*
 * ObTabletDDLCompleteMdsUserDataKey
 */
OB_SERIALIZE_MEMBER_SIMPLE(
    ObTabletDDLCompleteMdsUserDataKey,
    trans_id_);

int ObTabletDDLCompleteMdsUserDataKey::mds_serialize(
    char *buf,
    const int64_t buf_len,
    int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (pos >= buf_len) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    buf[pos++] = MAGIC_NUMBER;
    ret = ObMdsSerializeUtil::mds_key_serialize(trans_id_, buf, buf_len, pos);
  }
  return ret;
}

int ObTabletDDLCompleteMdsUserDataKey::mds_deserialize(
    const char *buf,
    const int64_t buf_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t tmp = 0;
  uint8_t magic_number = 0;
  if (pos >= buf_len) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    magic_number = buf[pos++];
    if (magic_number != MAGIC_NUMBER) {
      ob_abort();// compat case, just abort for fast fail
    } else {
      ret = ObMdsSerializeUtil::mds_key_deserialize(buf, buf_len, pos, tmp);
    }
  }
  if (OB_SUCC(ret)) {
    trans_id_ = tmp;
  }
  return ret;
}

int64_t ObTabletDDLCompleteMdsUserDataKey::mds_get_serialize_size() const
{
  return sizeof(MAGIC_NUMBER) + ObMdsSerializeUtil::mds_key_get_serialize_size(trans_id_);
}

/*
 * ObTabletDDLCompleteMdsUserData
 */
ObTabletDDLCompleteMdsUserData::ObTabletDDLCompleteMdsUserData():
    has_complete_(false), direct_load_type_(ObDirectLoadType::DIRECT_LOAD_INVALID), data_format_version_(0),
    snapshot_version_(0), table_key_(), storage_schema_(), write_stat_(),
    trans_id_(ObTabletDDLCompleteMdsUserDataKey::DDL_COMPLETE_TX_ID),
    start_scn_(), inc_major_commit_scn_(), allocator_("MdsDdlCom", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID())
{}

ObTabletDDLCompleteMdsUserData::~ObTabletDDLCompleteMdsUserData()
{
  write_stat_.reset();
  storage_schema_.reset();
  allocator_.reset();
}

bool ObTabletDDLCompleteMdsUserData::is_valid() const
{
  return (!has_complete_) ||
         (has_complete_  && table_key_.is_valid()
                         && (direct_load_type_ > ObDirectLoadType::DIRECT_LOAD_INVALID &&
                             direct_load_type_ < ObDirectLoadType::DIRECT_LOAD_MAX)
                         && storage_schema_.is_valid() && write_stat_.is_valid())
          || (is_incremental_major_direct_load(direct_load_type_)
              && storage_schema_.is_valid());
}

int ObTabletDDLCompleteMdsUserData::set_storage_schema(const ObStorageSchema &other)
{
  int ret = OB_SUCCESS;
  if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(other));
  } else if (OB_FAIL(storage_schema_.assign(allocator_, other))) {
    LOG_WARN("failed to assign storage schema", K(ret));
  } else{
    for (int64_t i = 0; OB_SUCC(ret) && i < storage_schema_.column_array_.count(); ++i) {
      ObStorageColumnSchema &cs = storage_schema_.column_array_.at(i);
      cs.orig_default_value_.reset();
    }
  }
  return ret;
}

int ObTabletDDLCompleteMdsUserData::assign(const ObTabletDDLCompleteMdsUserData &other)
{
  int ret = OB_SUCCESS;
  if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(other));
  } else if (other.has_complete_ && OB_FAIL(set_storage_schema(other.storage_schema_))) {
    LOG_WARN("failed to set storage schema", K(ret));
  } else if (other.has_complete_ && OB_FAIL(write_stat_.assign(other.write_stat_))) {
    LOG_WARN("failed to set storage schema", K(ret));
  } else {
    has_complete_         = other.has_complete_;
    direct_load_type_     = other.direct_load_type_;
    data_format_version_  = other.data_format_version_;
    snapshot_version_     = other.snapshot_version_;
    table_key_            = other.table_key_;
    trans_id_             = other.trans_id_;
    start_scn_            = other.start_scn_;
    inc_major_commit_scn_ = other.inc_major_commit_scn_;
  }
  return ret;
}

void ObTabletDDLCompleteMdsUserData::reset()
{
  has_complete_ = false;
  direct_load_type_ = ObDirectLoadType::DIRECT_LOAD_INVALID;
  data_format_version_ = 0;
  snapshot_version_ = 0;
  table_key_.reset();
  storage_schema_.reset();
  write_stat_.reset();
  trans_id_ = ObTabletDDLCompleteMdsUserDataKey::DDL_COMPLETE_TX_ID;
  start_scn_.reset();
  inc_major_commit_scn_.reset();
  allocator_.reset();
}

int ObTabletDDLCompleteMdsUserData::set_with_merge_arg(const ObTabletDDLCompleteArg &arg)
{
  int ret = OB_SUCCESS;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (arg.has_complete_ && nullptr == arg.get_storage_schema()) {
    ret= OB_ERR_UNEXPECTED;
    LOG_WARN("schema should not be null", K(ret));
  } else if (arg.has_complete_ && OB_FAIL(set_storage_schema(*arg.get_storage_schema()))) {
    LOG_WARN("failed to set storage schema", K(ret), K(arg.get_storage_schema()));
  } else if (arg.has_complete_ && OB_FAIL(write_stat_.assign(arg.write_stat_))) {
    LOG_WARN("failed to set write stat", K(ret));
  } else {
    has_complete_ = arg.has_complete_;
    direct_load_type_ = arg.direct_load_type_;
    data_format_version_ = arg.data_format_version_;
    snapshot_version_ = arg.snapshot_version_;
    table_key_ = arg.table_key_;
    trans_id_ = arg.trans_id_;
    start_scn_ = arg.start_scn_;
    inc_major_commit_scn_ = arg.rec_scn_;
  }
  return ret;
}

int64_t ObTabletDDLCompleteMdsUserData::get_serialize_size() const
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              has_complete_, direct_load_type_,
              data_format_version_, snapshot_version_,
              table_key_, write_stat_, trans_id_, start_scn_, inc_major_commit_scn_);
  if (has_complete_) {
    len += storage_schema_.get_serialize_size();
  }
  return len;
}

int ObTabletDDLCompleteMdsUserData::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, has_complete_, direct_load_type_,
              data_format_version_, snapshot_version_, table_key_,
              write_stat_, trans_id_, start_scn_, inc_major_commit_scn_);
  if (OB_FAIL(ret)) {
  } else if (has_complete_ && OB_FAIL(storage_schema_.serialize(buf, buf_len, pos))) {
    LOG_WARN("failed to serialize storage_schema", K(ret));
  }
  return ret;
}

int ObTabletDDLCompleteMdsUserData::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, has_complete_, direct_load_type_,
              data_format_version_, snapshot_version_, table_key_,
              write_stat_, trans_id_, start_scn_, inc_major_commit_scn_);
  if (OB_FAIL(ret)) {
  } else if (has_complete_ && OB_FAIL(storage_schema_.deserialize(allocator_, buf, data_len, pos))) {
    LOG_WARN("failed to deserialize stroage_schema", K(ret), KPC(this));
  }
  return ret;
}
} // namespace storage
} // namespace oceanbase
