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
    has_complete_(false), direct_load_type_(ObDirectLoadType::DIRECT_LOAD_INVALID), data_format_version_(0), snapshot_version_(0), table_key_()
{}

bool ObTabletDDLCompleteMdsUserData::is_valid() const
{
  return (!has_complete_) ||
         (has_complete_  && table_key_.is_valid() &&
                           direct_load_type_ > ObDirectLoadType::DIRECT_LOAD_INVALID &&
                           direct_load_type_ < ObDirectLoadType::DIRECT_LOAD_MAX);
}

int ObTabletDDLCompleteMdsUserData::assign(const ObTabletDDLCompleteMdsUserData &other)
{
  int ret = OB_SUCCESS;
  if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(other));
  } else {
    has_complete_         = other.has_complete_;
    direct_load_type_     = other.direct_load_type_;
    data_format_version_  = other.data_format_version_;
    snapshot_version_     = other.snapshot_version_;
    table_key_            = other.table_key_;
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
}

OB_SERIALIZE_MEMBER(
  ObTabletDDLCompleteMdsUserData,
  has_complete_,
  direct_load_type_,
  data_format_version_,
  snapshot_version_,
  table_key_
)
} // namespace storage
} // namespace oceanbase