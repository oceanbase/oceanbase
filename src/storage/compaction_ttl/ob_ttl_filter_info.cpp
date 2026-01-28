/**
 * Copyright (c) 2025 OceanBase
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
#include "storage/compaction_ttl/ob_ttl_filter_info.h"
#include "share/scn.h"
#include "share/compaction_ttl/ob_compaction_ttl_util.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace storage
{

OB_SERIALIZE_MEMBER_SIMPLE(ObTTLFilterInfoKey, tx_id_);

int ObTTLFilterInfoKey::mds_serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(serialization::encode_i8(buf, buf_len, pos, MAGIC_NUMBER))) {
    LOG_WARN("Fail to encode magic number", KR(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, tx_id_))) {
    LOG_WARN("Fail to encode tx_id", KR(ret), K_(tx_id));
  }

  return ret;
}

int ObTTLFilterInfoKey::mds_deserialize(const char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;

  int8_t magic_number = 0;

  if (OB_FAIL(serialization::decode_i8(buf, buf_len, pos, &magic_number))) {
    LOG_WARN("Fail to decode magic number", KR(ret));
  } else if (static_cast<uint8_t>(magic_number) != MAGIC_NUMBER) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("magic number mismatch", K(magic_number), K(MAGIC_NUMBER));
    ob_abort(); // compat case, just abort for fast fail
  } else if (OB_FAIL(serialization::decode_i64(buf, buf_len, pos, &tx_id_))) {
    LOG_WARN("Fail to decode tx_id", KR(ret), K_(tx_id));
  }

  return ret;
}

int64_t ObTTLFilterInfoKey::mds_get_serialize_size() const
{
  return sizeof(MAGIC_NUMBER) + sizeof(tx_id_);
}

void ObTTLFilterInfoKey::gene_info(char *buf, const int64_t buf_len, int64_t &pos) const
{
  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    J_KV(K_(tx_id));
  }
}

int ObTTLFilterInfo::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;

  LST_DO_CODE(OB_UNIS_ENCODE,
              info_,
              key_,
              commit_version_,
              ttl_filter_col_idx_,
              ttl_filter_value_);

  return ret;
}

int ObTTLFilterInfo::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;

  LST_DO_CODE(OB_UNIS_DECODE,
              info_,
              key_,
              commit_version_,
              ttl_filter_col_idx_,
              ttl_filter_value_);

  return ret;
}

int ObTTLFilterInfo::deserialize(ObIAllocator &unused_allocator,
                                 const char *buf,
                                 const int64_t buf_len,
                                 int64_t &pos)
{
  return deserialize(buf, buf_len, pos);
}

void ObTTLFilterInfo::on_commit(const SCN &commit_version, const SCN &commit_scn)
{ // fill commit version after mds trans commit
  commit_version_ = commit_version.get_val_for_tx();
}

int ObTTLFilterInfo::to_filter_col_type(const ObObjType &obj_type, ObTTLFilterColType &filter_col_type)
{

  int ret = OB_SUCCESS;

  switch (obj_type) {
  case ObIntType:
  case ObUInt64Type:
    filter_col_type = ObTTLFilterColType::INT64;
    break;
  case ObTimestampType:
  case ObDateTimeType:
    filter_col_type = ObTTLFilterColType::TIMESTAMP;
    break;
  case ObDateType:
    filter_col_type = ObTTLFilterColType::DATE;
    break;
  case ObTimestampTZType:
    filter_col_type = ObTTLFilterColType::TIMESTAMP_TZ;
    break;
  case ObTimestampLTZType:
    filter_col_type = ObTTLFilterColType::TIMESTAMP_LTZ;
    break;
  case ObTimestampNanoType:
    filter_col_type = ObTTLFilterColType::TIMESTAMP_NANO;
    break;
  case ObMySQLDateTimeType:
    filter_col_type = ObTTLFilterColType::MYSQL_DATETIME;
    break;
  default:
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected ttl type, should error in table_param or dml_param", KR(ret), K(obj_type));
    break;
  }

  return ret;
}

} // namespace storage

namespace share
{

bool ObTTLFlag::is_valid(const uint64_t tenant_data_version) const
{
  bool bool_ret = true;

  if (tenant_data_version < ObCompactionTTLUtil::COMPACTION_TTL_CMP_DATA_VERSION) {
    bool_ret = version_ == TTL_FLAG_VERSION_V1 && had_rowscn_as_ttl_ == 0 && ttl_type_ == ObTTLDefinition::NONE && reserved_ == 0;
  } else {
    bool_ret = version_ == TTL_FLAG_VERSION_V1 && reserved_ == 0;
  }

  return bool_ret;
}

ObTTLFlag::ObTTLFlag()
  : version_(TTL_FLAG_VERSION_V1),
    had_rowscn_as_ttl_(0),
    ttl_type_(ObTTLDefinition::NONE),
    reserved_(0)
{}

void ObTTLFlag::reset()
{
  version_ = TTL_FLAG_VERSION_V1;
  had_rowscn_as_ttl_ = 0;
  ttl_type_ = ObTTLDefinition::NONE;
  reserved_ = 0;
}


} // namespace share

} // namespace oceanbase
