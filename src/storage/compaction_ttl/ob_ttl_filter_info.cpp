/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "storage/compaction_ttl/ob_ttl_filter_info.h"
#include "share/scn.h"

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

int ObTTLFilterColTypeUtil::to_filter_col_type(const ObObjType &obj_type, ObTTLFilterColType &filter_col_type)
{
  int ret = OB_SUCCESS;

  switch (obj_type) {
  case ObIntType:
  case ObUInt64Type:
    filter_col_type = ObTTLFilterColType::INT64;
    break;
  case ObTimestampType:
    filter_col_type = ObTTLFilterColType::TIMESTAMP;
    break;
  case ObMySQLDateType: // ObMYSQLDateType means mysql's "Date" type
    filter_col_type = ObTTLFilterColType::MYSQL_DATE;
    break;
  case ObDateTimeType: // This is "Date" type for oracle, which is different from mysql's "Date" type
    filter_col_type = ObTTLFilterColType::ORACLE_DATE;
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

int ObTTLFilterColTypeUtil::to_obj_type(const ObTTLFilterColType &filter_col_type, ObObjType &obj_type)
{
  int ret = OB_SUCCESS;
  switch (filter_col_type) {
  case ObTTLFilterColType::INVALID:
  case ObTTLFilterColType::MAX:
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid filter col type for obj type conversion", KR(ret), K(filter_col_type));
    break;
  case ObTTLFilterColType::ROWSCN:
  case ObTTLFilterColType::INT64:
    obj_type = ObIntType;
    break;
  case ObTTLFilterColType::ORACLE_DATE:
    obj_type = ObDateTimeType;
    break;
  case ObTTLFilterColType::TIMESTAMP:
    obj_type = ObTimestampType;
    break;
  case ObTTLFilterColType::TIMESTAMP_TZ:
    obj_type = ObTimestampTZType;
    break;
  case ObTTLFilterColType::TIMESTAMP_LTZ:
    obj_type = ObTimestampLTZType;
    break;
  case ObTTLFilterColType::TIMESTAMP_NANO:
    obj_type = ObTimestampNanoType;
    break;
  case ObTTLFilterColType::MYSQL_DATETIME:
    obj_type = ObMySQLDateTimeType;
    break;
  case ObTTLFilterColType::MYSQL_DATE:
    obj_type = ObMySQLDateType;
    break;
  default:
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected ttl filter col type", KR(ret), K(filter_col_type));
    break;
  }
  return ret;
}

bool ObTTLFilterColTypeUtil::has_ttl_column_in_lob_meta(const ObTTLFilterColType ttl_type)
{
  return ttl_type != ObTTLFilterColType::INVALID
         && ttl_type != ObTTLFilterColType::ROWSCN
         && ttl_type < ObTTLFilterColType::MAX;
}

int ObTTLFilterInfo::fill_filter_value(const ObTTLFilterColType col_type,
                                       const int64_t ttl_filter_us,
                                       const ObTimeZoneInfo *timezone)
{
  int ret = OB_SUCCESS;

  ttl_filter_col_type_ = col_type;

  switch (col_type) {
  case ObTTLFilterColType::ROWSCN:
    ttl_filter_value_ = ttl_filter_us * 1000L; // us to ns
    break;
  case ObTTLFilterColType::INT64:
    // only for hbase
    ttl_filter_value_ = ttl_filter_us / 1000L; // us to ms
    break;
  case ObTTLFilterColType::ORACLE_DATE:
    // date means oracle's date type, which doesn't contain timezone information
    // we suppose the timezone is the tenant's timezone
    if (OB_FAIL(ObTimeConverter::timestamp_to_datetime(ttl_filter_us, timezone, ttl_filter_value_))) {
      LOG_WARN("failed to convert to datetime", KR(ret), K(ttl_filter_us));
    }
    break;
  case ObTTLFilterColType::TIMESTAMP_NANO:
    // timestamp_nano means oracle's timestamp(scale) type, which doesn't contain timezone
    // information we suppose the timezone is the tenant's timezone
    if (OB_FAIL(ObTimeConverter::timestamp_to_datetime(ttl_filter_us, timezone, ttl_filter_value_))) {
      LOG_WARN("failed to convert to datetime", KR(ret), K(ttl_filter_us));
    } else {
      ttl_filter_value_ *= 1000L; // us to ns
    }
    break;
  case ObTTLFilterColType::TIMESTAMP_TZ: {
    // timestamp tz means oracle's timestamp with time zone type, we pass a utf+0 timestamp for
    // storage compare
    ttl_filter_value_ = ttl_filter_us * 1000; // us to ns
    break;
  }
  case ObTTLFilterColType::TIMESTAMP_LTZ: {
    // timestamp ltz means oracle's timestamp with local time zone type, which is utf+0
    ttl_filter_value_ = ttl_filter_us * 1000; // us to ns
    break;
  }
  case ObTTLFilterColType::TIMESTAMP:
    // timestamp means mysql's timestamp type, which is utf+0 time
    ttl_filter_value_ = ttl_filter_us; // us
    break;
  case ObTTLFilterColType::MYSQL_DATETIME: {
    // mysql datetime means mysql's datetime type, which doesn't contain timezone information
    // we suppose the timezone is the tenant's timezone
    ObMySQLDateTime mdt_value;
    if (OB_FAIL(ObTimeConverter::timestamp_to_mdatetime(ttl_filter_us, timezone, mdt_value))) {
      LOG_WARN("failed to convert to mysql datetime", KR(ret), K(ttl_filter_us));
    } else {
      ttl_filter_value_ = mdt_value.datetime_;
    }
    break;
  }
  case ObTTLFilterColType::MYSQL_DATE: {
    // mysql date means mysql's date type, which doesn't contain timezone information
    // we suppose the timezone is the tenant's timezone
    ObMySQLDate md_value;
    if (OB_FAIL(ObTimeConverter::datetime_to_mdate(ttl_filter_us, timezone, md_value))) {
      LOG_WARN("failed to convert to mysql date", KR(ret), K(ttl_filter_us));
    } else {
      ttl_filter_value_ = md_value.date_;
    }
    break;
  }
  default:
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ttl filter col type", KR(ret), K(ttl_filter_col_type_));
    break;
  }

  return ret;
}

} // namespace storage

} // namespace oceanbase
