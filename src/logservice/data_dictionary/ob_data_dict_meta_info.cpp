/**
* Copyright (c) 2022 OceanBase
* OceanBase CE is licensed under Mulan PubL v2.
* You can use this software according to the terms and conditions of the Mulan PubL v2.
* You may obtain a copy of Mulan PubL v2 at:
*          http://license.coscl.org.cn/MulanPubL-2.0
* THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
* EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
* MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
* See the Mulan PubL v2 for more details.
*
* Define DataDictionaryService
*/


#include "ob_data_dict_meta_info.h"
#include "ob_data_dict_service.h"
#include "lib/oblog/ob_log_module.h"    // DDLOG
#include "logservice/palf/log_define.h" // LOG_INVALID_LSN_VAL
#include "lib/checksum/ob_crc64.h"      // ob_crc64
#include "lib/string/ob_sql_string.h"   // ObString
#include "share/inner_table/ob_inner_table_schema_constants.h" // OB_ALL_DATA_DICTIONARY_IN_LOG_TNAME
#include "share/rc/ob_tenant_base.h"    // MTL


namespace oceanbase
{
namespace datadict
{

////////////////////////////////// ObDataDictMetaInfoItem //////////////////////////////////

ObDataDictMetaInfoItem::ObDataDictMetaInfoItem()
{
  reset();
}

ObDataDictMetaInfoItem::~ObDataDictMetaInfoItem()
{
  reset();
}


void ObDataDictMetaInfoItem::reset()
{
  snapshot_scn_ = share::OB_INVALID_SCN_VAL;
  start_lsn_ = palf::LOG_INVALID_LSN_VAL;
  end_lsn_ = palf::LOG_INVALID_LSN_VAL;
}

void ObDataDictMetaInfoItem::reset(
    const uint64_t snapshot_scn,
    const uint64_t start_lsn,
    const uint64_t end_lsn)
{
  snapshot_scn_ = snapshot_scn;
  start_lsn_ = start_lsn;
  end_lsn_ = end_lsn;
}

DEFINE_SERIALIZE(ObDataDictMetaInfoItem)
{
  int ret = OB_SUCCESS;

  LST_DO_CODE(OB_UNIS_ENCODE,
      snapshot_scn_,
      start_lsn_,
      end_lsn_);

  return ret;
}

DEFINE_DESERIALIZE(ObDataDictMetaInfoItem)
{
  int ret = OB_SUCCESS;

  LST_DO_CODE(OB_UNIS_DECODE,
      snapshot_scn_,
      start_lsn_,
      end_lsn_);

  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObDataDictMetaInfoItem)
{
  int len = 0;

  LST_DO_CODE(OB_UNIS_ADD_LEN,
      snapshot_scn_,
      start_lsn_,
      end_lsn_);

  return len;
}

///////////////////////////////// ObDataDictMetaInfoHeader /////////////////////////////////

ObDataDictMetaInfoHeader::ObDataDictMetaInfoHeader()
{
  reset();
}

ObDataDictMetaInfoHeader::~ObDataDictMetaInfoHeader()
{
  reset();
}

void ObDataDictMetaInfoHeader::reset()
{
  magic_ = 0;
  meta_version_ = 0;
  tenant_id_ = OB_INVALID_TENANT_ID;
  item_cnt_ = 0;
  min_snapshot_scn_ = share::OB_INVALID_SCN_VAL;
  max_snapshot_scn_ = share::OB_INVALID_SCN_VAL;
  data_size_ = 0;
  checksum_ = 0;
}

int ObDataDictMetaInfoHeader::generate(
    const uint64_t tenant_id,
    const int32_t item_cnt,
    const uint64_t max_snapshot_scn,
    const uint64_t min_snapshot_scn,
    const char *data,
    const int64_t data_size)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    DDLOG(WARN, "data is null", K(ret), K(data));
  } else {
    magic_ = DATADICT_METAINFO_HEADER_MAGIC;
    meta_version_ = DATADICT_METAINFO_META_VERSION;
    tenant_id_ = tenant_id;
    item_cnt_ = item_cnt;
    max_snapshot_scn_ = max_snapshot_scn;
    min_snapshot_scn_ = min_snapshot_scn;
    data_size_ = data_size;
    checksum_ = ob_crc64(data, data_size);
  }

  return ret;
}

bool ObDataDictMetaInfoHeader::check_integrity(
    const char *data,
    const int64_t data_size) const
{
  bool bret = true;
  ObDataDictMetaInfoItem dummy_item;
  const int64_t item_size = dummy_item.get_serialize_size();

  if (DATADICT_METAINFO_HEADER_MAGIC != magic_) {
    bret = false;
    DDLOG_RET(WARN, OB_ERR_UNEXPECTED, "error magic number of datadict metainfo", KPC(this));
  } else if (OB_ISNULL(data)) {
    bret = false;
    DDLOG_RET(WARN, OB_ERR_UNEXPECTED, "data is null when check integrity", K(data), K(data_size_), KPC(this));
  } else if (data_size < data_size_) {
    bret = false;
    DDLOG_RET(WARN, OB_ERR_UNEXPECTED, "the size of buffer being deserialized is smaller than the data size",
        K(data_size), K(data_size_), KPC(this));
  } else {
    const int64_t data_crc_code = ob_crc64(data, data_size_);
    if (data_crc_code != checksum_) {
      bret = false;
      DDLOG_RET(WARN, OB_ERR_UNEXPECTED, "check sum mismatch", K(data), K(data_size_), K(data_crc_code), KPC(this));
    } else {
      // succ
    }
  }

  return bret;
}

DEFINE_SERIALIZE(ObDataDictMetaInfoHeader)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, magic_))) {
    DDLOG(WARN, "serialize magic failed", K(ret), K(buf), K(buf_len), K(pos), K(magic_));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, pos, meta_version_))) {
    DDLOG(WARN, "serialize meta_version failed", K(ret), K(buf), K(buf_len), K(pos), K(meta_version_));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, tenant_id_))) {
    DDLOG(WARN, "serialize tenant_id_ failed", K(ret), K(buf), K(buf_len), K(pos), K(tenant_id_));
  } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, item_cnt_))) {
    DDLOG(WARN, "serialize item_cnt failed", K(ret), K(buf), K(buf_len), K(pos), K(item_cnt_));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, min_snapshot_scn_))) {
    DDLOG(WARN, "serialize min_snapshot_scn failed", K(ret), K(buf), K(buf_len), K(pos), K(min_snapshot_scn_));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, max_snapshot_scn_))) {
    DDLOG(WARN, "serialize max_snapshot_scn failed", K(ret), K(buf), K(buf_len), K(pos), K(max_snapshot_scn_));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, data_size_))) {
    DDLOG(WARN, "serialize data_size failed", K(ret), K(buf), K(buf_len), K(pos), K(data_size_));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, checksum_))) {
    DDLOG(WARN, "serialize checksum failed", K(ret), K(buf), K(buf_len), K(pos), K(checksum_));
  }

  return ret;
}

DEFINE_DESERIALIZE(ObDataDictMetaInfoHeader)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, &magic_))) {
    DDLOG(WARN, "deserialize magic failed", K(ret), K(buf), K(data_len), K(pos), K(magic_));
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, pos, &meta_version_))) {
    DDLOG(WARN, "deserialize meta_version failed", K(ret), K(buf), K(data_len), K(pos), K(meta_version_));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, (int64_t *)&tenant_id_))) {
    DDLOG(WARN, "deserialize tenant_id_ failed", K(ret), K(buf), K(data_len), K(pos), K(tenant_id_));
  } else if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &item_cnt_))) {
    DDLOG(WARN, "deserialize item_cnt failed", K(ret), K(buf), K(data_len), K(pos), K(item_cnt_));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, (int64_t *)&min_snapshot_scn_))) {
    DDLOG(WARN, "deserialize min_snapshot_scn failed", K(ret), K(buf), K(data_len), K(pos), K(min_snapshot_scn_));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, (int64_t *)&max_snapshot_scn_))) {
    DDLOG(WARN, "deserialize max_snapshot_scn failed", K(ret), K(buf), K(data_len), K(pos), K(max_snapshot_scn_));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &data_size_))) {
    DDLOG(WARN, "deserialize data_size failed", K(ret), K(buf), K(data_len), K(pos), K(data_size_));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &checksum_))) {
    DDLOG(WARN, "deserialize checksum failed", K(ret), K(buf), K(data_len), K(pos), K(checksum_));
  }

  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObDataDictMetaInfoHeader)
{
  return serialization::encoded_length_i16(magic_) +
      serialization::encoded_length_i16(meta_version_) +
      serialization::encoded_length_i64(tenant_id_) +
      serialization::encoded_length_i32(item_cnt_) +
      serialization::encoded_length_i64(min_snapshot_scn_) +
      serialization::encoded_length_i64(max_snapshot_scn_) +
      serialization::encoded_length_i64(data_size_) +
      serialization::encoded_length_i64(checksum_);
}

//////////////////////////////////// ObDataDictMetaInfo ////////////////////////////////////

ObDataDictMetaInfo::ObDataDictMetaInfo()
{
  reset();
}

ObDataDictMetaInfo::~ObDataDictMetaInfo()
{
  reset();
}

void ObDataDictMetaInfo::reset()
{
  header_.reset();
  item_arr_.reset();
}

int ObDataDictMetaInfo::push_back(const ObDataDictMetaInfoItem &item)
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(item_arr_.push_back(item))) {
    DDLOG(WARN, "push back data dict meta info item failed", K(ret), K(item), K(item_arr_));
  }
  return ret;
}

bool ObDataDictMetaInfo::check_integrity() const
{
  bool bret = true;
  const int64_t item_arr_count = item_arr_.count();
  if (item_arr_count != header_.get_item_count()) {
    bret = false;
    DDLOG_RET(WARN, OB_ERR_UNEXPECTED, "item count in array and header is not equal", K(item_arr_count), K(header_));
  } else {
    const ObDataDictMetaInfoItem &max_scn_item = item_arr_.at(0);
    const ObDataDictMetaInfoItem &min_scn_item = item_arr_.at(item_arr_count-1);
    if (max_scn_item.snapshot_scn_ != header_.get_max_snapshot_scn() &&
        min_scn_item.snapshot_scn_ != header_.get_min_snapshot_scn()) {
      bret = false;
      DDLOG_RET(WARN, OB_ERR_UNEXPECTED, "min scn and max scn record in header is inconsistent with item_arr", K(header_),
          K(max_scn_item), K(min_scn_item));
    }
  }
  return bret;
}

DEFINE_SERIALIZE(ObDataDictMetaInfo)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_FAIL(header_.serialize(buf, buf_len, new_pos))) {
    DDLOG(WARN, "failed to serialize datadict metainfo header", K(ret),
        K(header_), K(buf), K(buf_len), K(pos));
  } else {
    const int64_t item_cnt = item_arr_.count();
    for (int64 i = 0; OB_SUCC(ret) && i < item_cnt; i++) {
      const ObDataDictMetaInfoItem &item = item_arr_.at(i);
      if (OB_FAIL(item.serialize(buf, buf_len, new_pos))) {
        DDLOG(WARN, "failed to serialize datadict metainfo item", K(ret), K(header_),
            K(buf), K(buf_len), K(pos), K(new_pos), K(item));
      } else {
        pos = new_pos;
      }
    }
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObDataDictMetaInfo)
{
  const int64_t item_cnt = item_arr_.count();
  ObDataDictMetaInfoItem dummy_item;
  const int64_t item_size = dummy_item.get_serialize_size();
  return header_.get_serialize_size() + item_cnt * item_size;
}

DEFINE_DESERIALIZE(ObDataDictMetaInfo)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(header_.deserialize(buf, data_len, pos))) {
    DDLOG(WARN, "failed to deserialize datadict metainfo header", K(ret),
        K(buf), K(data_len), K(pos));
  } else if (! header_.check_integrity(buf + pos, header_.get_data_size())) {
    ret = OB_INVALID_DATA;
    DDLOG(WARN, "data may be invalid", K(ret), K(buf), K(pos), K(data_len), K(header_));
  } else {
    const int32_t item_cnt = header_.get_item_count();
    if (0 >= item_cnt) {
      ret = OB_ERR_UNEXPECTED;
      DDLOG(WARN, "item count is less or equal than 0, unexpected", K(ret), K(header_));
    }
    for (int32_t i = 0; OB_SUCC(ret) && i < item_cnt && pos < data_len; i++) {
      ObDataDictMetaInfoItem item;
      if (OB_FAIL(item.deserialize(buf, data_len, pos))) {
        DDLOG(WARN, "datadict metainfo item deserialize failed", K(ret),
            K(buf), K(data_len), K(pos));
      } else if (OB_FAIL(item_arr_.push_back(item))){
        DDLOG(WARN, "item_arr push back datadict metainfo item failed", K(ret), K(item), K(item_arr_));
      }
    }
  }
  return ret;
}

/////////////////////////////////// MetaInfoQueryHelper ///////////////////////////////////

const char *MetaInfoQueryHelper::QUERY_META_INFO_SQL_STR =
    "SELECT snapshot_scn, start_lsn, end_lsn FROM %s ORDER BY snapshot_scn DESC;";
const char *MetaInfoQueryHelper::DATA_DICT_META_TABLE_NAME =
    share::OB_ALL_DATA_DICTIONARY_IN_LOG_TNAME;

int MetaInfoQueryHelper::get_data(
    const share::SCN &base_scn,
    char *data,
    const int64_t data_size,
    int64_t &real_size,
    share::SCN &scn)
{
  int ret = OB_SUCCESS;
  datadict::DataDictMetaInfoItemArr item_arr;

  if (OB_UNLIKELY(! base_scn.is_valid_and_not_min())
      || OB_ISNULL(data)
      || OB_UNLIKELY(data_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    DDLOG(WARN, "invalid args to get data_dict data", KR(ret), K(base_scn), KP(data), K(data_size));
  } else if (OB_FAIL(get_data_dict_meta_info_(base_scn, item_arr))) {
    DDLOG(WARN, "get data dict meta info failed", K(ret), K_(tenant_id), K(base_scn));
  } else if (item_arr.count() <= 0) {
    ret = OB_ENTRY_NOT_EXIST;
    int tmp_ret = OB_SUCCESS;

    if (OB_TMP_FAIL(mark_dump_data_dict_())) {
      DDLOG(WARN, "mark_dump_data_dict_ failed", KR(ret), KR(tmp_ret), K_(tenant_id), K(base_scn));
    } else {
      DDLOG(WARN, "no valid result was fetched from result", KR(ret), K_(tenant_id), K(base_scn));
    }
  } else if (OB_FAIL(generate_data_(item_arr, data, data_size, real_size, scn))) {
    DDLOG(WARN, "generate data from item_arr failed", KR(ret), K_(tenant_id), K(base_scn));
  }

  return ret;
}

int MetaInfoQueryHelper::generate_data_(const DataDictMetaInfoItemArr &item_arr,
    char *data,
    const int64_t data_size,
    int64_t &real_size,
    share::SCN &scn)
{
  int ret = OB_SUCCESS;
  const int64_t arr_size = item_arr.count();
  datadict::ObDataDictMetaInfoHeader header;
  const int64_t header_size = header.get_serialize_size();
  int64_t header_pos = 0;
  int64_t meta_data_pos = header_size;
  int64_t serialized_item_cnt = 0;
  bool have_enough_buffer = true;

  for (int64_t i = 0; OB_SUCC(ret) && i < arr_size && have_enough_buffer; i++) {
    const datadict::ObDataDictMetaInfoItem &item = item_arr.at(i);
    const int64_t item_size = item.get_serialize_size();
    if (meta_data_pos + item_size > data_size) {
      have_enough_buffer = false;
    } else if (OB_FAIL(item.serialize(data, data_size, meta_data_pos))) {
      DDLOG(WARN, "serialize datadict metainfo item failed", K(ret), K(data),
          K(data_size), K(meta_data_pos), K(tenant_id_));
    } else {
      // succ
      serialized_item_cnt++;
    }
  }
  if (OB_SUCC(ret)) {
    const datadict::ObDataDictMetaInfoItem &max_scn_item = item_arr.at(0);
    const datadict::ObDataDictMetaInfoItem &min_scn_item = item_arr.at(serialized_item_cnt-1);

    if (OB_FAIL(header.generate(tenant_id_, serialized_item_cnt, max_scn_item.snapshot_scn_,
        min_scn_item.snapshot_scn_, data + header_size, meta_data_pos - header_size))) {
      DDLOG(WARN, "generate metainfo header failed", K(ret), K(serialized_item_cnt),
          K(data), K(header_size), K(meta_data_pos));
    } else if (OB_FAIL(header.serialize(data, header_size, header_pos))) {
      DDLOG(WARN, "serialize metainfo header failed", K(ret), K(data), K(header_size),
          K(header_pos));
    } else {
      real_size = meta_data_pos;
      if (OB_FAIL(scn.convert_for_inner_table_field(max_scn_item.snapshot_scn_))) {
        DDLOG(WARN, "convert snapshot_scn(bigint) to scn failed", K(ret), K(max_scn_item));
      }
    }
  }

  return ret;
}

int MetaInfoQueryHelper::get_data_dict_meta_info_(const share::SCN &base_scn, DataDictMetaInfoItemArr &item_arr)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t record_count = 0;
  int64_t valid_record_count = 0;
  common::sqlclient::ObMySQLResult *sql_result = NULL;

  SMART_VAR(ObISQLClient::ReadResult, result) {
    if (OB_FAIL(sql.assign_fmt(QUERY_META_INFO_SQL_STR, DATA_DICT_META_TABLE_NAME))) {
      DDLOG(WARN, "assign format to sqlstring failed", K(ret), K(QUERY_META_INFO_SQL_STR),
          K(DATA_DICT_META_TABLE_NAME));
    } else if (OB_FAIL(sql_proxy_.read(result, tenant_id_, sql.ptr()))) {
      DDLOG(WARN, "sql proxy failed to read result when querying datadict metainfo", K(ret),
          K(tenant_id_), "sql", sql.ptr());
    } else if (OB_ISNULL((sql_result = result.get_result()))) {
      ret = OB_ERR_UNEXPECTED;
      DDLOG(WARN, "get sql result failed", K(ret), "sql", sql.ptr());
    } else if (OB_FAIL(parse_record_from_result_(base_scn, *sql_result, record_count, valid_record_count, item_arr))) {
      DDLOG(WARN, "parse record from result failed", KR(ret),
          K_(tenant_id), "sql", sql.ptr(), K(base_scn), K(record_count), K(valid_record_count));
    } else {
      DDLOG(INFO, "get_data_dict_meta_info", K(tenant_id_), K(base_scn), K(record_count), K(valid_record_count));
    }
  }

  return ret;
}

int MetaInfoQueryHelper::parse_record_from_result_(
    const share::SCN &base_scn,
    common::sqlclient::ObMySQLResult &result,
    int64_t &record_count,
    int64_t &valid_record_count,
    DataDictMetaInfoItemArr &item_arr)
{
  int ret = OB_SUCCESS;
  const uint64_t base_scn_val = base_scn.get_val_for_inner_table_field();

  while (OB_SUCC(ret)) {
    if (OB_FAIL(result.next())) {
      if (OB_ITER_END != ret) {
        DDLOG(WARN, "get next result failed",K(ret), K(record_count));
      }
    } else {
      record_count++;
      ObDataDictMetaInfoItem item;
      // +--------------+---------------------+
      // | Field        | Type                |
      // +--------------+---------------------+
      // | snapshot_scn | bigint(20) unsigned |
      // | start_lsn    | bigint(20) unsigned |
      // | end_lsn      | bigint(20) unsigned |
      // +--------------+---------------------+
      EXTRACT_UINT_FIELD_MYSQL(result, "snapshot_scn", item.snapshot_scn_, uint64_t);
      EXTRACT_UINT_FIELD_MYSQL(result, "start_lsn", item.start_lsn_, uint64_t);
      EXTRACT_UINT_FIELD_MYSQL(result, "end_lsn", item.end_lsn_, uint64_t);
      // only use record generated after base_scn.
      if (OB_SUCC(ret)
          && base_scn_val <= item.snapshot_scn_
          && item.snapshot_scn_ != share::OB_INVALID_SCN_VAL) {
        if (OB_FAIL(item_arr.push_back(item))) {
          DDLOG(WARN, "push item into item_arr failed", K(ret), K(item), K(item_arr),
              K(record_count), K(valid_record_count));
        } else {
          valid_record_count++;
        }
      }
    }
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }

  return ret;
}

int MetaInfoQueryHelper::mark_dump_data_dict_()
{
  int ret = OB_SUCCESS;
  ObDataDictService *dict_service = NULL;
  const uint64_t ctx_tenant_id = MTL_ID();

  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == ctx_tenant_id || ctx_tenant_id != tenant_id_)) {
    ret = OB_STATE_NOT_MATCH;
    DDLOG(WARN, "current mtl ctx tenant is invalid or not match", KR(ret), K(ctx_tenant_id), K_(tenant_id));
  } else if (OB_ISNULL(dict_service = MTL(ObDataDictService*))) {
    ret = OB_ERR_UNEXPECTED;
    DDLOG(WARN, "data_dict_service is not valid, server may not has resource for ctx_tenant", KR(ret), K(ctx_tenant_id));
  } else {
    dict_service->mark_force_dump_data_dict();
    DDLOG(INFO, "mark_force_dump_data_dict", K(ctx_tenant_id));
  }

  return ret;
}

}
}
