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

#include "mds_dump_kv_wrapper.h"
#include "mds_dump_node.h"
#include "mds_dump_obj_printer.h"
#include "src/storage/tablet/ob_mds_schema_helper.h"
#include "storage/blocksstable/ob_datum_row.h"

namespace oceanbase
{
namespace storage
{
namespace mds
{

MdsDumpKVStorageMetaInfo::MdsDumpKVStorageMetaInfo()
  : mds_table_id_(UINT8_MAX),
    key_crc_check_number_(UINT32_MAX),
    data_crc_check_number_(UINT32_MAX),
    status_(),
    writer_id_(0),
    seq_no_(),// 0 is currently used value, so default value use another value
    redo_scn_(),
    end_scn_(),
    trans_version_()
{
}

MdsDumpKVStorageMetaInfo::MdsDumpKVStorageMetaInfo(const MdsDumpKV &mds_dump_kv)
  : mds_table_id_(mds_dump_kv.v_.mds_table_id_),
    key_crc_check_number_(mds_dump_kv.k_.crc_check_number_),
    data_crc_check_number_(mds_dump_kv.v_.crc_check_number_),
    status_(mds_dump_kv.v_.status_),
    writer_id_(mds_dump_kv.v_.writer_id_),
    seq_no_(mds_dump_kv.v_.seq_no_),
    redo_scn_(mds_dump_kv.v_.redo_scn_),
    end_scn_(mds_dump_kv.v_.end_scn_),
    trans_version_(mds_dump_kv.v_.trans_version_)
{
}

int64_t MdsDumpKVStorageMetaInfo::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "{");
  databuff_printf(buf, buf_len, pos, "mds_table_id:%ld, ", (int64_t)mds_table_id_);
  databuff_printf(buf, buf_len, pos, "key_crc_check_number:0x%lx, ", (int64_t)key_crc_check_number_);
  databuff_printf(buf, buf_len, pos, "data_crc_check_number:0x%lx, ", (int64_t)data_crc_check_number_);
  databuff_printf(buf, buf_len, pos, "status:%s, ", to_cstring(status_));
  databuff_printf(buf, buf_len, pos, "seq_no:%s, ", to_cstring(seq_no_));
  databuff_printf(buf, buf_len, pos, "writer_id:%ld, ", writer_id_);
  databuff_printf(buf, buf_len, pos, "redo_scn:%s, ", obj_to_string(redo_scn_));
  databuff_printf(buf, buf_len, pos, "end_scn:%s, ", obj_to_string(end_scn_));
  databuff_printf(buf, buf_len, pos, "trans_version:%s", obj_to_string(trans_version_));
  databuff_printf(buf, buf_len, pos, "}");
  return pos;
}

MdsDumpKVStorageAdapter::MdsDumpKVStorageAdapter()
  : type_(UINT8_MAX),
    key_(),
    meta_info_(),
    user_data_()
{
}

MdsDumpKVStorageAdapter::MdsDumpKVStorageAdapter(const MdsDumpKV &mds_dump_kv)
  : type_(mds_dump_kv.k_.mds_unit_id_),
    key_(mds_dump_kv.k_.key_),
    meta_info_(mds_dump_kv),
    user_data_(mds_dump_kv.v_.user_data_)
{
}

int64_t MdsDumpKVStorageAdapter::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "{");
  databuff_printf(buf, buf_len, pos, "type:%ld, ", (int64_t)type_);
  MdsDumpObjPrinter key_printer;
  if (key_.empty()) {
    databuff_printf(buf, buf_len, pos, "duf_key:null, ");
  } else {
    key_printer.help_print<0>(true, meta_info_.mds_table_id_, type_, key_, buf, buf_len, pos);
    databuff_printf(buf, buf_len, pos, ", ");
  }
  databuff_printf(buf, buf_len, pos, "meta_info:%s, ", to_cstring(meta_info_));
  MdsDumpObjPrinter data_printer;
  if (user_data_.empty()) {
    databuff_printf(buf, buf_len, pos, "user_data:null");
  } else {
    data_printer.help_print<0>(false, meta_info_.mds_table_id_, type_, user_data_, buf, buf_len, pos);
  }
  databuff_printf(buf, buf_len, pos, "}");
  return pos;
}

int MdsDumpKVStorageAdapter::convert_to_mds_row(
    ObIAllocator &allocator,
    blocksstable::ObDatumRow &row) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    MDS_LOG(WARN, "invalid row", K(ret), K(row));
  } else if (OB_UNLIKELY(!meta_info_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    MDS_LOG(WARN, "meta info is invalid", K(ret), K_(meta_info));
  } else if (OB_UNLIKELY(row.get_column_count() != ObMdsSchemaHelper::MDS_MULTI_VERSION_ROW_COLUMN_CNT)) {
    ret = OB_INVALID_ARGUMENT;
    MDS_LOG(WARN, "row column count mismatch", K(ret), "row_column_count", row.get_column_count(), K(row));
  } else {
    const int64_t meta_info_size = meta_info_.get_serialize_size();
    char *buf = nullptr;
    int64_t pos = 0;
    if (OB_ISNULL(buf = static_cast<char *> (allocator.alloc(meta_info_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      MDS_LOG(WARN, "failed to alloc buf for seriaize DumpKVStorageMetaInfo",
          K(ret), K(meta_info_size), K(allocator.total()), K(allocator.used()));
    } else if (meta_info_.serialize(buf, meta_info_size, pos)) {
      MDS_LOG(WARN, "failed to serialize DumpKVStorageMetaInfo", K(meta_info_), K(meta_info_size), K(pos));
    } else if (OB_UNLIKELY(pos != meta_info_size)) {
      ret = OB_ERR_UNEXPECTED;
      MDS_LOG(WARN, "unexpected pos with serialize size", K(ret), K(pos), K(meta_info_size));
    } else {
      // DmlFlag: can use MdsNodeStatus to set correct dmlflag in future. @xuwang.txw
      row.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
      row.trans_id_.reset(); // commited row not set
      row.snapshot_version_ = meta_info_.trans_version_.get_val_for_gts();

      row.storage_datums_[ObMdsSchemaHelper::MDS_TYPE_IDX].set_uint(type_);
      row.storage_datums_[ObMdsSchemaHelper::UDF_KEY_IDX].set_string(key_);
      row.storage_datums_[ObMdsSchemaHelper::SNAPSHOT_IDX].set_int(-(meta_info_.trans_version_.get_val_for_tx()));
      row.storage_datums_[ObMdsSchemaHelper::SEQ_NO_IDX].set_int(-(meta_info_.seq_no_.cast_to_int()));
      row.storage_datums_[ObMdsSchemaHelper::META_INFO_IDX].set_string(buf, pos);
      row.storage_datums_[ObMdsSchemaHelper::USER_DATA_IDX].set_string(user_data_);
    }

    if (OB_FAIL(ret) && nullptr != buf) {
      allocator.free(buf);
      buf = nullptr;
    }
  }
  return ret;
}

int MdsDumpKVStorageAdapter::convert_from_mds_row(const blocksstable::ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    MDS_LOG(WARN, "invalid row", K(ret), K(row));
  } else if (OB_UNLIKELY(row.get_column_count() != ObMdsSchemaHelper::MDS_ROW_COLUMN_CNT)) {
    ret = OB_INVALID_ARGUMENT;
    MDS_LOG(WARN, "row column count mismatch", K(ret), "row_column_count", row.get_column_count(), K(row));
  } else {
    constexpr int md_type_idx = 0;
    constexpr int udf_key_idx = 1;
    constexpr int meta_info_idx = 2;
    constexpr int user_data_idx = 3;
    const char *buf = row.storage_datums_[meta_info_idx].ptr_;
    const int64_t buf_len = row.storage_datums_[meta_info_idx].len_;
    int64_t pos = 0;
    if (OB_FAIL(meta_info_.deserialize(buf, buf_len, pos))) {
      MDS_LOG(WARN, "fail to deserialize DumpKVStorageMetaInfo", K(ret), K(buf_len), K(row));
    } else if (OB_UNLIKELY(buf_len != pos)) {
      ret = OB_ERR_UNEXPECTED;
      MDS_LOG(WARN, "unexpected pos with serialize size", K(ret), K(buf_len), K(pos), K(row));
    } else {
      type_ = row.storage_datums_[md_type_idx].get_int8();
      key_ = row.storage_datums_[udf_key_idx].get_string();
      user_data_ = row.storage_datums_[user_data_idx].get_string();
    }
  }
  return ret;
}

int MdsDumpKVStorageAdapter::convert_from_mds_multi_version_row(const blocksstable::ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    MDS_LOG(WARN, "invalid row", K(ret), K(row));
  } else if (OB_UNLIKELY(row.get_column_count() != ObMdsSchemaHelper::MDS_MULTI_VERSION_ROW_COLUMN_CNT)) {
    ret = OB_INVALID_ARGUMENT;
    MDS_LOG(WARN, "multi version row column count mismatch", K(ret), "row_column_count", row.get_column_count(), K(row));
  } else {
    const char *buf = row.storage_datums_[ObMdsSchemaHelper::META_INFO_IDX].ptr_;
    const int64_t buf_len = row.storage_datums_[ObMdsSchemaHelper::META_INFO_IDX].len_;
    int64_t pos = 0;
    if (OB_FAIL(meta_info_.deserialize(buf, buf_len, pos))) {
      MDS_LOG(WARN, "fail to deserialize DumpKVStorageMetaInfo", K(ret), K(buf_len), K(row));
    } else if (OB_UNLIKELY(buf_len != pos)) {
      ret = OB_ERR_UNEXPECTED;
      MDS_LOG(WARN, "unexpected pos with serialize size", K(ret), K(buf_len), K(pos), K(row));
    } else {
      type_ = row.storage_datums_[ObMdsSchemaHelper::MDS_TYPE_IDX].get_int8();
      key_ = row.storage_datums_[ObMdsSchemaHelper::UDF_KEY_IDX].get_string();
      user_data_ = row.storage_datums_[ObMdsSchemaHelper::USER_DATA_IDX].get_string();
    }
  }
  return ret;
}
}
}
}