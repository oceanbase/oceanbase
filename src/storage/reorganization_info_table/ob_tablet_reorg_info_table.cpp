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
#include "ob_tablet_reorg_info_table.h"
#include "share/ob_ls_id.h"
#include "share/schema/ob_table_schema.h"
#include "storage/ls/ob_ls.h"
#include "storage/access/ob_store_row_iterator.h"
#include "storage/tx/ob_trans_ctx_mgr.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tx/ob_keep_alive_ls_handler.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "ob_tablet_reorg_info_table_schema_helper.h"
#include "storage/blocksstable/ob_datum_row.h"

namespace oceanbase
{
using namespace share;
using namespace palf;
using namespace transaction;

namespace storage
{

static const char *TABLET_REORG_INFO_DATA_TYPE[] = {
    "TRANSFER_IN",
    "TRANSFER_OUT",
    "SPLIT_SRC",
    "SPLIT_DST",
};

const char *ObTabletReorgInfoDataType::get_str(const TYPE &type)
{
  const char *str = nullptr;

  if (type < 0 || type >= MAX) {
    str = "UNKNOWN";
  } else {
    str = TABLET_REORG_INFO_DATA_TYPE[type];
  }
  return str;
}

ObTabletReorgInfoDataKey::ObTabletReorgInfoDataKey()
  : tablet_id_(),
    reorganization_scn_(),
    type_(ObTabletReorgInfoDataType::MAX)
{
}

ObTabletReorgInfoDataKey::~ObTabletReorgInfoDataKey()
{
}

void ObTabletReorgInfoDataKey::reset()
{
  tablet_id_.reset();
  reorganization_scn_.reset();
  type_ = ObTabletReorgInfoDataType::MAX;
}

bool ObTabletReorgInfoDataKey::is_valid() const
{
  return tablet_id_.is_valid()
      && reorganization_scn_.is_valid()
      && ObTabletReorgInfoDataType::is_valid(type_);
}

OB_SERIALIZE_MEMBER(ObTabletReorgInfoDataKey,
    tablet_id_, reorganization_scn_, type_);


ObTabletReorgInfoDataValue::ObTabletReorgInfoDataValue()
  : pos_(0)
{
  value_[0] = '\0';
}

ObTabletReorgInfoDataValue::~ObTabletReorgInfoDataValue()
{
}

int ObTabletReorgInfoDataValue::assign(const char *str, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (NULL == str) {
    pos_ = 0;
  } else {
    if (buf_len >= MAX_VALUE_SIZE) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("buf is not long enough", K(ret), K(buf_len));
    } else {
      MEMCPY(value_, str, buf_len);
      pos_ = buf_len;
    }
  }
  return ret;
}

int ObTabletReorgInfoDataValue::assign(const ObTabletReorgInfoDataValue &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(assign(other.value_, other.pos_))) {
      LOG_WARN("failed to assign tablet reorg info data value", K(other), K(ret));
    }
  }
  return ret;
}

ObTabletReorgInfoDataValue &ObTabletReorgInfoDataValue::operator =(
    const ObTabletReorgInfoDataValue &value)
{
  int ret = OB_SUCCESS;
  if (this != &value) {
    if (OB_FAIL(assign(value))) {
      LOG_WARN("failed to assign", K(ret), K(value));
    }
  }
  return *this;
}

bool ObTabletReorgInfoDataValue::is_empty() const
{
  return (0 == pos_);
}

bool ObTabletReorgInfoDataValue::is_valid() const
{
  return !is_empty();
}

int ObTabletReorgInfoDataValue::write_value(const ObITabletReorgInfoDataValue &data_value)
{
  int ret = OB_SUCCESS;
  if (!data_value.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet reorg info data value get invalid argument", K(ret), K(data_value));
  } else {
    const int64_t serialize_size = data_value.get_serialize_size();
    int64_t pos = 0;
    if (serialize_size >= MAX_VALUE_SIZE) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data value serialize size larger than max value size", K(ret), K(serialize_size), K(data_value));
    } else if (OB_FAIL(data_value.serialize(value_, MAX_VALUE_SIZE, pos))) {
      LOG_WARN("failed to serialize data value", K(ret), K(data_value));
    } else {
      pos_ = pos;
    }
  }
  return ret;
}

int ObTabletReorgInfoDataValue::write_value(const char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("write value get invalid argument", K(ret), KP(buf), K(buf_len));
  } else if (buf_len >= MAX_VALUE_SIZE) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buf is not long enough", K(ret), K(buf_len));
  } else {
    MEMCPY(value_, buf, buf_len);
    pos_ = buf_len;
  }
  return ret;
}

int ObTabletReorgInfoDataValue::get_value(ObITabletReorgInfoDataValue &data_value) const
{
  int ret = OB_SUCCESS;
  data_value.reset();
  int64_t pos = 0;
  if (is_empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet reorg info data is empty, unexpected", K(ret));
  } else if (OB_FAIL(data_value.deserialize(value_, pos_, pos))) {
    LOG_WARN("failed to deserialize data value", K(ret));
  }
  return ret;
}

int ObTabletReorgInfoDataValue::get_value(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  pos = 0;
  const int64_t local_value_length = size();

  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get value get invalid argument", K(ret), KP(buf), K(buf_len));
  } else if (buf_len < local_value_length) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buf is not long enough", K(ret), K(buf_len));
  } else {
    MEMCPY(buf, value_, local_value_length);
    pos = local_value_length;
  }
  return ret;
}

int64_t ObTabletReorgInfoDataValue::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (buf_len < pos_) {
    pos = 0;
  } else {
    pos = snprintf(buf, pos_, "%s", value_);
  }
  return pos;
}

ObTransferDataValue::ObTransferDataValue()
    : tablet_status_(),
      transfer_seq_(-1),
      relative_ls_id_(),
      transfer_scn_(),
      src_reorganization_scn_()
{
}

ObTransferDataValue::~ObTransferDataValue()
{
}

void ObTransferDataValue::reset()
{
  tablet_status_ = ObTabletStatus::MAX;
  transfer_seq_ = -1;
  relative_ls_id_.reset();
  transfer_scn_.reset();
  src_reorganization_scn_.reset();
}

bool ObTransferDataValue::is_valid() const
{
  return tablet_status_.is_valid()
      && transfer_seq_ >= 0
      && relative_ls_id_.is_valid()
      && transfer_scn_.is_valid()
      && src_reorganization_scn_.is_valid();
}

int64_t ObTransferDataValue::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "{");
  databuff_printf(buf, buf_len, pos, "tablet_status:%s", ObTabletStatus::get_str(tablet_status_));
  databuff_printf(buf, buf_len, pos, ", ");
  databuff_printf(buf, buf_len, pos, "transfer_seq:%ld", transfer_seq_);
  databuff_printf(buf, buf_len, pos, ", ");
  databuff_printf(buf, buf_len, pos, "relative_ls_id:%ld", relative_ls_id_.id());
  databuff_printf(buf, buf_len, pos, ", ");
  databuff_printf(buf, buf_len, pos, "transfer_scn:%ld", transfer_scn_.get_val_for_tx());
  databuff_printf(buf, buf_len, pos, ", ");
  databuff_printf(buf, buf_len, pos, "src_reorganization_scn:%ld", src_reorganization_scn_.get_val_for_tx());
  databuff_printf(buf, buf_len, pos, "}");
  return pos;
}

OB_SERIALIZE_MEMBER(ObTransferDataValue,
    tablet_status_, transfer_seq_, relative_ls_id_, transfer_scn_, src_reorganization_scn_);

ObTabletReorgInfoData::ObTabletReorgInfoData()
  : key_(),
    value_()
{
}

ObTabletReorgInfoData::~ObTabletReorgInfoData()
{
}

void ObTabletReorgInfoData::reset()
{
  key_.reset();
  value_.reset();
}

bool ObTabletReorgInfoData::is_valid() const
{
  return key_.is_valid()
      && value_.is_valid();
}

int ObTabletReorgInfoData::data_2_datum_row(
    common::ObIAllocator &allocator,
    blocksstable::ObDatumRow *datum_row) const
{
  int ret = OB_SUCCESS;
  const share::schema::ObTableSchema *table_schema = nullptr;
  storage::ObColDescArray column_list;

  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet reorg info data is invalid", K(ret), KPC(this));
  } else if (OB_ISNULL(datum_row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("data 2 datum row get invalid argument", K(ret), KP(datum_row), KPC(this));
  } else if (OB_ISNULL(table_schema = ObTabletReorgInfoTableSchemaHelper::get_instance().get_table_schema())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("member table schema should not be NULL", K(ret), KP(table_schema), KPC(this));
  } else if (OB_FAIL(table_schema->get_column_ids(column_list))) {
    LOG_WARN("failed to get column ids", K(ret), KPC(table_schema), KPC(this));
  } else if (datum_row->get_capacity() < column_list.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("datum row capacity is smaller than column list count", K(ret), KPC(datum_row), K(column_list));
  } else if (column_list.count() != ObTabletReorgInfoTableSchemaDef::REORG_INFO_ROW_COLUMN_CNT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column list count is not match with member row column count, unexpected", K(ret), K(column_list));
  } else {
    for (int64_t column_idx = 0; OB_SUCC(ret) && column_idx < column_list.count(); ++column_idx) {
      const ObTabletReorgInfoTableSchemaDef::REORG_INFO_TABLE_SCHEMA_COL_IDX type = static_cast<ObTabletReorgInfoTableSchemaDef::REORG_INFO_TABLE_SCHEMA_COL_IDX>(column_idx);
      switch (type) {
        case ObTabletReorgInfoTableSchemaDef::REORG_INFO_TABLE_SCHEMA_COL_IDX::TABLET_ID: {
          datum_row->storage_datums_[column_idx].set_uint(key_.tablet_id_.id());
          break;
        }
        case ObTabletReorgInfoTableSchemaDef::REORG_INFO_TABLE_SCHEMA_COL_IDX::REORGANIZATION_SCN : {
          datum_row->storage_datums_[column_idx].set_int(key_.reorganization_scn_.get_val_for_tx());
          break;
        }
        case ObTabletReorgInfoTableSchemaDef::REORG_INFO_TABLE_SCHEMA_COL_IDX::DATA_TYPE : {
          datum_row->storage_datums_[column_idx].set_int(static_cast<int64_t>(key_.type_));
          break;
        }
        case ObTabletReorgInfoTableSchemaDef::REORG_INFO_TABLE_SCHEMA_COL_IDX::DATA_VALUE: {
          const int64_t buffer_len = value_.size();
          char *buffer = nullptr;
          int64_t pos = 0;
          if (nullptr == (buffer = (char *)allocator.alloc(buffer_len))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            STORAGE_LOG(WARN, "fail to serialize tablet reorg info data value, cause buffer allocated failed",
                              KR(ret), K(*this));
          } else if (OB_FAIL(value_.get_value(buffer, buffer_len, pos))) {
            LOG_WARN("failed to get value", K(ret), KPC(this));
          } else {
            ObString value_string(pos, buffer);
            datum_row->storage_datums_[column_idx].set_string(value_string);
          }
          break;
        }
        default: {
          ret = OB_INVALID_ARGUMENT;
          LOG_ERROR("invalid type for fail", K(ret), K(type));
        }
      }
    }
  }
  return ret;
}

int ObTabletReorgInfoData::row_2_data(
    const blocksstable::ObDatumRow *new_row,
    share::SCN &trans_scn,
    int64_t &sql_no)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(new_row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row 2 data get invalid argument", K(ret), KP(new_row));
  } else if (new_row->get_column_count() != ObTabletReorgInfoTableSchemaDef::REORG_INFO_MULTI_VERSION_ROW_COLUMN_CNT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new row column count is not match schema column count", K(ret), KPC(new_row));
  } else {
    for (int64_t column_idx = 0; OB_SUCC(ret) && column_idx < new_row->get_column_count(); ++column_idx) {
      const ObDatum &obj = new_row->storage_datums_[column_idx];
      const ObTabletReorgInfoTableSchemaDef::REORG_INFO_SSTABLE_COL_IDX type = static_cast<ObTabletReorgInfoTableSchemaDef::REORG_INFO_SSTABLE_COL_IDX>(column_idx);
      switch (type) {
        case ObTabletReorgInfoTableSchemaDef::REORG_INFO_SSTABLE_COL_IDX::TABLET_ID : {
          key_.tablet_id_ = obj.get_int();
          break;
        }
        case ObTabletReorgInfoTableSchemaDef::REORG_INFO_SSTABLE_COL_IDX::REORGANIZATION_SCN : {
          SCN tmp_scn;
          if (OB_FAIL(tmp_scn.convert_for_tx(obj.get_int()))) {
            LOG_WARN("failed to convert for tx", K(ret), K(obj));
          } else {
            key_.reorganization_scn_ = tmp_scn;
          }
          break;
        }
        case ObTabletReorgInfoTableSchemaDef::REORG_INFO_SSTABLE_COL_IDX::DATA_TYPE : {
          SCN tmp_scn;
          key_.type_ = static_cast<ObTabletReorgInfoDataType::TYPE>(obj.get_int());
          break;
        }
        case ObTabletReorgInfoTableSchemaDef::REORG_INFO_SSTABLE_COL_IDX::TRANS_VERSION : {
          SCN tmp_scn;
          if (OB_FAIL(tmp_scn.convert_for_tx(obj.get_int()))) {
            LOG_WARN("failed to convert for tx", K(ret), K(obj));
          } else {
            trans_scn = tmp_scn;
          }
          break;
        }
        case ObTabletReorgInfoTableSchemaDef::REORG_INFO_SSTABLE_COL_IDX::SQL_SEQ : {
          sql_no = obj.get_int();
          break;
        }
        case ObTabletReorgInfoTableSchemaDef::REORG_INFO_SSTABLE_COL_IDX::DATA_VALUE : {
          const ObString &tmp_string = obj.get_string();
          if (OB_FAIL(value_.write_value(tmp_string.ptr(), tmp_string.length()))) {
            LOG_WARN("failed to write tablet reorg info data value", K(ret), K(tmp_string));
          }
          break;
        }
        default: {
          ret = OB_INVALID_ARGUMENT;
          LOG_ERROR("invalid type for fail", K(ret), K(type));
        }
      }
    }
  }
  return ret;
}

int ObTabletReorgInfoData::get_transfer_data_value(ObTransferDataValue &data_value) const
{
  int ret = OB_SUCCESS;
  data_value.reset();
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet reorg info data is invalid", K(ret), KPC(this));
  } else if (!ObTabletReorgInfoDataType::is_transfer(key_.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet reorg info data is not transfer, unexpected", K(ret), KPC(this));
  } else if (OB_FAIL(value_.get_value(data_value))) {
    LOG_WARN("failed to get transfer data value", K(ret), KPC(this));
  }
  return ret;
}

int64_t ObTabletReorgInfoData::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  int tmp_ret = OB_SUCCESS;
  databuff_printf(buf, buf_len, pos, "{");
  databuff_printf(buf, buf_len, pos, "key:");
  databuff_printf(buf, buf_len, pos, key_);
  databuff_printf(buf, buf_len, pos, ", ");
  pos += to_value_string(buf + pos, buf_len);
  databuff_printf(buf, buf_len, pos, "}");
  return pos;
}

int64_t ObTabletReorgInfoData::to_value_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  int tmp_ret = OB_SUCCESS;
  const int64_t value_buf_length = ObTabletReorgInfoTableSchemaDef::DATA_VALUE_COLUMN_LENGTH;
  char value_buf[value_buf_length] = {0};
  if (ObTabletReorgInfoDataType::is_transfer(key_.type_)) {
    ObTransferDataValue transfer_data_value;
    if (OB_SUCCESS != (tmp_ret = get_transfer_data_value(transfer_data_value))) {
      LOG_WARN_RET(tmp_ret, "failed to get transfer data value", KPC(this));
    } else {
      const int64_t tmp_pos = transfer_data_value.to_string(value_buf, buf_len);
    }
  }
  databuff_printf(buf, buf_len, pos, "value:%s", value_buf);
  return pos;
}

ObTabletReorgInfoTable::ObTabletReorgInfoTable()
  : is_inited_(false),
    ls_id_(),
    ls_(nullptr)
{
}

ObTabletReorgInfoTable::~ObTabletReorgInfoTable()
{
}

int ObTabletReorgInfoTable::init(ObLS *ls)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet reorg info table is already init", K(ret));
  } else if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init tablet reorg info table get invalid argument", K(ret), KP(ls));
  } else {
    ls_ = ls;
    ls_id_ = ls->get_ls_id();
    is_inited_ = true;
  }
  return ret;
}

int ObTabletReorgInfoTable::start()
{
  int ret = OB_SUCCESS;
  LOG_INFO("tablet reorg info table start finish", KR(ret), KPC(this));
  return ret;
}

void ObTabletReorgInfoTable::stop()
{
  LOG_INFO("tablet reorg info table stop finish", KPC(this));
}

void ObTabletReorgInfoTable::destroy()
{
  ls_id_.reset();
  ls_ = nullptr;
  is_inited_ = false;
}

int ObTabletReorgInfoTable::offline()
{
  int ret = OB_SUCCESS;
  ObTabletHandle handle;
  ObTablet *tablet = nullptr;
  ObLSTabletService *ls_tablet_svr = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet reorg info table is not init", KR(ret));
  } else if (OB_ISNULL(ls_tablet_svr = ls_->get_tablet_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls tablet service should not be NULL", K(ret), KP(ls_tablet_svr));
  } else if (OB_FAIL(ls_tablet_svr->get_tablet(LS_REORG_INFO_TABLET, handle))) {
    LOG_WARN("get tablet failed", K(ret));
    if (OB_TABLET_NOT_EXIST == ret) {
      // a ls that of migrate does not have member tablet
      ret = OB_SUCCESS;
    }
  } else if (OB_ISNULL(tablet = handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("member table tablet should not be NULL", K(ret), KP(tablet));
  } else if (OB_FAIL(tablet->release_memtables())) {
    LOG_WARN("failed to release tablet reorg info table memtables", K(ret), KPC(ls_));
  }
  return ret;
}

int ObTabletReorgInfoTable::online()
{
  int ret = OB_SUCCESS;
  ObTabletHandle handle;
  ObTablet *tablet = nullptr;
  ObLSTabletService *ls_tablet_svr = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet reorg info table is not init.", KR(ret));
  } else if (OB_ISNULL(ls_tablet_svr = ls_->get_tablet_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls tablet service should not be NULL", K(ret), KP(ls_tablet_svr));
  } else if (OB_FAIL(ls_tablet_svr->get_tablet(LS_REORG_INFO_TABLET, handle))) {
    LOG_WARN("get tablet failed", K(ret));
  } else if (OB_ISNULL(tablet = handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet reorg info tablet should not be NULL", K(ret), KP(tablet));
  }
  return ret;
}

int ObTabletReorgInfoTable::remove_tablet()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet reorg info table is not init.", KR(ret));
  } else if (OB_FAIL(ls_->remove_ls_inner_tablet(ls_id_, LS_REORG_INFO_TABLET))) {
    LOG_WARN("remove ls inner tablet failed", K(ret), K(ls_id_), K(LS_REORG_INFO_TABLET));
    if (OB_TABLET_NOT_EXIST == ret) {
      //do nothing
    }
  }
  return ret;
}

int ObTabletReorgInfoTable::create_tablet(
    const share::SCN &create_scn)
{
  int ret = OB_SUCCESS;
  const schema::ObTableSchema *table_schema = nullptr;
  ObArenaAllocator arena_allocator;
  ObCreateTabletSchema create_tablet_schema;
  const lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::MYSQL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet reorg info table is not init.", KR(ret));
  } else if (!create_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet reorg info table create tablet get invalid argument", K(ret), K(create_scn));
  } else if (OB_ISNULL(table_schema = ObTabletReorgInfoTableSchemaHelper::get_instance().get_table_schema())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("member table table schema should not be NULL", K(ret), KP(table_schema));
  } else if (OB_FAIL(create_tablet_schema.init(arena_allocator, *table_schema, compat_mode,
      false/*skip_column_info*/, DATA_CURRENT_VERSION))) {
    LOG_WARN("failed to init storage schema", KR(ret), K(table_schema));
  } else if (OB_FAIL(ls_->create_ls_inner_tablet(ls_id_,
      LS_REORG_INFO_TABLET,
      ObLS::LS_INNER_TABLET_FROZEN_SCN,
      create_tablet_schema,
      create_scn))) {
    LOG_WARN("create tablet reorg info table tablet failed", K(ret), K(ls_id_),
        K(LS_REORG_INFO_TABLET), K(ObLS::LS_INNER_TABLET_FROZEN_SCN),
        K(table_schema), K(compat_mode), K(create_scn));
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(remove_tablet())) {
      LOG_WARN("remove tablet failed", K(tmp_ret));
    }
  }
  return ret;
}

int ObTabletReorgInfoTable::update_already_recycled_scn(
    const share::SCN &already_recycled_scn)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet reorg info table is not init.", KR(ret));
  } else if (!already_recycled_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update already recycled scn get invalid argument", K(ret), K(already_recycled_scn));
  } else if (OB_FAIL(recycle_scn_cache_.update_already_recycled_scn(already_recycled_scn))) {
    LOG_WARN("failed to update already recycled scn", K(ret), K(already_recycled_scn));
  } else {
    LOG_INFO("update already recycled scn", K(already_recycled_scn), K(recycle_scn_cache_));
  }
  return ret;
}

int ObTabletReorgInfoTable::update_can_recycle_scn(
    const share::SCN &can_recycle_scn)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tabelt reorg info table is not init.", KR(ret));
  } else if (!can_recycle_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update already recycle scn get invalid argument", K(ret), K(can_recycle_scn));
  } else if (OB_FAIL(recycle_scn_cache_.update_can_recycle_scn(can_recycle_scn))) {
    LOG_WARN("failed to update can recycle scn", K(ret), K(can_recycle_scn));
  } else {
    LOG_INFO("update can recycle scn", K(can_recycle_scn), K(recycle_scn_cache_));
  }
  return ret;
}

int ObTabletReorgInfoTable::get_can_recycle_scn(share::SCN &can_recycle_scn)
{
  int ret = OB_SUCCESS;
  can_recycle_scn.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet reorg info table is not init.", KR(ret));
  } else {
    can_recycle_scn = recycle_scn_cache_.get_can_reycle_scn();
  }
  return ret;
}

int ObTabletReorgInfoTable::init_tablet_for_compat()
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  const share::SCN create_scn(SCN::base_scn());

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet reorg info table is not init.", KR(ret));
  } else if (OB_FAIL(ls_->get_tablet_svr()->is_tablet_exist(LS_REORG_INFO_TABLET, is_exist))) {
    LOG_WARN("failed to check tablet exist", K(ret));
  } else if (is_exist) {
    //do nothing
  } else if (OB_FAIL(create_tablet(create_scn))) {
    LOG_WARN("failed to create member tablet", K(ret), KPC(ls_));
  }
  return ret;
}

ObTabletReorgInfoTable::RecycleSCNCache::RecycleSCNCache()
  : lock_(),
    already_recycled_scn_(share::SCN::min_scn()),
    can_recycle_scn_(share::SCN::min_scn()),
    update_ts_(0)
{
}

void ObTabletReorgInfoTable::RecycleSCNCache::reset()
{
  common::SpinWLockGuard guard(lock_);
  already_recycled_scn_.reset();
  can_recycle_scn_.reset();
  update_ts_ = 0;
}

int ObTabletReorgInfoTable::RecycleSCNCache::update_already_recycled_scn(
    const share::SCN &already_recycled_scn)
{
  int ret = OB_SUCCESS;
  common::SpinWLockGuard guard(lock_);
  if (already_recycled_scn.is_valid()) {
    already_recycled_scn_ = share::SCN::max(already_recycled_scn_, already_recycled_scn);
    update_ts_ = ObTimeUtil::current_time();
  }
  return ret;
}

int ObTabletReorgInfoTable::RecycleSCNCache::update_can_recycle_scn(
    const share::SCN &can_recycle_scn)
{
  int ret = OB_SUCCESS;
  common::SpinWLockGuard guard(lock_);
  if (can_recycle_scn.is_valid()) {
    can_recycle_scn_ = share::SCN::max(can_recycle_scn_, can_recycle_scn);
    update_ts_ = ObTimeUtil::current_time();
  }
  return ret;
}

SCN ObTabletReorgInfoTable::RecycleSCNCache::get_already_recycle_scn()
{
  common::SpinRLockGuard guard(lock_);
  return already_recycled_scn_;
}

SCN ObTabletReorgInfoTable::RecycleSCNCache::get_can_reycle_scn()
{
  common::SpinRLockGuard guard(lock_);
  return can_recycle_scn_;
}


} //storage
} //oceanbase
