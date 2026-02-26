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

#include "share/ob_dml_sql_splicer.h" // ObDMLSqlSplicer
#include "lib/mysqlclient/ob_mysql_proxy.h" // ObISqlClient, SMART_VAR
#include "lib/mysqlclient/ob_mysql_transaction.h" // ObMySQLTransaction
#include "ob_tablet_reorg_info_table_operation.h"
#include "ob_tablet_reorg_info_table_schema_helper.h"
#include "share/inner_table/ob_inner_table_schema.h" // OB_ALL_TRANSFER_TASK_TNAME
#include "share/location_cache/ob_location_struct.h" // ObTabletLSCache
#include "share/schema/ob_schema_utils.h" // ObSchemaUtils
#include "observer/ob_server_struct.h" // GCTX
#include "storage/blocksstable/ob_datum_row_utils.h"
#include "observer/ob_inner_sql_connection.h"
#include "storage/access/ob_table_scan_iterator.h"
#include "storage/ob_inner_tablet_access_service.h"
#include "storage/blocksstable/ob_datum_row_iterator.h"
#include "storage/tx_storage/ob_ls_service.h"
#define USING_LOG_PREFIX STORAGE

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace storage
{

int ObTabletReorgInfoTableDataGenerator::gen_transfer_reorg_info_data(
    const common::ObTabletID &tablet_id,
    const share::SCN &reorganization_scn,
    const ObTabletStatus &tablet_status,
    const share::ObLSID &relative_ls_id,
    const int64_t transfer_seq,
    const share::SCN &transfer_scn,
    const share::SCN &src_reorganization_scn,
    common::ObIArray<ObTabletReorgInfoData> &reorg_info_data)
{
  //member_table_data is in/out parameter
  int ret = OB_SUCCESS;
  if (!tablet_id.is_valid() || !reorganization_scn.is_valid() || !tablet_status.is_valid()
      || !relative_ls_id.is_valid() || transfer_seq < 0 || !transfer_scn.is_valid()
      || (tablet_status != ObTabletStatus::TRANSFER_IN && tablet_status != ObTabletStatus::TRANSFER_OUT)
      || !src_reorganization_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("gen tablet reorg info table data get invalid argument", K(ret), K(tablet_id), K(reorganization_scn), K(tablet_status),
        K(relative_ls_id), K(transfer_seq), K(transfer_scn), K(src_reorganization_scn));
  } else {
    ObTransferDataValue transfer_data_value;
    ObTabletReorgInfoData data;
    data.key_.tablet_id_ = tablet_id;
    data.key_.reorganization_scn_ = reorganization_scn;
    data.key_.type_ = tablet_status == ObTabletStatus::TRANSFER_IN ? ObTabletReorgInfoDataType::TRANSFER_IN : ObTabletReorgInfoDataType::TRANSFER_OUT;
    transfer_data_value.tablet_status_ = tablet_status;
    transfer_data_value.transfer_seq_ = transfer_seq;
    transfer_data_value.relative_ls_id_ = relative_ls_id;
    transfer_data_value.transfer_scn_ = transfer_scn;
    transfer_data_value.src_reorganization_scn_ = src_reorganization_scn;

    if (OB_FAIL(data.value_.write_value(transfer_data_value))) {
      LOG_WARN("failed to write transfer data value", K(ret), K(transfer_data_value));
    } else if (!data.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("member data is invalid, unexpected", K(ret), K(data));
    } else if (OB_FAIL(reorg_info_data.push_back(data))) {
      LOG_WARN("failed to push back data", K(ret), K(data));
    }
  }
  return ret;
}

ObTabletReorgInfoTableWriteOperator::ObTabletReorgInfoTableWriteOperator()
  : is_inited_(false),
    allocator_(),
    sql_proxy_(nullptr),
    tenant_id_(OB_INVALID_ID),
    ls_id_()
{
}

ObTabletReorgInfoTableWriteOperator::~ObTabletReorgInfoTableWriteOperator()
{
}

int ObTabletReorgInfoTableWriteOperator::init(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  const int64_t buffer_len = 16 * 1024; //16K
  char *buffer = nullptr;
  int64_t pos = 0;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet reorg info table write operator already init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet reorg info table write operator init get invalid argument", K(ret), K(tenant_id), K(ls_id));
  } else if (nullptr == (buffer = (char *)allocator_.alloc(buffer_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buffer", KR(ret), K(buffer_len));
  } else if (FALSE_IT(MEMSET(buffer, 0, buffer_len))) {
  } else if (!data_buffer_.set_data(buffer, buffer_len)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to set data buffer", K(ret), KP(buffer), K(buffer_len));
  } else {
    sql_proxy_ = &sql_proxy;
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletReorgInfoTableWriteOperator::insert_row(
    const ObTabletReorgInfoData &data)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet reorg info table write operator do not init", K(ret));
  } else if (!data.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("insert row get invalid argument", K(ret), K(data));
  } else if (OB_FAIL(inner_insert_row_(data, affected_rows))) {
    LOG_WARN("failed to do inner insert row", K(ret), K(tenant_id_), K(ls_id_), K(data));
  }

  if (OB_SUCC(ret)) {
    if (1 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected rows unexpected", K(ret), K(affected_rows), K(tenant_id_), K(ls_id_), K(data));
    } else if (OB_FAIL(flush_and_wait_())) {
      LOG_WARN("failed to flush and wait", K(ret), K(tenant_id_), K(ls_id_));
    }
  }
  return ret;
}

int ObTabletReorgInfoTableWriteOperator::insert_rows(
    const common::ObIArray<ObTabletReorgInfoData> &member_table_data)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  if (!is_inited_) {
     ret = OB_NOT_INIT;
     LOG_WARN("tablet reorg info table write operator do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < member_table_data.count(); ++i) {
      const ObTabletReorgInfoData &data = member_table_data.at(i);
      if (!data.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("tablet reorg info table data is invalid", K(ret), K(data));
      } else if (OB_FAIL(inner_insert_row_(data, affected_rows))) {
        LOG_WARN("failed to do inner insert row", K(ret), K(data));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(flush_and_wait_())) {
        LOG_WARN("failed to flush and wait", K(ret), K(tenant_id_), K(ls_id_));
      } else if (affected_rows != member_table_data.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("member table data count is not match with affected rows, unexpected", K(ret),
            K(member_table_data), K(affected_rows));
      }
    }
  }
  return ret;
}

int ObTabletReorgInfoTableWriteOperator::inner_insert_row_(
    const ObTabletReorgInfoData &data,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  blocksstable::ObDatumRow *datum_row = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet reorg info table write operator do not init", K(ret));
  } else if (OB_FAIL(blocksstable::ObDatumRowUtils::ob_create_row(allocator,
      ObTabletReorgInfoTableSchemaDef::REORG_INFO_ROW_COLUMN_CNT, datum_row))) {
    LOG_WARN("create current datum row failed", K(ret), K(data));
  } else if (OB_FAIL(data.data_2_datum_row(allocator, datum_row))) {
    LOG_WARN("failed to convert data to datum row", K(ret), K(data));
  } else if (OB_FAIL(fill_data_(datum_row))) {
    LOG_WARN("failed to fill data", K(ret), K(tenant_id_), K(ls_id_));
  } else {
    affected_rows++;
  }

  if (OB_NOT_NULL(datum_row)) {
    datum_row->~ObDatumRow();
    datum_row = nullptr;
  }
  return ret;
}

int ObTabletReorgInfoTableWriteOperator::fill_data_(
    blocksstable::ObDatumRow *datum_row)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet reorg info table write operator do not init", K(ret));
  } else if (OB_ISNULL(datum_row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fill data get invalid argument", K(ret), KP(datum_row), K(tenant_id_), K(ls_id_));
  } else if (NULL == (data_buffer_.get_data())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buffer should not be NULL", K(ret), K(data_buffer_));
  } else if (datum_row->get_serialize_size() > data_buffer_.get_remain()) {
    if (0 == data_buffer_.get_position()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("data is too large", K(ret), KPC(datum_row));
    } else if (OB_FAIL(flush_and_wait_())) {
      STORAGE_LOG(WARN, "failed to flush_and_wait", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialization::encode(data_buffer_.get_data(),
        data_buffer_.get_capacity(),
        data_buffer_.get_position(),
        *datum_row))) {
      LOG_WARN("failed to encode", K(ret), KPC(datum_row));
    }
  }
  return ret;
}

int ObTabletReorgInfoTableWriteOperator::flush_and_wait_()
{
  int ret = OB_SUCCESS;
  observer::ObInnerSQLConnection *conn = NULL;
  int64_t affected_rows = -1;

  if (0 == data_buffer_.get_position()) {
    //do nothing
  } else if (OB_ISNULL(conn = static_cast<observer::ObInnerSQLConnection *>(sql_proxy_->get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("conn is NULL", KR(ret));
  } else if (OB_FAIL(conn->execute_inner_tablet_write(tenant_id_, ls_id_,
      LS_REORG_INFO_TABLET, data_buffer_.get_data(), data_buffer_.get_position(), affected_rows))) {
    LOG_WARN("failed to execute inner tablet write", K(ret), K(tenant_id_), K(ls_id_));
  } else {
    data_buffer_.get_position() = 0;
  }
  return ret;
}


ObTabletReorgInfoTableReadOperator::ObTabletReorgInfoTableReadOperator()
  : is_inited_(false),
    new_range_(),
    iter_(),
    allow_read_(true)
{
  for (int64_t i = 0; i < ObTabletReorgInfoTableSchemaDef::ROWKEY_COLUMN_NUM; ++i) {
    start_key_obj_[i].reset();
    end_key_obj_[i].reset();
  }
}

ObTabletReorgInfoTableReadOperator::~ObTabletReorgInfoTableReadOperator()
{
}

int ObTabletReorgInfoTableReadOperator::init(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObTabletReorgInfoDataKey &key,
    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet reorg info table read operation is already init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || !ls_id.is_valid() || !key.is_valid() || timeout_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init tablet reorg info table read operation get invalid argument", K(ret),
        K(tenant_id), K(ls_id), K(key), K(timeout_us));
  } else {
    const bool is_get = true;
    start_key_obj_[0].set_int(key.tablet_id_.id());
    start_key_obj_[1].set_int(key.reorganization_scn_.get_val_for_tx());
    start_key_obj_[2].set_int(static_cast<int64_t>(key.type_));
    const ObRowkey rowkey(start_key_obj_, ObTabletReorgInfoTableSchemaDef::ROWKEY_COLUMN_NUM);
    if (OB_FAIL(new_range_.build_range(ObTabletReorgInfoTableSchemaDef::REORG_INFO_TABLE_ID, rowkey))) {
      LOG_WARN("fail to build key range", K(ret), K(rowkey));
    } else if (OB_FAIL(inner_init_(tenant_id, ls_id, is_get, timeout_us))) {
      LOG_WARN("failed to init", K(ret), K(tenant_id), K(ls_id), K(is_get));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTabletReorgInfoTableReadOperator::init(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet reorg info table read operation is already init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || !ls_id.is_valid() || !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init tablet reorg info table read operation get invalid argument", K(ret),
        K(tenant_id), K(ls_id), K(tablet_id));
  } else {
    const bool is_get = false;
    start_key_obj_[0].set_int(tablet_id.id());
    start_key_obj_[1].set_min_value();
    start_key_obj_[2].set_min_value();
    end_key_obj_[0].set_int(tablet_id.id());
    end_key_obj_[1].set_max_value();
    end_key_obj_[2].set_max_value();
    new_range_.table_id_ = ObTabletReorgInfoTableSchemaDef::REORG_INFO_TABLE_ID;
    new_range_.start_key_.assign(start_key_obj_, ObTabletReorgInfoTableSchemaDef::ROWKEY_COLUMN_NUM);
    new_range_.end_key_.assign(end_key_obj_, ObTabletReorgInfoTableSchemaDef::ROWKEY_COLUMN_NUM);
    new_range_.border_flag_.unset_inclusive_start();
    new_range_.border_flag_.unset_inclusive_end();
    new_range_.flag_ = 0;
    if (OB_FAIL(inner_init_(tenant_id, ls_id, is_get, timeout_us))) {
      LOG_WARN("failed to init", K(ret), K(tenant_id), K(ls_id), K(is_get));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTabletReorgInfoTableReadOperator::init(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet reorg info table read operation is already init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init tablet reorg info table read operation get invalid argument", K(ret),
        K(tenant_id), K(ls_id));
  } else {
    const bool is_get = false;
    new_range_.start_key_.assign(start_key_obj_, ObTabletReorgInfoTableSchemaDef::ROWKEY_COLUMN_NUM);
    new_range_.end_key_.assign(end_key_obj_, ObTabletReorgInfoTableSchemaDef::ROWKEY_COLUMN_NUM);
    new_range_.table_id_ = ObTabletReorgInfoTableSchemaDef::REORG_INFO_TABLE_ID;
    new_range_.set_whole_range();
    if (OB_FAIL(inner_init_(tenant_id, ls_id, is_get, timeout_us))) {
      LOG_WARN("failed to init", K(ret), K(tenant_id), K(ls_id), K(is_get));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTabletReorgInfoTableReadOperator::init(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObTabletReorgInfoDataType::TYPE &start_type,
    const ObTabletReorgInfoDataType::TYPE &end_type,
    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet reorg info table read operation is already init", K(ret));
  } else if (OB_INVALID_ID == tenant_id || !ls_id.is_valid() || !ObTabletReorgInfoDataType::is_valid(start_type)
      || !ObTabletReorgInfoDataType::is_valid(end_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init tablet reorg info table read operation get invalid argument", K(ret),
        K(tenant_id), K(ls_id), K(start_type), K(end_type));
  } else {
    const bool is_get = false;
    start_key_obj_[0].set_min_value();
    start_key_obj_[1].set_min_value();
    start_key_obj_[2].set_int(static_cast<int64_t>(start_type));
    end_key_obj_[0].set_max_value();
    end_key_obj_[1].set_max_value();
    end_key_obj_[2].set_int(static_cast<int64_t>(end_type));
    new_range_.table_id_ = ObTabletReorgInfoTableSchemaDef::REORG_INFO_TABLE_ID;
    new_range_.start_key_.assign(start_key_obj_, ObTabletReorgInfoTableSchemaDef::ROWKEY_COLUMN_NUM);
    new_range_.end_key_.assign(end_key_obj_, ObTabletReorgInfoTableSchemaDef::ROWKEY_COLUMN_NUM);
    new_range_.border_flag_.unset_inclusive_start();
    new_range_.border_flag_.unset_inclusive_end();
    new_range_.flag_ = 0;
    if (OB_FAIL(inner_init_(tenant_id, ls_id, is_get, timeout_us))) {
      LOG_WARN("failed to init", K(ret), K(tenant_id), K(ls_id), K(is_get));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}


int ObTabletReorgInfoTableReadOperator::inner_init_(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const bool is_get,
    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = nullptr;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  const int64_t abs_timeout_ts = timeout_us + ObClockGenerator::getClock();
  ObMigrationStatus migration_status;

  if (OB_ISNULL(ls_service = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObLSService from MTL", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), K(ls_handle));
  } else if (OB_FAIL(ls->get_migration_status(migration_status))) {
    LOG_WARN("failed to get migration status", K(ret), K(ls_id), KPC(ls));
  } else if (ObMigrationStatusHelper::check_allow_gc_abandoned_ls(migration_status)) {
    allow_read_ = false;
    LOG_INFO("ls is abandoned, do now allow read", K(ret), KPC(ls));
  } else if (OB_FAIL(iter_.init(tenant_id, ls_id, new_range_, is_get, abs_timeout_ts))) {
    LOG_WARN("failed to init iter", K(ret), K(tenant_id), K(ls_id));
  }
  return ret;
}

int ObTabletReorgInfoTableReadOperator::get_next(
    ObTabletReorgInfoData &data)
{
  int ret = OB_SUCCESS;
  data.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet reorg info table read operation do not init", K(ret));
  } else if (!allow_read_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(iter_.get_next_row(data))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next row", K(ret), K(data));
    }
  }
  return ret;
}

int ObTabletReorgInfoTableReadOperator::get_next(
    ObTabletReorgInfoData &data,
    share::SCN &commit_scn)
{
  int ret = OB_SUCCESS;
  data.reset();
  commit_scn.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet reorg info table read operation do not init", K(ret));
  } else if (!allow_read_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(iter_.get_next_row(data, commit_scn))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next row", K(ret), K(data));
    }
  }
  return ret;
}


ObTransferInfoIterator::ObTransferInfoIterator()
  : is_inited_(false),
    read_op_()
{
}

ObTransferInfoIterator::~ObTransferInfoIterator()
{
}

int ObTransferInfoIterator::init(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObTabletReorgInfoDataType::TYPE &start_type,
    const ObTabletReorgInfoDataType::TYPE &end_type)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("transfer info iterator init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id || !ls_id.is_valid() || !ObTabletReorgInfoDataType::is_transfer(start_type)
      || !ObTabletReorgInfoDataType::is_transfer(end_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("transfer info iterator init get invalid argument", K(ret), K(tenant_id), K(ls_id), K(start_type), K(end_type));
  } else if (OB_FAIL(read_op_.init(tenant_id, ls_id, start_type, end_type))) {
    LOG_WARN("failed to init member table read op", K(ret), K(tenant_id), K(ls_id), K(start_type), K(end_type));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObTransferInfoIterator::get_next(
    ObTabletReorgInfoDataKey &key,
    ObTransferDataValue &transfer_value)
{
  int ret = OB_SUCCESS;
  key.reset();
  transfer_value.reset();
  ObTabletReorgInfoData data;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer info iterator get invalid argument", K(ret));
  } else if (OB_FAIL(read_op_.get_next(data))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next data", K(ret));
    }
  } else if (!ObTabletReorgInfoDataType::is_transfer(data.key_.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("member table data type is unexpected", K(ret), K(data));
  } else if (OB_FAIL(data.get_transfer_data_value(transfer_value))) {
    LOG_WARN("failed to get transfer data value", K(ret), K(data));
  } else {
    key = data.key_;
  }
  return ret;
}

}
}
