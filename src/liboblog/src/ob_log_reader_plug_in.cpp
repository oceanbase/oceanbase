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

#define USING_LOG_PREFIX OBLOG

#include <MetaInfo.h>                            // ITableMeta
#include "lib/string/ob_string.h"                // ObString
#include "ob_log_reader_plug_in.h"
#include "ob_log_binlog_record.h"
#include "ob_log_store_service.h"
#include "ob_log_utils.h"
#include "ob_log_instance.h"

using namespace oceanbase::common;
using namespace oceanbase::logmessage;

namespace oceanbase
{
namespace liboblog
{

ObLogReader::ObLogReader() :
    inited_(false),
    store_service_stat_(),
    store_service_(NULL)
{
}

ObLogReader::~ObLogReader()
{
  destroy();
}

int ObLogReader::init(IObStoreService &store_service)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("ObLogReader has been initialized");
    ret = OB_INIT_TWICE;
  } else {
    store_service_ = &store_service;
    inited_ = true;
  }

  return ret;
}

void ObLogReader::destroy()
{
  if (inited_) {
    inited_ = false;
    store_service_stat_.reset();
    store_service_ = NULL;
  }
}

int ObLogReader::read(ObLogRowDataIndex &row_data_index)
{
  int ret = OB_SUCCESS;
  ObLogTenantGuard guard;
  ObLogTenant *tenant = NULL;
  const uint64_t tenant_id = row_data_index.get_tenant_id();
  void *column_family_handle = NULL;
  ObLogBR *br = NULL;
  ILogRecord *binlog_record  = NULL;
  std::string key;
  std::string value;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogReader has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(TCTX.get_tenant_guard(tenant_id, guard))) {
    LOG_ERROR("get_tenant_guard fail", KR(ret));
  } else {
    tenant = guard.get_tenant();
    column_family_handle = tenant->get_cf();
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(row_data_index.get_storage_key(key))) {
    LOG_ERROR("get_storage_key fail", KR(ret), "key", key.c_str(), K(row_data_index));
  } else if (OB_FAIL(read_store_service_(column_family_handle, row_data_index, key, value))) {
    LOG_ERROR("read_store_service_ fail", KR(ret), K(row_data_index));
  } else if (OB_FAIL(row_data_index.construct_serilized_br_data(br))) {
    LOG_ERROR("construct_serilized_br_data fail", KR(ret), K(row_data_index));
  } else if (OB_ISNULL(br)) {
    LOG_ERROR("ObLogBR is NULL");
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(binlog_record = br->get_data())) {
    LOG_ERROR("binlog_record is NULL", K(row_data_index));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(binlog_record->parse(value.c_str(), value.length()))) {
    LOG_ERROR("binlog_record parse fail", K(ret), K(binlog_record), K(row_data_index));
  } else {
    const int record_type = binlog_record->recordType();
    LOG_DEBUG("binlog_record parse succ", "record_type", print_record_type(record_type),
        K(binlog_record), K(row_data_index));

    store_service_stat_.do_data_stat(value.length());
  }

  return ret;
}

int ObLogReader::read_store_service_(void *column_family_handle,
    ObLogRowDataIndex &row_data_index,
    std::string &key,
    std::string &value)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogReader has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(store_service_)) {
    LOG_ERROR("store_service_ is NULL");
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(store_service_->get(column_family_handle, key, value))) {
    LOG_ERROR("store_service_ get fail", KR(ret), K(key.c_str()), "value_len", value.length(), K(row_data_index));
  } else {
    LOG_DEBUG("store_service_ get succ", K(key.c_str()), "value_len", value.length());
  }

  return ret;
}

// rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);
//_LOG_INFO("[READER] [STAT] perf=%s", rocksdb::get_perf_context()->ToString().c_str());
// rocksdb::get_perf_context()->Reset();
// rocksdb::get_iostats_context()->Reset();
// rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableTime); // open profiling

int ObLogReader::print_serilized_br_value_(const std::string &key,
    ObLogBR &task)
{
  int ret = OB_SUCCESS;
  ObArray<BRColElem> new_values;
  ILogRecord *binlog_record  = NULL;
  ITableMeta *table_meta = NULL;

  if (OB_ISNULL(binlog_record = task.get_data())) {
    LOG_ERROR("binlog_record is NULL", K(task));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(table_meta = LogMsgFactory::createTableMeta())) {
    LOG_ERROR("table_meta is NULL");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (0 != binlog_record->getTableMeta(table_meta)) {
    LOG_ERROR("getTableMeta fail");
    ret = OB_ERR_UNEXPECTED;
  } else {
    bool is_table_meta_null = false;
    int64_t col_count = 0;

    if (NULL == table_meta) {
      is_table_meta_null = true;
    } else {
      col_count = table_meta->getColCount();
    }

    // get_br_value(binlog_record, new_values);
    LOG_INFO("store_service_", "key", key.c_str(), K(is_table_meta_null), K(col_count), K(new_values));
  }

  return ret;
}

} // namespace liboblog
} // namespace oceanbase
