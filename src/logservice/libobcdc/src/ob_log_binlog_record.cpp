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
 *
 * Binlog Record
 */

#define USING_LOG_PREFIX OBLOG

#ifdef OB_USE_DRCMSG
#include <drcmsg/MD.h>                        // ITableMeta
#endif

#include "ob_log_binlog_record.h"
#include "ob_log_utils.h"
#include "ob_log_instance.h"                  // TCTX

using namespace oceanbase::common;

namespace oceanbase
{
namespace libobcdc
{

ObLogBR::ObLogBR() : ObLogResourceRecycleTask(ObLogResourceRecycleTask::BINLOG_RECORD_TASK),
                     data_(nullptr),
                     host_(nullptr),
                     stmt_task_(nullptr),
                     next_br_(nullptr),
                     valid_(true),
                     tenant_id_(OB_INVALID_TENANT_ID),
                     schema_version_(OB_INVALID_VERSION),
                     commit_version_(0),
                     row_index_(0),
                     part_trans_task_count_(0)
{
}

ObLogBR::~ObLogBR()
{
  reset();

  destruct_data_();
}

void ObLogBR::construct_data_(const bool creating_binlog_record)
{
  data_ = DRCMessageFactory::createBinlogRecord(TCTX.drc_message_factory_binlog_record_type_, creating_binlog_record);

  if (OB_ISNULL(data_)) {
    OBLOG_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "DRCMessageFactory::createBinlogRecord fails");
  } else {
    // set user data pointer to the pointer hold the binlog record
    data_->setUserData(this);
  }
}

void ObLogBR::destruct_data_()
{
  if (NULL != data_) {
    DRCMessageFactory::destroy(data_);
    data_ = NULL;
  }
}

void ObLogBR::reset()
{
  if (NULL != data_) {
    data_->clear();

    // note reset all filed used by libobcdc, cause clear() may won't reset fields

    // clear new/old column array
    data_->setNewColumn(NULL, 0);
    data_->setOldColumn(NULL, 0);

    // clear TableMeta and IDBMeta
    data_->setTableMeta(NULL);
    data_->setTbname(NULL);
    data_->setDBMeta(NULL);
    data_->setDbname(NULL);

    // set user data pointer to the pointer hold the binlog record
    data_->setUserData(this);
  }

  host_ = nullptr;
  stmt_task_ = nullptr;
  next_br_ = nullptr;
  valid_ = true;
  tenant_id_ = OB_INVALID_TENANT_ID;
  schema_version_ = OB_INVALID_VERSION;
  commit_version_ = 0;
  part_trans_task_count_ = 0;
}

int ObLogBR::set_table_meta(ITableMeta *table_meta)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(data_)) {
    LOG_ERROR("IBinlogRecord has not been created");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(table_meta)) {
    LOG_ERROR("invalid argument", K(table_meta));
    ret = OB_INVALID_ARGUMENT;
  } else {
    data_->setTableMeta(table_meta);
    data_->setTbname(table_meta->getName());
  }

  return ret;
}

int ObLogBR::set_db_meta(IDBMeta *db_meta)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(data_)) {
    LOG_ERROR("IBinlogRecord has not been created");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(db_meta)) {
    LOG_ERROR("invalid argument", K(db_meta));
    ret = OB_INVALID_ARGUMENT;
  } else {
    data_->setDBMeta(db_meta);
    data_->setDbname(db_meta->getName());
  }

  return ret;
}

int ObLogBR::init_data(const RecordType type,
    const uint64_t cluster_id,
    const int64_t tenant_id,
    const uint64_t row_index,
    const common::ObString &trace_id,
    const common::ObString &trace_info,
    const common::ObString &unique_id,
    const int64_t schema_version,
    const int64_t commit_version,
    const int64_t part_trans_task_count,
    const common::ObString *major_version_str)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(data_)) {
    LOG_ERROR("IBinlogRecord has not been created");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(EUNKNOWN == type)
      || OB_UNLIKELY(! is_valid_cluster_id_(cluster_id))
      || OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
      || OB_UNLIKELY(OB_INVALID_TIMESTAMP == schema_version)
      || OB_UNLIKELY(commit_version <= 0)) {
    LOG_ERROR("invalid argument", K(type), K(cluster_id), K(tenant_id), K(schema_version), K(commit_version));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(verify_part_trans_task_count_(type, part_trans_task_count))) {
    LOG_ERROR("verify_part_trans_task_count_ fail", KR(ret), K(type),
        "type", print_record_type(type), K(part_trans_task_count));
  } else {
    // set to invalid_data
    int src_category = SRC_NO;
    // NOTE: init checkpint to 0 (sec/microsecond)
    uint64_t checkpoint_sec = 0;
    uint64_t checkpoint_usec = 0;

    data_->setRecordType(type);
    data_->setSrcCategory(src_category);
    data_->setCheckpoint(checkpoint_sec, checkpoint_usec);
    data_->setId(0); // always set id to 0
    data_->setSrcType(SRC_OCEANBASE_1_0);  // for OB 1.0

    // means that two consecutive statements operate on different fields
    // set this field to true for performance
    data_->setFirstInLogevent(true);

    // treate cluster_id as thread_id
    // convert from 64 bit to 32 bit
    data_->setThreadId(static_cast<uint32_t>(cluster_id));
    // set trans commit timestamp (second)
    int64_t commit_version_usec = commit_version / NS_CONVERSION;
    data_->setTimestamp(commit_version_usec / 1000000);
    // set trans commit timestamp (microsecond)
    // note: combine getTimestamp() and getRecordUsec() as complete trans commit timestamp
    data_->setRecordUsec(static_cast<uint32_t>(commit_version_usec % 1000000));

    // won't use this field
    data_->putFilterRuleVal("0", 1);
    // set unique id to binlog record
    data_->putFilterRuleVal(unique_id.ptr(), unique_id.length());
    // set OBTraceID
    data_->putFilterRuleVal(trace_id.ptr(), trace_id.length());

    // set ObTraceInfo
    data_->setObTraceInfo(trace_info.ptr());

    // put major version(from int32_t to char*) to the forth field
    if (EBEGIN == type) {
      if (OB_ISNULL(major_version_str)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("major version str for EBEGIN statement should not be null!", KR(ret), K(cluster_id),
            K(type), K(trace_id));
      } else {
        data_->putFilterRuleVal(major_version_str->ptr(), major_version_str->length());
      }
    }

    set_next(NULL);
    valid_ = true;
    tenant_id_ = tenant_id;
    row_index_ = row_index;
    schema_version_ = schema_version;
    commit_version_ = commit_version;
    part_trans_task_count_ = part_trans_task_count;
  }

  return ret;
}

bool ObLogBR::is_valid_cluster_id_(const uint64_t cluster_id) const
{
  bool bool_ret = false;
  bool_ret = is_valid_cluster_id(cluster_id)
    || (MIN_DRC_CLUSTER_ID <= cluster_id && MAX_DRC_CLUSTER_ID >= cluster_id);
  return bool_ret;
}

int ObLogBR::put_old(IBinlogRecord *br, const bool is_changed)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(br)) {
    LOG_ERROR("invalid argument", K(br));
    ret = OB_INVALID_ARGUMENT;
  } else {
    // DRC proto
    // mark value of OldCol to empty string, use global unique empty string value
    // value of unchanged OldCol as NULL
    const char *val = is_changed ? COLUMN_VALUE_IS_EMPTY : COLUMN_VALUE_IS_NULL;

    int64_t pos = (NULL == val ? 0 : strlen(val));

    (void)br->putOld(val, static_cast<int>(pos));
  }

  return ret;
}

int ObLogBR::get_record_type(int &record_type)
{
  int ret = OB_SUCCESS;
  record_type = 0;

  if (OB_ISNULL(data_)) {
    LOG_ERROR("data_ is null", K(data_));
    ret = OB_ERR_UNEXPECTED;
  } else {
    record_type = data_->recordType();
  }

  return ret;
}

int ObLogBR::setInsertRecordTypeForHBasePut(const RecordType type)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(data_)) {
    LOG_ERROR("IBinlogRecord has not been created");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(EINSERT != type)) {
    LOG_ERROR("invalid argument", "type", print_record_type(type));
  } else {
    data_->setRecordType(type);
  }

  return ret;
}

int ObLogBR::verify_part_trans_task_count_(const RecordType type,
    const int64_t part_trans_task_count)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(EUNKNOWN == type)) {
    LOG_ERROR("invalid argument", K(type), "type", print_record_type(type));
    ret = OB_INVALID_ARGUMENT;
  } else {
    if ((EDDL == type) || (EBEGIN == type) || (ECOMMIT == type)) {
      // verify part_trans_task_count, should greater than 0 if DDL/BEGIN/COMMIT
      if (OB_UNLIKELY(part_trans_task_count <= 0)) {
        LOG_ERROR("part_trans_task_count is not greater than 0", K(type),
            "type", print_record_type(type), K(part_trans_task_count));
        ret = OB_ERR_UNEXPECTED;
      } else {
        // do nothing
      }
    }
  }

  return ret;
}

// unserilized Binlog record
ObLogUnserilizedBR::ObLogUnserilizedBR() : ObLogBR()
{
  construct_unserilized_data_();

  ObLogBR::reset();
}

ObLogUnserilizedBR::~ObLogUnserilizedBR()
{
}

void ObLogUnserilizedBR::construct_unserilized_data_()
{
  const bool creating_binlog_record = true;
  construct_data_(creating_binlog_record);
}

// serilized Binlog Record
ObLogSerilizedBR::ObLogSerilizedBR() : ObLogBR()
{
  construct_serilized_data_();

  ObLogBR::reset();
}

ObLogSerilizedBR::~ObLogSerilizedBR()
{
}

void ObLogSerilizedBR::construct_serilized_data_()
{
  const bool creating_binlog_record = false;
  construct_data_(creating_binlog_record);
}

} // end namespace libobcdc
} // end namespace oceanbase
