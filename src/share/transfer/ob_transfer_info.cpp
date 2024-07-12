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

#define USING_LOG_PREFIX SHARE
#include "share/transfer/ob_transfer_info.h"
#include "share/schema/ob_schema_struct.h"  // ObBasePartition
#include "lib/profile/ob_trace_id.h"        // TraceId
#include "storage/tablelock/ob_table_lock_rpc_struct.h" // ObLockObjRequest
#include "lib/mysqlclient/ob_mysql_transaction.h" // ObMysqlTransaction
#include "observer/ob_inner_sql_connection.h" // ObInnerSQLConnection
#include "share/ob_share_util.h" // ObShareUtil
#include "storage/tablelock/ob_lock_inner_connection_util.h" // ObInnerConnectionLockUtil

using namespace oceanbase;
using namespace share;
using namespace common;
using namespace palf;
using namespace share::schema;
using namespace share;
using namespace transaction::tablelock;
using namespace observer;

ObTransferStatus &ObTransferStatus::operator=(const ObTransferStatus &status)
{
  status_ = status.status_;
  return *this;
}

ObTransferStatus &ObTransferStatus::operator=(const STATUS &status)
{
  status_ = status;
  return *this;
}

const char *ObTransferStatus::str() const
{
  const char *str = "INVALID_STATUS";
  switch (status_) {
  case INIT: {
      str = "INIT";
      break;
    }
  case START: {
      str = "START";
      break;
    }
  case DOING: {
      str = "DOING";
      break;
    }
  case COMPLETED: {
      str = "COMPLETED";
      break;
    }
  case ABORTED: {
      str = "ABORTED";
      break;
    }
  case FAILED: {
      str = "FAILED";
      break;
    }
  case CANCELED: {
      str = "CANCELED";
      break;
    }
  default: {
      str = "INVALID_STATUS";
    }
  }
  return str;
}

int ObTransferStatus::parse_from_str(const ObString &str)
{
  int ret = OB_SUCCESS;
  if (0 == str.case_compare("INIT")) {
    status_ = INIT;
  } else if (0 == str.case_compare("START")) {
    status_ = START;
  } else if (0 == str.case_compare("DOING")) {
    status_ = DOING;
  } else if (0 == str.case_compare("COMPLETED")) {
    status_ = COMPLETED;
  } else if (0 == str.case_compare("ABORTED")) {
    status_ = ABORTED;
  } else if (0 == str.case_compare("FAILED")) {
    status_ = FAILED;
  } else if (0 == str.case_compare("CANCELED")) {
    status_ = CANCELED;
  } else {
    status_ = MAX_STATUS;
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid transfer status str", KR(ret), K(str));
  }
  return ret;
}

bool ObTransferStatus::is_finish_status() const
{
  return COMPLETED == status_
      || FAILED == status_
      || CANCELED == status_;
}

int ObTransferStatusHelper::check_can_change_status(
    const ObTransferStatus &old_status,
    const ObTransferStatus &new_status,
    bool &can_change)
{
  int ret = OB_SUCCESS;
  can_change = false;
  if (OB_UNLIKELY(!old_status.is_valid() || !new_status.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invlaid status", KR(ret), K(old_status), K(new_status));
  } else if (old_status == new_status) {
    can_change = true;
  } else {
    switch(old_status) {
    case ObTransferStatus::INIT: {
        if (new_status == ObTransferStatus::START
            || new_status == ObTransferStatus::CANCELED
            || new_status == ObTransferStatus::COMPLETED) {
          can_change = true;
        }
        break;
      }
    case ObTransferStatus::START: {
        if (new_status == ObTransferStatus::DOING
            || new_status == ObTransferStatus::ABORTED) {
          can_change = true;
        }
        break;
      }
    case ObTransferStatus::DOING: {
        if (new_status == ObTransferStatus::COMPLETED) {
          can_change = true;
        }
        break;
      }
    case ObTransferStatus::ABORTED: {
        if (new_status == ObTransferStatus::FAILED) {
          can_change = true;
        }
        break;
      }
    default: {
        can_change = false;
      }
    }
  }
  return ret;
}

/////////////// ObTransferRefreshStatus///////////////
ObTransferRefreshStatus &ObTransferRefreshStatus::operator=(const ObTransferRefreshStatus &other)
{
  status_ = other.status_;
  return *this;
}

ObTransferRefreshStatus &ObTransferRefreshStatus::operator=(const ObTransferRefreshStatus::STATUS &status)
{
  status_ = status;
  return *this;
}

const char *ObTransferRefreshStatus::str() const
{
  const char *str = "INVALID";
  switch (status_) {
    case UNKNOWN: {
      str = "UNKNOWN";
      break;
    }
    case DOING: {
      str = "DOING";
      break;
    }
    case DONE: {
      str = "DONE";
      break;
    }
    default: {
      break;
    }
  }
  return str;
}

// change ObTransferStatus to ObTransferRefreshStatus
void ObTransferRefreshStatus::convert_from(const ObTransferStatus &status)
{
  if (status.is_init_status()
      || status.is_start_status()) {
    status_ = UNKNOWN;
  } else if (status.is_doing_status()
             || status.is_completed_status()) {
    status_ = DOING;
  } else if (status.is_aborted_status()
             || status.is_canceled_status()
             || status.is_failed_status()) {
    status_ = DONE;
  } else {
    status_ = INVALID;
  }
}

// update status according to state machine
void ObTransferRefreshStatus::update(
     const ObTransferRefreshStatus &other,
     bool &changed)
{
  changed = false;
  const ObTransferRefreshStatus::STATUS &new_status = other.status_;
  switch (status_) {
    case INVALID : {
      if (INVALID != new_status) {
        changed = true;
        status_ = new_status;
      }
      break;
    }
    case UNKNOWN: {
      if (DOING == new_status
          || DONE == new_status) {
        changed = true;
        status_ = other.status_;
      }
      break;
    }
    case DOING: {
      if (DONE == new_status) {
        changed = true;
        status_ = new_status;
      }
      break;
    }
    case DONE: {
      break;
    }
    default: {
      break;
    }
  }
}

int ObTransferRefreshInfo::init(
    const ObTransferTaskID &task_id,
    const ObTransferRefreshStatus &status)
{
  int ret = OB_SUCCESS;
  task_id_ = task_id;
  status_ = status;
  return ret;
}

void ObTransferRefreshInfo::reset()
{
  task_id_.reset();
  status_.reset();
}

ObTransferTabletInfo::ObTransferTabletInfo()
  : tablet_id_(),
    transfer_seq_(OB_INVALID_TRANSFER_SEQ)
{
}

void ObTransferTabletInfo::reset()
{
  tablet_id_.reset();
  transfer_seq_ = OB_INVALID_TRANSFER_SEQ;
}

int ObTransferTabletInfo::init(
    const ObTabletID &tablet_id,
    const int64_t transfer_seq)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_id.is_valid() || transfer_seq <= OB_INVALID_TRANSFER_SEQ)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tablet_id), K(transfer_seq));
  } else {
    tablet_id_ = tablet_id;
    transfer_seq_ = transfer_seq;
  }
  return ret;
}

int ObTransferTabletInfo::parse_from_display_str(const common::ObString &str)
{
  int ret = OB_SUCCESS;
  uint64_t tablet_id = 0;
  errno = 0;
  if (OB_UNLIKELY(2 != sscanf(str.ptr(), "%lu:%ld", &tablet_id, &transfer_seq_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ObTransferTabletInfo str", KR(ret), K(str), K(errno), KERRMSG);
  } else {
    tablet_id_ = tablet_id; // ObTabletID <- uint64_t
  }
  return ret;
}

int ObTransferTabletInfo::to_display_str(char *buf, const int64_t len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(len <= 0 || pos < 0 || pos >= len || !is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(len), K(pos), KPC(this));
  } else if (OB_FAIL(databuff_printf(buf, len, pos, "%lu:%ld", tablet_id_.id(), transfer_seq_))) {
    LOG_WARN("databuff_printf failed", KR(ret), K(len), K(pos), K(buf), KPC(this));
  }
  return ret;
}

bool ObTransferTabletInfo::operator==(const ObTransferTabletInfo &other) const
{
  return other.tablet_id_ == tablet_id_
      && other.transfer_seq_ == transfer_seq_;
}

OB_SERIALIZE_MEMBER(ObTransferTabletInfo, tablet_id_, transfer_seq_);

void ObTransferPartInfo::reset()
{
  table_id_ = OB_INVALID_ID;
  part_object_id_ = OB_INVALID_ID;
}

int ObTransferPartInfo::init(const ObObjectID &table_id, const ObObjectID &part_object_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == table_id || OB_INVALID_ID == part_object_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid id", KR(ret), K(table_id), K(part_object_id));
  } else {
    table_id_ = table_id;
    part_object_id_ = part_object_id;
  }
  return ret;
}

int ObTransferPartInfo::init(const schema::ObBasePartition &part_schema)
{
  int ret = OB_SUCCESS;
  table_id_ = part_schema.get_table_id();
  part_object_id_ = part_schema.get_object_id();
  return ret;
}

int ObTransferPartInfo::parse_from_display_str(const common::ObString &str)
{
  int ret = OB_SUCCESS;
  errno = 0;
  if (OB_UNLIKELY(2 != sscanf(str.ptr(), "%lu:%lu", &table_id_, &part_object_id_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ObTransferTabletInfo str", KR(ret), K(str), K(errno), KERRMSG);
  }
  return ret;
}

int ObTransferPartInfo::to_display_str(char *buf, const int64_t len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(len <= 0 || pos < 0 || pos >= len || !is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(len), K(pos), KPC(this));
  } else if (OB_FAIL(databuff_printf(buf, len, pos, "%lu:%lu", table_id_, part_object_id_))) {
    LOG_WARN("databuff_printf failed", KR(ret), K(len), K(pos), K(buf), KPC(this));
  }
  return ret;
}

bool ObTransferPartInfo::operator==(const ObTransferPartInfo &other) const
{
  return other.table_id_ == table_id_
      && other.part_object_id_ == part_object_id_;
}

int ObDisplayTabletID::parse_from_display_str(const common::ObString &str)
{
  int ret = OB_SUCCESS;
  uint64_t tablet_id = 0;
  errno = 0;
  if (OB_UNLIKELY(1 != sscanf(str.ptr(), "%lu", &tablet_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ObDisplayTabletID str", KR(ret), K(str), K(errno), KERRMSG);
  } else {
    tablet_id_ = tablet_id; // ObTabletID <- uint64_t
  }
  return ret;
}

int ObDisplayTabletID::to_display_str(char *buf, const int64_t len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(len <= 0 || pos < 0 || pos >= len || !is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(len), K(pos), KPC(this));
  } else if (OB_FAIL(databuff_printf(buf, len, pos, "%lu", tablet_id_.id()))) {
    LOG_WARN("databuff_printf failed", KR(ret), K(len), K(pos), K(buf), KPC(this));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObDisplayTabletID, tablet_id_);

static const char* TRANSFER_TASK_COMMENT_ARRAY[] =
{
  "",/*EMPTY_COMMENT*/
  "Wait for member list to be same",
  "Task completed as no valid partition",
  "Task canceled",
  "Unable to process task due to transaction timeout",
  "Unable to process task due to inactive server in member list",
  "Wait to retry due to the last failure",
  "Unknow"/*MAX_COMMENT*/
};

const char *oceanbase::share::transfer_task_comment_to_str(const ObTransferTaskComment &comment)
{
  STATIC_ASSERT(ARRAYSIZEOF(TRANSFER_TASK_COMMENT_ARRAY) == (int64_t)ObTransferTaskComment::MAX_COMMENT + 1,
      "transfer task comment array size mismatch enum ObTransferTaskComment count");
  OB_ASSERT(comment >= 0 && comment <= ObTransferTaskComment::MAX_COMMENT);
  return TRANSFER_TASK_COMMENT_ARRAY[comment];
}

ObTransferTaskComment oceanbase::share::str_to_transfer_task_comment(const common::ObString &str)
{
  ObTransferTaskComment comment = ObTransferTaskComment::MAX_COMMENT;
  for (int64_t i = 0; i < ARRAYSIZEOF(TRANSFER_TASK_COMMENT_ARRAY); i++) {
    if (0 == str.case_compare(TRANSFER_TASK_COMMENT_ARRAY[i])) {
      comment = static_cast<ObTransferTaskComment>(i);
      break;
    }
  }
  return comment;
}

int ObTransferTask::TaskStatus::init(const ObTransferTaskID task_id, const ObTransferStatus &status)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!task_id.is_valid()|| !status.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(task_id), K(status));
  } else {
    task_id_ = task_id;
    status_ = status;
  }
  return ret;
}

void ObTransferTask::TaskStatus::reset()
{
  task_id_.reset();
  status_.reset();
}

ObTransferTask::TaskStatus &ObTransferTask::TaskStatus::operator=(
    const TaskStatus &other)
{
  task_id_ = other.task_id_;
  status_ = other.status_;
  return *this;
}

///////////////////////////////////////////////////////////////

ObTransferTask::ObTransferTask()
    : task_id_(),
      src_ls_(),
      dest_ls_(),
      part_list_(),
      not_exist_part_list_(),
      lock_conflict_part_list_(),
      table_lock_tablet_list_(),
      tablet_list_(),
      start_scn_(),
      finish_scn_(),
      status_(),
      trace_id_(),
      result_(-1),
      comment_(ObTransferTaskComment::EMPTY_COMMENT),
      balance_task_id_(),
      table_lock_owner_id_(),
      data_version_(0)
{
}


void ObTransferTask::reset()
{
  task_id_.reset();
  src_ls_.reset();
  dest_ls_.reset();
  part_list_.reset();
  not_exist_part_list_.reset();
  lock_conflict_part_list_.reset();
  table_lock_tablet_list_.reset(),
  tablet_list_.reset();
  start_scn_.reset();
  finish_scn_.reset();
  status_.reset();
  trace_id_.reset();
  result_ = -1;
  comment_ = ObTransferTaskComment::EMPTY_COMMENT;
  balance_task_id_.reset();
  table_lock_owner_id_.reset();
  data_version_ = 0;
}

// init by necessary info, other members take default values
int ObTransferTask::init(
    const ObTransferTaskID task_id,
    const ObLSID &src_ls,
    const ObLSID &dest_ls,
    const ObTransferPartList &part_list,
    const ObTransferStatus &status,
    const common::ObCurTraceId::TraceId &trace_id,
    const ObBalanceTaskID balance_task_id,
    const uint64_t data_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!task_id.is_valid()
      || !src_ls.is_valid()
      || !dest_ls.is_valid()
      || part_list.empty()
      || !status.is_valid()
      || trace_id.is_invalid()
      || ! balance_task_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid aruguments", KR(ret), K(task_id), K(src_ls),
        K(dest_ls), K(part_list), K(status), K(trace_id), K(balance_task_id));
  } else {
    reset();
    if (OB_FAIL(part_list_.assign(part_list))) {
      LOG_WARN("fail to assign part_list", KR(ret), K(part_list));
    } else {
      task_id_ = task_id;
      src_ls_ = src_ls;
      dest_ls_ = dest_ls;
      status_ = status;
      trace_id_ = trace_id;
      balance_task_id_ = balance_task_id;
      start_scn_.set_min();
      finish_scn_.set_min();
      data_version_ = data_version;
    }
  }
  return ret;
}

// init all members
int ObTransferTask::init(
    const ObTransferTaskID task_id,
    const ObLSID &src_ls,
    const ObLSID &dest_ls,
    const ObString &part_list_str,
    const ObString &not_exist_part_list_str,
    const ObString &lock_conflict_part_list_str,
    const ObString &table_lock_tablet_list_str,
    const ObString &tablet_list_str,
    const share::SCN &start_scn,
    const share::SCN &finish_scn,
    const ObTransferStatus &status,
    const common::ObCurTraceId::TraceId &trace_id,
    const int result,
    const ObTransferTaskComment &comment,
    const ObBalanceTaskID balance_task_id,
    const transaction::tablelock::ObTableLockOwnerID &lock_owner_id,
    const uint64_t data_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!task_id.is_valid()
      || !src_ls.is_valid()
      || !dest_ls.is_valid()
      || !status.is_valid()
      || trace_id.is_invalid()
      || ! balance_task_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid aruguments", KR(ret), K(task_id), K(src_ls),
        K(dest_ls), K(status), K(trace_id), K(balance_task_id));
  } else if (OB_FAIL(part_list_.parse_from_display_str(part_list_str))) {
    LOG_WARN("fail to parse from string", KR(ret), K(part_list_str));
  } else if (OB_FAIL(not_exist_part_list_.parse_from_display_str(
      not_exist_part_list_str))) {
    LOG_WARN("fail to parse from string", KR(ret), K(not_exist_part_list_str));
  } else if (OB_FAIL(lock_conflict_part_list_.parse_from_display_str(
      lock_conflict_part_list_str))) {
    LOG_WARN("fail to parse from string", KR(ret), K(lock_conflict_part_list_str));
  } else if (OB_FAIL(table_lock_tablet_list_.parse_from_display_str(table_lock_tablet_list_str))) {
    LOG_WARN("fail to parse from string", KR(ret), K(table_lock_tablet_list_str));
  } else if (OB_FAIL(tablet_list_.parse_from_display_str(tablet_list_str))) {
    LOG_WARN("fail to parse from string", KR(ret), K(tablet_list_str));
  } else {
    task_id_ = task_id;
    src_ls_ = src_ls;
    dest_ls_ = dest_ls;
    start_scn_ = start_scn;
    finish_scn_ = finish_scn;
    status_ = status;
    trace_id_ = trace_id;
    result_ = result;
    comment_ = comment;
    balance_task_id_ = balance_task_id;
    table_lock_owner_id_ = lock_owner_id;
    data_version_ = data_version;
  }
  return ret;
}

int ObTransferTask::assign(const ObTransferTask &other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
    // skip
  } else if (OB_FAIL(part_list_.assign(other.part_list_))) {
    LOG_WARN("fail to assign part_list", KR(ret), K(other));
  } else if (OB_FAIL(not_exist_part_list_.assign(other.not_exist_part_list_))) {
    LOG_WARN("fail to assign not_exist_part_list", KR(ret), K(other));
  } else if (OB_FAIL(lock_conflict_part_list_.assign(other.lock_conflict_part_list_))) {
    LOG_WARN("fail to assign lock_conflict_part_list", KR(ret), K(other));
  } else if (OB_FAIL(table_lock_tablet_list_.assign(other.table_lock_tablet_list_))) {
    LOG_WARN("fail to assign table_lock_tablet_list", KR(ret), K(other));
  } else if (OB_FAIL(tablet_list_.assign(other.tablet_list_))) {
    LOG_WARN("fail to assign tablet_list", KR(ret), K(other));
  } else {
    task_id_ = other.task_id_;
    src_ls_ = other.src_ls_;
    dest_ls_ = other.dest_ls_;
    start_scn_ = other.start_scn_;
    finish_scn_ = other.finish_scn_;
    status_ = other.status_;
    trace_id_ = other.trace_id_;
    result_ = other.result_;
    comment_ = other.comment_;
    balance_task_id_ = other.balance_task_id_;
    table_lock_owner_id_ = other.table_lock_owner_id_;
    data_version_ = other.data_version_;
  }
  return ret;
}

bool ObTransferTask::is_valid() const
{
  return task_id_.is_valid()
      && src_ls_.is_valid()
      && dest_ls_.is_valid()
      && status_.is_valid()
      && !trace_id_.is_invalid()
      && balance_task_id_.is_valid()
      && (!part_list_.empty()
          || !not_exist_part_list_.empty()
          || !lock_conflict_part_list_.empty());
}


ObTransferTaskInfo::ObTransferTaskInfo()
  : tenant_id_(OB_INVALID_ID),
    src_ls_id_(),
    dest_ls_id_(),
    task_id_(),
    trace_id_(),
    status_(),
    table_lock_owner_id_(),
    table_lock_tablet_list_(),
    tablet_list_(),
    start_scn_(),
    finish_scn_(),
    result_(OB_SUCCESS),
    data_version_(0)
{
}

void ObTransferTaskInfo::reset()
{
  tenant_id_ = OB_INVALID_ID;
  src_ls_id_.reset();
  dest_ls_id_.reset();
  task_id_.reset();
  trace_id_.reset();
  status_.reset();
  table_lock_owner_id_.reset();
  table_lock_tablet_list_.reset();
  tablet_list_.reset();
  start_scn_.reset();
  finish_scn_.reset();
  result_ = OB_SUCCESS;
  data_version_ = 0;
}

// table_lock_tablet_list_ may be empty
bool ObTransferTaskInfo::is_valid() const
{
  return tenant_id_ != OB_INVALID_ID
      && src_ls_id_.is_valid()
      && dest_ls_id_.is_valid()
      && task_id_.is_valid()
      && !trace_id_.is_invalid()
      && status_.is_valid()
      && table_lock_owner_id_.is_valid()
      && !tablet_list_.empty()
      && data_version_ >= 0;
}

int ObTransferTaskInfo::convert_from(const uint64_t tenant_id, const ObTransferTask &task)
{
  int ret = OB_SUCCESS;
  if (!task.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(task));
  } else if (OB_FAIL(table_lock_tablet_list_.assign(task.get_table_lock_tablet_list()))) {
    LOG_WARN("fail to assign table lock tablet list", KR(ret), K(task));
  } else if (OB_FAIL(tablet_list_.assign(task.get_tablet_list()))) {
    LOG_WARN("fail to assign tablet list", KR(ret), K(task));
  } else {
    tenant_id_ = tenant_id;
    src_ls_id_  = task.get_src_ls();
    dest_ls_id_ = task.get_dest_ls();
    task_id_ = task.get_task_id();
    trace_id_ = task.get_trace_id();
    status_ = task.get_status();
    table_lock_owner_id_ = task.get_table_lock_owner_id();
    start_scn_ = task.get_start_scn();
    finish_scn_ = task.get_finish_scn();
    result_ = task.get_result();
    data_version_ = task.get_data_version();
  }
  return ret;
}

int ObTransferTaskInfo::assign(const ObTransferTaskInfo &task_info)
{
  int ret = OB_SUCCESS;
  if (!task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("assign transfer task info get invalid argument", K(ret), K(task_info));
  } else if (OB_FAIL(table_lock_tablet_list_.assign(task_info.table_lock_tablet_list_))) {
    LOG_WARN("failed to assign table lock tablet list", K(ret), K(task_info));
  } else if (OB_FAIL(tablet_list_.assign(task_info.tablet_list_))) {
    LOG_WARN("failed to assign tablet list", K(ret), K(task_info));
  } else {
    tenant_id_ = task_info.tenant_id_;
    src_ls_id_ = task_info.src_ls_id_;
    dest_ls_id_ = task_info.dest_ls_id_;
    task_id_ = task_info.task_id_;
    trace_id_ = task_info.trace_id_;
    status_ = task_info.status_;
    table_lock_owner_id_ = task_info.table_lock_owner_id_;
    start_scn_ = task_info.start_scn_;
    finish_scn_ = task_info.finish_scn_;
    result_ = task_info.result_;
    data_version_ = task_info.data_version_;
  }
  return ret;
}

int ObTransferTaskInfo::fill_tablet_ids(ObIArray<ObTabletID> &tablet_ids) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_ids.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, tablet_ids should be empty", K(ret));
  } else if (OB_FAIL(tablet_ids.reserve(tablet_list_.count()))) {
    LOG_WARN("failed to reserve tablet_ids", K(ret));
  } else {
    FOREACH_X(it, tablet_list_, OB_SUCC(ret)) {
      const ObTabletID &tablet_id = it->tablet_id();
      if (OB_UNLIKELY(!tablet_id.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid tablet id", K(ret), K(tablet_id));
      } else if (OB_FAIL(tablet_ids.push_back(tablet_id))) {
        LOG_WARN("failed to push back tablet_id", K(ret), K(tablet_id));
      }
    }
  }
  return ret;
}

int ObTransferLockUtil::lock_tablet_on_dest_ls_for_table_lock(
    ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const ObLSID &dest_ls,
    const transaction::tablelock::ObTableLockOwnerID &lock_owner_id,
    const ObDisplayTabletList &table_lock_tablet_list)
{
  int ret = OB_SUCCESS;
  if (table_lock_tablet_list.empty()) {
    // skip
  } else {
    ObLockAloneTabletRequest lock_arg;
    lock_arg.op_type_ = OUT_TRANS_LOCK;
    if (OB_FAIL(process_table_lock_on_tablets_(
        trans,
        tenant_id,
        dest_ls,
        lock_owner_id,
        table_lock_tablet_list,
        lock_arg))) {
      LOG_WARN("process table lock on tablets failed", KR(ret),
          K(dest_ls), K(lock_owner_id), K(table_lock_tablet_list), K(lock_arg));
    }
  }
  return ret;
}

int ObTransferLockUtil::unlock_tablet_on_src_ls_for_table_lock(
    ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const ObLSID &src_ls,
    const transaction::tablelock::ObTableLockOwnerID &lock_owner_id,
    const ObDisplayTabletList &table_lock_tablet_list)
{
  int ret = OB_SUCCESS;
  if (table_lock_tablet_list.empty()) {
    // skip
  } else {
    ObUnLockAloneTabletRequest unlock_arg;
    unlock_arg.op_type_ = OUT_TRANS_UNLOCK;
    if (OB_FAIL(process_table_lock_on_tablets_(
        trans,
        tenant_id,
        src_ls,
        lock_owner_id,
        table_lock_tablet_list,
        unlock_arg))) {
      LOG_WARN("process table lock on tablets failed", KR(ret),
          K(src_ls), K(lock_owner_id), K(table_lock_tablet_list), K(unlock_arg));
    }
  }
  return ret;
}

int ObTransferLockUtil::process_table_lock_on_tablets_(
  ObMySQLTransaction &trans,
  const uint64_t tenant_id,
  const ObLSID &ls_id,
  const transaction::tablelock::ObTableLockOwnerID &lock_owner_id,
  const ObDisplayTabletList &table_lock_tablet_list,
  ObLockAloneTabletRequest &lock_arg)
{
  int ret = OB_SUCCESS;
  lock_arg.tablet_ids_.reset();
  ObTimeoutCtx ctx;
  ObInnerSQLConnection *conn = NULL;
  const int64_t DEFAULT_TIMEOUT = GCONF.internal_sql_execute_timeout;
  if (OB_UNLIKELY(!ls_id.is_valid_with_tenant(tenant_id)
      || !lock_owner_id.is_valid()
      || table_lock_tablet_list.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(ls_id), K(tenant_id), K(lock_owner_id), K(table_lock_tablet_list));
  } else if (OB_ISNULL(conn = static_cast<ObInnerSQLConnection *>(trans.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("connection is null", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, DEFAULT_TIMEOUT))) {
    LOG_WARN("fail to set default_timeout_ctx", KR(ret));
  } else if (OB_FAIL(lock_arg.tablet_ids_.reserve(table_lock_tablet_list.count()))) {
    LOG_WARN("reserve failed", KR(ret), "count", table_lock_tablet_list.count());
  } else {
    lock_arg.lock_mode_ = ROW_SHARE;
    lock_arg.timeout_us_ = ctx.get_timeout();
    lock_arg.ls_id_ = ls_id;
    lock_arg.owner_id_ = lock_owner_id;
    ARRAY_FOREACH(table_lock_tablet_list, idx) {
      const ObTabletID &tablet_id = table_lock_tablet_list.at(idx).tablet_id();
      if (OB_FAIL(lock_arg.tablet_ids_.push_back(tablet_id))) {
        LOG_WARN("push back failed", KR(ret), K(tablet_id));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OUT_TRANS_LOCK == lock_arg.op_type_) {
    if (OB_FAIL(ObInnerConnectionLockUtil::lock_tablet(tenant_id, lock_arg, conn))) {
      LOG_WARN("lock tablet failed", KR(ret), K(tenant_id), K(lock_arg));
    }
  } else if (OUT_TRANS_UNLOCK == lock_arg.op_type_) {
    if (OB_FAIL(ObInnerConnectionLockUtil::unlock_tablet(tenant_id, static_cast<ObUnLockAloneTabletRequest &>(lock_arg), conn))) {
      LOG_WARN("unock tablet failed", KR(ret), K(tenant_id), K(lock_arg));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lock arg", KR(ret), K(lock_arg));
  }
  return ret;
}
