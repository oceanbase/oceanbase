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

#define USING_LOG_PREFIX SHARE_PT

#include "share/partition_table/ob_partition_info.h"
#include "lib/time/ob_time_utility.h"
#include "lib/container/ob_se_array_iterator.h"
#include "share/config/ob_server_config.h"
#include "ob_replica_filter.h"

namespace oceanbase {
namespace share {

using namespace common;
static const char* replica_display_status_strs[] = {
    "REPLICA_STATUS_NORMAL",
    "REPLICA_STATUS_OFFLINE",
    "REPLICA_STATUS_FLAG",
    "REPLICA_STATUS_UNMERGED",
};

const char* ob_replica_status_str(const ObReplicaStatus status)
{
  STATIC_ASSERT(ARRAYSIZEOF(replica_display_status_strs) == REPLICA_STATUS_MAX,
      "type string array size mismatch with enum replica status count");
  const char* str = NULL;
  if (status >= 0 && status < REPLICA_STATUS_MAX) {
    str = replica_display_status_strs[status];
  } else {
    LOG_WARN("invalid replica status", K(status));
  }
  return str;
}

int get_replica_status(const char* str, ObReplicaStatus& status)
{
  int ret = OB_SUCCESS;
  if (NULL == str) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(str));
  } else {
    status = REPLICA_STATUS_MAX;
    for (int64_t i = 0; i < ARRAYSIZEOF(replica_display_status_strs); ++i) {
      if (STRCASECMP(replica_display_status_strs[i], str) == 0) {
        status = static_cast<ObReplicaStatus>(i);
        break;
      }
    }
    if (REPLICA_STATUS_MAX == status) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("display status str not found", K(ret), K(str));
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObPartitionReplica::Member, server_, timestamp_);
OB_SERIALIZE_MEMBER(ObPartitionReplica::FailMsg, dest_server_, task_type_, last_fail_timestamp_, count_, start_pos_);

void ObPartitionReplica::FailMsg::assign(const FailMsg& other)
{
  this->dest_server_ = other.dest_server_;
  this->count_ = other.count_;
  this->start_pos_ = other.start_pos_;
  this->task_type_ = other.task_type_;
  for (int64_t i = 0; i < MAX_FAILED_TIMES_COUNT; ++i) {
    this->last_fail_timestamp_[i] = other.last_fail_timestamp_[i];
  }
}

int ObPartitionReplica::FailMsg::add_failed_timestamp(int64_t timestamp)
{
  int ret = OB_SUCCESS;
  if (0 > start_pos_ || 0 > count_ || MAX_FAILED_TIMES_COUNT <= start_pos_ || MAX_FAILED_TIMES_COUNT < count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("start pos or count error", K(ret), K(start_pos_), K(count_));
  } else {
    int64_t index = (start_pos_ + count_) % MAX_FAILED_TIMES_COUNT;
    last_fail_timestamp_[index] = timestamp;
    if (count_ < MAX_FAILED_TIMES_COUNT) {
      // if the offset in no more than 16, push back directly,
      // else cover the elements already exist in the array
      count_++;
    } else {
      // when the start pos is covered, increment the start pos
      start_pos_ = (start_pos_ + 1) % MAX_FAILED_TIMES_COUNT;
    }
    // try to ensure the monotony of time for blacklist
  }
  return ret;
}

int ObPartitionReplica::FailMsg::remove_useless_timestamp(int64_t current_time, int64_t timeout)
{
  int ret = OB_SUCCESS;
  if (0 > start_pos_ || 0 > count_ || MAX_FAILED_TIMES_COUNT <= start_pos_ || MAX_FAILED_TIMES_COUNT < count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("start pos or count is error", K(ret), K(start_pos_), K(count_));
  } else if (0 == count_) {
    // nothing to do
  } else {
    for (int64_t i = count_ - 1; i >= 0; --i) {
      // try to traverse from the end to find the first data which not fit the requirements
      if (timeout <= (current_time - get_failed_timestamp(i))) {
        start_pos_ = (start_pos_ + i + 1) % MAX_FAILED_TIMES_COUNT;
        count_ -= (i + 1);
        break;
      }
    }
  }
  return ret;
}

void ObPartitionReplica::FailMsg::reset()
{
  start_pos_ = 0;
  count_ = 0;
}

int64_t ObPartitionReplica::FailMsg::get_last_fail_time() const
{
  int64_t ret = -1;
  if (0 > start_pos_ || 0 > count_ || MAX_FAILED_TIMES_COUNT <= start_pos_ || MAX_FAILED_TIMES_COUNT < count_) {
    LOG_ERROR("start pos or count is error", K(start_pos_), K(count_));
  } else if (0 == count_) {
    LOG_DEBUG("empty array");
  } else {
    ret = last_fail_timestamp_[(start_pos_ + count_ - 1) % MAX_FAILED_TIMES_COUNT];
  }
  return ret;
}
int64_t ObPartitionReplica::FailMsg::get_failed_timestamp(int64_t index) const
{
  int64_t ret = -1;
  if (0 > start_pos_ || 0 > count_ || MAX_FAILED_TIMES_COUNT <= start_pos_ || MAX_FAILED_TIMES_COUNT < count_) {
    LOG_ERROR("start pos or count is error", K(start_pos_), K(count_));
  } else if (index >= count_ || 0 > index) {
    LOG_WARN("out of range", K(index), K(count_));
  } else {
    ret = last_fail_timestamp_[(start_pos_ + index) % MAX_FAILED_TIMES_COUNT];
  }
  return ret;
}

ObPartitionReplica::ObPartitionReplica() : allocator_(ObModIds::OB_RS_PARTITION_TABLE_TEMP, DEFAULT_ALLOCATOR_SIZE)
{
  reset();
}

ObPartitionReplica::~ObPartitionReplica()
{
  reset();
}

void ObPartitionReplica::init_basic_variables()
{
  table_id_ = OB_INVALID_ID;
  partition_id_ = OB_INVALID_INDEX;
  partition_cnt_ = 0;

  zone_.reset();
  server_.reset();
  sql_port_ = OB_INVALID_INDEX;
  unit_id_ = OB_INVALID_ID;

  role_ = FOLLOWER;
  member_list_.reset();
  row_count_ = 0;
  data_size_ = 0;
  data_version_ = 0;
  data_checksum_ = 0;
  row_checksum_.reset();
  modify_time_us_ = 0;
  create_time_us_ = 0;
  member_time_us_ = 0;

  is_original_leader_ = false;
  __discard__is_previous_leader_ = false;

  in_member_list_ = false;
  rebuild_ = false;
  is_remove_ = false;
  replica_status_ = REPLICA_STATUS_NORMAL;
  replica_type_ = REPLICA_TYPE_FULL;

  to_leader_time_ = 0;
  required_size_ = 0;
  status_ = REPLICA_STATUS_NORMAL;
  is_restore_ = REPLICA_NOT_RESTORE;
  partition_checksum_ = 0;
  quorum_ = OB_INVALID_COUNT;
  additional_replica_status_ = REPLICA_STATUS_NORMAL;
  fail_list_.reset();
  recovery_timestamp_ = 0;
  property_.reset();
  data_file_id_ = 0;
}
void ObPartitionReplica::reuse()
{
  init_basic_variables();
  allocator_.reuse();
}

void ObPartitionReplica::reset()
{
  init_basic_variables();
  allocator_.reset();
}

int ObPartitionReplica::assign(const ObPartitionReplica& other)
{
  int ret = OB_SUCCESS;
  reset();
  table_id_ = other.table_id_;
  partition_id_ = other.partition_id_;
  partition_cnt_ = other.partition_cnt_;

  zone_ = other.zone_;
  server_ = other.server_;
  sql_port_ = other.sql_port_;
  unit_id_ = other.unit_id_;

  role_ = other.role_;
  if (OB_FAIL(copy_assign(member_list_, other.member_list_))) {
    LOG_WARN("failed to assign member_list_", K(ret));
  }
  if (OB_SUCC(ret)) {
    row_count_ = other.row_count_;
    data_size_ = other.data_size_;
    data_version_ = other.data_version_;
    data_checksum_ = other.data_checksum_;
    row_checksum_ = other.row_checksum_;
    modify_time_us_ = other.modify_time_us_;
    create_time_us_ = other.create_time_us_;
    member_time_us_ = other.member_time_us_;

    is_original_leader_ = other.is_original_leader_;
    __discard__is_previous_leader_ = other.__discard__is_previous_leader_;

    in_member_list_ = other.in_member_list_;
    rebuild_ = other.rebuild_;
    is_remove_ = other.is_remove_;
    replica_status_ = other.replica_status_;
    replica_type_ = other.replica_type_;

    to_leader_time_ = other.to_leader_time_;
    required_size_ = other.required_size_;
    status_ = other.status_;
    is_restore_ = other.is_restore_;
    partition_checksum_ = other.partition_checksum_;
    quorum_ = other.quorum_;
    additional_replica_status_ = other.additional_replica_status_;
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(deep_copy_faillist(other.fail_list_))) {
      LOG_WARN("failed to deep copy fail_list", K(ret), K(fail_list_), "other_fail_list", other.fail_list_);
      reset_fail_list();
    }
  }
  if (OB_SUCC(ret)) {
    recovery_timestamp_ = other.recovery_timestamp_;
    if (OB_FAIL(copy_assign(property_, other.property_))) {
      LOG_WARN("fail to assign property", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    data_file_id_ = other.data_file_id_;
  }
  return ret;
}

int ObPartitionReplica::deep_copy_faillist(const common::ObString& fail_list)
{
  int ret = OB_SUCCESS;
  if (fail_list.empty()) {
    reset_fail_list();
  } else {
    char* buf = NULL;
    const int64_t length = fail_list.length() + 1;
    if (NULL == (buf = static_cast<char*>(allocator_.alloc(length)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to allocator memory", K(ret), K(length), K(fail_list));
    } else {
      MEMCPY(buf, fail_list.ptr(), length - 1);
      // need a '\0' end because the string may be used as a pure char* string
      buf[length - 1] = '\0';
      fail_list_.assign_ptr(buf, static_cast<ObString::obstr_size_t>(length - 1));
    }
  }
  return ret;
}

bool ObPartitionReplica::simple_equal_with(const ObPartitionReplica& other)
{
  bool is_same = true;
  if (table_id_ != other.table_id_ || partition_id_ != other.partition_id_ || partition_cnt_ != other.partition_cnt_ ||
      zone_ != other.zone_ || server_ != other.server_ || sql_port_ != other.sql_port_ || role_ != other.role_ ||
      row_count_ != other.row_count_ || data_size_ != other.data_size_ || data_checksum_ != other.data_checksum_ ||
      is_original_leader_ != other.is_original_leader_ || rebuild_ != other.rebuild_ ||
      replica_type_ != other.replica_type_ || required_size_ != other.required_size_ ||
      additional_replica_status_ != other.additional_replica_status_ ||
      recovery_timestamp_ != other.recovery_timestamp_ ||
      property_.get_memstore_percent() != other.property_.get_memstore_percent() ||
      data_file_id_ != other.data_file_id_) {
    is_same = false;
  } else if (is_leader_like() && to_leader_time_ != other.to_leader_time_) {
    is_same = false;
  } else {
    // check member_list
    bool finded = false;
    for (int i = 0; is_same && i < member_list_.count(); i++) {
      finded = false;
      for (int j = 0; is_same && j < other.member_list_.count(); j++) {
        if (member_list_.at(i) == other.member_list_.at(j)) {
          finded = true;
          break;
        }
      }
      if (false == finded) {
        is_same = false;
      }
    }

    if (is_same) {
      // check row_checksum
      ObRowChecksumValue tmp = other.row_checksum_;
      if (row_checksum_.checksum_ != tmp.checksum_ || row_checksum_.column_count_ != tmp.column_count_) {
        is_same = false;
      } else {
        row_checksum_.sort();
        tmp.sort();
        for (int i = 0; is_same && i < row_checksum_.column_count_; i++) {
          if (row_checksum_.column_checksum_array_[i] != tmp.column_checksum_array_[i]) {
            is_same = false;
          }
        }
      }
    }
  }
  return is_same;
}

int64_t ObPartitionReplica::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();

  J_KV(KT_(table_id),
      K_(partition_id),
      K_(partition_cnt),
      K_(zone),
      K_(server),
      K_(sql_port),
      K_(unit_id),
      K_(role),
      K_(member_list),
      K_(row_count),
      K_(data_size),
      K_(data_version),
      K_(data_checksum),
      K_(row_checksum),
      K_(modify_time_us),
      K_(create_time_us),
      K_(member_time_us),
      K_(is_original_leader),
      K_(in_member_list),
      K_(rebuild),
      K_(to_leader_time),
      "replica_status",
      ob_replica_status_str(replica_status_),
      K_(replica_type),
      K_(required_size),
      "status",
      ob_replica_status_str(status_),
      K_(is_restore),
      K_(partition_checksum),
      K_(quorum),
      K_(recovery_timestamp),
      K_(fail_list),
      K_(property),
      K_(data_file_id));

  J_OBJ_END();
  return pos;
}

ObPartitionKey ObPartitionReplica::partition_key() const
{
  // simple getter no need to check %table_id_ ...
  return ObPartitionKey(table_id_, partition_id_, partition_cnt_);
}

OB_SERIALIZE_MEMBER(ObPartitionReplica, table_id_, partition_id_, partition_cnt_, zone_, server_, sql_port_, unit_id_,
    role_, member_list_, row_count_, data_size_, data_version_, data_checksum_, row_checksum_, modify_time_us_,
    create_time_us_, is_original_leader_, __discard__is_previous_leader_, in_member_list_, replica_status_, rebuild_,
    to_leader_time_, replica_type_, member_time_us_, required_size_, status_, is_restore_, partition_checksum_, quorum_,
    additional_replica_status_, fail_list_, recovery_timestamp_, property_, data_file_id_);

int ObPartitionReplica::member_list2text(const MemberList& member_list, char* text, const int64_t length)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char buf[MAX_IP_PORT_LENGTH];
  if (NULL == text || length <= 0 || member_list.count() < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(text), K(length), "member count", member_list.count());
  }
  FOREACH_CNT_X(m, member_list, OB_SUCCESS == ret)
  {
    if (0 != pos) {
      if (pos + 1 < length) {
        text[pos++] = ',';
      } else {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("buffer not enough", K(ret), K(pos), K(length));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(m->server_.ip_port_to_string(buf, sizeof(buf)))) {
      LOG_WARN("convert server to string failed", K(ret), "member", *m);
    } else {
      int n = snprintf(text + pos, length - pos, "%s:%ld", buf, m->timestamp_);
      if (n < 0 || n >= length - pos) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("snprintf error or buf not enough", K(ret), K(n), K(length), K(pos));
      } else {
        pos += n;
      }
    }
  }
  return ret;
}

int ObPartitionReplica::text2member_list(const char* text, MemberList& member_list)
{
  int ret = OB_SUCCESS;
  char* member_text = nullptr;
  Member member;
  char* save_ptr1 = nullptr;
  member_list.reset();
  if (nullptr == text) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(text));
  }
  while (OB_SUCC(ret)) {
    member_text = strtok_r((nullptr == member_text ? const_cast<char*>(text) : nullptr), ",", &save_ptr1);
    /*
     * ipv4 format: a.b.c.d:port:timestamp,...
     * ipv6 format: [a:b:c:d:e:f:g:h]:port:timestamp,...
     */
    if (nullptr != member_text) {
      char* timestamp_str = nullptr;
      char* end_ptr = nullptr;
      if (OB_NOT_NULL(timestamp_str = strrchr(member_text, ':'))) {
        *timestamp_str++ = '\0';
        member.timestamp_ = strtoll(timestamp_str, &end_ptr, 10);
        if (end_ptr == timestamp_str || *end_ptr != '\0') {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("strtoll failed", K(ret));
        } else if (OB_FAIL(member.server_.parse_from_cstring(member_text))) {
          LOG_ERROR("server parse_from_cstring failed", K(member_text), K(ret));
        } else if (OB_FAIL(member_list.push_back(member))) {
          LOG_WARN("push back failed", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("parse member text failed", K(member_text), K(ret));
      }
    } else {
      break;
    }
  }  // while
  return ret;
}

int ObPartitionReplica::fail_list2text(const FailList& fail_list, char* text, const int64_t length)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char buf[MAX_IP_PORT_LENGTH];
  if (NULL == text || length <= 0 || fail_list.count() < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(text), K(length), "member count", fail_list.count());
  }
  FOREACH_CNT_X(f, fail_list, OB_SUCCESS == ret)
  {
    if (pos != 0) {
      if (pos + 1 < length) {  // failmsg isolation token
        text[pos++] = ';';
      } else {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("buffer not enough", K(ret), K(pos), K(length));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(f)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failmsg is null", K(ret), K(fail_list));
      } else if (OB_FAIL(f->dest_server_.ip_port_to_string(buf, sizeof(buf)))) {
        LOG_WARN("convert server to string failed", K(ret), "failed message", *f);
      } else if (OB_FAIL(databuff_printf(text, length, pos, "%s,%ld,%ld", buf, f->task_type_, f->count_))) {
        LOG_WARN("failed to string", K(ret), "failmsg", *f);
      } else {
        int64_t count = 0;
        while (count < f->count_ && OB_SUCC(ret)) {
          if (OB_FAIL(databuff_printf(text, length, pos, ",%ld", f->get_failed_timestamp(count)))) {
            LOG_WARN("faile to printf time", K(ret));
          }
          count++;
        }
      }
    }
  }
  return ret;
}
int ObPartitionReplica::text2fail_list(const char* text, FailList& fail_list)
{
  int ret = OB_SUCCESS;
  if (NULL == text) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(text));
  } else {
    char* save_ptr1 = NULL;
    fail_list.reset();
    char* failmsg_str = NULL;
    while (OB_SUCC(ret)) {
      FailMsg fail_msg;
      char* save_ptr2 = NULL;
      failmsg_str = strtok_r((NULL == failmsg_str ? const_cast<char*>(text) : NULL), ";", &save_ptr1);
      if (failmsg_str == NULL) {
        break;
      } else {
        char* ser_str = NULL;
        char* task_type = NULL;
        char* failed_times = NULL;
        if (NULL == (ser_str = strtok_r(failmsg_str, ",", &save_ptr2)) ||
            OB_FAIL(fail_msg.dest_server_.parse_from_cstring(ser_str))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail parse server address", K(ret));
        } else if (NULL == save_ptr2 || NULL == (task_type = strtok_r(NULL, ",", &save_ptr2))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to strtok task type", K(ret));
        } else if (NULL == save_ptr2 || NULL == (failed_times = strtok_r(NULL, ",", &save_ptr2))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to strtok fail times", K(ret));
        } else {
          fail_msg.task_type_ = atoll(task_type);
          int64_t failmsg_count = atoll(failed_times);
          int64_t count = 0;
          while (OB_SUCC(ret) && count < failmsg_count) {
            char* last_fail_timestamp = NULL;
            if (NULL == save_ptr2 || NULL == (last_fail_timestamp = strtok_r(NULL, ",", &save_ptr2))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("failed to strtok timestamp", K(ret), K(count), K(failmsg_count), "str", save_ptr2);
            } else if (OB_FAIL(fail_msg.add_failed_timestamp(atoll(last_fail_timestamp)))) {
              LOG_WARN("failed to add failed timestamp", K(ret), K(fail_msg), K(last_fail_timestamp));
            }
            count++;
          }
          if (OB_FAIL(ret)) {

          } else if (fail_msg.count_ != failmsg_count) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed time count is wrong", K(ret), K(failmsg_count), K(fail_msg));
          } else if (OB_FAIL(fail_list.push_back(fail_msg))) {
            LOG_WARN("fail to push back", K(ret), K(fail_msg));
          } else {
            // nothing
          }
        }  // end else
      }
    }  // end while
  }
  LOG_DEBUG("read from string fail list", K(fail_list));
  return ret;
}
int ObPartitionReplica::process_faillist(FailList& fail_list)
{
  int ret = OB_SUCCESS;
  int64_t now = ObTimeUtility::current_time();
  bool found = false;
  FailList fail_list_old;
  if (1 != fail_list.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail_list have only one failmsg", K(ret), K(fail_list));
  } else if (fail_list_.empty()) {
    // if faillist is empty,return the current one directly,no need to process
  } else if (OB_FAIL(text2fail_list(fail_list_.ptr(), fail_list_old))) {
    LOG_WARN("failed to get fail list", K(ret), K(fail_list_old));
  } else {
    const FailMsg& failmsg = fail_list.at(0);                                         // only one
    const int64_t interval = 10 * 60 * 1000 * 1000;                                   // 10min
    for (int64_t idx = fail_list_old.count() - 1; OB_SUCC(ret) && idx >= 0; --idx) {  // update fail list all
      const FailMsg& old_failmsg = fail_list_old.at(idx);
      if (interval <= (now - old_failmsg.get_last_fail_time())) {
        // FailMsg is long ago
        if (OB_FAIL(fail_list_old.remove(idx))) {
          LOG_WARN("failed to remove long time ago fail task", K(ret), K(idx), K(fail_list_old));
        }
      } else if (!found) {
        if (old_failmsg.dest_server_ == failmsg.dest_server_ && old_failmsg.task_type_ == failmsg.task_type_) {
          // check if this is a failure with the same type
          // record this failure and clean up the ones generated 10min ago.
          found = true;
          int64_t new_last_fail_time = OB_INVALID_TIMESTAMP;
          if (1 > failmsg.count_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("non failed time", K(ret), K(failmsg));
          } else {
            new_last_fail_time = now;
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(fail_list_old.at(idx).add_failed_timestamp(new_last_fail_time))) {
            LOG_WARN("failed to add failed timestamp", K(ret), K(failmsg), K(new_last_fail_time));
          } else if (OB_FAIL(fail_list_old.at(idx).remove_useless_timestamp(now, interval))) {
            // remove the message generated long time ago
            LOG_WARN("failed to remove useless timestamp", K(ret), K(failmsg), K(now), K(interval));
          }
        }
      }  // end not find
    }    // end for progress faillist
    if (OB_SUCC(ret) && !found) {
      if (OB_FAIL(fail_list_old.push_back(failmsg))) {
        LOG_WARN("fail to push back", K(ret), K(failmsg));
      }
    }
    if (OB_SUCC(ret)) {
      fail_list.reset();
      if (OB_FAIL(copy_assign(fail_list, fail_list_old))) {
        LOG_WARN("failed to assign fail list", K(ret), K(fail_list), K(fail_list_old));
      }
    }
  }
  return ret;
}

int ObPartitionReplica::set_faillist(const FailList& fail_list)
{
  int ret = OB_SUCCESS;
  reset_fail_list();
  if (0 < fail_list.count()) {
    char* fail_list_str = NULL;
    if (NULL == (fail_list_str = static_cast<char*>(allocator_.alloc(OB_MAX_FAILLIST_LENGTH)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else if (OB_FAIL(ObPartitionReplica::fail_list2text(fail_list, fail_list_str, OB_MAX_FAILLIST_LENGTH))) {
      LOG_WARN("failed to string fail list", K(ret), K(fail_list));
    } else {
      fail_list_.assign_ptr(fail_list_str, static_cast<int32_t>(strlen(fail_list_str)));
    }
  }
  return ret;
}

bool ObPartitionReplica::is_in_blacklist(int64_t task_type, ObAddr dest_server) const
{
  bool bret = false;
  int ret = OB_SUCCESS;
  int64_t now = ObTimeUtility::current_time();
  FailList fail_list;
  if (GCONF.balance_blacklist_failure_threshold == 0) {
    // do nothing ,close black list
  } else if (!fail_list_.empty()) {
    if (OB_FAIL(text2fail_list(fail_list_.ptr(), fail_list))) {
      LOG_WARN("failed to get fail list", K(ret), K(fail_list));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < fail_list.count(); ++j) {  // update fail list
      if (fail_list.at(j).dest_server_ == dest_server && fail_list.at(j).task_type_ == task_type &&
          fail_list.at(j).get_failed_count() >= GCONF.balance_blacklist_failure_threshold &&
          GCONF.balance_blacklist_retry_interval > (now - fail_list.at(j).get_last_fail_time())) {
        // the frequency of failure exceeds the tolerance, however not timeout, still in faillist
        bret = true;
        LOG_INFO("the task in black list", K(fail_list.at(j)));
        break;
      }
    }
  }
  return bret;
}

ObPartitionInfo::ObPartitionInfo()
    : table_id_(OB_INVALID_ID), partition_id_(OB_INVALID_INDEX), allocator_(NULL), replicas_()
{}

ObPartitionInfo::ObPartitionInfo(common::ObIAllocator& allocator)
    : table_id_(OB_INVALID_ID),
      partition_id_(OB_INVALID_INDEX),
      allocator_(&allocator),
      replicas_(common::OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(allocator))
{}

ObPartitionInfo::~ObPartitionInfo()
{
  reset();
}

void ObPartitionInfo::reset()
{
  table_id_ = OB_INVALID_ID;
  partition_id_ = OB_INVALID_INDEX;
  allocator_ = NULL;
  replicas_.reset();
}

int64_t ObPartitionInfo::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KT_(table_id), K_(partition_id), K_(replicas));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER(ObPartitionInfo, table_id_, partition_id_, replicas_);

int ObPartitionInfo::verify_checksum(const ObPartitionReplica& replica) const
{
  int ret = OB_SUCCESS;
  if (!is_valid() || !replica.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "partition", *this, K(replica));
  } else {
    if (REPLICA_STATUS_NORMAL != replica.status_) {
      // nothing todo
    } else if (REPLICA_TYPE_LOGONLY == replica.replica_type_) {
      //|| REPLICA_TYPE_MMONLY == replica.replica_type_) {
      if (0 == replica.data_checksum_ && 0 == replica.row_checksum_.checksum_) {
      } else {
        ret = OB_CHECKSUM_ERROR;
        LOG_ERROR("partition replica checksum mismatch", K(ret), K(replica));
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < replicas_.count(); ++i) {
        if (replica.data_version_ == replicas_.at(i).data_version_ &&
            replica.partition_cnt_ == replicas_.at(i).partition_cnt_) {
          if (replica.is_restore_ || replicas_.at(i).is_restore_) {
            // to need to do checksum operation for replica with is_restore
          } else if (REPLICA_STATUS_NORMAL != replicas_.at(i).status_) {
            // ignore for logonly replica
          } else if (REPLICA_TYPE_LOGONLY == replicas_.at(i).replica_type_) {
            if (0 == replicas_.at(i).data_checksum_ && 0 == replicas_.at(i).row_checksum_.checksum_) {
            } else {
              ret = OB_CHECKSUM_ERROR;
              LOG_ERROR("log only replica must not has checksum",
                  K(ret),
                  "old replica data checksum",
                  replicas_.at(i).data_checksum_,
                  "old replica row checksum",
                  replicas_.at(i).row_checksum_,
                  "old replica",
                  replicas_.at(i));
            }
          } else if (replicas_.at(i).data_checksum_ != replica.data_checksum_ ||
                     replicas_.at(i).row_checksum_.checksum_ != replica.row_checksum_.checksum_) {
            ret = OB_CHECKSUM_ERROR;
            LOG_ERROR("partition replica checksum mismatch",
                K(ret),
                "old replica data checksum",
                replicas_.at(i).data_checksum_,
                "old replica row checksum",
                replicas_.at(i).row_checksum_,
                "new replica data checksum",
                replica.data_checksum_,
                "new replica row checksum",
                replica.row_checksum_,
                "old replica",
                replicas_.at(i),
                "new replica",
                replica);
          } else {
            // do nothing
          }

          if (OB_FAIL(ret)) {
            LOG_ERROR("partition replica checksum mismatch",
                K(ret),
                "old replica data checksum",
                replicas_.at(i).data_checksum_,
                "old replica row checksum",
                replicas_.at(i).row_checksum_,
                "new replica data checksum",
                replica.data_checksum_,
                "new replica row checksum",
                replica.row_checksum_,
                "old replica",
                replicas_.at(i),
                "new replica",
                replica);
          }
        }
      }
    }
  }
  return ret;
}

int ObPartitionInfo::find(const common::ObAddr& server, const ObPartitionReplica*& replica) const
{
  replica = NULL;
  int ret = OB_SUCCESS;
  int64_t idx = OB_INVALID_INDEX;
  if (!is_valid() || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "partition", *this, K(server));
  } else if (OB_FAIL(find_idx(server, idx))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("find index failed", K(ret), K(server));
    }
  } else if (idx < 0 || idx >= replicas_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid index", K(ret), K(idx), "replica_count", replicas_.count());
  } else {
    replica = &replicas_.at(idx);
  }
  return ret;
}

// no more than 1 replica in cluster has the role with a value of leader
// however, give the fault tolerance, we do not get the role of replicas completely
// depending on the value in meta table
bool ObPartitionInfo::is_leader_like(int64_t index) const
{
  bool is_leader = true;
  if (!replicas_.at(index).is_leader_like()) {
    is_leader = false;
  } else {
    FOREACH_CNT(r, replicas_)
    {
      if (OB_ISNULL(r)) {
        LOG_WARN("get invalie replica", K(replicas_), K(r));
      } else if (r->to_leader_time_ > replicas_.at(index).to_leader_time_) {
        is_leader = false;
        break;
      }
    }
  }
  return is_leader;
}

// the one with a role set to 1 and maximum to_leader_time is the leader
int ObPartitionInfo::find_leader_v2(const ObPartitionReplica*& replica) const
{
  int ret = OB_SUCCESS;
  replica = NULL;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "partition", *this);
  } else {
    int64_t laster_to_leader_time = 0;
    int64_t index = 0;
    int64_t find_index = -1;
    FOREACH_CNT_X(r, replicas_, OB_SUCC(ret))
    {
      if (r->to_leader_time_ > laster_to_leader_time) {
        find_index = index;
        laster_to_leader_time = r->to_leader_time_;
      }
      index++;
    }
    if (find_index != -1) {
      if (!replicas_.at(find_index).is_strong_leader()) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("fail to get leader replica",
            K(ret),
            K(find_index),
            "role",
            replicas_.at(find_index).role_,
            "to_leader_time",
            replicas_.at(find_index).to_leader_time_,
            K(*this));
      } else {
        replica = &replicas_.at(find_index);
      }
    } else {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("fail to get leader replica", K(ret), K(*this), "replica count", replicas_.count());
    }
  }
  return ret;
}

int ObPartitionInfo::find_latest_leader(const ObPartitionReplica*& replica) const
{
  int ret = OB_ENTRY_NOT_EXIST;
  replica = NULL;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "partition", *this);
  } else {
    FOREACH_CNT(r, replicas_)
    {
      if (r->to_leader_time_ > 0 && (NULL == replica || r->to_leader_time_ > replica->to_leader_time_)) {
        replica = &(*r);
      }
    }
    if (NULL != replica) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObPartitionInfo::find_leader_by_election(const ObPartitionReplica*& replica) const
{
  int ret = OB_SUCCESS;
  replica = NULL;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), "partition", *this);
  } else if (OB_FAIL(find_latest_leader(replica))) {
    LOG_WARN("failed to find latest leader", KR(ret));
  } else if (OB_ISNULL(replica)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("replica is null", KR(ret));
  } else if (!replica->is_leader_by_election()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("replica is not leader for balance", KR(ret), KPC(replica));
  }
  if (OB_FAIL(ret)) {  // reset
    replica = NULL;
  }
  return ret;
}

int ObPartitionInfo::find_idx(const ObPartitionReplica& replica, int64_t& idx) const
{
  int ret = OB_SUCCESS;
  idx = OB_INVALID_INDEX;
  if (!is_valid() || !replica.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "partition", *this, K(replica));
  } else if (OB_FAIL(find_idx(replica.server_, idx))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("find index failed", K(ret), K(replica));
    }
  }
  return ret;
}

int ObPartitionInfo::find_idx(const ObAddr& server, int64_t& idx) const
{
  idx = OB_INVALID_INDEX;
  int ret = OB_ENTRY_NOT_EXIST;
  if (!is_valid() || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "partition", *this, K(server));
  } else {
    for (int64_t i = 0; OB_ENTRY_NOT_EXIST == ret && i < replicas_.count(); ++i) {
      if (server == replicas_.at(i).server_) {
        idx = i;
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObPartitionInfo::add_replica_ignore_checksum_error(const ObPartitionReplica& replica, bool& is_checksum_error)
{
  int ret = OB_SUCCESS;
  is_checksum_error = false;
  if (OB_FAIL(add_replica(replica))) {
    if (OB_CHECKSUM_ERROR == ret) {
      LOG_ERROR("ERROR, ERROR, find replica checksum error", K(ret), "pkey", replica.partition_key(), K(replica));
      is_checksum_error = true;
      if (OB_FAIL(replicas_.push_back(replica))) {
        LOG_WARN("fail to push back replicas", K(ret));
      }
    }
  }
  return ret;
}

int ObPartitionInfo::add_replica(const ObPartitionReplica& replica)
{
  int ret = OB_SUCCESS;
  if (!is_valid() || !replica.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "partition", *this, K(replica));
  } else if (table_id_ != replica.table_id_ || partition_id_ != replica.partition_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("replica not belong to this partition", K(ret), KT_(table_id), K_(partition_id), K(replica));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(verify_checksum(replica))) {
      LOG_WARN("verify checksum failed", K(ret));
    }
    if (OB_CHECKSUM_ERROR == ret && GCONF.ignore_replica_checksum_error) {
      LOG_INFO("checksum error ignored", K(replica));
      ret = OB_SUCCESS;
    }
  }

  if (OB_SUCC(ret)) {
    int64_t idx = OB_INVALID_INDEX;
    ret = find_idx(replica, idx);
    if (OB_SUCCESS != ret && OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("find index failed", K(ret), K(replica));
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      if (OB_FAIL(replicas_.push_back(replica))) {
        LOG_WARN("insert replica failed", K(ret));
      }
    } else {  // OB_SUCCESS == ret
      if (idx < 0 || idx >= replicas_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid index", K(ret), K(idx), "replica_count", replicas_.count());
      } else if (OB_FAIL(replicas_.at(idx).assign(replica))) {
        LOG_WARN("failed to assign replicas_.at(idx)", K(ret), K(idx));
      }
    }
  }
  return ret;
}

int ObPartitionInfo::remove(const ObAddr& server)
{
  int ret = OB_SUCCESS;
  if (!is_valid() || !server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "partition", *this, K(server));
  } else {
    int64_t idx = OB_INVALID_INDEX;
    if (OB_FAIL(find_idx(server, idx))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      }
    } else if (idx < 0 || idx >= replicas_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid index", K(ret), K(idx), "replica_count", replicas_.count());
    } else {
      if (OB_FAIL(replicas_.remove(idx))) {
        LOG_WARN("remove replica failed", K(ret), K(idx), K(replica_count()));
      }
    }
  }
  return ret;
}

int ObPartitionInfo::set_unit_id(const ObAddr& server, const uint64_t unit_id)
{
  int ret = OB_SUCCESS;
  if (!is_valid() || !server.is_valid() || OB_INVALID_ID == unit_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "partition", *this, K(server), K(unit_id));
  } else {
    int64_t idx = OB_INVALID_INDEX;
    if (OB_FAIL(find_idx(server, idx))) {
      // return OB_ENTRY_NOT_EXIST too.
      LOG_WARN("find index failed", K(ret), K(server));
    } else if (idx < 0 || idx >= replicas_.count()) {
      LOG_WARN("invalid index", K(ret), K(idx), "replica_count", replicas_.count());
    } else {
      replicas_.at(idx).unit_id_ = unit_id;
    }
  }
  return ret;
}

int ObPartitionInfo::filter(const ObIReplicaFilter& filter)
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "partition", *this);
  }
  for (int64_t i = replicas_.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
    bool pass = true;
    if (OB_FAIL(filter.check(replicas_.at(i), pass))) {
      LOG_WARN("filter replica failed", K(ret), "replica", replicas_.at(i));
    } else {
      if (!pass) {
        if (OB_FAIL(replicas_.remove(i))) {
          LOG_WARN("remove replica failed", K(ret), "idx", i, K(replica_count()));
        }
      }
    }
  }
  return ret;
}

/* required_size and data_size of (encryption) logony replica is 0,we need to fill values for
 * (encryption) logonly replicas when executing load balance, the rules are as follows:
 * fill the required_size and data_size of the latest leader to (encryption) logonly replicas
 * fill the values using the replicas whose data_version is the maximum one;
 */
int ObPartitionInfo::update_replica_status()
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "partition", *this);
  }
  ObPartitionReplica::MemberList* member_list = NULL;
  ObPartitionReplica* size_sample = NULL;
  FOREACH_CNT_X(r, replicas_, NULL == member_list && OB_SUCCESS == ret)
  {
    if (ObReplicaTypeCheck::is_replica_with_ssstore(r->replica_type_)) {
      if (NULL == size_sample) {
        size_sample = &(*r);
      } else if (r->data_version_ > size_sample->data_version_) {
        size_sample = &(*r);
      } else {
      }  // do nothing
    }
    if (r->is_leader_by_election()) {
      member_list = &r->member_list_;
    }
  }

  FOREACH_CNT_X(r, replicas_, OB_SUCCESS == ret)
  {
    bool in_leader_member_list = (NULL == member_list) && ObReplicaTypeCheck::is_paxos_replica_V2(r->replica_type_);
    int64_t in_member_time_us = 0;
    if (NULL != member_list) {
      FOREACH_X(s, *member_list, !in_leader_member_list)
      {
        if (r->server_ == *s) {
          in_leader_member_list = true;
          in_member_time_us = s->timestamp_;
        }
      }
    }
    r->in_member_list_ = in_leader_member_list;
    r->member_time_us_ = in_member_time_us;
    ObReplicaStatus status_in_table = r->status_;
    if (r->status_ == REPLICA_STATUS_OFFLINE) {
      r->replica_status_ = REPLICA_STATUS_OFFLINE;
    } else if (r->data_version_ >= 0) {
      // replica_status_ rulse:
      // 1 paxos replicas (FULL,LOGONLY),NORMAL when in leader's member_list otherwise offline.
      // 2 non_paxos replicas (READONLY),NORMAL all the time
      // 3 if non_paxos replicas are deleted by partition service, status in meta table is set to
      // REPLICA_STATUS_OFFLINE,
      //    then set replica_status to REPLICA_STATUS_OFFLINE
      if (in_leader_member_list) {
        r->replica_status_ = REPLICA_STATUS_NORMAL;
      } else if (!ObReplicaTypeCheck::is_replica_type_valid(r->replica_type_)) {
        // invalid replicas
        r->replica_status_ = REPLICA_STATUS_OFFLINE;
      } else if (ObReplicaTypeCheck::is_paxos_replica_V2(r->replica_type_)) {
        // FULL, LOGONLY.
        r->replica_status_ = REPLICA_STATUS_OFFLINE;
      } else {
        // READONLY and so on.
        r->replica_status_ = REPLICA_STATUS_NORMAL;
      }
    } else if (r->data_version_ < 0) {
      r->replica_status_ = REPLICA_STATUS_FLAG;
    }
    if (status_in_table == REPLICA_STATUS_UNMERGED) {
      r->additional_replica_status_ = status_in_table;
    } else {
      r->additional_replica_status_ = r->replica_status_;
    }
    if (OB_FAIL(ret)) {
    } else if (ObReplicaTypeCheck::is_replica_type_valid(r->replica_type_) &&
               !ObReplicaTypeCheck::is_replica_with_ssstore(r->replica_type_)) {
      if (NULL != size_sample) {
        r->data_size_ = size_sample->data_size_;
        r->required_size_ = size_sample->required_size_;
      } else {
      }  // size_sample null let data_size and required_size zero
    } else {
    }  // replica with ssstore, do nothing
  }
  return ret;
}

void ObPartitionInfo::reset_row_checksum()
{
  FOREACH_CNT(r, replicas_)
  {
    r->row_checksum_.reset();
  }
}

int ObPartitionInfo::assign(const ObPartitionInfo& other)
{
  int ret = OB_SUCCESS;
  table_id_ = other.table_id_;
  partition_id_ = other.partition_id_;
  if (OB_FAIL(copy_assign(replicas_, other.replicas_))) {
    LOG_WARN("failed to copy replicas_", K(ret));
  }
  return ret;
}

bool ObPartitionInfo::simple_equal_with(const ObPartitionInfo& other)
{
  bool is_same = true;
  if (table_id_ != other.table_id_ || partition_id_ != other.partition_id_ ||
      replicas_.count() != other.replicas_.count()) {
    is_same = false;
  } else {
    bool finded = false;
    for (int i = 0; is_same && i < replicas_.count(); i++) {
      finded = false;
      for (int j = 0; is_same && j < other.replicas_.count(); j++) {
        if (replicas_.at(i).simple_equal_with(other.replicas_.at(j))) {
          finded = true;
          break;
        }
      }
      if (false == finded) {
        is_same = false;
      }
    }
  }
  return is_same;
}

// all replica in physical restore status
bool ObPartitionInfo::in_physical_restore() const
{
  bool bret = true;
  if (replicas_.count() <= 0) {
    bret = false;
  } else {
    int ret = OB_SUCCESS;
    FOREACH_CNT_X(r, replicas_, OB_SUCC(ret) && bret)
    {
      if (OB_ISNULL(r)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("replica is null", K(ret), K_(table_id), K_(partition_id));
      } else if (!r->in_physical_restore_status()) {
        bret = false;
      }
    }
    if (OB_FAIL(ret)) {
      bret = false;
    }
  }
  return bret;
}

// no less than one replica in standby restore status,return true
bool ObPartitionInfo::in_standby_restore() const
{
  bool bret = false;
  if (replicas_.count() <= 0) {
    bret = false;
  } else {
    FOREACH_CNT_X(r, replicas_, !bret)
    {
      if (OB_ISNULL(r)) {
        LOG_WARN("replica is null", K_(table_id), K_(partition_id));
      } else if (r->in_standby_restore()) {
        bret = true;
        break;
      }
    }
  }
  return bret;
}

int ObPartitionInfo::get_partition_cnt(int64_t& partition_cnt)
{
  int ret = OB_SUCCESS;
  partition_cnt = OB_INVALID_COUNT;
  if (0 == replica_count()) {
    ret = OB_PARTITION_NOT_EXIST;
    LOG_WARN("replicas num is zero", K(ret));
  } else if (OB_INVALID_INDEX == partition_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition_id is invalid", K(ret));
  } else {
    // logic here is the same with bPartitionKey::get_partition_cnt()
    partition_cnt = is_twopart(partition_id_) ? 0 : replicas_.at(0).partition_cnt_;
  }
  return ret;
}

int ObPartitionInfo::alloc_new_partition_info(common::ObIAllocator& allocator, ObPartitionInfo*& partition)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  if (OB_ISNULL(buf = allocator.alloc(sizeof(ObPartitionInfo)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc partition", K(ret));
  } else {
    partition = new (buf) ObPartitionInfo(allocator);
  }
  return ret;
}
}  // end namespace share
}  // end namespace oceanbase
