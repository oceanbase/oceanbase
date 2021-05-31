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

#define USING_LOG_PREFIX SQL_EXE

#include "sql/executor/ob_task_event.h"
#include "sql/executor/ob_distributed_scheduler.h"

using namespace oceanbase::common;
namespace oceanbase {
namespace sql {

ObTaskSmallResult::ObTaskSmallResult()
    : has_data_(false),
      data_len_(0),
      data_buf_(),
      affected_rows_(0),
      found_rows_(0),
      last_insert_id_(0),
      matched_rows_(0),
      duplicated_rows_(0)
{}

ObTaskSmallResult::~ObTaskSmallResult()
{}

void ObTaskSmallResult::reset()
{
  has_data_ = false;
  data_len_ = 0;
  affected_rows_ = 0;
  found_rows_ = 0;
  last_insert_id_ = 0;
  matched_rows_ = 0;
  duplicated_rows_ = 0;
}

bool ObTaskSmallResult::equal(const ObTaskSmallResult& other) const
{
  bool is_eq = (has_data_ == other.has_data_ && data_len_ == other.data_len_);
  for (int64_t i = 0; is_eq && i < data_len_ && i < MAX_DATA_BUF_LEN; ++i) {
    if (data_buf_[i] != other.data_buf_[i]) {
      is_eq = false;
    }
  }
  return is_eq;
}

int ObTaskSmallResult::assign(const ObTaskSmallResult& other)
{
  int ret = OB_SUCCESS;
  has_data_ = other.has_data_;
  if (OB_UNLIKELY(other.data_len_ > MAX_DATA_BUF_LEN)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("data len is too long", K(ret), K(other.data_len_), LITERAL_K(MAX_DATA_BUF_LEN));
  } else {
    data_len_ = other.data_len_;
    MEMCPY(data_buf_, other.data_buf_, other.data_len_);
    affected_rows_ = other.affected_rows_;
    found_rows_ = other.found_rows_;
    last_insert_id_ = other.last_insert_id_;
    matched_rows_ = other.matched_rows_;
    duplicated_rows_ = other.duplicated_rows_;
  }
  return ret;
}

int ObTaskSmallResult::assign_from_ir_item(const ObIIntermResultItem& ir_item)
{
  int ret = OB_SUCCESS;
  has_data_ = true;
  if (OB_UNLIKELY(ir_item.get_data_len() > MAX_DATA_BUF_LEN)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("data len is too long", K(ret), K(ir_item.get_data_len()), LITERAL_K(MAX_DATA_BUF_LEN));
  } else {
    data_len_ = ir_item.get_data_len();
    if (OB_FAIL(ir_item.copy_data(data_buf_, data_len_))) {
      LOG_WARN("copy result to task result failed", K(ret), "interm_result_item", ir_item);
    }
  }
  return ret;
}

int ObTaskResultBuf::add_slice_event(const ObSliceEvent& slice_event)
{
  if (slice_events_buf_.count() > 0) {
    if (slice_events_buf_.at(0).get_ob_slice_id().get_task_id() != slice_event.get_ob_slice_id().get_task_id()) {
      BACKTRACE(ERROR, true, "different task id");
    }
  }
  return slice_events_buf_.push_back(slice_event);
}

OB_SERIALIZE_MEMBER(ObTaskResultBuf, task_location_, slice_events_buf_);

const int64_t ObShuffleKey::HASH_IDX = 0;
const int64_t ObShuffleKey::COUNT_IDX = 1;
const int64_t ObShuffleKey::LOWER_IDX = 0;
const int64_t ObShuffleKey::UPPER_IDX = 1;

int ObShuffleKey::assign(ObIAllocator& allocator, const ObShuffleKey& other)
{
  int ret = OB_SUCCESS;
  type_ = other.type_;
  if (OB_FAIL(deep_copy_obj(allocator, other.values_[0], values_[0]))) {
    LOG_WARN("fail to deep copy values[0]", K(ret), K(other.values_[0]));
  } else if (OB_FAIL(deep_copy_obj(allocator, other.values_[1], values_[1]))) {
    LOG_WARN("fail to deep copy values[1]", K(ret), K(other.values_[1]));
  }
  return ret;
}

int ObShuffleKey::set_shuffle_type(const share::schema::ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  if (table_schema.is_range_part())
    set_shuffle_type(ST_RANGE);
  else if (table_schema.is_key_part())
    set_shuffle_type(ST_KEY);
  else if (table_schema.is_hash_part())
    set_shuffle_type(ST_HASH);
  else if (table_schema.is_list_part())
    set_shuffle_type(ST_LIST);
  else {
    ret = OB_NOT_IMPLEMENT;
    LOG_WARN("the shuffle type is still not inplement", K(ret));
  }
  return ret;
}

int ObShuffleKey::set_sub_shuffle_type(const share::schema::ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  if (table_schema.is_range_subpart())
    set_shuffle_type(ST_RANGE);
  else if (table_schema.is_key_subpart())
    set_shuffle_type(ST_KEY);
  else if (table_schema.is_hash_subpart())
    set_shuffle_type(ST_HASH);
  else if (table_schema.is_list_subpart())
    set_shuffle_type(ST_LIST);
  else {
    ret = OB_NOT_IMPLEMENT;
    LOG_WARN("the shuffle type is still not inplement", K(ret));
  }
  return ret;
}

bool ObShuffleKey::equal(const ObShuffleKey& other) const
{
  bool is_equal = false;
  if (ST_NONE == type_ || ST_NONE == other.type_) {
    is_equal = false;
  } else if (type_ == other.type_) {
    is_equal = (values_[0] == other.values_[0] && values_[1] == other.values_[1]);
  }
  return is_equal;
}

bool ObShuffleKey::match(const ObShuffleKey& other) const
{
  bool is_match = false;
  if (ST_NONE == type_ || ST_NONE == other.type_) {
    is_match = true;
  } else if (type_ == other.type_) {
    is_match = (values_[0] == other.values_[0] && values_[1] == other.values_[1]);
  }
  return is_match;
}

int ObShuffleKey::compare(const ObShuffleKey& other, int& cmp) const
{
  int ret = OB_SUCCESS;
  cmp = 0;
  if (type_ != other.type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is different", K(ret), K_(type), K_(other.type));
  } else if (ST_HASH == type_ || ST_KEY == type_) {
    if (values_[COUNT_IDX] != other.values_[COUNT_IDX]) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("hash part count is different",
          "count",
          values_[COUNT_IDX].get_int(),
          "other.count",
          other.values_[COUNT_IDX].get_int());
    } else {
      cmp = values_[HASH_IDX].compare(other.values_[HASH_IDX]);
    }
  } else if (ST_RANGE == type_) {
    if (values_[LOWER_IDX] == other.values_[LOWER_IDX] && values_[UPPER_IDX] == other.values_[UPPER_IDX]) {
      cmp = 0;
    } else if (values_[UPPER_IDX] <= other.values_[LOWER_IDX]) {
      cmp = -1;
    } else if (values_[LOWER_IDX] >= other.values_[UPPER_IDX]) {
      cmp = 1;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("range intersect", K(ret), K(values_[0]), K(values_[1]), K(other.values_[0]), K(other.values_[1]));
    }
  }
  return ret;
}

DEF_TO_STRING(ObShuffleKey)
{
  int64_t pos = 0;
  J_OBJ_START();
  switch (type_) {
    case ST_HASH:
      J_KV("type", "hash", "value", values_[HASH_IDX], "count", values_[COUNT_IDX]);
      break;
    case ST_RANGE:
      J_KV("type", "range", "lower", values_[LOWER_IDX], "upper", values_[UPPER_IDX]);
      break;
    default:
      J_KV("type", "none");
      break;
  }
  J_OBJ_END();
  return pos;
}

void ObShuffleKeys::reset()
{
  part_key_.reset();
  subpart_key_.reset();
}

int ObShuffleKeys::assign(ObIAllocator& allocator, const ObShuffleKeys& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(part_key_.assign(allocator, other.part_key_))) {
    LOG_WARN("fail to assian part key", K(ret), K(other.part_key_));
  } else if (OB_FAIL(subpart_key_.assign(allocator, other.subpart_key_))) {
    LOG_WARN("fail to assian subpart key", K(ret), K(other.subpart_key_));
  }
  return ret;
}

int ObShuffleKeys::compare(const ObShuffleKeys& other, bool cmp_part, bool cmp_subpart, int& cmp) const
{
  int ret = OB_SUCCESS;
  if (cmp_part && OB_FAIL(part_key_.compare(other.part_key_, cmp))) {
    LOG_WARN("fail to compare part key", K(ret));
  } else if (cmp_subpart && 0 == cmp && OB_FAIL(subpart_key_.compare(other.subpart_key_, cmp))) {
    LOG_WARN("fail to compare subpart key", K(ret));
  }
  return ret;
}

void ObSliceEvent::reset()
{
  ob_slice_id_.reset();
  shuffle_keys_.reset();
  shuffle_partition_key_.reset();
  small_result_.reset();
}

bool ObSliceEvent::equal(const ObSliceEvent& other) const
{
  return (ob_slice_id_.equal(other.ob_slice_id_) && shuffle_partition_key_ == other.shuffle_partition_key_ &&
          small_result_.equal(other.small_result_));
}

int ObSliceEvent::assign(const ObSliceEvent& other)
{
  int ret = OB_SUCCESS;
  ob_slice_id_ = other.ob_slice_id_;
  shuffle_keys_.part_key_ = other.shuffle_keys_.part_key_;
  shuffle_keys_.subpart_key_ = other.shuffle_keys_.subpart_key_;
  shuffle_partition_key_ = other.shuffle_partition_key_;
  if (OB_FAIL(small_result_.assign(other.small_result_))) {
    LOG_WARN("fail to assign slice small result", K(ret), K(other));
  }
  return ret;
}

int ObSliceEvent::assign(ObIAllocator& allocator, const ObSliceEvent& other)
{
  int ret = OB_SUCCESS;
  ob_slice_id_ = other.ob_slice_id_;
  if (OB_FAIL(shuffle_keys_.assign(allocator, other.shuffle_keys_))) {
    LOG_WARN("fail to assign shuffle keys", K(ret), K(other));
  } else if (FALSE_IT(shuffle_partition_key_ = other.shuffle_partition_key_)) {
    // nothing.
  } else if (OB_FAIL(small_result_.assign(other.small_result_))) {
    LOG_WARN("fail to assign slice small result", K(ret), K(other));
  }
  return ret;
}

ObTaskEvent::ObTaskEvent()
    : task_loc_(),
      err_code_(static_cast<const int64_t>(OB_ERR_UNEXPECTED)),
      inited_(false),
      slice_events_(ObModIds::OB_SQL_EXECUTOR_TASK_EVENT, OB_MALLOC_NORMAL_BLOCK_SIZE),
      ts_task_recv_done_(0),
      ts_result_send_begin_(0)
{}

ObTaskEvent::~ObTaskEvent()
{}

bool ObTaskEvent::equal(const ObTaskEvent& other) const
{
  bool is_equal = (task_loc_.equal(other.task_loc_) && err_code_ == other.err_code_ && inited_ == other.inited_ &&
                   slice_events_.count() == other.slice_events_.count());
  for (int64_t i = 0; is_equal && i < slice_events_.count(); ++i) {
    if (slice_events_.at(i).equal(other.slice_events_.at(i))) {
      is_equal = false;
    }
  }
  return is_equal;
}

int ObTaskEvent::init(const ObTaskLocation& task_loc, int64_t err_code)
{
  int ret = OB_SUCCESS;
  if (!task_loc.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task location is invalid", K(ret), K(task_loc));
  } else {
    task_loc_ = task_loc;
    err_code_ = err_code;
    inited_ = true;
  }
  return ret;
}

int ObTaskEvent::add_slice_event(const ObSliceEvent& slice_event)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(slice_events_.push_back(slice_event))) {
    LOG_WARN("fail to push back slice event", K(ret), K(slice_event));
  }
  return ret;
}

void ObTaskEvent::reset()
{
  task_loc_.reset();
  err_code_ = static_cast<const int64_t>(OB_ERR_UNEXPECTED);
  inited_ = false;
  slice_events_.reset();
}

int ObTaskEvent::assign(ObIAllocator& allocator, const ObTaskEvent& other)
{
  int ret = OB_SUCCESS;
  task_loc_ = other.task_loc_;
  err_code_ = other.err_code_;
  inited_ = other.inited_;
  ts_task_recv_done_ = other.ts_task_recv_done_;
  ts_result_send_begin_ = other.ts_result_send_begin_;
  ObSliceEvent buf_event;
  slice_events_.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < other.slice_events_.count(); i++) {
    if (OB_FAIL(buf_event.assign(allocator, other.slice_events_.at(i)))) {
      LOG_WARN("fail to assign task event", K(ret), K(other.slice_events_.at(i)));
    } else if (OB_FAIL(slice_events_.push_back(buf_event))) {
      LOG_WARN("fail to push back task event", K(ret), K(buf_event));
    } else {
      buf_event.reset();
    }
  }
  return ret;
}

void ObTaskCompleteEvent::reset()
{
  ObTaskEvent::reset();
  extend_info_.reset();
  trans_result_.reset();
  implicit_cursors_.reset();
}

bool ObTaskCompleteEvent::equal(const ObTaskCompleteEvent& other) const
{
  return (ObTaskEvent::equal(other) && extend_info_ == other.extend_info_);
}

int ObTaskCompleteEvent::assign(ObIAllocator& allocator, const ObTaskCompleteEvent& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTaskEvent::assign(allocator, other))) {
    LOG_WARN("fail to assign ObTaskEvent", K(ret), K(other));
  } else if (OB_FAIL(trans_result_.assign(other.trans_result_))) {
    LOG_WARN("fail to assign trans result", K(ret));
  } else if (OB_FAIL(implicit_cursors_.assign(other.implicit_cursors_))) {
    LOG_WARN("fail to assign implicit cursors", K(ret), K(other.implicit_cursors_));
  } else {
    extend_info_ = other.extend_info_;
  }
  return ret;
}

OB_DEF_SERIALIZE(ObIntermResultItem)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, row_count_, data_len_);
  if (OB_SUCC(ret)) {
    if (data_len_ > 0) {
      if (OB_ISNULL(data_buf_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("data buffer is NULL", K(ret), K(data_len_));
      } else {
        MEMCPY(buf + pos, data_buf_, data_len_);
        pos += data_len_;
      }
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObIntermResultItem)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, row_count_, data_len_);
  if (OB_SUCC(ret)) {
    if (data_len_ > 0) {
      if (OB_UNLIKELY(NULL != data_buf_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("data buffer is not NULL", K(ret), K(data_len_));
      } else if (OB_ISNULL(data_buf_ = static_cast<char*>(allocator_.alloc(data_len_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(ret), K(data_len_));
      } else {
        MEMCPY(data_buf_, buf + pos, data_len_);
        pos += data_len_;
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObIntermResultItem)
{
  int64_t len = 0;
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ADD_LEN, row_count_, data_len_);
  if (OB_SUCC(ret)) {
    if (data_len_ > 0) {
      len += data_len_;
    }
  }
  return len;
}

OB_DEF_SERIALIZE(ObTaskSmallResult)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, has_data_, data_len_);
  if (OB_SUCC(ret)) {
    if (has_data_ && data_len_ > 0) {
      MEMCPY(buf + pos, data_buf_, data_len_);
      pos += data_len_;
    }
  }
  LST_DO_CODE(OB_UNIS_ENCODE, affected_rows_, found_rows_, last_insert_id_, matched_rows_, duplicated_rows_);
  return ret;
}

OB_DEF_DESERIALIZE(ObTaskSmallResult)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, has_data_, data_len_);
  if (OB_SUCC(ret)) {
    if (has_data_ && data_len_ > 0) {
      MEMCPY(data_buf_, buf + pos, data_len_);
      pos += data_len_;
    }
  }
  LST_DO_CODE(OB_UNIS_DECODE, affected_rows_, found_rows_, last_insert_id_, matched_rows_, duplicated_rows_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTaskSmallResult)
{
  int64_t len = 0;
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ADD_LEN, has_data_, data_len_);
  if (OB_SUCC(ret)) {
    if (has_data_ && data_len_ > 0) {
      len += data_len_;
    }
  }
  LST_DO_CODE(OB_UNIS_ADD_LEN, affected_rows_, found_rows_, last_insert_id_, matched_rows_, duplicated_rows_);
  return len;
}

OB_SERIALIZE_MEMBER(ObShuffleKey, type_, values_);
OB_SERIALIZE_MEMBER(ObShuffleKeys, part_key_, subpart_key_);
OB_SERIALIZE_MEMBER(ObSliceEvent, ob_slice_id_, shuffle_partition_key_, small_result_, shuffle_keys_);
OB_SERIALIZE_MEMBER(
    ObTaskEvent, task_loc_, err_code_, inited_, slice_events_, ts_task_recv_done_, ts_result_send_begin_);
OB_SERIALIZE_MEMBER_INHERIT(ObTaskCompleteEvent, ObTaskEvent, extend_info_, trans_result_, implicit_cursors_);
int ObTaskCompleteEvent::merge_implicit_cursors(const ObIArray<ObImplicitCursorInfo>& implicit_cursors)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < implicit_cursors.count(); ++i) {
    if (implicit_cursors.at(i).stmt_id_ >= 0) {
      if (OB_FAIL(implicit_cursors_.push_back(implicit_cursors.at(i)))) {
        LOG_WARN("store implicit cursors failed", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObMiniTaskResult::assign(const ObMiniTaskResult& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(task_result_.assign(other.task_result_))) {
    LOG_WARN("assign task result failed", K(ret), K(other));
  } else if (OB_FAIL(extend_result_.assign(other.extend_result_))) {
    LOG_WARN("assign extend result failed", K(ret), K(other));
  }
  return ret;
}

int ObMiniTaskResult::append_mini_task_result(
    const ObMiniTaskResult& mini_task_result, bool is_static_engine /* = false */)
{
  int ret = OB_SUCCESS;
  if (is_static_engine) {
    if (OB_FAIL(task_result_.get_datum_store().append_datum_store(mini_task_result.task_result_.get_datum_store()))) {
      LOG_WARN("append row store to task result failed", K(ret), K(mini_task_result));
    } else if (OB_FAIL(extend_result_.get_datum_store().append_datum_store(
                   mini_task_result.extend_result_.get_datum_store()))) {
      LOG_WARN("append extend result failed", K(ret), K(mini_task_result));
    }
  } else {
    if (OB_FAIL(task_result_.get_row_store().append_row_store(mini_task_result.task_result_))) {
      LOG_WARN("append row store to task result failed", K(ret), K(mini_task_result));
    } else if (OB_FAIL(extend_result_.get_row_store().append_row_store(mini_task_result.extend_result_))) {
      LOG_WARN("append extend result failed", K(ret), K(mini_task_result));
    }
  }
  return ret;
}

void ObMiniTaskRetryInfo::process_minitask_event(const ObMiniTaskEvent& event, int task_ret, int extend_ret)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != task_ret && need_retry_) {
    LOG_TRACE(
        "Minitask need retry", K(event.get_task_id()), K(event.get_task_result().get_task_result().get_found_rows()));
    /**
     * Why is it subtracted by 1, because the addition of scanner in the last line of execution caused an error,
     * in fact, it was not successfully brought back.
     * Why not subtract 1 at the remote end, because the remote end may get 0 after subtracting 1,
     * the local office cannot judge that the 0 here means that the remote end is the old version
     * or the remote execution of the scan failed once.
     */
    int64_t scan_times = event.get_task_result().get_task_result().get_found_rows() - 1;
    /**
     * The first time it is executed in non-retry mode,
     * the relationship between found rows and range count is not guaranteed at this time.
     * Directly mark the number of successful scans as 0, that is, all result rows are invalid.
     */
    int64_t succ_range_count = is_retry_execution() ? scan_times : 0;
    if (OB_FAIL(failed_task_lists_.push_back(std::make_pair(event.get_task_id(), succ_range_count)))) {
      LOG_WARN("Failed to push back task id", K(ret));
    }
    retry_ret_ = ret;
    need_retry_ = need_retry_ && (OB_OVERSIZE_NEED_RETRY == task_ret)  // The error code of task is 4019
                  && (OB_SUCCESS == retry_ret_)                        // push back successfully
                  && (OB_SUCCESS == extend_ret);  // exetend ret may be unsuccessful in insert, delete, modify,
                                                  // and do not try again in this case.
    const bool old_version = (-1 == scan_times);
    /**
     * In the revised version, the remote execution will set found rows to the scan times of the multi part table scan.
     * The previous version did not use this variable, the value is 0.
     * When this value is 0, it means that the remote end is the old version, and we will follow the single line when we
     * retry. During the upgrade process, the remote execution has both a single-line version and a version that
     * supports best-effort return to the table, and then the single-row return table is also used.
     */
    retry_by_single_range_ = retry_by_single_range_ || old_version;
  }
  if (is_master_changed_error(task_ret) || is_server_down_error(task_ret) || is_server_status_error(task_ret) ||
      is_unit_migrate(task_ret) || is_transaction_rpc_timeout_err(task_ret) ||
      is_has_no_readable_replica_err(task_ret) || is_select_dup_follow_replic_err(task_ret) ||
      is_partition_change_error(task_ret)) {
    if (OB_FAIL(not_master_tasks_.push_back(event.get_task_id()))) {
      LOG_WARN("failed to push back task id", K(task_ret));
    }
  }
}

OB_SERIALIZE_MEMBER(ObMiniTaskResult, task_result_, extend_result_);
OB_SERIALIZE_MEMBER(ObRemoteResult, task_id_, result_, has_more_);
}  // namespace sql
}  // namespace oceanbase
