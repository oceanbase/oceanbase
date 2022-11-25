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

using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{

ObTaskSmallResult::ObTaskSmallResult()
  : has_data_(false),
    data_len_(0),
    data_buf_(),
    affected_rows_(0),
    found_rows_(0),
    last_insert_id_(0),
    matched_rows_(0),
    duplicated_rows_(0)
{
}

ObTaskSmallResult::~ObTaskSmallResult()
{
}

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

bool ObTaskSmallResult::equal(const ObTaskSmallResult &other) const
{
  bool is_eq = (has_data_ == other.has_data_ && data_len_ == other.data_len_);
  for (int64_t i = 0; is_eq && i < data_len_ && i < MAX_DATA_BUF_LEN; ++i) {
    if (data_buf_[i] != other.data_buf_[i]) {
      is_eq = false;
    }
  }
  return is_eq;
}

int ObTaskSmallResult::assign(const ObTaskSmallResult &other)
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

void ObSliceEvent::reset()
{
  ob_slice_id_.reset();
  small_result_.reset();
}

bool ObSliceEvent::equal(const ObSliceEvent &other) const
{
  return (ob_slice_id_.equal(other.ob_slice_id_)
          && small_result_.equal(other.small_result_));
}

int ObSliceEvent::assign(const ObSliceEvent &other)
{
  int ret = OB_SUCCESS;
  ob_slice_id_ = other.ob_slice_id_;
  if (OB_FAIL(small_result_.assign(other.small_result_))) {
    LOG_WARN("fail to assign slice small result", K(ret), K(other));
  }
  return ret;
}

int ObSliceEvent::assign(ObIAllocator &allocator, const ObSliceEvent &other)
{
  int ret = OB_SUCCESS;
  ob_slice_id_ = other.ob_slice_id_;
  if (OB_FAIL(small_result_.assign(other.small_result_))) {
    LOG_WARN("fail to assign slice small result", K(ret), K(other));
  }
  return ret;
}

ObTaskEvent::ObTaskEvent()
  : task_loc_(),
    err_code_(static_cast<const int64_t>(OB_ERR_UNEXPECTED)),
    inited_(false),
    ts_task_recv_done_(0),
    ts_result_send_begin_(0)
{
}

ObTaskEvent::~ObTaskEvent()
{
}

bool ObTaskEvent::equal(const ObTaskEvent &other) const
{
  bool is_equal = (task_loc_.equal(other.task_loc_)
                   && err_code_ == other.err_code_
                   && inited_ == other.inited_);
  return is_equal;
}

int ObTaskEvent::init(const ObTaskLocation &task_loc, int64_t err_code)
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

void ObTaskEvent::reset()
{
  task_loc_.reset();
  err_code_ = static_cast<const int64_t>(OB_ERR_UNEXPECTED);
  inited_ = false;
}

int ObTaskEvent::assign(ObIAllocator &allocator, const ObTaskEvent &other)
{
  int ret = OB_SUCCESS;
  task_loc_ = other.task_loc_;
  err_code_ = other.err_code_;
  inited_ = other.inited_;
  ts_task_recv_done_ = other.ts_task_recv_done_;
  ts_result_send_begin_ = other.ts_result_send_begin_;
  return ret;
}

OB_DEF_SERIALIZE(ObTaskSmallResult)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              has_data_,
              data_len_);
  if (OB_SUCC(ret)) {
    if (has_data_ && data_len_ > 0) {
      MEMCPY(buf + pos, data_buf_, data_len_);
      pos += data_len_;
    }
  }
  LST_DO_CODE(OB_UNIS_ENCODE,
              affected_rows_,
              found_rows_,
              last_insert_id_,
              matched_rows_,
              duplicated_rows_);
  return ret;
}

OB_DEF_DESERIALIZE(ObTaskSmallResult)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
              has_data_,
              data_len_);
  if (OB_SUCC(ret)) {
    if (has_data_ && data_len_ > 0) {
      MEMCPY(data_buf_, buf + pos, data_len_);
      pos += data_len_;
    }
  }
  LST_DO_CODE(OB_UNIS_DECODE,
              affected_rows_,
              found_rows_,
              last_insert_id_,
              matched_rows_,
              duplicated_rows_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTaskSmallResult)
{
  int64_t len = 0;
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              has_data_,
              data_len_);
  if (OB_SUCC(ret)) {
    if (has_data_ && data_len_ > 0) {
      len += data_len_;
    }
  }
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              affected_rows_,
              found_rows_,
              last_insert_id_,
              matched_rows_,
              duplicated_rows_);
  return len;
}

OB_SERIALIZE_MEMBER(ObSliceEvent, ob_slice_id_, small_result_);
OB_SERIALIZE_MEMBER(ObTaskEvent, task_loc_, err_code_, inited_, ts_task_recv_done_, ts_result_send_begin_);

int ObMiniTaskResult::assign(const ObMiniTaskResult &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(task_result_.assign(other.task_result_))) {
    LOG_WARN("assign task result failed", K(ret), K(other));
  } else if (OB_FAIL(extend_result_.assign(other.extend_result_))) {
    LOG_WARN("assign extend result failed", K(ret), K(other));
  }
  return ret;
}

int ObMiniTaskResult::append_mini_task_result(const ObMiniTaskResult &mini_task_result)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(task_result_.get_datum_store()
              .append_datum_store(mini_task_result.task_result_.get_datum_store()))) {
    LOG_WARN("append row store to task result failed", K(ret), K(mini_task_result));
  } else if (OB_FAIL(extend_result_.get_datum_store()
                     .append_datum_store(mini_task_result.extend_result_.get_datum_store()))) {
    LOG_WARN("append extend result failed", K(ret), K(mini_task_result));
  }
  return ret;
}

void ObMiniTaskRetryInfo::process_minitask_event(const ObMiniTaskEvent &event, int task_ret,
    int extend_ret) {
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != task_ret && need_retry_) {
    LOG_TRACE("Minitask need retry", K(event.get_task_id()),
        K(event.get_task_result().get_task_result().get_found_rows()));
    /**
     * 为什么减1，因为执行的最后一行加入scanner导致了错误，其实他并没有被成功的带回来。
     * 为什么不在远端减1，因为远端减1后可能得到0，即本处无法判断这里的0意味着远端是旧版本
     * 还是远端执行扫描1次发生了失败。
     */
    int64_t scan_times = event.get_task_result().get_task_result().get_found_rows() - 1;
    /**
     * 第一次执行的时候是非retry模式，这时候不保证found rows与range count的关系，
     * 直接标记成功扫描的数量为0，即所有结果行都无效。
     */
    int64_t succ_range_count = is_retry_execution() ? scan_times : 0;
    if (OB_FAIL(failed_task_lists_.push_back(std::make_pair(event.get_task_id(),
              succ_range_count)))) {
      LOG_WARN("Failed to push back task id", K(ret));
    }
    retry_ret_ = ret;
    need_retry_ = need_retry_
                  && (OB_OVERSIZE_NEED_RETRY == task_ret)    // task的错误码是4019
                  && (OB_SUCCESS == retry_ret_)              // push back成功
                  && (OB_SUCCESS == extend_ret);             // exetend ret在插入删除修改操作中才可能是不成功，这种情况不重试。
    const bool old_version = (-1 == scan_times);
    /**
     * 当次修改的版本中，远端执行会将found rows设置为multi part table scan的扫描次数.
     * 前面版本没有使用这个变量，值为0.
     * 当这个值为0的时候意味着远端为旧版本，我们进行重试就按照单行.
     * 升级过程中，远端执行既有单行版本，又有支持尽力回表的版本，那也按照单行来回表。
     */
    retry_by_single_range_ = retry_by_single_range_ || old_version;
  }
  if (is_master_changed_error(task_ret)
      || is_server_down_error(task_ret)
      || is_server_status_error(task_ret)
      || is_unit_migrate(task_ret)
      || is_transaction_rpc_timeout_err(task_ret)
      || is_has_no_readable_replica_err(task_ret)
      || is_select_dup_follow_replic_err(task_ret)
      || is_partition_change_error(task_ret)) {
    if (OB_FAIL(not_master_tasks_.push_back(event.get_task_id()))) {
      LOG_WARN("failed to push back task id", K(task_ret));
    }
  }
}

OB_SERIALIZE_MEMBER(ObMiniTaskResult, task_result_, extend_result_);
OB_SERIALIZE_MEMBER(ObRemoteResult, task_id_, result_, has_more_);
}/* ns sql*/
}/* ns oceanbase */
