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
 * Binlog Record Queue
 */

#define USING_LOG_PREFIX OBLOG

#include "share/ob_define.h"
#include "ob_log_binlog_record_queue.h"
#include "ob_log_utils.h"                 // get_timestamp

using namespace oceanbase::common;
namespace oceanbase
{
namespace libobcdc
{

int BRQueue::init(const int64_t queue_size)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("BRQueue has been initialized");
    ret = OB_INIT_TWICE;
  } else if (0 >= queue_size) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != (ret = queue_.init(queue_size))) {
    LOG_ERROR("init fixed queue fail", KR(ret), K(queue_size));
  } else {
    dml_br_count_ = 0;
    ddl_br_count_ = 0;
    part_trans_task_count_ = 0;
    inited_ = true;
    LOG_INFO("br_queue init succ", K(queue_size));
  }

  return ret;
}

void BRQueue::destroy()
{
  inited_ = false;
  dml_br_count_ = 0;
  ddl_br_count_ = 0;
  part_trans_task_count_ = 0;
  queue_.destroy();
}

int BRQueue::push(ObLogBR *data, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  IBinlogRecord *br_data = NULL;
  bool need_accumulate_stat = true;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("BRQueue has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(data)) {
    LOG_ERROR("invalid argument", K(data));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(br_data = data->get_data())) {
    LOG_ERROR("binlog record data is invalid", K(data));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(do_stat_for_part_trans_task_count_(*data, need_accumulate_stat))) {
    LOG_ERROR("do_stat_for_part_trans_task_count_ fail", KR(ret), K(need_accumulate_stat));
  } else {
    int64_t end_time = timeout + get_timestamp();
    int record_type = br_data->recordType();

    while (true) {
      ret = queue_.push(data);

      if (OB_UNLIKELY(OB_SIZE_OVERFLOW != ret)) {
        break;
      }

      int64_t left_time = end_time - get_timestamp();

      if (OB_UNLIKELY(left_time <= 0)) {
        ret = OB_TIMEOUT;
        break;
      }

      cond_.timedwait(left_time);
    }

    if (OB_FAIL(ret)) {
      if (OB_TIMEOUT != ret) {
        LOG_ERROR("push data into fixed queue fail", KR(ret), K(data));
      }
    } else {
      if (EDDL == record_type) {
        ATOMIC_INC(&ddl_br_count_);
      } else if (HEARTBEAT != record_type && EBEGIN != record_type && ECOMMIT != record_type) {
        ATOMIC_INC(&dml_br_count_);
      } else {
        // do nothing
      }

      cond_.signal();
    }
  }

  return ret;
}

int BRQueue::pop(IBinlogRecord *&record, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  int32_t major_version = 0;
  uint64_t tenant_id = OB_INVALID_ID;

  if (OB_FAIL(pop(record, major_version, tenant_id, timeout))) {
    LOG_ERROR("pop BinlogRecord faili", KR(ret), K(record));
  }

  return ret;
}

int BRQueue::pop(IBinlogRecord *&record,
    int32_t &major_version,
    uint64_t &tenant_id,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  ObLogBR *next_br = NULL;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("BRQueue has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(pop_next_br_(next_br, timeout))) {
    if (OB_TIMEOUT != ret) {
      LOG_ERROR("pop binlog record from br_queue fail", KR(ret));
    }
  } else if (OB_ISNULL(next_br)) {
    LOG_ERROR("pop binlog record from br_queue fail", KR(ret), K(next_br));
    ret = OB_ERR_UNEXPECTED;
  } else {
    record = next_br->get_data();
    major_version = 0;
    tenant_id = next_br->get_tenant_id();
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(record)) {
      LOG_ERROR("binlog record data is invalid", K(record), K(next_br));
      ret = OB_ERR_UNEXPECTED;
    } else {
      int record_type = record->recordType();

      if (EDDL == record_type) {
        ATOMIC_DEC(&ddl_br_count_);
      } else if (HEARTBEAT != record_type && EBEGIN != record_type && ECOMMIT != record_type) {
        ATOMIC_DEC(&dml_br_count_);
      } else {
        // do nothing
      }
    }
  }

  return ret;
}

int BRQueue::pop_next_br_(ObLogBR *&data, const int64_t timeout)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("BRQueue has not been initialized");
    ret = OB_NOT_INIT;
  } else {
    int64_t end_time = timeout + get_timestamp();

    while (true) {
      ret = queue_.pop(data);

      if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
        break;
      }

      int64_t left_time = end_time - get_timestamp();

      if (OB_UNLIKELY(left_time <= 0)) {
        ret = OB_TIMEOUT;
        break;
      }

      cond_.timedwait(left_time);
    }

    if (OB_FAIL(ret)) {
      if (OB_TIMEOUT != ret) {
        LOG_ERROR("pop data from fixed queue fail", KR(ret));
      }
    } else {
      cond_.signal();
      bool need_accumulate_stat = false;

      if (OB_FAIL(do_stat_for_part_trans_task_count_(*data, need_accumulate_stat))) {
        LOG_ERROR("do_stat_for_part_trans_task_count_ fail", KR(ret), K(need_accumulate_stat));
      }
    }
  }

  return ret;
}

int64_t BRQueue::get_dml_br_count() const
{
  return ATOMIC_LOAD(&dml_br_count_);
}

int64_t BRQueue::get_ddl_br_count() const
{
  return ATOMIC_LOAD(&ddl_br_count_);
}

int64_t BRQueue::get_part_trans_task_count() const
{
  return ATOMIC_LOAD(&part_trans_task_count_);
}

int BRQueue::do_stat_for_part_trans_task_count_(ObLogBR &data,
    bool need_accumulate_stat)
{
  int ret = OB_SUCCESS;
  int record_type = 0;
  int64_t part_trans_task_count = 0;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("BRQueue has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(data.get_record_type(record_type))) {
    LOG_ERROR("data get_record_type fail", KR(ret),
        "record_type", print_record_type(record_type));
  } else {
    part_trans_task_count = data.get_part_trans_task_count();

    if ((EDDL == record_type) || (EBEGIN == record_type)) {
      if (need_accumulate_stat) {
        // enter BRQueue
        (void)ATOMIC_AAF(&part_trans_task_count_, part_trans_task_count);
      } else {
        // leave BRQueue
        (void)ATOMIC_AAF(&part_trans_task_count_, -part_trans_task_count);
      }
    } else {
      // do nothing
    }
  }

  return ret;
}

} // namespace libobcdc
} // namespace oceanbase
