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

// #ifdef OB_BUILD_KAFKA
#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/table/ob_kafka_table_row_iter.h"
#include "share/external_table/ob_external_table_utils.h"
#include "storage/routine_load/ob_routine_load_table_operator.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace sql
{
//NOTE: cleanup resources
ObKAFKATableRowIterator::~ObKAFKATableRowIterator()
{
  int ret = OB_SUCCESS;
  if (NULL != bit_vector_cache_) {
    malloc_alloc_.free(bit_vector_cache_);
    bit_vector_cache_ = NULL;
  }
  clear_state_buf_();
  if (NULL != consumer_) {
    // rd_kafka_resp_err_t kafka_err = rd_kafka_consumer_close(consumer_);
    // if (RD_KAFKA_RESP_ERR_NO_ERROR != kafka_err) {
    //   LOG_WARN("fail to rd_kafka_consumer_close", KR(ret), KR(kafka_err), KCSTRING(rd_kafka_err2str(kafka_err)));
    // }
    rd_kafka_destroy(consumer_);
    consumer_ = NULL;
  }
}

void ObKAFKATableRowIterator::clear_state_buf_()
{
  if (NULL != state_.buf_) {
    malloc_alloc_.free(state_.buf_);
    state_.buf_ = NULL;
  }
  if (NULL != state_.escape_buf_) {
    malloc_alloc_.free(state_.escape_buf_);
    state_.escape_buf_ = NULL;
  }
  if (state_.kafka_partition_offset_map_.created()) {
    state_.kafka_partition_offset_map_.destroy();
  }
}

//TODO: 这个reset什么时候调用
void ObKAFKATableRowIterator::reset()
{
  // reset state_ to initial values for rescan
  state_.reuse();
}

int ObKAFKATableRowIterator::init(const storage::ObTableScanParam *scan_param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan param is null", KR(ret));
  } else {
    common::ObArenaAllocator arena_alloc;
    ObSEArray<std::pair<common::ObString, common::ObString>, 16> custom_properties;
    arena_alloc_.set_attr(lib::ObMemAttr(scan_param->tenant_id_, "KAFKARowIter"));
    malloc_alloc_.set_attr(lib::ObMemAttr(scan_param->tenant_id_, "KAFKARowIter"));
    if (OB_FAIL(ObExternalTableRowIterator::init(scan_param))) {
      LOG_WARN("fail to init scan param", KR(ret));
    } else if (OB_FAIL(scan_param_->external_file_format_.kafka_format_.decrypt(arena_alloc, custom_properties))) {
      LOG_WARN("failed to decrypt kafka format", KR(ret));
    } else if (OB_FAIL(parser_.init(scan_param->external_file_format_.csv_format_))) {
      LOG_WARN("fail to init parser", KR(ret));
    } else if (OB_FAIL(expand_buf())) {
      LOG_WARN("fail to expand buf", KR(ret));
    } else if (OB_ISNULL(kafka_error_msg_ = static_cast<char *>(arena_alloc_.alloc(OB_MAX_ERROR_MSG_LEN)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate kafka error msg", KR(ret));
    } else {
      MEMSET(kafka_error_msg_, '\0', OB_MAX_ERROR_MSG_LEN);
    }
    if (FAILEDx(init_consumer(scan_param->tenant_id_,
                              scan_param->external_file_format_.kafka_format_.job_id_,
                              custom_properties))) {
      LOG_WARN("fail to init consumer", KR(ret));
    } else if (OB_UNLIKELY(0 >= scan_param->scan_tasks_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected task count", KR(ret), K(scan_param->scan_tasks_.count()));
    } else if (OB_FAIL(state_.kafka_partition_offset_map_.create(scan_param->scan_tasks_.count(),
                            lib::ObMemAttr(scan_param->tenant_id_, "KafkaHashMap")))) {
      LOG_WARN("fail to create kafka partition offset map", KR(ret), K(scan_param->scan_tasks_.count()));
    } else {
      state_.job_id_ = scan_param->external_file_format_.kafka_format_.job_id_;
    }
    if (OB_SUCC(ret)) {
      if (scan_param->external_file_format_.csv_format_.file_column_nums_ *
          scan_param_->op_->get_eval_ctx().max_batch_size_ * sizeof(ObCSVGeneralParser::FieldValue)
          > OB_MAX_CSV_BATCHLINE_BUF_SIZE) {
        if (OB_FAIL(parser_.get_fields_per_line().prepare_allocate(
                    scan_param->external_file_format_.csv_format_.file_column_nums_))) {
          LOG_WARN("failed to prepare_allocate parser_.fields_per_line_",
            K(scan_param->external_file_format_.csv_format_.file_column_nums_));
        } else {
          use_handle_batch_lines_ = false;
        }
      } else {
        if (OB_FAIL(parser_.get_fields_per_line().prepare_allocate(
                    scan_param->external_file_format_.csv_format_.file_column_nums_ *
                    scan_param_->op_->get_eval_ctx().max_batch_size_))) {
          LOG_WARN("failed to prepare_allocate parser_.fields_per_line_",
            K(scan_param->external_file_format_.csv_format_.file_column_nums_),
            K(scan_param_->op_->get_eval_ctx().max_batch_size_));
        } else {
          use_handle_batch_lines_ = true;
        }
      }
    }
    //TODO: error file
    LOG_INFO("kafka table row iter init", KR(ret), K(state_), K(scan_param->scan_tasks_.count()));
  }
  return ret;
}

int ObKAFKATableRowIterator::init_consumer(
    const uint64_t tenant_id,
    const int64_t job_id,
    const ObIArray<std::pair<common::ObString, common::ObString>> &custom_properties)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator arena_alloc;
  rd_kafka_conf_t *conf = NULL;
  int64_t thread_id = get_itid();
  char group_id[64] = {'\0'};
  snprintf(group_id, sizeof(group_id), "ob_group_%ld", thread_id);
  if (NULL == kafka_error_msg_) {
    if (OB_ISNULL(kafka_error_msg_ = static_cast<char *>(arena_alloc_.alloc(OB_MAX_ERROR_MSG_LEN)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate kafka error msg", KR(ret));
    } else {
      MEMSET(kafka_error_msg_, '\0', OB_MAX_ERROR_MSG_LEN);
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(conf = rd_kafka_conf_new())) {
    ret = OB_KAFKA_ERROR;
    LOG_WARN("unexpected null conf", KR(ret), KP(conf));
  } else {
    //TODO: 每消费n条进行提交，消息保留策略
    ARRAY_FOREACH_N(custom_properties, i, cnt) {
      const ObString &first_str = custom_properties.at(i).first;
      const ObString &second_str = custom_properties.at(i).second;
      char *first_ptr = nullptr;
      char *second_ptr = nullptr;
      if (OB_ISNULL(first_ptr = static_cast<char *>(arena_alloc.alloc(first_str.length() + 1)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate", KR(ret), K(first_str.length()));
      } else {
        MEMCPY(first_ptr, first_str.ptr(), first_str.length());
        first_ptr[first_str.length()] = '\0';
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(second_ptr = static_cast<char *>(arena_alloc.alloc(second_str.length() + 1)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate", KR(ret), K(second_str.length()));
      } else {
        MEMCPY(second_ptr, second_str.ptr(), second_str.length());
        second_ptr[second_str.length()] = '\0';
      }
      if (OB_SUCC(ret)) {
        MEMSET(kafka_error_msg_, '\0', OB_MAX_ERROR_MSG_LEN);
        if (RD_KAFKA_CONF_OK != rd_kafka_conf_set(conf, first_ptr, second_ptr,
                                          kafka_error_msg_, OB_MAX_ERROR_MSG_LEN)) {
          ret = OB_KAFKA_ERROR;
          LOG_WARN("fail to rd kafka conf set", KR(ret), KCSTRING(kafka_error_msg_));
          if (OB_INVALID_ID == job_id && NULL != kafka_error_msg_ && strlen(kafka_error_msg_) > 0) {
            LOG_USER_ERROR(OB_KAFKA_ERROR, kafka_error_msg_);
          }
        }
      }
      if (NULL != first_ptr) {
        arena_alloc.free(first_ptr);
      }
      if (NULL != second_ptr) {
        arena_alloc.free(second_ptr);
      }
    }
    if (OB_SUCC(ret)) {
      MEMSET(kafka_error_msg_, '\0', OB_MAX_ERROR_MSG_LEN);
      if (RD_KAFKA_CONF_OK != rd_kafka_conf_set(conf, "group.id", group_id,
                kafka_error_msg_, OB_MAX_ERROR_MSG_LEN)) {
        ret = OB_KAFKA_ERROR;
        LOG_WARN("fail to rd kafka conf set", KR(ret), KCSTRING(kafka_error_msg_));
      }
    }
    if (OB_SUCC(ret)) {
      MEMSET(kafka_error_msg_, '\0', OB_MAX_ERROR_MSG_LEN);
      if (OB_ISNULL(consumer_ = rd_kafka_new(RD_KAFKA_CONSUMER, conf, kafka_error_msg_, OB_MAX_ERROR_MSG_LEN))) {
        ret = OB_KAFKA_ERROR;
        LOG_WARN("fail to rd kafka new", KR(ret), KCSTRING(kafka_error_msg_));
        if (OB_INVALID_ID == job_id && NULL != kafka_error_msg_ && strlen(kafka_error_msg_) > 0) {
          LOG_USER_ERROR(OB_KAFKA_ERROR, kafka_error_msg_);
        }
      }
    }
    if (OB_FAIL(ret)) {
      if (NULL != conf) {
        rd_kafka_conf_destroy(conf);
      }
    }
  }
  if (OB_SUCCESS != ret
      && NULL != kafka_error_msg_ && strlen(kafka_error_msg_) > 0
      && OB_INVALID_ID != job_id) {
    int tmp_ret = OB_SUCCESS;
    ObRoutineLoadTableOperator table_op;
    ObString error_msg(static_cast<int32_t>(strlen(kafka_error_msg_)), kafka_error_msg_);
    if (OB_TMP_FAIL(table_op.init(tenant_id, GCTX.sql_proxy_))) {
      LOG_WARN("failed to init table op", KR(ret), K(tenant_id));
    } else if (OB_TMP_FAIL(table_op.update_error_msg(job_id, error_msg))) {
      LOG_WARN("fail to update error msg", KR(ret), K(job_id));
    }
  }
  return ret;
}

int ObKAFKATableRowIterator::parse_partitions_and_offsets(
    const uint64_t tenant_id,
    const int64_t job_id,
    const ObString topic_name,
    const ObString &p_str,
    const ObString &o_str,
    ObIArray<int32_t> &partition_arr,
    ObIArray<int64_t> &offset_arr)
{
  int ret = OB_SUCCESS;
  partition_arr.reset();
  offset_arr.reset();
  char *topic_name_copy = nullptr;
  bool is_datetime = false;
  if (OB_ISNULL(topic_name_copy = static_cast<char *>(arena_alloc_.alloc(topic_name.length() + 1)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate", KR(ret), K(topic_name.length()));
  } else {
    MEMCPY(topic_name_copy, topic_name.ptr(), topic_name.length());
    topic_name_copy[topic_name.length()] = '\0';
  }
  if (OB_SUCC(ret)) {
    if (!p_str.empty()) {
      if (OB_FAIL(split_partitions(p_str, partition_arr))) {
        LOG_WARN("fail to split partitions", KR(ret), K(p_str));
      }
    } else if (OB_FAIL(get_all_partitions_from_source_(topic_name_copy, partition_arr))) {
      LOG_WARN("fail to get all partitions from source", KR(ret), KCSTRING(topic_name_copy));
    }
  }
  if (OB_SUCC(ret)) {
    if (!o_str.empty()) {
      if (OB_FAIL(split_offsets(o_str, offset_arr, is_datetime))) {
        LOG_WARN("fail to split offsets", KR(ret), K(o_str));
      }
    } else if (OB_FAIL(offset_arr.push_back(RD_KAFKA_OFFSET_END))) {
      LOG_WARN("fail to push back", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(offset_arr.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected offset_arr", KR(ret), K(p_str), K(o_str), K(partition_arr), K(offset_arr));
    } else if (offset_arr.count() != partition_arr.count() && 1 != offset_arr.count()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported kafka offsets or partitions", KR(ret), K(p_str), K(o_str), K(partition_arr), K(offset_arr));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "kafka partitions/offsets");
    } else if (offset_arr.count() < partition_arr.count()) {
      int64_t first_offset = offset_arr.at(0);
      for (int64_t i = offset_arr.count(); OB_SUCC(ret) && i < partition_arr.count(); i++) {
        if (OB_FAIL(offset_arr.push_back(first_offset))) {
          LOG_WARN("fail to push back", KR(ret), K(first_offset));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (is_datetime) {
      if (OB_FAIL(parser_datetime_offsets_(topic_name_copy, partition_arr, offset_arr))) {
        LOG_WARN("fail to parser datetime offsets_", KR(ret), KCSTRING(topic_name_copy), K(partition_arr), K(offset_arr));
      }
    } else if (OB_FAIL(convert_special_offsets_(topic_name_copy, partition_arr, offset_arr))) {
      LOG_WARN("fail to convert special offsets", KR(ret), KCSTRING(topic_name_copy), K(partition_arr), K(offset_arr));
    }
  }
  if (NULL != topic_name_copy) {
    arena_alloc_.free(topic_name_copy);
  }
  return ret;
}

int ObKAFKATableRowIterator::expand_buf()
{
  int ret = OB_SUCCESS;
  const int64_t MAX_BUFFER_SIZE = (1 << 30); //MEMORY LIMIT 1G
  int64_t new_buf_len = 0;
  char *old_buf = state_.buf_;
  char *new_buf = nullptr;
  char *new_escape_buf = nullptr;

  if (nullptr != old_buf) {
    new_buf_len = state_.buf_len_ * 2;
  } else {
    //TODO: 待定
    new_buf_len = OB_MALLOC_BIG_BLOCK_SIZE;
    // new_buf_len = 10;
  }

  if (OB_UNLIKELY(new_buf_len > MAX_BUFFER_SIZE)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("buffer size overflow", KR(ret), K(new_buf_len));
  } else if (OB_ISNULL(new_buf = static_cast<char *>(malloc_alloc_.alloc(new_buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc buf", KR(ret), K(new_buf_len));
  } else if (OB_ISNULL(new_escape_buf = static_cast<char *>(malloc_alloc_.alloc(new_buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc escape buf", KR(ret), K(new_buf_len));
  // } else if (OB_UNLIKELY(state_.pos_ != state_.buf_)) {
  //   ret = OB_ERR_UNEXPECTED;
  //   LOG_WARN("unexpected state_.pos_", KR(ret), K(state_));
  } else {
    int64_t remain_len =  (nullptr != old_buf) ? (state_.data_end_ - state_.pos_) : 0;
    if (remain_len > 0) {
      MEMCPY(new_buf, state_.pos_, remain_len);
    }
    if (nullptr != state_.buf_) {
      malloc_alloc_.free(state_.buf_);
      state_.buf_ = nullptr;
    }
    if (nullptr != state_.escape_buf_) {
      malloc_alloc_.free(state_.escape_buf_);
      state_.escape_buf_ = nullptr;
    }
    state_.buf_ = new_buf;
    state_.buf_len_ = new_buf_len;
    state_.pos_ = new_buf;
    state_.data_end_ = new_buf + remain_len;
    state_.escape_buf_ = new_escape_buf;
    state_.escape_buf_end_ = new_escape_buf + new_buf_len;
  }
  if (OB_FAIL(ret)) {
    if (nullptr != new_buf) {
      malloc_alloc_.free(new_buf);
    }
    if (nullptr != new_escape_buf) {
      malloc_alloc_.free(new_escape_buf);
    }
  }
  return ret;
}

int ObKAFKATableRowIterator::split_partitions(
    const ObString &p_str,
    ObIArray<int32_t> &partition_arr)
{
  int ret = OB_SUCCESS;
  partition_arr.reset();
  if (OB_UNLIKELY(p_str.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(p_str));
  } else {
    ObString remain_str = p_str;
    ObString item_str;
    while (OB_SUCC(ret) && !remain_str.empty()) {
      item_str = remain_str.split_on(',').trim_space_only();
      if (item_str.empty()) {
        item_str = remain_str.trim_space_only();
        remain_str.reset();
      }
      if (OB_SUCC(ret)) {
        bool valid = false;
        int32_t value = ObFastAtoi<int32_t>::atoi(item_str.ptr(), item_str.ptr() + item_str.length(), valid);
        if (!valid) {
          ret = OB_INVALID_DATA;
          LOG_WARN("fail to convert string to int", KR(ret), K(item_str), K(value));
        } else if (OB_UNLIKELY(RD_KAFKA_PARTITION_UA == value)) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not supported kafka partitions", KR(ret), K(value));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "kafka partitions");
        } else if (OB_FAIL(partition_arr.push_back(value))) {
          LOG_WARN("fail to push back to array", KR(ret), K(value));
        }
      }
    }
    // Check for duplicate partition values
    if (OB_SUCC(ret) && partition_arr.count() > 1) {
      for (int64_t i = 0; OB_SUCC(ret) && i < partition_arr.count() - 1; ++i) {
        for (int64_t j = i + 1; OB_SUCC(ret) && j < partition_arr.count(); ++j) {
          if (partition_arr.at(i) == partition_arr.at(j)) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("duplicate partition value found", KR(ret), K(partition_arr), K(i), K(j));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "duplicate kafka partitions");
          }
        }
      }
    }
  }
  return ret;
}

int ObKAFKATableRowIterator::split_offsets(
    const ObString &o_str,
    ObIArray<int64_t> &offset_arr,
    bool &is_datetime)
{
  int ret = OB_SUCCESS;
  offset_arr.reset();
  is_datetime = false;
  static const char* OFFSET_BEGINNING = "OFFSET_BEGINNING";
  static const char* OFFSET_END = "OFFSET_END";
  bool is_normal_format = false;
  if (OB_UNLIKELY(o_str.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(o_str));
  } else {
    int64_t value = OB_INVALID_ID;
    ObString remain_str = o_str;
    ObString item_str;
    while (OB_SUCC(ret) && !remain_str.empty()) {
      item_str = remain_str.split_on(',').trim_space_only();
      if (item_str.empty()) {
        item_str = remain_str.trim_space_only();
        remain_str.reset();
      }
      if (item_str.is_numeric()) {
        bool valid = false;
        value = ObFastAtoi<int64_t>::atoi(item_str.ptr(), item_str.ptr() + item_str.length(), valid);
        if (!valid) {
          ret = OB_INVALID_DATA;
          LOG_WARN("fail to convert string to int", KR(ret), K(item_str), K(value));
        } else if (value < 0 || RD_KAFKA_OFFSET_INVALID == value) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not supported kafka offsets", KR(ret), K(item_str), K(value));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "kafka offsets");
        } else {
          is_normal_format = true;
        }
      } else if (0 == item_str.case_compare(OFFSET_BEGINNING)) {
        value = RD_KAFKA_OFFSET_BEGINNING;
        is_normal_format = true;
      } else if (0 == item_str.case_compare(OFFSET_END)) {
        value = RD_KAFKA_OFFSET_END;
        is_normal_format = true;
      } else {
        //TODO: 现在不支持time格式
        // int64_t datetime_value = 0;
        // //TODO: session timezone
        // ObTimeConvertCtx cvrt_ctx(NULL, false);
        // if (OB_FAIL(ObTimeConverter::str_to_datetime(item_str, cvrt_ctx, datetime_value))) {
        //   LOG_WARN("fail to convert string to datatime", KR(ret), K(item_str));
        // } else if (!ObTimeConverter::is_valid_datetime(datetime_value)) {
        //   ret = OB_ERR_UNEXPECTED;
        //   LOG_WARN("invalid datetime", KR(ret), K(item_str), K(datetime_value));
        // } else {
        //   value = datetime_value;
        //   is_datetime = true;
        // }
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported kafka offsets", KR(ret), K(item_str));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "kafka offsets");
      }
      if (FAILEDx(offset_arr.push_back(value))) {
        LOG_WARN("fail to push back", KR(ret), K(value));
      }
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(is_datetime && is_normal_format)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected offset str", KR(ret), K(o_str), K(is_datetime), K(is_normal_format));
    }
  }
  return ret;
}

int ObKAFKATableRowIterator::get_all_partitions_from_source_(
    const char *topic_name,
    ObIArray<int32_t> &partition_arr)
{
  int ret = OB_SUCCESS;
  partition_arr.reset();
  int64_t timeout_ms = 2000;
  if (OB_ISNULL(consumer_)
      || OB_ISNULL(kafka_error_msg_)
      || OB_ISNULL(topic_name)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected consumer_ or kafka_error_msg_ or topic_name", KR(ret), KP(consumer_), KP(kafka_error_msg_), KP(topic_name));
  } else {
    rd_kafka_topic_t *rkt = NULL;
    const rd_kafka_metadata_t *metadata = NULL;
    if (OB_ISNULL(rkt = rd_kafka_topic_new(consumer_, topic_name, NULL))) {
      ret = OB_KAFKA_ERROR;
      LOG_WARN("fail to rd kafka topic new", KR(ret), KP(rkt), KCSTRING(topic_name));
    } else {
      int64_t retry_count = 0;
      while (OB_SUCC(ret)) {
        retry_count++;
        rd_kafka_resp_err_t err = rd_kafka_metadata(
          consumer_,
          0,                 // all_topics=0（仅查询指定主题）
          rkt,               // 主题对象
          &metadata,         // 输出元数据
          timeout_ms         // 超时时间(ms)
        );
        if (OB_UNLIKELY(err != RD_KAFKA_RESP_ERR_NO_ERROR)) {
          if (is_kafka_error_retriable(err)) {
            if (retry_count >= 3) {
              ret = OB_KAFKA_ERROR;
            } else {
              ret = OB_KAFKA_NEED_RETRY;
            }
          } else {
            ret = OB_KAFKA_ERROR;
          }
          LOG_WARN("fail to load metadata from kafka", KR(ret), K(retry_count), K(err), KCSTRING(rd_kafka_err2str(err)), KCSTRING(topic_name));
          if (OB_KAFKA_NEED_RETRY == ret) {
            ret = OB_SUCCESS;
            if (NULL != metadata) {
              rd_kafka_metadata_destroy(metadata);
              metadata = NULL;
            }
          } else if (OB_KAFKA_ERROR == ret) {
            copy_kafka_error_msg_(rd_kafka_err2str(err));
            if (NULL != kafka_error_msg_ && strlen(kafka_error_msg_) > 0) {
              LOG_USER_ERROR(OB_KAFKA_ERROR, kafka_error_msg_);
            }
          }
        } else {
          break;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(metadata)) {
        ret = OB_KAFKA_ERROR;
        LOG_WARN("metadata is null", KR(ret), KP(metadata));
      } else if (OB_UNLIKELY(0 == metadata->topic_cnt)) {
        ret = OB_KAFKA_ERROR;
        LOG_WARN("metadata topic_cnt is 0", KR(ret), K(metadata->topic_cnt));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < metadata->topic_cnt; i++) {
          const rd_kafka_metadata_topic_t *t = &metadata->topics[i];
          if (0 == strcmp(t->topic, topic_name)) {
            for (int64_t j = 0; OB_SUCC(ret) && j < t->partition_cnt; j++) {
              if (OB_FAIL(partition_arr.push_back(t->partitions[j].id))) {
                LOG_WARN("fail to push back", KR(ret), K(t->partitions[j].id));
              }
            }
            break;
          }
        }
      }
      if (OB_SUCC(ret) && partition_arr.empty()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported kafka partitions", KR(ret), K(partition_arr));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "kafka topic without partitions");
      }
    }
    //clean resources
    if (NULL != rkt) {
      rd_kafka_topic_destroy(rkt);
    }
    if (NULL != metadata) {
      rd_kafka_metadata_destroy(metadata);
    }
  }
  return ret;
}

int ObKAFKATableRowIterator::convert_special_offsets_(
    const char *topic_name,
    const ObIArray<int32_t> &partition_arr,
    ObIArray<int64_t> &offset_arr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == topic_name
                  || partition_arr.empty()
                  || partition_arr.count() != offset_arr.count()
                  || NULL == consumer_
                  || NULL == kafka_error_msg_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null topic_name or partition_arr or offset_arr or consumer_ or kafka_error_msg_",
             KR(ret), KP(topic_name), K(partition_arr), K(offset_arr), KP(consumer_), KP(kafka_error_msg_));
  } else {
    // Convert RD_KAFKA_OFFSET_BEGINNING and RD_KAFKA_OFFSET_END to actual offsets
    for (int64_t i = 0; OB_SUCC(ret) && i < offset_arr.count(); i++) {
      int64_t retry_count = 0;
      int32_t partition_id = partition_arr.at(i);
      int64_t low = 0;
      int64_t high = 0;
      while (OB_SUCC(ret)) {
        retry_count++;
        low = 0;
        high = 0;
        rd_kafka_resp_err_t err = rd_kafka_query_watermark_offsets(
            consumer_, topic_name, partition_id, &low, &high, 1000);
        if (OB_UNLIKELY(RD_KAFKA_RESP_ERR_NO_ERROR != err)) {
          if (is_kafka_error_retriable(err)) {
            if (retry_count >= 3) {
              ret = OB_KAFKA_ERROR;
            } else {
              ret = OB_KAFKA_NEED_RETRY;
            }
          } else {
            ret = OB_KAFKA_ERROR;
          }
          LOG_WARN("fail to rd_kafka_query_watermark_offsets for special offset",
                  KR(ret), K(retry_count), K(err), KCSTRING(rd_kafka_err2str(err)), K(partition_id), K(i));
          if (OB_KAFKA_NEED_RETRY == ret) {
            ret = OB_SUCCESS;
          } else if (OB_KAFKA_ERROR == ret) {
            copy_kafka_error_msg_(rd_kafka_err2str(err));
            if (NULL != kafka_error_msg_ && strlen(kafka_error_msg_) > 0) {
              LOG_USER_ERROR(OB_KAFKA_ERROR, kafka_error_msg_);
            }
          }
        } else {
          break;
        }
      }
      if (OB_SUCC(ret)) {
        if (RD_KAFKA_OFFSET_BEGINNING == offset_arr.at(i)) {
          offset_arr.at(i) = low;
        } else if (RD_KAFKA_OFFSET_END == offset_arr.at(i)) {
          offset_arr.at(i) = high;
        } else if (OB_UNLIKELY(offset_arr.at(i) < low)) {
          ret = OB_KAFKA_ERROR;
          LOG_WARN("unexpected kafka offset", KR(ret), K(offset_arr.at(i)), K(low), K(high));
          LOG_USER_ERROR(OB_KAFKA_ERROR, "unexpected kafka offset");
        }
      }
    }
  }
  return ret;
}

int ObKAFKATableRowIterator::parser_datetime_offsets_(
    const char *topic_name,
    const ObIArray<int32_t> &partition_arr,
    ObIArray<int64_t> &offset_arr)
{
  int ret = OB_SUCCESS;
  //TODO: 如果默认消费所有分区，那么是否可以指定一个offset？
  if (OB_UNLIKELY(NULL == topic_name
                  || partition_arr.empty()
                  || partition_arr.count() != offset_arr.count()
                  || NULL == consumer_
                  || NULL == kafka_error_msg_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected partition_arr or offset_arr or consumer_ or kafka_error_msg_", KR(ret), K(partition_arr), K(offset_arr), KP(consumer_), KP(kafka_error_msg_));
  } else {
    int64_t partition_cnt = partition_arr.count();
    rd_kafka_topic_partition_list_t *tplist = rd_kafka_topic_partition_list_new(partition_cnt);
    if (OB_ISNULL(tplist)) {
      ret = OB_KAFKA_ERROR;
      LOG_WARN("unexpected null tplist", KR(ret));
    } else {
      ARRAY_FOREACH_N(partition_arr, i, cnt) {
        rd_kafka_topic_partition_t * tp = rd_kafka_topic_partition_list_add(tplist, topic_name, partition_arr.at(i));
        if (OB_ISNULL(tp)) {
          ret = OB_KAFKA_ERROR;
          LOG_WARN("unexpected null tp", KR(ret));
        } else {
          tp->offset = offset_arr.at(i);
        }
      }
      if (OB_SUCC(ret)) {
        rd_kafka_resp_err_t err = rd_kafka_offsets_for_times(consumer_, tplist, 5000);
        if (OB_UNLIKELY(RD_KAFKA_RESP_ERR_NO_ERROR != err)) {
          if (is_kafka_error_retriable(err)) {
            ret = OB_KAFKA_NEED_RETRY;
          } else {
            ret = OB_KAFKA_ERROR;
          }
          if (OB_KAFKA_ERROR == ret) {
            copy_kafka_error_msg_(rd_kafka_err2str(err));
          }
          LOG_WARN("fail to rd kafak offsets for times", KR(ret), K(err), KCSTRING(rd_kafka_err2str(err)));
        } else if (OB_UNLIKELY(NULL == tplist
                               || partition_arr.count() != tplist->cnt)) {
          ret = OB_KAFKA_ERROR;
          LOG_WARN("fail to rd kafak offsets for times", KR(ret));
        } else {
          for (int64_t i = 0; i < tplist->cnt; i++) {
            offset_arr.at(i) = tplist->elems[i].offset;
          }
        }
      }
    }
    //clean resources
    if (NULL != tplist) {
      rd_kafka_topic_partition_list_destroy(tplist);
    }
  }
  return ret;
}

//TODO: 可能需要判断一些空指针
int ObKAFKATableRowIterator::get_next_row()
{
  int ret = OB_SUCCESS;
  int64_t returned_row_cnt = 0;
  const ExprFixedArray &file_column_exprs = *(scan_param_->ext_file_column_exprs_);
  ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
  bool is_oracle_mode = lib::is_oracle_mode();
  ObSEArray<ObCSVGeneralParser::LineErrRec, 4> error_msgs;
  ObKafkaSingleRowHandler handle_one_line(this, file_column_exprs, eval_ctx, is_oracle_mode, returned_row_cnt);
  ObSqlString parser_err_msg;
  if (state_.need_expand_buf_) {
    if (OB_FAIL(expand_buf())) {
      LOG_WARN("fail to expand buf", KR(ret));
    } else {
      state_.need_expand_buf_ = false;
    }
  }
  while (OB_SUCC(ret)
         && returned_row_cnt <= 0) {
    if (0 < state_.task_cnt_ && state_.pos_ < state_.data_end_) {
      //返回一条row出去
      int64_t nrows = 1;
      ret = parser_.scan<decltype(handle_one_line), true>(state_.pos_, state_.data_end_, nrows,
              state_.escape_buf_, state_.escape_buf_end_, handle_one_line, error_msgs);
      if (OB_SUCC(ret)) {
        //一条msg可能有多条row
        state_.row_idx_ += nrows;
        if (OB_UNLIKELY(error_msgs.count() > 0)) {
          if (OB_FAIL(handle_error_msgs_(error_msgs))) {
            LOG_WARN("fail to handle error messages", KR(ret), K(error_msgs));
          }
        }
      } else {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(parser_err_msg.append_fmt("message parser failed."
                      "This round consumed message count: %ld, parsed row index: %ld",
                      state_.msg_cnt_, state_.row_idx_))) {
          LOG_WARN("fail to append fmt", KR(tmp_ret), K(state_));
        }
        LOG_WARN("fail to scan", KR(ret), KR(tmp_ret), K(state_));
      }
    } else if (0 == state_.task_cnt_ && OB_FAIL(get_all_tasks_())) {
      LOG_WARN("fail to get all tasks", KR(ret), K(state_));
    } else if (state_.task_reach_end_) {
      if (OB_FAIL(finish_tasks_())) {
        LOG_WARN("fail to finish tasks", KR(ret), K(state_));
      } else {
        ret = OB_ITER_END;
        LOG_INFO("kafka iter end", KR(ret), K(state_));
      }
    } else {
      if (OB_FAIL(consume_from_kafka_())) {
        LOG_WARN("fail to consume from kafka", KR(ret), K(state_));
      }
    }
  }
  if (OB_SUCC(ret)) {
    for (int i = 0; OB_SUCC(ret) && i < file_column_exprs.count(); i++) {
      file_column_exprs.at(i)->set_evaluated_flag(eval_ctx);
    }
    for (int i = 0; OB_SUCC(ret) && i < column_exprs_.count(); i++) {
      ObExpr *column_expr = column_exprs_.at(i);
      ObExpr *column_convert_expr = scan_param_->ext_column_dependent_exprs_->at(i);
      ObDatum *convert_datum = NULL;
      OZ (column_convert_expr->eval(eval_ctx, convert_datum));
      if (OB_SUCC(ret)) {
        column_expr->locate_datum_for_write(eval_ctx) = *convert_datum;
        column_expr->set_evaluated_flag(eval_ctx);
      }
    }
  }
  if ((NULL != kafka_error_msg_ && strlen(kafka_error_msg_) > 0) || !parser_err_msg.empty()) {
    int tmp_ret = OB_SUCCESS;
    ObRoutineLoadTableOperator table_op;
    int64_t job_id = scan_param_->external_file_format_.kafka_format_.job_id_;
    ObString error_msg;
    if (OB_UNLIKELY(OB_SUCCESS == ret)) {
      tmp_ret = OB_ERR_UNEXPECTED;
      ret = tmp_ret;
      LOG_WARN("unexpected success", KR(tmp_ret));
    } else if (NULL != kafka_error_msg_ && strlen(kafka_error_msg_) > 0) {
      error_msg.assign_ptr(kafka_error_msg_, static_cast<int32_t>(strlen(kafka_error_msg_)));
    } else {
      error_msg = parser_err_msg.string();
    }
    if (OB_SUCCESS == tmp_ret) {
      if (OB_TMP_FAIL(table_op.init(scan_param_->tenant_id_, GCTX.sql_proxy_))) {
        LOG_WARN("failed to init table op", KR(ret), K(scan_param_->tenant_id_));
      } else if (OB_TMP_FAIL(table_op.update_error_msg(job_id, error_msg))) {
        LOG_WARN("fail to update error msg", KR(tmp_ret), K(job_id));
      }
    }
  }
  return ret;
}

int ObKAFKATableRowIterator::get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  int64_t returned_row_cnt = 0;
  const ExprFixedArray &file_column_exprs = *(scan_param_->ext_file_column_exprs_);
  ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
  bool enable_rich_format = scan_param_->op_->enable_rich_format_;
  bool is_oracle_mode = lib::is_oracle_mode();
  ObSEArray<ObCSVGeneralParser::LineErrRec, 4> error_msgs;
  ObSqlString parser_err_msg;

  if (OB_ISNULL(bit_vector_cache_)) {
    void *mem = nullptr;
    if (OB_ISNULL(mem = malloc_alloc_.alloc(ObBitVector::memory_size(eval_ctx.max_batch_size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for skip", K(ret), K(eval_ctx.max_batch_size_));
    } else {
      bit_vector_cache_ = to_bit_vector(mem);
      bit_vector_cache_->reset(eval_ctx.max_batch_size_);
    }
  }
  if (OB_SUCC(ret)) {
    if (state_.need_expand_buf_) {
      if (OB_FAIL(expand_buf())) {
        LOG_WARN("fail to expand buf", KR(ret));
      } else {
        state_.need_expand_buf_ = false;
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObKafkaBatchRowHandler handle_one_line(this, file_column_exprs, eval_ctx, is_oracle_mode, returned_row_cnt);
    while (OB_SUCC(ret)
            && returned_row_cnt <= 0) {
      if (0 < state_.task_cnt_ && state_.pos_ < state_.data_end_) {
        //返回一条row出去
        int64_t nrows = capacity;
        if (state_.has_escape_) {
          if (use_handle_batch_lines_ && enable_rich_format) {
            ret = parser_.scan<decltype(handle_one_line), true, true>(state_.pos_, state_.data_end_, nrows,
              state_.escape_buf_, state_.escape_buf_end_, handle_one_line, error_msgs);
          } else {
            ret = parser_.scan<decltype(handle_one_line), true, false>(state_.pos_, state_.data_end_, nrows,
              state_.escape_buf_, state_.escape_buf_end_, handle_one_line, error_msgs);
          }
        } else {
          if (use_handle_batch_lines_ && enable_rich_format) {
            ret = parser_.scan<decltype(handle_one_line), false, true>(state_.pos_, state_.data_end_, nrows,
              state_.escape_buf_, state_.escape_buf_end_, handle_one_line, error_msgs);
          } else {
            ret = parser_.scan<decltype(handle_one_line), false, false>(state_.pos_, state_.data_end_, nrows,
              state_.escape_buf_, state_.escape_buf_end_, handle_one_line, error_msgs);
          }
        }
        if (OB_SUCC(ret)) {
          state_.row_idx_ += nrows;
          if (nrows >= 1
              && nrows < capacity
              && !state_.task_reach_end_
              && state_.buf_len_ * 2 <= OB_MALLOC_BIG_BLOCK_SIZE) {
            //TODO: This part of the logic will not lead to
            state_.need_expand_buf_ = true;
          }
          if (OB_UNLIKELY(error_msgs.count() > 0)) {
            if (OB_FAIL(handle_error_msgs_(error_msgs))) {
              LOG_WARN("fail to handle error messages", KR(ret), K(error_msgs));
            }
          }
        } else {
          int tmp_ret = OB_SUCCESS;
          if (OB_TMP_FAIL(parser_err_msg.append_fmt("message parser failed."
                        "This round consumed message count: %ld, parsed row index: %ld",
                        state_.msg_cnt_, state_.row_idx_))) {
            LOG_WARN("fail to append fmt", KR(tmp_ret), K(state_));
          }
          LOG_WARN("fail to scan", KR(ret), KR(tmp_ret), K(state_));
        }
      } else if (0 == state_.task_cnt_ && OB_FAIL(get_all_tasks_())) {
        LOG_WARN("fail to get all tasks", KR(ret), K(state_));
      } else if (state_.task_reach_end_) {
        if (OB_FAIL(finish_tasks_())) {
          LOG_WARN("fail to finish tasks", KR(ret), K(state_));
        } else {
          ret = OB_ITER_END;
          LOG_INFO("kafka iter end", KR(ret), K(state_));
        }
      } else {
        if (OB_FAIL(consume_from_kafka_())) {
          LOG_WARN("fail to consume from kafka", KR(ret));
        }
      }
    }
    if (OB_ITER_END == ret && returned_row_cnt > 0) {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)) {
      for (int i = 0; OB_SUCC(ret) && i < file_column_exprs.count(); i++) {
        file_column_exprs.at(i)->set_evaluated_flag(eval_ctx);
      }
      if (enable_rich_format) {
        //TODO: 新的向量化2.0格式,定长用fixed, 变长用discrete
        for (int i = 0; OB_SUCC(ret) && i < column_exprs_.count(); i++) {
          ObExpr *column_expr = column_exprs_.at(i);
          ObExpr *column_convert_expr = scan_param_->ext_column_dependent_exprs_->at(i);
          OZ (column_convert_expr->eval_vector(eval_ctx, *bit_vector_cache_, returned_row_cnt, true));
          if (OB_SUCC(ret)) {
            ObExpr *to = column_exprs_.at(i);
            ObExpr *from = scan_param_->ext_column_dependent_exprs_->at(i);
            VectorHeader &to_vec_header = to->get_vector_header(eval_ctx);
            VectorHeader &from_vec_header = from->get_vector_header(eval_ctx);
            if (from_vec_header.format_ == VEC_UNIFORM_CONST) {
              ObDatum *from_datum =
                static_cast<ObUniformBase *>(from->get_vector(eval_ctx))->get_datums();
              OZ(to->init_vector(eval_ctx, VEC_UNIFORM, returned_row_cnt));
              ObUniformBase *to_vec = static_cast<ObUniformBase *>(to->get_vector(eval_ctx));
              ObDatum *to_datums = to_vec->get_datums();
              for (int64_t j = 0; j < returned_row_cnt && OB_SUCC(ret); j++) {
                to_datums[j] = *from_datum;
              }
            } else if (from_vec_header.format_ == VEC_UNIFORM) {
              ObUniformBase *uni_vec = static_cast<ObUniformBase *>(from->get_vector(eval_ctx));
              ObDatum *src = uni_vec->get_datums();
              ObDatum *dst = to->locate_batch_datums(eval_ctx);
              if (src != dst) {
                MEMCPY(dst, src, returned_row_cnt * sizeof(ObDatum));
              }
              OZ(to->init_vector(eval_ctx, VEC_UNIFORM, returned_row_cnt));
            } else if (OB_FAIL(to_vec_header.assign(from_vec_header))) {
              LOG_WARN("assign vector header failed", K(ret));
            }
            column_exprs_.at(i)->set_evaluated_projected(eval_ctx);
          }
        }
      } else {
        for (int i = 0; OB_SUCC(ret) && i < column_exprs_.count(); i++) {
          ObExpr *column_expr = column_exprs_.at(i);
          ObExpr *column_convert_expr = scan_param_->ext_column_dependent_exprs_->at(i);
          OZ (column_convert_expr->eval_batch(eval_ctx, *bit_vector_cache_, returned_row_cnt));
          if (OB_SUCC(ret)) {
            ObDatum &datum = *column_convert_expr->locate_batch_datums(eval_ctx);
            MEMCPY(column_expr->locate_batch_datums(eval_ctx),
                  column_convert_expr->locate_batch_datums(eval_ctx), sizeof(ObDatum) * returned_row_cnt);
            column_expr->set_evaluated_flag(eval_ctx);
          }
        }
      }
    }
    count = returned_row_cnt;
  }
  if ((NULL != kafka_error_msg_ && strlen(kafka_error_msg_) > 0) || !parser_err_msg.empty()) {
    int tmp_ret = OB_SUCCESS;
    ObRoutineLoadTableOperator table_op;
    int64_t job_id = scan_param_->external_file_format_.kafka_format_.job_id_;
    ObString error_msg;
    if (OB_UNLIKELY(OB_SUCCESS == ret)) {
      tmp_ret = OB_ERR_UNEXPECTED;
      ret = tmp_ret;
      LOG_WARN("unexpected success", KR(tmp_ret));
    } else if (NULL != kafka_error_msg_ && strlen(kafka_error_msg_) > 0) {
      error_msg.assign_ptr(kafka_error_msg_, static_cast<int32_t>(strlen(kafka_error_msg_)));
    } else {
      error_msg = parser_err_msg.string();
    }
    if (OB_SUCCESS == tmp_ret) {
      if (OB_TMP_FAIL(table_op.init(scan_param_->tenant_id_, GCTX.sql_proxy_))) {
        LOG_WARN("failed to init table op", KR(ret), K(scan_param_->tenant_id_));
      } else if (OB_TMP_FAIL(table_op.update_error_msg(job_id, error_msg))) {
        LOG_WARN("fail to update error msg", KR(tmp_ret), K(job_id));
      }
    }
  }
  return ret;
}

int ObKAFKATableRowIterator::finish_tasks_()
{
  int ret = OB_SUCCESS;
  // if (0 >= state_.msg_cnt_) {
  //   //do nothing
  // } else {
    const int64_t job_id = scan_param_->external_file_format_.kafka_format_.job_id_;
    if (OB_FAIL(calculate_lag_offset_())) {
      LOG_WARN("fail to caculate lag offset", KR(ret));
    } else {
      ObMySQLTransaction trans;
      ObRoutineLoadTableOperator table_op;
      ObRLoadDynamicFields dynamic_fields;
      uint64_t tenant_id = scan_param_->tenant_id_;
      bool need_update = true;
      common::ObArenaAllocator allocator;
      //TODO: tenant_id
      if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id))) {
        LOG_WARN("fail to start trans", KR(ret), K(tenant_id));
      } else if (OB_FAIL(table_op.init(tenant_id, &trans))) {
        LOG_WARN("failed to init table op", KR(ret), K(tenant_id));
      } else if (OB_FAIL(table_op.get_dynamic_fields(job_id, true /*lock row*/, allocator, dynamic_fields))) {
        LOG_WARN("failed to get dynamic fields", KR(ret), K(job_id));
      } else if (OB_FAIL(check_need_update_tmp_results_(dynamic_fields.tmp_progress_, need_update))) {
        LOG_WARN("fail to check need update tmp results", KR(ret), K(dynamic_fields.tmp_progress_), K(state_));
      } else if (need_update) {
        ObSqlString tmp_progress;
        ObSqlString tmp_lag;
        if (!dynamic_fields.tmp_progress_.empty()
            && OB_FAIL(tmp_progress.append_fmt("%.*s", dynamic_fields.tmp_progress_.length(), dynamic_fields.tmp_progress_.ptr()))) {
          LOG_WARN("failed to append existing tmp_progress", KR(ret), K(dynamic_fields.tmp_progress_));
        } else if (!dynamic_fields.tmp_lag_.empty()
            && OB_FAIL(tmp_lag.append_fmt("%.*s", dynamic_fields.tmp_lag_.length(), dynamic_fields.tmp_lag_.ptr()))) {
          LOG_WARN("failed to append existing tmp_lag", KR(ret), K(dynamic_fields.tmp_lag_));
        } else {
          typedef common::hash::ObHashMap<int32_t, std::pair<int64_t, int64_t>>::const_iterator MapIterator;
          for (MapIterator it = state_.kafka_partition_offset_map_.begin();
               OB_SUCC(ret) && it != state_.kafka_partition_offset_map_.end(); ++it) {
            int32_t kafka_partition = it->first;
            int64_t next_consume_offset = it->second.first;  // offset
            int64_t offset_lag = it->second.second;  // lag
            if (!tmp_progress.empty() && OB_FAIL(tmp_progress.append(","))) {
              LOG_WARN("failed to append comma", KR(ret));
            } else if (OB_FAIL(tmp_progress.append_fmt("%d:%ld", kafka_partition, next_consume_offset))) {
              LOG_WARN("failed to append fmt", KR(ret), K(kafka_partition), K(next_consume_offset));
            } else if (!tmp_lag.empty() && OB_FAIL(tmp_lag.append(","))) {
              LOG_WARN("failed to append comma", KR(ret));
            } else if (OB_FAIL(tmp_lag.append_fmt("%d:%ld", kafka_partition, offset_lag))) {
              LOG_WARN("failed to append fmt", KR(ret), K(kafka_partition), K(offset_lag));
            }
          }
          if (FAILEDx(table_op.update_tmp_results(job_id, tmp_progress.string(), tmp_lag.string()))) {
            LOG_WARN("failed to update tmp results", KR(ret), K(job_id), K(tmp_progress), K(tmp_lag));
          }
        }
      }
      if (trans.is_started()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
          LOG_WARN("trans end failed", "is_commit", (OB_SUCCESS == ret), KR(tmp_ret));
          ret = (OB_SUCC(ret)) ? tmp_ret : ret;
        }
      }
    }
  // }
  LOG_INFO("finish tasks", KR(ret), K(state_));
  return ret;
}

int ObKAFKATableRowIterator::check_need_update_tmp_results_(
    const ObString &tmp_progress,
    bool &need_update)
{
  int ret = OB_SUCCESS;
  need_update = true;
  int64_t duplicate_cnt = 0;
  ObString remain_str = tmp_progress;
  while (OB_SUCC(ret) && !remain_str.empty()) {
    ObString item_str = remain_str.split_on(',');
    if (item_str.empty()) {
      item_str = remain_str;
      remain_str.reset();
    }
    ObString item_partition = item_str.split_on(':');
    if (OB_UNLIKELY(item_partition.empty() || item_str.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected partition str or offset str", KR(ret), K(item_partition), K(item_str));
    } else {
      bool valid = false;
      int32_t key = ObFastAtoi<int32_t>::atoi(item_partition.ptr(), item_partition.ptr() + item_partition.length(), valid);
      if (!valid) {
        ret = OB_INVALID_DATA;
        LOG_WARN("fail to convert string to int", KR(ret), K(item_partition), K(key));
      } else {
        std::pair<int64_t, int64_t> offset_lag_pair;
        if (OB_FAIL(state_.kafka_partition_offset_map_.get_refactored(key, offset_lag_pair))) {
          if (OB_HASH_NOT_EXIST != ret) {
            LOG_WARN("fail to get from kafka_partition_offset_map_", KR(ret), K(key));
          } else {
            ret = OB_SUCCESS;
          }
        } else {
          duplicate_cnt++;
        }
      }
    }
  }
  if (OB_SUCC(ret) && duplicate_cnt > 0) {
    if (OB_UNLIKELY(state_.kafka_partition_offset_map_.size() != duplicate_cnt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected kafka_partition_offset_map_.size() != duplicate_cnt", KR(ret), K(state_.kafka_partition_offset_map_.size()), K(duplicate_cnt));
    } else {
      need_update = false;
    }
  }
  return ret;
}

int ObKAFKATableRowIterator::calculate_lag_offset_()
{
  int ret = OB_SUCCESS;
  char *topic_name = state_.topic_name_;
  if (OB_UNLIKELY(NULL == topic_name
                  || NULL == consumer_
                  || NULL == kafka_error_msg_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null topic_name or consumer_ or kafka_error_msg_", KR(ret), KP(topic_name), KP(consumer_), KP(kafka_error_msg_));
  } else if (OB_UNLIKELY(!state_.kafka_partition_offset_map_.created() || state_.kafka_partition_offset_map_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("kafka_partition_offset_map_ is not created or empty", KR(ret), K(state_));
  } else {
    typedef common::hash::ObHashMap<int32_t, std::pair<int64_t, int64_t>>::const_iterator MapIterator;
    for (MapIterator it = state_.kafka_partition_offset_map_.begin();
         OB_SUCC(ret) && it != state_.kafka_partition_offset_map_.end(); ++it) {
      int32_t kafka_partition = it->first;
      int64_t next_consume_offset = it->second.first;  // current offset
      int64_t lag_offset = RD_KAFKA_OFFSET_INVALID;  // will be calculated
      if (OB_UNLIKELY(RD_KAFKA_PARTITION_UA == kafka_partition)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected kafka_partition", KR(ret), K(kafka_partition));
      } else {
        int64_t low = RD_KAFKA_OFFSET_INVALID;
        int64_t high = RD_KAFKA_OFFSET_INVALID;
        rd_kafka_resp_err_t err = rd_kafka_query_watermark_offsets(consumer_, topic_name, kafka_partition, &low, &high, 1000);
        if (OB_UNLIKELY(RD_KAFKA_RESP_ERR_NO_ERROR != err)) {
          if (is_kafka_error_retriable(err)) {
            ret = OB_KAFKA_NEED_RETRY;
          } else {
            ret = OB_KAFKA_ERROR;
          }
          if (OB_KAFKA_ERROR == ret) {
            copy_kafka_error_msg_(rd_kafka_err2str(err));
          }
          LOG_WARN("fail to rd_kafka_query_watermark_offsets", KR(ret), K(err), K(kafka_partition), KCSTRING(rd_kafka_err2str(err)));
        } else {
          if (OB_UNLIKELY(RD_KAFKA_OFFSET_INVALID == next_consume_offset
                          || RD_KAFKA_OFFSET_INVALID == low
                          || RD_KAFKA_OFFSET_INVALID == high
                          || next_consume_offset < low)) {
            ret = OB_KAFKA_ERROR;
            LOG_WARN("unexpected kafka offset", KR(ret), K(kafka_partition), K(next_consume_offset), K(low), K(high));
          } else {
            lag_offset = high - next_consume_offset;
            if (OB_UNLIKELY(lag_offset < 0)) {
              LOG_WARN("lag_offset is negative", KR(ret), K(kafka_partition), K(low), K(high), K(next_consume_offset));
            }
          }
          if (OB_SUCC(ret)) {
            std::pair<int64_t, int64_t> new_value = std::make_pair(next_consume_offset, lag_offset);
            if (OB_FAIL(state_.kafka_partition_offset_map_.set_refactored(kafka_partition, new_value, 1))) {
              LOG_WARN("fail to update kafka partition offset map", KR(ret), K(kafka_partition), K(next_consume_offset), K(lag_offset));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObKAFKATableRowIterator::get_all_tasks_()
{
  int ret = OB_SUCCESS;
  int64_t task_cnt = 0;
  if (OB_UNLIKELY(NULL == scan_param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected scan_param_", KR(ret), KP(scan_param_));
  } else if (OB_UNLIKELY(0 >= scan_param_->scan_tasks_.count()
                         || state_.pos_ != state_.data_end_ || state_.pos_ != state_.buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected state_ or task_cnt", KR(ret), K(state_), K(scan_param_->scan_tasks_.count()));
  } else {
    state_.task_cnt_ = scan_param_->scan_tasks_.count();
    task_cnt = state_.task_cnt_;
  }
  if (OB_SUCC(ret)) {
    if (NULL == state_.topic_name_) {
      const ObString topic_str = scan_param_->external_file_format_.kafka_format_.topic_;
      if (OB_ISNULL(state_.topic_name_ = static_cast<char *>(arena_alloc_.alloc(topic_str.length() + 1)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate", KR(ret), K(topic_str.length()));
      } else {
        MEMCPY(state_.topic_name_, topic_str.ptr(), topic_str.length());
        state_.topic_name_[topic_str.length()] = '\0';
      }
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < task_cnt; i++) {
      const char delimiter = ':';
      ObString file_url = scan_param_->scan_tasks_.at(i)->file_url_;
      ObString partition_str = file_url.split_on(delimiter);
      ObString offset_str = file_url;
      bool partition_valid = false;
      bool offset_valid = false;
      int32_t kafka_partition = ObFastAtoi<int32_t>::atoi(partition_str.ptr(), partition_str.ptr() + partition_str.length(), partition_valid);
      int64_t next_consume_offset = ObFastAtoi<int64_t>::atoi(offset_str.ptr(), offset_str.ptr() + offset_str.length(), offset_valid);
      if (OB_UNLIKELY(!partition_valid || !offset_valid)) {
        ret = OB_INVALID_DATA;
        LOG_WARN("fail to convert string to int", KR(ret), K(partition_str), K(offset_str), K(state_));
      } else if (OB_FAIL(state_.kafka_partition_offset_map_.set_refactored(kafka_partition, std::make_pair(next_consume_offset, RD_KAFKA_OFFSET_INVALID), 0))) {
        LOG_WARN("fail to set kafka partition offset map", KR(ret), K(kafka_partition), K(next_consume_offset));
      }
    }
  }
  return ret;
}

int ObKAFKATableRowIterator::consume_from_kafka_()
{
  int ret = OB_SUCCESS;
  const int64_t TIMEOUT_MS = 1000;
  const ObCSVGeneralFormat &csv_format = scan_param_->external_file_format_.csv_format_;
  const int64_t MAX_BATCH_ROWS = scan_param_->external_file_format_.kafka_format_.max_batch_rows_;
  const int64_t MAX_BATCH_SIZE = scan_param_->external_file_format_.kafka_format_.max_batch_size_;
  const int64_t MAX_BATCH_INTERVAL = scan_param_->external_file_format_.kafka_format_.max_batch_interval_;
  int64_t origin_consumed_time = state_.consumed_time_;
  int64_t start_time = ObTimeUtility::current_time();
  int64_t consume_cnt = 0;
  int64_t timeout_us = 100 * 1000;
  if (OB_ISNULL(consumer_)
      || OB_ISNULL(kafka_error_msg_)
      || OB_UNLIKELY(0 >= MAX_BATCH_ROWS
                    || 0 >= MAX_BATCH_SIZE
                    || 0 >= MAX_BATCH_INTERVAL)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null consumer_ or kafka_error_msg_", KR(ret), KP(consumer_), KP(kafka_error_msg_), K(MAX_BATCH_ROWS), K(MAX_BATCH_SIZE), K(MAX_BATCH_INTERVAL));
  } else if (OB_UNLIKELY(state_.task_reach_end_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected state_.task_reach_end_", KR(ret), K(state_));
  } else if (OB_UNLIKELY(state_.pos_ != state_.data_end_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected state_.pos_ != state_.data_end_", KR(ret), KP(state_.pos_), KP(state_.data_end_));
  } else if (OB_FAIL(assign_partitions_for_consumer_())) {
    LOG_WARN("fail to assign partitions for consumer_", KR(ret));
  } else {
    state_.pos_ = state_.buf_;
    state_.data_end_ = state_.buf_;
  }
  //TODO: 下一次要读取的消息长度+已读取的总消息长度可能大于MAX_BATCH_SIZE
  while (OB_SUCC(ret)) {
    // Check for PX interruption/session kill to avoid blocking when tenant is deleted
    if (OB_FAIL(scan_param_->op_->get_eval_ctx().exec_ctx_.check_status())) {
      LOG_WARN("check status failed during get_next_rows", KR(ret));
      break;
    }
    if (state_.msg_cnt_ >= MAX_BATCH_ROWS
        || state_.consumed_bytes_ >= MAX_BATCH_SIZE
        || state_.consumed_time_ >= MAX_BATCH_INTERVAL) {
      state_.task_reach_end_ = true;
      break;
    }
    // timeout_ms = MIN(TIMEOUT_MS, (MAX_BATCH_INTERVAL - state_.consumed_time_) / 1000);
    rd_kafka_message_t *msg = rd_kafka_consumer_poll(consumer_, 0); //1s
    if (msg) {
      int64_t EXTRA_STR_LEN = csv_format.line_term_str_.length() + 1;
      if (OB_UNLIKELY(RD_KAFKA_RESP_ERR_NO_ERROR != msg->err)) {
        if (is_kafka_error_retriable(msg->err)) {
          ret = OB_KAFKA_NEED_RETRY;
        } else {
          ret = OB_KAFKA_ERROR;
        }
        if (OB_KAFKA_ERROR == ret) {
          copy_kafka_error_msg_(rd_kafka_message_errstr(msg));
        }
        LOG_WARN("fail to rd_kafka_consumer_poll", KR(ret), K(msg->err), KCSTRING(rd_kafka_message_errstr(msg)));
      } else {
        const int64_t buf_used_bytes = state_.data_end_ - state_.buf_;
        if (msg->len + EXTRA_STR_LEN > state_.buf_len_ - buf_used_bytes
            && 0 < consume_cnt) {
          if (1 == consume_cnt) {
            // state_.need_expand_buf_ = true;
            LOG_WARN("kafka message is too big", KR(ret), K(state_), K(msg->len));
          }
          rd_kafka_message_destroy(msg);
          break;
        }
        while (OB_SUCC(ret)
              && msg->len + EXTRA_STR_LEN > state_.buf_len_ - buf_used_bytes) {
          //如果buf一条消息都放不下，则扩容
          if (OB_FAIL(expand_buf())) {
            LOG_WARN("fail to expand buf", KR(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_UNLIKELY(msg->len + EXTRA_STR_LEN > state_.buf_len_ - buf_used_bytes
                          || state_.buf_ + buf_used_bytes != state_.data_end_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected state_.buf_len_", KR(ret), K(state_), K(msg->len), K(buf_used_bytes));
          } else {
            int64_t pos = 0;
            if (OB_FAIL(databuff_printf(state_.buf_ + buf_used_bytes, state_.buf_len_ - buf_used_bytes, pos, "%.*s",
                                                                        msg->len, msg->payload))) {
              LOG_WARN("fail to printf", KR(ret), K(state_), K(msg->len), KP(msg->payload));
            //添加行结束符
            } else if (OB_FAIL(databuff_printf(state_.buf_ + buf_used_bytes, state_.buf_len_ - buf_used_bytes, pos, "%.*s",
                                             csv_format.line_term_str_.length(), csv_format.line_term_str_.ptr()))) {
              LOG_WARN("fail to printf", KR(ret), K(state_), K(pos));
            } else {
              state_.data_end_ += pos;
              state_.msg_cnt_++;
              state_.consumed_time_ = origin_consumed_time + (ObTimeUtility::current_time() - start_time);
              state_.consumed_bytes_ += msg->len;
              consume_cnt++;
              std::pair<int64_t, int64_t> offset_lag_pair;
              if (OB_FAIL(state_.kafka_partition_offset_map_.get_refactored(msg->partition, offset_lag_pair))) {
                LOG_WARN("fail to get from kafka_partition_offset_map_", KR(ret), K(msg->partition));
              } else {
                offset_lag_pair.first = msg->offset + 1;
                if (OB_FAIL(state_.kafka_partition_offset_map_.set_refactored(msg->partition, offset_lag_pair, 1))) {
                  LOG_WARN("fail to set kafka_partition_offset_map_", KR(ret), K(msg->partition));
                }
              }
            }
          }
        }
      }
      rd_kafka_message_destroy(msg);
    } else {
      timeout_us = MIN(timeout_us, (MAX_BATCH_INTERVAL - state_.consumed_time_));
      ob_usleep(timeout_us);
      state_.consumed_time_ = origin_consumed_time + (ObTimeUtility::current_time() - start_time);
    }
  }
  if (OB_SUCC(ret)) {
    state_.has_escape_ = (nullptr != memchr(state_.buf_,
                          parser_.get_format().field_escaped_char_,
                          state_.data_end_ - state_.pos_));
  }
  return ret;
}

int ObKAFKATableRowIterator::assign_partitions_for_consumer_()
{
  int ret = OB_SUCCESS;
  char *topic_name = state_.topic_name_;
  if (OB_UNLIKELY(NULL == topic_name || NULL == kafka_error_msg_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null topic_name or kafka_error_msg_", KR(ret), K(state_), KP(kafka_error_msg_));
  } else if (OB_UNLIKELY(!state_.kafka_partition_offset_map_.created()
                         || state_.kafka_partition_offset_map_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("kafka_partition_offset_map_ is not created or empty", KR(ret), K(state_));
  } else {
    int64_t partition_count = state_.kafka_partition_offset_map_.size();
    rd_kafka_topic_partition_list_t *tplist = rd_kafka_topic_partition_list_new(partition_count);
    if (OB_ISNULL(tplist)) {
      ret = OB_KAFKA_ERROR;
      LOG_WARN("unexpected null tplist", KR(ret));
    } else {
      // 遍历 kafka_partition_offset_map_ 添加所有分区
      typedef common::hash::ObHashMap<int32_t, std::pair<int64_t, int64_t>>::const_iterator MapIterator;
      for (MapIterator it = state_.kafka_partition_offset_map_.begin();
           OB_SUCC(ret) && it != state_.kafka_partition_offset_map_.end(); ++it) {
        int32_t kafka_partition = it->first;
        int64_t start_offset = it->second.first;  // first is offset, second is lag
        if (RD_KAFKA_PARTITION_UA == kafka_partition || RD_KAFKA_OFFSET_INVALID == start_offset) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid kafka_partition or start_offset", KR(ret), K(kafka_partition), K(start_offset));
        } else {
          rd_kafka_topic_partition_t *tp = rd_kafka_topic_partition_list_add(tplist, topic_name, kafka_partition);
          if (OB_ISNULL(tp)) {
            ret = OB_KAFKA_ERROR;
            LOG_WARN("fail to add partition to tplist", KR(ret), K(kafka_partition));
          } else {
            tp->offset = start_offset;
          }
        }
      }
      if (OB_SUCC(ret)) {
        rd_kafka_resp_err_t kafka_err = rd_kafka_assign(consumer_, tplist);
        if (RD_KAFKA_RESP_ERR_NO_ERROR != kafka_err) {
          if (is_kafka_error_retriable(kafka_err)) {
            ret = OB_KAFKA_NEED_RETRY;
          } else {
            ret = OB_KAFKA_ERROR;
          }
          if (OB_KAFKA_ERROR == ret) {
            copy_kafka_error_msg_(rd_kafka_err2str(kafka_err));
          }
          LOG_WARN("fail to rd_kafka_assign", KR(ret), K(kafka_err), KCSTRING(rd_kafka_err2str(kafka_err)));
        }
      }
      //clean resources
      if (NULL != tplist) {
        rd_kafka_topic_partition_list_destroy(tplist);
      }
    }
  }
  return ret;
}

int ObKAFKATableRowIterator::handle_error_msgs_(
    const ObIArray<ObCSVGeneralParser::LineErrRec> &error_msgs)
{
  int ret = OB_SUCCESS;
  ObExecContext &exec_ctx = scan_param_->op_->get_eval_ctx().exec_ctx_;
  ObSQLSessionInfo* session_info = exec_ctx.get_my_session();
  ObDiagnosisManager& diagnosis_manager = exec_ctx.get_diagnosis_manager();

  if (session_info->is_diagnosis_enabled()) {
    for (int i = 0; OB_SUCC(ret) && i < error_msgs.count(); ++i) {
      int64_t line_num = error_msgs.at(i).line_no + 1 + parser_.get_format().skip_header_lines_;
      if (OB_FAIL(diagnosis_manager.missing_col_idxs_.push_back(line_num))) {
        LOG_WARN("failed to push back missing column number into array", K(ret), K(line_num));
      }
    }
  } else {
    for (int i = 0; i < error_msgs.count(); ++i) {
      LOG_WARN("parse row warning",
               "task msg cnt", state_.msg_cnt_,
               "task row idx", state_.row_idx_,
               "ret", common::ob_error_name(error_msgs.at(i).err_code));
    }
  }
  return ret;
}

int ObKafkaSingleRowHandler::operator()(const ObCSVGeneralParser::HandleOneLineParam param) {
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < file_column_exprs_.count(); ++i) {
    ObDatum &datum = file_column_exprs_.at(i)->locate_datum_for_write(eval_ctx_);
    if (file_column_exprs_.at(i)->type_ == T_PSEUDO_EXTERNAL_FILE_COL) {
      int64_t loc_idx = file_column_exprs_.at(i)->extra_ - 1;
      if (OB_UNLIKELY(loc_idx < 0 || loc_idx > param.fields_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected loc_idx", K(ret), K(loc_idx), K(param.fields_));
      } else {
        if (param.fields_.at(loc_idx).is_null_ || (0 == param.fields_.at(loc_idx).len_ && is_oracle_mode_)) {
          datum.set_null();
        } else {
          datum.set_string(param.fields_.at(loc_idx).ptr_, param.fields_.at(loc_idx).len_);
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected file column type", K(ret), K(file_column_exprs_.at(i)->type_), K(i));
    }
  }
  returned_row_cnt_++;
  return ret;
}

int ObKafkaBatchRowHandler::operator()(const ObCSVGeneralParser::HandleOneLineParam param) {
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < file_column_exprs_.count(); ++i) {
    ObDatum *datums = file_column_exprs_.at(i)->locate_batch_datums(eval_ctx_);
    if (file_column_exprs_.at(i)->type_ == T_PSEUDO_EXTERNAL_FILE_COL) {
      int64_t loc_idx = file_column_exprs_.at(i)->extra_ - 1;
      if (OB_UNLIKELY(loc_idx < 0 || loc_idx > param.fields_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected loc_idx", K(ret), K(loc_idx), K(param.fields_));
      } else {
        if (param.fields_.at(loc_idx).is_null_ || (0 == param.fields_.at(loc_idx).len_ && is_oracle_mode_)) {
          datums[returned_row_cnt_].set_null();
        } else {
          datums[returned_row_cnt_].set_string(param.fields_.at(loc_idx).ptr_, param.fields_.at(loc_idx).len_);
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected file column type", K(ret), K(file_column_exprs_.at(i)->type_), K(i));
    }
  }
  returned_row_cnt_++;
  return ret;
}

int ObKafkaBatchRowHandler::operator()(ObCSVGeneralParser::HandleBatchLinesParam param) {
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < file_column_exprs_.count() && param.batch_size_ > 0; ++i) {
    if (file_column_exprs_.at(i)->type_ == T_PSEUDO_EXTERNAL_FILE_COL) {
      StrDiscVec *string_vec = NULL;
      if (OB_FAIL(file_column_exprs_.at(i)->init_vector_for_write(eval_ctx_, VectorFormat::VEC_DISCRETE,
          eval_ctx_.max_batch_size_))) {
        LOG_WARN("init_vector failed", KR(ret));
      } else if (OB_ISNULL(string_vec =
          static_cast<StrDiscVec *>(file_column_exprs_.at(i)->get_vector(eval_ctx_)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("string vec is null", KR(ret));
      } else {
        int64_t loc_idx = file_column_exprs_.at(i)->extra_ - 1;
        if (OB_UNLIKELY(loc_idx < 0 || (loc_idx + (param.batch_size_ - 1) * param.field_cnt_) > param.fields_.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected loc_idx", KR(ret), K(loc_idx), K(param.fields_));
        } else {
          for (int j = 0; OB_SUCC(ret) && j < param.batch_size_; ++j) {
            const ObCSVGeneralParser::FieldValue& field = param.fields_.at(loc_idx);
            if (field.is_null_ || (0 == field.len_ && is_oracle_mode_)) {
              string_vec->set_null(j);
            } else {
              string_vec->set_string(j, field.ptr_, field.len_);
            }
            loc_idx += param.field_cnt_;
          }
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected file column type", K(ret), K(file_column_exprs_.at(i)->type_), K(i));
    }
  }
  returned_row_cnt_+= param.batch_size_;
  return ret;
}

DEF_TO_STRING(ObKAFKAIteratorState)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME("ob_kafka_iterator_state");
  J_COLON();
  J_KV(K(job_id_),
       K(task_cnt_),
       K(msg_cnt_),
       K(row_idx_),
       K(consumed_bytes_),
       K(consumed_time_));
  J_COMMA();
  BUF_PRINTF("kafka_partition_offset_map:");
  J_OBJ_START();
  if (kafka_partition_offset_map_.created()) {
    int64_t map_size = kafka_partition_offset_map_.size();
    BUF_PRINTF("size=%ld, ", map_size);
    BUF_PRINTF("entries:[");
    int64_t entry_idx = 0;
    typedef common::hash::ObHashMap<int32_t, std::pair<int64_t, int64_t>>::const_iterator MapIterator;
    for (MapIterator it = kafka_partition_offset_map_.begin();
         it != kafka_partition_offset_map_.end(); ++it) {
      if (entry_idx > 0) {
        J_COMMA();
      }
      BUF_PRINTF("partition=%d->(offset=%ld, lag=%ld)",
                 it->first, it->second.first, it->second.second);
      entry_idx++;
    }
    BUF_PRINTF("]");
  } else {
    BUF_PRINTF("not_created");
  }
  J_OBJ_END();
  J_COMMA();
  J_KV(K(task_reach_end_),
       K(has_escape_),
       K(need_expand_buf_),
       K(buf_len_),
       KP(buf_),
       KP(pos_),
       KP(data_end_),
       KP(escape_buf_),
       KP(escape_buf_end_),
       KCSTRING(topic_name_));
  J_OBJ_END();
  return pos;
}

void ObKAFKATableRowIterator::copy_kafka_error_msg_(const char *err_str)
{
  if (NULL != kafka_error_msg_) {
    if (OB_ISNULL(err_str)) {
      kafka_error_msg_[0] = '\0';
    } else {
      int64_t len = strlen(err_str);
      int64_t copy_len = (len < OB_MAX_ERROR_MSG_LEN - 1) ? len : (OB_MAX_ERROR_MSG_LEN - 1);
      MEMCPY(kafka_error_msg_, err_str, copy_len);
      kafka_error_msg_[copy_len] = '\0';
    }

  }
}

bool ObKAFKATableRowIterator::is_kafka_error_retriable(rd_kafka_resp_err_t err) {
  switch (err) {
    // 网络和连接相关
    case RD_KAFKA_RESP_ERR__TRANSPORT:
    case RD_KAFKA_RESP_ERR__RESOLVE:
    case RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN:
    case RD_KAFKA_RESP_ERR_NETWORK_EXCEPTION:
    case RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE:
    case RD_KAFKA_RESP_ERR__SSL:
    case RD_KAFKA_RESP_ERR__UNKNOWN_BROKER:

    // 超时相关
    case RD_KAFKA_RESP_ERR__TIMED_OUT:
    case RD_KAFKA_RESP_ERR__MSG_TIMED_OUT:
    case RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT:
    case RD_KAFKA_RESP_ERR__TIMED_OUT_QUEUE:

    // Leader/Coordinator 相关
    case RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE:
    case RD_KAFKA_RESP_ERR_NOT_LEADER_FOR_PARTITION:
    case RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE:
    case RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS:
    case RD_KAFKA_RESP_ERR_NOT_COORDINATOR:
    case RD_KAFKA_RESP_ERR_PREFERRED_LEADER_NOT_AVAILABLE:

    // Broker 可用性相关
    case RD_KAFKA_RESP_ERR_BROKER_NOT_AVAILABLE:
    case RD_KAFKA_RESP_ERR__NODE_UPDATE:

    // 资源和副本相关
    case RD_KAFKA_RESP_ERR__QUEUE_FULL:
    case RD_KAFKA_RESP_ERR__ISR_INSUFF:
    case RD_KAFKA_RESP_ERR_NOT_ENOUGH_REPLICAS:
    case RD_KAFKA_RESP_ERR_NOT_ENOUGH_REPLICAS_AFTER_APPEND:
    case RD_KAFKA_RESP_ERR_REPLICA_NOT_AVAILABLE:
    case RD_KAFKA_RESP_ERR_OFFSET_NOT_AVAILABLE:

    // 操作状态相关
    case RD_KAFKA_RESP_ERR__IN_PROGRESS:
    case RD_KAFKA_RESP_ERR__PREV_IN_PROGRESS:
    case RD_KAFKA_RESP_ERR__INTR:
    case RD_KAFKA_RESP_ERR__PARTIAL:

    // 明确的重试标记
    case RD_KAFKA_RESP_ERR__RETRY:
    case RD_KAFKA_RESP_ERR__WAIT_COORD:
    case RD_KAFKA_RESP_ERR__WAIT_CACHE:

    // 配额限制相关（可以重试，等待配额恢复）
    case RD_KAFKA_RESP_ERR_THROTTLING_QUOTA_EXCEEDED:
      return true;

    default:
      return false;
  }
}

} // end namespace sql
} // end namespace oceanbase

// #endif // OB_BUILD_KAFKA