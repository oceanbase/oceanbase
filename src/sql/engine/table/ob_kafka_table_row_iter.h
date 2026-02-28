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

#ifndef OCEANBASE_SQL_ENGINE_TABLE_OB_KAFKA_TABLE_ROW_ITER_H_
#define OCEANBASE_SQL_ENGINE_TABLE_OB_KAFKA_TABLE_ROW_ITER_H_

// #ifdef OB_BUILD_KAFKA

#include "sql/engine/table/ob_external_table_access_service.h"

_Pragma("clang diagnostic push")
_Pragma("clang diagnostic ignored \"-Wignored-qualifiers\"")
#include <librdkafka/rdkafka.h>
_Pragma("clang diagnostic pop")

namespace oceanbase
{
namespace sql
{

struct ObKAFKAIteratorState
{
  ObKAFKAIteratorState()
    : job_id_(OB_INVALID_ID),
      task_cnt_(0),
      msg_cnt_(0),
      row_idx_(0),
      consumed_bytes_(0),
      consumed_time_(0),
      kafka_partition_offset_map_(),
      task_reach_end_(false),
      has_escape_(true),
      need_expand_buf_(false),
      buf_len_(0),
      buf_(NULL),
      pos_(NULL),
      data_end_(NULL),
      escape_buf_(NULL),
      escape_buf_end_(NULL),
      topic_name_(NULL) {}
  void reuse()
  {
    job_id_ = OB_INVALID_ID;
    task_cnt_ = 0;
    msg_cnt_ = 0;
    row_idx_ = 0;
    consumed_bytes_ = 0;
    consumed_time_ = 0;
    //TODO
    kafka_partition_offset_map_.reuse();
    task_reach_end_ = false;
    has_escape_ = true;
    need_expand_buf_ = false;
    pos_ = buf_;
    data_end_ = buf_;
  }
  DECLARE_VIRTUAL_TO_STRING;
  int64_t job_id_;
  int64_t task_cnt_;
  int64_t msg_cnt_;
  int64_t row_idx_;
  int64_t consumed_bytes_;
  int64_t consumed_time_;
  // int64_t next_consume_offset_;
  // int64_t offset_lag_;
  // int32_t kafka_partition_;
  //partition and offset correspond one-to-one.
  common::hash::ObHashMap<int32_t, std::pair<int64_t, int64_t>> kafka_partition_offset_map_;
  bool task_reach_end_;
  bool has_escape_;
  bool need_expand_buf_;
  int64_t buf_len_;
  char *buf_;
  const char *pos_;
  const char *data_end_;
  char *escape_buf_;
  char *escape_buf_end_;
  char *topic_name_; //以'\0'结尾, 可直接用于kafka接口
};

/**
 * @brief Kafka Table Row Iterator
 *
 * This iterator consumes messages from Kafka topics and converts them to table rows.
 * Similar to ObODPSTableRowIterator, it provides a streaming interface for Kafka data.
 */
class ObKAFKATableRowIterator : public ObExternalTableRowIterator
{
public:
  ObKAFKATableRowIterator() :
    state_(),
    bit_vector_cache_(NULL),
    parser_(),
    arena_alloc_(),
    malloc_alloc_(),
    consumer_(NULL),
    kafka_error_msg_(NULL),
    use_handle_batch_lines_(false) {}
  ObKAFKATableRowIterator(const uint64_t tenant_id) :
    state_(),
    bit_vector_cache_(NULL),
    parser_(),
    arena_alloc_(lib::ObMemAttr(tenant_id, "KAFKARowIter")),
    malloc_alloc_(lib::ObMemAttr(tenant_id, "KAFKARowIter")),
    consumer_(NULL),
    kafka_error_msg_(NULL),
    use_handle_batch_lines_(false) {}
  virtual ~ObKAFKATableRowIterator();
public:
  virtual int init(const storage::ObTableScanParam *scan_param) override;
  virtual int get_next_row() override;
  virtual int get_next_rows(int64_t &count, int64_t capacity) override;
  virtual int get_next_row(ObNewRow *&row) override {
    UNUSED(row);
    return OB_ERR_UNEXPECTED;
  }
  virtual void reset() override;

public:
  enum TYPE {
    PARTITIONS = 0,
    OFFSETS,
    MAX,
  };

public:
  int init_consumer(const uint64_t tenant_id,
                    const int64_t job_id,
                    const ObIArray<std::pair<common::ObString, common::ObString>> &custom_properties);
  int parse_partitions_and_offsets(const uint64_t tenant_id,
                                    const int64_t job_id,
                                    const ObString topic_name,
                                    const ObString &p_str,
                                    const ObString &o_str,
                                    ObIArray<int32_t> &partition_arr,
                                    ObIArray<int64_t> &offset_arr);
  int parser_partitions(const ObString topic_name, const ObString &p_str, ObIArray<int32_t> &partition_arr);
  int parser_offsets(const ObString &o_str, ObIArray<int64_t> &offset_arr);
  int expand_buf();
  static int split_partitions(const ObString &p_str, ObIArray<int32_t> &partition_arr);
  static int split_offsets(const ObString &o_str, ObIArray<int64_t> &offset_arr, bool &is_datetime);

private:
  int get_all_partitions_from_source_(const char *topic_name, ObIArray<int32_t> &partition_arr);
  int convert_special_offsets_(const char *topic_name, const ObIArray<int32_t> &partition_arr, ObIArray<int64_t> &offset_arr);
  int parser_datetime_offsets_(const char *topic_name, const ObIArray<int32_t> &partition_arr, ObIArray<int64_t> &offset_arr);
  int finish_tasks_();
  int check_need_update_tmp_results_(const ObString &tmp_progress, bool &need_update);
  int calculate_lag_offset_();
  int get_next_task_();
  int get_all_tasks_();
  int validate_and_reset_task_state();
  int consume_from_kafka_();
  int assign_partitions_for_consumer_();
  int handle_error_msgs_(const ObIArray<ObCSVGeneralParser::LineErrRec> &error_msgs);
  void clear_state_buf_();
  bool is_kafka_error_retriable(rd_kafka_resp_err_t err);
  void copy_kafka_error_msg_(const char *err_str);

private:
  ObKAFKAIteratorState state_;
  ObBitVector *bit_vector_cache_;
  ObCSVGeneralParser parser_;
  ObArenaAllocator arena_alloc_;
  common::ObMalloc malloc_alloc_;
  rd_kafka_t *consumer_;
  char *kafka_error_msg_;
  bool use_handle_batch_lines_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObKAFKATableRowIterator);
};

//TODO: bad file record
class ObKafkaSingleRowHandler {
  public:
  ObKafkaSingleRowHandler(ObKAFKATableRowIterator *kafka_iter,
                          const ExprFixedArray &file_column_exprs,
                          ObEvalCtx &eval_ctx,
                          bool is_oracle_mode,
                          int64_t &returned_row_cnt)
      : kafka_iter_(kafka_iter),
        file_column_exprs_(file_column_exprs),
        eval_ctx_(eval_ctx),
        is_oracle_mode_(is_oracle_mode),
        returned_row_cnt_(returned_row_cnt) {}
  int operator()(const ObCSVGeneralParser::HandleOneLineParam param);
  int operator()(ObCSVGeneralParser::HandleBatchLinesParam param) {
    UNUSED(param);
    return OB_SUCCESS;
  }
private:
  ObKAFKATableRowIterator *kafka_iter_;
  const ExprFixedArray &file_column_exprs_;
  ObEvalCtx &eval_ctx_;
  bool is_oracle_mode_;
  int64_t &returned_row_cnt_;
};

class ObKafkaBatchRowHandler {
  public:
  ObKafkaBatchRowHandler(ObKAFKATableRowIterator *kafka_iter,
                        const ExprFixedArray &file_column_exprs,
                        ObEvalCtx &eval_ctx,
                        bool is_oracle_mode,
                        int64_t &returned_row_cnt)
      : kafka_iter_(kafka_iter),
        file_column_exprs_(file_column_exprs),
        eval_ctx_(eval_ctx),
        is_oracle_mode_(is_oracle_mode),
        returned_row_cnt_(returned_row_cnt) {}
  int operator()(const ObCSVGeneralParser::HandleOneLineParam param);
  int operator()(ObCSVGeneralParser::HandleBatchLinesParam param);
private:
  ObKAFKATableRowIterator *kafka_iter_;
  const ExprFixedArray &file_column_exprs_;
  ObEvalCtx &eval_ctx_;
  bool is_oracle_mode_;
  int64_t &returned_row_cnt_;
};

} // end namespace sql
} // end namespace oceanbase

// #endif // OB_BUILD_KAFKA

#endif // OCEANBASE_SQL_ENGINE_TABLE_OB_KAFKA_TABLE_ROW_ITER_H_