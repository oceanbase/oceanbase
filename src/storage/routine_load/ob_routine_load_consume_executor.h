/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_ROUTINE_LOAD_CONSUME_EXECUTOR_H_
#define OCEANBASE_ROUTINE_LOAD_CONSUME_EXECUTOR_H_

#include "storage/routine_load/ob_routine_load_table_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
} // namespace sql

namespace storage
{
struct ObRoutineLoadConsumeArg
{
public:
  ObRoutineLoadConsumeArg() :
      job_id_(OB_INVALID_ID),
      par_str_pos_(-1),
      off_str_pos_(-1),
      par_str_len_(0),
      off_str_len_(0) {}
public:
  bool is_valid() const { return OB_INVALID_ID != job_id_
                                 && !job_name_.empty()
                                 && !exec_sql_.empty(); }
  int64_t job_id_;
  uint32_t par_str_pos_; //the position of the substring "kafka_partitions" in the string "exec_sql"
  uint32_t off_str_pos_; //the position of the substring "kafka_offsets" in the string "exec_sql"
  uint32_t par_str_len_; //the length of the substring "kafka_partitions" in the string "exec_sql"
  uint32_t off_str_len_; //the length of the substring "kafka_offsets" in the string "exec_sql"
  ObString job_name_;
  ObString exec_sql_;
  TO_STRING_KV(K_(job_id), K_(par_str_pos),
               K_(off_str_pos), K_(par_str_len),
               K_(off_str_len), K_(job_name), K_(exec_sql));
};

class ObRoutineLoadConsumeExecutor
{
public:
  ObRoutineLoadConsumeExecutor();
  int execute(sql::ObExecContext &ctx, const ObRoutineLoadConsumeArg &arg);
private:
  int check_and_parser_(ObArenaAllocator &allocator,
                        const ObRLoadDynamicFields &dynamic_fields,
                        ObString &new_kafka_partitions,
                        ObString &new_kafka_offsets);
  int parser_and_check_tmp_results_(ObArenaAllocator &allocator,
                                    const ObRLoadDynamicFields &dynamic_fields,
                                    ObString &new_kafka_partitions,
                                    ObString &new_kafka_offsets,
                                    int64_t &data_cnt);
  int generate_new_exec_sql_(ObArenaAllocator &allocator,
                             const ObRoutineLoadConsumeArg &arg,
                             const ObString &new_kafka_partitions,
                             const ObString &new_kafka_offsets,
                             ObString &new_exec_sql,
                             uint32_t &new_par_str_pos,
                             uint32_t &new_off_str_pos);
private:
  sql::ObExecContext *ctx_;
  uint64_t tenant_id_;
private:
  DISABLE_COPY_ASSIGN(ObRoutineLoadConsumeExecutor);
};

} // namespace storage
} // namespace oceanbase

#endif
