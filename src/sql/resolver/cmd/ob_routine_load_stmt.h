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

#ifndef OCEANBASE_SQL_OB_ROUTINE_LOAD_STMT_H_
#define OCEANBASE_SQL_OB_ROUTINE_LOAD_STMT_H_

#include "sql/resolver/cmd/ob_cmd_stmt.h"
#include "sql/resolver/dml/ob_hint.h"
#include "sql/resolver/cmd/ob_load_data_stmt.h"

namespace oceanbase
{
namespace sql
{

class ObCreateRoutineLoadStmt : public ObCMDStmt
{
public:
  // explicit ObCreateRoutineLoadStmt(common::ObIAllocator *name_pool)
  //   : ObCMDStmt(name_pool, stmt::T_CREATE_ROUTINE_LOAD),
  //     job_name_(),

  //     {}
  ObCreateRoutineLoadStmt()
    : ObCMDStmt(stmt::T_CREATE_ROUTINE_LOAD),
      tenant_id_(OB_INVALID_TENANT_ID),
      database_id_(OB_INVALID_ID),
      table_id_(OB_INVALID_ID),
      parallel_(0),
      max_batch_interval_s_(60),
      // par_str_pos_(-1),
      // off_str_pos_(-1),
      // par_str_len_(0),
      // off_str_len_(0),
      dupl_action_(ObLoadDupActionType::LOAD_STOP_ON_DUP),
      field_or_var_list_(),
      part_ids_(),
      part_names_(),
      kafka_custom_properties_()
      {}
  virtual ~ObCreateRoutineLoadStmt() {}

  OB_INLINE void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  OB_INLINE void set_database_id(const uint64_t database_id) { database_id_ = database_id; }
  OB_INLINE void set_table_id(const uint64_t table_id) { table_id_ = table_id; }
  OB_INLINE void set_parallel(const uint64_t parallel) { parallel_ = parallel; }
  OB_INLINE void set_max_batch_interval_s(const int64_t max_batch_interval_s) { max_batch_interval_s_ = max_batch_interval_s; }
  // OB_INLINE void set_partition_str_pos(const uint32_t par_str_pos) { par_str_pos_ = par_str_pos; }
  // OB_INLINE void set_offsets_str_pos(const uint32_t off_str_pos) { off_str_pos_ = off_str_pos; }
  // OB_INLINE void set_partition_str_len(const uint32_t par_str_len) { par_str_len_ = par_str_len; }
  // OB_INLINE void set_offsets_str_len(const uint32_t off_str_len) { off_str_len_ = off_str_len; }
  OB_INLINE void set_dupl_action(const ObLoadDupActionType dupl_action) { dupl_action_ = dupl_action; }
  OB_INLINE void set_job_name(const ObString &job_name) { job_name_ = job_name; }
  OB_INLINE void set_database_name(const ObString &database_name) { database_name_ = database_name; }
  OB_INLINE void set_table_name(const ObString &table_name) { table_name_ = table_name; }
  OB_INLINE void set_combined_name(const char *buf, const int64_t len) { combined_name_.assign_ptr(buf, len); }
  OB_INLINE void set_where_clause(const ObString &where_clause) { where_clause_ = where_clause; }
  OB_INLINE void set_kafka_partitions_str(const ObString &partitions_str) { kafka_partitions_str_ = partitions_str; }
  OB_INLINE void set_kafka_offsets_str(const ObString &offsets_str) { kafka_offsets_str_ = offsets_str; }
  OB_INLINE void set_topic_name(const ObString &topic_name) { topic_name_ = topic_name; }
  OB_INLINE void set_job_prop_sql_str(const char *buf, const int64_t len) { job_properties_sql_str_.assign_ptr(buf, len); }
  OB_INLINE void set_job_prop_json_str(const char *buf, const int64_t len) { job_properties_json_str_.assign_ptr(buf, len); }
  OB_INLINE void set_exec_env(const ObString &exec_env) { exec_env_ = exec_env; }

  OB_INLINE uint64_t get_tenant_id() const { return tenant_id_; }
  OB_INLINE uint64_t get_database_id() const { return database_id_; }
  OB_INLINE uint64_t get_table_id() const { return table_id_; }
  OB_INLINE int64_t get_parallel() const { return parallel_; }
  OB_INLINE int64_t get_max_batch_interval_s() const { return max_batch_interval_s_; }
  // OB_INLINE uint32_t get_partition_str_pos() const { return par_str_pos_; }
  // OB_INLINE uint32_t get_offsets_str_pos() const { return off_str_pos_; }
  // OB_INLINE uint32_t get_partition_str_len() const { return par_str_len_; }
  // OB_INLINE uint32_t get_offsets_str_len() const { return off_str_len_; }
  OB_INLINE ObLoadDupActionType get_dupl_action() const { return dupl_action_; }
  OB_INLINE const ObString &get_job_name() const { return job_name_; }
  OB_INLINE const ObString &get_database_name() const { return database_name_; }
  OB_INLINE const ObString &get_table_name() const { return table_name_; }
  OB_INLINE const ObString &get_combined_name() const { return combined_name_; }
  OB_INLINE const ObString &get_where_clause() const { return where_clause_; }
  OB_INLINE const ObString &get_kafka_partitions_str() const { return kafka_partitions_str_; }
  OB_INLINE const ObString &get_kafka_offsets_str() const { return kafka_offsets_str_; }
  OB_INLINE const ObString &get_topic_name() const { return topic_name_; }
  OB_INLINE const ObString &get_job_prop_sql_str() const { return job_properties_sql_str_; }
  OB_INLINE const ObString &get_job_prop_json_str() const { return job_properties_json_str_; }
  OB_INLINE const ObString &get_exec_env() const { return exec_env_; }
  OB_INLINE const common::ObIArray<FieldOrVarStruct> &get_field_or_var_list() const { return field_or_var_list_; }
  OB_INLINE common::ObIArray<FieldOrVarStruct> &get_field_or_var_list() { return field_or_var_list_; }
  OB_INLINE const common::ObIArray<ObObjectID> &get_part_ids() const { return part_ids_; }
  OB_INLINE common::ObIArray<ObObjectID> &get_part_ids() { return part_ids_; }
  OB_INLINE const common::ObIArray<ObString> &get_part_names() const { return part_names_; }
  OB_INLINE common::ObIArray<ObString> &get_part_names() { return part_names_; }
  OB_INLINE const common::ObIArray<std::pair<common::ObString, common::ObString>> &get_kafka_custom_properties() const { return kafka_custom_properties_; }
  OB_INLINE common::ObIArray<std::pair<common::ObString, common::ObString>> &get_kafka_custom_properties() { return kafka_custom_properties_; }

  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_),
               K(tenant_id_),
               K(database_id_),
               K(table_id_),
               K(parallel_),
               K(max_batch_interval_s_),
              //  K(par_str_pos_),
              //  K(off_str_pos_),
              //  K(par_str_len_),
              //  K(off_str_len_),
               K(dupl_action_),
               K(job_name_),
               K(database_name_),
               K(table_name_),
               K(combined_name_),
               K(where_clause_),
               K(kafka_partitions_str_),
               K(kafka_offsets_str_),
               K(topic_name_),
               K(job_properties_sql_str_),
               K(job_properties_json_str_),
               K(exec_env_),
               K(field_or_var_list_),
               K(part_ids_),
               K(part_names_),
               K(kafka_custom_properties_));

private:
  //TODO:check string length, ObFixedLengthString
  uint64_t tenant_id_;
  uint64_t database_id_;
  uint64_t table_id_;
  int64_t parallel_;
  int64_t max_batch_interval_s_;
  // uint32_t par_str_pos_; //the position of the substring "kafka_partitions" in the string "exec_sql"
  // uint32_t off_str_pos_; //the position of the substring "kafka_offsets" in the string "exec_sql"
  // uint32_t par_str_len_; //the length of the substring "kafka_partitions" in the string "exec_sql"
  // uint32_t off_str_len_; //the length of the substring "kafka_offsets" in the string "exec_sql"
  ObLoadDupActionType dupl_action_;
  common::ObString job_name_;
  common::ObString database_name_;
  common::ObString table_name_;
  common::ObString combined_name_;
  common::ObString where_clause_;
  common::ObString kafka_partitions_str_;
  common::ObString kafka_offsets_str_;
  common::ObString topic_name_;
  common::ObString job_properties_sql_str_;
  common::ObString job_properties_json_str_;
  common::ObString exec_env_;
  common::ObSEArray<FieldOrVarStruct, 4> field_or_var_list_;
  common::ObArray<ObObjectID> part_ids_;
  common::ObArray<ObString> part_names_;
  common::ObSEArray<std::pair<common::ObString, common::ObString>, 16> kafka_custom_properties_;
  DISALLOW_COPY_AND_ASSIGN(ObCreateRoutineLoadStmt);
};

class ObPauseRoutineLoadStmt : public ObCMDStmt
{
public:
  ObPauseRoutineLoadStmt()
    : ObCMDStmt(stmt::T_PAUSE_ROUTINE_LOAD),
      tenant_id_(OB_INVALID_TENANT_ID),
      job_name_()
      {}
  virtual ~ObPauseRoutineLoadStmt() {}

  OB_INLINE void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  OB_INLINE void set_job_name(const ObString &job_name) { job_name_ = job_name; }

  OB_INLINE uint64_t get_tenant_id() const { return tenant_id_; }
  OB_INLINE const ObString &get_job_name() const { return job_name_; }

  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_),
               K(tenant_id_),
               K(job_name_));

private:
  uint64_t tenant_id_;
  common::ObString job_name_;
  DISALLOW_COPY_AND_ASSIGN(ObPauseRoutineLoadStmt);
};

class ObResumeRoutineLoadStmt : public ObCMDStmt
{
public:
  ObResumeRoutineLoadStmt()
    : ObCMDStmt(stmt::T_RESUME_ROUTINE_LOAD),
      tenant_id_(OB_INVALID_TENANT_ID),
      job_name_()
      {}
  virtual ~ObResumeRoutineLoadStmt() {}

  OB_INLINE void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  OB_INLINE void set_job_name(const ObString &job_name) { job_name_ = job_name; }

  OB_INLINE uint64_t get_tenant_id() const { return tenant_id_; }
  OB_INLINE const ObString &get_job_name() const { return job_name_; }

  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_),
               K(tenant_id_),
               K(job_name_));

private:
  uint64_t tenant_id_;
  common::ObString job_name_;
  DISALLOW_COPY_AND_ASSIGN(ObResumeRoutineLoadStmt);
};

class ObStopRoutineLoadStmt : public ObCMDStmt
{
public:
  ObStopRoutineLoadStmt()
    : ObCMDStmt(stmt::T_STOP_ROUTINE_LOAD),
      tenant_id_(OB_INVALID_TENANT_ID),
      job_name_()
      {}
  virtual ~ObStopRoutineLoadStmt() {}

  OB_INLINE void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  OB_INLINE void set_job_name(const ObString &job_name) { job_name_ = job_name; }

  OB_INLINE uint64_t get_tenant_id() const { return tenant_id_; }
  OB_INLINE const ObString &get_job_name() const { return job_name_; }

  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_),
               K(tenant_id_),
               K(job_name_));

private:
  uint64_t tenant_id_;
  common::ObString job_name_;
  DISALLOW_COPY_AND_ASSIGN(ObStopRoutineLoadStmt);
};

}
}

#endif //OCEANBASE_SQL_OB_ROUTINE_LOAD_STMT_H_
