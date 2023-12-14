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

#pragma once

#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace share
{
class ObDMLSqlSplicer;
namespace schema
{
struct ObMViewRefreshStatsRecordId
{
public:
  ObMViewRefreshStatsRecordId()
    : refresh_id_(OB_INVALID_ID), mview_id_(OB_INVALID_ID), retry_id_(OB_INVALID_ID)
  {
  }
  bool is_valid() const
  {
    return OB_INVALID_ID != refresh_id_ && OB_INVALID_ID != mview_id_ && OB_INVALID_ID != retry_id_;
  }
  TO_STRING_KV(K_(refresh_id), K_(mview_id), K_(retry_id));

public:
  int64_t refresh_id_;
  uint64_t mview_id_;
  int64_t retry_id_;
};

class ObMViewRefreshRunStats : public ObSchema
{
  OB_UNIS_VERSION(1);

public:
  ObMViewRefreshRunStats();
  explicit ObMViewRefreshRunStats(common::ObIAllocator *allocator);
  ObMViewRefreshRunStats(const ObMViewRefreshRunStats &src_schema);
  virtual ~ObMViewRefreshRunStats();

  ObMViewRefreshRunStats &operator=(const ObMViewRefreshRunStats &src_schema);
  int assign(const ObMViewRefreshRunStats &other);

  bool is_valid() const override;
  void reset() override;
  int64_t get_convert_size() const override;

#define DEFINE_GETTER_AND_SETTER(type, name)            \
  OB_INLINE type get_##name() const { return name##_; } \
  OB_INLINE void set_##name(type name) { name##_ = name; }

#define DEFINE_STRING_GETTER_AND_SETTER(name)                      \
  OB_INLINE const ObString &get_##name() const { return name##_; } \
  OB_INLINE int set_##name(const ObString &name) { return deep_copy_str(name, name##_); }

  DEFINE_GETTER_AND_SETTER(uint64_t, tenant_id);
  DEFINE_GETTER_AND_SETTER(int64_t, refresh_id);
  DEFINE_GETTER_AND_SETTER(uint64_t, run_user_id);
  DEFINE_GETTER_AND_SETTER(int64_t, num_mvs_total);
  DEFINE_GETTER_AND_SETTER(int64_t, num_mvs_current);
  DEFINE_STRING_GETTER_AND_SETTER(mviews);
  DEFINE_STRING_GETTER_AND_SETTER(base_tables);
  DEFINE_STRING_GETTER_AND_SETTER(method);
  DEFINE_STRING_GETTER_AND_SETTER(rollback_seg);
  DEFINE_GETTER_AND_SETTER(bool, push_deferred_rpc);
  DEFINE_GETTER_AND_SETTER(bool, refresh_after_errors);
  DEFINE_GETTER_AND_SETTER(int, purge_option);
  DEFINE_GETTER_AND_SETTER(int, parallelism);
  DEFINE_GETTER_AND_SETTER(int, heap_size);
  DEFINE_GETTER_AND_SETTER(bool, atomic_refresh);
  DEFINE_GETTER_AND_SETTER(bool, nested);
  DEFINE_GETTER_AND_SETTER(bool, out_of_place);
  DEFINE_GETTER_AND_SETTER(int64_t, number_of_failures);
  DEFINE_GETTER_AND_SETTER(int64_t, start_time);
  DEFINE_GETTER_AND_SETTER(int64_t, end_time);
  DEFINE_GETTER_AND_SETTER(int64_t, elapsed_time);
  DEFINE_GETTER_AND_SETTER(int64_t, log_purge_time);
  DEFINE_GETTER_AND_SETTER(bool, complete_stats_avaliable);
  DEFINE_STRING_GETTER_AND_SETTER(trace_id);

#undef DEFINE_GETTER_AND_SETTER
#undef DEFINE_STRING_GETTER_AND_SETTER

  int gen_insert_run_stats_dml(uint64_t exec_tenant_id, share::ObDMLSqlSplicer &dml) const;
  static int insert_run_stats(ObISQLClient &sql_client, const ObMViewRefreshRunStats &run_stats);
  static int dec_num_mvs_current(ObISQLClient &sql_client, uint64_t tenant_id, int64_t refresh_id,
                                 int64_t dec_val);
  static int drop_all_run_stats(ObISQLClient &sql_client, uint64_t tenant_id,
                                int64_t &affected_rows, int64_t limit = -1);
  static int drop_empty_run_stats(ObISQLClient &sql_client, uint64_t tenant_id,
                                  int64_t &affected_rows, int64_t limit = -1);

  TO_STRING_KV(K_(tenant_id),
               K_(refresh_id),
               K_(run_user_id),
               K_(num_mvs_total),
               K_(num_mvs_current),
               K_(mviews),
               K_(base_tables),
               K_(method),
               K_(rollback_seg),
               K_(push_deferred_rpc),
               K_(refresh_after_errors),
               K_(purge_option),
               K_(parallelism),
               K_(heap_size),
               K_(atomic_refresh),
               K_(nested),
               K_(out_of_place),
               K_(number_of_failures),
               K_(start_time),
               K_(end_time),
               K_(elapsed_time),
               K_(log_purge_time),
               K_(complete_stats_avaliable),
               K_(trace_id));

public:
  uint64_t tenant_id_;
  int64_t refresh_id_;
  uint64_t run_user_id_;
  int64_t num_mvs_total_;
  int64_t num_mvs_current_;
  ObString mviews_;
  ObString base_tables_;
  ObString method_;
  ObString rollback_seg_;
  bool push_deferred_rpc_;
  bool refresh_after_errors_;
  int purge_option_;
  int parallelism_;
  int heap_size_;
  bool atomic_refresh_;
  bool nested_;
  bool out_of_place_;
  int64_t number_of_failures_;
  int64_t start_time_;
  int64_t end_time_;
  int64_t elapsed_time_;
  int64_t log_purge_time_;
  bool complete_stats_avaliable_;
  ObString trace_id_;
};

class ObMViewRefreshStats : public ObSchema
{
  OB_UNIS_VERSION(1);
  static const int UNEXECUTED_STATUS = 1;

public:
  ObMViewRefreshStats();
  explicit ObMViewRefreshStats(common::ObIAllocator *allocator);
  ObMViewRefreshStats(const ObMViewRefreshStats &src_schema);
  virtual ~ObMViewRefreshStats();

  ObMViewRefreshStats &operator=(const ObMViewRefreshStats &src_schema);
  int assign(const ObMViewRefreshStats &other);

  bool is_valid() const override;
  void reset() override;
  int64_t get_convert_size() const override;

#define DEFINE_GETTER_AND_SETTER(type, name)            \
  OB_INLINE type get_##name() const { return name##_; } \
  OB_INLINE void set_##name(type name) { name##_ = name; }

  DEFINE_GETTER_AND_SETTER(uint64_t, tenant_id);
  DEFINE_GETTER_AND_SETTER(int64_t, refresh_id);
  DEFINE_GETTER_AND_SETTER(uint64_t, mview_id);
  DEFINE_GETTER_AND_SETTER(int64_t, retry_id);
  DEFINE_GETTER_AND_SETTER(share::schema::ObMVRefreshType, refresh_type);
  DEFINE_GETTER_AND_SETTER(int64_t, start_time);
  DEFINE_GETTER_AND_SETTER(int64_t, end_time);
  DEFINE_GETTER_AND_SETTER(int64_t, elapsed_time);
  DEFINE_GETTER_AND_SETTER(int64_t, log_purge_time);
  DEFINE_GETTER_AND_SETTER(int64_t, initial_num_rows);
  DEFINE_GETTER_AND_SETTER(int64_t, final_num_rows);
  DEFINE_GETTER_AND_SETTER(int64_t, num_steps);
  DEFINE_GETTER_AND_SETTER(int, result);

#undef DEFINE_GETTER_AND_SETTER

  int gen_insert_refresh_stats_dml(uint64_t tenant_id, share::ObDMLSqlSplicer &dml) const;
  static int insert_refresh_stats(ObISQLClient &sql_client,
                                  const ObMViewRefreshStats &refresh_stats);
  static int drop_all_refresh_stats(ObISQLClient &sql_client, uint64_t tenant_id,
                                    int64_t &affected_rows, int64_t limit = -1);
  static int drop_refresh_stats_record(ObISQLClient &sql_client, uint64_t tenant_id,
                                       const ObMViewRefreshStatsRecordId &record_id);

  class FilterParam
  {
  public:
    FilterParam() : mview_id_(OB_INVALID_ID), retention_period_(0), flag_(0) {}

#define DEFINE_GETTER_AND_SETTER(type, name)                  \
  OB_INLINE bool has_##name() const { return has_##name##_; } \
  OB_INLINE type get_##name() const { return name##_; }       \
  OB_INLINE void set_##name(type name)                        \
  {                                                           \
    name##_ = name;                                           \
    has_##name##_ = true;                                     \
  }

    DEFINE_GETTER_AND_SETTER(uint64_t, mview_id);
    DEFINE_GETTER_AND_SETTER(int64_t, retention_period);

#undef DEFINE_GETTER_AND_SETTER

    bool has_no_flag() const { return flag_ == 0; }
    bool has_any_flag() const { return flag_ != 0; }
    bool is_valid() const
    {
      return (!has_mview_id_ || OB_INVALID_ID != mview_id_) &&
             (!has_retention_period_ || retention_period_ > 0);
    }
    TO_STRING_KV(K_(mview_id), K_(retention_period), K_(has_mview_id), K_(has_retention_period));

  private:
    uint64_t mview_id_;
    int64_t retention_period_;
    union
    {
      uint64_t flag_;
      struct
      {
        uint64_t has_mview_id_ : 1;
        uint64_t has_retention_period_ : 1;
        uint64_t reserved : 62;
      };
    };
  };

  static int collect_record_ids(ObISQLClient &sql_client, uint64_t tenant_id,
                                const FilterParam &filter_param,
                                ObIArray<ObMViewRefreshStatsRecordId> &record_ids,
                                int64_t limit = -1);

  TO_STRING_KV(K_(tenant_id),
               K_(refresh_id),
               K_(mview_id),
               K_(retry_id),
               K_(refresh_type),
               K_(start_time),
               K_(end_time),
               K_(elapsed_time),
               K_(log_purge_time),
               K_(initial_num_rows),
               K_(final_num_rows),
               K_(num_steps),
               K_(result));

public:
  uint64_t tenant_id_;
  int64_t refresh_id_;
  uint64_t mview_id_;
  int64_t retry_id_;
  share::schema::ObMVRefreshType refresh_type_;
  int64_t start_time_;
  int64_t end_time_;
  int64_t elapsed_time_;
  int64_t log_purge_time_;
  int64_t initial_num_rows_;
  int64_t final_num_rows_;
  int64_t num_steps_;
  int result_;
};

class ObMViewRefreshChangeStats : public ObSchema
{
  OB_UNIS_VERSION(1);

public:
  ObMViewRefreshChangeStats();
  explicit ObMViewRefreshChangeStats(common::ObIAllocator *allocator);
  ObMViewRefreshChangeStats(const ObMViewRefreshChangeStats &src_schema);
  virtual ~ObMViewRefreshChangeStats();

  ObMViewRefreshChangeStats &operator=(const ObMViewRefreshChangeStats &src_schema);
  int assign(const ObMViewRefreshChangeStats &other);

  bool is_valid() const override;
  void reset() override;
  int64_t get_convert_size() const override;

#define DEFINE_GETTER_AND_SETTER(type, name)            \
  OB_INLINE type get_##name() const { return name##_; } \
  OB_INLINE void set_##name(type name) { name##_ = name; }

  DEFINE_GETTER_AND_SETTER(uint64_t, tenant_id);
  DEFINE_GETTER_AND_SETTER(int64_t, refresh_id);
  DEFINE_GETTER_AND_SETTER(uint64_t, mview_id);
  DEFINE_GETTER_AND_SETTER(int64_t, retry_id);
  DEFINE_GETTER_AND_SETTER(uint64_t, detail_table_id);
  DEFINE_GETTER_AND_SETTER(int64_t, num_rows_ins);
  DEFINE_GETTER_AND_SETTER(int64_t, num_rows_upd);
  DEFINE_GETTER_AND_SETTER(int64_t, num_rows_del);
  DEFINE_GETTER_AND_SETTER(int64_t, num_rows);

#undef DEFINE_GETTER_AND_SETTER

  int gen_insert_change_stats_dml(uint64_t exec_tenant_id, share::ObDMLSqlSplicer &dml) const;
  static int insert_change_stats(ObISQLClient &sql_client,
                                 const ObMViewRefreshChangeStats &change_stats);
  static int drop_all_change_stats(ObISQLClient &sql_client, uint64_t tenant_id,
                                   int64_t &affected_rows, int64_t limit = -1);
  static int drop_change_stats_record(ObISQLClient &sql_client, uint64_t tenant_id,
                                      const ObMViewRefreshStatsRecordId &record_id);

  TO_STRING_KV(K_(tenant_id),
               K_(refresh_id),
               K_(mview_id),
               K_(retry_id),
               K_(detail_table_id),
               K_(num_rows_ins),
               K_(num_rows_upd),
               K_(num_rows_del),
               K_(num_rows));

public:
  uint64_t tenant_id_;
  int64_t refresh_id_;
  uint64_t mview_id_;
  int64_t retry_id_;
  uint64_t detail_table_id_;
  int64_t num_rows_ins_;
  int64_t num_rows_upd_;
  int64_t num_rows_del_;
  int64_t num_rows_;
};

class ObMViewRefreshStmtStats : public ObSchema
{
  OB_UNIS_VERSION(1);
  static const int UNEXECUTED_RESULT = 1;

public:
  ObMViewRefreshStmtStats();
  explicit ObMViewRefreshStmtStats(common::ObIAllocator *allocator);
  ObMViewRefreshStmtStats(const ObMViewRefreshStmtStats &src_schema);
  virtual ~ObMViewRefreshStmtStats();

  ObMViewRefreshStmtStats &operator=(const ObMViewRefreshStmtStats &src_schema);
  int assign(const ObMViewRefreshStmtStats &other);

  bool is_valid() const override;
  void reset() override;
  int64_t get_convert_size() const override;

#define DEFINE_GETTER_AND_SETTER(type, name)            \
  OB_INLINE type get_##name() const { return name##_; } \
  OB_INLINE void set_##name(type name) { name##_ = name; }

#define DEFINE_STRING_GETTER_AND_SETTER(name)                      \
  OB_INLINE const ObString &get_##name() const { return name##_; } \
  OB_INLINE int set_##name(const ObString &name) { return deep_copy_str(name, name##_); }

  DEFINE_GETTER_AND_SETTER(uint64_t, tenant_id);
  DEFINE_GETTER_AND_SETTER(int64_t, refresh_id);
  DEFINE_GETTER_AND_SETTER(uint64_t, mview_id);
  DEFINE_GETTER_AND_SETTER(int64_t, retry_id);
  DEFINE_GETTER_AND_SETTER(int64_t, step);
  DEFINE_STRING_GETTER_AND_SETTER(sql_id);
  DEFINE_STRING_GETTER_AND_SETTER(stmt);
  DEFINE_GETTER_AND_SETTER(int64_t, execution_time);
  DEFINE_STRING_GETTER_AND_SETTER(execution_plan);
  DEFINE_GETTER_AND_SETTER(int, result);

#undef DEFINE_GETTER_AND_SETTER
#undef DEFINE_STRING_GETTER_AND_SETTER

  int gen_insert_stmt_stats_dml(uint64_t exec_tenant_id, share::ObDMLSqlSplicer &dml) const;
  static int insert_stmt_stats(ObISQLClient &sql_client, const ObMViewRefreshStmtStats &stmt_stats);
  static int drop_all_stmt_stats(ObISQLClient &sql_client, uint64_t tenant_id,
                                 int64_t &affected_rows, int64_t limit = -1);
  static int drop_stmt_stats_record(ObISQLClient &sql_client, uint64_t tenant_id,
                                    const ObMViewRefreshStatsRecordId &record_id);

  TO_STRING_KV(K_(tenant_id),
               K_(refresh_id),
               K_(mview_id),
               K_(retry_id),
               K_(step),
               K_(sql_id),
               K_(stmt),
               K_(execution_time),
               K_(execution_plan),
               K_(result));

public:
  uint64_t tenant_id_;
  int64_t refresh_id_;
  uint64_t mview_id_;
  int64_t retry_id_;
  int64_t step_;
  ObString sql_id_;
  ObString stmt_;
  int64_t execution_time_;
  ObString execution_plan_;
  int result_;
};

} // namespace schema
} // namespace share
} // namespace oceanbase
