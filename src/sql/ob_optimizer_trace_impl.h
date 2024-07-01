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

#ifndef _OB_OPTIMIZER_TRACE_IMPL_H
#define _OB_OPTIMIZER_TRACE_IMPL_H

#include "lib/file/ob_file.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/hash_func/murmur_hash.h"
#include "common/storage/ob_io_device.h"
#include "sql/printer/ob_raw_expr_printer.h"
#include "common/ob_smart_call.h"
#include "lib/container/ob_array.h"
#include <type_traits>

namespace oceanbase
{
namespace common {
class ObObj;
class ObDSResultItem;
}
using namespace common;
namespace sql
{
class ObDMLStmt;
class ObSelectStmt;
class ObRawExpr;
class ObLogPlan;
class ObJoinOrder;
class Path;
class JoinPath;
struct JoinInfo;
class OptTableMetas;
class TableItem;
class ObSQLSessionInfo;
struct CandidatePlan;
class OptSystemStat;
class ObSkylineDim;
struct ColumnItem;

class ObOptimizerTraceImpl;

inline ObOptimizerTraceImpl** get_local_tracer()
{
  // use thread local for raw thread.
  RLOCAL_INLINE(ObOptimizerTraceImpl*, optimizer_tracer);
  return &optimizer_tracer;
}

#define TITLE_LINE "------------------------------------------------------"

#define KV(x) #x, "(", x , ")"
#define KV_(x) #x, "(", x##_, ")"

#define BEGIN_OPT_TRACE(session, sql_id)                                  \
  ObOptimizerTraceImpl *copy_tracer = NULL;                               \
  do {                                                                    \
    ObOptimizerTraceImpl** local_tracer = get_local_tracer();             \
    if (OB_ISNULL(local_tracer) || OB_ISNULL(session)) {                  \
    } else {                                                              \
      copy_tracer = *local_tracer;                                        \
      if (session->is_user_session() &&                                   \
          session->get_optimizer_tracer().enable(sql_id)) {               \
        session->get_optimizer_tracer().open();                           \
      }                                                                   \
      *local_tracer = &(session->get_optimizer_tracer());                 \
    }                                                                     \
  } while (0);                                                            \

#define END_OPT_TRACE(session)                                            \
  do {                                                                    \
    ObOptimizerTraceImpl** local_tracer = get_local_tracer();             \
    if (OB_ISNULL(local_tracer) || OB_ISNULL(session)) {                  \
    } else {                                                              \
      if (session->is_user_session() &&                                   \
          session->get_optimizer_tracer().enable()) {                     \
        session->get_optimizer_tracer().close();                          \
      }                                                                   \
      *local_tracer = copy_tracer;                                        \
    }                                                                     \
  } while (0);                                                            \

#define CHECK_TRACE                                                       \
  ObOptimizerTraceImpl** local_tracer = get_local_tracer();               \
  ObOptimizerTraceImpl *tracer = NULL;                                    \
  if (OB_ISNULL(local_tracer) ||                                          \
      OB_ISNULL(tracer=*local_tracer)) {                                  \
  } else                                                                  \

#define CHECK_TRACE_ENABLED                   \
  CHECK_TRACE if (tracer->enable())           \

#define CHECK_CAN_TRACE_LOG                   \
  CHECK_TRACE if (tracer->can_trace_log())    \

#define RESUME_OPT_TRACE      \
  do {                        \
    CHECK_TRACE_ENABLED {             \
      tracer->resume_trace(); \
    }                         \
  } while (0);                \

#define STOP_OPT_TRACE        \
  do {                        \
    CHECK_TRACE_ENABLED {     \
      tracer->stop_trace();   \
    }                         \
  } while (0);                \

#define RESTART_OPT_TRACE     \
  do {                        \
    CHECK_TRACE_ENABLED {     \
      tracer->restart_trace();\
    }                         \
  } while (0);                \

#define OPT_TRACE_BEGIN_SECTION   \
  do {                            \
    CHECK_CAN_TRACE_LOG {         \
      tracer->increase_section(); \
    }                             \
  } while (0);                    \

#define OPT_TRACE_END_SECTION     \
  do {                            \
    CHECK_CAN_TRACE_LOG {         \
      tracer->decrease_section(); \
    }                             \
  } while (0);                    \

#define OPT_TRACE(args...)              \
  do {                                  \
    CHECK_CAN_TRACE_LOG {               \
      tracer->new_line();               \
      SMART_CALL(tracer->append(args)); \
    }                                   \
  } while (0);                          \

#define OPT_TRACE_TITLE(args...)        \
  do {                                  \
    CHECK_CAN_TRACE_LOG {               \
      tracer->append_title(args);       \
    }                                   \
  } while (0);                          \

#define BEGIN_OPT_TRACE_EVA_COST                \
  do {                                          \
    CHECK_TRACE_ENABLED {                       \
      if (!tracer->enable_trace_eva_cost()) {   \
        STOP_OPT_TRACE;                         \
      } else {                                  \
        OPT_TRACE_BEGIN_SECTION;                \
        OPT_TRACE_TITLE("BEGIN EVALUATE COST FOR STMT");  \
      }                                         \
    }                                           \
  } while (0);

#define END_OPT_TRACE_EVA_COST                  \
  do {                                          \
    CHECK_TRACE_ENABLED {                       \
      if (!tracer->enable_trace_eva_cost()) {   \
        RESUME_OPT_TRACE;                       \
      } else {                                  \
        OPT_TRACE_TITLE("END EVALUATE COST FOR STMT");  \
        OPT_TRACE_END_SECTION;                  \
      }                                         \
    }                                           \
  } while (0);

#define OPT_TRACE_ENV                             \
  do {                                            \
    CHECK_CAN_TRACE_LOG {                         \
      tracer->append_title("SYSTEM ENVIRONMENT"); \
      tracer->trace_env();                        \
    }                                             \
  } while (0);                                    \

#define OPT_TRACE_SESSION_INFO                    \
  do {                                            \
    CHECK_CAN_TRACE_LOG {                         \
      tracer->append_title("SESSION INFO");       \
      tracer->trace_session_info();               \
    }                                             \
  } while (0);                                    \

#define OPT_TRACE_PARAMETERS                      \
  do {                                            \
    CHECK_CAN_TRACE_LOG {                         \
      tracer->append_title("OPTIMIZER PARAMETERS");\
      tracer->trace_parameters();                 \
    }                                             \
  } while (0);                                    \

#define OPT_TRACE_STATIS(stmt, table_metas)       \
  do {                                            \
    CHECK_CAN_TRACE_LOG {                         \
      tracer->trace_static(stmt, table_metas);    \
    }                                             \
  } while (0);                                    \

#define OPT_TRACE_TRANSFORM_SQL(stmt)     \
  do {                                    \
    CHECK_CAN_TRACE_LOG {                 \
      tracer->trace_trans_sql(stmt);      \
    }                                     \
  } while (0);                            \

#define OPT_TRACE_TIME_USED               \
  do {                                    \
    CHECK_CAN_TRACE_LOG {                 \
      tracer->trace_time_used();          \
    }                                     \
  } while (0);                            \

#define OPT_TRACE_MEM_USED                \
  do {                                    \
    CHECK_CAN_TRACE_LOG {                 \
      tracer->trace_mem_used();           \
    }                                     \
  } while (0);                            \

#define ENABLE_OPT_TRACE_COST_MODEL               \
  do {                                            \
    CHECK_CAN_TRACE_LOG {                         \
      tracer->set_enable_trace_cost_model(true);  \
    }                                             \
  } while(0);                                     \

#define DISABLE_OPT_TRACE_COST_MODEL              \
  do {                                            \
    CHECK_CAN_TRACE_LOG {                         \
      tracer->set_enable_trace_cost_model(false); \
    }                                             \
  } while(0);                                     \

#define OPT_TRACE_COST_MODEL(args...)             \
  do {                                            \
    CHECK_CAN_TRACE_LOG {                         \
      if (tracer->enable_trace_cost_model()) {    \
        OPT_TRACE("  ", args);                    \
      }                                           \
    }                                             \
  } while(0);                                     \

class LogFileAppender {
public:
  LogFileAppender();
  int open();
  void close();
  int set_identifier(const common::ObString &identifier);
  int append(const char* buf, int64_t buf_len);
private:
  int check_log_file_full(bool &is_full);
  int generate_log_file_name();
  int open_log_file();
private:
  static const int64_t MAX_LOG_FILE_SIZE = 256*1024*1024;
  common::ObArenaAllocator allocator_;
  common::ObFileAppender log_handle_;
  common::ObString identifier_;
  ObSqlString log_file_name_;
};

class ObOptimizerTraceImpl
{
public:
  ObOptimizerTraceImpl();
  ~ObOptimizerTraceImpl();
  int enable_trace(const common::ObString &identifier,
                   const common::ObString &sql_id,
                   const int trace_level);

  int set_parameters(const common::ObString &identifier,
                    const common::ObString &sql_id,
                    const int trace_level);
  void reset();
  int open();
  void close();
  inline bool enable() const { return enable_; }
  bool enable(const common::ObString &sql_id);

  inline bool can_trace_log() const { return enable() && (trace_state_ & 1); }
  inline void set_enable(bool value) { enable_ = value; }
  inline bool enable_trace_time_used() const { return trace_level_ > 0; }
  inline bool enable_trace_mem_used() const { return trace_level_ > 0; }
  inline bool enable_trace_trans_sql() const { return trace_level_ > 1; }
  inline void set_enable_trace_cost_model(bool enable) { enable_trace_cost_model_ = enable; }
  inline bool enable_trace_cost_model() const { return trace_level_ > 2 && enable_trace_cost_model_; }
  inline bool enable_trace_eva_cost() const { return trace_level_ > 3; }
  inline void increase_section() { ++section_; }
  inline void decrease_section() { if (section_ > 0) --section_; }
  inline void set_session_info(ObSQLSessionInfo* info) { session_info_ = info; }
  void resume_trace();
  void stop_trace();
  void restart_trace();

/***********************************************/
////print basic type
/***********************************************/
  int new_line();
  int append_lower(const char* msg);
  int append_ptr(const void *ptr);
  int append();
  int append(const bool &value);
  int append(const char* msg);
  int append(const common::ObString &msg);
  int append(const int64_t &value);
  int append(const uint64_t &value);
  int append(const uint32_t &value);
  int append(const double & value);
  int append(const ObObj& value);
  int append(const OpParallelRule& rule);
  int append(const ObTableLocationType& type);
  int append(const ObPhyPlanType& type);
  int append(const OptSystemStat& stat);
/***********************************************/
////print plan info
/***********************************************/
  int append(const ObLogPlan *log_plan);
  int append(const ObJoinOrder *join_order);
  int append(const Path *value);
  int append(const JoinPath *value);
  int append(const JoinInfo& info);
  int append(const TableItem *table);
  int append(const ObShardingInfo *info);
  int append(const CandidatePlan &plan);
  int append(const ObDSResultItem &ds_result);
  int append(const ObSkylineDim &dim);
/***********************************************/
////print template type
/***********************************************/
  //for class ObRawExpr
  template <typename T>
  typename std::enable_if<std::is_base_of<ObRawExpr, T>::value, int>::type
  append(const T* expr);

  //for class ObDMLStmt
  template <typename T>
  typename std::enable_if<std::is_base_of<ObDMLStmt, T>::value, int>::type
  append(const T* value);

  //for ObIArray<ObRawExpr*>
  template <typename T>
  typename std::enable_if<std::is_base_of<ObIArray<ObRawExpr*>, T>::value, int>::type
  append(const T& value);

  //for ObIArrayWrap<uint64_t>
  template <typename T>
  typename std::enable_if<std::is_base_of<ObIArrayWrap<uint64_t>, T>::value, int>::type
  append(const T& value);

  //for ObIArrayWrap<int64_t>
  template <typename T>
  typename std::enable_if<std::is_base_of<ObIArrayWrap<int64_t>, T>::value, int>::type
  append(const T& value);

  //for ObIArray<ObDSResultItem>
  template <typename T>
  typename std::enable_if<std::is_base_of<ObIArray<ObDSResultItem>, T>::value, int>::type
  append(const T& value);

  //for ObIArray<ColumnItem>
  template <typename T>
  typename std::enable_if<std::is_base_of<ObIArray<ColumnItem>, T>::value, int>::type
  append(const T& value);

  //template for function append
  template<typename T1, typename T2, typename ...ARGS>
  int append(const T1& value1, const T2& value2, const ARGS&... args);
/***********************************************/

  template<typename ...ARGS>
  int append_key_value(const char* key, const ARGS&... args);

  template<typename ...ARGS>
  int append_title(const ARGS&... args);

  int trace_env();
  int trace_parameters();
  int trace_session_info();
  int trace_static(const ObDMLStmt *stmt, OptTableMetas &table_metas);
  int trace_trans_sql(const ObDMLStmt *stmt);
  int trace_time_used();
  int trace_mem_used();
private:
  common::ObArenaAllocator allocator_;
  LogFileAppender log_handle_;
  common::ObString sql_id_;
  ObSQLSessionInfo *session_info_;
  int64_t start_time_us_;
  int64_t last_time_us_;
  int64_t last_mem_;
  int section_;
  int trace_level_;
  bool enable_;
  uint64_t trace_state_;
  bool enable_trace_cost_model_;
};

//for class ObRawExpr
template <typename T>
typename std::enable_if<std::is_base_of<ObRawExpr, T>::value, int>::type
ObOptimizerTraceImpl::append(const T* expr)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t buf_len = OB_MAX_SQL_LENGTH;
  int64_t pos = 0;
  common::ObArenaAllocator allocator("OptimizerTrace");
  if (OB_ISNULL(buf = (char*)allocator.alloc(buf_len))) {
    ret = OB_ERR_UNEXPECTED;
    //LOG_WARN("failed to alloc buffer", K(ret));
  } else if (OB_NOT_NULL(expr)) {
    // TODO: @zhenling, use a valid schema guard in each query
    ObRawExprPrinter expr_printer(buf, buf_len, &pos, NULL);
    const ObRawExpr *raw_expr = static_cast<const ObRawExpr*>(expr);
    if (OB_FAIL(expr_printer.do_print(const_cast<ObRawExpr*>(raw_expr), T_NONE_SCOPE))) {
      //LOG_WARN("failed to print expr", K(ret));
    } else if (OB_FAIL(log_handle_.append(buf, pos))) {
      //LOG_WARN("failed to append expr", K(ret));
    }
  }
  return ret;
}

//for class ObDMLStmt
template <typename T>
typename std::enable_if<std::is_base_of<ObDMLStmt, T>::value, int>::type
ObOptimizerTraceImpl::append(const T* value)
{
  int ret = OB_SUCCESS;
  ObString sql;
  ObObjPrintParams print_params;
  common::ObArenaAllocator allocator("OptimizerTrace");
  // TODO: @zhenling, use a valid schema guard in each query
  if (OB_FAIL(ObSQLUtils::reconstruct_sql(allocator,
                                          value,
                                          sql,
                                          NULL,
                                          print_params))) {
    //LOG_WARN("failed to construct sql", K(ret));
  } else if (OB_FAIL(append(sql))) {
    //LOG_WARN("failed to append sql", K(ret));
  }
  return ret;
}

//for ObIArray<ObRawExpr*>
template <typename T>
typename std::enable_if<std::is_base_of<ObIArray<ObRawExpr*>, T>::value, int>::type
ObOptimizerTraceImpl::append(const T& value)
{
  int ret = OB_SUCCESS;
  append("[");
  for (int i = 0; OB_SUCC(ret) && i < value.count(); ++i) {
    if (i > 0) {
      append(", ");
    }
    ret = append(value.at(i));
  }
  append("]");
  return ret;
}

//for ObIArray<uint64_t>
template <typename T>
typename std::enable_if<std::is_base_of<ObIArrayWrap<uint64_t>, T>::value, int>::type
ObOptimizerTraceImpl::append(const T& value)
{
  int ret = OB_SUCCESS;
  append("[");
  for (int i = 0; OB_SUCC(ret) && i < value.count(); ++i) {
    if (i > 0) {
      append(", ");
    }
    ret = append(value.at(i));
  }
  append("]");
  return ret;
}

//for ObIArray<int64_t>
template <typename T>
typename std::enable_if<std::is_base_of<ObIArrayWrap<int64_t>, T>::value, int>::type
ObOptimizerTraceImpl::append(const T& value)
{
  int ret = OB_SUCCESS;
  append("[");
  for (int i = 0; OB_SUCC(ret) && i < value.count(); ++i) {
    if (i > 0) {
      append(", ");
    }
    ret = append(value.at(i));
  }
  append("]");
  return ret;
}

//for ObIArray<ObDSResultItem>
template <typename T>
typename std::enable_if<std::is_base_of<ObIArray<ObDSResultItem>, T>::value, int>::type
ObOptimizerTraceImpl::append(const T& value)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < value.count(); ++i) {
    if (OB_FAIL(append(value.at(i)))) {
    } else if (OB_FAIL(new_line())) {
    }
  }
  return ret;
}

template <typename T>
typename std::enable_if<std::is_base_of<ObIArray<ColumnItem>, T>::value, int>::type
ObOptimizerTraceImpl::append(const T& value)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < value.count(); ++i) {
    if (OB_FAIL(append(value.at(i).column_name_))) {
    } else if (i > 0 && OB_FAIL(new_line())) {
    }
  }
  return ret;
}

//template for function append
template<typename T1, typename T2, typename ...ARGS>
int ObOptimizerTraceImpl::append(const T1& value1, const T2& value2, const ARGS&... args)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append(value1))) {
    //LOG_WARN
  } else if (OB_FAIL(append(" "))) {
    //LOG_WARN
  } else if (OB_FAIL(append(value2))) {
    //LOG_WARN
  } else if (OB_FAIL(append(" "))) {
    //LOG_WARN
  } else if (OB_FAIL(append(args...))) {
    //LOG_WARN
  }
  return ret;
}

template<typename ...ARGS>
int ObOptimizerTraceImpl::append_key_value(const char* key, const ARGS&... args)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append(key))) {
    //LOG_WARN
  } else if (OB_FAIL(append(":\t"))) {
    //LOG_WARN
  } else if (OB_FAIL(append(args...))) {
    //LOG_WARN
  }
  return ret;
}

template<typename ...ARGS>
int ObOptimizerTraceImpl::append_title(const ARGS&... args)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(new_line())) {
    //LOG_WARN
  } else if (OB_FAIL(append(TITLE_LINE))) {
    //LOG_WARN
  } else if (OB_FAIL(new_line())) {
    //LOG_WARN
  } else if (OB_FAIL(append(args...))) {
    //LOG_WARN
  } else if (OB_FAIL(new_line())) {
    //LOG_WARN
  } else if (OB_FAIL(append(TITLE_LINE))) {
    //LOG_WARN
  }
  return ret;
}

}
}

#endif /* _OB_OPTIMIZER_TRACE_IMPL_H */
