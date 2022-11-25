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

#ifndef OCEANBASE_SQL_OB_EXEC_STAT_COLLECTOR_H_
#define OCEANBASE_SQL_OB_EXEC_STAT_COLLECTOR_H_

#include "lib/utility/ob_macro_utils.h"
#include "lib/string/ob_string.h"
#include "lib/trace/ob_trace_event.h"
#include "sql/monitor/ob_phy_operator_monitor_info.h"
#include "sql/monitor/ob_exec_stat.h"
#include "lib/net/ob_addr.h"
namespace oceanbase
{
namespace observer
{
class ObInnerSQLResult;
}
namespace sql
{
class ObExecContext;
class ObPhyPlanMonitorInfo;
class ObPhyOperatorMonitorInfo;
class ObSql;
class ObPhysicalPlan;
class ObSQLSessionInfo;
class ObExecStatCollector
{
public:
  ObExecStatCollector() : length_(0) {}
  ~ObExecStatCollector() {}
  int collect_monitor_info(uint64_t job_id,
                           uint64_t task_id,
                           ObPhyOperatorMonitorInfo &op_info);

  int collect_plan_monitor_info(uint64_t job_id,
                           uint64_t task_id,
                           ObPhyPlanMonitorInfo *monitor_info);
  int add_raw_stat(const common::ObString &str);
  int get_extend_info(common::ObIAllocator &allocator, common::ObString &str);
  void reset();
private:
  template<class T>
      int add_stat(const T *value);

  /* functions */
  DISALLOW_COPY_AND_ASSIGN(ObExecStatCollector);
  static const int64_t MAX_STAT_BUF_COUNT = 10240;
  char extend_buf_[MAX_STAT_BUF_COUNT];
  int64_t length_;
};

class ObExecStatDispatch
{
public:
  ObExecStatDispatch() : stat_str_(), pos_(0){};
  ~ObExecStatDispatch() {};
  int set_extend_info(const common::ObString &str);
  //负责将字符串解析成正常数据结构；
  int dispatch(bool need_add_monitor,
               ObPhyPlanMonitorInfo *monitor_info,
               bool need_update_plan,
               ObPhysicalPlan *plan);
private:
  int get_next_type(StatType &type);
  template<class T>
  int get_value(T *value);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExecStatDispatch);
  common::ObString stat_str_;
  int64_t pos_;
};

class ObExecStatUtils
{
public:
  template <class T>
      OB_INLINE static void record_exec_timestamp(const T &process,
                                        bool is_first,  //是否是第一次执行时记录，而不是retry
                                        ObExecTimestamp &exec_timestamp)
      {
        exec_timestamp.rpc_send_ts_ = process.get_send_timestamp();
        exec_timestamp.receive_ts_ = process.get_receive_timestamp();
        exec_timestamp.enter_queue_ts_ = process.get_enqueue_timestamp();
        exec_timestamp.run_ts_ = process.get_run_timestamp();
        exec_timestamp.before_process_ts_ = process.get_process_timestamp();
        exec_timestamp.single_process_ts_ = process.get_single_process_timestamp();
        exec_timestamp.process_executor_ts_ = process.get_exec_start_timestamp();
        exec_timestamp.executor_end_ts_ = process.get_exec_end_timestamp();

        if (is_first) {
          /* packet 遇见的事件顺序：
           * send -> receive -> enter_queue -> run -> before_process -> single_process -> executor_start -> executor_end
           * multistmt 场景特殊处理，第二个及之后的 sql 的 net_t、net_wait_ 均为 0
           */
          if (OB_UNLIKELY(exec_timestamp.multistmt_start_ts_ > 0)) {
            exec_timestamp.net_t_ = 0;
            exec_timestamp.net_wait_t_ = 0;
          } else {
            exec_timestamp.net_t_ = exec_timestamp.receive_ts_ - exec_timestamp.rpc_send_ts_;
            exec_timestamp.net_wait_t_ = exec_timestamp.enter_queue_ts_ - exec_timestamp.receive_ts_;
          }
        }
      }

private:
  DISALLOW_COPY_AND_ASSIGN(ObExecStatUtils);
  ObExecStatUtils();
  ~ObExecStatUtils();
};

}
}
#endif /* OCEANBASE_COMMON_STAT_OB_EXEC_STAT_COLLECTOR_H_ */
//// end of header file
