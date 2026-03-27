/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

 #ifndef _OB_SQL_STAT_DUMP_TASK_H_
 #define _OB_SQL_STAT_DUMP_TASK_H_

 #include "lib/task/ob_timer.h"
 #include "share/wr/ob_wr_snapshot_rpc_processor.h"

 namespace oceanbase
 {
 namespace share
 {

 class ObSqlStatDumpTask : public common::ObTimerTask
 {
 public:
   constexpr static int64_t REFRESH_INTERVAL = 10 * 60 * 1000L * 1000L;
   ObSqlStatDumpTask(): is_inited_(false){}
   virtual ~ObSqlStatDumpTask() = default;
   static ObSqlStatDumpTask &get_instance();
   int init();
   void cancel_current_task();
   int schedule_one_task(int64_t interval_us = 0);
   int modify_sqlstat_interval(int64_t minutes);
   int64_t get_sql_stat_interval(bool is_lazy_load = true);
   virtual void runTimerTask() override;
 private:
   obrpc::ObWrRpcProxy wr_proxy_;
   bool is_inited_;
   int64_t sqlstat_interval_;
 };
 }
 }
 #endif /* _OB_SQL_STAT_DUMP_TASK_H_ */
 //// end of header file