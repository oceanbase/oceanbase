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

#ifndef OCEANBASE_SQL_OB_SQL_TASK_
#define OCEANBASE_SQL_OB_SQL_TASK_

#include "rpc/frame/ob_req_processor.h"
#include "observer/ob_srv_task.h"

namespace oceanbase
{
namespace sql
{
class ObSql;
class ObSqlTaskHandler : public rpc::frame::ObReqProcessor
{
public:
  ObSqlTaskHandler() : task_(NULL), sql_engine_(NULL) {}
  ~ObSqlTaskHandler() {}
  int init(observer::ObSrvTask *task, ObSql *sql_engine);
  void reset();
protected:
  int run();
private:
  DISALLOW_COPY_AND_ASSIGN(ObSqlTaskHandler);
  observer::ObSrvTask *task_;
  ObSql *sql_engine_;
};

class ObSqlTask : public observer::ObSrvTask
{
  friend class ObSqlTaskFactory;
public:
  ObSqlTask() : msg_type_(0), size_(0), handler_() {}
  ~ObSqlTask() {}
  int init(const int msg_type, const ObReqTimestamp &req_ts, const char *buf, const int64_t size, ObSql *sql_engine);
  void reset();
  int get_msg_type() const { return msg_type_; }
  const char *get_buf() const { return buf_; }
  int64_t get_size() const { return size_; }
  rpc::frame::ObReqProcessor &get_processor() { return handler_; }
  const ObReqTimestamp &get_req_ts() const { return req_ts_; }
  TO_STRING_KV(KP(this), K_(msg_type));
public:
  static const int64_t MAX_SQL_TASK_SIZE = 16 * 1024 - 128;
private:
  int msg_type_;
  char buf_[MAX_SQL_TASK_SIZE];
  int64_t size_;
  ObSqlTaskHandler handler_;
  ObReqTimestamp req_ts_;
};

class ObSqlTaskFactory
{
public:
  ObSqlTaskFactory() {}
  ~ObSqlTaskFactory() { destroy(); }
  int init();
  void destroy();
public:
  ObSqlTask *alloc(const uint64_t tenant_id);
  void free(ObSqlTask *task);
  static ObSqlTaskFactory &get_instance();
private:
  ObSqlTask *alloc_(const uint64_t tenant_id);
  void free_(ObSqlTask *task);
};

} // transaction 
} // oceanbase

#endif // OCEANBASE_SQL_OB_SQL_TASK_
