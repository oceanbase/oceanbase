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

#ifndef OB_TEST_OB_TEST_ERROR_TRANSACTION_THREAD_H_
#define OB_TEST_OB_TEST_ERROR_TRANSACTION_THREAD_H_

#include <pthread.h>
#include "test_ob_trans_service_ctx.h"

namespace oceanbase {
using namespace transaction;
using namespace share;
using namespace storage;
using namespace obrpc;

namespace unittest {
enum ObTransServiceType {
  OB_TRANS_SERVICE_TYPE_UNKNOW = -1,
  OB_TRANS_SERVICE_SCHEDULER = 0,
  OB_TRANS_SERVICE_COORDINATOR = 1,
  OB_TRANS_SERVICE_PATICIPANT = 2,
  OB_TRANS_SERVICE_TYPE_NUM = 3,
};

class ObTransServiceHandler {
public:
  ObTransServiceHandler();
  ~ObTransServiceHandler();

  virtual int onThreadCreate(void* arg) = 0;
  virtual int onThreadHandler(void* arg) = 0;
  virtual int onThreadExit(void* args) = 0;

  void* get_trans_service_ctx();

private:
  static void destroy_thread_key(void* ptr);
  int create_thread_key();
  int delete_thread_key();

public:
  pthread_key_t key_;
};

class ObTransServiceThread : public share::ObThreadPool {
public:
  ObTransServiceThread() : inited_(false)
  {}
  ~ObTransServiceThread()
  {}

  int start();
  int stop();
  int wait();
  int setThreadParameter(const int threadCount, ObTransServiceHandler* chandler, void* arg);
  void* get_trans_service();
  void run1();

private:
  void* arg_;
  bool inited_;
  ObTransServiceHandler* chandler_;
};

}  // namespace unittest
}  // namespace oceanbase

#endif
