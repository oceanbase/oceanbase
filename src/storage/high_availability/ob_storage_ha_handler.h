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

#ifndef OCEABASE_STORAGE_HA_HANDLER_
#define OCEABASE_STORAGE_HA_HANDLER_

#include "lib/thread/thread_pool.h"
#include "lib/container/ob_se_array.h"

namespace oceanbase
{
namespace storage
{

class ObIHighAvaiableHandler
{
public:
  ObIHighAvaiableHandler();
  virtual ~ObIHighAvaiableHandler() = default;
private:
  DISALLOW_COPY_AND_ASSIGN(ObIHighAvaiableHandler);
};

class ObHighAvaiableHandlerMgr : public lib::ThreadPool
{
public:
  ObHighAvaiableHandlerMgr();
  virtual ~ObHighAvaiableHandlerMgr();
  int init();
  void destroy();
  void run1() final;
  void wakeup();
private:
  bool is_inited_;
  common::SpinRWLock lock_;
  common::ObSEArray<ObIHighAvaiableHandler *, 2> task_list_;
  DISALLOW_COPY_AND_ASSIGN(ObHighAvaiableHandlerMgr);
};



}
}
#endif
