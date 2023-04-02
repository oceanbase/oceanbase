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

#ifndef OCEABASE_STORAGE_TENANT_MEMSTORE_PRINTER_
#define OCEABASE_STORAGE_TENANT_MEMSTORE_PRINTER_

#include "lib/task/ob_timer.h"          // ObTimerTask
#include "lib/lock/ob_mutex.h"          // ObMutex

namespace oceanbase
{
namespace storage
{

// this class is a timer that will call ObTenantMemoryPrinter to
// print the tenant memstore usage info.
class ObPrintTenantMemoryUsage : public ObTimerTask
{
public:
  ObPrintTenantMemoryUsage() {}
  virtual ~ObPrintTenantMemoryUsage() {}
public:
  virtual void runTimerTask();
private:
  DISALLOW_COPY_AND_ASSIGN(ObPrintTenantMemoryUsage);
};

class ObTenantMemoryPrinter
{
public:
  static ObTenantMemoryPrinter &get_instance();
  // register memstore printer to a timer thread,
  // which thread is used to print the tenant memstore usage.
  // @param[in] tg_id, the thread tg id.
  int register_timer_task(int tg_id);
  // print all the tenant memstore usage.
  int print_tenant_usage();
private:
  ObTenantMemoryPrinter() : print_mutex_(common::ObLatchIds::TENANT_MEM_USAGE_LOCK) {}
  virtual ~ObTenantMemoryPrinter() {}
  int print_tenant_usage_(const uint64_t tenant_id,
                          char *print_buf,
                          int64_t buf_len,
                          int64_t &pos);
private:
  // the timer will register to a print thread.
  ObPrintTenantMemoryUsage print_task_;
  // the mutex is used to make sure not print concurrently.
  lib::ObMutex print_mutex_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantMemoryPrinter);
};

} // storage
} // oceanbase
#endif
