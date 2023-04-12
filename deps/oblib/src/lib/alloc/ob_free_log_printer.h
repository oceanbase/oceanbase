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

#ifndef _OB_FREE_LOG_PRINTER_H_
#define _OB_FREE_LOG_PRINTER_H_
#include "lib/lock/ob_mutex.h"
namespace oceanbase
{
namespace lib
{
class AObject;
class ObFreeLogPrinter
{
public:
  const int64_t MAX_FREE_LOG_TIME = 30 * 1000 * 1000; //30s
  enum Level
  {
    DEFAULT = 0,
    CTX = 1,
    TENANT = 2,
    SERVER = 3,
  };
  ObFreeLogPrinter();
  static ObFreeLogPrinter& get_instance();
  static int get_level();
  void enable_free_log(int64_t tenant_id , int64_t ctx_id, int level);
  void disable_free_log();
  void print_free_log(int64_t tenant_id, int64_t ctx_id, AObject *obj);
private:
  int64_t last_enable_time_;
  int64_t tenant_id_;
  int64_t ctx_id_;
  int level_;
  ObMutex lock_;
};
}
}

#endif