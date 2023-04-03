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

#ifndef _OB_RS_TEST_UTILS_H
#define _OB_RS_TEST_UTILS_H 1
#include <gtest/gtest.h>
#include "lib/utility/ob_test_util.h"
#include "rootserver/ob_server_manager.h"
#include "rootserver/ob_root_utils.h"
#include "lib/oblog/ob_log.h"
#include "lib/json/ob_json.h"
namespace oceanbase
{
namespace rootserver
{
class ObFakeCB : public ObIStatusChangeCallback
{
public:
  ObFakeCB() {}
  int wakeup_balancer() { return OB_SUCCESS; }
  int wakeup_daily_merger() { return OB_SUCCESS; }
  int on_start_server(const common::ObAddr &server) {UNUSED(server); return OB_SUCCESS;}
  int on_stop_server(const common::ObAddr &server) {UNUSED(server); return OB_SUCCESS;}
  int on_server_status_change(const common::ObAddr &server) {UNUSED(server); return OB_SUCCESS;}
};

class ObFakeServerChangeCB : public ObIServerChangeCallback
{
public:
  ObFakeServerChangeCB() {}
  virtual ~ObFakeServerChangeCB() {}
  virtual int on_server_change()
  {
    return OB_SUCCESS;
  }
};


class ObNeverStopForTestOnly : public share::ObCheckStopProvider
{
public:
  ObNeverStopForTestOnly() {}
  virtual ~ObNeverStopForTestOnly() {}
  virtual int check_stop() const { return OB_SUCCESS; }
};


// parse the case file using JSON parser
void ob_parse_case_file(common::ObArenaAllocator &allocator, const char* case_file, json::Value *&root);
// compare the result and output for the case
void ob_check_result(const char* base_dir, const char* casename);

} // end namespace rootserver
} // end namespace oceanbase

inline void init_oblog_for_rs_test(const char* test_name)
{
  char buf[256];
  snprintf(buf, 256, "rm -f %s.log*", test_name);
  system(buf);
  snprintf(buf, 256, "%s.log", test_name);
  OB_LOGGER.set_file_name(buf, true);
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_mod_log_levels("ALL.*:INFO,COMMON.*:ERROR");
}
#endif /* _OB_RS_TEST_UTILS_H */
