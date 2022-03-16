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

#define USING_LOG_PREFIX COMMON
#include "ob_admin_usec_executor.h"

using namespace oceanbase::share;
using namespace oceanbase::common;

namespace oceanbase {
namespace tools {

ObAdminUsecExecutor::ObAdminUsecExecutor() : cmd_(ObAdminUsecCmd::MAX_CMD), usec_(0)
{}

int ObAdminUsecExecutor::execute(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  reset();

  if (OB_FAIL(parse_cmd(argc - 1, argv + 1))) {
    LOG_WARN("fail to parse cmd", K(ret));
  } else if (ObAdminUsecCmd::TO_TIME == cmd_) {
    ObObj result;
    int32_t offset = 0;
    char buf[10] = {0};
    if (time_zone_.empty()) {
      // if the member time_zone is empty, we use UTC+8 by default.
      time_zone_.assign_buffer(buf, 10);
      strcpy(buf, "+8:00");
      time_zone_.set_length(static_cast<int32_t>(strlen(buf)));
      LOG_INFO("use default time zone", K_(time_zone));
    }
    if (OB_FAIL(tz_info_.set_timezone(time_zone_))) {
      LOG_WARN("fail to set time zone", K(ret));
    } else if (OB_FAIL(tz_info_.get_timezone_offset(USEC_TO_SEC(usec_), offset))) {
      LOG_WARN("fail to get offset between utc and local", K(ret));
    } else {
      usec_ += SEC_TO_USEC(offset);
      if (!ObTimeConverter::is_valid_datetime(usec_)) {
        ret = OB_DATETIME_FUNCTION_OVERFLOW;
        LOG_WARN("datetime overflow", K(ret), K(usec_));
      } else {
        result.set_timestamp(usec_);
        LOG_INFO("usec to time", K(result), K_(usec), K_(time_zone), K(offset));
        fprintf(stdout, "\n%s, UTC%s\n", to_cstring(result), to_cstring(time_zone_));
      }
    }
    if (OB_FAIL(ret)) {
      print_usage();
    }
  } else {
    print_usage();
  }

  return ret;
}

void ObAdminUsecExecutor::reset()
{
  cmd_ = ObAdminUsecCmd::MAX_CMD;
  usec_ = 0;
}

int ObAdminUsecExecutor::parse_cmd(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  int opt = 0;
  const char *opt_string = "ht:z:";
  struct option longopts[] = {{"help", 0, NULL, 'h'}, {"to_time", 1, NULL, 't'}, {"time_zone", 1, NULL, 'z'}};

  while ((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1) {
    switch (opt) {
      case 'h': {
        print_usage();
        break;
      }
      case 't': {
        cmd_ = ObAdminUsecCmd::TO_TIME;
        usec_ = static_cast<int64_t>(strtol(optarg, NULL, 10));
        break;
      }
      case 'z': {
        time_zone_.assign_ptr(optarg, strlen(optarg));
        break;
      }
      default: {
        print_usage();
        ret = OB_INVALID_ARGUMENT;
      }
    }
  }
  return ret;
}

void ObAdminUsecExecutor::print_usage()
{
  fprintf(stderr,
      "\nUSAGE:\n"
      "        ob_admin usec_tool -t usec [-z time_zone]\n"
      "EXAMPLE:\n"
      "        ob_admin usec_tool -t 1625104800000000\n"
      "        ob_admin usec_tool -t 1625104800000000 -z +8:00\n"
      "        ob_admin usec_tool -t 1625104800000000 -z -8:00\n");
}

}  // namespace tools
}  // namespace oceanbase
