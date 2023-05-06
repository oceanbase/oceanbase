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
 *
 * OBCDC Utilities
 */

#define USING_LOG_PREFIX OBLOG

#include "ob_log_utils.h"

#include <sys/types.h>
#include <net/if.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <linux/sockios.h>
#include <sys/ioctl.h>
#include <arpa/inet.h>
#include <stdlib.h>                                     // strtoll
#include <openssl/md5.h>                                // MD5

#include "lib/string/ob_string.h"                       // ObString
#include "lib/utility/serialization.h"                  // serialization
#include "lib/charset/ob_charset.h"                     // ObCharset
#include "lib/time/ob_time_utility.h"                   // ObTimeUtility
#include "lib/file/file_directory_utils.h"              // FileDirectoryUtils
#include "share/schema/ob_table_schema.h"               // ObTableSchema
#include "share/schema/ob_column_schema.h"              // ObColumnSchemaV2
#include "share/schema/ob_schema_struct.h"
#include "share/ob_get_compat_mode.h"
#include "rpc/obmysql/ob_mysql_global.h"                // MYSQL_TYPE_*
#include "ob_log_config.h"

using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace logfetcher
{

int print_human_tstamp(char *buf, const int64_t buf_len, int64_t &pos,
    const int64_t usec_tstamp)
{
  int ret = common::OB_SUCCESS;
  if (common::OB_INVALID_TIMESTAMP == usec_tstamp) {
    ret = common::databuff_printf(buf, buf_len, pos, "[INVALID]");
  }
  else {
    struct timeval tv;
    tv.tv_sec = usec_tstamp / _SEC_;
    tv.tv_usec = usec_tstamp % _SEC_;
    struct tm tm;
    ::localtime_r((const time_t *) &tv.tv_sec, &tm);
    ret = common::databuff_printf(buf, buf_len, pos,
                                  "[%04d-%02d-%02d %02d:%02d:%02d.%06ld]",
                                  tm.tm_year + 1900,
                                  tm.tm_mon + 1,
                                  tm.tm_mday,
                                  tm.tm_hour,
                                  tm.tm_min,
                                  tm.tm_sec,
                                  tv.tv_usec);
  }
  return ret;
}

int print_human_timeval(char *buf,
    const int64_t buf_len,
    int64_t &pos,
    const int64_t usec_tval)
{
  int ret = common::OB_SUCCESS;
  if (INT64_MAX == usec_tval) {
    ret = common::databuff_printf(buf, buf_len, pos, "[INVALID_TVAL]");
  }
  else {
    bool negative = (usec_tval < 0);
    struct timeval tv;
    if (negative) {
      tv.tv_sec = (0 - usec_tval) / _SEC_;
      tv.tv_usec = (0 - usec_tval) % _SEC_;
    }
    else {
      tv.tv_sec = usec_tval / _SEC_;
      tv.tv_usec = usec_tval % _SEC_;
    }
    int64_t hr = static_cast<int64_t>(tv.tv_sec) / 3600;
    int64_t min = (static_cast<int64_t>(tv.tv_sec) / 60) % 60;
    int64_t sec = static_cast<int64_t>(tv.tv_sec) % 60;
    ret = common::databuff_printf(buf, buf_len, pos,
                                  "[%s%02ld:%02ld:%02ld.%06ld]",
                                  negative ? "-" : "",
                                  hr,
                                  min,
                                  sec,
                                  tv.tv_usec);
  }
  return ret;
}

int get_local_ip(ObString &local_ip)
{
  int ret = OB_SUCCESS;
  const static char *DEFAULT_IP = "127.0.0.1";
  const static char *DEFAULT_DEV = "DEFAULT";
  const static int64_t BUFSIZE = 128;
  int sock_fd = 0;
  struct ifconf conf;
  struct ifreq *ifr = NULL;
  char buff[BUFSIZE];
  int64_t ifreq_num = 0;
  const char *ret_ip = DEFAULT_IP;
  const char *local_dev = DEFAULT_DEV;

  if (OB_UNLIKELY(local_ip.size() <= 0) || OB_ISNULL(local_ip.ptr())) {
    LOG_ERROR("invalid argument", K(local_ip));
    ret = OB_INVALID_ARGUMENT;
  } else {
    sock_fd = socket(PF_INET, SOCK_DGRAM, 0);
    if (sock_fd < 0){
      LOG_ERROR("socket fail", K(sock_fd), K(errno), KERRMSG);
      ret = OB_ERR_UNEXPECTED;
    } else {
      conf.ifc_len = BUFSIZE;
      conf.ifc_buf = buff;

      ioctl(sock_fd, SIOCGIFCONF, &conf);
      ifreq_num = conf.ifc_len / sizeof(struct ifreq);
      ifr = conf.ifc_req;

      if (OB_UNLIKELY(ifreq_num <= 0) || OB_ISNULL(ifr)) {
        LOG_WARN("no valid network device, set default IP", K(ifreq_num), KP(ifr));
        ret_ip = DEFAULT_IP;
      } else {
        // 设置默认IP
        ret_ip = DEFAULT_IP;

        for (int64_t i = 0; i < ifreq_num; i++) {
          struct sockaddr_in *sin = reinterpret_cast<struct sockaddr_in *>(&ifr->ifr_addr);

          ioctl(sock_fd, SIOCGIFFLAGS, ifr);

          if (((ifr->ifr_flags & IFF_LOOPBACK) == 0) && (ifr->ifr_flags & IFF_UP)) {
            ret_ip = inet_ntoa(sin->sin_addr);
            local_dev = ifr->ifr_name;
            break;
          }

          ifr++;
        }
      }
      close(sock_fd);
      sock_fd = 0;
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(ret_ip)) {
        LOG_ERROR("get local ip fail", K(ret_ip));
        ret = OB_ERR_UNEXPECTED;
      } else {
        int64_t local_ip_buf_size = local_ip.size();
        char *local_ip_buf = local_ip.ptr();

        ObString::obstr_size_t len = snprintf(local_ip_buf, local_ip_buf_size, "%s", ret_ip);
        if (OB_UNLIKELY(len < 0) || OB_UNLIKELY(len >= local_ip_buf_size)) {
          LOG_ERROR("buffer not enough", K(len), K(local_ip_buf_size), K(ret_ip));
          ret = OB_BUF_NOT_ENOUGH;
        } else {
          local_ip.set_length(len);

          LOG_INFO("[LOCAL_ADDR]", "DEV", local_dev, "IP", local_ip);
        }
      }
    }
  }

  return ret;
}

double get_delay_sec(const int64_t tstamp_ns)
{
  int64_t delta = (ObTimeUtility::current_time() - tstamp_ns / NS_CONVERSION);
  double delay_sec =  static_cast<double>(delta) / 1000000.0;
  return delay_sec;
}

#define RETRY_FUNC_CALL(stop_flag, func, args...) \
    RETRY_FUNC_CALL_ON_ERROR(OB_TIMEOUT, stop_flag, func, ##args)

#define RETRY_FUNC_CALL_ON_ERROR(err_no, stop_flag, func, args...) \
  do {\
    if (OB_SUCC(ret)) \
    { \
      ret = (err_no); \
      while ((err_no) == ret && ! (stop_flag)) \
      { \
        ret = OB_SUCCESS; \
        ret = func(args); \
      } \
      if ((stop_flag)) \
      { \
        ret = OB_IN_STOP_STATE; \
      } \
    } \
  } while (0)

ObLogTimeMonitor::ObLogTimeMonitor(const char *log_msg_prefix, bool enable)
{
  enable_ = enable;
  if (enable_) {
    log_msg_prefix_ = log_msg_prefix;
    start_time_usec_ = get_timestamp();
  } else {
    log_msg_prefix_ = NULL;
    start_time_usec_ = 0;
  }
  last_mark_time_usec_ = start_time_usec_;
}

ObLogTimeMonitor::~ObLogTimeMonitor()
{
  if (enable_) {
    int64_t cost_time = get_timestamp() - start_time_usec_;
    _LOG_INFO("[TIME_MONITOR] %s: cost:%ld; start:%ld, start_ts:%s", log_msg_prefix_, cost_time, start_time_usec_, TS_TO_STR(start_time_usec_));
    enable_ = false;
    log_msg_prefix_ = NULL;
    start_time_usec_ = 0;
    last_mark_time_usec_ = 0;
  }
}

int64_t ObLogTimeMonitor::mark_and_get_cost(const char *log_msg_suffix, bool need_print)
{
  int64_t cost = 0;
  if (enable_) {
    int64_t cur_ts = get_timestamp();
    cost = cur_ts - last_mark_time_usec_;
    if (need_print) {
      _LOG_INFO("[TIME_MONITOR] %s-%s: cost %ld", log_msg_prefix_, log_msg_suffix, cost);
    }
    last_mark_time_usec_ = cur_ts;
  }
  return cost;
}

char *lbt_oblog()
{
  int ret = OB_SUCCESS;
  //As lbt used when print error log, can not print error log
  //in this function and functions called.
  static __thread void *addrs[100];
  static __thread char buf[LBT_BUFFER_LENGTH];
  int size = backtrace(addrs, 100);
  char **res = backtrace_symbols(addrs, 100);
  int64_t pos = 0;

  for (int idx = 0; OB_SUCC(ret) && idx < size; ++idx) {
    char *res_idx = res[idx];

    if (NULL != res_idx) {
      if (OB_FAIL(databuff_printf(buf, LBT_BUFFER_LENGTH, pos, "%s", res_idx))) {
        LOG_ERROR("databuff_printf fail", KR(ret), K(buf), K(pos), K(LBT_BUFFER_LENGTH));
      }
    }
  }

  if (NULL != res) {
    free(res);
  }

  return buf;
}

//////////////////////////////////////////////////////////////////

} // namespace libocdc
} // namespace oceanbase
