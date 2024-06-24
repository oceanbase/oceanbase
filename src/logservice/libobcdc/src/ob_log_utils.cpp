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
#include "ob_log_schema_cache_info.h"                   // ColumnSchemaInfo

using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace libobcdc
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

RecordType get_record_type(const ObDmlRowFlag &dml_flag)
{
  RecordType record_type = EUNKNOWN;

  // Set record type
  // Note: The REPLACE type is not handled, it does not exist in Redo
  // Note: must judge is_delete_insert first because PUT is also is_insert, but it's flag_type is DF_TYPE_INSERT_DELETE
  if (OB_UNLIKELY(dml_flag.is_delete_insert())) {
    record_type = EPUT;
  } else if (dml_flag.is_insert()) {
    record_type = EINSERT;
  } else if (dml_flag.is_update()) {
    record_type = EUPDATE;
  } else if (dml_flag.is_delete()) {
    record_type = EDELETE;
  }  else {
    record_type = EUNKNOWN;
  }

  return record_type;
}

const char *print_dml_flag(const blocksstable::ObDmlRowFlag &dml_flag)
{
  const char *str = "UNKNOWN";

  if (dml_flag.is_delete_insert()) {
    str = "put";
  } else if (dml_flag.is_insert()) {
    str = "insert";
  } else if (dml_flag.is_update()) {
    str = "update";
  } else if (dml_flag.is_delete()) {
    str = "delete";
  } else if (dml_flag.is_lock()) {
    str = "lock";
  } else {
    str = "UNKNOWN";
  }

  return str;
}

int64_t get_non_hidden_column_count(const share::schema::ObTableSchema &table_schema)
{
  int64_t ret_column_count = 0;
  ObTableSchema::const_column_iterator cs_iter = table_schema.column_begin();
  ObTableSchema::const_column_iterator cs_iter_end = table_schema.column_end();

  for (; cs_iter != cs_iter_end; cs_iter++) {
    const ObColumnSchemaV2 &column_schema = **cs_iter;

    if (! column_schema.is_hidden()) {
      ret_column_count++;
    }
  }

  return ret_column_count;
}

const char *print_record_type(int type)
{
  static const char *str = "UNKNOWN";

  switch (type)
  {
    case EDELETE:
      str = "DELETE";
      break;

    case EINSERT:
      str = "INSERT";
      break;

    case EREPLACE:
      str = "REPLACE";
      break;

    case EUPDATE:
      str = "UPDATE";
      break;

    case HEARTBEAT:
      str = "HEARTBEAT";
      break;

    case CONSISTENCY_TEST:
      str = "CONSISTENCY_TEST";
      break;

    case EBEGIN:
      str = "EBEGIN";
      break;

    case ECOMMIT:
      str = "ECOMMIT";
      break;

    case EDDL:
      str = "EDDL";
      break;

    case EROLLBACK:
      str = "EROLLBACK";
      break;

    case EDML:
      str = "EDML";
      break;

    case EPUT:
      str = "EPUT";
      break;

    default:
      str = "UNKNOWN";
      break;
  }

  return str;
}

const char *print_src_category(int src_category)
{
  static const char *sc_name = "UNKNOWN";

  switch (src_category)
  {
    case SRC_FULL_RECORDED:
      sc_name = "SRC_FULL_RECORDED";
      break;

    case SRC_FULL_RETRIEVED:
      sc_name = "SRC_FULL_RETRIEVED";
      break;

    case SRC_FULL_FAKED:
      sc_name = "SRC_FULL_FAKED";
      break;

    case SRC_PART_RECORDED:
      sc_name = "SRC_PART_RECORDED";
      break;

    default:
      sc_name = "UNKNOWN";
      break;
  }

  return sc_name;
}

const char *print_record_src_type(int type)
{
  static const char *st_name = "UNKNOWN";

  switch (type)
  {
    case SRC_MYSQL:
      st_name = "SRC_MYSQL";
      break;

    case SRC_OCEANBASE:
      st_name = "SRC_OCEANBASE";
      break;

    case SRC_HBASE:
      st_name = "SRC_HBASE";
      break;

    case SRC_ORACLE:
      st_name = "SRC_ORACLE";
      break;

    case SRC_OCEANBASE_1_0:
      st_name = "SRC_OCEANBASE_1_0";
      break;

    case SRC_UNKNOWN:
      st_name = "SRC_UNKNOWN";
      break;

    default:
      st_name = "UNKNOWN";
      break;
  }

  return st_name;
}

const char *print_compat_mode(const lib::Worker::CompatMode &compat_mode)
{
  const char *compat_mode_str = "INVALID";

  switch (compat_mode) {
    case lib::Worker::CompatMode::MYSQL:
      compat_mode_str = "MYSQL";
      break;

    case lib::Worker::CompatMode::ORACLE:
      compat_mode_str = "ORACLE";
      break;

     default:
      compat_mode_str = "INVALID";
      break;
  }

  return compat_mode_str;
}

const char *get_ctype_string(int ctype)
{
  const char *sc_type = "UNKNOWN";

  switch (ctype)
  {
    case oceanbase::obmysql::MYSQL_TYPE_DECIMAL:
      sc_type = "MYSQL_TYPE_DECIMAL";
      break;

    case oceanbase::obmysql::MYSQL_TYPE_TINY:
      sc_type = "MYSQL_TYPE_TINY";
      break;

    case oceanbase::obmysql::MYSQL_TYPE_SHORT:
      sc_type = "MYSQL_TYPE_SHORT";
      break;

    case oceanbase::obmysql::MYSQL_TYPE_LONG:
      sc_type = "MYSQL_TYPE_LONG";
      break;

    case oceanbase::obmysql::MYSQL_TYPE_FLOAT:
      sc_type = "MYSQL_TYPE_FLOAT";
      break;

    case oceanbase::obmysql::MYSQL_TYPE_DOUBLE:
      sc_type = "MYSQL_TYPE_DOUBLE";
      break;

    case oceanbase::obmysql::MYSQL_TYPE_NULL:
      sc_type = "MYSQL_TYPE_NULL";
      break;

    case oceanbase::obmysql::MYSQL_TYPE_TIMESTAMP:
      sc_type = "MYSQL_TYPE_TIMESTAMP";
      break;

    case oceanbase::obmysql::MYSQL_TYPE_LONGLONG:
      sc_type = "MYSQL_TYPE_LONGLONG";
      break;

    case oceanbase::obmysql::MYSQL_TYPE_INT24:
      sc_type = "MYSQL_TYPE_INT24";
      break;

    case oceanbase::obmysql::MYSQL_TYPE_DATE:
      sc_type = "MYSQL_TYPE_DATE";
      break;

    case oceanbase::obmysql::MYSQL_TYPE_TIME:
      sc_type = "MYSQL_TYPE_TIME";
      break;

    case oceanbase::obmysql::MYSQL_TYPE_DATETIME:
      sc_type = "MYSQL_TYPE_DATETIME";
      break;

    case oceanbase::obmysql::MYSQL_TYPE_YEAR:
      sc_type = "MYSQL_TYPE_YEAR";
      break;

    case oceanbase::obmysql::MYSQL_TYPE_NEWDATE:
      sc_type = "MYSQL_TYPE_NEWDATE";
      break;

    case oceanbase::obmysql::MYSQL_TYPE_VARCHAR:
      sc_type = "MYSQL_TYPE_VARCHAR";
      break;

    case oceanbase::obmysql::MYSQL_TYPE_BIT:
      sc_type = "MYSQL_TYPE_BIT";
      break;

    case oceanbase::obmysql::MYSQL_TYPE_OB_TIMESTAMP_WITH_TIME_ZONE:
      sc_type = "MYSQL_TYPE_OB_TIMESTAMP_WITH_TIME_ZONE";
      break;
    case oceanbase::obmysql::MYSQL_TYPE_OB_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      sc_type = "MYSQL_TYPE_OB_TIMESTAMP_WITH_LOCAL_TIME_ZONE";
      break;
    case oceanbase::obmysql::MYSQL_TYPE_OB_TIMESTAMP_NANO:
      sc_type = "MYSQL_TYPE_OB_TIMESTAMP_NANO";
      break;
    case oceanbase::obmysql::MYSQL_TYPE_OB_RAW:
      sc_type = "MYSQL_TYPE_OB_RAW";
      break;

    case oceanbase::obmysql::MYSQL_TYPE_ROARINGBITMAP:
      sc_type = "MYSQL_TYPE_ROARINGBITMAP";
      break;

    case oceanbase::obmysql::MYSQL_TYPE_NEWDECIMAL:
      sc_type = "MYSQL_TYPE_NEWDECIMAL";
      break;

    case oceanbase::obmysql::MYSQL_TYPE_ENUM:
      sc_type = "MYSQL_TYPE_ENUM";
      break;

    case oceanbase::obmysql::MYSQL_TYPE_SET:
      sc_type = "MYSQL_TYPE_SET";
      break;

    case oceanbase::obmysql::MYSQL_TYPE_TINY_BLOB:
      sc_type = "MYSQL_TYPE_TINY_BLOB";
      break;

    case oceanbase::obmysql::MYSQL_TYPE_MEDIUM_BLOB:
      sc_type = "MYSQL_TYPE_MEDIUM_BLOB";
      break;

    case oceanbase::obmysql::MYSQL_TYPE_LONG_BLOB:
      sc_type = "MYSQL_TYPE_LONG_BLOB";
      break;

    case oceanbase::obmysql::MYSQL_TYPE_BLOB:
      sc_type = "MYSQL_TYPE_BLOB";
      break;

    case oceanbase::obmysql::MYSQL_TYPE_VAR_STRING:
      sc_type = "MYSQL_TYPE_VAR_STRING";
      break;

    case oceanbase::obmysql::MYSQL_TYPE_STRING:
      sc_type = "MYSQL_TYPE_STRING";
      break;

    case oceanbase::obmysql::MYSQL_TYPE_JSON:
      sc_type = "MYSQL_TYPE_JSON";
      break;

    case oceanbase::obmysql::MYSQL_TYPE_GEOMETRY:
      sc_type = "MYSQL_TYPE_GEOMETRY";
      break;

    case oceanbase::obmysql::MYSQL_TYPE_NOT_DEFINED:
      sc_type = "MYSQL_TYPE_NOT_DEFINED";
      break;

    case oceanbase::obmysql::MYSQL_TYPE_OB_INTERVAL_YM:
      sc_type = "MYSQL_TYPE_OB_INTERVAL_YM";
      break;

    case oceanbase::obmysql::MYSQL_TYPE_OB_INTERVAL_DS:
      sc_type = "MYSQL_TYPE_OB_INTERVAL_DS";
      break;

    case oceanbase::obmysql::MYSQL_TYPE_OB_NUMBER_FLOAT:
      sc_type = "MYSQL_TYPE_OB_NUMBER_FLOAT";
      break;
    case oceanbase::obmysql::MYSQL_TYPE_OB_NVARCHAR2:
      sc_type = "MYSQL_TYPE_OB_NVARCHAR2";
      break;

    case oceanbase::obmysql::MYSQL_TYPE_OB_NCHAR:
      sc_type = "MYSQL_TYPE_OB_NCHAR";
      break;

    case oceanbase::obmysql::MYSQL_TYPE_OB_UROWID:
      sc_type = "MYSQL_TYPE_OB_UROWID";
      break;

    case drcmsg_field_types::DRCMSG_TYPE_ORA_BINARY_FLOAT:
      sc_type = "MYSQL_TYPE_ORA_BINARY_FLOAT";
      break;

    case drcmsg_field_types::DRCMSG_TYPE_ORA_BINARY_DOUBLE:
      sc_type = "MYSQL_TYPE_ORA_BINARY_DOUBLE";
      break;

    case drcmsg_field_types::DRCMSG_TYPE_ORA_XML:
      sc_type = "MYSQL_TYPE_ORA_XML";
      break;

    default:
      sc_type = "UNKNOWN";
      break;
  }

  return sc_type;
}

bool is_lob_type(const int ctype)
{
  bool bool_ret = false;
  switch (ctype)
  {
    case oceanbase::obmysql::MYSQL_TYPE_TINY_BLOB:
    case oceanbase::obmysql::MYSQL_TYPE_MEDIUM_BLOB:
    case oceanbase::obmysql::MYSQL_TYPE_LONG_BLOB:
    case oceanbase::obmysql::MYSQL_TYPE_BLOB:
      bool_ret = true;
      break;

    default:
      bool_ret = false;
      break;
  }

  return bool_ret;
}

bool is_string_type(const int ctype)
{
  return (ctype == oceanbase::obmysql::MYSQL_TYPE_VAR_STRING ||
          ctype == oceanbase::obmysql::MYSQL_TYPE_STRING ||
          ctype == oceanbase::obmysql::MYSQL_TYPE_OB_NCHAR ||
          ctype == oceanbase::obmysql::MYSQL_TYPE_OB_NVARCHAR2);
}

bool is_json_type(const int ctype)
{
  return (oceanbase::obmysql::MYSQL_TYPE_JSON == ctype);
}

bool is_geometry_type(const int ctype)
{
  return (ctype == oceanbase::obmysql::MYSQL_TYPE_GEOMETRY);
}

bool is_xml_type(const int ctype)
{
  return (ctype == drcmsg_field_types::DRCMSG_TYPE_ORA_XML);
}

bool is_roaringbitmap_type(const int ctype)
{
  return (ctype == oceanbase::obmysql::MYSQL_TYPE_ROARINGBITMAP);
}

double get_delay_sec(const int64_t tstamp_ns)
{
  int64_t delta = (ObTimeUtility::current_time() - tstamp_ns / NS_CONVERSION);
  double delay_sec =  static_cast<double>(delta) / 1000000.0;
  return delay_sec;
}

bool is_ddl_table(const uint64_t table_id)
{
  return (share::OB_ALL_DDL_OPERATION_TID == table_id);
}

int64_t get_ddl_table_id()
{
  return share::OB_ALL_DDL_OPERATION_TID;
}

bool is_mysql_client_errno(int err)
{
  return (err <= -2000 && err >= -3000);
}

bool is_mysql_server_errno(int err)
{
  return (err >= -1999 && err <= -1000);
}

bool is_ob_sql_errno(int err)
{
  return (err > -6000 && err <= -5000);
}

bool is_ob_trans_errno(int err)
{
  return (err > -7000 && err <= -6000);
}

bool is_ob_election_errno(int err)
{
  return (err > -7100 && err <= -7000);
}

void *ob_cdc_malloc(
    const int64_t nbyte,
    const lib::ObLabel &lable,
    const uint64_t tenant_id)
{
  ObMemAttr memattr;
  memattr.tenant_id_ = tenant_id;
  memattr.label_ = lable;

  return ob_malloc(nbyte, memattr);
}

void ob_cdc_free(void *ptr)
{
  ob_free(ptr);
}

void column_cast(common::ObObj &obj, const share::schema::ObColumnSchemaV2 &column_schema)
{
  // Neither the NULL type nor the Ext type update Meta information
  if (! obj.is_null() && ! obj.is_ext()) {
    // update meta
    obj.set_meta_type(column_schema.get_meta_type());

    // Update scale separately
    // The scale information of the bit type has a different meaning than the scale information of the normal type
    if (obj.is_bit()) {
      obj.set_scale(column_schema.get_accuracy().get_precision());
    } else {
      obj.set_scale(column_schema.get_accuracy().get_scale());
    }
  }
}

void column_cast(common::ObObj &obj, const ColumnSchemaInfo &column_schema_info)
{
  // Neither the NULL type nor the Ext type update Meta information
  if (! obj.is_null() && ! obj.is_ext()) {
    // upadte Meta
    obj.set_meta_type(column_schema_info.get_meta_type());

    // Update scale separately
    // The scale information of the bit type has a different meaning than the scale information of the normal type
    if (obj.is_bit()) {
      obj.set_scale(column_schema_info.get_accuracy().get_precision());
    } else {
      obj.set_scale(column_schema_info.get_accuracy().get_scale());
    }
  }
}

int Runnable::create()
{
  int ret = common::OB_SUCCESS;
  int errcode = 0;
  if (0 != (errcode = pthread_create(&thread_, NULL, pthread_routine, this))) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_ERROR("err create pthread", KR(ret), K(errcode));
  }
  else {
    joinable_ = true;
  }
  return ret;
}

int Runnable::join()
{
  int ret = common::OB_SUCCESS;
  int errcode = 0;
  void *thread_ret = NULL;
  if (!joinable_ || 0 != (errcode = pthread_join(thread_, &thread_ret))) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_ERROR("err join pthread", KR(ret), K(errcode));
  } else if (OB_FAIL(static_cast<int>(reinterpret_cast<int64_t>(thread_ret)))) {
    LOG_WARN("thread exit on err", KR(ret));
  }
  else {
    joinable_ = false;
  }
  return ret;
}

void *Runnable::pthread_routine(void *arg)
{
  return reinterpret_cast<void*>((static_cast<MyType*>(arg))->routine());
}

int ObLogKVCollection::KVPair::init(const char *delimiter)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("init twice", K(inited_));
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(delimiter)) {
    LOG_ERROR("invalid argument", K(delimiter));
    ret = OB_INVALID_ARGUMENT;
  } else {
    delimiter_ = delimiter;
    inited_ = true;
  }
  return ret;
}

int ObLogKVCollection::KVPair::set_key_and_value(const char *key, const char *value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    LOG_ERROR("kv not init", K(inited_));
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(key) || OB_ISNULL(value)) {
    LOG_ERROR("data for kvpair should not be null", K(key), K(value));
    ret = OB_INVALID_DATA;
  } else {
    key_ = key;
    value_ = value;
  }
  return ret;
}

bool ObLogKVCollection::KVPair::is_valid() const
{
  return inited_ && NULL != delimiter_ && NULL != key_ && NULL != value_;
}

int ObLogKVCollection::KVPair::length() const
{
  int len = 0;
  if (is_valid()) {
    len += strlen(key_);
    len += strlen(value_);
    len += strlen(delimiter_);
  }
  return len;
}

int ObLogKVCollection::KVPair::serialize(char *buf, int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    LOG_ERROR("kv pair not valid", K_(inited), K_(delimiter), K_(key), K_(value));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
    LOG_ERROR("invalid argument", K(buf), K(buf_len), K(pos));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s%s%s", key_, delimiter_, value_))) {
    LOG_ERROR("databuff_printf fail", K(ret), K(buf_len), K(pos), K_(key), K_(value), K_(delimiter));
  } else if (OB_UNLIKELY(pos >= buf_len)) {
    LOG_ERROR("buf is not enough", K(pos), K(buf_len));
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    buf[pos] = '\0';
  }
  return ret;
}

int ObLogKVCollection::KVPair::deserialize(char *buf)
{
  int ret = OB_SUCCESS;
  char *save_ptr = NULL;
  if (!inited_) {
    LOG_ERROR("kv pair not init", K_(inited));
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(buf)) {
    LOG_ERROR("invalid argument", K(buf));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(key_ = strtok_r(buf, delimiter_, &save_ptr))) {
    LOG_ERROR("key is null", K(save_ptr));
    ret = OB_INVALID_DATA;
  } else if (OB_ISNULL(value_ = strtok_r(NULL, delimiter_, &save_ptr))) {
    LOG_ERROR("value is null", K(save_ptr));
    ret = OB_INVALID_DATA;
  } else {
    LOG_DEBUG("found key and value", K_(key), K_(value));
  }
  return ret;
}

int ObLogKVCollection::init(const char *kv_delimiter, const char *pair_delimiter)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("init twice", K(inited_));
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(kv_delimiter_ = kv_delimiter) || OB_ISNULL(pair_delimiter_ = pair_delimiter)){
    LOG_ERROR("invalid argument", K(kv_delimiter), K(pair_delimiter));
    ret = OB_INVALID_ARGUMENT;
  } else {
    kv_pairs_.reset();
    inited_ = true;
  }
  return ret;
}

bool ObLogKVCollection::is_valid() const
{
  bool valid = true;
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(kv_delimiter_) || OB_ISNULL(pair_delimiter_)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "invalid argument", K_(inited), K_(kv_delimiter), K_(pair_delimiter));
    valid = false;
  } else {
    int64_t idx = 0;
    int64_t size = kv_pairs_.size();
    while (idx < size && valid) {
      KVPair pair = kv_pairs_.at(idx);
      valid = pair.is_valid();
      idx++;
    }
  }
  return valid;
}

int ObLogKVCollection::length() const
{
  int len = 0;
  int64_t idx = 0;
  int64_t size = kv_pairs_.size();
  if (is_valid()) {
    while (idx < size) {
      KVPair pair = kv_pairs_.at(idx);
      len += pair.length();
      if (idx != size-1) {
        len += strlen(pair_delimiter_);
      }
      idx++;
    }
  }
  return len;
}

int ObLogKVCollection::append_kv_pair(KVPair &kv_pair)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid() || OB_UNLIKELY(!kv_pair.is_valid()))) {
    LOG_ERROR("oblog kv collection or kv_pair not valid", K(kv_pair));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(kv_pairs_.push_back(kv_pair))) {
    LOG_ERROR("push kv_pair to kv collection failed", KR(ret), K(kv_pair), K_(kv_pairs));
  }
  return ret;
}

int ObLogKVCollection::serialize(char *kv_str, const int64_t kv_str_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(kv_str) || OB_UNLIKELY(kv_str_len <= 0)) {
    LOG_ERROR("invalid argument", K(kv_str), K(kv_str_len), K(pos));
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t size = kv_pairs_.size();
    int64_t idx = 0;
    while (OB_SUCC(ret) && idx < size) {
      KVPair kvpair = kv_pairs_.at(idx);
      if (OB_FAIL(kvpair.serialize(kv_str, kv_str_len, pos))) {
        LOG_ERROR("serialize kvpair fail", K(kv_str), K(kv_str_len), K(pos));
      } else if (idx < size - 1 && OB_FAIL(databuff_printf(kv_str, kv_str_len, pos, "%s", pair_delimiter_))) {
        LOG_ERROR("append pair delimiter fail", KR(ret), K(idx), K(size), K(kv_str), K(kv_str_len), K(pos), K_(pair_delimiter));
      } else if (OB_UNLIKELY(pos >= kv_str_len)) {
        LOG_ERROR("buf is not enough", K(kv_str), K(pos), K(kv_str_len));
        ret = OB_BUF_NOT_ENOUGH;
      } else {
        kv_str[pos] = '\0';
      }
      idx++;
    }
  }
  return ret;
}

int ObLogKVCollection::deserialize(char *kv_str)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    LOG_ERROR("kv collection not init", K_(inited));
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(kv_str) || OB_ISNULL(kv_delimiter_) || OB_ISNULL(pair_delimiter_)) {
    LOG_ERROR("invalid argument", K(kv_str), K_(kv_delimiter), K_(pair_delimiter));
    ret = OB_INVALID_ARGUMENT;
  } else {
    char *kv = NULL;
    char *save_ptr = NULL;
    while (OB_SUCC(ret) && (NULL != (kv = strtok_r(kv_str, pair_delimiter_, &save_ptr)))) {
      KVPair kvpair;
      if (OB_FAIL(kvpair.init(kv_delimiter_))) {
        LOG_ERROR("failed to init kv pair", KR(ret), K(kv), K(kv_str), K(save_ptr));
        ret = OB_INVALID_DATA;
      } else if (OB_FAIL(kvpair.deserialize(kv))) {
        LOG_ERROR("failed to deserialilze kv str", KR(ret), K(kvpair), K(kv));
      } else {
        kv_pairs_.push_back(kvpair);
      }
      kv_str = NULL;
    }
  }
  return ret;
}

int ObLogKVCollection::contains_key(const char *key, bool &contain)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    LOG_ERROR("kv collection not valid", K(key));
    ret = OB_ERR_UNEXPECTED;
  } else {
    contain = false;
    int64_t size = kv_pairs_.size();
    int64_t idx = 0;
    while (idx < size && !contain) {
      KVPair kv = kv_pairs_.at(idx);
      const char *key_ = kv.get_key();
      contain = (0 == strcmp(key, key_));
      idx++;
    }
  }
  return ret;
}

int ObLogKVCollection::get_value_of_key(const char *key, const char *&value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    LOG_ERROR("kv collection invalid", K(key), K(kv_pairs_));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(NULL == key)) {
    LOG_ERROR("invalid argument(key should not be null)");
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t size = kv_pairs_.size();
    int64_t idx = 0;
    bool found = false;
    while (idx < size && !found) {
      KVPair kv = kv_pairs_.at(idx);
      const char *key_ = kv.get_key();
      found = (0 == strcmp(key, key_));
      value = kv.get_value();
      idx++;
    }
    if (!found) {
      ret = OB_ENTRY_NOT_EXIST;
    }
    if (NULL == value) {
      ret = OB_INVALID_DATA;
    }
  }
  return ret;
}

int split(char *str, const char *delimiter,
    const int64_t expect_res_cnt, const char **res, int64_t &res_cnt)
{
  int ret = OB_SUCCESS;
  res_cnt = 0;

  if ((OB_ISNULL(str) || OB_UNLIKELY(0 == strlen(str))) ||
      (OB_ISNULL(delimiter) || OB_UNLIKELY(0 == strlen(delimiter))) ||
      OB_UNLIKELY(expect_res_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KP(str), KP(delimiter), K(expect_res_cnt));
  } else {
    char *ptr = NULL;
    char *p = NULL;
    int64_t idx = 0;

    ptr = strtok_r(str, delimiter, &p);

    while (OB_SUCCESS == ret && ptr != NULL) {
      if (idx++ < expect_res_cnt) {
        *res++ = ptr;
        ++res_cnt;
        ptr = strtok_r(NULL, delimiter, &p);
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("split error, check input parameter", KP(str), KP(delimiter),
            K(idx), K(expect_res_cnt), K(res_cnt));
      }
    }
  }

  return ret;
}

int split_int64(const ObString &str, const char delimiter, ObIArray<int64_t> &ret_array)
{
  int ret = OB_SUCCESS;
  void *buffer = NULL;
  int64_t buf_len = str.length() + 1; // include the last '\0'
  ObString data_str;

  if (str.length() <= 0) {
    // empty string
  } else if (OB_ISNULL(buffer = ob_cdc_malloc(buf_len))) {
    LOG_ERROR("allocate memory for buffer fail", K(buffer), K(str.length()));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    ObDataBuffer data_buffer(static_cast<char *>(buffer), buf_len);

    if (OB_FAIL(data_str.clone(str, data_buffer))) {
      LOG_ERROR("clone string fail", KR(ret), K(str), K(data_buffer));
    } else {
      bool done = false;

      // fill '\0' at tail
      data_str.ptr()[data_str.length()] = '\0';

      // Split all substrings, converting each one to int
      while (data_str.length() > 0 && OB_SUCCESS == ret && ! done) {
        ObString ret_str = data_str.split_on(delimiter);

        if (ret_str.length() <= 0) {
          // If the ret_str pointer is not empty but has a length of 0, there is no valid content before the separator
          // Continue parsing
          if (NULL != ret_str.ptr()) {
            continue;
          }

          // If ret_str is empty, the target string does not contain the corresponding separator
          // In this case the target string is analysed directly
          ret_str = data_str;
          done = true;
        }

        char *end_ptr = NULL;

        // file '\0'
        ret_str.ptr()[ret_str.length()] = '\0';

        // clear errno
        errno = 0;

        // convert string to int
        int64_t ret_int_val = strtoll(ret_str.ptr(), &end_ptr, 10);

        // If there is an error in the conversion process, or if the characters in the string are not all converted to numbers
        // it means that the original string is faulty and cannot be converted to string
        if (errno != 0 || (NULL != end_ptr && *end_ptr != '\0')) {
          LOG_ERROR("strtoll convert string to int value fail", K(ret_int_val), K(ret_str),
              K(end_ptr), KP(ret_str.ptr()), KP(end_ptr), "error", strerror(errno));
          ret = OB_INVALID_DATA;
        } else if (OB_FAIL(ret_array.push_back(ret_int_val))) {
          LOG_ERROR("push back into array fail", KR(ret), K(ret_int_val), K(ret_array));
        } else {
          // success
          LOG_DEBUG("split_int64", K(done), K(str), K(delimiter), K(data_str), K(ret_str),
              K(ret_int_val), K(ret_array));
        }
      }
    }
  }

  if (NULL != buffer) {
    ob_free(buffer);
    buffer = NULL;
  }

  return ret;
}

const char *calc_md5_cstr(const char *buf, const int64_t length)
{
  const char *md5_cstr = "";

  static const int64_t MD5_SUM_LEN = 16;
  static const int64_t MD5_STR_LEN = MD5_SUM_LEN * 2 + 1;
  static char MD5_SUM_BUF[MD5_SUM_LEN];
  static char MD5_STR_BUF[MD5_STR_LEN];
  const unsigned char *in_buf = reinterpret_cast<const unsigned char *>(buf);
  unsigned char *out_buf = reinterpret_cast<unsigned char *>(MD5_SUM_BUF);

  if (NULL == buf) {
    md5_cstr = "NULL";
  } else {
    // Calculate MD5
    (void)MD5(in_buf, length, out_buf);
    // Print to hex
    (void)to_hex_cstr(MD5_SUM_BUF, MD5_SUM_LEN, MD5_STR_BUF, MD5_STR_LEN);
    (void)ObCharset::casedn(CS_TYPE_UTF8MB4_GENERAL_CI, MD5_STR_BUF, MD5_STR_LEN, MD5_STR_BUF, MD5_STR_LEN);
    md5_cstr = MD5_STR_BUF;
  }
  return md5_cstr;
}

void calc_crc_checksum(uint64_t &crc_value, const char *buf, const int64_t length)
{
  crc_value = ob_crc64(crc_value, buf, length);
}

int deep_copy_str(const ObString &src,
    ObString &dest,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;

  if (src.length() > 0) {
    int64_t len = src.length() + 1;
    if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(len)))) {
      LOG_ERROR("allocate memory fail", K(len));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMCPY(buf, src.ptr(), len - 1);
      buf[len - 1] = '\0';
      dest.assign_ptr(buf, static_cast<ObString::obstr_size_t>(len - 1));
    }
  } else {
    dest.reset();
  }

  return ret;
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

int get_tenant_compat_mode(const uint64_t tenant_id,
    lib::Worker::CompatMode &compat_mode,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  const static int64_t GET_TENANT_COMPAT_MODE_TIMEOUT = 1 * 1000 * 1000;

  RETRY_FUNC_CALL(stop_flag, get_tenant_compat_mode, tenant_id, compat_mode, GET_TENANT_COMPAT_MODE_TIMEOUT);

  return ret;
}

int get_tenant_compat_mode(const uint64_t tenant_id,
    lib::Worker::CompatMode &compat_mode,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  bool done = false;
  int64_t end_time = ObTimeUtility::current_time() + timeout;

  while (OB_SUCC(ret) && ! done) {
    if (OB_FAIL(share::ObCompatModeGetter::instance().get_tenant_mode(tenant_id, compat_mode))) {
      LOG_WARN("ObCompatModeGetter get_tenant_mode fail", KR(ret), K(tenant_id), "compat_mode", print_compat_mode(compat_mode));
    } else if (OB_UNLIKELY(lib::Worker::CompatMode::INVALID == compat_mode)) {
      LOG_ERROR("tenant compat_mode is invalid", K(tenant_id), "compat_mode", print_compat_mode(compat_mode));
    } else {
      done = true;
    }

    if (! done) {
      // Retry to get it again
      ret = OB_SUCCESS;
      // After a failure to acquire the tenant schema, and in order to ensure that the modules can handle the performance, usleep for a short time
      ob_usleep(100);
    }

    int64_t left_time = end_time - ObTimeUtility::current_time();

    if (left_time <= 0) {
      ret = OB_TIMEOUT;
    }
  } // while

  return ret;
}

constexpr uint64_t BackupTableHelper::inner_table_ids[];
bool BackupTableHelper::is_sys_table_exist_on_backup_mode(const bool is_sys_table,
    const uint64_t table_id)
{
  int bret = false;
  if (! is_backup_mode() || (! is_sys_table)) {
    bret = false;
  } else {
    int32_t size = sizeof(inner_table_ids) / sizeof(uint64_t);
    const uint64_t pure_tb_id = table_id;
    bret = std::binary_search(inner_table_ids, inner_table_ids+size, pure_tb_id);
  }
  return bret;
}

int BackupTableHelper::get_table_ids_on_backup_mode(common::ObIArray<uint64_t> &table_ids)
{
  int ret = OB_SUCCESS;
  if (! is_backup_mode()) {
    // skip
  } else {
    uint64_t table_id = OB_INVALID_ID;
    int32_t size = sizeof(inner_table_ids) / sizeof(uint64_t);
    for (int32_t i = 0; i < size && OB_SUCC(ret); i++) {
      table_id = inner_table_ids[i];
      if (OB_LIKELY(is_sys_table(table_id))) {
        table_ids.push_back(table_id);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid sys table id", K(i), K(table_id));
      }
    }
  }
  return ret;
}

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


bool is_backup_mode()
{
  return (TCONF.enable_backup_mode != 0);
}

char *lbt_oblog()
{
  int ret = OB_SUCCESS;
  //As lbt used when print error log, can not print error log
  //in this function and functions called.
  static __thread void *addrs[100];
  static __thread char buf[LBT_BUFFER_LENGTH];
  int size = ob_backtrace(addrs, 100);
  char **res = backtrace_symbols(addrs, 100);
  int64_t pos = 0;

  for (int idx = 0; OB_SUCC(ret) && idx < size; ++idx) {
    char *res_idx = res[idx];
    int tmp_ret = OB_SUCCESS;

    if (OB_NOT_NULL(res_idx)) {
      if (OB_TMP_FAIL(databuff_printf(buf, LBT_BUFFER_LENGTH, pos, "%s", res_idx))) {
        if (OB_SIZE_OVERFLOW != ret) {
          LOG_WARN("atabuff_printf fail when lbt, ignore", KR(tmp_ret), K(idx), K(size), K(buf), K(pos),
              K(LBT_BUFFER_LENGTH));
        }
      }
    }
  }

  if (OB_NOT_NULL(res)) {
    free(res);
  }

  return buf;
}

int get_br_value(IBinlogRecord *br,
    ObArray<BRColElem> &new_values)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(br)) {
    LOG_ERROR("invalid argument");
    ret = OB_INVALID_ARGUMENT;
  } else {
    IStrArray *new_cols = br->parsedNewCols();
    int64_t new_cols_count = 0;
    if (NULL != new_cols) {
      new_cols_count = new_cols->size();
    }

    const char *new_col_value = NULL;
    size_t new_col_value_len = 0;
    int64_t index = 0;

    while (OB_SUCC(ret) && index < new_cols_count) {
      ret = new_cols->elementAt(index, new_col_value, new_col_value_len);
      BRColElem new_col_elem(new_col_value, new_col_value_len);

      if (OB_FAIL(new_values.push_back(new_col_elem))) {
        LOG_ERROR("new_values push_back fail", KR(ret));
      } else {
        ++index;
      }
    }
  }

  return ret;
}

int get_mem_br_value(IBinlogRecord *br,
    std::string &key_string,
    ObArray<BRColElem> &new_values)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(br)) {
    LOG_ERROR("invalid argument");
    ret = OB_INVALID_ARGUMENT;
  } else {
    int record_type = br->recordType();
    int64_t new_cols_count = 0;
    binlogBuf *old_cols = br->oldCols((unsigned int &)new_cols_count);
    binlogBuf *new_cols = br->newCols((unsigned int &)new_cols_count);
    int64_t index = 0;

    while (OB_SUCC(ret) && index < new_cols_count) {
      const char *new_col_value = new_cols[index].buf;
      const char *old_col_value = old_cols[index].buf;
      size_t new_col_value_len = static_cast<size_t>(new_cols[index].buf_used_size);

      BRColElem new_col_elem(new_col_value, new_col_value_len);
      // LOG_DEBUG("new_row_value ", KP(br), KP(new_cols[index].buf));
      if (EDELETE == record_type) {
        _LOG_INFO("key_str: %s, binlog: ptr=%p, col_idx=%ld; old_col_value=%s, old_col_ptr=%p",
          key_string.c_str(), &br, index, old_col_value, &(old_cols[index].buf));
      } else if (EINSERT == record_type) {
        _LOG_INFO("key_str: %s, binlog: ptr=%p, col_idx=%ld; new_col_value=%s, new_col_ptr=%p;",
          key_string.c_str(), &br, index, new_col_value, &(new_cols[index].buf));
      } else if (EUPDATE == record_type) {
        _LOG_INFO("key_str: %s, binlog: ptr=%p, col_idx=%ld; new_col_value=%s, new_col_ptr=%p; old_col_value=%s, old_col_ptr=%p",
          key_string.c_str(), &br, index, new_col_value, &(new_cols[index].buf), old_col_value, &(old_cols[index].buf));
      }

      if (OB_FAIL(new_values.push_back(new_col_elem))) {
        LOG_ERROR("new_values push_back fail", KR(ret));
      } else {
        ++index;
      }
    }
  }

  return ret;
}

int print_unserilized_br_value(IBinlogRecord *binlog_record,
    const char *key_c_str,
    const char *trans_id_c_str)
{
  int ret = OB_SUCCESS;
  ObArray<BRColElem> new_values;
  bool is_table_meta_null = false;
  ITableMeta *table_meta = NULL;
  std::string key_str(key_c_str);
  std::string trans_id_str(trans_id_c_str);
  std::string key = key_str + trans_id_str;

  if (0 != binlog_record->getTableMeta(table_meta)) {
    LOG_ERROR("getTableMeta fail");
    ret = OB_ERR_UNEXPECTED;
  } else {
    int64_t col_count = 0;

    if (NULL == table_meta) {
      is_table_meta_null = true;
    } else {
      col_count = table_meta->getColCount();
    }

    if (OB_ISNULL(binlog_record)) {
      LOG_ERROR("invalid argument", K(binlog_record));
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(get_mem_br_value(binlog_record, key, new_values))) {
      LOG_ERROR("get_mem_br_value fail", KR(ret));
    } else {
      LOG_INFO("ObLogBR info", "key", key.c_str(), K(new_values), K(is_table_meta_null),
          K(col_count));
    }
  }

  return ret;
}

int print_serilized_br_value(std::string &key,
    const std::string &drc_message_factory_binlog_record_type,
    const char *br_string,
    const size_t br_string_len)
{
  int ret = OB_SUCCESS;
  IBinlogRecord *binlog_record = DRCMessageFactory::createBinlogRecord(drc_message_factory_binlog_record_type, false/*creating_binlog_record*/);
  ObArray<BRColElem> new_values;
  ITableMeta *table_meta = NULL;

  if (OB_ISNULL(binlog_record)) {
    LOG_ERROR("invalid argument", K(binlog_record));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(binlog_record->parse(br_string, br_string_len))) {
    LOG_ERROR("binlog_record parse fail", K(ret), K(binlog_record));
  } else if (OB_FAIL(get_br_value(binlog_record, new_values))) {
    LOG_ERROR("get_mem_br_value fail", KR(ret));
  } else if (OB_ISNULL(table_meta = DRCMessageFactory::createTableMeta())) {
    LOG_ERROR("table_meta is NULL");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (0 != binlog_record->getTableMeta(table_meta)) {
    LOG_ERROR("getTableMeta fail");
    ret = OB_ERR_UNEXPECTED;
  } else {
    bool is_table_meta_null = false;
    int64_t col_count = 0;

    if (NULL == table_meta) {
      is_table_meta_null = true;
    } else {
      col_count = table_meta->getColCount();
    }

    LOG_INFO("store_service_ serilized but before put", "key", key.c_str(), K(new_values), K(is_table_meta_null),
        K(col_count));
  }

  if (NULL != table_meta) {
    DRCMessageFactory::destroy(table_meta);
  }

  return ret;
}

int c_str_to_int(const char *str, int64_t &num)
{
  int ret = OB_SUCCESS;
  errno = 0;
  char *end_str = NULL;
  if (OB_ISNULL(str) || OB_UNLIKELY(0 == strlen(str))) {
    LOG_ERROR("c_str_to_int str should not null");
    ret = OB_INVALID_ARGUMENT;
  } else {
    num = strtoll(str, &end_str, 10);
    if (errno != 0 || (NULL != end_str && *end_str != '\0')) {
      LOG_ERROR("strtoll convert string to int value fail", K(str), K(num),
        "error", strerror(errno), K(end_str));
      ret = OB_INVALID_DATA;
    }
  }
  return ret;
}

bool is_ddl_tablet(const share::ObLSID &ls_id, const common::ObTabletID &tablet_id)
{
  return ls_id.is_sys_ls() && share::OB_ALL_DDL_OPERATION_TID == tablet_id.id();
}

bool is_all_ddl_operation_lob_aux_tablet(const share::ObLSID &ls_id, const common::ObTabletID &tablet_id)
{
  // "__all_ddl_operation_aux_lob_meta"
  return ls_id.is_sys_ls() && share::OB_ALL_DDL_OPERATION_AUX_LOB_META_TID == tablet_id.id();
}

int sort_and_unique_lsn_arr(ObLogLSNArray &lsn_arr)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> duplicated_log_idx_arr;
  palf::LSN prev_lsn;

  // sort lsn_arr
  lib::ob_sort(lsn_arr.begin(), lsn_arr.end(), CDCLSNComparator());
  // get duplicate misslog lsn idx
  for(int64_t idx = 0; OB_SUCC(ret) && idx < lsn_arr.count(); idx++) {
    palf::LSN &cur_lsn = lsn_arr[idx];
    if (prev_lsn == cur_lsn) {
      if (OB_FAIL(duplicated_log_idx_arr.push_back(idx))) {
        LOG_ERROR("push_back_duplicate_missing_log_lsn_arr fail", KR(ret), K(cur_lsn), K(prev_lsn), K(cur_lsn), K(idx));
      }
    }
    if (OB_SUCC(ret)) {
      prev_lsn = cur_lsn;
    }
  }
  // remove duplicate misslog lsn
  for(int64_t idx = duplicated_log_idx_arr.count() -1; OB_SUCC(ret) && idx >= 0; idx--) {
    int64_t duplicate_log_idx = duplicated_log_idx_arr[idx];
    if (OB_UNLIKELY(0 > duplicate_log_idx || duplicate_log_idx > lsn_arr.count())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid duplicate_cur_lsn_idx", KR(ret), K(lsn_arr), K(duplicated_log_idx_arr), K(idx), K(duplicate_log_idx));
    } else if (OB_FAIL(lsn_arr.remove(duplicate_log_idx))) {
      LOG_ERROR("remove_duplicate_misslog failed", KR(ret), K(lsn_arr), K(duplicate_log_idx));
    }
  }

  LOG_DEBUG("sort_and_unique_missing_log_lsn", KR(ret), K(duplicated_log_idx_arr), K(lsn_arr));

  return ret;
}

int write_to_file(const char *file_path, const char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  char *p = nullptr; // tmp file path
  char tmp_file[MAX_PATH_SIZE] = "";

  if (OB_ISNULL(file_path) || OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arguments", KR(ret), KP(file_path), KP(buf), K(buf_len));
  } else if (OB_NOT_NULL(p = strrchr(const_cast<char*>(file_path), '/'))) {
    // Create the corresponding directory
    char dir_buffer[OB_MAX_FILE_NAME_LENGTH];
    snprintf(dir_buffer, OB_MAX_FILE_NAME_LENGTH, "%.*s", (int)(p - file_path), file_path);
    if (OB_FAIL(common::FileDirectoryUtils::create_full_path(dir_buffer))) {
      LOG_ERROR("create_full_path for timezone_info.conf failed", KR(ret), KCSTRING(dir_buffer), KCSTRING(p));
    }
  }

  if (OB_SUCC(ret)) {
    snprintf(tmp_file, MAX_PATH_SIZE, "%s.tmp", file_path);
    FILE *fp = NULL;
    int64_t write_len = 0;

    if (OB_ISNULL(fp = fopen(tmp_file, "w+"))) {
      ret = OB_IO_ERROR;
      LOG_ERROR("open tmp_file failed", KR(ret), KERRMSG, KCSTRING(file_path), KCSTRING(tmp_file));
    } else if (OB_UNLIKELY(0 >= (write_len = fwrite(buf, 1, buf_len, fp)))) {
      ret = OB_IO_ERROR;
      LOG_ERROR("write to file failed", KR(ret), K(write_len), K(buf_len), KERRMSG);
    }

    if (OB_NOT_NULL(fp)) {
      fclose(fp);
      fp = NULL;
    }

    if (OB_SUCC(ret)) {
      // copy tmp_file to file by user when arbserver exit
      if (0 != ::rename(tmp_file, file_path)) {
        ret = OB_ERR_SYS;
        LOG_WARN("fail to move tmp config file", KERRMSG, K(ret));
      }
    }
  }

  return ret;
}

int read_from_file(const char *file_path, char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  FILE *fp = nullptr;

  if (OB_ISNULL(file_path) || OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arguments", KR(ret), KP(file_path), KP(buf), K(buf_len));
  } else if (OB_ISNULL(fp = fopen(file_path, "rb"))) {
    ret = OB_IO_ERROR;
    LOG_ERROR("can't open file", KR(ret), KCSTRING(file_path));
  } else {
    int64_t read_len = fread(buf, 1, buf_len - 1, fp);

    if (0 != ferror(fp)) {
      ret = OB_IO_ERROR;
      LOG_ERROR("read file error!", KR(ret), KCSTRING(file_path), KERRNOMSG(errno));
    } else if (0 == feof(fp)) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_ERROR("config file is too long!", KR(ret), KCSTRING(file_path), K(buf_len));
    } else if (read_len <= 0) {
      ret = OB_EMPTY_RESULT;
      LOG_WARN("config file is empty", KR(ret), KCSTRING(file_path));
    } else if (read_len >= buf_len) {
      ret = OB_SIZE_OVERFLOW;
      LOG_ERROR("fread buffer overflow", KR(ret), K(read_len), K(buf_len));
    }
  }

  if (OB_NOT_NULL(fp)) {
    fclose(fp);
    fp = NULL;
  }

  return ret;
}

int convert_to_compat_mode(const common::ObCompatibilityMode &compatible_mode,
    lib::Worker::CompatMode &compat_mode)
{
  int ret = OB_SUCCESS;
  if (common::ObCompatibilityMode::MYSQL_MODE == compatible_mode) {
    compat_mode = lib::Worker::CompatMode::MYSQL;
  } else if (common::ObCompatibilityMode::ORACLE_MODE == compatible_mode) {
    compat_mode = lib::Worker::CompatMode::ORACLE;
  } else {
    ret = OB_INVALID_DATA;
    LOG_ERROR("invalid compatible_mode", KR(ret), K(compatible_mode));
  }

  return ret;
}

//////////////////////////////////////////////////////////////////

} // namespace libocdc
} // namespace oceanbase
