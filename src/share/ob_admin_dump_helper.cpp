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

#include "share/ob_admin_dump_helper.h"
#include <cstdio>
#include "lib/ob_errno.h"

namespace oceanbase
{
using namespace common;
namespace share
{

int ObAdminLogNormalDumper::start_object()
{
  fprintf(stdout, " {");
  return OB_SUCCESS;
}

int ObAdminLogNormalDumper::end_object()
{
  fprintf(stdout, "} ");
  return OB_SUCCESS;
}

int ObAdminLogNormalDumper::dump_key(const char *key)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(key)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid key is NULL", K(ret));
  } else {
    fprintf(stdout, " %s:", key);
  }
  return ret;
}

int ObAdminLogNormalDumper::dump_int64(int64_t arg)
{
  int ret = OB_SUCCESS;
  fprintf(stdout, " %ld ", arg);
  return ret;
}

int ObAdminLogNormalDumper::dump_uint64(uint64_t arg)
{
  int ret = OB_SUCCESS;
  fprintf(stdout, " %lu ", arg);
  return ret;
}

int ObAdminLogNormalDumper::dump_string(const char *str)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(str)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid str is NULL", K(ret));
  } else {
    fprintf(stdout, " %s ", str);
  }
  return ret;
}

int ObAdminLogStatDumper::start_object()
{
  return OB_SUCCESS;
}

int ObAdminLogStatDumper::end_object()
{
  return OB_SUCCESS;
}

int ObAdminLogStatDumper::dump_key(const char *key)
{
  UNUSED(key);
  return OB_SUCCESS;
}

int ObAdminLogStatDumper::dump_int64(int64_t arg)
{
  UNUSED(arg);
  return OB_SUCCESS;
}

int ObAdminLogStatDumper::dump_uint64(uint64_t arg)
{
  UNUSED(arg);
  return OB_SUCCESS;
}

int ObAdminLogStatDumper::dump_string(const char *str)
{
  UNUSED(str);
  return OB_SUCCESS;
}

int ObAdminLogJsonDumper::start_object()
{
  return  rapidjson::PrettyWriter<rapidjson::StringBuffer>::StartObject() ? OB_SUCCESS : OB_ERR_UNEXPECTED;
}

int ObAdminLogJsonDumper::end_object()
{
  return  rapidjson::PrettyWriter<rapidjson::StringBuffer>::EndObject() ? OB_SUCCESS : OB_ERR_UNEXPECTED;
}

int ObAdminLogJsonDumper::dump_key(const char *arg)
{
  return  rapidjson::PrettyWriter<rapidjson::StringBuffer>::Key(arg) ? OB_SUCCESS : OB_ERR_UNEXPECTED;
}

int ObAdminLogJsonDumper::dump_int64(int64_t arg)
{
  return  rapidjson::PrettyWriter<rapidjson::StringBuffer>::Int64(arg) ? OB_SUCCESS : OB_ERR_UNEXPECTED;
}

int ObAdminLogJsonDumper::dump_uint64(uint64_t arg)
{
  return  rapidjson::PrettyWriter<rapidjson::StringBuffer>::Uint64(arg) ? OB_SUCCESS : OB_ERR_UNEXPECTED;
}

int ObAdminLogJsonDumper::dump_string(const char *arg)
{
  return  rapidjson::PrettyWriter<rapidjson::StringBuffer>::String(arg) ? OB_SUCCESS : OB_ERR_UNEXPECTED;
}

bool ObAdminLogDumpFilter::is_valid() const
{
  return (is_tx_id_valid() || is_tablet_id_valid());
}

int ObAdminLogDumpFilter::parse(const char *str)
{
  int ret = OB_SUCCESS;
  char *ptr1 = NULL;
  char *saveptr1 = NULL;
  char *token1 = NULL;
  char buf[1024];
  char tmp[128];
  const char *TABLET_ID_STR = "tablet_id";
  const char *TX_ID_STR = "tx_id";
  if (NULL != str) {
    strncpy(buf, str, sizeof(buf));
    buf[sizeof(buf) - 1] = '\0';
    for (ptr1 = buf; ;ptr1 = NULL) {
      token1 = strtok_r(ptr1, ";", &saveptr1);
      if (NULL == token1) {
        break;
      } else {
        int i = 0;
        char *ptr2 = NULL;
        char *saveptr2 = NULL;
        char *token2 = NULL;
        for (i = 1, ptr2 = token1; ;ptr2 = NULL, i++) {
          token2 = strtok_r(ptr2, "=", &saveptr2);
          if (NULL == token2) {
            break;
          } else if (1 == (i % 2)) {
            strncpy(tmp, token2, sizeof(tmp));
            tmp[sizeof(tmp) - 1] = '\0';
          } else {
            if (0 == strcmp(tmp, TABLET_ID_STR)) {
              tablet_id_ = atol(token2);
            } else if (0 == strcmp(tmp, TX_ID_STR)) {
              tx_id_ = atol(token2);
            } else {
              // do nothing
            }
          }
        }
      }
    }
  }
  return ret;
}

ObAdminLogDumpFilter &ObAdminLogDumpFilter::operator= (const ObAdminLogDumpFilter &rhs)
{

  tx_id_ = rhs.tx_id_;
  tablet_id_ = rhs.tablet_id_;
  return *this;
}

void ObLogStat::reset()
{
  group_entry_header_size_ = 0;
  log_entry_header_size_ = 0;
  log_base_header_size_ = 0;
  tx_block_header_size_ = 0;
  tx_log_header_size_ = 0;
  tx_redo_log_size_ = 0;
  mutator_size_ = 0;
  new_row_size_ = 0;
  old_row_size_ = 0;
  total_group_entry_count_ = 0;
  total_log_entry_count_ = 0;
  total_tx_log_count_ = 0;
  total_tx_redo_log_count_ = 0;
  normal_row_count_ = 0;
  table_lock_count_ = 0;
}

int64_t ObLogStat::total_size() const
{
  int64_t total_size = group_entry_header_size_
      + log_entry_header_size_
      + log_base_header_size_
      + tx_block_header_size_
      + tx_log_header_size_
      + tx_redo_log_size_;
  return total_size;
}
void ObAdminMutatorStringArg::reset()
{
  buf_ = nullptr;
  buf_len_ = 0;
  flag_ = LogFormatFlag::NO_FORMAT;
  //pos_ = 0;
  //:w
  //:wtx_id_ = 0;
  writer_ptr_ = nullptr;
  filter_.reset();
  log_stat_ = NULL;
}

void ObAdminMutatorStringArg::reset_buf()
{
  MEMSET(buf_, 0,  buf_len_);
  pos_ = 0;
}

ObAdminMutatorStringArg &ObAdminMutatorStringArg::operator= (const ObAdminMutatorStringArg &rhs)
{

  buf_ = rhs.buf_;
  buf_len_ = rhs.buf_len_;
  pos_ = rhs.pos_;
  flag_ = rhs.flag_;
  //tx_id_ = rhs.tx_id_;
  writer_ptr_ = rhs.writer_ptr_;
  filter_ = rhs.filter_;
  log_stat_ = rhs.log_stat_;
  return *this;
}

}//end of namespace share
}//end of namespace oceanbase
