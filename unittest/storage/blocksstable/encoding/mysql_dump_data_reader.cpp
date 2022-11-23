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

#define USING_LOG_PREFIX STORAGE

#include "mysql_dump_data_reader.h"
#include "share/ob_define.h"

namespace oceanbase
{
using namespace common;

MysqlDumpDataReader::MysqlDumpDataReader() : value_cnt_(0), data_index_(0)
{
}

int MysqlDumpDataReader::init(const char *file)
{
  int ret = OB_SUCCESS;
  stream_.open(file);
  if (stream_.fail()) {
    ret = OB_ERROR;
    LOG_WARN("open file error", K(ret), K(file), K(errno));
    return ret;
  }
  bool is_insert = false;
  while (OB_SUCC(next_sql())) {
    if (cur_sql_.size() > 6 && strncmp(cur_sql_.data(), "INSERT", 6) == 0) {
      is_insert = true;
      break;
    } else {
      schema_sqls_.push_back(cur_sql_);
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  } else if (OB_FAIL(ret)) {
    LOG_WARN("init failed", K(ret));
  } else {
    if (is_insert) {
      if (OB_FAIL(parse_insert_sql())) {
        LOG_WARN("parse insert sql failed", K(ret));
      }
    }
  }
  return ret;
}

int MysqlDumpDataReader::reset()
{
  int ret = OB_SUCCESS;
  stream_.clear();
  stream_.seekg(0, std::ios::beg);
  bool is_insert = false;
  while (OB_SUCC(next_sql())) {
    if (cur_sql_.size() > 6 && strncmp(cur_sql_.data(), "INSERT", 6) == 0) {
      is_insert = true;
      break;
    }
  }
  if (is_insert) {
    if (OB_FAIL(parse_insert_sql())) {
      LOG_WARN("parse insert sql failed", K(ret));
    }
  }
  return ret;
}

int MysqlDumpDataReader::next_sql()
{
  int ret = OB_SUCCESS;
  cur_sql_.clear();
  cur_line_.clear();
  while (std::getline(stream_, cur_line_)) {
    if (cur_line_.empty()) { // skip empty line
    } else if (cur_line_.size() >= 2
        && (strncmp(cur_line_.data(), "/*", 2) == 0
            || strncmp(cur_line_.data(), "--", 2) == 0)) { // skip comment line
    } else {
      if (!cur_sql_.empty()) {
        cur_sql_.push_back('\n');
      }
      cur_sql_.append(cur_line_);
      if (cur_sql_[cur_sql_.size() - 1] == ';') {
        break;
      }
    }
    cur_line_.clear();
  }
  if (cur_sql_.empty()) {
    ret = OB_ITER_END;
  }
  return ret;
}

int MysqlDumpDataReader::next_data(std::vector<ObString> &data)
{
  int ret = OB_SUCCESS;
  while (data_index_ >= datas_.size() && OB_SUCC(ret)) {
    if (OB_FAIL(next_sql())) {
    } else if (OB_FAIL(parse_insert_sql())) {
      LOG_WARN("parse insert sql failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    data.clear();
    data.reserve(value_cnt_);
    for (int64_t i = 0; i < value_cnt_; ++i) {
      data.push_back(datas_.at(data_index_));
      data_index_++;
    }
  }
  return ret;
}

int MysqlDumpDataReader::parse_insert_sql()
{
  int ret = OB_SUCCESS;
  if (cur_sql_.size() < 6 || strncmp(cur_sql_.c_str(), "INSERT", 6) != 0) {
    LOG_INFO("not insert sql, ignore", K(ret), K(cur_sql_.c_str()));
  } else {
    char *sql = const_cast<char *>(cur_sql_.c_str());
    value_cnt_ = 0;
    data_index_ = 0;
    datas_.clear();
    const char *begin_str = " VALUES (";
    char *p = strstr(sql, begin_str);
    if (NULL == sql) {
      ret = OB_ERROR;
      LOG_WARN("value begin mark not found", K(ret), K(begin_str), K(sql));
      return ret;
    }
    p += strlen(begin_str);
    bool quote = false;
    char *begin = NULL;
    p = sql;
    while (*p) {
      if (quote) {
        switch (*p) {
          case '\\':
            p++;
            if (!*p) {
              ret = OB_ERROR;
              LOG_WARN("unterminated string", K(ret));
              return ret;
            }
            p++;
            break;
          case '\'':
            quote = false;
            p++;
            datas_.push_back(ObString(p - begin, begin));
            begin = NULL;
            break;
          default:
            p++;
        }
      } else {
        switch (*p) {
          case '(':
          case ';':
            begin = NULL;
            *p = '\0';
            p++;
            break;
          case ')':
          case ',':
            if (NULL != begin) {
              datas_.push_back(ObString(p - begin, begin));
              begin = NULL;
            }
            if (value_cnt_ == 0 && *p == ')') {
              value_cnt_ = datas_.size();
            }
            *p = '\0';
            p++;
            break;
          case '\'':
            quote = true;
            // intentionally no break
          default:
            if (NULL == begin) {
              begin = p;
            }
            p++;
        }
      }
    }
    LOG_DEBUG("parsed data", K(value_cnt_), K(datas_.size()));
  }
  return ret;
}

} // end namespace oceanbase
