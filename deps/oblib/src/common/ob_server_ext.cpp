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

#include "lib/oblog/ob_log.h"
#include "common/ob_server_ext.h"
#include "lib/ob_define.h"
using namespace oceanbase::common;

ObServerExt::ObServerExt()
{
  //memset(hostname_, '\0', OB_MAX_HOST_NAME_LENGTH);
  hostname_[0] = '\0';
  magic_num_ = reinterpret_cast<int64_t>("erverExt");
}

ObServerExt::~ObServerExt()
{
}

int ObServerExt::init(char *hname, common::ObAddr server)
{
  int ret = OB_SUCCESS;
  if (NULL == hname || static_cast<int64_t>(strlen(hname)) >= OB_MAX_HOST_NAME_LENGTH) {
    _OB_LOG(WARN, "invalid param, hname=%s", hname);
    ret = OB_INVALID_ARGUMENT;
  } else {
    MEMCPY(hostname_, hname, strlen(hname) + 1);
    if (!server_.set_ipv4_addr(server.get_ipv4(), server.get_port())) {
      ret = OB_ERR_UNEXPECTED;
      _OB_LOG(WARN, "fail to set ipv4 addr, ret=%d", ret);
    }
  }
  _OB_LOG(INFO, "magic_num=%ld", magic_num_);
  return ret;
}

const char *ObServerExt::get_hostname() const
{
  return hostname_;
}

int ObServerExt::deep_copy(const ObServerExt &server_ext)
{
  int64_t server_name_len = strlen(server_ext.get_hostname());
  MEMCPY(hostname_, server_ext.get_hostname(), server_name_len + 1);
  server_ = server_ext.get_server();
  return (server_.is_valid()) ? OB_SUCCESS: OB_ERR_UNEXPECTED;
}

char *ObServerExt::get_hostname()
{
  return hostname_;
}
int ObServerExt::set_hostname(const char *hname)
{
  int ret = OB_SUCCESS;
  if (NULL == hname || static_cast<int64_t>(strlen(hname)) >= OB_MAX_HOST_NAME_LENGTH) {
    _OB_LOG(WARN, "invalid param, hname=%s", hname);
    ret = OB_INVALID_ARGUMENT;
  } else {
    MEMCPY(hostname_, hname, strlen(hname) + 1);
  }
  return ret;
}

const ObAddr &ObServerExt::get_server()const
{
  return server_;
}

ObAddr &ObServerExt::get_server()
{
  return server_;
}

int ObServerExt::serialize(char *buf, const int64_t buf_len, int64_t &pos)const
{
  int ret = OB_SUCCESS;

  if (NULL == buf || buf_len <= 0 || pos >= buf_len) {
    _OB_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld", buf, buf_len, pos);
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t str_len = strlen(hostname_);
    if (pos  + str_len + (int64_t)sizeof(int64_t) * 2 >= buf_len) {
      _OB_LOG(WARN, "buf is not enough, pos=%ld, buf_len=%ld", pos, buf_len);
      ret = OB_ERROR;
    } else {
      *(reinterpret_cast<int64_t *>(buf + pos)) = magic_num_;
      pos += sizeof(int64_t);
      *(reinterpret_cast<int64_t *>(buf + pos)) = str_len;
      pos += sizeof(int64_t);
      strncpy(buf + pos, hostname_, str_len);
      pos += str_len;
      ret = server_.serialize(buf, buf_len, pos);
      if (OB_SUCCESS != ret) {
        _OB_LOG(WARN, "ObAddr rs_server serialize fail");
      }
    }
  }
  return ret;
}

int ObServerExt::deserialize(const char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;

  if (NULL == buf || buf_len <= 0 || pos >= buf_len) {
    _OB_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld", buf, buf_len, pos);
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t magic_num = 0;
    magic_num = *(reinterpret_cast<const int64_t *>(buf + pos));
    if (magic_num_ != magic_num) {
      ret = OB_NOT_THE_OBJECT;
      _OB_LOG(WARN, "wrong magic num, can't deserilize the buffer to ObServerExt, ret=%d", ret);
    } else {
      pos += sizeof(int64_t);
      int64_t str_len = 0;
      str_len = *(reinterpret_cast<const int64_t *>(buf + pos));
      pos += sizeof(int64_t);
      strncpy(hostname_, buf + pos, str_len);
      hostname_[str_len] = '\0';
      pos += str_len;
      ret = server_.deserialize(buf, buf_len, pos);
      if (OB_SUCCESS != ret) {
        _OB_LOG(WARN, "ObAddr rs_server deserialize fail.");
      }
    }
  }
  return ret;
}

int64_t ObServerExt::get_serialize_size(void)const
{
  return server_.get_serialize_size() + strlen(hostname_) + 2 * (int64_t)sizeof(int64_t);
}
