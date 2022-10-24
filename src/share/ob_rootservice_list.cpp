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

#define USING_LOG_PREFIX SHARE

#include "ob_rootservice_list.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "share/ob_errno.h"
#include "lib/string/ob_sql_string.h"

namespace oceanbase
{

namespace share
{

int ObRootServiceList::assign(const common::ObIArray<common::ObAddr> &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(rootservice_list_.assign(other))) {
    LOG_WARN("fail to assign", KR(ret), K(other));
  }
  return ret;
}

int ObRootServiceList::assign(const ObRootServiceList &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(rootservice_list_.assign(other.get_rs_list_arr()))) {
      LOG_WARN("fail to assign", KR(ret), K(other));
    }
  }
  return ret;
}

bool ObRootServiceList::is_valid() const
{
  return 0 < rootservice_list_.count();
}


int ObRootServiceList::parse_from_string(ObString &server_list_str)
{
  int ret = OB_SUCCESS;
  rootservice_list_.reset();
  ObArray<ObString> addr_string;

  ObString trimed_string = server_list_str.trim();
  if (OB_FAIL(split_on(trimed_string, ';', addr_string))) {
    LOG_WARN("fail to split string", KR(ret), K(trimed_string), K(server_list_str));
  } else {
    for (int64_t i = 0; i < addr_string.count() && OB_SUCC(ret); i++) {
      ObAddr addr;
      if (OB_FAIL(addr.parse_from_string(addr_string.at(i)))) {
        LOG_WARN("fail to parse from string", KR(ret), K(i), "string", addr_string.at(i));
      } else if (OB_FAIL(rootservice_list_.push_back(addr))) {
        LOG_WARN("fail to push back", KR(ret), K(addr), K_(rootservice_list));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (rootservice_list_.count() <= 0) {
    LOG_INFO("cluster rootservice list is empty", K(rootservice_list_), K(server_list_str));
  }

  return ret;
}

int ObRootServiceList::rootservice_list_to_str(ObSqlString &server_list_str) const
{
  int ret = OB_SUCCESS;
  server_list_str.reuse();

  for (int64_t i = 0; OB_SUCC(ret) && i < rootservice_list_.count(); ++i) {
    char ip_buf[OB_IP_STR_BUFF] = "";
    ObAddr addr = rootservice_list_.at(i);

    const bool need_append_delimiter = (i != rootservice_list_.count() - 1);

    if (OB_UNLIKELY(!addr.ip_to_string(ip_buf, sizeof(ip_buf)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("convert ip to string failed", KR(ret), K(addr));
    } else if (OB_FAIL(server_list_str.append_fmt("%s%s%s:%d%s",
        addr.using_ipv4() ? "" : "[",
        ip_buf,
        addr.using_ipv4() ? "" : "]",
        addr.get_port(),
        need_append_delimiter ? ";" : ""))) {
      LOG_WARN("fail to append_fmt", KR(ret), K(server_list_str), K(addr), K(need_append_delimiter));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (server_list_str.length() <= 0) {
    LOG_INFO("cluster rootservice list is empty", K(rootservice_list_), K(server_list_str));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObRootServiceList, rootservice_list_);

} // namespace share
} // namespace oceanbase
