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

#ifndef OB_VTOA_UTIL_H
#define OB_VTOA_UTIL_H

#include <linux/types.h>
#include <netinet/in.h>
#include "lib/hash/ob_hashmap.h"
#include <lib/net/ob_addr.h>

namespace oceanbase
{
namespace lib
{
struct vtoa_get_vs4rds;
class ObVTOAUtility
{
public:
  static int get_virtual_addr(const int connfd, bool &is_vip, int64_t &vid, ObAddr &vaddr);

private:
  static const int VTOA_BASE_CTL_VS = 64 + 1024 + 64 + 64 + 64 + 64;
  static const int VTOA_SO_GET_VS = VTOA_BASE_CTL_VS + 1;
  static const int VTOA_SO_GET_VS4RDS = VTOA_BASE_CTL_VS + 2;
  // only support get ipv4 addr for RDS product, support on vtoa 1.x.x and 2.x.x version
  static int get_vip4rds(int sockfd, struct vtoa_get_vs4rds *vs, socklen_t *len);
  ObVTOAUtility() = delete;
};

}  // namespace lib
}  // namespace oceanbase

#endif // OB_VTOA_UTIL_H