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

#define USING_LOG_PREFIX LIB
#include "ob_vtoa_util.h"
#include <sys/socket.h>
#include <sys/types.h>
#include "lib/net/ob_addr.h"
#include "lib/net/ob_net_util.h"
#include "lib/ob_define.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase
{
namespace lib
{
struct vtoa_vs
{
  __u32 vid;     // VPC ID
  __be32 vaddr;  // vip
  __be16 vport;  // vport
};

struct vtoa_get_vs4rds
{
  // which connection
  __u16 protocol;
  __be32 caddr;  // client address
  __be16 cport;
  __be32 daddr;  // destination address
  __be32 dport;

  // the virtual server
  struct vtoa_vs entrytable;
};

int ObVTOAUtility::get_vip4rds(int sockfd, struct vtoa_get_vs4rds *vs, socklen_t *len)
{
  int ret = OB_SUCCESS;

  struct sockaddr_in saddr;
  socklen_t saddrlen = sizeof(saddr);
  struct sockaddr_in daddr;
  socklen_t daddrlen = sizeof(daddr);

  if (OB_ISNULL(vs) || OB_ISNULL(len) || OB_UNLIKELY(*len != sizeof(struct vtoa_get_vs4rds))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(sockfd), KP(vs), KP(len), K(ret));
  } else if (getpeername(sockfd, reinterpret_cast<struct sockaddr *>(&saddr), &saddrlen) < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to getpeername", K(sockfd), KERRNOMSG(errno), K(ret));
  } else if (getsockname(sockfd, reinterpret_cast<struct sockaddr *>(&daddr), &daddrlen) < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to getsockname", K(sockfd), KERRNOMSG(errno), K(ret));
  } else {
    vs->protocol = IPPROTO_TCP;
    vs->caddr = saddr.sin_addr.s_addr;
    vs->cport = saddr.sin_port;
    vs->daddr = daddr.sin_addr.s_addr;
    vs->dport = daddr.sin_port;

    if (getsockopt(sockfd, IPPROTO_IP, VTOA_SO_GET_VS4RDS, vs, len) < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_DEBUG("getsockopt VTOA_SO_GET_VS4RDS may not support", K(sockfd), KERRNOMSG(errno));
    }
  }
  return ret;
}

int ObVTOAUtility::get_virtual_addr(const int connfd, bool &is_slb, int64_t &vid, ObAddr &vaddr)
{
  int ret = OB_SUCCESS;
  is_slb = false;
  vaddr.reset();
  vid = -1;
  ObAddr caddr;
  struct vtoa_get_vs4rds vs;
  socklen_t vs_len = sizeof(struct vtoa_get_vs4rds);

  if (OB_FAIL(ObVTOAUtility::get_vip4rds(connfd, &vs, &vs_len))) {
    LOG_DEBUG("fail to get_vip4rds", K(ret), KERRNOMSG(errno));
  } else {
    vid = vs.entrytable.vid;
    vaddr.set_ipv4_addr(ntohl(vs.entrytable.vaddr), ntohs(vs.entrytable.vport));
    caddr.set_ipv4_addr(ntohl(vs.caddr), ntohs(vs.cport));
  }

  ObAddr daddr(ntohl(vs.daddr), ntohs(vs.dport));

  if (OB_SUCCESS == ret) {
    is_slb = true;
    LOG_INFO("SLB connect",
        "protocol", vs.protocol,
        "fd", connfd,
        "vid", vid,
        "vaddr", vaddr,
        "caddr",caddr,
        "daddr", daddr);
  } else if (ENOPROTOOPT == errno || ESRCH == errno) {
    // VTOA not installed or no VTOA/VIP
    ret = OB_SUCCESS;
    LOG_DEBUG("direct connect",
        "protocol", vs.protocol,
        "fd", connfd,
        "caddr",caddr,
        "daddr", daddr,
        KERRNOMSG(errno));
  } else {
    LOG_WARN("fail to get vip addr", K(ret), KERRNOMSG(errno));
  }
  return ret;
}

}  // namespace lib
}  // namespace oceanbase