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

#ifndef OCEANBASE_UNITTEST_CLUSTER_OB_UTILS_H
#define OCEANBASE_UNITTEST_CLUSTER_OB_UTILS_H

#include <sys/stat.h>
#include <sys/types.h>
#include <gmock/gmock.h>
#include <unistd.h>
#include <sys/types.h>          /* See NOTES */
#include <sys/socket.h>
#include <netinet/in.h>
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/time/ob_time_utility.h"

namespace oceanbase
{
namespace unittest
{

std::string __attribute__ ((weak)) _executeShellCommand(std::string command)
{
  char buffer[256];
  std::string result = "";
  const char * cmd = command.c_str();
  FILE* pipe = popen(cmd, "r");
  if (!pipe) throw std::runtime_error("popen() failed!");
    try {
        while (!feof(pipe))
            if (fgets(buffer, 128, pipe) != NULL)
                result += buffer;
    } catch (...) {
        pclose(pipe);
        throw;
    }
  pclose(pipe);
  return result;
}

bool __attribute__ ((weak)) _isAvailablePort(unsigned short usPort)
{
  char shellCommand[256], pcPort[6];
  sprintf(shellCommand, "netstat -lntu | awk '{print $4}' | grep ':' | cut -d \":\" -f 2 | sort | uniq | grep %hu", usPort);
  sprintf(pcPort, "%hu", usPort);

  std::string output =  _executeShellCommand(std::string(shellCommand));

  if(output.find(std::string(pcPort)) != std::string::npos)
    return false;
  else
    return true;
}

int __attribute__ ((weak)) listen_occupy_port(int port, int &server_fd_ret)
{
  struct sockaddr_in address;
  int opt = 1;
  int server_fd;
  if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
    perror("socket failed");
    return -1;
  }
  memset(&address, '0', sizeof(address));

  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR,
                                                  &opt, sizeof(opt))) {
    perror("setsockopt");
    return -1;
  }
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = INADDR_ANY;
  address.sin_port = htons(port);

  if (bind(server_fd, (struct sockaddr *)&address,
                                 sizeof(address))<0) {
    perror("bind failed");
    return -1;
  }
  if (listen(server_fd, 3) < 0) {
    perror("listen");
    return -1;
  }
  server_fd_ret = server_fd;
  return 0;
}

void __attribute__ ((weak)) get_netport_range(int &start_port, int &end_port)
{
  char *mit_network_start_port_env = getenv("mit_network_start_port");
  char *mit_network_port_num_env = getenv("mit_network_port_num");
  if (mit_network_start_port_env != nullptr && mit_network_port_num_env != nullptr) {
    start_port = atoi(mit_network_start_port_env);
    end_port = atoi(mit_network_port_num_env) + start_port;
    start_port = (start_port+3) / 3 * 3;
    STORAGE_LOG(INFO, "get netport from env", K(start_port), K(end_port));
  } else {
    //srand(oceanbase::common::ObTimeUtility::current_time());
    start_port = 11000;
    end_port = start_port + 100;
    STORAGE_LOG(INFO, "get netport rand", K(start_port), K(end_port));
  }
}

int64_t __attribute__ ((weak)) get_rpc_port(int &server_fd_ret)
{
  int find_port = 0;
  server_fd_ret = 0;
  int server_fd = -1;
  int server_fd1 = -1;
  int server_fd2 = -1;
  int start_port = 0;
  int end_port = 0;
  get_netport_range(start_port, end_port);
  for (int port = start_port; port + 2 < end_port; port = port + 3) {
    if (_isAvailablePort(port) && _isAvailablePort(port + 1) && _isAvailablePort(port + 2)) {
      bool can_use = false;
      bool can_use1 = false;
      bool can_use2 = false;
      can_use = (listen_occupy_port(port, server_fd) == 0);
      if (can_use) {
        can_use1 = (listen_occupy_port(port+1, server_fd1) == 0);
        if (can_use1) {
          can_use2 = (listen_occupy_port(port+2, server_fd2) == 0);
        }
      }
      if (can_use && can_use1 && can_use2) {
        find_port = port + 1;
        server_fd_ret = server_fd;
        close(server_fd1);
        close(server_fd2);
        break;
      } else {
        STORAGE_LOG(INFO, "find port fail continue try", K(can_use), K(can_use1), K(can_use2), K(port));
        if (can_use) {
          close(server_fd);
        }
        if (can_use1) {
          close(server_fd1);
        }
        if (can_use2) {
          close(server_fd2);
        }
        sleep(1);
      }
    }
  }
  STORAGE_LOG(INFO, "find port", K(find_port));
  if (find_port == 0) {
    STORAGE_LOG_RET(WARN, OB_ERR_UNEXPECTED, "find port fail", K(find_port));
    STORAGE_LOG_RET(WARN, OB_ERR_UNEXPECTED, "net", "ss", _executeShellCommand("ss -antlp").c_str());
    OB_ASSERT(false);
  }
  return find_port;
}
}
}
#endif
