/**
 * Copyright (c) 2025 OceanBase
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

#include <sys/types.h>
#include <dirent.h>
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/utility.h"
#include "lib/oblog/ob_log.h"
#include "share/ob_errno.h"
#include "share/io/ob_io_manager.h"
#include "share/resource_manager/ob_resource_plan_info.h"
#include "share/resource_manager/ob_resource_manager.h"
#include "lib/resource/ob_affinity_ctrl.h"

using namespace oceanbase::common;
using namespace oceanbase::lib;

ObAffinityCtrl::ObAffinityCtrl()
  : num_nodes_(0), inited_(false)
{}

int ObAffinityCtrl::init()
{
  int ret = OB_SUCCESS;
  DIR *dir;
  struct dirent *de;

  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObAffnityCtrl already inited");
  } else if (NULL == (dir = opendir("/sys/devices/system/node"))){
    LOG_WARN("/sys/devices/system/node open failed", K(errno));
    ret = OB_ERR_UNEXPECTED;
  } else {
    while ((de = readdir(dir)) != NULL) {
      int node_idx;
      if (strncmp(de->d_name, "node", 4)) {
      } else {
        node_idx = strtoul(de->d_name + 4, NULL, 0);
        if (num_nodes_ < node_idx)
          num_nodes_ = node_idx;
      }
    }
    closedir(dir);
    num_nodes_ += 1;

    if (num_nodes_ > OB_MAX_NUMA_NUM) {
      LOG_WARN("ObAffnityCtrl init failed, num_nodes_ too large", K(num_nodes_), K(OB_MAX_NUMA_NUM));
      num_nodes_ = 0;
      ret = OB_ERR_UNEXPECTED;
    } else {
      for (int node = 0; node < num_nodes_; node++) {
        char path[128];

        snprintf(path, sizeof(path), "/sys/devices/system/node/node%d", node);
        if (NULL == (dir = opendir(path))) {
          LOG_ERROR("NUMA node dir open failed", K(path), K(errno));
          num_nodes_ = 0;
          ret = OB_ERR_UNEXPECTED;
        } else {
          LOG_INFO("Detected node topology", K(node));
          CPU_ZERO(&nodes_[node].cpu_set_mask);
          while ((de = readdir(dir))) {
            if (de->d_type == DT_LNK && strncmp(de->d_name, "cpu", 3) == 0) {
              int cpu;
              if (sscanf(de->d_name + 3, "%d", &cpu) == 1) {
                LOG_INFO("   cpu ", K(cpu));
                CPU_SET(cpu, &nodes_[node].cpu_set_mask);
              }
            }
          }
          closedir(dir);
          inited_ = true;
        }
      }
    }
  }

  return ret;
}

ObAffinityCtrl &ObAffinityCtrl::get_instance() {
  static ObAffinityCtrl affi_ctrl;

  return affi_ctrl;
}

int ObAffinityCtrl::run_on_node(const int node) {
  int ret = OB_SUCCESS;

  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (node >= num_nodes_ || node < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (-1 == ::sched_setaffinity(0, sizeof(cpu_set_t), &nodes_[node].cpu_set_mask)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("sched_setaffinity syscall error", K(ret), K(errno), K(node));
  } else {
    get_tls_node() = node;
  }

  return ret;
}

int ObAffinityCtrl::thread_bind_to_node(const int node_hint) {
  int ret = OB_SUCCESS;

  if (!inited_ || 0 == num_nodes_) {
    ret = OB_NOT_INIT;
  } else {
    int to_bind_node = node_hint % num_nodes_;
    ret = run_on_node(to_bind_node);
  }

  return ret;
}

static inline int call_mbind(void *addr, const size_t len, const unsigned long nodemask, const int mode, const unsigned int flag) {
  int ret = OB_SUCCESS;

  if (-1 == ::syscall(__NR_mbind, addr, len, mode, &nodemask, OB_MAX_NUMA_NUM, flag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("mbind memory failed", K(ret), K(errno), K(nodemask), K(mode), K(addr));
  }

  return ret;
}

int ObAffinityCtrl::memory_move_to_node(void *addr, const size_t len, const int node) {
  int ret = OB_SUCCESS;
  unsigned long nodemask;

  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (node >= num_nodes_ || node < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    nodemask = 1 << node;
    ret = call_mbind(addr, len, nodemask, MPOL_PREFERRED, MPOL_MF_MOVE_ALL);
  }

  return ret;
}

int ObAffinityCtrl::memory_bind_to_node(void *addr, const size_t len, const int node) {
  int ret = OB_SUCCESS;
  unsigned long nodemask;

  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (node >= num_nodes_ || node < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    nodemask = 1 << node;
    ret = call_mbind(addr, len, nodemask, MPOL_PREFERRED, 0);
  }

  return ret;
}

int ObAffinityCtrl::memory_set_interleave(void *addr, const size_t len) {
  int ret = OB_SUCCESS;

  if (!inited_) {
    ret = OB_NOT_INIT;
  } else {
    ret = call_mbind(addr, len, OB_ALL_NUMA_NODEMASK, MPOL_INTERLEAVE, 0);
  }

  return ret;
}
