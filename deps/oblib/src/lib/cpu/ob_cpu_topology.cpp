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

#include "lib/cpu/ob_cpu_topology.h"

#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include "lib/ob_define.h"

using namespace oceanbase::common;

ObCpuTopology::ObCpuTopology() : core_number_(0), node_number_(0), nodes_(), cores_map_(), init_(false)
{
  for (int64_t i = 0; i < MAX_NODE_NUMBER; i++) {
    nodes_[i].core_number_ = 0;
  }
}

int ObCpuTopology::init()
{
  int err = OB_SUCCESS;
  FILE* fp = NULL;
  if ((fp = popen("lscpu -p", "r")) == NULL) {
    LIB_LOG(ERROR, "to get cpu topology error");
  } else {
    char buf[BUFSIZ];
    int64_t core_id = 0;
    int64_t node_id = 0;
    while (NULL != fgets(buf, BUFSIZ, fp)) {
      if (buf[0] == '#') {
        continue;
      }
      char* p = strchr(buf, ',');
      *p = '\0';
      core_id = atoll(buf);
      p = strchr(p + 1, ',');
      char* node_id_str = p + 1;
      p = strchr(node_id_str, ',');
      *p = '\0';
      node_id = atoll(node_id_str);
      if (node_id + 1 > node_number_) {
        node_number_ = node_id + 1;
      }
      if (core_id + 1 > core_number_) {
        core_number_ = core_id + 1;
      }
      if (core_id >= 0 && core_id < MAX_CORE_NUMBER && node_id >= 0 && node_id < MAX_NODE_NUMBER &&
          nodes_[node_id].core_number_ >= 0 && nodes_[node_id].core_number_ < MAX_CORE_NUMBER_PER_NODE &&
          core_number_ < MAX_CORE_NUMBER && node_number_ < MAX_NODE_NUMBER) {
        nodes_[node_id].cores_[nodes_[node_id].core_number_++] = core_id;
        cores_map_[core_id] = node_id;
      } else {
        LIB_LOG(ERROR, "Too many cores", K_(core_number), K_(node_number));
      }
    }
    pclose(fp);
    fp = NULL;

    init_ = true;

    for (int64_t i = 0; i < node_number_; i++) {
      int pos = 0;
      for (int64_t j = 0; j < nodes_[i].core_number_; j++) {
        pos += snprintf(buf + pos, BUFSIZ - pos, "%ld,", nodes_[i].cores_[j]);
      }
      _LIB_LOG(INFO, "node_id: %ld core_list: %s", i, buf);
    }
    if (core_number_ > 0 && core_number_ <= MAX_CORE_NUMBER) {
      for (int64_t i = 0; i < core_number_; i++) {
        _LIB_LOG(INFO, "core_id: %ld  ==>  node_id: %ld", i, cores_map_[i]);
      }
    }
  }
  return err;
}

int64_t ObCpuTopology::get_core_num()
{
  return core_number_;
}

int64_t ObCpuTopology::get_node_num()
{
  return node_number_;
}

int64_t ObCpuTopology::get_thread_node_id()
{
  return get_tl_info().node_id_;
}

int64_t ObCpuTopology::get_thread_core_id()
{
  return get_tl_info().core_id_;
}

ObCpuTopology::CoreInfo* ObCpuTopology::get_cores_by_node(int64_t node_id)
{
  CoreInfo* core_info = NULL;
  if (node_id < node_number_) {
    core_info = &nodes_[node_id];
  }
  return core_info;
}

void ObCpuTopology::bind_cpu(uint64_t core_id)
{
  ThreadLocalInfo pre_bind_info = get_tl_info();
  if (core_id < static_cast<uint64_t>(core_number_)) {
    get_tl_info().node_id_ = cores_map_[core_id];
    get_tl_info().core_id_ = core_id;
    get_tl_info().valid_ = 1;
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
  } else {
    LIB_LOG(ERROR, "bind_cpu get an invalid", K(core_id));
  }
  if (pre_bind_info.valid_ == 1) {
    _LIB_LOG(INFO,
        "bind_cpu tid=%ld core_id=%ld node_id=%ld, "
        "replaced bind info core_id=%ld node_id=%ld",
        syscall(__NR_gettid),
        core_id,
        cores_map_[core_id],
        pre_bind_info.core_id_,
        pre_bind_info.node_id_);
  } else {
    LIB_LOG(INFO, "bind_cpu", K(syscall(__NR_gettid)), K(core_id), K(cores_map_[core_id]));
  }
}

ObCpuTopology::ThreadLocalInfo& ObCpuTopology::get_tl_info()
{
  // Note: Thread CPU binding information
  static __thread ThreadLocalInfo tl_info_;
  return tl_info_;
}

namespace oceanbase {
namespace common {

int64_t __attribute__((weak)) get_cpu_count()
{
  return get_cpu_num();
}
}  // namespace common
}  // namespace oceanbase
