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

#ifndef OBDEV_SRC_SQL_ENGINE_CONNECTOR_OB_JAVA_ENV_H_
#define OBDEV_SRC_SQL_ENGINE_CONNECTOR_OB_JAVA_ENV_H_

#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_string.h"
#include "lib/lock/ob_mutex.h"
#include "lib/lock/ob_rwlock.h"
#include "lib/allocator/page_arena.h"

namespace oceanbase
{

namespace sql
{

class ObJavaEnv {
public:
  ObJavaEnv() : is_inited_(false) {
    arena_alloc_.set_attr(SET_IGNORE_MEM_VERSION(lib::ObMemAttr(OB_SYS_TENANT_ID, "JavaHomeEnv")));
  }

public:
  static ObJavaEnv &getInstance();
  ObJavaEnv &operator=(const ObJavaEnv &) = delete;

public:
  bool is_env_inited();
  int check_path_exists(const char* path, bool &found);
  int setup_java_home();
  int setup_java_opts();
  // This api will setup the connector_path and class_path
  int setup_useful_path();
  int setup_java_env();

private:
  int setup_extra_runtime_lib_path();

private:
  bool is_inited_;
  const char *java_home_;
  const char *java_opts_;
  const char *class_path_;
  const char *connector_path_;
  const char *ld_library_path_;

  bool is_inited_java_home_ = false;
  bool is_inited_java_opts_ = false;
  bool is_inited_hdfs_opts_ = false;
  bool is_inited_cls_path_ = false;
  bool is_inited_conn_path_ = false;

private:
  obsys::ObRWLock setup_env_lock_;

private:
  const char *JAVA_HOME = "JAVA_HOME";
  const char *JAVA_OPTS = "JAVA_OPTS";
  const char *CLASSPATH = "CLASSPATH";
  const char *LD_LIBRARY_PATH = "LD_LIBRARY_PATH";
  const char *LIBHDFS_OPTS = "LIBHDFS_OPTS";
  const char *CONNECTOR_PATH = "CONNECTOR_PATH";

  const char *HADOOP_LIB_PATH_PREFIX = "hadoop";
  const char *HADOOP_COMMON_LIB_PREFIX = "common";
  const char *HADOOP_HDFS_LIB_PREFIX = "hdfs";

  // Libs path in connector path, current only support ODPS libs
  const char *LIB_PATH_PREFIX = "lib";
  const char *ODPS_LIBS = "odps-connector-lib";

private:
  common::ObArenaAllocator arena_alloc_;
};

} // namespace sql
} // namespace oceanbase

#endif /* OBDEV_SRC_SQL_ENGINE_CONNECTOR_OB_JAVA_ENV_H_ */