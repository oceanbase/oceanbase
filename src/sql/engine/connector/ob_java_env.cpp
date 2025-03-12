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
#define USING_LOG_PREFIX SQL_ENG

#include <cstdlib>
#include <dirent.h>

#include "ob_java_env.h"

#include "share/ob_errno.h"
#include "share/config/ob_server_config.h"
#include "lib/alloc/alloc_assist.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"


namespace oceanbase
{
namespace sql
{

ObJavaEnv &ObJavaEnv::getInstance()
{
  static ObJavaEnv java_env;
  return java_env;
}

bool ObJavaEnv::is_env_inited()
{
  int ret = OB_SUCCESS;
  if (!GCONF.ob_enable_java_env) {
    is_inited_ = false;
  } else {
    is_inited_ = true;
    if (!is_inited_java_home_) {
      is_inited_ = false;
      LOG_WARN("java home is not inited success", K(ret));
    }
    if (!is_inited_java_opts_) {
      is_inited_ = false;
      LOG_WARN("java opts is not inited success", K(ret));
    }
    if (!is_inited_hdfs_opts_) {
      is_inited_ = false;
      LOG_WARN("hdfs opts is not inited success", K(ret));
    }
    if (!is_inited_conn_path_) {
      is_inited_ = false;
      LOG_WARN("connector path is not inited success", K(ret));
    }
    if (!is_inited_cls_path_) {
      is_inited_ = false;
      LOG_WARN("class path is not inited success", K(ret));
    }
  }
  return is_inited_;
}

int ObJavaEnv::check_path_exists(const char* path, bool &found)
{
  int ret = OB_SUCCESS;
  DIR *dirp = NULL;
  found = false;
  if (OB_ISNULL(path) || NULL == (dirp = opendir(path))) {
    found = false;
  } else {
    // do nothing
  }
  if (OB_NOT_NULL(dirp)) {
    if (0 != closedir(dirp)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to close dirp", K(ret), KP(dirp));
    } else {
      found = true;
    }
  }
  return ret;
}

int ObJavaEnv::setup_java_home()
{
  int ret = OB_SUCCESS;
  ObString temp_java_home;
  bool found = false;
  if (OB_ISNULL(GCONF.ob_java_home)) {
    ret = OB_JNI_PARAMS_ERROR;
    LOG_WARN("ob_java_home was not configured", K(ret));
  } else if (OB_FAIL(ob_write_string(arena_alloc_,
                                     GCONF.ob_java_home.get_value_string(),
                                     temp_java_home, true))) {
    LOG_WARN("failed to copy java home variables from global conf", K(ret));
  } else if (OB_FAIL(check_path_exists(temp_java_home.ptr(), found))) {
    LOG_WARN("java home is not valid", K(ret), K(temp_java_home));
  } else if (!found) {
    ret = OB_JNI_JAVA_HOME_NOT_FOUND_ERROR;
    LOG_WARN("unable to find out java home path", K(ret), K(temp_java_home));
  } else {
    java_home_ = temp_java_home.ptr();
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(java_home_)) {
      ret = OB_JNI_PARAMS_ERROR;
      LOG_WARN("failed to setup JAVA_HOME with null vairables", K(ret));
    } else {
      if (0 != setenv(JAVA_HOME, java_home_, 1)) {
        ret = OB_JNI_PARAMS_ERROR;
        LOG_WARN("faieled to setup JAVA_HOME", K(ret), K_(java_home));
      } else if (STRCMP(java_home_, std::getenv(JAVA_HOME))) {
        ret = OB_JNI_PARAMS_ERROR;
        LOG_WARN("failed to set JAVA_HOME from varibables", K(ret), K_(java_home));
      } else {
        is_inited_java_home_ = true;
      }
    }
  }
  return ret;
}

int ObJavaEnv::setup_java_opts()
{
  int ret = OB_SUCCESS;
  ObString temp_java_opts;
  if (OB_ISNULL(GCONF.ob_java_opts)) {
    ret = OB_JNI_PARAMS_ERROR;
    LOG_WARN("ob_java_opts was not configured", K(ret));
  } else if (OB_FAIL(ob_write_string(arena_alloc_, GCONF.ob_java_opts.get_value_string(),
                                     temp_java_opts, true))) {
    LOG_WARN("failed to copy java options variables from global conf", K(ret));
  } else {
    java_opts_ = temp_java_opts.ptr();
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(java_opts_)) {
      ret = OB_JNI_JAVA_OPTS_NOT_FOUND_ERROR;
      LOG_WARN("failed to setup JAVA_OPTS with null vairables", K(ret));
    } else {
      if (0 != setenv(JAVA_OPTS, java_opts_, 1)) {
        ret = OB_JNI_JAVA_OPTS_NOT_FOUND_ERROR;
        LOG_WARN("faieled to setup JAVA_OPTS", K(ret), K_(java_opts));
      } else if (STRCMP(java_opts_, std::getenv(JAVA_OPTS))) {
        ret = OB_JNI_JAVA_OPTS_NOT_FOUND_ERROR;
        LOG_WARN("failed to set JAVA_OPTS from varibables", K(ret), K_(java_opts));
      } else {
        is_inited_java_opts_ = true;
      }
    }
  }

  // LIBHDFS_OPTS is same as JAVA_OPTS
  if (OB_SUCC(ret)) {
    if (0 != setenv(LIBHDFS_OPTS, java_opts_, 1)) {
      ret = OB_JNI_PARAMS_ERROR;
      LOG_WARN("faieled to setup LIBHDFS_OPTS", K(ret), K_(java_opts));
    } else if (STRCMP(java_opts_, std::getenv(LIBHDFS_OPTS))) {
      ret = OB_JNI_PARAMS_ERROR;
      LOG_WARN("failed to set LIBHDFS_OPTS from varibables", K(ret),
               K_(java_opts));
    } else {
      is_inited_hdfs_opts_ = true;
    }
  }
  return ret;
}

int ObJavaEnv::setup_useful_path()
{
  int ret = OB_SUCCESS;
  ObString temp_path;
  bool found = false;
  if (OB_ISNULL(GCONF.ob_java_connector_path)) {
    ret = OB_JNI_CONNECTOR_PATH_NOT_FOUND_ERROR;
    LOG_WARN("ob_java_connector_path was not configured", K(ret));
  } else if (OB_FAIL(ob_write_string(
                 arena_alloc_, GCONF.ob_java_connector_path.get_value_string(),
                 temp_path, true))) {
    LOG_WARN("failed to copy path variables from global conf", K(ret));
  } else if (OB_FAIL(check_path_exists(temp_path.ptr(), found))) {
    LOG_WARN("unable to find out connector path", K(ret), K(temp_path));
  } else {
    connector_path_ = temp_path.ptr();
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(connector_path_)) {
      ret = OB_JNI_CONNECTOR_PATH_NOT_FOUND_ERROR;
      LOG_WARN("unable to find out java connector path", K(ret));
    }

    int len = 0;
    if (OB_SUCC(ret)) {
      len = std::strlen(connector_path_);
      if (0 == len) {
        ret = OB_JNI_CONNECTOR_PATH_NOT_FOUND_ERROR;
        LOG_WARN("connector path length is 0", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
      /* do nothing */
    } else {
      // class_path should get from connector path.
      // Note: current class path should be
      // `${CONNECTOR_PATH}/hadoop/hdfs/*:${CONNECTOR_PATH}/hadoop/hdfs/lib/*`.
      ObSqlString tmp_class_path;
      ObString class_path;
      const char *fmt = nullptr;
      // Append original classpath
      if (len > 0 && connector_path_[len - 1] == '/') {
        fmt = "%s:%s*:%s%s/%s/*:%s%s/%s/%s/*:%s%s/%s/*:%s%s/%s/%s/*:%s";
      } else {
        fmt = "%s/:%s/*:%s/%s/%s/*:%s/%s/%s/%s/*:%s/%s/%s/*:%s/%s/%s/%s/*:%s";
      }

      const char *original_cp =
          OB_ISNULL(std::getenv(CLASSPATH)) ? "" : std::getenv(CLASSPATH);
      LOG_INFO("get original class path", K(ret), K(original_cp));
      if (OB_FAIL(tmp_class_path.assign_fmt(
              fmt,
              connector_path_, // ${CONNECTOR_PATH}/ setup for other conf file
              connector_path_, // ${CONNECTOR_PATH}/*
              connector_path_, HADOOP_LIB_PATH_PREFIX,
              HADOOP_COMMON_LIB_PREFIX, // ${CONNECTOR_PATH}/hadoop/common/*
              connector_path_, HADOOP_LIB_PATH_PREFIX, HADOOP_COMMON_LIB_PREFIX,
              LIB_PATH_PREFIX, // ${CONNECTOR_PATH}/hadoop/common/lib/*
              connector_path_, HADOOP_LIB_PATH_PREFIX,
              HADOOP_HDFS_LIB_PREFIX, // ${CONNECTOR_PATH}/hadoop/hdfs/*
              connector_path_, HADOOP_LIB_PATH_PREFIX, HADOOP_HDFS_LIB_PREFIX,
              LIB_PATH_PREFIX, // ${CONNECTOR_PATH}/hadoop/hdfs/lib/*,
              original_cp // Original classpath from env
              ))) {
        LOG_WARN("failed to init class path", K(ret));
      } else if (OB_FAIL(ob_write_string(arena_alloc_, tmp_class_path.string(),
                                         class_path, true))) {
        LOG_WARN("failed to write class path", K(ret), K(tmp_class_path));
      } else {
        class_path_ = class_path.ptr();
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(connector_path_)) {
      ret = OB_JNI_CONNECTOR_PATH_NOT_FOUND_ERROR;
      LOG_WARN("failed to setup CONNECTOR_PATH with null vairables", K(ret));
    } else {
      if (0 != setenv(CONNECTOR_PATH, connector_path_, 1)) {
        ret = OB_JNI_CONNECTOR_PATH_NOT_FOUND_ERROR;
        LOG_WARN("faieled to setup CONNECTOR_PATH", K(ret), K_(connector_path));
      } else if (STRCMP(connector_path_, std::getenv(CONNECTOR_PATH))) {
        ret = OB_JNI_CONNECTOR_PATH_NOT_FOUND_ERROR;
        LOG_WARN("failed to set CONNECTOR_PATH from varibables", K(ret), K_(connector_path));
      } else {
        is_inited_conn_path_ = true;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(class_path_)) {
      ret = OB_JNI_PARAMS_ERROR;
      LOG_WARN("failed to setup CLASS_PATH with null vairables", K(ret));
    } else {
      if (0 != setenv(CLASSPATH, class_path_, 1)) {
        ret = OB_JNI_PARAMS_ERROR;
        LOG_WARN("faieled to setup CONNECTOR_PATH", K(ret), K_(class_path));
      } else if (STRCMP(class_path_, std::getenv(CLASSPATH))) {
        ret = OB_JNI_PARAMS_ERROR;
        LOG_WARN("failed to set CLASS_PATH from varibables", K(ret), K_(class_path));
      } else {
        is_inited_cls_path_ = true;
      }
    }
  }
  return ret;
}

int ObJavaEnv::setup_java_env() {
  int ret = OB_SUCCESS;
  obsys::ObWLockGuard wg(setup_env_lock_);
  if (!GCONF.ob_enable_java_env) {
    ret = OB_JNI_NOT_ENABLE_JAVA_ENV_ERROR;
    LOG_WARN("observer is not enable java env", K(ret));
  } else {
    if (OB_FAIL(setup_java_home())) {
      LOG_WARN("failed to setup java home", K(ret));
    } else if (OB_FAIL(setup_java_opts())) {
      LOG_WARN("failed to setup java options", K(ret));
    } else if (OB_FAIL(setup_useful_path())) {
      LOG_WARN("failed to setup useful path", K(ret));
    } else {
      /* do nothing */
    }
  }

  const char *jh = std::getenv(JAVA_HOME);
  const char *jo = std::getenv(JAVA_OPTS);
  const char *ljo = std::getenv(LIBHDFS_OPTS);
  const char *ch = std::getenv(CONNECTOR_PATH);
  const char *cp = std::getenv(CLASSPATH);
  LOG_INFO("setup env variables", K(ret), K(jh), K(jo), K(ljo), K(ch), K(cp));
  return ret;
}

} // namespace sql
} // namespace oceanbase
