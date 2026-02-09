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
namespace common
{

ObJavaEnv &ObJavaEnv::getInstance()
{
  static ObJavaEnv java_env;
  return java_env;
}

bool ObJavaEnv::is_env_inited()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
  } else if (!GCONF.ob_enable_java_env) {
    is_inited_ = false;
  } else {
    is_inited_ = true;
    if (!is_inited_java_home_) {
      is_inited_ = false;
    }
    if (!is_inited_java_opts_) {
      is_inited_ = false;
    }
    if (!is_inited_hdfs_opts_) {
      is_inited_ = false;
    }
    if (!is_inited_conn_path_) {
      is_inited_ = false;
    }
    if (!is_inited_cls_path_) {
      is_inited_ = false;
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
  if (OB_UNLIKELY(is_inited_java_home_)) {
    // java home is inited by other thread
  } else if (OB_ISNULL(GCONF.ob_java_home) || GCONF.ob_java_home.get_value_string().empty()) {
    // try to find java home int std::getenv("JAVA_HOME")
    const char *java_home = std::getenv("JAVA_HOME");
    if (OB_ISNULL(java_home)) {
      ret = OB_JNI_JAVA_HOME_NOT_FOUND_ERROR;
      LOG_WARN("JAVA_HOME is not set", K(ret));
      LOG_USER_ERROR(OB_JNI_JAVA_HOME_NOT_FOUND_ERROR, "JAVA_HOME is not set");
    } else if (OB_FAIL(ob_write_string(arena_alloc_, ObString(java_home), temp_java_home, true))) {
      LOG_WARN("failed to copy JAVA_HOME from env", K(ret));
    } else if (OB_FAIL(check_path_exists(temp_java_home.ptr(), found))) {
      LOG_WARN("JAVA_HOME is not valid", K(ret), K(temp_java_home));
      LOG_USER_ERROR(OB_JNI_JAVA_HOME_NOT_FOUND_ERROR, temp_java_home.ptr());
    } else {
      java_home_ = temp_java_home.ptr();
      is_inited_java_home_ = true;
    }
  } else {
    if (OB_FAIL(ob_write_string(arena_alloc_,
                                     GCONF.ob_java_home.get_value_string(),
                                     temp_java_home, true))) {
      LOG_WARN("failed to copy java home variables from global conf", K(ret));
    } else if (OB_FAIL(check_path_exists(temp_java_home.ptr(), found))) {
      LOG_WARN("java home is not valid", K(ret), K(temp_java_home));
    } else if (!found) {
      ret = OB_JNI_JAVA_HOME_NOT_FOUND_ERROR;
      LOG_WARN("java home is not valid", K(ret), K(temp_java_home));
      LOG_USER_ERROR(OB_JNI_JAVA_HOME_NOT_FOUND_ERROR, temp_java_home.ptr());
    } else {
      java_home_ = temp_java_home.ptr();
      if (OB_ISNULL(java_home_)) {
        ret = OB_JNI_PARAMS_ERROR;
        LOG_WARN("failed to setup JAVA_HOME with null variables", K(ret), K(GCONF.ob_java_home.get_value_string()));
        LOG_USER_WARN(OB_JNI_PARAMS_ERROR, "failed to setup JAVA_HOME with null variables", K(ret));
      } else if (0 != setenv(JAVA_HOME, java_home_, 1)) {
        ret = OB_JNI_PARAMS_ERROR;
        LOG_WARN("faieled to setup JAVA_HOME", K(ret), K_(java_home));
      } else if (STRCMP(java_home_, std::getenv(JAVA_HOME))) {
        ret = OB_JNI_PARAMS_ERROR;
        LOG_WARN("failed to set JAVA_HOME from variables", K(ret), K_(java_home));
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
  if (OB_ISNULL(GCONF.ob_java_opts) || GCONF.ob_java_opts.get_value_string().empty()) {
    const char *java_opts = std::getenv("JAVA_OPTS");
    if (OB_ISNULL(java_opts)) {
      ret = OB_JNI_JAVA_OPTS_NOT_FOUND_ERROR;
      LOG_WARN("JAVA_OPTS is not set", K(ret));
      LOG_USER_ERROR(OB_JNI_JAVA_OPTS_NOT_FOUND_ERROR, "JAVA_OPTS is not set");
    } else if (OB_FAIL(ob_write_string(arena_alloc_, ObString(java_opts), temp_java_opts, true))) {
      LOG_WARN("failed to copy JAVA_OPTS from env", K(ret));
    } else {
      java_opts_ = temp_java_opts.ptr();
    }
  } else {
    if (OB_FAIL(ob_write_string(arena_alloc_, GCONF.ob_java_opts.get_value_string(),
                                      temp_java_opts, true))) {
      LOG_WARN("failed to copy java options variables from global conf", K(ret));
    } else {
      java_opts_ = temp_java_opts.ptr();
    }
  }

  if (OB_SUCC(ret) && OB_LIKELY(!is_inited_java_opts_)) {
    if (OB_ISNULL(java_opts_)) {
      ret = OB_JNI_JAVA_OPTS_NOT_FOUND_ERROR;
      LOG_WARN("failed to setup JAVA_OPTS with null variables", K(ret));
    } else {
      if (0 != setenv(JAVA_OPTS, java_opts_, 1)) {
        ret = OB_JNI_JAVA_OPTS_NOT_FOUND_ERROR;
        LOG_WARN("faieled to setup JAVA_OPTS", K(ret), K_(java_opts));
      } else if (STRCMP(java_opts_, std::getenv(JAVA_OPTS))) {
        ret = OB_JNI_JAVA_OPTS_NOT_FOUND_ERROR;
        LOG_WARN("failed to set JAVA_OPTS from variables", K(ret), K_(java_opts));
      } else {
        is_inited_java_opts_ = true;
      }
    }
  }

  // LIBHDFS_OPTS is same as JAVA_OPTS
  if (OB_SUCC(ret) && OB_LIKELY(!is_inited_hdfs_opts_)) {
    if (0 != setenv(LIBHDFS_OPTS, java_opts_, 1)) {
      ret = OB_JNI_PARAMS_ERROR;
      LOG_WARN("faieled to setup LIBHDFS_OPTS", K(ret), K_(java_opts));
    } else if (STRCMP(java_opts_, std::getenv(LIBHDFS_OPTS))) {
      ret = OB_JNI_PARAMS_ERROR;
      LOG_WARN("failed to set LIBHDFS_OPTS from variables", K(ret),
               K_(java_opts));
    } else {
      is_inited_hdfs_opts_ = true;
    }
  }
  return ret;
}

int ObJavaEnv::find_jni_packages_in_ld_library_path(ObString &out_path)
{
  int ret = OB_SUCCESS;
  bool found = false;
  const char *ld_library_path = std::getenv("LD_LIBRARY_PATH");
  if (OB_ISNULL(ld_library_path)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("LD_LIBRARY_PATH is not set", K(ret));
  } else {
    ObSqlString ld_path_copy;
    if (OB_FAIL(ld_path_copy.assign(ld_library_path))) {
      LOG_WARN("failed to copy LD_LIBRARY_PATH", K(ret));
    } else {
      char *saveptr = NULL;
      char *path_token = strtok_r(ld_path_copy.ptr(), ":", &saveptr);
      while (OB_SUCC(ret) && !found && path_token != NULL) {
        ObSqlString jni_path;
        if (OB_FAIL(jni_path.append_fmt("%s/jni_packages/current", path_token))) {
          LOG_WARN("failed to build jni_packages path", K(ret));
        } else if (OB_FAIL(check_path_exists(jni_path.ptr(), found))) {
          LOG_WARN("failed to check jni_packages path", K(ret), K(jni_path));
        } else if (found) {
          if (OB_FAIL(ob_write_string(arena_alloc_, ObString(jni_path.ptr()), out_path, true))) {
            LOG_WARN("failed to copy jni_packages path", K(ret));
          } else {
            LOG_INFO("found jni_packages in LD_LIBRARY_PATH", K(jni_path));
          }
        }
        path_token = strtok_r(NULL, ":", &saveptr);
      }
      if (OB_SUCC(ret) && !found) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("jni_packages/current not found in LD_LIBRARY_PATH", K(ret));
      }
    }
  }
  return ret;
}

int ObJavaEnv::setup_useful_path()
{
  int ret = OB_SUCCESS;
  ObString temp_path;
  bool found = false;
  if (OB_ISNULL(GCONF.ob_java_connector_path) || GCONF.ob_java_connector_path.get_value_string().empty()) {
    // 尝试在 LD_LIBRARY_PATH 下查找 jni_packages/current
    if (OB_FAIL(find_jni_packages_in_ld_library_path(temp_path))) {
      ret = OB_JNI_CONNECTOR_PATH_NOT_FOUND_ERROR;
      LOG_WARN("ob_java_connector_path was not configured and jni_packages/current not found in LD_LIBRARY_PATH", K(ret));
    } else {
      connector_path_ = temp_path.ptr();
      found = true;
    }
  }
  if (!found) { // we can cover error code here
    if (OB_FAIL(ob_write_string(
                 arena_alloc_, GCONF.ob_java_connector_path.get_value_string(),
                 temp_path, true))) {
      LOG_WARN("failed to copy path variables from global conf", K(ret));
    } else if (OB_FAIL(check_path_exists(temp_path.ptr(), found))) {
      LOG_WARN("unable to find out connector path", K(ret), K(temp_path));
    } else if (!found) {
      ret = OB_JNI_CONNECTOR_PATH_NOT_FOUND_ERROR;
      LOG_WARN("unable to find out java connector path ob_java_connector_path should be set", K(ret));
    } else {
      connector_path_ = temp_path.ptr();
    }
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
        fmt = "%s:%s*:%s%s/%s/*:%s%s/%s/%s/*:%s%s/%s/*:%s%s/%s/%s/*:%s%s/*:%s";
      } else {
        fmt = "%s/:%s/*:%s/%s/%s/*:%s/%s/%s/%s/*:%s/%s/%s/*:%s/%s/%s/%s/*:%s/%s/*:%s";
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
              connector_path_, JAVA_UDF_PREFIX, // ${CONNECTOR_PATH}/java-udf/*
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
  if (OB_FAIL(ret)) {
  } else {
    ObSqlString hadoop_jar_path;
    if (OB_FAIL(hadoop_jar_path.append_fmt("%s/%s", connector_path_, HADOOP_LIB_PATH_PREFIX))) {
    } else if (OB_FAIL(check_path_exists(hadoop_jar_path.ptr(), found))) {
      LOG_WARN("can't open dir path", K(hadoop_jar_path), K(ret));
    } else if (!found) {
      ret = OB_JNI_CONNECTOR_PATH_NOT_FOUND_ERROR;
      LOG_USER_ERROR(OB_JNI_CONNECTOR_PATH_NOT_FOUND_ERROR, "can't open hadoop class jar path");
      LOG_WARN("can't open hadoop class jar path", K(connector_path_), K(ret));
    } else {
      // found hadoop jar path
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(connector_path_)) {
      ret = OB_JNI_CONNECTOR_PATH_NOT_FOUND_ERROR;
      LOG_WARN("failed to setup CONNECTOR_PATH with null variables", K(ret));
    } else {
      if (0 != setenv(CONNECTOR_PATH, connector_path_, 1)) {
        ret = OB_JNI_CONNECTOR_PATH_NOT_FOUND_ERROR;
        LOG_WARN("faieled to setup CONNECTOR_PATH", K(ret), K_(connector_path));
      } else if (STRCMP(connector_path_, std::getenv(CONNECTOR_PATH))) {
        ret = OB_JNI_CONNECTOR_PATH_NOT_FOUND_ERROR;
        LOG_WARN("failed to set CONNECTOR_PATH from variables", K(ret), K_(connector_path));
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

int ObJavaEnv::setup_extra_runtime_lib_path()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCONF._ob_additional_lib_path)) {
    ret = OB_JNI_PARAMS_ERROR;
    LOG_WARN("extra java runtime lib path is null", K(ret));
  } else {
    const char *extra_lib_path = GCONF._ob_additional_lib_path.get_value();
    if (OB_ISNULL(extra_lib_path)) {
      ret = OB_JNI_PARAMS_ERROR;
      LOG_WARN("extra java runtime lib path is null", K(ret));
    } else {
      const char *original_ld_lib_path = std::getenv(LD_LIBRARY_PATH);
      ObSqlString final_ld_lib_path;
      ObString tmp_ld_library_path;
      if (OB_FAIL(OB_NOT_NULL(original_ld_lib_path)
                  && final_ld_lib_path.append_fmt("%s:", original_ld_lib_path))) {
        LOG_WARN("failed to append original ld library path", K(ret), K(original_ld_lib_path));
      } else if (OB_FAIL(final_ld_lib_path.append_fmt("%s:", extra_lib_path))) {
        LOG_WARN("failed to append extra java runtime lib path",
                 K(ret),
                 K(extra_lib_path),
                 K(original_ld_lib_path));
      } else if (OB_FAIL(ob_write_string(arena_alloc_,
                                         final_ld_lib_path.string(),
                                         tmp_ld_library_path,
                                         true /*c_style*/))) {
        LOG_WARN("failed to write final ld library path", K(ret));
      } else {
        ld_library_path_ = tmp_ld_library_path.ptr();
      }
    }

    if (OB_FAIL(ret)) {
    } else if (0 != setenv(LD_LIBRARY_PATH, ld_library_path_, 1)) {
      ret = OB_JNI_PARAMS_ERROR;
      LOG_WARN("failed to setup LD_LIBRARY_PATH", K(ret), K_(ld_library_path));
    } else if (STRCMP(ld_library_path_, std::getenv(LD_LIBRARY_PATH))) {
      ret = OB_JNI_PARAMS_ERROR;
      LOG_WARN("failed to set LD_LIBRARY_PATH from variables", K(ret), K_(ld_library_path));
    } else {
      LOG_INFO("succ to setup LD_LIBRARY_PATH", K(ret), K_(ld_library_path));
    }
  }
  return ret;
}

int ObJavaEnv::setup_java_env()
{
  int ret = OB_SUCCESS;
  obsys::ObWLockGuard<> wg(setup_env_lock_);
  if (!GCONF.ob_enable_java_env) {
    ret = OB_JNI_NOT_ENABLE_JAVA_ENV_ERROR;
    LOG_WARN("observer is not enable java env", K(ret));
  } else {
    if (OB_FAIL(setup_java_home())) {
      LOG_WARN("failed to setup java home", K(ret));
    } else if (OB_FAIL(setup_java_opts())) {
      LOG_WARN("failed to setup java options", K(ret));
    }
  }

  jh_ = std::getenv(JAVA_HOME);
  jo_ = std::getenv(JAVA_OPTS);

  LOG_INFO("setup basic java variables", K(ret), K(jh_), K(jo_));
  return ret;
}

int ObJavaEnv::setup_java_env_classpath_and_ldlib_path()
{
  int ret = OB_SUCCESS;
  obsys::ObWLockGuard<> wg(setup_env_lock_);
  if (!GCONF.ob_enable_java_env) {
    ret = OB_JNI_NOT_ENABLE_JAVA_ENV_ERROR;
    LOG_WARN("observer is not enable java env", K(ret));
  } else {
    if (OB_FAIL(setup_useful_path())) {
      LOG_WARN("failed to setup useful path", K(ret));
    } else if (OB_FAIL(setup_extra_runtime_lib_path())) {
      LOG_WARN("failed to setup extra runtime lib path", K(ret));
    }
  }

  jh_ = std::getenv(JAVA_HOME);
  jo_ = std::getenv(JAVA_OPTS);
  ljo_ = std::getenv(LIBHDFS_OPTS);
  ch_ = std::getenv(CONNECTOR_PATH);
  cp_ = std::getenv(CLASSPATH);
  ld_ = std::getenv(LD_LIBRARY_PATH);
  LOG_INFO("setup all env variables", K(ret), K(jh_), K(jo_), K(ljo_), K(ch_), K(cp_), K(ld_));
  return ret;
}

int ObJavaEnv::check_version_valid()
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(version_valid_ == VALID)) {
    ret = OB_SUCCESS;
    LOG_INFO("java env version is valid", K(ret));
  } else if (OB_UNLIKELY(version_valid_ == NOT_INITED)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(version_valid_ == NOT_VALID)) {
    ret = OB_VERSION_NOT_MATCH;
    LOG_INFO("java env version is not valid check version and try again", K(ret));
  }
  return ret;
}

int ObJavaEnv::set_version_valid(VersionValid valid)
{
  int ret = OB_SUCCESS;
  version_valid_ = valid;
  return ret;
}

} // namespace sql
} // namespace oceanbase
