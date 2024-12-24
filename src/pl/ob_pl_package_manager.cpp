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

#define USING_LOG_PREFIX PL

#include "pl/ob_pl_package_manager.h"
#include "pl/ob_pl.h"
#include "pl/ob_pl_package.h"
#include "pl/ob_pl_resolver.h"
#include "pl/ob_pl_compile.h"
#include "pl/ob_pl_stmt.h"
#include "pl/ob_pl_package_state.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/plan_cache/ob_cache_object_factory.h"
#include "sql/plan_cache/ob_plan_cache.h"
#include "sql/ob_sql_utils.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_req_time_service.h"
#include "lib/file/file_directory_utils.h"
#include "pl/pl_cache/ob_pl_cache_mgr.h"
#include "sql/session/ob_session_val_map.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
using namespace sql;

namespace pl
{

// Usage: ObCharStream *s -> s.open() -> s.next().is_eos() ... -> s.close()
class ObCharStream
{
public:
  ObCharStream(const char *name) : eos_flag_(false), name_(name) {}
  virtual ~ObCharStream() {}
  ObCharStream(const ObCharStream &) = delete;
  const ObCharStream &operator=(const ObCharStream &) = delete;

  const char *get_name() { return name_; }
  virtual int open() = 0;
  virtual const ObCharStream &next(char &c) = 0;
  virtual bool is_eos() const { return eos_flag_; }
  virtual int close() = 0;

  VIRTUAL_TO_STRING_KV(K_(eos_flag), K_(name));

protected:
  static const char EOS = '\0';
  bool eos_flag_;

private:
  const char *const name_;
};

// read package sql from array embeded in the oberver binary file
class ObCStringStream final : public ObCharStream
{
public:
  ObCStringStream(const char *package_name, const char *data)
      : ObCharStream(package_name), data_(data), cursor_(nullptr) {}
  ~ObCStringStream() {}

  int open() override {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(data_)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("package sql data is null", K(ret), K(data_));
    } else if (0 == strlen(data_)) {
      LOG_INFO("package sql file is empty or not exists", K(ret));
    } else {
      cursor_ = data_;
    }
    return ret;
  }
  const ObCharStream &next(char &c) override {
    if (OB_NOT_NULL(cursor_) && !eos_flag_) {
      c = *cursor_++;
      eos_flag_ = (c == EOS);
    } else {
      c = EOS;
    }
    return *this;
  }
  int close() override {
    int ret = OB_SUCCESS;
    if (OB_NOT_NULL(cursor_)) {
      if (EOS != *(cursor_ - 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("C string stream has not been completely consumed", K(ret),
                 K(cursor_ - data_), K(data_), K(cursor_));
      } else {
        cursor_ = nullptr;
        eos_flag_ = false;
      }
    }
    return ret;
  }

  INHERIT_TO_STRING_KV("ObCharStream", ObCharStream,
                       "C string stream bytes comsumed", cursor_ - data_);

private:
  const char *const data_;
  const char *cursor_;
};

// read package sql from file under admin/
class ObFileStream final : public ObCharStream
{
public:
  ObFileStream(const char *package_name, const char *file_path)
      : ObCharStream(package_name), file_path_(file_path), file_(nullptr) {}
  ~ObFileStream() {
    // make sure file_ closed
    if (file_) {
      fclose(file_);
      file_ = nullptr;
    }
  }

  int open() override {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(file_path_) || 0 != access(file_path_, F_OK)) {
      ret = OB_FILE_NOT_EXIST;
      LOG_WARN("package sql file not exists", K(ret), K(file_path_));
    } else if (OB_ISNULL(file_ = fopen(file_path_, "rb"))) {
      ret = OB_IO_ERROR;
      LOG_WARN("package sql file open failed", K(ret), K(file_path_));
    }
    return ret;
  }
  const ObCharStream &next(char &c) override {
    int ch;
    if (OB_NOT_NULL(file_) && !eos_flag_) {
      if (EOF == (ch = fgetc(file_))) {
        c = EOS;
        eos_flag_ = true;
      } else {
        c = static_cast<char>(ch);
      }
    } else {
      c = EOS;
    }
    return *this;
  }
  int close() override {
    int ret = OB_SUCCESS;
    if (OB_NOT_NULL(file_)) {
      if (0 == feof(file_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("file content has not been completely consumed", K(ret), K(errno));
      } else if (0 != fclose(file_)) {
        ret = OB_IO_ERROR;
        LOG_WARN("close file failed", K(ret), K(file_path_), K(file_));
      } else {
        file_ = nullptr;
        eos_flag_ = false;
      }
    }
    return ret;
  }

  INHERIT_TO_STRING_KV("ObCharStream", ObCharStream, K_(file_path),
                       "file stream bytes comsumed", file_ ? ftell(file_) : -1);

private:
  const char *const file_path_;
  FILE *file_;
};

int ObPLPackageManager::read_package_sql(ObCharStream &stream, char* buf, int64_t buf_len, bool &eos)
{
  int ret = OB_SUCCESS;
  enum {S_LINE_START, S_NORMAL, S_COMMENT, S_TERMINATE} state = S_LINE_START;
  if (OB_ISNULL(buf)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("sql buffer is null", K(ret));
  } else if (buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buffer length is invalid", K(buf_len), K(ret));
  } else {
    char *p = buf;
    char *p_start = p;
    char *p_end = p + buf_len - 1;
    char c;
    // clear buffer
    *p = '\0';
    *p_end = '\0';
    while (OB_SUCC(ret) && state != S_TERMINATE) {
      if (stream.next(c).is_eos()) {
        ret = OB_ITER_END;
      } else {
        if (p >= p_end) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("query is too long", K(buf), K(buf_len), K(ret));
        } else {
          switch (state) {
            case S_LINE_START: {
              // row start with '#' or '--' is comment
              if (isspace(c)) {
                // ignore space
                state = S_LINE_START;
              } else if ('#' == c) {
                state = S_COMMENT;
              } else if ('-' == c) {
                if (stream.next(c).is_eos()) {
                  ret = OB_ITER_END;
                } else if ('-' == c) {
                  state = S_COMMENT;
                } else {
                  *p++ = '-';
                  *p++ = c;
                  state = S_NORMAL;
                }
              } else {
                *p++ = c;
                state = S_NORMAL;
              }
            }
            break;
            case S_COMMENT: {
              if (c == '\n') {
                state = S_LINE_START;
              }
            }
            break;
            case S_NORMAL: {
              /* if ('#' == c) {
                state = S_COMMENT;
              } else */ if ('/' == c) {
                if ((p != p_start) && '/' == *(p - 1)) {
                  *(p - 1) = '\0';
                  state = S_TERMINATE;
                } else {
                  *p++ = c;
                  state = S_NORMAL;
                }
              } else {
                *p++ = c;
                if ('\n' == c) {
                  state = S_LINE_START;
                } else {
                  state = S_NORMAL;
                }
              }
            }
            break;
            default: {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("parser package file with wrong state", K(state), K(ret));
            }
            break;
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
      if (stream.is_eos()) {
        eos = true;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("read package file error", K(ret), K(stream));
      }
    }
  }
  return ret;
}

int ObPLPackageManager::read_and_exec_package_sql(ObMySQLProxy &sql_proxy,
                                                  ObCharStream &stream,
                                                  ObCompatibilityMode compa_mode)
{
  int ret = OB_SUCCESS;
  if (!sql_proxy.is_inited() || !sql_proxy.is_active()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql_proxy not inited or not active", "sql_proxy inited",
             sql_proxy.is_inited(), "sql_proxy active", sql_proxy.is_active(), K(ret));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(stream.open())) {
      LOG_WARN("failed to open package file data stream", K(ret), K(stream));
    } else {
      // system tenant will run with mysql compatibility mode
      // but we need to create system packages with oralce compatibility
      // here hack to oracle mode
      bool eof = false;
      bool create_external_table = false;
      ObSessionParam param;
      ObSessionParam *param_ptr = nullptr;
      int64_t sql_mode = SMO_STRICT_ALL_TABLES | SMO_NO_ZERO_IN_DATE | SMO_NO_AUTO_CREATE_USER;
      // allow affected_rows > 0 when exec sql in external_table_alert_log.sql
      if (strcmp(stream.get_name(), "external_table_alert_log") == 0) {
        create_external_table = true;
        param.sql_mode_ = &sql_mode;
        param_ptr = &param;
      }
      // do not cache the compilation results of system packages into the PL cache when loading system packages.
      param.enable_pl_cache_ = false;
      SMART_VAR(char[OB_MAX_SQL_LENGTH], sql_buf) {
        while (OB_SUCC(ret) && !eof) {
          if (FAILEDx(read_package_sql(stream, sql_buf, OB_MAX_SQL_LENGTH, eof))) {
            LOG_WARN("fail to read package sql data", K(ret));
          } else if (strlen(sql_buf) != 0
                     && OB_FAIL(sql_proxy.write(OB_SYS_TENANT_ID,
                                                sql_buf,
                                                affected_rows,
                                                static_cast<int64_t>(compa_mode),
                                                &param))) {
            LOG_WARN("fail to exec package sql", K(sql_buf), K(ret));
          } else if (affected_rows != 0 && !create_external_table) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("affected_rows expected to be zero", K(ret), K(affected_rows), K(stream.get_name()));
          } else {
            OZ (ObSPIService::force_refresh_schema(OB_SYS_TENANT_ID));
          }
          LOG_INFO("package source data consumed", K(ret), K(stream));
        }
      }
      if (create_external_table && OB_SUCC(ret)) {
        uint64_t data_version = 0;
        common::ObString alter_table_sql("alter external table sys_external_tbs.__all_external_alert_log_info auto_refresh immediate");
        if (OB_FAIL(GET_MIN_DATA_VERSION(OB_SYS_TENANT_ID, data_version))) {
          LOG_WARN("fail to get sys tenant data version", KR(ret), K(data_version));
        } else if (data_version >= DATA_VERSION_4_3_3_0) {
          if (OB_FAIL(sql_proxy.write(OB_SYS_TENANT_ID,
                                      alter_table_sql,
                                      affected_rows,
                                      static_cast<int64_t>(compa_mode),
                                      param_ptr))) {
            LOG_WARN("fail to alter auto_refresh flag of external table ", K(ret), K(alter_table_sql));
          } else {
            LOG_INFO("seccess to alter auto_refresh flag", KR(ret), K(alter_table_sql));
          }
        }
      }
    }
  }
  return ret;
}

// import source file content array declarations, which is defined in system_package.cpp generated by CMake.
extern const int64_t syspack_source_count;
extern const std::pair<const char * const, const char* const> syspack_source_contents[];

int ObPLPackageManager::get_syspack_source_file_content(const char *file_name, const char *&content)
{
  int ret = OB_SUCCESS;
  OX (content = nullptr);
  if (OB_ISNULL(file_name)) {
    // return nullptr as `content`
    LOG_INFO("file name c string is null", K(file_name));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < syspack_source_count; i++) {
      if (0 == ObString(file_name).case_compare(syspack_source_contents[i].first)) {
        content = syspack_source_contents[i].second;
        break;
      }
    }
    // `content` not found, report error
    OV (OB_NOT_NULL(content), OB_ERR_UNEXPECTED, "system source file not found", ret, file_name);
  }
  return ret;
}

int ObPLPackageManager::load_sys_package(ObMySQLProxy &sql_proxy,
                                         const ObSysPackageFile &pack_file_info,
                                         ObCompatibilityMode compa_mode,
                                         bool from_file)
{
  int ret = OB_SUCCESS;
  const char *package_name = pack_file_info.package_name;
  const char *spec_file = pack_file_info.package_spec_file_name;
  const char *body_file = pack_file_info.package_body_file_name;

  const int64_t begin_time = ObTimeUtility::current_time();
  LOG_INFO("load sys package", K(package_name), K(spec_file), K(body_file), K(begin_time));

  if (from_file) {
    const char *sys_package_dir = "admin";
    char spec_file_path[MAX_PATH_SIZE] = {0};
    char body_file_path[MAX_PATH_SIZE] = {0};
    if (OB_SUCC(ret) && OB_NOT_NULL(spec_file)) {
      OZ (databuff_printf(spec_file_path, MAX_PATH_SIZE, "%s/%s", sys_package_dir, spec_file));
      ObFileStream spec_stream{package_name, spec_file_path};
      OZ (read_and_exec_package_sql(sql_proxy, spec_stream, compa_mode), spec_stream);
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(body_file)) {
      OZ (databuff_printf(body_file_path, MAX_PATH_SIZE, "%s/%s", sys_package_dir, body_file));
      ObFileStream body_stream{package_name, body_file_path};
      OZ (read_and_exec_package_sql(sql_proxy, body_stream, compa_mode), body_stream);
    }
  } else {
    const char *spec_content = nullptr;
    const char *body_content = nullptr;
    OZ (get_syspack_source_file_content(spec_file, spec_content));
    if (OB_SUCC(ret) && OB_NOT_NULL(spec_content)) {
      ObCStringStream spec_stream{package_name, spec_content};
      OZ (read_and_exec_package_sql(sql_proxy, spec_stream, compa_mode), spec_stream);
    }
    OZ (get_syspack_source_file_content(body_file, body_content));
    if (OB_SUCC(ret) && OB_NOT_NULL(body_content)) {
      ObCStringStream body_stream{package_name, body_content};
      OZ (read_and_exec_package_sql(sql_proxy, body_stream, compa_mode), body_stream);
    }
  }

  const int64_t now = ObTimeUtility::current_time();
  LOG_INFO("load sys package finish", K(ret), K(package_name), "total_time_used", now - begin_time);
  return ret;
}

static const ObSysPackageFile oracle_syspack_file_list[] = {
#ifdef OB_BUILD_ORACLE_PL
  {"dbms_standard", "dbms_standard.sql", nullptr},
  {"dbms_output", "dbms_output.sql", "dbms_output_body.sql"},
  {"dbms_metadata", "dbms_metadata.sql", "dbms_metadata_body.sql"},
  {"dbms_spm", "dbms_spm.sql", "dbms_spm_body.sql"},
  {"utl_raw", "utl_raw.sql", "utl_raw_body.sql"},
  {"dbms_lob", "dbms_lob.sql", "dbms_lob_body.sql"},
  {"sa_components", "sa_components.sql", "sa_components_body.sql"},
  {"sa_label_admin", "sa_label_admin.sql", "sa_label_admin_body.sql"},
  {"sa_policy_admin", "sa_policy_admin.sql", "sa_policy_admin_body.sql"},
  {"sa_session", "sa_session.sql", "sa_session_body.sql"},
  {"sa_sysdba", "sa_sysdba.sql", "sa_sysdba_body.sql"},
  {"sa_user_admin", "sa_user_admin.sql", "sa_user_admin_body.sql"},
  {"utl_i18n", "utl_i18n.sql", "utl_i18n_body.sql"},
  {"dbms_crypto", "dbms_crypto.sql", "dbms_crypto_body.sql"},
  {"dbms_random", "dbms_random.sql", "dbms_random_body.sql"},
  {"dbms_debug", "dbms_debug.sql", "dbms_debug_body.sql"},
  {"utl_inaddr", "utl_inaddr.sql", "utl_inaddr_body.sql"},
  {"utl_encode", "dbms_utl_encode.sql", "dbms_utl_encode_body.sql"},
  {"dbms_warning", "dbms_warning.sql", "dbms_warning_body.sql"},
  {"dbms_errlog", "dbms_errlog.sql", "dbms_errlog_body.sql"},
  {"dbms_lock", "dbms_lock.sql", "dbms_lock_body.sql"},
  {"dbms_sql", "dbms_sql.sql", "dbms_sql_body.sql"},
  {"dbms_xa", "dbms_xa.sql", "dbms_xa_body.sql"},
  {"dbms_resource_manager", "dbms_resource_manager.sql", "dbms_resource_manager_body.sql"},
  {"dbms_utility", "dbms_utility.sql", "dbms_utility_body.sql"},
  {"odciconst", "odciconst.sql", nullptr},
  {"dbms_stats", "dbms_stats.sql", "dbms_stats_body.sql"},
  {"dbms_any", "dbms_any.sql", "dbms_any_body.sql"},
  {"xml_type", "xml_type.sql", "xml_type_body.sql"},
  {"dbms_ijob", "dbms_ijob.sql", "dbms_ijob_body.sql"},
  {"dbms_job", "dbms_job.sql", "dbms_job_body.sql"},
  {"dbms_ischeduler", "dbms_ischeduler.sql", "dbms_ischeduler_body.sql"},
  {"dbms_scheduler", "dbms_scheduler.sql", "dbms_scheduler_body.sql"},
  {"catodci", "catodci.sql", nullptr},
  {"dbms_describe", "dbms_describe.sql", "dbms_describe_body.sql"},
  {"utl_file", "utl_file.sql", "utl_file_body.sql"},
  {"dbms_plan_cache", "dbms_plancache.sql", "dbms_plancache_body.sql"},
  {"dbms_sys_error", "dbms_sys_error.sql", "dbms_sys_error_body.sql"},
  {"dbms_preprocessor", "dbms_preprocessor.sql", "dbms_preprocessor_body.sql"},
  {"dbms_audit_mgmt", "dbms_audit_mgmt.sql", "dbms_audit_mgmt_body.sql"},
  {"dbms_application", "dbms_application.sql", "dbms_application_body.sql"},
  {"dbms_session", "dbms_session.sql", "dbms_session_body.sql"},
  {"dbms_monitor", "dbms_monitor.sql", "dbms_monitor_body.sql"},
  {"dbms_xplan", "dbms_xplan.sql", "dbms_xplan_body.sql"},
  {"dbms_workload_repository", "dbms_workload_repository.sql", "dbms_workload_repository_body.sql"},
  {"dbms_ash_internal", "dbms_ash_internal.sql", "dbms_ash_internal_body.sql"},
  {"dbms_rls", "dbms_rls.sql", "dbms_rls_body.sql"},
  {"dbms_udr", "dbms_udr.sql", "dbms_udr_body.sql"},
  {"json_element_t", "json_element_type.sql", "json_element_type_body.sql"},
  {"json_object_t", "json_object_type.sql", "json_object_type_body.sql"},
  {"dbms_mview", "dbms_mview.sql", "dbms_mview_body.sql"},
  {"dbms_mview_stats", "dbms_mview_stats.sql", "dbms_mview_stats_body.sql"},
  {"dbms_space", "dbms_space.sql", "dbms_space_body.sql"},
  {"json_array_t", "json_array_type.sql", "json_array_type_body.sql"},
  {"xmlsequence", "xml_sequence_type.sql", nullptr},
  {"utl_recomp", "utl_recomp.sql", "utl_recomp_body.sql"},
  {"sdo_geometry", "sdo_geometry.sql", "sdo_geometry_body.sql"},
  {"sdo_geom", "sdo_geom.sql", "sdo_geom_body.sql"},
  {"dbms_external_table", "dbms_external_table.sql", "dbms_external_table_body.sql"},
  {"dbms_profiler", "dbms_profiler.sql", "dbms_profiler_body.sql"},
#endif
};
static const ObSysPackageFile mysql_syspack_file_list[] = {
  {"dbms_stats", "dbms_stats_mysql.sql", "dbms_stats_body_mysql.sql"},
  {"dbms_scheduler", "dbms_scheduler_mysql.sql", "dbms_scheduler_mysql_body.sql"},
  {"dbms_ischeduler", "dbms_ischeduler_mysql.sql", "dbms_ischeduler_mysql_body.sql"},
  {"dbms_application", "dbms_application_mysql.sql", "dbms_application_body_mysql.sql"},
  {"dbms_session", "dbms_session_mysql.sql", "dbms_session_body_mysql.sql"},
  {"dbms_monitor", "dbms_monitor_mysql.sql", "dbms_monitor_body_mysql.sql"},
  {"dbms_resource_manager", "dbms_resource_manager_mysql.sql", "dbms_resource_manager_body_mysql.sql"},
  {"dbms_xplan", "dbms_xplan_mysql.sql", "dbms_xplan_mysql_body.sql"},
#ifdef OB_BUILD_ORACLE_PL
  {"dbms_spm", "dbms_spm_mysql.sql", "dbms_spm_body_mysql.sql"},
#endif
  {"dbms_udr", "dbms_udr_mysql.sql", "dbms_udr_body_mysql.sql"},
  {"dbms_workload_repository", "dbms_workload_repository_mysql.sql", "dbms_workload_repository_body_mysql.sql"},
  {"dbms_mview", "dbms_mview_mysql.sql", "dbms_mview_body_mysql.sql"},
  {"dbms_mview_stats", "dbms_mview_stats_mysql.sql", "dbms_mview_stats_body_mysql.sql"},
  {"dbms_trusted_certificate_manager", "dbms_trusted_certificate_manager_mysql.sql", "dbms_trusted_certificate_manager_body_mysql.sql"},
  {"dbms_ob_limit_calculator", "dbms_ob_limit_calculator_mysql.sql", "dbms_ob_limit_calculator_body_mysql.sql"},
  {"dbms_external_table", "dbms_external_table_mysql.sql", "dbms_external_table_body_mysql.sql"},
  {"external_table_alert_log", "external_table_alert_log.sql", nullptr},
  {"dbms_vector", "dbms_vector_mysql.sql", "dbms_vector_body_mysql.sql"},
  {"dbms_space", "dbms_space_mysql.sql", "dbms_space_body_mysql.sql"}
};

// for now! we only have one special system package "__DBMS_UPGRADE"
static const ObSysPackageFile oracle_special_syspack_file_list[] = {
  {"__dbms_upgrade", "__dbms_upgrade.sql", "__dbms_upgrade_body.sql"},
};
static const ObSysPackageFile mysql_special_syspack_file_list[] = {
  {"__dbms_upgrade", "__dbms_upgrade_mysql.sql", "__dbms_upgrade_body_mysql.sql"},
};

int ObPLPackageManager::load_sys_package(ObMySQLProxy &sql_proxy,
                                         ObString &package_name,
                                         ObCompatibilityMode compa_mode,
                                         bool from_file)
{
  int ret = OB_SUCCESS;
  const ObSysPackageFile *pack_file_info = nullptr;

#define SEARCH_SYSPACK_FILE_BY_NAME(syspack_file_list)                                      \
  do {                                                                                      \
    if (pack_file_info == nullptr) {                                                        \
      int sys_package_count = ARRAYSIZEOF(syspack_file_list);                               \
      for (int64_t i = 0; OB_SUCC(ret) && i < sys_package_count; ++i) {                     \
        if (0 == package_name.case_compare(ObString(syspack_file_list[i].package_name))) {  \
          pack_file_info = &syspack_file_list[i];                                           \
          break;                                                                            \
        }                                                                                   \
      }                                                                                     \
    }                                                                                       \
  } while (0)

  if (ObCompatibilityMode::ORACLE_MODE == compa_mode) {
    SEARCH_SYSPACK_FILE_BY_NAME(oracle_syspack_file_list);
    SEARCH_SYSPACK_FILE_BY_NAME(oracle_special_syspack_file_list);
  } else if (ObCompatibilityMode::MYSQL_MODE == compa_mode) {
    SEARCH_SYSPACK_FILE_BY_NAME(mysql_syspack_file_list);
    SEARCH_SYSPACK_FILE_BY_NAME(mysql_special_syspack_file_list);
  }
#undef SEARCH_SYSPACK_FILE_BY_NAME

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(pack_file_info)) {
    ret = OB_ERR_PACKAGE_DOSE_NOT_EXIST;
    LOG_WARN("package not exists", K(ret), K(package_name), K(compa_mode));
    LOG_USER_ERROR(OB_ERR_PACKAGE_DOSE_NOT_EXIST,
                   "PACKAGE",
                   ObString("oceanbase").length(), ObString("oceanbase").ptr(),
                   package_name.length(), package_name.ptr());
  } else {
    OZ (load_sys_package(sql_proxy, *pack_file_info, compa_mode, from_file));
  }
  return ret;
}

int ObPLPackageManager::load_sys_package_list(ObMySQLProxy &sql_proxy,
                                              const ObSysPackageFile *sys_package_list,
                                              int sys_package_count,
                                              ObCompatibilityMode compa_mode,
                                              bool from_file)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(sys_package_list));
  LOG_INFO("load sys package list begin", "sys package total count", sys_package_count);
  for (int i = 0; OB_SUCC(ret) && i < sys_package_count; ++i) {
    OZ (load_sys_package(sql_proxy, sys_package_list[i], compa_mode, from_file));
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("load sys package list failed", K(ret), K(compa_mode));
  } else {
    LOG_INFO("load sys package list success", K(ret), K(compa_mode));
  }
  return ret;
}

int ObPLPackageManager::load_all_common_sys_package(
    ObMySQLProxy &sql_proxy, ObCompatibilityMode compa_mode, bool from_file) {
  int ret = OB_SUCCESS;
  if (compa_mode == ObCompatibilityMode::OCEANBASE_MODE) {
    OZ (load_sys_package_list(sql_proxy, oracle_syspack_file_list,
                              ARRAYSIZEOF(oracle_syspack_file_list),
                              ObCompatibilityMode::ORACLE_MODE,
                              from_file));
    OZ (load_sys_package_list(sql_proxy, mysql_syspack_file_list,
                              ARRAYSIZEOF(mysql_syspack_file_list),
                              ObCompatibilityMode::MYSQL_MODE,
                              from_file));
  } else if (compa_mode == ObCompatibilityMode::ORACLE_MODE) {
    OZ (load_sys_package_list(sql_proxy, oracle_syspack_file_list,
                              ARRAYSIZEOF(oracle_syspack_file_list),
                              ObCompatibilityMode::ORACLE_MODE,
                              from_file));
  } else if (compa_mode == ObCompatibilityMode::MYSQL_MODE) {
    OZ (load_sys_package_list(sql_proxy, mysql_syspack_file_list,
                              ARRAYSIZEOF(mysql_syspack_file_list),
                              ObCompatibilityMode::MYSQL_MODE,
                              from_file));
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("load all common sys package success!", K(ret), K(from_file));
  } else {
    LOG_WARN("load all common sys package failed!", K(ret), K(from_file));
  }
  return ret;
}

int ObPLPackageManager::load_all_special_sys_package(ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_ORACLE_PL
  OZ (load_sys_package_list(sql_proxy, oracle_special_syspack_file_list,
                            ARRAYSIZEOF(oracle_special_syspack_file_list),
                            ObCompatibilityMode::ORACLE_MODE,
                            false /* from_file */));
#endif
  OZ (load_sys_package_list(sql_proxy, mysql_special_syspack_file_list,
                            ARRAYSIZEOF(mysql_special_syspack_file_list),
                            ObCompatibilityMode::MYSQL_MODE,
                            false /* from_file */));
  return ret;
}

int ObPLPackageManager::load_all_sys_package(ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  OZ (load_all_common_sys_package(sql_proxy, ObCompatibilityMode::OCEANBASE_MODE, false /* from_file */));
  OZ (load_all_special_sys_package(sql_proxy));
  if (OB_SUCC(ret)) {
    LOG_INFO("load all sys package success!", K(ret));
  } else {
    LOG_INFO("load all sys package failed!", K(ret));
  }
  return ret;
}

int ObPLPackageManager::get_package_var(const ObPLResolveCtx &resolve_ctx, uint64_t package_id,
                                        const ObString &var_name, const ObPLVar *&var, int64_t &var_idx)
{
  int ret = OB_SUCCESS;
  var = NULL;
  var_idx = OB_INVALID_INDEX;
  if (OB_INVALID_ID == package_id || var_name.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("package id or var name invalid", K(package_id), K(var_name), K(ret));
  } else {
    ObPLPackage *package_spec = NULL;
    if (OB_FAIL(get_cached_package_spec(resolve_ctx, package_id, package_spec))) {
      LOG_WARN("get cached package spec failed", K(package_id), K(ret));
    } else if (OB_ISNULL(package_spec)){
      ret = OB_ERR_PACKAGE_DOSE_NOT_EXIST;
      LOG_WARN("package not exist", K(package_id), K(ret));
    } else {
      if (OB_FAIL(package_spec->get_var(var_name, var, var_idx))) {
        LOG_WARN("package get var failed", K(package_id), K(var_name), K(ret));
      }
    }
  }
  return ret;
}

int ObPLPackageManager::get_package_var(const ObPLResolveCtx &resolve_ctx, uint64_t package_id,
                                        int64_t var_idx, const ObPLVar *&var)
{
  int ret = OB_SUCCESS;
  var = NULL;
  if (OB_INVALID_ID == package_id || OB_INVALID_INDEX == var_idx) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("package id or var index invalid", K(package_id), K(var_idx), K(ret));
  } else {
    ObPLPackage *package_spec = NULL;
    ObPLPackage *package_body = NULL;
    if (OB_FAIL(get_cached_package(resolve_ctx, package_id, package_spec, package_body, true))) {
      LOG_WARN("get cached package failed", K(package_id), K(ret));
    } else if (OB_ISNULL(package_spec)){
      ret = OB_ERR_PACKAGE_DOSE_NOT_EXIST;
      LOG_WARN("package spec not exist", K(package_id), K(ret));
    } else {
      const ObPLPackage *tmp_package = NULL;
      if (package_id == package_spec->get_id()) {
        tmp_package = package_spec;
      } else if (!OB_ISNULL(package_body) && package_id == package_body->get_id()) {
        tmp_package = package_body;
      } else {
        ret = OB_ERR_PACKAGE_DOSE_NOT_EXIST;
        LOG_WARN("package body not exist", K(package_id), K(ret));
        LOG_USER_ERROR(OB_ERR_PACKAGE_DOSE_NOT_EXIST, "PACKAGE BODY",
                             package_spec->get_db_name().length(), package_spec->get_db_name().ptr(),
                             package_spec->get_name().length(), package_spec->get_name().ptr());
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(tmp_package->get_var(var_idx, var))) {
          LOG_WARN("package get var failed", K(ret));
        } else if (OB_ISNULL(var)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("package var not found", K(package_id), K(var_idx), K(ret));
        } else if (!var->is_readonly() && OB_ISNULL(package_body)) {// 对于非constant值, 需要保证body的合法性
          OZ (get_cached_package(resolve_ctx, package_id, package_spec, package_body, false));
        }
      }
    }
  }
  return ret;
}

int ObPLPackageManager::get_package_type(const ObPLResolveCtx &resolve_ctx,
                                         uint64_t package_id,
                                         const ObString &type_name,
                                         const ObUserDefinedType *&user_type,
                                         bool log_user_error)
{
  int ret = OB_SUCCESS;
  user_type = NULL;
  if (OB_INVALID_ID == package_id || type_name.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("package id or type name invalid", K(package_id), K(type_name), K(ret));
  } else {
    observer::ObReqTimeGuard req_timeinfo_guard;
    ObPLPackage *package_spec = NULL;
    if (OB_FAIL(get_cached_package_spec(resolve_ctx, package_id, package_spec))) {
      LOG_WARN("get cached package failed", K(ret));
    } else if (OB_ISNULL(package_spec)){
      ret = OB_ERR_PACKAGE_DOSE_NOT_EXIST;
      LOG_WARN("package spec not exist", K(package_id), K(ret));
    } else {
      if (OB_FAIL(package_spec->get_type(type_name, user_type))) {
        LOG_WARN("package get type failed", K(package_id), K(type_name), K(ret));
      } else if (OB_ISNULL(user_type)) {
        ret = OB_ERR_SP_UNDECLARED_TYPE;
        LOG_WARN("package type not found", K(package_id), K(type_name), K(ret));
        if (log_user_error) {
          LOG_USER_ERROR(OB_ERR_SP_UNDECLARED_TYPE, type_name.length(), type_name.ptr());
        } else {}
      } else {}
    }
  }
  return ret;
}

int ObPLPackageManager::get_package_type(const ObPLResolveCtx &resolve_ctx, uint64_t package_id,
                                         uint64_t type_id, const ObUserDefinedType *&user_type)
{
  int ret = OB_SUCCESS;
  user_type = NULL;
  if (OB_INVALID_ID == package_id || OB_INVALID_INDEX == type_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("package id or type index invalid", K(package_id), K(type_id), K(ret));
  } else {
    ObPLPackage *package_spec = NULL;
    ObPLPackage *package_body = NULL;
    if (OB_FAIL(get_cached_package(resolve_ctx, package_id, package_spec, package_body, true))) {
      LOG_WARN("get cached package failed", K(ret));
    } else if (OB_ISNULL(package_spec)){
      ret = OB_ERR_PACKAGE_DOSE_NOT_EXIST;
      LOG_WARN("package spec not exist", K(package_id), K(ret));
    } else {
      const ObPLPackage *tmp_package = NULL;
      if (package_id == package_spec->get_id()) {
        tmp_package = package_spec;
      } else if (OB_NOT_NULL(package_body) && package_id == package_body->get_id()) {
        tmp_package = package_body;
      } else {
        ret = OB_ERR_PACKAGE_DOSE_NOT_EXIST;
        LOG_WARN("package body not exist", K(package_id), K(ret));
        LOG_USER_ERROR(OB_ERR_PACKAGE_DOSE_NOT_EXIST, "PACKAGE BODY",
                             package_spec->get_db_name().length(), package_spec->get_db_name().ptr(),
                             package_spec->get_name().length(), package_spec->get_name().ptr());
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(tmp_package->get_type(type_id, user_type))) {
          LOG_WARN("get package type failed", K(ret));
        } else if (OB_ISNULL(user_type)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("package type not found", K(ret));
        } else {}
      }
    }
  }
  return ret;
}

int ObPLPackageManager::get_package_expr(const ObPLResolveCtx &resolve_ctx,
                                         uint64_t package_id,
                                         int64_t expr_idx,
                                         ObSqlExpression *&expr)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == package_id || OB_INVALID_ID == expr_idx) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("package id or expr idx invalid", K(ret), K(package_id), K(expr_idx));
  } else {
    observer::ObReqTimeGuard req_timeinfo_guard;
    ObPLPackage *package_spec = NULL;
    if (OB_FAIL(get_cached_package_spec(resolve_ctx, package_id, package_spec))) {
      LOG_WARN("get cached package failed", K(ret));
    } else if (OB_ISNULL(package_spec)){
      ret = OB_ERR_PACKAGE_DOSE_NOT_EXIST;
      LOG_WARN("package spec not exist", K(package_id), K(ret));
    } else if (OB_ISNULL(expr = package_spec->get_default_expr(expr_idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("package expr not exist",
               K(ret), K(expr_idx), K(package_id), K(package_spec->get_default_exprs().count()));
    }
  }
  return ret;
}

int ObPLPackageManager::get_package_expr(const ObPLResolveCtx &resolve_ctx,
                                         ObRawExprFactory &expr_factory,
                                         uint64_t package_id,
                                         int64_t expr_idx,
                                         ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  const ObPackageInfo *package_spec_info = NULL;
  const ObPackageInfo *package_body_info = NULL;
  CK (package_id != OB_INVALID_ID);
  CK (expr_idx != OB_INVALID_ID);
  OZ (get_package_schema_info(resolve_ctx.schema_guard_, package_id, package_spec_info, package_body_info));
  CK (OB_NOT_NULL(package_spec_info));
  CK (package_spec_info->get_package_id() == package_id);
  if (OB_SUCC(ret)) {
    ObPLCompiler compiler(resolve_ctx.allocator_,
                          resolve_ctx.session_info_,
                          resolve_ctx.schema_guard_,
                          resolve_ctx.package_guard_,
                          resolve_ctx.sql_proxy_);
    const uint64_t tenant_id = package_spec_info->get_tenant_id();
    uint64_t db_id = package_spec_info->get_database_id();
    uint64_t package_spec_id = package_spec_info->get_package_id();
    ObPLBlockNS *null_parent_ns = NULL;
    const ObDatabaseSchema *db_schema = NULL;
    uint64_t effective_tenant_id = resolve_ctx.session_info_.get_effective_tenant_id();
    HEAP_VAR(ObPLPackageAST, package_spec_ast, resolve_ctx.allocator_) {
      ObString source;
      if (package_spec_info->is_for_trigger()) {
        OZ (ObTriggerInfo::gen_package_source(package_spec_info->get_tenant_id(),
                                              package_spec_info->get_package_id(),
                                              source,
                                              PACKAGE_TYPE,
                                              resolve_ctx.schema_guard_,
                                              resolve_ctx.allocator_));
      } else {
        source = package_spec_info->get_source();
      }
      OZ (ObSQLUtils::convert_sql_text_from_schema_for_resolve(
        resolve_ctx.allocator_, resolve_ctx.session_info_.get_dtc_params(), source));
      OZ (resolve_ctx.schema_guard_.get_database_schema(tenant_id, db_id, db_schema));
      OZ (package_spec_ast.init(db_schema->get_database_name_str(),
                                package_spec_info->get_package_name(),
                                PL_PACKAGE_SPEC,
                                package_spec_info->get_database_id(),
                                package_spec_id,
                                package_spec_info->get_schema_version(),
                                NULL));
      {
        ObPLCompilerEnvGuard guard(
          *package_spec_info, resolve_ctx.session_info_, resolve_ctx.schema_guard_, ret);
        OZ (compiler.analyze_package(source, null_parent_ns,
                                     package_spec_ast, package_spec_info->is_for_trigger()));
      }
      CK (expr_idx >= 0 && package_spec_ast.get_exprs().count() > expr_idx);
      CK (OB_NOT_NULL(package_spec_ast.get_expr(expr_idx)));
      OZ (ObPLExprCopier::copy_expr(expr_factory, package_spec_ast.get_expr(expr_idx), expr));
    }
  }
  return ret;
}

int ObPLPackageManager::get_package_cursor(const ObPLResolveCtx &resolve_ctx,
                                           uint64_t package_id,
                                           int64_t cursor_id,
                                           const ObPLCursor *&cursor)
{
  int ret = OB_SUCCESS;
  ObPLPackage *package_spec = NULL;
  ObPLPackage *package_body = NULL;
  ObPLPackage *tmp_package = NULL;
  cursor = NULL;
  CK (package_id != OB_INVALID_ID);
  CK (cursor_id != OB_INVALID_INDEX);
  OZ (get_cached_package(resolve_ctx, package_id, package_spec, package_body));
  OX (tmp_package = package_spec != NULL && package_spec->get_id() == package_id
      ? package_spec
        : package_body != NULL && package_body->get_id() == package_id
          ? package_body : NULL);
  if (OB_SUCC(ret) && OB_ISNULL(tmp_package)) {
    ret = OB_ERR_PACKAGE_DOSE_NOT_EXIST;
    LOG_WARN("package spec does not exist", K(ret), K(package_id));
  }
  OZ (tmp_package->get_cursor(cursor_id, cursor));
  return ret;
}

int ObPLPackageManager::get_package_cursor(const ObPLResolveCtx &resolve_ctx,
                                           uint64_t package_id,
                                           const ObString &cursor_name,
                                           const ObPLCursor *&cursor,
                                           int64_t &cursor_idx)
{
  int ret = OB_SUCCESS;
  ObPLPackage *package_spec = NULL;
  ObPLPackage *package_body = NULL;
  ObPLPackage *tmp_package = NULL;
  cursor = NULL;
  cursor_idx = OB_INVALID_INDEX;
  CK (package_id != OB_INVALID_ID);
  CK (!cursor_name.empty());
  OZ (get_cached_package(resolve_ctx, package_id, package_spec, package_body));
  OX (tmp_package = package_spec != NULL && package_spec->get_id() == package_id
      ? package_spec
        : package_body != NULL && package_body->get_id() == package_id
          ? package_body : NULL);
  if (OB_SUCC(ret) && OB_ISNULL(tmp_package)) {
    ret = OB_ERR_PACKAGE_DOSE_NOT_EXIST;
    LOG_WARN("package spec does not exist", K(ret), K(package_id));
  }
  OZ (tmp_package->get_cursor(cursor_name, cursor, cursor_idx));
  if (OB_FAIL(ret) || OB_ISNULL(cursor)) {
  } else if (ObPLCursor::DUP_DECL == cursor->get_state()) {
    ret = OB_ERR_SP_DUP_CURSOR;
    LOG_WARN("too many declarations of cursor match this call",
             K(ret), K(cursor_name), K(cursor_idx));
  } else if (ObPLCursor::DECLARED == cursor->get_state()) {
    CK (tmp_package != package_body);
    CK (OB_NOT_NULL(package_body));
    OZ (package_body->get_cursor(
      cursor->get_package_id(), cursor->get_routine_id(), cursor->get_index(), cursor));
  }
  return ret;
}

int ObPLPackageManager::get_package_condition(const ObPLResolveCtx &resolve_ctx,
                                              uint64_t package_id,
                                              const ObString &condition_name,
                                              const ObPLCondition *&value)
{
  int ret = OB_SUCCESS;
  ObPLPackage *package_spec = NULL;
  ObPLPackage *package_body = NULL;
  ObPLPackage *tmp_package = NULL;
  value = NULL;
  CK (package_id != OB_INVALID_ID);
  CK (!condition_name.empty());
  OZ (get_cached_package(resolve_ctx, package_id, package_spec, package_body, true));
  OX (tmp_package = (package_spec != NULL && package_spec->get_id() == package_id) ? package_spec :
    (package_body != NULL && package_body->get_id() == package_id) ? package_body :
    NULL);
  if (OB_SUCC(ret) && OB_ISNULL(tmp_package)) {
    ret = OB_ERR_PACKAGE_DOSE_NOT_EXIST;
    LOG_WARN("package spec does not exist", K(ret), K(package_id));
  }
  CK (OB_NOT_NULL(tmp_package));
  OZ (tmp_package->get_condition(condition_name, value));
  return ret;
}

int ObPLPackageManager::get_package_routine(const ObPLResolveCtx &ctx,
                                            sql::ObExecContext &exec_ctx,
                                            uint64_t package_id,
                                            int64_t routine_idx,
                                            ObPLFunction *&routine)
{
  int ret = OB_SUCCESS;
  routine = NULL;
  bool is_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recusive", K(ret));
  }
  CK (OB_LIKELY(OB_INVALID_ID != package_id));
  CK (OB_LIKELY(OB_INVALID_INDEX != routine_idx));

  if (OB_SUCC(ret)) {
    ObPLPackage *package_spec = NULL;
    ObPLPackage *package_body = NULL;
    OZ (get_cached_package(ctx, package_id, package_spec, package_body));
    CK (OB_NOT_NULL(package_spec));
    if (OB_SUCC(ret) && OB_ISNULL(package_body)){
      ret = OB_ERR_PACKAGE_DOSE_NOT_EXIST;
      LOG_USER_ERROR(OB_ERR_PACKAGE_DOSE_NOT_EXIST, "PACKAGE BODY",
                     package_spec->get_db_name().length(), package_spec->get_db_name().ptr(),
                     package_spec->get_name().length(), package_spec->get_name().ptr());
    }
    OZ (package_body->get_routine(routine_idx, routine));
    if (OB_SUCC(ret) && OB_ISNULL(routine)) {
      ret = OB_ERR_SP_DOES_NOT_EXIST;
      LOG_WARN("can not found package routine in package body", K(ret), K(routine_idx), K(routine));
    } else {
      ObPLPackageState *dummy_state = NULL;
      if (OB_SUCC(ret) && OB_NOT_NULL(package_body->get_init_routine())) {
        // call一个pacakge 函数的时候，去执行package的init 函数
        OZ (get_package_state(ctx, exec_ctx, package_id, dummy_state));
      }
    }
  }
  return ret;
}

int ObPLPackageManager::get_package_var_val(const ObPLResolveCtx &resolve_ctx,
                                            sql::ObExecContext &exec_ctx,
                                            uint64_t package_id,
                                            int64_t spec_version,
                                            int64_t body_version,
                                            int64_t var_idx,
                                            ObObj &var_val)
{
  int ret = OB_SUCCESS;
  const ObPLVar *var = NULL;
  ObPLPackageState *package_state = NULL;
  ObPackageStateVersion version(spec_version, body_version);
  CK (package_id != OB_INVALID_ID);
  CK (var_idx != OB_INVALID_INDEX);
  OZ (get_package_item_state(
    resolve_ctx.session_info_, package_id, version, package_state));
  if (OB_SUCC(ret) && OB_ISNULL(package_state)) {
    OZ (get_package_var(resolve_ctx, package_id, var_idx, var),
        K(package_id), K(var_idx));
    CK (OB_NOT_NULL(var));
    OZ (get_package_state(
        resolve_ctx, exec_ctx, package_id, package_state, var->is_readonly()),
        K(package_id), K(var_idx), K(var->is_readonly()));
  }
  OZ (package_state->get_package_var_val(var_idx, var_val),
      K(var_idx));
  return ret;
}

int ObPLPackageManager::set_package_var_val(const ObPLResolveCtx &resolve_ctx,
                                            sql::ObExecContext &exec_ctx,
                                            uint64_t package_id,
                                            int64_t var_idx,
                                            const ObObj &var_val,
                                            bool need_deserialize,
                                            bool from_proxy)
{
  int ret = OB_SUCCESS;
  bool need_free_new = false;
  bool need_free_old = false;
  ObPLPackageState *package_state = NULL;
  ObObj old_var_val;
  ObObj new_var_val;
  const ObPLVar *var = NULL;
  CK (package_id != OB_INVALID_ID);
  CK (var_idx != OB_INVALID_INDEX);
  OZ (get_package_state(resolve_ctx, exec_ctx, package_id, package_state),
                        K(package_id), K(var_idx), K(var_val));
  OZ (package_state->get_package_var_val(var_idx, old_var_val), K(package_id), K(var_idx));
  OZ (get_package_var(resolve_ctx, package_id, var_idx, var), K(package_id), K(var_idx));
  OV (OB_NOT_NULL(var), OB_ERR_UNEXPECTED, K(package_id), K(var_idx));
  if (need_deserialize) {
    OZ (var->get_type().init_session_var(resolve_ctx,
                                         var->get_type().is_cursor_type() ?
                                          package_state->get_pkg_cursor_allocator()
                                          : package_state->get_pkg_allocator(),
                                         exec_ctx,
                                         NULL,
                                         false,
                                         new_var_val), K(package_id), K(var_idx), K(var_val));
    OX (need_free_new = true);
    if (OB_FAIL(ret)) {
    } else if (var->get_type().is_cursor_type()) {
      OV (var_val.is_tinyint() || var_val.is_number() || var_val.is_decimal_int(), OB_ERR_UNEXPECTED, K(var_val));
      if (OB_SUCC(ret)
          && (var_val.is_tinyint()
              ? var_val.get_bool()
                : (var_val.is_number() ? !var_val.is_zero_number() : !var_val.is_zero_decimalint()))) {
        if (from_proxy) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("can not sync package open cursor from proxy,"
                   "need route current sql to orignal server", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "sync package open cursor from proxy");
        } else {
          ObPLCursorInfo *cursor = reinterpret_cast<ObPLCursorInfo *>(new_var_val.get_ext());
          CK (OB_NOT_NULL(cursor));
          OX (cursor->set_sync_cursor());
        }
      }
    } else if (var->get_type().is_opaque_type()) {
      if (var_val.is_null()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("can not sync package opaque type", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "sync package opaque type");
      }
    } else {
      OZ (var->get_type().deserialize(resolve_ctx,
                                      var->get_type().is_cursor_type() ?
                                        package_state->get_pkg_cursor_allocator()
                                        : package_state->get_pkg_allocator(),
                                      var_val.get_hex_string().ptr(),
                                      var_val.get_hex_string().length(),
                                      new_var_val), K(package_id), K(var_idx), K(var_val));
    }
    LOG_DEBUG("deserialize package var", K(package_id), K(var_idx), K(var_val), K(new_var_val));
  } else {
    new_var_val = var_val;
  }
  if (OB_SUCC(ret) && var->is_not_null() && new_var_val.is_null()) {
    ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
    LOG_WARN("not null check violated", K(var->is_not_null()), K(var_val.is_null()), K(ret));
  }
  OZ (package_state->set_package_var_val(var_idx, new_var_val, resolve_ctx, !need_deserialize));
  OX (need_free_old = true);
  OX (need_free_new = false);
  OZ (update_special_package_status(resolve_ctx, package_id, *var, old_var_val, new_var_val));

  if (OB_NOT_NULL(var) && var->get_type().is_cursor_type() && !var->get_type().is_cursor_var()) {
    // package ref cursor variable, refrence outside, do not destruct old var val.
  } else {
    if (OB_FAIL(ret) && need_free_new) {
      ObUserDefinedType::destruct_objparam(package_state->get_pkg_allocator(), new_var_val, &(resolve_ctx.session_info_));
    }
    if (need_free_old) {
      if (new_var_val.is_null() &&
          old_var_val.is_pl_extend() &&
          var->get_type().get_type() != PL_CURSOR_TYPE &&
          var->get_type().get_type() != PL_REF_CURSOR_TYPE) {
        // do nothing
      } else {
        ObUserDefinedType::destruct_objparam(package_state->get_pkg_allocator(), old_var_val, &(resolve_ctx.session_info_));
      }
    }
  }
  if (!need_deserialize) {
    OZ (package_state->update_changed_vars(var_idx));
    OX (resolve_ctx.session_info_.set_pl_can_retry(false));
  }
  return ret;
}

int ObPLPackageManager::update_special_package_status(const ObPLResolveCtx &resolve_ctx,
                                                      uint64_t package_id,
                                                      const ObPLVar &var,
                                                      const ObObj &old_val,
                                                      const ObObj &new_val)
{
  int ret = OB_SUCCESS;

  ObPLPackage *package_spec = nullptr;
  ObPLPackage *package_body = nullptr;

  OZ (get_cached_package(resolve_ctx, package_id, package_spec, package_body));

  CK (OB_NOT_NULL(package_spec));

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (get_tenant_id_by_object_id(package_id) == OB_SYS_TENANT_ID &&
               0 == package_spec->get_name().compare("DBMS_PROFILER")) {
#ifdef OB_BUILD_ORACLE_PL
    OZ (ObDBMSProfiler::notify_package_variable_change(resolve_ctx.session_info_, var, old_val, new_val));
#endif // OB_BUILD_ORACLE_PL
  }

  return ret;
}


int ObPLPackageManager::load_package_spec(const ObPLResolveCtx &resolve_ctx,
                                          const ObPackageInfo &package_spec_info,
                                          ObPLPackage *&package_spec)
{
  int ret = OB_SUCCESS;
  DISABLE_SQL_MEMLEAK_GUARD;
  package_spec = NULL;
  const uint64_t tenant_id = package_spec_info.get_tenant_id();
  uint64_t db_id = package_spec_info.get_database_id();
  uint64_t package_id = package_spec_info.get_package_id();
  ObPLBlockNS *null_parent_ns = NULL;
  uint64_t effective_tenant_id = resolve_ctx.session_info_.get_effective_tenant_id();
  HEAP_VAR(ObPLPackageAST, package_spec_ast, resolve_ctx.allocator_) {
    const ObDatabaseSchema *db_schema = NULL;
    ObPLCompiler compiler(resolve_ctx.allocator_,
                          resolve_ctx.session_info_,
                          resolve_ctx.schema_guard_,
                          resolve_ctx.package_guard_,
                          resolve_ctx.sql_proxy_);
    OZ (resolve_ctx.schema_guard_.get_database_schema(tenant_id, db_id, db_schema));
    CK (OB_NOT_NULL(db_schema));
    OZ (package_spec_ast.init(db_schema->get_database_name_str(),
                              package_spec_info.get_package_name(),
                              PL_PACKAGE_SPEC,
                              package_spec_info.get_database_id(),
                              package_id,
                              package_spec_info.get_schema_version(),
                              NULL));
    // generate cacheobj_guard to protect package and package will be destoried by map's destructor
    ObCacheObjGuard* cacheobj_guard = NULL;
    void* buf = NULL;
    if (OB_ISNULL(buf = resolve_ctx.package_guard_.alloc_.alloc(sizeof(ObCacheObjGuard)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory.", K(ret));
    } else if (FALSE_IT(cacheobj_guard = new (buf)ObCacheObjGuard(PACKAGE_SPEC_HANDLE))) {
      // do nothing
    } else {
      OZ (ObCacheObjectFactory::alloc(*cacheobj_guard, ObLibCacheNameSpace::NS_PKG,
                                                            effective_tenant_id));
      OX (package_spec = static_cast<ObPLPackage*>(cacheobj_guard->get_cache_obj()));
      CK (OB_NOT_NULL(package_spec));
      OZ (package_spec->init(package_spec_ast));
      OZ (compiler.compile_package(package_spec_info,
                                  null_parent_ns,
                                  package_spec_ast,
                                  *package_spec));
      if (OB_SUCC(ret)) {
        if (package_spec->get_can_cached()
            && OB_FAIL(add_package_to_plan_cache(resolve_ctx, package_spec))) {
          LOG_WARN("failed to add package spec to cached", K(ret));
          ret = OB_SUCCESS; // cache add failed, need not fail execute path
        } else {
          LOG_DEBUG("success to add package spec to cached",
                    K(ret), K(package_id), K(package_spec->get_can_cached()));
        }
      }
      OZ (resolve_ctx.package_guard_.put(package_id, cacheobj_guard), package_id);
      if (OB_FAIL(ret) && OB_NOT_NULL(package_spec)) {
        // pointer should be free manually
        cacheobj_guard->~ObCacheObjGuard();
        package_spec = NULL;
      }
    }
  }
  return ret;
}

int ObPLPackageManager::load_package_body(const ObPLResolveCtx &resolve_ctx,
                                          const share::schema::ObPackageInfo &package_spec_info,
                                          const share::schema::ObPackageInfo &package_body_info,
                                          ObPLPackage *&package_body)
{
  int ret = OB_SUCCESS;
  DISABLE_SQL_MEMLEAK_GUARD;
  package_body = NULL;
  ObPLCompiler compiler(resolve_ctx.allocator_,
                        resolve_ctx.session_info_,
                        resolve_ctx.schema_guard_,
                        resolve_ctx.package_guard_,
                        resolve_ctx.sql_proxy_);
  const uint64_t tenant_id = package_spec_info.get_tenant_id();
  uint64_t db_id = package_spec_info.get_database_id();
  uint64_t package_spec_id = package_spec_info.get_package_id();
  uint64_t package_body_id = package_body_info.get_package_id();
  ObPLBlockNS *null_parent_ns = NULL;
  const ObDatabaseSchema *db_schema = NULL;
  uint64_t effective_tenant_id = resolve_ctx.session_info_.get_effective_tenant_id();
  HEAP_VARS_2((ObPLPackageAST, package_spec_ast, resolve_ctx.allocator_),
              (ObPLPackageAST, package_body_ast, resolve_ctx.allocator_)) {
    ObString source;
    if (package_spec_info.is_for_trigger()) {
      OZ (ObTriggerInfo::gen_package_source(package_spec_info.get_tenant_id(),
                                            package_spec_info.get_package_id(),
                                            source,
                                            share::schema::PACKAGE_TYPE,
                                            resolve_ctx.schema_guard_,
                                            resolve_ctx.allocator_));
    } else {
      source = package_spec_info.get_source();
    }
    OZ (resolve_ctx.schema_guard_.get_database_schema(tenant_id, db_id, db_schema));
    OZ (package_spec_ast.init(db_schema->get_database_name_str(),
                              package_spec_info.get_package_name(),
                              PL_PACKAGE_SPEC,
                              package_spec_info.get_database_id(),
                              package_spec_id,
                              package_spec_info.get_schema_version(),
                              NULL));
    if (package_spec_info.is_invoker_right()) {
      OX (package_spec_ast.get_compile_flag().add_invoker_right());
    }
    OZ (ObSQLUtils::convert_sql_text_from_schema_for_resolve(
          resolve_ctx.allocator_, resolve_ctx.session_info_.get_dtc_params(), source));
    {
      ObPLCompilerEnvGuard guard(
        package_spec_info, resolve_ctx.session_info_, resolve_ctx.schema_guard_, ret);
      OZ (compiler.analyze_package(source, null_parent_ns,
                                   package_spec_ast, package_spec_info.is_for_trigger()));
    }

    OZ (package_body_ast.init(db_schema->get_database_name_str(),
                              package_body_info.get_package_name(),
                              PL_PACKAGE_BODY,
                              package_body_info.get_database_id(),
                              package_body_id,
                              package_body_info.get_schema_version(),
                              &package_spec_ast));
    // generate cacheobj_guard to protect package and package will be
    // destoried by map's destructor
    ObCacheObjGuard* cacheobj_guard = NULL;
    void* buf = NULL;
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(buf = resolve_ctx.package_guard_.alloc_.alloc(sizeof(ObCacheObjGuard)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory.", K(ret));
    } else if (FALSE_IT(cacheobj_guard = new (buf)ObCacheObjGuard(PACKAGE_BODY_HANDLE))) {
      // do nothing
    } else {
      OZ (ObCacheObjectFactory::alloc(*cacheobj_guard, ObLibCacheNameSpace::NS_PKG,
                                                            effective_tenant_id));
      OX (package_body = static_cast<ObPLPackage*>(cacheobj_guard->get_cache_obj()));
      CK (OB_NOT_NULL(package_body));
      OZ (package_body->init(package_body_ast));

      for (int64_t i = 0; OB_SUCC(ret) &&  i < package_spec_ast.get_dependency_table().count(); ++i) {
        OZ (package_body_ast.add_dependency_object(package_spec_ast.get_dependency_table().at(i)));
      }
      OZ (compiler.compile_package(package_body_info,
                                  &(package_spec_ast.get_body()->get_namespace()),
                                  package_body_ast,
                                  *package_body));
      if (OB_SUCC(ret)
          && package_body->get_can_cached()
          && OB_FAIL(add_package_to_plan_cache(resolve_ctx, package_body))) {
        LOG_WARN("add package body to plan cache failed", K(package_body_id), K(ret));
        ret = OB_SUCCESS; //cache add failed, need not fail execute path
      }
      OZ (resolve_ctx.package_guard_.put(package_body_id, cacheobj_guard));
      if (OB_FAIL(ret) && OB_NOT_NULL(package_body)) {
        // pointer should be free manually
        cacheobj_guard->~ObCacheObjGuard();
        package_body = NULL;
      }
    }
  }
  return ret;
}

int ObPLPackageManager::check_version(const ObPLResolveCtx &resolve_ctx, uint64_t package_id,
                                      const ObPackageStateVersion &state_version, bool &match)
{
  int ret = OB_SUCCESS;
  match = true;
  if (OB_INVALID_ID == package_id
      || !state_version.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant or package id is invalid", K(ret), K(package_id));
  } else {
    ObPLPackage *package_spec = NULL;
    ObPLPackage *package_body = NULL;
    if (OB_FAIL(get_cached_package(resolve_ctx, package_id, package_spec, package_body))) {
      LOG_WARN("failed to get cached package", K(ret), K(package_id));
    } else {
      if (state_version.package_version_ == package_spec->get_version()) {
        if (OB_NOT_NULL(package_body) && state_version.package_body_version_ != package_body->get_version()) {
          match = false;
        }
      } else {
        match = false;
      }
    }
  }
  return ret;
}

int ObPLPackageManager::get_package_schema_info(ObSchemaGetterGuard &schema_guard,
                                                uint64_t package_id,
                                                const ObPackageInfo *&package_spec_info,
                                                const ObPackageInfo *&package_body_info)
{
  int ret = OB_SUCCESS;
  package_spec_info = NULL;
  package_body_info = NULL;
  int64_t compatible_mode = lib::is_oracle_mode() ? COMPATIBLE_ORACLE_MODE
                                                  : COMPATIBLE_MYSQL_MODE;
  if (!ObTriggerInfo::is_trigger_package_id(package_id)) {
    const uint64_t tenant_id = get_tenant_id_by_object_id(package_id);
    const ObPackageInfo *tmp_package_info = NULL;
    if (OB_FAIL(schema_guard.get_package_info(tenant_id, package_id, tmp_package_info))) {
      LOG_WARN("failed to get package info", K(tenant_id), K(package_id), K(ret));
    } else if (OB_ISNULL(tmp_package_info)) {
      ret = OB_ERR_PACKAGE_DOSE_NOT_EXIST;
      LOG_WARN("package info is NULL", K(package_id), K(ret));
    } else {
      if (share::schema::PACKAGE_TYPE == tmp_package_info->get_type()) {
        package_spec_info = tmp_package_info;
        if (OB_FAIL(schema_guard.get_package_info(tmp_package_info->get_tenant_id(),
                                                  tmp_package_info->get_database_id(),
                                                  tmp_package_info->get_package_name(),
                                                  share::schema::PACKAGE_BODY_TYPE,
                                                  compatible_mode,
                                                  package_body_info))) {
          LOG_WARN("failed to get package body info", "package name", package_spec_info->get_package_name(), K(ret));
        }
      } else {
        package_body_info = tmp_package_info;
        if (OB_FAIL(schema_guard.get_package_info(tmp_package_info->get_tenant_id(),
                                                  tmp_package_info->get_database_id(),
                                                  tmp_package_info->get_package_name(),
                                                  share::schema::PACKAGE_TYPE,
                                                  compatible_mode,
                                                  package_spec_info))) {
          LOG_WARN("failed to get package info", "package name", package_body_info->get_package_name(), K(ret));
        } else if (OB_ISNULL(package_spec_info)) {
          ret = OB_ERR_PACKAGE_DOSE_NOT_EXIST;
          LOG_WARN("package body info is NULL", K(ret));
          {
            ObString db_name("");
            const ObDatabaseSchema *database_schema = NULL;
            if (OB_SUCCESS == schema_guard.get_database_schema(tmp_package_info->get_tenant_id(),
                tmp_package_info->get_database_id(), database_schema)) {
              if (NULL != database_schema) {
                db_name =database_schema->get_database_name_str();
              }
            }
            LOG_USER_ERROR(OB_ERR_PACKAGE_DOSE_NOT_EXIST, "PACKAGE",
                           db_name.length(), db_name.ptr(),
                           tmp_package_info->get_package_name().length(),
                           tmp_package_info->get_package_name().ptr());
          }
        }
      }
    }
  } else {
    if (OB_FAIL(schema_guard.get_package_info_from_trigger(MTL_ID(),
                                                           package_id,
                                                           package_spec_info,
                                                           package_body_info))) {
      LOG_WARN("failed to get package info from trigger", K(ret), K(package_id));
    }
  }
  return ret;
}

int ObPLPackageManager::get_cached_package_spec(const ObPLResolveCtx &resolve_ctx,
                                                uint64_t package_id,
                                                ObPLPackage *&package_spec)
{
  int ret = OB_SUCCESS;
  package_spec = NULL;
  bool is_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recusive", K(ret));
  }
  CK (OB_LIKELY(OB_INVALID_ID != package_id));

  ObCacheObjGuard* guard = NULL;
  OX (ret = resolve_ctx.package_guard_.get(package_id, guard));
  if (OB_SUCC(ret) && OB_NOT_NULL(guard)) {
    OX (package_spec = static_cast<ObPLPackage*>(guard->get_cache_obj()));
    CK (OB_NOT_NULL(package_spec));
  } else if (OB_HASH_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
    const ObPackageInfo *package_info = NULL;
    const uint64_t tenant_id = get_tenant_id_by_object_id(package_id);
    OZ (resolve_ctx.schema_guard_.get_package_info(tenant_id, package_id, package_info), package_id);
    OV (OB_NOT_NULL(package_info), OB_ERR_UNEXPECTED, K(tenant_id), K(package_id));
    OZ (get_package_from_plan_cache(resolve_ctx, package_id, package_spec), package_id);
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(package_spec)) {
        OZ (load_package_spec(resolve_ctx, *package_info, package_spec), package_id);
      } else {
        if (package_info->get_schema_version() != package_spec->get_version()) {
          ret = OB_NEED_RETRY;
          LOG_ERROR("package version changed",
                    K(package_id), K(ret), KPC(package_info), KPC(package_spec));
        }
      }
    }
  } else if (OB_FAIL(ret)) {
    LOG_WARN("failed to get package from local cache", K(ret), K(package_id));
  }
  return ret;
}

int ObPLPackageManager::get_cached_package(const ObPLResolveCtx &resolve_ctx,
                                           uint64_t package_id,
                                           ObPLPackage *&package_spec,
                                           ObPLPackage *&package_body,
                                           bool for_static_member)
{
  int ret = OB_SUCCESS;
  package_spec = NULL;
  package_body = NULL;
  bool is_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recusive", K(ret));
  }
  CK (OB_LIKELY(OB_INVALID_ID != package_id));
  if (OB_SUCC(ret)) {
    const ObPackageInfo *package_spec_info = NULL;
    const ObPackageInfo *package_body_info = NULL;
    uint64_t package_spec_id = OB_INVALID_ID;
    uint64_t package_body_id = OB_INVALID_ID;
    OZ (get_package_schema_info(resolve_ctx.schema_guard_, package_id, package_spec_info, package_body_info));
    OV (OB_NOT_NULL(package_spec_info), OB_ERR_UNEXPECTED, K(package_id));
    OX (package_spec_id = package_spec_info->get_package_id());
    OZ (get_cached_package_spec(resolve_ctx, package_spec_id, package_spec));
    CK (OB_NOT_NULL(package_spec));
    if (OB_SUCC(ret)
        && OB_NOT_NULL(package_body_info)
        && (!for_static_member || package_id != package_spec_id)) {
      OX (package_body_id = package_body_info->get_package_id());

      ObCacheObjGuard* guard = NULL;
      OX (ret = resolve_ctx.package_guard_.get(package_body_id, guard));
      if (OB_SUCC(ret) && OB_NOT_NULL(guard)) {
        OX (package_body = static_cast<ObPLPackage*>(guard->get_cache_obj()));
        CK (OB_NOT_NULL(package_body));
      } else if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        OZ (get_package_from_plan_cache(resolve_ctx, package_body_id, package_body));
        if (OB_SUCC(ret) && OB_ISNULL(package_body)) {
          OZ (load_package_body(resolve_ctx, *package_spec_info, *package_body_info, package_body));
          CK (OB_NOT_NULL(package_body));
        }
        if (OB_SUCC(ret)
            && package_body_info->get_schema_version() != package_body->get_version()) {
          ret = OB_NEED_RETRY;
          LOG_ERROR("package body version changed", K(package_body_id), K(ret));
        }
      } else {
        LOG_WARN("failed to get package body from local cache", K(ret), K(package_body_id));
      }
    }
  }
  return ret;
}

int ObPLPackageManager::get_package_item_state(ObSQLSessionInfo &session_info,
                                               int64_t package_id,
                                               const ObPackageStateVersion &state_version,
                                               ObPLPackageState *&package_state)
{
  int ret = OB_SUCCESS;
  int hash_ret = session_info.get_package_state(package_id, package_state);
  if (OB_HASH_NOT_EXIST == hash_ret) {
    package_state = NULL;
  } else if (hash_ret != OB_SUCCESS) {
    ret = hash_ret;
    LOG_WARN("failed to get package state from session", K(hash_ret), K(ret));
  } else if (OB_NOT_NULL(package_state) && !package_state->check_version(state_version)) {
    package_state = NULL;
  }
  return ret;
}

int ObPLPackageManager::get_package_item_state(const ObPLResolveCtx &resolve_ctx,
                                               ObExecContext &exec_ctx,
                                               ObPLPackage &package,
                                               const ObPackageStateVersion &state_version,
                                               ObPLPackageState *&package_state)
{
  int ret = OB_SUCCESS;
  package_state = NULL;
  ObIAllocator &session_allocator = resolve_ctx.session_info_.get_package_allocator();
  uint64_t package_id = package.get_id();
  bool need_new = false;
  int hash_ret = resolve_ctx.session_info_.get_package_state(package_id, package_state);
  if (OB_HASH_NOT_EXIST == hash_ret) {
    need_new = true;
  } else if (OB_SUCCESS != hash_ret) {
    ret = hash_ret;
    LOG_WARN("get package state failed", K(package_id), K(ret));
  } else if (!package_state->check_version(state_version)) {
    OZ (resolve_ctx.session_info_.del_package_state(package_id));
    if (OB_SUCC(ret)) {
      package_state->reset(&(resolve_ctx.session_info_));
      package_state->~ObPLPackageState();
      session_allocator.free(package_state);
      package_state = NULL;
      need_new = true;
    }
  }
  if (OB_SUCC(ret) && need_new) {
    CK (OB_ISNULL(package_state));
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(package_state =
        static_cast<ObPLPackageState *>(session_allocator.alloc(sizeof(ObPLPackageState))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("memory allocate failed", K(ret));
    } else {
      new (package_state)
        ObPLPackageState(package_id, state_version, package.get_serially_reusable());
      ExecCtxBak exec_ctx_bak;
      sql::ObExecEnv exec_env_bak;
      ObArenaAllocator tmp_allocator;
      OX (exec_ctx_bak.backup(exec_ctx));
      OZ (exec_env_bak.load(resolve_ctx.session_info_, &tmp_allocator));
      OZ (package_state->init());
      if (OB_SUCC(ret)) {
        OZ (package.get_exec_env().store(resolve_ctx.session_info_));
        sql::ObPhysicalPlanCtx phy_plan_ctx(exec_ctx.get_allocator());
        OX (exec_ctx.set_physical_plan_ctx(&phy_plan_ctx));
        if (OB_SUCC(ret) && package.get_expr_op_size() > 0)  {
          OZ (exec_ctx.init_expr_op(package.get_expr_op_size()));
        }
        // 要先加到SESSION上然后在初始化, 反之会造成死循环
        OZ (resolve_ctx.session_info_.add_package_state(package_id, package_state));
        if (OB_SUCC(ret)) {
          // TODO bin.lb: how about the memory?
          //
          OZ(package.get_frame_info().pre_alloc_exec_memory(exec_ctx));
        }
        int tmp_ret = OB_SUCCESS;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(package.instantiate_package_state(resolve_ctx, exec_ctx, *package_state))) {
          if (OB_SUCCESS != (tmp_ret = resolve_ctx.session_info_.del_package_state(package_id))) {
            // 删除失败, 为了避免一个未知的状态, 重新初始化这段内存, 使之是无效状态
            new (package_state)
              ObPLPackageState(package_id, state_version, package.get_serially_reusable());
            LOG_WARN("failed to del package state", K(ret), K(package_id), K(tmp_ret));
          } else {
            // 删除成功将内存释放
            package_state->reset(&(resolve_ctx.session_info_));
            package_state->~ObPLPackageState();
            session_allocator.free(package_state);
            package_state = NULL;
            LOG_WARN("failed to call instantiate_package_state", K(ret), K(package_id));
          }
        }
        if (package.get_expr_op_size() > 0) {
          //Memory leak
          //Must be reset before free expr_op_ctx!
          exec_ctx.reset_expr_op();
          exec_ctx.get_allocator().free(exec_ctx.get_expr_op_ctx_store());
        }
        exec_ctx_bak.restore(exec_ctx);
        if (OB_SUCCESS != (tmp_ret = exec_env_bak.store(resolve_ctx.session_info_))) {
          LOG_WARN("failed to restore package exec env", K(ret), K(tmp_ret));
          ret = OB_SUCCESS == ret ? tmp_ret : ret;
        }
      }
    }
  }
  return ret;
}

int ObPLPackageManager::get_package_state(const ObPLResolveCtx &resolve_ctx,
                                          sql::ObExecContext &exec_ctx,
                                          uint64_t package_id,
                                          ObPLPackageState *&package_state,
                                          bool for_static_member)
{
  int ret = OB_SUCCESS;
  package_state = NULL;
  if (OB_INVALID_ID == package_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid package id", K(ret));
  } else {
    ObPLPackage* package_spec = NULL;
    ObPLPackage* package_body = NULL;
    if (OB_FAIL(get_cached_package(
      resolve_ctx, package_id, package_spec, package_body, for_static_member))) {
      LOG_WARN("get package failed", K(package_id), K(ret));
    } else if (OB_ISNULL(package_spec)) {
      ret = OB_ERR_PACKAGE_DOSE_NOT_EXIST;
      LOG_WARN("package spec not exist", K(package_id), K(ret));
    } else {
      ObPackageStateVersion state_version(package_spec->get_version(), NULL==package_body?OB_INVALID_VERSION:package_body->get_version());
      ObPLPackageState *package_spec_state = NULL;
      ObPLPackageState *package_body_state = NULL;
      if (OB_FAIL(get_package_item_state(resolve_ctx, exec_ctx, *package_spec, state_version, package_spec_state))) {
        LOG_WARN("get pacakge spec state failed", K(ret));
      } else if (!OB_ISNULL(package_body) && OB_FAIL(get_package_item_state(resolve_ctx, exec_ctx, *package_body, state_version, package_body_state))) {
        LOG_WARN("get pacakge body state failed", K(ret));
      } else {
        if (package_id == package_spec->get_id()) {
          package_state = package_spec_state;
        } else {
          package_state = package_body_state;
        }
      }
    }
  }
  return ret;
}

int ObPLPackageManager::add_package_to_plan_cache(const ObPLResolveCtx &resolve_ctx, ObPLPackage *package)
{
  int ret = OB_SUCCESS;
  ObPlanCache *plan_cache = NULL;
  if (OB_ISNULL(package)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cached package is null", K(package));
  } else if (OB_ISNULL(plan_cache = resolve_ctx.session_info_.get_plan_cache())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan cache is null");
  } else {
    int64_t tenant_id = resolve_ctx.session_info_.get_effective_tenant_id();
    uint64_t package_id = package->get_id();
    ObString sql("package");
    //ObArenaAllocator allocator(ObModIds::OB_PL_TEMP);

    //HEAP_VAR(ObExecContext, exec_ctx, allocator) {

      ObPLCacheCtx pc_ctx;
      uint64_t database_id = OB_INVALID_ID;
      resolve_ctx.session_info_.get_database_id(database_id);

      pc_ctx.session_info_ = &resolve_ctx.session_info_;
      pc_ctx.schema_guard_ = &resolve_ctx.schema_guard_;
      (void)ObSQLUtils::md5(sql,pc_ctx.sql_id_, (int32_t)sizeof(pc_ctx.sql_id_));
      pc_ctx.key_.namespace_ = ObLibCacheNameSpace::NS_PKG;
      pc_ctx.key_.db_id_ = database_id;
      pc_ctx.key_.key_id_ = package_id;
      pc_ctx.key_.sessid_ =
        (get_tenant_id_by_object_id(package_id) != OB_SYS_TENANT_ID && resolve_ctx.session_info_.is_pl_debug_on())
          ? resolve_ctx.session_info_.get_sessid() : 0;
      pc_ctx.key_.mode_ = resolve_ctx.session_info_.get_pl_profiler() != nullptr
                          ? ObPLObjectKey::ObjectMode::PROFILE : ObPLObjectKey::ObjectMode::NORMAL;

      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(ObPLCacheMgr::add_pl_cache(resolve_ctx.session_info_.get_plan_cache(), package, pc_ctx))) {
        if (OB_SQL_PC_PLAN_DUPLICATE == ret) {
          LOG_INFO("package has been added by others, need not add again", K(package_id), K(ret));
          ret = OB_SUCCESS;
        } else if (OB_REACH_MEMORY_LIMIT == ret || OB_SQL_PC_PLAN_SIZE_LIMIT == ret) {
          if (REACH_TIME_INTERVAL(1000000)) { //1s, 当内存达到上限时, 该日志打印会比较频繁, 所以以1s为间隔打印
            LOG_INFO("can't add plan to plan cache",
                     K(package_id), K(package->get_mem_size()), K(plan_cache->get_mem_used()), K(ret));
          }
          ret = OB_SUCCESS;
        } else if (OB_REACH_MAX_CONCURRENT_NUM != ret) { //如果是达到限流上限, 则将错误码抛出去
          LOG_WARN("add package to ObPlanCache failed",
                    K(package_id), K(ret), K(package->get_dependency_table()));
          ret = OB_SUCCESS; //add package出错, 覆盖错误码, 确保因plan cache失败不影响正常执行路径
        }
      } else {
        LOG_INFO("add pl package to plan cache success",
                 K(ret), K(package_id), K(package->get_dependency_table()), K(pc_ctx.key_));
      }
      //exec_ctx.set_physical_plan_ctx(NULL);
    //}
  }
  return ret;
}

int ObPLPackageManager::get_package_from_plan_cache(const ObPLResolveCtx &resolve_ctx,
                                                    uint64_t package_id,
                                                    ObPLPackage *&package)
{
  int ret = OB_SUCCESS;
  package = NULL;
  bool is_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recusive", K(ret));
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else {
    ObString sql("package");
    //SMART_VAR(ObExecContext, exec_ctx, resolve_ctx.allocator_) {

      uint64_t database_id = OB_INVALID_ID;
      resolve_ctx.session_info_.get_database_id(database_id);

      ObPLCacheCtx pc_ctx;
      pc_ctx.session_info_ = &resolve_ctx.session_info_;
      pc_ctx.schema_guard_ = &resolve_ctx.schema_guard_;
      (void)ObSQLUtils::md5(sql,pc_ctx.sql_id_, (int32_t)sizeof(pc_ctx.sql_id_));
      pc_ctx.key_.namespace_ = ObLibCacheNameSpace::NS_PKG;
      pc_ctx.key_.db_id_ = database_id;
      pc_ctx.key_.key_id_ = package_id;
      pc_ctx.key_.sessid_ =
        (get_tenant_id_by_object_id(package_id) != OB_SYS_TENANT_ID && resolve_ctx.session_info_.is_pl_debug_on())
          ? resolve_ctx.session_info_.get_sessid() : 0;
      pc_ctx.key_.mode_ = resolve_ctx.session_info_.get_pl_profiler() != nullptr
                          ? ObPLObjectKey::ObjectMode::PROFILE : ObPLObjectKey::ObjectMode::NORMAL;

      // get package from plan cache
      ObCacheObjGuard* cacheobj_guard = NULL;
      void* buf = NULL;
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(buf = resolve_ctx.package_guard_.alloc_.alloc(sizeof(ObCacheObjGuard)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory.", K(ret));
      } else if (FALSE_IT(cacheobj_guard = new (buf)ObCacheObjGuard(GET_PKG_HANDLE))) {
        // do nothing
      } else if (OB_FAIL(ObPLCacheMgr::get_pl_cache(resolve_ctx.session_info_.get_plan_cache(), *cacheobj_guard, pc_ctx))) {
        LOG_INFO("get pl package from plan cache failed", K(ret), K(package_id));
        if (OB_ERR_UNEXPECTED != ret) {
          ret = OB_SUCCESS;
        }
      } else if (FALSE_IT(package = static_cast<ObPLPackage*>(cacheobj_guard->get_cache_obj()))) {
        // do nothing
      } else if (OB_NOT_NULL(package)) {
        if (OB_FAIL(resolve_ctx.package_guard_.put(package_id, cacheobj_guard))) {
          LOG_WARN("failed to put package to package guard", K(ret), K(package_id));
          // pointer should be free manualy
          cacheobj_guard->~ObCacheObjGuard();
          package = NULL;
        } else {
          LOG_DEBUG("get package from plan cache success", K(ret), K(package_id));
        }
      } else {}
    //}
  }
  return ret;
}

int ObPLPackageManager::destory_package_state(sql::ObSQLSessionInfo &session_info, uint64_t package_id)
{
  int ret = OB_SUCCESS;
  ObPLPackageState *package_state;
  if (OB_FAIL(session_info.get_package_state(package_id, package_state))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("get package state failed", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_ISNULL(package_state)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("package state is null", K(ret));
  } else if (OB_FAIL(session_info.del_package_state(package_id))) {
    LOG_WARN("delete package state failed", K(package_state), K(ret));
  } else {
    package_state->reset(&session_info);
    package_state->~ObPLPackageState();
    session_info.get_package_allocator().free(package_state);
    package_state = NULL;
  }
  return ret;
}

int ObPLPackageManager::notify_package_variable_deserialize(ObBasicSessionInfo *session, const ObString &name, const ObSessionVariable &value)
{
  int ret = OB_SUCCESS;

#ifdef OB_BUILD_ORACLE_PL

  ObPackageVarSetName pkg_var_info;
  ObArenaAllocator allocator;

  CK (OB_NOT_NULL(session));

  OZ (pkg_var_info.decode(allocator, name), name);

  if (OB_SUCC(ret) && OB_SYS_TENANT_ID == get_tenant_id_by_object_id(pkg_var_info.package_id_)) {
    ObSchemaGetterGuard schema_guard;
    const ObPackageInfo *package_info = nullptr;

    if (OB_ISNULL(GCTX.schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL GCTX.schema_service_", K(ret), K(GCTX.schema_service_));
    } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
      LOG_WARN("failed to get schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.get_package_info(OB_SYS_TENANT_ID, pkg_var_info.package_id_, package_info))) {
      LOG_WARN("failed to get package info", K(ret), K(pkg_var_info), KPC(package_info));
    } else if (OB_ISNULL(package_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL package info", K(ret), K(pkg_var_info), KPC(package_info));
    } else if (0 == package_info->get_package_name().compare("DBMS_PROFILER")) {
      if (OB_FAIL(ObDBMSProfiler::set_profiler_by_user_var_deserialize(*static_cast<ObSQLSessionInfo*>(session),
                                                                       pkg_var_info,
                                                                       value))) {
        LOG_WARN("[DBMS_PROFILER] failed to set session profiler status by package var deserialize",
                 K(ret), K(pkg_var_info), K(value));
      }
    }
  }

#endif // OB_BUILD_ORACLE_PL

  return ret;
}

} // end namespace pl
} // end namespace oceanbase
