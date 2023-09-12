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

#ifndef _OB_RS_JOB_TABLE_OPERATOR_H
#define _OB_RS_JOB_TABLE_OPERATOR_H 1

#include "lib/net/ob_addr.h"
#include "share/ob_dml_sql_splicer.h"

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
namespace sqlclient
{
class ObMySQLResult;
}
}
namespace rootserver
{
// define template <..> ob_build_dml_elements(dml, name1, value1, name2, value2, ...)
#define BUILD_DML_TEMPLATE_TYPE(N) CAT(typename T, N)
#define BUILD_DML_ARG_PAIR(N) const char* CAT(key, N), const CAT(T, N) &CAT(obj, N)
#define BUILD_DML_ADD_ONE(N)                         \
  if (OB_SUCC(ret)) {                                   \
    ret = dml.add_column( CAT(key, N), CAT(obj,N));     \
  }

#define BUILD_DML_ELEMENTS(N)                                           \
  template < LST_DO_(N, BUILD_DML_TEMPLATE_TYPE, (,), PROC_ONE, ONE_TO_HUNDRED) > \
  int ob_build_dml_elements(::oceanbase::share::ObDMLSqlSplicer &dml,   \
                            LST_DO_(N, BUILD_DML_ARG_PAIR, (,), PROC_ONE, ONE_TO_HUNDRED) \
                            )                                           \
  {                                                                     \
    int ret = oceanbase::common::OB_SUCCESS;                            \
    LST_DO_(N, BUILD_DML_ADD_ONE, (), PROC_ONE, ONE_TO_HUNDRED); \
    return ret;                                                         \
  }

BUILD_DML_ELEMENTS(1);
BUILD_DML_ELEMENTS(2);
BUILD_DML_ELEMENTS(3);
BUILD_DML_ELEMENTS(4);
BUILD_DML_ELEMENTS(5);
BUILD_DML_ELEMENTS(6);
BUILD_DML_ELEMENTS(7);
BUILD_DML_ELEMENTS(8);
BUILD_DML_ELEMENTS(9);
BUILD_DML_ELEMENTS(10);
BUILD_DML_ELEMENTS(11);
BUILD_DML_ELEMENTS(12);
BUILD_DML_ELEMENTS(13);
BUILD_DML_ELEMENTS(14);
BUILD_DML_ELEMENTS(15);
BUILD_DML_ELEMENTS(16);
BUILD_DML_ELEMENTS(17);
BUILD_DML_ELEMENTS(18);
BUILD_DML_ELEMENTS(19);
BUILD_DML_ELEMENTS(20);

// @note modify ObRsJobTableOperator::job_type_str if you modify ObRsJobType
enum ObRsJobType
{
  JOB_TYPE_INVALID = 0,
  JOB_TYPE_ALTER_TENANT_LOCALITY,
  JOB_TYPE_ROLLBACK_ALTER_TENANT_LOCALITY, // deprecated in V4.2
  JOB_TYPE_MIGRATE_UNIT,
  JOB_TYPE_DELETE_SERVER,
  JOB_TYPE_SHRINK_RESOURCE_TENANT_UNIT_NUM, // deprecated in V4.2
  JOB_TYPE_RESTORE_TENANT,
  JOB_TYPE_UPGRADE_STORAGE_FORMAT_VERSION,
  JOB_TYPE_STOP_UPGRADE_STORAGE_FORMAT_VERSION,
  JOB_TYPE_CREATE_INNER_SCHEMA,
  JOB_TYPE_UPGRADE_POST_ACTION,
  JOB_TYPE_UPGRADE_SYSTEM_VARIABLE,
  JOB_TYPE_UPGRADE_SYSTEM_TABLE,
  JOB_TYPE_UPGRADE_BEGIN,
  JOB_TYPE_UPGRADE_VIRTUAL_SCHEMA,
  JOB_TYPE_UPGRADE_SYSTEM_PACKAGE,
  JOB_TYPE_UPGRADE_ALL_POST_ACTION,
  JOB_TYPE_UPGRADE_INSPECTION,
  JOB_TYPE_UPGRADE_END,
  JOB_TYPE_UPGRADE_ALL,
  JOB_TYPE_ALTER_RESOURCE_TENANT_UNIT_NUM,
  JOB_TYPE_ALTER_TENANT_PRIMARY_ZONE,
  JOB_TYPE_MAX
};

enum ObRsJobStatus
{
  JOB_STATUS_INVALID = 0,
  JOB_STATUS_INPROGRESS,
  JOB_STATUS_SUCCESS,
  JOB_STATUS_FAILED,
  JOB_STATUS_SKIP_CHECKING_LS_STATUS,
  JOB_STATUS_MAX
};

struct ObRsJobInfo
{
  int64_t job_id_;  // required
  ObRsJobType job_type_;  // required
  common::ObString job_type_str_;
  ObRsJobStatus job_status_;  // required
  common::ObString job_status_str_;
  int64_t result_code_;
  int64_t progress_;// required
  int64_t gmt_create_;// required
  int64_t gmt_modified_;// required
  int64_t tenant_id_;
  common::ObString tenant_name_;
  int64_t database_id_;
  common::ObString database_name_;
  int64_t table_id_;
  common::ObString table_name_;
  int64_t partition_id_;
  common::ObAddr svr_addr_;
  int64_t unit_id_;
  common::ObAddr rs_addr_;// required
  common::ObString sql_text_;
  common::ObString extra_info_;
  int64_t resource_pool_id_;
  //
  int deep_copy_self();
  TO_STRING_KV(K_(job_id),
               K_(job_type),
               K_(job_type_str),
               K_(job_status),
               K_(job_status_str),
               K_(result_code),
               K_(progress),
               K_(gmt_create),
               K_(gmt_modified),
               K_(tenant_id),
               K_(tenant_name),
               K_(database_id),
               K_(database_name),
               K_(table_id),
               K_(table_name),
               K_(partition_id),
               K_(svr_addr),
               K_(unit_id),
               K_(rs_addr),
               K_(sql_text),
               K_(extra_info),
               K_(resource_pool_id)
               );
private:
  common::ObArenaAllocator allocator_;
};

class ObRsJobTableOperator
{
public:
  static const char* get_job_type_str(ObRsJobType job_type);
  static ObRsJobType get_job_type(const common::ObString &job_type_str);
  static ObRsJobStatus get_job_status(const common::ObString &job_status_str);
  static bool is_valid_job_type(const ObRsJobType &rs_job_type);
public:
  ObRsJobTableOperator();
  virtual ~ObRsJobTableOperator() = default;
  int init(common::ObMySQLProxy *sql_client, const common::ObAddr &rs_addr);

  // create a new job with the specified properties
  // @return job_id will be -1 on error
  int create_job(ObRsJobType job_type, share::ObDMLSqlSplicer &pairs, int64_t &job_id, common::ObISQLClient &trans);
  // get job info by id
  int get_job(int64_t job_id, ObRsJobInfo &job_info);
  // find the one job with the search conditions
  int find_job(const ObRsJobType job_type, share::ObDMLSqlSplicer &pairs, int64_t &job_id, common::ObISQLClient &trans);
  // update the job with the specified values
  int update_job(int64_t job_id, share::ObDMLSqlSplicer &pairs, common::ObISQLClient &trans);
  int update_job_progress(int64_t job_id, int64_t progress, common::ObISQLClient &trans);
  int complete_job(int64_t job_id, int result_code, common::ObISQLClient &trans);

  // misc
  int64_t get_max_job_id() const { return max_job_id_; }
  int64_t get_row_count() const { return row_count_; }
  void reset_max_job_id() { max_job_id_ = -1; }
  common::ObMySQLProxy* get_proxy() { return sql_client_;  }
private:
  // types and constants
  static const char* const TABLE_NAME;
  static const int64_t MAX_ROW_COUNT = 100000;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObRsJobTableOperator);
  // function members
  int alloc_job_id(int64_t &job_id);
  int load_max_job_id(int64_t &max_job_id, int64_t &row_count);
  int cons_job_info(const common::sqlclient::ObMySQLResult &res, ObRsJobInfo &job_info);
private:
  // data members
  bool inited_;
  int64_t max_job_id_;
  int64_t row_count_;
  common::ObMySQLProxy *sql_client_;
  common::ObAddr rs_addr_;
  common::ObLatch latch_;
};

class ObRsJobTableOperatorSingleton
{
public:
  static ObRsJobTableOperator &get_instance();
};

} // end namespace rootserver
} // end namespace oceanbase

#define THE_RS_JOB_TABLE ::oceanbase::rootserver::ObRsJobTableOperatorSingleton::get_instance()

// usage: ret = RS_JOB_CREATE_WITH_RET(job_id, JOB_TYPE_ALTER_TENANT_LOCALITY, "tenant_id", 1010);
// no need to fill the following column, all these columns are filled automatically:
// job_type, job_status, gmt_create, gmt_modified, rs_svr_ip, rs_svr_port, progress(0)
#define RS_JOB_CREATE_WITH_RET(job_id, job_type, trans, args...)        \
  ({                                                                    \
    job_id = ::oceanbase::common::OB_INVALID_ID;                    \
    int tmp_ret = ::oceanbase::common::OB_SUCCESS;                      \
    ::oceanbase::share::ObDMLSqlSplicer dml;                            \
    tmp_ret = ::oceanbase::rootserver::ob_build_dml_elements(dml, ##args); \
    if (::oceanbase::common::OB_SUCCESS == tmp_ret) {                   \
      tmp_ret = THE_RS_JOB_TABLE.create_job(job_type, dml, job_id, (trans)); \
    }                                                                   \
    tmp_ret;                                                         \
  })

// the same with RS_JOB_CRATE_WITH_RET
#define RS_JOB_CREATE_EXT(job_id, job_type, trans, args...)             \
  ({                                                                    \
    job_id = -1;                                                        \
    int tmp_ret = ::oceanbase::common::OB_SUCCESS;                      \
    ::oceanbase::share::ObDMLSqlSplicer dml;                            \
    tmp_ret = ::oceanbase::rootserver::ob_build_dml_elements(dml, ##args); \
    if (::oceanbase::common::OB_SUCCESS == tmp_ret) {                   \
      tmp_ret = THE_RS_JOB_TABLE.create_job(JOB_TYPE_ ## job_type, dml, job_id, (trans)); \
    }                                                                   \
    tmp_ret;                                                         \
  })

// update any column except prgress and status
#define RS_JOB_UPDATE(job_id, trans, args...)                           \
  ({                                                                    \
    int tmp_ret = ::oceanbase::common::OB_SUCCESS;                      \
    ::oceanbase::share::ObDMLSqlSplicer dml;                            \
    tmp_ret = ::oceanbase::rootserver::ob_build_dml_elements(dml, ##args); \
    if (::oceanbase::common::OB_SUCCESS == tmp_ret) {            \
      tmp_ret = THE_RS_JOB_TABLE.update_job((job_id), dml, (trans));    \
    }                                                                   \
    tmp_ret;                                                            \
    })

// update the progress
#define RS_JOB_UPDATE_PROGRESS(job_id, progress, trans)                \
  THE_RS_JOB_TABLE.update_job_progress((job_id), (progress), (trans))

// job finished:
// 1. result_code == 0, update status(SUCCESS) and progress(100) automatically
// 2. result_code == -4762, update status(SKIP_CHECKING_LS_STATUS),
//                          the job is finished without checking whether ls are balanced
// 3. result_code == -4072, update status(CANCELED) the job is cancelled by a new job
// 4. else,  update status(FAILED) automatically
#define RS_JOB_COMPLETE(job_id, result_code, trans)                    \
  THE_RS_JOB_TABLE.complete_job((job_id), (result_code), (trans))

// usage: ret = RS_JOB_GET(job_id, job_info);
#define RS_JOB_GET(job_id, job_info)                    \
  THE_RS_JOB_TABLE.get_job((job_id), (job_info))

// usage: ret = RS_JOB_FIND(ALTER_TENANT_LOCALITY, job_id, "tenant_id", 1010);
#define RS_JOB_FIND(job_type, job_id, trans, args...)                                  \
  ({                                                                    \
    int tmp_ret = ::oceanbase::common::OB_SUCCESS;                      \
    ::oceanbase::share::ObDMLSqlSplicer dml;                            \
    tmp_ret = ::oceanbase::rootserver::ob_build_dml_elements(dml, ##args); \
    if (::oceanbase::common::OB_SUCCESS == tmp_ret) {                   \
      tmp_ret = THE_RS_JOB_TABLE.find_job(JOB_TYPE_ ## job_type, dml, job_id, (trans));      \
    }                                                                   \
    tmp_ret;                                                            \
  })

#endif /* _OB_RS_JOB_TABLE_OPERATOR_H */
