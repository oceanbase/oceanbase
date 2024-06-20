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

#ifndef SRC_SHARE_OB_DAG_WARNING_HISTORY_MGR_H_
#define SRC_SHARE_OB_DAG_WARNING_HISTORY_MGR_H_

#include "common/ob_simple_iterator.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/container/ob_bit_set.h"
#include "lib/string/ob_string.h"
#include "lib/ob_errno.h"
#include "lib/hash/ob_hashmap.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/compaction/ob_compaction_diagnose.h"

namespace oceanbase
{
namespace share
{

struct ObDagWarningInfo : public compaction::ObIDiagnoseInfo
{
public:
  enum ObDagStatus {
    ODS_WARNING = 0,
    ODS_RETRYED,
    ODS_MAX,
  };

  static const char * ObDagStatusStr[ODS_MAX];

  static const char *get_dag_status_str(enum ObDagStatus status);

  ObDagWarningInfo();
  ~ObDagWarningInfo();
  OB_INLINE void reset();
  TO_STRING_KV(K_(tenant_id), K_(task_id), K_(dag_type), K_(dag_ret), K_(dag_status),
      K_(gmt_create), K_(gmt_modified), K_(retry_cnt), K_(hash), K_(location));
  virtual void shallow_copy(ObIDiagnoseInfo *other) override;
  virtual void update(ObIDiagnoseInfo *other) override;
  virtual int64_t get_hash() const override;
public:
  share::ObDagId task_id_;
  share::ObDagType::ObDagTypeEnum dag_type_;
  int64_t dag_ret_;
  ObDagStatus dag_status_;
  int64_t gmt_create_;
  int64_t gmt_modified_;
  int64_t retry_cnt_;
  int64_t hash_;
  share::ObDiagnoseLocation location_;
};

OB_INLINE void ObDagWarningInfo::reset()
{
  tenant_id_ = 0;
  task_id_.reset();
  dag_type_ = share::ObDagType::DAG_TYPE_MAX;
  info_param_ = NULL;
  dag_ret_ = OB_SUCCESS;
  dag_status_ = ODS_MAX;
  gmt_create_ = 0;
  gmt_modified_ = 0;
  retry_cnt_ = 0;
  hash_ = 0;
  location_.reset();
}

/*
 * ObDagWarningHistoryManager
 * */

class ObDagWarningHistoryManager : public compaction::ObIDiagnoseInfoMgr {
public:
  static int mtl_init(ObDagWarningHistoryManager *&dag_warning_history);
  static int64_t cal_max();
  ObDagWarningHistoryManager()
    : compaction::ObIDiagnoseInfoMgr(),
      buf_()
  {}
  ~ObDagWarningHistoryManager() { destroy(); }

  int add_dag_warning_info(share::ObIDag *dag);
  int add_dag_warning_info(ObDagWarningInfo &input_info);
  void destroy() {
    ObIDiagnoseInfoMgr::destroy();
    COMMON_LOG(INFO, "ObDagWarningHistoryManager destroy finish");
  }

public:
  static const int64_t MEMORY_PERCENTAGE = 1;   // max size = tenant memory size * MEMORY_PERCENTAGE / 100
  static const int64_t POOL_MAX_SIZE = 64LL * 1024LL * 1024LL; // 64MB

private:
  char buf_[compaction::ObIBasicInfoParam::MAX_INFO_PARAM_SIZE];
};

#define DEFINE_DAG_WARN_INFO_PARAM_ADD(n_int)                                                    \
  template<typename T = int64_t>                                                                 \
  int ADD_DAG_WARN_INFO_PARAM(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator,       \
                ObDagType::ObDagTypeEnum type,  INFO_PARAM_INT##n_int)                           \
  {                                                                                              \
    int64_t __pos = 0;                                                                           \
    int ret = OB_SUCCESS;                                                                        \
    out_param = nullptr;                                                                         \
    void *buf = nullptr;                                                                         \
    compaction::ObDiagnoseInfoParam<n_int, 0> param;                                             \
    compaction::ObDiagnoseInfoParam<n_int, 0> *info_param = nullptr;                             \
    if (OB_ISNULL(buf = allocator.alloc(param.get_deep_copy_size()))) {                          \
      ret = OB_ALLOCATE_MEMORY_FAILED;                                                           \
      COMMON_LOG(WARN, "failed to alloc info param", K(ret));                                      \
    } else if (OB_ISNULL(info_param = (new (buf) compaction::ObDiagnoseInfoParam<n_int, 0>()))) {    \
      ret = OB_ERR_UNEXPECTED;                                                                   \
      COMMON_LOG(WARN, "failed to new. info_param is null", K(ret));                                            \
    } else {                                                                                     \
      info_param->type_.dag_type_ = type;                                                        \
      info_param->struct_type_ = compaction::ObInfoParamStructType::DAG_WARNING_INFO_PARAM;      \
      INT_TO_PARAM_##n_int                                                                       \
      out_param = info_param;                                                                    \
    }                                                                                            \
    return ret;                                                                                  \
  }

#define DEFINE_DAG_WARN_INFO_PARAM_ADD_EXTRA(n, n_int)                                 \
  template <typename T = int64_t, LOG_TYPENAME_TN##n>                                            \
  int ADD_DAG_WARN_INFO_PARAM(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator,       \
                ObDagType::ObDagTypeEnum type, INFO_PARAM_INT##n_int, LOG_PARAMETER_KV##n)       \
  {                                                                                              \
    int64_t __pos = 0;                                                                           \
    int ret = OB_SUCCESS;                                                                        \
    out_param = nullptr;                                                                         \
    void *buf = nullptr;                                                                         \
    compaction::ObDiagnoseInfoParam<n_int, INFO_PARAM_STR_LENGTH(n)> param;                                      \
    compaction::ObDiagnoseInfoParam<n_int, INFO_PARAM_STR_LENGTH(n)> *info_param = nullptr;                      \
    if (OB_ISNULL(buf = allocator.alloc(param.get_deep_copy_size()))) {                          \
      ret = OB_ALLOCATE_MEMORY_FAILED;                                                           \
      COMMON_LOG(WARN, "failed to alloc info param", K(ret));                                     \
    } else if (OB_ISNULL(info_param = (new (buf) compaction::ObDiagnoseInfoParam<n_int, INFO_PARAM_STR_LENGTH(n)>()))) {   \
      ret = OB_ERR_UNEXPECTED;                                                                   \
      COMMON_LOG(WARN, "failed to new. info_param is null", K(ret));                                            \
    } else {                                                                                     \
      info_param->type_.dag_type_ = type;                                                        \
      info_param->struct_type_ = compaction::ObInfoParamStructType::DAG_WARNING_INFO_PARAM;      \
      INT_TO_PARAM_##n_int                                                                       \
      char *buf = info_param->comment_;                                                          \
      const int64_t buf_size = INFO_PARAM_STR_LENGTH(n);                                                         \
      SIMPLE_TO_STRING_##n                                                                       \
      if (OB_FAIL(ret)) {                                                                        \
        COMMON_LOG(WARN, "failed to fill parameter kv into info param", K(ret));                 \
        ret = OB_SUCCESS;                                                                        \
      }                                                                                          \
      if (__pos < buf_size) {                                                                    \
        buf[__pos-1] = '\0';                                                                     \
      } else {                                                                                   \
        buf[buf_size - 1] = '\0';                                                                \
      }                                                                                          \
      out_param = info_param;                                                                    \
    }                                                                                            \
    return ret;                                                                                  \
  }

DEFINE_DAG_WARN_INFO_PARAM_ADD(1)
DEFINE_DAG_WARN_INFO_PARAM_ADD(2)
DEFINE_DAG_WARN_INFO_PARAM_ADD(3)
DEFINE_DAG_WARN_INFO_PARAM_ADD(4)
DEFINE_DAG_WARN_INFO_PARAM_ADD(5)
DEFINE_DAG_WARN_INFO_PARAM_ADD(6)
DEFINE_DAG_WARN_INFO_PARAM_ADD(7)

DEFINE_DAG_WARN_INFO_PARAM_ADD_EXTRA(1,1)
DEFINE_DAG_WARN_INFO_PARAM_ADD_EXTRA(1,2)
DEFINE_DAG_WARN_INFO_PARAM_ADD_EXTRA(1,3)
DEFINE_DAG_WARN_INFO_PARAM_ADD_EXTRA(1,4)
DEFINE_DAG_WARN_INFO_PARAM_ADD_EXTRA(2,2)
DEFINE_DAG_WARN_INFO_PARAM_ADD_EXTRA(2,3)

DEFINE_DAG_WARN_INFO_PARAM_ADD_EXTRA(3,3)
DEFINE_DAG_WARN_INFO_PARAM_ADD_EXTRA(3,4)
}//storage
}//oceanbase

#endif /* SRC_STORAGE_OB_DAG_WARNING_HISTORY_MGR_H_ */
