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

#ifdef DAG_TYPE_SETTING_DEF
DAG_TYPE_SETTING_DEF(MINOR_MERGE, 2)
DAG_TYPE_SETTING_DEF(MAJOR_MERGE, 3)
DAG_TYPE_SETTING_DEF(MINI_MERGE, 5)
DAG_TYPE_SETTING_DEF(TYPE_SETTING_END, 0)
#endif

#ifndef SRC_SHARE_SCHEDULER_OB_DAG_TYPE_H_
#define SRC_SHARE_SCHEDULER_OB_DAG_TYPE_H_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_vector.h"
#include "lib/allocator/ob_mod_define.h"
namespace oceanbase {
namespace share {

struct ObDagTypeIds {
  enum ObDagTypeIdEnum {
#define DAG_TYPE_SETTING_DEF(def, default_score) def,
#include "share/scheduler/ob_dag_type.h"
#undef DAG_TYPE_SETTING_DEF
  };
};

struct TypeBasicSetting {
  int64_t default_score_;
  static const int64_t DEFAULT_SCORE = 2;
  TypeBasicSetting(int64_t default_score = DEFAULT_SCORE) : default_score_(default_score)
  {}
  virtual ~TypeBasicSetting(){};
};
struct ObTenantTypeSetting;

struct ObTenantSetting {
  typedef common::ObVector<ObTenantTypeSetting> TenantTypeSettingVec;
  const static int64_t NOT_SET = -999;
  int64_t tenant_id_;
  int32_t max_thread_num_;
  TenantTypeSettingVec type_settings_;
  ObTenantSetting(int64_t tenant_id = -1, int32_t max_thread_num = NOT_SET)
      : tenant_id_(tenant_id), max_thread_num_(max_thread_num), type_settings_(0, NULL, common::ObModIds::OB_SCHEDULER)
  {}
  VIRTUAL_TO_STRING_KV(K_(tenant_id), K_(max_thread_num));
};
// type setting in tenant
struct ObTenantTypeSetting {
  int64_t type_id_;
  int64_t score_;
  int64_t up_limit_;
  ObTenantTypeSetting(
      int64_t type_id = -1, int64_t score = ObTenantSetting::NOT_SET, int64_t uplimit = ObTenantSetting::NOT_SET)
      : type_id_(type_id), score_(score), up_limit_(uplimit)
  {}
};

extern const TypeBasicSetting OB_DAG_TYPE[];
extern const common::ObString ObDagTypeStr[];

}  // namespace share
}  // namespace oceanbase
#endif
