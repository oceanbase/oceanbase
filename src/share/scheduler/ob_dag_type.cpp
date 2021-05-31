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

#include "share/scheduler/ob_dag_type.h"

namespace oceanbase {
namespace share {

const TypeBasicSetting OB_DAG_TYPE[] = {
#define DAG_TYPE_SETTING_DEF(def, default_score) {default_score},
#include "share/scheduler/ob_dag_type.h"
#undef DAG_TYPE_SETTING_DEF
};

const common::ObString ObDagTypeStr[ObDagTypeIds::TYPE_SETTING_END] = {"MINOR_MERGE", "MAJOR_MERGE", "MINI_MERGE"};

}  // namespace share
}  // namespace oceanbase
