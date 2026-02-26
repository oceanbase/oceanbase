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

#ifndef OCEANBASE_SHARE_DOMAIN_INDEX_BUILDER_UTIL_H_
#define OCEANBASE_SHARE_DOMAIN_INDEX_BUILDER_UTIL_H_

#include "src/rootserver/ddl_task/ob_ddl_task.h"

namespace oceanbase
{
namespace share
{

struct ObDomainDependTaskStatus final
{
public:
ObDomainDependTaskStatus()
    : ret_code_(INT64_MAX), task_id_(0)
{}
~ObDomainDependTaskStatus() = default;
TO_STRING_KV(K_(task_id), K_(ret_code));
public:
int64_t ret_code_;
int64_t task_id_;
};

class ObDomainIndexBuilderUtil
{
public:
  static int prepare_aux_table(bool &task_submitted,
                                uint64_t &aux_table_id,
                                int64_t &res_task_id,
                                const common::TCRWLock& lock,
                                const uint64_t& data_table_id,
                                const uint64_t& tenant_id,
                                const int64_t& task_id,
                                obrpc::ObCreateIndexArg& index_arg,
                                rootserver::ObRootService *root_service,
                                common::hash::ObHashMap<uint64_t, ObDomainDependTaskStatus> &map,
                                const oceanbase::common::ObAddr &addr,
                                int map_num,
                                const int64_t snapshot_version);
  static int retrieve_complete_domain_index(const ObIArray<ObTableSchema> &shared_schema_array,
                                            const ObIArray<ObTableSchema> &domain_schema_array,
                                            const ObIArray<ObTableSchema> &aux_schema_array,
                                            ObArenaAllocator &allocator,
                                            const uint64_t new_data_table_id,
                                            ObIArray<ObTableSchema> &rebuid_index_schemas,
                                            const bool need_doc_id,
                                            const bool need_vid);

private:
  static int locate_aux_index_schema_by_name(const ObString &inner_index_name,
                                             const uint64_t new_data_table_id,
                                             const ObIArray<ObTableSchema> &domain_index_schemas,
                                             const share::schema::ObIndexType type,
                                             ObArenaAllocator &allocator,
                                             int64_t &index_aux_schema_idx);
};


}//end namespace share
}//end namespace oceanbase

#endif //OCEANBASE_SHARE_DOMAIN_INDEX_BUILDER_UTIL_H_
