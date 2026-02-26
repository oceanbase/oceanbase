/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef _OB_TABLE_COMMON_STRUCT_H
#define _OB_TABLE_COMMON_STRUCT_H

#include "share/table/ob_table_rpc_struct.h"
#include "observer/table/ob_table_context.h"
#include "observer/table/object_pool/ob_table_object_pool.h"
#include "observer/table/ob_table_schema_cache.h"
#include "observer/table/ob_table_audit.h"
#include "observer/table/utils/ob_table_trans_utils.h"

namespace oceanbase
{
namespace table
{

class ObTableExecCtx
{
public:
  ObTableExecCtx(common::ObIAllocator& alloc,
                 table::ObTableApiSessGuard &sess_guard,
                 table::ObKvSchemaCacheGuard &schema_cache_guard,
                 table::ObTableAuditCtx *audit_ctx,
                 table::ObTableApiCredential &credential,
                 share::schema::ObSchemaGetterGuard &schema_guard,
                 table::ObTableTransParam &trans_param)
    : alloc_(alloc),
      cb_alloc_(nullptr),
      sess_guard_(sess_guard),
      schema_cache_guard_(schema_cache_guard),
      audit_ctx_(audit_ctx),
      credential_(credential),
      schema_guard_(schema_guard),
      trans_param_(trans_param),
      timeout_ts_(-1),
      table_name_(),
      table_id_(common::OB_INVALID_ID),
      ls_id_(ObLSID::INVALID_LS_ID),
      entity_factory_(nullptr),
      simple_schema_(nullptr),
      table_schema_(nullptr),
      is_async_commit_(false)
  {}
  ~ObTableExecCtx() = default;
  TO_STRING_KV(K_(sess_guard),
               K_(schema_cache_guard),
               K_(audit_ctx),
               K_(credential),
               K_(timeout_ts),
               K_(table_name),
               K_(table_id),
               K_(ls_id),
               K_(is_async_commit)
               );
public:
  OB_INLINE common::ObIAllocator &get_allocator() const { return alloc_; }
  OB_INLINE void set_cb_allocator(common::ObIAllocator *alloc) { cb_alloc_ = alloc; }
  OB_INLINE common::ObIAllocator *get_cb_allocator() const { return cb_alloc_; }
  OB_INLINE table::ObTableApiSessGuard &get_sess_guard() const { return sess_guard_; }
  OB_INLINE table::ObKvSchemaCacheGuard &get_schema_cache_guard() const { return schema_cache_guard_; }
  OB_INLINE table::ObTableAuditCtx *get_audit_ctx() const { return audit_ctx_; }
  OB_INLINE table::ObTableApiCredential &get_credential() const { return credential_; }
  OB_INLINE share::schema::ObSchemaGetterGuard &get_schema_guard() const { return schema_guard_; }
  OB_INLINE table::ObTableTransParam &get_trans_param() const { return trans_param_; }
  OB_INLINE const transaction::ObTxReadSnapshot &get_tx_snapshot() const { return trans_param_.tx_snapshot_; }
  OB_INLINE int64_t get_timeout_ts() const { return timeout_ts_; }
  OB_INLINE void set_timeout_ts(int64_t timeout_ts) { timeout_ts_ = timeout_ts; }
  OB_INLINE const common::ObString &get_table_name() const { return table_name_; }
  OB_INLINE void set_table_name(const common::ObString &table_name) { table_name_ = table_name; }
  OB_INLINE uint64_t get_table_id() const { return table_id_; }
  OB_INLINE void set_table_id(const uint64_t table_id) { table_id_ = table_id; }
  OB_INLINE void set_audit_ctx(table::ObTableAuditCtx &audit_ctx) { audit_ctx_ = &audit_ctx; }
  OB_INLINE ObITableEntityFactory *get_entity_factory() const { return entity_factory_; }
  OB_INLINE void set_entity_factory(ObITableEntityFactory *entity_factory) { entity_factory_ = entity_factory; }
  OB_INLINE const share::ObLSID &get_ls_id() const { return ls_id_; }
  OB_INLINE void set_ls_id(const share::ObLSID &ls_id) { ls_id_ = ls_id; }
  OB_INLINE const share::schema::ObTableSchema *get_table_schema() const { return table_schema_; }
  OB_INLINE void set_table_schema(const share::schema::ObTableSchema *table_schema) { table_schema_ = table_schema; }
  // if table_schema is set and simple_schema is not set, return table_schema_ directly to avoid init simple_schema_ repeatedly.
  OB_INLINE const share::schema::ObSimpleTableSchemaV2 *get_simple_schema() const
  {
    return simple_schema_ == nullptr ? (table_schema_ != nullptr ?  table_schema_ : nullptr) : simple_schema_;
  }
  OB_INLINE void set_simple_schema(const share::schema::ObSimpleTableSchemaV2 *simple_schema) { simple_schema_ = simple_schema; }
  OB_INLINE void set_async_commit(bool is_async) { is_async_commit_ = is_async; }
  OB_INLINE bool is_async_commit() const { return is_async_commit_; }
private:
  common::ObIAllocator &alloc_;
  common::ObIAllocator *cb_alloc_;
  table::ObTableApiSessGuard &sess_guard_;
  table::ObKvSchemaCacheGuard &schema_cache_guard_;
  table::ObTableAuditCtx *audit_ctx_;
  table::ObTableApiCredential &credential_;
  share::schema::ObSchemaGetterGuard &schema_guard_;
  table::ObTableTransParam &trans_param_;
  int64_t timeout_ts_;
  common::ObString table_name_;
  uint64_t table_id_;
  share::ObLSID ls_id_;
  ObITableEntityFactory *entity_factory_;
  const share::schema::ObSimpleTableSchemaV2 *simple_schema_;
  const share::schema::ObTableSchema *table_schema_;
  bool is_async_commit_;
};

struct ObTableQMParam
{
  const table::ObITableQueryAndMutate &query_and_mutate_;
  uint64_t table_id_;
  common::ObTabletID tablet_id_;
};


} // end of namespace table
} // end of namespace oceanbase

#endif // _OB_TABLE_COMMON_STRUCT_H
