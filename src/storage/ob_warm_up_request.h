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

#ifndef SRC_STORAGE_OB_WARM_UP_REQUEST_H_
#define SRC_STORAGE_OB_WARM_UP_REQUEST_H_

#include "share/ob_define.h"
#include "lib/net/ob_addr.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/ob_i_data_access_service.h"
#include "lib/container/ob_se_array.h"
#include "ob_dml_param.h"
#include "lib/allocator/ob_fifo_allocator.h"
#include "memtable/ob_memtable_interface.h"
#include "lib/list/ob_dlist.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"
#include "ob_i_table.h"
#include "common/ob_store_range.h"

namespace oceanbase {
namespace storage {
struct ObWarmUpRequestType {
  enum ObWarmUpRequestEnum {
    GET_REQUEST = 1,
    MULTI_GET_REQUEST = 2,
    SCAN_REQUEST = 3,
    EXIST_WARM_REQUEST = 4,
    GET_WARM_REQUEST = 5,
    MULTI_GET_WARM_REQUEST = 6,
    SCAN_WARM_REQUEST = 7,
    MULTI_SCAN_WARM_REQUEST = 8,
    MAX_REQUEST_TYPE,
  };
};

class ObIWarmUpRequest {
public:
  explicit ObIWarmUpRequest(common::ObIAllocator& allocator);
  virtual ~ObIWarmUpRequest();
  virtual ObWarmUpRequestType::ObWarmUpRequestEnum get_request_type() const = 0;
  virtual int warm_up(
      memtable::ObIMemtableCtxFactory* memctx_factory, const common::ObIArray<ObITable*>& stores) const = 0;
  OB_INLINE const common::ObPartitionKey& get_pkey() const
  {
    return pkey_;
  }
  OB_INLINE uint64_t get_index_id() const
  {
    return table_id_;
  }
  TO_STRING_KV(K_(pkey), K_(table_id));
  OB_UNIS_VERSION_V(1);

protected:
  int assign(const common::ObPartitionKey& pkey, const uint64_t table_id,
      const common::ObIArray<share::schema::ObColDesc>& column_ids);
  int prepare_store_ctx(memtable::ObIMemtableCtxFactory* memctx_factory, ObStoreCtx* store_ctx) const;
  void revert_store_ctx(memtable::ObIMemtableCtxFactory* memctx_factory, ObStoreCtx* store_ctx) const;
  static const int64_t DEFAULT_WARM_UP_REQUEST_TIMEOUT_US = 1000 * 1000;  // 1s
  bool is_inited_;                                                        // not serialized
  common::ObPartitionKey pkey_;
  uint64_t table_id_;
  common::ObSEArray<share::schema::ObColDesc, 16, common::ObIAllocator&> column_ids_;
  common::ObIAllocator& allocator_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObIWarmUpRequest);
};

class ObIWarmUpReadRequest : public ObIWarmUpRequest {
public:
  explicit ObIWarmUpReadRequest(common::ObIAllocator& allocator);
  virtual ~ObIWarmUpReadRequest();
  OB_UNIS_VERSION_V(1);

protected:
  int assign(const ObTableAccessParam& param, const ObTableAccessContext& ctx);
  int prepare(ObTableAccessParam& param, ObTableAccessContext& context, common::ObArenaAllocator& allocator,
      blocksstable::ObBlockCacheWorkingSet& block_cache_ws, ObStoreCtx& store_ctx) const;
  int64_t schema_version_;
  int64_t rowkey_cnt_;
  common::ObSEArray<int32_t, 16, common::ObIAllocator&> out_cols_project_;
  common::ObSEArray<share::schema::ObColumnParam*, 16, common::ObIAllocator&> out_cols_param_;
  common::ObQueryFlag query_flag_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObIWarmUpReadRequest);
};

class ObWarmUpExistRequest : public ObIWarmUpRequest {
public:
  explicit ObWarmUpExistRequest(common::ObIAllocator& allocator);
  virtual ~ObWarmUpExistRequest();
  int assign(const common::ObPartitionKey& pkey, const int64_t table_id, const common::ObStoreRowkey& rowkey,
      const common::ObIArray<share::schema::ObColDesc>& column_ids);
  virtual ObWarmUpRequestType::ObWarmUpRequestEnum get_request_type() const;
  virtual int warm_up(memtable::ObIMemtableCtxFactory* memctx_factory, const common::ObIArray<ObITable*>& stores) const;
  OB_UNIS_VERSION_V(1);

private:
  common::ObStoreRowkey rowkey_;
  DISALLOW_COPY_AND_ASSIGN(ObWarmUpExistRequest);
};

class ObWarmUpGetRequest : public ObIWarmUpReadRequest {
public:
  explicit ObWarmUpGetRequest(common::ObIAllocator& allocator);
  virtual ~ObWarmUpGetRequest();
  int assign(const ObTableAccessParam& param, const ObTableAccessContext& ctx, const common::ObExtStoreRowkey& rowkey);
  virtual ObWarmUpRequestType::ObWarmUpRequestEnum get_request_type() const;
  virtual int warm_up(memtable::ObIMemtableCtxFactory* memctx_factory, const common::ObIArray<ObITable*>& stores) const;
  OB_UNIS_VERSION_V(1);

private:
  common::ObExtStoreRowkey rowkey_;
  DISALLOW_COPY_AND_ASSIGN(ObWarmUpGetRequest);
};

class ObWarmUpMultiGetRequest : public ObIWarmUpReadRequest {
public:
  explicit ObWarmUpMultiGetRequest(common::ObIAllocator& allocator);
  virtual ~ObWarmUpMultiGetRequest();
  int assign(const ObTableAccessParam& param, const ObTableAccessContext& ctx,
      const common::ObIArray<common::ObExtStoreRowkey>& rowkeys);
  virtual ObWarmUpRequestType::ObWarmUpRequestEnum get_request_type() const;
  virtual int warm_up(memtable::ObIMemtableCtxFactory* memctx_factory, const common::ObIArray<ObITable*>& stores) const;
  OB_UNIS_VERSION_V(1);

private:
  common::ObArray<common::ObExtStoreRowkey, common::ObIAllocator&> rowkeys_;
  DISALLOW_COPY_AND_ASSIGN(ObWarmUpMultiGetRequest);
};

class ObWarmUpScanRequest : public ObIWarmUpReadRequest {
public:
  explicit ObWarmUpScanRequest(common::ObIAllocator& allocator);
  virtual ~ObWarmUpScanRequest();
  int assign(const ObTableAccessParam& param, const ObTableAccessContext& ctx, const common::ObExtStoreRange& range);
  virtual ObWarmUpRequestType::ObWarmUpRequestEnum get_request_type() const;
  virtual int warm_up(memtable::ObIMemtableCtxFactory* memctx_factory, const common::ObIArray<ObITable*>& stores) const;
  OB_UNIS_VERSION_V(1);

private:
  common::ObExtStoreRange range_;
  DISALLOW_COPY_AND_ASSIGN(ObWarmUpScanRequest);
};

class ObWarmUpMultiScanRequest : public ObIWarmUpReadRequest {
public:
  explicit ObWarmUpMultiScanRequest(common::ObIAllocator& allocator);
  virtual ~ObWarmUpMultiScanRequest();
  int assign(const ObTableAccessParam& param, const ObTableAccessContext& ctx,
      const common::ObIArray<common::ObExtStoreRange>& ranges);
  virtual ObWarmUpRequestType::ObWarmUpRequestEnum get_request_type() const;
  virtual int warm_up(memtable::ObIMemtableCtxFactory* memctx_factory, const common::ObIArray<ObITable*>& stores) const;
  OB_UNIS_VERSION_V(1);

private:
  common::ObSEArray<common::ObExtStoreRange, 2, common::ObIAllocator&> ranges_;
  DISALLOW_COPY_AND_ASSIGN(ObWarmUpMultiScanRequest);
};

typedef common::ObList<const ObIWarmUpRequest*, common::ObIAllocator&> ObWarmUpRequestList;

class ObWarmUpRequestWrapper {
public:
  ObWarmUpRequestWrapper();
  virtual ~ObWarmUpRequestWrapper();
  int add_request(const ObIWarmUpRequest& request);
  OB_INLINE const ObWarmUpRequestList& get_requests() const
  {
    return request_list_;
  }
  OB_INLINE void reuse()
  {
    request_list_.reset();
    allocator_.reuse();
  }
  TO_STRING_KV(K_(request_list));
  OB_UNIS_VERSION(1);

private:
  common::ObArenaAllocator allocator_;
  ObWarmUpRequestList request_list_;
};
}  // namespace storage

namespace obrpc {
struct ObWarmUpRequestArg {
  storage::ObWarmUpRequestWrapper wrapper_;
  OB_INLINE void reuse()
  {
    wrapper_.reuse();
  }
  TO_STRING_KV(K_(wrapper));
  OB_UNIS_VERSION(1);
};
}  // namespace obrpc
}  // namespace oceanbase
#endif /* SRC_STORAGE_OB_WARM_UP_REQUEST_H_ */
