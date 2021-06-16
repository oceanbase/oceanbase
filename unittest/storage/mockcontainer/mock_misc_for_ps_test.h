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

#ifndef OCEANBASE_FOR_PS_TEST_H
#define OCEANBASE_FOR_PS_TEST_H

#include "storage/ob_partition_storage.h"
#include "storage/memtable/ob_memtable_interface.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"
#include "storage/ob_partition_component_factory.h"
#include "common/row/ob_row_store.h"
#include "lib/time/ob_time_utility.h"
#include "lib/allocator/page_arena.h"
#include "memtable/utils_mod_allocator.h"
#include "mock_ob_iterator.h"
#include "mockcontainer/ob_restore_schema.h"
#include "share/ob_tenant_mgr.h"
#include "share/ob_srv_rpc_proxy.h"
#include "../../share/schema/mock_schema_service.h"
#include <gtest/gtest.h>

namespace oceanbase {
namespace unittest {
using namespace common;
using namespace storage;
using namespace share::schema;
using namespace memtable;
using namespace sql;
using namespace transaction;

class TestTransVersion {
public:
  static const int64_t TRANS_VERSION = 1;
};
class MyIter : public ObStoreRowIterator {
public:
  enum IterType {
    T_INVALID,
    T_GET,
    T_SCAN,
  };

public:
  MyIter()
  {
    clear();
  }
  virtual ~MyIter()
  {}
  inline virtual int get_next_row(const ObStoreRow*& row);
  inline virtual void reset();
  inline int set_type(const IterType& type, const ObIArray<common::ObStoreRowkey>* rowkeys = NULL);
  inline void clear();

private:
  IterType type_;
  ObStoreRow row_;
  const common::ObIArray<common::ObStoreRowkey>* rowkeys_;
  int64_t cur_;
};

void MyIter::reset()
{
  if (T_SCAN == type_) {
    cur_ = 0;
  }
}

void MyIter::clear()
{
  type_ = T_INVALID;
  row_.row_val_.cells_ = NULL;
  row_.row_val_.count_ = 0;
  row_.flag_ = ObActionFlag::OP_ROW_DOES_NOT_EXIST;
  row_.reset_dml();
  rowkeys_ = NULL;
  cur_ = 0;
}

int MyIter::set_type(const IterType& type, const ObIArray<common::ObStoreRowkey>* rowkeys)
{
  int ret = OB_SUCCESS;
  if (T_INVALID == type) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "wrong type");
  } else {
    clear();
    if (T_SCAN == type) {
      type_ = type;
    } else if (T_GET == type) {
      if (!rowkeys || rowkeys->count() <= 0) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "rowkeys must be specified");
      } else {
        type_ = type;
        rowkeys_ = rowkeys;
      }
    }
  }
  return ret;
}

int MyIter::get_next_row(const ObStoreRow*& row)
{
  int ret = OB_SUCCESS;
  if (T_SCAN == type_) {
    ret = OB_ITER_END;
  } else if (T_GET == type_) {
    if (!rowkeys_) {
      ret = OB_NOT_INIT;
    } else if (cur_ >= rowkeys_->count()) {
      ret = OB_ITER_END;
    } else {
      row_.row_val_.cells_ = const_cast<ObObj*>(rowkeys_->at(cur_).ptr());
      row_.row_val_.count_ = rowkeys_->at(cur_).length();
      ++cur_;
      row = &row_;
    }
  } else {
    ret = OB_NOT_INIT;
  }
  return ret;
}

class MySSStore : public ObIStore {
public:
  MySSStore() : allocator_(ObModIds::OB_ST_TEMP, OB_MALLOC_NORMAL_BLOCK_SIZE)
  {
    set_version(ObVersion(1));
    not_exist_row_.row_val_.cells_ = NULL;
    not_exist_row_.row_val_.count_ = 0;
    not_exist_row_.flag_ = ObActionFlag::OP_ROW_DOES_NOT_EXIST;
    not_exist_row_.reset_dml();
  }
  virtual ~MySSStore()
  {}
  virtual void destroy()
  {}
  virtual int exist(const ObStoreCtx& ctx, const uint64_t table_id, const common::ObStoreRowkey& rowkey,
      const common::ObIArray<share::schema::ObColDesc>& column_ids, bool& is_exist, bool& has_found);
  virtual int get(const ObStoreCtx& ctx, const ObQueryFlag flag, const uint64_t table_id,
      const common::ObStoreRowkey& rowkey, const common::ObIArray<share::schema::ObColDesc>& column_ids,
      const ObStoreRow*& row);
  virtual int scan(const ObStoreCtx& ctx, const ObQueryFlag flag, const uint64_t table_id,
      const common::ObStoreRange& key_range, const common::ObIArray<share::schema::ObColDesc>& column_ids,
      ObStoreRowIterator*& row_iter);
  virtual int multi_get(const ObStoreCtx& ctx, const ObQueryFlag flag, const uint64_t table_id,
      const common::ObIArray<common::ObStoreRowkey>& rowkeys,
      const common::ObIArray<share::schema::ObColDesc>& column_ids, ObStoreRowIterator*& row_iter);
  virtual int set(const ObStoreCtx& ctx, const uint64_t table_id, const int64_t rowkey_size,
      const common::ObIArray<share::schema::ObColDesc>& column_ids, ObStoreRowIterator& row_iter)
  {
    UNUSED(ctx);
    UNUSED(table_id);
    UNUSED(rowkey_size);
    UNUSED(column_ids);
    UNUSED(row_iter);
    return OB_SUCCESS;
  }
  virtual int set(const ObStoreCtx& ctx, const uint64_t table_id, const int64_t rowkey_size,
      const common::ObIArray<share::schema::ObColDesc>& column_ids, const ObStoreRow& row)
  {
    UNUSED(ctx);
    UNUSED(table_id);
    UNUSED(rowkey_size);
    UNUSED(column_ids);
    UNUSED(row);
    return OB_SUCCESS;
  }

  virtual int lock(const ObStoreCtx& ctx, const uint64_t table_id,
      const common::ObIArray<share::schema::ObColDesc>& columns, common::ObNewRowIterator& row_iter)
  {
    UNUSED(ctx);
    UNUSED(table_id);
    UNUSED(row_iter);
    UNUSED(columns);
    return OB_SUCCESS;
  }
  virtual int lock(const ObStoreCtx& ctx, const uint64_t table_id,
      const common::ObIArray<share::schema::ObColDesc>& columns, const common::ObNewRow& row)
  {
    UNUSED(ctx);
    UNUSED(table_id);
    UNUSED(row);
    UNUSED(columns);
    return OB_SUCCESS;
  }
  virtual int revert_iter(ObStoreRowIterator* iter)
  {
    allocator_.free(iter);
    return OB_SUCCESS;
  }
  virtual int revert_row(const ObStoreRow* row)
  {
    UNUSED(row);
    return OB_SUCCESS;
  }
  virtual void reset();

  virtual enum ObStoreType get_store_type() const
  {
    return MAJOR_SSSTORE;
  }

  virtual bool is_contain(const uint64_t table_id) const
  {
    UNUSED(table_id);
    return false;
  }

  virtual int estimate_get_cost(const ObQueryFlag query_flag, const uint64_t table_id,
      const common::ObIArray<common::ObStoreRowkey>& rowkeys, const common::ObIArray<share::schema::ObColDesc>& columns,
      ObPartitionEst& cost_metrics)
  {
    UNUSED(query_flag);
    UNUSED(table_id);
    UNUSED(rowkeys);
    UNUSED(columns);
    UNUSED(cost_metrics);
    return 0;
  }

  virtual int estimate_scan_cost(const ObQueryFlag query_flag, const uint64_t table_id,
      const common::ObStoreRange& key_range, const common::ObIArray<share::schema::ObColDesc>& columns,
      ObPartitionEst& cost_metrics)
  {
    UNUSED(query_flag);
    UNUSED(table_id);
    UNUSED(key_range);
    UNUSED(columns);
    UNUSED(cost_metrics);
    return 0;
  }

private:
  common::ObArenaAllocator allocator_;
  ObStoreRow not_exist_row_;
};

inline void MySSStore::reset()
{
  allocator_.clear();
}

inline int MySSStore::exist(const ObStoreCtx& ctx, const uint64_t table_id, const common::ObStoreRowkey& rowkey,
    const common::ObIArray<share::schema::ObColDesc>& column_ids, bool& is_exist, bool& has_found)
{
  UNUSED(ctx);
  UNUSED(table_id);
  UNUSED(rowkey);
  UNUSED(column_ids);
  is_exist = false;
  has_found = false;
  return common::OB_SUCCESS;
}

// return row not exist
inline int MySSStore::get(const ObStoreCtx& ctx, const ObQueryFlag flag, const uint64_t table_id,
    const common::ObStoreRowkey& rowkey, const common::ObIArray<ObColDesc>& column_ids, const ObStoreRow*& row)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  UNUSED(flag);
  UNUSED(table_id);
  UNUSED(rowkey);
  UNUSED(column_ids);
  UNUSED(stat);
  row = &not_exist_row_;
  return ret;
}

// return END
inline int MySSStore::scan(const ObStoreCtx& ctx, const ObQueryFlag flag, const uint64_t table_id,
    const common::ObStoreRange& key_range, const common::ObIArray<share::schema::ObColDesc>& column_ids,
    ObStoreRowIterator*& row_iter)
{
  int ret = OB_SUCCESS;
  void* ptr = NULL;
  UNUSED(ctx);
  UNUSED(flag);
  UNUSED(table_id);
  UNUSED(key_range);
  UNUSED(column_ids);
  if (NULL == (ptr = allocator_.alloc(sizeof(MyIter)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "no momery");
  } else {
    MyIter* iter = new (ptr) MyIter();
    if (OB_SUCCESS != (ret = iter->set_type(MyIter::T_SCAN))) {
      STORAGE_LOG(WARN, "fail to set scan iterator");
    } else {
      row_iter = iter;
    }
  }
  return ret;
}

// return all empty row(s)
inline int MySSStore::multi_get(const ObStoreCtx& ctx, const ObQueryFlag flag, const uint64_t table_id,
    const common::ObIArray<common::ObStoreRowkey>& rowkeys,
    const common::ObIArray<share::schema::ObColDesc>& column_ids, ObStoreRowIterator*& row_iter)
{
  int ret = OB_SUCCESS;
  void* ptr = NULL;
  UNUSED(ctx);
  UNUSED(flag);
  UNUSED(table_id);
  UNUSED(rowkeys);
  UNUSED(column_ids);
  if (NULL == (ptr = allocator_.alloc(sizeof(MyIter)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "no momery");
  } else {
    MyIter* iter = new (ptr) MyIter();
    if (OB_SUCCESS != (ret = iter->set_type(MyIter::T_GET, &rowkeys))) {
      STORAGE_LOG(WARN, "fail to set get iterator");
    } else {
      row_iter = iter;
    }
  }
  return ret;
}

class MySchemaService : public oceanbase::share::schema::MockSchemaService {
public:
  inline int init(const char* file_name);
  void get_schema_guard(ObSchemaGetterGuard*& schema_guard)
  {
    schema_guard = schema_guard_;
  }
  // inline virtual int release_schema(const ObSchemaManager *schema);
  // inline virtual const ObSchemaManager *get_schema_manager_by_version(const int64_t version = 0,
  //                                                                    const bool for_merge = false);
private:
  // inline virtual const ObSchemaManager *get_user_schema_manager(const int64_t version);
  // inline virtual int get_all_schema(ObSchemaManager &out_schema, const int64_t frozen_version = -1);
private:
  ObRestoreSchema restore_schema_;
  ObSchemaGetterGuard* schema_guard_;
  // ObSchemaManager *manager_;
};

int MySchemaService::init(const char* file_name)
{
  int ret = OB_SUCCESS;
  schema_guard_ = NULL;
  if (OB_SUCCESS != (ret = restore_schema_.init()) ||
      OB_SUCCESS != (ret = restore_schema_.parse_from_file(file_name, schema_guard_))) {
    STORAGE_LOG(ERROR, "fail to get schema manger");
  } else {
  }
  return ret;
}

// int MySchemaService::get_all_schema(ObSchemaManager &out_schema, const int64_t frozen_version)
//{
//  UNUSED(frozen_version);
//  return out_schema.assign(*manager_, true);
//}
//
// const ObSchemaManager *MySchemaService::get_user_schema_manager(
//    const int64_t version)
//{
//  UNUSED(version);
//  return manager_;
//}
//
// const ObSchemaManager *MySchemaService::get_schema_manager_by_version(
//    const int64_t version,
//    const bool for_merge)
//{
//  UNUSED(version);
//  UNUSED(for_merge);
//  return manager_;
//}
//
// int MySchemaService::release_schema(const ObSchemaManager *schema)
//{
//  UNUSED(schema);
//  return OB_SUCCESS;
//}

class TestObSchemaService : public MySchemaService {};

class MyNewRowIter : public ObNewRowIterator {
public:
  MyNewRowIter() : iter_(NULL)
  {}
  virtual ~MyNewRowIter()
  {}

  void set_store_row_iter(ObStoreRowIterator& iter);
  virtual void reset();
  virtual int get_next_row(ObNewRow*& row);

private:
  ObStoreRowIterator* iter_;
};
inline void MyNewRowIter::reset()
{
  /*
  if(iter_) {
    iter_->reset();
  }
  */
}
inline void MyNewRowIter::set_store_row_iter(ObStoreRowIterator& iter)
{
  iter_ = &iter;
}
inline int MyNewRowIter::get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (iter_) {
    const ObStoreRow* store_row = NULL;
    if (OB_SUCCESS == (ret = iter_->get_next_row(store_row))) {
      row = const_cast<ObNewRow*>(&store_row->row_val_);
    }
  }
  return ret;
}

template <typename T, int64_t ARRAY_SIZE>
class TestArray : public ObIArray<T> {
public:
  TestArray() : count_(0)
  {}
  virtual ~TestArray()
  {}

public:
  virtual int push_back(const T& obj);
  virtual void pop_back()
  {}
  virtual int pop_back(T& obj)
  {
    UNUSED(obj);
    return OB_ERR_UNEXPECTED;
  }
  virtual int remove(int64_t idx)
  {
    UNUSED(idx);
    return OB_ERR_UNEXPECTED;
  }
  virtual int at(int64_t idx, T& obj) const;
  virtual T& at(int64_t idx);
  virtual const T& at(int64_t idx) const;
  virtual int64_t count() const
  {
    return count_;
  }
  virtual void reset()
  {}
  virtual void reuse()
  {
    count_ = 0;
  }
  virtual void destroy()
  {}
  virtual void reserve(int64_t capacity)
  {
    UNUSED(capacity);
  }
  virtual int assign(const ObIArray<T>& other)
  {
    UNUSED(other);
    return OB_ERR_UNEXPECTED;
  }

private:
  T array_[ARRAY_SIZE];
  int64_t count_;
};

template <typename T, int64_t ARRAY_SIZE>
int TestArray<T, ARRAY_SIZE>::push_back(const T& obj)
{
  int ret = OB_SUCCESS;
  if (ARRAY_SIZE <= count_) {
    ret = OB_ERROR_OUT_OF_RANGE;
    STORAGE_LOG(WARN, "push back to TestArray failed");
  } else {
    array_[count_++] = obj;
  }
  return ret;
}

template <typename T, int64_t ARRAY_SIZE>
int TestArray<T, ARRAY_SIZE>::at(int64_t idx, T& obj) const
{
  int ret = OB_SUCCESS;
  if (0 > idx || idx >= count_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid idx value", K(idx));
  } else {
    obj = array_[idx];
  }
  return ret;
}

template <typename T, int64_t ARRAY_SIZE>
T& TestArray<T, ARRAY_SIZE>::at(int64_t idx)
{
  OB_ASSERT(0 <= idx && idx < count_);
  return array_[idx];
}

template <typename T, int64_t ARRAY_SIZE>
const T& TestArray<T, ARRAY_SIZE>::at(int64_t idx) const
{
  OB_ASSERT(0 <= idx && idx < count_);
  return array_[idx];
}

template <typename T>
class TestArray2 : public TestArray<T, 1> {
public:
  virtual int push_back(const T& obj)
  {
    UNUSED(obj);
    return OB_ERROR_OUT_OF_RANGE;
  }
};

class TestPartitionComponentFactory : public ObPartitionComponentFactory {
public:
  TestPartitionComponentFactory()
  {}
  virtual ~TestPartitionComponentFactory()
  {}
  virtual ObSortedStores* get_sorted_stores()
  {
    return NULL;
  }
};

class TestObMemtable : public ObMemtable {
public:
  virtual int set(const storage::ObStoreCtx& ctx, const uint64_t table_id, const int64_t rowkey_len,
      const common::ObIArray<share::schema::ObColDesc>& columns, const storage::ObStoreRow& row)
  {
    int ret = OB_SUCCESS;
    UNUSED(ctx);
    UNUSED(rowkey_len);
    UNUSED(columns);
    UNUSED(row);
    if (combine_id(1, 3005) == table_id) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "construct a test err for delete rows");
    }
    return ret;
  }
};

class TestObMemtable2 : public ObMemtable {
public:
  virtual int set(const storage::ObStoreCtx& ctx, const uint64_t table_id, const int64_t rowkey_len,
      const common::ObIArray<share::schema::ObColDesc>& columns, const storage::ObStoreRow& row)
  {
    UNUSED(ctx);
    UNUSED(table_id);
    UNUSED(rowkey_len);
    UNUSED(columns);
    UNUSED(row);
    return OB_SUCCESS;
  }

  virtual int get(const storage::ObStoreCtx& ctx, const ObQueryFlag query_flag, const uint64_t table_id,
      const common::ObStoreRowkey& rowkey, const common::ObIArray<share::schema::ObColDesc>& columns,
      const ObStoreRow*& row)
  {
    UNUSED(ctx);
    UNUSED(query_flag);
    UNUSED(table_id);
    UNUSED(rowkey);
    UNUSED(columns);
    UNUSED(row);
    UNUSED(stat);
    return OB_ERR_UNEXPECTED;
  }
};

class TestObMemtable3 : public ObMemtable {
public:
  TestObMemtable3()
  {
    row_.flag_ = ObActionFlag::OP_DEL_ROW;
  }

  virtual int set(const storage::ObStoreCtx& ctx, const uint64_t table_id, const int64_t rowkey_len,
      const common::ObIArray<share::schema::ObColDesc>& columns, const storage::ObStoreRow& row)
  {
    UNUSED(ctx);
    UNUSED(table_id);
    UNUSED(rowkey_len);
    UNUSED(columns);
    UNUSED(row);
    return OB_SUCCESS;
  }

  virtual int get(const storage::ObStoreCtx& ctx, const ObQueryFlag query_flag, const uint64_t table_id,
      const common::ObStoreRowkey& rowkey, const common::ObIArray<share::schema::ObColDesc>& columns,
      const ObStoreRow*& row)
  {
    int ret = OB_SUCCESS;
    UNUSED(ctx);
    UNUSED(query_flag);
    UNUSED(rowkey);
    UNUSED(columns);
    UNUSED(row);
    UNUSED(stat);
    if (combine_id(1, 3001) != table_id) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "construct a test err for update rows");
    } else {
      row = &row_;
    }
    return ret;
  }

private:
  ObStoreRow row_;
};

class TestNewRowIter : public ObNewRowIterator {
public:
  inline TestNewRowIter();
  virtual ~TestNewRowIter()
  {}
  inline virtual void reset()
  {}
  inline virtual int get_next_row(ObNewRow*& row);

private:
  ObNewRow row_;
};

TestNewRowIter::TestNewRowIter()
{
  row_.cells_ = NULL;
  row_.count_ = 0;
}

int TestNewRowIter::get_next_row(ObNewRow*& row)
{
  row = &row_;
  return OB_SUCCESS;
}

inline void check_result_iter(ObNewRowIterator& result, ObStoreRowIterator& expected)
{
  int ret1 = OB_SUCCESS;
  int ret2 = OB_SUCCESS;
  int i = 0;
  ObNewRow* row1 = NULL;
  const ObStoreRow* row2 = NULL;
  ret1 = result.get_next_row(row1);
  ret2 = expected.get_next_row(row2);
  while (OB_SUCCESS == ret1 && OB_SUCCESS == ret2) {
    ASSERT_EQ(row1->count_, row2->row_val_.count_);
    for (int j = 0; j < row1->count_; ++j) {
      // FIXME
      if (ObVarcharType == row1->cells_[j].get_type()) {
        row1->cells_[j].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      }
      ASSERT_EQ(row1->cells_[j], row2->row_val_.cells_[j]) << "row:" << i << " col:" << j;
    }
    ++i;
    ret1 = result.get_next_row(row1);
    ret2 = expected.get_next_row(row2);
  }
  ASSERT_EQ(ret1, ret2);
  ASSERT_EQ(OB_ITER_END, ret2);
}

inline int set_trans_desc(ObTransDesc& trans_desc)
{
  int ret = OB_SUCCESS;
  ObAddr addr(ObAddr::IPV4, "127.0.0.1", 9021);
  ObTransID trans_id(addr);
  ObStartTransParam trans_param;
  trans_param.set_access_mode(ObTransAccessMode::READ_WRITE);
  trans_param.set_type(ObTransType::TRANS_USER);
  trans_param.set_isolation(ObTransIsolation::READ_COMMITED);
  int64_t snapshot_version = 2;
  if (OB_SUCCESS != (ret = trans_desc.set_trans_id(trans_id))) {
    STORAGE_LOG(WARN, "set trans_id error", K(ret));
  } else if (OB_SUCCESS != (ret = trans_desc.set_snapshot_version(snapshot_version))) {
    STORAGE_LOG(WARN, "set snapshot_version error", K(snapshot_version), K(ret));
  } else if (OB_SUCCESS != (ret = trans_desc.set_trans_param(trans_param))) {
    STORAGE_LOG(WARN, "set trans_param error", K(ret));
  } else {
    trans_desc.inc_sql_no();
  }
  return ret;
}

inline int init_tenant_mgr()
{
  ObTenantManager& tm = ObTenantManager::get_instance();
  ObAddr self;
  self.set_ip_addr("127.0.0.1", 8086);
  rpc::frame::ObReqTransport req_transport(NULL, NULL);
  obrpc::ObSrvRpcProxy rpc_proxy;
  int ret = tm.init(self, rpc_proxy, &req_transport, &ObServerConfig::get_instance());
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = tm.add_tenant(OB_SYS_TENANT_ID);
  EXPECT_EQ(OB_SUCCESS, ret);
  const int64_t ulmt = 16LL << 30;
  const int64_t llmt = 8LL << 30;
  ret = tm.set_tenant_mem_limit(OB_SYS_TENANT_ID, ulmt, llmt);
  EXPECT_EQ(OB_SUCCESS, ret);
  return OB_SUCCESS;
}

}  // namespace unittest
}  // namespace oceanbase
#endif
