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

#ifndef OCEANBASE_COMMON_OB_TSI_FACTORY_
#define OCEANBASE_COMMON_OB_TSI_FACTORY_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <typeinfo>
#include "lib/ob_define.h"
#include "lib/allocator/page_arena.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/coro/co_local_storage.h"

namespace oceanbase {
namespace common {
enum TSICommonType {
  TSI_COMMON_OBJPOOL_1 = 1001,
  TSI_COMMON_SCAN_PARAM_1,
  TSI_COMMON_SCANNER_1,
  TSI_COMMON_MUTATOR_1,
  TSI_COMMON_THE_META_1,
  TSI_COMMON_GET_PARAM_1,
  TSI_COMMON_MULTI_WAKEUP_1,
  TSI_COMMON_PACKET_TRACE_ID_1,
  TSI_COMMON_SEQ_ID_1,
  TSI_COMMON_OBSERVER_1,
  TSI_COMMON_TO_CSTRING_BUFFER_OBJ_1,
  TSI_COMMON_TO_CSTRING_BUFFER_OBJ_2,
  TSI_COMMON_TO_CSTRING_BUFFER_OBJ_3,
  TSI_COMMON_DEBUG_SYNC_ARRAY,
};

enum TSISSTableType {
  TSI_SSTABLE_FILE_BUFFER_1 = 2001,
  TSI_SSTABLE_THREAD_AIO_BUFFER_MGR_ARRAY_1,
  TSI_SSTABLE_MODULE_ARENA_1,
  TSI_SSTABLE_COMPRESS_BUFFER_MODULE_ARENA_1,
  TSI_SSTABLE_COLUMN_CHECKSUM_MODULE_ARENA_1
};

enum TSIBlockSSTableType {
  TSI_BLOCKSSTABLE_FILE_BUFFER_1 = 11001,
  TSI_BLOCKSSTABLE_FILE_BUFFER_2 = 11002,
  TSI_BLOCKSSTABLE_ROW_CHECKSUM_GENERATOR_1,
  TSI_BLOCKSSTABLE_BLOCK_INDEX_MGR,
  TSI_BLOCKSSTABLE_BLOCK_INDEX_TRANSFORMER,
  TSI_BLOCKSSTABLE_TABLET_ARRAY_1,
  TSI_BLOCKSSTABLE_GET_READER_ARRAY_1,
  TSI_BLOCKSSTABLE_SCAN_READER_ARRAY_1,
  TSI_BLOCKSSTABLE_CHUNK_LOG_WRITER_1,
  TSI_BLOCKSSTABLE_MACRO_BLOCK_META_LOG_1,
  TSI_BLOCKSSTABLE_SSTABLE_SCHEMA_1,
  TSI_BLOCKSSTABLE_FETCH_DATA_1,
  TSI_BLOCKSSTABLE_SEND_DATA_1,
};

enum TSIChunkserverType {
  TSI_CS_SCANNER_1 = 3001,
  TSI_CS_NEW_SCANNER_1,
  TSI_CS_NEW_SCANNER_2,
  TSI_CS_GET_PARAM_1,
  TSI_CS_SCAN_PARAM_1,
  TSI_CS_SQL_SCAN_PARAM_1,
  TSI_CS_SQL_GET_PARAM_1,
  TSI_CS_TABLET_REPORT_INFO_LIST_1,
  TSI_CS_TABLET_REPORT_INFO_LIST_2,
  TSI_CS_SSTABLE_GETTER_1,
  TSI_CS_GET_THREAD_CONTEXT_1,
  TSI_CS_SSTABLE_SCANNER_1,
  TSI_CS_SCHEMA_DECODER_ASSIS_1,
  TSI_CS_THEEAD_META_WRITER_1,
  TSI_CS_COMPACTSSTABLE_ITERATOR_1,
  TSI_CS_COMPACTSSTABLE_GET_SCANEER_1,
  TSI_CS_COLUMNFILTER_1,
  TSI_CS_QUERY_SERVICE_1,
  TSI_CS_TABLET_SERVICE_1,
  TSI_CS_STATIC_DATA_SERVICE_1,
  TSI_CS_MULTI_TABLET_MERGER_1,
  TSI_CS_TABLE_IMPORT_INFO_1,
  TSI_CS_FETCH_DATA_1,
  TSI_CS_FETCH_DATA_2,
  TSI_CS_SSTABLE_SCAN_PARAM_1,
  TSI_CS_MEDIATING_ALLOCATOR_1,
  TSI_CS_TABLE_LOCAL_INDEX_BUILDER_1,
  TSI_CS_TABLE_LOCAL_INDEX_SAMPLER_1,
  TSI_CS_TABLE_GLOBAL_INDEX_BUILDER_1,
  TSI_CS_MIGRATE_SCAN_1,
  TSI_CS_MIGRATE_SCAN_2,
  TSI_CS_TABLET_DATA_CORRECTION_REPORT_INFO_LIST,
};

enum TSIUpdateserverType {
  TSI_UPS_SCANNER_1 = 4001,
  TSI_UPS_NEW_SCANNER_1,
  TSI_UPS_NEW_SCANNER_2,
  TSI_UPS_GET_PARAM_1,
  TSI_UPS_SCAN_PARAM_1,
  TSI_UPS_INC_SCAN_1,
  TSI_UPS_INC_GET_1,
  TSI_UPS_MUTATOR_1,
  TSI_UPS_SCANNER_ARRAY_1,
  TSI_UPS_UPS_MUTATOR_1,
  TSI_UPS_TABLE_UTILS_SET_1,
  TSI_UPS_COLUMN_FILTER_1,
  TSI_UPS_COLUMN_MAP_1,
  TSI_UPS_TABLE_LIST_1,
  TSI_UPS_ROW_COMPACTION_1,
  TSI_UPS_ROW_COMPACTION_2,
  TSI_UPS_CLIENT_WRAPPER_TSI_1,
  TSI_UPS_FIXED_SIZE_BUFFER_1,
  TSI_UPS_FIXED_SIZE_BUFFER_2,
  TSI_UPS_SCAN_PARAM_2,
  TSI_UPS_SQL_SCAN_PARAM_1,
  TSI_UPS_ARENA_ALLOC_1,
  TSI_UPS_SCAN_MERGE_1,
  TSI_UPS_GET_MERGE_1,
  TSI_UPS_SQL_MULTI_SCAN_MERGE_1,
  TSI_UPS_SCHEMA_MGR_1,
  TSI_UPS_SINGLE_GET_PARAM_1,
  TSI_UPS_EMPTY_SCAN_1,
  TSI_UPS_EMPTY_GET_1,
  TSI_UPS_TABLE_DUMP_1,
};

enum TSIMergeserverType {
  TSI_MS_SCANNER_1 = 5001,
  TSI_MS_ORG_GET_PARAM_1,
  TSI_MS_DECODED_GET_PARAM_1,
  TSI_MS_GET_PARAM_WITH_NAME_1,
  TSI_MS_ORG_SCAN_PARAM_1,
  TSI_MS_DECODED_SCAN_PARAM_1,
  TSI_MS_SCHEMA_DECODER_ASSIS_1,
  TSI_MS_GET_EVENT_1,
  TSI_MS_SCAN_EVENT_1,
  TSI_MS_MS_SCAN_PARAM_1,
  TSI_MS_ORG_MUTATOR_1,
  TSI_MS_DECODED_MUTATOR_1,
  TSI_MS_UPS_SCANNER_1,
  TSI_MS_NEW_SCANNER_1,
  TSI_MS_NEW_SCANNER_2,
  TSI_MS_SQL_SCAN_PARAM_1,
  TSI_MS_SERVER_COUNTER_ID,
  TSI_MS_MGET_STRING_BUF_1,
  TSI_MS_SCAN_STRING_BUF_1,
};

enum TSIOlapDrive {
  TSI_OLAP_SCAN_EXTRA_INFO_1 = 6001,
  TSI_OLAP_THREAD_ROW_KEY_1,
  TSI_OLAP_GET_PARAM_1,
  TSI_OLAP_SCAN_PARAM_1,
  TSI_OLAP_SCANNER_1,
  TSI_OLAP_MUTATOR_1,
};

enum TSISqlType {
  TSI_SQL_GET_PARAM_1 = 7001,
  TSI_SQL_GET_PARAM_2 = 7002,
  TSI_SQL_EXPR_STACK_1 = 7003,
  TSI_SQL_EXPR_EXTRA_PARAMS_1 = 7005,
  TSI_SQL_TP_ARENA_1 = 7006,
  TSI_SQL_ROW_1 = 7007,
  TSI_SQL_PLAN_EXECUTOR_1 = 7008,
  TSI_SQL_EXPLAIN_FORMATOR_1 = 7009,
  TSI_SQL_CALC_BUF_1 = 7010,
};

enum TSIMySQLType {
  TSI_MYSQL_CLIENT_WAIT_1 = 8001,
  TSI_MYSQL_RESULT_SET_1,
  TSI_MYSQL_PREPARE_RESULT_1,
  TSI_MYSQL_SESSION_KEY_1,
};

enum TSIRootserverType {
  TSI_RS_SCANNER_1 = 9001,
  TSI_RS_GET_PARAM_1,
  TSI_RS_MS_PROVIDER_1,
  TSI_RS_NEW_SCANNER_1,
  TSI_RS_SQL_SCAN_PARAM_1,
  TSI_RS_NEW_SCANNER_2,
  TSI_RS_SCHEMA_MGR_1,
};

enum TSIProxyserverType {
  TSI_YUNTI_PROXY_READER_1 = 10001,
  TSI_YUNTI_PROXY_READER_2,
};

enum TSILiboblogType {
  TSI_LIBOBLOG_PARTITION = 11001,
  TSI_LIBOBLOG_DML_STMT,
  TSI_LIBOBLOG_MYSQL_ADAPTOR,
  TSI_LIBOBLOG_META_MANAGER,
  TSI_LIBOBLOG_ROW_VALUE,
  TSI_LIBOBLOG_SERVER_SELECTOR,
  TSI_LIBOBLOG_MYSQL_QUERY_RESULT,
};

enum TSICLogType {
  TSI_CLOG_READER_TSIINFO = 12001,
  TSI_CLOG_WRITE_AIOPARAM,
};

enum TSIMemtableType {};

enum TSITransType {};

// thread local
#define GET_TSI0(type) ::oceanbase::common::TSIFactory::get_instance<type, 0>()
#define GET_TSI_MULT0(type, tag) ::oceanbase::common::TSIFactory::get_instance<type, tag>()
// co-routine local
#ifndef NOCOROUTINE
#define GET_TSI(type) ::oceanbase::lib::CoLocalStorage::get_instance<type>()
#define GET_TSI_MULT(type, tag) ::oceanbase::lib::CoLocalStorage::get_instance<type, tag>()
#else
#define GET_TSI(type) ::oceanbase::common::TSIFactory::get_instance<type, 0>()
#define GET_TSI_MULT(type, tag) ::oceanbase::common::TSIFactory::get_instance<type, tag>()
#endif

template <class T>
class Wrapper {
public:
  Wrapper() : instance_(NULL)
  {}
  ~Wrapper()
  {
    if (NULL != instance_) {
      delete instance_;
      instance_ = NULL;
    }
  }

public:
  T*& get_instance()
  {
    return instance_;
  }

private:
  T* instance_;
};

#define GET_TSI_ARGS(type, num, args...)                                  \
  ({                                                                      \
    type* __type_ret__ = NULL;                                            \
    Wrapper<type>* __type_wrapper__ = GET_TSI_MULT(Wrapper<type>, num);   \
    if (NULL != __type_wrapper__) {                                       \
      __type_ret__ = __type_wrapper__->get_instance();                    \
      if (NULL == __type_ret__) {                                         \
        __type_wrapper__->get_instance() = new (std::nothrow) type(args); \
        __type_ret__ = __type_wrapper__->get_instance();                  \
      }                                                                   \
    }                                                                     \
    __type_ret__;                                                         \
  })
//  GET_TSI(Wrapper<type>) ? (GET_TSI(Wrapper<type>)->get_instance() ? (GET_TSI(Wrapper<type>)->get_instance()) :
//      (GET_TSI(Wrapper<type>)->get_instance() = new(std::nothrow) type(args))) : NULL

class TSINodeBase {
public:
  TSINodeBase() : next(NULL)
  {}
  virtual ~TSINodeBase()
  {
    next = NULL;
  }
  TSINodeBase* next;
};

template <class T>
class TSINode : public TSINodeBase {
public:
  explicit TSINode(T* instance) : instance_(instance)
  {}
  virtual ~TSINode()
  {
    // FIXME: support print log at this point.
    //
    // Currently syslog need COVAR information e.g. TRACE ID,
    // log_limiter. But when thread exits rouine's context has been
    // destroyed.
    if (NULL != instance_) {
      // _LIB_LOG(INFO, "delete instance [%s] %p", typeid(T).name(), instance_);
      instance_->~T();
      instance_ = NULL;
    }
  }

private:
  T* instance_;
};

template <class T, int N>
class TSINode<T[N]> : public TSINodeBase {
public:
  explicit TSINode(T (*instance)[N]) : instance_(*instance)
  {}
  virtual ~TSINode()
  {
    if (nullptr != instance_) {
      for (int i = 0; i < N; i++) {
        instance_[i].~T();
      }
      instance_ = nullptr;
    }
  }

private:
  T* instance_;
};

class ThreadSpecInfo {
  static const int64_t PAGE_SIZE = common::ModuleArena::DEFAULT_PAGE_SIZE;

public:
  ThreadSpecInfo() : list_(NULL), mod_(ObModIds::OB_TSI_FACTORY), alloc_(PAGE_SIZE, mod_)
  {}
  ~ThreadSpecInfo()
  {
    TSINodeBase* iter = list_;
    while (NULL != iter) {
      TSINodeBase* tmp = iter;
      iter = iter->next;
      tmp->~TSINodeBase();
    }
    list_ = NULL;
  }

public:
  template <class T>
  T* get_instance()
  {
    T* instance = NULL;
    TSINode<T>* node = NULL;
    void* instance_buffer = nullptr;
    void* node_buffer = nullptr;
    while (nullptr == instance) {
      if (nullptr == instance_buffer) {
        instance_buffer = alloc_.alloc(sizeof(T));
      }
      if (nullptr != instance_buffer && nullptr == node_buffer) {
        node_buffer = alloc_.alloc(sizeof(TSINode<T>));
      }
      if (nullptr == instance_buffer || nullptr == node_buffer) {
        if (REACH_TIME_INTERVAL(10 * 1000L * 1000L)) {
          LIB_LOG(WARN, "new instance fail", "type", typeid(T).name());
        }
        lib::this_routine::usleep(100L * 1000L);
      } else {
        instance = reinterpret_cast<T*>(new (instance_buffer) T());
        node = new (node_buffer) TSINode<T>(instance);
        _LIB_LOG(INFO, "new instance succ [%s] %p size=%ld, tsi=%p", typeid(T).name(), instance, sizeof(T), this);
        if (NULL == list_) {
          list_ = node;
          list_->next = NULL;
        } else {
          node->next = list_;
          list_ = node;
        }
      }
    }
    return instance;
  };

private:
  TSINodeBase* list_;
  common::ModulePageAllocator mod_;
  common::ModuleArena alloc_;
};

class TSIFactory;
extern TSIFactory& get_tsi_fatcory();
extern void tsi_factory_init();
extern void tsi_factory_destroy();

class TSIFactory {
  static const pthread_key_t INVALID_THREAD_KEY = INT32_MAX;

public:
  TSIFactory() : key_(INVALID_THREAD_KEY)
  {}
  ~TSIFactory()
  {}

public:
  int init()
  {
    int ret = OB_SUCCESS;
    if (0 != pthread_key_create(&key_, destroy_thread_data_)) {
      _LIB_LOG(WARN, "pthread_key_create fail errno=%u", errno);
      ret = OB_ERROR;
    }
    return ret;
  }
  int destroy()
  {
    int ret = OB_SUCCESS;
    if (INVALID_THREAD_KEY != key_) {
      void* ptr = pthread_getspecific(key_);
      destroy_thread_data_(ptr);
      if (0 != pthread_key_delete(key_)) {
        ret = OB_ERR_UNEXPECTED;
        _LIB_LOG(WARN, "pthread_key_delete fail errno=%u", errno);
      } else {
        key_ = INVALID_THREAD_KEY;
      }
    }
    return ret;
  }
  template <class T>
  T* new_instance()
  {
    T* instance = NULL;
    if (INVALID_THREAD_KEY != key_) {
      ThreadSpecInfo* tsi = (ThreadSpecInfo*)pthread_getspecific(key_);
      if (NULL == tsi) {
        tsi = new (std::nothrow) ThreadSpecInfo();
        // As some log call to_cstring and to_cstring->GET_TSI_MULT->get_instance->new_instance,
        // so this place do not print log.
        // if (NULL != tsi) {
        //_LIB_LOG(INFO, "new tsi succ %p key=%d", tsi, key_);
        //}
        if (NULL != tsi && 0 != pthread_setspecific(key_, tsi)) {
          _LIB_LOG(WARN, "pthread_setspecific fail errno=%u key=%d", errno, key_);
          delete tsi;
          tsi = NULL;
        }
      }
      if (NULL != tsi) {
        instance = tsi->get_instance<T>();
      }
    }
    return instance;
  }
  template <class T, size_t num>
  inline static T* get_instance()
  {
    // Note: Accelerating TSI object access.
    static __thread T* instance = NULL;
    if (OB_UNLIKELY(NULL == instance)) {
      instance = get_tsi_fatcory().new_instance<T>();
    }
    return instance;
  }

private:
  static void destroy_thread_data_(void* ptr)
  {
    if (NULL != ptr) {
      ThreadSpecInfo* tsi = (ThreadSpecInfo*)ptr;
      delete tsi;
    }
  }

private:
  pthread_key_t key_;
};

}  // namespace common
}  // namespace oceanbase

#endif  // OCEANBASE_COMMON_TSI_FACTORY_
