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

#include "lib/hash/ob_hashtable.h"
#include "lib/hash/ob_serialization.h"
#include "lib/allocator/ob_malloc.h"
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#include <ext/hash_map>

using namespace oceanbase;
using namespace common;
using namespace hash;
using namespace std;
using namespace __gnu_cxx;

const static uint32_t VALUE_SIZE = 1024;
const static uint32_t ITEM_NUM = 1024 * 1024;

struct value_t
{
  uint64_t key;
  char value[VALUE_SIZE];
  template <class _archive>
  int serialization(_archive &ar)
  {
    return ar.push(this, sizeof(value_t));
  };
  template <class _archive>
  int deserialization(_archive &ar)
  {
    return ar.pop(this, sizeof(value_t));
  };
};

class allocer_t
{
  const static uint32_t BLOCK_SIZE = 1024 * 1024;
public:
  allocer_t()
  {
    cur_buffer_pos_ = 0;
    cur_buffer_ = new char[BLOCK_SIZE];
  };
  ~allocer_t()
  {
    cur_buffer_pos_ = 0;
    if (NULL != cur_buffer_) {
      delete [] cur_buffer_;
      cur_buffer_ = NULL;
    }
  };
  void deallocate(void *buffer)
  {
    UNUSED(buffer);
  };
  void *allocate()
  {
    char *ret = NULL;
    uint32_t size = sizeof(HashTableTypes<value_t>::AllocType);
    if ((cur_buffer_pos_ + size) >= BLOCK_SIZE)
    {
      cur_buffer_ = new char[BLOCK_SIZE];
      cur_buffer_pos_ = 0;
    }
    ret = cur_buffer_ + cur_buffer_pos_;
    cur_buffer_pos_ += size;
    return ret;
  };
  void inc_ref()
  {
  };
  void dec_ref()
  {
  };
private:
  uint32_t cur_buffer_pos_;
  char *cur_buffer_;
};

class hashfunc_t
{
public:
  int operator () (uint64_t key, uint64_t &hash_val)
  {
    hash_val = key;
    return OB_SUCCESS;
  };
};

class getkey_t
{
public:
  uint64_t operator () (const value_t &v)
  {
    return v.key;
  };
};

class equal_t
{
public:
  bool operator () (uint64_t k1, uint64_t k2)
  {
    return (k1 == k2);
  };
};

//typedef ObHashTable<uint64_t, value_t, hashfunc_t, equal_t, getkey_t, allocer_t, NoPthreadDefendMode> HashTable;
typedef ObHashTable<uint64_t, value_t, hashfunc_t, equal_t, getkey_t, SimpleAllocer<HashTableTypes<value_t>::AllocType>, ReadWriteDefendMode, BigArray> HashTable;
HashTable *ght = NULL;
typedef hash_map<uint64_t, value_t> StdHashTable;
StdHashTable *sght = NULL;
uint32_t rd_thread_num = 10;
uint32_t wr_thread_num = 10;
pthread_rwlock_t glock = PTHREAD_RWLOCK_INITIALIZER;
typedef void *(*p_thread_func)(void*);

struct thread_data_t
{
  uint64_t thread_num;
  value_t *values;
};

void *ht_rd_thread_func(void *data)
{
  thread_data_t *thread_data = (thread_data_t*)data;
  uint64_t start_pos = ITEM_NUM / rd_thread_num * thread_data->thread_num;
  uint64_t end_pos = start_pos + ITEM_NUM / rd_thread_num;
  for (uint64_t i = start_pos; i < end_pos; i++)
  {
    value_t value;
    if (OB_SUCCESS != ght->get_refactored(thread_data->values[i].key, value))
    {
      fprintf(stderr, "get fail i=%lu\n", i);
      exit(-1);
    }
  }
  return NULL;
}

void *sht_rd_thread_func(void *data)
{
  thread_data_t *thread_data = (thread_data_t*)data;
  uint64_t start_pos = ITEM_NUM / rd_thread_num * thread_data->thread_num;
  uint64_t end_pos = start_pos + ITEM_NUM / rd_thread_num;
  for (uint64_t i = start_pos; i < end_pos; i++)
  {
    pthread_rwlock_rdlock(&glock);
    (*sght)[(thread_data->values)[i].key];
    pthread_rwlock_unlock(&glock);
  }
  return NULL;
}

void *ht_wr_thread_func(void *data)
{
  thread_data_t *thread_data = (thread_data_t*)data;
  uint64_t start_pos = ITEM_NUM / wr_thread_num * thread_data->thread_num;
  uint64_t end_pos = start_pos + ITEM_NUM / wr_thread_num;
  for (uint64_t i = start_pos; i < end_pos; i++)
  {
    if (OB_SUCCESS != ght->set_refactored((thread_data->values)[i].key, (thread_data->values)[i], 1))
    {
      fprintf(stderr, "set fail i=%lu\n", i);
      exit(-1);
    }
  }
  return NULL;
}

void *sht_wr_thread_func(void *data)
{
  thread_data_t *thread_data = (thread_data_t*)data;
  uint64_t start_pos = ITEM_NUM / wr_thread_num * thread_data->thread_num;
  uint64_t end_pos = start_pos + ITEM_NUM / wr_thread_num;
  for (uint64_t i = start_pos; i < end_pos; i++)
  {
    pthread_rwlock_wrlock(&glock);
    (*sght)[(thread_data->values)[i].key] = (thread_data->values)[i];
    pthread_rwlock_unlock(&glock);
  }
  return NULL;
}

void multi_thread_test(const char* type, value_t *values, p_thread_func rd_thread_func, p_thread_func wr_thread_func)
{
  int64_t timeu = get_cur_microseconds_time();
  pthread_t *rd_pd = new pthread_t[rd_thread_num];
  pthread_t *wr_pd = new pthread_t[wr_thread_num];
  thread_data_t *rd_thread_datas = new thread_data_t[rd_thread_num];
  thread_data_t *wr_thread_datas = new thread_data_t[wr_thread_num];
  for(uint32_t i = 0; i < wr_thread_num; i++)
  {
    wr_thread_datas[i].thread_num = i;
    wr_thread_datas[i].values = values;
    pthread_create(wr_pd + i, NULL, wr_thread_func, (void*)(wr_thread_datas + i));
  }
  for(uint32_t i = 0; i < rd_thread_num; i++)
  {
    rd_thread_datas[i].thread_num = i;
    rd_thread_datas[i].values = values;
    pthread_create(rd_pd + i, NULL, rd_thread_func, (void*)(rd_thread_datas + i));
  }
  for(uint32_t i = 0; i < wr_thread_num; i++)
  {
    pthread_join(wr_pd[i], NULL);
  }
  for(uint32_t i = 0; i < rd_thread_num; i++)
  {
    pthread_join(rd_pd[i], NULL);
  }
  delete[] rd_thread_datas;
  delete[] wr_thread_datas;
  delete[] rd_pd;
  delete[] wr_pd;
  fprintf(stdout, "[%s][multi_thread] rd_thread_num=%u wr_thread_num=%u timeu=%ld\n",
        type, rd_thread_num, wr_thread_num, get_cur_microseconds_time() - timeu);
}

void test_data_build(value_t *values)
{
  memset(values, -1, sizeof(value_t) * ITEM_NUM);
  for (uint64_t i = 0; i < ITEM_NUM; i++)
  {
    values[i].key = (i + 1) * (i + 1);
  }
}

void benz_ht_set(value_t *values)
{
  int64_t timeu = get_cur_microseconds_time();
  for (uint64_t i = 0; i < ITEM_NUM; i++)
  {
    if (OB_SUCCESS != ght->set_refactored(values[i].key, values[i], 0))
    {
      fprintf(stderr, "set fail i=%lu\n", i);
      exit(-1);
    }
  }
  fprintf(stdout, "[ht][set] num=%u timeu=%ld\n", ITEM_NUM, get_cur_microseconds_time() - timeu);
}

void benz_ht_get(value_t *values)
{
  int64_t timeu = get_cur_microseconds_time();
  for (uint64_t i = 0; i < ITEM_NUM; i++)
  {
    value_t value;
    if (OB_SUCCESS != ght->get_refactored(values[i].key, value))
    {
      fprintf(stderr, "get fail i=%lu\n", i);
      exit(-1);
    }
  }
  fprintf(stdout, "[ht][get] num=%u timeu=%ld\n", ITEM_NUM, get_cur_microseconds_time() - timeu);
}

void benz_sht_set(value_t *values)
{
  int64_t timeu = get_cur_microseconds_time();
  for (uint64_t i = 0; i < ITEM_NUM; i++)
  {
    (*sght)[values[i].key] = values[i];
  }
  fprintf(stdout, "[std][set] num=%u timeu=%ld\n", ITEM_NUM, get_cur_microseconds_time() - timeu);
}

void benz_sht_get(value_t *values)
{
  int64_t timeu = get_cur_microseconds_time();
  for (uint64_t i = 0; i < ITEM_NUM; i++)
  {
    (*sght)[values[i].key];
  }
  fprintf(stdout, "[std][get] num=%u timeu=%ld\n", ITEM_NUM, get_cur_microseconds_time() - timeu);
}

int main(int argc, char **argv)
{
  int ret = OB_SUCCESS;

  if (1 == argc) {
    //use default value 10, 10
  } else if (3 <= argc) {
    rd_thread_num = atoi(argv[1]);
    wr_thread_num = atoi(argv[2]);
  } else {
    fprintf(stderr, "Usage: test_hash_benz <rd_thread_num> <wr_thread_num>\n");
    ret = OB_ERROR;
  }

  if (OB_SUCC(ret)) {
    value_t *values = new value_t[ITEM_NUM];
    test_data_build(values);

    ght = new HashTable();
    sght = new StdHashTable(ITEM_NUM);
    //allocer_t allocer;
    SimpleAllocer<HashTableTypes<value_t>::AllocType> allocer;
    ObMalloc ballocer;
    ght->create(cal_next_prime(ITEM_NUM), &allocer, &ballocer);

    benz_sht_set(values);
    benz_sht_get(values);
    benz_ht_set(values);
    benz_ht_get(values);
    multi_thread_test("sht", values, sht_rd_thread_func, sht_wr_thread_func);
    multi_thread_test("ht", values, ht_rd_thread_func, ht_wr_thread_func);

    {
      SimpleArchive ar;
      fprintf(stderr, "ar init ret=%d\n", ar.init("./hash.data", SimpleArchive::FILE_OPEN_WFLAG));
      fprintf(stderr, "serialize ret=%d\n", ght->serialization(ar));
      ar.destroy();
    }

    {
      HashTable temp;
      SimpleArchive ar;
      fprintf(stderr, "ar init ret=%d\n", ar.init("./hash.data", SimpleArchive::FILE_OPEN_RFLAG));
      fprintf(stderr, "serialize ret=%d\n", temp.deserialization(ar, &allocer));
      fprintf(stderr, "hash size=%lu\n", temp.size());
      ar.destroy();
    }

    delete sght;
    delete ght;
    delete[] values;
  }

  return ret;
}
