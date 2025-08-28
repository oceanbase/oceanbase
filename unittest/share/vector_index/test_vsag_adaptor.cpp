/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX LIB

#include <gtest/gtest.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include "lib/vector/ob_vector_util.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_log.h"
#include "vsag/vsag.h"
#include "vsag/errors.h"
#include "vsag/dataset.h"
#include "vsag/search_param.h"
#include "vsag/index.h"
#include "vsag/options.h"
#include "vsag/factory.h"
#include "roaring/roaring64.h"

namespace oceanbase
{
namespace common
{

using namespace vsag;
class TestVsagAdaptor : public ::testing::Test
{
public:
  TestVsagAdaptor(): logger_() {}
  ~TestVsagAdaptor() {}

  virtual void SetUp();
  virtual void TearDown() {}

private:
  obvectorutil::ObVsagLogger logger_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestVsagAdaptor);
};

void TestVsagAdaptor::SetUp()
{
  obvsag::set_logger(&logger_);
}

class DefaultVsagAllocator : public vsag::Allocator
{
public:
  constexpr static int64_t MEM_PTR_HEAD_SIZE = sizeof(int64_t);
  DefaultVsagAllocator() = default;
  virtual ~DefaultVsagAllocator() = default;

  DefaultVsagAllocator(const DefaultVsagAllocator&) = delete;
  DefaultVsagAllocator(DefaultVsagAllocator&&) = delete;

public:
  std::string Name() override { return "DefaultVsagAllocator"; }

  void* Allocate(size_t size) override;

  void Deallocate(void* p) override;

  void* Reallocate(void* p, size_t size) override;


  int64_t total_{0};
};

void* DefaultVsagAllocator::Allocate(size_t size)
{
  void *ret_ptr = nullptr;
  if (size != 0) {
    int64_t actual_size = MEM_PTR_HEAD_SIZE + size;
    void *ptr = malloc(actual_size);
    if (nullptr != ptr) {
      // total_ += size;
      ATOMIC_AAF(&total_, actual_size);
      *(int64_t*)ptr = actual_size;
      ret_ptr = (char*)ptr + MEM_PTR_HEAD_SIZE;
    }
    // if (total_ < 0) abort();
    // LOG_INFO("allocate memoery", KP(this), KP(ptr), K(size), K(total_));
  }
  if (size != 0 && nullptr == ret_ptr) {
    abort();
  }
  return ret_ptr;
}

void DefaultVsagAllocator::Deallocate(void* p)
{
  if (nullptr != p) {
    void *size_ptr = (char*)p - MEM_PTR_HEAD_SIZE;
    int64_t size = *(int64_t *)size_ptr;
    // total_ -= size;
    ATOMIC_SAF(&total_, size);
    free((char*)p - MEM_PTR_HEAD_SIZE);
    // if (total_ < 0) abort();
    // LOG_INFO("free memoery", KP(this), KP(p), K(size), K(total_));
    p = nullptr;
  }
}

void* DefaultVsagAllocator::Reallocate(void* p, size_t size)
{
  void *new_ptr = nullptr;
  if (size == 0) {
    if (nullptr != p) {
      Deallocate(p);
      p = nullptr;
    }
  } else if (nullptr == p) {
    new_ptr = Allocate(size);
  } else {
    void *size_ptr = (char*)p - MEM_PTR_HEAD_SIZE;
    int64_t old_size = *(int64_t *)size_ptr - MEM_PTR_HEAD_SIZE;
    if (old_size >= size) {
      new_ptr = p;
    } else {
      new_ptr = Allocate(size);
      if (nullptr == new_ptr || nullptr == p) {
      } else {
        memcpy(new_ptr, p, old_size);
        Deallocate(p);
        p = nullptr;
      }
    }
  }
  return new_ptr;
}


class TestFilter : public obvsag::FilterInterface
{
public:
  TestFilter(roaring::api::roaring64_bitmap_t *bitmap) : bitmap_(bitmap) {}
  ~TestFilter() {}
  bool test(int64_t id) override { return roaring::api::roaring64_bitmap_contains(bitmap_, id); }
  bool test(const char* data) override { return true; }
public:
  roaring::api::roaring64_bitmap_t* bitmap_;
};

TEST_F(TestVsagAdaptor, test_hnsw)
{
  ASSERT_TRUE(obvsag::is_init());
  obvsag::VectorIndexPtr index_handler = nullptr;
  int dim = 1536;
  int max_degree = 16;
  int ef_search = 200;
  int ef_construction = 100;
  DefaultVsagAllocator default_allocator;
  const char* const METRIC_L2 = "l2";
  const char* const METRIC_IP = "ip";

  const char* const DATATYPE_FLOAT32 = "float32";
  void * test_ptr = default_allocator.Allocate(10);
  ASSERT_EQ(0, obvsag::create_index(index_handler,
                                                    obvsag::HNSW_TYPE,
                                                    DATATYPE_FLOAT32,
                                                    METRIC_L2,
                                                    dim,
                                                    max_degree,
                                                    ef_construction,
                                                    ef_search,
                                                    &default_allocator));
  int num_vectors = 10000;
  auto ids = new int64_t[num_vectors];
  auto vectors = new float[dim * num_vectors];
  std::mt19937 rng;
  rng.seed(47);
  std::uniform_real_distribution<> distrib_real;
  for (int64_t i = 0; i < num_vectors; ++i) {
      ids[i] = i;
  }
  for (int64_t i = 0; i < dim * num_vectors; ++i) {
      vectors[i] = distrib_real(rng);
  }
  ASSERT_EQ(0, obvsag::build_index(index_handler, vectors, ids, dim, num_vectors));

  int64_t num_size = 0;
  ASSERT_EQ(0, obvsag::get_index_number(index_handler, num_size));
  std::cout<<"after add index, size is "<< num_size <<std::endl;

  int inc_num = 10000;
  auto inc = new float[dim * inc_num];
  for (int64_t i = 0; i < dim * inc_num; ++i) {
      inc[i] = distrib_real(rng);
  }
  auto ids2 = new int64_t[inc_num];
  for (int64_t i = 0; i < inc_num; ++i) {
      ids2[i] = i + num_vectors;
  }

  ASSERT_EQ(0, obvsag::add_index(index_handler, inc, ids2, dim,inc_num));
  ASSERT_EQ(0, obvsag::get_index_number(index_handler, num_size));
  std::cout<<"after add index, size is "<< num_size <<std::endl;
  
  const float* result_dist;
  const int64_t* result_ids;
  int64_t result_size = 0;

  roaring::api::roaring64_bitmap_t* r1 = roaring::api::roaring64_bitmap_create();
  TestFilter testfilter(r1);
  const char *extra_info = nullptr;
  ASSERT_EQ(0, obvsag::knn_search(index_handler, vectors+dim*(num_vectors-1), dim, 10,
                                                result_dist,result_ids,result_size, 
                                                100, false/*need_extra_info*/, extra_info, &testfilter, false, false, nullptr, 1));
  default_allocator.Deallocate((void*)result_ids);
  default_allocator.Deallocate((void*)result_dist);
  roaring64_bitmap_add_range(r1, 0, 19800);

  ASSERT_EQ(0, obvsag::knn_search(index_handler, vectors+dim*(num_vectors-1), dim, 10,
                                                result_dist,result_ids,result_size, 
                                                100, false/*need_extra_info*/, extra_info, &testfilter, false, false, nullptr, 0.01));
  ASSERT_EQ(nullptr, extra_info);
  // const float *distances;
  // ret_knn_search = obvsag::cal_distance_by_id(index_handler, vectors+dim*(num_vectors-1), result_ids, result_size, distances);
  for (int i = 0; i < result_size; i++) {
      std::cout << "result: " << result_ids[i] << " " << result_dist[i] << std::endl;
      // std::cout << "calres: " << result_ids[i] << " " << distances[i] << std::endl;
  }
  default_allocator.Deallocate((void*)result_ids);
  default_allocator.Deallocate((void*)result_dist);
  obvsag::delete_index(index_handler);
  default_allocator.Deallocate(test_ptr);
  ASSERT_EQ(0, default_allocator.total_);
}

TEST_F(TestVsagAdaptor, test_hnswsq)
{
  ASSERT_TRUE(obvsag::is_init());
  obvsag::VectorIndexPtr index_handler = nullptr;
  int dim = 128;
  int max_degree = 16;
  int ef_search = 200;
  int ef_construction = 100;
  DefaultVsagAllocator default_allocator;
  const char* const METRIC_L2 = "l2";
  const char* const METRIC_IP = "ip";

  const char* const DATATYPE_FLOAT32 = "float32";
  void * test_ptr = default_allocator.Allocate(10);
  ASSERT_EQ(0, obvsag::create_index(index_handler,
                                                    obvsag::HNSW_SQ_TYPE,
                                                    DATATYPE_FLOAT32,
                                                    METRIC_L2,
                                                    dim,
                                                    max_degree,
                                                    ef_construction,
                                                    ef_search,
                                                    &default_allocator));
  int num_vectors = 10000;
  auto ids = new int64_t[num_vectors];
  auto vectors = new float[dim * num_vectors];
  std::mt19937 rng;
  rng.seed(47);
  std::uniform_real_distribution<> distrib_real;
  for (int64_t i = 0; i < num_vectors; ++i) {
      ids[i] = i + num_vectors*10;
  }
  for (int64_t i = 0; i < dim * num_vectors; ++i) {
      vectors[i] = distrib_real(rng);
  }
  ASSERT_EQ(0, obvsag::build_index(index_handler, vectors, ids, dim, num_vectors));

  int64_t num_size = 0;
  ASSERT_EQ(0, obvsag::get_index_number(index_handler, num_size));
  std::cout<<"after add index, size is "<< num_size <<std::endl;
  
  const float* result_dist;
  const int64_t* result_ids;
  int64_t result_size = 0;
  auto query_vector = new float[dim];
  for (int64_t i = 0; i < dim; ++i) {
      query_vector[i] = distrib_real(rng);
  }

  roaring::api::roaring64_bitmap_t* r1 = roaring::api::roaring64_bitmap_create();

  roaring::api::roaring64_bitmap_add(r1, 18);
  roaring::api::roaring64_bitmap_add(r1, 1169);
  roaring::api::roaring64_bitmap_add(r1, 1285);
  std::cout << "before search" << std::endl;
  TestFilter testfilter(r1);
  const char *extra_info = nullptr;
  ASSERT_EQ(0, obvsag::knn_search(index_handler, query_vector, dim, 10,
                                                result_dist,result_ids,result_size, 
                                                100, false/*need_extra_info*/, extra_info, &testfilter));
  
  for (int i = 0; i < result_size; i++) {
      std::cout << "result: " << result_ids[i] << " " << result_dist[i] << std::endl;
  }

  default_allocator.Deallocate((void*)result_ids);
  default_allocator.Deallocate((void*)result_dist);

  int inc_num = 1000;
  auto inc = new float[dim * inc_num];
  for (int64_t i = 0; i < dim * inc_num; ++i) {
      inc[i] = distrib_real(rng);
  }
  auto ids2 = new int64_t[inc_num];
  for (int64_t i = 0; i < inc_num; ++i) {
      ids2[i] = i + num_vectors*100;
  }

  // const std::string dir = "./";
  // int ret_serialize_single = obvsag::serialize(index_handler,dir);
  // int ret_deserilize_single_bin = 
  //                 obvsag::deserialize_bin(index_handler,dir);
  // ret_knn_search = obvsag::knn_search(index_handler, query_vector, dim, 10,
  //                                              result_dist,result_ids,result_size, 
  //                                              100, r1);
  // for (int i = 0; i < result_size; i++) {
  //     std::cout << "result: " << result_ids[i] << " " << result_dist[i] << std::endl;
  // }
  obvsag::delete_index(index_handler);
  default_allocator.Deallocate(test_ptr);
  ASSERT_EQ(0, default_allocator.total_);
}

TEST_F(TestVsagAdaptor, test_hgraph_extra_info)
{
  ASSERT_TRUE(obvsag::is_init());
  obvsag::VectorIndexPtr index_handler = NULL;
  int dim = 1536;
  int max_degree = 16;
  int ef_search = 200;
  int ef_construction = 100;
  DefaultVsagAllocator default_allocator;
  const char* const METRIC_L2 = "l2";
  const char* const METRIC_IP = "ip";

  const char* const DATATYPE_FLOAT32 = "float32";
  void * test_ptr = default_allocator.Allocate(10);
  int extra_info_sz = 32;
  std::cout<<"test create_index: "<<std::endl;
  ASSERT_EQ(0, obvsag::create_index(index_handler,
                                                    obvsag::HGRAPH_TYPE,
                                                    DATATYPE_FLOAT32,
                                                    METRIC_L2,
                                                    dim,
                                                    max_degree,
                                                    ef_construction,
                                                    ef_search,
                                                    &default_allocator,
                                                    extra_info_sz));

  int num_vectors = 10000;
  auto ids = new int64_t[num_vectors];
  auto vectors = new float[dim * num_vectors];
  std::mt19937 rng;
  rng.seed(47);
  std::uniform_real_distribution<> distrib_real;
  for (int64_t i = 0; i < num_vectors; ++i) {
      ids[i] = i;
  }
  for (int64_t i = 0; i < dim * num_vectors; ++i) {
      vectors[i] = distrib_real(rng);
  }
  std::cout<<"test build_index: "<<std::endl;
  char extra_infos[extra_info_sz * num_vectors];
  for (int i = 0; i < extra_info_sz * num_vectors; i++) {
      extra_infos[i] = rand() % 9 + '0';
  }
  ASSERT_EQ(0, obvsag::build_index(index_handler, vectors, ids, dim, num_vectors, extra_infos));

  int64_t num_size = 0;
  std::cout<<"test get_index_number: "<<std::endl;
  ASSERT_EQ(0, obvsag::get_index_number(index_handler, num_size));
  std::cout<<"after add index, size is "<< num_size <<std::endl;

  int inc_num = 10000;
  auto inc = new float[dim * inc_num];
  for (int64_t i = 0; i < dim * inc_num; ++i) {
      inc[i] = distrib_real(rng);
  }
  auto ids2 = new int64_t[inc_num];
  for (int64_t i = 0; i < inc_num; ++i) {
      ids2[i] = i + num_vectors;
  }

  std::cout<<"test add_index: "<<std::endl;
  char extra_info[extra_info_sz];
  for (int i = 0; i < extra_info_sz; i++) {
      extra_info[i] = rand() % 9 + '0';
  }
  ASSERT_EQ(0, obvsag::add_index(index_handler, inc, ids2, dim,inc_num, extra_info));
  ASSERT_EQ(0, obvsag::get_index_number(index_handler, num_size));
  std::cout<<"after add index, size is "<< num_size <<std::endl;

  const float* result_dist;
  const int64_t* result_ids;
  int64_t result_size = 0;

  roaring::api::roaring64_bitmap_t* r1 = roaring::api::roaring64_bitmap_create();
  const char *extra_info_search = nullptr;
  TestFilter testfilter(r1);
  std::cout<<"test knn_search: "<<std::endl;
  ASSERT_EQ(0, obvsag::knn_search(index_handler, vectors+dim*(num_vectors-1), dim, 10,
                                                result_dist,result_ids,result_size,
                                                100, true/*need_extra_info*/, extra_info_search, &testfilter));
  std::string s1(extra_info_search, extra_info_search + extra_info_sz);
  std::cout << s1 << std::endl;
  default_allocator.Deallocate((void*)result_ids);
  default_allocator.Deallocate((void*)result_dist);
  default_allocator.Deallocate((void*)extra_info_search);
  roaring64_bitmap_add_range(r1, 0, 19800);

  std::cout<<"test knn_search2: "<<std::endl;
  ASSERT_EQ(0, obvsag::knn_search(index_handler, vectors+dim*(num_vectors-1), dim, 10,
                                                result_dist,result_ids,result_size,
                                                100, true/*need_extra_info*/, extra_info_search, &testfilter));
  std::string s2(extra_info_search, extra_info_search + extra_info_sz);
  std::cout << s2 << std::endl;
  const float *distances = nullptr;
  ASSERT_EQ(0, obvsag::cal_distance_by_id(index_handler, vectors+dim*(num_vectors-1), result_ids, result_size, distances));
  for (int i = 0; i < result_size; i++) {
      std::cout << "result: " << result_ids[i] << " " << result_dist[i] << std::endl;
      std::cout << "calres: " << result_ids[i] << " " << distances[i] << std::endl;
  }
  default_allocator.Deallocate((void*)result_ids);
  default_allocator.Deallocate((void*)result_dist);
  default_allocator.Deallocate((void*)extra_info_search);
  default_allocator.Deallocate((void*)distances);
  obvsag::delete_index(index_handler);
  default_allocator.Deallocate(test_ptr);
  ASSERT_EQ(0, default_allocator.total_);
}

TEST_F(TestVsagAdaptor, test_hgraph_iter_filter)
{
  std::cout<<"test hgraph_iter_filter_example: "<<std::endl;
  ASSERT_TRUE(obvsag::is_init());
  obvsag::VectorIndexPtr index_handler = NULL;
  int dim = 1536;
  int max_degree = 16;
  int ef_search = 200;
  int ef_construction = 100;
  DefaultVsagAllocator default_allocator;
  const char* const METRIC_L2 = "l2";
  const char* const METRIC_IP = "ip";

  const char* const DATATYPE_FLOAT32 = "float32";
  void * test_ptr = default_allocator.Allocate(10);
  ASSERT_EQ(0, obvsag::create_index(index_handler,
                                                    obvsag::HNSW_SQ_TYPE,
                                                    DATATYPE_FLOAT32,
                                                    METRIC_L2,
                                                    dim,
                                                    max_degree,
                                                    ef_construction,
                                                    ef_search,
                                                    &default_allocator));

  int num_vectors = 10000;
  auto ids = new int64_t[num_vectors];
  auto vectors = new float[dim * num_vectors];
  std::mt19937 rng;
  rng.seed(47);
  std::uniform_real_distribution<> distrib_real;
  for (int64_t i = 0; i < num_vectors; ++i) {
      ids[i] = i;
  }
  for (int64_t i = 0; i < dim * num_vectors; ++i) {
      vectors[i] = distrib_real(rng);
  }

  int inc_num = 10000;
  auto inc = new float[dim * inc_num];
  for (int64_t i = 0; i < dim * inc_num; ++i) {
      inc[i] = distrib_real(rng);
  }
  auto ids2 = new int64_t[inc_num];
  for (int64_t i = 0; i < inc_num; ++i) {
      ids2[i] = i + num_vectors;
  }
  ASSERT_EQ(0, obvsag::build_index(index_handler, vectors, ids, dim, num_vectors));
  ASSERT_EQ(0, obvsag::add_index(index_handler, inc, ids2, dim,inc_num));
  
  const float* result_dist;
  const int64_t* result_ids;
  int64_t result_size = 0;
  
  // vsag::IteratorContext *filter_ctx = nullptr;
  void *iter_ctx = nullptr;

  roaring::api::roaring64_bitmap_t* r1 = roaring::api::roaring64_bitmap_create();
  roaring64_bitmap_add_range(r1, 0, 500);
  TestFilter testfilter(r1);
  const float *distances;
  const char* extra_infos = nullptr;

  ASSERT_EQ(0, obvsag::knn_search(index_handler, vectors+dim*(num_vectors-1), dim, 10,
                                                result_dist,result_ids,result_size, 
                                                100, false, extra_infos, &testfilter, false, false, 0.97, iter_ctx, false));
  ASSERT_NE(nullptr, iter_ctx);
  ASSERT_EQ(0, obvsag::cal_distance_by_id(index_handler, vectors+dim*(num_vectors-1), result_ids, result_size, distances));
  std::cout << "-------- result1: --------" << std::endl;
  for (int i = 0; i < result_size; i++) {
      std::cout << "result: " << result_ids[i] << " " << result_dist[i] << std::endl;
      std::cout << "calres: " << result_ids[i] << " " << distances[i] << std::endl;
  }    
  default_allocator.Deallocate((void*)result_ids);
  default_allocator.Deallocate((void*)result_dist);  
  default_allocator.Deallocate((void*)distances);

  const float *distances2;
  const float* result_dist2;
  const int64_t* result_ids2;
  int64_t result_size2 = 0;
  std::cout << "-------- result2: --------" << std::endl;
  ASSERT_EQ(0, obvsag::knn_search(index_handler, vectors+dim*(num_vectors-1), dim, 10,
                                                result_dist2,result_ids2,result_size2, 
                                                100, false, extra_infos, &testfilter, false, false, 0.97, iter_ctx, false));
  ASSERT_EQ(0, obvsag::cal_distance_by_id(index_handler, vectors+dim*(num_vectors-1), result_ids2, result_size2, distances2));
  for (int i = 0; i < result_size2; i++) {
      std::cout << "result: " << result_ids2[i] << " " << result_dist2[i] << std::endl;
      std::cout << "calres: " << result_ids2[i] << " " << distances2[i] << std::endl;
  }

  default_allocator.Deallocate((void*)result_ids2);
  default_allocator.Deallocate((void*)result_dist2);
  default_allocator.Deallocate((void*)distances2);

  const float *distances3 = nullptr;
  const float* result_dist3 = nullptr;
  const int64_t* result_ids3 = nullptr;
  int64_t result_size3 = 0;
  std::cout << "-------- result3: --------" << std::endl;
  ASSERT_EQ(0, obvsag::knn_search(index_handler, vectors+dim*(num_vectors-1), dim, 20,
                                                result_dist3,result_ids3,result_size3, 
                                                100, false, extra_infos, &testfilter, false, false, 0.97, iter_ctx, true));
  ASSERT_EQ(0, obvsag::cal_distance_by_id(index_handler, vectors+dim*(num_vectors-1), result_ids3, result_size3, distances3));
  for (int i = 0; i < result_size3; i++) {
      std::cout << "result: " << result_ids3[i] << " " << result_dist3[i] << std::endl;
      std::cout << "calres: " << result_ids3[i] << " " << distances3[i] << std::endl;
  }
  default_allocator.Deallocate((void*)result_ids3);
  default_allocator.Deallocate((void*)result_dist3);
  default_allocator.Deallocate((void*)distances3);
  obvsag::delete_iter_ctx(iter_ctx);
  obvsag::delete_index(index_handler);
  default_allocator.Deallocate(test_ptr);
  ASSERT_EQ(0, default_allocator.total_);
}

TEST_F(TestVsagAdaptor, test_hnsw_iter_filter)
{
  std::cout<<"test iter_filter_example: "<<std::endl;
  ASSERT_TRUE(obvsag::is_init());
  obvsag::VectorIndexPtr index_handler = NULL;
  int dim = 1536;
  int max_degree = 16;
  int ef_search = 200;
  int ef_construction = 100;
  DefaultVsagAllocator default_allocator;
  const char* const METRIC_L2 = "l2";
  const char* const METRIC_IP = "ip";

  const char* const DATATYPE_FLOAT32 = "float32";
  void * test_ptr = default_allocator.Allocate(10);
  ASSERT_EQ(0, obvsag::create_index(index_handler,
                                                    obvsag::HNSW_TYPE,
                                                    DATATYPE_FLOAT32,
                                                    METRIC_L2,
                                                    dim,
                                                    max_degree,
                                                    ef_construction,
                                                    ef_search,
                                                    &default_allocator));
  int num_vectors = 10000;
  auto ids = new int64_t[num_vectors];
  auto vectors = new float[dim * num_vectors];
  std::mt19937 rng;
  rng.seed(47);
  std::uniform_real_distribution<> distrib_real;
  for (int64_t i = 0; i < num_vectors; ++i) {
      ids[i] = i;
  }
  for (int64_t i = 0; i < dim * num_vectors; ++i) {
      vectors[i] = distrib_real(rng);
  }

  int inc_num = 10000;
  auto inc = new float[dim * inc_num];
  for (int64_t i = 0; i < dim * inc_num; ++i) {
      inc[i] = distrib_real(rng);
  }
  auto ids2 = new int64_t[inc_num];
  for (int64_t i = 0; i < inc_num; ++i) {
      ids2[i] = i + num_vectors;
  }

  ASSERT_EQ(0, obvsag::add_index(index_handler, inc, ids2, dim,inc_num));
  
  const float* result_dist;
  const int64_t* result_ids;
  int64_t result_size = 0;
  
  // vsag::IteratorContext *filter_ctx = nullptr;
  void *iter_ctx = nullptr;

  roaring::api::roaring64_bitmap_t* r1 = roaring::api::roaring64_bitmap_create();
  roaring64_bitmap_add_range(r1, 0, 500);
  TestFilter testfilter(r1);
  const float *distances;
  const char* extra_infos = nullptr;

  ASSERT_EQ(0, obvsag::knn_search(index_handler, vectors+dim*(num_vectors-1), dim, 10,
                                                result_dist,result_ids,result_size, 
                                                100, false, extra_infos, &testfilter, false, false, 0.97, iter_ctx, false));
  ASSERT_NE(nullptr, iter_ctx);
  ASSERT_EQ(0, obvsag::cal_distance_by_id(index_handler, vectors+dim*(num_vectors-1), result_ids, result_size, distances));
  std::cout << "-------- result1: --------" << std::endl;
  for (int i = 0; i < result_size; i++) {
      std::cout << "result: " << result_ids[i] << " " << result_dist[i] << std::endl;
      std::cout << "calres: " << result_ids[i] << " " << distances[i] << std::endl;
  }
  default_allocator.Deallocate((void*)result_ids);
  default_allocator.Deallocate((void*)result_dist);
  default_allocator.Deallocate((void*)distances);

  const float *distances2;
  const float* result_dist2;
  const int64_t* result_ids2;
  int64_t result_size2 = 0;
  std::cout << "-------- result2: --------" << std::endl;
  ASSERT_EQ(0, obvsag::knn_search(index_handler, vectors+dim*(num_vectors-1), dim, 10,
                                                result_dist2,result_ids2,result_size2, 
                                                100, false, extra_infos, &testfilter, false, false, 0.97, iter_ctx, false));
  ASSERT_EQ(0, obvsag::cal_distance_by_id(index_handler, vectors+dim*(num_vectors-1), result_ids2, result_size2, distances2));
  for (int i = 0; i < result_size2; i++) {
      std::cout << "result: " << result_ids2[i] << " " << result_dist2[i] << std::endl;
      std::cout << "calres: " << result_ids2[i] << " " << distances2[i] << std::endl;
  }
  default_allocator.Deallocate((void*)result_ids2);
  default_allocator.Deallocate((void*)result_dist2);
  default_allocator.Deallocate((void*)distances2);

  const float *distances3;
  const float* result_dist3;
  const int64_t* result_ids3;
  int64_t result_size3 = 0;
  std::cout << "-------- result3: --------" << std::endl;
  ASSERT_EQ(0, obvsag::knn_search(index_handler, vectors+dim*(num_vectors-1), dim, 20,
                                                result_dist3,result_ids3,result_size3, 
                                                100, false, extra_infos, &testfilter, false, false, 0.97, iter_ctx, true));
  ASSERT_EQ(0, obvsag::cal_distance_by_id(index_handler, vectors+dim*(num_vectors-1), result_ids3, result_size3, distances3));
  for (int i = 0; i < result_size3; i++) {
      std::cout << "result: " << result_ids3[i] << " " << result_dist3[i] << std::endl;
      std::cout << "calres: " << result_ids3[i] << " " << distances3[i] << std::endl;
  }
  default_allocator.Deallocate((void*)result_ids3);
  default_allocator.Deallocate((void*)result_dist3);
  default_allocator.Deallocate((void*)distances3);
  obvsag::delete_iter_ctx(iter_ctx);
  obvsag::delete_index(index_handler);
  default_allocator.Deallocate(test_ptr);
  ASSERT_EQ(0, default_allocator.total_);
}

TEST_F(TestVsagAdaptor, test_hgraph_bq)
{
  ASSERT_TRUE(obvsag::is_init());
  obvsag::VectorIndexPtr index_handler = NULL;
  int dim = 128;
  int max_degree = 16;
  int ef_search = 200;
  int ef_construction = 100;
  DefaultVsagAllocator default_allocator;
  const char* const METRIC_L2 = "l2";

  const char* const DATATYPE_FLOAT32 = "float32";
  void * test_ptr = default_allocator.Allocate(10);
  ASSERT_EQ(0, obvsag::create_index(index_handler,
                                                    obvsag::HNSW_BQ_TYPE,
                                                    DATATYPE_FLOAT32,
                                                    METRIC_L2,
                                                    dim,
                                                    max_degree,
                                                    ef_construction,
                                                    ef_search,
                                                    &default_allocator));

  int num_vectors = 10000;
  auto ids = new int64_t[num_vectors];
  auto vectors = new float[dim * num_vectors];
  std::mt19937 rng;
  rng.seed(47);
  std::uniform_real_distribution<> distrib_real;
  for (int64_t i = 0; i < num_vectors; ++i) {
      ids[i] = i + num_vectors*10;
  }
  for (int64_t i = 0; i < dim * num_vectors; ++i) {
      vectors[i] = distrib_real(rng);
  }
  ASSERT_EQ(0, obvsag::build_index(index_handler, vectors, ids, dim, num_vectors));

  int64_t num_size = 0;
  ASSERT_EQ(0, obvsag::get_index_number(index_handler, num_size));
  std::cout<<"after add index, size is "<< num_size << std::endl;
  
  const float* result_dist;
  const int64_t* result_ids;
  int64_t result_size = 0;
  auto query_vector = new float[dim];
  for (int64_t i = 0; i < dim; ++i) {
      query_vector[i] = distrib_real(rng);
  }

  roaring::api::roaring64_bitmap_t* r1 = roaring::api::roaring64_bitmap_create();

  roaring::api::roaring64_bitmap_add(r1, 18);
  roaring::api::roaring64_bitmap_add(r1, 1169);
  roaring::api::roaring64_bitmap_add(r1, 1285);
  std::cout << "before search" << std::endl;
  TestFilter testfilter(r1);
  const char *extra_info = nullptr;
  ASSERT_EQ(0, obvsag::knn_search(index_handler, query_vector, dim, 10,
                                                result_dist,result_ids,result_size, 
                                                100, false/*need_extra_info*/, extra_info, &testfilter));
  
  for (int i = 0; i < result_size; i++) {
      std::cout << "result: " << result_ids[i] << " " << result_dist[i] << std::endl;
  }
  default_allocator.Deallocate((void*)result_ids);
  default_allocator.Deallocate((void*)result_dist);
  int inc_num = 1000;
  auto inc = new float[dim * inc_num];
  for (int64_t i = 0; i < dim * inc_num; ++i) {
      inc[i] = distrib_real(rng);
  }
  auto ids2 = new int64_t[inc_num];
  for (int64_t i = 0; i < inc_num; ++i) {
      ids2[i] = i + num_vectors*100;
  }
  obvsag::delete_index(index_handler);
  default_allocator.Deallocate(test_ptr);
  ASSERT_EQ(0, default_allocator.total_);
}

}  // namespace common
}  // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  // system("rm -f test_vsag_adaptor.log");
  // OB_LOGGER.set_file_name("test_vsag_adaptor.log");
  OB_LOGGER.set_log_level("INFO");
  return RUN_ALL_TESTS();
}