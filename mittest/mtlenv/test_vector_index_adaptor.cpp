/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE

#include <gtest/gtest.h>
#define private public
#define protected public

#include "lib/string/ob_sql_string.h"
#include "roaring/roaring64.h"
#include "share/vector_index/ob_plugin_vector_index_adaptor.h"
#include "share/vector_index/ob_plugin_vector_index_serialize.h"
#include "storage/ob_i_store.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "lib/oblog/ob_log_module.h"

#include "lib/allocator/ob_mod_define.h"
#include "lib/allocator/page_arena.h"
#include "lib/vector/ob_vector_util.h"

#undef private
#undef protected
#include <random>
#include<iostream>

namespace oceanbase {


using namespace storage;
using namespace common;

class TestVectorIndexAdaptor : public ::testing::Test {
public:
  TestVectorIndexAdaptor()
  {}
  ~TestVectorIndexAdaptor()
  {}

  static void SetUpTestCase()
  {
    EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
    MTL(transaction::ObTransService*)->tx_desc_mgr_.tx_id_allocator_ =
      [](transaction::ObTransID &tx_id) { tx_id = transaction::ObTransID(1001); return OB_SUCCESS; };
    ObServerStorageMetaService::get_instance().is_started_ = true;
  }
  static void TearDownTestCase()
  {
    MockTenantModuleEnv::get_instance().destroy();
  }
  virtual void SetUp()
  {
    ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());

  }
  virtual void TearDown()
  {}

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestVectorIndexAdaptor);
};


void* ObArenaAllocator_malloc_adapter(size_t size, ObArenaAllocator* allocator) {
  // 调用 ObArenaAllocator 的 alloc 方法并转换 size
  return allocator->alloc(static_cast<int64_t>(size));
}

ObArenaAllocator myAllocator(ObModIds::TEST);

void* global_allocator_malloc(size_t size) {
  return ObArenaAllocator_malloc_adapter(size, &myAllocator);
}

void* global_allocator_free(void *size) {
  void *res = nullptr;
  return res;
}

/*
TEST_F(TestVectorIndexAdaptor, bitmap_alloc)
{
  roaring_memory_t memory_hook;
  memory_hook.malloc = &global_allocator_malloc;
  roaring_init_memory_hook(memory_hook);

  roaring::api::roaring64_bitmap_t *bitmap = roaring::api::roaring64_bitmap_create();
  roaring::api::roaring64_bitmap_add(bitmap, 3);
  roaring::api::roaring64_bitmap_add(bitmap, 5);
  roaring::api::roaring64_bitmap_add(bitmap, 7);

  ASSERT_EQ(roaring64_bitmap_contains(bitmap, 5), true);
  ASSERT_EQ(roaring64_bitmap_contains(bitmap, 20), false);
}
*/

/*
void *ob_bitmap_mallloc(size_t size)
{
  return oceanbase::common::ob_malloc(size, oceanbase::common::ObModIds::TEST);
}

void *ob_bitmap_realloc(void *ptr, size_t size)
{
  ObMemAttr attr(oceanbase::common::OB_SERVER_TENANT_ID, "test");
  return oceanbase::common::ob_realloc(ptr, size, attr);
}

void *ob_bitmap_aligned_malloc(size_t a, size_t b)
{
  ObMemAttr attr(oceanbase::common::OB_SERVER_TENANT_ID, "test");
  return oceanbase::common::ob_malloc_align(a, b, attr);
}

void ob_bitmap_free(void *ptr)
{
  oceanbase::common::ob_free(ptr);
}

void ob_bitmap_aligned_free(void *ptr)
{
  oceanbase::common::ob_free_align(ptr);
}


TEST_F(TestVectorIndexAdaptor, bitmap_malloc)
{
  roaring_memory_t memory_hook;
  memory_hook.malloc = &ob_bitmap_mallloc;
  memory_hook.realloc = &ob_bitmap_realloc;
  memory_hook.aligned_malloc = &ob_bitmap_aligned_malloc;
  memory_hook.free = &ob_bitmap_free;
  memory_hook.aligned_free = &ob_bitmap_aligned_free;
  roaring_init_memory_hook(memory_hook);

  roaring::api::roaring64_bitmap_t *bitmap = roaring::api::roaring64_bitmap_create();
  roaring::api::roaring64_bitmap_add(bitmap, 3);

  ASSERT_EQ(roaring64_bitmap_contains(bitmap, 3), true);

}

TEST_F(TestVectorIndexAdaptor, bitmap_flip)
{
  ObArenaAllocator allocator(ObModIds::TEST);

  roaring::api::roaring64_bitmap_t *bitmap = roaring::api::roaring64_bitmap_create();
  roaring::api::roaring64_bitmap_add(bitmap, 3);
  roaring::api::roaring64_bitmap_add(bitmap, 5);
  roaring::api::roaring64_bitmap_add(bitmap, 7);

  ASSERT_EQ(roaring64_bitmap_contains(bitmap, 5), true);
  ASSERT_EQ(roaring64_bitmap_contains(bitmap, 20), false);

  ASSERT_EQ(roaring64_bitmap_minimum(bitmap), 3);
  ASSERT_EQ(roaring64_bitmap_maximum(bitmap), 7);

  roaring::api::roaring64_bitmap_t *flip_bitmap =
        roaring64_bitmap_flip_closed(bitmap, 3, 7);

  ASSERT_EQ(roaring64_bitmap_contains(flip_bitmap, 5), false);
  ASSERT_EQ(roaring64_bitmap_contains(flip_bitmap, 7), false);
  ASSERT_EQ(roaring64_bitmap_contains(flip_bitmap, 4), true);


  std::cout << roaring64_bitmap_contains(bitmap, 5) << std::endl;
}


TEST_F(TestVectorIndexAdaptor, result)
{
  int dim = 10;
  void* index = nullptr;
  std::mt19937 rng;
  rng.seed(47);
  //ASSERT_EQ(obvectorlib::init(), true);
  std::uniform_real_distribution<> distrib_real;
  ASSERT_EQ(obvectorlib::create_index(dim, dim, 200, 100, index), 0);

  float a[110] = {0.203846,0.205289,0.880265,0.824340,0.615737,0.496899,0.983632,0.865571,0.248373,0.542833,
  0.735541,0.670776,0.903237,0.447223,0.232028,0.659316,0.765661,0.226980,0.579658,0.933939,
  0.327936,0.048756,0.084670,0.389642,0.970982,0.370915,0.181664,0.940780,0.013905,0.628127,
  0.148869,0.878546,0.028024,0.326642,0.044912,0.144034,0.717580,0.442633,0.637534,0.633993,
  0.334970,0.857377,0.886132,0.668270,0.983913,0.418145,0.208459,0.190118,0.959676,0.796483,
  0.117582,0.302352,0.471198,0.248725,0.315868,0.717533,0.028496,0.710370,0.007130,0.710913,
  0.551185,0.231134,0.075354,0.230557,0.248149,0.383390,0.483179,0.238120,0.289662,0.970101,
  0.185221,0.315131,0.558301,0.543172,0.335010,0.556101,0.595842,0.168794,0.567442,0.062338,
  0.928764,0.254038,0.272721,0.648755,0.966464,0.200054,0.093298,0.901419,0.676738,0.122339,
  0.345999,0.254102,0.950869,0.275233,0.844568,0.215723,0.302821,0.563644,0.811224,0.175574,
  0.615558,0.613338,0.031494,0.114999,0.713017,0.792606,0.551865,0.990780,0.034867,0.062117};

  int64_t vids[11] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};

  ASSERT_EQ(obvectorlib::add_index(dim, a, vids, 11, index), 0);

  int k = 3;
  int64_t result_size = 0;
  const float* res_dist = new float[k];
  const int64_t* res_ids = new int64_t[k];
  std::map<int64_t, bool> myMap;
  std::shared_ptr<std::map<int64_t, bool>> myMapPtr = std::make_shared<std::map<int64_t, bool>>(myMap);

  float search[10] = {0.712338,0.603321,0.133444,0.428146,0.876387,0.763293,0.408760,0.765300,0.560072,0.900498};

  ASSERT_EQ(obvectorlib::knn_search1(dim, index, search, k, res_dist, res_ids, result_size, myMapPtr), 0);

  std::cout << "vid: " << res_ids[0] << " " << "dis: " << res_dist[0] << std::endl;

}
*/

/*
TEST_F(TestVectorIndexAdaptor, vsag_index)
{
  int dim = 16;
  void* index = nullptr;
  std::mt19937 rng;
  rng.seed(47);
  //ASSERT_EQ(obvectorlib::init(), true);
  std::uniform_real_distribution<> distrib_real;
  ASSERT_EQ(obvectorlib::create_index(dim, dim, 200, 100, index), 0);
  std::cout << "index: " << index << " " <<  (index == nullptr) << std::endl;
  ASSERT_EQ(index == nullptr, 0);

  int num_vectors = 10;
  float *dis = new float[num_vectors * dim];
  for (int cnt = 0; cnt < 10; cnt++) {
    for (int64_t i = 0; i < num_vectors * dim; ++i) {
      dis[i] = distrib_real(rng);
    }
    ASSERT_EQ(obvectorlib::add_index(dim, dis, num_vectors, index), 0);
    std::cout << "add_index " << cnt << " finish." << std::endl;
  }

  std::cout << "add_index finish" << std::endl;

  float *search = new float[dim];
  for (int64_t i = 0; i < dim; ++i) {
    search[i] = distrib_real(rng);
  }
  int k = 10;
  std::cout << "search info finish" << std::endl;
  const float* res_dist = new float[k];
  const int64_t* res_ids = new int64_t[k];
  int64_t result_size = 0;

  ASSERT_EQ(obvectorlib::knn_search(dim, index, search, k, res_dist, res_ids, result_size), 0);

  std::cout << "knn_search" << std::endl;
  for (int i = 0; i < result_size; i++) {
    std::cout << "vid: " << res_ids[i] << " " << "dis: " << res_dist[i] << std::endl;
  }

  roaring::api::roaring64_bitmap_t *bitmap = roaring::api::roaring64_bitmap_create();

  uint64_t *vid = new uint64_t[20];
  for (int i = 0; i < 20; i++) {
    vid[i] = i + 1;
  }
  std::cout << "build roaring bitmap" << std::endl;
  roaring::api::roaring64_bitmap_add_many(bitmap, 20, vid);
  std::map<int64_t, bool> myMap;

  std::cout << "get roaring bitmap iter" << std::endl;
  uint64_t *buf = new uint64_t[20];
  roaring::api::roaring64_iterator_t *roaring_iter = roaring64_iterator_create(bitmap);
  uint64_t ele_cnt = roaring64_iterator_read(roaring_iter, buf, 20);

  std::cout << "get roaring bitmap buf" << std::endl;
  for (int i = 0; i < ele_cnt; i++) {
    std::cout << buf[i] << " ";
    myMap[buf[i]] = false;
  }
  std::shared_ptr<std::map<int64_t, bool>> myMapPtr = std::make_shared<std::map<int64_t, bool>>(myMap);
  std::cout << std::endl << "before knn search 1" << std::endl;

  const float* res_dist1 = new float[k];
  const int64_t* res_ids1 = new int64_t[k];
  int64_t result_size1 = 0;
  ASSERT_EQ(obvectorlib::knn_search1(dim, index, search, k, res_dist1, res_ids1, result_size1, myMapPtr), 0);

  std::cout << "knn_search1 result" << std::endl;
  for (int i = 0; i < result_size; i++) {
    std::cout << "vid: " << res_ids1[i] << " " << "dis: " << res_dist1[i] << std::endl;
  }

}

using namespace oceanbase::share;
TEST_F(TestVectorIndexAdaptor, adapt)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObVectorIndexHNSWParam param;
  param.type_ = ObVectorIndexAlgorithmType::VIAT_HNSW;
  param.lib_ = ObVectorIndexAlgorithmLib::VIAL_VSAG;
  param.dim_ = 10;
  param.m_ = 10;
  param.ef_construction_ = 200;
  param.ef_search_ = 200;

  // serialize param
  int64_t param_len = param.get_serialize_size();
  char *param_buf = static_cast<char *>(allocator.alloc(sizeof(char) * param_len));
  int64_t pos = 0;
  ASSERT_EQ(param.serialize(param_buf, param_len, pos), 0);

  // init adaptor
  ObPluginVectorIndexAdaptor adaptor(&allocator);
  adaptor.init_incr_tablet();
  ASSERT_EQ(adaptor.init(ObString(param_len, param_buf)), 0);

  // adaptor param
  int64_t dim = 0;
  ASSERT_EQ(adaptor.get_dim(dim), 0);
  std::cout << "dim: " << dim << std::endl;

  // insert_rows
  ObStoreRow *rows = nullptr;
  int64_t row_cnt = 100;
  rows = static_cast<ObStoreRow *>(allocator.alloc(sizeof(ObStoreRow) * row_cnt));
  ASSERT_EQ(rows == nullptr, 0);
  std::cout << "create rows" << std::endl;

  std::mt19937 rng;
  rng.seed(88);
  std::uniform_real_distribution<> distrib_real;
  float a = distrib_real(rng);
  std::cout << "new float" << std::endl;
  uint64_t vid = 0;

  for (int i = 0; i < row_cnt; i++) {
    float *vector = static_cast<float *>(allocator.alloc(sizeof(float) * dim));
    ASSERT_EQ(vector == nullptr, 0);
    for (int j = 0; j < dim; j++) {
      vector[j] = distrib_real(rng);
      std::cout << vector[j] << " ";
    }
    std::cout << std::endl;
    char *vector_void = reinterpret_cast<char *>(vector);
    int64_t vector_str_len = 40;

    ObStoreRow *row = nullptr;
    ASSERT_EQ(OB_SUCCESS, malloc_store_row(allocator, 3, row, FLAT_ROW_STORE));
    row->row_val_.cells_[0].set_uint64(vid++);
    row->row_val_.cells_[2].set_string(ObVarcharType, vector_void, vector_str_len);
    rows[i] = *row;
  }
  std::cout << "build row finish." << std::endl;

  ASSERT_EQ(adaptor.insert_rows(rows, row_cnt), 0);

  float *search = new float[dim];
  std::map<int64_t, bool> myMap;
  int k = 5;
  for (int64_t i = 0; i < dim; ++i) {
    search[i] = distrib_real(rng);
  }

  const float* res_dist1 = new float[k];
  const int64_t* res_ids1 = new int64_t[k];
  int64_t result_size1 = 0;
  std::shared_ptr<std::map<int64_t, bool>> myMapPtr;
  uint64_t insert_min = roaring64_bitmap_minimum(adaptor.get_incr_ibitmap());
  uint64_t insert_max = roaring64_bitmap_maximum(adaptor.get_incr_ibitmap());
  roaring::api::roaring64_bitmap_t *insert_flip_bitmap =
        roaring64_bitmap_flip_closed(adaptor.get_incr_ibitmap(), insert_min, insert_max);
  ASSERT_EQ(ObPluginVectorIndexAdaptor::cast_roaringbitmap_to_stdmap(insert_flip_bitmap, myMapPtr), 0);
  ASSERT_EQ(obvectorlib::knn_search1(dim, adaptor.get_incr_index(), search, k, res_dist1, res_ids1, result_size1, myMapPtr), 0);

  std::cout << "knn_search1 result" << std::endl;
  for (int i = 0; i < result_size1; i++) {
    std::cout << "vid: " << res_ids1[i] << " " << "dis: " << res_dist1[i] << std::endl;
  }

}
*/

using namespace oceanbase::share;


TEST_F(TestVectorIndexAdaptor, vsag_add_duplicate)
{
  obvectorlib::VectorIndexPtr index_handler = nullptr;
  std::mt19937 rng;
  rng.seed(47);
  int dim = 128;
  int max_degree = 16;
  int ef_search = 200;
  int ef_construction = 100;
  const char* const METRIC_L2 = "l2";
  const char* const DATATYPE_FLOAT32 = "float32";

  ASSERT_EQ(obvectorutil::create_index(index_handler,
                                       obvectorlib::HNSW_TYPE,
                                       DATATYPE_FLOAT32,
                                       METRIC_L2,
                                       dim,
                                       max_degree,
                                       ef_construction,
                                       ef_search), 0);

  int num_vectors = 1000;

  int64_t *ids = new int64_t[num_vectors];
  float *vecs = new float[dim * num_vectors];

  std::uniform_real_distribution<> distrib_real;
  for (int64_t i = 0; i < num_vectors; ++i) {
    ids[i] = i;
  }

  for (int64_t i = 0; i < num_vectors * dim; ++i) {
    vecs[i] = distrib_real(rng);
  }

  for (int64_t i = 0; i < 10; ++i) {
    ASSERT_EQ(0, obvectorutil::add_index(index_handler, vecs + i * 100, ids + i * 100, dim, 100));
  }

  std::cout << "add duplicate vector" << std::endl;

  ASSERT_EQ(0, obvectorutil::add_index(index_handler, vecs, ids, dim, 100));

  int64_t index_size = 0;
  ASSERT_EQ(0, obvectorutil::get_index_number(index_handler, index_size));

  std::cout << "vecotr num: " << index_size << std::endl;

}

TEST_F(TestVectorIndexAdaptor, vsag_build_index)
{
  //ASSERT_EQ(obvectorutil::example(), 0);

  obvectorlib::VectorIndexPtr index_handler = nullptr;
  std::mt19937 rng;
  rng.seed(47);
  int dim = 128;
  int max_degree = 16;
  int ef_search = 200;
  int ef_construction = 100;
  const char* const METRIC_L2 = "l2";
  const char* const DATATYPE_FLOAT32 = "float32";

  ASSERT_EQ(obvectorutil::create_index(index_handler,
                                       obvectorlib::HNSW_TYPE,
                                       DATATYPE_FLOAT32,
                                       METRIC_L2,
                                       dim,
                                       max_degree,
                                       ef_construction,
                                       ef_search), 0);

  int num_vectors = 10000;

  int64_t *ids = new int64_t[num_vectors];
  float *vecs = new float[dim * num_vectors];

  std::uniform_real_distribution<> distrib_real;
  for (int64_t i = 0; i < num_vectors; ++i) {
    ids[i] = i;
  }

  for (int64_t i = 0; i < num_vectors * dim; ++i) {
    vecs[i] = distrib_real(rng);
  }

  ASSERT_EQ(0, obvectorutil::build_index(index_handler, vecs, ids, dim, num_vectors));

  int64_t index_size = 0;
  ASSERT_EQ(0, obvectorutil::get_index_number(index_handler, index_size));
  ASSERT_EQ(index_size, num_vectors);

  const float* result_dist0;
  const int64_t* result_ids0;
  const float* result_dist1;
  const int64_t* result_ids1;
  int64_t expect_cnt = 10;
  int64_t result_size = 0;
  roaring::api::roaring64_bitmap_t *bitmap = roaring::api::roaring64_bitmap_create();

  std::cout << "===================== Query Vec ================" << std::endl;
  float *query_vecs = new float[dim];
  for (int64_t i = 0; i < dim; ++i) {
    query_vecs[i] = distrib_real(rng);
    if (i == 0) {
      std::cout << "[" << vecs[i] << ", ";
    } else if (i == dim - 1) {
      std::cout << vecs[i] << "]" << std::endl;
    } else {
      std::cout << vecs[i] << ", ";
    }
  }

  std::cout << "===================== Query Result ================" << std::endl;

  ASSERT_EQ(0, obvectorutil::knn_search(index_handler,
                                       query_vecs,
                                       dim,
                                       expect_cnt,
                                       result_dist0,
                                       result_ids0,
                                       result_size,
                                       ef_search,
                                       bitmap));
  for (int64_t i = 0; i < result_size; ++i) {
    std::cout << i <<  " id: " << result_ids0[i] << " dis: " << result_dist0[i] << std::endl;
    for (int64_t j = 0; j < dim; ++j) {
      if (j == 0) {
        std::cout << "[" << vecs[i * dim + j] << ", ";
      } else if (j == dim - 1) {
        std::cout << vecs[i * dim + j] << "]" << std::endl;
      } else {
        std::cout << vecs[i * dim + j] << ", ";
      }
    }
  }

  std::cout << "===================== Query StdMap ================" << std::endl;

  roaring64_bitmap_add(bitmap, 7055);
  roaring64_bitmap_add(bitmap, 2030);
  ASSERT_EQ(0, obvectorutil::knn_search(index_handler,
                                       query_vecs,
                                       dim,
                                       expect_cnt,
                                       result_dist1,
                                       result_ids1,
                                       result_size,
                                       ef_search,
                                       bitmap));

  for (int64_t i = 0; i < result_size; ++i) {
    std::cout << i <<  " id: " << result_ids1[i] << " dis: " << result_dist1[i] << std::endl;
    for (int64_t j = 0; j < dim; ++j) {
      if (j == 0) {
        std::cout << "[" << vecs[i * dim + j] << ", ";
      } else if (j == dim - 1) {
        std::cout << vecs[i * dim + j] << "]" << std::endl;
      } else {
        std::cout << vecs[i * dim + j] << ", ";
      }
    }
  }
}

TEST_F(TestVectorIndexAdaptor, vsag_add_index)
{
  //ASSERT_EQ(obvectorutil::example(), 0);

  obvectorlib::VectorIndexPtr index_handler = nullptr;
  std::mt19937 rng;
  rng.seed(50);
  int dim = 128;
  int max_degree = 16;
  int ef_search = 200;
  int ef_construction = 100;
  const char* const METRIC_L2 = "l2";
  const char* const DATATYPE_FLOAT32 = "float32";

  ASSERT_EQ(obvectorutil::create_index(index_handler,
                                       obvectorlib::HNSW_TYPE,
                                       DATATYPE_FLOAT32,
                                       METRIC_L2,
                                       dim,
                                       max_degree,
                                       ef_construction,
                                       ef_search), 0);

  int num_vectors = 10000;

  int64_t *ids = new int64_t[num_vectors];
  float *vecs = new float[dim * num_vectors];

  std::uniform_real_distribution<> distrib_real;
  for (int64_t i = 0; i < num_vectors; ++i) {
    ids[i] = i;
  }

  for (int64_t i = 0; i < num_vectors * dim; ++i) {
    vecs[i] = distrib_real(rng);
  }

  for (int64_t i = 0; i < 100; ++i) {
    ASSERT_EQ(0, obvectorutil::add_index(index_handler, vecs + i * 100, ids + i * 100, dim, 100));
  }
  //ASSERT_EQ(0, obvectorutil::add_index(index_handler, vecs, ids, dim, num_vectors));

  int64_t index_size = 0;
  ASSERT_EQ(0, obvectorutil::get_index_number(index_handler, index_size));
  ASSERT_EQ(index_size, num_vectors);

  const float* result_dist0;
  const int64_t* result_ids0;
  const float* result_dist1;
  const int64_t* result_ids1;
  int64_t expect_cnt = 10;
  int64_t result_size = 0;
  roaring::api::roaring64_bitmap_t *bitmap = roaring::api::roaring64_bitmap_create();

  std::cout << "===================== Query Vec ================" << std::endl;
  float *query_vecs = new float[dim];
  for (int64_t i = 0; i < dim; ++i) {
    query_vecs[i] = distrib_real(rng);
    if (i == 0) {
      std::cout << "[" << vecs[i] << ", ";
    } else if (i == dim - 1) {
      std::cout << vecs[i] << "]" << std::endl;
    } else {
      std::cout << vecs[i] << ", ";
    }
  }

  std::cout << "===================== Query Result ================" << std::endl;

  ASSERT_EQ(0, obvectorutil::knn_search(index_handler,
                                       query_vecs,
                                       dim,
                                       expect_cnt,
                                       result_dist0,
                                       result_ids0,
                                       result_size,
                                       ef_search,
                                       bitmap));
  for (int64_t i = 0; i < result_size; ++i) {
    std::cout << i <<  " id: " << result_ids0[i] << " dis: " << result_dist0[i] << std::endl;
    for (int64_t j = 0; j < dim; ++j) {
      if (j == 0) {
        std::cout << "[" << vecs[i * dim + j] << ", ";
      } else if (j == dim - 1) {
        std::cout << vecs[i * dim + j] << "]" << std::endl;
      } else {
        std::cout << vecs[i * dim + j] << ", ";
      }
    }
  }

  std::cout << "===================== Query StdMap ================" << std::endl;

  roaring64_bitmap_add(bitmap, 545);
  roaring64_bitmap_add(bitmap, 3720);
  ASSERT_EQ(0, obvectorutil::knn_search(index_handler,
                                       query_vecs,
                                       dim,
                                       expect_cnt,
                                       result_dist1,
                                       result_ids1,
                                       result_size,
                                       ef_search,
                                       bitmap));

  for (int64_t i = 0; i < result_size; ++i) {
    std::cout << i <<  " id: " << result_ids1[i] << " dis: " << result_dist1[i] << std::endl;
    for (int64_t j = 0; j < dim; ++j) {
      if (j == 0) {
        std::cout << "[" << vecs[i * dim + j] << ", ";
      } else if (j == dim - 1) {
        std::cout << vecs[i * dim + j] << "]" << std::endl;
      } else {
        std::cout << vecs[i * dim + j] << ", ";
      }
    }
  }
}

TEST_F(TestVectorIndexAdaptor, test_insert)
{
  //ASSERT_EQ(obvectorutil::example(), 0);

  obvectorlib::VectorIndexPtr index_handler = nullptr;
  std::mt19937 rng;
  rng.seed(50);
  int dim = 3;
  int max_degree = 16;
  int ef_search = 200;
  int ef_construction = 100;
  const char* const METRIC_L2 = "l2";
  const char* const DATATYPE_FLOAT32 = "float32";

  ASSERT_EQ(obvectorutil::create_index(index_handler,
                                       obvectorlib::HNSW_TYPE,
                                       DATATYPE_FLOAT32,
                                       METRIC_L2,
                                       dim,
                                       max_degree,
                                       ef_construction,
                                       ef_search), 0);

  int num_vectors = 40;

  int64_t *ids = new int64_t[num_vectors];
  float *vecs = new float[dim * num_vectors];

  std::uniform_real_distribution<> distrib_real;
  for (int64_t i = 0; i < num_vectors; ++i) {
    ids[i] = i;
  }

  for (int64_t i = 0; i < num_vectors * dim; ++i) {
    vecs[i] = distrib_real(rng);
  }

  // for (int64_t i = 0; i < 100; ++i) {
  //   ASSERT_EQ(0, obvectorutil::add_index(index_handler, vecs + i * 100, ids + i * 100, dim, 100));
  // }
  ASSERT_EQ(0, obvectorutil::add_index(index_handler, vecs, ids, dim, num_vectors));

  int64_t index_size = 0;
  ASSERT_EQ(0, obvectorutil::get_index_number(index_handler, index_size));
  ASSERT_EQ(index_size, num_vectors);

  const float* result_dist0;
  const int64_t* result_ids0;
  const float* result_dist1;
  const int64_t* result_ids1;
  int64_t expect_cnt = 10;
  int64_t result_size = 0;
  roaring::api::roaring64_bitmap_t *bitmap = roaring::api::roaring64_bitmap_create();

  std::cout << "===================== Query Vec ================" << std::endl;
  float *query_vecs = new float[dim];
  for (int64_t i = 0; i < dim; ++i) {
    query_vecs[i] = distrib_real(rng);
    if (i == 0) {
      std::cout << "[" << vecs[i] << ", ";
    } else if (i == dim - 1) {
      std::cout << vecs[i] << "]" << std::endl;
    } else {
      std::cout << vecs[i] << ", ";
    }
  }

  std::cout << "===================== Query Result ================" << std::endl;

  ASSERT_EQ(0, obvectorutil::knn_search(index_handler,
                                       query_vecs,
                                       dim,
                                       expect_cnt,
                                       result_dist0,
                                       result_ids0,
                                       result_size,
                                       ef_search,
                                       bitmap));
  for (int64_t i = 0; i < result_size; ++i) {
    std::cout << i <<  " id: " << result_ids0[i] << " dis: " << result_dist0[i] << std::endl;
    for (int64_t j = 0; j < dim; ++j) {
      if (j == 0) {
        std::cout << "[" << vecs[i * dim + j] << ", ";
      } else if (j == dim - 1) {
        std::cout << vecs[i * dim + j] << "]" << std::endl;
      } else {
        std::cout << vecs[i * dim + j] << ", ";
      }
    }
  }

  std::cout << "===================== Query StdMap ================" << std::endl;

  roaring64_bitmap_add(bitmap, 3720);
    roaring64_bitmap_add(bitmap, 545);
  ASSERT_EQ(0, obvectorutil::knn_search(index_handler,
                                       query_vecs,
                                       dim,
                                       expect_cnt,
                                       result_dist1,
                                       result_ids1,
                                       result_size,
                                       ef_search,
                                       bitmap));

  for (int64_t i = 0; i < result_size; ++i) {
    std::cout << i <<  " id: " << result_ids1[i] << " dis: " << result_dist1[i] << std::endl;
    for (int64_t j = 0; j < dim; ++j) {
      if (j == 0) {
        std::cout << "[" << vecs[i * dim + j] << ", ";
      } else if (j == dim - 1) {
        std::cout << vecs[i * dim + j] << "]" << std::endl;
      } else {
        std::cout << vecs[i * dim + j] << ", ";
      }
    }
  }
}

class ObTestHNSWSerializeCallback {
public:
  struct CbParam : public ObOStreamBuf::CbParam {
    CbParam()
      : allocator_(nullptr), data_(nullptr), size_(0)
    {}
    virtual ~CbParam() {}
    bool is_valid() const
    {
      return nullptr != allocator_;
    }
    ObIAllocator *allocator_;
    void *data_;
    int64_t size_;
  };
public:
  ObTestHNSWSerializeCallback()
  {}
  int operator()(const char *data, const int64_t data_size, share::ObOStreamBuf::CbParam &cb_param)
  {
    int ret = OB_SUCCESS;
    ObTestHNSWSerializeCallback::CbParam &param = static_cast<ObTestHNSWSerializeCallback::CbParam&>(cb_param);
    char *buf = (char*)param.allocator_->alloc(data_size + param.size_);
    if (OB_ISNULL(buf)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      MEMCPY(buf, param.data_, param.size_);
      MEMCPY(buf + param.size_, data, data_size);
      param.data_ = buf;
      param.size_ += data_size;
    }
    return ret;
  }
private:
};

class ObTestHNSWDeserializeCallback {
public:
  struct CbParam : public ObIStreamBuf::CbParam {
    CbParam()
      : allocator_(nullptr), data_(nullptr), size_(0), cur_pos_(0), part_size_(0)
    {}
    virtual ~CbParam() {}
    bool is_valid() const
    {
      return nullptr != data_
             && nullptr != allocator_;
    }
    ObIAllocator *allocator_;
    void *data_;
    int64_t size_;
    int64_t cur_pos_;
    int64_t part_size_;
  };
public:
  ObTestHNSWDeserializeCallback()
  {}
  int operator()(char *&data, const int64_t data_size, int64_t &read_size, share::ObIStreamBuf::CbParam &cb_param)
  {
    int ret = OB_SUCCESS;
    ObTestHNSWDeserializeCallback::CbParam &param = static_cast<ObTestHNSWDeserializeCallback::CbParam&>(cb_param);
    if (param.cur_pos_ <= param.size_) {
      read_size = (param.size_ - param.cur_pos_) > param.part_size_ ? param.part_size_ : (param.size_ - param.cur_pos_);
      data = ((char*)param.data_) + param.cur_pos_;
      param.cur_pos_ += read_size;
    } else {
      ret = OB_ITER_END;
    }
    LOG_INFO("[Vsag] get des data", K(ret), K(data), K(read_size), K(param.size_), K(param.cur_pos_), K(param.part_size_));
    return ret;
  }
private:
};

TEST_F(TestVectorIndexAdaptor, test_ser_deser)
{
  void* raw_memory = (void*)malloc(sizeof(common::obvectorutil::ObVsagLogger));
  common::obvectorutil::ObVsagLogger* ob_logger = new (raw_memory)common::obvectorutil::ObVsagLogger();
  obvectorlib::set_logger(ob_logger);
  obvectorlib::VectorIndexPtr index_handler = nullptr;
  std::mt19937 rng;
  rng.seed(50);
  int dim = 3;
  int max_degree = 16;
  int ef_search = 200;
  int ef_construction = 100;
  const char* const METRIC_L2 = "l2";
  const char* const DATATYPE_FLOAT32 = "float32";
  ASSERT_EQ(obvectorutil::create_index(index_handler,
                                       obvectorlib::HNSW_TYPE,
                                       DATATYPE_FLOAT32,
                                       METRIC_L2,
                                       dim,
                                       max_degree,
                                       ef_construction,
                                       ef_search), 0);
  int num_vectors = 40;

  int64_t *ids = new int64_t[num_vectors];
  float *vecs = new float[dim * num_vectors];

  std::uniform_real_distribution<> distrib_real;
  for (int64_t i = 0; i < num_vectors; ++i) {
    ids[i] = i;
  }

  for (int64_t i = 0; i < num_vectors * dim; ++i) {
    vecs[i] = distrib_real(rng);
  }

  // for (int64_t i = 0; i < 100; ++i) {
  //   ASSERT_EQ(0, obvectorutil::add_index(index_handler, vecs + i * 100, ids + i * 100, dim, 100));
  // }
  ASSERT_EQ(0, obvectorutil::add_index(index_handler, vecs, ids, dim, num_vectors));

  int64_t index_size = 0;
  ASSERT_EQ(0, obvectorutil::get_index_number(index_handler, index_size));
  ASSERT_EQ(index_size, num_vectors);

  // do serialize
  ObArenaAllocator allocator;
  ObVectorIndexSerializer index_seri(allocator);
  ObTestHNSWSerializeCallback ser_callback;
  ObOStreamBuf::Callback ser_cb = ser_callback;

  ObTestHNSWSerializeCallback::CbParam ser_param;
  ser_param.allocator_ = &allocator;
  ASSERT_EQ(0, index_seri.serialize(index_handler, ser_param, ser_cb));

  // do deserialize
  obvectorlib::VectorIndexPtr des_index_handler = nullptr;
  ObTestHNSWDeserializeCallback des_callback;
  ObIStreamBuf::Callback des_cb = des_callback;

  ObTestHNSWDeserializeCallback::CbParam des_param;
  des_param.allocator_ = &allocator;
  des_param.data_ = ser_param.data_;
  des_param.size_ = ser_param.size_;
  des_param.cur_pos_ = 0;
  des_param.part_size_ = 10;
  ASSERT_EQ(obvectorutil::create_index(des_index_handler,
                                      obvectorlib::HNSW_TYPE,
                                      DATATYPE_FLOAT32,
                                      METRIC_L2,
                                      dim,
                                      max_degree,
                                      ef_construction,
                                      ef_search), 0);
  ASSERT_EQ(0, index_seri.deserialize(des_index_handler, des_param, des_cb));
  // check vector count
  ASSERT_EQ(0, obvectorutil::get_index_number(des_index_handler, index_size));
  ASSERT_EQ(index_size, num_vectors);
}

class VecIndexAlloctor : public ObIAllocator
{
public:
  VecIndexAlloctor();
  ~VecIndexAlloctor();

  virtual void *alloc(const int64_t sz);
  void *alloc(const int64_t size, const ObMemAttr &attr);
  virtual void *realloc(void *ptr, const int64_t oldsz, const int64_t newsz);
  virtual void free(void *ptr);
  int64_t used();
  int64_t total();
  int64_t global_total();
  constexpr static int64_t MEM_PTR_HEAD_SIZE = sizeof(int64_t);

private:
  static std::atomic<int64_t> global_total_;
  ObMemAttr attr_;
  std::atomic<int64_t> used_;
  int64_t total_;
};

class ObVsagAllocator : public vsag::Allocator
{
  void* Allocate(size_t size) override {
    return malloc(size);
  }

  void Deallocate(void* p) override {
    return free(p);
  }

  void* Reallocate(void* p, size_t size) override {
    return realloc(p, size);
  }

};

std::atomic<int64_t>VecIndexAlloctor::global_total_(0);

void *VecIndexAlloctor::alloc(const int64_t size)
{
  int64_t actual_size = MEM_PTR_HEAD_SIZE + size;
  global_total_ += actual_size;
  used_ += actual_size;

  void *ptr = nullptr;
  ptr = ob_malloc(actual_size, attr_);
  *(int64_t*)ptr = actual_size;
  return (char*)ptr + MEM_PTR_HEAD_SIZE;
}

void VecIndexAlloctor::free(void *ptr)
{
  void *size_ptr = (char*)ptr - sizeof(uint64_t);
  int64_t size = *(int64_t *)size_ptr;
  global_total_ -= size;
  used_ -= size;

  ob_free((char*)ptr - MEM_PTR_HEAD_SIZE);
}


void *VecIndexAlloctor::realloc(void *ptr, const int64_t oldsz, const int64_t newsz)
{
  void *new_ptr = nullptr;
  if (newsz < oldsz) {
    return new_ptr;
  } else {
    int64_t actual_size = MEM_PTR_HEAD_SIZE + newsz;
    global_total_ += newsz - oldsz + MEM_PTR_HEAD_SIZE;
    used_ += newsz - oldsz + MEM_PTR_HEAD_SIZE;

    new_ptr = ob_realloc((char*)ptr - MEM_PTR_HEAD_SIZE, actual_size, attr_);
    *(char*)new_ptr = actual_size;
  }

  return (char*)new_ptr + MEM_PTR_HEAD_SIZE;
}

class ExampleAllocator : public vsag::Allocator {
public:
  std::string
  Name() override {
    return "myallocator";
  }

  void* Allocate(size_t size) override {
    return malloc(size);
  }

  void Deallocate(void* p) override {
    return free(p);
  }

  void* Reallocate(void* p, size_t size) override {
    return realloc(p, size);
  }
};

class VsagMemContext : public vsag::Allocator
{
public:
  VsagMemContext() {};
  ~VsagMemContext() { DESTROY_CONTEXT(mem_context_); }
  int init();

  std::string Name() override {
    return "ObVsagAlloc";
  }
  void* Allocate(size_t size) override {
    return mem_context_->get_malloc_allocator().alloc(size);
  }

  void Deallocate(void* p) override {
    return mem_context_->get_malloc_allocator().free(p);
  }

  void* Reallocate(void* p, size_t size) override {
    void *new_ptr = nullptr;
    if (size == 0) {
      if (OB_NOT_NULL(p)) {
        mem_context_->get_malloc_allocator().free(p);
      }
    } else {
      new_ptr = mem_context_->get_malloc_allocator().alloc(size);
      if (OB_ISNULL(new_ptr) || OB_ISNULL(p)) {
      } else {
        MEMCPY(new_ptr, p, size);
        mem_context_->get_malloc_allocator().free(p);
      }
    }
    return new_ptr;
  }

  int64_t total() {
    return mem_context_->malloc_used();
  }

private:
  lib::MemoryContext mem_context_;

};

int VsagMemContext::init()
{
  INIT_SUCC(ret);
  lib::ContextParam param;
  param.set_mem_attr(MTL_ID())
    .set_properties(lib::ALLOC_THREAD_SAFE | lib::RETURN_MALLOC_DEFAULT)
    .set_page_size(OB_MALLOC_MIDDLE_BLOCK_SIZE)
    .set_ablock_size(lib::INTACT_MIDDLE_AOBJECT_SIZE);
  if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
    //LOG_WARN("create memory entity failed", K(ret));
  } else {

  }
  return ret;
}

TEST_F(TestVectorIndexAdaptor, mem_ctx_child)
{
  lib::ContextParam parent_param;
  lib::MemoryContext parent_mem_context;
  parent_param.set_mem_attr(MTL_ID())
    .set_properties(lib::ADD_CHILD_THREAD_SAFE | lib::ALLOC_THREAD_SAFE | lib::RETURN_MALLOC_DEFAULT)
    .set_page_size(OB_MALLOC_MIDDLE_BLOCK_SIZE)
    .set_ablock_size(lib::INTACT_MIDDLE_AOBJECT_SIZE);
  ASSERT_EQ(ROOT_CONTEXT->CREATE_CONTEXT(parent_mem_context, parent_param), 0);

  lib::ContextParam param;
  lib::MemoryContext mem_context;
  param.set_page_size(OB_MALLOC_MIDDLE_BLOCK_SIZE);
  ASSERT_EQ(parent_mem_context->CREATE_CONTEXT(mem_context, param), 0);

  int64_t *vid = nullptr;
  vid = static_cast<int64_t *>(mem_context->get_malloc_allocator().alloc(sizeof(int64_t) * 1000));
  ASSERT_EQ(vid != NULL, 1);

  std::cout << "Alloc MemoryContext: " << mem_context->used() << std::endl;
  std::cout << "Alloc MemoryContext: " << mem_context->hold() << std::endl;
  std::cout << "Alloc MemoryContext: " << parent_mem_context->used() << std::endl;
  std::cout << "Alloc MemoryContext: " << parent_mem_context->hold() << std::endl;
}

TEST_F(TestVectorIndexAdaptor, mem_ctx)
{
  VsagMemContext vsag_mem_context;
  ASSERT_EQ(vsag_mem_context.init(), 0);
  std::cout << "INIT MemoryContext: " << vsag_mem_context.total() << std::endl;
  int64_t *vid = nullptr;
  vid = static_cast<int64_t *>(vsag_mem_context.Allocate(sizeof(int64_t) * 1000));
  std::cout << "Alloc MemoryContext: " << vsag_mem_context.total() << std::endl;
  vsag_mem_context.Deallocate(vid);
  vid = nullptr;
  std::cout << "Free MemoryContext: " << vsag_mem_context.total() << std::endl;

  vid = static_cast<int64_t *>(vsag_mem_context.Allocate(sizeof(int64_t) * 1000));
  std::cout << "Alloc MemoryContext: " << vsag_mem_context.total() << std::endl;

  vid = static_cast<int64_t *>(vsag_mem_context.Reallocate(vid, sizeof(int64_t) * 2000));
  std::cout << "Realloc MemoryContext: " << vsag_mem_context.total() << std::endl;

  vsag_mem_context.Deallocate(vid);
  vid = nullptr;
  std::cout << "Free MemoryContext: " << vsag_mem_context.total() << std::endl;

}

#if 0
TEST_F(TestVectorIndexAdaptor, vsag_alloc)
{
  VsagMemContext vsag_mem_context;
  ASSERT_EQ(vsag_mem_context.init(), 0);
  obvectorlib::VectorIndexPtr index_handler = nullptr;
  std::mt19937 rng;
  rng.seed(50);
  int dim = 128;
  int max_degree = 16;
  int ef_search = 200;
  int ef_construction = 100;
  const char* const METRIC_L2 = "l2";
  const char* const DATATYPE_FLOAT32 = "float32";
  std::cout << "MemoryUsed0: " << vsag_mem_context.total() << std::endl;
  ASSERT_EQ(obvectorutil::create_index(index_handler,
                                       obvectorlib::HNSW_TYPE,
                                       DATATYPE_FLOAT32,
                                       METRIC_L2,
                                       dim,
                                       max_degree,
                                       ef_construction,
                                       ef_search,
                                       &vsag_mem_context), 0);
  std::cout << "MemoryUsed1: " << vsag_mem_context.total() << std::endl;
  int num_vectors = 100;

  int64_t *ids = new int64_t[num_vectors];
  float *vecs = new float[dim * num_vectors];

  std::uniform_real_distribution<> distrib_real;
  for (int64_t i = 0; i < num_vectors; ++i) {
    ids[i] = i;
  }

  for (int64_t i = 0; i < num_vectors * dim; ++i) {
    vecs[i] = distrib_real(rng);
  }

  ASSERT_EQ(0, obvectorutil::add_index(index_handler, vecs, ids, dim, num_vectors));
  std::cout << "MemoryUsed2: " << vsag_mem_context.total() << std::endl;

  int64_t index_size = 0;
  ASSERT_EQ(0, obvectorutil::get_index_number(index_handler, index_size));
  ASSERT_EQ(index_size, num_vectors);

  const float* result_dist0;
  const int64_t* result_ids0;
  const float* result_dist1;
  const int64_t* result_ids1;
  int64_t expect_cnt = 10;
  int64_t result_size = 0;
  roaring::api::roaring64_bitmap_t *bitmap = roaring::api::roaring64_bitmap_create();

  float *query_vecs = new float[dim];
  for (int64_t i = 0; i < dim; ++i) {
    query_vecs[i] = distrib_real(rng);
    if (i == 0) {
      std::cout << "[" << vecs[i] << ", ";
    } else if (i == dim - 1) {
      std::cout << vecs[i] << "]" << std::endl;
    } else {
      std::cout << vecs[i] << ", ";
    }
  }

  ASSERT_EQ(0, obvectorutil::knn_search(index_handler,
                                       query_vecs,
                                       dim,
                                       expect_cnt,
                                       result_dist0,
                                       result_ids0,
                                       result_size,
                                       ef_search,
                                       bitmap));
  for (int64_t i = 0; i < result_size; ++i) {
    std::cout << i <<  " id: " << result_ids0[i] << " dis: " << result_dist0[i] << std::endl;
    // for (int64_t j = 0; j < dim; ++j) {
    //   if (j == 0) {
    //     std::cout << "[" << vecs[i * dim + j] << ", ";
    //   } else if (j == dim - 1) {
    //     std::cout << vecs[i * dim + j] << "]" << std::endl;
    //   } else {
    //     std::cout << vecs[i * dim + j] << ", ";
    //   }
    // }
  }
  std::cout << "MemoryUsed3: " << vsag_mem_context.total() << std::endl;

  obvectorutil::delete_index(index_handler);
  std::cout << "MemoryUsed: " << vsag_mem_context.total() << std::endl;
}
#endif

};

int main(int argc, char** argv)
{
  system("rm -f test_vector_index_adaptor.log*");
  system("rm -fr run_*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_vector_index_adaptor.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
