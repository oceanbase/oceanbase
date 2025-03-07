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

#include "storage/ob_i_store.h"
#include "mtlenv/mock_tenant_module_env.h"


#undef private
#undef protected
#include <ctime>

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
  ASSERT_EQ(0, index_seri.serialize(index_handler, ser_param, ser_cb, MTL_ID()));

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
  ASSERT_EQ(0, index_seri.deserialize(des_index_handler, des_param, des_cb, MTL_ID()));
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

TEST_F(TestVectorIndexAdaptor, vsag_build_hnswsq_index)
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
                                       obvectorlib::HNSW_SQ_TYPE,
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

TEST_F(TestVectorIndexAdaptor, vsag_build_hnswsq_index_query)
{
  obvectorlib::VectorIndexPtr index_handler_sq = nullptr;
  obvectorlib::VectorIndexPtr index_handler = nullptr;
  std::mt19937 rng;
  rng.seed(47);
  int dim = 128;
  int max_degree = 16;
  int ef_search = 200;
  int ef_construction = 100;
  const char* const METRIC_L2 = "l2";
  const char* const DATATYPE_FLOAT32 = "float32";

  ASSERT_EQ(obvectorutil::create_index(index_handler_sq,
                                       obvectorlib::HNSW_SQ_TYPE,
                                       DATATYPE_FLOAT32,
                                       METRIC_L2,
                                       dim,
                                       max_degree,
                                       ef_construction,
                                       ef_search), 0);

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

  ASSERT_EQ(0, obvectorutil::build_index(index_handler_sq, vecs, ids, dim, num_vectors));
  ASSERT_EQ(0, obvectorutil::build_index(index_handler, vecs, ids, dim, num_vectors));

  int64_t index_size = 0;
  ASSERT_EQ(0, obvectorutil::get_index_number(index_handler_sq, index_size));
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

  ASSERT_EQ(0, obvectorutil::knn_search(index_handler_sq,
                                       query_vecs,
                                       dim,
                                       expect_cnt,
                                       result_dist0,
                                       result_ids0,
                                       result_size,
                                       ef_search,
                                       bitmap));
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
    std::cout << i <<  " id: " << result_ids0[i] << " dis: " << result_dist0[i] << std::endl;
    std::cout << i <<  " id: " << result_ids1[i] << " dis: " << result_dist1[i] << std::endl;
  }

}

/*  too much to execute all
TEST_F(TestVectorIndexAdaptor, test_for_hnsw_insert_time)
{
  obvectorlib::VectorIndexPtr index_handler = nullptr;
  std::mt19937 rng;
  rng.seed(47);
  int dim = 128;
  int max_degree = 16;
  int ef_search = 200;
  int ef_construction = 200;
  const char* const METRIC_L2 = "l2";
  const char* const DATATYPE_FLOAT32 = "float32";
  int num_vectors = 100000;
  int batch_cnt = 1000;

  int64_t *ids = new int64_t[num_vectors];
  float *vecs = new float[dim * num_vectors];

  std::uniform_real_distribution<> distrib_real;
  for (int64_t i = 0; i < num_vectors; ++i) {
    ids[i] = i;
  }

  for (int64_t i = 0; i < num_vectors * dim; ++i) {
    vecs[i] = distrib_real(rng);
  }

  std::time_t start_timestamp = std::time(nullptr);
  ASSERT_EQ(obvectorutil::create_index(index_handler,
                                       obvectorlib::HNSW_TYPE,
                                       DATATYPE_FLOAT32,
                                       METRIC_L2,
                                       dim,
                                       max_degree,
                                       ef_construction,
                                       ef_search), 0);


  ASSERT_EQ(0, obvectorutil::build_index(index_handler, vecs, ids, dim, batch_cnt));

  for (int64_t i = batch_cnt; i < num_vectors; i++) {
    ASSERT_EQ(0, obvectorutil::add_index(index_handler, vecs + dim * i, ids + i, dim, 1));
  }
  std::time_t end_timestamp = std::time(nullptr);
  std::cout << "hnsw cos: " << end_timestamp - start_timestamp << std::endl;
}

TEST_F(TestVectorIndexAdaptor, test_for_hnswsq_insert_time)
{
  obvectorlib::VectorIndexPtr index_handler = nullptr;
  std::mt19937 rng;
  rng.seed(47);
  int dim = 128;
  int max_degree = 32;
  int ef_search = 200;
  int ef_construction = 200;
  const char* const METRIC_L2 = "l2";
  const char* const DATATYPE_FLOAT32 = "float32";
  int num_vectors = 100000;
  int batch_cnt = 1000;

  int64_t *ids = new int64_t[num_vectors];
  float *vecs = new float[dim * num_vectors];

  std::uniform_real_distribution<> distrib_real;
  for (int64_t i = 0; i < num_vectors; ++i) {
    ids[i] = i;
  }

  for (int64_t i = 0; i < num_vectors * dim; ++i) {
    vecs[i] = distrib_real(rng);
  }

  std::time_t start_timestamp = std::time(nullptr);
  ASSERT_EQ(obvectorutil::create_index(index_handler,
                                       obvectorlib::HNSW_SQ_TYPE,
                                       DATATYPE_FLOAT32,
                                       METRIC_L2,
                                       dim,
                                       max_degree,
                                       ef_construction,
                                       ef_search), 0);


  ASSERT_EQ(0, obvectorutil::build_index(index_handler, vecs, ids, dim, batch_cnt));

  for (int64_t i = batch_cnt; i < num_vectors; i++) {
    ASSERT_EQ(0, obvectorutil::add_index(index_handler, vecs + dim * i, ids + i, dim, 1));
  }
  std::time_t end_timestamp = std::time(nullptr);
  std::cout << "hnswsq cos: " << end_timestamp - start_timestamp << std::endl;
}

*/

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
