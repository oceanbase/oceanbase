
#include "ob_vsag_lib.h"
#include "ob_vsag_lib_c.h"
#include "nlohmann/json.hpp"
#include "roaring/roaring64.h"
#include <vsag/vsag.h>
#include "vsag/errors.h"
#include "vsag/dataset.h"
#include "vsag/bitset.h"
#include "vsag/allocator.h"
#include "vsag/factory.h"
#include "vsag/constants.h"

#include "default_logger.h"
#include "vsag/logger.h"

#include <fstream>
#include <chrono>

namespace obvectorlib {

struct SlowTaskTimer {
    SlowTaskTimer(const std::string& name, int64_t log_threshold_ms = 0);
    ~SlowTaskTimer();

    std::string name;
    int64_t threshold;
    std::chrono::steady_clock::time_point start;
};

SlowTaskTimer::SlowTaskTimer(const std::string& n, int64_t log_threshold_ms)
    : name(n), threshold(log_threshold_ms) {
    start = std::chrono::steady_clock::now();
}

SlowTaskTimer::~SlowTaskTimer() {
    auto finish = std::chrono::steady_clock::now();
    std::chrono::duration<double, std::milli> duration = finish - start;
    if (duration.count() > threshold) {
        if (duration.count() >= 1000) {
            vsag::logger::debug("  {0} cost {1:.3f}s", name, duration.count() / 1000);
        } else {
            vsag::logger::debug("  {0} cost {1:.3f}ms", name, duration.count());
        }
    }
}

class HnswIndexHandler
{
public:
  HnswIndexHandler() = delete;

  HnswIndexHandler(bool is_create, bool is_build, bool use_static,
                   int max_degree, int ef_construction, int ef_search, int dim,
                   std::shared_ptr<vsag::Index> index, vsag::Allocator* allocator):
      is_created_(is_create),
      is_build_(is_build),
      use_static_(use_static),
      max_degree_(max_degree),
      ef_construction_(ef_construction),
      ef_search_(ef_search),
      dim_(dim),
      index_(index),
      allocator_(allocator)
  {}

  ~HnswIndexHandler() {
    index_ = nullptr;
    vsag::logger::debug("   after deconstruction, hnsw index addr {} : use count {}", (void*)allocator_, index_.use_count());
  }
  void set_build(bool is_build) { is_build_ = is_build;}
  bool is_build(bool is_build) { return is_build_;}
  int build_index(const vsag::DatasetPtr& base);
  int get_index_number();
  int add_index(const vsag::DatasetPtr& incremental);
  int knn_search(const vsag::DatasetPtr& query, int64_t topk,
                const std::string& parameters,
                const float*& dist, const int64_t*& ids, int64_t &result_size,
                const std::function<bool(int64_t)>& filter);
  std::shared_ptr<vsag::Index>& get_index() {return index_;}
  void set_index(std::shared_ptr<vsag::Index> hnsw) {index_ = hnsw;}
  vsag::Allocator* get_allocator() {return allocator_;}
  inline bool get_use_static() {return use_static_;}
  inline int get_max_degree() {return max_degree_;}
  inline int get_ef_construction() {return ef_construction_;}
  inline int get_ef_search() {return ef_search_;}
  inline int get_dim() {return dim_;}
  
private:
  bool is_created_;
  bool is_build_;
  bool use_static_;
  int max_degree_;
  int ef_construction_;
  int ef_search_;
  int dim_;
  std::shared_ptr<vsag::Index> index_;
  vsag::Allocator* allocator_;
};

int HnswIndexHandler::build_index(const vsag::DatasetPtr& base) 
{
    vsag::ErrorType error = vsag::ErrorType::UNKNOWN_ERROR;
    if (const auto num = index_->Build(base); num.has_value()) {
        return 0;
    } else {
        error = num.error().type;
    }
    return static_cast<int>(error);
}

int HnswIndexHandler::get_index_number() 
{
    return index_->GetNumElements();
}

int HnswIndexHandler::add_index(const vsag::DatasetPtr& incremental) 
{
    vsag::ErrorType error = vsag::ErrorType::UNKNOWN_ERROR;
    if (const auto num = index_->Add(incremental); num.has_value()) {
        vsag::logger::debug(" after add index, index count {}", get_index_number());
        return 0;
    } else {
        error = num.error().type;
    }
    return static_cast<int>(error);
}

int HnswIndexHandler::knn_search(const vsag::DatasetPtr& query, int64_t topk,
               const std::string& parameters,
               const float*& dist, const int64_t*& ids, int64_t &result_size,
               const std::function<bool(int64_t)>& filter) {
    vsag::logger::debug("  search_parameters:{}", parameters);
    vsag::logger::debug("  topk:{}", topk);
    vsag::ErrorType error = vsag::ErrorType::UNKNOWN_ERROR;

    auto result = index_->KnnSearch(query, topk, parameters, filter);
    if (result.has_value()) {
        //result的生命周期
        result.value()->Owner(false);
        ids = result.value()->GetIds();
        dist = result.value()->GetDistances();
        result_size = result.value()->GetDim();
        // print the results
        for (int64_t i = 0; i < result_size; ++i) {
            vsag::logger::debug("  knn search id : {}, distance : {}",ids[i],dist[i]);
        }
        return 0; 
    } else {
        error = result.error().type;
    }

    return static_cast<int>(error);
}

bool is_init_ = vsag::init();

void
set_log_level(int64_t level_num) {
    vsag::Logger::Level log_level = static_cast<vsag::Logger::Level>(level_num);
    vsag::Options::Instance().logger()->SetLevel(log_level);
}

bool is_init() {
    vsag::logger::debug("TRACE LOG[Init VsagLib]:");
    if (is_init_) {
        vsag::logger::debug("   Init VsagLib success");
    } else {
        vsag::logger::debug("   Init VsagLib fail");
    }
    return is_init_; 
}


void set_logger(void *logger_ptr) {
    vsag::Options::Instance().set_logger(static_cast<vsag::Logger*>(logger_ptr));
    vsag::Logger::Level log_level = static_cast<vsag::Logger::Level>(1);//default is debug level
    vsag::Options::Instance().logger()->SetLevel(log_level);
}

void set_block_size_limit(uint64_t size) {
    vsag::Options::Instance().set_block_size_limit(size);
}

bool is_supported_index(IndexType index_type) {
    return INVALID_INDEX_TYPE < index_type && index_type < MAX_INDEX_TYPE;
}

int create_index(VectorIndexPtr& index_handler, IndexType index_type,
                 const char* dtype,
                 const char* metric, int dim,
                 int max_degree, int ef_construction, int ef_search, void* allocator)
{   
    vsag::logger::debug("TRACE LOG[test_create_index]:");
    vsag::ErrorType error = vsag::ErrorType::UNKNOWN_ERROR;
    int ret = 0;
    if (dtype == nullptr || metric == nullptr) {
        vsag::logger::debug("   null pointer addr, dtype:{}, metric:{}", (void*)dtype, (void*)metric);
        return static_cast<int>(error);
    }
    SlowTaskTimer t("create_index");
    vsag::Allocator* vsag_allocator = NULL;
    bool is_support = is_supported_index(index_type);
    vsag::logger::debug("   index type : {}, is_supported : {}", static_cast<int>(index_type), is_support);
    if (allocator == NULL) {
        vsag_allocator = NULL;
        vsag::logger::debug("   allocator is null ,use default_allocator");
    } else {
        vsag_allocator =  static_cast<vsag::Allocator*>(allocator);
        vsag::logger::debug("   allocator_addr:{}",allocator);
    }

    if (is_support) {
        // create index
        std::shared_ptr<vsag::Index> hnsw;
        bool use_static = false;
        nlohmann::json hnsw_parameters{{"max_degree", max_degree},
                                {"ef_construction", ef_construction},
                                {"ef_search", ef_search},
                                {"use_static", use_static}};
        nlohmann::json index_parameters{{"dtype", dtype}, {"metric_type", metric}, {"dim", dim}, {"hnsw", hnsw_parameters}}; 
        if (auto index = vsag::Factory::CreateIndex("hnsw", index_parameters.dump(), vsag_allocator);
            index.has_value()) {
            hnsw = index.value();
            HnswIndexHandler* hnsw_index = new HnswIndexHandler(true,
                                                                false,
                                                                use_static,
                                                                max_degree,
                                                                ef_construction,
                                                                ef_search,
                                                                dim,
                                                                hnsw,
                                                                vsag_allocator);
            index_handler = static_cast<VectorIndexPtr>(hnsw_index);
            vsag::logger::debug("   success to create hnsw index , index parameter:{}, allocator addr:{}",index_parameters.dump(), (void*)vsag_allocator);
            return 0;
        } else {
            error = index.error().type;
            vsag::logger::debug("   fail to create hnsw index , index parameter:{}", index_parameters.dump());
        }
    } else {
        error = vsag::ErrorType::UNSUPPORTED_INDEX;
        vsag::logger::debug("   fail to create hnsw index , index type not supported:{}", static_cast<int>(index_type));
    }
    ret = static_cast<int>(error);
    if (ret != 0) {
        vsag::logger::error("   create index error happend, ret={}", static_cast<int>(error));
    }
    return ret;
}

int build_index(VectorIndexPtr& index_handler,float* vector_list, int64_t* ids, int dim, int size) {
    vsag::logger::debug("TRACE LOG[build_index]:");
    vsag::ErrorType error = vsag::ErrorType::UNKNOWN_ERROR;
    int ret =  0;
    if (index_handler == nullptr || vector_list == nullptr || ids == nullptr) {
        vsag::logger::debug("   null pointer addr, index_handler:{}, ids:{}, ids:{}",
                                                   (void*)index_handler, (void*)vector_list, (void*)ids);
        return static_cast<int>(error);
    }
    SlowTaskTimer t("build_index");
    HnswIndexHandler* hnsw = static_cast<HnswIndexHandler*>(index_handler);
    auto dataset = vsag::Dataset::Make();
    dataset->Dim(dim)
    ->NumElements(size)
    ->Ids(ids)
    ->Float32Vectors(vector_list)
    ->Owner(false);
    ret = hnsw->build_index(dataset);
    if (ret != 0) {
        vsag::logger::error("   build index error happend, ret={}", ret);
    }
    return ret;
}


int add_index(VectorIndexPtr& index_handler,float* vector, int64_t* ids, int dim, int size) {
    vsag::logger::debug("TRACE LOG[add_index]:");
    vsag::ErrorType error = vsag::ErrorType::UNKNOWN_ERROR;
    int ret = 0;
    if (index_handler == nullptr || vector == nullptr || ids == nullptr) {
        vsag::logger::debug("   null pointer addr, index_handler:{}, ids:{}, ids:{}",
                                                   (void*)index_handler, (void*)vector, (void*)ids);
        return static_cast<int>(error);
    }
    HnswIndexHandler* hnsw = static_cast<HnswIndexHandler*>(index_handler);
    SlowTaskTimer t("add_index");
    // add index
    auto incremental = vsag::Dataset::Make();
        incremental->Dim(dim)
            ->NumElements(size)
            ->Ids(ids)
            ->Float32Vectors(vector)
            ->Owner(false);
    ret = hnsw->add_index(incremental);
    if (ret != 0) {
        vsag::logger::error("   add index error happend, ret={}", ret);
    }
    return ret;
}

int get_index_number(VectorIndexPtr& index_handler, int64_t &size) {
    vsag::ErrorType error = vsag::ErrorType::UNKNOWN_ERROR;
    if (index_handler == nullptr) {
        vsag::logger::debug("   null pointer addr, index_handler:{}", (void*)index_handler);
        return static_cast<int>(error);
    }
    HnswIndexHandler* hnsw = static_cast<HnswIndexHandler*>(index_handler);
    size = hnsw->get_index_number();
    return 0;
}

int knn_search(VectorIndexPtr& index_handler,float* query_vector,int dim, int64_t topk,
               const float*& dist, const int64_t*& ids, int64_t &result_size, int ef_search,
               void* invalid) {
    vsag::logger::debug("TRACE LOG[knn_search]:");
    vsag::ErrorType error = vsag::ErrorType::UNKNOWN_ERROR;
    int ret = 0;
    if (index_handler == nullptr || query_vector == nullptr) {
        vsag::logger::debug("   null pointer addr, index_handler:{}, query_vector:{}",
                                                   (void*)index_handler, (void*)query_vector);
        return static_cast<int>(error);
    }
    SlowTaskTimer t("knn_search");
    roaring::api::roaring64_bitmap_t *bitmap = static_cast<roaring::api::roaring64_bitmap_t*>(invalid);
    auto filter = [bitmap](int64_t id) -> bool {
        return roaring::api::roaring64_bitmap_contains(bitmap, id);
    };
    nlohmann::json search_parameters{{"hnsw", {{"ef_search", ef_search}}}};
    HnswIndexHandler* hnsw = static_cast<HnswIndexHandler*>(index_handler);
    auto query = vsag::Dataset::Make();
    query->NumElements(1)->Dim(dim)->Float32Vectors(query_vector)->Owner(false);
    ret = hnsw->knn_search(query, topk, search_parameters.dump(), dist, ids, result_size, filter);
    if (ret != 0) {
        vsag::logger::error("   knn search error happend, ret={}", ret);
    }
    return ret;
}

int serialize(VectorIndexPtr& index_handler, const std::string dir) {
    vsag::logger::debug("TRACE LOG[serialize]:");
    vsag::ErrorType error = vsag::ErrorType::UNKNOWN_ERROR;
    int ret =  0;
    if (index_handler == nullptr) {
        vsag::logger::debug("   null pointer addr, index_handler:{}", (void*)index_handler);
        return static_cast<int>(error);
    }
    HnswIndexHandler* hnsw = static_cast<HnswIndexHandler*>(index_handler);
    if (auto bs = hnsw->get_index()->Serialize(); bs.has_value()) {
        hnsw = nullptr;
        auto keys = bs->GetKeys();
        for (auto key : keys) {
            vsag::Binary b = bs->Get(key);
            std::ofstream file(dir + "hnsw.index." + key, std::ios::binary);
            file.write((const char*)b.data.get(), b.size);
            file.close();
        }
        std::ofstream metafile(dir + "hnsw.index._meta", std::ios::out);
        for (auto key : keys) {
            metafile << key << std::endl;
        }
        metafile.close();
        return 0;
    } else {
        error = bs.error().type;
    }
    ret = static_cast<int>(error);
    if (ret != 0) {
        vsag::logger::error("   serialize error happend, ret={}", ret);
    }
    return ret;
}

int fserialize(VectorIndexPtr& index_handler, std::ostream& out_stream) {
    vsag::logger::debug("TRACE LOG[fserialize]:");
    vsag::ErrorType error = vsag::ErrorType::UNKNOWN_ERROR;
    int ret = 0;
    if (index_handler == nullptr) {
        vsag::logger::debug("   null pointer addr, index_handler:{}", (void*)index_handler);
        return static_cast<int>(error);
    }
    HnswIndexHandler* hnsw = static_cast<HnswIndexHandler*>(index_handler);
    if (auto bs = hnsw->get_index()->Serialize(out_stream); bs.has_value()) {
        return 0;
    } else {
        error = bs.error().type;
    }
    ret = static_cast<int>(error);
    if (ret != 0) {
        vsag::logger::error("   fserialize error happend, ret={}", ret);
    }
    return ret;
}

int fdeserialize(VectorIndexPtr& index_handler, std::istream& in_stream) {
    vsag::logger::debug("TRACE LOG[fdeserialize]:");
    vsag::ErrorType error = vsag::ErrorType::UNKNOWN_ERROR;
    int ret = 0;
    if (index_handler == nullptr) {
        vsag::logger::debug("   null pointer addr, index_handler:{}", (void*)index_handler);
        return static_cast<int>(error);
    }
    HnswIndexHandler* hnsw = static_cast<HnswIndexHandler*>(index_handler);
    std::shared_ptr<vsag::Index> hnsw_index;
    bool use_static = hnsw->get_use_static();
    int max_degree = hnsw->get_max_degree();
    int ef_construction = hnsw->get_ef_construction();
    int ef_search = hnsw->get_ef_search();
    int dim = hnsw->get_dim();
    nlohmann::json hnsw_parameters{{"max_degree", max_degree},
                                {"ef_construction", ef_construction},
                                {"ef_search", ef_search},
                                {"use_static", use_static}};
    nlohmann::json index_parameters{
        {"dtype", "float32"}, {"metric_type", "l2"}, {"dim", dim}, {"hnsw", hnsw_parameters}};
    vsag::logger::debug("   Deserilize hnsw index , index parameter:{}, allocator addr:{}",index_parameters.dump(),(void*)hnsw->get_allocator());
    if (auto index = vsag::Factory::CreateIndex("hnsw", index_parameters.dump(), hnsw->get_allocator());
        index.has_value()) {
        hnsw_index = index.value();
    } else {
        error = index.error().type;
        ret = static_cast<int>(error);
    }
    if (ret != 0) {

    } else if (auto bs = hnsw_index->Deserialize(in_stream); bs.has_value()) {
        hnsw->set_index(hnsw_index);
        return 0;
    } else {
        error = bs.error().type;
        ret = static_cast<int>(error);
    }
    if (ret != 0) {
        vsag::logger::error("   fdeserialize error happend, ret={}", ret);
    }
    return ret;
}

int deserialize_bin(VectorIndexPtr& index_handler,const std::string dir) {
    vsag::logger::debug("TRACE LOG[deserialize]:");
    vsag::ErrorType error = vsag::ErrorType::UNKNOWN_ERROR;
    int ret = 0;
    if (index_handler == nullptr) {
        vsag::logger::debug("   null pointer addr, index_handler={}", (void*)index_handler);
        return static_cast<int>(error);
    }
    HnswIndexHandler* hnsw = static_cast<HnswIndexHandler*>(index_handler);
    std::ifstream metafile(dir + "hnsw.index._meta", std::ios::in);
    std::vector<std::string> keys;
    std::string line;
    while (std::getline(metafile, line)) {
        keys.push_back(line);
    }
    metafile.close();

    vsag::BinarySet bs;
    for (auto key : keys) {
        std::ifstream file(dir + "hnsw.index." + key, std::ios::in);
        file.seekg(0, std::ios::end);
        vsag::Binary b;
        b.size = file.tellg();
        b.data.reset(new int8_t[b.size]);
        file.seekg(0, std::ios::beg);
        file.read((char*)b.data.get(), b.size);
        bs.Set(key, b);
    }
    bool use_static = hnsw->get_use_static();
    int max_degree = hnsw->get_max_degree();
    int ef_construction = hnsw->get_ef_construction();
    int ef_search = hnsw->get_ef_search();
    int dim = hnsw->get_dim();
    nlohmann::json hnsw_parameters{{"max_degree", max_degree},
                                {"ef_construction", ef_construction},
                                {"ef_search", ef_search},
                                {"use_static", use_static}};
    nlohmann::json index_parameters{
        {"dtype", "float32"}, {"metric_type", "l2"}, {"dim", dim}, {"hnsw", hnsw_parameters}};
    vsag::logger::debug("   Deserilize hnsw index , index parameter:{}, allocator addr:{}",index_parameters.dump(),(void*)hnsw->get_allocator());
    std::shared_ptr<vsag::Index> hnsw_index;
    if (auto index = vsag::Factory::CreateIndex("hnsw", index_parameters.dump(),hnsw->get_allocator());
        index.has_value()) {
        hnsw_index = index.value();
    } else {
        error = index.error().type;
        return static_cast<int>(error);
    }
    hnsw_index->Deserialize(bs);
    hnsw->set_index(hnsw_index);
    return 0;
}

int delete_index(VectorIndexPtr& index_handler) {
    vsag::logger::debug("TRACE LOG[delete_index]");
    vsag::logger::debug("   delete index handler addr {} : hnsw index use count {}",(void*)static_cast<HnswIndexHandler*>(index_handler)->get_index().get(),static_cast<HnswIndexHandler*>(index_handler)->get_index().use_count());
    if (index_handler != NULL) {
        delete static_cast<HnswIndexHandler*>(index_handler);
        index_handler = NULL;
    }
    return 0;
}

int64_t example() {
    return 0;
}

extern bool is_init_c() {
    return is_init();
}

extern void set_logger_c(void *logger_ptr) {
    set_logger(logger_ptr);
}

extern void set_block_size_limit_c(uint64_t size) {
    set_block_size_limit(size);
}

extern bool is_supported_index_c(IndexType index_type) {
    return is_supported_index(index_type);
}

extern int create_index_c(VectorIndexPtr& index_handler, IndexType index_type,
                 const char* dtype,
                 const char* metric, int dim,
                 int max_degree, int ef_construction, int ef_search, void* allocator)
{   
    return create_index(index_handler, index_type, dtype, metric, dim, max_degree, ef_construction, ef_search, allocator);
}

extern int build_index_c(VectorIndexPtr& index_handler,float* vector_list, int64_t* ids, int dim, int size) {

    return build_index(index_handler, vector_list, ids, dim, size);
}


extern int add_index_c(VectorIndexPtr& index_handler,float* vector, int64_t* ids, int dim, int size) {
    return  add_index(index_handler, vector, ids, dim, size);
}

extern int get_index_number_c(VectorIndexPtr& index_handler, int64_t &size) {
    return get_index_number(index_handler, size);
}

extern int knn_search_c(VectorIndexPtr& index_handler,float* query_vector,int dim, int64_t topk,
               const float*& dist, const int64_t*& ids, int64_t &result_size, int ef_search, void* invalid) {
    return knn_search(index_handler, query_vector, dim, topk, dist, ids, result_size, ef_search, invalid);
}

extern int serialize_c(VectorIndexPtr& index_handler, const std::string dir) {
    return serialize(index_handler, dir);
}

extern int fserialize_c(VectorIndexPtr& index_handler, std::ostream& out_stream) {
    return fserialize(index_handler, out_stream);
}

extern int delete_index_c(VectorIndexPtr& index_handler) {
    return delete_index(index_handler);
}
extern int fdeserialize_c(VectorIndexPtr& index_handler, std::istream& in_stream) {
    return fdeserialize(index_handler, in_stream);
}

extern int deserialize_bin_c(VectorIndexPtr& index_handler,const std::string dir) {
    return deserialize_bin(index_handler, dir);
}

} //namespace obvectorlib
