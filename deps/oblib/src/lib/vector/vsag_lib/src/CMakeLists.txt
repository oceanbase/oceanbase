
add_subdirectory (simd)

file (GLOB CPP_SRCS "*.cpp")
file (GLOB CPP_FACTORY_SRCS "factory/*.cpp")
file (GLOB CPP_CONJUGATE_GRAPH_SRCS "impl/*.cpp")
file (GLOB CPP_INDEX_SRCS "index/*.cpp")
file (GLOB CPP_HNSWLIB_SRCS "algorithm/hnswlib/*.cpp")
list (FILTER CPP_SRCS EXCLUDE REGEX "_test.cpp")
list (FILTER CPP_FACTORY_SRCS EXCLUDE REGEX "_test.cpp")
list (FILTER CPP_CONJUGATE_GRAPH_SRCS EXCLUDE REGEX "_test.cpp")
list (FILTER CPP_INDEX_SRCS EXCLUDE REGEX "_test.cpp")

set (VSAG_SRCS ${CPP_SRCS} ${CPP_FACTORY_SRCS} ${CPP_INDEX_SRCS} ${CPP_CONJUGATE_GRAPH_SRCS} ${CPP_HNSWLIB_SRCS})
add_library (vsag_test SHARED ${VSAG_SRCS})
add_library (vsag_static_test STATIC ${VSAG_SRCS})

set (VSAG_DEP_LIBS diskann pthread m dl simd fmt::fmt-header-only nlohmann_json::nlohmann_json roaring)
target_link_libraries (vsag_test ${VSAG_DEP_LIBS})
target_link_libraries (vsag_static_test ${VSAG_DEP_LIBS})
maybe_add_dependencies (vsag_test spdlog roaring openblas boost mkl)
maybe_add_dependencies (vsag_static_test spdlog roaring openblas boost mkl)
