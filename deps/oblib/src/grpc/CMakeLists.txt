set(DEVEL_PATH "${DEP_3RD_DIR}/usr/local/oceanbase/deps/devel")

option(protobuf_MODULE_COMPATIBLE TRUE)
set(_PROTOBUF_LIBPROTOBUF "${DEVEL_PATH}/lib64/libprotobuf.a")
set(_REFLECTION "${DEVEL_PATH}/lib/libgrpc++_reflection.a")
set(_PROTOBUF_PROTOC "${DEVEL_PATH}/bin/protoc")
set(_GRPC_GRPCPP "${DEVEL_PATH}/lib/libgrpc++.a")
set(_GRPC_GRPC "${DEVEL_PATH}/lib/libgrpc.a")

set(hw_proto_srcs "newlogstorepb.pb.cpp")
set(hw_grpc_srcs "newlogstorepb.grpc.pb.cpp")

include_directories(
      ${CMAKE_CURRENT_BINARY_DIR}
      ${DEP_3RD_DIR}/usr/include
      ${DEP_3RD_DIR}/usr/local/oceanbase/deps/devel/include
)
message(${CMAKE_CURRENT_BINARY_DIR})

add_library(hw_grpc_proto
      ${hw_grpc_srcs}
      ${hw_proto_srcs}
      health.pb.cpp
      health.grpc.pb.cpp)
target_link_libraries(hw_grpc_proto
      ${_REFLECTION}
      ${_GRPC_GRPCPP}
      ${_PROTOBUF_LIBPROTOBUF})

add_library(oblib_grpc STATIC
    ob_grpc_context.cpp
)
target_link_libraries(oblib_grpc
      PUBLIC oblib_base
      hw_grpc_proto
      ${DEVEL_PATH}/lib/libgrpc_unsecure.a
      ${DEVEL_PATH}/lib/libaddress_sorting.a
      ${DEVEL_PATH}/lib64/libcares.a
      ${DEVEL_PATH}/lib/libgrpc++_error_details.a
      ${DEVEL_PATH}/lib/libgrpc_plugin_support.a
      ${DEVEL_PATH}/lib/libgpr.a
      ${DEVEL_PATH}/lib/libgrpcpp_channelz.a
      ${DEVEL_PATH}/lib/libgrpc.a
      ${DEVEL_PATH}/lib/libgrpc++_reflection.a
      ${DEVEL_PATH}/lib/libupb.a
      ${DEVEL_PATH}/lib64/libabsl_flags_commandlineflag_internal.a
      ${DEVEL_PATH}/lib64/libabsl_status.a
      ${DEVEL_PATH}/lib64/libabsl_examine_stack.a
      ${DEVEL_PATH}/lib64/libabsl_random_internal_randen_hwaes_impl.a
      ${DEVEL_PATH}/lib64/libabsl_hash.a
      ${DEVEL_PATH}/lib64/libabsl_time_zone.a
      ${DEVEL_PATH}/lib64/libabsl_cordz_info.a
      ${DEVEL_PATH}/lib64/libabsl_strings.a
      ${DEVEL_PATH}/lib64/libabsl_strings_internal.a
      ${DEVEL_PATH}/lib64/libabsl_random_internal_pool_urbg.a
      ${DEVEL_PATH}/lib64/libabsl_synchronization.a
      ${DEVEL_PATH}/lib64/libabsl_failure_signal_handler.a
      ${DEVEL_PATH}/lib64/libabsl_strerror.a
      ${DEVEL_PATH}/lib64/libabsl_log_severity.a
      ${DEVEL_PATH}/lib64/libabsl_debugging_internal.a
      ${DEVEL_PATH}/lib64/libabsl_hashtablez_sampler.a
      ${DEVEL_PATH}/lib64/libabsl_stacktrace.a
      ${DEVEL_PATH}/lib64/libabsl_time.a
      ${DEVEL_PATH}/lib64/libabsl_int128.a
      ${DEVEL_PATH}/lib64/libabsl_symbolize.a
      ${DEVEL_PATH}/lib64/libabsl_cordz_handle.a
      ${DEVEL_PATH}/lib64/libabsl_random_internal_seed_material.a
      ${DEVEL_PATH}/lib64/libabsl_random_internal_randen_hwaes.a
      ${DEVEL_PATH}/lib64/libabsl_bad_any_cast_impl.a
      ${DEVEL_PATH}/lib64/libabsl_cord.a
      ${DEVEL_PATH}/lib64/libabsl_flags_program_name.a
      ${DEVEL_PATH}/lib64/libabsl_random_internal_randen.a
      ${DEVEL_PATH}/lib64/libabsl_flags_commandlineflag.a
      ${DEVEL_PATH}/lib64/libabsl_raw_hash_set.a
      ${DEVEL_PATH}/lib64/libabsl_bad_variant_access.a
      ${DEVEL_PATH}/lib64/libabsl_leak_check_disable.a
      ${DEVEL_PATH}/lib64/libabsl_graphcycles_internal.a
      ${DEVEL_PATH}/lib64/libabsl_statusor.a
      ${DEVEL_PATH}/lib64/libabsl_flags.a
      ${DEVEL_PATH}/lib64/libabsl_bad_optional_access.a
      ${DEVEL_PATH}/lib64/libabsl_str_format_internal.a
      ${DEVEL_PATH}/lib64/libabsl_raw_logging_internal.a
      ${DEVEL_PATH}/lib64/libabsl_civil_time.a
      ${DEVEL_PATH}/lib64/libabsl_flags_private_handle_accessor.a
      ${DEVEL_PATH}/lib64/libabsl_random_seed_sequences.a
      ${DEVEL_PATH}/lib64/libabsl_exponential_biased.a
      ${DEVEL_PATH}/lib64/libabsl_flags_usage.a
      ${DEVEL_PATH}/lib64/libabsl_spinlock_wait.a
      ${DEVEL_PATH}/lib64/libabsl_cordz_functions.a
      ${DEVEL_PATH}/lib64/libabsl_base.a
      ${DEVEL_PATH}/lib64/libabsl_cordz_sample_token.a
      ${DEVEL_PATH}/lib64/libabsl_random_internal_randen_slow.a
      ${DEVEL_PATH}/lib64/libabsl_periodic_sampler.a
      ${DEVEL_PATH}/lib64/libabsl_scoped_set_env.a
      ${DEVEL_PATH}/lib64/libabsl_random_seed_gen_exception.a
      ${DEVEL_PATH}/lib64/libabsl_cord_internal.a
      ${DEVEL_PATH}/lib64/libabsl_malloc_internal.a
      ${DEVEL_PATH}/lib64/libabsl_low_level_hash.a
      ${DEVEL_PATH}/lib64/libabsl_flags_usage_internal.a
      ${DEVEL_PATH}/lib64/libabsl_city.a
      ${DEVEL_PATH}/lib64/libabsl_random_distributions.a
      ${DEVEL_PATH}/lib64/libabsl_demangle_internal.a
      ${DEVEL_PATH}/lib64/libabsl_flags_internal.a
      ${DEVEL_PATH}/lib64/libabsl_flags_parse.a
      ${DEVEL_PATH}/lib64/libabsl_flags_reflection.a
      ${DEVEL_PATH}/lib64/libabsl_flags_marshalling.a
      ${DEVEL_PATH}/lib64/libabsl_throw_delegate.a
      ${DEVEL_PATH}/lib64/libabsl_random_internal_distribution_test_util.a
      ${DEVEL_PATH}/lib64/libabsl_flags_config.a
      ${DEVEL_PATH}/lib64/libabsl_random_internal_platform.a
      ${DEVEL_PATH}/lib64/libabsl_leak_check.a
      ${_REFLECTION}
      ${_GRPC_GRPCPP}
      ${_PROTOBUF_LIBPROTOBUF}
)
