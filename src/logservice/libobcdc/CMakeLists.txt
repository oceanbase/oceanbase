set(CMAKE_POSITION_INDEPENDENT_CODE ON)
add_definitions(-D_GLIBCXX_USE_CXX11_ABI=0)

# libobcdc LTO / sample PGO / hotfunc (profile files under libobcdc/profile/)
# ThinLTO / sample PGO 仅在 CDC 构建(BUILD_CDC_ONLY=ON)中由 Env.cmake 的
# ENABLE_CDC_COMPILE_OPTIMIZATIONS 全局启用(覆盖 oceanbase_static 全部代码,
# 与 observer 的 AUTO_FDO/HOTFUNC 完全独立);此处仅在全局未启用时对 CDC 目标兜底,
# hotfunc 符号重排是链接期选项,始终按 CDC 目标粒度控制。
if(NOT DEFINED ENABLE_CDC_THIN_LTO)
  if(ENABLE_CDC_COMPILE_OPTIMIZATIONS AND NOT ENABLE_THIN_LTO)
    set(ENABLE_CDC_THIN_LTO ON)
  else()
    set(ENABLE_CDC_THIN_LTO OFF)
  endif()
endif()
if(NOT DEFINED ENABLE_CDC_SAMPLE_PGO)
  if(ENABLE_CDC_COMPILE_OPTIMIZATIONS AND NOT AUTO_FDO_OPT)
    set(ENABLE_CDC_SAMPLE_PGO ON)
  else()
    set(ENABLE_CDC_SAMPLE_PGO OFF)
  endif()
endif()
if(NOT DEFINED ENABLE_CDC_HOTFUNC)
  set(ENABLE_CDC_HOTFUNC ${ENABLE_CDC_COMPILE_OPTIMIZATIONS})
endif()

add_subdirectory(src)
add_subdirectory(tests)
