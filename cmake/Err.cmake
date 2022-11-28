macro(ob_def_error ERR_ID ERR_MSG)
  if (NOT DEFINED ${ERR_ID})
    set(${ERR_ID} ${ERR_MSG})
  endif()
endmacro()

ob_def_error(E1001 "[E1001] Header files are not allowed in CMakeLists.txt")
ob_def_error(HELP_LINK "https://yuque.antfin.com/docs/share/f2745c4d-afc9-4c80-8348-88741f902bab?#uVf51")

