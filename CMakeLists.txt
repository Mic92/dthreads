cmake_minimum_required(VERSION 3.1)
project(tthread)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

set(CMAKE_THREAD_PREFER_PTHREAD)
find_package(Threads)
if(NOT CMAKE_USE_PTHREADS_INIT)
  message(SEND_ERROR "Pthreads required")
endif(NOT CMAKE_USE_PTHREADS_INIT)

find_package(LibDL)
if(NOT LIBDL_FOUND)
  message(SEND_ERROR "libdl required")
endif(NOT LIBDL_FOUND)

find_package(PAPI REQUIRED)

INCLUDE (CheckCXXSourceCompiles)
CHECK_CXX_SOURCE_COMPILES(
"
int main()
{
__builtin_return_address(0);
}
" HAS_BUILTIN_RETURN_ADDRESS)

if(LIB_INSTALL_DIR)
else()
  set(LIB_INSTALL_DIR lib)
endif()

option (BENCHMARK "Enable Benchmarking" OFF)
if (BENCHMARK)
  add_subdirectory(eval)
endif()

add_subdirectory(src)
add_subdirectory(include)

option (TESTING "Enable Testing" OFF)
if (TESTING)
  enable_testing()
  add_subdirectory(test)
endif()
