include(ExternalProject)

file(WRITE gcc.bldconf "
# Generated configuration file
export LIBS=''
export EXTRA_LIBS=''
export CC='gcc'
export CXX='g++'
export LD='${CMAKE_LINKER}'
export RANLIB='${CMAKE_RANLIB}'
export STRIP='${CMAKE_STRIP}'
export AR='${CMAKE_AR}'
export CFLAGS='-g -O3 -funroll-loops -fprefetch-loop-arrays -rdynamic'
export CXXFLAGS='-g -O3 -funroll-loops -fprefetch-loop-arrays -fpermissive -fno-exceptions -rdynamic'
export CPPFLAGS=''
export LDFLAGS='${CMAKE_MODULE_LINKER_FLAGS}'
export CC_ver=$($CC --version)
export CXX_ver=$($CXX --version)
export LD_ver=$($LD --version)
export LD_LIBRARY_PATH=
export MAKE='make'
export M4=m4
")

ExternalProject_Add(parsec
  URL http://parsec.cs.princeton.edu/download/3.0/parsec-3.0.tar.gz
  PATCH_COMMAND
  sh -c "find . -name '*.pod' -print0 | xargs -0 sed -ie 's/=item \\([0-9]\\+\\)/=item C\\1/'" &&
  sh -c "find . -name 'parsec_barrier.hpp' -type f -print0 | xargs -0 sed -ie 's!^#define ENABLE_AUTOMATIC_DROPIN!//#define ENABLE_AUTOMATIC_DROPIN!'" &&
  cp ${CMAKE_CURRENT_SOURCE_DIR}/tests/streamcluster/streamcluster.cpp ${CMAKE_CURRENT_SOURCE_DIR}/parsec-3.0/pkgs/kernels/streamcluster/src
  CONFIGURE_COMMAND ${CMAKE_COMMAND} -E copy ../gcc.bldconf config/gcc.bldconf
  SOURCE_DIR parsec-3.0
  BUILD_COMMAND bin/parsecmgmt -a build -c gcc-pthreads -p canneal -p dedup -p blackscholes -p streamcluster -p ferret -p swaptions -p vips
  INSTALL_COMMAND ""
  BUILD_IN_SOURCE 1
  LOG_BUILD 1
)

set(PARSEC_APP_PATH ${CMAKE_CURRENT_SOURCE_DIR}/parsec-3.0/pkgs)
set(TEST_PATH ${CMAKE_CURRENT_SOURCE_DIR}/tests/)
include(AddParsecBenchmark)

if(${NCORES} EQUAL 8)
  set(CANNEAL_THREADS 7)
  set(DEDUP_THREADS 2)
elseif(${NCORES} EQUAL 4)
  set(CANNEAL_THREADS 3)
  set(DEDUP_THREADS 1)
else() # EQUAL 2
  set(CANNEAL_THREADS 1)
  set(DEDUP_THREADS 1)
endif()

AddParsecBenchmark(blackscholes
  ARGS ${NCORES} in_64K.txt prices.txt
  PATH apps/blackscholes
  ARCHIVE input_simlarge.tar)

AddParsecBenchmark(canneal
  ARGS ${CANNEAL_THREADS} 15000 2000 ${TEST_PATH}/canneal/400000.nets 128
  PATH kernels/canneal
  ARCHIVE input_simlarge.tar)

AddParsecBenchmark(dedup
  ARGS -c -p -t ${DEDUP_THREADS} -i ${TEST_PATH}/dedup/FC-6-x86_64-disc1.iso -o output.dat.ddp
  PATH kernels/dedup
  ARCHIVE input_native.tar)

AddParsecBenchmark(ferret
  ARGS ${TEST_PATH}/ferret/corel lsh ${TEST_PATH}/ferret/queries 10 20 1 output.txt
  PATH apps/ferret
  ARCHIVE input_native.tar)

AddParsecBenchmark(swaptions
  ARGS -ns 128 -sm 50000 -nt ${NCORES}
  PATH apps/swaptions)

AddParsecBenchmark(streamcluster
  ARGS 10 20 128 16384 16384 1000 none output.txt ${NCORES}
  PATH kernels/streamcluster)

AddParsecBenchmark(vips
  ARGS im_benchmark ${TEST_PATH}/vips/orion_18000x18000.v output.v
  ENV IM_CONCURRENCY=${NCORES}
  PATH apps/vips
  ARCHIVE input_native.tar)

AddParsecBenchmark(raytrace
  ARGS ${TEST_PATH}/raytrace/thai_statue.obj -automove -nthreads ${NCORES} -frames 200 -res 1920 1080
  PATH apps/raytrace
  EXE rtview
  ARCHIVE input_native.tar)

set(parsec_benchmarks blacksholes canneal dedup ferret swaptions streamcluster vips raytrace)
foreach(bench ${parsec_benchmarks})
  list(APPEND parsec_build_targets bench-${bench})
endforeach()
add_custom_target(build-parsec DEPENDS ${parsec_build_targets})
