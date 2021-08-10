<h1 align="center">Viper: An Efficient Hybrid PMem-DRAM Key-Value Store</h1>
<p align="center">This repository contains the code to our <a href="https://hpi.de/fileadmin/user_upload/fachgebiete/rabl/publications/2021/viper_vldb21.pdf"> VLDB '21 paper<a/>.<p/>


### Using Viper
Viper is an embedded header-only key-value store for persistent memory.
You can download it and include it in your application without using CMake (check out the [Downloading Viper](#downloading-viper) and [Dependencies](#dependencies) sections below).
Here is a short example of Viper's interface (this is the content of `playground.cpp`).

```cpp
#include <iostream>
#include "viper/viper.hpp"

int main(int argc, char** argv) {
  const size_t initial_size = 1073741824;  // 1 GiB
  auto viper_db = viper::Viper<uint64_t, uint64_t>::create("/mnt/pmem2/viper", initial_size);
  
  // To modify records in Viper, you need to use a Viper Client.
  auto v_client = viper_db->get_client();
  
  for (uint64_t key = 0; key < 10; ++key) {
    const uint64_t value = key + 10;
    v_client.put(key, value);
  }
  
  for (uint64_t key = 0; key < 11; ++key) {
    uint64_t value;
    const bool found = v_client.get(key, &value);
    if (found) {
      std::cout << "Record: " << key << " --> " << value << std::endl;
    } else {
      std::cout << "No record found for key: " << key << std::endl;
    }
  }
}
```

### Downloading Viper
As Viper is header-only, you only need to download the header files and include them in your code as shown above.
You do not need to use Viper's CMakeLists.txt.
Just make sure you have the [dependencies](#dependencies) installed.
Here is a common way to do include Viper using `FetchContent` in CMake.
By default, this will fetch all dependencies.

```cmake
include(FetchContent)

FetchContent_Declare(
  viper
  GIT_REPOSITORY https://github.com/hpides/viper.git
)
FetchContent_MakeAvailable(viper)

# ... other CMake stuff

# Link Viper to get the transitive dependencies
target_link_libraries(your-target viper)
```

This avoids calling all the Viper CMake code, which is mainly needed for the benchmark code.
Of course, you can also simply download the code from GitHub into a third_party directory or use your preferred method.
Check out the CMake options for Viper at the top of [CMakeLists.txt](https://github.com/hpides/viper/blob/master/CMakeLists.txt)
for more details on what to include and build.

  
### Dependencies
Viper depends on [concurrentqueue 1.0.3](https://github.com/cameron314/concurrentqueue).
As Viper is header-only, you should make sure that this dependency is available.
Check out the CMake options for Viper at the top of the [CMakeLists.txt](https://github.com/hpides/viper/blob/master/CMakeLists.txt)
for more details.
Check out Viper's [CMakeLists.txt](https://github.com/hpides/viper/blob/c5a3707001dac131421f98a36ebf4f5309b19e35/CMakeLists.txt#L28-L36) to see an example of how to add `concurrentqueue` as a dependency.
You can find the licenses of the dependencies in the [LICENSE file](https://github.com/hpides/viper/blob/master/LICENSE).

### Building the Benchmarks
First off, if you want to compare your system's performance against Viper, it's probably best to include Viper in your 
benchmark framework instead of relying on this one.

To build the benchmarks, you need to use pass:
```
-DVIPER_BUILD_BENCHMARKS=ON -DVIPER_PMDK_PATH=/path/to/pmdk -DLIBPMEMOBJ++_PATH=/path/to/libpmemobj++
```
  
CMake should download and build all dependencies automatically.
You can then run the individual benchmarks (executables with `_bm` suffix) in the `benchmark` directory.

**NOTE**: not all benchmarks will complete by default, due to things such as out-of-memory errors.
These problems are in some third party systems that I could not fix.
You might need to play around with them for a bit and remove certain runs/configurations and run them manually bit-by-bit.
You will also need to specify some benchmark info in the `benchmark.hpp`, such as the data directories and CPU-affinity.


### Cite Our Work
If you use Viper in your work, please cite us.

```bibtex
@article{benson_viper_2021,
  author    = {Lawrence Benson and Hendrik Makait and Tilmann Rabl},
  title     = {Viper: An Efficient Hybrid PMem-DRAM Key-Value Store},
  journal   = {Proceedings of the {VLDB} Endowment},
  volume    = {14},
  number    = {9},
  year      = {2021},
  pages     = {1544--1556},
  doi       = {10.14778/3461535.3461543}
}
```
