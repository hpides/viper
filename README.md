<p align="center">
  <img src="https://github.com/hpides/viper/blob/gh-pages/assets/images/viper-logo.png" alt="Viper Logo" width="300"/>
</p>
<h1 align="center">Viper: An Efficient Hybrid PMem-DRAM Key-Value Store</h1>
<p align="center">This repository contains the code to our <a href="https://hpi.de/fileadmin/user_upload/fachgebiete/rabl/publications/2021/viper_vldb21.pdf"> VLDB '21 paper<a/>.<p/>


### Using Viper
Viper is an embedded header-only key-value store for persistent memory.
You can download it and include it in your application without using CMake (check out the [Downloading Viper](#downloading-viper) and [Dependencies](#dependencies) sections below).
Here is a short example of Viper's interface. 

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
      std::cout << "No record found for: " << key << std::endl;
    }
  }
}
```

### Downloading Viper
As Viper is header-only, you only need to download the header files and include them in your code as shown above.
You do not need to use Viper's CMakeLists.txt.
Just make sure you have the [dependencies](#dependencies) installed.
Here is a common way to do include Viper using `FetchContent` in CMake.

```cmake
include(FetchContent)

FetchContent_Declare(
  viper
  GIT_REPOSITORY https://github.com/hpides/viper.git
)

FetchContent_GetProperties(viper)
if(NOT viper_POPULATED)
  FetchContent_Populate(viper)
endif()
include_directories(${viper_SOURCE_DIR}/include)
```

This avoids calling all the Viper CMake code, which is mainly needed for the benchmark code.
Of course you can also simply download the code from GitHub into a third_party directory or use your preferred method.

  
### Dependencies
Viper depends on [libpmem 1.10](https://github.com/pmem/pmdk) and [concurrentqueue 1.0.2](https://github.com/cameron314/concurrentqueue).
As Viper is header-only, you should make sure that these dependencies are available.
Check out Viper's [CMakeLists.txt](https://github.com/hpides/viper/blob/c5a3707001dac131421f98a36ebf4f5309b19e35/CMakeLists.txt#L28-L36) to see an example of how to add `concurrentqueue` as a dependency. 

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
  doi       = {10.14778/3461535.3461543},
}
```
