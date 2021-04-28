# Viper: An Efficient Hybrid PMem-DRAM Key-Value Store
This repository contains the code to our [VLDB '21 paper](https://hpi.de/fileadmin/user_upload/fachgebiete/rabl/publications/2021/viper_vldb21.pdf).

### Using Viper
Viper is an embedded header-only key-value store for persistent memory.
You can download it and include it in your application. 
Here is a short example of Viper's interface. 

```cpp
#include <iostream>
#include "viper/viper.hpp"

int main(int argc, char** argv) {
    const size_t inital_size = 1073741824;  // 1 GiB
    auto viper_db = viper::Viper<uint64_t, uint64_t>::create("/mnt/pmem2/viper", inital_size);

    // To modify records in Viper, you need to use a Viper Client.
    auto v_client = viper_db->get_client();

    for (uint64_t key = 0; key < 10; ++key) {
        const uint64_t value = key + 10;
        v_client.put(key, value);
    }

    for (uint64_t key = 0; key < 10; ++key) {
        uint64_t value;
        v_client.get(key, &value);
        std::cout << "Record: " << key << " --> " << value << std::endl;
    }
}
```

If you use Viper in your work, please cite us.
```
@article{benson_viper_2021,
  author    = {Lawrence Benson and Hendrik Makait and Tilmann Rabl},
  title     = {Viper: An Efficient Hybrid PMem-DRAM Key-Value Store},
  journal   = {Proceedings of the {VLDB} Endowment},
  volume    = {14},
  number    = {9},
  year      = {2021},
  doi       = {10.14778/3461535.3461543},
}
```
