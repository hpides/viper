#pragma once

#include <array>

namespace viper::kv_bm {

class BMKeyFixed {
  public:
    BMKeyFixed() {}
    BMKeyFixed(uint64_t key) {
        uuid[0] = key;
        uuid[1] = key;
    }

    BMKeyFixed(uint64_t key0, uint64_t key1) {
        uuid[0] = key0;
        uuid[1] = key1;
    }

    inline bool operator==(const BMKeyFixed other) const {
        return uuid[0] == other.uuid[0] && uuid[1] == other.uuid[1];
    }

    // 16 byte
    std::array<uint64_t, 2> uuid;
};

class BMKeyVariable {
  public:
    BMKeyVariable() {}

};


class BMValueFixed {
  public:
    BMValueFixed(uint64_t value_placeholder) {
        data.fill(value_placeholder);
    }

    BMValueFixed& operator=(const BMValueFixed& other) {
        this->data = other.data;
        return *this;
    }

    BMValueFixed(const std::array<uint64_t, 25>& data) : data(data) {}
    BMValueFixed(const BMValueFixed& other) : data(other.data) {}

    BMValueFixed() {}

    // 200 byte
    std::array<uint64_t, 25> data;
};

class BMValueVariable {
  public:
    BMValueVariable() {}

};

}  // namespace viper::kv_bm