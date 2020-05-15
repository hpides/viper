//#include "dash_fixture.hpp"
//#include "libpmemobj++/transaction.hpp"
//
//void viper::kv_bm::DashLinearFixture::InitMap(uint64_t num_prefill_inserts, const bool re_init) {
//    if (dash_initialized_ && !re_init) {
//        return;
//    }
//
//    dash_ = pmem_pool_.root().get();
//
//    for (uint64_t key = 0; key < num_prefill_inserts; ++key) {
//        dash_->Insert(key, (char*) key);
//    }
//    dash_initialized_ = true;
//}
//
//void viper::kv_bm::DashLinearFixture::DeInitMap() {
//    dash_initialized_ = false;
//}
//
//void viper::kv_bm::DashLinearFixture::insert_empty(uint64_t start_idx, uint64_t end_idx) {
//    for (uint64_t key = start_idx; key < end_idx; ++key) {
//        // uint64_t key = uniform_distribution(rnd_engine_);
//        dash_->Insert(key, (char*) (key*100));
//    }
//}
//
//void viper::kv_bm::DashLinearFixture::setup_and_insert(uint64_t start_idx, uint64_t end_idx) {
//    insert_empty(start_idx, end_idx);
//}
//
//uint64_t viper::kv_bm::DashLinearFixture::setup_and_find(uint64_t start_idx, uint64_t end_idx) {
//    return 0;
//}
