#include "faster_fixture.hpp"

namespace viper::kv_bm {

void FasterFixture::InitMap(uint64_t num_prefill_inserts, const bool re_init) {
    if (faster_initialized_ && !re_init) {
        return;
    }

    base_dir_ = DB_NVM_DIR;
    db_dir_ = random_file(base_dir_);
    db_ = std::make_unique<faster_t>((1L << 27), 17179869184, db_dir_);

    db_->StartSession();
    prefill(num_prefill_inserts);
    db_->Refresh();
    db_->CompletePending(true);
    db_->StopSession();

    faster_initialized_ = true;
}
void FasterFixture::DeInitMap() {
    db_ = nullptr;
    faster_initialized_ = false;
    std::filesystem::remove_all(db_dir_);
}
void FasterFixture::insert_empty(uint64_t start_idx, uint64_t end_idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<UpsertContext> context{ctxt};
        assert(result == Status::Ok);
    };

    db_->StartSession();

    for (uint64_t key = start_idx; key < end_idx; ++key) {
        if (key % kRefreshInterval == 0) {
            db_->Refresh();
            if (key % kCompletePendingInterval == 0) {
                db_->CompletePending(false);
            }
        }

        UpsertContext context{key, key};
        db_->Upsert(context, callback, 1);
    }

    db_->Refresh();
    db_->CompletePending(true);
    db_->StopSession();
}

void FasterFixture::setup_and_insert(uint64_t start_idx, uint64_t end_idx) {
    insert_empty(start_idx, end_idx);
}

uint64_t FasterFixture::setup_and_find(uint64_t start_idx, uint64_t end_idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<ReadContext> context{ctxt};
        assert(result == Status::Ok);
    };

    uint64_t found_counter = 0;
    db_->StartSession();

    for (uint64_t key = start_idx; key < end_idx; ++key) {
        if (key % kRefreshInterval == 0) {
            db_->Refresh();
            if (key % kCompletePendingInterval == 0) {
                db_->CompletePending(false);
            }
        }

        FasterValue result;
        ReadContext context{key, &result};
        const bool found = db_->Read(context, callback, key) == FASTER::core::Status::Ok;
        found_counter += found && (result.value_.data[0] == key);
    }

    db_->Refresh();
    db_->CompletePending(true);
    db_->StopSession();

    return found_counter;
}

uint64_t FasterFixture::setup_and_delete(uint64_t start_idx, uint64_t end_idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<ReadContext> context{ctxt};
        assert(result == Status::Ok);
    };

    uint64_t delete_counter = 0;
    db_->StartSession();

    for (uint64_t key = start_idx; key < end_idx; ++key) {
        if (key % kRefreshInterval == 0) {
            db_->Refresh();
            if (key % kCompletePendingInterval == 0) {
                db_->CompletePending(false);
            }
        }

        DeleteContext context{key};
        delete_counter += db_->Delete(context, callback, key) == FASTER::core::Status::Ok;
    }

    db_->Refresh();
    db_->CompletePending(true);
    db_->StopSession();

    return delete_counter;
}

}  // namespace viper::kv_bm