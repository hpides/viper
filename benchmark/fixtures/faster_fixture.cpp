#include "faster_fixture.hpp"

void viper::kv_bm::FasterFixture::InitMap(uint64_t num_prefill_inserts, const bool re_init) {
    if (faster_initialized_ && !re_init) {
        return;
    }

    db_ = std::make_unique<faster_t>((1L << 27), 17179869184, db_file_);

    auto callback = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<UpsertContext> context{ctxt };
        assert(result == Status::Ok);
    };

    db_->StartSession();

    for (uint64_t key = 0; key < num_prefill_inserts; ++key) {
        if (key % kRefreshInterval == 0) {
            db_->Refresh();
            if (key % kCompletePendingInterval == 0) {
                db_->CompletePending(false);
            }
        }

        UpsertContext context{ key, key };
        db_->Upsert(context, callback, 1);
    }

    db_->Refresh();
    db_->CompletePending(true);
    db_->StopSession();

    faster_initialized_ = true;
}
void viper::kv_bm::FasterFixture::DeInitMap() {
    db_ = nullptr;
    faster_initialized_ = false;
}
void viper::kv_bm::FasterFixture::insert_empty(uint64_t start_idx, uint64_t end_idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<UpsertContext> context{ctxt };
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

        UpsertContext context{ key, key*100 };
        db_->Upsert(context, callback, 1);
    }

    db_->Refresh();
    db_->CompletePending(true);
    db_->StopSession();
}
void viper::kv_bm::FasterFixture::setup_and_insert(uint64_t start_idx, uint64_t end_idx) {
    insert_empty(start_idx, end_idx);
}
uint64_t viper::kv_bm::FasterFixture::setup_and_find(uint64_t start_idx, uint64_t end_idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<ReadContext> context{ctxt };
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
