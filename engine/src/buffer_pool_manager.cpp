#include "buffer_pool_manager.h"
#include <algorithm>

BufferPoolManager::BufferPoolManager(size_t pool_size, StorageManager* storage_manager)
    : pool_size_(pool_size), storage_manager_(storage_manager) {
    frames_.reserve(pool_size);
    for (size_t i = 0; i < pool_size; ++i) {
        frames_.emplace_back(std::make_unique<Frame>());
        free_list_.push_back(frames_[i].get());
    }
}

Page* BufferPoolManager::FetchPage(page_id_t page_id) {
    auto it = page_table_.find(page_id);
    if (it != page_table_.end()) {
        Frame* frame = it->second;
        frame->pin_count++;
        lru_list_.remove(frame);
        lru_list_.push_front(frame);
        return frame->page.get();
    }

    Frame* frame = GetVictimFrame();
    if (!frame) {
        return nullptr;
    }

    if (frame->page && frame->is_dirty) {
        if (!FlushFrame(frame)) {
            return nullptr;
        }
    }

    if (frame->page) {
        page_table_.erase(frame->page->GetPageId());
    }

    frame->page = storage_manager_->ReadPage(page_id);
    if (!frame->page) {
        return nullptr;
    }

    frame->pin_count = 1;
    frame->is_dirty = false;
    page_table_[page_id] = frame;
    lru_list_.push_front(frame);

    return frame->page.get();
}

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        return false;
    }

    Frame* frame = it->second;
    if (frame->pin_count <= 0) {
        return false;
    }

    frame->pin_count--;
    if (is_dirty) {
        frame->is_dirty = true;
    }

    if (frame->pin_count == 0) {
        lru_list_.remove(frame);
        lru_list_.push_back(frame);
    }

    return true;
}

bool BufferPoolManager::FlushPage(page_id_t page_id) {
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        return false;
    }

    Frame* frame = it->second;
    return FlushFrame(frame);
}

Page* BufferPoolManager::NewPage(page_id_t* page_id) {
    Frame* frame = GetVictimFrame();
    if (!frame) {
        return nullptr;
    }

    if (frame->page && frame->is_dirty) {
        if (!FlushFrame(frame)) {
            return nullptr;
        }
    }

    if (frame->page) {
        page_table_.erase(frame->page->GetPageId());
    }

    *page_id = storage_manager_->AllocatePage();
    frame->page = std::make_unique<Page>(*page_id);
    frame->pin_count = 1;
    frame->is_dirty = true;
    page_table_[*page_id] = frame;
    lru_list_.push_front(frame);

    return frame->page.get();
}

bool BufferPoolManager::DeletePage(page_id_t page_id) {
    auto it = page_table_.find(page_id);
    if (it != page_table_.end()) {
        Frame* frame = it->second;
        if (frame->pin_count > 0) {
            return false;
        }

        page_table_.erase(page_id);
        lru_list_.remove(frame);
        free_list_.push_front(frame);
        frame->page.reset();
        frame->is_dirty = false;
        frame->pin_count = 0;
    }

    return true;
}

BufferPoolManager::Frame* BufferPoolManager::GetVictimFrame() {
    if (!free_list_.empty()) {
        Frame* frame = free_list_.front();
        free_list_.pop_front();
        return frame;
    }

    for (auto it = lru_list_.rbegin(); it != lru_list_.rend(); ++it) {
        Frame* frame = *it;
        if (frame->pin_count == 0) {
            lru_list_.remove(frame);
            return frame;
        }
    }

    return nullptr;
}

bool BufferPoolManager::FlushFrame(Frame* frame) {
    if (!frame->page) {
        return false;
    }

    frame->page->SetDirty(frame->is_dirty);
    if (frame->is_dirty && !storage_manager_->WritePage(*frame->page)) {
        return false;
    }

    frame->is_dirty = false;
    return true;
}