#pragma once
#include "page.h"
#include "storage_manager.h"
#include <unordered_map>
#include <list>
#include <memory>

class BufferPoolManager {
public:
    explicit BufferPoolManager(size_t pool_size, StorageManager* storage_manager);
    ~BufferPoolManager() = default;

    Page* FetchPage(page_id_t page_id);
    bool UnpinPage(page_id_t page_id, bool is_dirty);
    bool FlushPage(page_id_t page_id);
    Page* NewPage(page_id_t* page_id);
    bool DeletePage(page_id_t page_id);

private:
    struct Frame {
        std::unique_ptr<Page> page;
        int pin_count{0};
        bool is_dirty{false};
    };

    size_t pool_size_;
    StorageManager* storage_manager_;
    std::unordered_map<page_id_t, Frame*> page_table_;
    std::vector<std::unique_ptr<Frame>> frames_;
    std::list<Frame*> free_list_;
    std::list<Frame*> lru_list_;

    Frame* GetVictimFrame();
    bool FlushFrame(Frame* frame);
};