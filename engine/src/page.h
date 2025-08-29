#pragma once
#include <cstdint>
#include <vector>

constexpr size_t PAGE_SIZE = 4096;  // 4KB pages
using page_id_t = uint32_t;

class Page {
public:
    Page(page_id_t page_id);
    ~Page() = default;

    page_id_t GetPageId() const { return page_id_; }
    char* GetData() { return data_.data(); }
    const char* GetData() const { return data_.data(); }
    
    bool IsDirty() const { return is_dirty_; }
    void SetDirty(bool dirty) { is_dirty_ = dirty; }

private:
    page_id_t page_id_;
    std::vector<char> data_;
    bool is_dirty_{false};
};