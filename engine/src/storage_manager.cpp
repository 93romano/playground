#include "storage_manager.h"
#include <iostream>

StorageManager::StorageManager(const std::string& db_file) : db_file_(db_file) {
    if (!OpenFile()) {
        std::cerr << "Failed to open database file: " << db_file_ << std::endl;
    }
}

StorageManager::~StorageManager() {
    CloseFile();
}

bool StorageManager::OpenFile() {
    file_stream_.open(db_file_, std::ios::in | std::ios::out | std::ios::binary);
    
    if (!file_stream_.is_open()) {
        file_stream_.clear();
        file_stream_.open(db_file_, std::ios::out | std::ios::binary);
        file_stream_.close();
        file_stream_.open(db_file_, std::ios::in | std::ios::out | std::ios::binary);
    }
    
    if (file_stream_.is_open()) {
        file_stream_.seekg(0, std::ios::end);
        auto file_size = file_stream_.tellg();
        next_page_id_ = static_cast<page_id_t>(file_size / PAGE_SIZE);
        return true;
    }
    
    return false;
}

void StorageManager::CloseFile() {
    if (file_stream_.is_open()) {
        file_stream_.close();
    }
}

std::unique_ptr<Page> StorageManager::ReadPage(page_id_t page_id) {
    auto page = std::make_unique<Page>(page_id);
    
    file_stream_.seekg(page_id * PAGE_SIZE, std::ios::beg);
    file_stream_.read(page->GetData(), PAGE_SIZE);
    
    if (file_stream_.gcount() != PAGE_SIZE) {
        std::cerr << "Warning: Read less than expected page size" << std::endl;
    }
    
    return page;
}

bool StorageManager::WritePage(const Page& page) {
    file_stream_.seekp(page.GetPageId() * PAGE_SIZE, std::ios::beg);
    file_stream_.write(page.GetData(), PAGE_SIZE);
    file_stream_.flush();
    
    return file_stream_.good();
}

page_id_t StorageManager::AllocatePage() {
    return next_page_id_++;
}