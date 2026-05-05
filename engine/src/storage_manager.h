#pragma once
#include "page.h"
#include <fstream>
#include <string>
#include <memory>

class StorageManager {
public:
    explicit StorageManager(const std::string& db_file);
    ~StorageManager();

    std::unique_ptr<Page> ReadPage(page_id_t page_id);
    bool WritePage(const Page& page);
    page_id_t AllocatePage();

private:
    std::string db_file_;
    std::fstream file_stream_;
    page_id_t next_page_id_{0};
    
    bool OpenFile();
    void CloseFile();
};