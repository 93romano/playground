#include "storage_manager.h"
#include <iostream>

// 생성자 - 서버 초기화
StorageManager::StorageManager(const std::string& db_file) : db_file_(db_file) {
    if (!OpenFile()) {
        std::cerr << "Failed to open database file: " << db_file_ << std::endl;
    }
}

StorageManager::~StorageManager() {
    CloseFile();
}

// OpenFile() - 데이터베이스 연결
bool StorageManager::OpenFile() {
    // 파일 열기
    file_stream_.open(db_file_, std::ios::in | std::ios::out | std::ios::binary);
    
    // 파일이 없으면 새로 생성
    if (!file_stream_.is_open()) {
        // 파일 클리어
        file_stream_.clear();
        // 파일 생성
        file_stream_.open(db_file_, std::ios::out | std::ios::binary);
        file_stream_.close();

        file_stream_.open(db_file_, std::ios::in | std::ios::out | std::ios::binary);
    }
    // 파일이 있으면 파일 크기 확인
    if (file_stream_.is_open()) {
        // 파일 크기 확인
        file_stream_.seekg(0, std::ios::end);
        auto file_size = file_stream_.tellg();
        // 다음 페이지 번호 계산
        next_page_id_ = static_cast<page_id_t>(file_size / PAGE_SIZE);
        // 파일 연결 성공
        return true;
    }
    
    return false;
}

// CloseFile() - 데이터베이스 연결 종료
void StorageManager::CloseFile() {
    // 파일 연결 종료
    if (file_stream_.is_open()) {
        file_stream_.close();
    }
}

// ReadPage() - 데이터 조회
std::unique_ptr<Page> StorageManager::ReadPage(page_id_t page_id) {
    auto page = std::make_unique<Page>(page_id);
    
    // 파일 포인터 이동
    file_stream_.seekg(page_id * PAGE_SIZE, std::ios::beg);
    // 파일 읽기
    file_stream_.read(page->GetData(), PAGE_SIZE);
    // 파일 읽기 크기 확인
    
    if (file_stream_.gcount() != PAGE_SIZE) {
        std::cerr << "Warning: Read less than expected page size" << std::endl;
    }
    
    return page;
}
// WritePage() - 데이터 저장
bool StorageManager::WritePage(const Page& page) {
    // 파일 포인터 이동
    file_stream_.seekp(page.GetPageId() * PAGE_SIZE, std::ios::beg);
    // 파일 쓰기
    file_stream_.write(page.GetData(), PAGE_SIZE);
    // 파일 플러시
    file_stream_.flush();
    // 파일 쓰기 확인
    return file_stream_.good();
}

// AllocatePage() - 새 ID 생성 (자동증가 ID)
page_id_t StorageManager::AllocatePage() {
    // 다음 페이지 번호 증가
    return next_page_id_++;
}