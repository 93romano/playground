#include "page.h"

Page::Page(page_id_t page_id) : page_id_(page_id), data_(PAGE_SIZE, 0) {
}