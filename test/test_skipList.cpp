#include "skipList.h"
#include <cassert>
#include <chrono>
#include <iostream>
#include <string>
#include <vector>

using namespace dehua_lsm;
using namespace std::chrono;

void test_performance_put(SkipList &list, int count) {
    auto start = high_resolution_clock::now();
    for (int i = 0; i < count; ++i) {
        list.put("key" + std::to_string(i), "val" + std::to_string(i), i);
    }
    auto end = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end - start).count();
    std::cout << "[TIME] Put " << count << " entries took: " << duration << " ms\n";
}

void test_performance_get(SkipList &list, int count) {
    auto start = high_resolution_clock::now();
    for (int i = 0; i < count; i += 10) {
        auto it = list.get("key" + std::to_string(i), i);
        assert(it.is_valid());
        assert(it.get_value() == "val" + std::to_string(i));
    }
    auto end = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end - start).count();
    std::cout << "[TIME] Get " << count / 10 << " entries took: " << duration << " ms\n";
}

void test_performance_remove(SkipList &list, int count) {
    auto start = high_resolution_clock::now();
    for (int i = 0; i < count; i += 2) {
        list.remove("key" + std::to_string(i));
    }
    auto end = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end - start).count();
    std::cout << "[TIME] Remove " << count / 2 << " entries took: " << duration << " ms\n";

    // 验证已删除的部分
    for (int i = 0; i < count; i += 2) {
        auto it = list.get("key" + std::to_string(i), count);
        assert(!it.is_valid());
    }
    // 验证未删除的部分
    for (int i = 1; i < count; i += 2) {
        auto it = list.get("key" + std::to_string(i), count);
        assert(it.is_valid());
        assert(it.get_value() == "val" + std::to_string(i));
    }

    std::cout << "[PASS] Remove correctness check passed.\n";
}

int main() {
    const int count = 10000;
    SkipList list;

    std::cout << "===== SkipList Performance Test =====\n";

    test_performance_put(list, count);
    test_performance_get(list, count);
    test_performance_remove(list, count);

    std::cout << "===== All Tests Passed =====\n";
    return 0;
}
