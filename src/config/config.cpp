#include "spdlog/spdlog.h"
#include "config.h"
#include <fstream>
#include <iostream>
#include <sstream>
#include <mutex>

namespace dehua_lsm {

static std::mutex config_mutex;
static Config* global_config_instance = nullptr;

Config::Config(const std::string& filePath) : config_file_path_(filePath) {
    setDefaultValues();
    loadFromFile(filePath);
}

Config::~Config() {}

void Config::setDefaultValues() {
    lsm_tol_mem_size_limit_ = 67108864;
    lsm_per_mem_size_limit_ = 4194304;
    lsm_block_size_ = 32768;
    lsm_sst_level_ratio_ = 4;

    lsm_block_cache_capacity_ = 1024;
    lsm_block_cache_k_ = 8;

    redis_expire_header_ = "REDIS_EXPIRE_";
    redis_hash_value_preffix_ = "REDIS_HASH_VALUE_";
    redis_field_prefix_ = "REDIS_FIELD_";
    redis_field_separator_ = '$';
    redis_list_separator_ = '#';
    redis_sorted_set_prefix_ = "REDIS_SORTED_SET_";
    redis_sorted_set_score_len_ = 32;
    redis_set_prefix_ = "REDIS_SET_";

    bloom_filter_expected_size_ = 65536;
    bloom_filter_expected_error_rate_ = 0.1;
}

bool Config::loadFromFile(const std::string& filePath) {
    std::ifstream file(filePath);
    if (!file.is_open()) {
        std::cerr << "Failed to open config file: " << filePath << std::endl;
        return false;
    }

    std::string line;
    while (std::getline(file, line)) {
        if (line.empty() || line[0] == '#') continue;

        auto pos = line.find('=');
        if (pos == std::string::npos) continue;

        std::string key = line.substr(0, pos);
        std::string value = line.substr(pos + 1);

        key.erase(0, key.find_first_not_of(" \t"));
        key.erase(key.find_last_not_of(" \t") + 1);
        value.erase(0, value.find_first_not_of(" \t"));
        value.erase(value.find_last_not_of(" \t") + 1);

        // set values manually
        if (key == "LSM_TOL_MEM_SIZE_LIMIT") lsm_tol_mem_size_limit_ = std::stoll(value);
        else if (key == "LSM_PER_MEM_SIZE_LIMIT") lsm_per_mem_size_limit_ = std::stoll(value);
        else if (key == "LSM_BLOCK_SIZE") lsm_block_size_ = std::stoi(value);
        else if (key == "LSM_SST_LEVEL_RATIO") lsm_sst_level_ratio_ = std::stoi(value);

        else if (key == "LSM_BLOCK_CACHE_CAPACITY") lsm_block_cache_capacity_ = std::stoi(value);
        else if (key == "LSM_BLOCK_CACHE_K") lsm_block_cache_k_ = std::stoi(value);

        else if (key == "REDIS_EXPIRE_HEADER") redis_expire_header_ = value;
        else if (key == "REDIS_HASH_VALUE_PREFFIX") redis_hash_value_preffix_ = value;
        else if (key == "REDIS_FIELD_PREFIX") redis_field_prefix_ = value;
        else if (key == "REDIS_FIELD_SEPARATOR") redis_field_separator_ = value[0];
        else if (key == "REDIS_LIST_SEPARATOR") redis_list_separator_ = value[0];
        else if (key == "REDIS_SORTED_SET_PREFIX") redis_sorted_set_prefix_ = value;
        else if (key == "REDIS_SORTED_SET_SCORE_LEN") redis_sorted_set_score_len_ = std::stoi(value);
        else if (key == "REDIS_SET_PREFIX") redis_set_prefix_ = value;

        else if (key == "BLOOM_FILTER_EXPECTED_SIZE") bloom_filter_expected_size_ = std::stoi(value);
        else if (key == "BLOOM_FILTER_EXPECTED_ERROR_RATE") bloom_filter_expected_error_rate_ = std::stod(value);
    }

    return true;
}

bool Config::saveToFile(const std::string& filePath) {
    std::ofstream file(filePath);
    if (!file.is_open()) return false;
    file << "# Generated config file\n";
    file << "LSM_TOL_MEM_SIZE_LIMIT = " << lsm_tol_mem_size_limit_ << "\n";
    file << "LSM_PER_MEM_SIZE_LIMIT = " << lsm_per_mem_size_limit_ << "\n";
    file << "LSM_BLOCK_SIZE = " << lsm_block_size_ << "\n";
    file << "LSM_SST_LEVEL_RATIO = " << lsm_sst_level_ratio_ << "\n";
    file << "LSM_BLOCK_CACHE_CAPACITY = " << lsm_block_cache_capacity_ << "\n";
    file << "LSM_BLOCK_CACHE_K = " << lsm_block_cache_k_ << "\n";
    file << "REDIS_EXPIRE_HEADER = " << redis_expire_header_ << "\n";
    file << "REDIS_HASH_VALUE_PREFFIX = " << redis_hash_value_preffix_ << "\n";
    file << "REDIS_FIELD_PREFIX = " << redis_field_prefix_ << "\n";
    file << "REDIS_FIELD_SEPARATOR = " << redis_field_separator_ << "\n";
    file << "REDIS_LIST_SEPARATOR = " << redis_list_separator_ << "\n";
    file << "REDIS_SORTED_SET_PREFIX = " << redis_sorted_set_prefix_ << "\n";
    file << "REDIS_SORTED_SET_SCORE_LEN = " << redis_sorted_set_score_len_ << "\n";
    file << "REDIS_SET_PREFIX = " << redis_set_prefix_ << "\n";
    file << "BLOOM_FILTER_EXPECTED_SIZE = " << bloom_filter_expected_size_ << "\n";
    file << "BLOOM_FILTER_EXPECTED_ERROR_RATE = " << bloom_filter_expected_error_rate_ << "\n";
    return true;
}

// Getters
long long Config::getLsmTolMemSizeLimit() const { return lsm_tol_mem_size_limit_; }
long long Config::getLsmPerMemSizeLimit() const { return lsm_per_mem_size_limit_; }
int Config::getLsmBlockSize() const { return lsm_block_size_; }
int Config::getLsmSstLevelRatio() const { return lsm_sst_level_ratio_; }

int Config::getLsmBlockCacheCapacity() const { return lsm_block_cache_capacity_; }
int Config::getLsmBlockCacheK() const { return lsm_block_cache_k_; }

const std::string& Config::getRedisExpireHeader() const { return redis_expire_header_; }
const std::string& Config::getRedisHashValuePreffix() const { return redis_hash_value_preffix_; }
const std::string& Config::getRedisFieldPrefix() const { return redis_field_prefix_; }
char Config::getRedisFieldSeparator() const { return redis_field_separator_; }
char Config::getRedisListSeparator() const { return redis_list_separator_; }
const std::string& Config::getRedisSortedSetPrefix() const { return redis_sorted_set_prefix_; }
int Config::getRedisSortedSetScoreLen() const { return redis_sorted_set_score_len_; }
const std::string& Config::getRedisSetPrefix() const { return redis_set_prefix_; }

int Config::getBloomFilterExpectedSize() const { return bloom_filter_expected_size_; }
double Config::getBloomFilterExpectedErrorRate() const { return bloom_filter_expected_error_rate_; }

const Config& Config::getInstance(const std::string& config_path) {
    std::lock_guard<std::mutex> lock(config_mutex);
    if (!global_config_instance) {
        global_config_instance = new Config(config_path);
    }
    return *global_config_instance;
}

} // namespace dehua_lsm
