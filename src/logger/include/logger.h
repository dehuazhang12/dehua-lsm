#ifndef LOGGER_H
#define LOGGER_H
#include <string>

namespace dehua_lsm {

void init_spdlog_file();
void reset_log_level(const std::string &level);

} // namespace dehua_lsm
#endif // LOGGER_H