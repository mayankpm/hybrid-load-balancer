// Minimal dependency-free test harness.
#pragma once

#include <chrono>
#include <filesystem>
#include <functional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

namespace lbtest {

struct TestCase {
  std::string name;
  std::function<void()> fn;
};

inline std::vector<TestCase>& Registry() {
  static std::vector<TestCase> tests;
  return tests;
}

struct Registrar {
  Registrar(const char* suite, const char* name, std::function<void()> fn) {
    Registry().push_back({std::string(suite) + "." + name, std::move(fn)});
  }
};

struct Failure : std::runtime_error {
  using std::runtime_error::runtime_error;
};

// Enums have no operator<<; print their underlying value instead.
template <typename T>
decltype(auto) Printable(const T& v) {
  if constexpr (std::is_enum_v<T>) {
    return static_cast<long long>(v);
  } else {
    return (v);
  }
}

template <typename A, typename B>
void CheckEq(const A& a, const B& b, const char* ea, const char* eb, const char* file, int line) {
  if (!(a == b)) {
    std::ostringstream os;
    os << file << ":" << line << ": CHECK_EQ(" << ea << ", " << eb << ") failed: " << Printable(a) << " vs " << Printable(b);
    throw Failure(os.str());
  }
}

// Scratch directory removed when the test finishes.
class TempDir {
 public:
  TempDir() {
    static int counter = 0;
    auto base = std::filesystem::temp_directory_path() / "hybridlb-tests";
    path_ = (base / (std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + "-" +
                     std::to_string(counter++)))
                .string();
    std::filesystem::create_directories(path_);
  }
  ~TempDir() {
    std::error_code ec;
    std::filesystem::remove_all(path_, ec);
  }
  const std::string& path() const { return path_; }
  std::string sub(const std::string& name) const { return path_ + "/" + name; }

 private:
  std::string path_;
};

inline void SleepMs(int ms) { std::this_thread::sleep_for(std::chrono::milliseconds(ms)); }

}  // namespace lbtest

#define LB_CONCAT_(a, b) a##b
#define LB_CONCAT(a, b) LB_CONCAT_(a, b)

#define TEST(suite, name)                                                                   \
  static void suite##_##name();                                                             \
  static ::lbtest::Registrar LB_CONCAT(reg_, __LINE__)(#suite, #name, suite##_##name);      \
  static void suite##_##name()

#define CHECK(cond)                                                                                      \
  do {                                                                                                   \
    if (!(cond)) throw ::lbtest::Failure(std::string(__FILE__) + ":" + std::to_string(__LINE__) + \
                                         ": CHECK(" #cond ") failed");                                  \
  } while (0)

#define CHECK_EQ(a, b) ::lbtest::CheckEq((a), (b), #a, #b, __FILE__, __LINE__)
