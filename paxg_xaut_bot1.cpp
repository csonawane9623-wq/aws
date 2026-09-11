// ============================================================
//        PAXG / XAUT SPREAD ARBITRAGE BOT - FAST C++
//        Delta Exchange India
//
// Key latency improvements vs. the original version:
//   1) Current public WS endpoint + ob_l1 (100 ms) feed.
//   2) No position REST call on every ticker/update.
//   3) Background position/balance reconciliation only.
//   4) Leverage is prepared at startup, never in the hot path.
//   5) PAXG and XAUT orders are sent concurrently.
//   6) Exit orders are sent concurrently and reduce_only=true.
//   7) No immediate post-trade position REST lookup unless explicitly enabled.
//   8) Lightweight L1 market state; no full orderbook copies in evaluate().
//   9) Reusable CURL easy handles for lower repeated REST overhead.
//  10) WebSocket callback only updates market state and evaluates logic.
//
// Build (MSYS2 UCRT64):
//   g++ -O3 -DNDEBUG -std=c++17 paxg_xaut_fast.cpp \
//       -I/ucrt64/include \
//       -L/ucrt64/lib \
//       -lixwebsocket -lcurl -lssl -lcrypto -lz -lws2_32 -lcrypt32 \
//       -o paxg_xaut_fast.exe
//
// .env:
//   API_KEY6=your_api_key
//   API_SECRET6=your_api_secret
//   TELEGRAM_TOKEN=your_token       (optional)
//   TELEGRAM_CHAT_ID=your_chat_id   (optional)
//   CA_BUNDLE_PATH=/ucrt64/etc/ssl/certs/ca-bundle.crt (optional)
// ============================================================

#include <iostream>
#include <fstream>
#include <string>
#include <unordered_map>
#include <vector>
#include <optional>
#include <sstream>
#include <iomanip>
#include <chrono>
#include <thread>
#include <mutex>
#include <atomic>
#include <condition_variable>
#include <queue>
#include <cmath>
#include <ctime>
#include <csignal>
#include <stdexcept>
#include <random>
#include <algorithm>
#include <future>
#include <functional>
#include <cstdlib>
#include <cstring>

#include <nlohmann/json.hpp>
#include <curl/curl.h>
#include <openssl/hmac.h>
#include <openssl/evp.h>
#include <ixwebsocket/IXWebSocket.h>

#ifdef _WIN32
#include <windows.h>
#endif

using json = nlohmann::json;

// ============================================================
// CONFIG
// ============================================================

static std::string API_KEY;
static std::string API_SECRET;
static std::string TELEGRAM_TOKEN;
static std::string TELEGRAM_CHAT_ID;

static constexpr const char* BASE_URL = "https://api.india.delta.exchange";
static constexpr const char* WS_URL   = "wss://public-socket.india.delta.exchange";

static constexpr const char* SYMBOL_1 = "PAXGUSD";
static constexpr const char* SYMBOL_2 = "XAUTUSD";

// Strategy: preserved from original bot.
static constexpr double ENTRY_SPREAD = 0.40;
static constexpr double EXIT_SPREAD  = 1.60;

// Risk: preserved from original bot.
static constexpr int    LEVERAGE        = 100;
static constexpr double CAPITAL_PERCENT = 1.0;   // 1% of available USD balance

// Execution / safety.
static constexpr double MAX_SLIPPAGE             = 0.0025; // 0.25%
static constexpr double MIN_TOP_OF_BOOK_USD      = 1500.0;
static constexpr int    BALANCE_REFRESH_SEC      = 30;
static constexpr int    POSITION_REFRESH_SEC     = 15;
static constexpr int    DASHBOARD_REFRESH_MS     = 250;
static constexpr int    PNL_UPDATE_INTERVAL_SEC  = 300;
static constexpr int    MARKET_DATA_STALE_MS     = 1500;
static constexpr int    TRADE_COOLDOWN_SEC       = 0;

// Set true only if you want an extra REST reconciliation immediately after
// an order response that is missing average_fill_price. It is disabled for speed.
static constexpr bool ENABLE_POST_TRADE_POSITION_LOOKUP = false;

static constexpr int    WS_CONNECT_TIMEOUT_SEC = 10;
static constexpr int    WS_RECONNECT_BASE_SEC  = 1;
static constexpr int    WS_RECONNECT_MAX_SEC   = 60;

static const std::string USER_AGENT =
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36";

// ============================================================
// TIME / LOGGING
// ============================================================

static long long now_sec()
{
    using namespace std::chrono;
    return duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
}

static long long now_ms()
{
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

static std::mutex g_log_mutex;

static void log_line(const char* level, const std::string& msg)
{
    std::lock_guard<std::mutex> lk(g_log_mutex);
    std::time_t t = std::time(nullptr);
    char buf[32]{};
    std::tm tm{};
#ifdef _WIN32
    localtime_s(&tm, &t);
#else
    localtime_r(&t, &tm);
#endif
    std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm);
    std::cout << buf << " [" << level << "] " << msg << '\n';
}

static void log_info(const std::string& m)  { log_line("INFO ", m); }
static void log_warn(const std::string& m)  { log_line("WARN ", m); }
static void log_error(const std::string& m) { log_line("ERROR", m); }

// ============================================================
// ENV
// ============================================================

static void trim(std::string& s)
{
    const auto first = s.find_first_not_of(" \t\r\n");
    if (first == std::string::npos) { s.clear(); return; }
    const auto last = s.find_last_not_of(" \t\r\n");
    s = s.substr(first, last - first + 1);
}

static void load_dotenv(const std::string& path = ".env")
{
    std::ifstream file(path);
    if (!file.is_open()) return;

    std::string line;
    while (std::getline(file, line))
    {
        trim(line);
        if (line.empty() || line[0] == '#') continue;

        const auto eq = line.find('=');
        if (eq == std::string::npos) continue;

        std::string key = line.substr(0, eq);
        std::string val = line.substr(eq + 1);
        trim(key);
        trim(val);

        if (val.size() >= 2 &&
            ((val.front() == '"' && val.back() == '"') ||
             (val.front() == '\'' && val.back() == '\'')))
        {
            val = val.substr(1, val.size() - 2);
        }

#ifdef _WIN32
        if (!std::getenv(key.c_str())) _putenv_s(key.c_str(), val.c_str());
#else
        if (!std::getenv(key.c_str())) setenv(key.c_str(), val.c_str(), 0);
#endif
    }
}

static std::string require_env(const char* name)
{
    const char* v = std::getenv(name);
    if (!v || !*v)
        throw std::runtime_error(std::string("Missing environment variable: ") + name);
    return std::string(v);
}

static std::string optional_env(const char* name, const std::string& def = "")
{
    const char* v = std::getenv(name);
    return (v && *v) ? std::string(v) : def;
}

// ============================================================
// JSON / CRYPTO
// ============================================================

static double json_to_double(const json& v)
{
    if (v.is_string()) return std::stod(v.get<std::string>());
    if (v.is_number()) return v.get<double>();
    throw std::runtime_error("Expected numeric JSON value: " + v.dump());
}

static std::string json_to_string(const json& v)
{
    if (v.is_string())  return v.get<std::string>();
    if (v.is_number())  return std::to_string(v.get<double>());
    if (v.is_boolean()) return v.get<bool>() ? "true" : "false";
    return v.dump();
}

static std::string hmac_sha256_hex(const std::string& key, const std::string& data)
{
    unsigned char digest[EVP_MAX_MD_SIZE]{};
    unsigned int digest_len = 0;
    HMAC(EVP_sha256(),
         reinterpret_cast<const unsigned char*>(key.data()), static_cast<int>(key.size()),
         reinterpret_cast<const unsigned char*>(data.data()), static_cast<int>(data.size()),
         digest, &digest_len);

    static const char hex[] = "0123456789abcdef";
    std::string out;
    out.resize(digest_len * 2);
    for (unsigned int i = 0; i < digest_len; ++i)
    {
        out[2 * i]     = hex[(digest[i] >> 4) & 0xF];
        out[2 * i + 1] = hex[digest[i] & 0xF];
    }
    return out;
}

static std::string random_hex(std::size_t len = 24)
{
    static thread_local std::mt19937_64 rng{std::random_device{}()};
    static constexpr char hex[] = "0123456789abcdef";
    std::uniform_int_distribution<int> d(0, 15);
    std::string out;
    out.reserve(len);
    for (std::size_t i = 0; i < len; ++i) out.push_back(hex[d(rng)]);
    return out;
}

static size_t curl_write_cb(char* ptr, size_t size, size_t nmemb, void* userdata)
{
    auto* buf = static_cast<std::string*>(userdata);
    buf->append(ptr, size * nmemb);
    return size * nmemb;
}

// ============================================================
// CURL TLS / CONNECTION REUSE
// ============================================================

class CurlSession
{
public:
    CurlSession()
    {
        curl_ = curl_easy_init();
        if (!curl_) throw std::runtime_error("curl_easy_init failed");
    }

    ~CurlSession()
    {
        if (curl_) curl_easy_cleanup(curl_);
    }

    CurlSession(const CurlSession&) = delete;
    CurlSession& operator=(const CurlSession&) = delete;

    CURL* get() const { return curl_; }

private:
    CURL* curl_{nullptr};
};

static CURL* thread_curl()
{
    thread_local CurlSession session;
    return session.get();
}

// ============================================================
// ASYNC TELEGRAM
// ============================================================

class SingleWorkerQueue
{
public:
    explicit SingleWorkerQueue(std::size_t max_queue = 8)
        : max_queue_(max_queue)
    {
        worker_ = std::thread([this] { run(); });
    }

    ~SingleWorkerQueue()
    {
        {
            std::lock_guard<std::mutex> lk(mu_);
            stop_ = true;
        }
        cv_.notify_one();
        if (worker_.joinable()) worker_.join();
    }

    bool post(std::function<void()> task)
    {
        {
            std::lock_guard<std::mutex> lk(mu_);
            if (stop_ || queue_.size() >= max_queue_) return false;
            queue_.push(std::move(task));
        }
        cv_.notify_one();
        return true;
    }

private:
    void run()
    {
        for (;;)
        {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lk(mu_);
                cv_.wait(lk, [this] { return stop_ || !queue_.empty(); });
                if (stop_ && queue_.empty()) return;
                task = std::move(queue_.front());
                queue_.pop();
            }

            try { task(); }
            catch (const std::exception& e) { log_error(std::string("Worker error: ") + e.what()); }
            catch (...) { log_error("Worker error: unknown exception"); }
        }
    }

    std::size_t max_queue_;
    std::mutex mu_;
    std::condition_variable cv_;
    std::queue<std::function<void()>> queue_;
    std::thread worker_;
    bool stop_{false};
};

static std::atomic<long long> g_last_telegram_sec{0};
static SingleWorkerQueue* g_telegram_queue = nullptr;

static void send_telegram_impl(const std::string& msg)
{
    if (TELEGRAM_TOKEN.empty() || TELEGRAM_CHAT_ID.empty()) return;

    const long long now = now_sec();
    const long long prev = g_last_telegram_sec.load(std::memory_order_relaxed);
    if (now - prev < 2) return;
    g_last_telegram_sec.store(now, std::memory_order_relaxed);

    CURL* curl = thread_curl();
    if (!curl) return;

    const std::string url =
        std::string("https://api.telegram.org/bot") + TELEGRAM_TOKEN + "/sendMessage";
    const json body = {{"chat_id", TELEGRAM_CHAT_ID}, {"text", msg}};
    const std::string body_str = body.dump();
    std::string response;

    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    headers = curl_slist_append(headers, ("User-Agent: " + USER_AGENT).c_str());

    curl_easy_reset(curl);
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body_str.c_str());
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, static_cast<long>(body_str.size()));
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, 5000L);
    curl_easy_setopt(curl, CURLOPT_TCP_NODELAY, 1L);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2TLS);
    curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);
    curl_easy_setopt(curl, CURLOPT_TCP_KEEPIDLE, 30L);
    curl_easy_setopt(curl, CURLOPT_TCP_KEEPINTVL, 10L);

    const CURLcode rc = curl_easy_perform(curl);
    if (rc != CURLE_OK)
        log_error(std::string("Telegram CURL: ") + curl_easy_strerror(rc));

    curl_slist_free_all(headers);
}

static void send_telegram(const std::string& msg)
{
    if (g_telegram_queue)
        g_telegram_queue->post([msg] { send_telegram_impl(msg); });
}

// ============================================================
// DELTA REST CLIENT
// ============================================================

class DeltaClient
{
public:
    DeltaClient(std::string key, std::string secret)
        : key_(std::move(key)), secret_(std::move(secret)) {}

    std::pair<std::string, std::string> sign(const std::string& method,
                                              const std::string& path,
                                              const std::string& query,
                                              const std::string& payload) const
    {
        const std::string timestamp = std::to_string(now_sec());
        const std::string data = method + timestamp + path + query + payload;
        return {hmac_sha256_hex(secret_, data), timestamp};
    }

    std::optional<json> request(const std::string& method,
                                const std::string& path,
                                const json& data = json{},
                                bool auth = false,
                                const std::string& query = "") const
    {
        const std::string payload =
            (data.is_null() || data.empty()) ? std::string{} : data.dump();

        const std::string url = std::string(BASE_URL) + path + query;
        CURL* curl = thread_curl();
        if (!curl) return std::nullopt;

        struct curl_slist* headers = nullptr;
        headers = curl_slist_append(headers, "Content-Type: application/json");
        headers = curl_slist_append(headers, "Accept: application/json");
        headers = curl_slist_append(headers, ("User-Agent: " + USER_AGENT).c_str());

        if (auth)
        {
            const auto [signature, timestamp] = sign(method, path, query, payload);
            headers = curl_slist_append(headers, ("api-key: " + key_).c_str());
            headers = curl_slist_append(headers, ("timestamp: " + timestamp).c_str());
            headers = curl_slist_append(headers, ("signature: " + signature).c_str());
        }

        std::string response;
        curl_easy_reset(curl);
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_cb);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
        curl_easy_setopt(curl, CURLOPT_USERAGENT, USER_AGENT.c_str());
        curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, 10000L);
        curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, 2500L);
        curl_easy_setopt(curl, CURLOPT_TCP_NODELAY, 1L);
        curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
        curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2TLS);
        curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);
        curl_easy_setopt(curl, CURLOPT_TCP_KEEPIDLE, 30L);
        curl_easy_setopt(curl, CURLOPT_TCP_KEEPINTVL, 10L);

        if (method == "POST")
        {
            curl_easy_setopt(curl, CURLOPT_POST, 1L);
            curl_easy_setopt(curl, CURLOPT_POSTFIELDS, payload.c_str());
            curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, static_cast<long>(payload.size()));
        }
        else
        {
            curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
        }

        const CURLcode rc = curl_easy_perform(curl);
        curl_slist_free_all(headers);

        if (rc != CURLE_OK)
        {
            log_error(std::string("REST CURL: ") + curl_easy_strerror(rc) + " path=" + path);
            return std::nullopt;
        }

        try { return json::parse(response); }
        catch (const std::exception& e)
        {
            log_error(std::string("REST JSON parse error: ") + e.what());
            return std::nullopt;
        }
    }

    std::optional<json> get_product(const std::string& symbol) const
    {
        return request("GET", "/v2/products/" + symbol);
    }

    std::optional<json> get_balance() const
    {
        return request("GET", "/v2/wallet/balances", {}, true);
    }

    // Delta /v2/positions is the real-time position endpoint and requires
    // either product_id or underlying_asset_symbol.
    std::optional<json> get_position(int product_id) const
    {
        return request(
            "GET",
            "/v2/positions",
            {},
            true,
            "?product_id=" + std::to_string(product_id)
        );
    }

    std::optional<json> set_leverage(int product_id, int leverage) const
    {
        const json body = {{"leverage", std::to_string(leverage)}};
        return request("POST",
                       "/v2/products/" + std::to_string(product_id) + "/orders/leverage",
                       body, true);
    }

    std::optional<json> place_market_order(int product_id,
                                           const std::string& side,
                                           int size,
                                           bool reduce_only = false) const
    {
        const json body = {
            {"product_id", product_id},
            {"size", size},
            {"side", side},
            {"order_type", "market_order"},
            {"reduce_only", reduce_only},
            {"client_order_id", random_hex(24)}
        };
        return request("POST", "/v2/orders", body, true);
    }

    bool validate_credentials() const
    {
        auto res = get_balance();
        if (!res || !res->value("success", false))
        {
            log_error("Credential validation failed.");
            if (res) log_error(res->dump());
            return false;
        }
        log_info("API credentials validated.");
        return true;
    }

private:
    std::string key_;
    std::string secret_;
};

// ============================================================
// BOT MARKET STATE
// ============================================================

struct Quote
{
    double bid{0.0};
    double ask{0.0};
    double bid_size{0.0};
    double ask_size{0.0};
    long long ts_ms{0};

    bool valid() const
    {
        return bid > 0.0 && ask > 0.0 && ask >= bid && ts_ms > 0;
    }

    double mid() const { return (bid + ask) * 0.5; }

    double l1_bid_usd(double contract_value) const
    {
        return bid * bid_size * contract_value;
    }

    double l1_ask_usd(double contract_value) const
    {
        return ask * ask_size * contract_value;
    }
};

struct ProductInfo
{
    int id{0};
    double contract_value{0.0};
    int position_size_limit{0};
    std::string trading_status;
};

static std::optional<double> extract_fill_price(const std::optional<json>& response)
{
    if (!response || !response->contains("result")) return std::nullopt;
    const json& result = (*response)["result"];
    if (!result.is_object()) return std::nullopt;
    if (!result.contains("average_fill_price") || result["average_fill_price"].is_null())
        return std::nullopt;
    try { return json_to_double(result["average_fill_price"]); }
    catch (...) { return std::nullopt; }
}

static bool response_success(const std::optional<json>& response)
{
    return response && response->value("success", false);
}


static std::string response_error(const std::optional<json>& response)
{
    if (!response)
        return "no HTTP response";

    try
    {
        if (response->contains("error"))
        {
            const json& err = (*response)["error"];

            if (err.is_object())
            {
                const std::string code =
                    err.contains("code") ? json_to_string(err["code"]) : "";
                const std::string msg =
                    err.contains("message") ? json_to_string(err["message"]) : "";

                if (!code.empty() && !msg.empty())
                    return code + ": " + msg;
                if (!code.empty())
                    return code;
                if (!msg.empty())
                    return msg;
            }
        }
    }
    catch (...) {}

    return response->dump();
}

// ============================================================
// BOT
// ============================================================

class PAXGXAUTBot
{
public:
    PAXGXAUTBot()
        : client_(API_KEY, API_SECRET),
          telegram_queue_(8)
    {
        g_telegram_queue = &telegram_queue_;
        load_products();

        // Critical for latency: do this BEFORE the market feed starts.
        prepare_leverage();

        refresh_balance_cache();
        recover_positions_on_startup();

        log_info("Fast bot initialized.");
        log_info("WS: public-socket.india.delta.exchange / ob_l1");
    }

    ~PAXGXAUTBot()
    {
        running_.store(false, std::memory_order_release);
        if (sync_thread_.joinable()) sync_thread_.join();
        if (g_telegram_queue == &telegram_queue_) g_telegram_queue = nullptr;
    }

    void run()
    {
        start_background_sync();

        int backoff = WS_RECONNECT_BASE_SEC;
        while (running_.load(std::memory_order_acquire))
        {
            try
            {
                websocket_loop();
                backoff = WS_RECONNECT_BASE_SEC;
            }
            catch (const std::exception& e)
            {
                log_error(std::string("WebSocket exception: ") + e.what());
                send_telegram(std::string("WebSocket exception\n") + e.what());
            }

            if (!running_.load(std::memory_order_acquire)) break;

            log_warn("Reconnecting in " + std::to_string(backoff) + "s...");
            std::this_thread::sleep_for(std::chrono::seconds(backoff));
            backoff = std::min(backoff * 2, WS_RECONNECT_MAX_SEC);
        }
    }

private:
    DeltaClient client_;
    SingleWorkerQueue telegram_queue_;

    std::unordered_map<std::string, ProductInfo> product_;

    Quote quote1_;
    Quote quote2_;
    mutable std::mutex market_mu_;

    std::atomic<bool> positions_open_{false};
    std::atomic<bool> trade_in_flight_{false};
    std::atomic<bool> running_{true};

    double cached_balance_{0.0};
    long long last_balance_refresh_{0};
    mutable std::mutex balance_mu_;

    // Position bookkeeping.
    std::string entry_direction_;
    double entry_price_1_{0.0};
    double entry_price_2_{0.0};
    int entry_size1_{0};
    int entry_size2_{0};
    double entry_spread_{0.0};
    long long entry_time_{0};
    std::atomic<long long> last_exit_time_{0};
    std::atomic<long long> last_pnl_update_{0};
    mutable std::mutex position_mu_;

    long long last_dashboard_ms_{0};

    std::thread sync_thread_;
    std::atomic<bool> sync_started_{false};

    std::unordered_map<std::string, bool> leverage_ready_;

    // --------------------------------------------------------
    // Product loading
    // --------------------------------------------------------
    void load_products()
    {
        for (const char* symbol : {SYMBOL_1, SYMBOL_2})
        {
            auto data = client_.get_product(symbol);
            if (!response_success(data))
                throw std::runtime_error(std::string("Cannot load product: ") + symbol);

            const json& p = (*data)["result"];
            ProductInfo info;
            info.id = p.at("id").get<int>();
            info.contract_value = json_to_double(p.at("contract_value"));
            info.position_size_limit = p.at("position_size_limit").get<int>();
            info.trading_status = json_to_string(p.at("trading_status"));
            product_[symbol] = info;

            log_info(std::string("Loaded ") + symbol +
                     " id=" + std::to_string(info.id) +
                     " contract_value=" + std::to_string(info.contract_value));
        }
    }

    // --------------------------------------------------------
    // Startup leverage
    // --------------------------------------------------------
    void prepare_leverage()
    {
        std::vector<std::future<bool>> futures;
        futures.reserve(2);

        for (const char* symbol : {SYMBOL_1, SYMBOL_2})
        {
            const int pid = product_.at(symbol).id;
            futures.push_back(std::async(std::launch::async, [this, symbol, pid]
            {
                auto r = client_.set_leverage(pid, LEVERAGE);
                const bool ok = response_success(r);
                if (ok) log_info(std::string("Leverage ready: ") + symbol + " x" + std::to_string(LEVERAGE));
                else   log_error(std::string("Failed to set leverage: ") + symbol);
                return ok;
            }));
        }

        for (std::size_t i = 0; i < futures.size(); ++i)
        {
            const bool ok = futures[i].get();
            leverage_ready_[i == 0 ? SYMBOL_1 : SYMBOL_2] = ok;
            if (!ok) throw std::runtime_error("Leverage initialization failed.");
        }
    }

    // --------------------------------------------------------
    // Balance cache
    // --------------------------------------------------------
    void refresh_balance_cache()
    {
        auto data = client_.get_balance();
        if (!response_success(data) || !data->contains("result") || !(*data)["result"].is_array())
            return;

        for (const auto& asset : (*data)["result"])
        {
            if (!asset.contains("asset_symbol") || asset["asset_symbol"].is_null()) continue;
            if (json_to_string(asset["asset_symbol"]) != "USD") continue;
            if (!asset.contains("available_balance") || asset["available_balance"].is_null()) continue;

            const double bal = json_to_double(asset["available_balance"]);
            {
                std::lock_guard<std::mutex> lk(balance_mu_);
                cached_balance_ = bal;
                last_balance_refresh_ = now_sec();
            }
            return;
        }
    }

    double balance_snapshot() const
    {
        std::lock_guard<std::mutex> lk(balance_mu_);
        return cached_balance_;
    }

    // --------------------------------------------------------
    // Position REST reconciliation (background only)
    // --------------------------------------------------------
    // --------------------------------------------------------
    // Position REST reconciliation
    //
    // Delta's GET /v2/positions requires product_id and returns
    // the latest position for that product.
    //
    // We query PAXG and XAUT concurrently. nullopt means the API
    // request failed; an empty vector means the account is confirmed flat.
    // --------------------------------------------------------
    std::optional<std::vector<json>> fetch_open_positions_realtime()
    {
        const int pid1 = product_.at(SYMBOL_1).id;
        const int pid2 = product_.at(SYMBOL_2).id;

        auto f1 = std::async(std::launch::async, [this, pid1]()
        {
            return client_.get_position(pid1);
        });

        auto f2 = std::async(std::launch::async, [this, pid2]()
        {
            return client_.get_position(pid2);
        });

        const auto r1 = f1.get();
        const auto r2 = f2.get();

        if (!response_success(r1) || !response_success(r2))
        {
            log_error("Position reconciliation API failure.");
            if (r1 && !response_success(r1))
                log_error("PAXG position response: " + r1->dump());
            if (r2 && !response_success(r2))
                log_error("XAUT position response: " + r2->dump());
            return std::nullopt;
        }

        std::vector<json> open;
        open.reserve(2);

        auto append_if_open = [&](const std::optional<json>& response,
                                  const char* symbol)
        {
            if (!response || !response->contains("result")) return;

            const json& result = (*response)["result"];
            if (!result.is_object())
            {
                log_warn(std::string("Unexpected position result for ") +
                         symbol + ": " + result.dump());
                return;
            }

            int size = 0;
            try
            {
                if (result.contains("size") && !result["size"].is_null())
                    size = result["size"].get<int>();
            }
            catch (...)
            {
                return;
            }

            if (size != 0)
            {
                json p = result;
                p["product_id"] = product_.at(symbol).id;
                p["product_symbol"] = symbol;
                open.push_back(std::move(p));
            }
        };

        append_if_open(r1, SYMBOL_1);
        append_if_open(r2, SYMBOL_2);

        return open;
    }

    std::vector<json> fetch_open_positions()
    {
        const auto positions = fetch_open_positions_realtime();
        return positions ? *positions : std::vector<json>{};
    }

    void background_sync_loop()
    {
        auto next_balance = std::chrono::steady_clock::now();
        auto next_position = std::chrono::steady_clock::now();

        while (running_.load(std::memory_order_acquire))
        {
            const auto now = std::chrono::steady_clock::now();

            if (now >= next_balance)
            {
                refresh_balance_cache();
                next_balance = now + std::chrono::seconds(BALANCE_REFRESH_SEC);
            }

            if (now >= next_position)
            {
                // Never let a background reconciliation overwrite state while
                // a trade is actively being executed.
                if (!trade_in_flight_.load(std::memory_order_acquire))
                {
                    const auto positions = fetch_open_positions_realtime();

                    if (positions)
                    {
                        const bool is_open = !positions->empty();
                        positions_open_.store(is_open, std::memory_order_release);

                        log_info(std::string("Position sync: ") +
                                 (is_open ? "OPEN" : "FLAT"));
                    }
                    else
                    {
                        // NEVER turn an active local position into FLAT
                        // merely because the REST request failed.
                        log_warn("Position sync failed; preserving current state.");
                    }
                }
                next_position = now + std::chrono::seconds(POSITION_REFRESH_SEC);
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

    void start_background_sync()
    {
        bool expected = false;
        if (sync_started_.compare_exchange_strong(expected, true))
            sync_thread_ = std::thread([this] { background_sync_loop(); });
    }

    // --------------------------------------------------------
    // Startup recovery
    // --------------------------------------------------------
    void recover_positions_on_startup()
    {
        const auto positions = fetch_open_positions();
        if (positions.empty()) return;

        bool has1 = false;
        bool has2 = false;
        int size1 = 0;
        int size2 = 0;
        double price1 = 0.0;
        double price2 = 0.0;

        for (const auto& p : positions)
        {
            if (!p.contains("product_id")) continue;
            const int pid = p["product_id"].get<int>();
            const int sz  = p.value("size", 0);
            const double ep = (p.contains("entry_price") && !p["entry_price"].is_null())
                                ? json_to_double(p["entry_price"])
                                : 0.0;

            if (pid == product_[SYMBOL_1].id)
            {
                has1 = sz != 0;
                size1 = sz;
                price1 = ep;
            }
            if (pid == product_[SYMBOL_2].id)
            {
                has2 = sz != 0;
                size2 = sz;
                price2 = ep;
            }
        }

        if (has1 && has2)
        {
            std::lock_guard<std::mutex> lk(position_mu_);
            positions_open_.store(true, std::memory_order_release);
            entry_time_ = now_sec();
            entry_price_1_ = price1;
            entry_price_2_ = price2;
            entry_size1_ = std::abs(size1);
            entry_size2_ = std::abs(size2);
            entry_spread_ = std::abs(price1 - price2);
            entry_direction_ = (size1 < 0) ? "SHORT_PAXG" : "LONG_PAXG";
            last_pnl_update_.store(entry_time_, std::memory_order_relaxed);

            log_warn("Recovered existing hedge: " + entry_direction_);
            return;
        }

        if (has1 || has2)
        {
            log_error("Partial hedge found at startup; flattening it.");
            close_positions_from_snapshot(positions, "Startup Partial Hedge");
        }
    }

    // --------------------------------------------------------
    // Quote updates
    // --------------------------------------------------------
    void update_quote(const json& data)
    {
        const std::string symbol = data.value("sy", data.value("symbol", ""));
        if (symbol != SYMBOL_1 && symbol != SYMBOL_2) return;

        Quote q;
        if (data.contains("bp")) q.bid = json_to_double(data["bp"]);
        if (data.contains("ap")) q.ask = json_to_double(data["ap"]);
        if (data.contains("bs")) q.bid_size = json_to_double(data["bs"]);
        if (data.contains("as")) q.ask_size = json_to_double(data["as"]);
        q.ts_ms = now_ms();

        if (!q.valid()) return;

        {
            std::lock_guard<std::mutex> lk(market_mu_);
            if (symbol == SYMBOL_1) quote1_ = q;
            else quote2_ = q;
        }

        evaluate();
    }

    bool market_snapshot(Quote& q1, Quote& q2) const
    {
        std::lock_guard<std::mutex> lk(market_mu_);
        q1 = quote1_;
        q2 = quote2_;
        return q1.valid() && q2.valid();
    }

    // --------------------------------------------------------
    // Fast strategy math
    // --------------------------------------------------------
    struct Edge
    {
        double spread{0.0};
        std::string direction;
    };

    Edge compute_mid_edge(const Quote& q1, const Quote& q2) const
    {
        const double raw = q1.mid() - q2.mid();
        return {std::abs(raw), (raw > 0.0) ? "SHORT_PAXG" : "LONG_PAXG"};
    }

    double executable_edge(const Quote& q1, const Quote& q2, const std::string& direction) const
    {
        // This is used only as an execution sanity check; the strategy trigger
        // remains the original absolute mid-price spread.
        if (direction == "SHORT_PAXG")
            return q1.bid - q2.ask;
        return q2.bid - q1.ask;
    }

    bool quotes_fresh(const Quote& q1, const Quote& q2) const
    {
        const long long t = now_ms();
        return q1.valid() && q2.valid() &&
               (t - q1.ts_ms <= MARKET_DATA_STALE_MS) &&
               (t - q2.ts_ms <= MARKET_DATA_STALE_MS);
    }

    bool liquidity_ok(const Quote& q, const std::string& symbol) const
    {
        const auto& p = product_.at(symbol);
        const double bid_usd = q.l1_bid_usd(p.contract_value);
        const double ask_usd = q.l1_ask_usd(p.contract_value);
        return std::min(bid_usd, ask_usd) >= MIN_TOP_OF_BOOK_USD;
    }

    bool spread_cost_ok(const Quote& q) const
    {
        const double mid = q.mid();
        if (mid <= 0.0) return false;
        const double spread = (q.ask - q.bid) / mid;
        return spread <= MAX_SLIPPAGE;
    }

    std::pair<int, int> compute_sizes(const Quote& q1, const Quote& q2) const
    {
        const double balance = balance_snapshot();
        const double capital_each = (balance * CAPITAL_PERCENT) * 0.5;

        const auto& p1 = product_.at(SYMBOL_1);
        const auto& p2 = product_.at(SYMBOL_2);

        const double notional1 = capital_each * LEVERAGE;
        const double notional2 = capital_each * LEVERAGE;

        const int size1 = std::clamp(
            std::max(1, static_cast<int>(notional1 / (q1.mid() * p1.contract_value))),
            1, p1.position_size_limit);

        const int size2 = std::clamp(
            std::max(1, static_cast<int>(notional2 / (q2.mid() * p2.contract_value))),
            1, p2.position_size_limit);

        return {size1, size2};
    }

    // --------------------------------------------------------
    // PnL bookkeeping
    // --------------------------------------------------------
    double compute_pnl(double current1, double current2) const
    {
        std::lock_guard<std::mutex> lk(position_mu_);
        if (entry_price_1_ <= 0.0 || entry_price_2_ <= 0.0) return 0.0;

        const double cv1 = product_.at(SYMBOL_1).contract_value;
        const double cv2 = product_.at(SYMBOL_2).contract_value;

        if (entry_direction_ == "SHORT_PAXG")
        {
            return entry_size1_ * cv1 * (entry_price_1_ - current1) +
                   entry_size2_ * cv2 * (current2 - entry_price_2_);
        }

        return entry_size1_ * cv1 * (current1 - entry_price_1_) +
               entry_size2_ * cv2 * (entry_price_2_ - current2);
    }

    void reset_position_state()
    {
        std::lock_guard<std::mutex> lk(position_mu_);
        positions_open_.store(false, std::memory_order_release);
        entry_direction_.clear();
        entry_price_1_ = 0.0;
        entry_price_2_ = 0.0;
        entry_size1_ = 0;
        entry_size2_ = 0;
        entry_spread_ = 0.0;
        entry_time_ = 0;
        last_exit_time_.store(now_sec(), std::memory_order_relaxed);
    }

    // --------------------------------------------------------
    // Entry execution
    // --------------------------------------------------------
    void launch_entry(const std::string& direction)
    {
        bool expected = false;
        if (!trade_in_flight_.compare_exchange_strong(expected, true)) return;

        Quote q1, q2;
        if (!market_snapshot(q1, q2))
        {
            trade_in_flight_.store(false, std::memory_order_release);
            return;
        }

        std::thread([this, direction, q1, q2]
        {
            open_trade(direction, q1, q2);
        }).detach();
    }

    void open_trade(const std::string& direction, Quote q1, Quote q2)
    {
        auto clear_inflight = [this]()
        {
            trade_in_flight_.store(false, std::memory_order_release);
        };

        try
        {
            if (now_sec() - last_exit_time_.load(std::memory_order_relaxed) < TRADE_COOLDOWN_SEC)
            {
                clear_inflight();
                return;
            }

            if (!quotes_fresh(q1, q2))
            {
                log_warn("Entry skipped: stale market data.");
                clear_inflight();
                return;
            }

            if (product_.at(SYMBOL_1).trading_status != "operational" ||
                product_.at(SYMBOL_2).trading_status != "operational")
            {
                log_warn("Entry skipped: market not operational.");
                clear_inflight();
                return;
            }

            if (!liquidity_ok(q1, SYMBOL_1) || !liquidity_ok(q2, SYMBOL_2) ||
                !spread_cost_ok(q1) || !spread_cost_ok(q2))
            {
                clear_inflight();
                return;
            }

            const double balance = balance_snapshot();
            if (balance <= 0.0)
            {
                log_warn("Entry skipped: balance unavailable.");
                clear_inflight();
                return;
            }

            // Re-read the latest in-memory quotes immediately before sizing/order.
            Quote latest1, latest2;
            if (market_snapshot(latest1, latest2) && quotes_fresh(latest1, latest2))
            {
                q1 = latest1;
                q2 = latest2;
            }

            const auto [size1, size2] = compute_sizes(q1, q2);
            const int pid1 = product_.at(SYMBOL_1).id;
            const int pid2 = product_.at(SYMBOL_2).id;

            const std::string side1 = (direction == "SHORT_PAXG") ? "sell" : "buy";
            const std::string side2 = (direction == "SHORT_PAXG") ? "buy"  : "sell";

            // IMPORTANT: both legs start concurrently.
            auto f1 = std::async(std::launch::async, [this, pid1, side1, size1]
            {
                return client_.place_market_order(pid1, side1, size1, false);
            });

            auto f2 = std::async(std::launch::async, [this, pid2, side2, size2]
            {
                return client_.place_market_order(pid2, side2, size2, false);
            });

            auto res1 = f1.get();
            auto res2 = f2.get();

            const bool ok1 = response_success(res1);
            const bool ok2 = response_success(res2);

            if (!ok1 || !ok2)
            {
                const std::string err1 = ok1 ? "OK" : response_error(res1);
                const std::string err2 = ok2 ? "OK" : response_error(res2);

                log_error("Entry failed. PAXG=" + err1 +
                          " | XAUT=" + err2);

                std::ostringstream fail_msg;
                fail_msg << "ENTRY FAILED\n"
                         << "PAXG: " << err1 << "\n"
                         << "XAUT: " << err2 << "\n"
                         << "Requested Size: " << size1
                         << " / " << size2;

                send_telegram(fail_msg.str());

                // Roll back any leg that did get filled.
                if (ok1 && !ok2)
                {
                    log_warn("Rolling back PAXG leg.");
                    client_.place_market_order(pid1,
                                               side1 == "buy" ? "sell" : "buy",
                                               size1,
                                               true);
                }
                else if (!ok1 && ok2)
                {
                    log_warn("Rolling back XAUT leg.");
                    client_.place_market_order(pid2,
                                               side2 == "buy" ? "sell" : "buy",
                                               size2,
                                               true);
                }

                clear_inflight();
                return;
            }

            const auto fill1 = extract_fill_price(res1);
            const auto fill2 = extract_fill_price(res2);

            double actual1 = fill1.value_or(q1.mid());
            double actual2 = fill2.value_or(q2.mid());

            if (!fill1 || !fill2)
            {
                log_warn("Missing average_fill_price; using market mid for local bookkeeping.");
            }

            if ((!fill1 || !fill2) && ENABLE_POST_TRADE_POSITION_LOOKUP)
            {
                const auto positions = fetch_open_positions();
                for (const auto& p : positions)
                {
                    const int pid = p.value("product_id", 0);
                    if (!p.contains("entry_price") || p["entry_price"].is_null()) continue;
                    if (pid == pid1) actual1 = json_to_double(p["entry_price"]);
                    if (pid == pid2) actual2 = json_to_double(p["entry_price"]);
                }
            }

            {
                std::lock_guard<std::mutex> lk(position_mu_);
                entry_direction_ = direction;
                entry_price_1_ = actual1;
                entry_price_2_ = actual2;
                entry_size1_ = size1;
                entry_size2_ = size2;
                entry_spread_ = std::abs(actual1 - actual2);
                entry_time_ = now_sec();
                last_pnl_update_.store(entry_time_, std::memory_order_relaxed);
            }
            positions_open_.store(true, std::memory_order_release);

            std::ostringstream msg;
            msg << "ENTRY\n"
                << ((direction == "SHORT_PAXG") ? "SHORT PAXG / LONG XAUT\n"
                                                : "LONG PAXG / SHORT XAUT\n")
                << std::fixed << std::setprecision(2)
                << "PAXG Entry: " << actual1 << "\n"
                << "XAUT Entry: " << actual2 << "\n"
                << "Spread: " << std::abs(actual1 - actual2) << "\n"
                << "PAXG Size: " << size1 << "\n"
                << "XAUT Size: " << size2 << "\n"
                << "Balance: $" << balance_snapshot();

            log_info(msg.str());
            send_telegram(msg.str());
            clear_inflight();
        }
        catch (const std::exception& e)
        {
            log_error(std::string("Entry execution exception: ") + e.what());
            send_telegram(std::string("ENTRY EXCEPTION\n") + e.what());
            clear_inflight();
        }
    }

    // --------------------------------------------------------
    // Exit execution
    // --------------------------------------------------------
    void launch_exit(const std::string& reason, Quote q1, Quote q2)
    {
        bool expected = false;
        if (!trade_in_flight_.compare_exchange_strong(expected, true)) return;

        std::thread([this, reason, q1, q2]
        {
            close_all(reason, q1, q2);
        }).detach();
    }

    void close_positions_from_snapshot(const std::vector<json>& positions,
                                       const std::string& reason)
    {
        struct Leg { int pid; int size; };
        std::vector<Leg> legs;

        for (const auto& p : positions)
        {
            const int pid = p.value("product_id", 0);
            const int sz = p.value("size", 0);
            if (pid != 0 && sz != 0) legs.push_back({pid, sz});
        }

        std::vector<std::future<std::optional<json>>> futures;
        for (const auto& leg : legs)
        {
            const std::string side = (leg.size > 0) ? "sell" : "buy";
            const int abs_size = std::abs(leg.size);
            futures.push_back(std::async(std::launch::async, [this, leg, side, abs_size]
            {
                return client_.place_market_order(leg.pid, side, abs_size, true);
            }));
        }

        bool all_ok = true;
        for (auto& f : futures)
            all_ok = response_success(f.get()) && all_ok;

        if (all_ok)
        {
            reset_position_state();
            refresh_balance_cache();
            send_telegram("EXIT\nReason: " + reason);
        }
        else
        {
            log_error("Close failed. Manual position check required.");
            send_telegram("CLOSE FAILED\nReason: " + reason + "\nCheck positions immediately.");
        }
    }

    void close_all(const std::string& reason, Quote q1, Quote q2)
    {
        try
        {
            // Prefer our in-memory sizes to avoid a REST position lookup in the
            // normal fast exit path. This is safe because every successful entry
            // sets these fields immediately.
            int size1 = 0;
            int size2 = 0;
            std::string direction;
            double ep1 = 0.0, ep2 = 0.0, entry_spread = 0.0;
            long long entry_time = 0;

            {
                std::lock_guard<std::mutex> lk(position_mu_);
                size1 = entry_size1_;
                size2 = entry_size2_;
                direction = entry_direction_;
                ep1 = entry_price_1_;
                ep2 = entry_price_2_;
                entry_spread = entry_spread_;
                entry_time = entry_time_;
            }

            if (size1 <= 0 || size2 <= 0)
            {
                log_warn("Fast exit state incomplete; using background REST position lookup.");
                const auto positions = fetch_open_positions();
                close_positions_from_snapshot(positions, reason);
                trade_in_flight_.store(false, std::memory_order_release);
                return;
            }

            const bool short_paxg = (direction == "SHORT_PAXG");
            const std::string exit_side1 = short_paxg ? "buy" : "sell";
            const std::string exit_side2 = short_paxg ? "sell" : "buy";
            const int pid1 = product_.at(SYMBOL_1).id;
            const int pid2 = product_.at(SYMBOL_2).id;

            // Both closing orders launch concurrently.
            auto f1 = std::async(std::launch::async, [this, pid1, exit_side1, size1]
            {
                return client_.place_market_order(pid1, exit_side1, size1, true);
            });

            auto f2 = std::async(std::launch::async, [this, pid2, exit_side2, size2]
            {
                return client_.place_market_order(pid2, exit_side2, size2, true);
            });

            const auto res1 = f1.get();
            const auto res2 = f2.get();
            const bool ok1 = response_success(res1);
            const bool ok2 = response_success(res2);

            if (!ok1 || !ok2)
            {
                log_error("Exit failed on one or both legs.");
                send_telegram("CLOSE FAILED\nReason: " + reason + "\nCheck positions immediately.");

                // If one leg succeeded, the other still needs attention. Do NOT
                // blindly issue another order because exchange state may differ.
                trade_in_flight_.store(false, std::memory_order_release);
                return;
            }

            const double exit_spread = std::abs(q1.mid() - q2.mid());
            const double pnl = compute_pnl(q1.mid(), q2.mid());
            const long long hold_sec = (entry_time > 0) ? now_sec() - entry_time : 0;

            reset_position_state();
            refresh_balance_cache();

            std::ostringstream msg;
            msg << "EXIT\n"
                << "Reason: " << reason << "\n"
                << "Direction: " << direction << "\n"
                << std::fixed << std::setprecision(2)
                << "Entry Spread: " << entry_spread << "\n"
                << "Exit Spread: " << exit_spread << "\n"
                << "Estimated PnL: $" << pnl << "\n"
                << "Hold: " << hold_sec << " sec\n"
                << "Balance: $" << balance_snapshot();

            log_info(msg.str());
            send_telegram(msg.str());
        }
        catch (const std::exception& e)
        {
            log_error(std::string("Exit execution exception: ") + e.what());
            send_telegram(std::string("EXIT EXCEPTION\n") + e.what());
        }

        trade_in_flight_.store(false, std::memory_order_release);
    }

    // --------------------------------------------------------
    // Strategy evaluation
    // --------------------------------------------------------
    void evaluate()
    {
        Quote q1, q2;
        if (!market_snapshot(q1, q2)) return;
        if (!quotes_fresh(q1, q2)) return;

        const Edge edge = compute_mid_edge(q1, q2);
        const bool open = positions_open_.load(std::memory_order_acquire);
        const bool inflight = trade_in_flight_.load(std::memory_order_acquire);

        const double entry_liq1 = std::min(
            q1.l1_bid_usd(product_.at(SYMBOL_1).contract_value),
            q1.l1_ask_usd(product_.at(SYMBOL_1).contract_value));
        const double entry_liq2 = std::min(
            q2.l1_bid_usd(product_.at(SYMBOL_2).contract_value),
            q2.l1_ask_usd(product_.at(SYMBOL_2).contract_value));

        const bool market_ok = liquidity_ok(q1, SYMBOL_1) &&
                               liquidity_ok(q2, SYMBOL_2) &&
                               spread_cost_ok(q1) && spread_cost_ok(q2);

        if (!open)
        {
            if (!inflight && market_ok && edge.spread >= ENTRY_SPREAD)
            {
                // Extra sanity check: make sure the executable edge has not already
                // vanished at the current top of book.
                const double ex_edge = executable_edge(q1, q2, edge.direction);
                if (ex_edge > 0.0)
                    launch_entry(edge.direction);
            }
        }
        else
        {
            if (!inflight)
            {
                const long long now = now_sec();
                if (now - last_pnl_update_.load(std::memory_order_relaxed) >= PNL_UPDATE_INTERVAL_SEC)
                {
                    last_pnl_update_.store(now, std::memory_order_relaxed);
                    std::string direction;
                    double ep1 = 0.0, ep2 = 0.0, es = 0.0;
                    long long entry_time = 0;
                    {
                        std::lock_guard<std::mutex> lk(position_mu_);
                        direction = entry_direction_;
                        ep1 = entry_price_1_;
                        ep2 = entry_price_2_;
                        es = entry_spread_;
                        entry_time = entry_time_;
                    }

                    std::ostringstream msg;
                    msg << "LIVE PNL UPDATE\n"
                        << "Direction: " << direction << "\n"
                        << std::fixed << std::setprecision(2)
                        << "PAXG Entry / Now: " << ep1 << " / " << q1.mid() << "\n"
                        << "XAUT Entry / Now: " << ep2 << " / " << q2.mid() << "\n"
                        << "Entry Spread: " << es << "\n"
                        << "Current Spread: " << edge.spread << "\n"
                        << "Unrealized PnL: $" << compute_pnl(q1.mid(), q2.mid()) << "\n"
                        << "Hold Time: " << ((entry_time > 0) ? (now - entry_time) : 0) << " sec";
                    send_telegram(msg.str());
                }

                if (edge.spread <= EXIT_SPREAD)
                    launch_exit("Spread Normalized", q1, q2);
            }
        }

        dashboard(q1, q2, edge, open, market_ok, entry_liq1, entry_liq2);
    }

    // --------------------------------------------------------
    // Dashboard (rate limited)
    // --------------------------------------------------------
    void dashboard(const Quote& q1, const Quote& q2, const Edge& edge,
                   bool open, bool market_ok,
                   double liq1, double liq2)
    {
        const long long t = now_ms();
        if (t - last_dashboard_ms_ < DASHBOARD_REFRESH_MS) return;
        last_dashboard_ms_ = t;

        std::string direction;
        double entry_spread = 0.0;
        {
            std::lock_guard<std::mutex> lk(position_mu_);
            direction = entry_direction_;
            entry_spread = entry_spread_;
        }

        std::cout << "\033c"
                  << "============================================================\n"
                  << "PAXG / XAUT FAST SPREAD ARBITRAGE BOT\n"
                  << "============================================================\n"
                  << std::fixed << std::setprecision(2)
                  << "PAXG Bid/Ask : " << q1.bid << " / " << q1.ask << "\n"
                  << "XAUT Bid/Ask : " << q2.bid << " / " << q2.ask << "\n"
                  << "PAXG Mid     : " << q1.mid() << "\n"
                  << "XAUT Mid     : " << q2.mid() << "\n"
                  << "Spread       : " << edge.spread << "\n"
                  << "Entry        : >= " << ENTRY_SPREAD << "\n"
                  << "Exit         : <= " << EXIT_SPREAD << "\n"
                  << "Direction    : " << (open ? direction : "WAIT") << "\n"
                  << "Entry Spread : " << (open ? entry_spread : 0.0) << "\n"
                  << "Balance      : $" << balance_snapshot() << "\n"
                  << "PAXG L1 Liq  : $" << liq1 << "\n"
                  << "XAUT L1 Liq  : $" << liq2 << "\n"
                  << "Market OK    : " << (market_ok ? "YES" : "NO") << "\n"
                  << "Open         : " << (open ? "YES" : "NO") << "\n"
                  << "In Flight    : "
                  << (trade_in_flight_.load(std::memory_order_acquire) ? "YES" : "NO") << "\n"
                  << "============================================================\n";
    }

    // --------------------------------------------------------
    // WebSocket
    // --------------------------------------------------------
    void websocket_loop()
    {
        log_info("Connecting public WebSocket...");

        ix::WebSocket ws;
        ws.setUrl(WS_URL);
        ws.setPingInterval(20);

        std::string ca_file = optional_env("CA_BUNDLE_PATH");
        if (ca_file.empty()) ca_file = "/ucrt64/ssl/certs/ca-bundle.crt";

        ix::SocketTLSOptions tls;
        tls.caFile = ca_file;
        ws.setTLSOptions(tls);

        std::atomic<bool> opened{false};
        std::atomic<long long> last_heartbeat{now_sec()};

        ws.setOnMessageCallback([this, &ws, &opened, &last_heartbeat]
            (const ix::WebSocketMessagePtr& msg)
        {
            switch (msg->type)
            {
                case ix::WebSocketMessageType::Open:
                {
                    opened.store(true, std::memory_order_release);
                    last_heartbeat.store(now_sec(), std::memory_order_relaxed);
                    log_info("Public WebSocket connected.");

                    ws.send(R"({"type":"enable_heartbeat"})");

                    // ob_l1 is published at ~100 ms cadence and is the main
                    // speed improvement over the old v2/ticker path.
                    json sub = {
                        {"type", "subscribe"},
                        {"payload", {
                            {"channels", json::array({
                                {
                                    {"name", "ob_l1"},
                                    {"symbols", {SYMBOL_1, SYMBOL_2}}
                                }
                            })}
                        }}
                    };
                    ws.send(sub.dump());
                    send_telegram("WebSocket Connected\nFast ob_l1 feed active");
                    break;
                }

                case ix::WebSocketMessageType::Message:
                {
                    try
                    {
                        const json data = json::parse(msg->str);
                        const std::string type = data.value("type", "");
                        if (type == "heartbeat")
                        {
                            last_heartbeat.store(now_sec(), std::memory_order_relaxed);
                            return;
                        }
                        if (type == "ob_l1") update_quote(data);
                    }
                    catch (const std::exception& e)
                    {
                        log_error(std::string("WS parse error: ") + e.what());
                    }
                    break;
                }

                case ix::WebSocketMessageType::Error:
                    log_error("WebSocket error: " + msg->errorInfo.reason);
                    break;

                case ix::WebSocketMessageType::Close:
                    log_warn("WebSocket closed.");
                    break;

                default:
                    break;
            }
        });

        ws.start();

        for (int i = 0; i < WS_CONNECT_TIMEOUT_SEC; ++i)
        {
            if (opened.load(std::memory_order_acquire)) break;
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        if (!opened.load(std::memory_order_acquire))
        {
            log_error("WebSocket did not open within timeout.");
            ws.stop();
            return;
        }

        while (running_.load(std::memory_order_acquire) &&
               ws.getReadyState() != ix::ReadyState::Closed)
        {
            const long long hb_age = now_sec() - last_heartbeat.load(std::memory_order_relaxed);
            if (hb_age > 35)
            {
                log_warn("Heartbeat timeout; restarting WebSocket.");
                ws.stop();
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(250));
        }

        ws.stop();
    }
};

// ============================================================
// MAIN
// ============================================================

int main()
{
    load_dotenv(".env");

    try
    {
        API_KEY          = require_env("API_KEY6");
        API_SECRET       = require_env("API_SECRET6");
        TELEGRAM_TOKEN   = optional_env("TELEGRAM_TOKEN");
        TELEGRAM_CHAT_ID = optional_env("TELEGRAM_CHAT_ID");
    }
    catch (const std::exception& e)
    {
        std::cerr << "[FATAL] " << e.what() << "\n";
        std::cerr << "Required .env entries:\n"
                  << "  API_KEY6=your_api_key\n"
                  << "  API_SECRET6=your_api_secret\n";
        return 1;
    }

    curl_global_init(CURL_GLOBAL_DEFAULT);

    try
    {
        if (TELEGRAM_TOKEN.empty() || TELEGRAM_CHAT_ID.empty())
            log_warn("Telegram disabled.");

        PAXGXAUTBot bot;
        bot.run();
    }
    catch (const std::exception& e)
    {
        log_error(std::string("Fatal: ") + e.what());
        curl_global_cleanup();
        return 1;
    }

    curl_global_cleanup();
    return 0;
}
