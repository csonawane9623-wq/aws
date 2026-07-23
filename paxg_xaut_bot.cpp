// ============================================================
//              PAXG / XAUT SPREAD ARBITRAGE BOT
//              Optimized for Speed and Efficiency
// ============================================================
//
// Build (MSYS2 UCRT64):
//   g++ -std=c++17 paxg_xaut_bot.cpp \
//       -I/ucrt64/include \
//       -L/ucrt64/lib \
//       -lixwebsocket -lcurl -lssl -lcrypto -lz -lws2_32 -lcrypt32 \
//       -o paxg_xaut_bot.exe
//
// .env file format:
//   API_KEY1=your_api_key
//   API_SECRET1=your_api_secret
//   TELEGRAM_TOKEN=your_token        (optional)
//   TELEGRAM_CHAT_ID=your_chat_id   (optional)
// ============================================================

#include <iostream>
#include <fstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <optional>
#include <functional>
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

#include <nlohmann/json.hpp>
#include <curl/curl.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>
#include <ixwebsocket/IXWebSocket.h>

#ifdef _WIN32
#include <stdlib.h>
#else
#include <unistd.h>
#endif

using json = nlohmann::json;

// ============================================================
// .ENV LOADER
// ============================================================

static void load_dotenv(const std::string& path = ".env")
{
    std::ifstream file(path);
    if (!file.is_open()) return;

    std::string line;
    while (std::getline(file, line))
    {
        if (!line.empty() && line.back() == '\r') line.pop_back();
        if (line.empty() || line[0] == '#') continue;

        auto eq = line.find('=');
        if (eq == std::string::npos) continue;

        std::string key = line.substr(0, eq);
        std::string val = line.substr(eq + 1);

        if (val.size() >= 2 &&
            ((val.front() == '"'  && val.back() == '"') ||
             (val.front() == '\'' && val.back() == '\'')))
            val = val.substr(1, val.size() - 2);

        auto trim = [](std::string& s) {
            s.erase(0, s.find_first_not_of(" \t"));
            s.erase(s.find_last_not_of(" \t") + 1);
        };
        trim(key); trim(val);

#ifdef _WIN32
        if (!std::getenv(key.c_str())) _putenv_s(key.c_str(), val.c_str());
#else
        setenv(key.c_str(), val.c_str(), 0);
#endif
    }
}

// ============================================================
// CONFIG
// ============================================================

static std::string require_env(const char* name)
{
    const char* val = std::getenv(name);
    if (!val || std::string(val).empty())
        throw std::runtime_error(
            std::string("Missing required environment variable: ") + name);
    return std::string(val);
}

static std::string optional_env(const char* name, const std::string& def = "")
{
    const char* val = std::getenv(name);
    if (!val || std::string(val).empty()) return def;
    return std::string(val);
}

std::string API_KEY;
std::string API_SECRET;
std::string TELEGRAM_TOKEN;
std::string TELEGRAM_CHAT_ID;

const std::string BASE_URL = "https://api.india.delta.exchange";
const std::string WS_URL   = "wss://socket.india.delta.exchange";

// ============================================================
// SYMBOLS
// ============================================================

const std::string SYMBOL_1 = "PAXGUSD";
const std::string SYMBOL_2 = "XAUTUSD";

// ============================================================
// STRATEGY
// ============================================================

const double ENTRY_SPREAD = 7.0;
const double STOP_SPREAD  = 100.0;

// ============================================================
// RISK
// ============================================================

const int    LEVERAGE              = 100;
const double CAPITAL_PERCENT       = 0.50;
const int    MAX_POSITION_HOLD_SEC = 3600000;
const int    COOLDOWN_AFTER_EXIT   = 0;

// ============================================================
// FILTERS
// ============================================================

const double MAX_SLIPPAGE      = 0.0025;
const double MIN_ORDERBOOK_USD = 5000.0;

// ============================================================
// TIMING
// Key insight: REST position refresh is expensive.
// We only hit REST every 30s; all intra-period decisions
// use the in-memory positions_open_ flag which is updated
// immediately after every open/close order.
// ============================================================
const int POSITION_REFRESH_SEC  = 2;   // REST sync interval
const int BALANCE_REFRESH_SEC   = 2;   // balance cache TTL
const int DASHBOARD_REFRESH_MS  = 50;  // max dashboard FPS (2/sec)

// ============================================================
// WEBSOCKET
// ============================================================
const int WS_CONNECT_TIMEOUT_SEC = 10;
const int WS_RECONNECT_BASE_SEC  = 1;
const int WS_RECONNECT_MAX_SEC   = 60;

// ============================================================
// USER-AGENT
// ============================================================

const std::string USER_AGENT =
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36";

// ============================================================
// UTILITIES
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

static void log_info(const std::string& msg)
{
    std::lock_guard<std::mutex> lk(g_log_mutex);
    std::time_t t = std::time(nullptr);
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", std::localtime(&t));
    std::cout << buf << " [INFO]  " << msg << "\n";
}

static void log_error(const std::string& msg)
{
    std::lock_guard<std::mutex> lk(g_log_mutex);
    std::time_t t = std::time(nullptr);
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", std::localtime(&t));
    std::cerr << buf << " [ERROR] " << msg << "\n";
}

static void log_warn(const std::string& msg)
{
    std::lock_guard<std::mutex> lk(g_log_mutex);
    std::time_t t = std::time(nullptr);
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", std::localtime(&t));
    std::cout << buf << " [WARN]  " << msg << "\n";
}

static std::string random_hex(int len)
{
    static const char hex_chars[] = "0123456789abcdef";
    static std::mt19937 rng{ std::random_device{}() };
    static std::mutex   rng_mutex;
    std::lock_guard<std::mutex> lk(rng_mutex);
    std::uniform_int_distribution<int> dist(0, 15);
    std::string result;
    result.reserve(len);
    for (int i = 0; i < len; ++i)
        result += hex_chars[dist(rng)];
    return result;
}

static std::string hmac_sha256_hex(const std::string& key, const std::string& data)
{
    unsigned char digest[EVP_MAX_MD_SIZE];
    unsigned int  digest_len = 0;
    HMAC(EVP_sha256(),
         key.data(),  static_cast<int>(key.size()),
         reinterpret_cast<const unsigned char*>(data.data()),
         static_cast<int>(data.size()),
         digest, &digest_len);
    std::ostringstream oss;
    for (unsigned int i = 0; i < digest_len; ++i)
        oss << std::hex << std::setw(2) << std::setfill('0')
            << static_cast<int>(digest[i]);
    return oss.str();
}

static double json_to_double(const json& v)
{
    if (v.is_string()) return std::stod(v.get<std::string>());
    if (v.is_number()) return v.get<double>();
    throw std::runtime_error("json_to_double: unexpected type: " + v.dump());
}

static std::string json_to_string(const json& v)
{
    if (v.is_string())  return v.get<std::string>();
    if (v.is_number())  return std::to_string(v.get<double>());
    if (v.is_boolean()) return v.get<bool>() ? "true" : "false";
    return v.dump();
}

static size_t my_write_callback(char* ptr, size_t size, size_t nmemb, void* userdata)
{
    std::string* buf = static_cast<std::string*>(userdata);
    buf->append(ptr, size * nmemb);
    return size * nmemb;
}

// ============================================================
// ASYNC WORK QUEUE
// Offloads blocking REST calls off the WebSocket thread so
// ticker messages are never delayed by network I/O.
// ============================================================

class WorkQueue
{
public:
    WorkQueue() : stop_(false)
    {
        worker_ = std::thread([this] { run(); });
    }

    ~WorkQueue()
    {
        {
            std::lock_guard<std::mutex> lk(mu_);
            stop_ = true;
        }
        cv_.notify_one();
        if (worker_.joinable()) worker_.join();
    }

    // Post a task; returns immediately.
    void post(std::function<void()> task)
    {
        {
            std::lock_guard<std::mutex> lk(mu_);
            // Drop the task if the queue is already backed up (> 4 pending).
            // This prevents unbounded queuing if REST calls are slow.
            if (queue_.size() > 4) return;
            queue_.push(std::move(task));
        }
        cv_.notify_one();
    }

private:
    void run()
    {
        while (true)
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
            catch (const std::exception& e)
            { log_error(std::string("WorkQueue task threw: ") + e.what()); }
        }
    }

    std::mutex              mu_;
    std::condition_variable cv_;
    std::queue<std::function<void()>> queue_;
    std::thread             worker_;
    bool                    stop_;
};

// ============================================================
// TELEGRAM (async, non-blocking)
// ============================================================

static std::atomic<long long> g_last_telegram_time{ 0 };
static WorkQueue*              g_telegram_queue = nullptr;

static void send_telegram_impl(const std::string& msg)
{
    try
    {
        if (now_sec() - g_last_telegram_time.load() < 2) return;
        if (TELEGRAM_TOKEN.empty() || TELEGRAM_CHAT_ID.empty()) return;

        std::string url =
            "https://api.telegram.org/bot" + TELEGRAM_TOKEN + "/sendMessage";

        json body = { { "chat_id", TELEGRAM_CHAT_ID }, { "text", msg } };
        std::string body_str    = body.dump();
        std::string response_buf;

        CURL* curl = curl_easy_init();
        if (!curl) return;

        struct curl_slist* headers = nullptr;
        headers = curl_slist_append(headers, "Content-Type: application/json");
        headers = curl_slist_append(headers, ("User-Agent: " + USER_AGENT).c_str());

        curl_easy_setopt(curl, CURLOPT_URL,           url.c_str());
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS,    body_str.c_str());
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER,    headers);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, my_write_callback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA,     &response_buf);
        curl_easy_setopt(curl, CURLOPT_USERAGENT,     USER_AGENT.c_str());
        curl_easy_setopt(curl, CURLOPT_TIMEOUT,       5L);

        curl_easy_perform(curl);
        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);

        g_last_telegram_time.store(now_sec());
    }
    catch (const std::exception& e)
    {
        log_error(std::string("Telegram Error: ") + e.what());
    }
}

// Fire-and-forget: posts to the work queue, never blocks the caller.
static void send_telegram(const std::string& msg)
{
    if (g_telegram_queue)
        g_telegram_queue->post([msg] { send_telegram_impl(msg); });
}

// ============================================================
// DELTA CLIENT
// ============================================================

struct OrderbookLevel { double limit_price; double size; };
struct Orderbook
{
    std::vector<OrderbookLevel> bids;
    std::vector<OrderbookLevel> asks;
};

class DeltaClient
{
public:
    DeltaClient() : key_(API_KEY), secret_(API_SECRET) {}

    std::pair<std::string, std::string> sign(
        const std::string& method,
        const std::string& path,
        const std::string& query_string = "",
        const std::string& payload      = "") const
    {
        std::string timestamp = std::to_string(now_sec());
        std::string sig_data  = method + timestamp + path + query_string + payload;
        return { hmac_sha256_hex(secret_, sig_data), timestamp };
    }

    std::optional<json> request(
        const std::string& method,
        const std::string& path,
        const json&        data   = json{},
        bool               auth   = false,
        const json&        params = json{}) const
    {
        std::string payload      = (data.is_null() || data.empty()) ? "" : data.dump();
        std::string query_string;

        if (!params.is_null() && params.is_object() && !params.empty())
        {
            std::ostringstream qs;
            qs << "?";
            bool first = true;
            for (auto& [k, v] : params.items())
            {
                if (!first) qs << "&";
                qs << k << "=" << v.get<std::string>();
                first = false;
            }
            query_string = qs.str();
        }

        std::string url = BASE_URL + path + query_string;

        struct curl_slist* headers = nullptr;
        headers = curl_slist_append(headers, "Content-Type: application/json");
        headers = curl_slist_append(headers, ("User-Agent: " + USER_AGENT).c_str());

        if (auth)
        {
            auto [signature, timestamp] = sign(method, path, query_string, payload);
            headers = curl_slist_append(headers, ("api-key: "   + key_      ).c_str());
            headers = curl_slist_append(headers, ("timestamp: " + timestamp ).c_str());
            headers = curl_slist_append(headers, ("signature: " + signature ).c_str());
        }

        std::string response_buf;
        CURL* curl = curl_easy_init();
        if (!curl) { curl_slist_free_all(headers); return std::nullopt; }

        curl_easy_setopt(curl, CURLOPT_URL,           url.c_str());
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER,    headers);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, my_write_callback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA,     &response_buf);
        curl_easy_setopt(curl, CURLOPT_USERAGENT,     USER_AGENT.c_str());
        curl_easy_setopt(curl, CURLOPT_TIMEOUT,       10L);
        // Keep TCP connection alive between requests
        curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);
        curl_easy_setopt(curl, CURLOPT_TCP_KEEPIDLE,  30L);
        curl_easy_setopt(curl, CURLOPT_TCP_KEEPINTVL, 10L);

        if (method == "POST")
        {
            curl_easy_setopt(curl, CURLOPT_POST,          1L);
            curl_easy_setopt(curl, CURLOPT_POSTFIELDS,    payload.c_str());
            curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, static_cast<long>(payload.size()));
        }
        else
        {
            curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
        }

        CURLcode res = curl_easy_perform(curl);
        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);

        if (res != CURLE_OK)
        {
            log_error(std::string("CURL error: ") + curl_easy_strerror(res));
            return std::nullopt;
        }

        try   { return json::parse(response_buf); }
        catch (...) { log_error("JSON parse error: " + response_buf); return std::nullopt; }
    }

    std::optional<json> get_product(const std::string& symbol) const
    { return request("GET", "/v2/products/" + symbol); }

    std::optional<json> get_balance() const
    { return request("GET", "/v2/wallet/balances", {}, true); }

    std::optional<json> get_positions() const
    { return request("GET", "/v2/positions/margined", {}, true); }

    std::optional<json> set_leverage(int product_id, int leverage) const
    {
        json body = { { "leverage", std::to_string(leverage) } };
        return request("POST",
            "/v2/products/" + std::to_string(product_id) + "/orders/leverage",
            body, true);
    }

    std::optional<json> place_market_order(
        int product_id, const std::string& side, int size) const
    {
        json body = {
            { "product_id",      product_id     },
            { "size",            size           },
            { "side",            side           },
            { "order_type",      "market_order" },
            { "client_order_id", random_hex(32) }
        };
        return request("POST", "/v2/orders", body, true);
    }

    bool validate_credentials() const
    {
        log_info("Validating API credentials...");
        auto res = get_balance();
        if (!res) { log_error("Credential check: no response"); return false; }
        bool ok = res->value("success", false);
        if (!ok)
        {
            std::string code = "unknown";
            if (res->contains("error") && (*res)["error"].contains("code"))
                code = json_to_string((*res)["error"]["code"]);
            log_error("Credential check failed — error code: " + code);
            log_error("Full response: " + res->dump());
        }
        else log_info("API credentials validated OK.");
        return ok;
    }

private:
    std::string key_;
    std::string secret_;
};

// ============================================================
// BOT
// ============================================================

class PAXGXAUTBot
{
public:
    PAXGXAUTBot()
        : prices_({ { SYMBOL_1, std::nullopt }, { SYMBOL_2, std::nullopt } })
        , positions_open_(false)
        , entry_time_(0)
        , entry_spread_(std::nullopt)
        , entry_direction_("")
        , last_exit_time_(0)
        , last_position_refresh_(0)
        , cached_balance_(0.0)
        , last_balance_refresh_(0)
        , last_dashboard_ms_(0)
        , trade_in_flight_(false)
    {
        if (!client_.validate_credentials())
            throw std::runtime_error(
                "API credential validation failed. "
                "Check API_KEY1 / API_SECRET1 in your .env file.");

        load_products();
        refresh_balance_cache();     // warm the cache before the WS starts
        recover_positions_on_startup();
    }

    void run()
    {
        int backoff_sec = WS_RECONNECT_BASE_SEC;
        while (true)
        {
            try
            {
                websocket_loop();
                backoff_sec = WS_RECONNECT_BASE_SEC;
            }
            catch (const std::exception& e)
            {
                log_error(std::string("WebSocket Crash: ") + e.what());
                send_telegram(std::string("WebSocket Crash\n") + e.what());
            }
            log_info("Reconnecting in " + std::to_string(backoff_sec) + "s...");
            std::this_thread::sleep_for(std::chrono::seconds(backoff_sec));
            backoff_sec = std::min(backoff_sec * 2, WS_RECONNECT_MAX_SEC);
        }
    }

private:
    DeltaClient client_;
    WorkQueue   rest_queue_;   // all blocking REST calls go here

    std::unordered_map<std::string, json>                  products_;
    std::unordered_map<std::string, Orderbook>             orderbooks_;
    std::unordered_map<std::string, std::optional<double>> prices_;

    bool                  positions_open_;
    long long             entry_time_;
    std::optional<double> entry_spread_;
    std::string           entry_direction_;
    long long             last_exit_time_;
    long long             last_position_refresh_;

    // Cached balance — refreshed on a timer, not on every tick
    double                cached_balance_;
    long long             last_balance_refresh_;

    // Dashboard rate-limiter
    long long             last_dashboard_ms_;

    // Guard: prevents multiple overlapping open/close orders
    std::atomic<bool>     trade_in_flight_;

    std::unordered_set<std::string> leverage_done_;
    std::mutex state_mutex_;   // protects prices_, orderbooks_

    // --------------------------------------------------------
    // Load Products
    // --------------------------------------------------------
    void load_products()
    {
        for (const auto& symbol : { SYMBOL_1, SYMBOL_2 })
        {
            auto data = client_.get_product(symbol);
            if (!data || !(*data)["success"].get<bool>())
                throw std::runtime_error("Cannot load product: " + symbol);
            products_[symbol] = (*data)["result"];
            log_info("Loaded Product: " + symbol);
        }
    }

    // --------------------------------------------------------
    // Balance Cache
    // Called from the REST worker thread only.
    // --------------------------------------------------------
    void refresh_balance_cache()
    {
        auto data = client_.get_balance();
        if (!data || !data->value("success", false)) return;
        if (!data->contains("result") || !(*data)["result"].is_array()) return;

        for (const auto& asset : (*data)["result"])
        {
            if (!asset.contains("asset_symbol") || asset["asset_symbol"].is_null())
                continue;
            if (json_to_string(asset["asset_symbol"]) != "USD") continue;
            if (!asset.contains("available_balance") ||
                asset["available_balance"].is_null()) continue;

            cached_balance_      = json_to_double(asset["available_balance"]);
            last_balance_refresh_ = now_sec();
            log_info("Balance refreshed: " + std::to_string(cached_balance_));
            return;
        }
        log_warn("refresh_balance_cache: USD wallet not found");
    }

    // --------------------------------------------------------
    // fetch_open_positions
    // --------------------------------------------------------
    std::vector<json> fetch_open_positions()
    {
        auto data = client_.get_positions();
        if (!data) { log_error("fetch_open_positions: no response"); return {}; }
        if (!data->value("success", false))
        {
            std::string code = "unknown";
            if (data->contains("error") && (*data)["error"].contains("code"))
                code = json_to_string((*data)["error"]["code"]);
            log_error("fetch_open_positions: API error — " + code);
            return {};
        }
        if (!data->contains("result")) { log_error("fetch_open_positions: no result"); return {}; }

        const json& result = (*data)["result"];
        const json* arr_ptr = nullptr;
        json        fallback_arr;

        if (result.is_array()) arr_ptr = &result;
        else if (result.is_object())
        {
            if (result.contains("open_positions") && result["open_positions"].is_array())
                arr_ptr = &result["open_positions"];
            else { fallback_arr = json::array({ result }); arr_ptr = &fallback_arr; }
        }
        else { log_error("fetch_open_positions: unexpected result type"); return {}; }

        std::vector<json> open;
        for (const auto& p : *arr_ptr)
        {
            int sz = 0;
            if (p.contains("size") && !p["size"].is_null())
                sz = p["size"].get<int>();
            if (sz != 0) open.push_back(p);
        }

        log_info("fetch_open_positions: total open = " + std::to_string(open.size()));
        return open;
    }

    // --------------------------------------------------------
    // Recover Positions on Startup
    // --------------------------------------------------------
    void recover_positions_on_startup()
    {
        log_info("Checking existing positions...");
        auto positions = fetch_open_positions();

        if (positions.empty()) { log_info("No open positions found."); return; }

        std::vector<std::string> found_symbols;
        for (const auto& p : positions)
        {
            int pid = p["product_id"].get<int>();
            for (const auto& [symbol, product] : products_)
                if (product["id"].get<int>() == pid)
                    found_symbols.push_back(symbol);
        }

        bool has1 = std::find(found_symbols.begin(), found_symbols.end(), SYMBOL_1)
                    != found_symbols.end();
        bool has2 = std::find(found_symbols.begin(), found_symbols.end(), SYMBOL_2)
                    != found_symbols.end();

        if (has1 && has2)
        {
            positions_open_ = true;
            entry_time_     = now_sec(); // approximate — we lost the real entry time

            // Determine direction from which leg is short vs long.
            // size > 0 = long, size < 0 = short.
            for (const auto& p : positions)
            {
                int pid = p["product_id"].get<int>();
                int sz  = 0;
                if (p.contains("size") && !p["size"].is_null())
                    sz = p["size"].get<int>();

                if (products_[SYMBOL_1]["id"].get<int>() == pid)
                {
                    // SYMBOL_1 (PAXG): negative size = short PAXG
                    entry_direction_ = (sz < 0) ? "SHORT_PAXG" : "LONG_PAXG";
                }
            }

            // Recover entry spread from the average entry price fields if available,
            // otherwise leave as nullopt and it will be populated on the next tick.
            // Two-pass: collect both entry prices then compute spread
            double entry_price_1 = 0.0;
            double entry_price_2 = 0.0;

            for (const auto& p : positions)
            {
                if (!p.contains("entry_price") || p["entry_price"].is_null()) continue;
                int pid = p["product_id"].get<int>();

                if (products_[SYMBOL_1]["id"].get<int>() == pid)
                    entry_price_1 = json_to_double(p["entry_price"]);

                if (products_[SYMBOL_2]["id"].get<int>() == pid)
                    entry_price_2 = json_to_double(p["entry_price"]);
            }

            if (entry_price_1 > 0.0 && entry_price_2 > 0.0)
                entry_spread_ = entry_price_1 - entry_price_2;

            // If we couldn't recover entry_spread_ from entry_price fields,
            // flag it clearly so the dashboard shows "Recovering..." instead of None.
            if (!entry_spread_)
                log_warn("recover_positions_on_startup: entry_price not available, "
                         "entry_spread will show None until next tick.");

            log_warn("Recovered Existing Hedge — direction: " + entry_direction_);
            send_telegram("Recovered Existing Hedge\nDirection: " + entry_direction_);
        }
        else if (has1 || has2)
        {
            log_error("Partial Hedge Found!");
            send_telegram("WARNING: Partial Hedge Found!");
            close_all("Startup Cleanup");
        }
        else
        {
            log_info("Open positions found but none match our symbols.");
        }
    }

    // --------------------------------------------------------
    // Ensure Leverage
    // --------------------------------------------------------
    void ensure_leverage(const std::string& symbol)
    {
        if (leverage_done_.count(symbol)) return;
        int pid  = products_[symbol]["id"].get<int>();
        auto res = client_.set_leverage(pid, LEVERAGE);
        if (res && (*res)["success"].get<bool>())
        {
            leverage_done_.insert(symbol);
            log_info("Leverage Set: " + symbol);
        }
        else
        {
            std::string code = "unknown";
            if (res && res->contains("error") && (*res)["error"].contains("code"))
                code = json_to_string((*res)["error"]["code"]);
            log_error("Failed to set leverage for " + symbol + " — " + code);
        }
    }

    // --------------------------------------------------------
    // Orderbook Metrics — runs on WS thread, no I/O
    // --------------------------------------------------------
    struct OBMetrics { bool ok; double slippage; double liquidity; };

    std::optional<OBMetrics> orderbook_metrics(
        const std::string& symbol,
        const std::unordered_map<std::string, Orderbook>& obs) const
    {
        auto it = obs.find(symbol);
        if (it == obs.end()) return std::nullopt;

        const Orderbook& ob = it->second;
        if (ob.bids.empty() || ob.asks.empty()) return std::nullopt;

        double best_bid = ob.bids[0].limit_price;
        double best_ask = ob.asks[0].limit_price;
        double mid      = (best_bid + best_ask) / 2.0;
        double slippage = std::abs(best_ask - best_bid) / mid;

        double cv = json_to_double(products_.at(symbol)["contract_value"]);

        auto sum_liq = [&](const std::vector<OrderbookLevel>& levels) {
            double total = 0.0;
            int    n     = std::min((int)levels.size(), 5);
            for (int i = 0; i < n; ++i)
                total += levels[i].limit_price * levels[i].size * cv;
            return total;
        };

        double liquidity = std::min(sum_liq(ob.bids), sum_liq(ob.asks));
        bool   ok        = (slippage <= MAX_SLIPPAGE && liquidity >= MIN_ORDERBOOK_USD);
        return OBMetrics{ ok, slippage, liquidity };
    }

    // --------------------------------------------------------
    // Compute Sizes — pure math, no I/O
    // --------------------------------------------------------
    std::pair<int, int> compute_sizes(double p1, double p2) const
    {
        double capital_each = (cached_balance_ * CAPITAL_PERCENT) / 2.0;

        double cv1 = json_to_double(products_.at(SYMBOL_1)["contract_value"]);
        double cv2 = json_to_double(products_.at(SYMBOL_2)["contract_value"]);

        int size1 = std::max(static_cast<int>((capital_each * LEVERAGE / p1) / cv1), 1);
        int size2 = std::max(static_cast<int>((capital_each * LEVERAGE / p2) / cv2), 1);

        int limit1 = products_.at(SYMBOL_1)["position_size_limit"].get<int>();
        int limit2 = products_.at(SYMBOL_2)["position_size_limit"].get<int>();

        if (size1 > limit1) { log_warn("size1 clamped"); size1 = limit1; }
        if (size2 > limit2) { log_warn("size2 clamped"); size2 = limit2; }

        return { size1, size2 };
    }

    // --------------------------------------------------------
    // Open Trade — runs on rest_queue_ worker thread
    // --------------------------------------------------------
    void open_trade_async(const std::string& direction, double p1, double p2,
                          const std::unordered_map<std::string, Orderbook>& obs)
    {
        // Cooldown check (uses only cheap in-memory state)
        if (now_sec() - last_exit_time_ < COOLDOWN_AFTER_EXIT)
        { trade_in_flight_.store(false); return; }

        std::string status1 = json_to_string(products_[SYMBOL_1]["trading_status"]);
        std::string status2 = json_to_string(products_[SYMBOL_2]["trading_status"]);
        if (status1 != "operational" || status2 != "operational")
        {
            log_warn("open_trade: skipping — not operational");
            trade_in_flight_.store(false); return;
        }

        auto m1 = orderbook_metrics(SYMBOL_1, obs);
        auto m2 = orderbook_metrics(SYMBOL_2, obs);
        if (!m1 || !m2 || !m1->ok || !m2->ok)
        { trade_in_flight_.store(false); return; }

        ensure_leverage(SYMBOL_1);
        ensure_leverage(SYMBOL_2);

        // Refresh balance right before sizing if stale
        if (now_sec() - last_balance_refresh_ >= BALANCE_REFRESH_SEC)
            refresh_balance_cache();

        auto [size1, size2] = compute_sizes(p1, p2);

        int pid1 = products_[SYMBOL_1]["id"].get<int>();
        int pid2 = products_[SYMBOL_2]["id"].get<int>();

        std::optional<json> res1, res2;
        std::string         signal;

        if (direction == "SHORT_PAXG")
        {
            res1   = client_.place_market_order(pid1, "sell", size1);
            res2   = client_.place_market_order(pid2, "buy",  size2);
            signal = "SHORT PAXG / LONG XAUT";
        }
        else
        {
            res1   = client_.place_market_order(pid1, "buy",  size1);
            res2   = client_.place_market_order(pid2, "sell", size2);
            signal = "LONG PAXG / SHORT XAUT";
        }

        bool ok1 = res1 && (*res1)["success"].get<bool>();
        bool ok2 = res2 && (*res2)["success"].get<bool>();

        if (!ok1 || !ok2)
        {
            log_error("Order Failed");
            send_telegram("Order Failed");
            trade_in_flight_.store(false);
            return;
        }

        double current_spread = p1 - p2;
        positions_open_  = true;
        entry_time_      = now_sec();
        entry_spread_    = current_spread;
        entry_direction_ = direction;

        log_info(signal);
        std::ostringstream msg;
        msg << "ENTRY\n" << signal << "\nSpread: "
            << std::fixed << std::setprecision(2) << current_spread;
        send_telegram(msg.str());

        trade_in_flight_.store(false);
    }

    // --------------------------------------------------------
    // Close All — runs on rest_queue_ worker thread
    // --------------------------------------------------------
    // --------------------------------------------------------
    // close_all — synchronous wrapper used at startup before
    // the WebSocket and rest_queue_ are running.
    // --------------------------------------------------------
    void close_all(const std::string& reason)
    {
        close_all_async(reason);
    }

    // --------------------------------------------------------
    // Close All — runs on rest_queue_ worker thread
    // --------------------------------------------------------
    void close_all_async(const std::string& reason)
    {
        auto positions = fetch_open_positions();

        if (positions.empty()) { reset_state(); trade_in_flight_.store(false); return; }

        bool all_ok = true;
        for (const auto& p : positions)
        {
            int sz = 0;
            if (p.contains("size") && !p["size"].is_null())
                sz = p["size"].get<int>();
            if (sz == 0) continue;

            std::string side   = (sz > 0) ? "sell" : "buy";
            int         pid    = p["product_id"].get<int>();
            int         abs_sz = std::abs(sz);

            auto res = client_.place_market_order(pid, side, abs_sz);
            bool ok  = res && (*res)["success"].get<bool>();
            if (!ok)
            {
                std::string code = "unknown";
                if (res && res->contains("error") && (*res)["error"].contains("code"))
                    code = json_to_string((*res)["error"]["code"]);
                log_error("close_all: order failed for pid=" +
                          std::to_string(pid) + " — " + code);
                all_ok = false;
            }
        }

        if (!all_ok)
        {
            log_error("close_all: some orders failed. Manual intervention needed.");
            send_telegram("CLOSE FAILED\nReason: " + reason +
                          "\nCheck positions manually!");
            trade_in_flight_.store(false);
            return;
        }

        reset_state();
        refresh_balance_cache();
        send_telegram("CLOSED\nReason: " + reason);
        trade_in_flight_.store(false);
    }

    void reset_state()
    {
        positions_open_  = false;
        entry_time_      = 0;
        entry_spread_    = std::nullopt;
        entry_direction_ = "";
        last_exit_time_  = now_sec();
    }

    // --------------------------------------------------------
    // Dashboard — rate-limited to DASHBOARD_REFRESH_MS
    // Does NOT make any REST calls.
    // --------------------------------------------------------
    void dashboard(double spread, const std::string& signal, int open_count,
                   const std::optional<OBMetrics>& m1,
                   const std::optional<OBMetrics>& m2)
    {
        long long now = now_ms();
        if (now - last_dashboard_ms_ < DASHBOARD_REFRESH_MS) return;
        last_dashboard_ms_ = now;

        std::cout << "\033c";
        std::string sep(70, '=');
        std::cout << sep << "\nPAXG/XAUT SPREAD ARBITRAGE BOT\n" << sep << "\n\n";
        std::cout << std::fixed << std::setprecision(2);
        std::cout << "PAXG    : " << *prices_[SYMBOL_1] << "\n";
        std::cout << "XAUT    : " << *prices_[SYMBOL_2] << "\n";
        std::cout << "Balance : $" << cached_balance_   << "\n\n";
        std::cout << "Spread        : " << spread           << "\n";
        std::cout << "Abs Spread    : " << std::abs(spread) << "\n";
        std::cout << "Entry Trigger : >= " << ENTRY_SPREAD  << " (both directions)\n";
        std::cout << "Stop Spread   : "   << STOP_SPREAD    << "\n\n";
        std::cout << "Entry Direction : " << entry_direction_ << "\n";
        if (entry_spread_)
            std::cout << "Entry Spread    : " << *entry_spread_ << "\n";
        else if (positions_open_)
            std::cout << "Entry Spread    : Recovering (waiting for next tick)...\n";
        else
            std::cout << "Entry Spread    : None\n";

        if (m1)
        {
            std::cout << "PAXG Slippage  : "
                      << std::setprecision(4) << m1->slippage * 100 << "%\n";
            std::cout << "PAXG Liquidity : $"
                      << std::setprecision(2) << m1->liquidity << "\n";
        }
        std::cout << "\n";
        if (m2)
        {
            std::cout << "XAUT Slippage  : "
                      << std::setprecision(4) << m2->slippage * 100 << "%\n";
            std::cout << "XAUT Liquidity : $"
                      << std::setprecision(2) << m2->liquidity << "\n";
        }

        std::cout << "\n";
        std::cout << "Positions Open (flag): " << (positions_open_ ? "True" : "False") << "\n";
        std::cout << "Positions Open (API) : " << open_count << " position(s)\n";
        std::cout << "Signal               : " << signal << "\n\n";
        std::cout << sep << "\n";
    }

    // --------------------------------------------------------
    // Evaluate Strategy — called on WS thread, ZERO blocking I/O.
    // All REST work is posted to rest_queue_.
    // --------------------------------------------------------
    void evaluate()
    {
        // --- 1. Snapshot shared state under mutex (fast) ---
        std::optional<double> p1, p2;
        std::unordered_map<std::string, Orderbook> obs;
        {
            std::lock_guard<std::mutex> lk(state_mutex_);
            p1  = prices_[SYMBOL_1];
            p2  = prices_[SYMBOL_2];
            obs = orderbooks_;
        }

        if (!p1 || !p2) return;

        double spread = *p1 - *p2;

        // --- 2. Compute metrics purely in memory (fast) ---
        auto m1 = orderbook_metrics(SYMBOL_1, obs);
        auto m2 = orderbook_metrics(SYMBOL_2, obs);

        // --- 3. Periodic REST position sync (off WS thread) ---
        long long now = now_sec();
        if (now - last_position_refresh_ >= POSITION_REFRESH_SEC)
        {
            last_position_refresh_ = now;
            rest_queue_.post([this]
            {
                auto real_positions = fetch_open_positions();
                int  cnt            = static_cast<int>(real_positions.size());
                positions_open_     = (cnt > 0);
            });
        }

        // --- 4. Periodic balance refresh (off WS thread) ---
        if (now - last_balance_refresh_ >= BALANCE_REFRESH_SEC)
        {
            last_balance_refresh_ = now; // set eagerly to avoid double-posting
            rest_queue_.post([this] { refresh_balance_cache(); });
        }

        // --- 5. Strategy logic (all in-memory, fast) ---
        std::string signal     = "WAIT";
        int         open_count = positions_open_ ? 1 : 0;

        if (!positions_open_)
        {
            if (std::abs(spread) >= ENTRY_SPREAD && !trade_in_flight_.load())
            {
                std::string direction = (spread > 0) ? "SHORT_PAXG" : "LONG_PAXG";
                signal = (spread > 0) ? "SHORT PAXG / LONG XAUT" : "LONG PAXG / SHORT XAUT";

                trade_in_flight_.store(true);

                // Capture everything needed; post to worker so WS thread is free instantly
                double cp1 = *p1, cp2 = *p2;
                auto   cobs = obs;
                rest_queue_.post([this, direction, cp1, cp2, cobs]
                {
                    open_trade_async(direction, cp1, cp2, cobs);
                });
            }
        }
        else
        {
            long long hold_time      = (entry_time_ > 0) ? (now - entry_time_) : 0;
            bool      exit_condition = false;

            if      (entry_direction_ == "SHORT_PAXG" && spread <= 0) exit_condition = true;
            else if (entry_direction_ == "LONG_PAXG"  && spread >= 0) exit_condition = true;

            if ((exit_condition || std::abs(spread) >= STOP_SPREAD ||
                 (entry_time_ > 0 && hold_time >= MAX_POSITION_HOLD_SEC))
                && !trade_in_flight_.load())
            {
                if (exit_condition)             signal = "EXIT";
                else if (std::abs(spread) >= STOP_SPREAD) signal = "STOP LOSS";
                else                            signal = "TIME EXIT";

                std::string reason =
                    exit_condition             ? "Spread Crossed Zero" :
                    std::abs(spread) >= STOP_SPREAD ? "Spread Explosion"   :
                                                  "Max Hold Time";

                trade_in_flight_.store(true);
                rest_queue_.post([this, reason] { close_all_async(reason); });
            }
            else signal = "HOLDING";
        }

        // --- 6. Dashboard (rate-limited, no I/O) ---
        dashboard(spread, signal, open_count, m1, m2);
    }

    // --------------------------------------------------------
    // Parse orderbook levels
    // --------------------------------------------------------
    static std::vector<OrderbookLevel> parse_levels(const json& arr)
    {
        std::vector<OrderbookLevel> levels;
        if (!arr.is_array()) return levels;
        for (const auto& item : arr)
        {
            OrderbookLevel lv;
            lv.limit_price = json_to_double(item["limit_price"]);
            lv.size        = json_to_double(item["size"]);
            levels.push_back(lv);
        }
        return levels;
    }

    // --------------------------------------------------------
    // WebSocket Loop
    // --------------------------------------------------------
    void websocket_loop()
    {
        log_info("Connecting WebSocket...");

        ix::WebSocket ws;
        ws.setUrl(WS_URL);
        ws.setPingInterval(20);

        std::atomic<bool> ever_opened{ false };

        ws.setOnMessageCallback([this, &ws, &ever_opened](const ix::WebSocketMessagePtr& msg)
        {
            if (msg->type == ix::WebSocketMessageType::Open)
            {
                ever_opened.store(true);
                log_info("WebSocket Connected");
                send_telegram("WebSocket Connected");

                json sub = {
                    { "type", "subscribe" },
                    { "payload", {
                        { "channels", json::array({
                            {
                                { "name",    "v2/ticker"            },
                                { "symbols", { SYMBOL_1, SYMBOL_2 } }
                            },
                            {
                                { "name",    "l2_orderbook"         },
                                { "symbols", { SYMBOL_1, SYMBOL_2 } }
                            }
                        }) }
                    }}
                };
                ws.send(sub.dump());
            }
            else if (msg->type == ix::WebSocketMessageType::Message)
            {
                handle_ws_message(msg->str);
            }
            else if (msg->type == ix::WebSocketMessageType::Error)
            {
                log_error("WebSocket Error: " + msg->errorInfo.reason);
            }
            else if (msg->type == ix::WebSocketMessageType::Close)
            {
                log_info("WebSocket Closed");
            }
        });

        ws.start();

        for (int i = 0; i < WS_CONNECT_TIMEOUT_SEC; ++i)
        {
            if (ws.getReadyState() == ix::ReadyState::Open) break;
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        if (!ever_opened.load())
        {
            log_error("WebSocket failed to open within " +
                      std::to_string(WS_CONNECT_TIMEOUT_SEC) +
                      "s — check URL/TLS/network and will retry.");
            send_telegram("WebSocket failed to open — retrying...");
            ws.stop();
            return;
        }

        while (ws.getReadyState() != ix::ReadyState::Closed)
            std::this_thread::sleep_for(std::chrono::seconds(1));

        ws.stop();
        log_warn("WebSocket loop exited — will reconnect.");
    }

    // --------------------------------------------------------
    // Handle WebSocket message — must return as fast as possible
    // --------------------------------------------------------
    void handle_ws_message(const std::string& raw)
    {
        try
        {
            json        data     = json::parse(raw);
            std::string msg_type = data.value("type", "");

            if (msg_type == "v2/ticker")
            {
                std::string symbol = data.value("symbol", "");
                if (!data.contains("mark_price") || data["mark_price"].is_null()) return;

                double mark_price = json_to_double(data["mark_price"]);

                {
                    std::lock_guard<std::mutex> lk(state_mutex_);
                    if (prices_.find(symbol) == prices_.end()) return;
                    prices_[symbol] = mark_price;
                }

                // evaluate() does zero blocking I/O — safe to call inline
                evaluate();
            }
            else if (msg_type == "l2_orderbook")
            {
                std::string symbol = data.value("symbol", "");
                if (symbol != SYMBOL_1 && symbol != SYMBOL_2) return;

                Orderbook ob;
                ob.bids = parse_levels(data.value("buy",  json::array()));
                ob.asks = parse_levels(data.value("sell", json::array()));

                {
                    std::lock_guard<std::mutex> lk(state_mutex_);
                    orderbooks_[symbol] = std::move(ob);
                }
            }
        }
        catch (const std::exception& e)
        {
            log_error(std::string("WS message parse error: ") + e.what());
        }
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
        API_KEY          = require_env("API_KEY");
        API_SECRET       = require_env("API_SECRET");
        TELEGRAM_TOKEN   = optional_env("TELEGRAM_TOKEN");
        TELEGRAM_CHAT_ID = optional_env("TELEGRAM_CHAT_ID");

        log_info("Credentials loaded — API_KEY1 prefix: " +
                 API_KEY.substr(0, std::min((int)API_KEY.size(), 6)) + "...");
        if (TELEGRAM_TOKEN.empty())
            log_warn("TELEGRAM_TOKEN not set — Telegram alerts disabled.");
    }
    catch (const std::exception& e)
    {
        std::cerr << "[FATAL] " << e.what() << "\n\n";
        std::cerr << "  API_KEY=your_api_key\n";
        std::cerr << "  API_SECRET=your_api_secret\n";
        std::cerr << "  TELEGRAM_TOKEN=your_token        (optional)\n";
        std::cerr << "  TELEGRAM_CHAT_ID=your_chat_id   (optional)\n";
        return 1;
    }

    curl_global_init(CURL_GLOBAL_DEFAULT);

    // Telegram gets its own queue so it never blocks trading logic
    WorkQueue telegram_queue;
    g_telegram_queue = &telegram_queue;

    try
    {
        PAXGXAUTBot bot;
        bot.run();
    }
    catch (const std::exception& e)
    {
        log_error(std::string("Fatal: ") + e.what());
        g_telegram_queue = nullptr;
        curl_global_cleanup();
        return 1;
    }

    g_telegram_queue = nullptr;
    curl_global_cleanup();
    return 0;
}
