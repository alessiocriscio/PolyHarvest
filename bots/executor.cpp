// executor.cpp — Low-latency Polymarket order execution engine
// Polymarket CLOB V2 — EIP-712 signed limit orders
// Deps: C++20, libcurl, libssl/libcrypto, nlohmann/json

#include <algorithm>
#include <arpa/inet.h>
#include <array>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <netinet/in.h>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>
#include <chrono>

#include <curl/curl.h>
#include <openssl/bn.h>
#include <openssl/ec.h>
#include <openssl/ecdsa.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/obj_mac.h>
#include <openssl/sha.h>

#include <nlohmann/json.hpp>

using json = nlohmann::json;

// ─── constants ───────────────────────────────────────────────────────────────
static constexpr const char *CLOB_HOST = "https://clob.polymarket.com";
static constexpr const char *ORDER_ENDPOINT = "/order";
static constexpr int LISTEN_PORT = 9999;
static constexpr int CHAIN_ID = 137; // Polygon
static constexpr const char *EXCHANGE_ADDR =
    "0xe111180000d2663c0091e4f400237545b87b996b";
static constexpr const char *DOMAIN_NAME = "ClobAuthDomain";
static constexpr const char *DOMAIN_VERSION = "2";

// ─── hex helpers ─────────────────────────────────────────────────────────────
static std::string to_hex(const uint8_t *data, size_t len) {
  std::ostringstream ss;
  for (size_t i = 0; i < len; ++i)
    ss << std::hex << std::setfill('0') << std::setw(2) << (int)data[i];
  return ss.str();
}

static std::vector<uint8_t> from_hex(const std::string &h) {
  std::string s = h;
  if (s.size() >= 2 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X'))
    s = s.substr(2);
  if (s.size() % 2 != 0)
    s = "0" + s;
  std::vector<uint8_t> out(s.size() / 2);
  for (size_t i = 0; i < out.size(); ++i)
    out[i] = static_cast<uint8_t>(std::stoul(s.substr(i * 2, 2), nullptr, 16));
  return out;
}

// ─── standalone keccak-256 (portable, no OpenSSL digest dependency) ──────────
namespace keccak_impl {

static constexpr int ROUNDS = 24;
static constexpr uint64_t RC[ROUNDS] = {
    0x0000000000000001ULL, 0x0000000000008082ULL, 0x800000000000808aULL,
    0x8000000080008000ULL, 0x000000000000808bULL, 0x0000000080000001ULL,
    0x8000000080008081ULL, 0x8000000000008009ULL, 0x000000000000008aULL,
    0x0000000000000088ULL, 0x0000000080008009ULL, 0x000000008000000aULL,
    0x000000008000808bULL, 0x800000000000008bULL, 0x8000000000008089ULL,
    0x8000000000008003ULL, 0x8000000000008002ULL, 0x8000000000000080ULL,
    0x000000000000800aULL, 0x800000008000000aULL, 0x8000000080008081ULL,
    0x8000000000008080ULL, 0x0000000080000001ULL, 0x8000000080008008ULL};
static constexpr int ROT[25] = {0,  1, 62, 28, 27, 36, 44, 6,  55,
                                20, 3, 10, 43, 25, 39, 41, 45, 15,
                                21, 8, 18, 2,  61, 56, 14};
static constexpr int PI[25] = {0,  10, 7,  11, 17, 18, 3,  5,  16, 8, 21, 24, 4,
                                15, 23, 19, 13, 12, 2,  20, 14, 22, 9, 6,  1};

inline uint64_t rotl64(uint64_t x, int n) { return (x << n) | (x >> (64 - n)); }

static void keccakf(uint64_t st[25]) {
  for (int r = 0; r < ROUNDS; ++r) {
    // θ
    uint64_t C[5], D[5];
    for (int i = 0; i < 5; ++i)
      C[i] = st[i] ^ st[i + 5] ^ st[i + 10] ^ st[i + 15] ^ st[i + 20];
    for (int i = 0; i < 5; ++i) {
      D[i] = C[(i + 4) % 5] ^ rotl64(C[(i + 1) % 5], 1);
      for (int j = 0; j < 25; j += 5)
        st[j + i] ^= D[i];
    }
    // ρ + π
    uint64_t tmp[25];
    for (int i = 0; i < 25; ++i)
      tmp[PI[i]] = rotl64(st[i], ROT[i]);
    // χ
    for (int j = 0; j < 25; j += 5)
      for (int i = 0; i < 5; ++i)
        st[j + i] = tmp[j + i] ^ (~tmp[j + (i + 1) % 5] & tmp[j + (i + 2) % 5]);
    // ι
    st[0] ^= RC[r];
  }
}

static void hash(const uint8_t *data, size_t len, uint8_t out[32]) {
  constexpr size_t RATE = 136; // 1088 bits / 8
  uint64_t st[25] = {};

  // absorb
  size_t offset = 0;
  while (offset + RATE <= len) {
    for (size_t i = 0; i < RATE / 8; ++i) {
      uint64_t lane;
      std::memcpy(&lane, data + offset + i * 8, 8);
      st[i] ^= lane;
    }
    keccakf(st);
    offset += RATE;
  }

  // last block + padding
  uint8_t block[RATE] = {};
  size_t remaining = len - offset;
  std::memcpy(block, data + offset, remaining);
  block[remaining] = 0x01; // Keccak padding (NOT SHA3 0x06)
  block[RATE - 1] |= 0x80;

  for (size_t i = 0; i < RATE / 8; ++i) {
    uint64_t lane;
    std::memcpy(&lane, block + i * 8, 8);
    st[i] ^= lane;
  }
  keccakf(st);

  // squeeze (256 bits = 32 bytes, fits in one block)
  std::memcpy(out, st, 32);
}

} // namespace keccak_impl

static std::array<uint8_t, 32> keccak256(const uint8_t *data, size_t len) {
  std::array<uint8_t, 32> out{};
  keccak_impl::hash(data, len, out.data());
  return out;
}

static std::array<uint8_t, 32> keccak256(const std::vector<uint8_t> &v) {
  return keccak256(v.data(), v.size());
}

static std::array<uint8_t, 32> keccak256(const std::string &s) {
  return keccak256(reinterpret_cast<const uint8_t *>(s.data()), s.size());
}

// ─── ABI-encode helpers (uint256 / address / bytes32) ────────────────────────
static std::vector<uint8_t> abi_encode_uint256(const std::string &decimal_str) {
  BIGNUM *bn = BN_new();
  BN_dec2bn(&bn, decimal_str.c_str());
  std::vector<uint8_t> buf(32, 0);
  int n = BN_num_bytes(bn);
  BN_bn2bin(bn, buf.data() + (32 - n));
  BN_free(bn);
  return buf;
}

static std::vector<uint8_t> abi_encode_address(const std::string &addr) {
  auto raw = from_hex(addr);
  std::vector<uint8_t> buf(32, 0);
  if (raw.size() >= 20)
    std::copy(raw.end() - 20, raw.end(), buf.begin() + 12);
  return buf;
}

static std::vector<uint8_t> abi_encode_bytes32(const std::vector<uint8_t> &b) {
  std::vector<uint8_t> buf(32, 0);
  size_t n = std::min(b.size(), (size_t)32);
  std::copy(b.begin(), b.begin() + n, buf.begin());
  return buf;
}

// ─── high-performance random 256-bit salt (no multiple BIGNUM allocations) ──
static std::string random_salt() {
  static thread_local std::mt19937_64 gen(std::random_device{}());
  std::uniform_int_distribution<uint64_t> dist;

  uint64_t p0 = dist(gen);
  uint64_t p1 = dist(gen);
  uint64_t p2 = dist(gen);
  uint64_t p3 = dist(gen);

  uint8_t bytes[32];
  for (int i = 0; i < 8; ++i) {
    bytes[i]      = static_cast<uint8_t>((p0 >> (56 - i * 8)) & 0xFF);
    bytes[8 + i]  = static_cast<uint8_t>((p1 >> (56 - i * 8)) & 0xFF);
    bytes[16 + i] = static_cast<uint8_t>((p2 >> (56 - i * 8)) & 0xFF);
    bytes[24 + i] = static_cast<uint8_t>((p3 >> (56 - i * 8)) & 0xFF);
  }

  BIGNUM *bn = BN_bin2bn(bytes, 32, nullptr);
  char *s = BN_bn2dec(bn);
  std::string out(s);
  OPENSSL_free(s);
  BN_free(bn);
  return out;
}

// ─── EIP-712 hashing (Polymarket V2) ─────────────────────────────────────────

// EIP-712 domain type hash
static std::array<uint8_t, 32> domain_type_hash() {
  static const std::string t =
      "EIP712Domain(string name,string version,uint256 chainId,address "
      "verifyingContract)";
  return keccak256(t);
}

// EIP-712 order type hash
static std::array<uint8_t, 32> order_type_hash() {
  static const std::string t =
      "Order(uint256 salt,address maker,address signer,uint256 tokenId,"
      "uint256 makerAmount,uint256 takerAmount,uint256 side,"
      "uint256 signatureType,uint256 timestamp,bytes32 metadata,bytes32 "
      "builder)";
  return keccak256(t);
}

static std::array<uint8_t, 32>
compute_domain_separator(const std::string &/*maker_addr*/) {
  auto dt = domain_type_hash();
  auto name_hash = keccak256(std::string(DOMAIN_NAME));
  auto ver_hash = keccak256(std::string(DOMAIN_VERSION));
  auto chain_enc = abi_encode_uint256(std::to_string(CHAIN_ID));
  auto addr_enc = abi_encode_address(EXCHANGE_ADDR);

  std::vector<uint8_t> buf;
  buf.insert(buf.end(), dt.begin(), dt.end());
  buf.insert(buf.end(), name_hash.begin(), name_hash.end());
  buf.insert(buf.end(), ver_hash.begin(), ver_hash.end());
  buf.insert(buf.end(), chain_enc.begin(), chain_enc.end());
  buf.insert(buf.end(), addr_enc.begin(), addr_enc.end());
  return keccak256(buf);
}

struct OrderParams {
  std::string salt;
  std::string maker;  // address
  std::string signer; // address (same as maker for EOA)
  std::string token_id;
  std::string maker_amount;
  std::string taker_amount;
  std::string side;              // "0" = BUY, "1" = SELL
  std::string sig_type;          // "0" = EOA
  std::string timestamp;         // ms
  std::vector<uint8_t> metadata; // 32 bytes, default zero
  std::vector<uint8_t> builder;  // 32 bytes, default zero
};

static std::array<uint8_t, 32> hash_order_struct(const OrderParams &o) {
  auto ot = order_type_hash();
  std::vector<uint8_t> buf;
  buf.insert(buf.end(), ot.begin(), ot.end());

  auto append = [&](const std::vector<uint8_t> &v) {
    buf.insert(buf.end(), v.begin(), v.end());
  };
  append(abi_encode_uint256(o.salt));
  append(abi_encode_address(o.maker));
  append(abi_encode_address(o.signer));
  append(abi_encode_uint256(o.token_id));
  append(abi_encode_uint256(o.maker_amount));
  append(abi_encode_uint256(o.taker_amount));
  append(abi_encode_uint256(o.side));
  append(abi_encode_uint256(o.sig_type));
  append(abi_encode_uint256(o.timestamp));
  append(abi_encode_bytes32(o.metadata));
  append(abi_encode_bytes32(o.builder));
  return keccak256(buf);
}

static std::array<uint8_t, 32> eip712_hash(const OrderParams &o) {
  auto ds = compute_domain_separator(o.maker);
  auto sh = hash_order_struct(o);
  std::vector<uint8_t> buf;
  buf.push_back(0x19);
  buf.push_back(0x01);
  buf.insert(buf.end(), ds.begin(), ds.end());
  buf.insert(buf.end(), sh.begin(), sh.end());
  return keccak256(buf);
}

// ─── HMAC-SHA256 for L2 API auth ─────────────────────────────────────────────
static std::string hmac_sha256_hex(const std::string &key,
                                   const std::string &data) {
  unsigned char out[EVP_MAX_MD_SIZE];
  unsigned int len = 0;
  HMAC(EVP_sha256(), key.data(), static_cast<int>(key.size()),
       reinterpret_cast<const unsigned char *>(data.data()), data.size(), out,
       &len);
  return to_hex(out, len);
}

// ─── libcurl callback ───────────────────────────────────────────────────────
static size_t curl_write_cb(void *ptr, size_t sz, size_t nm, std::string *s) {
  s->append(static_cast<char *>(ptr), sz * nm);
  return sz * nm;
}

// ─── convert user-facing price/size to maker/taker amounts ───────────────────
static void compute_amounts(const std::string &side, double price, double size,
                            std::string &maker_amount,
                            std::string &taker_amount) {
  // amounts in 6-decimal base units (1e6)
  uint64_t size_units = static_cast<uint64_t>(size * 1e6);
  uint64_t cost_units = static_cast<uint64_t>(price * size * 1e6);
  if (side == "BUY") {
    maker_amount = std::to_string(cost_units); // collateral offered
    taker_amount = std::to_string(size_units); // tokens wanted
  } else {
    maker_amount = std::to_string(size_units); // tokens offered
    taker_amount = std::to_string(cost_units); // collateral wanted
  }
}

// ─── main execution engine ───────────────────────────────────────────────────
class Executor {
public:
  Executor() {
    // Load credentials from environment
    const char *pk = std::getenv("POLY_PRIVATE_KEY");
    const char *ak = std::getenv("POLY_API_KEY");
    const char *pp = std::getenv("POLY_PASSPHRASE");
    const char *sec = std::getenv("POLY_API_SECRET");

    if (!pk || !ak || !pp)
      throw std::runtime_error(
          "Missing env: POLY_PRIVATE_KEY, POLY_API_KEY, POLY_PASSPHRASE");

    api_key_ = ak;
    passphrase_ = pp;
    secret_ = sec ? sec : "";
    privkey_ = from_hex(std::string(pk));

    // 1. Initialize OpenSSL keys once
    ec_key_ = EC_KEY_new_by_curve_name(NID_secp256k1);
    if (!ec_key_) {
      throw std::runtime_error("EC_KEY_new_by_curve_name failed");
    }

    BIGNUM *priv_bn = BN_bin2bn(privkey_.data(), privkey_.size(), nullptr);
    if (!priv_bn) {
      EC_KEY_free(ec_key_);
      throw std::runtime_error("BN_bin2bn failed");
    }
    EC_KEY_set_private_key(ec_key_, priv_bn);

    const EC_GROUP *grp = EC_KEY_get0_group(ec_key_);
    EC_POINT *pub = EC_POINT_new(grp);
    if (!pub) {
      BN_free(priv_bn);
      EC_KEY_free(ec_key_);
      throw std::runtime_error("EC_POINT_new failed");
    }
    EC_POINT_mul(grp, pub, priv_bn, nullptr, nullptr, nullptr);
    EC_KEY_set_public_key(ec_key_, pub);

    // Cache order and half-order
    order_ = BN_new();
    half_order_ = BN_new();
    if (!order_ || !half_order_) {
      EC_POINT_free(pub);
      BN_free(priv_bn);
      EC_KEY_free(ec_key_);
      throw std::runtime_error("BN_new failed");
    }
    BN_copy(order_, EC_GROUP_get0_order(grp));
    BN_rshift1(half_order_, order_);

    // Derive wallet address from public key
    size_t plen = EC_POINT_point2oct(grp, pub, POINT_CONVERSION_UNCOMPRESSED,
                                     nullptr, 0, nullptr);
    std::vector<uint8_t> pubbuf(plen);
    EC_POINT_point2oct(grp, pub, POINT_CONVERSION_UNCOMPRESSED, pubbuf.data(),
                       plen, nullptr);

    // Keccak-256 of x||y (skip the 0x04 prefix)
    auto hash = keccak256(pubbuf.data() + 1, plen - 1);
    address_ = "0x" + to_hex(hash.data() + 12, 20);

    // Free local temporaries
    EC_POINT_free(pub);
    BN_free(priv_bn);

    // Tor configuration
    const char *tor_env = std::getenv("USE_TOR");
    use_tor_ = (tor_env && std::string(tor_env) == "1");

    // 2. Initialize Persistent libcurl handle (HTTP Keep-Alive)
    curl_global_init(CURL_GLOBAL_ALL);
    curl_handle_ = curl_easy_init();
    if (!curl_handle_) {
      throw std::runtime_error("curl_easy_init failed");
    }

    if (use_tor_) {
      std::cerr << "[executor] SOCKS5 proxy enabled: 127.0.0.1:9050\n";
      curl_easy_setopt(curl_handle_, CURLOPT_PROXY, "socks5h://127.0.0.1:9050");
      curl_easy_setopt(curl_handle_, CURLOPT_PROXYTYPE, CURLPROXY_SOCKS5_HOSTNAME);
    }

    // Configure common low-latency libcurl options
    curl_easy_setopt(curl_handle_, CURLOPT_WRITEFUNCTION, curl_write_cb);
    curl_easy_setopt(curl_handle_, CURLOPT_TIMEOUT, 5L);
    curl_easy_setopt(curl_handle_, CURLOPT_TCP_KEEPALIVE, 1L);
    curl_easy_setopt(curl_handle_, CURLOPT_TCP_KEEPIDLE, 120L);
    curl_easy_setopt(curl_handle_, CURLOPT_TCP_KEEPINTVL, 60L);

    std::cerr << "[executor] Wallet address: " << address_ << "\n";
  }

  ~Executor() {
    if (curl_handle_) {
      curl_easy_cleanup(curl_handle_);
    }
    if (ec_key_) {
      EC_KEY_free(ec_key_);
    }
    if (order_) {
      BN_free(order_);
    }
    if (half_order_) {
      BN_free(half_order_);
    }
    curl_global_cleanup();
  }

  // mathematically correct signature generation with exact v recovery
  std::string sign_digest(const std::array<uint8_t, 32> &digest) const {
    ECDSA_SIG *sig = ECDSA_do_sign(digest.data(), 32, ec_key_);
    if (!sig) {
      throw std::runtime_error("ECDSA_do_sign failed");
    }

    const BIGNUM *r_bn = nullptr;
    const BIGNUM *s_bn = nullptr;
    ECDSA_SIG_get0(sig, &r_bn, &s_bn);

    BIGNUM *s_adj = BN_dup(s_bn);
    if (!s_adj) {
      ECDSA_SIG_free(sig);
      throw std::runtime_error("BN_dup failed");
    }

    // Ensure low-S canonical form (EIP-2)
    if (BN_cmp(s_adj, half_order_) > 0) {
      BN_sub(s_adj, order_, s_adj);
    }

    // Mathematically rigorous public key recovery to determine correct v
    int v = 27;
    const EC_GROUP *grp = EC_KEY_get0_group(ec_key_);
    const EC_POINT *Q_pub = EC_KEY_get0_public_key(ec_key_);

    BN_CTX *ctx = BN_CTX_new();
    if (ctx) {
      BN_CTX_start(ctx);
      BIGNUM *r_inv = BN_CTX_get(ctx);
      BIGNUM *e_bn = BN_CTX_get(ctx);
      BIGNUM *neg_e = BN_CTX_get(ctx);

      if (r_inv && e_bn && neg_e) {
        BN_mod_inverse(r_inv, r_bn, order_, ctx);
        BN_bin2bn(digest.data(), 32, e_bn);
        BN_mod_sub(neg_e, order_, e_bn, order_, ctx); // neg_e = order - e

        EC_POINT *R = EC_POINT_new(grp);
        EC_POINT *Q_cand = EC_POINT_new(grp);

        if (R && Q_cand) {
          for (int y_bit = 0; y_bit <= 1; ++y_bit) {
            // Reconstruct point R with x = r and y-parity = y_bit
            if (EC_POINT_set_compressed_coordinates(grp, R, r_bn, y_bit, ctx) == 1) {
              // Q_cand = neg_e * G + s_adj * R
              if (EC_POINT_mul(grp, Q_cand, neg_e, R, s_adj, ctx) == 1) {
                // Q_cand = r_inv * Q_cand
                if (EC_POINT_mul(grp, Q_cand, nullptr, Q_cand, r_inv, ctx) == 1) {
                  // Compare with our public key
                  if (EC_POINT_cmp(grp, Q_cand, Q_pub, ctx) == 0) {
                    v = 27 + y_bit;
                    break;
                  }
                }
              }
            }
          }
        }
        if (Q_cand) EC_POINT_free(Q_cand);
        if (R) EC_POINT_free(R);
      }
      BN_CTX_end(ctx);
      BN_CTX_free(ctx);
    }

    // Pack r (32 bytes) || s (32 bytes) || v (1 byte)
    std::vector<uint8_t> r_bytes(32, 0), s_bytes(32, 0);
    BN_bn2bin(r_bn, r_bytes.data() + (32 - BN_num_bytes(r_bn)));
    BN_bn2bin(s_adj, s_bytes.data() + (32 - BN_num_bytes(s_adj)));

    std::string hex_sig =
        "0x" + to_hex(r_bytes.data(), 32) + to_hex(s_bytes.data(), 32);
    uint8_t vb = static_cast<uint8_t>(v);
    hex_sig += to_hex(&vb, 1);

    BN_free(s_adj);
    ECDSA_SIG_free(sig);
    return hex_sig;
  }

  // Process one incoming signal → place order → return result JSON
  json process_signal(const json &sig) {
    try {
      auto start_time = std::chrono::high_resolution_clock::now();

      std::string token_id = sig.at("token_id").get<std::string>();
      std::string side_str = sig.at("side").get<std::string>(); // "BUY"/"SELL"
      double price = sig.at("price").get<double>();
      double size = sig.at("size").get<double>();

      std::string side_num = (side_str == "BUY") ? "0" : "1";

      std::string maker_amt, taker_amt;
      compute_amounts(side_str, price, size, maker_amt, taker_amt);

      auto now_ms = std::to_string(
          std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::system_clock::now().time_since_epoch())
              .count());

      OrderParams op;
      op.salt = random_salt();
      op.maker = address_;
      op.signer = address_;
      op.token_id = token_id;
      op.maker_amount = maker_amt;
      op.taker_amount = taker_amt;
      op.side = side_num;
      op.sig_type = "0"; // EOA
      op.timestamp = now_ms;
      op.metadata = std::vector<uint8_t>(32, 0);
      op.builder = std::vector<uint8_t>(32, 0);

      // EIP-712 sign (utilizes cached keys)
      auto digest = eip712_hash(op);
      auto sig_hex = sign_digest(digest);

      // Build POST body
      json body;
      body["order"] = {{"salt", op.salt},
                       {"maker", op.maker},
                       {"signer", op.signer},
                       {"tokenID", op.token_id},
                       {"makerAmount", op.maker_amount},
                       {"takerAmount", op.taker_amount},
                       {"side", side_str},
                       {"signatureType", 0},
                       {"timestamp", op.timestamp},
                       {"signature", sig_hex},
                       {"expiration", "0"},
                       {"taker", "0x0000000000000000000000000000000000000000"}};
      std::string clob_order_type = "GTC";
      if (sig.contains("order_type") && sig.at("order_type").get<std::string>() == "MARKET") {
        clob_order_type = "FOK";
      }
      body["orderType"] = clob_order_type;
      body["tokenID"] = token_id;

      std::string body_str = body.dump();

      // L2 HMAC auth headers
      auto ts_sec = std::to_string(std::time(nullptr));
      std::string hmac_msg = ts_sec + "POST" + ORDER_ENDPOINT + body_str;
      std::string hmac_sig = hmac_sha256_hex(secret_, hmac_msg);

      // HTTP Headers list (rebuild list is cheap, handles dynamic headers)
      struct curl_slist *hdrs = nullptr;
      hdrs = curl_slist_append(hdrs, "Content-Type: application/json");
      hdrs = curl_slist_append(hdrs, ("POLY_ADDRESS: " + address_).c_str());
      hdrs = curl_slist_append(hdrs, ("POLY_SIGNATURE: " + hmac_sig).c_str());
      hdrs = curl_slist_append(hdrs, ("POLY_TIMESTAMP: " + ts_sec).c_str());
      hdrs = curl_slist_append(hdrs, ("POLY_API_KEY: " + api_key_).c_str());
      hdrs = curl_slist_append(hdrs, ("POLY_PASSPHRASE: " + passphrase_).c_str());

      std::string url = std::string(CLOB_HOST) + ORDER_ENDPOINT;
      std::string response;

      // Re-configure curl handle (reuses connection)
      curl_easy_setopt(curl_handle_, CURLOPT_URL, url.c_str());
      curl_easy_setopt(curl_handle_, CURLOPT_HTTPHEADER, hdrs);
      curl_easy_setopt(curl_handle_, CURLOPT_POSTFIELDS, body_str.c_str());
      curl_easy_setopt(curl_handle_, CURLOPT_WRITEDATA, &response);

      CURLcode res = curl_easy_perform(curl_handle_);
      long http_code = 0;
      curl_easy_getinfo(curl_handle_, CURLINFO_RESPONSE_CODE, &http_code);
      curl_slist_free_all(hdrs);

      auto end_time = std::chrono::high_resolution_clock::now();
      auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
      std::cerr << "[executor] Total execution loop time: " << elapsed_us << " us\n";

      if (res != CURLE_OK) {
        return {{"status", "error"},
                {"order_id", ""},
                {"detail", curl_easy_strerror(res)}};
      }

      // Parse API response
      json api_resp;
      try {
        api_resp = json::parse(response);
      } catch (...) {
        return {{"status", "error"},
                {"order_id", ""},
                {"detail", "invalid JSON response"},
                {"http_code", http_code},
                {"raw", response}};
      }

      std::string order_id =
          api_resp.value("orderID", api_resp.value("order_id", ""));

      if (http_code >= 200 && http_code < 300 && !order_id.empty()) {
        std::cerr << "[executor] Order placed: " << order_id << "\n";
        return {{"status", "ok"}, {"order_id", order_id}};
      } else {
        std::cerr << "[executor] API error " << http_code << ": " << response
                  << "\n";
        return {{"status", "error"},
                {"order_id", order_id},
                {"http_code", http_code},
                {"detail", response}};
      }

    } catch (const std::exception &e) {
      std::cerr << "[executor] Exception: " << e.what() << "\n";
      return {{"status", "error"}, {"order_id", ""}, {"detail", e.what()}};
    }
  }

private:
  EC_KEY *ec_key_ = nullptr;
  BIGNUM *order_ = nullptr;
  BIGNUM *half_order_ = nullptr;
  CURL *curl_handle_ = nullptr;

  std::vector<uint8_t> privkey_;
  std::string address_;
  std::string api_key_;
  std::string passphrase_;
  bool use_tor_ = false;
  std::string secret_;
};

// ─── TCP socket server ──────────────────────────────────────────────────────
int main() {
  try {
    Executor executor;

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
      perror("socket");
      return 1;
    }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    addr.sin_port = htons(LISTEN_PORT);

    if (bind(server_fd, (sockaddr *)&addr, sizeof(addr)) < 0) {
      perror("bind");
      close(server_fd);
      return 1;
    }
    if (listen(server_fd, 4) < 0) {
      perror("listen");
      close(server_fd);
      return 1;
    }

    std::cerr << "[executor] Listening on 127.0.0.1:" << LISTEN_PORT << "\n";

    while (true) {
      sockaddr_in client_addr{};
      socklen_t client_len = sizeof(client_addr);
      int client_fd = accept(server_fd, (sockaddr *)&client_addr, &client_len);
      if (client_fd < 0) {
        perror("accept");
        continue;
      }

      // Read until we get a full JSON object (newline-delimited)
      std::string buf;
      char chunk[4096];
      while (true) {
        ssize_t n = recv(client_fd, chunk, sizeof(chunk) - 1, 0);
        if (n <= 0)
          break;
        chunk[n] = '\0';
        buf.append(chunk, n);
        if (buf.find('\n') != std::string::npos)
          break;
      }

      // Trim whitespace
      buf.erase(std::remove(buf.begin(), buf.end(), '\n'), buf.end());

      if (buf.empty()) {
        close(client_fd);
        continue;
      }

      std::cerr << "[executor] Received: " << buf << "\n";

      json signal;
      json result;
      try {
        signal = json::parse(buf);
        result = executor.process_signal(signal);
      } catch (const std::exception &e) {
        result = {{"status", "error"}, {"order_id", ""}, {"detail", e.what()}};
      }

      std::string resp = result.dump() + "\n";
      send(client_fd, resp.data(), resp.size(), 0);
      close(client_fd);
    }

    close(server_fd);
  } catch (const std::exception &e) {
    std::cerr << "[executor] Fatal: " << e.what() << "\n";
    return 1;
  }
  return 0;
}
