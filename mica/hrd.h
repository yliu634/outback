#pragma once

#include <assert.h>
#include <errno.h>
#include <infiniband/verbs.h>
#include <libmemcached/memcached.h>
#include <malloc.h>
#include <numaif.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>


static constexpr size_t kRoCE = false;  ///< Use RoCE
static constexpr size_t kHrdMaxInline = 16;
static constexpr size_t kHrdSQDepth = 128;   ///< Depth of all SEND queues
static constexpr size_t kHrdRQDepth = 2048;  ///< Depth of all RECV queues

static constexpr uint32_t kHrdInvalidNUMANode = 9;
static constexpr uint32_t kHrdDefaultPSN = 3185;
static constexpr uint32_t kHrdDefaultQKey = 0x11111111;
static constexpr size_t kHrdMaxLID = 256;
static constexpr size_t kHrdMaxUDQPs = 256;  ///< Maximum number of UD QPs

static constexpr size_t kHrdQPNameSize = 200;

// This needs to be a macro because we don't have Mellanox OFED for Debian
#define kHrdMlx5Atomics false
#define kHrdReservedNamePrefix "__HRD_RESERVED_NAME_PREFIX"

#define KB(x) (static_cast<size_t>(x) << 10)
#define KB_(x) (KB(x) - 1)
#define MB(x) (static_cast<size_t>(x) << 20)
#define MB_(x) (MB(x) - 1)

#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)
#define _unused(x) ((void)(x))  // Make production build happy

/// Check a condition at runtime. If the condition is false, throw exception.
static inline void rt_assert(bool condition, std::string throw_str) {
  if (unlikely(!condition)) throw std::runtime_error(throw_str);
}

/// Check a condition at runtime. If the condition is false, throw exception.
static inline void rt_assert(bool condition) {
  if (unlikely(!condition)) throw std::runtime_error("");
}

template <typename T>
static constexpr inline bool is_power_of_two(T x) {
  return x && ((x & T(x - 1)) == 0);
}

template <uint64_t power_of_two_number, typename T>
static constexpr inline T round_up(T x) {
  static_assert(is_power_of_two(power_of_two_number),
                "PowerOfTwoNumber must be a power of 2");
  return ((x) + T(power_of_two_number - 1)) & (~T(power_of_two_number - 1));
}

/// Return nanoseconds elapsed since timestamp \p t0
static double ns_since(const struct timespec& t0) {
  struct timespec t1;
  clock_gettime(CLOCK_REALTIME, &t1);
  return (t1.tv_sec - t0.tv_sec) * 1000000000.0 + (t1.tv_nsec - t0.tv_nsec);
}

/// Registry info about a QP
struct hrd_qp_attr_t {
  char name[kHrdQPNameSize];
  uint16_t lid;
  uint32_t qpn;
  union ibv_gid gid;  ///< GID, used for only RoCE

  // Info about the RDMA buffer associated with this QP
  uintptr_t buf_addr;
  uint32_t buf_size;
  uint32_t rkey;
};

struct hrd_conn_config_t {
  // Required params
  size_t num_qps = 0;  // num_qps > 0 is used as a validity check
  bool use_uc;
  volatile uint8_t* prealloc_buf;
  size_t buf_size;
  int buf_shm_key;

  // Optional params with their default values
  size_t sq_depth = kHrdSQDepth;
  size_t max_rd_atomic = 16;

  std::string to_string() {
    std::ostringstream ret;
    ret << "[num_qps " << std::to_string(num_qps) << ", use_uc "
        << std::to_string(use_uc) << ", buf size " << std::to_string(buf_size)
        << ", shm key " << std::to_string(buf_shm_key) << ", sq_depth "
        << std::to_string(sq_depth) << ", max_rd_atomic "
        << std::to_string(max_rd_atomic) << "]";
    return ret.str();
  }
};

struct hrd_dgram_config_t {
  size_t num_qps;
  volatile uint8_t* prealloc_buf;
  size_t buf_size;
  int buf_shm_key;
};

struct hrd_ctrl_blk_t {
  size_t local_hid;  // Local ID on the machine this process runs on

  // Info about the device/port to use for this control block
  size_t port_index;  // User-supplied. 0-based across all devices
  size_t numa_node;   // NUMA node id

  /// InfiniBand info resolved from \p phy_port, must be filled by constructor.
  struct {
    int device_id;               // Device index in list of verbs devices
    struct ibv_context* ib_ctx;  // The verbs device context
    uint8_t dev_port_id;         // 1-based port ID in device. 0 is invalid.
    uint16_t port_lid;           // LID of phy_port. 0 is invalid.

    union ibv_gid gid;  // GID, used only for RoCE
  } resolve;

  struct ibv_pd* pd;  // A protection domain for this control block

  // Connected QPs
  hrd_conn_config_t conn_config;
  struct ibv_qp** conn_qp;
  struct ibv_cq** conn_cq;
  volatile uint8_t* conn_buf;  // A buffer for RDMA over RC/UC QPs
  struct ibv_mr* conn_buf_mr;

  // Datagram QPs
  size_t num_dgram_qps;
  struct ibv_qp* dgram_qp[kHrdMaxUDQPs];
  struct ibv_cq *dgram_send_cq[kHrdMaxUDQPs], *dgram_recv_cq[kHrdMaxUDQPs];
  volatile uint8_t* dgram_buf;  // A buffer for RECVs on dgram QPs
  size_t dgram_buf_size;
  int dgram_buf_shm_key;
  struct ibv_mr* dgram_buf_mr;

  uint8_t pad[64];
};


void hrd_red_printf(const char* format, ...) {
#define RED_LIM 1000
  va_list args;
  int i;

  char buf1[RED_LIM], buf2[RED_LIM];
  memset(buf1, 0, RED_LIM);
  memset(buf2, 0, RED_LIM);

  va_start(args, format);

  // Marshal the stuff to print in a buffer
  vsnprintf(buf1, RED_LIM, format, args);

  // Probably a bad check for buffer overflow
  for (i = RED_LIM - 1; i >= RED_LIM - 50; i--) {
    assert(buf1[i] == 0);
  }

  // Add markers for red color and reset color
  snprintf(buf2, 1000, "\033[31m%s\033[0m", buf1);

  // Probably another bad check for buffer overflow
  for (i = RED_LIM - 1; i >= RED_LIM - 50; i--) {
    assert(buf2[i] == 0);
  }

  printf("%s", buf2);

  va_end(args);
}

uint8_t* hrd_malloc_socket(int shm_key, size_t size, size_t socket_id) {
  int shmid = shmget(shm_key, size, IPC_CREAT | IPC_EXCL | 0666 | SHM_HUGETLB);
  if (shmid == -1) {
    switch (errno) {
      case EACCES:
        hrd_red_printf(
            "HRD: SHM malloc error: Insufficient permissions."
            " (SHM key = %d)\n",
            shm_key);
        break;
      case EEXIST:
        hrd_red_printf(
            "HRD: SHM malloc error: Already exists."
            " (SHM key = %d)\n",
            shm_key);
        break;
      case EINVAL:
        hrd_red_printf(
            "HRD: SHM malloc error: SHMMAX/SHMIN mismatch."
            " (SHM key = %d, size = %d)\n",
            shm_key, size);
        break;
      case ENOMEM:
        hrd_red_printf(
            "HRD: SHM malloc error: Insufficient memory."
            " (SHM key = %d, size = %d)\n",
            shm_key, size);
        break;
      default:
        hrd_red_printf("HRD: SHM malloc error: A wild SHM error: %s.\n",
                       strerror(errno));
        break;
    }
    assert(false);
  }
}

