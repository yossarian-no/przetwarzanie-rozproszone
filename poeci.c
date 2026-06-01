#include <mpi.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>

#define MAX_PROC 128
#define DEBUG     0
#define WALLCLOCK 0

enum {
    REQUEST   = 1,
    ACK       = 2,
    ROLE_INFO = 3,
    RELEASE   = 4,
    DONE      = 5,
};

enum { RESTING, SEEKING, IN_CIRCLE, TOMBSTONE };

enum { ROLE_ALK, ROLE_ZAK, ROLE_SEP };

typedef struct {
    int tag;
    int sender;
    int ts;
    int req_ts;
    int req_id;
    int n_alk, n_zak, n_sep;
} Msg;

typedef struct { int ts, rank, done; } QEntry;

static int rank, P, K;
static int lclock;
static int state;

static int my_req_ts;
static int ack_count;

static QEntry queue[MAX_PROC];
static int    qsize;

static int circle_mates[MAX_PROC];
static int circle_size;
static int role_info_sent_to[MAX_PROC];
static int role_info_got[MAX_PROC];

static int my_alk, my_zak, my_sep;
static int hist_alk[MAX_PROC], hist_zak[MAX_PROC], hist_sep[MAX_PROC];

static int done_received;
static int my_role;

static pthread_mutex_t mu  = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  cond = PTHREAD_COND_INITIALIZER;

#if WALLCLOCK
#include <sys/time.h>
static double wall_ms(void)
{
    struct timeval tv; gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000.0 + tv.tv_usec / 1000.0;
}
#define LOG(...)  do { printf("[%.0f] [%d] [t%d] ", wall_ms(), rank, lclock); \
                       printf(__VA_ARGS__); printf("\n"); fflush(stdout); } while (0)
#else
#define LOG(...)  do { printf("[%d] [t%d] ", rank, lclock); printf(__VA_ARGS__); \
                       printf("\n"); fflush(stdout); } while (0)
#endif
#if DEBUG
#define DLOG(...) LOG(__VA_ARGS__)
#else
#define DLOG(...) ((void)0)
#endif

static const char *role_name(int r)
{
    return r == ROLE_ALK ? "przynoszacy alkohol"
         : r == ROLE_ZAK ? "przynoszacy zakaske"
         : "sep (tylko pije)";
}

static void clock_recv(int ts)
{
    if (ts > lclock) lclock = ts;
    lclock++;
}

static void send_to(int dest, int tag, int req_ts, int req_id,
                    int a, int z, int s)
{
    Msg m;
    memset(&m, 0, sizeof m);
    m.tag = tag; m.sender = rank;
    m.req_ts = req_ts; m.req_id = req_id;
    m.n_alk = a; m.n_zak = z; m.n_sep = s;
    m.ts = ++lclock;
    MPI_Send(&m, sizeof m, MPI_BYTE, dest, tag, MPI_COMM_WORLD);
}

static void broadcast(int tag, int req_ts, int req_id, int a, int z, int s)
{
    for (int i = 0; i < P; i++)
        if (i != rank) send_to(i, tag, req_ts, req_id, a, z, s);
}

static int q_cmp(QEntry x, QEntry y)
{
    if (x.ts != y.ts) return x.ts - y.ts;
    return x.rank - y.rank;
}

static int q_find(int ts, int r)
{
    QEntry e = { ts, r, 0 };
    for (int i = 0; i < qsize; i++)
        if (q_cmp(queue[i], e) == 0) return i;
    return -1;
}

static void q_insert(int ts, int r)
{
    QEntry e = { ts, r, 0 };
    int i = 0;
    while (i < qsize && q_cmp(queue[i], e) < 0) i++;
    if (i < qsize && q_cmp(queue[i], e) == 0) return;
    memmove(&queue[i + 1], &queue[i], (qsize - i) * sizeof(QEntry));
    queue[i] = e;
    qsize++;
}

static void q_mark_done(int ts, int r)
{
    int pos = q_find(ts, r);
    if (pos >= 0) queue[pos].done = 1;
}

static void q_cleanup(void)
{
    int changed = 1;
    while (changed) {
        changed = 0;
        for (int b = 0; (b + 1) * K <= qsize; b++) {
            int all_done = 1;
            for (int i = b * K; i < (b + 1) * K; i++)
                if (!queue[i].done) { all_done = 0; break; }
            if (all_done) {
                int s = b * K;
                memmove(&queue[s], &queue[s + K], (qsize - s - K) * sizeof(QEntry));
                qsize -= K;
                changed = 1;
                break;
            }
        }
    }
}

static void assign_roles(void)
{
    int ranks[MAX_PROC], a[MAX_PROC], z[MAX_PROC];
    int n = 0;

    ranks[n] = rank; a[n] = my_alk; z[n] = my_zak; n++;
    for (int i = 0; i < circle_size; i++) {
        int r = circle_mates[i];
        ranks[n] = r; a[n] = hist_alk[r]; z[n] = hist_zak[r]; n++;
    }

    int alk = 0;
    for (int i = 1; i < n; i++)
        if (a[i] < a[alk] || (a[i] == a[alk] && ranks[i] < ranks[alk])) alk = i;

    int zak = -1;
    for (int i = 0; i < n; i++) {
        if (i == alk) continue;
        if (zak < 0 || z[i] < z[zak] || (z[i] == z[zak] && ranks[i] < ranks[zak])) zak = i;
    }

    if (ranks[alk] == rank)            my_role = ROLE_ALK;
    else if (zak >= 0 && ranks[zak] == rank) my_role = ROLE_ZAK;
    else                               my_role = ROLE_SEP;
}

static void update_my_history(void)
{
    if (my_role == ROLE_ALK) my_alk++;
    else if (my_role == ROLE_ZAK) my_zak++;
    else my_sep++;
}

static void format_group(char *buf, const int *mates, int n)
{
    int all[MAX_PROC], m = 0;
    all[m++] = rank;
    for (int i = 0; i < n; i++) all[m++] = mates[i];
    for (int i = 0; i < m; i++)
        for (int j = i + 1; j < m; j++)
            if (all[j] < all[i]) { int t = all[i]; all[i] = all[j]; all[j] = t; }
    int off = 0;
    off += sprintf(buf + off, "[");
    for (int i = 0; i < m; i++)
        off += sprintf(buf + off, "%s%d", i ? "," : "", all[i]);
    sprintf(buf + off, "]");
}

static void try_enter_circle(void)
{
    if (state != SEEKING)      return;
    if (ack_count != P - 1)    return;

    int pos = q_find(my_req_ts, rank);
    if (pos < 0)               return;
    int g           = pos / K;
    int block_start = g * K;
    int block_end   = block_start + K;
    if (qsize < block_end)     return;

    int mates[MAX_PROC], n = 0;
    for (int i = block_start; i < block_end; i++)
        if (queue[i].rank != rank) mates[n++] = queue[i].rank;

    for (int i = 0; i < n; i++) {
        if (!role_info_sent_to[mates[i]]) {
            send_to(mates[i], ROLE_INFO, 0, 0, my_alk, my_zak, my_sep);
            role_info_sent_to[mates[i]] = 1;
        }
    }

    for (int i = 0; i < n; i++)
        if (!role_info_got[mates[i]]) return;

    memcpy(circle_mates, mates, n * sizeof(int));
    circle_size = n;

    assign_roles();
    state = IN_CIRCLE;
    char grp[512]; format_group(grp, circle_mates, circle_size);
    LOG("Jestem w kolku %s jako %s", grp, role_name(my_role));
    pthread_cond_signal(&cond);
}

static void on_request(const Msg *m)
{
    q_insert(m->req_ts, m->sender);
    send_to(m->sender, ACK, m->req_ts, m->sender, 0, 0, 0);
    DLOG("ACK -> %d", m->sender);
    try_enter_circle();
}

static void on_ack(const Msg *m)
{
    if (m->req_id != rank || m->req_ts != my_req_ts) return;
    if (state != SEEKING) return;
    ack_count++;
    DLOG("ACK %d/%d", ack_count, P - 1);
    try_enter_circle();
}

static void on_role_info(const Msg *m)
{
    role_info_got[m->sender] = 1;
    hist_alk[m->sender] = m->n_alk;
    hist_zak[m->sender] = m->n_zak;
    hist_sep[m->sender] = m->n_sep;
    try_enter_circle();
}

static void on_release(const Msg *m)
{
    q_mark_done(m->req_ts, m->req_id);
    q_cleanup();
    DLOG("RELEASE od %d", m->req_id);
    try_enter_circle();
}

static void release_circle(void)
{
    broadcast(RELEASE, my_req_ts, rank, 0, 0, 0);
    q_mark_done(my_req_ts, rank);
    q_cleanup();
    update_my_history();
    state = RESTING;
    LOG("Cale kolko skonczylo, zwalniam miejsce");
    pthread_cond_signal(&cond);
}

static void on_done(const Msg *m)
{
    (void)m;

    if (state != IN_CIRCLE && state != TOMBSTONE) return;
    done_received++;
    DLOG("DONE %d/%d", done_received, K - 1);
    if (state == TOMBSTONE && done_received == K - 1)
        release_circle();
}

static void *receiver(void *arg)
{
    (void)arg;
    for (;;) {
        Msg m;
        MPI_Recv(&m, sizeof m, MPI_BYTE, MPI_ANY_SOURCE, MPI_ANY_TAG,
                 MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        pthread_mutex_lock(&mu);
        clock_recv(m.ts);
        switch (m.tag) {
        case REQUEST:   on_request(&m);   break;
        case ACK:       on_ack(&m);       break;
        case ROLE_INFO: on_role_info(&m); break;
        case RELEASE:   on_release(&m);   break;
        case DONE:      on_done(&m);      break;
        }
        pthread_mutex_unlock(&mu);
    }
    return NULL;
}

static void start_seeking(void)
{
    state = SEEKING;
    ack_count = 0;
    circle_size = 0;
    done_received = 0;
    memset(role_info_got, 0, P * sizeof(int));
    memset(role_info_sent_to, 0, P * sizeof(int));

    my_req_ts = ++lclock;
    q_insert(my_req_ts, rank);
    LOG("Rozpoczynam staranie o kolko");
    broadcast(REQUEST, my_req_ts, rank, 0, 0, 0);
}

static void enter_tombstone(void)
{

    state = TOMBSTONE;
    LOG("Skonczylem biesiade, czekam na reszte kolka (nagrobek)");

    for (int i = 0; i < circle_size; i++)
        send_to(circle_mates[i], DONE, 0, 0, 0, 0, 0);

    if (done_received >= K - 1)
        release_circle();
}

int main(int argc, char **argv)
{
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    if (provided < MPI_THREAD_MULTIPLE) {
        fprintf(stderr, "Brak MPI_THREAD_MULTIPLE (provided=%d)\n", provided);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &P);

    if (argc < 2) {
        if (rank == 0) fprintf(stderr, "uzycie: %s K\n", argv[0]);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    K = atoi(argv[1]);

    if (K <= 0 || P < K) {
        if (rank == 0)
            fprintf(stderr, "BLAD: wymagane P >= K >= 1 (P=%d, K=%d)\n", P, K);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    srand((unsigned) (time(NULL) ^ (rank << 8)));
    state = RESTING;

    pthread_t tid;
    pthread_create(&tid, NULL, receiver, NULL);

    pthread_mutex_lock(&mu);
    for (;;) {

        LOG("Spie");
        pthread_mutex_unlock(&mu);
        sleep(1 + rand() % 3);
        pthread_mutex_lock(&mu);

        start_seeking();
        while (state == SEEKING)
            pthread_cond_wait(&cond, &mu);

        pthread_mutex_unlock(&mu);
        sleep(1 + rand() % 3);
        pthread_mutex_lock(&mu);

        enter_tombstone();
        while (state == TOMBSTONE)
            pthread_cond_wait(&cond, &mu);

    }
    pthread_mutex_unlock(&mu);

    MPI_Finalize();
    return 0;
}

