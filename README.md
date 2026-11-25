## Social Media Posting
Written in Go with hybrid fan-out caching (Redis), replicated storage (Cassandra and MySQL), cache delivery throughput with Kafka, and a solution to the dual-write problem with no loss nor duplicates. Inspired by Twitter's architecture.

---

### Caching

Opening feeds can be slow across thousands of users, which requires searching your database for all follows and obtaining their latest post. One way to improve feed fetching to deliver posts to a local cache or feed mailbox upon posting, and users who want to access their feed can just open the mailbox, which is quick. The posts are also saved to your database for long term storage. There is a problem: if a user has several followers, delivering to all those mailboxes can also be slow because there are so many, and this can slow down feed deliveries for everyone. Thus, celebrities should be handled differently: they are sent to a separate cache. Not a mailbox, but a regular cache where each key is a celebrity and the value holds their latest post. Kafka is used to speed up the cache delivery throughput.

---

### Data Structures

Both a B-tree and LSM tree are special data structures that allow data to persist on disk. However, they both maintain a copy of the latest data in memory. The difference is how the data is stored in disk.
- Writing: The LSM tree is not completely ordered. It is split into chunks called SSTables, and each chunk is ordered. On the other hand, a B-tree is completely ordered. The result is that LSM trees are good at fast writes compared to a B-tree when there is heavy concurrent writing load because under a disk transfer bottleneck, the LSM tree just has to shove the in-memory (memtable) into an SSTable. Done. The B-tree must carefully slot each entry.
- Reading: B-trees are faster when the goal is to read old data in long stretches because everything is ordered.

Posts are stored in Cassandra, which uses LSM trees under the hood. Why choose LSM trees here? Think about our use case for posting: it happens constantly and we want to ensure fast writes for users. Reading from the database does not happen much, except When looking up a specific profile to fetch old posts. The Kafka-style append only log is the worst option because the keys aren't sorted at all.

User data—accounts and follow relationships—are stored in MySQL. This one uses B-trees under the hood. We want reads to be fast because people login more often (including refresh tokens) than they do signing up, and every Kafka feed delivery must fetch the list of followers from MySQL, which is more reading. I used a secondary index to find followers by the user quickly, at the cost of more storage (creates another B-tree) and write time. I also added a separate column for follower count to quickly check if someone is a celebrity to decide delivery behavior.

---

### Replication

To prevent data loss, I replicated both posts and user data. The idea is that if one database dies, the others act as backups.

For posts, the nodes are leaderless. Writes go to all replicas at once and the database can even accept multiple writes at once, making posting very fast, which is important to users. A technique called anti-entropy is used to repair and reconcile the data in the background in Cassandra, which Twitter calls Replica Reconciliation for their own Manhattan Database.

As for user data, there is a constraint: users should not be allowed to sign up with the same email or same username. If this was tried on a leaderless setup, two replicas may accept the writes at the same time, and now you have a consistency problem. Both usernames exist in the database with different emails. Or a recovering node may accept or reject a username when it shouldn't. One simple fix is to make all writes flow one by one through a single source of truth, a leader replica. This leader checks for duplicates. Then use a write quorum to ensure that data is backed up on more than one node, in case the primary fails. Lastly, if the primary dies and a new one is picked, make sure that it has every committed write before it can accept reads and writes. This is nothing but leader-based Paxos or Group Replication with MySQL. You've now prevented invalid signups. Also, only the primary replica needs to be read to ensure updated information since it is guaranteed to be up to date, unlike leaderless, which needs to check all replicas to ensure that you're reading updated information. This makes reads faster on leader-based. There are two costs: 1) write availability: signups only go through if a majority of nodes are up, and 2) write speed: Paxos must communicate through a leader and then to other nodes to determine if a write should go through. It also stalls during leader election.

---

### Exactly-Once Deliveries

Finally, the dual-write problem was solved to ensure that posts that get posted make it to the feed as well. I used an outbox on Cassandra, polled every 10 seconds and sent to Kafka. However, there are situations where duplicates may occur.
- For example, with consistency level set to `ONE`, Cassandra only scans one replica for a single row. It intends to scan a chunk of rows and determines ahead of time which replica to scan based on an algorithm, something like which replica would respond the fastest. Then it scans the chunk off that replica. If that replica happens to be lagging in writes or deletions, there may be duplicates sent to Kafka or missing data. If consistency level is set to `ALL`, Cassandra checks all nodes and reconciles the data before reporting it, which has the least risk of loss or duplicates. The typical cost is more read time. However, I don't believe there's a problem for my use case. If the database is sharded, polling can happen in parallel across storage. It also scans only new rows in the outbox table, which is quick. Hence I used consistency level  `ALL`. The only issue is that `ALL` fails if any node is down. To fix this, I failover to `QUORUM` which only checks a majority of nodes.
- Independent of the consistency level, duplicates may also occur if the poller dies after sending data but before deleting it from the outbox. I use idempotency keys in Redis to fix this.
- Lastly, another cause of duplicates is zombie workers in Kafka. A worker freezes, the system thinks it's dead, another worker takes over the partition, and the old worker wakes up like a zombie, continuing to process the same partition. Messages get processed twice. The idempotency key helps, but there is one edge case: two workers process the same message at once. In this case, they may check if the key exists, both receive a "no" and proceed to put the mail in Redis. One fix is to use Redis `WATCH` with a transaction: 1) the idempotency key in Redis (or lack there of) gets watched ahead of time by both workers 2) both workers prepare the mail in advance along with idempotency key in the same transaction 3) the first one to commit a transaction sends all the mail to Redis in a single batch upon which Redis delivers the mail with no interruptions from any other request 4) the second worker's transaction gets rejected due to the idempotency key having changed since the last watch. Redis's docs classify this as optimistic locking.

---

### Results

100K users, 100 celebrities (17K followers each), 6.7M follows. Each user follows 17 celebrities and about 50 regular users.

10,000 normal posts + 100 celeb posts tested.

| Strategy | Feed-Fetch p99 | Delivery p99 |
|---|---|---|
| Fan Out to Everyone | 17.50 ms | 11,308.30 ms |
| No Caching | 198.24 ms | 0 ms |
| Fan Out Only Normal Posts (Hybrid) | 17.00 ms | 4,345.29 ms |

Hybrid achieves **100.3%** of optimal read performance, where % = (hybrid − worst) / (fan out to everyone − worst).

Hybrid's performance is 11.7x better than no caching.

Hybrid does **22.7%** of full fan-out's delivery work: 500K normal posts + 100 celeb posts written to mailbox, instead of all 2.2M posts.

| Kafka consumers | Delivery throughput |
|---|---|
| 1 | 599 tweets/s |
| 4 | 1,970 tweets/s (**3.3x**) |

Added a secondary index to cut p99 follower-fetch latency from 11.84 s to 250.65 ms (47x), and denormalized the schema with a column for follower count to cut p99 popularity-check latency from 94.54 ms to 8.54 ms (11x).

---

### To setup and run:

On Linux, install [Go](https://go.dev/dl/), Java 17 (through your package manager, e.g. `sudo apt install openjdk-17-jdk-headless`), [Redis](https://redis.io/downloads/), [MySQL 8.4](https://dev.mysql.com/downloads/mysql/), [Kafka 4.3](https://kafka.apache.org/downloads), and [Cassandra 5.0](https://cassandra.apache.org/_/download.html) with their `bin` folders on your `PATH`. Then run `./start.sh` from the project folder.

To stop:
Run `./stop.sh`. Stopping does not delete the stored post and user data.

Both start and stop should take around a minute to finish.

I used ghz and sent commands to a gRPC API that calls functions to run performance benchmarks. The sign-ups and follow calls work, but it's slow when you have to populate 100K users and 6.7M follows relationships quickly. I used MySQL terminal directly for this. 100K users, 100 celebrities, each user follows 17 celebrities and 50 random users.
