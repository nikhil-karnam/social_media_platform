### Social Media Platform
Written in Go with hybrid fan-out, replicated LSM storage (Cassandra) and MySQL, a Redis feed cache with Kafka's throughput. Inspired by Twitter's architecture.

---

Opening feeds can be slow across thousands of users when searching the disk for all followees and obtaining their latest post. One way to improve feed loading is that when posts are posted, they automatically get delivered to a feed mailbox, and users who want to access their feed can quickly open the mailbox. The posts are also saved to an LSM tree sorted by poster ID followed by time of posting, for long term storage. However, if a user has several followers, delivering to all their mailboxes can also be slow because there are so many mailboxes, and this can bottleneck other posters' feed deliveries. Thus, celebrities should be handled differently: celebrity posts are saved on the LSM tree, and upon opening a feed, they are searched up and merged with the cache containing regular followees' posts.

The cache exists on Redis, capped at the latest 20 posts, and posting sends mail to Redis via 4 Kafka consumers. If Redis is down, feeds fetch every followee's newest post from the LSM tree instead.

Why LSM trees? Both a B-Tree and LSM tree are structures that maintain a copy of the latest data in memory. The difference is how the data is stored in disk. The LSM tree is not completely ordered. It is split into chunks called SSTables, and each chunk is ordered. On the other hand, a B-Tree is completely ordered. The result is that LSM trees are good at fast writes compared to a B-Tree when there is heavy concurrent writing load because under a disk transfer bottleneck, the LSM tree just has to shove the in-memory (memtable) into an SSTable. Done. The B-Tree must carefully slot each entry. B-trees are faster when the goal is to read old data in long stretches because everything is ordered. Think about our use case: We want to ensure fast writes for users (posting). Furthermore, feed loading fetches a single latest post for a celebrity, so the post is more likely to exist in RAM where reading bottlenecks go away. when looking up a specific profile to fetch old posts, the B-Tree would be faster, but this use case does not occur very frequently. This is why I think the LSM tree is a better choice here than the B-Tree. The Kafka-style append only log is the worst option because the keys aren't sorted at all.

User data—accounts and follow relationships—are stored in MySQL. I used a secondary index to access followers by their followees and vice versa quickly, at the cost of more storage and write time.

To prevent data loss, I replicated both posts and user data. The idea is that if one database dies, the others act as backups. For posts, the nodes are leaderless. Writes go to all replicas at once and the database can even accept multiple writes at once, making posting very fast, which is important to users. A technique called anti-entropy is used to repair and reconcile the data in the background in Cassandra, which Twitter calls Replica Reconciliation for their own Manhattan Database. As for user data, there is a constraint: users should not be allowed to sign up with the same email or same username. If this was tried on a leaderless setup, two replicas may accept the writes at the same time, and now you have a consistency problem. Both usernames exist in the database with different emails. One simple fix is to make all writes flow one by one through a single source of truth, a leader replica. This leader checks for duplicates and also uses a quorum to make sure that if a write exists in the database, it must also exist on a majority of nodes, ensuring data backup. Furthermore, if the primary dies and a new one is picked, make sure that it has every committed write before it can accept reads and writes. This is nothing but Paxos or Group Replication with MySQL. You've now prevented invalid signups. MySQL also uses B+ Trees under the hood, and in addition, only the primary replica needs to be read to ensure updated information since it is guaranteed to be up to date, unlike leaderless, which needs to check all replicas to ensure that you're reading updated information. This makes reads faster, which makes sense for user info: People login more often than they sign-up, and follower lists are fetched for each post's cache fan-out, which is more reading. The cost is two-fold: 1) write availability; signups only go through if a majority of nodes are up, and 2) latency; Paxos must communicate through a leader and then to other nodes to determine if a write should go through. It also stalls during leader election.

---

### Results

100K users, 100 celebrities (17K followers each), 6.7M follows. Each user follows 17 celebrities and about 50 regular users. 10,100 tweets per burst.

| Strategy | Feed fetch p99 | Delivery p99 |
|---|---|---|
| Fan out to everyone | 17.84 ms | 14,534.60 ms |
| Fan out to nobody | 185.16 ms | 0 ms |
| Hybrid | **56.54 ms** | **4,715.17 ms** |

Hybrid achieves **76.9%** of optimal read performance, where % = (hybrid − worst) / (optimal − worst).

Hybrid does **22.7%** of full fan-out's delivery work: 500K mailbox writes instead of 2.2M.

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
