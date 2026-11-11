### Social Media Platform
Written in Go with hybrid fan-out, replicated LSM storage (Pebble) and MySQL, a Redis feed cache with Kafka's throughput. Inspired by Twitter's architecture.

---

Opening feeds can be slow across thousands of users when searching the disk for all followees and obtaining their latest post. One way to improve feed loading is that when posts are posted, they automatically get delivered to a feed mailbox, and users who want to access their feed can quickly open the mailbox. The posts are also saved to an LSM tree sorted by poster ID followed by time of posting, for long term storage. However, if a user has several followers, delivering to all their mailboxes can also be slow because there are so many mailboxes, and this can bottleneck other posters' feed deliveries. Thus, celebrities should be handled differently: celebrity posts are saved on the LSM tree, and upon opening a feed, they are searched up and merged with the cache containing regular followees' posts.

The cache exists on Redis, capped at the latest 20 posts, and posting sends mail to Redis via 4 Kafka consumers. If Redis is down, feeds fetch every followee's newest post from the LSM tree instead.

Why LSM trees? Both a B-Tree and LSM tree are structures that maintain a copy of the latest data in memory. The difference is how the data is stored in disk. The LSM tree is not completely ordered. It is split into chunks called SSTables, and each chunk is ordered. On the other hand, a B-Tree is completely ordered. The result is that LSM trees are good at fast writes compared to a B-Tree when there is heavy concurrent writing load because under a disk transfer bottleneck, the LSM tree just has to shove the in-memory (memtable) into an SSTable. Done. The B-Tree must carefully slot each entry. B-trees are faster when the goal is to read old data in long stretches because everything is ordered. Think about our use case: We want to ensure fast writes for users (posting). Furthermore, feed loading fetches a single latest post for a celebrity, so the post is more likely to exist in RAM where reading bottlenecks go away. when looking up a specific profile to fetch old posts, the B-Tree would be faster, but this use case does not occur very frequently. This is why I think the LSM tree is a better choice here than the B-Tree. The Kafka-style append only log is the worst option because the keys aren't sorted at all.

User data—accounts and follow relationships—are stored in MySQL. I used a secondary index to access followers by their followees and vice versa quickly, at the cost of more storage and write time.

To prevent data loss, I replicated both posts and user data. The idea is that if one database dies, the others act as backups. For posts, the nodes are leaderless, and writes are always accepted. This is to prioritize posting speed, which is important to users. Each node runs as its own program and talks to the main program over gRPC. Twitter uses its own RPC framework, Finagle. A technique called anti-entropy is used to repair and reconcile the data, which Twitter calls Replica Reconciliation. As for user data, there is a reason I chose MySQL for storage. Two users should not be allowed to sign up with the same email. With a leaderless approach like the naive Cassandra INSERT, a write just goes to whatever nodes are up, with nothing stopping a second signup from claiming the same email. Even if you add a check to prevent duplicates, if one node is behind due to a previous crash and it doesn't have the updated emails, a user may send a signup request to that behind node, leading to the cluster now containing the same email for two different accounts. The user sees that their account is created and now reconciling the data from within is too late. The fix is that MySQL comes with a feature called Group Replication, which uses Paxos, a consensus algorithm that provides strong consistency. What does this mean? Well let's look at a few facts: it contains one primary node or leader through which all writes flow. Other nodes are not allowed to accept writes with the setting `super_read_only`, which is automatically handled by group replication. This primary is the master list and contains the truth. How? MySQL guarantees that before a new primary is picked, every surviving server contains every committed signup. MySQL is configured here to not allow duplicate emails or usernames. Furthermore, a quorum is used to ensure that if a signup is written into a database, it must exist across a majority of nodes. The purpose of this is that if the primary dies, the cluster does not lose that node's data because other nodes contain the same data. Putting these facts together, the strong consistency here is preventing duplicate usernames/emails both within a single node and across multiple nodes while also ensuring that data isn't lost when the primary dies. Uber is an example of a company that uses MySQL to enforce consistency in data. The cost is two-fold: 1) availability; signups only go through if a majority of nodes are up, and 2) latency; Paxos must communicate through a leader and then to other nodes to determine if a write should go through.

I spent some time thinking about a new solution: Instead of Paxos, why not just use a leaderless approach, but with quorums, rolling back false writes in the background, and also queuing requests (pessimistic locking)? You could, and it would lead to consistent data and give you faster writes than Paxos because the writes go to nodes concurrently rather than flowing through a leader. However, reads would be slower because you need to check if a majority of nodes have the data. With Paxos, the leader is guaranteed to have updated info, so you need to check only one node, making reads quicker. For a use case like account information, I would think people login more often than they create accounts, so we want to keep reads fast.

---

### Results

10K users, 10 celebrities (10K followers each), 1.1M follows. 1,010 tweets per burst.

| Strategy | Feed fetch p99 | Delivery p99 |
|---|---|---|
| Fan out to everyone | 24.15 ms | 186.45 ms |
| Fan out to nobody | 336.11 ms | 0 ms |
| **Hybrid** | **47.11 ms** | **13.45 ms** |

Hybrid achieves **92.6%** of optimal read and **92.8%** of optimal write performance,
where % = (hybrid − worst) / (optimal − worst).

| Kafka consumers | Delivery throughput |
|---|---|
| 1 | 249 tweets/s |
| 4 | 649 tweets/s (**2.6x**) |

Added a secondary index to cut p99 follower-fetch latency from 3.83 s to 132 ms (29x), and denormalized the schema with a column for follower count to cut p99 popularity-check latency from 26.8 ms to 14.7 ms (1.8x).

---

### To setup and run:

Install [Go](https://go.dev/dl/), Java 17+, [Redis](https://redis.io/downloads/), [MySQL 8.4](https://dev.mysql.com/downloads/mysql/), and [Kafka 4.3](https://kafka.apache.org/downloads) with their `bin` folders on your `PATH`. Then run `./start.sh` from the project folder.

To stop:
Run `./stop.sh`. Stopping does not delete the stored post and user data.
