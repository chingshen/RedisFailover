package redis;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

/*
 * Launch a redis server on localhost:
 * e.g. src/redis-server ./redis1.conf
 * e.g. src/redis-server ./redis2.conf
 * 
 * Launch a RedisNodeMonitor:
 * java -cp .:../lib/zookeeper-3.4.5.jar:../lib/slf4j-api-1.6.1.jar:../lib/slf4j-log4j12-1.6.1.jar:../lib/log4j-1.2.15.jar:../lib/jedis-2.1.0.jar redis/RedisNodeMonitor localhost 6001 
 */

public class RedisNodeMonitor implements Watcher,
		AsyncCallback.ChildrenCallback
{
	final static Logger logger = LoggerFactory.getLogger(RedisNodeMonitor.class);

	private Jedis jedis;
	private String zookeeperAddress = "localhost:2181";
	private String redisAddress;
	private String masterAddress;
	private ZooKeeper zooKeeper;
	private int sessionTimeout = 3000;
	private final int PING_INTERVAL = 3000;

	private AtomicBoolean isMaster = new AtomicBoolean(false);
	private AtomicLong sequenceNumber;

	public RedisNodeMonitor(String ip, int port)
	{
		this.redisAddress = ip + ":" + port;
		this.jedis = new Jedis(ip, port);
	}

	public void startRedisMonitor()
	{
		try
		{
			this.zooKeeper = new ZooKeeper(this.zookeeperAddress, sessionTimeout, this);
			createRootIfNotExists();
			createSlaveIfNotExists();

			String node = ZKRedisSetting.REDIS_SLAVE + "/" + redisAddress + ",";
			String znode = zooKeeper.create(node, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
			sequenceNumber = new AtomicLong(parseSequenceNumber(znode));
			zooKeeper.getChildren(ZKRedisSetting.REDIS_SLAVE, true, this, null);
			while (true)
			{
				Thread.sleep(PING_INTERVAL);
				jedis.ping();
			}

		} catch (JedisConnectionException e)
		{
			logger.info("Could not connect to Redis server: "
					+ this.redisAddress + ", " + e);
			e.printStackTrace();
			System.exit(-1);
		} catch (Exception e)
		{
			logger.info("Exception:", e);
			e.printStackTrace();
			System.exit(-1);
		}
	}

	private static void printUsage()
	{
		System.out.println("RedisNodeMonitor [redis_IP] [redis_Port]");
	}

	public static void main(String[] args)
	{
		if (args.length < 2)
		{
			printUsage();
			System.exit(1);
		}

		String ip = args[0];
		int port = Integer.parseInt(args[1]);
		RedisNodeMonitor nodeMonitor = new RedisNodeMonitor(ip, port);
		nodeMonitor.startRedisMonitor();
	}

	private long parseSequenceNumber(String znode)
	{
		return Long.parseLong(znode.substring(znode.lastIndexOf(",") + 1));
	}

	private long getLowestNumber(List<String> children)
	{
		long lowest = sequenceNumber.get();
		for (String child : children)
		{
			long current = parseSequenceNumber(child);
			if (current < lowest)
				lowest = current;
		}
		return lowest;
	}

	public void promoteToMaster()
	{
		jedis.slaveofNoOne();
		this.isMaster.set(true);
		logger.debug("---------------------------- promoteToMaster ----------------------------");
	}

	public void slaveOfMaster()
	{
		try
		{
			byte[] data = zooKeeper.getData(ZKRedisSetting.REDIS_MASTER, false, null);
			String masterData = new String(data, ZKRedisSetting.CHARSET);
			logger.info("data:" + masterData);

			if (this.masterAddress != null && masterData != null
					&& this.masterAddress.equals(masterData))
				return;

			this.masterAddress = masterData;
			String address[] = masterData.split(":");
			String ip = address[0];
			int port = Integer.parseInt(address[1]);
			logger.debug("---------------------------- slaveOfMaster ----------------------------");
			logger.debug("IP:" + ip + " Port:" + port);
			jedis.slaveofNoOne();
			jedis.slaveof(ip, port);
		} catch (KeeperException.NoNodeException ex)
		{
		} catch (KeeperException e)
		{
			e.printStackTrace();
		} catch (InterruptedException e)
		{
			e.printStackTrace();
		}

	}

	@Override
	public void process(WatchedEvent event)
	{
		EventType type = event.getType();
		KeeperState state = event.getState();
		String path = event.getPath();
		logger.debug("~~~~~~~~~~~~~~~~~~~~~~~~~~START~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
		logger.debug("Got ZooKeeper event, state: " + state + ", type: " + type
				+ ", path: " + path);
		logger.debug("~~~~~~~~~~~~~~~~~~~~~~~~~~~END~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

		if (type == Watcher.Event.EventType.None)
		{
			processNoneEvent(event);
		} else
			if (type == Watcher.Event.EventType.NodeCreated)
			{
				logger.debug("---------------------------- NodeCreated ----------------------------");
				if (!this.isMaster.get())
				{
					slaveOfMaster();
				}
			} else
				if (type == Watcher.Event.EventType.NodeChildrenChanged)
				{
					logger.debug("---------------------------- NodeChildrenChanged ----------------------------");
					try
					{
						if (!this.isMaster.get())
						{
							zooKeeper.exists(ZKRedisSetting.REDIS_MASTER, this);
							zooKeeper.getChildren(ZKRedisSetting.REDIS_SLAVE, true, this, null);
						}
					} catch (Exception e)
					{
						e.printStackTrace();
					}
				}
	}

	public void processNoneEvent(WatchedEvent event)
	{
		switch (event.getState())
		{
			case SyncConnected:
				try
				{
					Stat stat = zooKeeper.exists(ZKRedisSetting.REDIS_MASTER, false);
					if (stat == null)
					{
						zooKeeper.create(ZKRedisSetting.REDIS_MASTER, this.redisAddress.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
						logger.debug("Connected to Redis master on "
								+ this.redisAddress + ".");
						promoteToMaster();
					} else
					{
						slaveOfMaster();
					}
				} catch (Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
				break;
			case Disconnected:
				logger.debug("Disconnected");
				break;
			case Expired:
				logger.debug("Expired");
				break;
		}
	}

	public void createRootIfNotExists() throws KeeperException, InterruptedException
	{
		Stat stat = zooKeeper.exists(ZKRedisSetting.ROOT, false);
		if (stat == null)
		{
			zooKeeper.create(ZKRedisSetting.ROOT, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			logger.debug("Created " + ZKRedisSetting.ROOT + " node.");
		}

	}

	public void createSlaveIfNotExists() throws KeeperException, InterruptedException
	{
		Stat stat = zooKeeper.exists(ZKRedisSetting.REDIS_SLAVE, false);
		if (stat == null)
		{
			zooKeeper.create(ZKRedisSetting.REDIS_SLAVE, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			logger.debug("Created " + ZKRedisSetting.REDIS_SLAVE + " node.");
		}
	}

	@Override
	public void processResult(int rc, String path, Object ctx, List<String> children)
	{
		logger.debug("---------------------------- processResult ----------------------------");
		if (this.isMaster.get())
			return;

		if (rc == KeeperException.Code.OK.intValue())
		{
			if (getLowestNumber(children) == sequenceNumber.get())
			{
				try
				{
					if (zooKeeper.exists(ZKRedisSetting.REDIS_MASTER, false) == null)
					{
						zooKeeper.create(ZKRedisSetting.REDIS_MASTER, this.redisAddress.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
						logger.debug("Connected to Redis master on "
								+ this.redisAddress + ".");
						promoteToMaster();
					}
				} catch (Exception e)
				{
					e.printStackTrace();
				}
			} else
			{
				slaveOfMaster();
			}
		}
	}
}
