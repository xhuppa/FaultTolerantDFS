package edu.gmu.cs475;

import edu.gmu.cs475.internal.IKVStore;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.zookeeper.CreateMode;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class KVStore extends AbstractKVStore {

	// checks connection of client and gets updated by calling stateChanged
	private boolean isConnected = true;
	private LeaderLatch leaderLatch;
	private PersistentNode ephemeralNode;
	private TreeCache treeCache;
	// Map that holds a key & read/write lock
	private ConcurrentHashMap<String, ReentrantReadWriteLock> lockMap = new ConcurrentHashMap<>();
	private ConcurrentHashMap<String, String> keyValueMap = new ConcurrentHashMap<>();
	private ConcurrentHashMap<String, ArrayList<String>> clientMap = new ConcurrentHashMap<>();

	/**
	 * This callback is invoked once your client has started up and published an RMI endpoint.
	 * <p>
	 * In this callback, you will need to set-up your ZooKeeper connections, and then publish your
	 * RMI endpoint into ZooKeeper (publishing the hostname and port)
	 * <p>
	 * You will also need to set up any listeners to track ZooKeeper events
	 *
	 * @param localClientHostname Your client's hostname, which other clients will use to contact you
	 * @param localClientPort     Your client's port number, which other clients will use to contact you
	 */
	@Override
	public void initClient(String localClientHostname, int localClientPort) {
		String ephemeralNodePath = ZK_MEMBERSHIP_NODE + "/" + getLocalConnectString();
		String leaderPath = ZK_LEADER_NODE;

		// Never Use Protection!
		ephemeralNode = new PersistentNode(zk, CreateMode.EPHEMERAL, false, ephemeralNodePath, new byte[0]);
		ephemeralNode.start();
		// LeaderLatch.CloseMode.NOTIFY_LEADER notifies listeners if the latch closes
		leaderLatch = new LeaderLatch(zk, leaderPath, getLocalConnectString(), LeaderLatch.CloseMode.NOTIFY_LEADER);
		// Tree cache for the zk client that keeps all data from all children of the membership path
		treeCache = new TreeCache(zk, ZK_MEMBERSHIP_NODE);
		try {
			// Let LeaderLatch work its magic on electing a leader
			leaderLatch.start();
			treeCache.start();

		} catch (Exception e){ e.printStackTrace(); }
		leaderLatch.addListener(new LeaderLatchListener() {
			// Called when hasLeaderShip goes from false -> true
			@Override
			public void isLeader() {
				try {
					if(!leaderLatch.hasLeadership()) {
						System.out.println("Leader is : " + leaderLatch.getLeader().getId());
					}
				}catch (Exception e) { e.printStackTrace(); }
			}
			// Called when hasLeaderShip goes from true -> false
			@Override
			public void notLeader() {
				try {
					if (leaderLatch.hasLeadership()) {
						System.out.println(leaderLatch.getLeader().getId() + " is NOT the Leader");
					}
				}catch (Exception e) { e.printStackTrace(); }
			}
		});
	}

	/**
	 * Retrieve the value of a key
	 *
	 * @param key
	 * @return The value of the key or null if there is no such key
	 * @throws IOException if this client or the leader is disconnected from ZooKeeper
	 */
	@Override
	public String getValue(String key) throws IOException {
		if (!isConnected) {
			throw new IOException();
		}
		String value = null;
		try {
			// if the client already holds this key and value, return it
			if (keyValueMap.get(key) != null) {
				return keyValueMap.get(key);
			}
			IKVStore leaderID = connectToKVStore(leaderLatch.getLeader().getId());
			// See if leader has the value of this key. getLocalConnectString() is the client making the request
			value = leaderID.getValue(key, getLocalConnectString());
			// if value is null, return null but don't store it in map
			if (value == null) {
				System.out.println("Null Value");
				return null;
			}
			// Value is not null, store in map
			System.out.println("1st Get value key ");
			keyValueMap.put(key, value);
		} catch (Exception e) {
			throw new IOException();
		}
		return value;
			//return keyValueMap.get(key);
	}

	/**
	 * Update the value of a key. After updating the value, this new value will be locally cached.
	 *
	 * @param key
	 * @param value
	 * @throws IOException if this client or the leader is disconnected from ZooKeeper
	 */
	@Override
	public void setValue(String key, String value) throws IOException {
		if (!isConnected) {
			throw new IOException();
		}
		try {
			IKVStore leaderID = connectToKVStore(leaderLatch.getLeader().getId());
			leaderID.setValue(key, value, getLocalConnectString());
			keyValueMap.put(key, value);
		} catch (Exception e) {
			throw new IOException();
		}
	}

	/**
	 * Request the value of a key. The node requesting this value is expected to cache it for subsequent reads.
	 * <p>
	 * This command should ONLY be called as a request to the leader.
	 *
	 * @param key    The key requested
	 * @param fromID The ID of the client making the request (as returned by AbstractKVStore.getLocalConnectString())
	 * @return The value of the key, or null if there is no value for this key
	 * <p>
	 * DOES NOT throw any exceptions (the RemoteException is thrown by RMI if the connection fails)
	 */
	@Override
	public String getValue(String key, String fromID) throws RemoteException {
		// Store a lock with this key if it doesn't have one
		lockMap.computeIfAbsent(key, e -> new ReentrantReadWriteLock());
		lockMap.get(key).readLock().lock();
		try {
			try {
				if(keyValueMap.get(key) != null) {
					clientMap.computeIfAbsent(key, e-> new ArrayList<>());
					// Cache the client
					System.out.println("Get value key id, if");
					clientMap.get(key).add(fromID);
				}
			} catch (Exception e) {
				throw new RemoteException();
			}
			return keyValueMap.get(key);
		} finally {
			lockMap.get(key).readLock().unlock();
		}
	}

	/**
	 * Request that the value of a key is updated. The node requesting this update is expected to cache it for subsequent reads.
	 * <p>
	 * This command should ONLY be called as a request to the leader.
	 * <p>
	 * This command must wait for any pending writes on the same key to be completed
	 *
	 * @param key    The key to update
	 * @param value  The new value
	 * @param fromID The ID of the client making the request (as returned by AbstractKVStore.getLocalConnectString())
	 */
	@Override
	public void setValue(String key, String value, String fromID) throws IOException {
		// Store a lock with this key if it doesn't have one
		lockMap.computeIfAbsent(key, e -> new ReentrantReadWriteLock());
		lockMap.get(key).writeLock().lock();
		try {
			try {
			// get all the connected children
			Map<String, ChildData> children = this.treeCache.getCurrentChildren(ZK_MEMBERSHIP_NODE);
			if(this.clientMap.get(key) != null) {
				// get all the clients that are supposed to be connected and have the key cached
				for(String c: this.clientMap.get(key)) {
					// if the followers exist in the tree cahce that means they are connected
					if(children.containsKey(c)) {
						connectToKVStore(c).invalidateKey(key);
					}
				}
			}
			keyValueMap.put(key,value);
			// add the cache
			this.clientMap.put(key,new ArrayList<String>());
			this.clientMap.get(key).add(fromID);
		} catch (Exception e) {
				System.out.println("Exception in set value");
			}
		} finally {
			lockMap.get(key).writeLock().unlock();
		}
	}

	/**
	 * Instruct a node to invalidate any cache of the specified key.
	 * <p>
	 * This method is called BY the LEADER, targeting each of the clients that has cached this key.
	 *
	 * @param key key to invalidate
	 *            <p>
	 *            DOES NOT throw any exceptions (the RemoteException is thrown by RMI if the connection fails)
	 */
	@Override
	public void invalidateKey(String key) throws RemoteException {
		System.out.println("Invalidate called");
		keyValueMap.remove(key);
	}

	/**
	 * Called when ZooKeeper detects that your connection status changes
	 * @param curatorFramework
	 * @param connectionState
	 */
	@Override
	public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
		isConnected = connectionState.isConnected();
		if(isConnected) {
			//System.out.println("Still Connected");
		}
		else {
			if(connectionState == ConnectionState.LOST) {
				System.out.println("Connection is Lost");
				keyValueMap = null;
				clientMap = null;
				lockMap = null;
			}
			else if(connectionState == ConnectionState.SUSPENDED) {
				// wait?
				System.out.println("Connection is Suspended");
				keyValueMap = null;
				clientMap = null;
				lockMap = null;
			}
			else if(connectionState == ConnectionState.RECONNECTED) {
				System.out.println("Connection is Reconnected");
				try {
					if(zk.getZookeeperClient() != null && leaderLatch.getParticipants().size() > 0) {
						keyValueMap = new ConcurrentHashMap<>();
						lockMap = new ConcurrentHashMap<>();
						clientMap = new ConcurrentHashMap<>();
					}
				} catch (Exception e) { e.printStackTrace(); }
			}
			else if(connectionState == ConnectionState.READ_ONLY) {
				System.out.println("Connection is Read Only ");
			}
		}
	}

	/**
	 * Release any ZooKeeper resources that you setup here
	 * (The connection to ZooKeeper itself is automatically cleaned up for you)
	 */
	@Override
	protected void _cleanup() {
		// If still connected, close resources
		if(isConnected) {
			System.out.println("Cleanup ");
			try {
				treeCache.close();
			} catch (Exception e) { System.out.println("Failed to close Tree Cache"); }
			try {
				if(leaderLatch.getState() == LeaderLatch.State.STARTED) {
					leaderLatch.close();
				}
			} catch (Exception e) { System.out.println("Failed to close Leader Latch"); }
			try {
				ephemeralNode.close();
			} catch (Exception e) { System.out.println("Failed to close Ephemeral Node"); }
		}
	}
}

