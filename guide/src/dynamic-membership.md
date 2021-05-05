Dynamic Membership
==================
Throughout the lifecycle of a Raft cluster, nodes will come and go. New nodes may need to be added to the cluster for various application specific reasons. Nodes may experience hardware failure and end up going offline. This implementation of Raft offers four mechanisms for controlling these lifecycle events.

#### `Raft.add_non_voter`
This method will add a new non-voter to the cluster and will immediately begin syncing the node with the leader. This method may be called multiple times as needed. The `Future` returned by calling this method will resolve once the node is up-to-date and is ready to be added as a voting member of the cluster.

Note, that promoting a Non-Voter member to a Voter one is by no means obligatory. Applications can exploit the Non-Voter mechanism as they wish.

#### `Raft.remove_non_voter`
Removes a previously added Non-Voter node. The removed node will no longer receive updates and can be shut down safely.

Please be aware, that there is a special case when this method cannot be used to remove a Non-Voter. When adding a new node via `Raft.add_voter`, if the node is not synced with the rest of the cluster yet, then it is first registered as a Non-Voter to catch up. While the new node is catching up, it cannot be removed.

#### `Raft.add_voter`
Starts a cluster membership change, adding the specified node as a Voter member of the cluster. Will behave as follows:

- If the node being added is an in-sync Non-Voter, then proceeds to change the cluster configuration and replicate the change among cluster members.
- If the node being added is a Non-Voter out-of-sync, then this method will wait for the Non-Voter to catch up. If the Non-Voter is not able to catch up, then the change fails.
- If the node being added is not a Non-Voter yet, then this method will first turn it into a Non-Voter and begin the synchronization process. Once the node is synced/up to speed, then the configuration change proceeds.

It is recommended that applications always call `Raft.add_non_voter` first when adding new nodes to the cluster, as this offers a bit more flexibility. Once `Raft.add_voter` is called, no configuration changes can be requested until the reconfiguration process is complete (which is typically quite fast). If the node being added needs to be synced then no configuration change is possible until the synchronization process either completes or fails.

This method will return once the configuration change has finished (either with a failure or a success).

#### `Raft.remove_voter`
Removes a Voter node from the cluster. As opposed to `Raft.add_voter`, no preliminary steps are necessary, 

Once `Raft.remove_voter` is called, no configuration changes can be requested until the reconfiguration process is complete (which is typically quite fast).

Cluster auto-healing — where cluster members which have been offline for some period of time are automatically removed — is an application specific behavior, but is fully supported via this dynamic cluster membership system. Simply call `Raft.remove_voter` with the identifier of the dead node. Note, however, that the cluster leader will attempt to replicate the configuration change to the dead node. Thus, if you're removing a node, for example, for maintenance purposes, you should wait a little bit after this method returns, to actually take down the removed node.

Cluster leader stepdown is also fully supported. Nothing special needs to take place. Simply call `Raft.remove_voter` with the identifier of the leader. The leader will recognize that it is being removed from the cluster, and will stepdown once it has committed the config change to the cluster according to the safety protocols defined in the Raft spec.

This method will return once the configuration change has finished (either with a failure or a success).
