package org.vanilladb.dd.cache.calvin;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.dd.cache.CacheMgr;
import org.vanilladb.dd.cache.CachedRecord;
import org.vanilladb.dd.cache.LocalRecordMgr;
import org.vanilladb.dd.sql.RecordKey;

import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.remote.groupcomm.Tuple;

public class CalvinCacheMgr implements CacheMgr {
	static Map<TupleKey, CachedRecord> tupleMap = new ConcurrentHashMap<TupleKey, CachedRecord>();
	
	public CachedRecord read(RecordKey key, Transaction tx) {
		int nodeId = VanillaDdDb.partitionMetaMgr().getPartition(key);
		if (nodeId == VanillaDdDb.serverId()) {
			return LocalRecordMgr.read(key, tx);
		} else {
			TupleKey tupleKey = new TupleKey(tx.getTransactionNumber(), key);
			
			while(!tupleMap.containsKey(tupleKey))
				;
			
			return (CachedRecord) tupleMap.get(tupleKey);
		}
	}

	public void update(RecordKey key, CachedRecord rec, Transaction tx) {
		int nodeId = VanillaDdDb.partitionMetaMgr().getPartition(key);
		if (nodeId == VanillaDdDb.serverId())
			LocalRecordMgr.update(key, rec, tx);
	}

	public void insert(RecordKey key, CachedRecord rec, Transaction tx) {
		int nodeId = VanillaDdDb.partitionMetaMgr().getPartition(key);
		if (nodeId == VanillaDdDb.serverId())
			LocalRecordMgr.insert(key, rec, tx);
	}

	public void delete(RecordKey key, Transaction tx) {
		int nodeId = VanillaDdDb.partitionMetaMgr().getPartition(key);
		if (nodeId == VanillaDdDb.serverId())
			LocalRecordMgr.delete(key, tx);
	}
	
	public void addCacheTuple(Tuple tuple) {
		TupleKey tkey = new TupleKey(tuple.destTxNum, tuple.key);
		tupleMap.put(tkey, tuple.rec);
	}
	
	public void cleanCachedTuples(Transaction tx) {
		for(TupleKey tupleKey : tupleMap.keySet()) {
			if(tupleKey.getTxNum() == tx.getTransactionNumber()) {
				tupleMap.remove(tupleKey);
			}
		}
	}
}