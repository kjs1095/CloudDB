package org.vanilladb.dd.schedule.calvin;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.util.Set;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.dd.schedule.DdStoredProcedure;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.sql.RecordKey;
import org.vanilladb.dd.storage.tx.concurrency.ConservativeOrderedCcMgr;
import org.vanilladb.dd.storage.tx.recovery.DdRecoveryMgr;
import org.vanilladb.dd.cache.CachedRecord;
import org.vanilladb.dd.cache.calvin.CalvinCacheMgr;
import org.vanilladb.dd.remote.groupcomm.server.ConnectionMgr;
import org.vanilladb.dd.remote.groupcomm.TupleSet;

public abstract class CalvinStoredProcedure<H extends StoredProcedureParamHelper>
		implements DdStoredProcedure {

	// Protected resource
	protected Transaction tx;
	protected long txNum;
	protected H paramHelper;

	// Record keys
	private List<RecordKey> readKeys = new ArrayList<RecordKey>();
	private List<RecordKey> writeKeys = new ArrayList<RecordKey>();
	private RecordKey[] readKeysForLock, writeKeysForLock;
	
	private Set<Integer> participants = new HashSet<Integer>();
	private Set<Integer> activeParticipants = new HashSet<Integer>();
	private List<RecordKey> localReadKeys = new ArrayList<RecordKey>();
	private int masterId = -1;
	private int serverId = VanillaDdDb.serverId();
	
	public CalvinStoredProcedure(long txNum, H paramHelper) {
		this.txNum = txNum;
		this.paramHelper = paramHelper;

		if (paramHelper == null)
			throw new NullPointerException("paramHelper should not be null");
	}

	/*******************
	 * Abstract methods
	 *******************/

	/**
	 * Prepare the RecordKey for each record to be used in this stored
	 * procedure. Use the {@link #addReadKey(RecordKey)},
	 * {@link #addWriteKey(RecordKey)} method to add keys.
	 */
	protected abstract void prepareKeys();
	
	/**
	 * Perform the transaction logic and record the result of the transaction.
	 */
	protected abstract void performTransactionLogic();

	/**
	 * 
	 */
	private void analyzeRWSet() {
		for (RecordKey writeKey: writeKeys) {
			int partitionId = VanillaDdDb.partitionMetaMgr().getPartition(writeKey);
			participants.add(partitionId);
			activeParticipants.add(partitionId);
			
			if (masterId == -1)
				masterId = partitionId;
		}
		
		for (RecordKey readKey: readKeys) {
			int partitionId = VanillaDdDb.partitionMetaMgr().getPartition(readKey);
			participants.add(partitionId);
			if (partitionId == serverId)
				localReadKeys.add(readKey);
				
			if (masterId == -1)
				masterId = partitionId;
		}
		if (masterId == -1) {
			masterId = 0;
			participants.add(serverId);
			activeParticipants.add(serverId);
		}
		participants.add(masterId);
		activeParticipants.add(masterId);
	}
	
	/**********************
	 * Implemented methods
	 **********************/

	public void prepare(Object... pars) {
		// prepare parameters
		paramHelper.prepareParameters(pars);

		// create transaction
		boolean isReadOnly = paramHelper.isReadOnly();
		this.tx = VanillaDdDb.txMgr().newTransaction(
				Connection.TRANSACTION_SERIALIZABLE, isReadOnly, txNum);
		this.tx.addLifecycleListener(new DdRecoveryMgr(tx
				.getTransactionNumber()));

		// prepare keys
		prepareKeys();
		
		// phase 1: read/write set analysis
		analyzeRWSet();
	}

	public void requestConservativeLocks() {
		ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx
				.concurrencyMgr();

		readKeysForLock = readKeys.toArray(new RecordKey[0]);
		writeKeysForLock = writeKeys.toArray(new RecordKey[0]);

		ccMgr.prepareSp(readKeysForLock, writeKeysForLock);
	}

	@Override
	public final RecordKey[] getReadSet() {
		return readKeysForLock;
	}

	@Override
	public final RecordKey[] getWriteSet() {
		return writeKeysForLock;
	}

	@Override
	public SpResultSet execute() {
		
		try {
			// Get conservative locks it has asked before
			getConservativeLocks();

			// phase 2: Perform local reads
			TupleSet ts = new TupleSet(-1);
			CalvinCacheMgr cm = (CalvinCacheMgr) VanillaDdDb.cacheMgr();
			for (RecordKey localReadKey : localReadKeys){
				CachedRecord rec = cm.read(localReadKey, tx);
				ts.addTuple(localReadKey, txNum, txNum, rec);
			}
			
			// phase 3: Serve remote reads
			ConnectionMgr connMgr = VanillaDdDb.connectionMgr();
			if (localReadKeys.isEmpty() == false) {
				for (int partitionId: activeParticipants)
					if (partitionId != serverId)
						connMgr.pushTupleSet(partitionId, ts);
			}
			
			// Execute transaction
			// phase 5: tx logic execution and applying writes
			if (activeParticipants.contains(serverId))
				performTransactionLogic();

			// The transaction finishes normally
			tx.commit();

		} catch (Exception e) {
			tx.rollback();
			paramHelper.setCommitted(false);
			e.printStackTrace();
		} finally {
			((CalvinCacheMgr)VanillaDdDb.cacheMgr()).cleanCachedTuples(tx);
		}

		return paramHelper.createResultSet();
	} 
	
	@Override
	public boolean isReadOnly() {
		return paramHelper.isReadOnly();
	}
	
	@Override
	public boolean isMaster() {
		return masterId == serverId;
	}
	
	@Override
	public boolean isParticipant() {
		return participants.contains(serverId);
	}
	
	protected void addReadKey(RecordKey readKey) {
		readKeys.add(readKey);
	}

	protected void addWriteKey(RecordKey writeKey) {
		writeKeys.add(writeKey);
	}
	
	private void getConservativeLocks() {
		ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx
				.concurrencyMgr();
		ccMgr.executeSp(readKeysForLock, writeKeysForLock);
	}
}
