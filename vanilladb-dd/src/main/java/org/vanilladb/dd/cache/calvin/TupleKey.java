package org.vanilladb.dd.cache.calvin;

import org.vanilladb.dd.sql.RecordKey;

public class TupleKey {
	private long txNum;
	private RecordKey key;
	
	public TupleKey(long txNum, RecordKey key) {
		this.txNum = txNum;
		this.key = key;
	}
	
	public long getTxNum(){
		return txNum;
	}
	
	public RecordKey getRecordKey(){
		return key;
	}
	
	@Override
	public boolean equals(Object obj){
		if (obj == this)
			return true;
		if (obj == null)
			return false;
		if (obj.getClass() != TupleKey.class)
			return false;
		
		TupleKey t = (TupleKey)obj;
		return (t.txNum == this.txNum) && (this.key.equals(t.key));
	}
	
	@Override
	public int hashCode() {
		int hashCode = 17;
		hashCode = 31 * hashCode + (int) (txNum ^ (txNum >>> 32));
		hashCode = 31 * hashCode + key.hashCode();
		return hashCode;
	}
}
