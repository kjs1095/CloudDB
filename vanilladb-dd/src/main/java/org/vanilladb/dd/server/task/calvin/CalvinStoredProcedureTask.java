package org.vanilladb.dd.server.task.calvin;

import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.dd.schedule.DdStoredProcedure;
import org.vanilladb.dd.schedule.calvin.CalvinStoredProcedure;
import org.vanilladb.dd.server.VanillaDdDb;
import org.vanilladb.dd.server.task.StoredProcedureTask;

public class CalvinStoredProcedureTask extends StoredProcedureTask {
	
	private CalvinStoredProcedure<?> csp;
	
	public CalvinStoredProcedureTask(int cid, int rteId, long txNum,
			DdStoredProcedure sp) {
		super(cid, rteId, txNum, sp);
		
		csp = (CalvinStoredProcedure<?>) sp;
	}

	public void run() {
		SpResultSet rs = sp.execute();
		
		if (sp.isMaster())
			VanillaDdDb.connectionMgr().sendClientResponse(cid, rteId, txNum, rs);
	}
	
	public void lockConservatively() {
		csp.requestConservativeLocks();
	}
}
