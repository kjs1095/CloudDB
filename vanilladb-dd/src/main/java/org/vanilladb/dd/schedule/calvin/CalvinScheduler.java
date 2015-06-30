package org.vanilladb.dd.schedule.calvin;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;
import org.vanilladb.dd.remote.groupcomm.StoredProcedureCall;
import org.vanilladb.dd.schedule.DdStoredProcedure;
import org.vanilladb.dd.schedule.Scheduler;
import org.vanilladb.dd.server.task.calvin.CalvinStoredProcedureTask;
import org.vanilladb.dd.storage.tx.recovery.DdRecoveryMgr;
import org.vanilladb.dd.util.DDProperties;

public class CalvinScheduler extends Task implements Scheduler {
	private static final Class<?> FACTORY_CLASS;

	private CalvinStoredProcedureFactory factory;
	private BlockingQueue<StoredProcedureCall> spcQueue = new LinkedBlockingQueue<StoredProcedureCall>();
	
	static {
		FACTORY_CLASS = DDProperties.getLoader().getPropertyAsClass(
				CalvinScheduler.class.getName() + ".FACTORY_CLASS", null,
				CalvinStoredProcedureFactory.class);
		if (FACTORY_CLASS == null)
			throw new RuntimeException("Factory property is empty");
	}

	public CalvinScheduler() {
		try {
			factory = (CalvinStoredProcedureFactory) FACTORY_CLASS.newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			e.printStackTrace();
		}
	}

	public void schedule(StoredProcedureCall... calls) {
		try {
			for (int i = 0; i < calls.length; i++) {
				spcQueue.put(calls[i]);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		while (true) {
			try {
				// retrieve stored procedure call
				StoredProcedureCall call = spcQueue.take();
				if (call.isNoOpStoredProcCall())
					continue;

				// create store procedure and prepare
				DdStoredProcedure sp = factory.getStoredProcedure(
						call.getPid(), call.getTxNum());
				sp.prepare(call.getPars());

				// log request
				if (!sp.isReadOnly())
					DdRecoveryMgr.logRequest(call);

				if (sp.isParticipant()) {
					// create a new task for multi-thread
					CalvinStoredProcedureTask spt = new CalvinStoredProcedureTask(
							call.getClientId(), call.getRteId(), call.getTxNum(),
							sp);
					
					// perform conservative locking
					spt.lockConservatively();

					// hand over to a thread to run the task
					VanillaDb.taskMgr().runTask(spt);
				}

			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}
	}
}
