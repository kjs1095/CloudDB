package netdb.software.benchmark.procedure.vanilladddb.calvin;

import netdb.software.benchmark.procedure.SchemaBuilderProcParamHelper;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.dd.schedule.calvin.CalvinStoredProcedure;

public class SchemaBuilderProc extends CalvinStoredProcedure<SchemaBuilderProcParamHelper> {

	public SchemaBuilderProc(long txNum) {
		super(txNum, new SchemaBuilderProcParamHelper());
	}

	@Override
	public boolean isReadOnly() {
		return false;
	}

	@Override
	protected void prepareKeys() {
		// Do nothing
	}

	@Override
	protected void performTransactionLogic() {
		// Creating a table need to be executed directly 
		for (String cmd : paramHelper.getTableSchemas())
			VanillaDb.newPlanner().executeUpdate(cmd, tx);
		for (String cmd : paramHelper.getIndexSchemas())
			VanillaDb.newPlanner().executeUpdate(cmd, tx);
	}
}
