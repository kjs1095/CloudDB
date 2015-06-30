package netdb.software.benchmark.procedure.vanilladddb.calvin;

import netdb.software.benchmark.TransactionType;

import org.vanilladb.dd.schedule.calvin.CalvinStoredProcedure;
import org.vanilladb.dd.schedule.calvin.CalvinStoredProcedureFactory;

public class TpccStoredProcFactory implements CalvinStoredProcedureFactory {

	@Override
	public CalvinStoredProcedure<?> getStoredProcedure(int pid, long txNum) {
		CalvinStoredProcedure<?> sp;
		switch (TransactionType.values()[pid]) {
		case SCHEMA_BUILDER:
			sp = new SchemaBuilderProc(txNum);
			break;
		case TESTBED_LOADER:
			sp = new TestbedLoaderProc(txNum);
			break;
		case MICROBENCHMARK_TXN:
			sp = new MicroBenchmarkProc(txNum);
			break;
		default:
			sp = null;
		}
		return sp;
	}
}
