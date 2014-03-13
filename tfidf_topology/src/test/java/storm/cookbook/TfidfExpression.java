package storm.cookbook;

import java.util.Map;

import org.apache.thrift7.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class TfidfExpression extends BaseFunction {
	
	private Logger LOG = LoggerFactory.getLogger(TfidfExpression.class);
	private static final long serialVersionUID = 1L;
	private DRPCClient client = null;
	
	public void prepare(Map conf, TridentOperationContext context) {
		if ("true".equals(conf.get("LOCAL")))
			client = new DRPCClient("localhost", 3772);
	}
	
	private String execute(String function, String args) throws TException, DRPCExecutionException {
		if (client != null)
			return client.execute(function, args);
		
		if (function.equals("d"))
			return "100";
		
		return "50";
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		try {
			String result = execute("dQuery", "twitter");
			double d = Double.parseDouble(result);
			result = execute("dfQuery", tuple.getStringByField("term"));
			double df = Double.parseDouble(result);
			double tf = (double)tuple.getLongByField("tf");
			double tfidf = tf * Math.log(d / (1.0 + df));
			
			LOG.debug("Emitting new TFIDF(term, Document): ("
					+ tuple.getStringByField("term") + ", "
					+ tuple.getStringByField("documentId") + ") = " + tfidf);
		} catch(Exception e) {
			LOG.error(e.getStackTrace().toString());
		}
	}

}
