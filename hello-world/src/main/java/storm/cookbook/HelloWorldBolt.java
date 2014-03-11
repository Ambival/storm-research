package storm.cookbook;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class HelloWorldBolt extends BaseRichBolt {
	private int myCount = 0;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void execute(Tuple input) {
		String test = input.getStringByField("sentence");
		
		if ("Hello World".equals(test)) {
			++myCount;
			
			System.out.println("Found a Hello World! My count is now: " + Integer.toString(myCount));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

}
