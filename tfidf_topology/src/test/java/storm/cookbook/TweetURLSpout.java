package storm.cookbook;

import java.util.Map;

import redis.clients.jedis.Jedis;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TweetURLSpout extends BaseRichSpout {

	private String host;
	private int port;
	private SpoutOutputCollector collector;
	private Jedis jedis;

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		host = conf.get(Conf.REDIS_HOST_KEY).toString();
		port = Integer.valueOf(conf.get(Conf.REDIS_PORT_KEY).toString());
		
		this.collector = collector;

		connectToRedis();
	}

	private void connectToRedis() {
		jedis = new Jedis(host, port);
	}

	@Override
	public void nextTuple() {
		String url = jedis.rpop("url");
		
		if (url == null) {
			try { Thread.sleep(50); }
			catch (InterruptedException e) { }
		} else {
			collector.emit(new Values(url));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("url"));
	}

}
