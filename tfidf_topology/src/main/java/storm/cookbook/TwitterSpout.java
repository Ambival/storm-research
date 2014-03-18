package storm.cookbook;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Values;

public class TwitterSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;
	
	LinkedBlockingQueue<Status> queue = null;
	TwitterStream twitterStream;
	String[] trackTerms;
	long maxQueueDepth;
	SpoutOutputCollector collector;
	Logger LOG = LoggerFactory.getLogger(TwitterSpout.class);

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		queue = new LinkedBlockingQueue<Status>(1000);
		
		StatusListener listener = new StatusListener() {
			
			@Override
			public void onStatus(Status status) {
				if (queue.size() < maxQueueDepth) {
					LOG.trace("TWEET Received: " + status);
					queue.offer(status);
				} else {
					LOG.error("Queue is now full, the following message is dropped: " + status);
				}
			}

			@Override
			public void onScrubGeo(long arg0, long arg1) {
			}

			@Override
			public void onException(Exception arg0) {
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice arg0) {
			}

			@Override
			public void onStallWarning(StallWarning arg0) {
			}

			@Override
			public void onTrackLimitationNotice(int arg0) {
			}
		};
		
		twitterStream = new TwitterStreamFactory().getInstance();
		twitterStream.addListener(listener);

		FilterQuery filter = new FilterQuery();
		filter.count(0);
		filter.track(trackTerms);
		
		twitterStream.filter(filter);
	}

	@Override
	public void nextTuple() {
		Status ret = queue.poll();
		
		if (ret == null) {
			try { Thread.sleep(50); }
			catch (InterruptedException e) { }
		} else {
			collector.emit(new Values(ret));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
	}

}
