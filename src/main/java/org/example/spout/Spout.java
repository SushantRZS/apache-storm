package org.example.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;
import java.util.Random;

public class Spout extends BaseRichSpout {
    private SpoutOutputCollector spoutOutputCollector;
    /**
     * Called when a task for this component is initialized within a worker on the cluster. It provides the spout with the environment in
     * which the spout executes.
     *
     * <p>This includes the:
     *
     * @param conf      The Storm configuration for this spout. This is the configuration provided to the topology merged in with cluster
     *                  configuration on this machine.
     * @param context   This object can be used to get information about this task's place within the topology, including the task id and
     *                  component id of this task, input and output information, etc.
     * @param collector The collector is used to emit tuples from this spout. Tuples can be emitted at any time, including the open and
     *                  close methods. The collector is thread-safe and should be saved as an instance variable of this spout object.
     */
    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.spoutOutputCollector = collector;
    }

    /**
     * When this method is called, Storm is requesting that the Spout emit tuples to the output collector. This method should be
     * non-blocking, so if the Spout has no tuples to emit, this method should return. nextTuple, ack, and fail are all called in a tight
     * loop in a single thread in the spout task. When there are no tuples to emit, it is courteous to have nextTuple sleep for a short
     * amount of time (like a single millisecond) so as not to waste too much CPU.
     */
    @Override
    public void nextTuple() {
        final String[] words = new String[] {"apple", "banana", "guava", "pramod"};
        final Random rand = new Random();
        while(true) {
            spoutOutputCollector.emit(new Values(words[rand.nextInt(words.length)]), Timestamp.from(Instant.now()));
        }
    }

    /**
     * Declare the output schema for all the streams of this topology.
     *
     * @param declarer this is used to declare output stream ids, output fields, and whether or not each output stream is a direct stream
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("fruit", "timestamp"));
    }
}
