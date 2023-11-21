package org.example.bolt;

import org.apache.log4j.Logger;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Random;

public class Bolt extends BaseBasicBolt {
    private static final Logger LOGGER = Logger.getLogger(BaseBasicBolt.class);
    /**
     * Process the input tuple and optionally emit new tuples based on the input tuple.
     *
     * <p>All acking is managed for you. Throw a FailedException if you want to fail the tuple.
     *
     * @param input
     * @param collector
     */
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String fruit = input.getStringByField("Fruit");
        String[] place = new String[] {"Bangalore", "Delhi"};
        Random rand = new Random();
        if(!fruit.equalsIgnoreCase("pramod")) {
            String fruitPlace = fruit.concat(place[rand.nextInt(place.length)]);
            collector.emit(new Values(fruitPlace, Timestamp.from(Instant.now())));
        }
        else {
            LOGGER.info(String.format("Filtered out: %s", fruit));
        }
    }

    /**
     * Declare the output schema for all the streams of this topology.
     *
     * @param declarer this is used to declare output stream ids, output fields, and whether or not each output stream is a direct stream
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("fruitPlace", "timestamp"));
    }
}
