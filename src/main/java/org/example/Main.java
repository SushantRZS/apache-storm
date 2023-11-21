package org.example;

import org.apache.storm.Config;
import org.apache.storm.ILocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.example.bolt.Bolt;
import org.example.spout.Spout;

public class Main {
    public static void main(String[] args) throws Exception {
        runTopology();
    }

    public static void runTopology() throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("fruit-spout", new Spout());
        builder.setBolt("place-bole", new Bolt());
        Config config = new Config();
        config.setDebug(true);
        StormSubmitter.submitTopology("test-topology", config, builder.createTopology());
    }
}