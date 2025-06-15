package ebs.project;

import ebs.project.bolts.BrokerBolt;
import ebs.project.bolts.SubscriberBolt;
import ebs.project.spouts.SqsSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.Arrays;

public class App {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        var subscriber1 = new SubscriberBolt("sb1", "src/main/java/ebs/project/subscription_files/s1.txt");
        var subscriber2 = new SubscriberBolt("sb2", "src/main/java/ebs/project/subscription_files/s2.txt");

        builder.setSpout("sqs-spout", new SqsSpout());

        builder.setBolt("decoder-bolt", new BrokerBolt("db"), 1)
                .shuffleGrouping("sqs-spout");

        builder.setBolt("broker-bolt-1", new BrokerBolt("b1"), 1)
                .shuffleGrouping("decoder-bolt", "decoded-stream")
                .fieldsGrouping("broker-bolt-2", "subscription-stream", new Fields("subscriberId"))
                .fieldsGrouping("broker-bolt-3", "subscription-stream", new Fields("subscriberId"));;

        builder.setBolt("broker-bolt-2", new BrokerBolt("b2"), 1)
                .shuffleGrouping("broker-bolt-1", "notification-stream")
                .fieldsGrouping("subscriber-bolt-1", "subscription-stream", new Fields("subscriberId"));

        builder.setBolt("broker-bolt-3", new BrokerBolt("b3"), 1)
                .shuffleGrouping("broker-bolt-1", "notification-stream")
                .fieldsGrouping("subscriber-bolt-2", "subscription-stream", new Fields("subscriberId"));

        builder.setBolt("subscriber-bolt-1", subscriber1, 1)
                .shuffleGrouping("broker-bolt-2", "notification-stream");

        builder.setBolt("subscriber-bolt-2", subscriber2, 1)
                .shuffleGrouping("broker-bolt-3", "notification-stream");

        Config config = new Config();
        config.setDebug(true);

        config.setNumWorkers(3);

        config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("localhost"));
        config.put(Config.STORM_ZOOKEEPER_PORT, 2181);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("sqs-reader-topology", config, builder.createTopology());

        Thread.sleep(60000);
        cluster.shutdown();
    }
}
