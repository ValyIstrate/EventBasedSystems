package ebs.project;

import ebs.project.bolts.LogBolt;
import ebs.project.spouts.SqsSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("sqs-spout", new SqsSpout());
        builder.setBolt("log-bolt", new LogBolt()).shuffleGrouping("sqs-spout");

        Config config = new Config();
        config.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("sqs-reader-topology", config, builder.createTopology());

        // Keep app running
        Thread.sleep(60000);
        cluster.shutdown();
    }
}
