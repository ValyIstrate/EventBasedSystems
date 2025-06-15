package ebs.project.bolts;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class LogBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String msg = input.getStringByField("message");
        System.out.println("[LogBolt] Received message: " + msg);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // no output
    }
}