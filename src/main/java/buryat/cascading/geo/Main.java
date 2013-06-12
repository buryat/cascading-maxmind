package buryat.cascading.geo;

import cascading.CascadingException;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapred.JobConf;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        String geodbPath = args[0];
        String inputPath = args[1];
        String outputPath = args[2];

        JobConf jobConf = new JobConf();
        jobConf.setJarByClass(Main.class);

        try {
        DistributedCache.addCacheFile(new URI(geodbPath), jobConf);
        } catch (URISyntaxException e) {
            throw new CascadingException("Cannot add file " + geodbPath + " to distributed cache", e);
        }

        Properties properties =  AppProps.appProps()
                .setName("Geo")
                .buildProperties(jobConf);

        HadoopFlowConnector flowConnector = new HadoopFlowConnector(properties);


        Tap inputTap = new Hfs(
                new TextDelimited(
                        new Fields("ip"), "\t"
                ),
                inputPath
        );
        Tap outputTap = new Hfs(
                new TextDelimited(
                        true, "\t"
                ),
                outputPath,
                SinkMode.REPLACE
        );

        Pipe input = new Pipe("input");
        Pipe output = Geo.run(input);

        FlowDef flowDef = FlowDef.flowDef()
                .setName("geo")
                .addSource(input, inputTap)
                .addTailSink(output, outputTap);

        Flow flow = flowConnector.connect(flowDef);
        flow.writeDOT("dot/geo.dot");
        flow.complete();
    }
}
