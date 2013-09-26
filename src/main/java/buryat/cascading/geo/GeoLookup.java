package buryat.cascading.geo;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import cascading.CascadingException;

import java.io.File;
import java.io.IOException;

public class GeoLookup extends BaseOperation<Tuple> implements Function<Tuple> {
    public GeoLookup() {
        super(1, new Fields("ip"));
    }

    public GeoLookup(Fields fields) {
        super(1, fields);
    }

    LookupService geols;

    public void prepare(FlowProcess flowProcess, OperationCall<Tuple> call) {
        if (geols == null) {
            try {
                Path[] files = DistributedCache.getLocalCacheFiles(((HadoopFlowProcess) flowProcess).getJobConf());
                if (files == null) {
                    throw new CascadingException("No files");
                }
                for (Path file : files) {
                    if ((file.getName() != null) && (file.getName().equalsIgnoreCase("GeoIPCity.dat"))) {
                        geols = new LookupService(new File(file.toUri().getPath()), LookupService.GEOIP_MEMORY_CACHE);
                    }
                }
            } catch (IOException e) {
                throw new CascadingException("Error loading MaxMind GeoIPCity DB", e);
            }
        }
        if (geols == null) {
            throw new CascadingException("Error loading MaxMind GeoIPCity DB");
        }

        call.setContext(Tuple.size(3));
    }

    public void geoDbInit(String file) {
        try {
            geols = new LookupService(new File(file), LookupService.GEOIP_MEMORY_CACHE);
        } catch (IOException e) {
            throw new CascadingException("Error loading MaxMind GeoIPCity DB", e);
        }
    }

    public void operate(FlowProcess flowProcess, FunctionCall<Tuple> functionCall) {
        TupleEntry arguments = functionCall.getArguments();

        Tuple result = functionCall.getContext();
        String ip = arguments.getString(0);

        Location location = geols.getLocation(ip);

        if (location != null) {
            result.set(0, location.countryName);
            result.set(1, location.city);
            result.set(2, location.dma_code);
        } else {
            result.set(0, null);
            result.set(1, null);
            result.set(2, null);
        }

        functionCall.getOutputCollector().add(result);
    }
}