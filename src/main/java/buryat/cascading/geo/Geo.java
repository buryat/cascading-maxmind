package buryat.cascading.geo;

import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.SumBy;
import cascading.tuple.Fields;

public class Geo {
    public static Pipe ip(Pipe input) {
        return new Each(
                input,
                new Fields("ip"),
                new GeoLookup(new Fields("country", "city", "dma_code"))
        );
    }

    public static Pipe date(Pipe input) {
        input = new Each(
            input,
            new Fields("ip"),
            new GeoLookup(new Fields("country", "city", "dma_code")),
            new Fields("date", "dma_code", "count")
        );

        return new SumBy(
            input,
            new Fields("date", "dma_code"),
            new Fields("count"),
            new Fields("cnt"),
            Integer.class
        );
    }
}
