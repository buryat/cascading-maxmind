package buryat.cascading.geo;

import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

public class Geo {
    public static Pipe run(Pipe input) {
        return new Each(
                input,
                new Fields("ip"),
                new GeoLookup(new Fields("country", "city"))
        );
    }
}
