package buryat.cascading.geo;

import java.util.ArrayList;
import java.util.Iterator;

import cascading.tuple.TupleListCollector;
import junit.framework.Assert;
import org.junit.Test;

import cascading.CascadingTestCase;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

public class GeoTest {
    @Test
    public void ipLookup() {
        Fields fieldsDeclaration = new Fields("ip");
        GeoLookup geoLookup = new GeoLookup(fieldsDeclaration);
        geoLookup.geoDbInit("/tmp/GeoIPCity.dat");

        Tuple[] arguments = new Tuple[] {
                new Tuple("72.225.254.232"),
                new Tuple("93.158.134.11"),
                new Tuple("8.8.8.8"),
                new Tuple("107.208.243.15")
        };

        TupleListCollector collector = CascadingTestCase.invokeFunction(geoLookup, arguments, Fields.ALL);
        Iterator<Tuple> it = collector.iterator();
        ArrayList<Tuple> results = new ArrayList<Tuple>();
        while (it.hasNext()) {
            results.add(it.next());
        }

        ArrayList<Tuple> expected = new ArrayList<Tuple>();
        expected.add(new Tuple("United States", "New York", 501));
        expected.add(new Tuple("Russian Federation", null, 0));
        expected.add(new Tuple("United States", "Mountain View", 807));
        expected.add(new Tuple(null, null, null));

        Assert.assertEquals(expected, results);
    }
}
