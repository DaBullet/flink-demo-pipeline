import org.apache.flink.api.common.functions.FilterFunction;

public class MyCustomFilter implements FilterFunction<String> {

    public boolean filter(String s) throws Exception {
        try {
            Double.parseDouble(s);
            return true;
        } catch (Exception ex) {
            return false;
        }
    }
}