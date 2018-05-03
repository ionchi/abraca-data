package time;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.text.SimpleDateFormat;
import java.util.Date;

public class data extends UDF {
    public Text evaluate(Text text) {
        if(text == null) return null;
        long timestamp = Long.parseLong(text.toString());
// timestamp*1000 is to convert seconds to milliseconds
        Date date = new Date(timestamp*1000L);
// the format of your date
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy");
        String formattedDate = sdf.format(date);
        return new Text(formattedDate);
    }
}
