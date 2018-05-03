package time;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class data extends UDF {
    public static String unix2StringYear(long time) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time*1000);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy");
        return sdf.format(calendar.getTime()) ;
    }
}
