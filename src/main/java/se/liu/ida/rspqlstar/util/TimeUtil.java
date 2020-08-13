package se.liu.ida.rspqlstar.util;

import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

public class TimeUtil {
    private static Logger logger = Logger.getLogger(TimeUtil.class);
    public static long offset = 0;
    public static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

    static {
        df.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    public static long getLongTime(){
        return new Date().getTime() - offset;
    }

    public static long getTime(){
        return new Date().getTime() - offset;
    }

    /**
     * Return precise time based on nano time. Should only be used for measureing elapsed time!
     * @return
     */
    public static long getNanoTime(){
        return System.nanoTime();
    }

    public static Date getDate(){
        return new Date(new Date().getTime() - offset);
    }

    /**
     * Set the time offset. The offset is calculated as the unix time difference between
     * now and the reference time.
     * @param offset
     * @return
     */
    public static void setOffset(long offset){
        TimeUtil.offset = offset;
    }

    public static void silentSleep(long sleep){
        try {
            if(sleep <= 0) return;
            Thread.sleep(sleep);
        } catch (InterruptedException e) {
            logger.info(e.getMessage());
        }
    }
}
