package org.streamspf.hadoop.util;

public class CommonUtil {
    public final static int count = 1000;
    public final static String col_sep  = "&";

    public static String durationCal(String duration) {
        StringBuffer temp = new StringBuffer();

        if (duration.length() < 9) {
            int num = 9 - duration.length();
            for (int i = 0; i < num; i++) {
                temp.append("0");
            }
        }
        temp.append(duration);
        return temp.toString();
    }

    public static String protocolIdCal(String protocolId) {
        StringBuffer temp = new StringBuffer();

        if (protocolId.length() < 2) {
            temp.append("0");
        }
        temp.append(protocolId);
        return temp.toString();
    }
}
