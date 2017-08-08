package org.filetracker;

public class Util {

    private static final long second = 1000L;
    private static final long minute = second * 60L;
    private static final long hour = minute * 60L;
    private static final long day = hour * 24L;

    public static String longSpanToStringShort(long time, int precision) {
        long d = 0L, h = 0L, m = 0L, s = 0L, millisec = 0L;

        d = time / day;
        h = (time % day) / hour;
        m = (time % hour) / minute;
        s = (time % minute) / second;

        millisec = time % second;

        // int precision = 2;

        StringBuilder b = new StringBuilder(12);
        if (d != 0 && precision > 0) {
            b.append(d).append("d");
            precision--;
        }
        if (h != 0 && precision > 0) {
            b.append(h).append("h");
            precision--;
        }
        if (m != 0 && precision > 0) {
            b.append(m).append("m");
            precision--;
        }
        if (s != 0 && precision > 0) {
            b.append(s).append("s");
            precision--;
        }
        if (millisec != 0 && precision > 0 && time < 10000L) {
            b.append(millisec).append("ms");
            precision--;
        }
        return b.toString();
    }
    private static final double KB = 1024L;
    private static final double MB = KB * 1024L;
    private static final double GB = MB * 1024L;
    private static final double TB = GB * 1024L;
    private static final double PB = TB * 1024L;

    public static String greekSize(long size) {
        if (size < KB) {
            return Long.toString(size) + " B";
        } else if (size < MB) {
            return String.format("%.1f KB", (double) size / KB);
        } else if (size < GB) {
            return String.format("%.2f MB", (double) size / MB);
        } else if (size < TB) {
            return String.format("%.2f GB", (double) size / GB);
        } else if (size < PB) {
            return String.format("%.2f TB", (double) size / TB);
        } else {
            return String.format("%.4f PB", (double) size / PB);
        }
    }

    public static String greek(long size) {
        if (size < KB) {
            return Long.toString(size);
        } else if (size < MB) {
            return String.format("%.1fK", (double) size / KB);
        } else if (size < GB) {
            return String.format("%.2fM", (double) size / MB);
        } else if (size < TB) {
            return String.format("%.2fG", (double) size / GB);
        } else if (size < PB) {
            return String.format("%.2fT", (double) size / TB);
        } else {
            return String.format("%.4fP", (double) size / PB);
        }
    }
    

}
