package com.foxinmy.thrift;

import java.util.ArrayList;
import java.util.List;

public interface Utils {
    static String formatDuration(long durationMillis) {
        if (durationMillis <= 0) return "0 ms";

        long min = durationMillis / 60000;
        long sec = (durationMillis % 60000) / 1000;
        long ms = durationMillis % 1000;

        if (min > 0) return String.format("%d min", min);
        if (sec > 0) return String.format("%d sec", sec);
        return String.format("%d ms", ms);
    }

    static String abbreviateWithLength(String str, int maxWidth) {
        if (str == null) return null;
        if (str.length() <= maxWidth) return str;
        return str.substring(0, maxWidth)
               + " (truncated "
               + (str.length() - maxWidth)
               + " chars)";
    }

    static List<Throwable> getThrowableList(Throwable throwable) {
        final List<Throwable> list = new ArrayList<>();
        while (throwable != null && !list.contains(throwable)) {
            list.add(throwable);
            throwable = throwable.getCause();
        }
        return list;
    }

    static String getExceptionMessage(final Throwable th) {
        return th.getClass().getSimpleName() + ": " + th.getMessage();
    }
}