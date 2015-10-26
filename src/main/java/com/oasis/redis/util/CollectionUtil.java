package com.oasis.redis.util;

import java.util.Iterator;

/**
 * Collections utility tools.
 * 
 * @author Chris
 *
 */
public class CollectionUtil {

    /**
     * Join array to string with separator.
     * 
     * @param array
     * @param separator
     * @return array string
     */
    public static String join(Object[] array, String separator) {
        if (array == null) {
            return null;
        }

        StringBuilder buf = new StringBuilder(array.length * 16);
        for (int i = 0; i < array.length; i++) {
            if (i > 0) {
                buf.append(separator);
            }

            if (array[i] != null) {
                buf.append(array[i]);
            }
        }

        return buf.toString();
    }

    /**
     * Join collection to string with separator.
     * 
     * @param iterator
     * @param separator
     * @return array string
     */
    public static String join(Iterable<?> iterable, String separator) {
        if (iterable == null) {
            return null;
        }

        Iterator<?> iterator = iterable.iterator();

        if (!iterator.hasNext()) {
            return "";
        }

        Object first = iterator.next();
        if (!iterator.hasNext()) {
            return first == null ? "" : first.toString();
        }

        StringBuilder buf = new StringBuilder(256);
        if (first != null) {
            buf.append(first);
        }

        while (iterator.hasNext()) {
            buf.append(separator);

            Object obj = iterator.next();
            if (obj != null) {
                buf.append(obj);
            }
        }

        return buf.toString();
    }

}
