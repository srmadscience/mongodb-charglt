package ie.rolfe.mongodbcharglt.documents;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class AbstractBaseTable {

    public static long getLong(org.bson.Document document, String key) {
        Object tempObject = document.get(key);

        if (tempObject == null) {
            return Long.MIN_VALUE;
        }
        if (tempObject instanceof Integer) {
            return (int) tempObject;
        }
        return (long) tempObject;

    }

    public static Date getDate(org.bson.Document document, String key) {
        SimpleDateFormat sdf = new SimpleDateFormat("MMM dd, yyyy, HH:mm:ss a", Locale.ENGLISH);
        Object tempObject = document.get(key);

        if (tempObject == null) {
            return null;
        } else if (tempObject instanceof Date) {
            return (Date) tempObject;
        }

        Date dateTime = null;

        try {
            dateTime = sdf.parse(tempObject.toString());
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        return dateTime;
    }
}
