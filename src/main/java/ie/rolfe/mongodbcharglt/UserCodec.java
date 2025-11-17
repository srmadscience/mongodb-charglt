package ie.rolfe.mongodbcharglt;

import ie.rolfe.mongodbcharglt.documents.UserRecentTransactions;
import ie.rolfe.mongodbcharglt.documents.UserTable;
import ie.rolfe.mongodbcharglt.documents.UserUsageTable;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class UserCodec implements Codec<UserTable> {
    @Override
    public UserTable decode(BsonReader bsonReader, DecoderContext decoderContext) {

        UserTable ut = new UserTable();

        bsonReader.readStartDocument();
        while (bsonReader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            final String name = bsonReader.readName();
            switch (name) {
                case "userId":
                    ut.userId = bsonReader.readInt64();
                    break;
                case "userLastSeen":
                    if (bsonReader.getCurrentBsonType() == BsonType.DATE_TIME) {
                        ut.userLastSeen = new Date(bsonReader.readDateTime());
                    }

                    break;
                case "userSoftlockExpiry":
                    if (bsonReader.getCurrentBsonType() == BsonType.DATE_TIME) {
                        ut.userSoftlockExpiry = new Date(bsonReader.readDateTime());
                    }
                    break;

                case "urt":
                    HashMap<String, UserRecentTransactions> urt = new HashMap<String, UserRecentTransactions>();
                    bsonReader.readStartArray();

                    UserRecentTransactions newUrt = new UserRecentTransactions();

                    bsonReader.readStartDocument();

                    while (bsonReader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                        //ingredientDrafts.add(ingredientDraftCodec.decode(reader, decoderContext));
                        final String rowFieldName = bsonReader.readName();
                        switch (rowFieldName) {
                            case "userId":
                                ut.userId = bsonReader.readInt64();
                                break;
                            default:
                                throw new RuntimeException(String.format("Unknown key: %s", name));
                        }
                    }

                    urt.put(newUrt.userTxnId, newUrt);
                    newUrt = new UserRecentTransactions();

                    bsonReader.readEndArray();
                    //BsonType.
                    //ut.userRecentTransactions = urt;
                    bsonReader.readEndArray();
                    break;
                case "uu":
                    final HashMap<Long, UserUsageTable> uu = new HashMap<Long, UserUsageTable>();
                    bsonReader.readStartArray();
                    while (bsonReader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                        // uu.add(stepDraftCodec.decode(reader, decoderContext));
                    }
                    ut.userUsage = uu;
                    bsonReader.readEndArray();
                    break;

                default:
                    throw new RuntimeException(String.format("Unknown key: %s", name));
            }
        }

        return ut;
    }

    @Override
    public void encode(BsonWriter bsonWriter, UserTable userTable, EncoderContext encoderContext) {
        if (userTable != null) {

            bsonWriter.writeStartDocument();
            bsonWriter.writeName("userId");
            bsonWriter.writeInt64(userTable.userId);

            bsonWriter.writeName("userLastSeen");
            if (userTable.userLastSeen == null) {
                bsonWriter.writeNull();
            } else {
                bsonWriter.writeDateTime(userTable.userLastSeen.getTime());
            }

            bsonWriter.writeName("userSoftlockExpiry");
            if (userTable.userSoftlockExpiry == null) {
                bsonWriter.writeNull();
            } else {
                bsonWriter.writeDateTime(userTable.userSoftlockExpiry.getTime());
            }

            bsonWriter.writeName("urt");
            bsonWriter.writeStartArray();

            if (userTable.getUserRecentTransactions() != null && !userTable.getUserRecentTransactions().isEmpty()) {

                HashMap<String, UserRecentTransactions> urt = userTable.getUserRecentTransactions();
                for (Map.Entry<String, UserRecentTransactions> entry : urt.entrySet()) {
                    //bsonWriter.w
                    bsonWriter.writeName("userTxnId");
                    bsonWriter.writeString(entry.getValue().userTxnId);
                    bsonWriter.writeName("txnTime");
                    bsonWriter.writeDateTime(entry.getValue().txnTime.getTime());
                    bsonWriter.writeName("sessionId");
                    bsonWriter.writeInt64(entry.getValue().sessionId);
                    bsonWriter.writeName("approvedAmount");
                    bsonWriter.writeInt64(entry.getValue().approvedAmount);
                    bsonWriter.writeName("spentAmount");
                    bsonWriter.writeInt64(entry.getValue().spentAmount);
                    bsonWriter.writeName("purpose");
                    bsonWriter.writeString(entry.getValue().purpose);
                }
            }

            bsonWriter.writeEndArray();

            bsonWriter.writeName("uu");
            bsonWriter.writeStartArray();
            if (userTable.userUsage != null && !userTable.userUsage.isEmpty()) {

                HashMap<Long, UserUsageTable> uu = userTable.userUsage;
                for (Map.Entry<Long, UserUsageTable> entry : uu.entrySet()) {
                    bsonWriter.writeName("approvedAmount");
                    bsonWriter.writeInt64(entry.getValue().allocatedAmount);
                    bsonWriter.writeName("sessionId");
                    bsonWriter.writeInt64(entry.getValue().sessionId);
                    bsonWriter.writeName("lastDate");
                    bsonWriter.writeDateTime(entry.getValue().lastDate.getTime());
                }
                bsonWriter.writeEndArray();

                bsonWriter.writeEndDocument();

            }
        }
    }

    @Override
    public Class<UserTable> getEncoderClass() {
        return UserTable.class;
    }
}
