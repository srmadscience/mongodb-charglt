/*
 * Copyright (C) 2025 David Rolfe
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */
/*
 * Copyright (C) 2025 Volt Active Data Inc.
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */

package ie.rolfe.mongodbcharglt;

import com.google.gson.Gson;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoException;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import ie.rolfe.mongodbcharglt.documents.UserTable;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.voltdb.voltutil.stats.SafeHistogramCache;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;

/**
 * This is an abstract class that contains the actual logic of the demo code.
 */
public abstract class BaseChargingDemo {

    public static final long GENERIC_QUERY_USER_ID = 42;
    public static final int HISTOGRAM_SIZE_MS = 1000000;
    public static final long NO_SESSION = Long.MIN_VALUE;

    public static final String REPORT_QUOTA_USAGE = "ReportQuotaUsage";
    public static final String KV_PUT = "KV_PUT";
    public static final String KV_GET = "KV_GET";
    public static final String DELETE_DOC = "Delete Doc";
    public static final String DELETE_DOC_ERROR = "Delete Doc Error";
    public static final String ADD_DOC = "Add Doc";
    public static final String ADD_DOC_ERROR = "Add Doc Error";
    public static final String UNABLE_TO_MEET_REQUESTED_TPS = "UNABLE_TO_MEET_REQUESTED_TPS";
    public static final String EXTRA_MS = "EXTRA_MS";
    public static final int MONGO_DEFAULT_PORT = 27017;
    private static final String CHARGLT_DATABASE = "CHARGLT_DB";
    private static final String CHARGLT_USERS = "CHARGLT_USERS";
    private static final String ADD_CREDIT = "ADD_CREDIT";
    private static final String CLEAR_LOCK = "CLEAR_LOCK";
    public static SafeHistogramCache shc = SafeHistogramCache.getInstance();

    /**
     * Print a formatted message.
     *
     * @param message
     */
    public static void msg(String message) {

        SimpleDateFormat sdfDate;
        sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date now = new Date();
        String strDate = sdfDate.format(now);
        System.out.println(strDate + ":" + message);

    }

    /**
     * Connect to MongoDB using a comma delimited hostname list.
     *
     * @param connectionString list of hostnames
     * @return an instance of the DB client
     * @throws Exception for various reasons
     */
    @SuppressWarnings("deprecation")
    protected static MongoClient connectMongoDB(String connectionString) throws Exception {


        String uri = "mongodb://" + connectionString + ":" + MONGO_DEFAULT_PORT + "/";
        MongoClient mongoClient = null;

        // Create a new client and connect to the server
        try {
            mongoClient = MongoClients.create(uri);
        } catch (MongoException e) {
            e.printStackTrace();
        }

        return mongoClient;
    }

    /**
     * Convenience method to generate a JSON payload.
     *
     * @param length
     * @return
     */
    protected static String getExtraUserDataAsJsonString(int length, Gson gson, Random r) {

        ExtraUserData eud = new ExtraUserData();

        eud.loyaltySchemeName = "HelperCard";
        eud.loyaltySchemeNumber = getNewLoyaltyCardNumber(r);

        StringBuffer ourText = new StringBuffer();

        for (int i = 0; i < length / 2; i++) {
            ourText.append(Integer.toHexString(r.nextInt(256)));
        }

        eud.mysteriousHexPayload = ourText.toString();

        return gson.toJson(eud);
    }


    protected static void upsertAllUsers(int userCount, int tpMs, String ourJson, int initialCredit, MongoClient mongoClient, MongoClient otherClient)
            throws InterruptedException {

        final long startMsUpsert = System.currentTimeMillis();

        SafeHistogramCache shc = SafeHistogramCache.getInstance();
        long currentMs = System.currentTimeMillis();
        int tpThisMs = 0;
        Random r = new Random();
        Gson g = new Gson();

        MongoDatabase database = mongoClient.getDatabase(CHARGLT_DATABASE);
        MongoCollection<Document> collection = database.getCollection(CHARGLT_USERS);

        for (int i = 0; i < userCount; i++) {

            if (tpThisMs++ > tpMs) {

                while (currentMs == System.currentTimeMillis()) {
                    Thread.sleep(0, 50000);
                }

                currentMs = System.currentTimeMillis();
                tpThisMs = 0;
            }

            UserTable newUser = UserTable.getUserTable(ourJson, r.nextInt(initialCredit), i, startMsUpsert);
            newUser.addCredit(100, "Txn1");
            newUser.reportQuotaUsage(100, 10, 100, "TX2");
            String jsonObject = g.toJson(newUser, UserTable.class);

            Document document2 = Document.parse(jsonObject);
            final long startMs = System.currentTimeMillis();
            collection.insertOne(document2);
            shc.reportLatency(BaseChargingDemo.ADD_DOC, startMs, "Add time", 2000);
            shc.incCounter(BaseChargingDemo.ADD_DOC);

            if (i % 100000 == 1) {
                msg("Upserted " + i + " users...");

                if (shc.getCounter(BaseChargingDemo.ADD_DOC_ERROR) > 0) {
                    msg("Errors detected. Halting...");
                    break;
                } else {
                    queryUserAndStats(mongoClient, i);
                }

            }

        }


        long entriesPerMS = userCount / (System.currentTimeMillis() - startMsUpsert);
        msg("Upserted " + entriesPerMS + " users per ms...");
        msg(shc.toString());
    }


    protected static void deleteAllUsers(MongoClient mongoClient, int userCount, int tpMs) {

        final long startMsUpsert = System.currentTimeMillis();

        SafeHistogramCache shc = SafeHistogramCache.getInstance();
        long currentMs = System.currentTimeMillis();
        int tpThisMs = 0;

        MongoDatabase database = mongoClient.getDatabase(CHARGLT_DATABASE);
        MongoCollection<Document> collection = database.getCollection(CHARGLT_USERS);

        for (int i = 0; i < userCount; i++) {

            if (tpThisMs++ > tpMs) {

                while (currentMs == System.currentTimeMillis()) {
                    try {
                        Thread.sleep(0, 50000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }

                currentMs = System.currentTimeMillis();
                tpThisMs = 0;
            }

            long startNs = System.currentTimeMillis();
            DeleteResult dl = collection.deleteOne(eq(i));
            shc.reportLatency(BaseChargingDemo.DELETE_DOC, startNs, "Delete time", 2000);
            if (dl.getDeletedCount() == 1) {
                shc.incCounter(BaseChargingDemo.DELETE_DOC);
            } else {
                shc.incCounter(BaseChargingDemo.DELETE_DOC_ERROR);
            }

            if (i % 100000 == 1) {
                msg("Deleted " + i + " users...");
            }

        }

        long entriesPerMS = userCount / (System.currentTimeMillis() - startMsUpsert);
        msg("Deleted " + entriesPerMS + " users per ms...");
        msg(shc.toString());

    }

    /**
     * Convenience method to query a user a general stats and log the results.
     *
     *
     */
    protected static void queryUserAndStats(MongoClient mongoClient, long queryUserId) {
        MongoDatabase database = mongoClient.getDatabase(CHARGLT_DATABASE);
        MongoCollection<Document> collection = database.getCollection(CHARGLT_USERS);

        // Query user #queryUserId...
        msg("Query user #" + queryUserId + "...");
        getUser(queryUserId, collection, BaseChargingDemo::reportDocument);

        msg("Show amount of credit currently reserved for products...");
        //TODO
//        ClientResponse allocResponse = mongoClient.callProcedure("ShowCurrentAllocations__promBL");
//
//        for (int i = 0; i < allocResponse.getResults().length; i++) {
//            msg(System.lineSeparator() + allocResponse.getResults()[i].toFormattedString());
//        }
    }


    private static void getUser(long queryUserId, MongoCollection<Document> collection, java.util.function.Consumer<Document> nextStep) {
        Document userDoc = collection.find(eq(queryUserId)).first();

        reportDocument(userDoc);
    }


    static protected void reportDocument(Document document) {
        if (document == null) {
            msg("Document is null...");
        } else {
            UserTable foo = new UserTable(document);
            msg(foo.toString());

        }
    }


//    /**
//     *
//     * Convenience method to query all users who have a specific loyalty card id
//     *
//     * @param mainClient
//     * @param cardId
//     * @throws IOException
//     * @throws NoConnectionsException
//     * @throws ProcCallException
//     */
//    protected static void queryLoyaltyCard(Client mainClient, long cardId)
//            throws IOException, NoConnectionsException, ProcCallException {
//
//        // Query user #queryUserId...
//        msg("Query card #" + cardId + "...");
//        ClientResponse userResponse = mainClient.callProcedure("FindByLoyaltyCard", cardId);
//
//        for (int i = 0; i < userResponse.getResults().length; i++) {
//            msg(System.lineSeparator() + userResponse.getResults()[i].toFormattedString());
//        }
//
//    }
//

    /**
     *
     * Run a key value store benchmark for userCount users at tpMs transactions per
     * millisecond and with deltaProportion records sending the entire record.
     *
     * @param userCount
     * @param tpMs
     * @param durationSeconds
     * @param globalQueryFreqSeconds
     * @param jsonsize
     * @param mainClient
     * @param deltaProportion
     * @param extraMs
     * @return true if >=90% of requested throughput was achieved.
     * @throws InterruptedException
     */
    protected static boolean runKVBenchmark(int userCount, int tpMs, int durationSeconds, int globalQueryFreqSeconds,
                                            int jsonsize, MongoClient mainClient, int deltaProportion, int extraMs)
            throws InterruptedException {

        long lastGlobalQueryMs = 0;

        UserKVState[] userState = new UserKVState[userCount];

        Random r = new Random();
        Gson gson = new Gson();

        for (int i = 0; i < userCount; i++) {
            userState[i] = new UserKVState(i, shc);
        }

        final long startMsRun = System.currentTimeMillis();
        long currentMs = System.currentTimeMillis();
        int tpThisMs = 0;

        final long endtimeMs = System.currentTimeMillis() + (durationSeconds * 1000L);

        // How many transactions we've done...
        int tranCount = 0;
        int inFlightCount = 0;
        int lockCount = 0;
        int contestedLockCount = 0;
        int fullUpdate = 0;
        int deltaUpdate = 0;

        int firstSession = Integer.MIN_VALUE;

        while (endtimeMs > System.currentTimeMillis()) {

            if (tpThisMs++ > tpMs) {

                while (currentMs == System.currentTimeMillis()) {
                    Thread.sleep(0, 50000);

                }

                sleepExtraMSIfNeeded(extraMs);

                currentMs = System.currentTimeMillis();
                tpThisMs = 0;
            }

            // Find session to do a transaction for...
            int oursession = r.nextInt(userCount);

            if (firstSession == Integer.MIN_VALUE) {
                firstSession = oursession;
            }

            // See if session already has an active transaction and avoid
            // it if it does.


            if (userState[oursession].isTxInFlight()) {

                inFlightCount++;

            } else if (userState[oursession].getUserStatus() == UserKVState.STATUS_LOCKED_BY_SOMEONE_ELSE) {

                if (userState[oursession].getOtherLockTimeMs() + ReferenceData.LOCK_TIMEOUT_MS < System
                        .currentTimeMillis()) {

                    userState[oursession].startTran();
                    userState[oursession].setStatus(UserKVState.STATUS_TRYING_TO_LOCK);
                    GetAndLockUser(mainClient, userState[oursession], oursession,gson);
                    lockCount++;

                } else {
                    contestedLockCount++;
                }

            } else if (userState[oursession].getUserStatus() == UserKVState.STATUS_UNLOCKED) {

                userState[oursession].startTran();
                userState[oursession].setStatus(UserKVState.STATUS_TRYING_TO_LOCK);
                GetAndLockUser(mainClient, userState[oursession], oursession, gson);
                lockCount++;

            } else if (userState[oursession].getUserStatus() == UserKVState.STATUS_LOCKED) {

                userState[oursession].startTran();
                userState[oursession].setStatus(UserKVState.STATUS_UPDATING);

                if (deltaProportion > r.nextInt(101)) {
                    deltaUpdate++;
                    // Instead of sending entire JSON object across wire ask app to update loyalty
                    // number. For
                    // large values stored as JSON this can have a dramatic effect on network
                    // bandwidth
                    UpdateLockedUser(mainClient, userState[oursession],
                            userState[oursession].getLockId(), getNewLoyaltyCardNumber(r) + "",
                            ExtraUserData.NEW_LOYALTY_NUMBER,gson);
                } else {
                    fullUpdate++;
                    UpdateLockedUser(mainClient, userState[oursession],
                            userState[oursession].getLockId(), getExtraUserDataAsJsonString(jsonsize, gson, r), null, gson);
                }

            }

            tranCount++;
            userState[oursession].endTran(); //TODO - Fix when we make async

            if (tranCount % 100000 == 1) {
                msg("Transaction " + tranCount);
            }

            // See if we need to do global queries...
            if (lastGlobalQueryMs + (globalQueryFreqSeconds * 1000L) < System.currentTimeMillis()) {
                lastGlobalQueryMs = System.currentTimeMillis();

                queryUserAndStats(mainClient, firstSession);

            }

        }


        msg(tranCount + " transactions done...");
        msg("All entries in queue, waiting for it to drain...");
        //mainClient.drain();
        msg("Queue drained...");

        long transactionsPerMs = tranCount / (System.currentTimeMillis() - startMsRun);
        msg("processed " + transactionsPerMs + " entries per ms while doing transactions...");

        long lockFailCount = 0;
        for (int i = 0; i < userCount; i++) {
            lockFailCount += userState[i].getLockedBySomeoneElseCount();
        }

        msg(inFlightCount + " events where a tx was in flight were observed");
        msg(lockCount + " lock attempts");
        msg(contestedLockCount + " contested lock attempts");
        msg(lockFailCount + " lock attempt failures");
        msg(fullUpdate + " full updates");
        msg(deltaUpdate + " delta updates");

        double tps = tranCount;
        tps = tps / (System.currentTimeMillis() - startMsRun);
        tps = tps * 1000;

        reportRunLatencyStats(tpMs, tps);

        // Declare victory if we got >= 90% of requested TPS...
        return tps / (tpMs * 1000) > .9;
    }

    private static void GetAndLockUser(MongoClient mongoClient, UserKVState userKVState, int sessionId, Gson gson) {

        MongoDatabase kvDatabase = mongoClient.getDatabase(CHARGLT_DATABASE);
        MongoCollection<Document> collection = kvDatabase.getCollection(CHARGLT_USERS);
        // Sets transaction options
        TransactionOptions txnOptions = TransactionOptions.builder()
                .writeConcern(WriteConcern.MAJORITY)
                .build();

        Bson pk = eq(userKVState.id);
        final long startMs = System.currentTimeMillis();
        try (ClientSession session = mongoClient.startSession()) {
            // Uses withTransaction and lambda for transaction operations
            session.withTransaction(() -> {
                Document userDoc = collection.find(pk).first();
                if (userDoc != null) {
                    UserTable ut = new UserTable(userDoc);
                    ut.lock();
                    Document update = userDoc.append("userSoftlockExpiry", ut.userSoftlockExpiry).append("userSoftLockSessionId", ut.userSoftLockSessionId);
                    UpdateResult replaceResult = collection.replaceOne(pk, update);
                    if (replaceResult.getModifiedCount() == 0) {
                        msg("User not found");
                    } else {
                        userKVState.setLockId(ut.userSoftLockSessionId);
                    }
                }

                return null; // Return value as expected by the lambda
            }, txnOptions);

            shc.reportLatency(BaseChargingDemo.KV_GET, startMs, "KV Get time", 2000);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    private static void UpdateLockedUser(MongoClient mongoClient, UserKVState userKVState, long lockId, String jsonPayload, String deltaOperationName, Gson gson) {

        MongoDatabase kvDatabase = mongoClient.getDatabase(CHARGLT_DATABASE);
        MongoCollection<Document> collection = kvDatabase.getCollection(CHARGLT_USERS);
        // Sets transaction options
        TransactionOptions txnOptions = TransactionOptions.builder()
                .writeConcern(WriteConcern.MAJORITY)
                .build();

        Bson pk = eq(userKVState.id);
        final long startMs = System.currentTimeMillis();
        try (ClientSession session = mongoClient.startSession()) {
            // Uses withTransaction and lambda for transaction operations
            session.withTransaction(() -> {
                Document userDoc = collection.find(pk).first();
                if (userDoc != null) {
                    UserTable ut = new UserTable(userDoc);

                    if (ut.isLockedBySomeoneElse(lockId)) {
                        userKVState.lockedBySomeoneElseCount++;
                        msg(userKVState.id + ": locked by session " + ut.userId + " until " + ut.userSoftlockExpiry);
                    } else {

                        ut.unLock();
                        Document update = userDoc.append("userSoftlockExpiry", null).append("userSoftLockSessionId", NO_SESSION).append("jsonPayload", jsonPayload);
                        UpdateResult replaceResult = collection.replaceOne(pk, update);
                        if (replaceResult.getModifiedCount() == 0) {
                            msg("User not found");
                        }
                        userKVState.setLockId(NO_SESSION);
                    }
                }

                return null; // Return value as expected by the lambda
            }, txnOptions);
            shc.reportLatency(BaseChargingDemo.KV_PUT, startMs, "KV Put Time", 2000);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }


    /**
     * Used when we need to really slow down below 1 tx per ms..
     *
     * @param extraMs an arbitrary extra delay.
     */
    private static void sleepExtraMSIfNeeded(int extraMs) {
        if (extraMs > 0) {
            try {
                Thread.sleep(extraMs);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * Convenience method to remove unneeded records storing old allotments of
     * credit.
     *
     */
    protected static void clearUnfinishedTransactions(MongoClient mainClient)
            throws Exception {

        msg("Clearing unfinished transactions from prior runs...");

        //TODO
        //mainClient.callProcedure("@AdHoc", "DELETE FROM user_usage_table;");
        msg("...done");

    }

    /**
     *
     * Convenience method to clear outstaning locks between runs
     *
     * @param mongoClient
     */
    protected static void unlockAllRecords(MongoClient mongoClient) {

        msg("Clearing locked sessions from prior runs...");
        MongoDatabase restaurantsDatabase = mongoClient.getDatabase(CHARGLT_DATABASE);
        MongoCollection<Document> collection = restaurantsDatabase.getCollection(CHARGLT_USERS);
        BasicDBObject updateFields = new BasicDBObject();
        updateFields.append("userSoftlockExpiry", NO_SESSION);
        updateFields.append("userSoftLockSessionId", NO_SESSION);
        BasicDBObject setQuery = new BasicDBObject();
        setQuery.append("$set", updateFields);

        final long startMs = System.currentTimeMillis();
        UpdateResult userDoc = collection.updateMany(gt("userSoftlockExpiry", NO_SESSION), setQuery);
        msg("Unlocked " + userDoc.getModifiedCount() + " records");

        shc.reportLatency(BaseChargingDemo.CLEAR_LOCK, startMs, "Clear Locks", 10000);

        msg("...done");

    }


    /**
     *
     * Run a transaction benchmark for userCount users at tpMs per ms.
     *
     * @param userCount              number of users
     * @param tpMs                   transactions per milliseconds
     * @param durationSeconds
     * @param globalQueryFreqSeconds how often we check on global stats and a single
     *                               user
     * @param mainClient
     * @return true if within 90% of targeted TPS
     * @throws InterruptedException
     */
    protected static boolean runTransactionBenchmark(int userCount, int tpMs, int durationSeconds,
                                                     int globalQueryFreqSeconds, MongoClient mainClient, MongoClient otherClient, int extraMs)
            throws InterruptedException {

        Gson g = new Gson();
        MongoDatabase database = mainClient.getDatabase(CHARGLT_DATABASE);
        MongoCollection<Document> collection = database.getCollection(CHARGLT_USERS);

        // Used to track changes and be unique when we are running multiple threads
        final long pid = getPid();

        Random r = new Random();

        UserTransactionState[] users = new UserTransactionState[userCount];

        msg("Creating client records for " + users.length + " users");
        for (int i = 0; i < users.length; i++) {
            // We don't know a users credit till we've spoken to the server, so
            // we make an optimistic assumption...
            users[i] = new UserTransactionState(i, 1000);
        }

        final long startMsRun = System.currentTimeMillis();
        long currentMs = System.currentTimeMillis();
        int tpThisMs = 0;

        final long endtimeMs = System.currentTimeMillis() + (durationSeconds * 1000L);

        // How many transactions we've done...
        long tranCount = 0;
        long inFlightCount = 0;
        long addCreditCount = 0;
        long reportUsageCount = 0;
        long lastGlobalQueryMs = System.currentTimeMillis();

        msg("starting...");

        while (endtimeMs > System.currentTimeMillis()) {

            if (tpThisMs++ > tpMs) {

                while (currentMs == System.currentTimeMillis()) {
                    Thread.sleep(0, 50000);

                }

                sleepExtraMSIfNeeded(extraMs);

                currentMs = System.currentTimeMillis();
                tpThisMs = 0;
            }

            int randomuser = r.nextInt(userCount);

            if (users[randomuser].isTxInFlight()) {
                inFlightCount++;
            } else {

                users[randomuser].startTran();

                if (users[randomuser].spendableBalance < 1000) {

                    addCreditCount++;

                    final long extraCredit = r.nextInt(1000) + 1000;


                    final long startMs = System.currentTimeMillis();
                    addCredit(mainClient, randomuser, extraCredit,g);
                    shc.reportLatency(BaseChargingDemo.ADD_CREDIT, startMs, "ADD_CREDIT", 2000);
                    shc.incCounter(BaseChargingDemo.ADD_CREDIT);
                    users[randomuser].endTran();

                } else {

                    reportUsageCount++;

                    int unitsUsed = (int) (users[randomuser].currentlyReserved * 0.9);
                    int unitsWanted = r.nextInt(100);
                    final long startMs = System.currentTimeMillis();
                    reportQuotaUsage(mainClient, randomuser, unitsUsed,
                            unitsWanted, users[randomuser].sessionId,
                            "ReportQuotaUsage_" + pid + "_" + reportUsageCount + "_" + System.currentTimeMillis(), g);
                    shc.reportLatency(BaseChargingDemo.REPORT_QUOTA_USAGE, startMs, "REPORT_QUOTA_USAGE", 2000);
                    shc.incCounter(BaseChargingDemo.REPORT_QUOTA_USAGE);
                    users[randomuser].endTran();

                }
            }

            if (tranCount++ % 100000 == 0) {
                msg("On transaction #" + tranCount);
            }

            // See if we need to do global queries...
            if (lastGlobalQueryMs + (globalQueryFreqSeconds * 1000L) < System.currentTimeMillis()) {
                lastGlobalQueryMs = System.currentTimeMillis();

                queryUserAndStats(mainClient, GENERIC_QUERY_USER_ID);

            }

        }

        msg("finished adding transactions to queue");
        msg("Queue drained");

        long elapsedTimeMs = System.currentTimeMillis() - startMsRun;
        msg("Processed " + tranCount + " transactions in " + elapsedTimeMs + " milliseconds");

        double tps = tranCount;
        tps = tps / (elapsedTimeMs / 1000);

        msg("TPS = " + tps);

        msg("Add Credit calls = " + addCreditCount);
        msg("Report Usage calls = " + reportUsageCount);
        msg("Skipped because transaction was in flight = " + inFlightCount);

        reportRunLatencyStats(tpMs, tps);

        // Declare victory if we got >= 90% of requested TPS...
        return tps / (tpMs * 1000) > .9;
    }


    private static void addCredit(MongoClient mongoClient, int randomuser, long extraCredit, Gson g) {

        MongoDatabase restaurantsDatabase = mongoClient.getDatabase(CHARGLT_DATABASE);
        MongoCollection<Document> collection = restaurantsDatabase.getCollection(CHARGLT_USERS);
        // Sets transaction options
        TransactionOptions txnOptions = TransactionOptions.builder()
                .writeConcern(WriteConcern.MAJORITY)
                .build();

        try (ClientSession session = mongoClient.startSession()) {
            // Uses withTransaction and lambda for transaction operations
            session.withTransaction(() -> {
                Document userDoc = collection.find(eq(randomuser)).first();
                if (userDoc != null) {
                    collection.replaceOne(eq(randomuser), addRandomCredit(userDoc,g));
                }

                return null; // Return value as expected by the lambda
            }, txnOptions);

        } catch (Exception e) {
            e.printStackTrace();
        }


    }


    private static void reportQuotaUsage(MongoClient mainClient, int randomuser, int unitsUsed, int unitsWanted, long sessionId, String txnId, Gson gson) {

        MongoDatabase restaurantsDatabase = mainClient.getDatabase(CHARGLT_DATABASE);
        MongoCollection<Document> collection = restaurantsDatabase.getCollection(CHARGLT_USERS);

        // Sets transaction options
        TransactionOptions txnOptions = TransactionOptions.builder()
                .writeConcern(WriteConcern.MAJORITY)
                .build();

        try (ClientSession session = mainClient.startSession()) {
            // Uses withTransaction and lambda for transaction operations
            session.withTransaction(() -> {
                Document document = collection.find(eq(randomuser)).first();
                if (document != null) {
                    UserTable theUserTable = new UserTable(document);
                    theUserTable.reportQuotaUsage(unitsUsed, unitsWanted, sessionId, txnId);
                    String jsonObject = gson.toJson(theUserTable, UserTable.class);

                    collection.replaceOne(eq(randomuser), Document.parse(jsonObject));
                }

                return null; // Return value as expected by the lambda
            }, txnOptions);

        } catch (Exception e) {
            e.printStackTrace();
        }


    }


    public static Document addRandomCredit(Document document, Gson g ) {
        Random r = new Random();

        UserTable theUserTable = new UserTable(document);
        int amount = r.nextInt(1000);
        theUserTable.addCredit(amount, "AddCredit_" + amount + "_" + System.currentTimeMillis());
        String jsonObject = g.toJson(theUserTable, UserTable.class);
        return (Document.parse(jsonObject));


    }

    /**
     * Turn latency stats into a grepable string
     *
     * @param tpMs target transactions per millisecond
     * @param tps  observed TPS
     */
    private static void reportRunLatencyStats(int tpMs, double tps) {
        StringBuffer oneLineSummary = new StringBuffer("GREPABLE SUMMARY:");

        oneLineSummary.append(tpMs);
        oneLineSummary.append(':');

        oneLineSummary.append(tps);
        oneLineSummary.append(':');

        SafeHistogramCache.getProcPercentiles(shc, oneLineSummary, REPORT_QUOTA_USAGE);

        SafeHistogramCache.getProcPercentiles(shc, oneLineSummary, KV_PUT);

        SafeHistogramCache.getProcPercentiles(shc, oneLineSummary, KV_GET);

        msg(oneLineSummary.toString());

        msg(shc.toString());
    }

    /**
     * Get Linux process ID - used for pseudo unique ids
     *
     * @return Linux process ID
     */
    private static long getPid() {
        return ProcessHandle.current().pid();
    }

    /**
     * Return a loyalty card number
     *
     * @param r instance of Random
     * @return a random loyalty card number between 0 and 1 million
     */
    private static long getNewLoyaltyCardNumber(Random r) {
        return System.currentTimeMillis() % 1000000;
    }

    /**
     * get EXTRA_MS env variable if set
     *
     * @return extraMs
     */
    public static int getExtraMsIfSet() {

        int extraMs = 0;

        String extraMsEnv = System.getenv(EXTRA_MS);

        if (extraMsEnv != null && !extraMsEnv.isEmpty()) {
            msg("EXTRA_MS is '" + extraMsEnv + "'");
            extraMs = Integer.parseInt(extraMsEnv);
        }

        return extraMs;
    }

}
