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
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import ie.rolfe.mongodbcharglt.documents.ExtraUserData;
import ie.rolfe.mongodbcharglt.documents.UserTable;
import ie.rolfe.mongodbcharglt.documents.UserUsageTable;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;
import org.voltdb.voltutil.stats.SafeHistogramCache;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.ne;
import static com.mongodb.client.model.Updates.set;

/**
 * This is an abstract class that contains the actual logic of the demo code.
 * <p>
 * Benchmarks run on a single thread that sends requests using the MongoDB reactive streams driver.
 * Responses arrive on the driver's threads, which update the per-user state objects.
 */
public abstract class BaseChargingDemo {

    public static final long GENERIC_QUERY_USER_ID = 42;
    public static final long NO_SESSION = Long.MIN_VALUE;
    public static final Date NO_EXPIRY = new Date(0);

    public static final String REPORT_QUOTA_USAGE = "ReportQuotaUsage";
    public static final String KV_PUT = "KV_PUT";
    public static final String KV_GET = "KV_GET";
    public static final String DELETE_DOC = "Delete Doc";
    public static final String DELETE_DOC_ERROR = "Delete Doc Error";
    public static final String ADD_DOC = "Add Doc";
    public static final String ADD_DOC_ERROR = "Add Doc Error";
    public static final String UNABLE_TO_MEET_REQUESTED_TPS = "UNABLE_TO_MEET_REQUESTED_TPS";
    public static final String EXTRA_MS = "EXTRA_MS";
    public static final String MAX_IN_FLIGHT_ENV = "MAX_IN_FLIGHT";
    public static final int MONGO_DEFAULT_PORT = 27017;
    private static final String CHARGLT_DATABASE = "CHARGLT_DB";
    private static final String CHARGLT_USERS = "CHARGLT_USERS";
    private static final String ADD_CREDIT = "ADD_CREDIT";
    private static final String CLEAR_LOCK = "CLEAR_LOCK";
    private static final String CLEAR_UNFINISHED = "CLEAR_UNFINISHED";
    private static final String COUNT_USAGE_TOTAL_BY_DOC = "COUNT_USAGE_TOTAL_BY_DOC";
    private static final String TXN_RETRY = "TXN_RETRY";
    private static final String USER_NOT_FOUND = "USER_NOT_FOUND";
    private static final String ASYNC_ERROR = "ASYNC_ERROR";
    private static final int MAX_TXN_RETRIES = 10;
    private static final int DEFAULT_MAX_IN_FLIGHT = 64;
    public static SafeHistogramCache shc = SafeHistogramCache.getInstance();

    private static final TransactionOptions TXN_OPTIONS = TransactionOptions.builder()
            .writeConcern(WriteConcern.MAJORITY)
            .build();

    /**
     * How many requests we allow to be outstanding at once. More than the server can handle just
     * adds queueing latency without adding throughput.
     */
    private static final int MAX_IN_FLIGHT = getMaxInFlight();

    /**
     * A permit for each request we're allowed to have outstanding. Stops us queuing work faster than
     * MongoDB can do it, and lets us wait for everything to finish.
     */
    private static final Semaphore inFlight = new Semaphore(MAX_IN_FLIGHT);

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
     */
    protected static MongoClient connectMongoDB(String connectionString) {

        String uri = "mongodb://" + connectionString + ":" + MONGO_DEFAULT_PORT + "/";

        msg("Connecting to MongoDB at " + uri + ", up to " + MAX_IN_FLIGHT + " requests in flight");

        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(uri))
                .applyToConnectionPoolSettings(pool -> pool.maxSize(Math.max(100, MAX_IN_FLIGHT)))
                .build();

        return MongoClients.create(settings);
    }

    private static MongoCollection<Document> getCollection(MongoClient mongoClient) {
        return mongoClient.getDatabase(CHARGLT_DATABASE).getCollection(CHARGLT_USERS);
    }

    private static int getMaxInFlight() {
        String max = System.getenv(MAX_IN_FLIGHT_ENV);
        if (max == null || max.isEmpty()) {
            return DEFAULT_MAX_IN_FLIGHT;
        }
        return Integer.parseInt(max);
    }

    /**
     * Wait for a free slot to send a request in.
     */
    private static void startRequest() throws InterruptedException {
        inFlight.acquire();
    }

    /**
     * Every request must call this exactly once when it finishes, whether it worked or not.
     */
    private static void endRequest() {
        inFlight.release();
    }

    /**
     * Wait for all outstanding requests to finish.
     */
    private static void drain() throws InterruptedException {
        inFlight.acquire(MAX_IN_FLIGHT);
        inFlight.release(MAX_IN_FLIGHT);
    }

    /**
     * Send a request.
     *
     * @param request   the request
     * @param onSuccess called with the result, if there is one
     * @param onFinish  called exactly once after the request finishes, whether it worked or not
     */
    private static <T> void submit(Publisher<T> request, Consumer<T> onSuccess, Runnable onFinish)
            throws InterruptedException {

        startRequest();

        Mono.from(request)
                .doFinally(signal -> {
                    try {
                        onFinish.run();
                    } finally {
                        endRequest();
                    }
                })
                .subscribe(onSuccess, e -> {
                    shc.incCounter(ASYNC_ERROR);
                    msg("Request failed: " + e.getMessage());
                });
    }

    /**
     * Convenience method to generate a JSON payload.
     *
     * @param length
     * @return
     */
    public static ExtraUserData getExtraUserDataAsObject(int length, Gson gson, Random r) {

        ExtraUserData eud = new ExtraUserData();

        eud.loyaltySchemeName = "HelperCard";
        eud.loyaltySchemeNumber = getNewLoyaltyCardNumber(r);

        StringBuffer ourText = new StringBuffer();

        for (int i = 0; i < length / 2; i++) {
            ourText.append(Integer.toHexString(r.nextInt(256)));
        }

        eud.mysteriousHexPayload = ourText.toString();

        return eud;
    }


    protected static void upsertAllUsers(int userCount, int tpMs, ExtraUserData ourEud, int initialCredit,
                                         MongoClient mongoClient, int extraMs)
            throws InterruptedException {

        final long startMsUpsert = System.currentTimeMillis();

        TpMsThrottle throttle = new TpMsThrottle(tpMs, extraMs);
        Random r = new Random();

        MongoCollection<Document> collection = getCollection(mongoClient);
        ReplaceOptions upsert = new ReplaceOptions().upsert(true);

        for (int i = 0; i < userCount; i++) {

            throttle.await();

            UserTable newUser = UserTable.getUserTable(ourEud, r.nextInt(initialCredit), i, startMsUpsert);
            newUser.addCredit(100, "Txn_" + i);
            newUser.reportQuotaUsage(100, 10, 100, "TX2_" + i);

            final long startMs = System.currentTimeMillis();
            final boolean[] worked = {false};

            submit(collection.replaceOne(eq(i), newUser.toDocument(), upsert), result -> worked[0] = true, () -> {
                if (worked[0]) {
                    shc.reportLatency(BaseChargingDemo.ADD_DOC, startMs, "Add time", 2000);
                    shc.incCounter(BaseChargingDemo.ADD_DOC);
                } else {
                    shc.incCounter(BaseChargingDemo.ADD_DOC_ERROR);
                }
            });

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

        drain();

        long entriesPerMS = userCount / Math.max(1, System.currentTimeMillis() - startMsUpsert);
        msg("Upserted " + entriesPerMS + " users per ms...");
        msg(shc.toString());
    }


    protected static void deleteAllUsers(MongoClient mongoClient, int userCount, int tpMs, int extraMs)
            throws InterruptedException {

        final long startMsUpsert = System.currentTimeMillis();

        TpMsThrottle throttle = new TpMsThrottle(tpMs, extraMs);
        MongoCollection<Document> collection = getCollection(mongoClient);

        for (int i = 0; i < userCount; i++) {

            throttle.await();

            final long startMs = System.currentTimeMillis();
            final boolean[] deleted = {false};

            submit(collection.deleteOne(eq(i)), result -> deleted[0] = result.getDeletedCount() == 1, () -> {
                shc.reportLatency(BaseChargingDemo.DELETE_DOC, startMs, "Delete time", 2000);
                shc.incCounter(deleted[0] ? BaseChargingDemo.DELETE_DOC : BaseChargingDemo.DELETE_DOC_ERROR);
            });

            if (i % 100000 == 1) {
                msg("Deleted " + i + " users...");
            }

        }

        drain();

        long entriesPerMS = userCount / Math.max(1, System.currentTimeMillis() - startMsUpsert);
        msg("Deleted " + entriesPerMS + " users per ms...");
        msg(shc.toString());

    }

    /**
     * Convenience method to query a user a general stats and log the results
     */
    protected static void queryUserAndStats(MongoClient mongoClient, long queryUserId) {
        MongoCollection<Document> collection = getCollection(mongoClient);

        // Query user #queryUserId...
        msg("Query user #" + queryUserId + "...");
        reportDocument(Mono.from(collection.find(eq(queryUserId)).first()).block());

        msg("Show amount of credit currently reserved for products...");
        getCurrentReservedCredit(collection);

    }

    /**
     * Total reserved credit across all users, as a single aggregation on the server rather than
     * a doc by doc fetch.
     */
    private static void getCurrentReservedCredit(MongoCollection<Document> collection) {

        final long getDocByDocMs = System.currentTimeMillis();

        // userUsage is a map of session id -> reservation, so turn it into an array and add up allocatedAmount
        List<Bson> pipeline = List.of(
                new Document("$project", new Document("reserved",
                        new Document("$sum", new Document("$map",
                                new Document("input", new Document("$objectToArray", "$userUsage"))
                                        .append("in", "$$this.v.allocatedAmount"))))),
                new Document("$group", new Document("_id", null)
                        .append("total", new Document("$sum", "$reserved"))
                        .append("users", new Document("$sum", 1))));

        Document result = Mono.from(collection.aggregate(pipeline).first()).block();

        if (result == null) {
            msg("Total for 0 users is 0");
        } else {
            msg("Total for " + result.get("users") + " users is " + result.get("total"));
        }

        shc.reportLatency(BaseChargingDemo.COUNT_USAGE_TOTAL_BY_DOC, getDocByDocMs, "Time to count usage", 10000);
    }


    static protected void reportDocument(Document document) {
        if (document == null) {
            msg("Document is null...");
        } else {
            UserTable foo = new UserTable(document);
            msg(foo.toString());

        }
    }

    /**
     * Read a user, change it, and write it back, all inside a transaction. If MongoDB reports a transient
     * transaction error, such as a write conflict with another client, we retry with a new session -
     * which is what withTransaction does for the sync driver.
     *
     * @param mongoClient
     * @param userId
     * @param change      changes the UserTable and returns true if it needs to be written
     * @return the UserTable as written, or empty if the user doesn't exist
     */
    private static Mono<UserTable> readModifyWrite(MongoClient mongoClient, long userId, Predicate<UserTable> change) {

        MongoCollection<Document> collection = getCollection(mongoClient);

        return Mono.usingWhen(
                        Mono.from(mongoClient.startSession()),
                        session -> {
                            session.startTransaction(TXN_OPTIONS);

                            return Mono.from(collection.find(session, eq(userId)).first())
                                    .switchIfEmpty(Mono.defer(() -> {
                                        shc.incCounter(USER_NOT_FOUND);
                                        msg("User " + userId + " not found");
                                        return Mono.empty();
                                    }))
                                    .flatMap(userDoc -> {
                                        UserTable ut = new UserTable(userDoc);
                                        if (!change.test(ut)) {
                                            return Mono.just(ut);
                                        }
                                        return Mono.from(collection.replaceOne(session, eq(userId), ut.toDocument()))
                                                .thenReturn(ut);
                                    })
                                    .flatMap(ut -> Mono.from(session.commitTransaction()).thenReturn(ut));
                        },
                        BaseChargingDemo::endSession,
                        (session, error) -> endSession(session),
                        BaseChargingDemo::endSession)
                .retryWhen(Retry.max(MAX_TXN_RETRIES)
                        .filter(BaseChargingDemo::isRetryable)
                        .doBeforeRetry(signal -> shc.incCounter(TXN_RETRY)));
    }

    /**
     * Abort the session's transaction if it's still open, then close it.
     */
    private static Mono<Void> endSession(ClientSession session) {

        Mono<Void> abort = session.hasActiveTransaction() ? Mono.from(session.abortTransaction()) : Mono.empty();

        return abort.onErrorResume(e -> Mono.empty()).doFinally(signal -> session.close());
    }

    private static boolean isRetryable(Throwable e) {
        return e instanceof MongoException me
                && (me.hasErrorLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL)
                || me.hasErrorLabel(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL));
    }


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

        UserKVState[] userState = new UserKVState[userCount];

        Random r = new Random();
        Gson gson = new Gson();

        for (int i = 0; i < userCount; i++) {
            userState[i] = new UserKVState(i, shc);
        }

        final long startMsRun = System.currentTimeMillis();
        TpMsThrottle throttle = new TpMsThrottle(tpMs, extraMs);

        final long endtimeMs = System.currentTimeMillis() + (durationSeconds * 1000L);

        // How many transactions we've done...
        int tranCount = 0;
        int inFlightCount = 0;
        int lockCount = 0;
        int contestedLockCount = 0;
        int fullUpdate = 0;
        int deltaUpdate = 0;

        // Keep an eye on one user while we run...
        final int watchedUser = r.nextInt(userCount);
        ScheduledExecutorService globalQueries = startGlobalQueries(mainClient, watchedUser, globalQueryFreqSeconds);

        try {
            while (endtimeMs > System.currentTimeMillis()) {

                throttle.await();

                // Find session to do a transaction for...
                int oursession = r.nextInt(userCount);

                // See if session already has an active transaction and avoid
                // it if it does.
                if (userState[oursession].isTxInFlight()) {

                    inFlightCount++;

                } else if (userState[oursession].getUserStatus() == UserKVState.STATUS_LOCKED_BY_SOMEONE_ELSE) {

                    if (userState[oursession].getOtherLockTimeMs() + ReferenceData.LOCK_TIMEOUT_MS < System
                            .currentTimeMillis()) {

                        userState[oursession].startTran();
                        userState[oursession].setStatus(UserKVState.STATUS_TRYING_TO_LOCK);
                        GetAndLockUser(mainClient, userState[oursession], oursession);
                        lockCount++;

                    } else {
                        contestedLockCount++;
                    }

                } else if (userState[oursession].getUserStatus() == UserKVState.STATUS_UNLOCKED) {

                    userState[oursession].startTran();
                    userState[oursession].setStatus(UserKVState.STATUS_TRYING_TO_LOCK);
                    GetAndLockUser(mainClient, userState[oursession], oursession);
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
                        UpdateLockedUser(mainClient, userState[oursession], userState[oursession].getLockId(),
                                oursession, getNewLoyaltyCardNumber(r) + "", ExtraUserData.NEW_LOYALTY_NUMBER);
                    } else {
                        fullUpdate++;
                        UpdateLockedUser(mainClient, userState[oursession], userState[oursession].getLockId(),
                                oursession, getExtraUserDataAsObject(jsonsize, gson, r), null);
                    }

                }

                tranCount++;

                if (tranCount % 100000 == 1) {
                    msg("Transaction " + tranCount);
                }

            }

            msg(tranCount + " transactions done...");
            msg("All entries in queue, waiting for it to drain...");
            drain();
            msg("Queue drained...");

        } finally {
            globalQueries.shutdownNow();
        }

        long transactionsPerMs = tranCount / Math.max(1, System.currentTimeMillis() - startMsRun);
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
        msg("Retries caused by concurrent changes = " + shc.getCounter(TXN_RETRY));
        msg("Errors = " + shc.getCounter(ASYNC_ERROR));

        double tps = tranCount;
        tps = tps / (System.currentTimeMillis() - startMsRun);
        tps = tps * 1000;

        reportRunLatencyStats(tpMs, tps);

        // Declare victory if we got >= 90% of requested TPS...
        return tps / (tpMs * 1000) > .9;
    }

    private static void GetAndLockUser(MongoClient mongoClient, UserKVState userKVState, int sessionId)
            throws InterruptedException {

        final long startMs = System.currentTimeMillis();
        final long[] newLockId = {NO_SESSION};
        final UserTable[] lockedUser = {null};

        submit(readModifyWrite(mongoClient, sessionId, ut -> {
            // lock() returns NO_SESSION if someone else holds an unexpired lock
            newLockId[0] = ut.lock();
            return newLockId[0] != NO_SESSION;
        }), ut -> lockedUser[0] = ut, () -> {

            if (lockedUser[0] == null) {
                userKVState.setLockId(NO_SESSION);
            } else if (newLockId[0] == NO_SESSION) {
                userKVState.setLockId(NO_SESSION);
                userKVState.setStatus(UserKVState.STATUS_LOCKED_BY_SOMEONE_ELSE);
                userKVState.lockedBySomeoneElseCount++;
                userKVState.otherLockTimeMs = System.currentTimeMillis();
            } else {
                userKVState.setLockId(newLockId[0]);
            }

            shc.reportLatency(BaseChargingDemo.KV_GET, startMs, "KV Get time", 2000);
            // Must be last - tells the sending thread it can use this user again
            userKVState.endTran();
        });

    }

    private static void UpdateLockedUser(MongoClient mongoClient, UserKVState userKVState, long lockId,
                                         int sessionId, Object extraPayload, String deltaOperationName)
            throws InterruptedException {

        final long startMs = System.currentTimeMillis();

        submit(readModifyWrite(mongoClient, sessionId, ut -> {

            if (ut.isLockedBySomeoneElse(lockId)) {
                userKVState.lockedBySomeoneElseCount++;
                msg(sessionId + ": locked by session " + ut.userSoftLockSessionId + " until " + ut.userSoftlockExpiry);
                return false;
            }

            if (deltaOperationName != null && deltaOperationName.equals(ExtraUserData.NEW_LOYALTY_NUMBER)) {
                ut.userDataObject.loyaltySchemeNumber = Long.parseLong((String) extraPayload);
            } else {
                ut.userDataObject = (ExtraUserData) extraPayload;
            }

            ut.unLock();
            return true;

        }), ut -> {
        }, () -> {
            userKVState.setLockId(NO_SESSION);
            shc.reportLatency(BaseChargingDemo.KV_PUT, startMs, "KV Put Time", 2000);
            // Must be last - tells the sending thread it can use this user again
            userKVState.endTran();
        });

    }

    /**
     * Report on one user and global stats every freqSeconds, on a thread of its own so the
     * sending thread isn't held up.
     */
    private static ScheduledExecutorService startGlobalQueries(MongoClient mongoClient, long queryUserId,
                                                               int freqSeconds) {

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                queryUserAndStats(mongoClient, queryUserId);
            } catch (Exception e) {
                msg("Global query failed: " + e.getMessage());
            }
        }, freqSeconds, Math.max(1, freqSeconds), TimeUnit.SECONDS);

        return scheduler;
    }

    /**
     * Convenience method to remove unneeded records storing old allotments of
     * credit. Done as a single updateMany on the server.
     *
     */
    protected static void clearUnfinishedTransactions(MongoClient mongoClient) {

        msg("clearUnfinishedTransactions...");
        final long startMs = System.currentTimeMillis();

        UpdateResult result = Mono.from(getCollection(mongoClient)
                        .updateMany(ne("userUsage", new Document()), set("userUsage", new Document())))
                .block();

        shc.reportLatency(BaseChargingDemo.CLEAR_UNFINISHED, startMs, "clear unfinished", 10000);
        msg("...done. Cleared " + (result == null ? 0 : result.getModifiedCount()) + " users");

    }

    /**
     *
     * Convenience method to clear outstanding locks between runs
     *
     * @param mongoClient
     */
    protected static void unlockAllRecords(MongoClient mongoClient) {

        msg("Clearing locked sessions from prior runs...");
        BasicDBObject updateFields = new BasicDBObject();
        updateFields.append("userSoftlockExpiry", NO_EXPIRY);
        updateFields.append("userSoftLockSessionId", NO_SESSION);
        BasicDBObject setQuery = new BasicDBObject();
        setQuery.append("$set", updateFields);

        final long startMs = System.currentTimeMillis();
        UpdateResult result = Mono.from(getCollection(mongoClient)
                .updateMany(gt("userSoftLockSessionId", NO_SESSION), setQuery)).block();
        msg("Unlocked " + (result == null ? 0 : result.getModifiedCount()) + " records");

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
                                                     int globalQueryFreqSeconds, MongoClient mainClient, int extraMs)
            throws InterruptedException {

        // Used to track changes and be unique when we are running multiple processes
        final long pid = getPid();

        Random r = new Random();

        UserTransactionState[] users = new UserTransactionState[userCount];

        msg("Creating internal client records for " + users.length + " users");
        for (int i = 0; i < users.length; i++) {
            // We don't know a users credit till we've spoken to the server, so
            // we make an optimistic assumption...
            users[i] = new UserTransactionState(i, 2000);
        }

        final long startMsRun = System.currentTimeMillis();
        TpMsThrottle throttle = new TpMsThrottle(tpMs, extraMs);

        final long endtimeMs = System.currentTimeMillis() + (durationSeconds * 1000L);

        // How many transactions we've done...
        long tranCount = 0;
        long inFlightCount = 0;
        long addCreditCount = 0;
        long reportUsageCount = 0;

        msg("starting...");

        ScheduledExecutorService globalQueries = startGlobalQueries(mainClient, GENERIC_QUERY_USER_ID,
                globalQueryFreqSeconds);

        try {
            while (endtimeMs > System.currentTimeMillis()) {

                throttle.await();

                int randomuser = r.nextInt(userCount);

                if (users[randomuser].isTxInFlight()) {
                    inFlightCount++;
                } else {

                    users[randomuser].startTran();

                    if (users[randomuser].spendableBalance < 1000) {

                        addCreditCount++;

                        final long extraCredit = r.nextInt(1000) + 1000;
                        addCredit(mainClient, randomuser, extraCredit, users[randomuser]);

                    } else {

                        reportUsageCount++;

                        int unitsUsed = (int) (users[randomuser].currentlyReserved * 0.9);
                        int unitsWanted = r.nextInt(100);
                        reportQuotaUsage(mainClient, randomuser, unitsUsed, unitsWanted,
                                users[randomuser].sessionId,
                                "ReportQuotaUsage_" + pid + "_" + reportUsageCount + "_" + System.currentTimeMillis(),
                                users[randomuser]);

                    }
                }

                if (tranCount++ % 100000 == 0) {
                    msg("On transaction #" + tranCount);
                }

            }

            msg("finished adding transactions to queue");
            drain();
            msg("Queue drained");

        } finally {
            globalQueries.shutdownNow();
        }

        long elapsedTimeMs = System.currentTimeMillis() - startMsRun;
        msg("Processed " + tranCount + " transactions in " + elapsedTimeMs + " milliseconds");

        double tps = tranCount;
        tps = tps / (elapsedTimeMs / 1000d);

        msg("TPS = " + tps);

        msg("Add Credit calls = " + addCreditCount);
        msg("Report Usage calls = " + reportUsageCount);
        msg("Skipped because transaction was in flight = " + inFlightCount);
        msg("Retries caused by concurrent changes = " + shc.getCounter(TXN_RETRY));
        msg("Errors = " + shc.getCounter(ASYNC_ERROR));

        reportRunLatencyStats(tpMs, tps);

        // Declare victory if we got >= 90% of requested TPS...
        return tps / (tpMs * 1000) > .9;
    }


    private static void addCredit(MongoClient mongoClient, int randomuser, long extraCredit,
                                  UserTransactionState userTS) throws InterruptedException {

        final long startMs = System.currentTimeMillis();
        final String txnId = "AddCredit_" + getPid() + "_" + randomuser + "_" + System.nanoTime();

        submit(readModifyWrite(mongoClient, randomuser, ut -> {
            ut.addCredit(extraCredit, txnId);
            return true;
        }), ut -> {
            userTS.spendableBalance = ut.getAvailableCredit();
            shc.reportLatency(BaseChargingDemo.KV_PUT, startMs, "KV Put time", 2000);
        }, () -> {
            shc.reportLatency(BaseChargingDemo.ADD_CREDIT, startMs, "ADD_CREDIT", 2000);
            shc.incCounter(BaseChargingDemo.ADD_CREDIT);
            // Must be last - tells the sending thread it can use this user again
            userTS.endTran();
        });

    }


    private static void reportQuotaUsage(MongoClient mongoClient, int randomuser, int unitsUsed, int unitsWanted,
                                         long sessionId, String txnId, UserTransactionState userTS)
            throws InterruptedException {

        final long startMs = System.currentTimeMillis();

        submit(readModifyWrite(mongoClient, randomuser, ut -> {
            ut.reportQuotaUsage(unitsUsed, unitsWanted, sessionId, txnId);
            return true;
        }), ut -> {
            userTS.spendableBalance = ut.getAvailableCredit();
            UserUsageTable reservation = ut.getUserUsage(sessionId);
            userTS.currentlyReserved = reservation == null ? 0 : reservation.allocatedAmount;
            shc.reportLatency(BaseChargingDemo.KV_PUT, startMs, "KV Put time", 2000);
        }, () -> {
            shc.reportLatency(BaseChargingDemo.REPORT_QUOTA_USAGE, startMs, "REPORT_QUOTA_USAGE", 2000);
            shc.incCounter(BaseChargingDemo.REPORT_QUOTA_USAGE);
            // Must be last - tells the sending thread it can use this user again
            userTS.endTran();
        });

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
