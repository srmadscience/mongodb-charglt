/*
 * Copyright (C) 2025 David Rolfe
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */

package ie.rolfe.mongodbcharglt.documents;

import ie.rolfe.mongodbcharglt.ReferenceData;
import org.bson.Document;

import java.security.SecureRandom;
import java.util.*;

/**
 * CREATE table user_table
 * (userid bigint not null primary key
 * ,user_json_object varchar(8000)
 * ,user_last_seen TIMESTAMP DEFAULT NOW
 * ,user_softlock_sessionid bigint
 * ,user_softlock_expiry TIMESTAMP);
 */


public class UserTable extends AbstractBaseTable {

    public static final long FIVE_MINUTES_IN_MS = 1000 * 60 * 5;
    public static final String ADDED_BY_TXN = " added by Txn ";
    public static final String ALREADY_HAPPENED = " already happened";
    public static long TX_KEEP_MS = 300000;


    public long _id;

    public long userId;
    public String userJsonObject;
    public Date userLastSeen;
    public long userSoftLockSessionId = Long.MIN_VALUE;
    public Date userSoftlockExpiry;

    public HashMap<Long, UserUsageTable> userUsage = new HashMap<Long, UserUsageTable>();
    public HashMap<String, UserRecentTransactions> userRecentTransactions = new HashMap<String, UserRecentTransactions>();

    public long balance = 0;

    public UserTable(long userId, String userJsonObject, Date userLastSeen, Date userSoftlockExpiry, long userSoftLockSessionId) {
        this.userId = userId;
        _id = userId;
        this.userJsonObject = userJsonObject;
        this.userLastSeen = userLastSeen;
        this.userSoftlockExpiry = userSoftlockExpiry;
        this.userSoftLockSessionId = userSoftLockSessionId;
    }

    public UserTable() {

    }


    public UserTable(org.bson.Document document) {
        if (document != null) {
            _id = getLong(document, "_id");
            userId = getLong(document, "userId");
            userJsonObject = document.getString("userJsonObject");
            userLastSeen = getDate(document, "userLastSeen");
            userSoftlockExpiry = getDate(document, "userSoftlockExpiry");
            userSoftLockSessionId = getLong(document, "userSoftLockSessionId");
            balance  = getLong(document, "balance");

            Document urtDoc = (Document) document.get("userRecentTransactions");

            if (urtDoc != null) {
                Collection<Object> urtAsObjects = urtDoc.values();
                Object[] urtEntries = urtAsObjects.toArray();

                for (Object urtEntry : urtEntries) {
                    UserRecentTransactions newTx = new UserRecentTransactions((Document) urtEntry);
                    userRecentTransactions.put(newTx.userTxnId, newTx);
                }
            }

            Document uuDoc = (Document) document.get("userUsage");

            if (uuDoc != null) {
                Collection<Object> uuAsObjects = uuDoc.values();
                Object[] uuEntries = uuAsObjects.toArray();

                for (Object uuEntry : uuEntries) {
                    UserUsageTable newTx = new UserUsageTable((Document) uuEntry);
                    userUsage.put(newTx.sessionId, newTx);
                }
            }
        }

    }

    public static UserTable getUserTable(String ourJson, long initialCredit, long id, long startMsUpsert) {
        String txnId = "Create_" + id;
        final long approvedAmount = 0;
        String purpose = "Created";
        Date createDate = new Date(startMsUpsert);

        UserTable newUser = new UserTable(id, ourJson, createDate, null, Long.MIN_VALUE);

        UserRecentTransactions urt = new UserRecentTransactions(id, txnId, createDate, Long.MIN_VALUE, approvedAmount, initialCredit, purpose);
        newUser.addUserRecentTransaction(urt);

        return newUser;
    }

    public UserUsageTable getUserUsage(long sessionId) {

        return userUsage.get(sessionId);

    }

    public void setUserUsage(UserUsageTable userUsageTable) {
        userUsage.put(userUsageTable.sessionId, userUsageTable);
    }

    public HashMap<String, UserRecentTransactions> getUserRecentTransactions() {
        return userRecentTransactions;
    }

    public boolean txHasHappened(String txId) {


        for (Map.Entry<String, UserRecentTransactions> entry : userRecentTransactions.entrySet()) {
            if (entry.getValue().userTxnId.equals(txId)) {
                return true;
            } else if (entry.getValue().txnTime.before(new Date(System.currentTimeMillis() - TX_KEEP_MS))) {
                userRecentTransactions.remove(entry.getKey());
            }
        }
        return false;
    }

    public void addUserRecentTransaction(UserRecentTransactions theUserRecentTransaction) {
        userRecentTransactions.put(theUserRecentTransaction.userTxnId, theUserRecentTransaction);
        balance += theUserRecentTransaction.spentAmount;
    }

    public long lock() {

        if (userSoftlockExpiry == null || userSoftlockExpiry.before(new Date(System.currentTimeMillis() - ReferenceData.LOCK_TIMEOUT_MS))) {
            SecureRandom secureRandom = new SecureRandom();
            userSoftLockSessionId = Math.abs(secureRandom.nextLong());
            userSoftlockExpiry = new Date(System.currentTimeMillis() + ReferenceData.LOCK_TIMEOUT_MS);
            return (userSoftLockSessionId);
        }

        return Long.MIN_VALUE;

    }

    @Override
    public String toString() {
        return "UserTable{" +
                "_id=" + _id +
                ", userId=" + userId +
                ", userJsonObject='" + userJsonObject + '\'' +
                ", userLastSeen=" + userLastSeen +
                ", userSoftlockExpiry=" + userSoftlockExpiry +
                ", userUsage=" + userUsage +
                ", userRecentTransactions=" + userRecentTransactions +
                ", balance=" + balance +
                '}';
    }

    //    public Document toBSON() {
//
//        Document document = new Document();
//        document.append("_id", userId);
//        document.append("userId", userId);
//        document.append("userLastSeen", userLastSeen.getTime());
//        document.append("userSoftlockExpiry", userSoftlockExpiry);
//        document.append("userJsonObject", userJsonObject);
//        document.append("userUsage", List.of(userUsage.values()));
//        document.append("userRecentTransactions", List.of(userRecentTransactions.values()));
//
//        return document;
//    }

    public String addCredit(long extraCredit, String txnId) {

        String retstring = "";

        // Sanity Check: Has this transaction already happened?
        if (isTransactionNew(txnId)) {

            // Report credit add...
            retstring = extraCredit + ADDED_BY_TXN + txnId;
            UserRecentTransactions newTran = new UserRecentTransactions(userId, txnId, 0, extraCredit, "Add Credit");
            addUserRecentTransaction(newTran);
        } else {
            retstring = "Txn " + txnId + ALREADY_HAPPENED;
        }
        deleteOldTransactions(new Date(System.currentTimeMillis() - FIVE_MINUTES_IN_MS));

        return retstring;
    }

    int deleteOldTransactions(Date thresholdDate) {

        int deleted = 0;
        ArrayList<String> deleteList = new ArrayList<String>();

        for (Map.Entry<String, UserRecentTransactions> entry : userRecentTransactions.entrySet()) {
            if (entry.getValue().txnTime.before(thresholdDate)) {
                deleteList.add(entry.getKey());

            }
        }

        for (String key : deleteList) {
            userRecentTransactions.remove(key);
            deleted++;
        }

        return deleted;

    }

    public byte reportQuotaUsage(int unitsUsed, int unitsWanted, long inputSessionId, String txnId) {

        byte statusCode = ReferenceData.STATUS_OK;
        String decision = "none";

        // Sanity Check: Has this transaction already happened?
        if (isTransactionNew(txnId)) {

            int amountSpent = unitsUsed * -1;
            int approvedAmount = 0;


            if (unitsWanted == 0) {
                decision = "Recorded usage of " + amountSpent;
                statusCode = ReferenceData.STATUS_OK;
                UserRecentTransactions newTran = new UserRecentTransactions(userId, txnId, approvedAmount, amountSpent, decision);
                deleteReservation(inputSessionId);
                addUserRecentTransaction(newTran);
                return statusCode;
            }

            deleteReservation(inputSessionId);
            long availableCredit = getAvailableCredit() - unitsUsed;
            long amountApproved = 0;

            if (availableCredit <= 0) {

                decision = decision + "; Negative balance: " + availableCredit;
                statusCode = ReferenceData.STATUS_NO_MONEY;

            } else if (unitsWanted > availableCredit) {

                amountApproved = availableCredit;
                decision = decision + "; Allocated " + availableCredit + " units of " + unitsWanted + " asked for";
                statusCode = ReferenceData.STATUS_SOME_UNITS_ALLOCATED;

            } else {

                amountApproved = unitsWanted;
                decision = decision + "; Allocated " + unitsWanted;
                statusCode = ReferenceData.STATUS_ALL_UNITS_ALLOCATED;

            }

            updateReservation(amountApproved, inputSessionId);


            UserRecentTransactions newTran = new UserRecentTransactions(userId, txnId, approvedAmount, amountSpent, decision);
            addUserRecentTransaction(newTran);

        } else {
            statusCode = ReferenceData.STATUS_TXN_ALREADY_HAPPENED;
        }
        deleteOldTransactions(new Date(System.currentTimeMillis() - FIVE_MINUTES_IN_MS));

        return statusCode;
    }

    private void deleteReservation(long inputSessionId) {
        UserUsageTable uut = getUserUsage(inputSessionId);

        if (uut != null) {
            userUsage.remove(inputSessionId);
        }

    }

    private void updateReservation(long amountApproved, long inputSessionId) {
        UserUsageTable uut = getUserUsage(inputSessionId);

        if (uut != null) {
            uut.setAllocatedAmount(amountApproved);
        } else {
            uut = new UserUsageTable(userId, amountApproved, inputSessionId, new Date());
            userUsage.put(inputSessionId, uut);
        }

    }

    public long getAvailableCredit() {
        long availableCredit = balance;

        for (Map.Entry<Long, UserUsageTable> entry : userUsage.entrySet()) {

            availableCredit -= entry.getValue().allocatedAmount;
        }

        return availableCredit;
    }

    private void reportFinancialEvent(long amountSpent, String txnId, String decision) {
    }

    private boolean isTransactionNew(String txnId) {
        return !userRecentTransactions.containsKey(txnId);
    }

    public int getTxCount() {
        return userRecentTransactions.size();
    }

    public int getUserUsageCount() {
        return userUsage.size();
    }

    public void unLock() {
        userSoftLockSessionId = Long.MIN_VALUE;
        userSoftlockExpiry = null;
    }

    public boolean isLockedBySomeoneElse(long lockId) {

        if (userSoftLockSessionId == Long.MIN_VALUE) {
            return false;
        }

        return userSoftLockSessionId != lockId;
    }

    public void clearSessions() {

        userUsage.clear();
    }

    public long getUsageBalance() {

        long total = 0;
        if (userUsage != null) {
            for (Map.Entry<Long, UserUsageTable> entry : userUsage.entrySet()) {
                total += entry.getValue().allocatedAmount;
            }
        }
        return total;
    }
}
