/*
 * Copyright (C) 2025 David Rolfe
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */

package ie.rolfe.mongodbcharglt.documents;

import org.bson.Document;

import java.util.Date;

/**
 * create table user_recent_transactions
 * --MIGRATE TO TARGET user_transactions
 * (userid bigint not null
 * ,user_txn_id varchar(128) NOT NULL
 * ,txn_time TIMESTAMP DEFAULT NOW  not null
 * ,sessionid bigint
 * ,approved_amount bigint
 * ,spent_amount bigint
 * ,purpose  varchar(128)
 * ,primary key (userid, user_txn_id))
 * USING TTL 3600 SECONDS ON COLUMN txn_time BATCH_SIZE 200 MAX_FREQUENCY 1;
 */
public class UserRecentTransactions extends AbstractBaseTable {

    public long userId;
    public String userTxnId;
    public Date txnTime;
    public long sessionId;
    public long approvedAmount;
    public long spentAmount;
    public String purpose;


    public UserRecentTransactions(long userId, String userTxnId, Date txnTime, long sessionId, long approvedAmount, long spentAmount, String purpose) {
        this.userId = userId;
        this.userTxnId = userTxnId;
        this.txnTime = txnTime;
        this.sessionId = sessionId;
        this.approvedAmount = approvedAmount;
        this.spentAmount = spentAmount;
        this.purpose = purpose;
    }

    public UserRecentTransactions() {

    }

    public UserRecentTransactions(Document document) {


        userId = getLong(document, "userId");
        userTxnId = document.getString("userTxnId");
        txnTime = getDate(document, "txnTime");
        sessionId = getLong(document, "sessionId");
        sessionId = getLong(document, "sessionId");
        spentAmount = getLong(document, "spentAmount");
        purpose = document.getString("purpose");
    }

    public UserRecentTransactions(long userId, String txnId, int approvedAmount, long spentAmount, String purpose) {
        super();
        this.userId = userId;
        this.userTxnId = txnId;
        this.approvedAmount = approvedAmount;
        this.spentAmount = spentAmount;
        this.purpose = purpose;
        txnTime = new Date();
    }

    @Override
    public String toString() {
        return "UserRecentTransactions{" +
                "userId=" + userId +
                ", userTxnId='" + userTxnId + '\'' +
                ", txnTime=" + txnTime +
                ", sessionId=" + sessionId +
                ", approvedAmount=" + approvedAmount +
                ", spentAmount=" + spentAmount +
                ", purpose='" + purpose + '\'' +
                '}';
    }
}
