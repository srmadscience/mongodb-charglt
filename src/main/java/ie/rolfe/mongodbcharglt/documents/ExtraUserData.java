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

package ie.rolfe.mongodbcharglt.documents;


import org.bson.Document;

/**
 * Class that gets stored as JSON data.
 *
 */
public class ExtraUserData extends AbstractBaseTable {

    public static final String NEW_LOYALTY_NUMBER = "NEW_LOYALTY_NUMBER";

    public String mysteriousHexPayload;

    public String loyaltySchemeName;

    public long loyaltySchemeNumber;

    public ExtraUserData(String mysteriousHexPayload, String loyaltySchemeName, long loyaltySchemeNumber) {
        this.mysteriousHexPayload = mysteriousHexPayload;
        this.loyaltySchemeName = loyaltySchemeName;
        this.loyaltySchemeNumber = loyaltySchemeNumber;
    }

    public ExtraUserData(Document userDataObjectDoc) {

        mysteriousHexPayload =  userDataObjectDoc.getString("mysteriousHexPayload");
        loyaltySchemeName = userDataObjectDoc.getString("loyaltySchemeName");
        loyaltySchemeNumber = getLong(userDataObjectDoc, "loyaltySchemeNumber");


    }

    public ExtraUserData() {}

    @Override
    public String toString() {
        return "ExtraUserData{" +
                "mysteriousHexPayload='" + mysteriousHexPayload + '\'' +
                ", loyaltySchemeName='" + loyaltySchemeName + '\'' +
                ", loyaltySchemeNumber=" + loyaltySchemeNumber +
                '}';
    }
}
