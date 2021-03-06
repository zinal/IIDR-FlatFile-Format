/* _______________________________________________________ {COPYRIGHT-TOP} _____
 * IBM Confidential
 * IBM InfoSphere Data Replication Source Materials
 *
 * 5725-E30 IBM InfoSphere Data Replication
 * 5725-E30 IBM InfoSphere Data Replication for CDC for Netezza Technology
 * 5725-E30 IBM InfoSphere Data Replication for Database Migration
 *
 * 5724-U70 IBM InfoSphere Change Data Delivery
 * 5724-U70 IBM InfoSphere Change Data Delivery for Netezza Technology
 * 5724-Q36 IBM InfoSphere Change Data Delivery for Information Server
 * 5724-Q36 IBM InfoSphere Change Data Delivery for Netezza Technology for Information Server
 *
 * (C) Copyright IBM Corp. 2001, 2012, 2019  All Rights Reserved.
 *
 * The source code for this program is not published or otherwise
 * divested of its trade secrets, irrespective of what has been
 * deposited with the U.S. Copyright Office.
 * ________________________________________________________ {COPYRIGHT-END} _____*/

/****************************************************************************
**
** The following sample of source code ("Sample") is owned by International
** Business Machines Corporation or one of its subsidiaries ("IBM") and is
** copyrighted and licensed, not sold. You may use, copy, modify, and
** distribute the Sample in any form without payment to IBM.
**
** The Sample code is provided to you on an "AS IS" basis, without warranty of
** any kind. IBM HEREBY EXPRESSLY DISCLAIMS ALL WARRANTIES, EITHER EXPRESS OR
** IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
** MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. Some jurisdictions do
** not allow for the exclusion or limitation of implied warranties, so the above
** limitations or exclusions may not apply to you. IBM shall not be liable for
** any damages you suffer as a result of using, copying, modifying or
** distributing the Sample, even if IBM has been advised of the possibility of
** such damages.
*****************************************************************************/

package com.ibm.idrcdc.userexit;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

import com.datamirror.ts.jrncontrol.JournalControlFieldRegistry;
import com.datamirror.ts.target.publication.UserExitJournalHeader;
import com.datamirror.ts.target.publication.userexit.*;
import com.datamirror.ts.target.publication.userexit.datastage.DataStageDataFormatIF;
import com.datamirror.ts.util.trace.Trace;

/**
 * Format the data suitable for the DataStage sequential file reader and column
 * importer stages.
 * 
 * @author Alex Lavrov alexl@il.ibm.com
 * @author Frank Ketelaars fketelaars@nl.ibm.com
 * @author Maksim Zinal mzinal@ru.ibm.com
 */
public class FlatFileDataFormat implements DataStageDataFormatIF {
    
    public static final String VERSION = 
            "FlatFileDataFormat 3.4-MVZ 2020-07-08";

    public final char SUB_RLA_STANDARD = 'Y';
    public final char SUB_RLA_AUDIT = 'A';
    public final char SUB_RLA_AUDIT_B4 = 'B';
    public final char SUB_RLA_INS_UPD = 'I';
    public final char SUB_RLA_DEL_NONE = 'D';
    public final char SUB_RLA_NONE = 'N';
    public final char SUB_RLA_NON_UPD = 'U';

    private final static Charset UTF8_CS = StandardCharsets.UTF_8;

    public static final int BYTE_BUFFER_AUTO_INCREMENT_SIZE = 10000;
    public static final int BYTE_BUFFER_AUTO_INCREMENT_BREATHING_SPACE = 1000;
    public static final int BYTE_BUFFER_SPACE_FOR_FIELD_SEPARATORS = 100;
    private static final int NUM_TRAILING_COLUMNS = 
            JournalControlFieldRegistry.getNumberOfJournalControlFields();

    private SimpleDateFormat outTimestampFormat;
    private final SimpleDateFormat outDateOnlyFormat 
            = new SimpleDateFormat("yyyy-MM-dd");
    private final SimpleDateFormat outTimeOnlyFormat 
            = new SimpleDateFormat("HH:mm:ss");

    private int clobTruncationPoint;
    private int blobTruncationPoint;
    
    private final String simpleName;

    private final String FIXED_QUOTE;
    private final String FIXED_QUOTE_COLON_QUOTE;
    private final String FIXED_COMMA;
    private final String FIXED_LEFT_CURLY;
    private final String FIXED_RIGHT_CURLY;
    private final byte[] COMMA_AS_BYTE_ARRAY;
    private final byte[] QUOTE_AS_BYTE_ARRAY;
    private final byte[] COMMA_QUOTE_AS_BYTE_ARRAY;
    private final String QUOTE_COMMA_QUOTE;
    private final byte[] QUOTE_COMMA_QUOTE_AS_BYTE_ARRAY;
    private final byte[] QUOTE_COMMA_AS_BYTE_ARRAY;
    private final String ZERO_AS_STRING = "0";
    private final String ONE_AS_STRING = "1";

    private static final String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd hh:mm:ss";
    private static final String DEFAULT_COLUMN_SEPARATOR = ",";
    private static final String DEFAULT_COLUMN_DELIMITER = "\"";
    private static final String DEFAULT_NEW_LINE = "\n";
    private static final String DEFAULT_ESCAPE_CHARACTER = "\\";

    private String lineOutputFormat = "CSV";
    private boolean csvOutput = true;
    private boolean overrideJournalControlTimestampFormat = false;
    private String journalControlTimestampFormat = DEFAULT_DATETIME_FORMAT;
    private boolean overrideTimestampColumnFormat = false;
    private String timestampColumnFormat = DEFAULT_DATETIME_FORMAT;
    private String newLine = DEFAULT_NEW_LINE;
    private String columnSeparator = DEFAULT_COLUMN_SEPARATOR;
    private String columnDelimiter = DEFAULT_COLUMN_DELIMITER;
    private boolean stripControlCharacters = true;
    private boolean escapeControlCharacters = false;
    private String escapeCharacter = DEFAULT_ESCAPE_CHARACTER;
    private boolean stripTrailingSpaces = false;
    private String prefixNewLine = "";
    
    private final String escapedNewLine;
    private final String escapedColSep;
    private final String escapedColDel;

    // are we processing the after image?
    private boolean afterImage = false;
    // make the current operation type available in all callbacks
    private int currentOpType;

    // main output buffer
    private ByteBuffer outBuffer 
            = ByteBuffer.allocate(BYTE_BUFFER_AUTO_INCREMENT_SIZE);
    // special output buffers for all-NULL and for all-NULL-newline cases
    private ByteBuffer csvNullImage = null;
    private ByteBuffer csvNullImageNL = null;

    /**
     * Get string in the form of UTF-8 bytes buffer
     * @param inString
     * @return 
     */
    public static byte[] toByteArray(String inString) {
        return inString.getBytes(UTF8_CS);
    }

    /**
     * Add string to a byte buffer, in UTF-8 encoding
     * @param buf
     * @param inString
     * @return
     */
    public static ByteBuffer appendString(ByteBuffer buf, String inString) {
        ByteBuffer retVal;
        byte[] asBytes = inString.getBytes(UTF8_CS);
            
        if (buf.capacity() < buf.position() 
                + asBytes.length + BYTE_BUFFER_SPACE_FOR_FIELD_SEPARATORS) {
            int increment = BYTE_BUFFER_AUTO_INCREMENT_SIZE;
            if (increment < asBytes.length) {
                increment = asBytes.length + BYTE_BUFFER_AUTO_INCREMENT_BREATHING_SPACE;
            }
            retVal = ByteBuffer.allocate(buf.capacity() + increment);
            buf.flip();
            retVal.put(buf);
        } else {
            retVal = buf;
        }

        retVal.put(asBytes);
        return retVal;
    }

    /**
     * Add bytes to a byte buffer
     * @param buf
     * @param asBytes
     * @return 
     */
    public static ByteBuffer appendBytes(ByteBuffer buf, byte[] asBytes) {
        ByteBuffer retVal;

        if (buf.capacity() < buf.position() + asBytes.length 
                + BYTE_BUFFER_SPACE_FOR_FIELD_SEPARATORS) {
            int increment = 10000;
            if (increment < asBytes.length) {
                increment = asBytes.length + 1000;
            }
            retVal = ByteBuffer.allocate(buf.capacity() + increment);
            buf.flip();
            retVal.put(buf);
        } else {
            retVal = buf;
        }

        retVal.put(asBytes);
        return retVal;
    }
    
    public static String escape(String input, String esc) {
        if (input==null || input.length()==0)
            return input;
        if (esc==null || esc.length()==0)
            return input;
        final StringBuilder sb = new StringBuilder();
        for (char c : input.toCharArray()) {
            sb.append(esc).append(c);
        }
        return sb.toString();
    }
    
    public static String printable(String input) {
        if (input==null)
            return "<null>";
        final StringBuilder sb = new StringBuilder();
        for (char c : input.toCharArray()) {
            if (Character.isISOControl(c)) {
                sb.append("\\u{").append(Integer.toHexString((int)c)).append('}');
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    public FlatFileDataFormat() throws UserExitException {
        
        this.simpleName = this.getClass().getSimpleName();
        Trace.traceAlways(VERSION);

        loadConfigurationProperties();

        FIXED_QUOTE = "\"";
        FIXED_QUOTE_COLON_QUOTE = FIXED_QUOTE + ":" + FIXED_QUOTE;
        FIXED_COMMA = ",";
        FIXED_LEFT_CURLY = "{";
        FIXED_RIGHT_CURLY = "}";
        COMMA_AS_BYTE_ARRAY = toByteArray(columnSeparator);
        QUOTE_AS_BYTE_ARRAY = toByteArray(columnDelimiter);
        COMMA_QUOTE_AS_BYTE_ARRAY = toByteArray(columnSeparator + columnDelimiter);
        QUOTE_COMMA_QUOTE = columnDelimiter + columnSeparator + columnDelimiter;
        QUOTE_COMMA_QUOTE_AS_BYTE_ARRAY = toByteArray(QUOTE_COMMA_QUOTE);
        QUOTE_COMMA_AS_BYTE_ARRAY = toByteArray(columnDelimiter + columnSeparator);
        
        this.escapedNewLine = escape(newLine, escapeCharacter);
        this.escapedColDel = escape(columnDelimiter, escapeCharacter);
        this.escapedColSep = escape(columnSeparator, escapeCharacter);
        
        traceConfigReport();
    }

    /**
     * Load the configuration from the properties file found in the classpath.
     * Standard name is FlatFileDataFormat.properties
     * @throws UserExitException 
     */
    private void loadConfigurationProperties() throws UserExitException {
        final String propertiesFile = simpleName + ".properties";

        try {
            URL fileURL = this.getClass().getClassLoader().getResource(propertiesFile);
            if (fileURL==null)
                throw new UserExitException("Missing properties " + propertiesFile);
            Trace.traceAlways("Loading properties for data formatter " 
                    + this.getClass().getName() + " from " + fileURL);
            Properties prop = new Properties();
            try (InputStream configFileStream = this.getClass()
                    .getClassLoader().getResourceAsStream(propertiesFile)) {
                if (configFileStream==null)
                    throw new UserExitException("Cannot read properties file " + fileURL);
                prop.load(configFileStream);
            }
            
            lineOutputFormat = getProperty(prop, "lineOutputFormat", "CSV");
            if (lineOutputFormat.equalsIgnoreCase("CSV")) {
                csvOutput = true;
            } else if (lineOutputFormat.equals("JSON")) {
                csvOutput = false;
            } else {
                throw new UserExitException("Invalid value [" 
                        + lineOutputFormat + "] for property lineOutputFormat");
            }

            overrideJournalControlTimestampFormat = 
                    getProperty(prop, "overrideJournalControlTimestampFormat", false);
            journalControlTimestampFormat = 
                    getProperty(prop, "journalControlTimestampFormat", DEFAULT_DATETIME_FORMAT);
            overrideTimestampColumnFormat = 
                    getProperty(prop, "overrideTimestampColumnFormat", false);
            timestampColumnFormat = 
                    getProperty(prop, "timestampColumnFormat", DEFAULT_DATETIME_FORMAT);
            columnSeparator = 
                    getProperty(prop, "columnSeparator", DEFAULT_COLUMN_SEPARATOR);
            columnDelimiter = 
                    getProperty(prop, "columnDelimiter", DEFAULT_COLUMN_DELIMITER);
            newLine = getProperty(prop, "newLine", DEFAULT_NEW_LINE);
            prefixNewLine = getProperty(prop, "prefixNewLine", "");
            stripControlCharacters = 
                    getProperty(prop, "stripControlCharacters", true);
            escapeControlCharacters = 
                    getProperty(prop, "escapeControlCharacters", false);
            escapeCharacter = 
                    getProperty(prop, "escapeCharacter", DEFAULT_ESCAPE_CHARACTER);
            stripTrailingSpaces = 
                    getProperty(prop, "stripTrailingSpaces", false);

            // Set the default format for timestamps
            outTimestampFormat = new SimpleDateFormat(timestampColumnFormat);
        } catch(IllegalArgumentException | IOException ex) {
            Trace.traceAlways(ex);
            throw new UserExitException(ex);
        }
    }
    
    private void traceConfigReport() {
        // Trace the configuration settings for analysis
        final StringBuilder report = new StringBuilder();
        report.append(simpleName).append(" configuration: ");
        report.append("OF=").append(lineOutputFormat);
        report.append(", ").append("COLSEP=[")
                .append(printable(columnSeparator)).append(']');
        report.append(", ").append("COLDEL=[")
                .append(printable(columnDelimiter)).append(']');
        report.append(", ").append("NL=[")
                .append(printable(newLine)).append(']');
        report.append(", ").append("PNL=[")
                .append(printable(prefixNewLine)).append(']');
        report.append(", ").append("JCF.TS=")
                .append(overrideJournalControlTimestampFormat ? 
                        journalControlTimestampFormat :
                        "<default>");
        report.append(", ").append("ANY.TS=")
                .append(overrideTimestampColumnFormat ?
                        timestampColumnFormat :
                        "<default>");
        report.append(", ").append("CTL.CHARS=");
        if (stripControlCharacters) {
            report.append("strip");
        } else if (escapeControlCharacters) {
            report.append("escape[").append(printable(escapeCharacter)).append(']');
        } else {
            report.append("keep");
        }
        report.append(", ").append("RTRIM=").append(stripTrailingSpaces);
        
        Trace.traceAlways(report.toString());
    }

    /**
     * Get property string value
     * @param properties
     * @param property
     * @param defaultValue
     * @return 
     */
    private static String getProperty(Properties properties, String property, 
            String defaultValue) {
        String value = defaultValue;
        try {
            value = properties.getProperty(property, defaultValue);
        } catch (Exception e) {
            Trace.traceAlways("Error obtaining property " 
                    + property + ", using default value " + value);
        }
        return value;
    }

    /**
     * Get property boolean value
     * @param properties
     * @param property
     * @param defaultValue
     * @return 
     */
    private static boolean getProperty(Properties properties, 
            String property, boolean defaultValue) {
        boolean value = defaultValue;
        try {
            value = Boolean.parseBoolean(
                    properties.getProperty(property, Boolean.toString(defaultValue)));
        } catch (Exception e) {
            Trace.traceAlways(
                    "Error obtaining or converting property " 
                            + property + " to boolean, using default value " + value);
        }
        return value;
    }

    /**
     * Remember the truncation points.
     * @param maxClobLengthInChars
     * @param maxBlobLengthInBytes
     */
    @Override
    public void setLobTruncationPoint(int maxClobLengthInChars, int maxBlobLengthInBytes) {
        clobTruncationPoint = maxClobLengthInChars;
        blobTruncationPoint = maxBlobLengthInBytes;
    }

    /**
     * Create a string containing the data images for the row.
     * @param image
     * @return 
     * @throws DataTypeConversionException
     */
    @Override
    public ByteBuffer formatDataImage(DataRecordIF image) throws DataTypeConversionException {
        outBuffer.position(0);

        // process the data image
        if (image != null)
            handleImage(image);

        // Make sure the next data image is treated as the after image
        afterImage = true;

        return outBuffer;
    }
    
    private void handleImage(DataRecordIF image) throws DataTypeConversionException {
        boolean needToCloseQuote = false;
        for (int i = 1; i <= image.getColumnCount() - NUM_TRAILING_COLUMNS; i++) {
            // Determine column name 
            // (prefix with B_ if before image of update)
            String colName = image.getColumnName(i);
            if (!afterImage && currentOpType == DataStageDataFormatIF.FULL_UPDATE_RECORD) {
                colName = "B_" + colName;
            }
            // Get the value
            Object colObj = image.getObject(i);
            
            // For NULL values, we just leave the field empty
            needToCloseQuote = (colObj != null) ?
                    handleValue(colName, colObj, needToCloseQuote) :
                    handleNull(needToCloseQuote);
        }
        if (csvOutput) {
            // CSV format closing: closing quote + newline prefix
            if (needToCloseQuote)
                outBuffer.put(QUOTE_AS_BYTE_ARRAY);
            if (afterImage && prefixNewLine.length() > 0)
                outBuffer = appendString(outBuffer, prefixNewLine);
        } else {
            // JSON format closing: right curly bracket
            if (afterImage)
                 outBuffer = appendString(outBuffer, FIXED_RIGHT_CURLY);
        }
    }
    
    private boolean handleNull(boolean needToCloseQuote)
            throws DataTypeConversionException {
        if (csvOutput) {
            if (needToCloseQuote) {
                outBuffer.put(QUOTE_COMMA_AS_BYTE_ARRAY);
                needToCloseQuote = false;
            } else {
                outBuffer.put(COMMA_AS_BYTE_ARRAY);
            }
        }
        return needToCloseQuote;
    }
    
    private boolean handleValue(String colName, Object colObj, 
            boolean needToCloseQuote) throws DataTypeConversionException {
        if (csvOutput) {
            // For performance, we have this wacky logic to only do
            // one add of stuff between columns
            if (needToCloseQuote) {
                outBuffer.put(QUOTE_COMMA_QUOTE_AS_BYTE_ARRAY);
            } else {
                outBuffer.put(COMMA_QUOTE_AS_BYTE_ARRAY);
            }
            needToCloseQuote = true;
        } else {
            outBuffer = appendString(outBuffer, FIXED_COMMA);
        }

        if (colObj instanceof Time) {
            addElement(colName, outTimeOnlyFormat.format((Time) colObj));
        } else if (colObj instanceof Timestamp) {
            final String outString;
            if (!overrideTimestampColumnFormat) {
                outString = ((Timestamp) colObj).toString();
            } else {
                outString = outTimestampFormat.format((Timestamp) colObj);
            }
            addElement(colName, outString);
        } else if (colObj instanceof Date) {
            // This must be checked after Time, Timestamp, as such
            // objects are also Date objects
            addElement(colName, outDateOnlyFormat.format((Date) colObj));
        } else if (colObj instanceof byte[]) {
            byte[] val = (byte[]) colObj;
            if (val.length > blobTruncationPoint) {
                byte[] truncVal = new byte[blobTruncationPoint];
                ByteBuffer truncBuffer = ByteBuffer.wrap(truncVal);
                truncBuffer.put(val, 0, blobTruncationPoint);
                val = truncVal;
            }
            if (!csvOutput) {
                outBuffer = appendString(outBuffer,
                        FIXED_QUOTE + colName + FIXED_QUOTE_COLON_QUOTE);
            }
            outBuffer = appendBytes(outBuffer, val);
            if (!csvOutput) {
                outBuffer = appendString(outBuffer, FIXED_QUOTE);
            }
        } else if (colObj instanceof Boolean) {
            final String outString;
            if ((Boolean) colObj) {
                outString = ONE_AS_STRING;
            } else {
                outString = ZERO_AS_STRING;
            }
            addElement(colName, outString);
        } else if (colObj instanceof String) {
            String val = ((String) colObj);
            if (val.length() > clobTruncationPoint) {
                val = val.substring(0, clobTruncationPoint);
            }

            // Strip trailing spaces from the string
            if (stripTrailingSpaces) {
                int sz = val.length();
                if (sz > 0 && Character.isWhitespace(val.charAt(sz-1)))
                    val = val.replaceAll("\\s+$", "");
            }

            // Strip control characters from the string
            if (stripControlCharacters) {
                // Just strip all control characters
                if (!columnSeparator.isEmpty()) {
                    val = val.replace(columnSeparator, "");
                }
                if (!columnDelimiter.isEmpty()) {
                    val = val.replace(columnDelimiter, "");
                }
                if (!newLine.isEmpty()) {
                    val = val.replace(newLine, "");
                }
            } else if (escapeControlCharacters) {
                // Escape control characters in the string
                val = val.replace(escapeCharacter, escapeCharacter + escapeCharacter);
                if (!columnSeparator.isEmpty()) {
                    val = val.replace(columnSeparator, escapedColSep);
                }
                if (!columnDelimiter.isEmpty()) {
                    val = val.replace(columnDelimiter, escapedColDel);
                }
                if (!newLine.isEmpty()) {
                    val = val.replace(newLine, escapedNewLine);
                }
            }
            addElement(colName, val);
        } else if (colObj instanceof BigDecimal) {
            addElement(colName, ((BigDecimal) colObj).toString());
        } else {
            if (colObj==null)
                addElement(colName, "");
            else
                addElement(colName, colObj.toString());
        }
        return needToCloseQuote;
    }

    /**
     * Add element to the output buffer, depending if it's CSV of JSON
     */
    private void addElement(String colName, String data)
            throws DataTypeConversionException {
        if (csvOutput) {
            outBuffer = appendString(outBuffer, data);
        } else {
            outBuffer = appendString(outBuffer, getJsonElement(colName, data));
        }
    }

    /**
     * Return a ByteBuffer containing the appropriate null values for the row.
     * @param image
     * @return 
     * @throws DataTypeConversionException
     */
    @Override
    public ByteBuffer formatNullImage(DataRecordIF image) 
            throws DataTypeConversionException {
        final ByteBuffer returnByteBuffer;
        if (csvOutput) {
            // Is it a "regular" NULL image, or NULL-newline image?
            boolean regularNullImage = true;
            if (afterImage) {
                // We are processing the "after" NULL image.
                // Do we need the "newline prefix" in it?
                if (prefixNewLine.length() > 0)
                    regularNullImage = false;
            }
            // There is a separate data formatter for each table, so a null
            // image is the same for each row, so just need to create it once.
            // Grab the cached image.
            ByteBuffer bb = regularNullImage ? csvNullImage : csvNullImageNL;
            if (bb==null) {
                final StringBuilder out = new StringBuilder();
                if (image != null) {
                    for (int i = 1; i <= image.getColumnCount() - NUM_TRAILING_COLUMNS; i++) {
                        out.append(columnSeparator);
                    }
                }
                if (! regularNullImage) {
                    // Null image appended with the "newline prefix".
                    out.append(prefixNewLine);
                }
                bb = ByteBuffer.wrap(toByteArray(out.toString()));
                bb.position(bb.capacity());
                // Set the cached image
                if (regularNullImage)
                    csvNullImage = bb;
                else
                    csvNullImageNL = bb;
            }
            returnByteBuffer = bb;
        } else {
            String outString = "";
            if (afterImage) {
                outString = outString + FIXED_RIGHT_CURLY;
            }
            returnByteBuffer = ByteBuffer.wrap(toByteArray(outString));
            returnByteBuffer.position(returnByteBuffer.capacity());
        }

        // Make sure the next data image is treated as the after image
        afterImage = true;

        return returnByteBuffer;
    }

    /**
     * Return a ByteBuffer containing the journal control field values that are
     * of interest.
     *
     * @param event
     * @param opType
     * @return 
     * @throws DataTypeConversionException 
     */
    @Override
    public ByteBuffer formatJournalControlFields(ReplicationEventIF event, int opType)
            throws DataTypeConversionException {
        // Journal control fields precede the before image, 
        // which is followed by after image. Among other things, re-setting
        // this flag is to make sure that the JSON record 
        // is only closed after the full image has been processed.
        afterImage = false;

        // Determine the character to use to indicate the operation type
        char opChar = ' ';
        currentOpType = opType;
        switch (opType) {
            case DataStageDataFormatIF.INSERT_RECORD:
                opChar = SUB_RLA_INS_UPD;
                break;
            case DataStageDataFormatIF.DELETE_RECORD:
                opChar = SUB_RLA_DEL_NONE;
                break;
            case DataStageDataFormatIF.FULL_UPDATE_RECORD:
                opChar = SUB_RLA_NON_UPD;
                break;
            case DataStageDataFormatIF.BEFORE_UPDATE_RECORD:
                opChar = SUB_RLA_AUDIT_B4;
                break;
            case DataStageDataFormatIF.AFTER_UPDATE_RECORD:
                opChar = SUB_RLA_AUDIT;
                break;

        }
        String opString = "" + opChar;
        
        final String timestampString;
        final String commitIDString;
        final String userName;

        if (event==null) {
            // null event is for mock/performance test mode only
            timestampString = "2020-01-01 12:00:00";
            commitIDString = "123456";
            userName = "username";
        } else {
            // normal processing
            UserExitJournalHeader header = (UserExitJournalHeader) 
                    event.getJournalHeader();
            commitIDString = header.getCommitID();
            userName = header.getUserName();
            // Retrieve and format the journal control timestamp
            if (overrideJournalControlTimestampFormat) {
                timestampString = header.getDSOutputTimestamp().toString();
            } else {
                timestampString = new SimpleDateFormat(journalControlTimestampFormat)
                        .format(header.getDSOutputTimestamp());
            }

        }

        final String outString;
        if (csvOutput) {
            outString = columnDelimiter + timestampString 
                    + QUOTE_COMMA_QUOTE + commitIDString + QUOTE_COMMA_QUOTE
                    + opChar + QUOTE_COMMA_QUOTE + userName
                    + columnDelimiter;
        } else {
            // Compose the JSON string with audit columns, ending with ,"
            outString = FIXED_LEFT_CURLY 
                    + getJsonElement("AUD_TIMESTAMP", timestampString) + FIXED_COMMA
                    + getJsonElement("AUD_CCID", commitIDString) + FIXED_COMMA 
                    + getJsonElement("AUD_ENTTYP", opString) + FIXED_COMMA 
                    + getJsonElement("AUD_USER", userName);
        }

        ByteBuffer retVal = ByteBuffer.allocate(outString.length());
        retVal = appendString(retVal, outString);
        return retVal;
    }

    private String getJsonElement(String elementName, String elementValue) {
        return FIXED_QUOTE + elementName + FIXED_QUOTE_COLON_QUOTE
                + elementValue + FIXED_QUOTE;
    }

    /**
     * Indicate whether this table is being delivered to DataStage using flat
     * files or by direct connect. As CDC for DataStage only supports flat
     * files, the destination type is no longer relevant.
     * @param destination
     */
    @Override
    public void setDestinationType(int destination) {
    }

    @Override
    public void formatChangedRowFields(UserExitJournalHeader journalHeader, 
            DataRecordIF rowDataImage,
            Map<String, Object> changeRecord, int opType) 
        throws DataTypeConversionException {

    }

}
