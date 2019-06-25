package com.ibm.idrcdc.userexit;

import com.datamirror.ts.target.publication.UserExitJournalHeader;
import com.datamirror.ts.target.publication.userexit.DataRecordIF;
import com.datamirror.ts.target.publication.userexit.DataTypeConversionException;
import com.datamirror.ts.target.publication.userexit.EventServerIF;
import com.datamirror.ts.target.publication.userexit.InvalidSetDataException;
import com.datamirror.ts.target.publication.userexit.JournalHeaderIF;
import com.datamirror.ts.target.publication.userexit.ReplicationEventIF;
import com.datamirror.ts.target.publication.userexit.Timestamp12;
import com.ibm.idrcdc.userexit.FlatFileDataFormat;
import com.ibm.idrcdc.userexit.FlatFileDataFormat;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Random;

/**
 *
 * @author zinal
 */
public class TestPerf {
    
    public static void main(String[] args) {
        try {
            Thread.sleep(20000L);
            long tv0 = System.currentTimeMillis();
            final Record[] records = Record.makeRecords(100000);
            long tv1 = System.currentTimeMillis();
            System.out.println("Generated data in " + (tv1-tv0) + " millis");
            final FlatFileDataFormat ffdf = new FlatFileDataFormat();
            long tv2 = System.currentTimeMillis();
            System.out.println("Init data in " + (tv2-tv1) + " millis");
            for (int j=0; j<10000; ++j) {
                for ( int i=0; i<records.length; ++i ) {
                    ffdf.formatDataImage(records[i]);
                }
            }
            long tv3 = System.currentTimeMillis();
            System.out.println("Formatted data in " + (tv3-tv2) + " millis");
        } catch(Exception ex) {
            ex.printStackTrace(System.out);
            System.exit(1);
        }
    }
    
    public static class Record implements DataRecordIF {
        
        private static final Random random = new Random();
        private final Object[] data;
        
        public Record() {
            data = new Object[10];
            final long tv = System.currentTimeMillis();
            data[0] = new java.sql.Time(tv - (1000L) * random.nextInt(1000));
            data[1] = new java.sql.Timestamp(tv 
                    - (24L*3600L*1000L * random.nextInt(3650))
                    - (1000L) * random.nextInt(1000));
            data[2] = new java.sql.Date(tv 
                    - (24L*3600L*1000L * random.nextInt(3650)));
            final byte[] bytes = new byte[300];
            random.nextBytes(bytes);
            data[3] = bytes;
            data[4] = random.nextBoolean();
            data[5] = randomString(random.nextInt(50)+20);
            data[6] = randomString(random.nextInt(10)+10);
            data[7] = random.nextInt();
            data[8] = new BigDecimal(random.nextDouble() * 10000.0);
            data[9] = 10000.0 * random.nextDouble();
        }
        
        public static Record[] makeRecords(int count) {
            final Record[] r = new Record[count];
            for (int i=0; i<count; ++i)
                r[i] = new Record();
            return r;
        }
        
        public static final String CHARS = "0123456789"
                + "abcdefghijklmnopqrstuvwxyz"
                + "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                + "абвгдеёжзийклмнопрстуфхцчшщъыьэюя"
                + "АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ"
                + "+-*/$#@!_,;'\" ";
        
        public static String randomString(int len) {
            final StringBuilder sb = new StringBuilder();
            while (len-- > 0) {
                sb.append(CHARS.charAt(random.nextInt(CHARS.length())));
            }
            return sb.toString();
        }

        @Override
        public int getColumnCount() {
            return data.length;
        }

        @Override
        public String getColumnName(int i) {
            return "C" + i;
        }

        @Override
        public String getColumnNativeType(int i) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public String getColumnNativeType(String string) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public int getColumnNetworkDataType(int i) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public int getColumnNetworkDataType(String string) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public int getColumnSize(int i) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public int getColumnSize(String string) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public int getColumnLength(int i) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public int getColumnLength(String string) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public int getColumnScale(int i) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public int getColumnScale(String string) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public boolean isNullable(int i) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public boolean isNullable(String string) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public boolean isNull(int i) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public boolean isReadOnly(int i) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public boolean isDataAvailable(int i) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public boolean isKey(int i) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public String getString(int i) throws DataTypeConversionException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public boolean getBoolean(int i) throws DataTypeConversionException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public byte getByte(int i) throws DataTypeConversionException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public short getShort(int i) throws DataTypeConversionException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public int getInt(int i) throws DataTypeConversionException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public long getLong(int i) throws DataTypeConversionException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public float getFloat(int i) throws DataTypeConversionException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public double getDouble(int i) throws DataTypeConversionException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public BigDecimal getBigDecimal(int i) throws DataTypeConversionException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public byte[] getBytes(int i) throws DataTypeConversionException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Date getDate(int i) throws DataTypeConversionException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Time getTime(int i) throws DataTypeConversionException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Timestamp getTimestamp(int i) throws DataTypeConversionException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Timestamp12 getTimestamp12(int i) throws DataTypeConversionException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Object getObject(int i) throws DataTypeConversionException {
            return data[i];
        }

        @Override
        public boolean isNull(String string) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public boolean isReadOnly(String string) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public boolean isDataAvailable(String string) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public boolean isKey(String string) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public String getString(String string) throws DataTypeConversionException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public boolean getBoolean(String string) throws DataTypeConversionException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public byte getByte(String string) throws DataTypeConversionException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public short getShort(String string) throws DataTypeConversionException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public int getInt(String string) throws DataTypeConversionException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public long getLong(String string) throws DataTypeConversionException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public float getFloat(String string) throws DataTypeConversionException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public double getDouble(String string) throws DataTypeConversionException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public BigDecimal getBigDecimal(String string) throws DataTypeConversionException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public byte[] getBytes(String string) throws DataTypeConversionException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Date getDate(String string) throws DataTypeConversionException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Time getTime(String string) throws DataTypeConversionException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Timestamp getTimestamp(String string) throws DataTypeConversionException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Timestamp12 getTimestamp12(String string) throws DataTypeConversionException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Object getObject(String string) throws DataTypeConversionException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void setNull(int i) throws DataTypeConversionException, InvalidSetDataException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void setString(int i, String string) throws DataTypeConversionException, InvalidSetDataException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void setBoolean(int i, boolean bln) throws DataTypeConversionException, InvalidSetDataException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void setByte(int i, byte b) throws DataTypeConversionException, InvalidSetDataException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void setShort(int i, short s) throws DataTypeConversionException, InvalidSetDataException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void setInt(int i, int i1) throws DataTypeConversionException, InvalidSetDataException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void setLong(int i, long l) throws DataTypeConversionException, InvalidSetDataException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void setFloat(int i, float f) throws DataTypeConversionException, InvalidSetDataException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void setDouble(int i, double d) throws DataTypeConversionException, InvalidSetDataException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void setBigDecimal(int i, BigDecimal bd) throws DataTypeConversionException, InvalidSetDataException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void setBytes(int i, byte[] bytes) throws DataTypeConversionException, InvalidSetDataException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void setDate(int i, Date date) throws DataTypeConversionException, InvalidSetDataException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void setTime(int i, Time time) throws DataTypeConversionException, InvalidSetDataException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void setTimestamp(int i, Timestamp tmstmp) throws DataTypeConversionException, InvalidSetDataException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void setTimestamp12(int i, Timestamp12 tmstmp) throws DataTypeConversionException, InvalidSetDataException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void setNull(String string) throws DataTypeConversionException, InvalidSetDataException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void setString(String string, String string1) throws DataTypeConversionException, InvalidSetDataException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void setBoolean(String string, boolean bln) throws DataTypeConversionException, InvalidSetDataException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void setByte(String string, byte b) throws DataTypeConversionException, InvalidSetDataException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void setShort(String string, short s) throws DataTypeConversionException, InvalidSetDataException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void setInt(String string, int i) throws DataTypeConversionException, InvalidSetDataException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void setLong(String string, long l) throws DataTypeConversionException, InvalidSetDataException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void setFloat(String string, float f) throws DataTypeConversionException, InvalidSetDataException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void setDouble(String string, double d) throws DataTypeConversionException, InvalidSetDataException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void setBigDecimal(String string, BigDecimal bd) throws DataTypeConversionException, InvalidSetDataException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void setBytes(String string, byte[] bytes) throws DataTypeConversionException, InvalidSetDataException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void setDate(String string, Date date) throws DataTypeConversionException, InvalidSetDataException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void setTime(String string, Time time) throws DataTypeConversionException, InvalidSetDataException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void setTimestamp(String string, Timestamp tmstmp) throws DataTypeConversionException, InvalidSetDataException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void setTimestamp12(String string, Timestamp12 tmstmp) throws DataTypeConversionException, InvalidSetDataException {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        
    } // class Record
    
    public static class Event implements ReplicationEventIF {

        @Override
        public int getEventType() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public String getTableName() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public String getTablePath() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public String getSourceMemberName() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public JournalHeaderIF getBeforeJournalHeader() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public JournalHeaderIF getJournalHeader() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public DataRecordIF getData() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public DataRecordIF getSourceData() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public DataRecordIF getBeforeData() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public DataRecordIF getSourceBeforeData() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public DataRecordIF getCurrentData() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void setEventServerOperation(int i) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void setEventServerQueue(String string, String string1) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Connection getSharedConnection() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public EventServerIF getEventServer() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Object getUserExitSubscriptionContext() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void setUserExitSubscriptionContext(Object o) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void logEvent(String string) {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        
    } // class Event
    
    public static class Header extends UserExitJournalHeader {
        
        public Header() {
            super(null, true);
        }
        
    } // class Header
    
}
