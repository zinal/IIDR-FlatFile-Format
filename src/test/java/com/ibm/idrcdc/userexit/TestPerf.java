package com.ibm.idrcdc.userexit;

import com.datamirror.ts.target.publication.userexit.DataRecordIF;
import com.datamirror.ts.target.publication.userexit.DataTypeConversionException;
import com.datamirror.ts.target.publication.userexit.InvalidSetDataException;
import com.datamirror.ts.target.publication.userexit.Timestamp12;
import com.datamirror.ts.target.publication.userexit.datastage.DataStageDataFormatIF;
import com.ibm.idrcdc.userexit.FlatFileDataFormat;
import java.io.FileOutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Random;

/**
 *
 * @author zinal
 */
public class TestPerf {
    
    private static final boolean IS_WRITING = false;
    
    public static void main(String[] args) {
        try {
            long tv0 = System.currentTimeMillis();
            final int count = 50000;
            final Record[] recordsBefore = Record.makeRecords(count);
            final Record[] recordsAfter = Record.makeRecords(count);
            long tv1 = System.currentTimeMillis();
            System.out.println("Generated data in " + (tv1-tv0) + " millis");
            final FlatFileDataFormat ffdf = new FlatFileDataFormat();
            long tv2 = System.currentTimeMillis();
            System.out.println("Init data in " + (tv2-tv1) + " millis");
            
            try (FileChannel channel = 
                    new FileOutputStream("output.txt").getChannel()) {
                for (int j=0; j<100; ++j) {
                    for (int i=0; i<count; ++i) {
                        formatRecord(ffdf, recordsBefore[i], recordsAfter[i], channel);
                    }
                }
                
            }
            
            long tv3 = System.currentTimeMillis();
            System.out.println("Formatted data in " + (tv3-tv2) + " millis");
        } catch(Exception ex) {
            ex.printStackTrace(System.out);
            System.exit(1);
        }
    }
    
    private static void writeBuffer(ByteBuffer bb, FileChannel channel)
            throws Exception {
        bb.flip();
        if (IS_WRITING) {
            while (bb.hasRemaining())
                channel.write(bb);
        }
        bb.clear();
    }
    
    private static void formatRecord(FlatFileDataFormat ffdf, 
            Record dataBefore, Record dataAfter, 
            FileChannel channel) throws Exception {
        // Journal control formatting
        ByteBuffer bb = ffdf.formatJournalControlFields(null, 
                DataStageDataFormatIF.FULL_UPDATE_RECORD);
        writeBuffer(bb, channel);
        // Before image
        bb = ffdf.formatDataImage(dataBefore);
        writeBuffer(bb, channel);
        // After image
        bb = ffdf.formatDataImage(dataAfter);
        writeBuffer(bb, channel);
        bb = ByteBuffer.wrap(
            "\n".getBytes(Charset.forName("UTF-8"))
        );
        if (IS_WRITING) {
            while (bb.hasRemaining())
                channel.write(bb);
        }
    }
    
    public static class Record implements DataRecordIF {
        
        private static final Random random = new Random();
        private final Object[] data;
        
        public Record() {
            data = new Object[11];
            final long tv = System.currentTimeMillis();
            data[0] = new java.sql.Time(tv - (1000L) * random.nextInt(1000));
            data[1] = data[0];
            data[2] = new java.sql.Timestamp(tv 
                    - (24L*3600L*1000L * random.nextInt(3650))
                    - (1000L) * random.nextInt(1000));
            data[3] = new java.sql.Date(tv 
                    - (24L*3600L*1000L * random.nextInt(3650)));
            final byte[] bytes = new byte[300];
            random.nextBytes(bytes);
            data[4] = bytes;
            data[5] = random.nextBoolean();
            data[6] = randomString(random.nextInt(50)+20);
            data[7] = randomString(random.nextInt(10)+10) + "       ";
            data[8] = random.nextInt();
            data[9] = new BigDecimal(random.nextDouble() * 10000.0);
            data[10] = 10000.0 * random.nextDouble();
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
            // NUM_TRAILING_COLUMNS == 18
            return 18 + data.length - 1;
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
    
}
