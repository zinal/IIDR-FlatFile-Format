# IBM InfoSphere Data Replication User Exit - Flat File custom Formatter 

## Installation
The GitHub repository contains the source but also the data formatter user exit in its compiled form, enclosed in a jar file. If you wish, you can choose to build the jar file yourself using Ant, or to manually compile the user exit. If you wish to do so, please refer to the [Compilation](#compilation) section.

Download and unzip the master zip file from GitHub through the following link: [Download Zip](https://github.com/zinal/IIDR-FlatFile-Format/archive/master.zip). The unzipped directory can be placed anywhere on the server that runs the CDC for DataStage engine; we recommend to unzip it under the CDC for DataStage engine's home directory (hereafter referred to as `<cdc_home>`)

## Configuration
In most scenarios you will need to perform a couple of configuration tasks:
- Update the configuration properties in the `FlatFileDataFormat.properties` file
- Add the user exit and its configuration file to the classpath of the CDC engine

In the `FlatFileDataFormat.properties` file you can set the output format of the change records, either CSV or JSON. When specifying `JSON` as the output format, the Record Format in table Flat File properties must be set to *Single Record*.

### Setting the configuration properties
Update the `FlatFileDataFormat.properties` file with your favourite editor.

An example of the Flat File formatting properties can be found below
![User Exit Properties](Documentation/images/FlatFile_Format_properties.png)

### Update the CDC engine's classpath
Assuming you have unzipped the file under the `<cdc_home>` directory, and the directory is called `IIDR-FlatFile-Format-master`, add the following entries to the end of the classpath specified in the `<cdc_home>/conf/system.cp`: <br/>
`:IIDR-FlatFile-Format-master/lib/*:IIDR-FlatFile-Format-master`

In case the CDC engine runs on Windows, use semicolons (;) instead of colons (:).

Example classpath for CDC engine:
 ![Update Classpath](Documentation/images/Update_Classpath.png)
 
The `lib/*` classpath entry is needed to let CDC for DataStage find the jar file; the main directory holds the properties file that is read from within the FlatFile formatting user exit.

Once you have updated the classpath, restart the CDC instance(s) for the changes to take effect.

## Configuring the subscription
Now that the setup tasks have been done and the formatter user exit is available to the CDC engine, you must create a subscription that targets the CDC for DataStage, writing to flat files on the local file system and subsequently map the tables.

Finally, configure the subscription-level user exit. The full name of the user exit is: `com.ibm.idrcdc.userexit.FlatFileDataFormat`

![Subscription User Exit](Documentation/images/FlatFileFormat_UserExit.png)

## Replicating changes
Below you will find an example of three CSV change records: first and insert, then and update and finally a delete of the same record.

```
"2018-01-09 08:36:23.969","730860","I","DB2INST1",,,,,,,,,,,,,"876255","35","SOMMERVILLE NATIONAL LEASING"," ","255 DALESFORD RD."," ","LANSING","MI","A","49979","45000","251"
"2018-01-09 08:38:36.000","730861","U","DB2INST1","876255","35","SOMMERVILLE NATIONAL LEASING"," ","255 DALESFORD RD."," ","LANSING","MI","A","49979","45000","251","876255","35","SOMMERVILLE NATIONAL LEASING"," ","255 DALESFORD RD."," ","LANSING","MI","A","49980","45000","251"
"2018-01-09 08:38:50.000","730862","D","DB2INST1","876255","35","SOMMERVILLE NATIONAL LEASING"," ","255 DALESFORD RD."," ","LANSING","MI","A","49980","45000","251",,,,,,,,,,,,
```

If JSON has been chosen as the output format, the same changes would look as follows:
```
{"AUD_TIMESTAMP":"2018-01-09 08:36:23.969","AUD_CCID":"730860","AUD_ENTTYP":"I","AUD_USER":"DB2INST1","CUSTNO":"876255","BRANCH":"35","NAME1":"SOMMERVILLE NATIONAL LEASING","NAME2":" ","ADDRESS1":"255 DALESFORD RD.","ADDRESS2":" ","CITY":"LANSING","STATE":"MI","STATUS":"A","CRLIMIT":"49979","AMTYTD":"45000","REPNO":"251"}
{"AUD_TIMESTAMP":"2018-01-09 08:38:36.000","AUD_CCID":"730861","AUD_ENTTYP":"U","AUD_USER":"DB2INST1","B_CUSTNO":"876255","B_BRANCH":"35","B_NAME1":"SOMMERVILLE NATIONAL LEASING","B_NAME2":" ","B_ADDRESS1":"255 DALESFORD RD.","B_ADDRESS2":" ","B_CITY":"LANSING","B_STATE":"MI","B_STATUS":"A","B_CRLIMIT":"49979","B_AMTYTD":"45000","B_REPNO":"251","CUSTNO":"876255","BRANCH":"35","NAME1":"SOMMERVILLE NATIONAL LEASING","NAME2":" ","ADDRESS1":"255 DALESFORD RD.","ADDRESS2":" ","CITY":"LANSING","STATE":"MI","STATUS":"A","CRLIMIT":"49980","AMTYTD":"45000","REPNO":"251"}
{"AUD_TIMESTAMP":"2018-01-09 08:38:50.000","AUD_CCID":"730862","AUD_ENTTYP":"D","AUD_USER":"DB2INST1","CUSTNO":"876255","BRANCH":"35","NAME1":"SOMMERVILLE NATIONAL LEASING","NAME2":" ","ADDRESS1":"255 DALESFORD RD.","ADDRESS2":" ","CITY":"LANSING","STATE":"MI","STATUS":"A","CRLIMIT":"49980","AMTYTD":"45000","REPNO":"251"}
```

## Compilation
If you wish to compile the user exit yourself, you can use Maven.

The project depends on the `ts.jar`, which can be extracted from the `lib` folder of CDC agent and installed to local Maven repository on the developer's computer. The example script to install `ts.jar` is `iidr-cdc-setup-maven-libs.sh`, available as part of the project.

Once you have this installed the `ts.jar`, run Maven:
- Ensure the `mvn` executable is in the path;
- Run `mvn clean install`;
- First the sources will be compiled into their respective .class files and finally the class files are packaged into a jar file that is contained in the `lib` directory
