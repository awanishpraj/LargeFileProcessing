package com.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;


class DataBaseUtil {
    private static Logger logger = LoggerFactory.getLogger(DataBaseUtil.class);
    public static void main(String [] str){
        getDBConnection();
    }
    public static Connection getDBConnection() {

            Connection con = null;

            try {
                //Registering the HSQLDB JDBC driver
                Class.forName("org.hsqldb.jdbc.JDBCDriver");
                //Creating the connection with HSQLDB
                con = DriverManager.getConnection("jdbc:hsqldb:file:/C:/Users/Admin/IdeaProjects/LargeFileOperation/src/main/resources/hsqldb/eventdb/logeventdb;hsqldb.lock_file=false", "SA", "");
                if (con!= null){
                    System.out.println("Connection created successfully");

                }else{
                    System.out.println("Problem with creating connection");
                }

            }  catch (Exception e) {
                e.printStackTrace(System.out);
            }

        return con;
    }


     public static int createTable() {

            Connection con = null;
            Statement stmt = null;
            int result = 0;



            String createTblStr = "CREATE TABLE PUBLIC.EVENT_TBL(event_id VARCHAR(25) NOT NULL, duration VARCHAR(25), type VARCHAR(20), host VARCHAR(10))";
            String dropTable = "DROP TABLE \"PUBLIC\".\"EVENT_TBL\"";
            try {
                con = getDBConnection();

                boolean isTableExists = false;
                DatabaseMetaData meta = con.getMetaData();
                ResultSet res = meta.getTables(null, null, "EVENT_TBL",  new String[] {"TABLE"});
                while (res.next()) {
                    System.out.println(
                            " ******************  "+res.getString("TABLE_CAT")
                                    + ", "+res.getString("TABLE_SCHEM")
                                    + ", "+res.getString("TABLE_NAME")
                                    + ", "+res.getString("TABLE_TYPE")
                                    + ", "+res.getString("REMARKS"));

                    if (res.getString("TABLE_NAME").equalsIgnoreCase("event_tbl")){
                        isTableExists = true;
                        logger.info("Event table named {} already exists, so skipping table creation..." , result);
                        break;
                    }
                }

                if (!isTableExists) {
                    stmt = con.createStatement();
                    result = stmt.executeUpdate(createTblStr);
                    logger.info("Event table created : {}" , result);
                }


            }  catch (Exception e) {
                e.printStackTrace(System.out);
            }

            return result;
    }

    public static int insertEventQuery(String[] columArray) {

            Connection con = null;
            PreparedStatement stmt = null;
           // String insertQuerry = "INSERT INTO PUBLIC.event_tbl VALUES (?,?,?,?)";
            String col3 = (columArray[2] != null) ? columArray[2] : "";
            String col4 = (columArray[3] != null) ? columArray[3] : "";
            //INSERT INTO "PUBLIC"."EVENT_TBL"( "EVENT_ID", "DURATION", "TYPE", "HOST" ) VALUES ( 'scsmbstgre', '5', 'APPLICATION LOG', '12345')
            String insertQuerry1 = "INSERT INTO \"PUBLIC\".\"EVENT_TBL\"( \"EVENT_ID\", \"DURATION\", \"TYPE\", \"HOST\" ) VALUES ( \'" + columArray[0] + "\', \'" + columArray[1] + "\', \'"+ col3 + "\', \'" + col4 + "\')";
            int result = 0;
            try {
                con = getDBConnection();
                stmt = con.prepareStatement(insertQuerry1);
//                int index = 1;
//                for (String s : columArray) {
//                    stmt.setString(index++, s);
//                }

                result = stmt.executeUpdate();
                con.commit();
                logger.info("Event details inserted in the table : {}" , result);
            }catch (Exception e) {
                e.printStackTrace(System.out);
            }
            System.out.println(result+" rows effected" );
            System.out.println("Rows inserted successfully");
            return result;
    }
}
