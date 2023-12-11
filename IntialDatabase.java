package org.aquant;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.ibatis.jdbc.ScriptRunner;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Reader;
import java.util.List;

public class IntialDatabase {
    public static void driver(HashMap<String, String> configMap) throws Exception {

        Connection con = DriverManager
                .getConnection(configMap.get("db_url"), configMap.get("db_uname"),  configMap.get("db_pword"));
        ScriptRunner sr = new ScriptRunner(con);
        Reader reader = new BufferedReader(new FileReader(configMap.get("initial_sql")));
        sr.runScript(reader);


        Statement stm = con.createStatement();
        CSVParser cl_parser = new CSVParser(new FileReader("code_list.csv"), CSVFormat.EXCEL
                .withDelimiter(configMap.get("source_delimiter").charAt(0))
                .withFirstRecordAsHeader()
                .withIgnoreHeaderCase()
                .withTrim());
        List<CSVRecord> cl_records = cl_parser.getRecords();

        for (int i = 0 ; i < cl_records.size() ; i++) {
            CSVRecord tempRecord = cl_records.get(i);
            String query = configMap.get("query_prefix") + "  tier1.code_list  " + configMap.get("query_select");
            for ( int j = 0 ; j <tempRecord.size() ; j ++){
                query += tempRecord.get(j).replaceAll("'", "''") + configMap.get("query_midfix");
            }

            query = query.substring(0, query.length() - 3) + configMap.get("query_postfix");

            stm.addBatch(query);
        }

        CSVParser il_parser = new CSVParser(new FileReader("index_list.csv"), CSVFormat.EXCEL
                .withDelimiter(configMap.get("source_delimiter").charAt(0))
                .withFirstRecordAsHeader()
                .withIgnoreHeaderCase()
                .withTrim());
        List<CSVRecord> il_records = il_parser.getRecords();

        for (int i = 0 ; i < il_records.size() ; i++) {
            CSVRecord tempRecord = il_records.get(i);
            String query = configMap.get("query_prefix") + "  tier1.index_list  " + configMap.get("query_select");
            for ( int j = 0 ; j <tempRecord.size() ; j ++){
                query += tempRecord.get(j).replaceAll("'", "''") + configMap.get("query_midfix");
            }

            query = query.substring(0, query.length() - 3) + configMap.get("query_postfix");

            stm.addBatch(query);
        }


        stm.executeBatch();

        con.commit();
        stm.close();
        con.close();
    }
}
