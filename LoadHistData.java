package org.aquant;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.ibatis.jdbc.ScriptRunner;

import javax.net.ssl.HttpsURLConnection;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class LoadHistData {

    public static void driver(HashMap<String, String> configMap) throws Exception {

        ArrayList<String> codeList = new ArrayList<>();

        Connection con = DriverManager
                .getConnection(configMap.get("db_url"), configMap.get("db_uname"),  configMap.get("db_pword"));
        con.setAutoCommit(false);
        Statement stm = con.createStatement();

        String listQuery = "select code from tier1.code_list;";

        ResultSet rSet0 = stm.executeQuery(listQuery);

        while(rSet0.next()) {
            codeList.add(rSet0.getString(1));
        }


        for ( int i = 0 ; i < codeList.size() ; i ++){
            System.out.println(codeList.get(i));
            String sourceLink = configMap.get("code_prefix") +codeList.get(i)+ configMap.get("code_midfix")
                    + "&num_rows=" + configMap.get("row_count") + "&range_days=" + configMap.get("row_count")
                    + "&startDate=" + configMap.get("start_date") + "&endDate=" + configMap.get("end_date") ;

            URL url = new URL(sourceLink);
            HttpsURLConnection hConnection = (HttpsURLConnection)url.openConnection();
            hConnection.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.95 Safari/537.11");
            hConnection.setConnectTimeout(60000);
            BufferedReader bReader = new BufferedReader(new InputStreamReader(hConnection.getInputStream()));

            CSVParser cParser = new CSVParser(bReader, CSVFormat.EXCEL
                    .withDelimiter(configMap.get("source_delimiter").charAt(0))
                    .withFirstRecordAsHeader()
                    .withIgnoreHeaderCase()
                    .withTrim());
            List<CSVRecord> cRecords = cParser.getRecords();

            for (int j = 0 ; j < cRecords.size() ; j++) {
                CSVRecord tempRecord = cRecords.get(j);
                String query = configMap.get("query_prefix")  + " tier1.full_code_history "
                        + configMap.get("query_select")
                        + codeList.get(i) + configMap.get("query_midfix");
                for ( int k = 0 ; k <tempRecord.size() ; k ++){
                    query += tempRecord.get(k).replaceAll("'", "''") + configMap.get("query_midfix");
                }

                query = query.substring(0, query.length() - 3) + configMap.get("query_postfix");
                stm.addBatch(query);
//                System.out.println(query);
            }

            stm.executeBatch();
            con.commit();
        }

        stm.close();
        con.close();

    }
}
