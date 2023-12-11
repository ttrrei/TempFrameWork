package org.aquant;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;

import java.io.File;
import java.util.HashMap;

public class Main {

    public static void main(String[] args) throws Exception {

        Configurations configs = new Configurations() ;
        Configuration config = configs.properties(new File(args[0]));
        HashMap<String, String> configMap = new HashMap<>();

        configMap.put("source_delimiter",config.getString("source_delimiter"));
        configMap.put("code_prefix",config.getString("code_prefix"));
        configMap.put("code_midfix",config.getString("code_midfix"));
        configMap.put("index_prefix",config.getString("index_prefix"));
        configMap.put("index_posfix",config.getString("index_posfix"));
        configMap.put("db_url",config.getString("db_url"));
        configMap.put("db_uname",config.getString("db_uname"));
        configMap.put("db_pword",config.getString("db_pword"));
        configMap.put("start_date",config.getString("start_date"));
        configMap.put("end_date",config.getString("end_date"));
        configMap.put("row_count",config.getString("row_count"));
        configMap.put("initial_sql",config.getString("initial_sql"));
        configMap.put("query_prefix",config.getString("query_prefix"));
        configMap.put("query_select",config.getString("query_select"));
        configMap.put("query_midfix",config.getString("query_midfix"));
        configMap.put("query_postfix",config.getString("query_postfix"));
//        configMap.put("db_name",config.getString("db_name"));
//        configMap.put("tbl_name",config.getString("tbl_name"));
//        configMap.put("code_table",config.getString("code_table"));

//        IntialDatabase.driver(configMap);
//        LoadHistData.driver(configMap);
//        TODO Load Data from public to Tier 1
        GenerateIndicator.driver(configMap);
//          MakeMathIndicator.driver(configMap);
//        TODO

    }

}
