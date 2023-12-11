package org.aquant;

import com.google.gson.*;
import org.apache.commons.math3.fitting.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class MakeMathIndicator {

    public static void driver(HashMap<String, String> configMap)  throws Exception {
//        makeSource(configMap);
        makeTarget(configMap);
    }

    private static void makeTarget(HashMap<String, String> configMap)  throws Exception {

        ArrayList<String> attrList = new ArrayList<>(
                Arrays.asList("xema", "sema", "lema", "tema"));
        ArrayList<String> lensList = new ArrayList<>(
                Arrays.asList("5", "10", "25", "50"));

        for ( int i = 0 ; i < attrList.size() ; i ++)
            for ( int j = 0 ; j < lensList.size(); j ++)
                getTarget(configMap, attrList.get(i), lensList.get(j));

    }

    private static void getTarget(HashMap<String, String> configMap, String attr, String lens)  throws Exception {

        ArrayList<String> queryList = new ArrayList<String>();

        Connection con = DriverManager
                .getConnection(configMap.get("db_url"), configMap.get("db_uname"),  configMap.get("db_pword"));
        con.setAutoCommit(true);
        Statement stm = con.createStatement();

        String extractQuery = "select entity, idx, evaluation from tier2.eavt_source" +
                " where attribute = '" + attr + "' and len = " + lens;

        System.out.println(extractQuery);
        ResultSet rs = stm.executeQuery(extractQuery);

        while (rs.next()) {
            JsonParser jParser = new JsonParser();
            String code = rs.getString(1);
            String date = rs.getString(2);
            String gson = rs.getString(3);

            JsonArray linear = curveFitting(jParser.parse(gson).getAsJsonArray(), 1);
            JsonArray quadratic = curveFitting(jParser.parse(gson).getAsJsonArray(), 2);

            String inputQuery = "insert into tier2.eavt_target select '" + code + "','" + date + "','" + lens + "','"
                    + attr + "', 'linear', '" + linear + "'::jsonb"
                    + " union select '" + code + "','" + date + "','" + lens + "','"
                    + attr + "', 'quadratic', '" + quadratic + "'::jsonb";
            queryList.add(inputQuery);

            System.out.println(inputQuery);

        }

        for ( int i = 0 ; i < queryList.size() ; i ++) {
            System.out.println(queryList.get(i));
            stm.execute(queryList.get(i));
        }

        stm.close();
        con.close();

    }

    public static JsonArray curveFitting (JsonArray jArray, Integer degree){
        JsonArray ans = new JsonArray();

        WeightedObservedPoints obs = new WeightedObservedPoints();
        double lastValue = jArray.get(jArray.size()-1).getAsDouble();
        for ( int i = 0; i < jArray.size(); i ++){
            int x = i-jArray.size() + 1;
//            System.out.println((jArray.get(i).getAsDouble()-lastValue)/lastValue*50);
            if (!jArray.get(i).isJsonNull()){
                obs.add(x,(jArray.get(i).getAsDouble()-lastValue)/lastValue*50);

            }

        }
        if (obs.toList().size()>0){
            PolynomialCurveFitter fitter = PolynomialCurveFitter.create(degree);
            double[] coeff = fitter.fit(obs.toList());


            for ( int i = 0; i < coeff.length; i  ++){
                ans.add(coeff[i]);
            }
            return ans;
        }else
            return ans;
    }

    public static void makeSource(HashMap<String, String> configMap) throws Exception {

        ArrayList<String> indictors = new ArrayList<>(
                    Arrays.asList("xema", "sema", "lema", "tema"));

        ArrayList<String> length = new ArrayList<>(
                Arrays.asList("5", "10", "25", "50"));

        Connection con = DriverManager
                .getConnection(configMap.get("db_url"), configMap.get("db_uname"),  configMap.get("db_pword"));
        con.setAutoCommit(true);
        Statement stm = con.createStatement();

        String listQuery = "select code from tier2.vw_technical_indicator group by code";

        ArrayList<String> codeList = new ArrayList<>();

        ResultSet rSet0 = stm.executeQuery(listQuery);

        while(rSet0.next())
            codeList.add(rSet0.getString(1));


        for ( int i = 0 ; i < codeList.size() ; i ++){

            for (int j = 0 ; j < indictors.size(); j ++) {

                for ( int k = 0 ; k < length.size() ; k ++ ) {
                    String execQuery = "select tier2.reformat_eavt('" + codeList.get(i)
                            + configMap.get("query_midfix") + indictors.get(j) + "', "+length.get(k)+")";
                    System.out.println(execQuery);
                    stm.execute(execQuery);

                }


            }
        }
        stm.close();
        con.close();

    }
}
