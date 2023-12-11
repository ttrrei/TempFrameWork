package org.aquant;

import org.ta4j.core.*;
import org.ta4j.core.indicators.*;
import org.ta4j.core.indicators.adx.ADXIndicator;
import org.ta4j.core.indicators.helpers.ClosePriceIndicator;
import org.ta4j.core.indicators.helpers.*;
import java.sql.*;
import java.time.*;
import java.time.format.*;
import java.util.*;

public class GenerateIndicator {
    public static void driver(HashMap<String, String> configMap) throws Exception {
        //Get tech indicator code execute second
        //Get math indicator code execute last

        populateHeikinAshi(configMap);
        populateTechnicalIndicator(configMap);
    }

    private static void populateTechnicalIndicator(HashMap<String, String> configMap) throws Exception {

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        ArrayList<String> codeList = new ArrayList<>();

        Connection con = DriverManager
                .getConnection(configMap.get("db_url"), configMap.get("db_uname"),  configMap.get("db_pword"));
        con.setAutoCommit(true);
        Statement stm = con.createStatement();

        String listQuery = "select code from tier1.vw_top_trading group by code;";

        ResultSet rSet0 = stm.executeQuery(listQuery);

        while(rSet0.next())
            codeList.add(rSet0.getString(1));

        for ( int i = 0 ; i < codeList.size() ; i ++){

            BarSeries series = new BaseBarSeriesBuilder().build();
            String execQuery = "select * from tier1.vw_full_code_history where code = '"+ codeList.get(i)+"' order by date";
            ResultSet rSet = stm.executeQuery(execQuery);
            while(rSet.next()) {
                ZonedDateTime tempTime = ZonedDateTime.parse(rSet.getString(2) +" 00:00:00", formatter.withZone(ZoneId.systemDefault()));
                series.addBar(tempTime, rSet.getDouble(3), rSet.getDouble(4), rSet.getDouble(5), rSet.getDouble(6), rSet.getInt(7));
            }


            ArrayList<ArrayList<String>> techResult = calculateTechnicalIndicators(series);
            int idx = 0;
            System.out.println(i + "    " + codeList.get(i));
            for ( int j = 0 ; j < techResult.get(0).size(); j ++){

                idx ++;
                String iQuery = configMap.get("query_prefix") + " tier2.technical_indicator "
                                + configMap.get("query_select") + codeList.get(i) + configMap.get("query_midfix") + idx ;
                for ( int k = 0 ; k < techResult.size(); k ++){
                    iQuery += configMap.get("query_midfix") +  techResult.get(k).get(j);
                }
                iQuery += configMap.get("query_postfix");
                stm.execute(iQuery);
            }

        }
        stm.close();
        con.close();

    }

    private static ArrayList<ArrayList<String>> calculateTechnicalIndicators(BarSeries series) {
        ArrayList<ArrayList<String>> ans = new ArrayList<>();


        ClosePriceIndicator closePriceIndicator = new ClosePriceIndicator(series);



//      Very Clear with Completely understood logic
        EMAIndicator xtrmEMA = new EMAIndicator(closePriceIndicator, 5);
        EMAIndicator shortEMA = new EMAIndicator(closePriceIndicator, 10);
        EMAIndicator longEMA = new EMAIndicator(closePriceIndicator, 25);
        EMAIndicator trendEMA = new EMAIndicator(closePriceIndicator, 50);
        MACDIndicator macdIndicator = new MACDIndicator(closePriceIndicator, 10, 25);
        ParabolicSarIndicator parabolicSarIndicator = new ParabolicSarIndicator(series);

//      above or below zero and fast vs slow...
        CMOIndicator shortCMO = new CMOIndicator(closePriceIndicator, 10);
        CMOIndicator longCMO = new CMOIndicator(closePriceIndicator, 25);

//      need EMA on top of RSI ??
        RSIIndicator shortRSI = new RSIIndicator(closePriceIndicator, 10);
        RSIIndicator longRSI = new RSIIndicator(closePriceIndicator, 25);
//      Additional Smooth on RSI ?
        EMAIndicator shortRSIsEMA = new EMAIndicator(shortRSI, 10);
        EMAIndicator shortRSIlEMA = new EMAIndicator(shortRSI, 25);
        EMAIndicator longRSIsEMA = new EMAIndicator(longRSI, 10);
        EMAIndicator longRSIlEMA = new EMAIndicator(longRSI, 25);
//      Need to see how to use it.
        CCIIndicator shortCCI = new CCIIndicator(series,10);
        CCIIndicator longCCI = new CCIIndicator(series,25);
//      Need to see how to use it.
        StochasticOscillatorKIndicator stochK = new StochasticOscillatorKIndicator(series, 10);
        StochasticOscillatorDIndicator stochD = new StochasticOscillatorDIndicator(stochK);
//      Oscillation vs trend
        ChopIndicator sCHOP = new ChopIndicator(series, 10, 0);
//      close price is correct, how to use it
        CoppockCurveIndicator CpCv = new CoppockCurveIndicator(closePriceIndicator, 10,25,15);

        DPOIndicator shotDPO = new DPOIndicator(series,10);
        DPOIndicator longDPO = new DPOIndicator(series,25);

//        ATRIndicator gATR = new ATRIndicator(series, 10);
        PPOIndicator gPPO = new PPOIndicator(closePriceIndicator);
        ROCIndicator sROC = new ROCIndicator(closePriceIndicator,10);
        ROCIndicator lROC = new ROCIndicator(closePriceIndicator,25);
//        TO BE TEST AGAINST 25 ?
        ADXIndicator sADX = new ADXIndicator(series, 10);
        ADXIndicator lADX = new ADXIndicator(series, 25);

        StochasticRSIIndicator sStRSI = new StochasticRSIIndicator(series, 10);
        StochasticRSIIndicator lStRSI = new StochasticRSIIndicator(series, 25);

        ArrayList<String> xEmaList = new ArrayList<>();
        ArrayList<String> sEmaList = new ArrayList<>();
        ArrayList<String> lEmaList = new ArrayList<>();
        ArrayList<String> tEmaList = new ArrayList<>();
        ArrayList<String> macdList = new ArrayList<>();
        ArrayList<String> pSarList = new ArrayList<>();
        ArrayList<String> sCMOList = new ArrayList<>();
        ArrayList<String> lCMOList = new ArrayList<>();
        ArrayList<String> sRSIList = new ArrayList<>();
        ArrayList<String> lRSIList = new ArrayList<>();
        ArrayList<String> sRSIsEMAList = new ArrayList<>();
        ArrayList<String> sRSIlEMAList = new ArrayList<>();
        ArrayList<String> lRSIsEMAList = new ArrayList<>();
        ArrayList<String> lRSIlEMAList = new ArrayList<>();
        ArrayList<String> sCCIList = new ArrayList<>();
        ArrayList<String> lCCIList = new ArrayList<>();
        ArrayList<String> stcKList = new ArrayList<>();
        ArrayList<String> stcDList = new ArrayList<>();
        ArrayList<String> cpcvList = new ArrayList<>();
        ArrayList<String> sDPOList = new ArrayList<>();
        ArrayList<String> lDPOList = new ArrayList<>();

        ArrayList<String> gPPOList = new ArrayList<>();
        ArrayList<String> sROCList = new ArrayList<>();
        ArrayList<String> lROCList = new ArrayList<>();
        ArrayList<String> sADXList = new ArrayList<>();
        ArrayList<String> lADXList = new ArrayList<>();
        ArrayList<String> sStRSIList = new ArrayList<>();
        ArrayList<String> lStRSIList = new ArrayList<>();


        for (int i = 0 ; i < series.getBarCount(); i ++) {

            xEmaList.add(String.format("%.3f", xtrmEMA.getValue(i).doubleValue()));
            sEmaList.add(String.format("%.3f", shortEMA.getValue(i).doubleValue()));
            lEmaList.add(String.format("%.3f", longEMA.getValue(i).doubleValue()));
            tEmaList.add(String.format("%.3f", trendEMA.getValue(i).doubleValue()));
            macdList.add(String.format("%.3f", macdIndicator.getValue(i).doubleValue()));
            pSarList.add(String.format("%.3f", parabolicSarIndicator.getValue(i).doubleValue()));
            sCMOList.add(String.format("%.3f", shortCMO.getValue(i).doubleValue()));
            lCMOList.add(String.format("%.3f", longCMO.getValue(i).doubleValue()));
            sRSIList.add(String.format("%.3f", shortRSI.getValue(i).doubleValue()));
            lRSIList.add(String.format("%.3f", longRSI.getValue(i).doubleValue()));
            sRSIsEMAList.add(String.format("%.3f", shortRSIsEMA.getValue(i).doubleValue()));
            sRSIlEMAList.add(String.format("%.3f", shortRSIlEMA.getValue(i).doubleValue()));
            lRSIsEMAList.add(String.format("%.3f", longRSIsEMA.getValue(i).doubleValue()));
            lRSIlEMAList.add(String.format("%.3f", longRSIlEMA.getValue(i).doubleValue()));
            sCCIList.add(String.format("%.3f", shortCCI.getValue(i).doubleValue()));
            lCCIList.add(String.format("%.3f", longCCI.getValue(i).doubleValue()));
            stcKList.add(String.format("%.3f", stochK.getValue(i).doubleValue()));
            stcDList.add(String.format("%.3f", stochD.getValue(i).doubleValue()));
            cpcvList.add(String.format("%.3f", CpCv.getValue(i).doubleValue()));
            sDPOList.add(String.format("%.3f", shotDPO.getValue(i).doubleValue()));
            lDPOList.add(String.format("%.3f", longDPO.getValue(i).doubleValue()));

            gPPOList.add(String.format("%.3f", gPPO.getValue(i).doubleValue()));
            sROCList.add(String.format("%.3f", sROC.getValue(i).doubleValue()));
            lROCList.add(String.format("%.3f", lROC.getValue(i).doubleValue()));
            sADXList.add(String.format("%.3f", sADX.getValue(i).doubleValue()));
            lADXList.add(String.format("%.3f", lADX.getValue(i).doubleValue()));
            sStRSIList.add(String.format("%.3f", sStRSI.getValue(i).doubleValue()));
            lStRSIList.add(String.format("%.3f", lStRSI.getValue(i).doubleValue()));

        }

        ans.add(xEmaList);
        ans.add(sEmaList);
        ans.add(lEmaList);
        ans.add(tEmaList);
        ans.add(macdList);
        ans.add(pSarList);
        ans.add(sCMOList);
        ans.add(lCMOList);
        ans.add(sRSIList);
        ans.add(lRSIList);
        ans.add(sRSIsEMAList);
        ans.add(sRSIlEMAList);
        ans.add(lRSIsEMAList);
        ans.add(lRSIlEMAList);
        ans.add(sCCIList);
        ans.add(lCCIList);
        ans.add(stcKList);
        ans.add(stcDList);
        ans.add(cpcvList);
        ans.add(sDPOList);
        ans.add(lDPOList);

        ans.add(gPPOList);
        ans.add(sROCList);
        ans.add(lROCList);
        ans.add(sADXList);
        ans.add(lADXList);
        ans.add(sStRSIList);
        ans.add(lStRSIList);

        return ans;

    }

    private static void populateHeikinAshi(HashMap<String, String> configMap) throws Exception {

        ArrayList<String> codeList = new ArrayList<>();

        Connection con = DriverManager
                .getConnection(configMap.get("db_url"), configMap.get("db_uname"),  configMap.get("db_pword"));
        con.setAutoCommit(true);
        Statement stm = con.createStatement();

        String listQuery = "select code from tier1.vw_top_trading group by code;";

        ResultSet rSet0 = stm.executeQuery(listQuery);

        while(rSet0.next())
            codeList.add(rSet0.getString(1));

        for (int i = 0 ; i < codeList.size() ; i ++){
            String execQuery = "select tier1.calculate_heikin_ashi('" +codeList.get(i)+ "');";
            stm.execute(execQuery);
        }

        stm.close();
        con.close();
    }
}
