package cn.analysys;

import org.json.JSONObject;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MysqlToHBase {
    HBaseOperation HBaseOpe;

    {
        try {
            HBaseOpe = new HBaseOperation ();
        } catch (Exception e) {
            e.printStackTrace ();
        }
    }

    private static MysqLConnection conn = new MysqLConnection ();


    public  Map<String,JSONObject> cityToHBase(String cityOpeSql)throws Exception{
        //TODO 分别获取每个城市的基本信息（id,name,classify,parent_id,shape）
        Map<String,JSONObject> chinaCityMap = new HashMap<String, JSONObject> ();
        ResultSet rs = conn.Search (cityOpeSql,null);
        ResultSetMetaData metaData = rs.getMetaData ();
        int columnNum = metaData.getColumnCount ();
        Map<String, JSONObject> cityValueMap = new HashMap<String, JSONObject> ();
        while (rs.next ()){
            String cityMapKey = rs.getString ("id")+"_"+rs.getString ("name") ;
            Map<String,Object> rowValueMap = new HashMap<String, Object> ();
            for(int i=1;i<=columnNum;i++){
                String mapKey = metaData.getColumnName (i);
                rowValueMap.put (mapKey,rs.getObject (mapKey));
            }
            JSONObject rowValueJson = new JSONObject (rowValueMap);
            cityValueMap.put (cityMapKey,rowValueJson);
        }
        JSONObject cityJson = new JSONObject (cityValueMap);
        chinaCityMap.put ("China_City",cityJson);
        return  chinaCityMap;
    }

    public Map<String,JSONObject> circleToHBase(String citySql,String circleSql)throws Exception{
        //TODO 1获取所有的城市ID
        List<String> cityIDList = new ArrayList<String> ();
        ResultSet cityIDRS = conn.Search (citySql,null);
        //将结果集转为List
        while(cityIDRS.next ()){
            cityIDList.add (String.valueOf (cityIDRS.getObject (1)));
        }
       // System.out.println (cityIDList.size ());


        //TODO 2分别获取每个城市的Circle
        Map<String,JSONObject> cityCircleMap= new HashMap<String,JSONObject> ();
        for(String cityID:cityIDList){
            String[] ID= {cityID};
            ResultSet rs = conn.Search (circleSql,ID);
            ResultSetMetaData metaData = rs.getMetaData ();
            int columnNum = metaData.getColumnCount ();
            Map<String, JSONObject> circleValueMap = new HashMap<String, JSONObject> ();
            while (rs.next ()){
                String circleMapKey = String.valueOf (cityID+"_"+rs.getString ("id"));
                Map<String,Object> rowValueMap = new HashMap<String, Object> ();
                for(int i=1;i<=columnNum;i++){
                    String mapKey = metaData.getColumnName (i);
                    rowValueMap.put (mapKey,rs.getObject (mapKey));
                }
                JSONObject rowValueJson = new JSONObject (rowValueMap);
                circleValueMap.put (circleMapKey,rowValueJson);
            }
            JSONObject circleJson = new JSONObject (circleValueMap);
            cityCircleMap.put (String.valueOf (cityID),circleJson);
        }
        return cityCircleMap;
    }

    public static void main(String[] args) throws Exception {
        HBaseOperation HBaseOpe = new HBaseOperation ();
        HBaseOpe.getAllTables();
        MysqlToHBase m2h = new MysqlToHBase ();

        //导入城市信息到HBase——tmp_wuna_location
        /*String cityOpeSql  = "select * from analysys_multipolygon.china_city ";
        HBaseOpe.putData ("tmp_wuna_china_location","city",m2h.cityToHBase (cityOpeSql));*/


        //导入商圈信息到HBase——tmp_wuna_location
        String citySql = "select DISTINCT parent_id  from analysys_multipolygon.china_polygon_20190211";
        String circleSql  = "select id,name,classify,shape,coordinate " +
                "from analysys_multipolygon.china_polygon_20190211 " +
                "where parent_id = ?";
        HBaseOpe.putData ("tmp_wuna_china_location","circle",m2h.circleToHBase(citySql,circleSql));
        }



        //hbaseTest.createTable("tmp_test", new String[]{"city","circle"});
        /*hbaseTest.deleteTable("bd17:fromjava");
        hbaseTest.putData();
        hbaseTest.getData();
        hbaseTest.cleanUp();*/

    }

