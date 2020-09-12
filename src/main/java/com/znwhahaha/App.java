package com.znwhahaha;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.*;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;


/**
 * Hello world!
 *
 */
public class App 
{
    public static Configuration configuration;
    public static Connection conn;
    public static void init() throws IOException {
        configuration = HBaseConfiguration.create();
        conn = ConnectionFactory.createConnection(configuration);
    }

    /**
     * @ClassName : App
     * @Description : 功能说明:获取hbase表中指定年份的信息，返回结果为提取的论文分类号。
     * @param tablename
     * @param year
     * @Return : java.util.ArrayList<java.lang.String>
     * @Author : ZNWhahaha
     * @Date : 2020/9/1
    */
    public static ArrayList<String> GetHbaseTableCell(String tablename, String year) throws IOException {
        
        Table table = null;
        //标志位用于判断所循环到的cell年份是否正确
        Boolean flag = false;
        //临时存储sortnumber，当flag == true，既年份正确时，放入List中。
        String temporarysortnum = "";

        ArrayList<String> sortnumbers = new ArrayList<>();
        table = conn.getTable(TableName.valueOf(tablename));
        ResultScanner results = table.getScanner(new Scan());
        for (Result result : results){
            flag = false;
            for (Cell cell : result.rawCells()){
                String colName = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());

                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());

                if (colName.equals("time") && value.contains(year)){
                    //System.out.println(colName+"  "+value);
                    flag = true;
                }
                if (colName.equals("sortnumber")){
                    //sortnumbers.add(value);
                    temporarysortnum = value;
                }
            }
            if(flag == true){
                sortnumbers.add(temporarysortnum);
                //System.out.println("选中的sortnumber： "+temporarysortnum);
            }

        }
        return sortnumbers;
    }

    /**
     * @ClassName : App
     * @Description : 功能说明：将计算好的热点数写入Mysql数据库中
     * @param item

     * @Return : void
     * @Author : ZNWhahaha
     * @Date : 2020/9/2
    */
    public static void PutMysql(HotSpotItem item) throws ClassNotFoundException, SQLException {

        String jdbcname = "com.mysql.cj.jdbc.Driver";
        String sql = "insert into pf_hot_spots values(?,?,?,?,?,?,?)";

        //链接数据库
        Class.forName(jdbcname);
        java.sql.Connection con = DriverManager.getConnection("jdbc:mysql://10.0.252.45:3306/zldata?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC&useUnicode=true&characterEncoding=utf8","root","bigdata302");

        //存入item数据
        PreparedStatement preparedStatement = con.prepareStatement(sql);
        preparedStatement.setString(1, item.getId());
        preparedStatement.setString(2,item.getYears());
        preparedStatement.setString(3, item.getDatas());
        preparedStatement.setString(4,item.getParentid());
        preparedStatement.setString(5,item.getDeep());
        preparedStatement.setString(6,item.getTyprs());
        preparedStatement.setString(7,item.getName());

        preparedStatement.executeUpdate();
        con.close();
    }

    /**
     * @ClassName : App
     * @Description : 功能说明：计算小类的数值
     * @param hotspot
     * @param arrayList
     * @Return : void
     * @Author : ZNWhahaha
     * @Date : 2020/9/3
    */
    public static void DatasCount(String[][] hotspot,ArrayList<String> arrayList){
        String[] sortnumbs;
//        for (String al : arrayList){
//            sortnumbs = al.split("\\s+|\\t");
//            for (String ss: sortnumbs){
//                for (int i = 0; i < hotspot.length; i++) {
//                      if (Integer.valueOf(hotspot[i][0]) <100 && hotspot[i][4].contains(ss)){
//                          hotspot[i][5] = String.valueOf(Integer.valueOf(hotspot[i][5]) + 1);
//                      }
//                }
//            }
//        }
        for (String al : arrayList){
            for (int i = 0; i < hotspot.length; i++) {
                sortnumbs = hotspot[i][4].split("、");
                for (String ss : sortnumbs){
                    if (Integer.valueOf(hotspot[i][0]) < 100 && al.contains(ss)){
                        hotspot[i][5] = String.valueOf(Integer.valueOf(hotspot[i][5]) + 1);
                    }
                }

            }

        }
    }

    /**
     * @ClassName : App
     * @Description : 功能说明:当小类全部统计完毕后，计算大类
     * @param hotspot

     * @Return : void
     * @Author : ZNWhahaha
     * @Date : 2020/9/9
    */
    public static void DatasCount_Big(String[][] hotspot){
        for (int i = 1 ; i < 11 ; i++){
            int num = 0;
            for (int j = 0; j < hotspot.length - 10; j++) {
                if (hotspot[j][1].equals(hotspot[hotspot.length-i][0])){  //怀疑此处有问题
                    num += Integer.valueOf(hotspot[j][5]);
                }
            }
            hotspot[i][5] = String.valueOf(num);
        }
    }

    /**
     * @ClassName : App
     * @Description : 功能说明:对领域分类号文件ComparisonTable文件进行解析，将解析结果存储至二维数组中
     * @param

     * @Return : java.lang.String[][]
     *              返回二维数组的结构：行：科技热点领域
     *                              列：id   parent_id   deep   name   sortnumber   data
     * @Author : ZNWhahaha
     * @Date : 2020/9/2
    */
    public static String[][] ParsingTxTFile() throws IOException {
        String encoding = "UTF-8";
        String[][] hotspot = new String[66][6];
        int index = 0;
        File file = new File("/root/Hotspots/ComparisonTable.txt");
        if (file.isFile() && file.exists()){
            InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream(file),encoding);
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            String lineTxt = "";
            while((lineTxt = bufferedReader.readLine()) != null && index < hotspot.length){
//                System.out.println(lineTxt);
                hotspot[index] = lineTxt.split("\\s+|\\t");
                
                index++;
            }
            inputStreamReader.close();
        }
        else {
            System.out.println("找不到指定的文件");
        }
        return hotspot;
    }

    /**
     * @ClassName : App
     * @Description : 参数说明：     0 -> hbase表名
     *                             1 -> 开始年份
     *                             2 -> 结束年份
     * @param args

     * @Return : void
     * @Author : ZNWhahaha
     * @Date : 2020/9/3
    */
    public static void main( String[] args ) throws IOException, SQLException, ClassNotFoundException {
        WriteRunLog.writeToFiles("开始执行热点搜集程序");

        if (args.length != 3) {
            WriteRunLog.writeToFiles("抱歉！没有输入正确参数(参数为:Hbase表名、开始年份、结束年份)");
            System.exit(0);
        }

        HotSpotItem item = new HotSpotItem();
        String htablename = args[0];
        int beginyear = Integer.valueOf(args[1]);
        int endyear = Integer.valueOf(args[2]);

        //初始化hbase连接环境
        init();

        //提取txt文件内容
        String[][] hotspot = ParsingTxTFile();
        //GetHbaseTableCell(htablename,"2017");
        //按年份逐步执行
        for (int i = beginyear; i <= endyear; i++) {
            //获取对应年份的图书分类号
            ArrayList<String> sortnumbers = GetHbaseTableCell(htablename,String.valueOf(i));
            //对论文的图书分类号进行计数，并将结果保存在hotspot数组中
            DatasCount(hotspot,sortnumbers);
            DatasCount_Big(hotspot);
            //提取hotspot数组信息传递至HotSpotItem中，再存储至Mysql数据库中
            for (String[] mem :hotspot){
                item.setId(mem[0]);
                item.setYears(String.valueOf(i));
                item.setDatas(mem[5]);
                item.setParentid(mem[1]);
                item.setDeep(mem[2]);
                item.setTyprs("领域");
                item.setName(mem[3]);
                if(mem[3] != null && mem[3] != ""){
                    PutMysql(item);
                }

           }
        }
        WriteRunLog.writeToFiles("热点搜集程序执行完成");
    }
}
