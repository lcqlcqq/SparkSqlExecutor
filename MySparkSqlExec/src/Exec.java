import com.clearspring.analytics.util.Lists;

import java.util.List;
import java.util.Scanner;

public class Exec {
    public static void main(String[] args) {

        System.out.println("start");

        Main.init();

        Scanner sc = new Scanner(System.in);
        System.out.print("-> ");
        while(sc.hasNext()){
            try {
                //获取输入sql
                String input = sc.nextLine();
                String tableName = "";
                if (input.contains("from")) {
                    int i = input.indexOf("from") + 4;
                    while (input.charAt(i) == ' ') i++;
                    int j = input.indexOf(' ', i);
                    tableName = input.substring(i, j);
                }
                List<String> tableCols;
                if (input.contains("*")) {
                    tableCols = getAllCols(tableName);
                } else {
                    tableCols = getCols(tableName, input);
                }
                //执行sql语句
                Main.executeSql(input, tableCols);
                System.out.print("-> ");
            }
            catch (Exception e){
                System.out.println(e.getMessage());
                System.out.print("-> ");
            }
        }
    }

    //获取表的所有字段
    public static List<String> getAllCols(String table){
        Iterable<String> allCols = Main.getAllCols(table);
        List<String> lst = Lists.newArrayList();
        allCols.forEach(lst::add);
        return lst;
    }
    //获取sql语句里表所需的字段
    public static List<String> getCols(String table,String sql){
        Iterable<String> cols = Main.getCols(table, sql);
        List<String> lst = Lists.newArrayList();
        cols.forEach(lst::add);
        return lst;
    }

}
